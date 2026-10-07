"""Fast-trigger entry rungs — the 250ms sweep + the WS book-event push.

Why this exists (on-chain miss forensic, 2026-07-30, n=45 rejects): 84% of our
misses were consumed by a FASTER TAKER, median 1.0s before our arrival, ~5x our
size, at exactly our quoted ask. Our POST path is already at the venue floor
(sign ~1ms, POST ~300ms of which 250ms is the taker delay every taker pays), so
the only reclaimable latency is TRIGGER-TO-DECISION. The scanner samples once
per second on a boundary-aligned tick, so a fillable ask that appears mid-second
waits ~500ms on average before we even look at it.

Two extra trigger rungs, both converging on ONE evaluation path
(:func:`cryptobot.execution.pipeline.run_fast_entry_scan`, which is the tick's
own scoring + resolver + ``_commit_cell``):

* **sweep** — an APScheduler job at ``CRYPTOBOT_FAST_TRIGGER_MS`` (default
  250ms, clamped to >=100ms). Cheap-guard-first: outside a qualifying offset
  window :func:`fast_entry_candidate_sleeves` returns empty after pure integer
  math and the fire costs nothing. Mean sampling delay 500ms -> ~125ms.
* **event** — the venue WS book mirror fires a narrow ask-side callback; a
  dedicated coalescing worker thread runs the same scan within ~20ms of the
  push. Separately killable via ``CRYPTOBOT_FAST_TRIGGER_EVENT_ENABLED``
  because it is the rung that touches the venue's WS reader thread.

Thread model — the WS reader thread must NEVER do heavy work (it is the same
thread that feeds every book read in the process; blocking it silently degrades
the whole bot to REST). :meth:`FastEntryTrigger.on_ask_update` is therefore two
attribute reads plus an ``Event.set()``; all real work happens on this module's
own daemon worker thread.

Nothing here is venue-specific: the trigger logic reads only core config +
the portfolio's sleeve offset sets, and the event rung is registered through
``BookSourceChain.set_ask_update_callback``, which fans the hook out over
whichever sources expose one (so a venue whose mirror has no such hook simply
runs the sweep rung alone).

Per-second work (2026-07-30 latency pass) — the trigger side owns ONE piece of
it: :meth:`FastEntryTrigger._deps_for` builds the ``TickDeps`` bundle once per
integer second instead of once per fire. That is not just a saved dataclass
construction: the scan's own per-second caches
(``pipeline._fast_resolver_deps`` and ``pipeline._FastCommitStage``) key on the
bundle's IDENTITY, so a fresh object per fire defeated them outright. The rest
of the precompute lives in ``execution.pipeline`` where the scoring and the
commit stage are.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from cryptobot import config
from cryptobot.execution.pipeline import (
    TickDeps,
    fast_entry_candidate_sleeves,
    run_fast_entry_scan,
)
from cryptobot.strategy.portfolio_loader import V6Portfolio

logger = logging.getLogger(__name__)

ConnFactory = Callable[[], sqlite3.Connection]

# Sweep cadence floor. The 100ms fast-requote drain job is the proven floor for
# an APScheduler job on this box; below it the guard itself starts to cost more
# than the sampling delay it removes.
MIN_TRIGGER_MS = 100

# Event-rung minimum inter-scan gap. Derived from the sweep cadence rather than
# a fourth env knob: at the 250ms default this caps the event rung at ~16
# scans/s while still firing ~immediately for the first push after a quiet
# stretch (the gap only bites when the book is churning, and consecutive scans
# on a churning book would re-read nearly the same ladder anyway). Floored at
# 20ms — the reaction target from the forensic.
EVENT_MIN_GAP_FLOOR_S = 0.020


def fast_trigger_seconds(ms: Optional[int] = None) -> float:
    """Sweep cadence in seconds, clamped to >= :data:`MIN_TRIGGER_MS`.

    ``IntervalTrigger`` accepts float seconds, so 250ms is expressed as 0.25.
    """
    raw = config.FAST_TRIGGER_MS if ms is None else ms
    try:
        as_int = int(raw)
    except (TypeError, ValueError):
        as_int = MIN_TRIGGER_MS
    return max(MIN_TRIGGER_MS, as_int) / 1000.0


def event_min_gap_seconds() -> float:
    """Minimum gap between two event-rung scans (see :data:`EVENT_MIN_GAP_FLOOR_S`)."""
    return max(EVENT_MIN_GAP_FLOOR_S, fast_trigger_seconds() / 4.0)


@dataclass
class FastEntryTrigger:
    """Both fast rungs over one shared single-cell evaluation.

    ``conn_factory`` returns the CALLING thread's own sqlite3 connection (the
    ``ThreadLocalConnections`` handle) — the sweep job runs on an APScheduler
    pool thread and the event worker on its own thread, so each resolves its own
    connection exactly like the wal_checkpoint / requote_drain / held_position_fast
    jobs (audit finding #1).

    ``book_source`` is the ``BookSourceChain``; it is read only for
    ``noted_tokens()`` (the current entry universe) to filter WS pushes.
    """

    conn_factory: ConnFactory
    portfolio: V6Portfolio
    deps_factory: Callable[[], TickDeps]
    book_source: Optional[object] = None
    now_fn: Callable[[], float] = time.time
    monotonic_fn: Callable[[], float] = time.monotonic

    # --- event-rung state -------------------------------------------------
    # Tokens the tick has subscribed for the current (+ next) window. Refreshed
    # by the sweep; an empty set means "unknown" and disables the filter rather
    # than dropping every push.
    _universe: frozenset = field(default_factory=frozenset, repr=False)
    # Whether the last sweep saw a qualifying offset window. Read by the WS
    # callback as a single atomic attribute load. At most one sweep-cadence
    # stale, which can only cost a wasted wake — the worker re-derives the real
    # gate before scanning (``run_fast_entry_scan`` returns immediately when
    # ``fast_entry_candidate_sleeves`` is empty).
    _in_window: bool = field(default=False, repr=False)
    #: The exact tuple `_universe` was built from, compared by IDENTITY so an
    #: unchanged noted-token set costs nothing to re-check (see _refresh_universe).
    _noted_ref: object = field(default=None, repr=False)
    _wake: threading.Event = field(default_factory=threading.Event, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _worker: Optional[threading.Thread] = field(default=None, repr=False)
    _last_event_scan_m: float = field(default=0.0, repr=False)
    #: Monotonic stamp of the push that set :attr:`_wake` (see
    #: :meth:`on_ask_update`). Written on the WS recv thread, read on the worker
    #: — a plain float store/load, so a race can only mis-attribute ONE
    #: telemetry line by the inter-push interval, never affect a decision.
    _push_m: float = field(default=0.0, repr=False)
    #: Per-second TickDeps prebuild (latency, 2026-07-30). ``deps_factory()``
    #: constructs a multi-field dataclass per call; at ~16 fires/s that is pure
    #: repeat work, and the SCAN's own per-second caches
    #: (``pipeline._fast_resolver_deps`` / ``pipeline._FastCommitStage``) key on
    #: the bundle's IDENTITY, so a fresh object per fire defeated them outright.
    #: Rebuilt when the integer second advances.
    _deps_second: int = field(default=-1, repr=False)
    _deps_cached: Optional[TickDeps] = field(default=None, repr=False)
    # Counters for the operator log / tests. Never persisted — the fast rungs
    # write DB rows ONLY when something places or rejects (the tick's policy);
    # a per-firing row at 4Hz+ would be pure noise.
    events_seen: int = 0
    events_dropped: int = 0
    sweep_scans: int = 0

    # ------------------------------------------------------------------
    # Sweep rung
    # ------------------------------------------------------------------
    def run_sweep(self) -> dict:
        """The APScheduler job body. Never raises."""
        try:
            now_ts = int(self.now_fn())
            candidates = fast_entry_candidate_sleeves(self.portfolio, now_ts)
            self._in_window = bool(candidates)
            if not candidates:
                return {}
            self._refresh_universe()
            self.sweep_scans += 1
            # Hand the candidate list straight in — the guard above already paid
            # for it, and the scan would otherwise recompute the same thing.
            return self._scan("fast_sweep", now_ts=now_ts, candidates=candidates)
        except Exception:  # noqa: BLE001 — a job must never crash the scheduler
            logger.warning("fast_entry sweep raised", exc_info=True)
            return {}

    def _refresh_universe(self) -> None:
        """Re-read the chain's noted token set (the WS push filter).

        Skipped entirely when the event rung is off: the set exists ONLY to
        filter :meth:`on_ask_update`, so with no event rung this is a pure cost
        at the sweep cadence. Guarded on tuple identity because the tick
        re-notes the SAME tuple almost every second — rebuilding the frozenset
        4x/s off an unchanged tuple is work for nothing.
        """
        src = self.book_source
        if src is None or not config.FAST_TRIGGER_EVENT_ENABLED:
            return
        getter = getattr(src, "noted_tokens", None)
        if getter is None:
            return
        try:
            noted = getter() or ()
        except Exception:  # noqa: BLE001 — a bad source must never break the job
            logger.debug("fast_entry noted_tokens raised", exc_info=True)
            return
        if noted is self._noted_ref:
            return
        self._noted_ref = noted
        self._universe = frozenset(noted)

    # ------------------------------------------------------------------
    # Event rung
    # ------------------------------------------------------------------
    def on_ask_update(self, token: str, _venue_ts: float) -> None:
        """WS-reader-thread callback. MUST stay O(1) and non-blocking.

        Runs on the venue mirror's daemon recv thread at the measured ~822
        msg/s, so it does exactly three things: gate on the current entry
        universe, gate on the (sweep-maintained) in-window flag, and wake the
        worker. Everything else — scoring, book reads, DB, the order POST —
        happens on :meth:`_event_loop`'s own thread.

        ``_venue_ts`` is part of the mirror's callback contract but unused here:
        the worker re-derives everything from its own clock, and reading a
        venue stamp on the recv thread would buy nothing.

        One monotonic read was added (2026-07-30) AFTER both gates, so it is paid
        only by a push that actually wakes the worker (not by the ~822 msg/s that
        get dropped). It is what makes ``push_to_scan_ms`` measurable — without
        it the whole trigger-side segment (worker wake + the
        :func:`event_min_gap_seconds` throttle, up to 62ms at the 250ms default)
        is invisible, and that segment is currently the single largest
        UNMEASURED piece of push-to-POST.
        """
        self.events_seen += 1
        universe = self._universe
        if universe and token not in universe:
            self.events_dropped += 1
            return
        if not self._in_window:
            self.events_dropped += 1
            return
        self._push_m = self.monotonic_fn()
        self._wake.set()

    def start_event_worker(self) -> None:
        """Start the coalescing worker. Idempotent."""
        if self._worker is not None and self._worker.is_alive():
            return
        self._stop.clear()
        worker = threading.Thread(
            target=self._event_loop,
            name="cryptobot-fast-entry",
            daemon=True,
        )
        self._worker = worker
        worker.start()

    def _event_loop(self) -> None:
        """Wait for a push, then run ONE scan; coalesce whatever arrived while
        the scan was running into a single follow-up scan.

        ``Event`` (not a queue) IS the coalescing: N pushes between two scans
        collapse to one wake, so a churning book can never build a backlog. The
        flag is cleared BEFORE the scan, so a push that lands mid-scan re-sets it
        and the next iteration re-scans immediately — which is correct, that push
        carries a ladder the finished scan never saw.
        """
        min_gap = event_min_gap_seconds()
        while not self._stop.is_set():
            if not self._wake.wait(timeout=0.5):
                continue
            if self._stop.is_set():
                break
            self._wake.clear()
            push_m = self._push_m
            gap = self.monotonic_fn() - self._last_event_scan_m
            throttle_s = 0.0
            if gap < min_gap:
                # Too soon after the last scan: sleep out the remainder rather
                # than dropping the push (the ladder change is still unseen).
                throttle_s = min_gap - gap
                if self._stop.wait(timeout=throttle_s):
                    break
            self._last_event_scan_m = self.monotonic_fn()
            try:
                counters = self._scan("fast_event", now_ts=int(self.now_fn()))
            except Exception:  # noqa: BLE001 — worker must never die
                logger.warning("fast_entry event scan raised", exc_info=True)
                continue
            if counters.get("orders"):
                # One line per PLACEMENT only (never at push rate), carrying the
                # segment `run_fast_entry_scan`'s own `pre_ms` cannot see: the
                # push -> scan-start lead, split into the worker wake and the
                # min-gap throttle. Read it TOGETHER with `fast_entry placed`:
                # push_to_scan_ms + that line's pre_ms + trigger_to_post_ms is
                # the whole push-to-POST chain.
                logger.info(
                    "fast_entry event_lead push_to_scan_ms=%.1f "
                    "throttle_ms=%.1f min_gap_ms=%.0f",
                    max(0.0, (self._last_event_scan_m - push_m) * 1000.0),
                    throttle_s * 1000.0,
                    min_gap * 1000.0,
                )

    def stop(self) -> None:
        """Signal + join the event worker (bounded — it is a daemon thread)."""
        self._stop.set()
        self._wake.set()
        worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=5.0)
        self._worker = None

    # ------------------------------------------------------------------
    def _deps_for(self, now_ts: int) -> TickDeps:
        """The bundle for this integer second (see :attr:`_deps_cached`).

        Not locked: the sweep job thread and the event worker can both land in
        the same second, and the worst case is two constructions where one would
        do — the bundle is a set of stable references either way.

        Store ORDER is load-bearing: ``_deps_cached`` is assigned BEFORE
        ``_deps_second``, so a racing reader that observes ``_deps_second ==
        now_ts`` is guaranteed to have already observed a bundle built for that
        second. The READ below therefore has to MIRROR it — load the second
        first, the bundle second. Read them the other way round and a publish
        landing between the two loads serves the PREVIOUS second's bundle under
        a matching stamp, which is the staleness the store order was written to
        prevent (a stale bundle would carry the previous second's fetchers and
        caches).
        """
        second = self._deps_second
        cached = self._deps_cached
        if cached is not None and second == now_ts:
            return cached
        deps = self.deps_factory()
        self._deps_cached = deps
        self._deps_second = now_ts
        return deps

    def _scan(
        self,
        trigger_src: str,
        *,
        now_ts: int,
        candidates: Optional[list] = None,
    ) -> dict:
        return run_fast_entry_scan(
            self.conn_factory(),
            self.portfolio,
            now_ts=now_ts,
            deps=self._deps_for(now_ts),
            trigger_src=trigger_src,
            candidates=candidates,
        )
