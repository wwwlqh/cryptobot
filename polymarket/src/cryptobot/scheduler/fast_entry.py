"""Fast entry triggers: a 250ms sweep and a WebSocket book-update push.

On-chain forensics of our rejected orders showed most were taken by a faster
taker, about a second before our order arrived, at our quoted ask. Signing and
POSTing are already at the venue floor (the 250ms taker delay applies to
everyone), so what can still be cut is the delay between a fillable ask
appearing and our decision. A 1 Hz tick sees an ask that appears mid-second
~500ms late on average.

Two extra triggers share one evaluation path
(:func:`cryptobot.execution.pipeline.run_fast_entry_scan`, which uses the
tick's own scoring, resolver and ``_commit_cell``):

* **sweep**: a scheduler job every ``CRYPTOBOT_FAST_TRIGGER_MS`` (default
250ms, at least 100ms). Outside a qualifying entry window
:func:`fast_entry_candidate_sleeves` returns nothing after a few integer
comparisons, so a fire costs almost nothing. Average sampling delay drops from
~500ms to ~125ms. * **event**: the WS book mirror calls back on ask-side
updates and a coalescing worker thread runs the same scan within ~20ms. It has
its own switch (``CRYPTOBOT_FAST_TRIGGER_EVENT_ENABLED``) because it runs off
the WS reader thread.

The WS reader thread feeds every book read in the process and must never do
heavy work; blocking it would push the whole bot onto REST. So
:meth:`FastEntryTrigger.on_ask_update` is two attribute reads and an
``Event.set()``, and everything else runs on this module's worker thread.

Nothing here is venue-specific: the triggers read core config and the sleeves'
offset sets, and the event callback is registered through
``BookSourceChain.set_ask_update_callback``. A book source without a callback
hook just runs the sweep.

:meth:`FastEntryTrigger._deps_for` builds the ``TickDeps`` bundle once per
second instead of once per fire. Besides saving the construction, this lets the
scan's per-second caches (``pipeline._fast_resolver_deps`` and
``pipeline._FastCommitStage``), which are keyed on the bundle's identity,
actually hit.
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

# Minimum sweep cadence. 100ms is the fastest proven for a scheduler job on the
# live box; below it the check itself costs more than the delay it removes.
MIN_TRIGGER_MS = 100

# Minimum gap between event scans, derived from the sweep cadence instead of
# another setting. At the 250ms default this caps event scans at ~16/s while
# still reacting immediately to the first push after a quiet spell (the gap
# only matters on a churning book, where back-to-back scans would see nearly
# the same ladder). Never below 20ms, the reaction target.
EVENT_MIN_GAP_FLOOR_S = 0.020


def fast_trigger_seconds(ms: Optional[int] = None) -> float:
    """Sweep cadence in seconds, at least :data:`MIN_TRIGGER_MS`.

    ``IntervalTrigger`` takes float seconds, so 250ms is 0.25.
    """
    raw = config.FAST_TRIGGER_MS if ms is None else ms
    try:
        as_int = int(raw)
    except (TypeError, ValueError):
        as_int = MIN_TRIGGER_MS
    return max(MIN_TRIGGER_MS, as_int) / 1000.0


def event_min_gap_seconds() -> float:
    """Minimum gap between two event scans (see :data:`EVENT_MIN_GAP_FLOOR_S`).
    """
    return max(EVENT_MIN_GAP_FLOOR_S, fast_trigger_seconds() / 4.0)


@dataclass
class FastEntryTrigger:
    """Both fast triggers over one shared single-cell evaluation.

    ``conn_factory`` returns the calling thread's own sqlite3 connection (the
    ``ThreadLocalConnections`` handle): the sweep runs on a scheduler thread
    and the event worker on its own thread, and each needs its own connection.

    ``book_source`` is the ``BookSourceChain``; only ``noted_tokens()`` (the
    current entry universe) is read, to filter WS pushes.
    """

    conn_factory: ConnFactory
    portfolio: V6Portfolio
    deps_factory: Callable[[], TickDeps]
    book_source: Optional[object] = None
    now_fn: Callable[[], float] = time.time
    monotonic_fn: Callable[[], float] = time.monotonic

    # --- event state -------------------------------------------------------
    # Tokens the tick has subscribed for the current (and next) window,
    # refreshed by the sweep. An empty set means unknown and disables the
    # filter rather than dropping every push.
    _universe: frozenset = field(default_factory=frozenset, repr=False)
    # Whether the last sweep saw a qualifying entry window. The WS callback
    # reads it as one attribute load. It can be one sweep stale, which at worst
    # wakes the worker for nothing: the scan re-checks the real gate and
    # returns immediately when ``fast_entry_candidate_sleeves`` is empty.
    _in_window: bool = field(default=False, repr=False)
    # The tuple ``_universe`` was built from, compared by identity so an
    # unchanged token set costs nothing to re-check (see _refresh_universe).
    _noted_ref: object = field(default=None, repr=False)
    _wake: threading.Event = field(default_factory=threading.Event, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _worker: Optional[threading.Thread] = field(default=None, repr=False)
    _last_event_scan_m: float = field(default=0.0, repr=False)
    # Monotonic time of the push that set :attr:`_wake` (see
    # :meth:`on_ask_update`). Written on the WS thread and read on the worker
    # as a plain float; a race can only misattribute one telemetry line, never
    # a decision.
    _push_m: float = field(default=0.0, repr=False)
    # Per-second TickDeps bundle. ``deps_factory()`` builds a multi-field
    # dataclass, and the scan's per-second caches key on the bundle's identity,
    # so building one per fire would defeat them. Rebuilt when the second
    # changes.
    _deps_second: int = field(default=-1, repr=False)
    _deps_cached: Optional[TickDeps] = field(default=None, repr=False)
    # Counters for logs and tests; never persisted. The fast triggers only
    # write database rows when something places or rejects, like the tick.
    events_seen: int = 0
    events_dropped: int = 0
    sweep_scans: int = 0

    # ------------------------------------------------------------------
    # Sweep
    # ------------------------------------------------------------------
    def run_sweep(self) -> dict:
        """The scheduler job body. Never raises."""
        try:
            now_ts = int(self.now_fn())
            candidates = fast_entry_candidate_sleeves(self.portfolio, now_ts)
            self._in_window = bool(candidates)
            if not candidates:
                return {}
            self._refresh_universe()
            self.sweep_scans += 1
            # Pass the candidates in; the guard above already computed them.
            return self._scan("fast_sweep", now_ts=now_ts, candidates=candidates)
        except Exception:  # noqa: BLE001 — a job must never crash the scheduler
            logger.warning("fast_entry sweep raised", exc_info=True)
            return {}

    def _refresh_universe(self) -> None:
        """Re-read the chain's noted token set (the WS push filter).

        Skipped when the event trigger is off, since the set only exists to
        filter :meth:`on_ask_update`. Compared by tuple identity, because the
        tick passes the same tuple almost every second.
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
    # Event
    # ------------------------------------------------------------------
    def on_ask_update(self, token: str, _venue_ts: float) -> None:
        """WS reader-thread callback. Must stay O(1) and non-blocking.

        Runs on the mirror's receive thread at ~800 msg/s, so it only checks
        the token against the entry universe, checks the in-window flag kept by
        the sweep, and wakes the worker. Scoring, book reads, database writes
        and the order POST all happen on :meth:`_event_loop`'s thread.

        ``_venue_ts`` is part of the callback signature but unused; the worker
        uses its own clock.

        The one monotonic clock read comes after both checks, so only a push
        that wakes the worker pays for it. It makes ``push_to_scan_ms``
        measurable: the worker wake plus the :func:`event_min_gap_seconds`
        throttle (up to 62ms at the 250ms default) would otherwise be
        invisible.
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
        """Wait for a push, run one scan, and fold whatever arrived during the
        scan into a single follow-up scan.

        The ``Event`` is the coalescing: any number of pushes between two scans
        is one wake, so a churning book cannot build a backlog. The flag is
        cleared before the scan, so a push that lands mid-scan sets it again
        and triggers an immediate re-scan, which is right: that push carries a
        ladder the finished scan did not see.
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
                # than drop the push (its ladder change has not been seen yet).
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
                # One line per placement (never per push) with the part
                # ``run_fast_entry_scan``'s ``pre_ms`` cannot see: push to scan
                # start, split into worker wake and throttle. push_to_scan_ms +
                # the ``fast_entry placed`` line's pre_ms + trigger_to_post_ms
                # is the whole push-to-POST time.
                logger.info(
                    "fast_entry event_lead push_to_scan_ms=%.1f "
                    "throttle_ms=%.1f min_gap_ms=%.0f",
                    max(0.0, (self._last_event_scan_m - push_m) * 1000.0),
                    throttle_s * 1000.0,
                    min_gap * 1000.0,
                )

    def stop(self) -> None:
        """Signal and join the event worker (bounded; it is a daemon thread).
        """
        self._stop.set()
        self._wake.set()
        worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=5.0)
        self._worker = None

    # ------------------------------------------------------------------
    def _deps_for(self, now_ts: int) -> TickDeps:
        """The bundle for this second (see :attr:`_deps_cached`).

        Not locked: the sweep and the event worker can land in the same second,
        and the worst case is building the bundle twice; it only holds stable
        references.

        Store order matters: ``_deps_cached`` is written before
        ``_deps_second``, so a reader that sees ``_deps_second == now_ts`` has
        already seen that second's bundle. Reads must therefore load the second
        first and the bundle second; the other way round, a write landing
        between the two loads would serve the previous second's bundle under
        the current stamp.
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
