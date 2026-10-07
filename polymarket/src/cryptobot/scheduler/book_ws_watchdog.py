"""WS book-mirror STALL WATCHDOG — the gray-zone detector for a connected but
silent subscription (operator order, 2026-08-06).

Why this exists
---------------
The entry path reads books WS-mirror-first. The mirror's freshness cap was
lifted on the live box (``CRYPTOBOT_BOOK_WS_MAX_AGE_MS`` pinned to 600000 in
``polymarket/infra/install_remote.sh``) because on the ``*-updown-*`` books a
large snapshot age is market QUIETNESS, not feed lag — a real ladder can legally
hold for minutes, and capping age just spent REST round-trips to re-learn the
same book. Two of the three failure modes were already covered:

  * connection death → ``_clear_cache`` wipes the mirror, ``get`` serves None;
  * a rolled-off window's token → evicted by ``note_tokens`` (2026-07-25).

The residual, explicitly accepted, is the gray zone in between: the socket stays
CONNECTED (recv-liveness is re-stamped by pings and by other tokens' frames) but
updates for ONE subscribed token silently stop. The mirror cannot tell that book
from a quiet one, so it would price a dead ladder until the window rolls.

The detector must not give back the ~0ms hot path, so it is out-of-band: a
daemon-side scheduler job that cross-checks only SUSPECT tokens against REST,
never the tick.

Shape
-----
Every ``CRYPTOBOT_BOOK_WS_WATCHDOG_INTERVAL_S`` (default 5s), on a
scheduler-pool thread:

  1. take the mirror's serving set and compute each snapshot's VENUE-clock age;
  2. keep tokens older than ``STALL_SUSPECT_MS`` (default 5000) that are not
     suppressed by a recent verify (see below), oldest first, at most
     ``MAX_TOKENS_PER_PASS`` (default 4);
  3. ONE batch REST book fetch for that set (one rate-limit token regardless of
     count — ``fetch_books_batch``);
  4. compare top-of-book: price EXACT, size within ``SIZE_REL_TOL``
     (``venues.shadow_book.top_of_book_matches``, shared so the watchdog and the
     shadow comparator read snapshots the same way);
  5. MATCH → stamp "verified at now"; MISMATCH → evict from the mirror + count +
     log WARNING. The mirror re-populates the token on its next full ``book``
     frame.

Verify TTL — a quiet-but-correct book must not be re-fetched every 5s, or the
watchdog just re-creates the REST cost it was meant to avoid. A verified token
is re-checked only once ``VERIFY_TTL_MS`` (default 30000) has passed OR new ws
activity has landed since the verify (a newer snapshot ts) and the book has gone
quiet again.

Fail-open everywhere. A REST error/timeout (``None`` return), a token the venue
omitted, a mirror without the watchdog API, or ANY exception in a pass is a
silent skip: the watchdog never evicts on a failed verify and never raises into
the scheduler. It places no orders, gates nothing, and touches no DB.

REST budget, steady state: at most ONE POST /books per pass = 12/min at the
default 5s cadence, covering at most 4 tokens — bounded and independent of how
many books are quiet.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

from cryptobot.venues.shadow_book import top_of_book, top_of_book_matches

logger = logging.getLogger(__name__)


# Process-global counter snapshot, published by the watchdog and read by the
# dashboard payload builder (``observability.snapshot_builder``), which runs in
# this same process. A plain dict-swap under a lock: the readers are the
# dashboard thread only, and the numbers are telemetry, never a decision input.
_COUNTERS_LOCK = threading.Lock()
_COUNTERS: dict = {
    "armed": False,
    "checks": 0,
    "matches": 0,
    "mismatches": 0,
    "evictions": 0,
    "last_mismatch_ts": None,
}


def watchdog_counters() -> dict:
    """Copy of the published watchdog counters (zeros + ``armed=False`` when no
    watchdog was ever constructed — e.g. shadow mode, WS off, or a venue whose
    mirror has no watchdog API)."""
    with _COUNTERS_LOCK:
        return dict(_COUNTERS)


def reset_watchdog_counters() -> None:
    """Test seam only — the live process constructs at most one watchdog."""
    with _COUNTERS_LOCK:
        _COUNTERS.update({
            "armed": False,
            "checks": 0,
            "matches": 0,
            "mismatches": 0,
            "evictions": 0,
            "last_mismatch_ts": None,
        })


def mirror_supports_watchdog(mirror) -> bool:
    """True when ``mirror`` exposes the read-only serving-set + eviction API.

    Venue seam: only mirrors that implement ``serving_snapshots``/``evict`` can
    be watched. Anything else is left exactly as today (no job registered).
    """
    return callable(getattr(mirror, "serving_snapshots", None)) and callable(
        getattr(mirror, "evict", None)
    )


class BookWsWatchdog:
    """Decision logic for the stall cross-check. Pure aside from the injected
    fetch + the mirror's ``evict``; ``run_once`` is directly unit-tested."""

    def __init__(
        self,
        mirror,
        fetch_books,
        *,
        stall_suspect_ms: int = 5000,
        max_tokens_per_pass: int = 4,
        verify_ttl_ms: int = 30000,
        size_rel_tol: float = 0.02,
    ) -> None:
        self._mirror = mirror
        # Callable[[list[str]], Optional[dict[str, BookSnapshot]]] — the venue
        # adapter's batch fetch, pre-bound to the read-only CLOB client by the
        # caller (same partial() pattern as BookPrefetcher).
        self._fetch_books = fetch_books
        self._stall_suspect_s = max(0.0, float(stall_suspect_ms) / 1000.0)
        self._max_tokens = max(1, int(max_tokens_per_pass))
        self._verify_ttl_s = max(0.0, float(verify_ttl_ms) / 1000.0)
        self._size_rel_tol = max(0.0, float(size_rel_tol))
        # token -> (verified_at_wall_ts, snapshot venue ts that was verified)
        self._verified: dict[str, tuple[float, float]] = {}

        self.n_checks = 0
        self.n_matches = 0
        self.n_mismatches = 0
        self.n_evictions = 0
        self.last_mismatch_ts: Optional[float] = None
        self._publish()

    # -- counters ------------------------------------------------------------

    def _publish(self) -> None:
        with _COUNTERS_LOCK:
            _COUNTERS.update({
                "armed": True,
                "checks": self.n_checks,
                "matches": self.n_matches,
                "mismatches": self.n_mismatches,
                "evictions": self.n_evictions,
                "last_mismatch_ts": self.last_mismatch_ts,
            })

    # -- selection -----------------------------------------------------------

    def _suspects(self, snapshots: dict, *, now_ts: float) -> list[str]:
        """Serving-set tokens whose ladder is older than the suspect bound and
        not suppressed by a recent verify — OLDEST FIRST, capped."""
        scored: list[tuple[float, str]] = []
        for token, snap in snapshots.items():
            venue_ts = getattr(snap, "fetched_at_ts", None)
            if venue_ts is None:
                continue
            age_s = float(now_ts) - float(venue_ts)
            if age_s <= self._stall_suspect_s:
                continue
            stamp = self._verified.get(token)
            if stamp is not None:
                verified_at, verified_venue_ts = stamp
                # Suppressed while the TTL holds AND nothing new arrived on the
                # ws since we verified. A newer snapshot ts means the token DID
                # update and has gone quiet again — a fresh question.
                if (
                    (float(now_ts) - verified_at) < self._verify_ttl_s
                    and float(venue_ts) <= verified_venue_ts
                ):
                    continue
            scored.append((age_s, token))
        scored.sort(key=lambda pair: (-pair[0], pair[1]))
        return [token for _, token in scored[: self._max_tokens]]

    # -- one pass ------------------------------------------------------------

    def run_once(self, *, now_ts: Optional[float] = None) -> int:
        """Run one cross-check pass; return the number of tokens EVICTED.

        Never raises: every failure mode (no mirror API, REST error, missing
        token, unparseable snapshot) is a silent skip that leaves the mirror
        untouched — a failed verify is not evidence of a stall.
        """
        if now_ts is None:
            now_ts = time.time()
        if not mirror_supports_watchdog(self._mirror):
            return 0
        try:
            snapshots = self._mirror.serving_snapshots()
        except Exception:  # noqa: BLE001 — fail-open, never touch the mirror
            logger.debug("book ws watchdog: serving_snapshots raised", exc_info=True)
            return 0
        if not snapshots:
            return 0
        tokens = self._suspects(snapshots, now_ts=now_ts)
        if not tokens:
            return 0
        try:
            books = self._fetch_books(tokens)
        except Exception:  # noqa: BLE001 — REST error => skip silently
            logger.debug("book ws watchdog: REST fetch raised", exc_info=True)
            return 0
        if not books:
            # None (transport/HTTP/parse error) or {} (venue returned nothing
            # usable). Neither proves a stall — fail open.
            return 0

        evicted = 0
        for token in tokens:
            rest_snap = books.get(token)
            ws_snap = snapshots.get(token)
            if rest_snap is None or ws_snap is None:
                # Token absent from the batch response = empty ask book at the
                # venue OR a partial response; ambiguous, so never an eviction.
                continue
            self.n_checks += 1
            try:
                matched = top_of_book_matches(
                    ws_snap, rest_snap, size_rel_tol=self._size_rel_tol
                )
            except Exception:  # noqa: BLE001 — a weird snapshot is not a stall
                logger.debug("book ws watchdog: compare raised", exc_info=True)
                continue
            venue_ts = float(getattr(ws_snap, "fetched_at_ts", now_ts) or now_ts)
            if matched:
                self.n_matches += 1
                # Quiet AND correct: suppress re-checks so the watchdog does not
                # re-create the REST cost every interval.
                self._verified[token] = (float(now_ts), venue_ts)
                continue
            self.n_mismatches += 1
            self.last_mismatch_ts = float(now_ts)
            self._verified.pop(token, None)
            try:
                self._mirror.evict(token)
            except Exception:  # noqa: BLE001 — eviction failure must not raise
                logger.debug("book ws watchdog: evict raised", exc_info=True)
                continue
            self.n_evictions += 1
            evicted += 1
            logger.warning(
                "book WS mirror STALL tok=%s ws_age=%.1fs ws_top=%s rest_top=%s "
                "— evicted (next read falls through to REST; mirror repopulates "
                "on the next full book frame)",
                token[:8],
                float(now_ts) - venue_ts,
                top_of_book(ws_snap),
                top_of_book(rest_snap),
            )
        # Drop verify stamps for tokens that left the serving set (window roll),
        # so the map stays bounded by the live window.
        if len(self._verified) > len(snapshots):
            live = set(snapshots)
            self._verified = {
                t: v for t, v in self._verified.items() if t in live
            }
        self._publish()
        return evicted


def build_watchdog_job(watchdog: BookWsWatchdog) -> Callable[[], None]:
    """Wrap ``run_once`` in the house scheduler-job guard: a raising job must
    never crash the scheduler or reach the scanner."""

    def _job() -> None:
        try:
            watchdog.run_once()
        except Exception:  # noqa: BLE001 — watchdog must never crash the scheduler
            logger.warning("book_ws_watchdog job raised", exc_info=True)

    return _job


__all__ = [
    "BookWsWatchdog",
    "build_watchdog_job",
    "mirror_supports_watchdog",
    "reset_watchdog_counters",
    "watchdog_counters",
]
