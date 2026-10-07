"""WS book-mirror stall watchdog: detects a subscription that is connected but
has stopped updating one token.

Why
---
The entry path reads books from the WS mirror first, and the live deploy sets
no freshness cap (``CRYPTOBOT_BOOK_WS_MAX_AGE_MS=600000`` in
``polymarket/infra/install_remote.sh``): on up/down books a large snapshot age
usually means a quiet market, not a lagging feed, and capping it only spent
REST calls re-reading the same book. Two failure modes are already handled:

  * the connection dies: ``_clear_cache`` empties the mirror and ``get``
    serves None;
  * a window's token rolls off: ``note_tokens`` evicts it.

What remains is a socket that stays connected (pings and other tokens' frames
keep it marked live) while updates for one token stop. The mirror cannot tell
that book from a quiet one and would keep pricing it until the window ends.

The check must not slow the hot path, so it runs as a separate scheduler job
that checks only suspect tokens against REST.

How
---
Every ``CRYPTOBOT_BOOK_WS_WATCHDOG_INTERVAL_S`` (default 5s), on a scheduler
thread:

  1. take the mirror's cached books and each one's venue-clock age;
  2. keep tokens older than ``STALL_SUSPECT_MS`` (default 5000) that were not
     recently verified (see below), oldest first, at most
     ``MAX_TOKENS_PER_PASS`` (default 4);
  3. fetch them in one batch REST call (``fetch_books_batch``, one
     rate-limit token);
  4. compare top of book: price exactly, size within ``SIZE_REL_TOL``
     (``venues.shadow_book.top_of_book_matches``, shared with the shadow
     comparator);
  5. match: mark verified now. Mismatch: evict from the mirror, count, and
     log a WARNING. The mirror rebuilds the token from its next full
     ``book`` frame.

Verify TTL: a quiet but correct book must not be re-fetched every 5s, or the
watchdog recreates the REST cost it is meant to avoid. A verified token is
re-checked only after ``VERIFY_TTL_MS`` (default 30000), or once it has updated
since the check and gone quiet again.

Fails open: a REST error or timeout (``None``), a token missing from the
response, a mirror without the watchdog methods, or any exception in a pass is
skipped silently. It never evicts on a failed check, never raises into the
scheduler, places no orders, gates nothing and writes nothing to the database.

REST cost: at most one ``POST /books`` per pass (12 per minute at 5s), covering
at most 4 tokens, however many books are quiet.
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
