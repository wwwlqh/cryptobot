"""A3 latency: cached CLOB funder balance for the tick hot path.

``_load_live_context`` previously called ``order_client.check_balance()`` on
EVERY LIVE tick — a synchronous CLOB round trip inside the 1Hz tick body.
Since the fail-closed fix (funder_balance_unknown), a slow or failed inline
call ALSO suppressed trading for the tick. This cache decouples the two:

  * the 60s live-readiness scheduler job (main.py) already fetches the CLOB
    balance for ``live_readiness_reports`` — it now ALSO stores that value
    here (``store()``, zero extra network);
  * the tick reads the cached value via ``TickDeps.live_balance_fetcher``
    (zero network in steady state);
  * fail-closed semantics are preserved:
      - never populated (boot before the first readiness fire) → ONE
        synchronous prime attempt so the first LIVE tick isn't spuriously
        skipped; on failure → None → SKIP:funder_balance_unknown;
      - value older than ``hard_staleness_s`` (readiness refresher dead) →
        None → SKIP:funder_balance_unknown, same as a failed inline fetch.

Staleness bound rationale: the funder-balance gate is a coarse
wallet-drained/misconfigured floor check (group caps ± tolerance), not
per-order accounting — within-tick exposure is tracked separately by the
pipeline's pending/group deltas. A ≤5-minute-old balance is safe for that
purpose; anything older fails closed.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class BalanceCache:
    """Thread-safe cached wrapper around an ``() -> Optional[float]`` fetch."""

    def __init__(
        self,
        fetch: Callable[[], Optional[float]],
        *,
        hard_staleness_s: float = 300.0,
    ) -> None:
        self._fetch = fetch
        self._hard_staleness_s = float(hard_staleness_s)
        self._lock = threading.Lock()
        self._value: Optional[float] = None
        self._fetched_at: Optional[float] = None

    # ---- writers -----------------------------------------------------

    def store(self, value: Optional[float]) -> None:
        """Absorb a balance another job already fetched (readiness job).
        ``None`` (failed fetch) is ignored — the previous value keeps aging
        toward the hard staleness bound rather than being clobbered."""
        if value is None:
            return
        with self._lock:
            self._value = float(value)
            self._fetched_at = time.monotonic()

    def refresh(self) -> Optional[float]:
        """Synchronous fetch + store. Called off the tick path (boot prime,
        never-populated fallback). Returns the fetched value (None on
        failure)."""
        try:
            value = self._fetch()
        except Exception:  # noqa: BLE001 — fetch failures degrade to None
            logger.warning("balance cache refresh raised", exc_info=True)
            value = None
        self.store(value)
        return value

    # ---- tick-facing reader -------------------------------------------

    def get(self) -> Optional[float]:
        """Cached balance for the tick's funder gate. Zero network in steady
        state; fail-closed (None) when the value is missing-and-unprimable
        or older than the hard staleness bound."""
        with self._lock:
            value = self._value
            fetched_at = self._fetched_at
        if fetched_at is None:
            # Boot gap: first LIVE tick can precede the first readiness
            # fire. One synchronous prime preserves today's behavior.
            return self.refresh()
        if (time.monotonic() - fetched_at) > self._hard_staleness_s:
            return None
        return value
