"""Cached CLOB funder balance for the tick.

Without this, every live tick would call ``order_client.check_balance()``, a
network round trip inside the 1 Hz tick, and a slow or failed call would also
block trading for that tick (funder_balance_unknown). Instead:

  * the 60s live-readiness job (main.py) already fetches the CLOB balance and
    also stores it here (``store()``, no extra network call);
  * the tick reads the cached value through ``TickDeps.live_balance_fetcher``;
  * it still fails closed:
      - never populated (before the first readiness run): one synchronous
        fetch so the first live tick is not skipped; if that fails, None and
        SKIP:funder_balance_unknown;
      - older than ``hard_staleness_s`` (readiness job stopped): None and
        SKIP:funder_balance_unknown.

The funder-balance gate is a coarse "wallet drained or misconfigured" check
against the group caps, not per-order accounting (the pipeline tracks in-tick
exposure separately), so a balance up to 5 minutes old is fine; anything older
fails closed.
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
