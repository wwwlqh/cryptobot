"""Data types shared by the engine and the exchange adapter.

The engine and the adapter both import these from here, so there is exactly one
class object per type and ``isinstance`` checks work across the boundary. That
matters most for ``FeeUnknown``: the pipeline gates on ``isinstance(market,
FeeUnknown)``.

Types and constants only; nothing here does I/O.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Order-book contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BookSnapshot:
    token_id: str
    # Tuples so the frozen dataclass is really immutable.
    #
    # ``asks``/``bids`` hold every valid level, sorted ascending by price.
    # ``total_ask_depth_usd`` is the sum of price * size over ``asks``; the
    # book-consumption gate reads it.
    asks: tuple[dict[str, float], ...]
    bids: tuple[dict[str, float], ...]
    # Local fetch time on the REST path (the REST book has no server timestamp
    # we trust), so book age there is always ~0. The WebSocket mirror stamps it
    # from the exchange clock.
    fetched_at_ts: float
    total_ask_depth_usd: float = 0.0

    @property
    def top_ask(self) -> float | None:
        if not self.asks:
            return None
        return self.asks[0]["price"]


class TokenBucket:
    """Token bucket with an optional server-429 cooldown.

    ``rate_per_s`` sets steady-state throughput and ``burst`` the maximum in
    flight. ``backoff_until`` pauses ``take()`` after the server returns 429,
    so the client backs off instead of risking an IP ban.
    """

    def __init__(self, rate_per_s: float, burst: int) -> None:
        self.rate = rate_per_s
        self.burst = burst
        self.tokens = float(burst)
        self.last = time.monotonic()
        self.backoff_until: float = 0.0
        self._lock = threading.Lock()

    def take(self, n: int = 1) -> bool:
        with self._lock:
            now = time.monotonic()
            if now < self.backoff_until:
                return False
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

    def notify_server_429(self, *, cooldown_s: float = 10.0) -> None:
        """Server returned 429: pause ``take()`` for ``cooldown_s`` seconds."""
        with self._lock:
            self.backoff_until = max(self.backoff_until, time.monotonic() + cooldown_s)


# ---------------------------------------------------------------------------
# Market-discovery contract
# ---------------------------------------------------------------------------


# Returned when a market's fee rate is unknown even from cache. The pipeline
# skips with SKIP:fee_unknown, distinct from SKIP:stale_book, so fee problems
# are visible separately from book problems.
class FeeUnknown:
    """Sentinel: the market exists but its fee rate is unknown."""


# ---------------------------------------------------------------------------
# Settlement contract
# ---------------------------------------------------------------------------

RESOLUTION_RESOLVED = "resolved"
RESOLUTION_OPEN = "open"
RESOLUTION_NOT_FOUND = "not_found"
RESOLUTION_BAD_PAYLOAD = "bad_payload"
RESOLUTION_HTTP_ERROR = "http_error"
# A closed market whose outcome prices are not a decisive 1/0 (a void or a
# 0.5/0.5 tie). Booking it as a loss would corrupt P&L, so the sweep leaves the
# row PENDING and escalates to CRITICAL after MAX_PENDING_SETTLE_AGE_S.
RESOLUTION_VOID = "void"
# Half-band around 1.0/0.0 that still counts as a decisive resolution. Decisive
# markets settle to 1/0; anything inside (DECISIVE_EPSILON, 1-DECISIVE_EPSILON)
# is treated as void/tie rather than a real outcome.
DECISIVE_EPSILON = 0.05


@dataclass(frozen=True)
class PolymarketResolution:
    """Final outcome of one crypto up/down market.

    ``up_price`` and ``down_price`` are Gamma's ``outcomePrices`` for the UP
    and DOWN tokens (1/0 once resolved), not underlying spot prices.
    """

    status: str
    coin: str
    interval_min: int
    window_start_ts: int
    window_end_ts: int
    condition_id: str | None = None
    won_up: int | None = None
    up_price: float | None = None
    down_price: float | None = None
    source: str = "polymarket_gamma"


__all__ = [
    "BookSnapshot",
    "TokenBucket",
    "FeeUnknown",
    "PolymarketResolution",
    "RESOLUTION_RESOLVED",
    "RESOLUTION_OPEN",
    "RESOLUTION_NOT_FOUND",
    "RESOLUTION_BAD_PAYLOAD",
    "RESOLUTION_HTTP_ERROR",
    "RESOLUTION_VOID",
    "DECISIVE_EPSILON",
]
