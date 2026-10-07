"""Shared venue DATA CONTRACT — the types core and every venue adapter agree on.

These classes were physically defined inside core's ``data/polymarket_clob.py``
and ``data/polymarket_gamma.py`` before the Phase-2 venue split, and both
the shared engine (``execution/pipeline.py``, ``scale_in.py``, ``held_position_pass.py``)
and the predict.fun adapter imported them from there. That made "the book DTO"
a *Polymarket* symbol that everyone depended on — so extracting the Polymarket
adapter into its own distribution would have forced core to import
``cryptobot_polymarket`` at module import time, and would have forced
``cryptobot_predictfun`` to import ``cryptobot_polymarket`` for a dataclass.

So the DTOs live HERE, in core, and the venue packages import them from core
(the allowed direction). Crucially there is still exactly ONE class object per
type — the venue packages re-export these names rather than redefining them —
so ``isinstance`` checks, dataclass identity and pickling are unchanged. That is
what keeps the split behavior-neutral.

``FeeUnknown`` in particular MUST stay a single class: the pipeline gates with
``isinstance(market, FeeUnknown)`` regardless of venue, so a per-venue class
would silently defeat those gates (that hazard is called out verbatim in
``cryptobot_predictfun.markets``, which is why it always imported the
Polymarket one).
Only the ``FEE_UNKNOWN`` *instance* is per-venue.

Nothing in here does I/O; it is types + constants only.
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
    # Tuples so frozen=True is enforceable. Mutable list[dict] inside a
    # frozen dataclass is a false-immutability footgun — the walker's
    # _sorted_asks creates a fresh list defensively, but the type system
    # could not have caught a future in-place mutation.
    #
    # `asks`/`bids` carry every valid level the endpoint returned, sorted
    # ascending by price. `total_ask_depth_usd` is kept as an explicit scalar
    # for the (still-live) book_consumption gate in `execution/gates.py` and
    # should match sum(price*size)
    # over `asks`; keeping it here avoids downstream consumers inventing their
    # own depth semantics.
    asks: tuple[dict[str, float], ...]
    bids: tuple[dict[str, float], ...]
    # LOCAL fetch time (the caller's now_ts) on the Polymarket REST path, NOT a
    # book-liveness signal: the /book REST payload has no server
    # timestamp/sequence we trust. Downstream gates.py book_top_ask_age_s =
    # now_ts - fetched_at_ts is therefore always ~0 and the BOOK_STALENESS_S
    # gate is a no-op there — see the cryptobot_polymarket.clob module
    # docstring (audit w19oxqf24 #12). Do not reinterpret this as "when the
    # book last changed". The WS mirrors DO stamp it from the venue clock.
    fetched_at_ts: float
    total_ask_depth_usd: float = 0.0

    @property
    def top_ask(self) -> float | None:
        if not self.asks:
            return None
        return self.asks[0]["price"]


class TokenBucket:
    """Token bucket with optional server-429 cooldown.

    `rate_per_s` controls steady-state throughput; `burst` is the maximum
    in-flight at any instant. ``backoff_until`` extends an automatic
    cooldown beyond the local limiter when the upstream server returns
    429 — we honor the cooldown rather than hammering the API and risking
    an IP ban (review finding #31).

    Venue-agnostic: both venues rate-limit their own endpoints with their own
    instances of this (Polymarket one bucket; predict.fun a split
    decision/prefetch pair).
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
        """Server returned HTTP 429. Pause `take()` for `cooldown_s` so the
        client honors the upstream rate-limit signal."""
        with self._lock:
            self.backoff_until = max(self.backoff_until, time.monotonic() + cooldown_s)


# ---------------------------------------------------------------------------
# Market-discovery contract
# ---------------------------------------------------------------------------


# Sentinel returned when the venue response can't supply fee_rate_bps even
# from cache. The pipeline maps this to SKIP:fee_unknown rather than the
# generic SKIP:stale_book — operators tuning fee thresholds need to see
# fee-side failures distinctly from book-side failures (review #52).
#
# ONE class, shared by every venue (see module docstring): the pipeline gates
# with isinstance() and a per-venue class would silently defeat that gate.
class FeeUnknown:
    """Sentinel — the market exists but its fee_rate_bps is unknown."""


# ---------------------------------------------------------------------------
# Settlement contract
# ---------------------------------------------------------------------------

RESOLUTION_RESOLVED = "resolved"
RESOLUTION_OPEN = "open"
RESOLUTION_NOT_FOUND = "not_found"
RESOLUTION_BAD_PAYLOAD = "bad_payload"
RESOLUTION_HTTP_ERROR = "http_error"
# A closed market whose outcomePrices are not a decisive 1/0 — e.g. a
# void/refund or exact tie that settles 0.5/0.5. Booking it as won_up=0 would
# fabricate a real DOWN win/loss and corrupt realized PnL, so it is surfaced as
# a distinct non-RESOLVED status; the resolution sweep leaves the row PENDING
# (and escalates to CRITICAL for manual review past MAX_PENDING_SETTLE_AGE_S).
RESOLUTION_VOID = "void"
# Half-band around 1.0/0.0 that still counts as a decisive resolution. Decisive
# markets settle to 1/0; anything inside (DECISIVE_EPSILON, 1-DECISIVE_EPSILON)
# is treated as void/tie rather than a real outcome.
DECISIVE_EPSILON = 0.05


@dataclass(frozen=True)
class PolymarketResolution:
    """Final venue outcome for one crypto up/down market.

    ``up_price`` and ``down_price`` are the venue's UP/DOWN outcome prices
    (Gamma ``outcomePrices`` on Polymarket), not underlying spot prices. They
    settle to 1/0 when the market is resolved.

    NAME KEPT DELIBERATELY: predict.fun already returns this exact type (its
    ``fetch_resolved_outcome`` builds one), the resolution sweep's parameter is
    ``polymarket_resolution_fetch``, and the ``source`` default is a real value
    persisted into settlement paths. Renaming it would be a behavior-visible
    churn for zero gain in Phase 2; it is the *settlement result* contract, not
    a Polymarket-only type.
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
