"""Shared execution-layer dataclasses.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U5 / D-LIVE-23:

  * `OrderResult` is the only new type here. It captures the outcome of a
    single `OrderClient.place_gtc_limit_order` call — order_id (CLOB),
    realized fill VWAP, filled size, terminal/retryable error_kind.
  * `BetSignal` is **NOT** redefined in this module. The walker imports
    `cryptobot.execution.ledger.BetSignal` directly because that dataclass
    is already wired into 10+ existing call sites; introducing a second
    type would collide on every record_bet call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class OrderResult:
    """Outcome of a `OrderClient.place_gtc_limit_order` invocation.

    `success=True` indicates the CLOB accepted the order. The realized fill
    (`filled_size_tokens` × `filled_price_vwap`) is populated when the CLOB
    response carries actual trade metadata; otherwise the `filled_*` fields
    stay `None` until `reconcile_open_orders` back-fills the matched fill on
    a later tick.

    On failure, `success=False` and `error_kind` carries the coarse retry
    bucket from `_classify_clob_error`: 'auth', 'insufficient_funds',
    'market_closed', 'invalid_amount', 'venue_unavailable', 'post_only',
    'no_liquidity', 'network', 'unknown'. The pipeline reads `error_kind`
    to decide between SKIP-and-move-on vs retry-on-next-tick.
    """

    # Success / failure summary
    success: bool = False
    error: Optional[str] = None
    error_kind: Optional[str] = None

    # CLOB-side identifiers
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None

    # Realized fill (None when CLOB hasn't returned trade metadata yet)
    filled_size_tokens: Optional[float] = None
    filled_price_vwap: Optional[float] = None
    filled_value_usd: Optional[float] = None
    limit_price: Optional[float] = None
    fill_ts: Optional[str] = None

    # A0 latency instrumentation — wall-clock ms the OrderClient spent in
    # `create_market_order` (local EIP-712 signing + any market-info
    # cache-miss GETs) vs the `post_order` round trip. None on failure
    # paths that never reached the corresponding step. The pipeline
    # accumulates these into TickResult for the stage='latency' trace.
    sign_ms: Optional[float] = None
    post_ms: Optional[float] = None

    @property
    def is_terminal_failure(self) -> bool:
        """`auth`, `insufficient_funds`, `market_closed`, `invalid_amount`
        are terminal — the caller should NOT retry (re-sending the same order
        just re-fails). `venue_unavailable`, `post_only`, `no_liquidity`,
        `network`, and `unknown` are transient and may retry on a later tick."""
        return self.error_kind in {
            "auth",
            "insufficient_funds",
            "market_closed",
            "invalid_amount",
        }
