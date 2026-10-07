"""Shared execution-layer types.

``OrderResult`` is the result of one ``OrderClient.place_gtc_limit_order``
call. ``BetSignal`` lives in ``cryptobot.execution.ledger``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class OrderResult:
    """Result of ``OrderClient.place_gtc_limit_order``.

    ``success=True`` means the CLOB accepted the order. The fill
    (``filled_size_tokens`` x ``filled_price_vwap``) is set when the response
    carries trade data; otherwise the ``filled_*`` fields stay ``None`` until
    ``reconcile_open_orders`` fills them in.

    On failure ``success=False`` and ``error_kind`` is the retry bucket from
    ``_classify_clob_error``: 'auth', 'insufficient_funds', 'market_closed',
    'invalid_amount', 'venue_unavailable', 'post_only', 'no_liquidity',
    'network', 'unknown'. The pipeline uses it to decide between skipping and
    retrying on a later tick.
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

    # Milliseconds spent in ``create_market_order`` (local EIP-712 signing plus
    # any market-info cache-miss GETs) and in the ``post_order`` round trip.
    # None on failure paths that never reached that step. The pipeline adds
    # these to TickResult for the latency trace.
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
