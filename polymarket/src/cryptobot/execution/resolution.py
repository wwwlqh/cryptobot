"""Oracle-price settlement with sanity bounds (legacy and test path).

  - per-coin bounds (config.RESOLUTION_PRICE_BOUNDS);
  - a maximum deviation from the last known live spot
    (config.MAX_RESOLUTION_PRICE_MOVE_PCT).

A price outside either raises ``ResolutionError("rejected")`` and the sweep
marks the row CANCELLED (pnl_net=0).

The price source is a pluggable ``Callable[[str, int], Optional[float]]`` so
tests can stub it; the default returns ``None`` (treated as upstream down).

Live settlement does not use this module: ``main._build_sweep_job`` settles
from Gamma's final ``outcomePrices`` and wires no price fetcher.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Callable, Optional

from cryptobot import config

logger = logging.getLogger(__name__)


PriceFetcher = Callable[[str, int], Optional[float]]


class ResolutionError(Exception):
    """Raised when a resolution price is found but fails sanity / deviation checks."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code   # "out_of_bounds" | "excessive_deviation"


@dataclass(frozen=True)
class ResolutionResult:
    price: float
    source: str   # "chainlink"


def _within_bounds(coin: str, price: float) -> bool:
    bounds = config.RESOLUTION_PRICE_BOUNDS.get(coin)
    if bounds is None:
        # Unknown coin — refuse (unlikely; we only support six known coins).
        return False
    lo, hi = bounds
    return math.isfinite(price) and lo <= price <= hi


def _within_deviation(reference: float, candidate: float, max_pct: float) -> bool:
    if not (math.isfinite(reference) and reference > 0):
        # No reference — skip deviation check (conservative caller will still
        # apply per-coin sanity bounds first).
        return True
    return abs(candidate - reference) / reference <= max_pct


def _default_chainlink_fetch(coin: str, ts: int) -> Optional[float]:
    """Stub Chainlink fetcher — production wiring lives in a follow-up unit."""
    logger.info("chainlink fetcher not configured for %s @ %d", coin, ts)
    return None


def fetch_resolution_price(
    coin: str,
    window_end_ts: int,
    *,
    reference_spot: Optional[float] = None,
    chainlink_fetch: PriceFetcher | None = None,
) -> Optional[ResolutionResult]:
    """Return (price, source) or None when the upstream fails.

    Raises ResolutionError when a returned price fails sanity bounds or the
    max-deviation guard from the supplied ``reference_spot``.
    """
    chainlink_fetch = chainlink_fetch or _default_chainlink_fetch

    candidate = chainlink_fetch(coin, window_end_ts)
    source = "chainlink"
    if candidate is None:
        return None

    if not _within_bounds(coin, float(candidate)):
        raise ResolutionError(
            "out_of_bounds",
            f"{coin} resolution price {candidate} outside per-coin bounds",
        )
    if reference_spot is not None and not _within_deviation(
        reference_spot, float(candidate), config.MAX_RESOLUTION_PRICE_MOVE_PCT
    ):
        raise ResolutionError(
            "excessive_deviation",
            f"{coin} resolution price {candidate} deviates > "
            f"{config.MAX_RESOLUTION_PRICE_MOVE_PCT * 100:.0f}% from reference "
            f"{reference_spot}",
        )
    return ResolutionResult(price=float(candidate), source=source)
