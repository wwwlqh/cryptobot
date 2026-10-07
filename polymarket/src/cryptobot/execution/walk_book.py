"""Edge-preserving order book walker.

The cap is an absolute amount added to the anchor price (5 cents by default,
not 5%).

Used for:

  - first entries (anchor = top ask at decision time);
  - top-ups (anchor = the slot's first-fill price, so the cap does not drift
    with the book).

Returns ``None`` when:

  - there are no asks;
  - an input is non-finite or negative;
  - no fill that keeps the edge floor reaches ``min_bet_usd``;
  - ``max_walk_price`` is set but the book has no finite top to anchor to.
"""

from __future__ import annotations

import logging
import math
from typing import Optional, Union

logger = logging.getLogger(__name__)

# Return shape from `walk_book_edge_preserving`. Tuple positions:
#   0 filled_usd:    float — total notional consumed
#   1 filled_shares: float — shares purchased
#   2 filled_vwap:   float — volume-weighted average fill price
#   3 limit_price:   float — most expensive consumed level (cap-touched, not exceeded)
#   4 realized_edge: float — VWAP edge after fees and shares purchased
#   5 fill_levels:   list[dict] — optional per-level forensics when return_levels=True
WalkResult = Union[
    tuple[float, float, float, float, float],
    tuple[float, float, float, float, float, list[dict[str, float]]],
]


def _level_price(level) -> float:
    """Return a numeric price for either object-style or dict-style book levels."""
    return float(level.price if hasattr(level, "price") else level["price"])


def _level_size(level) -> float:
    """Return a numeric size for either object-style or dict-style book levels."""
    return float(level.size if hasattr(level, "size") else level["size"])


def _sorted_asks(book):
    """Return ask levels sorted from cheapest to most expensive.

    Polymarket's `/book` responses can arrive in descending order — never
    assume the first ask is the best executable buy price.
    """
    asks = getattr(book, "asks", None)
    if asks is None and isinstance(book, dict):
        asks = book.get("asks")
    # Divergence #12 (ghost-fill regression): /book may return {} or
    # {"asks": null} on the rare CLOB degraded state. Treat both as
    # empty rather than letting sorted() raise.
    if asks is None:
        return []
    return sorted(asks, key=_level_price)


def _add_fill_level(
    levels: list[dict[str, float]],
    *,
    price: float,
    shares: float,
    usd: float,
) -> None:
    """Accumulate fill notional by exact price level for operator forensics."""
    if not (
        math.isfinite(price)
        and math.isfinite(shares)
        and math.isfinite(usd)
        and price > 0
        and shares > 0
        and usd > 0
    ):
        return
    for level in levels:
        if math.isclose(level["price"], price, rel_tol=0.0, abs_tol=1e-12):
            level["shares"] += shares
            level["usd"] += usd
            return
    levels.append({"price": price, "shares": shares, "usd": usd})


def walk_book_edge_preserving(
    book,
    target_usd: float,
    *,
    prob_safe_floor: float,
    fee_theta: float,
    min_edge: float,
    min_bet_usd: float,
    max_walk_price: float | None = None,
    walk_anchor_price: float | None = None,
    return_levels: bool = False,
) -> Optional[WalkResult]:
    """Walk asks up to ``target_usd`` while keeping ``min_edge``.

    Two stopping conditions; whichever comes first wins:

      1. Edge floor: the VWAP edge after taking the next level would fall below
         ``min_edge``. Take the smaller fill at the better edge rather than pay
         for depth the signal does not support.
      2. Price cap: the level's price is above ``walk_anchor_price +
         max_walk_price`` (or, with no anchor, the cheapest ask +
         ``max_walk_price``). A level exactly at the cap can still be taken.

    ``walk_anchor_price`` lets top-ups keep the cap at the first fill's anchor
    as the book moves; without it the cap would follow each tick's top ask.

    Returns ``(filled_usd, filled_shares, filled_vwap, limit_price,
    realized_edge)``, plus ``fill_levels`` when ``return_levels=True``.
    """
    # NaN / non-finite inputs poison comparisons (NaN < x is always False),
    # which would let the walker eat an arbitrarily bad book while still
    # "passing" the edge floor. Refuse any non-finite input up front.
    if (
        not math.isfinite(prob_safe_floor)
        or not math.isfinite(target_usd)
        or not math.isfinite(fee_theta)
        or not math.isfinite(min_edge)
        or not math.isfinite(min_bet_usd)
        or target_usd <= 0
    ):
        return None
    if max_walk_price is not None and (
        not math.isfinite(max_walk_price) or max_walk_price < 0
    ):
        logger.warning(
            "walk_book rejected non-finite/negative max_walk_price=%r — "
            "config drift; refusing to walk",
            max_walk_price,
        )
        return None
    if walk_anchor_price is not None and (
        not math.isfinite(walk_anchor_price) or walk_anchor_price <= 0
    ):
        return None

    def _fee(p: float) -> float:
        return fee_theta * p * (1.0 - p)

    asks = _sorted_asks(book)
    if not asks:
        return None

    # Anchor the price-walk cap. Caller-supplied anchor wins (sticky retry
    # behavior); otherwise scan asks for the first finite-priced level. A
    # book whose top is NaN/Inf must not silently disable the cap by falling
    # through to no-cap behavior — fail closed.
    if max_walk_price is None:
        walk_cap_price = None
    elif walk_anchor_price is not None:
        walk_cap_price = walk_anchor_price + max_walk_price
    else:
        anchor: float | None = None
        for level in asks:
            candidate = _level_price(level)
            if math.isfinite(candidate) and candidate > 0:
                anchor = candidate
                break
        if anchor is None:
            return None
        walk_cap_price = anchor + max_walk_price

    acc_usd = 0.0
    acc_shares = 0.0
    limit_price = 0.0
    fill_levels: list[dict[str, float]] = []

    for level in asks:
        price = _level_price(level)
        size = _level_size(level)
        if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size <= 0:
            continue

        # Price-walk cap: stop before consuming any level priced strictly
        # above anchor + max_walk_price. A level priced exactly at the cap
        # is still consumable (strict-greater break).
        if walk_cap_price is not None and price > walk_cap_price:
            break

        remaining = target_usd - acc_usd
        if remaining <= 0:
            break

        level_usd = price * size
        take_usd = min(level_usd, remaining)
        take_shares = take_usd / price

        new_usd = acc_usd + take_usd
        new_shares = acc_shares + take_shares
        if new_shares <= 0:
            continue
        new_vwap = new_usd / new_shares
        new_fee = _fee(new_vwap)
        new_edge = prob_safe_floor - new_vwap - new_fee

        if not math.isfinite(new_edge) or new_edge < min_edge:
            # Adding this level breaks the floor (or produced a non-finite edge).
            break

        acc_usd = new_usd
        acc_shares = new_shares
        limit_price = price
        _add_fill_level(fill_levels, price=price, shares=take_shares, usd=take_usd)

    if acc_shares <= 0 or acc_usd < min_bet_usd:
        return None

    filled_vwap = acc_usd / acc_shares
    realized_fee = _fee(filled_vwap)
    realized_edge = prob_safe_floor - filled_vwap - realized_fee
    if not math.isfinite(realized_edge) or realized_edge < min_edge:
        # Defensive: numerical drift should never occur, but fail closed.
        return None
    if return_levels:
        return acc_usd, acc_shares, filled_vwap, limit_price, realized_edge, fill_levels
    return acc_usd, acc_shares, filled_vwap, limit_price, realized_edge
