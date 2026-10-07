"""P&L and edge primitives for the Polymarket crypto up/down backtest.

One signal (model_p_up), two sides (Up / Down), and Polymarket's theta-shaped
crypto fee (theta = 0.07, highest at p = 0.5).

Public API:
    edge(model_p_up, yes_price, no_price, side) -> ndarray
    pnl_per_bet(side, fill_price, won_up) -> ndarray
    unit_pnl(side, fill_price, won_up, slippage_haircut) -> float
    cash_pnl(side, fill_price, won_up, stake_usd) -> float
    simulate_bet_queue(bets, *, exposure_cap, ...) -> dict
    LUT_BUCKET_COUNT, LUT_BUCKET_EDGES, lut_p_bucket_indices(p) -> ndarray
    atomic_to_parquet(df, path, **kwargs) -> None

Fee shape: the crypto taker fee is

    fee_per_share = theta * fill_price * (1 - fill_price)

about 1.75% at p = 0.5 and zero at 0 or 1 (not a flat 2% of winnings).

    edge_up   = model_p_up       - yes_price - POLY_FEE_THETA * yes_price * (1 - yes_price)
    edge_down = (1 - model_p_up) - no_price  - POLY_FEE_THETA * no_price  * (1 - no_price)

Per-share P&L here deducts the same fee from the win payout only. (Live grading
also charges it on losses; see ``cryptobot.execution.grade_fees``.)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Polymarket's published peak taker fee for crypto (docs.polymarket.com/
# trading/fees): fee = shares * theta * p * (1 - p), so the effective fee peaks
# at theta/4 = 1.75% at p = 0.5. Must equal cryptobot.config.POLY_FEE_THETA and
# cryptobot.strategy.parity.POLY_FEE_THETA (test_audit_parity, test_schema).
POLY_FEE_THETA = 0.07

# Shared exposure cap — pinned against the live copy by
# polymarket/src/cryptobot/tests/test_audit_parity.py.
CONCURRENT_EXPOSURE_CAP = 0.50  # 50% of capital allowed in pending notional


# Probability bucketing for hit-rate tables, kept equal to
# ``cryptobot.strategy.parity`` by test_audit_parity. Used by
# training/portfolio.py.
LUT_BUCKET_COUNT: int = 10
LUT_BUCKET_EDGES: np.ndarray = np.linspace(0.0, 1.0, LUT_BUCKET_COUNT + 1)


def lut_p_bucket_indices(p: np.ndarray, bucket_count: int = LUT_BUCKET_COUNT) -> np.ndarray:
    """Return clipped bucket indices for model probabilities ``p``.

    Same formula used everywhere a LUT is built or read:
        idx = clip(searchsorted(edges, p, side='right') - 1, 0, bucket_count - 1)
    """
    edges = LUT_BUCKET_EDGES if bucket_count == LUT_BUCKET_COUNT else np.linspace(0.0, 1.0, bucket_count + 1)
    return np.clip(
        np.searchsorted(edges, np.asarray(p), side="right") - 1,
        0,
        bucket_count - 1,
    )


def atomic_to_parquet(df: pd.DataFrame, path: str | os.PathLike[str], **kwargs: Any) -> None:
    """Write ``df`` to ``path`` atomically (write tmp, then ``os.replace``).

    Closes the half-written-file race against any reader that polls the
    canonical path while a rebuild is running.
    """
    out_path = Path(path)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    df.to_parquet(tmp, **kwargs)
    os.replace(tmp, out_path)


def edge(model_p_up: np.ndarray, yes_price: np.ndarray, no_price: np.ndarray, side: str) -> np.ndarray:
    """Fee-adjusted edge using the theta-shape Polymarket crypto fee.

    edge_up   = p   - yp - POLY_FEE_THETA * yp  * (1 - yp)
    edge_down = 1-p - np - POLY_FEE_THETA * np  * (1 - np)

    NaN/inf inputs propagate (caller is responsible for masking).
    """
    if side == "Up":
        return model_p_up - yes_price - POLY_FEE_THETA * yes_price * (1.0 - yes_price)
    return (1.0 - model_p_up) - no_price - POLY_FEE_THETA * no_price * (1.0 - no_price)


def unit_pnl(
    side: str,
    fill_price: float,
    won_up: bool | int,
    slippage_haircut: float = 0.0,
) -> float:
    """Per-share P&L for a scalar fill, shared with the live bot's resolution
    sweep so the two cannot drift.

    Matches ``pnl_per_bet`` on scalars:
        effective_fill = min(fill_price + slippage_haircut, 0.999)
        win  = (1 - effective_fill) - theta * effective_fill * (1 - effective_fill)
        lose = -effective_fill

    Accepts ``"YES"/"NO"`` (live) and ``"Up"/"Down"`` (backtest); YES == Up and
    NO == Down.

    The effective fill is clipped to ``[1e-6, 0.999]``, as in
    ``polymarket.backtest.training.modeling._cash_pnl_vector``. Keep this
    identical to ``cryptobot.strategy.parity.unit_pnl``.
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    won_side = (str(side).upper() in ("YES", "UP")) == bool(won_up)
    if won_side:
        return (1.0 - effective) - POLY_FEE_THETA * effective * (1.0 - effective)
    return -effective


def cash_pnl(
    side: str,
    fill_price: float,
    won_up: bool | int,
    stake_usd: float,
    slippage_haircut: float = 0.0,
) -> float:
    """Dollar P&L for a cash-sized bet.

    The effective fill is clipped to ``[1e-6, 0.999]``; keep this identical to
    ``cryptobot.strategy.parity.cash_pnl``.
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    return unit_pnl(side, fill_price, won_up, slippage_haircut) * (
        float(stake_usd) / effective
    )
