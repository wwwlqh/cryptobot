"""Shared P&L / edge primitives for the Polymarket crypto backtest.

Built for the crypto Up/Down market: one signal (model_p_up), two sides
(Up / Down), **theta-shape fee** on Polymarket crypto markets
(`feeRate = 0.07`, peak at p = 0.5).

Public API:
    edge(model_p_up, yes_price, no_price, side) -> ndarray
    pnl_per_bet(side, fill_price, won_up) -> ndarray
    unit_pnl(side, fill_price, won_up, slippage_haircut) -> float
    cash_pnl(side, fill_price, won_up, stake_usd) -> float
    simulate_bet_queue(bets, *, exposure_cap, ...) -> dict
    LUT_BUCKET_COUNT, LUT_BUCKET_EDGES, lut_p_bucket_indices(p) -> ndarray
    atomic_to_parquet(df, path, **kwargs) -> None

Fee shape (audit_parity Gap #5):
    Polymarket's actual taker fee for **crypto** markets is theta-style:
        fee_per_share = feeRate * fill_price * (1 - fill_price)
    with feeRate = 0.07 (peak ~1.75% at p=0.5, zero at p=0 or p=1). This is
    NOT a flat 2% on winnings as the prior implementation assumed; the prior
    formulation systematically overcharged near 0.5 and undercharged near 0/1.

    edge_up   = model_p_up       - yes_price - POLY_FEE_THETA * yes_price * (1 - yes_price)
    edge_down = (1 - model_p_up) - no_price  - POLY_FEE_THETA * no_price  * (1 - no_price)

    P&L per share is the same theta deduction applied to the WIN payout
    (no fee on a loss, per Polymarket V2 spec).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Polymarket crypto-category peak taker fee. Source: docs.polymarket.com/trading/fees.
# Theta-shape: fee = shares * theta * p * (1 - p), so peak effective fee = theta/4 at p=0.5.
# Updated 2026-05-16 from 0.018 to 0.072 per user correction.
# Updated 2026-08-05 from 0.072 to 0.07 (operator: the published value is 0.07;
# 0.072 was a transcription error). Peak effective fee = theta/4 = 1.75% at
# p=0.5. Kept in lockstep with cryptobot.config.POLY_FEE_THETA and
# cryptobot.strategy.parity.POLY_FEE_THETA (test_audit_parity, test_schema).
POLY_FEE_THETA = 0.07

# Shared exposure cap — pinned against the live copy by
# polymarket/src/cryptobot/tests/test_audit_parity.py.
CONCURRENT_EXPOSURE_CAP = 0.50  # 50% of capital allowed in pending notional


# Canonical LUT bucketing — the research-side single source of truth, kept in
# lockstep with ``cryptobot.strategy.parity`` by test_audit_parity. Its only
# remaining consumer is training/portfolio.py's rolling-window rebuild; the live
# LUT that also used it was retired 2026-07-31.
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
    """Scalar per-share P&L. Single source of truth shared by the backtest and
    the live scanner's ``polymarket/src/cryptobot/scheduler/resolution_sweep.py``.
    Keeping the formula in one place is the practical defense against
    backtest-parity drift.

    Matches `pnl_per_bet` on scalars:
        effective_fill = min(fill_price + slippage_haircut, 0.999)
        win  = (1 - effective_fill) - theta * effective_fill * (1 - effective_fill)
        lose = -effective_fill

    Accepts both ``"YES"/"NO"`` (cryptobot side vocabulary) and ``"Up"/"Down"``
    (backtest side vocabulary). YES == Up; NO == Down.

    Effective fill is clipped to ``[1e-6, 0.999]`` to match the exp044
    settlement (``polymarket.backtest.training.modeling._cash_pnl_vector``). Kept
    byte-identical to ``cryptobot.strategy.parity.unit_pnl`` (alignment #3).
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

    ``effective`` clipped to ``[1e-6, 0.999]`` to match the exp044 settlement
    and stay byte-identical to ``cryptobot.strategy.parity.cash_pnl`` (#3).
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    return unit_pnl(side, fill_price, won_up, slippage_haircut) * (
        float(stake_usd) / effective
    )
