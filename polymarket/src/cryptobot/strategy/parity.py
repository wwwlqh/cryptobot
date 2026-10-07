"""Runtime copy of the strategy math shared with the backtest.

The live service imports this module instead of ``backtest.sweep_lib`` so
the EC2 runtime can stay a small ``src``-first package. Backtest tests still
compare these formulas against ``backtest.sweep_lib`` to catch drift.
"""

from __future__ import annotations

import numpy as np


# Published Polymarket crypto peak taker fee. 2026-08-05: 0.072 -> 0.07
# (operator correction). Pinned == config.POLY_FEE_THETA == sweep_lib's.
POLY_FEE_THETA = 0.07
INITIAL_CAPITAL = 100.0
CONCURRENT_EXPOSURE_CAP = 0.50


def edge(model_p_up: np.ndarray, yes_price: np.ndarray, no_price: np.ndarray, side: str) -> np.ndarray:
    """Fee-adjusted edge using the theta-shape Polymarket crypto fee."""
    if side == "Up":
        return model_p_up - yes_price - POLY_FEE_THETA * yes_price * (1.0 - yes_price)
    return (1.0 - model_p_up) - no_price - POLY_FEE_THETA * no_price * (1.0 - no_price)


def unit_pnl(
    side: str,
    fill_price: float,
    won_up: bool | int,
    slippage_haircut: float = 0.0,
) -> float:
    """Scalar per-share PnL. Accepts YES/NO and Up/Down side names.

    Effective fill is clipped to ``[1e-6, 0.999]`` to match the exp044 backtest
    settlement (``polymarket.backtest.training.modeling._cash_pnl_vector`` uses
    ``np.clip(ask, 1e-6, 0.999)``). The lower bound matters only for sub-grid
    fills (< 1e-6) that the walker never produces live; it removes the former
    ``effective <= 0 -> NaN`` divergence (alignment finding #3). Keep this clip
    byte-identical to ``polymarket.backtest.engine.sweep_lib.unit_pnl``.
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

    ``unit_pnl`` is per share. Runtime ``bet_size`` is filled USD, so shares
    bought are ``stake_usd / effective_fill``. ``effective`` is clipped to
    ``[1e-6, 0.999]`` to match the exp044 backtest settlement and keep this
    byte-identical to ``polymarket.backtest.engine.sweep_lib.cash_pnl``.
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    return unit_pnl(side, fill_price, won_up, slippage_haircut) * (
        float(stake_usd) / effective
    )
