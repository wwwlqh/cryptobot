"""Runtime copy of the strategy math shared with the backtest.

The live service imports this module instead of ``backtest.sweep_lib`` so
the EC2 runtime can stay a small ``src``-first package. Backtest tests still
compare these formulas against ``backtest.sweep_lib`` to catch drift.
"""

from __future__ import annotations

import numpy as np


# Polymarket's published peak taker fee for crypto. Must equal
# config.POLY_FEE_THETA and sweep_lib's.
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
    """Per-share PnL for a scalar fill. Accepts YES/NO and Up/Down side names.

    The effective fill is clipped to ``[1e-6, 0.999]``, as in the backtest's
    settlement (``modeling._cash_pnl_vector``). Keep this identical to
    ``polymarket.backtest.engine.sweep_lib.unit_pnl``.
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
    """Dollar PnL for a cash-sized bet.

    ``unit_pnl`` is per share and ``bet_size`` is USD, so shares bought =
    ``stake_usd / effective_fill``. The effective fill is clipped to ``[1e-6,
    0.999]``; keep this identical to
    ``polymarket.backtest.engine.sweep_lib.cash_pnl``.
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    return unit_pnl(side, fill_price, won_up, slippage_haircut) * (
        float(stake_usd) / effective
    )
