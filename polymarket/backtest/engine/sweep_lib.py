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

Fee shape (audit_parity Gap #5, plan ref ~/.claude/plans/there-r-so-many-async-emerson.md):
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


def simulate_bet_queue(
    bets: pd.DataFrame,
    *,
    exposure_cap: float,
    directional_cap: float | None = None,
    sort_keys: list[str] | None = None,
    return_picks: bool = False,
) -> dict:
    """Settle a pre-sized bet queue under shared bankroll + exposure cap.

    Canonical implementation of the per-tick exposure-cap dance. Each caller
    translates its domain object into the normalized columns below and this
    function settles the queue identically for all of them.

    Required columns on ``bets``:
        entry_ts (int)        — entry time
        resolve_ts (int)      — settlement time
        notional (float)      — face notional this bet uses against the cap
        pl (float)            — pre-computed dollar PnL for this bet

    Optional column (required only when ``directional_cap`` is set):
        side (str)            — "Up" or "Down" / "Yes" or "No"

    Args:
        exposure_cap: total pending notional cap in dollars
        directional_cap: optional per-side pending notional cap in dollars
        sort_keys: extra columns to sort by after ``entry_ts`` for
            deterministic tie-breaking. Default: just ``entry_ts``.
        return_picks: if True, additionally returns the kept rows (sorted)
            and per-bet PnL array under keys ``picks`` and ``pls``.

    Returns dict with keys ``n_bets``, ``total_pnl``, ``win_rate``,
    ``max_dd``. ``win_rate`` is NaN on empty / fully-rejected input
    (callers want to distinguish a zero-bet day from a 0% WR day).
    """
    empty_result = dict(n_bets=0, total_pnl=0.0, win_rate=float("nan"), max_dd=0.0)
    if return_picks:
        empty_result["picks"] = bets.iloc[0:0]
        empty_result["pls"] = np.array([], dtype=np.float64)
    if bets.empty:
        return empty_result

    sort_cols = ["entry_ts"] + (list(sort_keys) if sort_keys else [])
    sorted_bets = bets.sort_values(sort_cols).reset_index(drop=True)

    # active entry = (resolve_ts, notional, side); side is "" when directional
    # cap is unused — sum() over an empty filter is 0 either way.
    track_side = directional_cap is not None
    if track_side and "side" not in sorted_bets.columns:
        raise ValueError("simulate_bet_queue: directional_cap requires a 'side' column")

    active: list[tuple[int, float, str]] = []
    pls: list[float] = []
    kept_idx: list[int] = []

    for pos, row in enumerate(sorted_bets.itertuples(index=False)):
        entry_ts = int(row.entry_ts)
        active = [(rt, nt, sd) for rt, nt, sd in active if rt > entry_ts]
        pending = sum(nt for _, nt, _ in active)

        notional = float(row.notional)
        if pending + notional > exposure_cap:
            continue

        if track_side:
            row_side = str(row.side)
            pending_same_side = sum(nt for _, nt, sd in active if sd == row_side)
            if pending_same_side + notional > directional_cap:
                continue
        else:
            row_side = ""

        pls.append(float(row.pl))
        kept_idx.append(pos)
        active.append((int(row.resolve_ts), notional, row_side))

    if not pls:
        return empty_result

    pls_arr = np.array(pls, dtype=np.float64)
    cum = np.cumsum(pls_arr)
    peak = np.maximum.accumulate(cum)
    max_dd = float((peak - cum).max())
    result = dict(
        n_bets=len(pls),
        total_pnl=float(pls_arr.sum()),
        win_rate=float((pls_arr > 0).mean()),
        max_dd=max_dd,
    )
    if return_picks:
        result["picks"] = sorted_bets.iloc[kept_idx].reset_index(drop=True)
        result["pls"] = pls_arr
    return result


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


def pnl_per_bet(side: str, fill_price: np.ndarray, won_up: np.ndarray) -> np.ndarray:
    """P&L per share for a single resolved bet.

    win  = (1 - fill_price) - theta * fill_price * (1 - fill_price)
         = (1 - fill_price) * (1 - theta * fill_price)
    lose = -fill_price

    Polymarket V2 only charges fees on the winning side, so the loss leg is
    fee-free.
    """
    won_side = won_up if side == "Up" else (1 - won_up)
    win = (1.0 - fill_price) - POLY_FEE_THETA * fill_price * (1.0 - fill_price)
    lose = -fill_price
    return np.where(won_side == 1, win, lose)


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
