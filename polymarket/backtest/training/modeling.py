"""Evaluate fresh-L2 residual-edge signals on ABCD walk-forward chunks.

This is a research harness for the "market edge is better than mine" phase.
It does not deploy anything. It answers a narrower question:

    Given only fresh executable L2 books, can either the current RF
    probability or a simple residual-edge model produce positive PnL on
    chunks B, C, and D while keeping max drawdown <= 30%?

The active goal uses the repo convention: 0.30 as a fraction means 30% max
drawdown. Earlier in the thread we also tested the literal 0.003 case; that is
too tight for a $100 Polymarket wallet with a practical $0.50 minimum order.

Output is JSON so the ce-optimize experiment log can record it verbatim.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# Keep compiled replay artifacts with the retained research caches instead of
# scattering them through source-tree __pycache__ directories. BT_CACHE_DIR is
# the legacy-first shim, so this lands beside every other research cache
# (backtest/cache/ in an existing checkout, polymarket/backtest/cache/ in a
# fresh clone) rather than at a hardcoded new-layout path. Imported here, ahead
# of sklearn/numba, because NUMBA_CACHE_DIR must be set before numba loads.
from polymarket.backtest import BT_CACHE_DIR, REPO_ROOT as ROOT

os.environ.setdefault("NUMBA_CACHE_DIR", str(BT_CACHE_DIR / "numba"))

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

SRC_DIR = ROOT / "core" / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from polymarket.backtest.training.portfolio import _load_book_idx, _load_full_table, _real_vwap
from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA
from cryptobot.execution.walk_book import walk_book_edge_preserving

CHUNK_A_START = pd.Timestamp("2026-02-09", tz="UTC")
CHUNK_B_START = pd.Timestamp("2026-03-16", tz="UTC")
CHUNK_C_START = pd.Timestamp("2026-04-06", tz="UTC")
CHUNK_D_START = pd.Timestamp("2026-05-01", tz="UTC")
HOLDOUT_START = pd.Timestamp("2026-05-13", tz="UTC")
DATA_END = pd.Timestamp("2026-05-19 06:00", tz="UTC")

COIN_MAP = {"btc": 0, "eth": 1, "sol": 2, "xrp": 3}
INTERVAL_MAP = {"5m": 5, "15m": 15}
FOLD_EMBARGO_SECONDS = 3600
PMDATA_MAX_STALENESS_SECONDS = 60
SELECTION_FOLDS = frozenset({"B", "C"})
VALIDATION_FOLD = "D"
ASK_BUCKET_DIAGNOSTIC_RANGES = {
    "all": ((0.0, 1.0),),
    "low": ((0.0, 0.4),),
    "mid": ((0.4, 0.6),),
    "high": ((0.6, 1.0),),
    "tail": ((0.0, 0.35), (0.65, 1.0)),
}

FEATURE_COLUMNS = [
    "model_p_side",
    "rf_edge",
    "ask",
    "other_ask",
    "book_spread",
    "log_depth",
    "depth_imbalance",
    "partial_ret_z_side",
    "abs_partial_ret_z",
    "coin_id",
    "interval_min",
    "entry_minute",
    "side_up",
    "hour_utc",
    "dow_utc",
    "p_mean_entry",
    "p_std_entry",
    "target_vs_entry_mean",
    "consensus_for_side",
]

FEATURE_GROUPS = {
    "rf": ["model_p_side", "rf_edge"],
    "price": ["ask", "other_ask", "book_spread"],
    "depth": ["log_depth", "depth_imbalance"],
    "momentum": ["partial_ret_z_side", "abs_partial_ret_z"],
    "identity": ["coin_id", "interval_min", "side_up"],
    "time": ["entry_minute", "hour_utc", "dow_utc"],
    "cross": ["p_mean_entry", "p_std_entry", "target_vs_entry_mean", "consensus_for_side"],
}


@dataclass
class FoldResult:
    fold: str
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    signal: str
    execution_price: str
    sizing_mode: str
    feature_set: str
    side_filter: str
    interval_filter: str
    ask_bucket: str
    book_consumption_max: float
    max_bet_usd: float | None
    threshold: float
    stake_frac: float
    n_candidates: int
    n_bets: int
    n_rejected_execution: int
    total_pnl: float
    pnl_frac: float
    win_rate: float | None
    max_dd_frac: float
    worst_30bet_dd_frac: float
    mean_pred_edge: float | None
    mean_fill: float | None
    mean_fill_slippage: float | None
    mean_notional: float | None
    max_notional: float | None
    final_equity: float
    dd_halted: bool
    passed: bool


@dataclass(frozen=True)
class ExecutionFill:
    """Executed notional and VWAP produced by the backtest execution model."""

    notional: float
    fill: float


def _parse_floats(raw: str) -> list[float]:
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


def _feature_columns(raw_groups: str) -> tuple[str, list[str]]:
    groups = [g.strip().lower() for g in raw_groups.split(",") if g.strip()]
    if not groups or groups == ["all"]:
        return "all", FEATURE_COLUMNS

    unknown = sorted(set(groups) - set(FEATURE_GROUPS))
    if unknown:
        raise ValueError(f"unknown feature group(s): {', '.join(unknown)}")

    columns: list[str] = []
    for group in groups:
        for col in FEATURE_GROUPS[group]:
            if col not in columns:
                columns.append(col)
    return "+".join(groups), columns


def _fee(ask: np.ndarray | pd.Series) -> np.ndarray:
    a = np.asarray(ask, dtype=np.float64)
    return POLY_FEE_THETA * a * (1.0 - a)


def _cash_pnl_vector(side_won: np.ndarray, ask: np.ndarray, notional: float) -> np.ndarray:
    ask = np.asarray(ask, dtype=np.float64)
    side_won = np.asarray(side_won, dtype=bool)
    effective_fill = np.clip(ask, 1e-6, 0.999)
    shares = notional / effective_fill
    win_per_share = (1.0 - effective_fill) - _fee(effective_fill)
    return np.where(side_won, shares * win_per_share, -notional)


def _max_dd_frac(pls: np.ndarray, capital: float) -> float:
    if len(pls) == 0:
        return 0.0
    equity = capital + np.cumsum(np.asarray(pls, dtype=np.float64))
    equity = np.concatenate(([capital], equity))
    peak = np.maximum.accumulate(equity)
    dd = peak - equity
    denom = np.maximum(peak, 1e-9)
    return float((dd / denom).max())


def _worst_rolling_dd_frac(pls: np.ndarray, capital: float, window: int = 30) -> float:
    if len(pls) < window:
        return 0.0
    # Prepend the starting capital so the rolling peak at the first window
    # position is the pre-bet bankroll, matching _max_dd_frac. Without this,
    # an opening losing streak's drawdown measured from initial capital is
    # silently excluded from the first window.
    equity = np.concatenate(([capital], capital + np.cumsum(np.asarray(pls, dtype=np.float64))))
    ser = pd.Series(equity)
    roll_peak = ser.rolling(window, min_periods=1).max()
    dd = roll_peak.to_numpy() - equity
    return float((dd / np.maximum(roll_peak.to_numpy(), 1e-9)).max())


def _prepare_base_frame(coins: set[str], intervals: set[str], label_col: str) -> pd.DataFrame:
    t0 = time.time()
    df = _load_full_table().copy()
    print(f"[prepare] loaded full fresh-L2 frame in {time.time() - t0:.1f}s", file=sys.stderr)
    df["coin"] = df["coin"].str.lower()
    df = df[df["coin"].isin(coins) & df["interval"].isin(intervals)].copy()
    df = df.dropna(subset=[label_col, "partial_ret_z_1h", "model_p_v1"])
    df[label_col] = df[label_col].astype(np.int8)

    entry = pd.to_datetime(df["entry_ts"], unit="s", utc=True)
    df["hour_utc"] = entry.dt.hour.astype(np.int8)
    df["dow_utc"] = entry.dt.dayofweek.astype(np.int8)
    df["coin_id"] = df["coin"].map(COIN_MAP).astype(np.int8)
    df["interval_min"] = df["interval"].map(INTERVAL_MAP).astype(np.int8)

    grp = df.groupby(["interval", "entry_ts"], observed=True)["model_p_v1"]
    df["p_mean_entry"] = grp.transform("mean")
    df["p_std_entry"] = grp.transform("std").fillna(0.0)
    df["n_entry"] = grp.transform("size").astype(np.int8)
    df["up_votes_entry"] = (
        df["model_p_v1"].gt(0.5).astype(np.int8)
        .groupby([df["interval"], df["entry_ts"]], observed=True)
        .transform("sum")
        .astype(np.int8)
    )
    return df


def _side_rows(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    side_specs = [
        ("Up", "up_ask_l2", "no_ask_l2", "up_depth_l2", "no_depth_l2", 1),
        ("Down", "no_ask_l2", "up_ask_l2", "no_depth_l2", "up_depth_l2", 0),
    ]
    for side, ask_col, other_ask_col, depth_col, other_depth_col, win_label in side_specs:
        sub = df[np.isfinite(df[ask_col]) & (df[ask_col] > 0.0) & (df[ask_col] < 1.0)].copy()
        if sub.empty:
            continue
        ask = sub[ask_col].astype(np.float64)
        other_ask = sub[other_ask_col].astype(np.float64).where(np.isfinite(sub[other_ask_col]), np.nan)
        depth = sub[depth_col].astype(np.float64).where(np.isfinite(sub[depth_col]), np.nan)
        other_depth = sub[other_depth_col].astype(np.float64).where(np.isfinite(sub[other_depth_col]), np.nan)
        model_p_side = sub["model_p_v1"] if side == "Up" else (1.0 - sub["model_p_v1"])
        y = (sub[label_col].astype(np.int8) == win_label).astype(np.int8)

        out = pd.DataFrame({
            "market_slug": sub["market_slug"].to_numpy(),
            "coin": sub["coin"].to_numpy(),
            "interval": sub["interval"].to_numpy(),
            "entry_minute": sub["entry_minute"].to_numpy(np.int16),
            "entry_ts": sub["entry_ts"].to_numpy(np.int64),
            "resolve_ts": sub["resolve_ts"].to_numpy(np.int64),
            "entry_dt": sub["entry_dt"].to_numpy(),
            "side": side,
            "y": y.to_numpy(np.int8),
            "ask": ask.to_numpy(np.float64),
            "other_ask": other_ask.to_numpy(np.float64),
            "depth": depth.to_numpy(np.float64),
            "other_depth": other_depth.to_numpy(np.float64),
            "model_p_side": model_p_side.to_numpy(np.float64),
            "coin_id": sub["coin_id"].to_numpy(np.int8),
            "interval_min": sub["interval_min"].to_numpy(np.int8),
            "hour_utc": sub["hour_utc"].to_numpy(np.int8),
            "dow_utc": sub["dow_utc"].to_numpy(np.int8),
            "p_mean_entry": sub["p_mean_entry"].to_numpy(np.float64),
            "p_std_entry": sub["p_std_entry"].to_numpy(np.float64),
            "n_entry": sub["n_entry"].to_numpy(np.int8),
            "up_votes_entry": sub["up_votes_entry"].to_numpy(np.int8),
            "partial_ret_z_1h": sub["partial_ret_z_1h"].to_numpy(np.float64),
        })
        out["side_up"] = 1 if side == "Up" else 0
        out["rf_edge"] = out["model_p_side"] - out["ask"] - _fee(out["ask"])
        out["book_spread"] = out["ask"] + out["other_ask"] - 1.0
        out["log_depth"] = np.log1p(out["depth"].clip(lower=0.0))
        out["depth_imbalance"] = np.log1p(out["depth"].clip(lower=0.0)) - np.log1p(out["other_depth"].clip(lower=0.0))
        sign = 1.0 if side == "Up" else -1.0
        out["partial_ret_z_side"] = sign * out["partial_ret_z_1h"]
        out["abs_partial_ret_z"] = out["partial_ret_z_1h"].abs()
        out["target_vs_entry_mean"] = out["model_p_side"] - np.where(
            side == "Up", out["p_mean_entry"], 1.0 - out["p_mean_entry"]
        )
        out["consensus_for_side"] = np.where(
            side == "Up", out["up_votes_entry"], out["n_entry"] - out["up_votes_entry"]
        )
        frames.append(out)
    if not frames:
        return pd.DataFrame()
    side_df = pd.concat(frames, ignore_index=True)
    side_df = side_df.replace([np.inf, -np.inf], np.nan)
    return side_df


def _sample_weights(train: pd.DataFrame, mode: str) -> np.ndarray | None:
    if mode == "none":
        return None

    entry = pd.to_datetime(train["entry_dt"], utc=True)
    if mode.startswith("exp"):
        half_life_days = float(mode.replace("exp", ""))
        age_days = (entry.max() - entry).dt.total_seconds().to_numpy(np.float64) / 86400.0
        weights = np.exp(-math.log(2.0) * age_days / half_life_days)
    elif mode == "recent_chunk_boost":
        weights = np.where(
            entry >= CHUNK_C_START,
            4.0,
            np.where(entry >= CHUNK_B_START, 2.0, 1.0),
        ).astype(np.float64)
    elif mode == "fold_recent_boost":
        # Recency weights within the fold: oldest third 1x, middle third 2x,
        # newest third 4x, rather than fixed calendar dates.
        n = len(train)
        if n == 0:
            return np.zeros(0, dtype=np.float64)
        order = np.argsort(entry.to_numpy(dtype="datetime64[ns]"), kind="stable")
        weights = np.ones(n, dtype=np.float64)
        first_cut = n // 3
        second_cut = (2 * n) // 3
        weights[order[first_cut:second_cut]] = 2.0
        weights[order[second_cut:]] = 4.0
    else:
        raise ValueError(f"unknown sample weight mode {mode!r}")

    mean = float(np.mean(weights))
    return weights / mean if mean > 0 else weights


def _build_hgb_pipe(
    *,
    seed: int,
    hgb_max_iter: int = 80,
    hgb_learning_rate: float = 0.06,
    hgb_max_leaf_nodes: int = 31,
    hgb_l2_regularization: float = 0.02,
):
    """Build the unfitted SimpleImputer(median) -> HGB pipeline.

    The one constructor for this estimator, so backtest replays and exported
    models are built identically. Do not inline it elsewhere.
    """
    model = HistGradientBoostingClassifier(
        max_iter=hgb_max_iter,
        learning_rate=hgb_learning_rate,
        max_leaf_nodes=hgb_max_leaf_nodes,
        l2_regularization=hgb_l2_regularization,
        random_state=seed,
    )
    return make_pipeline(SimpleImputer(strategy="median"), model)


def _fit_pipe(pipe, train: pd.DataFrame, feature_columns: list[str], sample_weight_mode: str):
    """Fit `pipe` on `train[feature_columns]` with the sleeve's sample weighting.

    Shared by `_fit_predict` (backtest) and `export_models` (live) so the
    fit — including the `recent_chunk_boost` sample weights and the
    `<final_step>__sample_weight` fit-kwarg wiring — is identical on both paths.
    """
    x_train = train[feature_columns]
    y_train = train["y"].astype(np.int8)
    weights = _sample_weights(train, sample_weight_mode)
    fit_kwargs = {}
    if weights is not None:
        step_name = pipe.steps[-1][0]  # == final estimator's lowercased class name
        fit_kwargs[f"{step_name}__sample_weight"] = weights
    pipe.fit(x_train, y_train, **fit_kwargs)
    return pipe


def _fit_predict(
    train: pd.DataFrame,
    test: pd.DataFrame,
    model_name: str,
    seed: int,
    sample_weight_mode: str,
    feature_columns: list[str],
    hgb_max_iter: int = 80,
    hgb_learning_rate: float = 0.06,
    hgb_max_leaf_nodes: int = 31,
    hgb_l2_regularization: float = 0.02,
) -> np.ndarray:
    x_test = test[feature_columns]
    if model_name == "hgb":
        pipe = _build_hgb_pipe(
            seed=seed,
            hgb_max_iter=hgb_max_iter,
            hgb_learning_rate=hgb_learning_rate,
            hgb_max_leaf_nodes=hgb_max_leaf_nodes,
            hgb_l2_regularization=hgb_l2_regularization,
        )
    elif model_name == "logistic":
        model = SGDClassifier(
            loss="log_loss",
            penalty="elasticnet",
            alpha=1e-5,
            l1_ratio=0.05,
            max_iter=2000,
            tol=1e-4,
            random_state=seed,
        )
        pipe = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), model)
    else:
        raise ValueError(f"unknown model {model_name!r}")
    _fit_pipe(pipe, train, feature_columns, sample_weight_mode)
    return pipe.predict_proba(x_test)[:, 1]


def _dedupe_best_side(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    keys = ["market_slug", "coin", "interval", "entry_ts"]
    return (
        candidates.sort_values(["pred_edge", "model_prob"], ascending=[False, False])
        .drop_duplicates(subset=keys, keep="first")
        .sort_values(["entry_ts", "interval", "coin", "side"])
        .reset_index(drop=True)
    )


def _book_for_execution_row(book_idx, slug: str, side: str, ts: int) -> tuple[dict, float] | None:
    """Return a live-walker compatible ask book and total ask notional."""
    entry = book_idx.index.get((slug, side, int(ts)))
    if entry is None:
        return None
    offset, n_lvls, is_cf = entry
    if is_cf:
        return None

    prices = book_idx.prices[offset:offset + n_lvls]
    if hasattr(book_idx, "sizes"):
        sizes = book_idx.sizes[offset:offset + n_lvls]
    else:
        cum_shares = book_idx.cum_shares[offset:offset + n_lvls]
        sizes = np.diff(np.concatenate(([0.0], np.asarray(cum_shares, dtype=np.float64))))

    asks: list[dict[str, float]] = []
    total_depth_usd = 0.0
    for price, size in zip(prices, sizes):
        p = float(price)
        s = float(size)
        if math.isfinite(p) and math.isfinite(s) and p > 0.0 and s > 0.0:
            asks.append({"price": p, "size": s})
            total_depth_usd += p * s
    if not asks:
        return None
    return {"asks": asks}, total_depth_usd


def _execution_fill_for_row(
    row,
    *,
    notional: float,
    execution_price: str,
    book_consumption_max: float,
    book_idx,
    max_walk_price: float | None = None,
    prob_safe_floor: float | None = None,
    min_edge: float = -1.0,
    min_bet_usd: float = 0.50,
) -> ExecutionFill | None:
    if execution_price == "top_ask":
        fill = float(row.ask)
        if math.isfinite(fill) and 0.0 < fill < 1.0 and notional >= min_bet_usd:
            return ExecutionFill(notional=float(notional), fill=fill)
        return None

    if book_idx is None:
        return None

    if max_walk_price is not None:
        if prob_safe_floor is None:
            prob_safe_floor = getattr(row, "model_prob", None)
        if prob_safe_floor is None:
            return None
        book_payload = _book_for_execution_row(
            book_idx,
            str(row.market_slug),
            str(row.side),
            int(row.entry_ts),
        )
        if book_payload is None:
            return None
        book, book_depth = book_payload
        walked = walk_book_edge_preserving(
            book,
            float(notional),
            prob_safe_floor=float(prob_safe_floor),
            fee_theta=POLY_FEE_THETA,
            min_edge=float(min_edge),
            min_bet_usd=float(min_bet_usd),
            max_walk_price=float(max_walk_price),
        )
        if walked is None:
            return None
        filled_usd = float(walked[0])
        fill = max(float(walked[2]), float(row.ask))
        realized_fee = POLY_FEE_THETA * fill * (1.0 - fill)
        realized_edge = float(prob_safe_floor) - fill - realized_fee
        if not math.isfinite(realized_edge) or realized_edge < float(min_edge):
            return None
        if not math.isfinite(book_depth) or book_depth <= 0.0:
            return None
        if filled_usd > book_depth * book_consumption_max + 1e-9:
            return None
        if math.isfinite(fill) and 0.0 < fill < 1.0 and filled_usd >= min_bet_usd:
            return ExecutionFill(notional=filled_usd, fill=fill)
        return None

    depth = float(row.depth)
    if not math.isfinite(depth) or depth <= 0.0:
        return None
    if notional > depth * book_consumption_max:
        return None

    fill, _ = _real_vwap(
        book_idx,
        str(row.market_slug),
        str(row.side),
        int(row.entry_ts),
        float(notional),
        depth,
    )
    if math.isfinite(fill) and 0.0 < fill < 1.0:
        return ExecutionFill(notional=float(notional), fill=max(float(fill), float(row.ask)))
    return None


def _attach_book_positions(df: pd.DataFrame, book_idx) -> pd.DataFrame:
    if book_idx is None or df.empty:
        return df
    if {"book_offset", "book_n_lvls"}.issubset(df.columns):
        return df
    out = df.copy()
    offsets = np.full(len(out), -1, dtype=np.int64)
    n_lvls = np.zeros(len(out), dtype=np.int32)
    for i, row in enumerate(out[["market_slug", "side", "entry_ts"]].itertuples(index=False)):
        entry = book_idx.index.get((str(row.market_slug), str(row.side), int(row.entry_ts)))
        if entry is None:
            continue
        offset, n, is_cf = entry
        if is_cf or n <= 0:
            continue
        offsets[i] = int(offset)
        n_lvls[i] = int(n)
    out["book_offset"] = offsets
    out["book_n_lvls"] = n_lvls
    return out


def _compiled_replay_impl(
    entry_ts,
    resolve_ts,
    ask,
    depth,
    pred_edge,
    y,
    book_offset,
    book_n_lvls,
    prices,
    cum_cost,
    cum_shares,
    capital,
    max_dd_frac,
    exposure_cap_frac,
    min_bet_usd,
    book_consumption_max,
    max_bet_usd,
    stake_frac,
    sizing_is_compound,
):
    n = len(entry_ts)
    active_resolve = np.empty(n, dtype=np.int64)
    active_entry = np.empty(n, dtype=np.int64)
    active_notional = np.empty(n, dtype=np.float64)
    active_pl = np.empty(n, dtype=np.float64)
    active_count = 0
    realized_pls = np.empty(n, dtype=np.float64)
    realized_count = 0
    kept_count = 0
    n_rejected_execution = 0
    equity = float(capital)
    peak = float(capital)
    dd_halted = False
    sum_pred_edge = 0.0
    sum_fill = 0.0
    sum_slippage = 0.0
    sum_notional = 0.0
    max_notional = 0.0
    starting_notional = capital * stake_frac
    if max_bet_usd >= 0.0 and starting_notional > max_bet_usd:
        starting_notional = max_bet_usd

    for i in range(n):
        cutoff_ts = entry_ts[i]
        while True:
            best_idx = -1
            best_resolve = 0
            best_entry = 0
            for j in range(active_count):
                r_ts = active_resolve[j]
                e_ts = active_entry[j]
                if r_ts <= cutoff_ts:
                    if best_idx < 0 or r_ts < best_resolve or (r_ts == best_resolve and e_ts < best_entry):
                        best_idx = j
                        best_resolve = r_ts
                        best_entry = e_ts
            if best_idx < 0:
                break
            pl = active_pl[best_idx]
            equity += pl
            realized_pls[realized_count] = pl
            realized_count += 1
            if equity > peak:
                peak = equity
            dd_ratio = (peak - equity) / peak if peak > 0.0 else 1.0
            if dd_ratio >= max_dd_frac or equity <= 0.0:
                dd_halted = True
            active_count -= 1
            if best_idx != active_count:
                active_resolve[best_idx] = active_resolve[active_count]
                active_entry[best_idx] = active_entry[active_count]
                active_notional[best_idx] = active_notional[active_count]
                active_pl[best_idx] = active_pl[active_count]

        if dd_halted:
            break

        notional = equity * stake_frac if sizing_is_compound else starting_notional
        if max_bet_usd >= 0.0 and notional > max_bet_usd:
            notional = max_bet_usd
        if notional < min_bet_usd:
            continue

        exposure_cap = (equity if sizing_is_compound else capital) * exposure_cap_frac
        pending = 0.0
        for j in range(active_count):
            pending += active_notional[j]
        if pending + notional > exposure_cap:
            continue

        offset = book_offset[i]
        n_book = book_n_lvls[i]
        row_depth = depth[i]
        if offset < 0 or n_book <= 0 or not np.isfinite(row_depth) or row_depth <= 0.0:
            n_rejected_execution += 1
            continue
        if notional > row_depth * book_consumption_max:
            n_rejected_execution += 1
            continue

        total_book_cost = float(cum_cost[offset + n_book - 1])
        if not np.isfinite(total_book_cost) or total_book_cost <= 0.0:
            n_rejected_execution += 1
            continue
        if notional >= total_book_cost:
            total_shares = float(cum_shares[offset + n_book - 1])
            if total_shares <= 0.0:
                n_rejected_execution += 1
                continue
            fill = total_book_cost / total_shares
        else:
            local_idx = 0
            while local_idx < n_book and float(cum_cost[offset + local_idx]) <= notional:
                local_idx += 1
            if local_idx >= n_book:
                total_shares = float(cum_shares[offset + n_book - 1])
                fill = total_book_cost / total_shares if total_shares > 0.0 else np.nan
            else:
                prev_cost = float(cum_cost[offset + local_idx - 1]) if local_idx > 0 else 0.0
                prev_shares = float(cum_shares[offset + local_idx - 1]) if local_idx > 0 else 0.0
                partial_price = float(prices[offset + local_idx])
                if partial_price <= 0.0:
                    n_rejected_execution += 1
                    continue
                partial_shares = (notional - prev_cost) / partial_price
                total_shares = prev_shares + partial_shares
                fill = notional / total_shares if total_shares > 0.0 else np.nan
        if fill < ask[i]:
            fill = ask[i]
        if not np.isfinite(fill) or fill <= 0.0 or fill >= 1.0:
            n_rejected_execution += 1
            continue

        fee = POLY_FEE_THETA * fill * (1.0 - fill)
        if y[i] == 1:
            shares = notional / fill
            pl = shares * ((1.0 - fill) - fee)
        else:
            pl = -notional
        if not np.isfinite(pl):
            continue

        active_resolve[active_count] = resolve_ts[i]
        active_entry[active_count] = entry_ts[i]
        active_notional[active_count] = notional
        active_pl[active_count] = pl
        active_count += 1
        kept_count += 1
        sum_pred_edge += pred_edge[i]
        sum_fill += fill
        sum_slippage += fill - ask[i]
        sum_notional += notional
        if notional > max_notional:
            max_notional = notional

    while active_count > 0:
        best_idx = 0
        best_resolve = active_resolve[0]
        best_entry = active_entry[0]
        for j in range(1, active_count):
            r_ts = active_resolve[j]
            e_ts = active_entry[j]
            if r_ts < best_resolve or (r_ts == best_resolve and e_ts < best_entry):
                best_idx = j
                best_resolve = r_ts
                best_entry = e_ts
        pl = active_pl[best_idx]
        equity += pl
        realized_pls[realized_count] = pl
        realized_count += 1
        if equity > peak:
            peak = equity
        dd_ratio = (peak - equity) / peak if peak > 0.0 else 1.0
        if dd_ratio >= max_dd_frac or equity <= 0.0:
            dd_halted = True
        active_count -= 1
        if best_idx != active_count:
            active_resolve[best_idx] = active_resolve[active_count]
            active_entry[best_idx] = active_entry[active_count]
            active_notional[best_idx] = active_notional[active_count]
            active_pl[best_idx] = active_pl[active_count]

    return (
        realized_pls[:realized_count].copy(),
        kept_count,
        n_rejected_execution,
        sum_pred_edge,
        sum_fill,
        sum_slippage,
        sum_notional,
        max_notional,
        equity,
        dd_halted,
    )


_compiled_replay = None
_compiled_replay_loaded = False


def _get_compiled_replay():
    global _compiled_replay, _compiled_replay_loaded
    if _compiled_replay_loaded:
        return _compiled_replay
    _compiled_replay_loaded = True
    if os.environ.get("NUMBA_DISABLE_JIT") == "1":
        return None
    try:
        from numba import njit

        _compiled_replay = njit(cache=True)(_compiled_replay_impl)
    except Exception:  # pragma: no cover - optional acceleration
        _compiled_replay = None
    return _compiled_replay


def _evaluate_one(
    test: pd.DataFrame,
    *,
    signal: str,
    execution_price: str,
    sizing_mode: str,
    feature_set: str,
    side_filter: str,
    interval_filter: str,
    ask_bucket: str,
    threshold: float,
    stake_frac: float,
    capital: float,
    max_dd_frac: float,
    min_fold_bets: int,
    exposure_cap_frac: float,
    min_bet_usd: float,
    book_consumption_max: float,
    max_bet_usd: float | None,
    fold_meta: tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp],
    book_idx=None,
    preselected_candidates: bool = False,
) -> FoldResult:
    name, train_start, train_end, test_start, test_end = fold_meta
    if preselected_candidates:
        candidates = test
    else:
        candidates = test[test["pred_edge"] >= threshold].copy()
        candidates = _dedupe_best_side(candidates)
    starting_notional = capital * stake_frac
    if max_bet_usd is not None:
        starting_notional = min(starting_notional, max_bet_usd)
    # Only kill candidates wholesale in fixed-size mode. In compound mode the
    # per-bet notional is equity*stake_frac which grows as equity compounds, so
    # an initial notional below min_bet_usd does NOT mean no bets are ever placed.
    # The per-bet floor at the replay loop (notional < min_bet_usd: continue)
    # handles individual bets correctly for both modes.
    if sizing_mode != "compound" and starting_notional < min_bet_usd:
        candidates = candidates.iloc[0:0].copy()
    n_candidates = int(len(candidates))
    n_rejected_execution = 0

    can_use_compiled_replay = (
        execution_price == "vwap"
        and book_idx is not None
        and n_candidates > 0
        and {"book_offset", "book_n_lvls"}.issubset(candidates.columns)
        and hasattr(book_idx, "cum_cost")
        and hasattr(book_idx, "cum_shares")
    )
    compiled_replay = _get_compiled_replay() if can_use_compiled_replay else None
    if compiled_replay is not None:
        pls, kept_count, n_rejected_execution, sum_pred_edge, sum_fill, sum_slippage, sum_notional, max_notional, _equity, dd_halted = compiled_replay(
            candidates["entry_ts"].to_numpy(np.int64),
            candidates["resolve_ts"].to_numpy(np.int64),
            candidates["ask"].to_numpy(np.float64),
            candidates["depth"].to_numpy(np.float64),
            candidates["pred_edge"].to_numpy(np.float64),
            candidates["y"].to_numpy(np.int8),
            candidates["book_offset"].to_numpy(np.int64),
            candidates["book_n_lvls"].to_numpy(np.int32),
            np.asarray(book_idx.prices),
            np.asarray(book_idx.cum_cost),
            np.asarray(book_idx.cum_shares),
            float(capital),
            float(max_dd_frac),
            float(exposure_cap_frac),
            float(min_bet_usd),
            float(book_consumption_max),
            -1.0 if max_bet_usd is None else float(max_bet_usd),
            float(stake_frac),
            bool(sizing_mode == "compound"),
        )
        max_dd = _max_dd_frac(pls, capital)
        worst_30 = _worst_rolling_dd_frac(pls, capital, 30)
        pnl = float(pls.sum()) if len(pls) else 0.0
        win_rate = float((pls > 0).mean()) if len(pls) else None
        mean_pred_edge = float(sum_pred_edge / kept_count) if kept_count else None
        mean_fill = float(sum_fill / kept_count) if kept_count else None
        mean_fill_slippage = float(sum_slippage / kept_count) if kept_count else None
        mean_notional = float(sum_notional / kept_count) if kept_count else None
        max_notional_out = float(max_notional) if kept_count else None
        passed = (
            len(pls) >= min_fold_bets
            and pnl > 0.0
            and max_dd <= max_dd_frac
            and worst_30 <= max_dd_frac
            and not bool(dd_halted)
        )
        return FoldResult(
            fold=name,
            train_start=train_start.isoformat(),
            train_end=train_end.isoformat(),
            test_start=test_start.isoformat(),
            test_end=test_end.isoformat(),
            signal=signal,
            execution_price=execution_price,
            sizing_mode=sizing_mode,
            feature_set=feature_set,
            side_filter=side_filter,
            interval_filter=interval_filter,
            ask_bucket=ask_bucket,
            book_consumption_max=book_consumption_max,
            max_bet_usd=max_bet_usd,
            threshold=threshold,
            stake_frac=stake_frac,
            n_candidates=n_candidates,
            n_bets=int(len(pls)),
            n_rejected_execution=int(n_rejected_execution),
            total_pnl=pnl,
            pnl_frac=pnl / capital,
            win_rate=win_rate,
            max_dd_frac=max_dd,
            worst_30bet_dd_frac=worst_30,
            mean_pred_edge=mean_pred_edge,
            mean_fill=mean_fill,
            mean_fill_slippage=mean_fill_slippage,
            mean_notional=mean_notional,
            max_notional=max_notional_out,
            final_equity=float(capital + pnl),
            dd_halted=bool(dd_halted),
            passed=passed,
        )

    equity = float(capital)
    peak = float(capital)
    dd_halted = False
    active: list[tuple[int, int, float, float]] = []
    realized: list[tuple[int, int, float]] = []
    kept_records: list[dict] = []
    if execution_price == "vwap" and book_idx is None:
        book_idx = _load_book_idx()

    def realize_until(cutoff_ts: float) -> None:
        nonlocal active, equity, peak, dd_halted
        due = [item for item in active if item[0] <= cutoff_ts]
        active = [item for item in active if item[0] > cutoff_ts]
        for resolve_ts, entry_ts, _notional, pl in sorted(due, key=lambda x: (x[0], x[1])):
            equity += pl
            realized.append((resolve_ts, entry_ts, pl))
            peak = max(peak, equity)
            dd_ratio = (peak - equity) / peak if peak > 0 else 1.0
            if dd_ratio >= max_dd_frac or equity <= 0.0:
                dd_halted = True

    for row in candidates.itertuples(index=False):
        entry_ts = int(row.entry_ts)
        realize_until(entry_ts)
        if dd_halted:
            break

        notional = equity * stake_frac if sizing_mode == "compound" else starting_notional
        if max_bet_usd is not None:
            notional = min(notional, max_bet_usd)
        if notional < min_bet_usd:
            continue
        exposure_cap = (equity if sizing_mode == "compound" else capital) * exposure_cap_frac
        pending = sum(nt for _, _, nt, _ in active)
        if pending + notional > exposure_cap:
            continue

        execution_fill = _execution_fill_for_row(
            row,
            notional=notional,
            execution_price=execution_price,
            book_consumption_max=book_consumption_max,
            book_idx=book_idx,
        )
        if execution_fill is None:
            n_rejected_execution += 1
            continue
        fill = execution_fill.fill
        executed_notional = execution_fill.notional

        pl = float(_cash_pnl_vector(np.array([int(row.y) == 1]), np.array([fill]), executed_notional)[0])
        if not math.isfinite(pl):
            continue
        active.append((int(row.resolve_ts), entry_ts, float(executed_notional), pl))
        kept_records.append({
            "entry_ts": entry_ts,
            "resolve_ts": int(row.resolve_ts),
            "ask": float(row.ask),
            "fill": float(fill),
            "pl": pl,
            "notional": float(executed_notional),
            "pred_edge": float(row.pred_edge),
        })

    realize_until(float("inf"))

    if kept_records:
        kept = pd.DataFrame(kept_records).sort_values(["resolve_ts", "entry_ts"]).reset_index(drop=True)
        realized_sorted = sorted(realized, key=lambda x: (x[0], x[1]))
        pls = np.array([pl for _, _, pl in realized_sorted], dtype=np.float64)
    else:
        kept = pd.DataFrame(columns=["ask", "fill", "pl", "notional", "pred_edge"])
        pls = np.array([], dtype=np.float64)

    max_dd = _max_dd_frac(pls, capital)
    worst_30 = _worst_rolling_dd_frac(pls, capital, 30)
    pnl = float(pls.sum()) if len(pls) else 0.0
    win_rate = float((pls > 0).mean()) if len(pls) else None
    mean_pred_edge = float(kept["pred_edge"].mean()) if len(kept) else None
    mean_fill = float(kept["fill"].mean()) if len(kept) else None
    mean_fill_slippage = float((kept["fill"] - kept["ask"]).mean()) if len(kept) else None
    mean_notional = float(kept["notional"].mean()) if len(kept) else None
    max_notional = float(kept["notional"].max()) if len(kept) else None
    passed = (
        len(pls) >= min_fold_bets
        and pnl > 0.0
        and max_dd <= max_dd_frac
        and worst_30 <= max_dd_frac
        and not dd_halted
    )
    return FoldResult(
        fold=name,
        train_start=train_start.isoformat(),
        train_end=train_end.isoformat(),
        test_start=test_start.isoformat(),
        test_end=test_end.isoformat(),
        signal=signal,
        execution_price=execution_price,
        sizing_mode=sizing_mode,
        feature_set=feature_set,
        side_filter=side_filter,
        interval_filter=interval_filter,
        ask_bucket=ask_bucket,
        book_consumption_max=book_consumption_max,
        max_bet_usd=max_bet_usd,
        threshold=threshold,
        stake_frac=stake_frac,
        n_candidates=n_candidates,
        n_bets=int(len(pls)),
        n_rejected_execution=n_rejected_execution,
        total_pnl=pnl,
        pnl_frac=pnl / capital,
        win_rate=win_rate,
        max_dd_frac=max_dd,
        worst_30bet_dd_frac=worst_30,
        mean_pred_edge=mean_pred_edge,
        mean_fill=mean_fill,
        mean_fill_slippage=mean_fill_slippage,
        mean_notional=mean_notional,
        max_notional=max_notional,
        final_equity=float(capital + pnl),
        dd_halted=dd_halted,
        passed=passed,
    )


def _summarize_grid(results: list[FoldResult]) -> dict:
    rows = [asdict(r) for r in results]
    if not rows:
        return {"rows": [], "best": None}
    df = pd.DataFrame(rows)
    group_cols = [
        "signal",
        "execution_price",
        "sizing_mode",
        "feature_set",
        "side_filter",
        "interval_filter",
        "ask_bucket",
        "book_consumption_max",
        "max_bet_usd",
        "threshold",
        "stake_frac",
    ]
    summary = (
        df.groupby(group_cols, dropna=False)
        .agg(
            all_folds_pass=("passed", "all"),
            folds=("fold", "count"),
            min_fold_pnl=("total_pnl", "min"),
            sum_pnl=("total_pnl", "sum"),
            max_dd_frac=("max_dd_frac", "max"),
            worst_30bet_dd_frac=("worst_30bet_dd_frac", "max"),
            min_bets=("n_bets", "min"),
            total_bets=("n_bets", "sum"),
            rejected_execution=("n_rejected_execution", "sum"),
            mean_win_rate=("win_rate", "mean"),
            mean_pred_edge=("mean_pred_edge", "mean"),
            mean_fill=("mean_fill", "mean"),
            mean_fill_slippage=("mean_fill_slippage", "mean"),
            mean_notional=("mean_notional", "mean"),
            max_notional=("max_notional", "max"),
            min_final_equity=("final_equity", "min"),
            any_dd_halted=("dd_halted", "any"),
        )
        .reset_index()
    )
    fold_pnl = df.pivot_table(
        index=group_cols,
        columns="fold",
        values="total_pnl",
        aggfunc="sum",
    ).reset_index()
    fold_pnl.columns = [
        f"fold_pnl_{col}" if col not in group_cols else col
        for col in fold_pnl.columns
    ]
    summary = summary.merge(fold_pnl, on=group_cols, how="left")
    summary["max_fold_pnl"] = df.groupby(group_cols, dropna=False)["total_pnl"].max().to_numpy()
    avg_fold_pnl = summary["sum_pnl"] / summary["folds"].replace(0, np.nan)
    summary["min_to_avg_pnl"] = summary["min_fold_pnl"] / avg_fold_pnl.replace(0.0, np.nan)
    summary["max_fold_share"] = np.where(
        summary["sum_pnl"] > 0.0,
        summary["max_fold_pnl"] / summary["sum_pnl"],
        np.nan,
    )
    pnl_std = df.groupby(group_cols, dropna=False)["total_pnl"].std(ddof=0).to_numpy()
    summary["fold_pnl_cv"] = pnl_std / avg_fold_pnl.abs().replace(0.0, np.nan)
    balance_penalty = (1.0 - summary["max_fold_share"].clip(lower=0.0, upper=1.0)).fillna(0.0)
    summary["balanced_score"] = summary["min_fold_pnl"] * np.log1p(summary["sum_pnl"].clip(lower=0.0)) * balance_penalty

    selection_mask = df["fold"].isin(SELECTION_FOLDS)
    validation_mask = df["fold"].eq(VALIDATION_FOLD)
    if selection_mask.any():
        selection = (
            df[selection_mask]
            .groupby(group_cols, dropna=False)
            .agg(
                selection_folds=("fold", "nunique"),
                selection_all_folds_pass=("passed", "all"),
                selection_min_fold_pnl=("total_pnl", "min"),
                selection_sum_pnl=("total_pnl", "sum"),
                selection_max_fold_pnl=("total_pnl", "max"),
                selection_max_dd_frac=("max_dd_frac", "max"),
                selection_worst_30bet_dd_frac=("worst_30bet_dd_frac", "max"),
                selection_min_bets=("n_bets", "min"),
                selection_total_bets=("n_bets", "sum"),
                selection_any_dd_halted=("dd_halted", "any"),
            )
            .reset_index()
        )
        selection_avg = selection["selection_sum_pnl"] / selection["selection_folds"].replace(0, np.nan)
        selection["selection_min_to_avg_pnl"] = (
            selection["selection_min_fold_pnl"] / selection_avg.replace(0.0, np.nan)
        )
        selection["selection_max_fold_share"] = np.where(
            selection["selection_sum_pnl"] > 0.0,
            selection["selection_max_fold_pnl"] / selection["selection_sum_pnl"],
            np.nan,
        )
        selection_penalty = (
            1.0 - selection["selection_max_fold_share"].clip(lower=0.0, upper=1.0)
        ).fillna(0.0)
        selection["selection_balanced_score"] = (
            selection["selection_min_fold_pnl"]
            * np.log1p(selection["selection_sum_pnl"].clip(lower=0.0))
            * selection_penalty
        )
        summary = summary.merge(selection, on=group_cols, how="left")
    else:
        summary["selection_folds"] = summary["folds"]
        summary["selection_all_folds_pass"] = summary["all_folds_pass"]
        summary["selection_min_fold_pnl"] = summary["min_fold_pnl"]
        summary["selection_sum_pnl"] = summary["sum_pnl"]
        summary["selection_max_dd_frac"] = summary["max_dd_frac"]
        summary["selection_worst_30bet_dd_frac"] = summary["worst_30bet_dd_frac"]
        summary["selection_min_bets"] = summary["min_bets"]
        summary["selection_total_bets"] = summary["total_bets"]
        summary["selection_any_dd_halted"] = summary["any_dd_halted"]
        summary["selection_min_to_avg_pnl"] = summary["min_to_avg_pnl"]
        summary["selection_max_fold_share"] = summary["max_fold_share"]
        summary["selection_balanced_score"] = summary["balanced_score"]

    if validation_mask.any():
        validation = (
            df[validation_mask]
            .groupby(group_cols, dropna=False)
            .agg(
                validation_folds=("fold", "nunique"),
                validation_pass=("passed", "all"),
                validation_total_pnl=("total_pnl", "sum"),
                validation_min_fold_pnl=("total_pnl", "min"),
                validation_max_dd_frac=("max_dd_frac", "max"),
                validation_worst_30bet_dd_frac=("worst_30bet_dd_frac", "max"),
                validation_min_bets=("n_bets", "min"),
                validation_total_bets=("n_bets", "sum"),
                validation_any_dd_halted=("dd_halted", "any"),
            )
            .reset_index()
        )
        summary = summary.merge(validation, on=group_cols, how="left")
    else:
        summary["validation_folds"] = 0
        summary["validation_pass"] = False
        summary["validation_total_pnl"] = np.nan
        summary["validation_min_fold_pnl"] = np.nan
        summary["validation_max_dd_frac"] = np.nan
        summary["validation_worst_30bet_dd_frac"] = np.nan
        summary["validation_min_bets"] = np.nan
        summary["validation_total_bets"] = 0
        summary["validation_any_dd_halted"] = False

    summary["selection_all_folds_pass"] = summary["selection_all_folds_pass"].fillna(False).astype(bool)
    summary["validation_pass"] = summary["validation_pass"].fillna(False).astype(bool)
    summary["finalist_pass"] = summary["selection_all_folds_pass"] & summary["validation_pass"]

    passing = summary[summary["all_folds_pass"]].copy()
    if passing.empty:
        order = summary.sort_values(
            ["balanced_score", "min_fold_pnl", "sum_pnl", "max_dd_frac"],
            ascending=[False, False, False, True],
        )
        best = order.iloc[0].to_dict() if len(order) else None
    else:
        best = passing.sort_values(
            ["balanced_score", "min_fold_pnl", "sum_pnl", "max_dd_frac"],
            ascending=[False, False, False, True],
        ).iloc[0].to_dict()
    selection_passing = summary[summary["selection_all_folds_pass"]].copy()
    selection_order = selection_passing if not selection_passing.empty else summary
    best_selection = selection_order.sort_values(
        ["selection_balanced_score", "selection_min_fold_pnl", "selection_sum_pnl", "selection_max_dd_frac"],
        ascending=[False, False, False, True],
    ).iloc[0].to_dict() if len(selection_order) else None
    finalist_passing = summary[summary["finalist_pass"]].copy()
    best_finalist = finalist_passing.sort_values(
        ["selection_balanced_score", "selection_min_fold_pnl", "selection_sum_pnl", "validation_max_dd_frac"],
        ascending=[False, False, False, True],
    ).iloc[0].to_dict() if len(finalist_passing) else None
    return {
        "rows": rows,
        "summary": summary.to_dict(orient="records"),
        "best": best,
        "best_selection": best_selection,
        "best_finalist": best_finalist,
    }


def _folds(scheme: str) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    if scheme == "abcd":
        return [
            ("B", CHUNK_A_START, CHUNK_B_START, CHUNK_B_START, CHUNK_C_START),
            ("C", CHUNK_A_START, CHUNK_C_START, CHUNK_C_START, CHUNK_D_START),
            ("D", CHUNK_A_START, CHUNK_D_START, CHUNK_D_START, DATA_END),
        ]
    if scheme == "tail":
        return [
            ("D_tail", CHUNK_A_START, CHUNK_D_START, HOLDOUT_START, DATA_END),
        ]
    raise ValueError(f"unknown fold scheme {scheme!r}")


def _with_embargo(
    folds: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
    embargo_seconds: int,
) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    if embargo_seconds <= 0:
        return folds
    delta = pd.Timedelta(seconds=int(embargo_seconds))
    out = []
    for name, train_start, train_end, test_start, test_end in folds:
        out.append((name, train_start, min(train_end, test_start - delta), test_start, test_end))
    return out


def _ts_to_utc(ts: int) -> pd.Timestamp:
    return pd.Timestamp(int(ts), unit="s", tz="UTC")


def _folds_for_frame(
    scheme: str,
    side_df: pd.DataFrame,
    *,
    embargo_seconds: int = FOLD_EMBARGO_SECONDS,
) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    """Return fold windows for a concrete fresh-executable side frame.

    Legacy schemes keep their fixed calendar boundaries, with an optional train
    embargo applied by the caller. The all-history schemes are deliberately
    frame aware: they must be computed after PMData fresh-L2 filtering so chunk
    density reflects the rows the optimizer can actually trade.

    The all-history WFO is expanding-window:
    - train A, test B
    - train A+B, test C
    - train A+B+C, test D
    """
    if scheme not in {"abcd_all_history", "abcd_calendar_all_history"}:
        return _with_embargo(_folds(scheme), embargo_seconds)

    if side_df.empty or "entry_ts" not in side_df.columns:
        raise ValueError(f"{scheme} requires a non-empty frame with entry_ts")
    ts = np.sort(side_df["entry_ts"].dropna().astype(np.int64).unique())
    if len(ts) < 4:
        raise ValueError(f"{scheme} requires at least four unique entry_ts values")

    if scheme == "abcd_all_history":
        chunks = np.array_split(ts, 4)
        if any(len(chunk) == 0 for chunk in chunks):
            raise ValueError("abcd_all_history could not build four non-empty chunks")
        a_start = _ts_to_utc(int(chunks[0][0]))
        b_start = _ts_to_utc(int(chunks[1][0]))
        c_start = _ts_to_utc(int(chunks[2][0]))
        d_start = _ts_to_utc(int(chunks[3][0]))
        data_end = _ts_to_utc(int(chunks[3][-1]) + 1)
    else:
        a_start = _ts_to_utc(int(ts[0]))
        data_end = _ts_to_utc(int(ts[-1]) + 1)
        span = data_end - a_start
        if span <= pd.Timedelta(0):
            raise ValueError("abcd_calendar_all_history requires positive time span")
        chunk = span / 4
        b_start = a_start + chunk
        c_start = a_start + chunk * 2
        d_start = a_start + chunk * 3

    folds = [
        ("B", a_start, b_start, b_start, c_start),
        ("C", a_start, c_start, c_start, d_start),
        ("D", a_start, d_start, d_start, data_end),
    ]
    return _with_embargo(folds, embargo_seconds)


def _filter_folds_by_name(
    folds: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
    raw_names: str | None,
) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    if not raw_names:
        return folds
    wanted = {name.strip().upper() for name in raw_names.split(",") if name.strip()}
    if not wanted:
        return folds
    selected = [fold for fold in folds if fold[0].upper() in wanted]
    missing = sorted(wanted - {fold[0].upper() for fold in selected})
    if missing:
        raise ValueError(f"requested fold(s) not available: {', '.join(missing)}")
    return selected


def _group_count_dict(df: pd.DataFrame, cols: list[str]) -> dict[str, int]:
    if df.empty or not set(cols).issubset(df.columns):
        return {}
    counts = df.groupby(cols, observed=True).size().sort_index()
    out: dict[str, int] = {}
    for key, value in counts.items():
        if not isinstance(key, tuple):
            key = (key,)
        out["/".join(str(part) for part in key)] = int(value)
    return out


def _ask_bucket_count_dict(df: pd.DataFrame) -> dict[str, int]:
    if df.empty or "ask" not in df.columns:
        return {}
    asks = df["ask"].to_numpy(np.float64)
    out: dict[str, int] = {}
    for name, ranges in ASK_BUCKET_DIAGNOSTIC_RANGES.items():
        mask = np.zeros(len(df), dtype=bool)
        for lo, hi in ranges:
            mask |= (asks >= lo) & (asks < hi)
        out[name] = int(mask.sum())
    return out


def _fold_window_diagnostics(
    side_df: pd.DataFrame,
    folds: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
    *,
    embargo_seconds: int,
) -> list[dict]:
    diagnostics: list[dict] = []
    for name, train_start, train_end, test_start, test_end in folds:
        train = side_df[(side_df["entry_dt"] >= train_start) & (side_df["entry_dt"] < train_end)]
        test = side_df[(side_df["entry_dt"] >= test_start) & (side_df["entry_dt"] < test_end)]
        diagnostics.append({
            "fold": name,
            "train_start": train_start.isoformat(),
            "train_end_after_embargo": train_end.isoformat(),
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "embargo_seconds": int(embargo_seconds),
            "test_days": float((test_end - test_start) / pd.Timedelta(days=1)),
            "train_days_after_embargo": float((train_end - train_start) / pd.Timedelta(days=1)),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "train_unique_entry_ts": int(train["entry_ts"].nunique()) if len(train) else 0,
            "test_unique_entry_ts": int(test["entry_ts"].nunique()) if len(test) else 0,
            "test_by_coin_interval_side": _group_count_dict(test, ["coin", "interval", "side"]),
            "test_ask_buckets": _ask_bucket_count_dict(test),
        })
    return diagnostics


def _fresh_l2_coverage_diagnostics(base: pd.DataFrame, side_df: pd.DataFrame) -> dict:
    out = {
        "base_rows": int(len(base)),
        "side_rows": int(len(side_df)),
        "pmdata_max_staleness_seconds": PMDATA_MAX_STALENESS_SECONDS,
        "base_by_coin_interval": _group_count_dict(base, ["coin", "interval"]),
        "side_by_coin_interval_side": _group_count_dict(side_df, ["coin", "interval", "side"]),
    }
    if {"up_ask_l2", "no_ask_l2"}.issubset(base.columns):
        out.update({
            "up_executable_rows": int(base["up_ask_l2"].notna().sum()),
            "down_executable_rows": int(base["no_ask_l2"].notna().sum()),
            "any_side_executable_rows": int((base["up_ask_l2"].notna() | base["no_ask_l2"].notna()).sum()),
            "both_side_executable_rows": int((base["up_ask_l2"].notna() & base["no_ask_l2"].notna()).sum()),
        })
    return out


def _book_index_manifest() -> dict | None:
    d = os.environ.get("CRYPTOBOT_BOOK_INDEX_DIR")
    if not d:
        return None
    path = Path(d) / "manifest.json"
    if not path.exists():
        return {"book_index_dir": d, "manifest_missing": True}
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"book_index_dir": d, "manifest_error": repr(exc)}
    return {"book_index_dir": d, **manifest}


def _maybe_sample(train: pd.DataFrame, max_train_rows: int | None, seed: int) -> pd.DataFrame:
    if max_train_rows is None or len(train) <= max_train_rows:
        return train
    return train.sample(n=max_train_rows, random_state=seed)


def run(args: argparse.Namespace) -> dict:
    coins = {c.strip().lower() for c in args.coins.split(",") if c.strip()}
    intervals = {i.strip() for i in args.intervals.split(",") if i.strip()}
    thresholds = _parse_floats(args.edge_thresholds)
    stake_fracs = _parse_floats(args.stake_fracs)
    signals = [s.strip() for s in args.signals.split(",") if s.strip()]
    exposure_cap_frac = args.exposure_cap_frac if args.exposure_cap_frac is not None else args.max_dd_frac
    feature_set, feature_columns = _feature_columns(args.feature_groups)
    side_filter = args.side_filter.strip()
    interval_filter = ",".join(sorted(intervals))
    ask_bucket = args.ask_bucket_label.strip()
    if args.fold_scheme == "abcd_all_history" and args.sample_weight_mode == "recent_chunk_boost":
        raise ValueError(
            "recent_chunk_boost uses fixed legacy chunk dates; use fold_recent_boost "
            "for abcd_all_history candidate runs"
        )

    base = _prepare_base_frame(coins, intervals, args.label_col)
    side_df = _side_rows(base, args.label_col)
    if side_filter.lower() != "both":
        side_df = side_df[side_df["side"].str.lower() == side_filter.lower()].copy()
    if args.ask_min is not None:
        side_df = side_df[side_df["ask"] >= args.ask_min].copy()
    if args.ask_max is not None:
        side_df = side_df[side_df["ask"] < args.ask_max].copy()
    if side_df.empty:
        raise RuntimeError("no fresh executable side rows after filtering")

    folds = _folds_for_frame(
        args.fold_scheme,
        side_df,
        embargo_seconds=args.fold_embargo_seconds,
    )
    folds = _filter_folds_by_name(folds, args.fold_names)
    results: list[FoldResult] = []
    fold_window_diagnostics = _fold_window_diagnostics(
        side_df,
        folds,
        embargo_seconds=args.fold_embargo_seconds,
    )
    book_idx = _load_book_idx() if args.execution_price == "vwap" else None
    fold_diagnostics: list[dict] = []
    for fold_idx, fold in enumerate(folds):
        name, train_start, train_end, test_start, test_end = fold
        train = side_df[(side_df["entry_dt"] >= train_start) & (side_df["entry_dt"] < train_end)].copy()
        test = side_df[(side_df["entry_dt"] >= test_start) & (side_df["entry_dt"] < test_end)].copy()
        train = _maybe_sample(train, args.max_train_rows, args.seed + fold_idx)
        print(
            f"[fold {name}] train={len(train):,} test={len(test):,} "
            f"window={test_start.date()}..{test_end.date()}",
            file=sys.stderr,
            flush=True,
        )
        if train.empty or test.empty:
            continue
        fold_diag = {
            "fold": name,
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "train_unique_entry_ts": int(train["entry_ts"].nunique()),
            "test_unique_entry_ts": int(test["entry_ts"].nunique()),
            "fresh_side_rows": int(len(test)),
            "test_side_win_rate": float(test["y"].mean()),
            "test_rf_edge_mean": float(test["rf_edge"].mean()),
        }

        prob_cols: dict[str, np.ndarray] = {}
        if "rf" in signals:
            prob_cols["rf"] = test["model_p_side"].to_numpy(np.float64)
        if "model" in signals or "blend" in signals:
            t0 = time.time()
            model_prob = _fit_predict(
                train,
                test,
                args.model,
                args.seed + fold_idx,
                args.sample_weight_mode,
                feature_columns,
                args.hgb_max_iter,
                args.hgb_learning_rate,
                args.hgb_max_leaf_nodes,
                args.hgb_l2_regularization,
            )
            print(f"[fold {name}] fitted {args.model} in {time.time() - t0:.1f}s", file=sys.stderr, flush=True)
            if "model" in signals:
                prob_cols["model"] = model_prob
            if "blend" in signals:
                prob_cols["blend"] = 0.5 * model_prob + 0.5 * test["model_p_side"].to_numpy(np.float64)

        for signal, probs in prob_cols.items():
            eval_df = test.copy()
            eval_df["model_prob"] = probs
            eval_df["pred_edge"] = eval_df["model_prob"] - eval_df["ask"] - _fee(eval_df["ask"])
            if book_idx is not None:
                eval_df = _attach_book_positions(eval_df, book_idx)
            fold_diag[f"{signal}_pred_edge_mean"] = float(eval_df["pred_edge"].mean())
            fold_diag[f"{signal}_pred_edge_p95"] = float(eval_df["pred_edge"].quantile(0.95))
            for threshold in thresholds:
                for stake_frac in stake_fracs:
                    results.append(
                        _evaluate_one(
                            eval_df,
                            signal=signal,
                            execution_price=args.execution_price,
                            sizing_mode=args.sizing_mode,
                            feature_set=feature_set,
                            side_filter=side_filter,
                            interval_filter=interval_filter,
                            ask_bucket=ask_bucket,
                            threshold=threshold,
                            stake_frac=stake_frac,
                            capital=args.capital,
                            max_dd_frac=args.max_dd_frac,
                            min_fold_bets=args.min_fold_bets,
                            exposure_cap_frac=exposure_cap_frac,
                            min_bet_usd=args.min_bet_usd,
                            book_consumption_max=args.book_consumption_max,
                            max_bet_usd=args.max_bet_usd,
                            fold_meta=fold,
                            book_idx=book_idx,
                        )
                    )
        fold_diagnostics.append(fold_diag)

    summary = _summarize_grid(results)
    live_min_notional_feasible = args.capital * args.max_dd_frac >= args.min_bet_usd
    output = {
        "schema_version": 1,
        "run_id": args.run_id,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "objective": "fresh-L2 residual-edge ABCD walk-forward measurement",
        "config": {
            "model": args.model,
            "label_col": args.label_col,
            "coins": sorted(coins),
            "intervals": sorted(intervals),
            "max_dd_frac": args.max_dd_frac,
            "capital": args.capital,
            "stake_fracs": stake_fracs,
            "edge_thresholds": thresholds,
            "exposure_cap_frac": exposure_cap_frac,
            "min_fold_bets": args.min_fold_bets,
            "max_train_rows": args.max_train_rows,
            "sample_weight_mode": args.sample_weight_mode,
            "hgb_max_iter": args.hgb_max_iter,
            "hgb_learning_rate": args.hgb_learning_rate,
            "hgb_max_leaf_nodes": args.hgb_max_leaf_nodes,
            "hgb_l2_regularization": args.hgb_l2_regularization,
            "feature_groups": args.feature_groups,
            "feature_set": feature_set,
            "feature_columns": feature_columns,
            "signals": signals,
            "min_bet_usd": args.min_bet_usd,
            "max_bet_usd": args.max_bet_usd,
            "execution_price": args.execution_price,
            "book_consumption_max": args.book_consumption_max,
            "fold_scheme": args.fold_scheme,
            "fold_embargo_seconds": args.fold_embargo_seconds,
            "fold_names": args.fold_names,
            "sizing_mode": args.sizing_mode,
            "side_filter": side_filter,
            "ask_min": args.ask_min,
            "ask_max": args.ask_max,
            "ask_bucket_label": ask_bucket,
            "live_min_notional_feasible_under_single_loss_cap": live_min_notional_feasible,
            "book_index_manifest": _book_index_manifest(),
        },
        "fresh_l2_coverage": _fresh_l2_coverage_diagnostics(base, side_df),
        "fold_window_diagnostics": fold_window_diagnostics,
        "fold_diagnostics": fold_diagnostics,
        "result": summary,
    }
    return output


def _json_safe(obj):
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if not math.isfinite(float(obj)):
            return None
        return float(obj)
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default=time.strftime("residual_%Y%m%d_%H%M%S"))
    p.add_argument("--model", choices=("logistic", "hgb"), default="logistic")
    p.add_argument("--hgb-max-iter", type=int, default=80)
    p.add_argument("--hgb-learning-rate", type=float, default=0.06)
    p.add_argument("--hgb-max-leaf-nodes", type=int, default=31)
    p.add_argument("--hgb-l2-regularization", type=float, default=0.02)
    p.add_argument("--label-col", default="won_up_polymarket")
    p.add_argument("--coins", default="btc,eth,sol,xrp")
    p.add_argument("--intervals", default="5m,15m")
    p.add_argument("--max-dd-frac", type=float, default=0.30)
    p.add_argument("--capital", type=float, default=100.0)
    p.add_argument("--stake-fracs", default="0.005,0.01,0.02,0.05,0.10")
    p.add_argument("--edge-thresholds", default="0.0,0.01,0.02,0.03,0.05,0.08,0.10")
    p.add_argument("--exposure-cap-frac", type=float, default=None)
    p.add_argument("--min-fold-bets", type=int, default=30)
    p.add_argument("--max-train-rows", type=int, default=None)
    p.add_argument("--signals", default="rf,model,blend")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--min-bet-usd", type=float, default=0.50)
    p.add_argument("--max-bet-usd", type=float, default=None)
    p.add_argument("--execution-price", choices=("top_ask", "vwap"), default="top_ask")
    p.add_argument("--sizing-mode", choices=("fixed", "compound"), default="fixed")
    p.add_argument("--book-consumption-max", type=float, default=0.30)
    p.add_argument("--fold-scheme", choices=("abcd", "tail", "abcd_all_history", "abcd_calendar_all_history"), default="abcd")
    p.add_argument("--fold-embargo-seconds", type=int, default=FOLD_EMBARGO_SECONDS)
    p.add_argument("--fold-names", default=None, help="optional comma-separated fold names, e.g. B,C or D")
    p.add_argument("--feature-groups", default="all")
    p.add_argument("--side-filter", choices=("both", "Up", "Down"), default="both")
    p.add_argument("--ask-min", type=float, default=None)
    p.add_argument("--ask-max", type=float, default=None)
    p.add_argument("--ask-bucket-label", default="all")
    p.add_argument(
        "--sample-weight-mode",
        choices=("none", "exp7", "exp14", "exp30", "recent_chunk_boost", "fold_recent_boost"),
        default="none",
    )
    p.add_argument("--out", default=None)
    args = p.parse_args(list(argv) if argv is not None else None)

    output = _json_safe(run(args))
    text = json.dumps(output, indent=2, sort_keys=True)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
