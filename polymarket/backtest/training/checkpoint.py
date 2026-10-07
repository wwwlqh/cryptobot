"""Leak-free walk-forward validation for HGB checkpoint grids.

Research only. Tests whether the HGB sleeves still work when decisions move
from whole-minute marks to sub-minute grids. It writes no live artifacts and
changes no runtime configuration.
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core" / "src"))
from polymarket.backtest import BT_RUNS_DIR, REPO_ROOT  # noqa: E402

from polymarket.backtest.training import modeling as ev  # noqa: E402
from polymarket.backtest.training import pmdata_target as etgt  # noqa: E402
from polymarket.backtest.training import offset_wfo as base  # noqa: E402
from polymarket.backtest.training import offset_validation as lk  # noqa: E402


OUT_DIR = BT_RUNS_DIR / "checkpoint_grid_hgb_orb_wfo"

COINS = lk.COINS
INTERVALS = lk.INTERVALS
LABEL_COL = lk.LABEL_COL
EMBARGO_SECONDS = lk.EMBARGO_SECONDS
CAPITAL_DEFAULT = lk.CAPITAL_DEFAULT
MIN_BET_USD_DEFAULT = lk.MIN_BET_USD_DEFAULT
MIN_WALLET_CHUNK_BETS_DEFAULT = lk.MIN_WALLET_CHUNK_BETS_DEFAULT

CHUNK_MODEL_KEY = lk.CHUNK_MODEL_KEY
_A0 = lk._A0
_B0 = lk._B0
_C0 = lk._C0
_D0 = lk._D0

GRID_SPECS: dict[str, dict[str, tuple[int, ...]]] = {
    "baseline60": {
        "5m": tuple(range(60, 300, 60)),
        "15m": tuple(range(60, 900, 60)),
    },
    "shift30": {
        "5m": tuple(range(30, 300, 60)),
        "15m": tuple(range(30, 900, 60)),
    },
    "shift15": {
        "5m": tuple(range(15, 300, 30)),
        "15m": tuple(range(15, 900, 30)),
    },
    "step5": {
        "5m": tuple(range(5, 300, 5)),
        "15m": tuple(range(5, 900, 5)),
    },
    # 1-second cadence over the last 30 seconds only (offsets 270-299 for 5m,
    # 870-899 for 15m). range_sec is pinned to 5 below so the opening-range
    # features are built the same way as step5's.
    "step1last": {
        "5m": tuple(range(270, 300, 1)),
        "15m": tuple(range(870, 900, 1)),
    },
}
GRID_ORDER = tuple(GRID_SPECS)

# Per-grid opening-range override. The default is the grid's smallest offset,
# which for a last-30-seconds grid would mean a 270s opening range, a different
# strategy. Pin step1last to step5's 5s range.
RANGE_SEC_OVERRIDE: dict[str, int] = {"step1last": 5}
# Names used only in the manifest's ``source_config`` / ``source_digest``
# strings to record where the reference numbers came from; the files are not
# read.
EXP044_LIVE_CONFIG = BT_RUNS_DIR / "exp044_live.json"
EXP044_STRATEGY_DIGEST = BT_RUNS_DIR / "opr_exp044_strategy_digest.md"


def _rel_or_abs(p: Path) -> str:
    # Provenance strings only. BT_RUNS_DIR honors CRYPTOBOT_ROOT *and* the
    # per-dir CRYPTOBOT_PM_BT_* overrides, so it can legitimately point outside
    # REPO_ROOT (e.g. platform jobs running from a worktree against the main
    # checkout's data). relative_to() would raise ValueError at IMPORT time and
    # take every importer of pool_study down with it — fall back to absolute.
    try:
        return p.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return p.as_posix()

EXP044_REFERENCE = {
    "grid": "baseline60",
    "works": True,
    "baseline_mode": "reference",
    "reference_name": "exp044 minute ORB+HGB live config",
    "selected_policy": "fixed exp044 reference; no checkpoint-grid retune",
    "source_config": _rel_or_abs(EXP044_LIVE_CONFIG),
    "source_digest": _rel_or_abs(EXP044_STRATEGY_DIGEST),
    "n_pool_rows": None,
    "n_grid_rows": 0,
    "full_total_pnl": 10783.10,
    "full_max_dd_frac": 0.192,
    "full_worst_30bet_dd_frac": None,
    "full_n_bets": 521,
    "fold_pnl": {"A": 95.71, "B": 3037.91, "C": 6200.97, "D": 1448.51},
    "fold_wr": {"A": 0.655, "B": 0.748, "C": 0.809, "D": 0.567},
    "fold_n": {"A": 84, "B": 234, "C": 136, "D": 67},
    "note": (
        "Reference numbers are the retained exp044 minute ORB+HGB candidate "
        "from opr_exp044_strategy_digest.md. This checkpoint-grid harness does "
        "not reselect or refit baseline60 unless --baseline-mode refit is used."
    ),
}

# The minute-boundary baseline sleeve recipe: each interval's HGB feature
# columns, hyperparameters, sample-weight mode, and the gate/sizing settings
# (``threshold`` / ``stake_frac`` / ``ask_bucket`` / ``book_consumption_max``)
# that ``etgt._sleeve_configs`` turns into wallet sleeves. The harness fits its
# own per-window models; only this recipe is reused. These are fixed research
# constants: changing a number re-specifies every grid compared against them.
EXP044_SLEEVES: dict[str, dict[str, Any]] = {
    "5m": {
        "name": "exp044_5m_pmdata_strict_thr075_s010",
        "feature_groups": "rf,price,identity,time",
        "feature_columns": [
            "model_p_side", "rf_edge", "ask", "other_ask", "book_spread",
            "coin_id", "interval_min", "side_up", "entry_minute", "hour_utc", "dow_utc",
        ],
        "hgb_hyperparams": {
            "max_iter": 120,
            "learning_rate": 0.04,
            "max_leaf_nodes": 63,
            "l2_regularization": 0.02,
        },
        "sample_weight_mode": "recent_chunk_boost",
        "side_filter": "both",
        "threshold": 0.075,
        "stake_frac": 0.01,
        "book_consumption_max": 0.3,
        "ask_bucket": "all:0:1",
    },
    "15m": {
        "name": "exp044_15m_pmdata_boost_thr023_s030",
        "feature_groups": "price,momentum",
        "feature_columns": [
            "ask", "other_ask", "book_spread", "partial_ret_z_side", "abs_partial_ret_z",
        ],
        "hgb_hyperparams": {
            "max_iter": 100,
            "learning_rate": 0.05,
            "max_leaf_nodes": 31,
            "l2_regularization": 0.02,
        },
        "sample_weight_mode": "recent_chunk_boost",
        "side_filter": "both",
        "threshold": 0.023,
        "stake_frac": 0.03,
        "book_consumption_max": 0.06,
        "ask_bucket": "mid:0.4:0.6",
    },
}
# Shaped like a model manifest so ``manifest["sleeves"][interval]`` works in
# _fit_hgb_pipelines / _side_rows_for_hgb.
EXP044_MANIFEST: dict[str, Any] = {
    "artifact_version": "hgb_v1",
    "exp044_run_id": "exp044_pmdata_hgb_strict15mboost_20260605",
    "label_col": LABEL_COL,
    "coins": list(COINS),
    "random_state": 42,
    "sleeves": EXP044_SLEEVES,
    "frozen_note": (
        "inlined from data/models/hgb_v1_manifest.json on 2026-07-31 (exp044 "
        "phase 3); the joblib and its loader are gone"
    ),
}

RF_HYPERPARAMS: dict[str, object] = {
    "n_estimators": 200,
    "max_depth": 4,
    "min_samples_leaf": 50,
    "random_state": 42,
    "n_jobs": -1,
}


@dataclass
class WindowModel:
    rf_estimators: dict[tuple[str, str, int], RandomForestClassifier]
    hgb_pipelines: dict[str, object]
    diagnostics: dict[str, Any]


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items() if k != "_bets"}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _parse_csv(raw: str) -> tuple[str, ...]:
    return tuple(x.strip() for x in str(raw).split(",") if x.strip())


def grid_seconds(name: str) -> dict[str, tuple[int, ...]]:
    if name not in GRID_SPECS:
        raise ValueError(f"unknown checkpoint grid {name!r}")
    return {interval: tuple(seconds) for interval, seconds in GRID_SPECS[name].items()}


def _interval_seconds(interval: str) -> int:
    if str(interval).lower() == "5m":
        return 300
    if str(interval).lower() == "15m":
        return 900
    raise ValueError(f"unsupported interval {interval!r}")


def _train_windows() -> dict[str, pd.Timestamp]:
    delta = pd.Timedelta(seconds=int(EMBARGO_SECONDS))
    return {"A": _B0 - delta, "AB": _C0 - delta, "ABC": _D0 - delta}


def _window_train_mask(rows: pd.DataFrame, train_end: pd.Timestamp) -> pd.Series:
    entry_dt = pd.to_datetime(rows["entry_dt"], utc=True)
    return (entry_dt >= _A0) & (entry_dt < train_end)


def _read_market_rows(*, since: str | None, until: str | None, limit_markets: int | None) -> pd.DataFrame:
    cols = ["market_slug", "coin", "interval", "slug_ts", "resolve_ts"]
    rows = pd.read_parquet(base.SNAP_FULL, columns=cols)
    rows["coin"] = rows["coin"].astype(str).str.lower()
    rows["interval"] = rows["interval"].astype(str).str.lower()
    rows = rows[rows["coin"].isin(set(COINS)) & rows["interval"].isin(set(INTERVALS))].copy()
    rows = rows.drop_duplicates("market_slug", keep="first").reset_index(drop=True)
    rows["slug_dt"] = pd.to_datetime(rows["slug_ts"], unit="s", utc=True)
    if since:
        rows = rows[rows["slug_dt"] >= pd.Timestamp(since, tz="UTC")].copy()
    if until:
        rows = rows[rows["slug_dt"] < pd.Timestamp(until, tz="UTC")].copy()
    if limit_markets is not None:
        rows = rows.head(int(limit_markets)).copy()
    return rows.reset_index(drop=True)


def _read_baseline60_rows(*, since: str | None, until: str | None, limit_markets: int | None) -> pd.DataFrame:
    cols = [
        "market_slug",
        "coin",
        "interval",
        "slug_ts",
        "resolve_ts",
        "entry_minute",
        "entry_ts",
        "ask",
        "other_ask",
        "book_spread",
        "ask_depth_top5_usd",
        "bid_depth_top5_usd",
    ]
    rows = pd.read_parquet(base.SNAP_FULL, columns=cols)
    rows["coin"] = rows["coin"].astype(str).str.lower()
    rows["interval"] = rows["interval"].astype(str).str.lower()
    rows = rows[rows["coin"].isin(set(COINS)) & rows["interval"].isin(set(INTERVALS))].copy()
    rows["checkpoint_sec"] = rows["entry_minute"].astype(int) * 60
    rows = rows[
        rows.apply(
            lambda r: int(r["checkpoint_sec"]) in GRID_SPECS["baseline60"][str(r["interval"])],
            axis=1,
        )
    ].copy()
    rows["offset_sec"] = rows["checkpoint_sec"].astype(np.int64)
    rows["range_sec"] = 60
    rows["entry_minute"] = rows["checkpoint_sec"].astype(float) / 60.0
    rows["entry_dt"] = pd.to_datetime(rows["entry_ts"], unit="s", utc=True)
    if since:
        rows = rows[rows["entry_dt"] >= pd.Timestamp(since, tz="UTC")].copy()
    if until:
        rows = rows[rows["entry_dt"] < pd.Timestamp(until, tz="UTC")].copy()
    if limit_markets is not None:
        markets = rows["market_slug"].drop_duplicates().head(int(limit_markets))
        rows = rows[rows["market_slug"].isin(set(markets))].copy()
    rows["up_depth"] = rows["ask_depth_top5_usd"].astype(float)
    rows["down_depth"] = rows["bid_depth_top5_usd"].astype(float)
    return rows.reset_index(drop=True)


def _expand_checkpoints(markets: pd.DataFrame, grid_name: str) -> pd.DataFrame:
    spec = grid_seconds(grid_name)
    frames: list[pd.DataFrame] = []
    for interval, seconds in spec.items():
        sub = markets[markets["interval"].astype(str).str.lower() == interval].copy()
        if sub.empty:
            continue
        repeated = sub.loc[sub.index.repeat(len(seconds))].copy()
        repeated["checkpoint_sec"] = np.tile(np.array(seconds, dtype=np.int64), len(sub))
        repeated["offset_sec"] = repeated["checkpoint_sec"].astype(np.int64)
        repeated["range_sec"] = int(RANGE_SEC_OVERRIDE.get(grid_name, min(seconds)))
        repeated["entry_ts"] = repeated["slug_ts"].astype(np.int64) + repeated["checkpoint_sec"].astype(np.int64)
        repeated["entry_minute"] = repeated["checkpoint_sec"].astype(float) / 60.0
        repeated["entry_dt"] = pd.to_datetime(repeated["entry_ts"], unit="s", utc=True)
        frames.append(repeated)
    if not frames:
        return markets.iloc[0:0].copy()
    out = pd.concat(frames, ignore_index=True)
    out = out[out["checkpoint_sec"].astype(int) < out["interval"].map(_interval_seconds)].copy()
    return out.reset_index(drop=True)


def _minute_rv_from_1s(spot_1s: pd.DataFrame) -> pd.Series:
    """Return causal 60-minute RV indexed at each minute boundary.

    A resampled value labelled ``t`` is the close of minute ``[t, t+60)``.
    At boundary ``t`` that close is still in the future, so shift once before
    forming the close-to-close return series.  The value at ``t`` therefore
    uses returns whose latest close is from the minute ending at ``t``.
    """
    spot = spot_1s.dropna(subset=["sec", "close"]).copy()
    spot["dt"] = pd.to_datetime(spot["sec"].astype(np.int64), unit="s", utc=True)
    minute_close = (
        pd.Series(spot["close"].astype(float).to_numpy(), index=spot["dt"])
        .sort_index()
        .resample("1min")
        .last()
        .ffill()
    )
    return np.log(minute_close).diff().rolling(60).std().shift(1)


def _attach_checkpoint_features(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    frames: list[pd.DataFrame] = []
    for coin, sub in rows.groupby("coin", observed=True):
        spot = base._load_1s(str(coin))
        sec = spot["sec"].to_numpy(dtype=np.int64)
        close = spot["close"].to_numpy(dtype=np.float64)
        rv_1h = _minute_rv_from_1s(spot)

        out = sub.copy()
        slug_ts = out["slug_ts"].astype(np.int64).to_numpy()
        entry_ts = out["entry_ts"].astype(np.int64).to_numpy()
        range_ts = slug_ts + out["range_sec"].astype(np.int64).to_numpy()
        open_spot = base._spot_at_seconds(sec, close, slug_ts)
        range_spot = base._spot_at_seconds(sec, close, range_ts)
        entry_spot = base._spot_at_seconds(sec, close, entry_ts)

        entry_minutes = out["entry_minute"].astype(float).to_numpy()
        range_minutes = out["range_sec"].astype(float).to_numpy() / 60.0
        grid = pd.to_datetime(out["slug_ts"], unit="s", utc=True)
        rv = rv_1h.reindex(grid).to_numpy(dtype=np.float64)

        range_ret = np.log(range_spot / open_spot)
        entry_ret = np.log(entry_spot / open_spot)
        entry_denom = rv * np.sqrt(entry_minutes)
        range_denom = rv * np.sqrt(range_minutes)

        out["range_ret"] = range_ret.astype(float)
        out["entry_ret"] = entry_ret.astype(float)
        out["partial_ret_z_1h"] = np.where(
            (entry_denom > 0) & np.isfinite(entry_denom),
            entry_ret / entry_denom,
            np.nan,
        ).astype(float)
        out["range_z"] = np.where(
            (range_denom > 0) & np.isfinite(range_denom),
            range_ret / range_denom,
            np.nan,
        ).astype(float)
        out["entry_z"] = out["partial_ret_z_1h"].astype(float)
        frames.append(out)
    return pd.concat(frames, ignore_index=True) if frames else rows.iloc[0:0].copy()


def _attach_checkpoint_books(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    records: list[dict[str, Any]] = []
    t0 = time.time()
    for i, ((coin, interval, slug), group) in enumerate(
        rows.groupby(["coin", "interval", "market_slug"], sort=False, observed=True),
        start=1,
    ):
        if i == 1 or i % 1000 == 0:
            print(
                f"[books] groups={i:,} rows_in={len(rows):,} rows_out={len(records):,} "
                f"elapsed={time.time() - t0:.1f}s",
                file=sys.stderr,
                flush=True,
            )
        series = base._load_top_book_series(str(coin), str(interval), str(slug))
        if series is None:
            continue
        for row in group.itertuples(index=False):
            book = base._extract_top_book_from_series(series, int(row.entry_ts))
            if book is None:
                continue
            data = row._asdict()
            data.update(book)
            records.append(data)
    return pd.DataFrame(records)


def _load_outcomes() -> pd.DataFrame:
    outcomes = pd.read_parquet(base.OUTCOMES, columns=["market_slug", "won_up"]).dropna(subset=["won_up"])
    outcomes["won_up"] = outcomes["won_up"].astype(int)
    return outcomes


def _prepare_grid_rows(markets: pd.DataFrame, grid_name: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    t0 = time.time()
    if grid_name == "baseline60":
        expanded = _read_baseline60_rows(since=None, until=None, limit_markets=None)
        if not markets.empty:
            expanded = expanded[expanded["market_slug"].isin(set(markets["market_slug"]))].copy()
        print(f"[prepare] grid={grid_name} expanded={len(expanded):,}", file=sys.stderr, flush=True)
        featured = _attach_checkpoint_features(expanded)
        featured = featured.dropna(subset=["range_ret", "entry_ret", "partial_ret_z_1h"]).copy()
        print(f"[prepare] grid={grid_name} featured={len(featured):,}", file=sys.stderr, flush=True)
        booked = featured.copy()
    else:
        expanded = _expand_checkpoints(markets, grid_name)
        print(f"[prepare] grid={grid_name} expanded={len(expanded):,}", file=sys.stderr, flush=True)
        featured = _attach_checkpoint_features(expanded)
        featured = featured.dropna(subset=["range_ret", "entry_ret", "partial_ret_z_1h"]).copy()
        print(f"[prepare] grid={grid_name} featured={len(featured):,}", file=sys.stderr, flush=True)
        booked = _attach_checkpoint_books(featured)
        print(f"[prepare] grid={grid_name} booked={len(booked):,}", file=sys.stderr, flush=True)
    if booked.empty:
        return booked, {
            "grid": grid_name,
            "market_rows": int(len(markets)),
            "expanded_rows": int(len(expanded)),
            "feature_rows": int(len(featured)),
            "book_rows": 0,
            "elapsed_s": round(time.time() - t0, 2),
        }
    rows = booked.merge(_load_outcomes(), on="market_slug", how="left").dropna(subset=["won_up"]).copy()
    rows["won_up"] = rows["won_up"].astype(int)
    rows["chunk"] = pd.to_datetime(rows["entry_ts"], unit="s", utc=True).map(base._chunk)
    rows = rows[rows["chunk"].isin(("A", "B", "C", "D"))].copy()
    rows["fold"] = rows["chunk"]
    diag = {
        "grid": grid_name,
        "market_rows": int(len(markets)),
        "expanded_rows": int(len(expanded)),
        "feature_rows": int(len(featured)),
        "book_rows": int(len(booked)),
        "labeled_rows": int(len(rows)),
        "chunk_rows": rows["chunk"].value_counts().sort_index().to_dict(),
        "elapsed_s": round(time.time() - t0, 2),
    }
    return rows.reset_index(drop=True), diag


def _rf_params(args: argparse.Namespace) -> dict[str, object]:
    params = dict(RF_HYPERPARAMS)
    params["n_estimators"] = int(args.rf_n_estimators)
    params["n_jobs"] = int(args.rf_n_jobs)
    params["random_state"] = int(args.seed)
    return params


def _fit_rf_slices(train_rows: pd.DataFrame, params: dict[str, object]) -> tuple[dict[tuple[str, str, int], RandomForestClassifier], dict[str, Any]]:
    estimators: dict[tuple[str, str, int], RandomForestClassifier] = {}
    skipped: list[dict[str, Any]] = []
    for key, group in train_rows.groupby(["coin", "interval", "checkpoint_sec"], observed=True):
        coin, interval, checkpoint_sec = str(key[0]), str(key[1]), int(key[2])
        sub = group[["partial_ret_z_1h", "won_up"]].dropna().copy()
        if sub.empty or sub["won_up"].nunique() < 2:
            skipped.append({
                "coin": coin,
                "interval": interval,
                "checkpoint_sec": checkpoint_sec,
                "rows": int(len(sub)),
                "reason": "empty-or-monoclass",
            })
            continue
        est = RandomForestClassifier(**params)
        x = pd.DataFrame({"partial_ret_z_1h": sub["partial_ret_z_1h"].astype(np.float32).to_numpy()})
        y = sub["won_up"].astype(np.int8).to_numpy()
        est.fit(x, y)
        estimators[(coin.lower(), interval.lower(), checkpoint_sec)] = est
    return estimators, {"n_slices": int(len(estimators)), "skipped_slices": skipped}


def _rf_probs_checkpoint(rows: pd.DataFrame, rf_estimators: dict[tuple[str, str, int], RandomForestClassifier]) -> np.ndarray:
    vals = np.full(len(rows), np.nan, dtype=np.float64)
    if rows.empty:
        return vals
    work = rows[["coin", "interval", "checkpoint_sec", "partial_ret_z_1h"]].reset_index()
    for key, group in work.groupby(["coin", "interval", "checkpoint_sec"], observed=True):
        coin, interval, checkpoint_sec = str(key[0]).lower(), str(key[1]).lower(), int(key[2])
        est = rf_estimators.get((coin, interval, checkpoint_sec))
        if est is None:
            continue
        frame = pd.DataFrame({"partial_ret_z_1h": group["partial_ret_z_1h"].astype(float).to_numpy()})
        proba = est.predict_proba(frame)
        if proba.shape[1] == 1:
            pred = np.full(len(group), 1.0 if int(est.classes_[0]) == 1 else 0.0)
        else:
            pred = proba[:, 1]
        vals[rows.index.get_indexer(group["index"].to_numpy())] = pred
    return vals


def _feature_frame_for_side(
    rows: pd.DataFrame,
    sleeve: etgt.ManifestSleeve,
    side: str,
    rf_estimators: dict[tuple[str, str, int], RandomForestClassifier],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    is_up = side == "Up"
    ask = rows["ask"].astype(float) if is_up else rows["other_ask"].astype(float)
    other = rows["other_ask"].astype(float) if is_up else rows["ask"].astype(float)
    other = other.where(np.isfinite(other), 1.0 - ask)
    valid = (
        np.isfinite(ask)
        & (ask > 0.0)
        & (ask < 1.0)
        & np.isfinite(rows["partial_ret_z_1h"].astype(float))
        & np.isfinite(rows["entry_minute"].astype(float))
    )
    if not valid.any():
        empty = rows.iloc[0:0].copy()
        return empty, pd.DataFrame(index=empty.index), ask.iloc[0:0], other.iloc[0:0]

    sub = rows.loc[valid].copy()
    ask = ask.loc[valid]
    other = other.loc[valid]
    z = sub["partial_ret_z_1h"].astype(float)
    sign = 1.0 if is_up else -1.0
    entry_dt = pd.to_datetime(sub["entry_ts"], unit="s", utc=True)
    rf_p_up = _rf_probs_checkpoint(sub, rf_estimators)
    model_p_side_rf = rf_p_up if is_up else (1.0 - rf_p_up)

    side_depth = sub["up_depth"].astype(float) if is_up else sub["down_depth"].astype(float)
    other_depth = sub["down_depth"].astype(float) if is_up else sub["up_depth"].astype(float)
    depth_sum = side_depth.to_numpy() + other_depth.to_numpy()

    feats = pd.DataFrame(index=sub.index)
    feats["model_p_side"] = model_p_side_rf
    feats["rf_edge"] = feats["model_p_side"].astype(float) - ask.to_numpy() - etgt._fee(ask)
    feats["ask"] = ask.to_numpy()
    feats["other_ask"] = other.to_numpy()
    feats["book_spread"] = ask.to_numpy() + other.to_numpy() - 1.0
    feats["log_depth"] = np.log1p(side_depth.to_numpy())
    feats["depth_imbalance"] = np.where(depth_sum > 0, (side_depth.to_numpy() - other_depth.to_numpy()) / depth_sum, np.nan)
    feats["partial_ret_z_side"] = sign * z.to_numpy()
    feats["abs_partial_ret_z"] = np.abs(z.to_numpy())
    feats["coin_id"] = sub["coin"].map(etgt.COIN_MAP).astype(float).to_numpy()
    feats["interval_min"] = float(etgt.INTERVAL_MIN[sleeve.interval])
    feats["side_up"] = 1.0 if is_up else 0.0
    feats["entry_minute"] = sub["entry_minute"].astype(float).to_numpy()
    feats["hour_utc"] = entry_dt.dt.hour.astype(float).to_numpy()
    feats["dow_utc"] = entry_dt.dt.dayofweek.astype(float).to_numpy()
    for col in sleeve.feature_columns:
        if col not in feats:
            feats[col] = np.nan
    return sub, feats[sleeve.feature_columns].astype(float), ask, other


def _side_rows_for_hgb(
    rows: pd.DataFrame,
    sleeves: dict[str, etgt.ManifestSleeve],
    rf_estimators: dict[tuple[str, str, int], RandomForestClassifier],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for interval, sleeve in sleeves.items():
        interval_rows = rows[rows["interval"].astype(str).str.lower() == interval].copy()
        if interval_rows.empty:
            continue
        sides = []
        if sleeve.side_filter in {"both", "up"}:
            sides.append("Up")
        if sleeve.side_filter in {"both", "down"}:
            sides.append("Down")
        for side in sides:
            sub, features, ask, other = _feature_frame_for_side(interval_rows, sleeve, side, rf_estimators)
            if sub.empty:
                continue
            finite = np.isfinite(features.to_numpy()).all(axis=1)
            if not finite.any():
                continue
            out = sub.loc[features.index[finite]].copy()
            out["side"] = side
            out["ask"] = ask.loc[out.index].to_numpy()
            out["other_ask"] = other.loc[out.index].to_numpy()
            out["sleeve"] = sleeve.label
            if side == "Up":
                out["y"] = out["won_up"].astype(np.int8)
            else:
                out["y"] = (1 - out["won_up"].astype(np.int8)).astype(np.int8)
            for col in sleeve.feature_columns:
                out[col] = features.loc[out.index, col].astype(float).to_numpy()
            frames.append(out)
    return pd.concat(frames, ignore_index=True) if frames else rows.iloc[0:0].copy()


def _fit_hgb_pipelines(
    train_rows: pd.DataFrame,
    manifest: dict[str, Any],
    sleeves: dict[str, etgt.ManifestSleeve],
    rf_estimators: dict[tuple[str, str, int], RandomForestClassifier],
    *,
    seed: int,
) -> tuple[dict[str, object], dict[str, Any]]:
    pipelines: dict[str, object] = {}
    diag: dict[str, Any] = {"side_rows": 0, "intervals": {}}
    for interval, sleeve in sleeves.items():
        side_rows = _side_rows_for_hgb(train_rows, {interval: sleeve}, rf_estimators)
        diag["side_rows"] += int(len(side_rows))
        meta = manifest["sleeves"][interval]
        hp = meta["hgb_hyperparams"]
        sample_weight_mode = meta.get("sample_weight_mode", "recent_chunk_boost")
        missing = [col for col in sleeve.feature_columns + ["y", "interval"] if col not in side_rows.columns]
        if missing:
            diag["intervals"][interval] = {
                "rows": 0,
                "fit": False,
                "sample_weight_mode": sample_weight_mode,
                "reason": f"missing-columns:{','.join(missing)}",
            }
            continue
        interval_mask = side_rows["interval"].astype(str).str.lower().eq(interval)
        complete_mask = side_rows.loc[interval_mask, sleeve.feature_columns + ["y"]].notna().all(axis=1)
        selected_idx = complete_mask.index[complete_mask.to_numpy()]
        tr = pd.DataFrame(index=selected_idx)
        for col in sleeve.feature_columns:
            tr[col] = side_rows.loc[selected_idx, col].to_numpy(dtype=np.float32, copy=True)
        tr["y"] = side_rows.loc[selected_idx, "y"].to_numpy(dtype=np.int8, copy=True)
        tr["entry_dt"] = side_rows.loc[selected_idx, "entry_dt"].to_numpy(copy=True)
        rec = {"rows": int(len(tr)), "fit": False, "sample_weight_mode": sample_weight_mode}
        if tr.empty or tr["y"].nunique() < 2:
            rec["reason"] = "empty-or-monoclass"
            diag["intervals"][interval] = rec
            del side_rows, tr
            gc.collect()
            continue
        pipe = ev._build_hgb_pipe(
            seed=seed,
            hgb_max_iter=int(hp["max_iter"]),
            hgb_learning_rate=float(hp["learning_rate"]),
            hgb_max_leaf_nodes=int(hp["max_leaf_nodes"]),
            hgb_l2_regularization=float(hp["l2_regularization"]),
        )
        ev._fit_pipe(pipe, tr, sleeve.feature_columns, sample_weight_mode)
        pipelines[interval] = pipe
        rec["fit"] = True
        diag["intervals"][interval] = rec
        del side_rows, tr
        gc.collect()
    return pipelines, diag


def _fit_window_model(
    rows: pd.DataFrame,
    manifest: dict[str, Any],
    sleeves: dict[str, etgt.ManifestSleeve],
    window_key: str,
    train_end: pd.Timestamp,
    args: argparse.Namespace,
) -> WindowModel:
    train = rows.loc[_window_train_mask(rows, train_end)].copy()
    rf_estimators, rf_diag = _fit_rf_slices(train, _rf_params(args))
    hgb_pipelines, hgb_diag = _fit_hgb_pipelines(
        train,
        manifest,
        sleeves,
        rf_estimators,
        seed=int(args.seed),
    )
    diag = {
        "window": window_key,
        "train_end": train_end.isoformat(),
        "train_rows": int(len(train)),
        "rf": rf_diag,
        "hgb": hgb_diag,
    }
    return WindowModel(rf_estimators=rf_estimators, hgb_pipelines=hgb_pipelines, diagnostics=diag)


def _fit_models(
    rows: pd.DataFrame,
    manifest: dict[str, Any],
    sleeves: dict[str, etgt.ManifestSleeve],
    args: argparse.Namespace,
) -> tuple[dict[str, WindowModel], dict[str, Any]]:
    models: dict[str, WindowModel] = {}
    diag: dict[str, Any] = {"windows": {}}
    for key, train_end in _train_windows().items():
        t0 = time.time()
        model = _fit_window_model(rows, manifest, sleeves, key, train_end, args)
        model.diagnostics["elapsed_s"] = round(time.time() - t0, 2)
        models[key] = model
        diag["windows"][key] = model.diagnostics
        print(
            f"[fit] window={key} train_rows={model.diagnostics['train_rows']:,} "
            f"rf={model.diagnostics['rf']['n_slices']} "
            f"hgb={list(model.hgb_pipelines)} elapsed={model.diagnostics['elapsed_s']:.1f}s",
            file=sys.stderr,
            flush=True,
        )
    return models, diag


def _score_side(
    rows: pd.DataFrame,
    sleeve: etgt.ManifestSleeve,
    side: str,
    model: WindowModel,
) -> pd.DataFrame:
    sub, features, ask, other = _feature_frame_for_side(rows, sleeve, side, model.rf_estimators)
    pipe = model.hgb_pipelines.get(sleeve.interval)
    if sub.empty or pipe is None:
        return rows.iloc[0:0].copy()
    finite = np.isfinite(features.to_numpy()).all(axis=1)
    if not finite.any():
        return rows.iloc[0:0].copy()
    sub = sub.loc[features.index[finite]].copy()
    ask = ask.loc[sub.index]
    other = other.loc[sub.index]
    model_prob = pipe.predict_proba(features.loc[sub.index])[:, 1]
    pred_edge = model_prob - ask.to_numpy() - etgt._fee(ask)
    out = sub.copy()
    out["side"] = side
    out["ask"] = ask.to_numpy()
    out["other_ask"] = other.to_numpy()
    out["model_prob"] = model_prob
    out["pred_edge"] = pred_edge
    out["sleeve"] = sleeve.label
    if side == "Up":
        out["y"] = out["won_up"].astype(np.int8)
    else:
        out["y"] = (1 - out["won_up"].astype(np.int8)).astype(np.int8)
    return out


def _score_rows_with_model(
    rows: pd.DataFrame,
    model: WindowModel,
    sleeves: dict[str, etgt.ManifestSleeve],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for interval, sleeve in sleeves.items():
        base_rows = rows[rows["interval"].astype(str).str.lower() == interval].copy()
        if base_rows.empty:
            continue
        side_frames = []
        if sleeve.side_filter in {"both", "up"}:
            side_frames.append(_score_side(base_rows, sleeve, "Up", model))
        if sleeve.side_filter in {"both", "down"}:
            side_frames.append(_score_side(base_rows, sleeve, "Down", model))
        side_df = pd.concat([x for x in side_frames if not x.empty], ignore_index=True) if side_frames else base_rows.iloc[0:0].copy()
        if side_df.empty:
            continue
        keep = etgt._ask_bucket_ok(side_df["ask"].astype(float), sleeve.ask_bucket_raw)
        keep &= side_df["pred_edge"].astype(float) >= sleeve.threshold
        filtered = side_df.loc[keep].copy()
        if filtered.empty:
            continue
        frames.append(ev._dedupe_best_side(filtered))
    if not frames:
        return rows.iloc[0:0].copy()
    cand = pd.concat(frames, ignore_index=True).sort_values(["entry_ts", "interval", "coin", "side"]).reset_index(drop=True)
    cand = etgt._attach_opr_columns(cand)
    cand = base._attach_raw_ladders(cand)
    return cand.reset_index(drop=True) if not cand.empty else cand


def _score_grid_pool(
    rows: pd.DataFrame,
    models: dict[str, WindowModel],
    sleeves: dict[str, etgt.ManifestSleeve],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    diag: dict[str, Any] = {"chunks": {}}
    for chunk in ("A", "B", "C", "D"):
        sub = rows[rows["chunk"].astype(str) == chunk].copy()
        if sub.empty:
            diag["chunks"][chunk] = {"input_rows": 0, "candidate_rows": 0}
            continue
        model_key = CHUNK_MODEL_KEY[chunk]
        cand = _score_rows_with_model(sub, models[model_key], sleeves)
        if not cand.empty:
            cand["chunk"] = chunk
            cand["fold"] = chunk
            frames.append(cand)
        diag["chunks"][chunk] = {"input_rows": int(len(sub)), "candidate_rows": int(len(cand))}
        print(f"[score] chunk={chunk} input={len(sub):,} candidates={len(cand):,}", file=sys.stderr, flush=True)
    pool = pd.concat(frames, ignore_index=True) if frames else rows.iloc[0:0].copy()
    return pool.sort_values(["entry_ts", "interval", "coin", "side"]).reset_index(drop=True), diag


def _build_pool_for_grid(
    grid_name: str,
    markets: pd.DataFrame,
    manifest: dict[str, Any],
    sleeves: dict[str, etgt.ManifestSleeve],
    args: argparse.Namespace,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    # Both paths below read the poly_snapshot tree (_prepare_grid_rows via the
    # top book series for shifted grids, and _score_grid_pool via the raw
    # ladders for every grid). Without the tree they return nothing and run()
    # would write an empty pool that reads as "no bets found", so check once
    # here. It sits in _build_pool_for_grid rather than run() so the default
    # ``--grids baseline60 --baseline-mode reference`` path, which never reads
    # a book, still works without the data.
    etgt.require_snapshot_dir()
    t0 = time.time()
    rows, prep_diag = _prepare_grid_rows(markets, grid_name)
    if rows.empty:
        return rows, {"grid": grid_name, "prepare": prep_diag, "model": {}, "score": {}, "elapsed_s": round(time.time() - t0, 2)}
    models, model_diag = _fit_models(rows, manifest, sleeves, args)
    pool, score_diag = _score_grid_pool(rows, models, sleeves)
    diag = {
        "grid": grid_name,
        "prepare": prep_diag,
        "model": model_diag,
        "score": score_diag,
        "pool_rows": int(len(pool)),
        "elapsed_s": round(time.time() - t0, 2),
    }
    return pool, diag


def _without_bets(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: v for k, v in row.items() if k not in {"_bets", "selected_bets"}}


def _run_grid_wfo(
    grid_name: str,
    pool: pd.DataFrame,
    wallet_sleeves: dict,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if pool.empty:
        return [], {"grid": grid_name, "works": False, "reason": "empty pool"}
    fixed_offsets = tuple(sorted(int(x) for x in pool["offset_sec"].dropna().unique()))
    rows, _accepted_best = lk._fixed_set_wfo(
        pool,
        fixed_offsets,
        wallet_sleeves,
        float(args.max_dd_frac),
        min_wallet_chunk_bets=int(args.min_wallet_chunk_bets),
        search_profile=str(args.search_profile),
        capital=float(args.capital),
        min_bet_usd=float(args.min_bet_usd),
    )
    best_train = next((r for r in rows if r["selection_pass"]), None)
    best_accepted = next((r for r in rows if r["acceptance_pass"]), None)
    selected = best_train
    works = bool(selected is not None and selected.get("acceptance_pass"))
    result = {
        "grid": grid_name,
        "fixed_offsets": list(fixed_offsets),
        "n_pool_rows": int(len(pool)),
        "n_grid_rows": int(len(rows)),
        "works": works,
        "selected_policy": "best train-ranked config; D is acceptance/verdict only",
        "selected_train": _without_bets(selected),
        "best_accepted_diagnostic": _without_bets(best_accepted),
    }
    if selected is not None and selected.get("_bets"):
        result["selected_bets"] = selected["_bets"]
    return rows, result


def _flatten_wfo_row(grid_name: str, row: dict[str, Any]) -> dict[str, Any]:
    cfg = row["config"]
    train = row["train"]
    full = row.get("full") or {}
    out: dict[str, Any] = {
        "grid": grid_name,
        "offsets": json.dumps(cfg.get("offsets", [])),
        "coins": ",".join(cfg.get("coins", [])),
        "pred_edge_min": cfg.get("pred_edge_min"),
        "breakout_bps": cfg.get("breakout_bps"),
        "min_entry_z": cfg.get("min_entry_z"),
        "stake_mult": cfg.get("stake_mult"),
        "max_bet_mult": cfg.get("max_bet_mult"),
        "book_consumption_mult": cfg.get("book_consumption_mult"),
        "coin_mult_name": cfg.get("coin_mult_name"),
        "exposure_cap": cfg.get("exposure_cap"),
        "selection_pass": bool(row.get("selection_pass")),
        "acceptance_pass": bool(row.get("acceptance_pass")),
        "train_total_pnl": train.get("total_pnl"),
        "train_max_dd_frac": train.get("max_dd_frac"),
        "train_worst_30bet_dd_frac": train.get("worst_30bet_dd_frac"),
        "full_total_pnl": full.get("total_pnl"),
        "full_max_dd_frac": full.get("max_dd_frac"),
        "full_worst_30bet_dd_frac": full.get("worst_30bet_dd_frac"),
        "full_n_bets": full.get("n_bets"),
        "resolution_duplicate_drops": row.get("resolution_dedup", {}).get("resolution_duplicate_drops"),
    }
    fp = full.get("fold_pnl") or train.get("fold_pnl") or {}
    fn = full.get("fold_n") or train.get("fold_n") or {}
    fw = full.get("fold_wr") or train.get("fold_wr") or {}
    for chunk in ("A", "B", "C", "D"):
        out[f"fold_pnl_{chunk}"] = fp.get(chunk)
        out[f"fold_n_{chunk}"] = fn.get(chunk)
        out[f"fold_wr_{chunk}"] = fw.get(chunk)
    vals = [float(fp.get(c, 0.0)) for c in ("A", "B", "C", "D")]
    out["worst_chunk_pnl"] = min(vals) if vals else None
    return out


def _summary_record(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("baseline_mode") == "reference":
        fp = result.get("fold_pnl") or {}
        fn = result.get("fold_n") or {}
        fw = result.get("fold_wr") or {}
        out = {
            "grid": result["grid"],
            "works": bool(result.get("works")),
            "baseline_mode": "reference",
            "n_pool_rows": result.get("n_pool_rows", 0),
            "n_grid_rows": result.get("n_grid_rows", 0),
            "full_total_pnl": result.get("full_total_pnl"),
            "full_max_dd_frac": result.get("full_max_dd_frac"),
            "full_worst_30bet_dd_frac": result.get("full_worst_30bet_dd_frac"),
            "full_n_bets": result.get("full_n_bets"),
        }
        for chunk in ("A", "B", "C", "D"):
            out[f"fold_pnl_{chunk}"] = fp.get(chunk)
            out[f"fold_n_{chunk}"] = fn.get(chunk)
            out[f"fold_wr_{chunk}"] = fw.get(chunk)
        vals = [float(fp[c]) for c in ("A", "B", "C", "D") if fp.get(c) is not None]
        out["worst_chunk_pnl"] = min(vals) if vals else None
        return out

    selected = result.get("selected_train") or {}
    full = selected.get("full") or {}
    train = selected.get("train") or {}
    fp = full.get("fold_pnl") or train.get("fold_pnl") or {}
    fn = full.get("fold_n") or train.get("fold_n") or {}
    fw = full.get("fold_wr") or train.get("fold_wr") or {}
    out = {
        "grid": result["grid"],
        "works": bool(result.get("works")),
        "n_pool_rows": result.get("n_pool_rows", 0),
        "n_grid_rows": result.get("n_grid_rows", 0),
        "full_total_pnl": full.get("total_pnl"),
        "full_max_dd_frac": full.get("max_dd_frac"),
        "full_worst_30bet_dd_frac": full.get("worst_30bet_dd_frac"),
        "full_n_bets": full.get("n_bets"),
    }
    for chunk in ("A", "B", "C", "D"):
        out[f"fold_pnl_{chunk}"] = fp.get(chunk)
        out[f"fold_n_{chunk}"] = fn.get(chunk)
        out[f"fold_wr_{chunk}"] = fw.get(chunk)
    vals = [float(fp.get(c, 0.0)) for c in ("A", "B", "C", "D")]
    out["worst_chunk_pnl"] = min(vals) if vals else None
    return out


def _write_digest(path: Path, summary_rows: list[dict[str, Any]], selected: dict[str, Any]) -> None:
    lines = [
        "# Checkpoint-Grid HGB+ORB WFO Digest",
        "",
        "- Spot source: local Binance 1s cache.",
        "- Book source: PMData raw L2 snapshots at-or-before checkpoint.",
        "- Baseline policy: baseline60 is the fixed exp044 minute ORB+HGB reference unless --baseline-mode refit is used.",
        "- Shifted-grid model protocol: local RF slices + local HGB refit per expanding WFO window.",
        "- Selection policy: best A/B/C train-ranked config; D is acceptance/verdict only.",
        "- Works verdict: all A/B/C/D chunks positive, min trade count met, DD gates passed.",
        "",
        "| grid | works | bets | pnl | maxDD | A | B | C | D | worst chunk |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_rows:
        def fmt(v, pct: bool = False) -> str:
            if v is None:
                return "n/a"
            return f"{100 * float(v):.2f}%" if pct else f"{float(v):.2f}"
        lines.append(
            "| {grid} | {works} | {bets} | {pnl} | {dd} | {a} | {b} | {c} | {d} | {worst} |".format(
                grid=row["grid"],
                works="PASS" if row.get("works") else "FAIL",
                bets=row.get("full_n_bets") if row.get("full_n_bets") is not None else 0,
                pnl=fmt(row.get("full_total_pnl")),
                dd=fmt(row.get("full_max_dd_frac"), pct=True),
                a=fmt(row.get("fold_pnl_A")),
                b=fmt(row.get("fold_pnl_B")),
                c=fmt(row.get("fold_pnl_C")),
                d=fmt(row.get("fold_pnl_D")),
                worst=fmt(row.get("worst_chunk_pnl")),
            )
        )
    lines += [
        "",
        "## Selected Payload",
        "",
        "```json",
        json.dumps(_json_safe(selected), indent=2, sort_keys=True),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    grid_names = _parse_csv(args.grids)
    unknown = sorted(set(grid_names) - set(GRID_SPECS))
    if unknown:
        raise ValueError(f"unknown grid(s): {', '.join(unknown)}")

    manifest = EXP044_MANIFEST
    manifest_sleeves = etgt._manifest_sleeves(manifest)
    wallet_sleeves = etgt._sleeve_configs(manifest_sleeves)
    markets = _read_market_rows(since=args.since, until=args.until, limit_markets=args.limit_markets)

    build_diag: dict[str, Any] = {
        "mode": args.mode,
        "grids": list(grid_names),
        "market_rows": int(len(markets)),
        "rf_hyperparams": _rf_params(args),
        "grid_specs": {k: {iv: list(v) for iv, v in grid_seconds(k).items()} for k in grid_names},
    }
    selected_payload: dict[str, Any] = {
        "objective": "checkpoint-grid HGB+ORB WFO",
        "selection_policy": (
            "baseline60 is fixed exp044 reference by default; shifted grids use "
            "best train-ranked config with D as acceptance/verdict only"
        ),
        "baseline_mode": args.baseline_mode,
        "results": {},
    }
    all_summary: list[dict[str, Any]] = []
    all_grid_rows: list[dict[str, Any]] = []

    for grid_name in grid_names:
        if grid_name == "baseline60" and args.baseline_mode == "reference":
            result = dict(EXP044_REFERENCE)
            selected_payload["results"][grid_name] = result
            all_summary.append(_summary_record(result))
            print("[reference] grid=baseline60 source=exp044_live.json", file=sys.stderr, flush=True)
            continue

        pool_path = out_dir / f"pool_{grid_name}.parquet"
        diag_path = out_dir / f"build_diag_{grid_name}.json"
        should_build = args.mode in {"build", "all"} or args.force or not pool_path.exists()
        if should_build:
            print(f"[build] grid={grid_name}", file=sys.stderr, flush=True)
            pool, diag = _build_pool_for_grid(grid_name, markets, manifest, manifest_sleeves, args)
            if not pool.empty:
                pool.to_parquet(pool_path, index=False)
            diag_path.write_text(json.dumps(_json_safe(diag), indent=2, sort_keys=True), encoding="utf-8")
            build_diag[grid_name] = diag
            print(f"[write] {pool_path} rows={len(pool):,}", file=sys.stderr, flush=True)
        else:
            pool = pd.read_parquet(pool_path)

        if args.mode == "build":
            continue

        print(f"[wfo] grid={grid_name} pool_rows={len(pool):,}", file=sys.stderr, flush=True)
        grid_rows, result = _run_grid_wfo(grid_name, pool, wallet_sleeves, args)
        selected_payload["results"][grid_name] = _without_bets(result)
        if result.get("selected_bets"):
            pd.DataFrame(result["selected_bets"]).to_parquet(out_dir / f"selected_bets_{grid_name}.parquet", index=False)
        if grid_rows:
            pd.DataFrame([_flatten_wfo_row(grid_name, row) for row in grid_rows]).to_csv(
                out_dir / f"wfo_grid_{grid_name}.csv",
                index=False,
            )
            all_grid_rows.extend(_flatten_wfo_row(grid_name, row) for row in grid_rows)
        all_summary.append(_summary_record(result))

    (out_dir / "build_diag.json").write_text(json.dumps(_json_safe(build_diag), indent=2, sort_keys=True), encoding="utf-8")
    if all_grid_rows:
        pd.DataFrame(all_grid_rows).to_csv(out_dir / "wfo_grid_summary.csv", index=False)
    if all_summary:
        pd.DataFrame(all_summary).to_csv(out_dir / "selected_summary.csv", index=False)
    (out_dir / "selected_config.json").write_text(
        json.dumps(_json_safe(selected_payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_digest(out_dir / "checkpoint_grid_digest.md", all_summary, selected_payload)
    return selected_payload


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("build", "wfo", "all"), default="all")
    parser.add_argument("--grids", default=",".join(GRID_ORDER))
    parser.add_argument(
        "--baseline-mode",
        choices=("reference", "refit"),
        default="reference",
        help="for baseline60, use fixed exp044 reference by default; refit only for diagnostics",
    )
    parser.add_argument("--since", default=None)
    parser.add_argument("--until", default=None)
    parser.add_argument("--limit-markets", type=int, default=None)
    parser.add_argument("--search-profile", choices=("baseline", "expanded"), default="baseline")
    parser.add_argument("--max-dd-frac", type=float, default=0.25)
    parser.add_argument("--min-wallet-chunk-bets", type=int, default=MIN_WALLET_CHUNK_BETS_DEFAULT)
    parser.add_argument("--capital", type=float, default=CAPITAL_DEFAULT)
    parser.add_argument("--min-bet-usd", type=float, default=MIN_BET_USD_DEFAULT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rf-n-estimators", type=int, default=int(RF_HYPERPARAMS["n_estimators"]))
    parser.add_argument("--rf-n-jobs", type=int, default=int(RF_HYPERPARAMS["n_jobs"]))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run(args)
    print(json.dumps(_json_safe({
        "out_dir": args.out_dir,
        "grids": args.grids,
        "results": result.get("results", {}),
    }), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
