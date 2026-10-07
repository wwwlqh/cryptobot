"""Train two pooled TWAP-60 HGBs, then compare shared entry cutoffs.

Exactly one estimator is fitted for 5m and one for 15m. Each pools BTC, ETH,
SOL, and XRP, treats ``coin_id`` as categorical, and gives every coin equal
total training weight. The model, feature centres, ask band, and edge gate are
frozen before timing replay; a candidate only changes the first eligible entry
second and is identical across all four coins.

Dense one-second training rows are strongly autocorrelated, so final fits use a
deterministic five-second stride. Exact forward scoring/replay stays at 1 Hz.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.studies.tick_latency import (
    run_twap15m60_refit_aug13 as core,
)
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact


COINS = tuple(core.COINS)
COMMON_TRAIN_START = max(core.TRAIN_START.values())
TRAIN_END = core.TRAIN_END
TRAIN_STRIDE_SECONDS = 5
ASK_LO, ASK_HI, EDGE_GATE = 0.55, 0.85, 0.10
BOOK_CONSUMPTION_MAX = 0.06
MODEL_FEATURE_COLUMNS = list(core.FEATURE_COLUMNS)
RAW_MODEL_COLUMNS = [
    "slug_ts", "offset_sec", "ask", "other_ask", "book_spread", "coin_id",
    "interval_min", "side_up", "entry_minute", "hour_utc", "dow_utc",
    "log_spot_over_binance_open", "basis_log", "z_denom", "win_true",
]
HYPERPARAMETERS = {
    "max_iter": 100,
    "learning_rate": 0.04,
    "max_leaf_nodes": 31,
    "l2_regularization": 0.02,
    "categorical_features": ["coin_id"],
    "random_state": 42,
}


@dataclass(frozen=True)
class IntervalStudy:
    interval: str
    interval_seconds: int
    widest_window_seconds: int
    feature_root: Path
    forward_start: pd.Timestamp
    forward_end: pd.Timestamp
    selection_windows: tuple[int, ...]
    diagnostic_windows: tuple[int, ...]

    @property
    def windows(self) -> tuple[int, ...]:
        return tuple(sorted(set(self.selection_windows + self.diagnostic_windows)))


OPTIMIZE_ROOT = REPO_ROOT / "polymarket" / "backtest" / "optimize_runs"
STUDIES = {
    "5m": IntervalStudy(
        "5m", 300, 299,
        OPTIMIZE_ROOT / "twap5m_entry_windows_fresh_20260814" / "features_299s",
        pd.Timestamp("2026-08-14 00:00:00", tz="UTC"),
        pd.Timestamp("2026-08-14 10:05:00", tz="UTC"),
        (60, 90, 120, 150, 180, 210, 240, 270, 299),
        (160,),
    ),
    "15m": IntervalStudy(
        "15m", 900, 600,
        OPTIMIZE_ROOT / "twap15m_entry_windows_20260814" / "features_600s",
        pd.Timestamp("2026-08-07", tz="UTC"),
        pd.Timestamp("2026-08-14", tz="UTC"),
        (60, 120, 180, 240, 300, 360, 420, 480, 540, 600),
        (520,),
    ),
}
OUT_ROOT = OPTIMIZE_ROOT / "twap_pooled_fixed_entry_windows_20260814"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _configure(study: IntervalStudy) -> None:
    core.INTERVAL = study.interval
    core.INTERVAL_MIN = study.interval_seconds // 60
    core.INTERVAL_S = study.interval_seconds
    core.TWAP_S = 60
    core.FORWARD_START = study.forward_start
    core.FORWARD_END = study.forward_end


def _window_rows(frame: pd.DataFrame, study: IntervalStudy, window_s: int) -> pd.DataFrame:
    lo = max(1, study.interval_seconds - window_s)
    out = frame[frame["offset_sec"] >= lo].copy()
    if out.empty or int(out["offset_sec"].min()) != lo:
        raise AssertionError(f"{study.interval} cache lacks T-{window_s}")
    if int(out["offset_sec"].max()) != study.interval_seconds - 1:
        raise AssertionError(f"{study.interval} cache does not end at T-1")
    return out


def _basis_centres(study: IntervalStudy) -> dict[str, float]:
    centres = {}
    common_start = int(COMMON_TRAIN_START.timestamp())
    for coin in COINS:
        frame = pd.read_parquet(
            study.feature_root / f"train_features_{coin}.parquet",
            columns=["market_slug", "slug_ts", "basis_log"],
        )
        markets = frame[frame["slug_ts"] >= common_start].drop_duplicates(
            "market_slug"
        )
        if markets.empty:
            raise ValueError(f"no common-period markets for {study.interval}:{coin}")
        centres[coin] = float(markets["basis_log"].median())
    return centres


def _with_pooled_features(frame: pd.DataFrame, centres: dict[str, float]) -> pd.DataFrame:
    out = frame.copy()
    centre = out["coin"].map(centres).to_numpy(float)
    if not np.isfinite(centre).all():
        raise ValueError("pooled feature frame contains an unknown coin")
    residual = np.clip(
        out["basis_log"].to_numpy(float) - centre,
        -core.BASIS_CLIP_LOG,
        core.BASIS_CLIP_LOG,
    )
    z_up = (
        out["log_spot_over_binance_open"].to_numpy(float) + residual
    ) / out["z_denom"].to_numpy(float)
    out["partial_ret_z_side"] = np.where(
        out["side_up"].to_numpy(np.int8) == 1, z_up, -z_up
    )
    out["abs_partial_ret_z"] = np.abs(z_up)
    return out


def _load_training_sample(
    study: IntervalStudy, centres: dict[str, float]
) -> tuple[pd.DataFrame, np.ndarray, dict]:
    frames, raw_rows, sampled_rows = [], {}, {}
    common_start = int(COMMON_TRAIN_START.timestamp())
    lo = max(1, study.interval_seconds - study.widest_window_seconds)
    for coin in COINS:
        frame = pd.read_parquet(
            study.feature_root / f"train_features_{coin}.parquet",
            columns=RAW_MODEL_COLUMNS,
        )
        frame = frame[
            (frame["slug_ts"] >= common_start)
            & (frame["slug_ts"] < int(TRAIN_END.timestamp()))
            & (frame["offset_sec"] >= lo)
        ].copy()
        raw_rows[coin] = int(len(frame))
        frame = frame[frame["offset_sec"] % TRAIN_STRIDE_SECONDS == 0].copy()
        sampled_rows[coin] = int(len(frame))
        frame["coin"] = coin
        frames.append(_with_pooled_features(frame, centres))
    pooled = pd.concat(frames, ignore_index=True)
    weights = np.empty(len(pooled), dtype=float)
    coins = pooled["coin"].to_numpy()
    for coin in COINS:
        mask = coins == coin
        weights[mask] = len(pooled) / (len(COINS) * int(mask.sum()))
    return pooled, weights, {
        "common_train_start": COMMON_TRAIN_START.isoformat(),
        "train_end_exclusive": TRAIN_END.isoformat(),
        "training_stride_seconds": TRAIN_STRIDE_SECONDS,
        "raw_rows_by_coin": raw_rows,
        "sampled_rows_by_coin": sampled_rows,
        "sampled_rows": int(len(pooled)),
        "equal_total_weight_per_coin": True,
    }


def _fit_interval(study: IntervalStudy):
    centres = _basis_centres(study)
    train, weights, metadata = _load_training_sample(study, centres)
    model = HistGradientBoostingClassifier(**HYPERPARAMETERS)
    print(f"[fit:{study.interval}] pooled rows={len(train):,}", flush=True)
    model.fit(
        train[MODEL_FEATURE_COLUMNS],
        train["win_true"].astype(np.int8),
        sample_weight=weights,
    )
    del train, weights
    root = OUT_ROOT / study.interval
    root.mkdir(parents=True, exist_ok=True)
    model_path = root / "pooled_hgb.joblib"
    joblib.dump(
        {
            "interval": study.interval,
            "coins": list(COINS),
            "pooled_across_coins": True,
            "fixed_model_window_seconds": study.widest_window_seconds,
            "target_twap_seconds": 60,
            "feature_columns": MODEL_FEATURE_COLUMNS,
            "categorical_features": ["coin_id"],
            "basis_center_log": centres,
            "basis_clip_log": core.BASIS_CLIP_LOG,
            "execution": {
                "ask_lo": ASK_LO, "ask_hi": ASK_HI, "gate": EDGE_GATE,
                "book_consumption_max": BOOK_CONSUMPTION_MAX,
            },
            "training": metadata,
            "model": model,
        },
        model_path,
    )
    return model, centres, {
        **metadata,
        "model_path": str(model_path.relative_to(REPO_ROOT)),
        "model_sha256": _sha256(model_path),
        "basis_center_log": centres,
    }


def _load_forward_scored(study: IntervalStudy, model, centres):
    frames, paths, markets_by_coin = [], {}, {}
    for coin in COINS:
        frame = pd.read_parquet(
            study.feature_root / f"forward_features_{coin}.parquet"
        )
        frame["coin"] = coin
        _window_rows(frame, study, study.widest_window_seconds)
        frames.append(frame)
        coin_paths = core._book_paths(coin, study.forward_start, study.forward_end)
        paths.update({path.stem: path for path in coin_paths})
        markets_by_coin[coin] = int(frame["market_slug"].nunique())
    forward = _with_pooled_features(pd.concat(frames, ignore_index=True), centres)
    forward["model_p"] = model.predict_proba(
        forward[MODEL_FEATURE_COLUMNS]
    )[:, 1]
    forward["pred_edge"] = (
        forward["model_p"] - forward["ask"]
        - ps.fee(forward["ask"].to_numpy(float))
    )
    return forward, paths, {
        "rows": int(len(forward)),
        "markets": int(forward["market_slug"].nunique()),
        "markets_by_coin": markets_by_coin,
    }


def _attach_offsets(rows: pd.DataFrame, scored: pd.DataFrame, study: IntervalStudy):
    out = rows.copy()
    starts = (
        scored[["market_slug", "slug_ts"]].drop_duplicates("market_slug")
        .set_index("market_slug")["slug_ts"].to_dict()
    )
    out["offset_sec"] = out.apply(
        lambda row: int(row["entry_ts"]) - int(starts[row["market_slug"]])
        if bool(row["entry_filled"]) else np.nan,
        axis=1,
    )
    out["seconds_to_settle"] = np.where(
        out["entry_filled"], study.interval_seconds - out["offset_sec"], np.nan
    )
    return out


def _replay_window(study: IntervalStudy, window_s: int, scored, paths):
    window = _window_rows(scored, study, window_s)
    slugs = set(window["market_slug"])
    window_paths = {slug: path for slug, path in paths.items() if slug in slugs}
    old_spec = dict(exact.SPECS[study.interval])
    exact.SPECS[study.interval] = {
        "intervals": (study.interval,), "ask_lo": ASK_LO,
        "ask_hi": ASK_HI, "gate": EDGE_GATE,
    }
    try:
        base = exact._replay(window, window_paths, tier_mult=1.0, t3=False)
        t3 = exact._replay(window, window_paths, tier_mult=1.0, t3=True)
    finally:
        exact.SPECS[study.interval] = old_spec
    base = _attach_offsets(base, window, study)
    t3 = _attach_offsets(t3, window, study)
    root = OUT_ROOT / study.interval / f"window_{window_s}s"
    root.mkdir(parents=True, exist_ok=True)
    base.to_parquet(root / "forward_base_trades.parquet", index=False)
    t3.to_parquet(root / "forward_t3_trades.parquet", index=False)
    bm, tm = core._exact_metrics(base), core._exact_metrics(t3)
    wr = bm["win_rate"] if bm["win_rate"] is not None else float("nan")
    print(
        f"[replay:{study.interval}:T-{window_s}] fills={bm['fills']} "
        f"WR={wr:.3f} PnL=${bm['pnl_usd']:.2f} DD=${bm['max_drawdown_usd']:.2f}",
        flush=True,
    )
    return {
        "decision_window": f"T-{window_s} through T-1 inclusive",
        "base_all": bm,
        "t3_all": tm,
        "base_by_coin": core._by(base, "coin"),
        "t3_by_coin": core._by(t3, "coin"),
        "base_by_day": core._by(
            base.assign(day=pd.to_datetime(base["entry_ts"], unit="s", utc=True)
                        .dt.strftime("%Y-%m-%d")),
            "day",
        ),
    }


def _comparison(windows: dict[str, dict]) -> list[dict]:
    rows = []
    for label in sorted(windows, key=lambda value: int(value.removesuffix("s"))):
        base, t3 = windows[label]["base_all"], windows[label]["t3_all"]
        rows.append({
            "window": label,
            "base_fills": base["fills"],
            "base_win_rate": base["win_rate"],
            "base_pnl_usd": base["pnl_usd"],
            "base_ev_per_trade_usd": base["ev_per_filled_trade_usd"],
            "base_max_drawdown_usd": base["max_drawdown_usd"],
            "t3_pnl_usd": t3["pnl_usd"],
            "t3_ev_per_trade_usd": t3["ev_per_filled_trade_usd"],
            "t3_max_drawdown_usd": t3["max_drawdown_usd"],
        })
    return rows


def _run_interval(study: IntervalStudy) -> dict:
    _configure(study)
    model, centres, training = _fit_interval(study)
    scored, paths, forward = _load_forward_scored(study, model, centres)
    windows = {
        f"{window_s}s": _replay_window(study, window_s, scored, paths)
        for window_s in study.windows
    }
    return {
        "interval": study.interval,
        "model_routing": "one pooled estimator across BTC/ETH/SOL/XRP",
        "refits_during_timing_comparison": 0,
        "gate_reselections_during_timing_comparison": 0,
        "selection_windows": list(study.selection_windows),
        "diagnostic_windows_not_eligible_for_selection": list(study.diagnostic_windows),
        "execution": {
            "ask_lo": ASK_LO, "ask_hi": ASK_HI, "gate": EDGE_GATE,
            "book_consumption_max": BOOK_CONSUMPTION_MAX,
        },
        "training": training,
        "forward": {
            **forward, "start": study.forward_start.isoformat(),
            "end_exclusive": study.forward_end.isoformat(),
        },
        "windows": windows,
        "comparison": _comparison(windows),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--intervals", nargs="+", choices=tuple(STUDIES), default=list(STUDIES)
    )
    args = parser.parse_args(argv)
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    started = time.time()
    target = OUT_ROOT / "report.json"
    prior_intervals = {}
    if target.exists():
        prior = json.loads(target.read_text(encoding="utf-8"))
        if prior.get("artifact") == "twap_pooled_fixed_entry_windows_20260814":
            prior_intervals = prior.get("intervals", {})
    results = dict(prior_intervals)
    results.update(
        {interval: _run_interval(STUDIES[interval]) for interval in args.intervals}
    )
    report = {
        "artifact": "twap_pooled_fixed_entry_windows_20260814",
        "status": "research_only_not_deployed",
        "target": "ordinary 1s 60-second TWAP direction",
        "settlement_note": (
            "training uses archived Chainlink TWAP labels; exact forward uses "
            "Gamma/PMData resolved outcomes"
        ),
        "coins": list(COINS),
        "model_count": len(results),
        "pooled_across_coins": True,
        "shared_timing_across_coins": True,
        "intervals": results,
        "elapsed_s": round(time.time() - started, 2),
    }
    target.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    for interval, body in results.items():
        print(f"[{interval}]", flush=True)
        print(json.dumps(body["comparison"], indent=2), flush=True)
    print(f"[done] {target}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
