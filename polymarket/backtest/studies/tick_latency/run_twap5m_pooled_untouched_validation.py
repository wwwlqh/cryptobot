"""Validate the frozen pooled 5m HGB after the timing-selection period.

The model, feature centres, ask band, edge gate, and candidate windows come
from ``run_twap_pooled_fixed_entry_windows``.  This runner builds features only
for market starts at/after 2026-08-14 10:05 UTC and never calls ``fit``.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.studies.tick_latency import (
    run_twap15m60_refit_aug13 as core,
)
from polymarket.backtest.studies.tick_latency import (
    run_twap5m_entry_window_fresh as fresh,
)
from polymarket.backtest.studies.tick_latency import (
    run_twap_pooled_fixed_entry_windows as pooled,
)


FORWARD_START = pd.Timestamp("2026-08-14 10:05:00", tz="UTC")
FORWARD_END = pd.Timestamp("2026-08-14 14:25:00", tz="UTC")
WINDOWS = (60, 150, 160, 299)
MODEL_ROOT = pooled.OUT_ROOT / "5m"
OUT_ROOT = pooled.OUT_ROOT / "5m_untouched_post1005"
FEATURE_PATH = OUT_ROOT / "forward_features.parquet"
INITIAL_SPOT = (
    core.FORWARD_SPOT / "{pair}_20260814T000000Z_20260814T100500Z.parquet"
)
VALIDATION_SPOT = (
    core.FORWARD_SPOT / "{pair}_20260814T100500Z_20260814T142500Z.parquet"
)


def _load_spot(coin: str) -> pd.DataFrame:
    pair = core.PAIRS[coin]
    parts = [
        core._load_forward_spot(coin),
        pd.read_parquet(Path(str(INITIAL_SPOT).format(pair=pair))),
        pd.read_parquet(Path(str(VALIDATION_SPOT).format(pair=pair))),
    ]
    out = pd.concat(parts, ignore_index=True)
    out[["open", "close"]] = out[["open", "close"]].astype(float)
    out["sec"] = out["sec"].astype(np.int64)
    return (
        out.sort_values("sec")
        .drop_duplicates("sec", keep="last")
        .reset_index(drop=True)
    )


def _load_or_build_features() -> tuple[pd.DataFrame, dict[str, Path], dict]:
    frames, path_map, agreements = [], {}, {}
    if FEATURE_PATH.exists():
        frame = pd.read_parquet(FEATURE_PATH)
        for coin in pooled.COINS:
            paths = core._book_paths(coin, FORWARD_START, FORWARD_END)
            path_map.update({path.stem: path for path in paths})
        return frame, path_map, {"cache_reused": True}

    feature_parts = []
    for coin in pooled.COINS:
        paths = core._book_paths(coin, FORWARD_START, FORWARD_END)
        spot = _load_spot(coin)
        labels, agreement = fresh._fresh_labels(paths, spot)
        target = OUT_ROOT / f"forward_features_{coin}.parquet"
        core.OFFSETS = range(1, 300)
        core._write_feature_cache(coin, "untouched_post1005", labels, spot, paths, target)
        part = pd.read_parquet(target)
        part["coin"] = coin
        feature_parts.append(part)
        path_map.update({path.stem: path for path in paths})
        agreements[coin] = agreement
    frame = pd.concat(feature_parts, ignore_index=True)
    frame.to_parquet(FEATURE_PATH, index=False)
    return frame, path_map, agreements


def main() -> int:
    started = time.time()
    pooled._configure(pooled.STUDIES["5m"])
    core.FORWARD_START = FORWARD_START
    core.FORWARD_END = FORWARD_END
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    payload = joblib.load(MODEL_ROOT / "pooled_hgb.joblib")
    if payload.get("pooled_across_coins") is not True:
        raise ValueError("5m validation model is not pooled")
    features, paths, agreements = _load_or_build_features()
    scored = pooled._with_pooled_features(features, payload["basis_center_log"])
    scored["model_p"] = payload["model"].predict_proba(
        scored[pooled.MODEL_FEATURE_COLUMNS]
    )[:, 1]
    scored["pred_edge"] = (
        scored["model_p"]
        - scored["ask"]
        - ps.fee(scored["ask"].to_numpy(float))
    )
    previous_root = pooled.OUT_ROOT
    pooled.OUT_ROOT = OUT_ROOT
    try:
        windows = {
            f"{window}s": pooled._replay_window(
                pooled.STUDIES["5m"], window, scored, paths
            )
            for window in WINDOWS
        }
    finally:
        pooled.OUT_ROOT = previous_root
    report = {
        "artifact": "twap5m_pooled_untouched_post1005_20260814",
        "status": "research_only_not_deployed",
        "model_sha256": pooled._sha256(MODEL_ROOT / "pooled_hgb.joblib"),
        "refits": 0,
        "gate_reselections": 0,
        "forward_start": FORWARD_START.isoformat(),
        "forward_end_exclusive": FORWARD_END.isoformat(),
        "markets": int(scored["market_slug"].nunique()),
        "markets_by_coin": {
            coin: int(part["market_slug"].nunique())
            for coin, part in scored.groupby("coin", sort=True)
        },
        "synthetic_60s_direction_vs_pmdata_resolution": agreements,
        "windows": windows,
        "comparison": pooled._comparison(windows),
        "elapsed_s": round(time.time() - started, 2),
    }
    target = OUT_ROOT / "report.json"
    target.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["comparison"], indent=2), flush=True)
    print(f"[done] {target}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
