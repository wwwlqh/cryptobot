"""Refit per-coin 5m HGBs for a future synthetic 60-second TWAP target.

This intentionally performs no August forward test and does not touch live
artifacts.  It follows the proven 15m model/execution shape while changing the
interval and checkpoint offsets to 5m: one estimator per coin, a 60-second
Chainlink/Binance opening contract, and decisions from T-60 through T-1.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.studies.tick_latency import (
    run_twap15m60_refit_aug13 as core,
)
from polymarket.backtest.training.offset_wfo import _load_1s


OUT_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap5m60_refit_only_20260814"
)
FIXED_STRATEGY = {
    "ask_lo": 0.60,
    "ask_hi": 0.95,
    "gate": 0.05,
    "book_consumption_max": 0.06,
    "live_coins": ["btc", "eth", "sol", "xrp"],
    "t3": True,
    "tier_a": False,
}


def _configure() -> None:
    core.INTERVAL = "5m"
    core.INTERVAL_MIN = 5
    core.INTERVAL_S = 300
    core.OFFSETS = range(240, 300)
    core.TWAP_S = 60
    core.MIN_TICKS = 50


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--coins", nargs="+", choices=core.COINS, default=list(core.COINS)
    )
    parser.add_argument("--rebuild-features", action="store_true")
    args = parser.parse_args(argv)
    _configure()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    started = time.time()
    coin_report = {}
    basis_centres = {}
    model_hashes = {}
    for coin in args.coins:
        cache = OUT_ROOT / f"train_features_{coin}.parquet"
        paths = core._book_paths(coin, core.TRAIN_START[coin], core.TRAIN_END)
        if args.rebuild_features or not cache.exists():
            print(
                f"[features] 5m synthetic60 {coin}: {len(paths):,} books",
                flush=True,
            )
            labels = core._training_labels(coin, paths)
            core._write_feature_cache(
                coin,
                "train60",
                labels,
                _load_1s(coin),
                paths,
                cache,
            )
        raw = pd.read_parquet(cache)
        if (
            int(raw["offset_sec"].min()) != 240
            or int(raw["offset_sec"].max()) != 299
        ):
            raise AssertionError(f"{coin} cache is not T-60 through T-1")
        centre = float(
            raw[["market_slug", "basis_log"]]
            .drop_duplicates()["basis_log"]
            .median()
        )
        basis_centres[coin] = centre
        featured = core._with_features(raw, centre)
        print(
            f"[fit] per-coin 5m synthetic60 HGB {coin}: "
            f"{len(featured):,} side-second rows",
            flush=True,
        )
        model = HistGradientBoostingClassifier(
            max_iter=100,
            learning_rate=0.04,
            max_leaf_nodes=31,
            l2_regularization=0.02,
            random_state=42,
        )
        model.fit(
            featured[core.FEATURE_COLUMNS], featured["win_true"].astype("int8")
        )
        model_path = OUT_ROOT / f"model_{coin}.joblib"
        joblib.dump(
            {
                "coin": coin,
                "interval": "5m",
                "feature_columns": core.FEATURE_COLUMNS,
                "basis_center_log": centre,
                "strategy": FIXED_STRATEGY,
                "model": model,
            },
            model_path,
        )
        model_hashes[coin] = _sha256(model_path)
        coin_report[coin] = {
            "rows": int(len(raw)),
            "markets": int(raw["market_slug"].nunique()),
            "start": pd.to_datetime(
                raw["slug_ts"].min(), unit="s", utc=True
            ).isoformat(),
            "end": pd.to_datetime(
                raw["slug_ts"].max(), unit="s", utc=True
            ).isoformat(),
            "basis_center_log": centre,
        }
        del raw, featured
    manifest = {
        "artifact": "twap5m60_refit_only_20260814",
        "status": "research_only_not_deployed",
        "interval": "5m",
        "model_routing": "per_coin",
        "decision_window": "T-60 through T-1 inclusive",
        "historical_target": (
            "ordinary Chainlink 1s arithmetic mean over "
            "(boundary-60,boundary] at open and close"
        ),
        "forward_test": None,
        "book_contract": "YES L2 only; DOWN ask derived as 1 - YES bid",
        "feature_columns": core.FEATURE_COLUMNS,
        "fixed_strategy": FIXED_STRATEGY,
        "training": coin_report,
        "basis_center_log": basis_centres,
        "train_rows": int(sum(body["rows"] for body in coin_report.values())),
        "train_coins": list(args.coins),
        "model_sha256_by_coin": model_hashes,
        "elapsed_s": round(time.time() - started, 2),
    }
    manifest_path = OUT_ROOT / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"[done] {manifest_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
