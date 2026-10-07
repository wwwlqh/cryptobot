"""Compatibility entry point for TWAP export and pre-cutover replay helpers.

Model selection used expanding B/C/D windows with a one-hour embargo; native
fold F was evaluated once and never used to tune the feature set, band, gate,
or hyperparameters. After that evaluation is recorded, this production export
fits the frozen model on every labeled row available through fold F. Production
export now delegates to :mod:`export_twap_symmetric_60s`; ``_production_frame``
remains here solely for the retained August 7 pre-cutover replay.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.studies.tick_latency.run_twap_wfo_full import (
    SEED,
    _load_side_frame,
)

OUT_DEFAULT = REPO_ROOT / "data" / "models" / "twap_symmetric_v1"
FEATURE_COLUMNS = [
    "ask",
    "other_ask",
    "book_spread",
    "coin_id",
    "interval_min",
    "side_up",
    "entry_minute",
    "hour_utc",
    "dow_utc",
    "partial_ret_z_side",
    "abs_partial_ret_z",
]


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _production_frame() -> pd.DataFrame:
    frame = _load_side_frame().copy()
    frame["partial_ret_z_side"] = frame["sym_z"].astype(float)
    frame["abs_partial_ret_z"] = frame["sym_z"].abs().astype(float)
    return frame.dropna(subset=FEATURE_COLUMNS + ["win_twap_s"]).copy()


def _build_legacy_production_model(out_dir: Path | str = OUT_DEFAULT) -> dict:
    """Retained 30s/60s exporter for historical replay only; never deploy."""
    started = time.time()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    train = _production_frame()
    if train.empty:
        raise RuntimeError("TWAP symmetric export loaded zero training rows")

    model = HistGradientBoostingClassifier(
        max_iter=100,
        learning_rate=0.04,
        max_leaf_nodes=31,
        l2_regularization=0.02,
        random_state=SEED,
    )
    model.fit(train[FEATURE_COLUMNS], train["win_twap_s"].astype(np.int8))
    probe = model.predict_proba(train[FEATURE_COLUMNS].iloc[:16])[:, 1]
    if not np.isfinite(probe).all():
        raise RuntimeError("TWAP symmetric export produced non-finite probabilities")

    model_path = out / "twap_hgb_v1.joblib"
    joblib.dump({"5m": model, "15m": model}, model_path)
    max_entry_ts = int(train["entry_ts"].max())
    min_entry_ts = int(train["entry_ts"].min())
    manifest = {
        "artifact": "twap_symmetric_v1",
        "target": "win_twap_s",
        "model_family": "HistGradientBoostingClassifier",
        "feature_columns": {
            "5m": list(FEATURE_COLUMNS),
            "15m": list(FEATURE_COLUMNS),
        },
        "chainlink_window_seconds": {"5m": 30, "15m": 60},
        "live_feature_contract": {
            "name": "binance_trailing_twap_centered_chainlink_basis_v1",
            "binance_window_seconds": {"5m": 30, "15m": 60},
            "basis_residual_clip_log": 0.0001,
            "basis_center_log": {
                "btc": {
                    "5m": 0.0007653637343482361,
                    "15m": 0.0007705237138092435,
                },
                "sol": {
                    "5m": 0.0007494865876785258,
                    "15m": 0.0007786021352305104,
                },
                "xrp": {
                    "5m": 0.0007436947435870979,
                    "15m": 0.0007483628430746186,
                },
            },
            "basis_center_fit_period": "2026-08-07/2026-08-10",
            "forward_check_period": "2026-08-11",
            "causal_window": "[boundary-window,boundary)",
        },
        "sleeves": {
            "5m": {
                "threshold": 0.06,
                "ask_bucket": "band:0.60:0.95",
                "side_filter": "both",
                "book_consumption_max": 0.30,
            },
            "15m": {
                "threshold": 0.05,
                "ask_bucket": "band:0.60:0.95",
                "side_filter": "both",
                "book_consumption_max": 0.06,
            },
        },
        "training_coins": sorted(train["coin"].astype(str).unique().tolist()),
        "live_coins": ["btc", "sol", "xrp"],
        "intervals": ["5m", "15m"],
        "folds": sorted(train["fold"].astype(str).unique().tolist()),
        "train_rows": int(len(train)),
        "train_markets": int(train["market_slug"].nunique()),
        "min_entry_ts": min_entry_ts,
        "min_entry_dt_utc": pd.to_datetime(
            min_entry_ts, unit="s", utc=True
        ).isoformat(),
        "max_entry_ts": max_entry_ts,
        "max_entry_dt_utc": pd.to_datetime(
            max_entry_ts, unit="s", utc=True
        ).isoformat(),
        "hyperparameters": {
            "max_iter": 100,
            "learning_rate": 0.04,
            "max_leaf_nodes": 31,
            "l2_regularization": 0.02,
            "random_state": SEED,
        },
        "selection_protocol": (
            "Expanding all-history B/C/D WFO, one-hour embargo, every fold "
            "n>=100 and EV>0; fold F touched once after selection"
        ),
        "selected_contract": (
            "BTC+SOL+XRP operator override with causal Binance opening TWAP and frozen centred/clipped "
            "Chainlink basis; both 5m and 15m; 0.60<ask<=0.95; fee-adjusted "
            "pred_edge>=0.06 (5m) / >=0.05 (15m); best executable model "
            "side; consume on first fill; 2% base stake; T3 on; Tier A off"
        ),
        "historical_input_note": (
            "Historical labels are a trailing arithmetic proxy reconstructed "
            "from the ordinary Chainlink 1s tape. Live serving requires the "
            "exact RTDS TWAP boundary sample plus a causal Binance opening TWAP "
            "and abstains when either is unavailable. BTC+SOL+XRP/T3 2% execution "
            "was replayed with exact Gamma TWAP labels on Aug 7-10; Aug 11 was "
            "the forward check for the frozen basis centres."
        ),
        "exact_forward_holdout": {
            "selection_period": "2026-08-07/2026-08-10",
            "final_period": "2026-08-11",
            "policy": "btc_sol_xrp_both_intervals_t3_tier_a_off_2pct_cap095_operator_override",
            "replay_equity_usd": 80.820665,
            "base_equity_fraction": 0.02,
            "development_fills": 376,
            "development_win_rate": 0.8271276595744681,
            "development_ev_per_dollar": 0.05618756395251683,
            "final_fills": 77,
            "final_win_rate": 0.8311688311688312,
            "final_ev_per_dollar": 0.040136725713754366,
            "full_period_fills": 453,
            "full_period_orders_including_t3": 587,
            "full_period_ev_per_dollar": 0.05358848435604454,
            "full_period_max_drawdown_usd": 15.5730530912101,
            "xrp_final_holdout_fills": 15,
            "xrp_final_holdout_win_rate": 0.7333333333333333,
            "xrp_final_holdout_ev_per_dollar": 0.013406069685700867,
            "operator_override": (
                "The operator widened the live fill cap to 0.95 on 2026-08-13; "
                "model weights, gates, coin universe, sizing, T3, and Tier A "
                "were unchanged."
            ),
        },
        "model_sha256": _sha256(model_path),
        "built_ts": int(time.time()),
        "elapsed_s": round(time.time() - started, 2),
    }
    manifest_path = out / "twap_symmetric_v1_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return manifest


from polymarket.backtest.training.export_twap_symmetric_60s import (  # noqa: E402
    build_production_model,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT_DEFAULT)
    args = parser.parse_args()
    manifest = build_production_model(args.out)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
