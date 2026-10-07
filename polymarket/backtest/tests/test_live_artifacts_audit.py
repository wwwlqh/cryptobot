from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from polymarket.backtest.audit.live_artifacts import FEATURES_11, run_audit


def _write_fixture(root: Path) -> None:
    config_dir = root / "backtest/optimize_runs"  # legacy-path-ok: fixture builds the LEGACY tree to exercise legacy_first
    model_dir = root / "data/models/checkpoint_v1"
    pool_dir = config_dir / "option3_pool/pool_step1last_option3"
    config_dir.mkdir(parents=True)
    model_dir.mkdir(parents=True)
    pool_dir.mkdir(parents=True)

    # Subminute-only (2026-07-21): the artifact carries only the two subminute
    # sleeves; the checkpoint manifest still lists the nl entries (ignored —
    # portfolio->manifest validation is one-directional), so they stay in the
    # fixture to satisfy the audit's MANIFEST_KEYS equality check.
    sleeves = []
    for label, interval, offsets, lo, hi, threshold in (
        ("subminute_5m", "5m", "270:299:1", 0.65, 0.85, 0.03),
        ("subminute_15m", "15m", "870:899:1", 0.65, 0.85, 0.03),
    ):
        sleeves.append({
            "label": label,
            "config": {
                "interval": interval,
                "checkpoint_offset_set": offsets,
                "min_fill_price": lo,
                "max_fill_price": hi,
                "threshold": threshold,
                "feature_groups": "rf,price,identity,time",
            },
        })
    (config_dir / "v2_step1last_live.json").write_text(json.dumps({"sleeves": sleeves}))
    manifest = {
        "feature_columns": {"5m": FEATURES_11, "15m": FEATURES_11},
        "sleeves": {key: {} for key in ("5m", "15m", "nl_5m", "nl_15m")},
    }
    (model_dir / "checkpoint_v1_manifest.json").write_text(json.dumps(manifest))
    for name in ("checkpoint_rf_v1.joblib", "checkpoint_hgb_v1.joblib"):
        (model_dir / name).write_bytes(b"model")
    # The tick report is a TRACKED verdict artifact: it moved with the code to
    # polymarket/backtest/studies/ in the 2026-07-28 restructure, unlike the
    # gitignored bulk data above, which stayed at its legacy path.
    tick_dir = root / "polymarket/backtest/studies/tick_latency"
    tick_dir.mkdir(parents=True)
    (tick_dir / "foldBE_stop_report_400.json").write_text(json.dumps({
        "latency_ms": 400,
        "scoring": {"dynamic_held_side_rescore": True},
        "execution": {"stop_order": "FOK full size"},
        "folds": {"B": {}, "C": {}, "D": {}, "E": {}},
    }))

    pd.DataFrame({
        "market_slug": ["m"],
        "entry_ts": [1],
        "offset_sec": [270],
        "ask": [0.7],
        "pred_edge": [0.04],
        "fold": ["E"],
    }).to_parquet(config_dir / "scored_11set_truth.parquet")
    pd.DataFrame({"x": [1]}).to_parquet(pool_dir / "part.parquet")
    for path in (
        root / "backtest/cache/binance_1s/part",  # legacy-path-ok
        root / "backtest/data/pmdata/poly_tick_last30.db",  # legacy-path-ok
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("cached")
    outcomes = root / "backtest/data/pmdata/pmdata_outcomes.parquet"  # legacy-path-ok
    outcomes.parent.mkdir(parents=True, exist_ok=True)
    outcomes.write_bytes(b"cached")


def test_current_live_artifact_chain_passes(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    report = run_audit(tmp_path)
    assert report["passed"], [item for item in report["checks"] if not item["passed"]]


def test_feature_drift_fails(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    path = tmp_path / "data/models/checkpoint_v1/checkpoint_v1_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["feature_columns"]["15m"] = FEATURES_11[:-1]
    path.write_text(json.dumps(manifest))
    report = run_audit(tmp_path)
    assert not report["passed"]
    check = next(item for item in report["checks"] if item["name"] == "11-feature manifest")
    assert not check["passed"]
