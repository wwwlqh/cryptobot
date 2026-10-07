"""Audit the current last-30-second subminute contract and its reproducibility inputs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from polymarket.backtest import REPO_ROOT as ROOT, legacy_first
FEATURES_11 = [
    "model_p_side",
    "rf_edge",
    "ask",
    "other_ask",
    "book_spread",
    "coin_id",
    "interval_min",
    "side_up",
    "entry_minute",
    "hour_utc",
    "dow_utc",
]
# Subminute-only since 2026-07-21 (FOLDF_HOLDOUT_20260721.md): the two nl_*
# sleeves were cut from live. MANIFEST_KEYS must KEEP the nl entries — the
# checkpoint_v1 model manifest on disk still carries them, and validation is
# portfolio->manifest one-directional, so the extra manifest entries are ignored
# and no model re-export is needed until a coordinated re-export happens.
SLEEVE_CONTRACTS = {
    "subminute_5m": ("5m", "270:299:1", 0.65, 0.85, 0.03),
    "subminute_15m": ("15m", "870:899:1", 0.65, 0.85, 0.03),
}
MANIFEST_KEYS = {"5m", "15m", "nl_5m", "nl_15m"}
TRUTH_COLUMNS = {"market_slug", "entry_ts", "offset_sec", "ask", "pred_edge", "fold"}


def _parquet_columns(path: Path) -> set[str]:
    import pyarrow.parquet as pq

    return set(pq.ParquetFile(path).schema.names)


def _add(checks: list[dict[str, Any]], name: str, passed: bool, detail: str) -> None:
    checks.append({"name": name, "passed": bool(passed), "detail": detail})


def run_audit(root: Path = ROOT) -> dict[str, Any]:
    """Return a machine-readable audit without modifying any artifact."""
    # v2_step1last_live.json is a LIVE contract, not a research output, so it
    # moved out of the research tree on 2026-07-28. Everything else below is
    # gitignored bulk data that stayed physically put — hence legacy_first.
    config_path = root / "polymarket/live/config/v2_step1last_live.json"
    if not config_path.is_file():
        legacy_config = root / "backtest/optimize_runs/v2_step1last_live.json"  # legacy-path-ok: explicit legacy fallback
        if legacy_config.is_file():
            config_path = legacy_config
    model_dir = root / "data/models/checkpoint_v1"
    manifest_path = model_dir / "checkpoint_v1_manifest.json"
    truth_path = legacy_first(root, "optimize_runs/scored_11set_truth.parquet")
    pool_dir = legacy_first(root, "optimize_runs/option3_pool/pool_step1last_option3")
    tick_report_path = root / "polymarket/backtest/studies/tick_latency/foldBE_stop_report_400.json"
    # pmdata poly_snapshot/ and cache/book_depth_index_pmdata/ were retired and
    # deleted 2026-07-31 (non-rebuildable); data/recorder_snapshot/ followed on
    # 2026-08-01 (deletion confirmed by the operator 2026-08-03, and its book
    # recorder was retired 2026-08-02) — auditing them would fail forever, so
    # the rerun contract is the surviving inputs only.
    rerun_inputs = {
        "pmdata outcomes": legacy_first(root, "data/pmdata/pmdata_outcomes.parquet"),
        "Binance 1s cache": legacy_first(root, "cache/binance_1s"),
        "last-30s tick cache": legacy_first(root, "data/pmdata/poly_tick_last30.db"),
    }
    checks: list[dict[str, Any]] = []

    _add(checks, "live config", config_path.is_file(), str(config_path))
    _add(checks, "checkpoint manifest", manifest_path.is_file(), str(manifest_path))
    if not config_path.is_file() or not manifest_path.is_file():
        return {"passed": False, "checks": checks}

    config = json.loads(config_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    sleeves = {item["label"]: item["config"] for item in config.get("sleeves", [])}
    _add(
        checks,
        "live sleeve set",
        set(sleeves) == set(SLEEVE_CONTRACTS),
        f"found={sorted(sleeves)}",
    )
    for label, (interval, offsets, ask_lo, ask_hi, threshold) in SLEEVE_CONTRACTS.items():
        sleeve = sleeves.get(label, {})
        passed = (
            sleeve.get("interval") == interval
            and sleeve.get("checkpoint_offset_set") == offsets
            and sleeve.get("min_fill_price") == ask_lo
            and sleeve.get("max_fill_price") == ask_hi
            and sleeve.get("threshold") == threshold
            and sleeve.get("feature_groups") == "rf,price,identity,time"
        )
        _add(checks, f"sleeve {label}", passed, f"interval={interval}, offsets={offsets}")

    features = manifest.get("feature_columns", {})
    features_ok = all(features.get(interval) == FEATURES_11 for interval in ("5m", "15m"))
    _add(
        checks,
        "11-feature manifest",
        features_ok,
        f"5m={len(features.get('5m', []))}, 15m={len(features.get('15m', []))}",
    )
    _add(
        checks,
        "manifest sleeve contracts",
        set(manifest.get("sleeves", {})) == MANIFEST_KEYS,
        f"found={sorted(manifest.get('sleeves', {}))}",
    )

    for name in ("checkpoint_rf_v1.joblib", "checkpoint_hgb_v1.joblib"):
        path = model_dir / name
        _add(checks, name, path.is_file() and path.stat().st_size > 0, str(path))

    _add(checks, "11-feature truth", truth_path.is_file(), str(truth_path))
    if truth_path.is_file():
        columns = _parquet_columns(truth_path)
        missing = sorted(TRUTH_COLUMNS - columns)
        _add(checks, "truth schema", not missing, f"missing={missing}")

    pool_parts = list(pool_dir.glob("*.parquet")) if pool_dir.is_dir() else []
    _add(checks, "option3 pool parts", bool(pool_parts), f"parts={len(pool_parts)}")
    _add(checks, "400 ms tick report", tick_report_path.is_file(), str(tick_report_path))
    if tick_report_path.is_file():
        tick_report = json.loads(tick_report_path.read_text(encoding="utf-8"))
        folds = tick_report.get("folds", {})
        tick_ok = (
            tick_report.get("latency_ms") == 400
            and tick_report.get("scoring", {}).get("dynamic_held_side_rescore") is True
            and tick_report.get("execution", {}).get("stop_order") == "FOK full size"
            and all(fold in folds for fold in ("B", "C", "D", "E"))
        )
        detail = (
            f"latency_ms={tick_report.get('latency_ms')}, "
            f"dynamic_exit={tick_report.get('scoring', {}).get('dynamic_held_side_rescore')}"
        )
        _add(checks, "400 ms dynamic exit004 contract", tick_ok, detail)
    for name, path in rerun_inputs.items():
        populated = path.is_file() or (path.is_dir() and next(path.rglob("*"), None) is not None)
        _add(checks, name, populated, str(path))

    return {"passed": all(item["passed"] for item in checks), "checks": checks}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--json", type=Path, help="Write a generated report outside source control")
    args = parser.parse_args(argv)
    report = run_audit(args.root.resolve())
    for check in report["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        print(f"[{status}] {check['name']}: {check['detail']}")
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
