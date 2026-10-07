"""Package the two production pooled 60-second-TWAP HGB models.

There is exactly one fixed estimator per interval.  BTC, ETH, SOL, and XRP are
pooled during training and ``coin_id`` is categorical.  The selected live
horizon is T-150 for both intervals, shared by all four coins.
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

from polymarket.backtest import REPO_ROOT

OUT_DEFAULT = REPO_ROOT / "data" / "models" / "twap_symmetric_v1"
SOURCE_ROOT = (
    REPO_ROOT / "polymarket" / "backtest" / "optimize_runs"
    / "twap_pooled_fixed_entry_windows_20260814"
)
FIVE_MINUTE_VALIDATION = SOURCE_ROOT / "5m_untouched_post1005"
EXECUTION_SENSITIVITY = SOURCE_ROOT / "ask_ceiling_sensitivity"
ENTRY_WINDOW_VALIDATION = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap_entry_windows_append_20260815"
    / "report.json"
)
COIN_IDS = {"btc": 0, "eth": 1, "sol": 2, "xrp": 3}
LIVE_COINS = tuple(COIN_IDS)
DECISION_WINDOWS = {"5m": 150, "15m": 150}
CHECKPOINT_OFFSET_SETS = {"5m": "150:299:1", "15m": "750:899:1"}
FEATURE_COLUMNS = [
    "ask", "other_ask", "book_spread", "coin_id", "interval_min",
    "side_up", "entry_minute", "hour_utc", "dow_utc",
    "partial_ret_z_side", "abs_partial_ret_z",
]
HYPERPARAMETERS = {
    "max_iter": 100,
    "learning_rate": 0.04,
    "max_leaf_nodes": 31,
    "l2_regularization": 0.02,
    "categorical_features": ["coin_id"],
    "random_state": 42,
}


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"required TWAP refit source missing: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"TWAP refit source is not an object: {path}")
    return value


def _validate_estimator(model, *, label: str) -> None:
    if int(getattr(model, "n_features_in_", 0)) != len(FEATURE_COLUMNS):
        raise ValueError(f"{label} model has the wrong feature count")
    if list(getattr(model, "feature_names_in_", [])) != FEATURE_COLUMNS:
        raise ValueError(f"{label} model has the wrong feature order")
    categorical = getattr(model, "is_categorical_", None)
    if (
        categorical is None
        or not bool(categorical[FEATURE_COLUMNS.index("coin_id")])
    ):
        raise ValueError(f"{label} model does not treat coin_id as categorical")


def _load_pooled(
    source: Path, interval: str
) -> tuple[object, dict, dict[str, float], dict, dict]:
    report = _read_json(source / "report.json")
    if report.get("artifact") != "twap_pooled_fixed_entry_windows_20260814":
        raise ValueError("pooled shared-horizon report has the wrong artifact id")
    if tuple(report.get("coins", ())) != LIVE_COINS:
        raise ValueError("pooled source does not contain all four coins")
    if report.get("pooled_across_coins") is not True:
        raise ValueError("source is not pooled across coins")
    body = (report.get("intervals") or {}).get(interval) or {}
    if body.get("model_routing") != "one pooled estimator across BTC/ETH/SOL/XRP":
        raise ValueError(f"{interval} source is not one pooled estimator")
    if body.get("refits_during_timing_comparison") != 0:
        raise ValueError(f"{interval} timing comparison refit the model")
    if body.get("gate_reselections_during_timing_comparison") != 0:
        raise ValueError(f"{interval} timing comparison reselected the gate")
    selected_seconds = DECISION_WINDOWS[interval]
    selected = (body.get("windows") or {}).get(f"{selected_seconds}s") or {}
    if selected.get("decision_window") != (
        f"T-{selected_seconds} through T-1 inclusive"
    ):
        raise ValueError(f"{interval} source lacks the frozen live horizon")

    payload = joblib.load(source / interval / "pooled_hgb.joblib")
    if not isinstance(payload, dict) or payload.get("interval") != interval:
        raise ValueError(f"{interval} pooled model payload is wrong")
    if payload.get("pooled_across_coins") is not True:
        raise ValueError(f"{interval} model is not pooled")
    if tuple(payload.get("coins", ())) != LIVE_COINS:
        raise ValueError(f"{interval} model coin universe is wrong")
    expected_fit_window = 299 if interval == "5m" else 600
    if payload.get("fixed_model_window_seconds") != expected_fit_window:
        raise ValueError(f"{interval} fixed model window is wrong")
    if payload.get("target_twap_seconds") != 60:
        raise ValueError(f"{interval} target is not 60 seconds")
    if list(payload.get("feature_columns") or []) != FEATURE_COLUMNS:
        raise ValueError(f"{interval} feature order is wrong")
    if payload.get("categorical_features") != ["coin_id"]:
        raise ValueError(f"{interval} coin_id is not categorical")
    model = payload.get("model")
    _validate_estimator(model, label=interval)
    centres = {
        coin: float(value)
        for coin, value in (payload.get("basis_center_log") or {}).items()
    }
    if set(centres) != set(LIVE_COINS):
        raise ValueError(f"{interval} basis centres are incomplete")
    execution = {
        key: float(payload["execution"][key])
        for key in ("ask_lo", "ask_hi", "gate", "book_consumption_max")
    }
    reported_execution = {
        key: float(body["execution"][key])
        for key in ("ask_lo", "ask_hi", "gate", "book_consumption_max")
    }
    if execution != reported_execution:
        raise ValueError(f"{interval} shared execution contract drifted")
    return model, body, centres, execution, payload


def _probe_models(models: dict[str, object]) -> None:
    checked = 0
    for coin_id in COIN_IDS.values():
        for interval, interval_min, entry_minute in (
            ("5m", 5.0, 4.5),
            ("15m", 15.0, 14.5),
        ):
            frame = pd.DataFrame(
                [[0.70, 0.31, 0.01, coin_id, interval_min, 1.0,
                  entry_minute, 12.0, 3.0, 0.5, 0.5]],
                columns=FEATURE_COLUMNS,
            )
            probability = float(models[interval].predict_proba(frame)[0, 1])
            if not np.isfinite(probability) or not 0.0 <= probability <= 1.0:
                raise RuntimeError(f"non-finite production probe: {interval}:{coin_id}")
            checked += 1
    if checked != 8:
        raise AssertionError("production probe did not cover every route")


def _training_summary(payloads: dict[str, dict]) -> dict:
    return {
        interval: {
            "sampled_rows": int(payload["training"]["sampled_rows"]),
            "raw_rows_by_coin": payload["training"]["raw_rows_by_coin"],
            "sampled_rows_by_coin": payload["training"]["sampled_rows_by_coin"],
            "start": str(payload["training"]["common_train_start"]),
            "end": str(payload["training"]["train_end_exclusive"]),
            "training_stride_seconds": int(
                payload["training"]["training_stride_seconds"]
            ),
            "equal_total_weight_per_coin": bool(
                payload["training"]["equal_total_weight_per_coin"]
            ),
        }
        for interval, payload in payloads.items()
    }


def _validation_summary(sensitivity: dict) -> dict:
    intervals = sensitivity["intervals"]
    five = intervals["5m"]["settings"]["ask_hi_0.85_base_0.01"]
    fifteen = intervals["15m"]["settings"]["ask_hi_0.85_base_0.01"]
    five_reference = intervals["5m"]["settings"]["ask_hi_0.85_base_0.02"]
    fifteen_reference = intervals["15m"]["settings"]["ask_hi_0.85_base_0.02"]
    return {
        "5m": {
            "period": "2026-08-14T00:00:00+00:00/2026-08-14T14:25:00+00:00",
            "markets": int(five["base"]["markets"]),
            "base": five["base"],
            "t3": five["t3"],
            "reference_2pct_base": five_reference["base"],
            "reference_2pct_t3": five_reference["t3"],
            "settlement_label": "PMData resolved winning_outcome",
            "caveat": (
                "Historical model/gate packaging check at T-299. The current "
                "T-150 timing evidence is pinned in entry_window_validation. "
                "Ask-hi 0.95 added zero fills and zero PnL versus 0.85."
            ),
        },
        "15m": {
            "period": "2026-08-07T00:00:00+00:00/2026-08-14T00:00:00+00:00",
            "markets": int(fifteen["base"]["markets"]),
            "base": fifteen["base"],
            "t3": fifteen["t3"],
            "reference_2pct_base": fifteen_reference["base"],
            "reference_2pct_t3": fifteen_reference["t3"],
            "caveat": (
                "Historical model/gate packaging check at T-300. The current "
                "T-150 timing evidence is pinned in entry_window_validation. "
                "Ask-hi 0.95 added zero fills and zero PnL versus 0.85."
            ),
        },
    }


def _entry_window_validation_summary(report: dict, model_sha256: str) -> dict:
    if report.get("artifact") != "twap_entry_windows_append_20260815":
        raise ValueError("entry-window validation has the wrong artifact id")
    if report.get("model_refits") != 0:
        raise ValueError("entry-window validation refit the frozen model")
    if report.get("model_sha256") != model_sha256:
        raise ValueError("entry-window validation used a different model")
    if report.get("pooled_across_all_four_coins") is not True:
        raise ValueError("entry-window validation is not pooled across four coins")
    if report.get("shared_window_across_all_four_coins") is not True:
        raise ValueError("entry-window validation used per-coin timing")

    results = {}
    for interval in ("5m", "15m"):
        interval_body = report["intervals"][interval]
        selected = interval_body["windows"][f"{DECISION_WINDOWS[interval]}s"]
        combined = selected["combined"]
        old_period = interval_body["old_period"]
        today_period = interval_body["today_period"]
        results[interval] = {
            "period": (
                f"{old_period['start']}/{today_period['end_exclusive']}"
            ),
            "trades": int(combined["trades"]),
            "wins": int(combined["wins"]),
            "win_rate": float(combined["win_rate"]),
            "pnl_usd": float(combined["pnl_usd"]),
            "ev_per_trade_usd": float(combined["ev_per_trade_usd"]),
            "max_drawdown_usd": float(combined["max_drawdown_usd"]),
            "appended_period_pnl_usd": float(selected["today"]["pnl_usd"]),
        }
    return {
        "artifact": report["artifact"],
        "model_refits": 0,
        "model_sha256": model_sha256,
        "pooled_across_all_four_coins": True,
        "shared_window_across_all_four_coins": True,
        "selected_window_seconds": dict(DECISION_WINDOWS),
        "topups_enabled": False,
        "results": results,
    }


def build_production_model(
    out_dir: Path | str = OUT_DEFAULT,
    *,
    source_root: Path | str = SOURCE_ROOT,
    five_minute_validation: Path | str = FIVE_MINUTE_VALIDATION,
    execution_sensitivity: Path | str = EXECUTION_SENSITIVITY,
    entry_window_validation: Path | str = ENTRY_WINDOW_VALIDATION,
) -> dict:
    """Package exactly two pooled estimators and a fail-closed manifest."""
    started = time.time()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    source = Path(source_root)
    model_5m, report_5m, centres_5m, execution_5m, payload_5m = _load_pooled(
        source, "5m"
    )
    model_15m, report_15m, centres_15m, execution_15m, payload_15m = _load_pooled(
        source, "15m"
    )
    untouched_5m = _read_json(Path(five_minute_validation) / "report.json")
    if untouched_5m.get("artifact") != "twap5m_pooled_untouched_post1005_20260814":
        raise ValueError("5m untouched validation has the wrong artifact id")
    if untouched_5m.get("refits") != 0 or untouched_5m.get("gate_reselections") != 0:
        raise ValueError("5m untouched validation changed the frozen strategy")
    if untouched_5m.get("model_sha256") != _sha256(
        source / "5m" / "pooled_hgb.joblib"
    ):
        raise ValueError("5m untouched validation used a different pooled model")
    sensitivity = _read_json(Path(execution_sensitivity) / "report.json")
    if sensitivity.get("artifact") != "twap_pooled_ask_ceiling_sensitivity_20260814":
        raise ValueError("execution sensitivity has the wrong artifact id")
    if sensitivity.get("model_refits") != 0 or sensitivity.get("gate_reselections") != 0:
        raise ValueError("execution sensitivity changed the frozen strategy")
    if sensitivity.get("model_sha256") != {
        interval: _sha256(source / interval / "pooled_hgb.joblib")
        for interval in ("5m", "15m")
    }:
        raise ValueError("execution sensitivity used different pooled models")
    models = {"5m": model_5m, "15m": model_15m}
    _probe_models(models)

    model_path = out / "twap_hgb_v1.joblib"
    joblib.dump(models, model_path)
    model_sha256 = _sha256(model_path)
    entry_windows = _read_json(Path(entry_window_validation))
    training = _training_summary({"5m": payload_5m, "15m": payload_15m})
    manifest = {
        "artifact": "twap_symmetric_v1",
        "contract_version": 3,
        "target": "win_twap_s",
        "historical_target": "ordinary Chainlink 1s 60-second arithmetic TWAP",
        "model_family": "HistGradientBoostingClassifier",
        "model_count": 2,
        "model_routing": {"5m": "pooled_all4", "15m": "pooled_all4"},
        "categorical_features": {"5m": ["coin_id"], "15m": ["coin_id"]},
        "feature_columns": {
            "5m": list(FEATURE_COLUMNS),
            "15m": list(FEATURE_COLUMNS),
        },
        "chainlink_window_seconds": {"5m": 60, "15m": 60},
        "decision_window_seconds": dict(DECISION_WINDOWS),
        "live_feature_contract": {
            "name": "binance_trailing_twap_centered_chainlink_basis_v2",
            "binance_window_seconds": {"5m": 60, "15m": 60},
            "basis_residual_clip_log": 0.0001,
            "basis_center_log": {
                coin: {"5m": centres_5m[coin], "15m": centres_15m[coin]}
                for coin in LIVE_COINS
            },
            "basis_center_fit_period": {
                coin: {
                    interval: training[interval]["start"]
                    + "/"
                    + training[interval]["end"]
                    for interval in ("5m", "15m")
                }
                for coin in LIVE_COINS
            },
            "causal_window": "[boundary-60,boundary)",
        },
        "sleeves": {
            "5m": {
                "threshold": 0.10,
                "ask_bucket": "band:0.55:0.85",
                "side_filter": "both",
                "book_consumption_max": 0.06,
                "checkpoint_offset_set": CHECKPOINT_OFFSET_SETS["5m"],
            },
            "15m": {
                "threshold": 0.10,
                "ask_bucket": "band:0.55:0.85",
                "side_filter": "both",
                "book_consumption_max": 0.06,
                "checkpoint_offset_set": CHECKPOINT_OFFSET_SETS["15m"],
            },
        },
        "execution_by_interval": {
            "5m": execution_5m,
            "15m": execution_15m,
        },
        "training_coins": list(LIVE_COINS),
        "live_coins": list(LIVE_COINS),
        "intervals": ["5m", "15m"],
        "train_rows": int(
            training["5m"]["sampled_rows"] + training["15m"]["sampled_rows"]
        ),
        "training": training,
        "min_entry_dt_utc": min(body["start"] for body in training.values()),
        "max_entry_dt_utc": max(body["end"] for body in training.values()),
        "hyperparameters": HYPERPARAMETERS,
        "selection_protocol": (
            "Fit one pooled all-four-coin HGB per interval before timing. Freeze "
            "the shared 0.55<ask<=0.85 and fee-adjusted edge>=0.10 execution "
            "contract. Compare one predeclared coarse shared-cutoff grid with "
            "zero model refits and zero gate reselections."
        ),
        "selected_contract": (
            "BTC+ETH+SOL+XRP pooled within interval; identical timing across "
            "coins and intervals; T-150 through T-1; 60s "
            "Chainlink/Binance opening contract; shared 0.55<ask<=0.85 and "
            "fee-adjusted pred_edge>=0.10; one base fill; 1% stake; top-ups "
            "and T3 off; Tier A off"
        ),
        "validation": _validation_summary(sensitivity),
        "entry_window_validation": _entry_window_validation_summary(
            entry_windows, model_sha256
        ),
        "source_artifacts": {
            "pooled_study": str(source.resolve()),
            "5m_untouched": str(Path(five_minute_validation).resolve()),
            "execution_sensitivity": str(Path(execution_sensitivity).resolve()),
            "entry_window_validation": str(Path(entry_window_validation).resolve()),
        },
        "model_sha256": model_sha256,
        "built_ts": int(time.time()),
        "elapsed_s": round(time.time() - started, 2),
    }
    manifest_path = out / "twap_symmetric_v1_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT_DEFAULT)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT)
    parser.add_argument(
        "--five-minute-validation", type=Path, default=FIVE_MINUTE_VALIDATION
    )
    parser.add_argument(
        "--execution-sensitivity", type=Path, default=EXECUTION_SENSITIVITY
    )
    parser.add_argument(
        "--entry-window-validation", type=Path, default=ENTRY_WINDOW_VALIDATION
    )
    args = parser.parse_args()
    manifest = build_production_model(
        args.out,
        source_root=args.source_root,
        five_minute_validation=args.five_minute_validation,
        execution_sensitivity=args.execution_sensitivity,
        entry_window_validation=args.entry_window_validation,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
