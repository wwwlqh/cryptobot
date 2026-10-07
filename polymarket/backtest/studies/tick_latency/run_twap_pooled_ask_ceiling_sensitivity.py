"""Replay the frozen pooled TWAP models under ask-ceiling and sizing changes.

This study never fits a model or selects a new edge gate.  It loads the exact
pooled 5m/15m estimators approved on 2026-08-14, keeps the shared timing and
``pred_edge >= 0.10`` contract fixed, and varies only ``ask_hi`` and the base
equity fraction.  Exact PMData L2 walking, FAK latency, and T3's $1 minimum
top-up are retained.
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
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.studies.tick_latency import (
    run_twap_pooled_fixed_entry_windows as pooled,
)


ASK_HIS = (0.85, 0.95)
BASE_FRACS = (0.02, 0.01)
EDGE_GATE = 0.10
ASK_LO = 0.55
WINDOWS = {"5m": 299, "15m": 300}
OUT_ROOT = pooled.OUT_ROOT / "ask_ceiling_sensitivity"
UNTOUCHED_ROOT = pooled.OUT_ROOT / "5m_untouched_post1005"
UNTOUCHED_START = pd.Timestamp("2026-08-14 10:05:00", tz="UTC")
UNTOUCHED_END = pd.Timestamp("2026-08-14 14:25:00", tz="UTC")


def _book_paths(study, *, start=None, end=None) -> dict[str, Path]:
    start = study.forward_start if start is None else start
    end = study.forward_end if end is None else end
    paths = {}
    for coin in pooled.COINS:
        paths.update({
            path.stem: path for path in core._book_paths(coin, start, end)
        })
    return paths


def _score(frame: pd.DataFrame, payload: dict) -> pd.DataFrame:
    scored = pooled._with_pooled_features(
        frame, payload["basis_center_log"]
    )
    scored["model_p"] = payload["model"].predict_proba(
        scored[pooled.MODEL_FEATURE_COLUMNS]
    )[:, 1]
    scored["pred_edge"] = (
        scored["model_p"]
        - scored["ask"]
        - ps.fee(scored["ask"].to_numpy(float))
    )
    return scored


def _selection_frame(study, payload: dict) -> tuple[pd.DataFrame, dict[str, Path]]:
    frames = []
    for coin in pooled.COINS:
        part = pd.read_parquet(
            study.feature_root / f"forward_features_{coin}.parquet"
        )
        part["coin"] = coin
        frames.append(part)
    return _score(pd.concat(frames, ignore_index=True), payload), _book_paths(study)


def _untouched_5m(payload: dict) -> tuple[pd.DataFrame, dict[str, Path]]:
    frame = pd.read_parquet(UNTOUCHED_ROOT / "forward_features.parquet")
    return _score(frame, payload), _book_paths(
        pooled.STUDIES["5m"], start=UNTOUCHED_START, end=UNTOUCHED_END
    )


def _replay(
    study,
    scored: pd.DataFrame,
    paths: dict[str, Path],
    *,
    ask_hi: float,
    base_frac: float,
    t3: bool,
) -> pd.DataFrame:
    window = pooled._window_rows(scored, study, WINDOWS[study.interval])
    slugs = set(window["market_slug"])
    selected_paths = {slug: path for slug, path in paths.items() if slug in slugs}
    if set(window["market_slug"].unique()) != set(selected_paths):
        raise ValueError(f"{study.interval} PMData path coverage is incomplete")
    old_spec = dict(exact.SPECS[study.interval])
    old_frac = exact.BASE_FRAC
    exact.SPECS[study.interval] = {
        "intervals": (study.interval,),
        "ask_lo": ASK_LO,
        "ask_hi": ask_hi,
        "gate": EDGE_GATE,
    }
    exact.BASE_FRAC = base_frac
    try:
        rows = exact._replay(
            window, selected_paths, tier_mult=1.0, t3=t3
        )
    finally:
        exact.SPECS[study.interval] = old_spec
        exact.BASE_FRAC = old_frac
    return pooled._attach_offsets(rows, window, study)


def _metrics(rows: pd.DataFrame) -> dict:
    return core._exact_metrics(rows)


def _one_setting(
    study,
    selection: pd.DataFrame,
    selection_paths: dict[str, Path],
    *,
    ask_hi: float,
    base_frac: float,
    untouched: pd.DataFrame | None = None,
    untouched_paths: dict[str, Path] | None = None,
) -> dict:
    modes = {}
    for mode, use_t3 in (("base", False), ("t3", True)):
        parts = [
            _replay(
                study, selection, selection_paths,
                ask_hi=ask_hi, base_frac=base_frac, t3=use_t3,
            )
        ]
        if untouched is not None and untouched_paths is not None:
            parts.append(
                _replay(
                    study, untouched, untouched_paths,
                    ask_hi=ask_hi, base_frac=base_frac, t3=use_t3,
                )
            )
        combined = pd.concat(parts, ignore_index=True)
        modes[mode] = _metrics(combined)
        modes[f"{mode}_by_coin"] = core._by(combined, "coin")
    return modes


def _delta(candidate: dict, reference: dict) -> dict:
    keys = (
        "fills", "wins", "win_rate", "pnl_usd", "ev_per_filled_trade_usd",
        "ev_per_dollar", "max_drawdown_usd", "deployed_usd", "topup_legs",
    )
    return {
        key: float(candidate[key]) - float(reference[key])
        for key in keys
        if candidate.get(key) is not None and reference.get(key) is not None
    }


def main() -> int:
    started = time.time()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    results = {}
    model_hashes = {}
    for interval in ("5m", "15m"):
        study = pooled.STUDIES[interval]
        pooled._configure(study)
        model_path = pooled.OUT_ROOT / interval / "pooled_hgb.joblib"
        payload = joblib.load(model_path)
        model_hashes[interval] = pooled._sha256(model_path)
        selection, paths = _selection_frame(study, payload)
        untouched = untouched_paths = None
        if interval == "5m":
            untouched, untouched_paths = _untouched_5m(payload)
        settings = {}
        for base_frac in BASE_FRACS:
            for ask_hi in ASK_HIS:
                label = f"ask_hi_{ask_hi:.2f}_base_{base_frac:.2f}"
                print(f"[{interval}] {label}", flush=True)
                settings[label] = _one_setting(
                    study,
                    selection,
                    paths,
                    ask_hi=ask_hi,
                    base_frac=base_frac,
                    untouched=untouched,
                    untouched_paths=untouched_paths,
                )
        reference = settings["ask_hi_0.85_base_0.02"]
        for label, setting in settings.items():
            setting["delta_vs_current_base"] = _delta(
                setting["base"], reference["base"]
            )
            setting["delta_vs_current_t3"] = _delta(
                setting["t3"], reference["t3"]
            )
        results[interval] = {
            "window_seconds": WINDOWS[interval],
            "period": (
                "selection+untouched" if interval == "5m"
                else f"{study.forward_start.isoformat()}/{study.forward_end.isoformat()}"
            ),
            "settings": settings,
        }
    report = {
        "artifact": "twap_pooled_ask_ceiling_sensitivity_20260814",
        "status": "research_only_not_deployed",
        "model_refits": 0,
        "gate_reselections": 0,
        "fixed_edge_gate": EDGE_GATE,
        "fixed_ask_lo": ASK_LO,
        "ask_hi_candidates": list(ASK_HIS),
        "base_fraction_candidates": list(BASE_FRACS),
        "model_sha256": model_hashes,
        "t3_note": (
            "T3 retains the live $1 minimum top-up; at 1% of a $100 bankroll "
            "the base entry is $1 and the top-up does not halve below $1."
        ),
        "intervals": results,
        "elapsed_s": round(time.time() - started, 2),
    }
    target = OUT_ROOT / "report.json"
    target.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        interval: {
            label: {
                "base": body["base"], "t3": body["t3"],
            }
            for label, body in result["settings"].items()
        }
        for interval, result in results.items()
    }, indent=2), flush=True)
    print(f"[done] {target}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
