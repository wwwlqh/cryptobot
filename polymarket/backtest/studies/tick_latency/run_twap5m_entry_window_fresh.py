"""Fresh 5m shared-entry-window comparison through 2026-08-14 10:00 UTC.

The timing comparison deliberately fits one full-window model per coin and
freezes its development-only WFO ask/edge specification before evaluating any
entry horizon.  Every horizon is common to BTC, ETH, SOL, and XRP; this runner
never selects a different time by coin.

The first scoreable instant is T-299 because partial-return z is undefined at
the exact opening tick (T-300, elapsed time zero).  T-299 therefore represents
the practical "trade the full last five minutes" candidate.

Fresh settlement labels come from PMData's resolved ``winning_outcome``.  The
opening anchor is the matching 60-second Binance TWAP proxy; the report also
records its direction agreement with the PMData resolution.  This is a
research-only experiment and does not edit live artifacts.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.studies.tick_latency import (
    run_twap15m60_refit_aug13 as core,
)
from polymarket.backtest.studies.tick_latency import (
    run_twap15m_entry_window_compare as shared,
)
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.training.offset_wfo import _load_1s


WINDOWS = (60, 90, 120, 150, 180, 210, 240, 270, 299)
WIDEST_WINDOW_S = 299
FORWARD_START = pd.Timestamp("2026-08-14 00:00:00", tz="UTC")
FORWARD_END = pd.Timestamp("2026-08-14 10:05:00", tz="UTC")
TRAIN_TAIL_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap5m60_refit_only_20260814"
)
OUT_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap5m_entry_windows_fresh_20260814"
)
FEATURE_ROOT = OUT_ROOT / "features_299s"


def _configure() -> None:
    core.INTERVAL = "5m"
    core.INTERVAL_MIN = 5
    core.INTERVAL_S = 300
    core.TWAP_S = 60
    core.MIN_TICKS = 50
    core.FORWARD_START = FORWARD_START
    core.FORWARD_END = FORWARD_END
    shared.OUT_ROOT = OUT_ROOT
    shared.FEATURE_ROOT = FEATURE_ROOT
    shared.WIDEST_WINDOW_S = WIDEST_WINDOW_S


def _window_rows(frame: pd.DataFrame, window_s: int) -> pd.DataFrame:
    lo = max(1, core.INTERVAL_S - window_s)
    out = frame[frame["offset_sec"] >= lo].copy()
    if out.empty or int(out["offset_sec"].min()) != lo:
        raise AssertionError(f"feature cache has no complete T-{window_s} window")
    if int(out["offset_sec"].max()) != core.INTERVAL_S - 1:
        raise AssertionError(f"feature cache does not end at T-1 for {window_s}s")
    return out


def _write_atomic(
    coin: str,
    kind: str,
    labels: dict[str, tuple[float, float]],
    spot: pd.DataFrame,
    paths: list[Path],
    target: Path,
) -> None:
    building = target.with_name(f"{target.name}.building")
    if building.exists():
        building.unlink()
    try:
        core._write_feature_cache(coin, kind, labels, spot, paths, building)
        building.replace(target)
    except BaseException:
        if building.exists():
            building.unlink()
        raise


def _assert_train_tail_parity(coin: str, wide: pd.DataFrame) -> None:
    reference_path = TRAIN_TAIL_ROOT / f"train_features_{coin}.parquet"
    if not reference_path.exists():
        raise FileNotFoundError(f"missing canonical 5m T-60 cache: {reference_path}")
    reference = pd.read_parquet(reference_path)
    tail = _window_rows(wide, 60)
    keys = ["market_slug", "offset_sec", "side_up"]
    reference = reference.sort_values(keys, kind="mergesort").reset_index(drop=True)
    tail = tail.sort_values(keys, kind="mergesort").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        tail[reference.columns],
        reference,
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )


def _load_or_build_train(coin: str, rebuild: bool) -> pd.DataFrame:
    target = FEATURE_ROOT / f"train_features_{coin}.parquet"
    if rebuild or not target.exists():
        paths = core._book_paths(coin, core.TRAIN_START[coin], core.TRAIN_END)
        labels = core._training_labels(coin, paths)
        spot = _load_1s(coin)
        prefix = target.with_name(f"{target.name}.prefix")
        core.OFFSETS = range(1, core.INTERVAL_S - 60)
        print(
            f"[features] historical {coin}: {len(paths):,} books, T-299..T-61",
            flush=True,
        )
        _write_atomic(coin, "train299_prefix", labels, spot, paths, prefix)
        baseline = TRAIN_TAIL_ROOT / f"train_features_{coin}.parquet"
        building = target.with_name(f"{target.name}.combining")
        try:
            combined = pd.concat(
                [pd.read_parquet(prefix), pd.read_parquet(baseline)],
                ignore_index=True,
            ).sort_values(
                ["market_slug", "offset_sec", "side_up"], kind="mergesort"
            )
            combined.to_parquet(building, index=False)
            building.replace(target)
        finally:
            if prefix.exists():
                prefix.unlink()
            if building.exists():
                building.unlink()
    wide = pd.read_parquet(target)
    _window_rows(wide, WIDEST_WINDOW_S)
    _assert_train_tail_parity(coin, wide)
    return wide


def _load_fresh_spot(coin: str) -> pd.DataFrame:
    historical = core._load_forward_spot(coin)
    pair = core.PAIRS[coin]
    fresh_path = (
        core.FORWARD_SPOT
        / f"{pair}_20260814T000000Z_20260814T100500Z.parquet"
    )
    if not fresh_path.exists():
        raise FileNotFoundError(f"missing fresh Binance tape: {fresh_path}")
    fresh = pd.read_parquet(fresh_path, columns=["sec", "open", "close"])
    out = pd.concat([historical, fresh], ignore_index=True)
    out[["open", "close"]] = out[["open", "close"]].astype(float)
    out["sec"] = out["sec"].astype(np.int64)
    return out.sort_values("sec").drop_duplicates("sec", keep="last").reset_index(
        drop=True
    )


def _resolved_won_up(path: Path) -> bool | None:
    frame = pd.read_parquet(path, columns=["winning_outcome"])
    values = frame["winning_outcome"].dropna().astype(str).str.strip().str.lower()
    if values.empty:
        return None
    value = values.iloc[-1]
    if value == "up":
        return True
    if value == "down":
        return False
    return None


def _fresh_labels(
    paths: list[Path], spot: pd.DataFrame
) -> tuple[dict[str, tuple[float, float]], dict]:
    sec = spot["sec"].to_numpy(np.int64)
    close = spot["close"].to_numpy(float)
    labels: dict[str, tuple[float, float]] = {}
    compared = matched = synthetic_up_n = actual_up_n = 0
    for path in paths:
        start = core._path_slug_ts(path)
        opening = core._trailing_binance_mean(sec, close, start)
        synthetic_close = core._trailing_binance_mean(
            sec, close, start + core.INTERVAL_S
        )
        actual_up = _resolved_won_up(path)
        if (
            actual_up is None
            or not np.isfinite(opening)
            or not np.isfinite(synthetic_close)
            or opening <= 0
            or synthetic_close <= 0
        ):
            continue
        synthetic_up = bool(synthetic_close >= opening)
        compared += 1
        matched += int(synthetic_up == actual_up)
        synthetic_up_n += int(synthetic_up)
        actual_up_n += int(actual_up)
        # _market_rows only uses chainlink_close to construct win_true.  Keep
        # the 60s opening anchor while stamping the exact PMData resolution.
        resolved_close = opening * (1.0 + (1e-12 if actual_up else -1e-12))
        labels[path.stem] = (opening, resolved_close)
    return labels, {
        "markets_compared": compared,
        "direction_matches": matched,
        "direction_agreement": matched / compared if compared else None,
        "synthetic_60s_up": synthetic_up_n,
        "pmdata_resolved_up": actual_up_n,
    }


def _load_or_build_forward(
    coin: str, rebuild: bool
) -> tuple[pd.DataFrame, list[Path], dict]:
    paths = core._book_paths(coin, FORWARD_START, FORWARD_END)
    spot = _load_fresh_spot(coin)
    labels, agreement = _fresh_labels(paths, spot)
    target = FEATURE_ROOT / f"forward_features_{coin}.parquet"
    if rebuild or not target.exists():
        core.OFFSETS = range(1, core.INTERVAL_S)
        print(
            f"[features] fresh {coin}: {len(paths):,} books, "
            f"{len(labels):,} resolved labels, T-299..T-1",
            flush=True,
        )
        _write_atomic(coin, "forward299", labels, spot, paths, target)
    wide = pd.read_parquet(target)
    _window_rows(wide, WIDEST_WINDOW_S)
    return wide, paths, agreement


def _fit_and_score(
    coin: str, *, rebuild: bool
) -> tuple[pd.DataFrame, list[Path], dict, pd.DataFrame, dict]:
    raw = _load_or_build_train(coin, rebuild)
    forward, paths, agreement = _load_or_build_forward(coin, rebuild)
    report, model, center, oos = core._wfo(coin, raw)
    spec = report["selected"]
    joblib.dump(
        {
            "coin": coin,
            "interval": "5m",
            "entry_window_seconds": WIDEST_WINDOW_S,
            "target_twap_seconds": core.TWAP_S,
            "feature_columns": core.FEATURE_COLUMNS,
            "basis_center_log": center,
            "basis_clip_log": core.BASIS_CLIP_LOG,
            "spec": {key: spec[key] for key in ("ask_lo", "ask_hi", "gate")},
            "model": model,
        },
        OUT_ROOT / f"model_{coin}.joblib",
    )
    scored = core._score(model, forward, center)
    oos_trades = core._prefilled_trades(
        oos, spec["ask_lo"], spec["ask_hi"], spec["gate"]
    )
    oos_trades.to_parquet(OUT_ROOT / f"wfo_trades_{coin}.parquet", index=False)
    report["forward_feature_rows_full_window"] = int(len(scored))
    report["forward_markets_with_features"] = int(scored["market_slug"].nunique())
    del raw, forward, model, oos
    return scored, paths, report, oos_trades, agreement


def _replay_window(
    window_s: int,
    *,
    coins: tuple[str, ...],
    scored_by_coin: dict[str, pd.DataFrame],
    paths_by_coin: dict[str, list[Path]],
    development: dict[str, dict],
) -> dict:
    root = OUT_ROOT / f"window_{window_s}s"
    root.mkdir(parents=True, exist_ok=True)
    all_base = []
    all_t3 = []
    for coin in coins:
        scored = _window_rows(scored_by_coin[coin], window_s)
        slugs = set(scored["market_slug"])
        paths = {path.stem: path for path in paths_by_coin[coin] if path.stem in slugs}
        spec = development[coin]["selected"]
        old_spec = dict(exact.SPECS["5m"])
        exact.SPECS["5m"] = {
            "intervals": ("5m",),
            "ask_lo": float(spec["ask_lo"]),
            "ask_hi": float(spec["ask_hi"]),
            "gate": float(spec["gate"]),
        }
        try:
            base = exact._replay(
                scored, paths, tier_mult=1.0, t3=False, coin_filter={coin}
            )
            t3 = exact._replay(
                scored, paths, tier_mult=1.0, t3=True, coin_filter={coin}
            )
        finally:
            exact.SPECS["5m"] = old_spec
        base = shared._attach_entry_offsets(base, scored)
        t3 = shared._attach_entry_offsets(t3, scored)
        all_base.append(base)
        all_t3.append(t3)
        metrics = core._exact_metrics(base)
        print(
            f"[forward:{window_s}s:{coin}] fills={metrics['fills']} "
            f"WR={metrics['win_rate'] if metrics['win_rate'] is not None else float('nan'):.3f} "
            f"PnL=${metrics['pnl_usd']:.2f}",
            flush=True,
        )
    base = pd.concat(all_base, ignore_index=True)
    t3 = pd.concat(all_t3, ignore_index=True)
    base.to_parquet(root / "forward_base_trades.parquet", index=False)
    t3.to_parquet(root / "forward_t3_trades.parquet", index=False)
    return {
        "decision_window": f"T-{window_s} through T-1 inclusive",
        "model_selection": (
            "same per-coin T-299 model and development-only WFO spec reused "
            "for every entry horizon"
        ),
        "selected_by_coin": {
            coin: development[coin]["selected"] for coin in coins
        },
        "forward": {
            "base_all": core._exact_metrics(base),
            "t3_all": core._exact_metrics(t3),
            "base_by_coin": core._by(base, "coin"),
            "t3_by_coin": core._by(t3, "coin"),
            "base_entry_bands": shared._entry_band_metrics(base),
            "t3_entry_bands": shared._entry_band_metrics(t3),
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--coins", nargs="+", choices=core.COINS, default=list(core.COINS)
    )
    parser.add_argument("--windows", nargs="+", type=int, default=list(WINDOWS))
    parser.add_argument("--rebuild-features", action="store_true")
    parser.add_argument(
        "--reuse-models",
        action="store_true",
        help="reuse the completed full-window models/specs for timing-only replays",
    )
    args = parser.parse_args(argv)
    coins = tuple(args.coins)
    windows = tuple(sorted(set(args.windows)))
    invalid = [value for value in windows if value < 60 or value > WIDEST_WINDOW_S]
    if invalid:
        parser.error(
            f"--windows must be between 60 and {WIDEST_WINDOW_S}; got "
            + ", ".join(map(str, invalid))
        )
    _configure()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    FEATURE_ROOT.mkdir(parents=True, exist_ok=True)
    started = time.time()
    target = OUT_ROOT / "report.json"
    prior_report = None
    if target.exists():
        prior_report = json.loads(target.read_text(encoding="utf-8"))
        if prior_report.get("coins") != list(coins):
            prior_report = None

    scored_by_coin = {}
    paths_by_coin = {}
    development = {}
    agreements = {}
    all_oos = []
    if args.reuse_models:
        if prior_report is None:
            parser.error("--reuse-models requires a completed matching report")
        development = prior_report["development"]
        agreements = prior_report["synthetic_60s_direction_vs_pmdata_resolution"]
        for coin in coins:
            artifact = joblib.load(OUT_ROOT / f"model_{coin}.joblib")
            forward = pd.read_parquet(
                FEATURE_ROOT / f"forward_features_{coin}.parquet"
            )
            scored_by_coin[coin] = core._score(
                artifact["model"], forward, float(artifact["basis_center_log"])
            )
            paths_by_coin[coin] = core._book_paths(coin, FORWARD_START, FORWARD_END)
            all_oos.append(
                pd.read_parquet(OUT_ROOT / f"wfo_trades_{coin}.parquet")
            )
    else:
        for coin in coins:
            scored, paths, coin_report, oos_trades, agreement = _fit_and_score(
                coin, rebuild=args.rebuild_features
            )
            scored_by_coin[coin] = scored
            paths_by_coin[coin] = paths
            development[coin] = coin_report
            agreements[coin] = agreement
            all_oos.append(oos_trades)

    new_window_reports = {
        f"{window_s}s": _replay_window(
            window_s,
            coins=coins,
            scored_by_coin=scored_by_coin,
            paths_by_coin=paths_by_coin,
            development=development,
        )
        for window_s in windows
    }
    oos = pd.concat(all_oos, ignore_index=True)
    for body in new_window_reports.values():
        body["development_wfo_selected_all"] = core._trade_metrics(oos)
        body["minimum_coin_fold_ev_per_dollar"] = min(
            float(development[coin]["selected"]["min_fold_ev_per_dollar"])
            for coin in coins
        )
    window_reports = {}
    if prior_report is not None:
        window_reports.update(prior_report.get("windows", {}))
    window_reports.update(new_window_reports)
    report = {
        "artifact": "twap5m_entry_windows_fresh_20260814",
        "status": "research_only_not_deployed",
        "question": "5m market shared entry horizon from T-60 through T-299",
        "target_contract": "synthetic 60-second TWAP opening anchor",
        "settlement_label": "PMData resolved winning_outcome",
        "development_end_exclusive": core.TRAIN_END.isoformat(),
        "fresh_forward_period": "2026-08-14T00:00:00Z/2026-08-14T10:05:00Z",
        "fresh_last_market_start": "2026-08-14T10:00:00Z",
        "coins": list(coins),
        "shared_horizon_only": True,
        "first_scoreable_second": "T-299 (T-300 has zero elapsed variance)",
        "model_policy": (
            "one per-coin full-window model/spec frozen before comparing horizons"
        ),
        "synthetic_60s_direction_vs_pmdata_resolution": agreements,
        "development": development,
        "windows": window_reports,
        "comparison": shared._comparison_table(window_reports),
        "elapsed_s": round(time.time() - started, 2),
    }
    target.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["comparison"], indent=2), flush=True)
    print(f"[done] {target}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
