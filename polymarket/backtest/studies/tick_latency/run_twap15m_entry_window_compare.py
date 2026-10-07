"""Compare 15m TWAP entry windows from 60 through 600 seconds.

The canonical 60-second refit is treated as the frozen baseline.  Every wider
candidate receives a leak-free per-coin HGB refit and development-only WFO
specification selection before the exact Aug 7-13 holdout is replayed.  A
single T-600 feature cache is shared by all candidates.

Each tested horizon is common to BTC, ETH, SOL, and XRP.  This runner never
selects a separate entry time by coin.

This is a research-only experiment.  It does not edit or export live artifacts.
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
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.training.offset_wfo import _load_1s


WINDOWS = tuple(range(90, 601, 30))
WIDEST_WINDOW_S = max(WINDOWS)
BASELINE_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap15m60_refit_20260814"
)
OUT_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap15m_entry_windows_20260814"
)
PRIOR_FEATURE_ROOT = OUT_ROOT / "features_300s"
FEATURE_ROOT = OUT_ROOT / "features_600s"


def _window_rows(frame: pd.DataFrame, window_s: int) -> pd.DataFrame:
    lo = core.INTERVAL_S - window_s
    out = frame[frame["offset_sec"] >= lo].copy()
    if out.empty or int(out["offset_sec"].min()) != lo:
        raise AssertionError(f"feature cache has no complete T-{window_s} window")
    if int(out["offset_sec"].max()) != core.INTERVAL_S - 1:
        raise AssertionError(f"feature cache does not end at T-1 for {window_s}s")
    return out


def _assert_tail_parity(
    coin: str,
    wide: pd.DataFrame,
    kind: str,
    *,
    reference_root: Path,
    window_s: int,
) -> None:
    reference_path = reference_root / f"{kind}_features_{coin}.parquet"
    if not reference_path.exists():
        raise FileNotFoundError(f"reference cache missing: {reference_path}")
    reference = pd.read_parquet(reference_path)
    tail = _window_rows(wide, window_s)
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


def _write_feature_cache_atomic(
    coin: str,
    kind: str,
    labels: dict[str, tuple[float, float]],
    spot: pd.DataFrame,
    paths: list[Path],
    target: Path,
) -> None:
    """Build beside the final cache so an interrupted run cannot look complete."""
    building = target.with_name(f"{target.name}.building")
    if building.exists():
        building.unlink()
    try:
        core._write_feature_cache(
            coin, kind, labels, spot, paths, building
        )
        building.replace(target)
    except BaseException:
        if building.exists():
            building.unlink()
        raise


def _extend_feature_cache(
    coin: str,
    kind: str,
    labels: dict[str, tuple[float, float]],
    spot: pd.DataFrame,
    paths: list[Path],
    target: Path,
) -> None:
    """Prepend T-600..T-301 rows to the validated T-300 cache."""
    prior = PRIOR_FEATURE_ROOT / f"{kind}_features_{coin}.parquet"
    if not prior.exists():
        core.OFFSETS = range(core.INTERVAL_S - WIDEST_WINDOW_S, core.INTERVAL_S)
        _write_feature_cache_atomic(coin, kind, labels, spot, paths, target)
        return

    prefix = target.with_name(f"{target.name}.prefix")
    core.OFFSETS = range(
        core.INTERVAL_S - WIDEST_WINDOW_S,
        core.INTERVAL_S - 300,
    )
    _write_feature_cache_atomic(
        coin, f"{kind}{WIDEST_WINDOW_S}_prefix", labels, spot, paths, prefix
    )
    building = target.with_name(f"{target.name}.combining")
    try:
        combined = pd.concat(
            [pd.read_parquet(prefix), pd.read_parquet(prior)],
            ignore_index=True,
        ).sort_values(
            ["market_slug", "offset_sec", "side_up"], kind="mergesort"
        )
        combined.to_parquet(building, index=False)
        building.replace(target)
    except BaseException:
        if building.exists():
            building.unlink()
        raise
    finally:
        if prefix.exists():
            prefix.unlink()


def _load_or_build_wide_features(
    coin: str,
    *,
    gamma: pd.DataFrame,
    rebuild: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, list[Path]]:
    train_cache = FEATURE_ROOT / f"train_features_{coin}.parquet"
    forward_cache = FEATURE_ROOT / f"forward_features_{coin}.parquet"
    train_paths = core._book_paths(coin, core.TRAIN_START[coin], core.TRAIN_END)
    forward_paths = core._book_paths(coin, core.FORWARD_START, core.FORWARD_END)

    if rebuild or not train_cache.exists():
        print(
            f"[features] historical {coin}: {len(train_paths):,} books, "
            f"T-{WIDEST_WINDOW_S}..T-1",
            flush=True,
        )
        _extend_feature_cache(
            coin,
            "train",
            core._training_labels(coin, train_paths),
            _load_1s(coin),
            train_paths,
            train_cache,
        )
    if rebuild or not forward_cache.exists():
        print(
            f"[features] exact forward {coin}: {len(forward_paths):,} books, "
            f"T-{WIDEST_WINDOW_S}..T-1",
            flush=True,
        )
        _extend_feature_cache(
            coin,
            "forward",
            core._forward_labels(coin, gamma),
            core._load_forward_spot(coin),
            forward_paths,
            forward_cache,
        )

    train = pd.read_parquet(train_cache)
    forward = pd.read_parquet(forward_cache)
    _window_rows(train, WIDEST_WINDOW_S)
    _window_rows(forward, WIDEST_WINDOW_S)
    for kind, frame in (("train", train), ("forward", forward)):
        _assert_tail_parity(
            coin,
            frame,
            kind,
            reference_root=BASELINE_ROOT,
            window_s=60,
        )
        _assert_tail_parity(
            coin,
            frame,
            kind,
            reference_root=PRIOR_FEATURE_ROOT,
            window_s=300,
        )
    return train, forward, forward_paths


def _attach_entry_offsets(rows: pd.DataFrame, scored: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    starts = (
        scored[["market_slug", "slug_ts"]]
        .drop_duplicates("market_slug")
        .set_index("market_slug")["slug_ts"]
        .to_dict()
    )
    out["offset_sec"] = out.apply(
        lambda row: (
            int(row["entry_ts"]) - int(starts[row["market_slug"]])
            if bool(row["entry_filled"])
            else np.nan
        ),
        axis=1,
    )
    out["seconds_to_settle"] = np.where(
        out["entry_filled"], core.INTERVAL_S - out["offset_sec"], np.nan
    )
    return out


def _entry_band_metrics(rows: pd.DataFrame) -> dict:
    filled = rows[rows["entry_filled"]].copy()
    bands = tuple(
        (f"T-{hi}_to_T-{max(1, hi - 29)}", max(1, hi - 29), hi)
        for hi in range(WIDEST_WINDOW_S, 0, -30)
    )
    out = {}
    for label, lo, hi in bands:
        part = filled[
            (filled["seconds_to_settle"] >= lo)
            & (filled["seconds_to_settle"] <= hi)
        ]
        deployed = float(part["deployed"].sum()) if len(part) else 0.0
        pnl = float(part["pnl"].sum()) if len(part) else 0.0
        out[label] = {
            "fills": int(len(part)),
            "wins": int(part["win"].sum()) if len(part) else 0,
            "win_rate": float(part["win"].mean()) if len(part) else None,
            "deployed_usd": deployed,
            "pnl_usd": pnl,
            "ev_per_dollar": pnl / deployed if deployed else None,
        }
    return out


def _baseline_summary(coins: tuple[str, ...]) -> dict:
    report_path = BASELINE_ROOT / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    oos = pd.concat(
        [pd.read_parquet(BASELINE_ROOT / f"wfo_trades_{coin}.parquet") for coin in coins],
        ignore_index=True,
    )
    base_rows = pd.read_parquet(BASELINE_ROOT / "forward_base_trades.parquet")
    t3_rows = pd.read_parquet(BASELINE_ROOT / "forward_t3_trades.parquet")
    # Baseline files contain all four coins.  Keep subset runs honest.
    base_rows = base_rows[base_rows["coin"].isin(coins)].copy()
    t3_rows = t3_rows[t3_rows["coin"].isin(coins)].copy()
    starts = {
        slug: int(str(slug).rsplit("-", 1)[1])
        for slug in base_rows["market_slug"].unique()
    }
    for rows in (base_rows, t3_rows):
        rows["offset_sec"] = rows.apply(
            lambda row: (
                int(row["entry_ts"]) - starts[row["market_slug"]]
                if bool(row["entry_filled"])
                else np.nan
            ),
            axis=1,
        )
        rows["seconds_to_settle"] = np.where(
            rows["entry_filled"], core.INTERVAL_S - rows["offset_sec"], np.nan
        )
    selected = {
        coin: report["coins"][coin]["selected"] for coin in coins
    }
    return {
        "source_report": str(report_path.relative_to(REPO_ROOT)),
        "decision_window": "T-60 through T-1 inclusive",
        "development_wfo_selected_all": core._trade_metrics(oos),
        "minimum_coin_fold_ev_per_dollar": min(
            float(spec["min_fold_ev_per_dollar"]) for spec in selected.values()
        ),
        "selected_by_coin": selected,
        "forward": {
            "base_all": core._exact_metrics(base_rows),
            "t3_all": core._exact_metrics(t3_rows),
            "base_by_coin": core._by(base_rows, "coin"),
            "t3_by_coin": core._by(t3_rows, "coin"),
            "base_entry_bands": _entry_band_metrics(base_rows),
            "t3_entry_bands": _entry_band_metrics(t3_rows),
        },
    }


def _run_window(
    window_s: int,
    *,
    coins: tuple[str, ...],
    caches: dict[str, tuple[pd.DataFrame, pd.DataFrame, list[Path]]],
) -> dict:
    window_root = OUT_ROOT / f"window_{window_s}s"
    window_root.mkdir(parents=True, exist_ok=True)
    report = {
        "decision_window": f"T-{window_s} through T-1 inclusive",
        "model_selection": (
            "per-coin expanding chronological WFO; one-hour embargo; "
            "ask band and edge gate selected on development folds only"
        ),
        "coins": {},
    }
    all_oos = []
    all_base = []
    all_t3 = []

    for coin in coins:
        wide_train, wide_forward, forward_paths = caches[coin]
        raw = _window_rows(wide_train, window_s)
        forward = _window_rows(wide_forward, window_s)
        coin_report, model, center, oos = core._wfo(coin, raw)
        spec = coin_report["selected"]
        joblib.dump(
            {
                "coin": coin,
                "interval": core.INTERVAL,
                "entry_window_seconds": window_s,
                "feature_columns": core.FEATURE_COLUMNS,
                "basis_center_log": center,
                "basis_clip_log": core.BASIS_CLIP_LOG,
                "spec": {key: spec[key] for key in ("ask_lo", "ask_hi", "gate")},
                "model": model,
            },
            window_root / f"model_{coin}.joblib",
        )
        oos_trades = core._prefilled_trades(
            oos, spec["ask_lo"], spec["ask_hi"], spec["gate"]
        )
        oos_trades.to_parquet(
            window_root / f"wfo_trades_{coin}.parquet", index=False
        )
        all_oos.append(oos_trades)

        scored = core._score(model, forward, center)
        slugs = set(scored["market_slug"])
        paths = {path.stem: path for path in forward_paths if path.stem in slugs}
        old_spec = dict(exact.SPECS[core.INTERVAL])
        exact.SPECS[core.INTERVAL] = {
            "intervals": (core.INTERVAL,),
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
            exact.SPECS[core.INTERVAL] = old_spec
        base = _attach_entry_offsets(base, scored)
        t3 = _attach_entry_offsets(t3, scored)
        all_base.append(base)
        all_t3.append(t3)
        coin_report["forward_feature_rows"] = int(len(scored))
        coin_report["forward_markets_with_features"] = int(
            scored["market_slug"].nunique()
        )
        coin_report["forward_base"] = core._exact_metrics(base)
        coin_report["forward_t3"] = core._exact_metrics(t3)
        report["coins"][coin] = coin_report
        print(
            f"[forward:{window_s}s:{coin}] "
            f"fills={coin_report['forward_base']['fills']} "
            f"WR={coin_report['forward_base']['win_rate']:.3f} "
            f"PnL=${coin_report['forward_base']['pnl_usd']:.2f}",
            flush=True,
        )

    base = pd.concat(all_base, ignore_index=True)
    t3 = pd.concat(all_t3, ignore_index=True)
    oos = pd.concat(all_oos, ignore_index=True)
    base.to_parquet(window_root / "forward_base_trades.parquet", index=False)
    t3.to_parquet(window_root / "forward_t3_trades.parquet", index=False)
    report["development_wfo_selected_all"] = core._trade_metrics(oos)
    report["minimum_coin_fold_ev_per_dollar"] = min(
        float(body["selected"]["min_fold_ev_per_dollar"])
        for body in report["coins"].values()
    )
    report["selected_by_coin"] = {
        coin: body["selected"] for coin, body in report["coins"].items()
    }
    report["forward"] = {
        "base_all": core._exact_metrics(base),
        "t3_all": core._exact_metrics(t3),
        "base_by_coin": core._by(base, "coin"),
        "t3_by_coin": core._by(t3, "coin"),
        "base_by_day": core._by(
            base.assign(
                day=pd.to_datetime(base["entry_ts"], unit="s", utc=True).dt.strftime(
                    "%Y-%m-%d"
                )
            ),
            "day",
        ),
        "t3_by_day": core._by(
            t3.assign(
                day=pd.to_datetime(t3["entry_ts"], unit="s", utc=True).dt.strftime(
                    "%Y-%m-%d"
                )
            ),
            "day",
        ),
        "base_entry_bands": _entry_band_metrics(base),
        "t3_entry_bands": _entry_band_metrics(t3),
    }
    return report


def _comparison_table(windows: dict[str, dict]) -> list[dict]:
    rows = []
    for label in sorted(windows, key=lambda value: int(value.removesuffix("s"))):
        body = windows[label]
        base = body["forward"]["base_all"]
        t3 = body["forward"]["t3_all"]
        rows.append(
            {
                "window": label,
                "development_wfo_ev_per_dollar": body[
                    "development_wfo_selected_all"
                ]["ev_per_dollar"],
                "minimum_coin_fold_ev_per_dollar": body[
                    "minimum_coin_fold_ev_per_dollar"
                ],
                "forward_base_fills": base["fills"],
                "forward_base_win_rate": base["win_rate"],
                "forward_base_pnl_usd": base["pnl_usd"],
                "forward_base_ev_per_trade_usd": base[
                    "ev_per_filled_trade_usd"
                ],
                "forward_base_ev_per_dollar": base["ev_per_dollar"],
                "forward_base_max_drawdown_usd": base["max_drawdown_usd"],
                "forward_t3_pnl_usd": t3["pnl_usd"],
                "forward_t3_ev_per_trade_usd": t3[
                    "ev_per_filled_trade_usd"
                ],
                "forward_t3_ev_per_dollar": t3["ev_per_dollar"],
                "forward_t3_max_drawdown_usd": t3["max_drawdown_usd"],
            }
        )
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--coins", nargs="+", choices=core.COINS, default=list(core.COINS)
    )
    parser.add_argument(
        "--windows", nargs="+", type=int, default=list(WINDOWS)
    )
    parser.add_argument("--rebuild-features", action="store_true")
    args = parser.parse_args(argv)
    coins = tuple(args.coins)
    windows = tuple(sorted(set(args.windows)))
    invalid_windows = [value for value in windows if not 60 <= value <= 600]
    if invalid_windows:
        parser.error(
            "--windows must be between 60 and 600 seconds; got "
            + ", ".join(map(str, invalid_windows))
        )
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    FEATURE_ROOT.mkdir(parents=True, exist_ok=True)
    started = time.time()

    gamma = pd.read_parquet(core.GAMMA)
    gamma = gamma[
        (gamma["slug_ts"] >= core._epoch(core.FORWARD_START))
        & (gamma["slug_ts"] < core._epoch(core.FORWARD_END))
        & gamma["price_to_beat"].notna()
        & gamma["final_price"].notna()
    ].copy()
    caches = {
        coin: _load_or_build_wide_features(
            coin, gamma=gamma, rebuild=args.rebuild_features
        )
        for coin in coins
    }

    target = OUT_ROOT / "report.json"
    prior_windows = {}
    if target.exists():
        prior_report = json.loads(target.read_text(encoding="utf-8"))
        if prior_report.get("coins") == list(coins):
            prior_windows = prior_report.get("windows", {})
    results = dict(prior_windows)
    results["60s"] = _baseline_summary(coins)
    for window_s in windows:
        print(f"[window] fitting and replaying {window_s}s", flush=True)
        results[f"{window_s}s"] = _run_window(
            window_s, coins=coins, caches=caches
        )

    report = {
        "artifact": "twap15m_entry_windows_20260814",
        "status": "research_only_not_deployed",
        "question": "15m entry horizon from T-60 through T-600 in 30s steps",
        "target_contract": "60-second Chainlink TWAP at both boundaries",
        "development_end_exclusive": core.TRAIN_END.isoformat(),
        "exact_forward_period": "2026-08-07/2026-08-13",
        "coins": list(coins),
        "feature_cache_tail_parity": (
            "T-300 rows in every 600s cache matched the validated 300s cache; "
            "T-60 rows also matched the canonical 60s cache"
        ),
        "windows": results,
        "comparison": _comparison_table(results),
        "elapsed_s": round(time.time() - started, 2),
    }
    target.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["comparison"], indent=2))
    print(f"[done] {target}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
