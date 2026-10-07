"""Full-history walk-forward refit for the reconstructed TWAP labels.

Research-only driver. It deliberately keeps the live model and configs
untouched. The development sample is split into four chronological chunks
(A/B/C/D) using the repository's ``abcd_all_history`` convention. B, C, and D
are scored out of sample with expanding training windows and a one-hour
embargo. Native pool fold F is held out as a final forward period.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT
from polymarket.backtest.lib import pool_study as ps  # noqa: E402
from polymarket.backtest.training import modeling as ev  # noqa: E402
from polymarket.backtest.training import checkpoint as checkpoint_training  # noqa: E402
from polymarket.backtest.training import offset_wfo as offset_training  # noqa: E402


OUT = Path(os.environ.get("TWAP_WFO_OUT", str(REPO_ROOT / "TWAP_WFO_FULL_20260812.json")))
EMBARGO_S = 3600
MIN_FOLD_BETS = 100
SEED = 42
LIVE_FAK_LIMIT_PAD = 0.01

BASE_FEATURES = [
    "ask", "other_ask", "book_spread", "coin_id", "interval_min", "side_up",
    "entry_minute", "hour_utc", "dow_utc",
]
FEATURES = BASE_FEATURES + ["z", "abs_z"]


def _side_win_target(up_label: pd.Series, side_up: pd.Series) -> pd.Series:
    """Convert a market-level Up label to a proposed-side win label."""
    out = pd.Series(np.nan, index=up_label.index, dtype=float)
    valid = up_label.notna() & side_up.notna()
    out.loc[valid] = (
        up_label.loc[valid].astype(np.int8)
        == side_up.loc[valid].astype(np.int8)
    ).astype(np.int8)
    return out


def _attach_causal_partial_ret_z(pool: pd.DataFrame) -> pd.DataFrame:
    """Recompute the pool's z feature with RV observable at window open.

    The retained option3 pool predates the causal-RV correction. Rebuilding the
    full pool requires retired PMData snapshots, so derive the corrected RV
    from the retained Binance 1-second caches and overwrite only this feature.
    """
    frames: list[pd.DataFrame] = []
    for coin, sub in pool.groupby("coin", observed=True, sort=False):
        spot = offset_training._load_1s(str(coin))
        rv = checkpoint_training._minute_rv_from_1s(spot)
        grid = pd.to_datetime(sub["slug_ts"].astype(np.int64), unit="s", utc=True)
        rv_open = rv.reindex(grid).to_numpy(dtype=np.float64)
        denom = rv_open * np.sqrt(sub["entry_minute"].to_numpy(dtype=np.float64))
        out = sub.copy()
        entry_ret = out["entry_ret"].to_numpy(dtype=np.float64)
        out["partial_ret_z_1h"] = np.where(
            (denom > 0.0) & np.isfinite(denom), entry_ret / denom, np.nan
        )
        frames.append(out)
    return pd.concat(frames, ignore_index=True) if frames else pool.copy()


def _load_side_frame() -> pd.DataFrame:
    labels_path = (
        BT_DATA_DIR / "pmdata"
        / "twap_labels_trailing_proxy.parquet"
    )
    labels = pd.read_parquet(
        labels_path,
        columns=[
            "market_slug", "coin", "interval", "open_point", "open_twap",
            "up_twap_c", "up_twap_s",
        ],
    )
    pool = ps.load_pool(grids=("step1last",), holdout_visible=True)
    pool = pool.merge(labels, on=["market_slug", "coin", "interval"], how="inner")
    pool = pool[pool["fold"].isin(["C", "D", "E", "F"])].copy()
    pool = _attach_causal_partial_ret_z(pool)
    sf = ps.side_frame(pool)

    # ``side_frame`` contains one Up and one Down row per candidate.  TWAP
    # labels are market-level Up labels, so convert them to the side-specific
    # target the live scorer predicts.  Using ``up_twap_*`` directly on both
    # rows grades a Down bet as a win when Up wins (and was the defect in the
    # preliminary 2026-08-12 run).
    sf["win_twap_c"] = _side_win_target(sf["up_twap_c"], sf["side_up"])
    sf["win_twap_s"] = _side_win_target(sf["up_twap_s"], sf["side_up"])

    # The existing point-open z is entry_ret / (rv_1h * sqrt(entry_minute)).
    # Re-anchor only the opening price to the saved opening TWAP. Close labels
    # are not used to form this feature.
    point_z = sf["partial_ret_z_1h"].astype(float)
    point_ret = sf["entry_ret"].astype(float)
    denom = point_ret / point_z.replace(0.0, np.nan)
    open_shift = np.log(
        sf["open_point"].astype(float) / sf["open_twap"].astype(float)
    )
    sym_z_up = point_z + open_shift / denom
    side_sign = np.where(sf["side_up"].astype(np.int8) == 1, 1.0, -1.0)
    # Match live ``build_feature_vector``: momentum is signed for the proposed
    # side, while abs_z is side-invariant.
    sf["sym_z"] = (sym_z_up * side_sign).where(np.isfinite(sym_z_up))
    sf["point_z"] = (point_z * side_sign).where(np.isfinite(point_z))

    dt = pd.to_datetime(sf["entry_ts"], unit="s", utc=True)
    sf["coin_id"] = sf["coin"].map(ps.COIN_MAP).astype(float)
    sf["interval_min"] = sf["interval"].map({"5m": 5, "15m": 15}).astype(float)
    sf["hour_utc"] = dt.dt.hour.astype(float)
    sf["dow_utc"] = dt.dt.dayofweek.astype(float)
    return sf


def _fit_and_score(train: pd.DataFrame, test: pd.DataFrame, z_col: str, target_col: str) -> pd.DataFrame:
    train = train.dropna(subset=BASE_FEATURES + [z_col, target_col]).copy()
    test = test.dropna(subset=BASE_FEATURES + [z_col, target_col]).copy()
    if train.empty or test.empty:
        return test.assign(model_p=np.nan, pred_edge=np.nan, win_true=np.nan)
    for frame in (train, test):
        frame["z"] = frame[z_col].astype(float)
        frame["abs_z"] = frame[z_col].abs().astype(float)
    model = HistGradientBoostingClassifier(
        max_iter=100,
        learning_rate=0.04,
        max_leaf_nodes=31,
        l2_regularization=0.02,
        random_state=SEED,
    )
    model.fit(train[FEATURES], train[target_col].astype(np.int8))
    out = test.copy()
    out["model_p"] = model.predict_proba(test[FEATURES])[:, 1]
    out["pred_edge"] = out["model_p"] - out["ask"] - ps.fee(out["ask"].to_numpy(float))
    out["win_true"] = out[target_col].astype(np.int8)
    return out


def _pnl_metrics(bets: pd.DataFrame) -> dict[str, float | int]:
    if bets.empty:
        return {"n": 0, "wr": float("nan"), "ev": float("nan"), "pnl": 0.0}
    p = bets["side_fill_ask"].to_numpy(float)
    w = bets["win_true"].to_numpy(float)
    pnl = w * (1.0 / p - 1.0) - (1.0 - w) - ps.fee(p)
    return {
        "n": int(len(bets)),
        "wr": float(w.mean()),
        "ev": float(pnl.mean()),
        "pnl": float(pnl.sum()),
    }


def _build_live_bets(scored: pd.DataFrame, spec: dict) -> pd.DataFrame:
    """Replay the current live best-side + consume-on-fill decision order.

    ``score_cell_hgb`` first drops sides outside the open-low/closed-high ask
    band, then chooses the higher-edge executable side at each second.  With
    ``CRYPTOBOT_V2_REJECT_RETRY=1`` a gate failure or rejected/unreachable FAK
    does not consume the market; the next second is eligible.  The first
    reachable, gate-passing best side is therefore the replayed fill.
    """
    d = scored[
        scored["interval"].isin(tuple(spec["intervals"]))
        & (scored["ask"] > float(spec["ask_lo"]))
        & (scored["ask"] <= float(spec["ask_hi"]))
    ].copy()
    off = d["offset_sec"] - np.where(d["interval"] == "5m", 270, 870)
    d = d[(off >= 0) & (off <= 29)]
    if d.empty:
        return d

    # Best side at this market-second, matching score_cell_hgb's max(edge).
    d = d.sort_values(
        ["market_slug", "entry_ts", "pred_edge"],
        ascending=[True, True, False],
        kind="mergesort",
    ).groupby(["market_slug", "entry_ts"], as_index=False).first()
    # The pool's legacy ``reachable`` flag permits up to a $0.05 move. Live
    # sends a FAK capped at decision ask + $0.01, so require the delayed fill
    # book's top ask to remain inside that exact deployed leash.
    d = d[
        d["reachable"]
        & d["side_fill_ask"].notna()
        & (d["side_fill_ask"] <= d["ask"] + LIVE_FAK_LIMIT_PAD + 1e-12)
        & (d["pred_edge"] >= float(spec["gate"]))
    ]
    if d.empty:
        return d
    # First fillable qualifying second consumes the market resolution.
    return d.sort_values(
        ["market_slug", "entry_ts"], kind="mergesort"
    ).groupby("market_slug", as_index=False).first()


def _fold_windows(dev: pd.DataFrame) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    # This is the same all-history ABCD construction covered by the modeling
    # tests. It is computed after TWAP-label filtering, so every chunk contains
    # rows that can actually be scored for this target.
    return ev._folds_for_frame("abcd_all_history", dev, embargo_seconds=EMBARGO_S)


def _assign_chunk(
    dev: pd.DataFrame,
    ts: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    return dev[(ts >= start) & (ts < end)].copy()


def _score_fold_grid(scored: dict[str, pd.DataFrame], specs: list[dict]) -> list[dict]:
    results: list[dict] = []
    for spec in specs:
        per_fold = []
        for fold, frame in scored.items():
            bet_spec = {
                "intervals": tuple(spec["intervals"]),
                "ask_lo": spec["ask_lo"],
                "ask_hi": spec["ask_hi"],
                "gate": spec["gate"],
                "reachable_only": True,
                "off_lo": 0,
                "off_hi": 29,
            }
            bets = _build_live_bets(frame, bet_spec)
            metrics = _pnl_metrics(bets)
            metrics["fold"] = fold
            per_fold.append(metrics)
        valid = all(
            int(x["n"]) >= MIN_FOLD_BETS and np.isfinite(float(x["ev"])) and float(x["ev"]) > 0
            for x in per_fold
        )
        total_n = sum(int(x["n"]) for x in per_fold)
        total_pnl = sum(float(x["pnl"]) for x in per_fold)
        results.append({
            **spec,
            "valid_all_wfo_folds": bool(valid),
            "wfo_n": total_n,
            "wfo_ev": total_pnl / total_n if total_n else float("nan"),
            "wfo_min_fold_ev": min(float(x["ev"]) for x in per_fold),
            "wfo_folds": per_fold,
        })
    return results


def main() -> None:
    sf = _load_side_frame()
    labels_path = (
        BT_DATA_DIR / "pmdata"
        / "twap_labels_trailing_proxy.parquet"
    )
    label_max_start = pd.to_datetime(
        pd.read_parquet(labels_path, columns=["w_start"])["w_start"].max(),
        unit="s",
        utc=True,
    )
    # Development uses all labeled pool data before native F. F is never used
    # to choose an interval, band, or gate.
    dev = sf[sf["fold"].isin(["C", "D", "E"])].copy()
    holdout = sf[sf["fold"] == "F"].copy()
    dev_ts = pd.to_datetime(dev["entry_ts"], unit="s", utc=True)
    windows = _fold_windows(dev)
    if [x[0] for x in windows] != ["B", "C", "D"]:
        raise RuntimeError(f"unexpected ABCD windows: {windows}")

    specs = []
    for target, z_col in (("twap_c", "point_z"), ("twap_s", "sym_z")):
        target_col = "win_twap_c" if target == "twap_c" else "win_twap_s"
        for intervals in (("5m",), ("15m",), ("5m", "15m")):
            for ask_lo, ask_hi in ((0.60, 0.85), (0.65, 0.85), (0.70, 0.90)):
                for gate in (0.02, 0.03, 0.04, 0.05, 0.06):
                    specs.append({
                        "target": target,
                        "z_col": z_col,
                        "target_col": target_col,
                        "intervals": intervals,
                        "ask_lo": ask_lo,
                        "ask_hi": ask_hi,
                        "gate": gate,
                    })

    scored_by_target: dict[str, dict[str, pd.DataFrame]] = {"twap_c": {}, "twap_s": {}}
    for target, z_col, target_col in (
        ("twap_c", "point_z", "win_twap_c"),
        ("twap_s", "sym_z", "win_twap_s"),
    ):
        for fold, train_start, train_end, test_start, test_end in windows:
            train = _assign_chunk(dev, dev_ts, train_start, train_end)
            test = _assign_chunk(dev, dev_ts, test_start, test_end)
            scored_by_target[target][fold] = _fit_and_score(train, test, z_col, target_col)

    grid_results = []
    for target in ("twap_c", "twap_s"):
        grid_results.extend(_score_fold_grid(scored_by_target[target], [s for s in specs if s["target"] == target]))

    valid = [x for x in grid_results if x["valid_all_wfo_folds"]]
    if not valid:
        raise RuntimeError("no strategy passed the all-B/C/D validity gate")
    best = max(valid, key=lambda x: (float(x["wfo_min_fold_ev"]), float(x["wfo_ev"]), int(x["wfo_n"])))

    # Final F is scored once using all development rows, with the same embargo
    # immediately before the F start. This is not used for selection.
    f_start = pd.to_datetime(holdout["entry_ts"].min(), unit="s", utc=True)
    f_train = dev[dev["entry_ts"] < int(f_start.timestamp()) - EMBARGO_S].copy()
    target = str(best["target"])
    z_col = str(best["z_col"])
    target_col = str(best["target_col"])
    f_scored = _fit_and_score(f_train, holdout, z_col, target_col)
    f_scored_by_target = {target: f_scored}
    f_bets = _build_live_bets(
        f_scored,
        {
            "intervals": tuple(best["intervals"]),
            "ask_lo": best["ask_lo"],
            "ask_hi": best["ask_hi"],
            "gate": best["gate"],
            "reachable_only": True,
            "off_lo": 0,
            "off_hi": 29,
        },
    )
    f_metrics = _pnl_metrics(f_bets)
    best["final_f_holdout"] = f_metrics
    best["final_f_valid"] = bool(
        f_metrics["n"] >= MIN_FOLD_BETS and np.isfinite(float(f_metrics["ev"])) and f_metrics["ev"] > 0
    )

    by_coin = {}
    for coin, sub in f_bets.groupby("coin", observed=True):
        by_coin[str(coin)] = _pnl_metrics(sub)
    best["final_f_by_coin"] = by_coin

    out = {
        "data": {
            "labels_path": str(
                BT_DATA_DIR / "pmdata"
                / "twap_labels_trailing_proxy.parquet"
            ),
            "label_max_start_utc": str(label_max_start),
            "pool_max_entry_utc": str(pd.to_datetime(sf["entry_ts"].max(), unit="s", utc=True)),
            "development_rows": int(len(dev)),
            "final_f_rows": int(len(holdout)),
            "development_unique_markets": int(dev["market_slug"].nunique()),
            "final_f_unique_markets": int(holdout["market_slug"].nunique()),
        },
        "protocol": {
            "development_chunks": [
                {
                    "name": name,
                    "train_start": str(train_start),
                    "train_end": str(train_end),
                    "test_start": str(test_start),
                    "test_end": str(test_end),
                }
                for name, train_start, train_end, test_start, test_end in windows
            ],
            "embargo_seconds": EMBARGO_S,
            "min_bets_per_wfo_fold": MIN_FOLD_BETS,
            "selection_rule": "all B/C/D folds n>=100 and EV>0; maximize minimum fold EV, then pooled WFO EV",
            "final_holdout": "native pool fold F, never used for selection",
        },
        "best": best,
        "all_valid_candidates": [x for x in grid_results if x["valid_all_wfo_folds"]],
        "candidate_count": len(grid_results),
    }
    valid_by_target = {}
    for target_name in ("twap_c", "twap_s"):
        target_valid = [
            x for x in out["all_valid_candidates"] if x["target"] == target_name
        ]
        if target_valid:
            valid_by_target[target_name] = max(
                target_valid,
                key=lambda x: (float(x["wfo_min_fold_ev"]), float(x["wfo_ev"]), int(x["wfo_n"])),
            )

    # Report the best TWAP-symmetric candidate for each interval shape and
    # score each one once on untouched F.  The global selection above remains
    # unchanged; this is a deployment diagnostic answering whether a sleeve
    # lost outright or merely ranked behind another sleeve.
    best_twap_s_by_interval = {}
    for interval_key in (("5m",), ("15m",), ("5m", "15m")):
        candidates = [
            x for x in out["all_valid_candidates"]
            if x["target"] == "twap_s" and tuple(x["intervals"]) == interval_key
        ]
        if not candidates:
            continue
        candidate = dict(max(
            candidates,
            key=lambda x: (float(x["wfo_min_fold_ev"]), float(x["wfo_ev"]), int(x["wfo_n"])),
        ))
        interval_scored = f_scored_by_target.get("twap_s")
        if interval_scored is None:
            interval_scored = _fit_and_score(
                f_train, holdout, "sym_z", "win_twap_s"
            )
            f_scored_by_target["twap_s"] = interval_scored
        interval_bets = _build_live_bets(interval_scored, candidate)
        candidate["final_f_holdout"] = _pnl_metrics(interval_bets)
        candidate["final_f_by_coin"] = {
            str(coin): _pnl_metrics(sub)
            for coin, sub in interval_bets.groupby("coin", observed=True)
        }
        best_twap_s_by_interval["+".join(interval_key)] = candidate
    summary = {
        "out": str(OUT),
        "protocol": out["protocol"],
        "data": out["data"],
        "candidate_count": out["candidate_count"],
        "valid_candidate_count": len(out["all_valid_candidates"]),
        "best": best,
        "best_by_target": valid_by_target,
        "best_twap_s_by_interval": best_twap_s_by_interval,
    }
    # Emit the result before attempting persistence so the calculation remains
    # recoverable on locked/synced workspaces. The report is patched in by the
    # caller when direct Python writes are unavailable.
    print(json.dumps(summary, indent=2, default=str))
    try:
        OUT.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    except PermissionError:
        pass


if __name__ == "__main__":
    main()
