"""Replay the frozen TWAP-60 release over its first live base-only period.

The bounded production ledger/signal exports are read-only research inputs.
PMData books and the deployed model bundle remain external, gitignored inputs;
this runner never fits a model.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT
from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.studies.tick_latency.run_twap_t3_tiera_holdout import (
    LATENCY_S,
    LIVE_FAK_LIMIT_PAD,
    _walk_limit,
)


DEFAULT_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap_live_apples_20260815"
)
DEFAULT_SIGNALS = DEFAULT_ROOT / "twap_signals_20260815.csv.gz"
DEFAULT_LEDGER = DEFAULT_ROOT / "twap_ledger_20260815.csv.gz"
DEFAULT_START = "2026-08-14 16:26:15"
DEFAULT_END = "2026-08-14 22:30:00"
SNAP = BT_DATA_DIR / "pmdata" / "poly_snapshot"
RELEASE_PATH = (
    REPO_ROOT / "polymarket" / "live" / "config" / "twap_symmetric_release.json"
)
MODEL_PATH = (
    REPO_ROOT / "data" / "models" / "twap_symmetric_v1" / "twap_hgb_v1.joblib"
)
PRIOR_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap_pooled_fixed_entry_windows_20260814"
)


def _load_live(
    signals_path: Path, ledger_path: Path, start: str, end: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    signals = pd.read_csv(signals_path)
    ledger = pd.read_csv(ledger_path)
    for frame in (signals, ledger):
        frame["created_at"] = pd.to_datetime(frame["created_at"], utc=True)
    lo, hi = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")
    signals = signals[
        (signals["created_at"] >= lo) & (signals["created_at"] < hi)
    ].copy()
    ledger = ledger[
        (ledger["created_at"] >= lo) & (ledger["created_at"] < hi)
    ].copy()
    return signals, ledger


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _interval(sleeve: str) -> str:
    return "15m" if sleeve.endswith("15m") else "5m"


def _book_path(row) -> Path:
    interval = _interval(str(row.sleeve))
    coin = str(row.coin).lower()
    slug = f"{coin}-updown-{interval}-{int(row.window_start_ts)}"
    return SNAP / coin / interval / f"{slug}.parquet"


def _resolved_up(frame: pd.DataFrame) -> bool | None:
    values = frame["winning_outcome"].dropna().astype(str).str.lower().str.strip()
    if values.empty:
        return None
    value = values.iloc[-1]
    if value == "up":
        return True
    if value == "down":
        return False
    return None


def _max_drawdown(pnl: pd.Series) -> float:
    cumulative = pnl.fillna(0.0).cumsum()
    peak = cumulative.cummax().clip(lower=0.0)
    return float((peak - cumulative).max()) if len(cumulative) else 0.0


def _metrics(frame: pd.DataFrame, *, pnl_col: str, win_col: str) -> dict:
    filled = frame[pnl_col].notna()
    sample = frame.loc[filled]
    wins = int(sample[win_col].sum())
    pnl = float(sample[pnl_col].sum())
    return {
        "trades": int(len(sample)),
        "wins": wins,
        "win_rate": float(wins / len(sample)) if len(sample) else None,
        "pnl_usd": pnl,
        "ev_per_trade_usd": float(pnl / len(sample)) if len(sample) else None,
        "max_drawdown_usd": _max_drawdown(sample[pnl_col]),
    }


def _group_metrics(frame: pd.DataFrame, column: str, *, pnl_col: str, win_col: str) -> dict:
    return {
        str(key): _metrics(part, pnl_col=pnl_col, win_col=win_col)
        for key, part in frame.groupby(column, sort=True)
    }


def _poisson_binomial_cdf(wins: int, probabilities: list[float]) -> float:
    distribution = np.array([1.0])
    for probability in probabilities:
        distribution = np.convolve(distribution, [1.0 - probability, probability])
    return float(distribution[: wins + 1].sum())


def _replay(signals: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    decisions = signals[signals["outcome"] == "WOULD_BET"].copy()
    decisions["interval"] = decisions["sleeve"].map(_interval)
    if decisions.duplicated(["coin", "sleeve", "window_start_ts"]).any():
        raise AssertionError("more than one live WOULD_BET per market")
    if ledger.duplicated(["coin", "sleeve", "window_start_ts"]).any():
        raise AssertionError("more than one live ledger row per market")
    joined = decisions.merge(
        ledger,
        on=["coin", "sleeve", "window_start_ts"],
        suffixes=("_signal", "_live"),
        validate="one_to_one",
    ).sort_values(["created_at_live", "id_live"], kind="mergesort")
    if len(joined) != len(decisions) or len(joined) != len(ledger):
        raise AssertionError(
            f"decision/ledger mismatch decisions={len(decisions)} "
            f"ledger={len(ledger)} joined={len(joined)}"
        )
    rows = []

    @lru_cache(maxsize=256)
    def load_book(path_text: str) -> tuple[te.TickBook, bool | None]:
        frame = pd.read_parquet(Path(path_text))
        return te.TickBook.from_frame(frame), _resolved_up(frame)

    for row in joined.itertuples(index=False):
        path = _book_path(row)
        if not path.exists():
            raise FileNotFoundError(path)
        book, resolved_up = load_book(str(path))
        side_text = str(row.side_signal).upper()
        book_side = "UP" if side_text in {"YES", "UP"} else "DOWN"
        decision_ts = (
            int(row.window_start_ts)
            + int(row.offset_sec_signal)
            + te.DECISION_ANCHOR_SECONDS
        )
        decision_i = book.index_at(decision_ts)
        execution_i = book.index_at(decision_ts + LATENCY_S)
        requested = float(row.target_usd)
        walked = None
        candidate = None
        pm_top_ask = None
        if decision_i is not None:
            decision_asks = book.ask_levels(decision_i, book_side)
            if decision_asks:
                pm_top_ask = float(decision_asks[0][0])
                walked = _walk_limit(decision_asks, requested)
        if walked is not None and execution_i is not None:
            candidate = te.fak_buy(
                book.ask_levels(execution_i, book_side),
                requested,
                min(float(walked) + LIVE_FAK_LIMIT_PAD, 0.99),
            )
        pm_win = None
        pm_pnl = None
        pm_vwap = None
        if candidate is not None and resolved_up is not None:
            pm_win = bool(resolved_up) == (book_side == "UP")
            pm_vwap = float(candidate.vwap)
            pm_pnl = float(
                candidate.shares * int(pm_win)
                - candidate.cash_usd
                - te.entry_fee_usd(candidate.vwap, candidate.shares)
            )
        live_win = str(row.outcome_live).upper() == "WIN"
        rows.append(
            {
                "market_slug": path.stem,
                "coin": row.coin,
                "sleeve": row.sleeve,
                "interval": row.interval,
                "window_start_ts": int(row.window_start_ts),
                "offset_sec": int(row.offset_sec_signal),
                "entry_ts": int(row.window_start_ts) + int(row.offset_sec_signal),
                "side": side_text,
                "model_p": float(row.model_p_signal),
                "signal_ask": float(row.ask),
                "signal_edge": float(row.edge),
                "live_fill_price": float(row.walked_vwap),
                "live_stake": float(row.bet_size),
                "live_win": live_win,
                "live_pnl": float(row.pnl_net),
                "pmdata_top_ask": pm_top_ask,
                "pmdata_walked": None if walked is None else float(walked),
                "pmdata_fill": candidate is not None,
                "pmdata_fill_vwap": pm_vwap,
                "pmdata_resolved_up": resolved_up,
                "pmdata_win": pm_win,
                "pmdata_pnl": pm_pnl,
                "outcome_match": None if pm_win is None else bool(pm_win == live_win),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["entry_ts", "market_slug"], kind="mergesort"
    ).reset_index(drop=True)


def _calibration(frame: pd.DataFrame) -> dict:
    probability = frame["model_p"].to_numpy(float)
    truth = frame["live_win"].to_numpy(float)
    bins = pd.cut(
        frame["model_p"], bins=[0.0, 0.65, 0.70, 0.75, 0.80, 0.85, 1.0],
        include_lowest=True,
    )
    by_bin = {}
    for key, part in frame.groupby(bins, observed=True):
        by_bin[str(key)] = {
            "trades": int(len(part)),
            "mean_model_p": float(part["model_p"].mean()),
            "win_rate": float(part["live_win"].mean()),
        }
    return {
        "mean_model_p": float(probability.mean()),
        "win_rate": float(truth.mean()),
        "calibration_gap": float(truth.mean() - probability.mean()),
        "brier_score": float(np.mean((probability - truth) ** 2)),
        "by_probability_bin": by_bin,
    }


def _prior_validation(release: dict) -> dict:
    return {
        interval: release["validation"][interval]["base"]
        for interval in ("5m", "15m")
    }


def _prior_trade_rows() -> pd.DataFrame:
    sources = (
        ("5m", PRIOR_ROOT / "5m" / "window_299s" / "forward_base_trades.parquet"),
        (
            "5m",
            PRIOR_ROOT
            / "5m_untouched_post1005"
            / "5m"
            / "window_299s"
            / "forward_base_trades.parquet",
        ),
        ("15m", PRIOR_ROOT / "15m" / "window_300s" / "forward_base_trades.parquet"),
    )
    frames = []
    for interval, path in sources:
        frame = pd.read_parquet(path)
        frame = frame[frame["entry_filled"]].copy()
        frame["interval"] = interval
        frames.append(frame)
    prior = pd.concat(frames, ignore_index=True)
    sides = []
    for row in prior.itertuples(index=False):
        coin = str(row.coin).lower()
        path = SNAP / coin / row.interval / f"{row.market_slug}.parquet"
        resolved_up = _resolved_up(pd.read_parquet(path, columns=["winning_outcome"]))
        if resolved_up is None:
            sides.append(None)
        else:
            sides.append("YES" if resolved_up == bool(row.win) else "NO")
    prior["side"] = sides
    return prior


def _build_report(
    replayed: pd.DataFrame,
    signals: pd.DataFrame,
    ledger: pd.DataFrame,
    start: str,
    end: str,
) -> dict:
    release = json.loads(RELEASE_PATH.read_text(encoding="utf-8"))
    prior = _prior_validation(release)
    prior_rows = _prior_trade_rows()
    prior_by_side = {
        str(side): {
            "trades": int(len(part)),
            "wins": int(part["win"].sum()),
            "win_rate": float(part["win"].mean()),
        }
        for side, part in prior_rows.dropna(subset=["side"]).groupby(
            "side", sort=True
        )
    }
    live = _metrics(replayed, pnl_col="live_pnl", win_col="live_win")
    pmdata = _metrics(replayed, pnl_col="pmdata_pnl", win_col="pmdata_win")
    expected_probabilities = [
        float(prior[interval]["win_rate"]) for interval in replayed["interval"]
    ]
    pm_fills = int(replayed["pmdata_fill"].sum())
    comparable = replayed[replayed["pmdata_win"].notna()]
    outcome_matches = int(comparable["outcome_match"].sum())
    ask_delta = replayed["pmdata_top_ask"] - replayed["signal_ask"]
    vwap_delta = replayed["pmdata_fill_vwap"] - replayed["live_fill_price"]
    hourly = replayed.assign(
        hour=pd.to_datetime(replayed["entry_ts"], unit="s", utc=True).dt.strftime(
            "%Y-%m-%d %H:00"
        )
    )
    clustered = replayed.assign(
        resolution_ts=replayed["window_start_ts"]
        + replayed["interval"].map({"5m": 300, "15m": 900})
    ).groupby("resolution_ts", sort=True).agg(
        trades=("market_slug", "size"),
        wins=("live_win", "sum"),
        pnl_usd=("live_pnl", "sum"),
    )
    return {
        "artifact": "twap_live_apples_to_apples_20260815",
        "period": {"start_utc": start, "end_utc_exclusive": end},
        "frozen_contract": {
            "model_refits": 0,
            "model_sha256": _sha256(MODEL_PATH),
            "release_model_sha256": release["model_sha256"],
            "coins": release["live_coins"],
            "decision_window_seconds": release["decision_window_seconds"],
            "thresholds": release["thresholds"],
            "execution": release["execution"],
        },
        "audit_counts": {
            "signal_rows": int(len(signals)),
            "would_bet_signals": int((signals["outcome"] == "WOULD_BET").sum()),
            "ledger_rows": int(len(ledger)),
            "t3_ledger_rows": int(ledger["scalein_topup"].sum()),
            "distinct_config_hashes": sorted(
                ledger["config_sha256"].dropna().astype(str).unique().tolist()
            ),
        },
        "prior_backtest": {
            "headline": prior,
            "by_side": prior_by_side,
            "side_counts": {
                str(key): int(value)
                for key, value in prior_rows["side"].value_counts().items()
            },
            "side_inference_note": (
                "Side counts and win rates are inferred from the original "
                "exact-replay rows; headline PnL uses the release's 1% resize."
            ),
        },
        "live_actual": {
            "all": live,
            "by_interval": _group_metrics(
                replayed, "interval", pnl_col="live_pnl", win_col="live_win"
            ),
            "by_coin": _group_metrics(
                replayed, "coin", pnl_col="live_pnl", win_col="live_win"
            ),
            "by_side": _group_metrics(
                replayed, "side", pnl_col="live_pnl", win_col="live_win"
            ),
            "by_hour": _group_metrics(
                hourly, "hour", pnl_col="live_pnl", win_col="live_win"
            ),
        },
        "same_decisions_pmdata_replay": {
            "all": pmdata,
            "by_interval": _group_metrics(
                replayed, "interval", pnl_col="pmdata_pnl", win_col="pmdata_win"
            ),
            "pmdata_fills": pm_fills,
            "fill_parity_rate": float(pm_fills / len(replayed)),
            "resolved_comparisons": int(len(comparable)),
            "outcome_matches": outcome_matches,
            "outcome_agreement": (
                float(outcome_matches / len(comparable)) if len(comparable) else None
            ),
            "median_pmdata_minus_live_signal_ask": float(ask_delta.median()),
            "p95_abs_pmdata_minus_live_signal_ask": float(
                ask_delta.abs().quantile(0.95)
            ),
            "median_pmdata_minus_live_fill_vwap": float(vwap_delta.median()),
            "p95_abs_pmdata_minus_live_fill_vwap": float(
                vwap_delta.abs().quantile(0.95)
            ),
        },
        "model_calibration_live": _calibration(replayed),
        "statistical_comparison": {
            "expected_wins_from_prior_interval_win_rates": float(
                math.fsum(expected_probabilities)
            ),
            "observed_wins": live["wins"],
            "probability_of_observed_or_fewer_wins_under_prior_rates": (
                _poisson_binomial_cdf(int(live["wins"]), expected_probabilities)
            ),
            "independence_caveat": (
                "The tail probability treats trades as independent; shared "
                "resolution times and cross-coin direction make it optimistic."
            ),
            "unique_resolution_clusters": int(len(clustered)),
            "mean_trades_per_resolution_cluster": float(clustered["trades"].mean()),
            "max_trades_in_one_resolution_cluster": int(clustered["trades"].max()),
            "losing_resolution_clusters": int((clustered["pnl_usd"] < 0).sum()),
            "worst_resolution_cluster_pnl_usd": float(clustered["pnl_usd"].min()),
        },
        "sizing": {
            "median_live_stake_usd": float(replayed["live_stake"].median()),
            "minimum_order_usd": 1.0,
            "ending_wallet_balance_usd": 58.575353,
            "effective_fraction_at_ending_balance": float(1.0 / 58.575353),
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    args = parser.parse_args(argv)
    signals, ledger = _load_live(
        args.signals, args.ledger, args.start, args.end
    )
    replayed = _replay(signals, ledger)
    report = _build_report(replayed, signals, ledger, args.start, args.end)
    DEFAULT_ROOT.mkdir(parents=True, exist_ok=True)
    replayed.to_parquet(DEFAULT_ROOT / "replay_rows.parquet", index=False)
    (DEFAULT_ROOT / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
