from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from polymarket.backtest.training import modeling as ev


def _side_frame(n: int = 40, *, start: int = 1_800_000_000, step_s: int = 3600) -> pd.DataFrame:
    entry_ts = np.array([start + i * step_s for i in range(n)], dtype=np.int64)
    return pd.DataFrame(
        {
            "entry_ts": entry_ts,
            "entry_dt": pd.to_datetime(entry_ts, unit="s", utc=True),
            "coin": ["btc"] * n,
            "interval": ["5m"] * n,
            "side": ["Up"] * n,
            "ask": np.linspace(0.1, 0.9, n),
        }
    )


def _fold_result(fold: str, *, pnl: float, passed: bool) -> ev.FoldResult:
    return ev.FoldResult(
        fold=fold,
        train_start="2026-01-01T00:00:00+00:00",
        train_end="2026-01-02T00:00:00+00:00",
        test_start="2026-01-02T00:00:00+00:00",
        test_end="2026-01-03T00:00:00+00:00",
        signal="model",
        execution_price="vwap",
        sizing_mode="compound",
        feature_set="rf",
        side_filter="both",
        interval_filter="5m",
        ask_bucket="all",
        book_consumption_max=0.1,
        max_bet_usd=None,
        threshold=0.055,
        stake_frac=0.01,
        n_candidates=10,
        n_bets=30,
        n_rejected_execution=0,
        total_pnl=pnl,
        pnl_frac=pnl / 100.0,
        win_rate=0.6,
        max_dd_frac=0.1,
        worst_30bet_dd_frac=0.1,
        mean_pred_edge=0.07,
        mean_fill=0.5,
        mean_fill_slippage=0.0,
        mean_notional=1.0,
        max_notional=1.0,
        final_equity=100.0 + pnl,
        dd_halted=False,
        passed=passed,
    )


def test_abcd_all_history_folds_use_filtered_frame_and_embargo() -> None:
    frame = _side_frame()

    folds = ev._folds_for_frame("abcd_all_history", frame, embargo_seconds=3600)
    a_start = pd.to_datetime(frame["entry_ts"].iloc[0], unit="s", utc=True)
    b_start = pd.to_datetime(frame["entry_ts"].iloc[10], unit="s", utc=True)
    c_start = pd.to_datetime(frame["entry_ts"].iloc[20], unit="s", utc=True)
    d_start = pd.to_datetime(frame["entry_ts"].iloc[30], unit="s", utc=True)

    assert [f[0] for f in folds] == ["B", "C", "D"]
    # 40 unique timestamps -> 10 per A/B/C/D chunk.
    assert folds[0] == ("B", a_start, b_start - pd.Timedelta(seconds=3600), b_start, c_start)
    assert folds[1] == ("C", a_start, c_start - pd.Timedelta(seconds=3600), c_start, d_start)
    assert folds[2] == (
        "D",
        a_start,
        d_start - pd.Timedelta(seconds=3600),
        d_start,
        pd.to_datetime(frame["entry_ts"].iloc[-1] + 1, unit="s", utc=True),
    )


def test_abcd_calendar_all_history_folds_use_equal_elapsed_time() -> None:
    start = 1_800_000_000
    data_end = start + 80 * 24 * 3600
    entry_ts = np.array(
        [
            start,
            start + 3600,
            start + 5 * 24 * 3600,
            start + 25 * 24 * 3600,
            start + 45 * 24 * 3600,
            start + 65 * 24 * 3600,
            data_end - 1,
        ],
        dtype=np.int64,
    )
    frame = pd.DataFrame(
        {
            "entry_ts": entry_ts,
            "entry_dt": pd.to_datetime(entry_ts, unit="s", utc=True),
            "coin": ["btc"] * len(entry_ts),
            "interval": ["5m"] * len(entry_ts),
            "side": ["Up"] * len(entry_ts),
            "ask": np.linspace(0.1, 0.9, len(entry_ts)),
        }
    )

    folds = ev._folds_for_frame("abcd_calendar_all_history", frame, embargo_seconds=3600)
    a_start = pd.to_datetime(start, unit="s", utc=True)
    b_start = a_start + pd.Timedelta(days=20)
    c_start = a_start + pd.Timedelta(days=40)
    d_start = a_start + pd.Timedelta(days=60)
    end = a_start + pd.Timedelta(days=80)

    assert [f[0] for f in folds] == ["B", "C", "D"]
    assert folds[0] == ("B", a_start, b_start - pd.Timedelta(seconds=3600), b_start, c_start)
    assert folds[1] == ("C", a_start, c_start - pd.Timedelta(seconds=3600), c_start, d_start)
    assert folds[2] == ("D", a_start, d_start - pd.Timedelta(seconds=3600), d_start, end)


def test_fold_recent_boost_is_fold_relative_and_normalized() -> None:
    frame = _side_frame(6)

    weights = ev._sample_weights(frame, "fold_recent_boost")

    assert weights.mean() == pytest.approx(1.0)
    assert weights[2] == pytest.approx(2.0 * weights[0])
    assert weights[4] == pytest.approx(4.0 * weights[0])
    assert list(weights[:2]) == pytest.approx([weights[0], weights[0]])
    assert list(weights[2:4]) == pytest.approx([weights[2], weights[2]])
    assert list(weights[4:]) == pytest.approx([weights[4], weights[4]])


def test_fold_name_filter_keeps_selection_fold_set_only() -> None:
    frame = _side_frame()
    folds = ev._folds_for_frame("abcd_all_history", frame, embargo_seconds=3600)

    selected = ev._filter_folds_by_name(folds, "B,C")

    assert [fold[0] for fold in selected] == ["B", "C"]
    with pytest.raises(ValueError, match="requested fold"):
        ev._filter_folds_by_name(folds, "Z")


def test_summary_separates_selection_from_final_validation() -> None:
    result = ev._summarize_grid(
        [
            _fold_result("B", pnl=10.0, passed=True),
            _fold_result("C", pnl=8.0, passed=True),
            _fold_result("D", pnl=-1.0, passed=False),
        ]
    )

    row = result["summary"][0]
    assert row["selection_all_folds_pass"] is True
    assert row["validation_pass"] is False
    assert row["finalist_pass"] is False
    assert result["best_selection"] is not None
    assert result["best_finalist"] is None


def test_fold_window_diagnostics_report_bucket_and_group_counts() -> None:
    frame = _side_frame()
    folds = ev._folds_for_frame("abcd_all_history", frame, embargo_seconds=3600)

    diagnostics = ev._fold_window_diagnostics(frame, folds, embargo_seconds=3600)

    assert diagnostics[0]["fold"] == "B"
    assert diagnostics[0]["test_unique_entry_ts"] == 10
    assert diagnostics[0]["test_by_coin_interval_side"] == {"btc/5m/Up": 10}
    assert set(diagnostics[0]["test_ask_buckets"]) == {"all", "low", "mid", "high", "tail"}


# --- Calibration summary -----------------------------------------------------


def test_calibration_summary_well_calibrated_has_zero_gap() -> None:
    # Predicted 0.5 everywhere, realized exactly 0.5 -> no calibration gap.
    model_prob = np.full(100, 0.5)
    won = np.tile([1.0, 0.0], 50)

    cal = ev._calibration_summary(model_prob, won, n_buckets=10)

    assert cal["n_bets"] == 100
    assert cal["n_buckets"] == 10
    assert cal["mean_pred"] == pytest.approx(0.5)
    assert cal["realized_wr"] == pytest.approx(0.5)
    assert cal["calibration_gap"] == pytest.approx(0.0)
    assert cal["high_conf_gap"] == pytest.approx(0.0)


def test_calibration_summary_overconfident_is_negative_gap() -> None:
    # Model says 0.9 win prob; every bet loses -> badly over-confident.
    model_prob = np.full(80, 0.9)
    won = np.zeros(80)

    cal = ev._calibration_summary(model_prob, won)

    assert cal["mean_pred"] == pytest.approx(0.9)
    assert cal["realized_wr"] == pytest.approx(0.0)
    assert cal["calibration_gap"] == pytest.approx(-0.9)
    assert cal["high_conf_n"] == 40
    assert cal["high_conf_gap"] == pytest.approx(-0.9)
    assert all(bucket["gap"] < 0 for bucket in cal["buckets"])


def test_calibration_summary_high_conf_isolates_upper_half() -> None:
    # Low-prob bets are well calibrated; only the high-confidence bets fail.
    model_prob = np.concatenate([np.full(50, 0.2), np.full(50, 0.8)])
    won = np.concatenate([np.tile([1.0, 0.0, 0.0, 0.0, 0.0], 10), np.zeros(50)])

    cal = ev._calibration_summary(model_prob, won, n_buckets=10)

    # Upper half is the 0.8-prob bets that never won.
    assert cal["high_conf_mean_pred"] == pytest.approx(0.8)
    assert cal["high_conf_realized_wr"] == pytest.approx(0.0)
    assert cal["high_conf_gap"] == pytest.approx(-0.8)


def test_calibration_summary_empty_and_mismatch() -> None:
    empty = ev._calibration_summary(np.array([]), np.array([]))
    assert empty == ev._empty_calibration()
    assert empty["n_bets"] == 0

    with pytest.raises(ValueError, match="same shape"):
        ev._calibration_summary(np.array([0.5, 0.6]), np.array([1.0]))


# --------------------------------------------------------------------------- #
# Migrated 2026-08-01 (phase 6) from tests/test_residual_replay_execution.py,
# which was deleted with training.residual_replay. These four touch ONLY
# training.modeling and are the surviving coverage of ev._execution_fill_for_row
# and ev._evaluate_one.
# --------------------------------------------------------------------------- #
class FakeBookIndex:
    def __init__(self, prices: list[float] | None = None, sizes: list[float] | None = None) -> None:
        prices = prices or [0.50, 0.58]
        sizes = sizes or [10.0, 10.0]
        self.index = {("slug", "Up", 100): (0, 2, 0)}
        self.prices = np.array(prices, dtype=np.float32)
        self.sizes = np.array(sizes, dtype=np.float32)
        self.cum_cost = np.cumsum(self.prices * self.sizes)
        self.cum_shares = np.cumsum(self.sizes)


def _row() -> SimpleNamespace:
    return SimpleNamespace(
        market_slug="slug",
        side="Up",
        entry_ts=100,
        ask=0.50,
        depth=float("nan"),
        model_prob=0.90,
    )


def test_compiled_replay_loading_is_lazy_when_jit_is_disabled(monkeypatch) -> None:
    monkeypatch.setenv("NUMBA_DISABLE_JIT", "1")
    monkeypatch.setattr(ev, "_compiled_replay", None)
    monkeypatch.setattr(ev, "_compiled_replay_loaded", False)
    assert ev._get_compiled_replay() is None


def test_execution_fill_uses_live_walker_partial_notional() -> None:
    result = ev._execution_fill_for_row(
        _row(),
        notional=10.0,
        execution_price="vwap",
        book_consumption_max=1.0,
        book_idx=FakeBookIndex(),
        max_walk_price=0.05,
        prob_safe_floor=0.90,
        min_edge=-1.0,
        min_bet_usd=0.50,
    )

    assert result is not None
    assert result.notional == pytest.approx(5.0)
    assert result.fill == pytest.approx(0.50)


def test_execution_fill_never_prices_below_candidate_ask() -> None:
    row = _row()
    row.ask = 0.55
    result = ev._execution_fill_for_row(
        row,
        notional=10.0,
        execution_price="vwap",
        book_consumption_max=1.0,
        book_idx=FakeBookIndex(prices=[0.45, 0.46], sizes=[20.0, 20.0]),
        max_walk_price=0.05,
        prob_safe_floor=0.90,
        min_edge=-1.0,
        min_bet_usd=0.50,
    )

    assert result is not None
    assert result.fill == pytest.approx(0.55)


def test_combo_matrix_uses_preselected_threshold_candidates() -> None:
    fold = (
        "B",
        pd.Timestamp("2026-01-01", tz="UTC"),
        pd.Timestamp("2026-01-02", tz="UTC"),
        pd.Timestamp("2026-01-02", tz="UTC"),
        pd.Timestamp("2026-01-03", tz="UTC"),
    )
    rows = pd.DataFrame(
        [
            {
                "market_slug": "slug-hi",
                "coin": "btc",
                "interval": "5m",
                "entry_ts": 100,
                "resolve_ts": 200,
                "side": "Up",
                "ask": 0.50,
                "depth": 100.0,
                "pred_edge": 0.10,
                "model_prob": 0.70,
                "y": 1,
            },
            {
                "market_slug": "slug-low",
                "coin": "btc",
                "interval": "5m",
                "entry_ts": 101,
                "resolve_ts": 201,
                "side": "Up",
                "ask": 0.50,
                "depth": 100.0,
                "pred_edge": -0.10,
                "model_prob": 0.30,
                "y": 1,
            },
        ]
    )
    candidates = ev._dedupe_best_side(rows[rows["pred_edge"] >= 0.05].copy())

    result = ev._evaluate_one(
        candidates,
        signal="model",
        execution_price="top_ask",
        sizing_mode="fixed",
        feature_set="rf",
        side_filter="both",
        interval_filter="5m",
        ask_bucket="all",
        threshold=0.05,
        stake_frac=0.01,
        capital=100.0,
        max_dd_frac=0.50,
        min_fold_bets=1,
        exposure_cap_frac=1.0,
        min_bet_usd=0.50,
        book_consumption_max=1.0,
        max_bet_usd=None,
        fold_meta=fold,
        preselected_candidates=True,
    )

    assert result.n_candidates == 1
    assert result.n_bets == 1


# --------------------------------------------------------------------------- #
# Direct coverage for the two drawdown primitives. Until 2026-08-01 these were
# exercised only indirectly, through residual_replay._evaluate_wallet; that
# wallet is gone, but BOTH are called by the LIVE checkpoint path
# (offset_validation._simulate_wallet), so they keep a test here.
# --------------------------------------------------------------------------- #
def test_max_dd_frac_measures_peak_to_trough_from_starting_capital() -> None:
    assert ev._max_dd_frac(np.array([], dtype=np.float64), 100.0) == 0.0
    # monotone gains never draw down
    assert ev._max_dd_frac(np.array([5.0, 5.0, 5.0]), 100.0) == pytest.approx(0.0)
    # +20 (peak 120) then -60 -> trough 60; dd = 60/120
    assert ev._max_dd_frac(np.array([20.0, -60.0]), 100.0) == pytest.approx(0.5)
    # an OPENING loss draws down from the starting bankroll, not from the first bet
    assert ev._max_dd_frac(np.array([-25.0, 10.0]), 100.0) == pytest.approx(0.25)


def test_worst_rolling_dd_frac_is_window_local_and_short_series_safe() -> None:
    pls = np.array([-10.0, 10.0, -10.0, 10.0])
    # fewer samples than the window -> 0.0 by contract
    assert ev._worst_rolling_dd_frac(pls, 100.0, window=30) == 0.0
    # within a window the worst peak-to-trough is the opening -10 off 100
    assert ev._worst_rolling_dd_frac(pls, 100.0, window=2) == pytest.approx(0.1)
