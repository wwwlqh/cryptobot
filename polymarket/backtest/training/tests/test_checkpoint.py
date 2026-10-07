from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from polymarket.backtest.training import checkpoint as cg


def test_grid_seconds_normalizes_requested_rounds() -> None:
    assert cg.grid_seconds("baseline60")["5m"] == (60, 120, 180, 240)
    assert cg.grid_seconds("shift30")["5m"] == (30, 90, 150, 210, 270)
    assert cg.grid_seconds("shift15")["15m"][-1] == 885
    assert cg.grid_seconds("step5")["5m"][:4] == (5, 10, 15, 20)
    assert cg.grid_seconds("step5")["15m"][-1] == 895


def test_expand_checkpoints_sets_fractional_entry_minute() -> None:
    markets = pd.DataFrame(
        {
            "market_slug": ["btc-updown-5m-1000", "btc-updown-15m-2000"],
            "coin": ["btc", "btc"],
            "interval": ["5m", "15m"],
            "slug_ts": [1000, 2000],
            "resolve_ts": [1300, 2900],
        }
    )

    out = cg._expand_checkpoints(markets, "shift30")

    first_5m = out[(out["interval"] == "5m") & (out["checkpoint_sec"] == 30)].iloc[0]
    assert first_5m["entry_ts"] == 1030
    assert first_5m["entry_minute"] == pytest.approx(0.5)
    assert first_5m["offset_sec"] == 30
    assert set(out[out["interval"] == "5m"]["checkpoint_sec"]) == {30, 90, 150, 210, 270}


def test_checkpoint_features_use_at_or_before_spot(monkeypatch) -> None:
    secs = np.arange(0, 5_000, dtype=np.int64)
    close = 100.0 + secs * 0.001
    close[1_031] = 9_999.0
    spot = pd.DataFrame({"sec": secs, "open": close, "close": close})
    monkeypatch.setattr(cg.base, "_load_1s", lambda coin: spot)

    rows = pd.DataFrame(
        {
            "market_slug": ["m1"],
            "coin": ["btc"],
            "interval": ["5m"],
            "slug_ts": [1000],
            "resolve_ts": [1300],
            "checkpoint_sec": [30],
            "offset_sec": [30],
            "range_sec": [30],
            "entry_ts": [1030],
            "entry_minute": [0.5],
            "entry_dt": [pd.Timestamp(1030, unit="s", tz="UTC")],
        }
    )

    out = cg._attach_checkpoint_features(rows)

    expected = np.log(close[1030] / close[1000])
    assert out["entry_ret"].iloc[0] == pytest.approx(expected)
    assert out["range_ret"].iloc[0] == pytest.approx(expected)
    assert out["entry_ret"].iloc[0] != pytest.approx(np.log(close[1031] / close[1000]))


def test_minute_rv_is_causal_at_boundary() -> None:
    minute = np.arange(62, dtype=np.int64)
    returns = 0.0001 + minute * 0.000001
    closes = 100.0 * np.exp(np.cumsum(returns))
    spot = pd.DataFrame(
        {"sec": minute * 60 + 59, "open": closes, "close": closes}
    )

    rv = cg._minute_rv_from_1s(spot)

    # At t=60m, minute [60m,61m) has not closed, leaving only 59 returns.
    assert np.isnan(rv.loc[pd.Timestamp(3600, unit="s", tz="UTC")])
    # At t=61m that minute is complete and exactly 60 causal returns exist.
    expected = pd.Series(np.log(closes)).diff().iloc[1:61].std()
    assert rv.loc[pd.Timestamp(3660, unit="s", tz="UTC")] == pytest.approx(expected)


def test_train_windows_exclude_d_with_embargo() -> None:
    windows = cg._train_windows()
    assert windows["ABC"] < cg._D0
    assert (cg._D0 - windows["ABC"]) == pd.Timedelta(seconds=cg.EMBARGO_SECONDS)
    # Migrated from tests/test_offset_validation.py 2026-08-01 (phase 6) with the
    # deletion of the duplicate lk._train_windows: every train window ends exactly
    # one embargo before its fold boundary, not just the ABC/D one.
    assert (cg._B0 - windows["A"]) == pd.Timedelta(seconds=cg.EMBARGO_SECONDS)
    assert (cg._C0 - windows["AB"]) == pd.Timedelta(seconds=cg.EMBARGO_SECONDS)

    rows = pd.DataFrame(
        {
            "entry_dt": [
                cg._D0 - pd.Timedelta(seconds=cg.EMBARGO_SECONDS + 1),
                cg._D0 - pd.Timedelta(seconds=cg.EMBARGO_SECONDS),
                cg._D0,
            ]
        }
    )
    assert cg._window_train_mask(rows, windows["ABC"]).tolist() == [True, False, False]


class _FakeEstimator:
    classes_ = np.array([0, 1])

    def predict_proba(self, frame):
        vals = frame["partial_ret_z_1h"].astype(float).to_numpy()
        return np.column_stack([1.0 - vals, vals])


def test_rf_probs_use_checkpoint_sec_without_integer_minute_cast() -> None:
    rows = pd.DataFrame(
        {
            "coin": ["btc", "btc"],
            "interval": ["5m", "5m"],
            "checkpoint_sec": [30, 90],
            "partial_ret_z_1h": [0.25, 0.75],
        }
    )
    estimators = {("btc", "5m", 30): _FakeEstimator()}

    got = cg._rf_probs_checkpoint(rows, estimators)

    assert got[0] == pytest.approx(0.25)
    assert np.isnan(got[1])


def test_resolution_dedupe_collapses_multiple_checkpoints() -> None:
    df = pd.DataFrame(
        {
            "market_slug": ["m1", "m1", "m2"],
            "entry_ts": [1030, 1090, 2005],
            "pred_edge": [0.10, 0.50, 0.20],
            "book_depth": [10.0, 20.0, 30.0],
            "ask": [0.40, 0.35, 0.50],
        }
    )

    out, stats = cg.lk._dedup_resolution_with_stats(df)

    assert len(out) == 2
    assert stats["resolution_duplicate_drops"] == 1
    assert out.loc[out["market_slug"] == "m1", "entry_ts"].iloc[0] == 1030


def test_without_bets_strips_large_bet_payloads() -> None:
    got = cg._without_bets({"grid": "baseline60", "_bets": [{"x": 1}], "selected_bets": [{"x": 2}]})
    assert got == {"grid": "baseline60"}


def test_baseline_reference_summary_does_not_require_refit() -> None:
    result = dict(cg.EXP044_REFERENCE)

    summary = cg._summary_record(result)

    assert summary["grid"] == "baseline60"
    assert summary["baseline_mode"] == "reference"
    assert summary["works"] is True
    assert summary["fold_pnl_D"] == pytest.approx(1448.51)
    assert summary["worst_chunk_pnl"] == pytest.approx(95.71)
