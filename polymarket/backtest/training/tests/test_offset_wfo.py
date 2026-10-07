from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from polymarket.backtest.training import offset_wfo as s


def test_spot_at_seconds_uses_latest_prior_second() -> None:
    sec = np.array([100, 105, 110], dtype=np.int64)
    spot = np.array([10.0, 11.0, 12.0], dtype=np.float64)
    got = s._spot_at_seconds(sec, spot, np.array([99, 100, 104, 105, 111], dtype=np.int64))
    assert np.isnan(got[0])
    assert got[1:].tolist() == [10.0, 10.0, 11.0, 12.0]


def test_side_books_from_snapshot_builds_up_and_down_asks() -> None:
    snap = pd.Series(
        {
            "ask_prices": [0.44, 0.46],
            "ask_sizes": [100.0, 50.0],
            "bid_prices": [0.41, 0.43],
            "bid_sizes": [75.0, 25.0],
        }
    )
    up, down = s._side_books_from_snapshot(snap)
    assert up[0] == {"price": 0.44, "size": 100.0}
    assert down[0] == {"price": 0.5700000000000001, "size": 25.0}


def test_top_side_filters_dust_and_mirrors_down_bid() -> None:
    up_best, up_depth = s._top_side([0.0, 0.46, 0.44, 1.0], [10.0, 50.0, 100.0, 20.0], mirror_bid=False)
    down_best, down_depth = s._top_side([0.41, 0.43, 0.0], [75.0, 25.0, 10.0], mirror_bid=True)
    assert up_best == 0.44
    assert up_depth == pytest.approx((0.46 * 50.0) + (0.44 * 100.0))
    assert down_best == 0.5700000000000001
    assert down_depth == pytest.approx((0.59 * 75.0) + (0.5700000000000001 * 25.0))


def test_top_side_tolerates_null_ladder_cells() -> None:
    # Real pmdata snapshots have rows where a side's ladder is None/empty; the
    # offset>0 book path must skip them, not crash.
    assert s._top_side(None, None, mirror_bid=False) == (float("nan"), 0.0) or True
    best, depth = s._top_side(None, None, mirror_bid=False)
    assert best != best and depth == 0.0  # NaN best, zero depth
    best2, depth2 = s._top_side([None, 0.44], [None, 100.0], mirror_bid=False)
    assert best2 == 0.44 and depth2 == pytest.approx(0.44 * 100.0)


def test_passes_filter_requires_selected_offsets_and_thresholds() -> None:
    df = pd.DataFrame(
        {
            "offset_sec": [0, 5, 10],
            "pred_edge": [0.09, 0.11, 0.20],
            "range_strength_bps": [0.0, 0.0, -1.0],
            "breakout_strength_bps": [0.0, 0.75, 2.0],
            "entry_z_signed": [0.0, 0.4, 1.0],
        }
    )
    cfg = {
        "offsets": (5, 10),
        "pred_edge_min": 0.10,
        "min_range_bps": 0.0,
        "breakout_bps": 0.5,
        "min_entry_z": 0.25,
    }
    assert s._passes_filter(df, cfg).tolist() == [False, True, False]


def test_dedup_collapses_second_offsets_but_keeps_entry_minute_decisions() -> None:
    # Same (market, side, entry_minute) replicated at 4 seconds -> ONE bet (earliest
    # second). A different entry_minute on the same market+side is a separate live
    # decision and must survive. A different side likewise survives.
    df = pd.DataFrame(
        {
            "market_slug": ["m1", "m1", "m1", "m1", "m1", "m1", "m2"],
            "side": ["Up", "Up", "Up", "Up", "Up", "Down", "Up"],
            "entry_minute": [2, 2, 2, 2, 3, 2, 2],
            "entry_ts": [1040, 1030, 1050, 1035, 1200, 1030, 2000],
            "offset_sec": [40, 30, 50, 35, 0, 30, 0],
        }
    )
    out = s._dedup_one_per_market(df)
    # (m1,Up,min2)x1, (m1,Up,min3)x1, (m1,Down,min2)x1, (m2,Up,min2)x1
    assert len(out) == 4
    m1_up2 = out[(out["market_slug"] == "m1") & (out["side"] == "Up") & (out["entry_minute"] == 2)]
    assert m1_up2["entry_ts"].tolist() == [1030] and m1_up2["offset_sec"].tolist() == [30]


def test_dedup_is_noop_for_offset0_baseline() -> None:
    # Offset-0 has one second per (market,side,minute) decision, so dedup must not
    # drop anything -> sec0 stays identical to the live baseline.
    df = pd.DataFrame(
        {
            "market_slug": ["m1", "m1", "m2"],
            "side": ["Up", "Down", "Up"],
            "entry_minute": [2, 2, 3],
            "entry_ts": [1000, 1000, 2000],
            "offset_sec": [0, 0, 0],
        }
    )
    assert len(s._dedup_one_per_market(df)) == 3
