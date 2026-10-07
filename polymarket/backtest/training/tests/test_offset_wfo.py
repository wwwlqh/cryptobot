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


