from __future__ import annotations

import pandas as pd

from polymarket.backtest.studies.tick_latency.run_twap_wfo_full import (
    _build_live_bets,
    _side_win_target,
)


def test_side_win_target_inverts_market_label_for_down_rows() -> None:
    up = pd.Series([1, 1, 0, 0])
    side_up = pd.Series([1, 0, 1, 0])
    assert _side_win_target(up, side_up).tolist() == [1, 0, 0, 1]


def test_side_win_target_preserves_missing_market_labels() -> None:
    result = _side_win_target(pd.Series([1.0, float("nan")]), pd.Series([0, 1]))
    assert result.iloc[0] == 0
    assert pd.isna(result.iloc[1])


def test_live_bets_picks_best_side_then_retries_unreachable_second() -> None:
    frame = pd.DataFrame(
        [
            # At t=270 Down is the best side, but its t+1 fill is unreachable.
            {"market_slug": "m1", "entry_ts": 100, "interval": "5m", "coin": "btc",
             "ask": 0.70, "offset_sec": 270, "pred_edge": 0.12,
             "reachable": True, "side_up": 1, "win_true": 0, "side_fill_ask": 0.71},
            {"market_slug": "m1", "entry_ts": 100, "interval": "5m", "coin": "btc",
             "ask": 0.69, "offset_sec": 270, "pred_edge": 0.18,
             "reachable": False, "side_up": 0, "win_true": 1, "side_fill_ask": 0.70},
            # Consume-on-fill retry: the next second's best side is fillable.
            {"market_slug": "m1", "entry_ts": 101, "interval": "5m", "coin": "btc",
             "ask": 0.72, "offset_sec": 271, "pred_edge": 0.14,
             "reachable": True, "side_up": 1, "win_true": 0, "side_fill_ask": 0.73},
            {"market_slug": "m1", "entry_ts": 101, "interval": "5m", "coin": "btc",
             "ask": 0.68, "offset_sec": 271, "pred_edge": 0.16,
             "reachable": True, "side_up": 0, "win_true": 1, "side_fill_ask": 0.69},
        ]
    )
    bets = _build_live_bets(
        frame,
        {"intervals": ("5m",), "ask_lo": 0.60, "ask_hi": 0.85, "gate": 0.06},
    )
    assert len(bets) == 1
    bet = bets.iloc[0]
    assert bet["entry_ts"] == 101
    assert bet["side_up"] == 0
    assert bet["win_true"] == 1


def test_live_bets_rejects_fill_outside_deployed_one_cent_fak_pad() -> None:
    frame = pd.DataFrame([
        {"market_slug": "m1", "entry_ts": 100, "interval": "5m", "coin": "btc",
         "ask": 0.70, "offset_sec": 270, "pred_edge": 0.12,
         "reachable": True, "side_up": 1, "win_true": 1, "side_fill_ask": 0.74},
        {"market_slug": "m1", "entry_ts": 101, "interval": "5m", "coin": "btc",
         "ask": 0.71, "offset_sec": 271, "pred_edge": 0.11,
         "reachable": True, "side_up": 1, "win_true": 1, "side_fill_ask": 0.72},
    ])

    bets = _build_live_bets(
        frame,
        {"intervals": ("5m",), "ask_lo": 0.60, "ask_hi": 0.85, "gate": 0.06},
    )

    assert len(bets) == 1
    assert bets.iloc[0]["entry_ts"] == 101
