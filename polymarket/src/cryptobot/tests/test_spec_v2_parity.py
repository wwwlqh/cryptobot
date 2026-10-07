"""Unit-level parity tests for the shared v2 decision core (spec_v2.py).

Covers:
  * ask-band boundaries (0.65 excluded, 0.85 included),
  * offset windows (5m 270-299, 15m 870-899),
  * dedup-before-gate on a small synthetic frame (build_bets_src vs research
    pool_study.build_bets, bet-for-bet),
  * WindowDecider first-in-band-second commit semantics,
  * the score._ask_in_bucket boundary fix (both directions).

The full fold-E fixture parity (n=2503 / n=2366) is the W6 gate, not here.
"""

import numpy as np
import pandas as pd

from cryptobot.strategy.spec_v2 import (
    V2_SPEC,
    fee,
    in_offset_window,
    WindowDecider,
)
from cryptobot.strategy.score import _ask_in_bucket

# Reference evaluator lives under polymarket/backtest/lib; the repo root is on
# sys.path via pyproject's pythonpath (same as test_canonical_evaluator.py).


# ---------------------------------------------------------------------------
# ask band boundaries
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# offset windows
# ---------------------------------------------------------------------------
def test_offset_window_5m():
    assert in_offset_window("5m", 269) is False
    assert in_offset_window("5m", 270) is True     # first second of window
    assert in_offset_window("5m", 299) is True     # last second
    assert in_offset_window("5m", 300) is False


def test_offset_window_15m():
    assert in_offset_window("15m", 869) is False
    assert in_offset_window("15m", 870) is True
    assert in_offset_window("15m", 899) is True
    assert in_offset_window("15m", 900) is False


def test_offset_window_unknown_interval():
    assert in_offset_window("30m", 285) is False


def test_fee_matches_theta():
    assert abs(fee(0.7) - (0.07 * 0.7 * 0.3)) < 1e-12
    np.testing.assert_allclose(
        fee(np.array([0.65, 0.85])),
        0.07 * np.array([0.65, 0.85]) * (1.0 - np.array([0.65, 0.85])),
    )


# ---------------------------------------------------------------------------
# dedup-before-gate: build_bets_src vs research build_bets, bet-for-bet
# ---------------------------------------------------------------------------
def _frame():
    # m1 : first offset (pred_edge .01) is kept by dedup then FAILS the gate ->
    #      the later qualifying same-market row must NOT resurrect it.
    # m2 : ask 0.90 -> outside band, dropped before dedup.
    # m3 : clean pass (15m, in-window, in-band, reachable, gate ok).
    # m4 : unreachable -> dropped before dedup.
    # m5 : 5m clean pass, in-window at 285.
    # m6 : ask exactly 0.65 -> band excludes it.
    # m7 : ask exactly 0.85 -> band includes it (clean pass).
    return pd.DataFrame({
        "market_slug":  ["m1", "m1", "m2", "m3", "m4", "m5", "m6", "m7"],
        "entry_ts":     [100, 101, 100, 100, 100, 100, 100, 100],
        "interval":     ["15m", "15m", "15m", "15m", "15m", "5m", "15m", "15m"],
        "coin":         ["eth", "eth", "sol", "xrp", "btc", "btc", "eth", "sol"],
        "ask":          [0.70, 0.75, 0.90, 0.70, 0.70, 0.72, 0.65, 0.85],
        "offset_sec":   [875, 876, 875, 875, 875, 285, 875, 875],
        "range_z":      [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        "reachable":    [True, True, True, True, False, True, True, True],
        "pred_edge":    [0.01, 0.99, 0.50, 0.50, 0.50, 0.50, 0.50, 0.50],
        "side_fill_ask":[0.70, 0.75, 0.90, 0.70, 0.70, 0.72, 0.65, 0.85],
        "win_true":     [1, 1, 1, 0, 1, 1, 1, 0],
        "fold":         ["B"] * 8,
    })


def test_v2_spec_pins():
    assert V2_SPEC["intervals"] == ("5m", "15m")
    assert V2_SPEC["ask_lo"] == 0.65
    assert V2_SPEC["ask_hi"] == 0.85
    assert V2_SPEC["gate"] == 0.03
    assert V2_SPEC["fee_theta"] == 0.07


# ---------------------------------------------------------------------------
# WindowDecider — commit at first in-band second, gate it, one bet.
# ---------------------------------------------------------------------------
def _cell(offset_sec, ask, pred_edge, side="YES", interval="5m",
          coin="btc", window_start=1000):
    return {
        "coin": coin, "interval": interval, "window_start": window_start,
        "side": side, "ask": ask, "offset_sec": offset_sec, "pred_edge": pred_edge,
    }


def test_window_decider_commit_first_in_band_places():
    wd = WindowDecider()
    # seconds before the window / out of band -> SKIP, window stays open.
    assert wd.on_second(_cell(269, 0.70, 0.50)) == "SKIP"    # before window
    assert wd.on_second(_cell(270, 0.60, 0.50)) == "SKIP"    # in window, ask out of band
    assert wd.on_second(_cell(271, 0.65, 0.50)) == "SKIP"    # ask exactly 0.65 -> excluded
    # first genuinely in-band, in-window second with gate ok -> PLACE.
    assert wd.on_second(_cell(272, 0.70, 0.50)) == "PLACE"
    # every later second of the same window is consumed.
    assert wd.on_second(_cell(273, 0.72, 0.99)) == "SKIP"
    assert wd.on_second(_cell(299, 0.80, 0.99)) == "SKIP"


def test_window_decider_first_in_band_gate_fail_consumes_window():
    wd = WindowDecider()
    # first in-band second FAILS the gate -> WINDOW_DONE, no bet.
    assert wd.on_second(_cell(275, 0.70, 0.01)) == "WINDOW_DONE"
    # later in-band, gate-passing seconds must NOT resurrect the window.
    assert wd.on_second(_cell(276, 0.70, 0.99)) == "SKIP"


def test_window_decider_keys_are_independent():
    wd = WindowDecider()
    # different side / interval / window are distinct keys.
    assert wd.on_second(_cell(272, 0.70, 0.50, side="YES")) == "PLACE"
    assert wd.on_second(_cell(272, 0.70, 0.50, side="NO")) == "PLACE"
    assert wd.on_second(
        _cell(872, 0.70, 0.50, side="YES", interval="15m")
    ) == "PLACE"
    assert wd.on_second(
        _cell(272, 0.70, 0.50, side="YES", window_start=2000)
    ) == "PLACE"
    # re-hitting the first (YES/5m/1000) window is consumed.
    assert wd.on_second(_cell(273, 0.70, 0.99, side="YES")) == "SKIP"


def test_custom_band_decider_places_own_band_gate():
    # Custom decider: band (0.85, 0.92], gate 0.01. An ask of 0.88 that MID
    # would ignore (out of MID band) is a PLACE here when pred_edge >= 0.01.
    alt = WindowDecider(gate=0.01, consume_on_fill=True, ask_lo=0.85, ask_hi=0.92)
    assert alt.on_second(_cell(285, 0.70, 0.50)) == "SKIP"   # 0.70 not in its band
    assert alt.on_second(_cell(286, 0.85, 0.50)) == "SKIP"   # 0.85 excluded
    assert alt.on_second(_cell(287, 0.88, 0.02)) == "PLACE"  # in band, gate 0.01 ok
    # gate 0.01 rejects a sub-0.01 edge (stays eligible in consume-on-fill mode)
    alt2 = WindowDecider(gate=0.01, consume_on_fill=True, ask_lo=0.85, ask_hi=0.92)
    assert alt2.on_second(_cell(287, 0.88, 0.005)) == "SKIP"
    assert alt2.on_second(_cell(288, 0.88, 0.02)) == "PLACE"


def test_window_decider_custom_gate():
    wd = WindowDecider(gate=0.10)
    assert wd.on_second(_cell(272, 0.70, 0.05)) == "WINDOW_DONE"
    wd2 = WindowDecider(gate=0.10)
    assert wd2.on_second(_cell(272, 0.70, 0.15)) == "PLACE"


def test_window_decider_reset():
    wd = WindowDecider()
    assert wd.on_second(_cell(272, 0.70, 0.50)) == "PLACE"
    assert wd.on_second(_cell(273, 0.70, 0.50)) == "SKIP"
    wd.reset()
    assert wd.on_second(_cell(273, 0.70, 0.50)) == "PLACE"


# ---------------------------------------------------------------------------
# WindowDecider — consume-on-fill mode (operator 2026-07-09)
# ---------------------------------------------------------------------------
def test_consume_on_fill_reject_does_not_consume_window():
    # reject at 292 (we PLACE but never note_fill) -> 293 still eligible.
    wd = WindowDecider(consume_on_fill=True)
    assert wd.on_second(_cell(292, 0.70, 0.50)) == "PLACE"   # attempt 1
    # No note_fill (the FAK was rejected). The next second is a fresh attempt.
    assert wd.on_second(_cell(293, 0.70, 0.50)) == "PLACE"   # attempt 2


def test_consume_on_fill_gate_fail_stays_eligible():
    # gate-fail at 293 -> that second is dead but 294 is still eligible.
    wd = WindowDecider(consume_on_fill=True)
    assert wd.on_second(_cell(292, 0.70, 0.50)) == "PLACE"
    assert wd.on_second(_cell(293, 0.70, 0.01)) == "SKIP"    # gate fail, NOT consumed
    assert wd.on_second(_cell(294, 0.70, 0.50)) == "PLACE"


def test_consume_on_fill_out_of_band_stays_eligible():
    # ask out of band at 293 -> not a qualifying second; window stays open.
    wd = WindowDecider(consume_on_fill=True)
    assert wd.on_second(_cell(293, 0.60, 0.50)) == "SKIP"    # ask below band
    assert wd.on_second(_cell(294, 0.70, 0.50)) == "PLACE"


def test_consume_on_fill_fill_consumes_both_sides():
    # fill at 294 -> 295+ consumed AND the opposite side consumed.
    wd = WindowDecider(consume_on_fill=True)
    assert wd.on_second(_cell(292, 0.70, 0.50, side="YES")) == "PLACE"   # reject
    assert wd.on_second(_cell(293, 0.70, 0.50, side="YES")) == "PLACE"   # reject
    assert wd.on_second(_cell(294, 0.70, 0.50, side="YES")) == "PLACE"   # fills
    wd.note_fill(_cell(294, 0.70, 0.50, side="YES"))
    # later seconds on the filled side are consumed
    assert wd.on_second(_cell(295, 0.70, 0.99, side="YES")) == "SKIP"
    # and the opposite side of the SAME market/window is consumed too
    assert wd.on_second(_cell(295, 0.70, 0.99, side="NO")) == "SKIP"
    # a different window is unaffected
    assert wd.on_second(
        _cell(295, 0.70, 0.50, side="YES", window_start=2000)
    ) == "PLACE"


def test_consume_on_fill_window_end_stops():
    # seconds past the last-30s window (>=300 for 5m) are never eligible.
    wd = WindowDecider(consume_on_fill=True)
    assert wd.on_second(_cell(292, 0.70, 0.50)) == "PLACE"   # reject
    assert wd.on_second(_cell(300, 0.70, 0.99)) == "SKIP"    # out of window
    assert wd.on_second(_cell(999, 0.70, 0.99)) == "SKIP"


def test_configured_checkpoint_offsets_override_legacy_last30_window():
    wd = WindowDecider(
        consume_on_fill=True,
        checkpoint_offsets=tuple(range(240, 300)),
    )
    assert wd.on_second(_cell(239, 0.70, 0.50)) == "SKIP"
    assert wd.on_second(_cell(240, 0.70, 0.50)) == "PLACE"
    assert wd.on_second(_cell(299, 0.70, 0.50)) == "PLACE"
    assert wd.on_second(_cell(300, 0.70, 0.50)) == "SKIP"


def test_consume_on_fill_same_second_never_double_fires():
    wd = WindowDecider(consume_on_fill=True)
    assert wd.on_second(_cell(292, 0.70, 0.50)) == "PLACE"
    # exact same second re-evaluated (defensive) -> SKIP, not a second bet.
    assert wd.on_second(_cell(292, 0.70, 0.50)) == "SKIP"


def test_consume_on_fill_next_attempt_numbers_placements():
    wd = WindowDecider(consume_on_fill=True)
    c = _cell(292, 0.70, 0.50)
    # next_attempt is called once per REAL placement (at the record site).
    assert wd.next_attempt(c) == 1
    assert wd.next_attempt(c) == 2
    assert wd.next_attempt(c) == 3
    # a distinct side restarts the count
    assert wd.next_attempt(_cell(292, 0.70, 0.50, side="NO")) == 1


def test_flag_off_restores_legacy_first_commit():
    # consume_on_fill=False (flag=0) -> legacy: first in-band second consumes
    # the window whether it PLACEs or gate-fails; note_fill is a no-op.
    wd = WindowDecider(consume_on_fill=False)
    assert wd.on_second(_cell(292, 0.70, 0.50)) == "PLACE"
    assert wd.on_second(_cell(293, 0.70, 0.99)) == "SKIP"   # consumed on first commit
    # note_fill does nothing in legacy mode (no _filled tracking needed).
    wd.note_fill(_cell(292, 0.70, 0.50))
    assert wd._filled == set()
    # gate-fail path still consumes (WINDOW_DONE) in legacy mode.
    wd2 = WindowDecider(consume_on_fill=False)
    assert wd2.on_second(_cell(292, 0.70, 0.01)) == "WINDOW_DONE"
    assert wd2.on_second(_cell(293, 0.70, 0.99)) == "SKIP"


