"""Golden tests for the frozen research evaluator (polymarket/backtest/lib).

The repo root reaches sys.path via pyproject's ``pythonpath = ["polymarket/src", "."]``
— no per-file path bootstrap. (One lived here until the 2026-07-28 venue-tree
restructure, where it silently became wrong: relative to the package's new
``polymarket/src/cryptobot/tests/`` home the same ``../../..`` resolves to
``polymarket/``,
not the repo root.)
"""
import numpy as np
import pandas as pd

from polymarket.backtest.lib.pool_study import build_bets, grade


def _frame():
    return pd.DataFrame({
        "market_slug": ["m1", "m1", "m2", "m3", "m4"],
        "entry_ts":    [100, 101, 100, 100, 100],
        "interval":    ["15m"] * 5,
        "coin":        ["eth", "eth", "sol", "xrp", "btc"],
        "ask":         [0.6, 0.65, 0.9, 0.7, 0.6],   # m2 fails ask band
        "offset_sec":  [875, 876, 875, 875, 875],
        "range_z":     [1.0, 1.0, 1.0, 0.1, 1.0],
        "reachable":   [True, True, True, True, False],  # m4 unreachable
        "pred_edge":   [0.01, 0.99, 0.5, 0.5, 0.5],   # m1 first row fails gate
        "side_fill_ask": [0.6, 0.65, 0.9, 0.7, 0.6],
        "win_true":    [1, 1, 1, 0, 1],
        "fold":        ["B"] * 5,
    })


def test_frozen_order_dedup_before_gate():
    # m1: first offset (pred_edge .01) is kept by dedup, then FAILS the gate ->
    # m1 produces NO bet (the later qualifying row must not resurrect it).
    bets = build_bets(_frame())
    assert set(bets["market_slug"]) == {"m3"}


def test_band_and_reachable_filters_precede_dedup():
    # m2 (ask .9) and m4 (unreachable) drop BEFORE dedup; orb gate drops m3.
    bets = build_bets(_frame(), {"orb_min": 0.5})
    assert len(bets) == 0


def test_grade_math_pins():
    bets = build_bets(_frame(), {"gate": 0.4})  # m3 only
    g = grade(bets, min_n=1)
    # win_true=0 at ask .7: pnl = -1 - fee(0.7); fee theta*0.7*0.3
    from polymarket.backtest.lib.pool_study import fee
    assert g["n"] == 1
    assert abs(g["ev"] - (-1.0 - float(fee(np.array([0.7]))[0]))) < 1e-9
