"""Backtest twin of the LIVE late-window skip gate.

``tick_topup_foldBE.apply_late_skip`` is the replay-side mirror of
``pipeline._late_window_skip`` (env ``CRYPTOBOT_LATE_WINDOW_SKIP``, pinned to
``BTC:5,ETH:5,SOL:5,XRP:5`` on 2026-08-01). The two must agree on the SAME
inclusive boundary, or the "live twin" is a different strategy.

The boundary is asserted here in the units the attempts frame carries
(``interval`` + ``offset_sec``); the live-side boundary lives in
polymarket/src/cryptobot/tests/test_late_window_gate.py.
"""
from __future__ import annotations

import pandas as pd

from polymarket.backtest.scripts import tick_topup_foldBE as drv


def _attempts(rows):
    return pd.DataFrame(
        [{"interval": iv, "offset_sec": off, "coin": coin}
         for iv, off, coin in rows]
    )


def test_off_by_default_returns_the_frame_untouched():
    """0 = OFF so historical reruns of the base driver reproduce."""
    frame = _attempts([("5m", 299, "btc"), ("15m", 899, "eth")])
    for off in (0, -1):
        out = drv.apply_late_skip(frame, off)
        assert out is frame
        assert len(out) == 2


def test_five_second_cutoff_5m_boundary():
    # 300 - offset <= 5  =>  offset >= 295 is dropped, 294 survives.
    frame = _attempts([("5m", off, "btc") for off in (293, 294, 295, 296, 299)])
    kept = sorted(drv.apply_late_skip(frame, 5)["offset_sec"])
    assert kept == [293, 294]


def test_five_second_cutoff_15m_boundary():
    # 900 - offset <= 5  =>  offset >= 895 is dropped, 894 survives.
    frame = _attempts([("15m", off, "eth") for off in (893, 894, 895, 898, 899)])
    kept = sorted(drv.apply_late_skip(frame, 5)["offset_sec"])
    assert kept == [893, 894]


def test_all_coins_are_dropped_alike():
    """Live is pinned *:5 — the replay must not privilege any coin."""
    frame = _attempts([("5m", 297, c) for c in ("btc", "eth", "sol", "xrp")])
    assert drv.apply_late_skip(frame, 5).empty


def test_early_window_untouched():
    frame = _attempts([("5m", 270, "btc"), ("15m", 870, "sol")])
    assert len(drv.apply_late_skip(frame, 5)) == 2


def test_unknown_interval_fails_open():
    """An interval the map does not know must never be silently dropped."""
    frame = _attempts([("1h", 3599, "btc"), ("5m", 299, "btc")])
    kept = drv.apply_late_skip(frame, 5)
    assert list(kept["interval"]) == ["1h"]


def test_empty_frame_is_a_no_op():
    frame = _attempts([]).reindex(columns=["interval", "offset_sec", "coin"])
    assert drv.apply_late_skip(frame, 5).empty


def test_runner_defaults_to_five_but_yields_to_an_explicit_flag():
    # Importing the runner monkey-patches the engine module (MID-only +
    # FOLDS+F) as a side effect; restore it so no later test in the session
    # inherits a different universe.
    from polymarket.backtest.engine import tick_exit004 as te

    orig_attempts, orig_folds = te.build_union_attempts, te.FOLDS
    try:
        from polymarket.backtest.studies.tick_latency.runners import run_midonly

        assert run_midonly.DEFAULT_LATE_SKIP == "5"
        assert run_midonly._with_default_late_skip(["--latency", "400"]) == [
            "--late-skip", "5", "--latency", "400",
        ]
        for override in (["--late-skip", "0"], ["--late-skip=10"]):
            assert run_midonly._with_default_late_skip(list(override)) == override
    finally:
        te.build_union_attempts = orig_attempts
        te.FOLDS = orig_folds
