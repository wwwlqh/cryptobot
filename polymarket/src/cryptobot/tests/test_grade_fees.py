"""Grade-time fee booking: calibrated Polymarket parabola on every entry fill."""

from __future__ import annotations

import pytest

from cryptobot import config
from cryptobot.execution.grade_fees import entry_fee_usd, graded_cash_pnl

THETA = 0.07
K = 0.864  # default CRYPTOBOT_POLY_FEE_CALIBRATION


def test_calibration_knob_default_is_0_864():
    """The wallet audit measured an effective coefficient k * theta = 0.06048."""
    assert config.POLY_FEE_CALIBRATION == 0.864
    assert config.POLY_FEE_CALIBRATION * config.POLY_FEE_THETA == pytest.approx(
        0.06048, rel=1e-12
    )


# ---------------------------------------------------------------------------
# entry_fee_usd
# ---------------------------------------------------------------------------


def test_entry_fee_is_calibrated_parabola():
    assert entry_fee_usd(entry_price=0.70, shares=5.0) == pytest.approx(
        K * THETA * 0.70 * 0.30 * 5.0
    )


def test_entry_fee_scales_with_knob(monkeypatch):
    monkeypatch.setattr(config, "POLY_FEE_CALIBRATION", 0.5)
    assert entry_fee_usd(entry_price=0.70, shares=5.0) == pytest.approx(
        0.5 * THETA * 0.70 * 0.30 * 5.0
    )


def test_entry_fee_clips_entry_price_and_floors_shares():
    # Same [1e-6, 0.999] clip as strategy.parity; non-positive shares pay 0.
    assert entry_fee_usd(entry_price=1.7, shares=5.0) == pytest.approx(
        K * THETA * 0.999 * (1.0 - 0.999) * 5.0
    )
    assert entry_fee_usd(entry_price=0.70, shares=0.0) == 0.0


# ---------------------------------------------------------------------------
# graded_cash_pnl
# ---------------------------------------------------------------------------


def test_win_books_payout_minus_calibrated_fee():
    stake, p = 10.0, 0.55
    shares = stake / p
    expected = (1.0 - p) * shares - K * THETA * p * (1.0 - p) * shares
    assert graded_cash_pnl("YES", p, True, stake) == pytest.approx(expected, abs=1e-12)


def test_loss_books_stake_plus_calibrated_fee():
    # The fee is charged on the fill, win or lose.
    stake, p = 10.0, 0.55
    shares = stake / p
    expected = -stake - K * THETA * p * (1.0 - p) * shares
    assert graded_cash_pnl("YES", p, False, stake) == pytest.approx(expected, abs=1e-12)


def test_side_aliases_match_parity_convention():
    stake, p = 10.0, 0.55
    yes_win = graded_cash_pnl("YES", p, True, stake)
    up_win = graded_cash_pnl("Up", p, True, stake)
    no_win = graded_cash_pnl("NO", p, False, stake)
    assert yes_win == up_win == no_win
    assert graded_cash_pnl("NO", p, True, stake) < 0


def test_win_is_backtest_cash_pnl_plus_calibration_refund():
    # The payout base matches the backtest; only the fee term is scaled by k.
    from polymarket.backtest.engine.sweep_lib import cash_pnl as backtest_cash_pnl

    stake, p = 10.0, 0.55
    shares = stake / p
    live = graded_cash_pnl("YES", p, True, stake)
    back = backtest_cash_pnl("YES", p, 1, stake, 0.0)
    assert live - back == pytest.approx(
        (1.0 - K) * THETA * p * (1.0 - p) * shares, abs=1e-9
    )
