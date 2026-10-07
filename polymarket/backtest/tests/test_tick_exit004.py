"""Tests for the canonical subminute + exit004 tick engine."""
import numpy as np
import pandas as pd
import pytest

from polymarket.backtest.engine import tick_exit004 as te


# --------------------------------------------------------------------------
# Settlement-fee parity with LIVE (2026-08-01).
#
# The replay's booked friction must equal what execution/grade_fees.py books on
# the ledger, or BT equity and live equity are not the same quantity. These
# tests fail loudly if either side moves.
# --------------------------------------------------------------------------

_FEE_GRID = [
    (p, s)
    for p in (0.05, 0.30, 0.50, 0.65, 0.70, 0.75, 0.80, 0.92, 0.99)
    for s in (0.5, 1.0, 13.7, 142.857, 1_000.0)
]


@pytest.mark.parametrize("price,shares", _FEE_GRID)
def test_entry_fee_matches_live_grade_fees(price, shares):
    from cryptobot.execution.grade_fees import entry_fee_usd as live_entry_fee

    assert te.entry_fee_usd(price, shares) == pytest.approx(
        live_entry_fee(entry_price=price, shares=shares),
        rel=0.0,
        abs=0.0,
    )


@pytest.mark.parametrize("price,shares", _FEE_GRID)
def test_entry_fee_is_the_calibrated_per_share_parabola(price, shares):
    """Pin the SHAPE independently of grade_fees so a same-day edit to both
    sides cannot silently redefine the fee: k * theta * p * (1-p) * shares."""
    from cryptobot import config

    expected = (
        float(config.POLY_FEE_CALIBRATION)
        * float(config.POLY_FEE_THETA)
        * price
        * (1.0 - price)
        * shares
    )
    assert te.entry_fee_usd(price, shares) == pytest.approx(expected, rel=1e-12)


@pytest.mark.parametrize("cash_usd", [10.0, 100.0, 2_500.0])
@pytest.mark.parametrize("price", [0.70, 0.75, 0.80])
def test_entry_fee_beats_the_retired_per_notional_form(cash_usd, price):
    """The old replay form charged `cash * theta * p * (1-p)` — per DOLLAR and
    uncalibrated. It under-charges by exactly p / POLY_FEE_CALIBRATION."""
    from cryptobot import config

    shares = cash_usd / price
    old = cash_usd * te.fee(price)
    new = te.entry_fee_usd(price, shares)
    assert old < new
    assert old / new == pytest.approx(price / float(config.POLY_FEE_CALIBRATION), rel=1e-12)


def test_side_level_conversion_uses_complementary_up_book():
    tick = _tick(
        [1.0],
        asks=[[0.82, 0.80]],
        ask_sizes=[[3.0, 2.0]],
        bids=[[0.18, 0.20]],
        bid_sizes=[[5.0, 4.0]],
    )
    assert np.allclose(tick.ask_levels(0, "UP"), ((0.80, 2.0), (0.82, 3.0)))
    assert np.allclose(tick.ask_levels(0, "DOWN"), ((0.80, 4.0), (0.82, 5.0)))
    assert np.allclose(tick.bid_levels(0, "UP"), ((0.20, 4.0), (0.18, 5.0)))
    assert np.allclose(tick.bid_levels(0, "DOWN"), ((0.20, 2.0), (0.18, 3.0)))


def test_fak_allows_partial_fill_under_padded_limit():
    fill = te.fak_buy(((0.80, 1.0), (0.81, 0.5), (0.82, 10.0)), 2.0, 0.81)
    assert fill is not None
    assert fill.cash_usd == pytest.approx(1.205)
    assert fill.shares == pytest.approx(1.5)
    assert fill.vwap == pytest.approx(1.205 / 1.5)


def test_fok_requires_full_size_and_respects_trigger_bid():
    levels = ((0.72, 1.0), (0.70, 1.0), (0.69, 10.0))
    assert te.fok_sell(levels, 2.5, 0.70) is None
    fill = te.fok_sell(levels, 2.0, 0.70)
    assert fill is not None
    assert fill.cash_usd == pytest.approx(1.42)
    assert fill.vwap == pytest.approx(0.71)


def _tick(seconds, *, asks, ask_sizes, bids, bid_sizes):
    return te.TickBook(
        seconds=np.asarray(seconds, dtype=float),
        ask_prices=np.asarray([np.asarray(x) for x in asks], dtype=object),
        ask_sizes=np.asarray([np.asarray(x) for x in ask_sizes], dtype=object),
        bid_prices=np.asarray([np.asarray(x) for x in bids], dtype=object),
        bid_sizes=np.asarray([np.asarray(x) for x in bid_sizes], dtype=object),
    )
