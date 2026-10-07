"""Calibrated grade-time fee booking (2026-07-23) — formula pins.

Pins every (venue x row-class) grade formula against hand-computed
arithmetic, the CRYPTOBOT_POLY_FEE_CALIBRATION knob (default 0.864, re-based
from the 0.84-at-theta-0.072 measurement when the published theta was
corrected to 0.07 — the audited product k*theta = 0.06048 is unchanged; from the
flat-to-flat wallet-tape audit: realized $1.73 vs modeled $2.18 over
2026-07-21 22:11 -> 2026-07-23), and the per-row ``fee_rate_bps``
passthrough on the predict.fun tent. The stop (CANCELLED) shape is pinned in
the sweep integration in test_resolution_sweep.py; here
the fee building blocks themselves are pinned so a drift in either fails
loudly at the source.
"""

from __future__ import annotations

import pytest

from cryptobot import config
from cryptobot.execution.grade_fees import entry_fee_usd, graded_cash_pnl

THETA = 0.07
K = 0.864  # documented default of CRYPTOBOT_POLY_FEE_CALIBRATION


def test_calibration_knob_default_is_0_864():
    """Flat-to-flat wallet-tape audit 2026-07-23: realized venue friction
    $1.73 vs $2.18 modeled by 0.072*p*(1-p) over the 07-21 22:11 -> 07-23
    window => the venue extracts an effective coefficient k*theta = 0.84 *
    0.072 = 0.06048. The published theta was corrected to 0.07 on 2026-08-05,
    so k re-bases to 0.84*0.072/0.07 = 0.864 and 0.864*0.07 = 0.06048 keeps
    booked fees identical. A drift here is a silent re-calibration."""
    assert config.POLY_FEE_CALIBRATION == 0.864
    assert config.POLY_FEE_CALIBRATION * config.POLY_FEE_THETA == pytest.approx(
        0.06048, rel=1e-12
    )


# ---------------------------------------------------------------------------
# entry_fee_usd — the one fee both settlement (WIN/LOSS) and the stop
# booking (CANCELLED) subtract
# ---------------------------------------------------------------------------


def test_entry_fee_polymarket_is_calibrated_parabola():
    # k * theta * p * (1-p) * shares, p=0.70, 5 shares.
    assert entry_fee_usd(
        venue="polymarket", entry_price=0.70, shares=5.0
    ) == pytest.approx(K * THETA * 0.70 * 0.30 * 5.0)


def test_entry_fee_polymarket_scales_with_knob(monkeypatch):
    monkeypatch.setattr(config, "POLY_FEE_CALIBRATION", 0.5)
    assert entry_fee_usd(
        venue="polymarket", entry_price=0.70, shares=5.0
    ) == pytest.approx(0.5 * THETA * 0.70 * 0.30 * 5.0)


def test_entry_fee_polymarket_ignores_row_fee_rate_bps():
    # Poly rows carry a decorative fee_rate_bps (72); the grade fee must come
    # from theta * calibration, never from that column.
    assert entry_fee_usd(
        venue="polymarket", entry_price=0.70, shares=5.0, fee_rate_bps=9_999.0
    ) == pytest.approx(K * THETA * 0.70 * 0.30 * 5.0)


def test_entry_fee_predictfun_is_tent_with_row_bps():
    # (fee_rate_bps/1e4) * min(p, 1-p) * shares — 200 bps on a 0.69 favorite.
    assert entry_fee_usd(
        venue="predictfun", entry_price=0.69, shares=5.1594, fee_rate_bps=200.0
    ) == pytest.approx(0.02 * 0.31 * 5.1594)


def test_entry_fee_predictfun_bps_passthrough():
    # The ROW's fee_rate_bps drives the tent — 300 bps is honored verbatim.
    assert entry_fee_usd(
        venue="predictfun", entry_price=0.40, shares=10.0, fee_rate_bps=300.0
    ) == pytest.approx(0.03 * 0.40 * 10.0)


def test_entry_fee_predictfun_zero_bps_is_a_fee_free_market():
    # Explicit 0.0 means a genuinely fee-free market, NOT "fall back to 200".
    assert entry_fee_usd(
        venue="predictfun", entry_price=0.40, shares=10.0, fee_rate_bps=0.0
    ) == 0.0


def test_entry_fee_predictfun_none_bps_falls_back_to_config_default():
    assert entry_fee_usd(
        venue="predictfun", entry_price=0.40, shares=10.0, fee_rate_bps=None
    ) == pytest.approx(
        (config.PREDICTFUN_DEFAULT_FEE_RATE_BPS / 10_000.0) * 0.40 * 10.0
    )


def test_entry_fee_calibration_knob_never_touches_predictfun(monkeypatch):
    # The 2% share fee is venue-measured exact — k must not scale the tent.
    monkeypatch.setattr(config, "POLY_FEE_CALIBRATION", 0.0)
    assert entry_fee_usd(
        venue="predictfun", entry_price=0.69, shares=5.0, fee_rate_bps=200.0
    ) == pytest.approx(0.02 * 0.31 * 5.0)


def test_entry_fee_unknown_or_null_venue_grades_as_polymarket():
    # Ledger venue is NOT NULL DEFAULT 'polymarket'; grade-time must keep
    # settling (a raise would strand rows PENDING), so legacy/garbage venues
    # take the polymarket shape — matching the column DEFAULT. The DECISION
    # path dispatch (predictfun.fees.fee_per_share) stays fail-closed.
    expected = K * THETA * 0.70 * 0.30 * 5.0
    for venue in (None, "", "kalshi"):
        assert entry_fee_usd(
            venue=venue, entry_price=0.70, shares=5.0
        ) == pytest.approx(expected)


def test_entry_fee_clips_entry_price_and_floors_shares():
    # Same [1e-6, 0.999] clip as strategy.parity — garbage prices can't make
    # a negative/runaway fee; non-positive shares fee 0.
    assert entry_fee_usd(
        venue="polymarket", entry_price=1.7, shares=5.0
    ) == pytest.approx(K * THETA * 0.999 * (1.0 - 0.999) * 5.0)
    assert entry_fee_usd(venue="polymarket", entry_price=0.70, shares=0.0) == 0.0


# ---------------------------------------------------------------------------
# graded_cash_pnl — WIN/LOSS x venue
# ---------------------------------------------------------------------------


def test_polymarket_win_keeps_shape_with_calibrated_fee_term():
    # WIN = S*(1-p) - k*theta*p*(1-p)*S with S = stake/p shares.
    stake, p = 10.0, 0.55
    shares = stake / p
    expected = (1.0 - p) * shares - K * THETA * p * (1.0 - p) * shares
    assert graded_cash_pnl(
        "YES", p, True, stake, venue="polymarket"
    ) == pytest.approx(expected, abs=1e-12)


def test_polymarket_loss_books_stake_plus_calibrated_fee():
    # LOSS = -S*p - k*theta*p*(1-p)*S — the venue charged the fee on the
    # entry fill, win or lose. -S*p == -stake exactly.
    stake, p = 10.0, 0.55
    shares = stake / p
    expected = -stake - K * THETA * p * (1.0 - p) * shares
    assert graded_cash_pnl(
        "YES", p, False, stake, venue="polymarket"
    ) == pytest.approx(expected, abs=1e-12)


def test_predictfun_win_books_tent_not_parabola():
    # Live row id=1 economics: 5.1594 sh @0.69. The OLD grade booked the
    # Polymarket parabola (1.51996 = S(1-p) - S*0.072*p*(1-p) at the old
    # theta; historical row value, not recomputed); the correct
    # predict.fun grade is S(1-p) - 0.02*min(p,1-p)*S.
    shares, p = 5.1594, 0.69
    stake = shares * p
    expected = (1.0 - p) * shares - 0.02 * (1.0 - p) * shares
    got = graded_cash_pnl(
        "YES", p, True, stake, venue="predictfun", fee_rate_bps=200.0
    )
    assert got == pytest.approx(expected, abs=1e-9)
    old_wrong = (1.0 - p) * shares - THETA * p * (1.0 - p) * shares
    assert got != pytest.approx(old_wrong)


def test_predictfun_loss_books_stake_plus_tent_fee():
    stake, p = 10.0, 0.40
    shares = stake / p
    expected = -stake - 0.02 * 0.40 * shares  # min(0.40, 0.60) = 0.40
    assert graded_cash_pnl(
        "YES", p, False, stake, venue="predictfun", fee_rate_bps=200.0
    ) == pytest.approx(expected, abs=1e-12)


def test_graded_cash_pnl_side_aliases_match_parity_convention():
    # YES/UP and NO/DOWN aliases behave like strategy.parity: a NO bet wins
    # when won_up is False.
    stake, p = 10.0, 0.55
    yes_win = graded_cash_pnl("YES", p, True, stake, venue="polymarket")
    up_win = graded_cash_pnl("Up", p, True, stake, venue="polymarket")
    no_win = graded_cash_pnl("NO", p, False, stake, venue="polymarket")
    assert yes_win == up_win == no_win
    assert graded_cash_pnl("NO", p, True, stake, venue="polymarket") < 0


def test_graded_win_is_backtest_cash_pnl_plus_calibration_refund():
    # Relationship to the frozen backtest formula: the payout base matches;
    # only the fee term is scaled by k, so live WIN = backtest WIN +
    # (1-k)*theta*p*(1-p)*S. Pins that the calibration touches NOTHING but
    # the fee term.
    from polymarket.backtest.engine.sweep_lib import cash_pnl as backtest_cash_pnl

    stake, p = 10.0, 0.55
    shares = stake / p
    live = graded_cash_pnl("YES", p, True, stake, venue="polymarket")
    back = backtest_cash_pnl("YES", p, 1, stake, 0.0)
    assert live - back == pytest.approx(
        (1.0 - K) * THETA * p * (1.0 - p) * shares, abs=1e-9
    )
