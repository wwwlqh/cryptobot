"""Audit-parity CI guards (#9, #10).

Divergences #9 (fee shape) and #10 (edge formula) between backtest and
live are currently *zero by inspection* — both branches read the same
``POLY_FEE_THETA`` constant and compute the same theta-shape edge
formula. These tests fail CI the moment that invariant breaks, so a
silent drift in either branch becomes a red build instead of a
production loss.

What we pin:

1. ``cryptobot.config.POLY_FEE_THETA == 0.07`` (the published
   Polymarket crypto-category peak taker fee).
2. ``backtest.sweep_lib.POLY_FEE_THETA == cryptobot.config.POLY_FEE_THETA``
   — the runtime and the backtest must agree on the constant.
3. ``backtest.sweep_lib`` must NOT re-expose the legacy flat
   ``POLY_FEE`` constant. Its presence indicates the theta-shape
   rewrite was reverted.
4. ``cryptobot.strategy.score._fee_adjusted_edge`` must equal
   ``backtest.sweep_lib.edge`` on a property-test sweep of YES sides
   (Polymarket Up=YES). Random (model_p, yes, no) draws are fed to
   both functions and the outputs are compared to 1e-12.
"""

from __future__ import annotations

import math
import random
import sys
from pathlib import Path

import pytest

from cryptobot import config
from cryptobot.strategy import parity
from cryptobot.strategy.score import _fee_adjusted_edge


# ``backtest`` is a package (repo root on sys.path via pytest
# pythonpath = ["src", "."]); ``sweep_lib`` lives in the engine subpackage.


def test_fee_theta_pinned_to_official_polymarket_rate():
    """Polymarket's crypto-category peak taker fee, per the published
    fee schedule. A drift here means we are paying or charging the
    wrong fee in either backtest or live."""
    assert config.POLY_FEE_THETA == 0.07


def test_backtest_and_live_agree_on_theta():
    from polymarket.backtest.engine import sweep_lib  # type: ignore[import-not-found]
    assert sweep_lib.POLY_FEE_THETA == config.POLY_FEE_THETA, (
        "sweep_lib.POLY_FEE_THETA (backtest) and cryptobot.config.POLY_FEE_THETA "
        "(live) must match — divergence #9 fix"
    )


def test_runtime_parity_constants_match_backtest():
    from polymarket.backtest.engine import sweep_lib  # type: ignore[import-not-found]

    assert parity.POLY_FEE_THETA == sweep_lib.POLY_FEE_THETA
    assert parity.INITIAL_CAPITAL == 100.0
    assert parity.CONCURRENT_EXPOSURE_CAP == sweep_lib.CONCURRENT_EXPOSURE_CAP


def test_legacy_flat_fee_constant_removed():
    """If sweep_lib re-exposes the pre-2026-05-16 flat ``POLY_FEE`` it
    means the theta-shape rewrite was reverted; halt CI."""
    from polymarket.backtest.engine import sweep_lib  # type: ignore[import-not-found]
    assert not hasattr(sweep_lib, "POLY_FEE"), (
        "sweep_lib.POLY_FEE re-introduced — backtest reverted to flat-fee "
        "shape. Live still uses theta-shape; this WILL cause silent divergence."
    )


@pytest.mark.parametrize("seed", [0, 1, 42, 99, 12345])
@pytest.mark.parametrize("side", ["Up", "Down"])
def test_edge_formula_matches_backtest(seed: int, side: str):
    """Property: for every (model_p, yes_ask, no_ask) triple, the live
    ``_fee_adjusted_edge`` must equal the backtest's ``edge(...)`` for
    the relevant side. Both compute
    ``model_p_side - ask_side - theta * ask_side * (1 - ask_side)``;
    this test catches a drift in either side's expression — for
    example, accidentally swapping ``ask`` for ``1 - ask`` in the
    slippage term.

    Down branch parity: ``sweep_lib.edge(model_p, yes, no, "Down")``
    must equal ``_fee_adjusted_edge(1 - model_p, no_ask)`` — the live
    code applies the same formula but feeds the complementary
    probability and the complementary side's ask.

    VENUE PIN (2026-08-01): ``_fee_adjusted_edge`` is venue-dispatched now and
    defaults to ``config.VENUE``. The research parity chain is POLYMARKET's
    (sweep_lib's theta parabola), so the venue is pinned explicitly here rather
    than inherited from whatever ``CRYPTOBOT_VENUE`` the runner happens to
    carry — on a predict.fun box this suite would otherwise compare a TENT
    against a PARABOLA and fail for the right reason at the wrong site.
    """
    import numpy as np
    from polymarket.backtest.engine import sweep_lib  # type: ignore[import-not-found]
    rng = random.Random(seed)
    for _ in range(200):
        model_p = rng.uniform(0.01, 0.99)
        yes_ask = rng.uniform(0.01, 0.99)
        no_ask = rng.uniform(0.01, 0.99)
        if side == "Up":
            live = _fee_adjusted_edge(model_p, yes_ask, venue="polymarket")
        else:  # "Down"
            live = _fee_adjusted_edge(1.0 - model_p, no_ask, venue="polymarket")
        runtime = float(parity.edge(
            np.array([model_p]), np.array([yes_ask]), np.array([no_ask]), side
        )[0])
        back = float(sweep_lib.edge(
            np.array([model_p]), np.array([yes_ask]), np.array([no_ask]), side
        )[0])
        assert math.isclose(runtime, back, rel_tol=1e-12, abs_tol=1e-12), (
            f"runtime parity edge drift on side={side} at model_p={model_p}, "
            f"yes_ask={yes_ask}, no_ask={no_ask}: runtime={runtime} backtest={back}"
        )
        assert math.isclose(live, back, rel_tol=1e-12, abs_tol=1e-12), (
            f"edge formula drift on side={side} at model_p={model_p}, "
            f"yes_ask={yes_ask}, no_ask={no_ask}: live={live} backtest={back}"
        )


def test_pnl_math_matches_backtest_for_yes_side():
    """``backtest.sweep_lib.unit_pnl('Up', fill, won_up, haircut)`` is
    the canonical per-share P&L for YES bets, and the frozen parity
    chain must keep matching it byte-for-byte. (Since the 2026-07-23
    calibrated fee grading the resolution sweep BOOKS through
    ``execution.grade_fees.graded_cash_pnl`` — venue-dispatched,
    fee-on-every-fill — but the parity chain itself stays untouched;
    ``resolution_sweep._cash_pnl`` remains this chain's runtime
    surface.) Verify the canonical function is reachable and obeys
    the documented shape: ``won → (1 - fill)(1 - theta*fill)``,
    ``lost → -fill``."""
    from polymarket.backtest.engine import sweep_lib  # type: ignore[import-not-found]
    theta = sweep_lib.POLY_FEE_THETA
    fill = 0.55
    pnl_win = parity.unit_pnl("Up", fill, 1)
    pnl_lose = parity.unit_pnl("Up", fill, 0)
    assert pnl_win == sweep_lib.unit_pnl("Up", fill, 1)
    assert pnl_lose == sweep_lib.unit_pnl("Up", fill, 0)
    expected_win = (1 - fill) * (1 - theta * fill)
    expected_lose = -fill
    assert math.isclose(pnl_win, expected_win, rel_tol=1e-12, abs_tol=1e-12)
    assert math.isclose(pnl_lose, expected_lose, rel_tol=1e-12, abs_tol=1e-12)


def test_cash_pnl_math_matches_backtest_for_cash_sized_yes_side():
    """A $10 losing YES bet must lose $10, not fill_price * $10."""
    from polymarket.backtest.engine import sweep_lib  # type: ignore[import-not-found]

    fill = 0.55
    stake = 10.0
    pnl_win = parity.cash_pnl("YES", fill, 1, stake)
    pnl_lose = parity.cash_pnl("YES", fill, 0, stake)
    assert pnl_win == sweep_lib.cash_pnl("Up", fill, 1, stake)
    assert pnl_lose == sweep_lib.cash_pnl("Up", fill, 0, stake)
    assert math.isclose(pnl_lose, -stake, rel_tol=1e-12, abs_tol=1e-12)
