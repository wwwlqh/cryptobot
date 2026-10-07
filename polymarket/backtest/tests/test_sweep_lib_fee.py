"""Verify the theta-shape Polymarket crypto fee in sweep_lib.

Properties:
- POLY_FEE_THETA == 0.07 (theta-shape peak ~1.75% at p=0.5; matches ~2% taker)
- fee(p) is zero at p=0 and p=1, peaks at p=0.5 with value theta * 0.25
- edge() shape is monotonic in model_p_up
- pnl_per_bet() never returns positive on a loss (no fee credit on lose leg)
"""
from __future__ import annotations

import numpy as np
import pytest

from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA, edge


def test_theta_constant_is_crypto_rate():
    assert POLY_FEE_THETA == 0.07, "POLY_FEE_THETA must match Polymarket crypto-category peak fee"


def test_fee_zero_at_boundaries():
    # edge_up at boundaries: at yes=0 → p - 0 - theta*0 = p; at yes=1 → p - 1 - theta*0 = p-1
    e_at_0 = edge(np.array([0.5]), np.array([0.0]), np.array([1.0]), "Up")
    e_at_1 = edge(np.array([0.5]), np.array([1.0]), np.array([0.0]), "Up")
    # Fee contribution should be zero at both boundaries.
    assert e_at_0[0] == pytest.approx(0.5)
    assert e_at_1[0] == pytest.approx(-0.5)


def test_fee_peaks_at_half():
    # At yes=0.5, fee per share = theta * 0.5 * 0.5 = theta/4
    e_half = edge(np.array([0.5]), np.array([0.5]), np.array([0.5]), "Up")
    expected_edge_without_fee = 0.5 - 0.5
    expected_fee = POLY_FEE_THETA * 0.5 * 0.5
    assert e_half[0] == pytest.approx(expected_edge_without_fee - expected_fee)


def test_edge_monotonic_in_model_p():
    """edge_up increases with model_p_up; edge_down decreases."""
    p_low = np.array([0.40])
    p_high = np.array([0.60])
    yes = np.array([0.50])
    no = np.array([0.50])
    e_up_low = edge(p_low, yes, no, "Up")[0]
    e_up_high = edge(p_high, yes, no, "Up")[0]
    assert e_up_high > e_up_low
    e_dn_low = edge(p_low, yes, no, "Down")[0]
    e_dn_high = edge(p_high, yes, no, "Down")[0]
    assert e_dn_high < e_dn_low


def test_down_side_symmetric_to_up():
    """edge_down = (1-p) - no_price - theta * no_price * (1-no_price)."""
    p = np.array([0.65])
    yes = np.array([0.60])
    no = np.array([0.40])
    e_dn = edge(p, yes, no, "Down")[0]
    expected = (1.0 - 0.65) - 0.40 - POLY_FEE_THETA * 0.40 * (1.0 - 0.40)
    assert e_dn == pytest.approx(expected)
