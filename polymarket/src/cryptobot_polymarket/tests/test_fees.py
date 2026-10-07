"""Polymarket taker fee: theta * p * (1 - p)."""

from __future__ import annotations

import pytest

from cryptobot.venues.fees import fee_per_share


def test_fee_matches_inline_formula():
    theta = 0.07
    for p in (0.55, 0.65, 0.80, 0.92):
        assert fee_per_share(p, theta=theta) == theta * p * (1.0 - p)


def test_fee_is_symmetric_and_peaks_at_the_middle():
    theta = 0.07
    assert fee_per_share(0.80, theta=theta) == pytest.approx(
        fee_per_share(0.20, theta=theta)
    )
    assert fee_per_share(0.50, theta=theta) > fee_per_share(0.80, theta=theta)
