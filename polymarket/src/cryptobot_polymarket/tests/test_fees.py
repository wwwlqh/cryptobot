"""Polymarket venue fee-per-share: the PARABOLA.

Split out of the old dual-venue ``predictfun/tests/test_predictfun_fees.py`` in
Phase 2, along with the shape itself.
"""

from __future__ import annotations

import pytest

from cryptobot_polymarket.fees import poly_fee_per_share


def test_parabola_matches_polymarket_inline_formula():
    # Must reproduce theta*p*(1-p) with the identical float-op order so a
    # venue-dispatched Polymarket call site is byte-identical to the inline
    # formula the walker / the frozen parity chain compute.
    theta = 0.07
    for p in (0.55, 0.65, 0.80, 0.92):
        assert poly_fee_per_share(p, theta) == theta * p * (1.0 - p)


def test_parabola_is_symmetric_and_peaks_at_the_middle():
    theta = 0.07
    assert poly_fee_per_share(0.80, theta) == pytest.approx(
        poly_fee_per_share(0.20, theta)
    )
    assert poly_fee_per_share(0.50, theta) > poly_fee_per_share(0.80, theta)


def test_adapter_requires_theta():
    # Fail-closed on the DECISION path: a Polymarket fee without theta is a
    # missing config value, not a default to guess at.
    from cryptobot.venues import get_adapter

    with pytest.raises(ValueError):
        get_adapter("polymarket").fee_per_share(0.80)


def test_adapter_walk_fee_fn_is_none():
    # None == "walk_book keeps its built-in parabola", i.e. the walker's
    # arithmetic is untouched on this venue.
    from cryptobot.venues import get_adapter

    assert get_adapter("polymarket").walk_fee_fn() is None
