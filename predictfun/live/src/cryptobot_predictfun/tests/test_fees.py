"""predict.fun venue fee-per-share: the TENT.

Phase 2 split this file. The dispatcher's tests moved to
``cryptobot/tests/test_venue_fees.py`` (core owns the dispatch now) and the
Polymarket parabola's to ``cryptobot_polymarket/tests/test_fees.py``. What
remains is this venue's own shape.
"""

from __future__ import annotations

import pytest

from cryptobot_predictfun import fees


def test_tent_shape_symmetric_and_linear():
    # (fee_rate_bps/10000) * min(p, 1-p). 200 bps => 2%.
    assert fees.predictfun_fee_per_share(0.80, 200.0) == pytest.approx(0.02 * 0.20)
    assert fees.predictfun_fee_per_share(0.20, 200.0) == pytest.approx(0.02 * 0.20)
    # Symmetric about 0.5, peaks at the middle.
    assert fees.predictfun_fee_per_share(0.50, 200.0) == pytest.approx(0.02 * 0.50)


def test_per_market_bps_override_changes_fee():
    assert fees.predictfun_fee_per_share(0.70, 200.0) == pytest.approx(0.02 * 0.30)
    assert fees.predictfun_fee_per_share(0.70, 400.0) == pytest.approx(0.04 * 0.30)


def test_default_bps_reads_config():
    from cryptobot import config

    assert fees.PREDICTFUN_DEFAULT_FEE_RATE_BPS == pytest.approx(
        float(config.PREDICTFUN_DEFAULT_FEE_RATE_BPS)
    )


def test_adapter_defaults_bps_when_missing():
    from cryptobot.venues import get_adapter

    got = get_adapter("predictfun").fee_per_share(0.70)
    expected = fees.predictfun_fee_per_share(
        0.70, fees.PREDICTFUN_DEFAULT_FEE_RATE_BPS
    )
    assert got == pytest.approx(expected)
