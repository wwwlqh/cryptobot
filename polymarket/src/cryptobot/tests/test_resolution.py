"""U7a resolution-fetch tests — sanity bounds + deviation guard."""

from __future__ import annotations

import pytest

from cryptobot.execution.resolution import (
    ResolutionError,
    fetch_resolution_price,
)


def test_chainlink_success():
    res = fetch_resolution_price(
        "BTC", 1_715_923_500,
        chainlink_fetch=lambda c, t: 67_500.0,
    )
    assert res is not None
    assert res.source == "chainlink"
    assert res.price == pytest.approx(67_500.0)


def test_upstream_down_returns_none():
    res = fetch_resolution_price(
        "BTC", 1_715_923_500,
        chainlink_fetch=lambda c, t: None,
    )
    assert res is None


def test_out_of_bounds_raises():
    # BTC bounds are (1_000, 10_000_000); 0 is out.
    with pytest.raises(ResolutionError) as exc_info:
        fetch_resolution_price(
            "BTC", 1_715_923_500,
            chainlink_fetch=lambda c, t: 0.0,
        )
    assert exc_info.value.code == "out_of_bounds"


def test_excessive_deviation_raises():
    # BTC reference $67k, candidate $30k → > 50% deviation.
    with pytest.raises(ResolutionError) as exc_info:
        fetch_resolution_price(
            "BTC", 1_715_923_500,
            reference_spot=67_000.0,
            chainlink_fetch=lambda c, t: 30_000.0,
        )
    assert exc_info.value.code == "excessive_deviation"


def test_within_deviation_passes():
    res = fetch_resolution_price(
        "BTC", 1_715_923_500,
        reference_spot=67_000.0,
        chainlink_fetch=lambda c, t: 68_500.0,   # 2.2% — well under 50% cap
    )
    assert res is not None


def test_unknown_coin_rejected():
    # Coin not in RESOLUTION_PRICE_BOUNDS → fails sanity check.
    with pytest.raises(ResolutionError) as exc_info:
        fetch_resolution_price(
            "ZZZ", 1_715_923_500,
            chainlink_fetch=lambda c, t: 100.0,
        )
    assert exc_info.value.code == "out_of_bounds"
