from __future__ import annotations

import numpy as np

from polymarket.backtest.scripts.build_twap_labels import (
    FIVE_MINUTE_60S_CUTOVER_TS,
    mean_between,
    point_at,
    twap_seconds,
)


def test_twap_proxy_uses_trailing_boundary_window() -> None:
    secs = np.arange(90, 131, dtype=np.int64)
    prices = secs.astype(float)

    opening, count = mean_between(
        secs, prices, 90, 100, lo_inclusive=False, hi_inclusive=True
    )

    assert count == 10
    assert opening == np.mean(np.arange(91, 101, dtype=float))


def test_trailing_open_can_use_previous_day_observation() -> None:
    midnight = 1_800_000_000
    secs = np.array([midnight - 2, midnight - 1, midnight], dtype=np.int64)
    prices = np.array([10.0, 11.0, 12.0])

    opening, count = mean_between(
        secs, prices, midnight - 3, midnight,
        lo_inclusive=False, hi_inclusive=True,
    )

    assert count == 3
    assert opening == 11.0


def test_point_price_still_uses_last_observation_at_or_before_boundary() -> None:
    secs = np.array([97, 99, 101], dtype=np.int64)
    prices = np.array([10.0, 11.0, 12.0])

    price, stale = point_at(secs, prices, 100, max_stale=5)

    assert price == 11.0
    assert stale == 1.0


def test_twap_seconds_preserves_pre_cutover_history_and_uses_60s_now() -> None:
    assert twap_seconds("5m", FIVE_MINUTE_60S_CUTOVER_TS - 300) == 30
    assert twap_seconds("5m", FIVE_MINUTE_60S_CUTOVER_TS) == 60
    assert twap_seconds("15m", FIVE_MINUTE_60S_CUTOVER_TS - 900) == 60
