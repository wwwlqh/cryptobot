from __future__ import annotations

import pytest

from cryptobot.strategy.partial_ret import PartialRetEngine
from polymarket.backtest.studies.tick_latency.run_twap_exact_aug7 import (
    _live_twap_z,
)


def test_exact_era_replay_z_matches_live_source_corrected_formula():
    engine = PartialRetEngine()
    state = engine._coin("btc")
    state.rv_1h_at_open_15m = 0.002
    state.window_start_twap_15m = 64_000.0

    live = engine.current_z_from_open(
        "btc",
        interval_min=15,
        live_spot=64_250.0,
        open_price=63_950.0,
        entry_minute=14.5,
        basis_center_log=0.0007705237138092435,
        basis_residual_clip_log=0.0001,
    )
    replay = _live_twap_z(
        live_spot=64_250.0,
        binance_twap_open=64_000.0,
        chainlink_open=63_950.0,
        rv_open=0.002,
        entry_minute=14.5,
        basis_center_log=0.0007705237138092435,
        basis_residual_clip_log=0.0001,
    )

    assert replay == pytest.approx(live)
