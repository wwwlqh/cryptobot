"""Test-only fixtures and helpers.

Anything here lives outside the production import surface — the live
scanner must never import from ``cryptobot.tests``. Moved here in the
2026-05-18 review pass (#7): a ``seed_for_tests`` helper that exists
solely to fast-forward ``PartialRetEngine`` past its 30-minute warmup
floor for unit/integration tests should not ship inside the production
``cryptobot.strategy.partial_ret`` module.
"""

from __future__ import annotations

from cryptobot.strategy.partial_ret import (
    PartialRetEngine,
    _floor_to,
)


def seed_for_tests(
    engine: PartialRetEngine,
    coin: str,
    *,
    now_ts: int,
    spot: float,
    rv_1h: float = 1e-3,
) -> None:
    """Test-only helper to fast-forward an engine past its warmup floor.

    Production tick path requires ~30+ minutes of 1m close history before
    ``current_z`` returns a value. Unit/integration tests that exercise
    downstream scoring need a usable engine without simulating an hour
    of ticks — this helper stamps the per-interval anchors and the rv
    snapshots directly to the supplied values.

    Uses ``engine._coin`` (single-underscore internal) intentionally:
    crossing that boundary from a test-fixture module is more acceptable
    than from the production module itself.
    """
    cs = engine._coin(coin)
    w5 = _floor_to(now_ts, 300)
    w15 = _floor_to(now_ts, 900)
    cs.window_start_ts_5m = w5
    cs.window_start_spot_5m = spot
    cs.rv_1h_at_open_5m = rv_1h
    cs.window_start_ts_15m = w15
    cs.window_start_spot_15m = spot
    cs.rv_1h_at_open_15m = rv_1h
    cs.last_tick_spot = spot
    cs.last_tick_ts = now_ts
    cs.last_minute_ts = _floor_to(now_ts, 60)
    cs.last_minute_close = spot
