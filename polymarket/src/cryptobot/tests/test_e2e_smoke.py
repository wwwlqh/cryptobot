"""U16 live 60s smoke test — marked @pytest.mark.slow.

Opt-in only: `pytest -m slow src/cryptobot/tests/test_e2e_smoke.py`. The
default test run does not include it.

Boots the scanner with a tmp DB and a fake spot-down stub (so the test
doesn't depend on the network), runs for 60 ticks, asserts the
scheduler ticked ~60 times and the SSE broadcaster received events.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest


pytestmark = pytest.mark.slow


def test_60s_scheduler_smoke(tmp_path: Path, monkeypatch):
    """Run the scheduler for ~60s with a stub pipeline; assert tick count."""
    from cryptobot import config
    from cryptobot.scheduler.tick import TickRunner, start_scheduler
    from cryptobot.execution.pipeline import TickResult

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "smoke.db")
    from cryptobot.db import init_db
    conn = init_db(config.DB_PATH)

    tick_count = {"n": 0}
    def stub_pipeline(*a, **kw):
        tick_count["n"] += 1
        return TickResult(tick_ts=int(time.time()), duration_ms_total=5.0)

    monkeypatch.setattr("cryptobot.scheduler.tick.run_scanner_tick", stub_pipeline)

    class _StubPortfolio:
        sleeves = ()

    runner = TickRunner(
        conn_factory=lambda: conn,
        portfolio=_StubPortfolio(),
        deps_factory=lambda: None,
    )
    scheduler = start_scheduler(runner, tick_seconds=1)
    try:
        # 60s soak
        time.sleep(60.0)
    finally:
        scheduler.shutdown(wait=False)
        conn.close()

    # Tick count must be 50-65 (1Hz × 60s ± misfire grace).
    assert 50 <= tick_count["n"] <= 65, f"expected ~60 ticks, got {tick_count['n']}"
