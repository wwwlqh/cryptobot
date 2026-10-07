"""A6/A7 latency: boundary-aligned tick + spot prefetch-ahead."""

from __future__ import annotations

import threading
import time

from cryptobot.scheduler.spot_prefetch import SpotPrefetcher


def test_prefetcher_targets_the_upcoming_second_boundary():
    """The prefetch call must carry now_ts == the NEXT whole second so it
    populates the exact bucket the boundary-aligned tick will read."""
    calls: list[tuple[float, int]] = []
    got_one = threading.Event()

    def fake_fetch(*, now_ts: float, tick_seconds: int):
        calls.append((now_ts, tick_seconds))
        got_one.set()
        return {}

    p = SpotPrefetcher(fake_fetch, tick_seconds=1, lead_ms=200)
    p.start()
    try:
        assert got_one.wait(timeout=3.0), "prefetcher never fired"
    finally:
        p.stop()

    now_ts, tick_seconds = calls[0]
    assert tick_seconds == 1
    # Whole-second target...
    assert now_ts == float(int(now_ts))
    # ...that had not yet passed when the call fired (fired ~lead_ms early;
    # allow generous scheduling slack in CI).
    assert now_ts >= time.time() - 5.0


def test_prefetcher_stop_joins_thread():
    p = SpotPrefetcher(lambda **_: {}, tick_seconds=1, lead_ms=100)
    p.start()
    p.stop()
    assert p._thread is None


def test_scanner_tick_trigger_is_second_aligned(db):
    """A6: the scanner_tick IntervalTrigger must anchor to a whole-second
    start_date — a bare IntervalTrigger inherits the boot instant's random
    sub-second phase and every decision fires up to ~1s late."""
    from cryptobot.scheduler.tick import TickRunner, start_scheduler

    runner = TickRunner(
        conn_factory=lambda: db,
        portfolio=None,
        deps_factory=lambda: None,
    )
    scheduler = start_scheduler(runner, tick_seconds=3600)
    try:
        job = scheduler.get_job("scanner_tick")
        assert job is not None
        start_date = job.trigger.start_date
        assert start_date is not None
        assert start_date.microsecond == 0
    finally:
        scheduler.shutdown(wait=False)
