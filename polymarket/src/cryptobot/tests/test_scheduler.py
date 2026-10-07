"""U10 scheduler tests — consecutive-failure CRITICAL + slow-tick WARN."""

from __future__ import annotations

import sqlite3
import time

import pytest

from cryptobot import config
from cryptobot.scheduler.tick import TickRunner
from cryptobot.execution.pipeline import TickResult


class _StubPortfolio:
    sleeves = ()


def _runner(conn: sqlite3.Connection, tick_fn) -> TickRunner:
    notifications = []
    runner = TickRunner(
        conn_factory=lambda: conn,
        portfolio=_StubPortfolio(),
        deps_factory=lambda: None,   # not used by the stub tick
        notify=lambda level, msg: notifications.append((level, msg)),
    )
    runner._notifications = notifications  # type: ignore[attr-defined]
    runner._stub_tick = tick_fn  # type: ignore[attr-defined]
    return runner


def test_successful_tick_resets_failure_counter(monkeypatch, db: sqlite3.Connection):
    runner = _runner(db, lambda: TickResult(tick_ts=1, duration_ms_total=10.0))
    monkeypatch.setattr(
        "cryptobot.scheduler.tick.run_scanner_tick",
        lambda *a, **kw: runner._stub_tick(),
    )
    runner.consecutive_failures = 3
    runner.run_one_tick()
    assert runner.consecutive_failures == 0
    assert runner.last_result is not None


def test_five_consecutive_errors_fire_critical(monkeypatch, db: sqlite3.Connection):
    def boom():
        raise RuntimeError("boom")

    runner = _runner(db, boom)
    monkeypatch.setattr(
        "cryptobot.scheduler.tick.run_scanner_tick",
        lambda *a, **kw: runner._stub_tick(),
    )

    # 4 errors — no CRITICAL yet.
    for _ in range(4):
        with pytest.raises(RuntimeError):
            runner.run_one_tick()
    crit = db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE status='CRITICAL'"
    ).fetchone()[0]
    assert crit == 0

    # 5th error fires CRITICAL once.
    with pytest.raises(RuntimeError):
        runner.run_one_tick()
    crit = db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE status='CRITICAL'"
    ).fetchone()[0]
    assert crit == 1
    levels = [n[0] for n in runner._notifications]  # type: ignore[attr-defined]
    assert levels == ["CRITICAL"]

    # 6th error MUST NOT duplicate the CRITICAL.
    with pytest.raises(RuntimeError):
        runner.run_one_tick()
    crit2 = db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE status='CRITICAL'"
    ).fetchone()[0]
    assert crit2 == 1


def test_slow_tick_logs_warn(monkeypatch, db: sqlite3.Connection):
    slow = lambda: TickResult(tick_ts=1, duration_ms_total=config.TICK_SOFT_DEADLINE_MS + 50.0)
    runner = _runner(db, slow)
    monkeypatch.setattr(
        "cryptobot.scheduler.tick.run_scanner_tick",
        lambda *a, **kw: runner._stub_tick(),
    )
    runner.run_one_tick()
    row = db.execute(
        "SELECT status, duration_ms FROM pipeline_health WHERE stage='tick' AND status='WARN'"
    ).fetchone()
    assert row is not None
    assert row["duration_ms"] >= config.TICK_SOFT_DEADLINE_MS


def test_timeout_skips_next_fire_while_orphan_running(
    monkeypatch, db: sqlite3.Connection
):
    """A hard-deadline timeout leaves the worker running in the background.
    The next scheduler fire must not start another scanner tick against
    shared state until the orphan finishes."""
    calls = {"n": 0}

    def slow_tick():
        calls["n"] += 1
        if calls["n"] == 1:
            time.sleep(0.25)
        return TickResult(tick_ts=1, duration_ms_total=10.0)

    runner = _runner(db, slow_tick)
    monkeypatch.setattr(config, "TICK_HARD_DEADLINE_MS", 20)
    monkeypatch.setattr(
        "cryptobot.scheduler.tick.run_scanner_tick",
        lambda *a, **kw: runner._stub_tick(),
    )

    with pytest.raises(TimeoutError):
        runner.run_one_tick()
    with pytest.raises(TimeoutError):
        runner.run_one_tick()
    assert calls["n"] == 1

    # Let the orphan finish, then a later fire can submit a fresh tick.
    assert runner.active_future is not None
    runner.active_future.result(timeout=1.0)
    runner.run_one_tick()
    assert calls["n"] == 2


def test_recovery_after_failures_resets_critical_flag(monkeypatch, db: sqlite3.Connection):
    state = {"fail": True}
    def maybe_fail():
        if state["fail"]:
            raise RuntimeError("boom")
        return TickResult(tick_ts=1, duration_ms_total=10.0)
    runner = _runner(db, maybe_fail)
    monkeypatch.setattr(
        "cryptobot.scheduler.tick.run_scanner_tick",
        lambda *a, **kw: runner._stub_tick(),
    )

    for _ in range(5):
        with pytest.raises(RuntimeError):
            runner.run_one_tick()
    assert runner.last_critical_count == 5

    state["fail"] = False
    runner.run_one_tick()
    assert runner.consecutive_failures == 0
    assert runner.last_critical_count == 0

    # If errors resume, a NEW CRITICAL should fire after 5 more.
    state["fail"] = True
    for _ in range(5):
        with pytest.raises(RuntimeError):
            runner.run_one_tick()
    crit = db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE status='CRITICAL'"
    ).fetchone()[0]
    assert crit == 2


def test_critical_alert_re_fires_at_every_threshold_multiple(
    monkeypatch, db: sqlite3.Connection
):
    """Reliability finding #4: once consecutive_failures crosses the
    threshold, additional CRITICAL alerts fire at every multiple
    (5, 10, 15, ...). Previously a single alert masked all subsequent
    failures until a successful tick reset the flag — the bot could
    fail indefinitely in silence."""
    notifications: list[tuple[str, str]] = []

    def always_fail():
        raise RuntimeError("boom")

    runner = _runner(db, always_fail)
    runner.notify = lambda level, msg: notifications.append((level, msg))
    monkeypatch.setattr(
        "cryptobot.scheduler.tick.run_scanner_tick",
        lambda *a, **kw: runner._stub_tick(),
    )

    for _ in range(15):
        with pytest.raises(RuntimeError):
            runner.run_one_tick()

    critical_notifies = [n for n in notifications if n[0] == "CRITICAL"]
    # Fires at consecutive_failures == 5, 10, 15.
    assert len(critical_notifies) == 3
    crit_rows = db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE status='CRITICAL'"
    ).fetchone()[0]
    assert crit_rows == 3


def test_start_scheduler_registers_resolution_sweep_when_provided(db: sqlite3.Connection):
    """Review #1 (the P0): without a registered sweep job, every PENDING
    row stays PENDING forever. start_scheduler MUST pass through the
    sweep job when the caller supplies one (main.py wires _build_sweep_job)."""
    from cryptobot.scheduler.tick import start_scheduler

    sweep_calls = {"n": 0}
    def _fake_sweep():
        sweep_calls["n"] += 1

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    # Don't actually let the scheduler tick — we only inspect the job list.
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,                 # effectively dormant for the test
        sweep_job=_fake_sweep,
        sweep_minutes=60,
    )
    try:
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert "scanner_tick" in job_ids
        assert "resolution_sweep" in job_ids
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_registers_order_reconciliation_when_provided(
    db: sqlite3.Connection,
):
    """Audit w19oxqf24 findings #3 + #6: reconcile_open_orders /
    reconcile_stale_attempts shipped but were never scheduled. start_scheduler
    MUST register the order-reconciliation job when the caller supplies one."""
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        order_recon_job=lambda: None,
        order_recon_seconds=60,
    )
    try:
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert "order_reconciliation" in job_ids
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_omits_order_reconciliation_when_not_provided(
    db: sqlite3.Connection,
):
    """No order_recon_job → no order_reconciliation job (Phase-1 DRY deploy)."""
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(runner, tick_seconds=3600)
    try:
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert "order_reconciliation" not in job_ids
    finally:
        scheduler.shutdown(wait=False)


def test_requote_drain_registered_at_fast_cadence(db: sqlite3.Connection):
    """2026-07-11: the requote drain ran at 60s, so the ~1s post-reject book
    re-read actually landed p50≈32s late — inside the last-30s entry window
    that re-read the near-terminal book, measuring the market OUTCOME instead
    of post-reject fillability. The drain must fire every ~1-2s."""
    from apscheduler.triggers.interval import IntervalTrigger
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        requote_drain_job=lambda: None,
    )
    try:
        job = scheduler.get_job("reject_requote_drain")
        assert job is not None
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval.total_seconds() <= 2
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_registers_clob_ping_when_provided(db: sqlite3.Connection):
    """Latency finding #8: the order-socket warmth ping must be registered
    when main.py supplies the job (OrderClient present) and the cadence is
    positive — without it the signing socket's warmth hangs on the 60s
    readiness fire vs keepalive_expiry=65s, a 5s margin."""
    from apscheduler.triggers.interval import IntervalTrigger
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        clob_ping_job=lambda: None,
        clob_ping_seconds=20,
    )
    try:
        job = scheduler.get_job("clob_ping")
        assert job is not None
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval.total_seconds() == 20
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_omits_clob_ping_when_disabled(db: sqlite3.Connection):
    """CRYPTOBOT_CLOB_PING_SECONDS=0 → no clob_ping job even when the caller
    supplies one (kill switch keeps the job set byte-identical)."""
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        clob_ping_job=lambda: None,
        clob_ping_seconds=0,
    )
    try:
        assert scheduler.get_job("clob_ping") is None
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_registers_gc_collect_when_provided(db: sqlite3.Connection):
    """Latency finding #6 / F8: the flag-gated periodic gc.collect() is
    registered through the start_scheduler param pattern (the clob_ping
    precedent) — start_scheduler is the single owner of the job set."""
    from apscheduler.triggers.interval import IntervalTrigger
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        gc_collect_job=lambda: None,
        gc_collect_seconds=60,
    )
    try:
        job = scheduler.get_job("gc_collect")
        assert job is not None
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval.total_seconds() == 60
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_omits_gc_collect_when_disabled(db: sqlite3.Connection):
    """CRYPTOBOT_GC_COLLECT_SECONDS=0 → no gc_collect job even when the
    caller supplies one; no job supplied (GC_TUNING off) → same. Flag-off
    wiring stays byte-identical."""
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        gc_collect_job=lambda: None,
        gc_collect_seconds=0,
    )
    try:
        assert scheduler.get_job("gc_collect") is None
    finally:
        scheduler.shutdown(wait=False)

    scheduler = start_scheduler(runner, tick_seconds=3600)
    try:
        assert scheduler.get_job("gc_collect") is None
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_omits_sweep_when_not_provided(db: sqlite3.Connection):
    """Bare call (no sweep_job) registers only the scanner tick — keeps
    existing test fixtures and the --once smoke path working."""
    from cryptobot.scheduler.tick import start_scheduler

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(runner, tick_seconds=3600)
    try:
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert job_ids == {"scanner_tick"}
    finally:
        scheduler.shutdown(wait=False)


def test_on_job_missed_callback_fires_on_missed_event(db: sqlite3.Connection):
    """Review-pass #18 closeout: synthesising an APScheduler
    ``EVENT_JOB_MISSED`` invokes the registered ``on_job_missed``
    callback. We don't try to provoke a real miss (timing-sensitive);
    instead we publish the event directly to the scheduler's event
    bus, which is exactly the path APScheduler itself takes on a
    real miss.
    """
    from apscheduler.events import EVENT_JOB_MISSED, JobExecutionEvent
    from cryptobot.scheduler.tick import start_scheduler

    received = []

    def _on_missed(job_id: str) -> None:
        received.append(job_id)

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        on_job_missed=_on_missed,
    )
    try:
        evt = JobExecutionEvent(
            EVENT_JOB_MISSED,
            "lut_reload",
            "default",
            scheduled_run_time=None,
        )
        scheduler._dispatch_event(evt)
        # _dispatch_event is synchronous so the listener has fired.
        assert received == ["lut_reload"]
    finally:
        scheduler.shutdown(wait=False)


def test_on_job_missed_callback_swallows_listener_errors(db: sqlite3.Connection):
    """A listener that raises must not break the scheduler. The
    observability path is best-effort."""
    from apscheduler.events import EVENT_JOB_MISSED, JobExecutionEvent
    from cryptobot.scheduler.tick import start_scheduler

    def _on_missed(_job_id: str) -> None:
        raise RuntimeError("synthetic listener failure")

    runner = _runner(db, lambda: TickResult(tick_ts=0, duration_ms_total=1.0))
    runner.deps_factory = lambda: None
    scheduler = start_scheduler(
        runner,
        tick_seconds=3600,
        on_job_missed=_on_missed,
    )
    try:
        evt = JobExecutionEvent(
            EVENT_JOB_MISSED,
            "scanner_tick",
            "default",
            scheduled_run_time=None,
        )
        # Must not raise.
        scheduler._dispatch_event(evt)
        # Scheduler still healthy.
        assert scheduler.running
    finally:
        scheduler.shutdown(wait=False)
