"""Shared fixtures for the cryptobot test suite.

`db` provides an in-memory SQLite connection with the production schema
already applied. Tests that need a path-backed DB (e.g. WAL pragma assertions
that ":memory:" cannot satisfy) should use the `tmp_db` fixture instead.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from cryptobot import config
from cryptobot.db import init_db


@pytest.fixture(autouse=True)
def _clear_gamma_market_cache() -> Iterator[None]:
    """A2 latency: polymarket_gamma caches successful market resolutions
    per (coin, interval, window) at module level for the window's lifetime.
    Tests reuse the same window timestamps across cases, so without this
    reset a market cached by one test short-circuits the next test's HTTP
    mock (call-count assertions see 0 calls; fee-cache paths never run).

    Also clears snapshot_builder's latency/subminute block cache (keyed
    per-conn + 5s TTL, but per-test isolation should never depend on id()
    reuse timing)."""
    from cryptobot_polymarket.gamma import clear_market_cache
    from cryptobot.observability.snapshot_builder import clear_aux_cache

    clear_market_cache()
    clear_aux_cache()
    yield
    clear_market_cache()
    clear_aux_cache()


@pytest.fixture(autouse=True)
def _neutralize_entry_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin config.ENTRY_SKIP_SECONDS=0 for the whole suite by default.

    The entry-skip lever suppresses all bet placement during the first N
    seconds of every minute. Every run_scanner_tick fixture in this suite
    drives ticks at exact-minute timestamps (now_ts % 60 == 0), so a nonzero N
    would short-circuit them all before scoring and break the scoring / dedup /
    parity assertions they were written for. N=0 is also the live default
    (2026-06-07: no skip, so live fills at the minute boundary aligned with the
    ORB+HGB backtest), so this fixture now matches production rather than
    overriding it. Tests that exercise the gate itself set
    config.ENTRY_SKIP_SECONDS explicitly inside the test.
    """
    monkeypatch.setattr(config, "ENTRY_SKIP_SECONDS", 0)


@pytest.fixture
def db() -> Iterator[sqlite3.Connection]:
    """In-memory SQLite connection with schema + migrations applied."""
    conn = init_db(":memory:")
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def tmp_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Path-backed SQLite connection so WAL pragma is meaningful."""
    conn = init_db(tmp_path / "cryptobot_test.db")
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def start_scheduler_jobs():
    """``{job_id: Job}`` for a ``start_scheduler(**kw)`` call.

    The shape every "is this optional job wired?" test needs: main.py owns
    whether a job is passed at all, so the assertion is "this id is (not) in the
    registered set" — and for the ones that care, the Job itself carries the
    trigger cadence. Shared here because several suites (fast-trigger,
    miss-forensics, ...) each grew their own copy, and the scheduler is always
    shut down in a finally so no APScheduler thread outlives the test.
    """
    from types import SimpleNamespace

    from cryptobot.scheduler.tick import start_scheduler

    def _jobs(runner=None, **kw) -> dict:
        sched = start_scheduler(
            runner or SimpleNamespace(run_one_tick=lambda: None), **kw
        )
        try:
            return {j.id: j for j in sched.get_jobs()}
        finally:
            sched.shutdown(wait=False)

    return _jobs
