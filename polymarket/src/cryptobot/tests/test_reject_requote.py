"""Reject re-quote telemetry: insert + deferred drain (stub book fetch) +
settlement backfill. No network, no order placement."""
from __future__ import annotations

import queue
import types

import pytest

from cryptobot_polymarket.gamma import (
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
)
from cryptobot.execution import reject_requote
from cryptobot.execution.reject_requote import (
    _interval_min_from_sleeve,
    backfill_reject_requote_outcomes,
    drain_requote_queue,
    enqueue_requote_reread,
    insert_reject_requote,
)


@pytest.fixture(autouse=True)
def _drain_module_queue():
    """The re-quote queue is a module-level global — empty it around each test."""
    while True:
        try:
            reject_requote._REQUOTE_QUEUE.get_nowait()
        except queue.Empty:
            break
    yield
    while True:
        try:
            reject_requote._REQUOTE_QUEUE.get_nowait()
        except queue.Empty:
            break


def _insert(db, **kw):
    defaults = dict(
        coin="BTC",
        sleeve="subminute_5m",
        offset_sec=270,
        window_start_ts=1_000_000,
        side="YES",
        limit_price=0.62,
        requested_usd=5.0,
    )
    defaults.update(kw)
    rid = insert_reject_requote(db, **defaults)
    db.commit()
    return rid


def test_insert_persists_intent(db):
    rid = _insert(db)
    assert rid is not None
    row = db.execute(
        "SELECT * FROM reject_requotes WHERE id = ?", (rid,)
    ).fetchone()
    assert row["coin"] == "BTC"
    assert row["sleeve"] == "subminute_5m"
    assert row["requested_usd"] == 5.0
    assert row["requote_ask"] is None
    assert row["requote_age_ms"] is None
    assert row["resolved_outcome"] is None


def test_drain_updates_requote_ask_and_age(db):
    rid = _insert(db)
    # Ready immediately: enqueue stamped at monotonic t=100, drain at t=101.5.
    enqueue_requote_reread(rid, "token-abc", delay_s=0.0, now_monotonic=100.0)

    def _stub_fetch(token_id, *, now_ts):
        assert token_id == "token-abc"
        return types.SimpleNamespace(top_ask=0.66)

    counters = drain_requote_queue(
        db, fetch_book=_stub_fetch, now_monotonic=101.5, now_ts=123.0
    )
    assert counters["updated"] == 1
    row = db.execute(
        "SELECT requote_ask, requote_age_ms FROM reject_requotes WHERE id = ?",
        (rid,),
    ).fetchone()
    assert row["requote_ask"] == 0.66
    assert row["requote_age_ms"] == 1500  # (101.5 - 100.0) * 1000


def test_drain_book_gone_records_null_ask_with_age(db):
    rid = _insert(db)
    enqueue_requote_reread(rid, "token-abc", delay_s=0.0, now_monotonic=100.0)

    def _stub_gone(token_id, *, now_ts):
        return None  # book vanished (the ghost-quote case)

    counters = drain_requote_queue(
        db, fetch_book=_stub_gone, now_monotonic=100.5, now_ts=1.0
    )
    assert counters["updated"] == 1
    row = db.execute(
        "SELECT requote_ask, requote_age_ms FROM reject_requotes WHERE id = ?",
        (rid,),
    ).fetchone()
    assert row["requote_ask"] is None
    assert row["requote_age_ms"] == 500


def test_drain_requeues_items_not_yet_ready(db):
    rid = _insert(db)
    # ready_at is in the FUTURE relative to the drain clock.
    enqueue_requote_reread(rid, "token-abc", delay_s=1.0, now_monotonic=100.0)
    calls = []

    def _stub(token_id, *, now_ts):
        calls.append(token_id)
        return types.SimpleNamespace(top_ask=0.5)

    counters = drain_requote_queue(
        db, fetch_book=_stub, now_monotonic=100.5, now_ts=1.0
    )
    assert counters["updated"] == 0
    assert counters["requeued"] == 1
    assert calls == []  # never re-read
    # A later drain past the ready time processes it.
    counters2 = drain_requote_queue(
        db, fetch_book=_stub, now_monotonic=101.5, now_ts=1.0
    )
    assert counters2["updated"] == 1
    assert calls == ["token-abc"]


class _RecordingLock:
    """Context-manager lock stub recording whether/when it is held."""

    def __init__(self):
        self.held = False
        self.acquisitions = 0

    def __enter__(self):
        self.held = True
        self.acquisitions += 1
        return self

    def __exit__(self, *exc):
        self.held = False
        return False


def test_drain_fetches_book_outside_write_lock(db):
    """Fast-cadence drain (2026-07-11): the shared write lock must never be
    held across the HTTP book re-read — only around the batched UPDATE."""
    rid = _insert(db)
    enqueue_requote_reread(rid, "token-abc", delay_s=0.0, now_monotonic=100.0)
    lock = _RecordingLock()
    held_during_fetch = []

    def _stub(token_id, *, now_ts):
        held_during_fetch.append(lock.held)
        return types.SimpleNamespace(top_ask=0.7)

    counters = drain_requote_queue(
        db, fetch_book=_stub, now_monotonic=101.0, now_ts=1.0, write_lock=lock
    )
    assert counters["updated"] == 1
    assert held_during_fetch == [False]  # fetch ran with the lock released
    assert lock.acquisitions == 1        # the batched write took it once
    row = db.execute(
        "SELECT requote_ask, requote_age_ms FROM reject_requotes WHERE id = ?",
        (rid,),
    ).fetchone()
    assert row["requote_ask"] == 0.7
    assert row["requote_age_ms"] == 1000


def test_drain_skips_write_lock_when_nothing_to_write(db):
    """The ~2s cadence makes the no-op drain the common case; it must not
    touch the write lock at all — neither on an empty queue nor when every
    queued item is still waiting out its re-read delay."""
    lock = _RecordingLock()
    counters = drain_requote_queue(
        db, fetch_book=lambda *a, **k: None, now_monotonic=100.0, now_ts=1.0,
        write_lock=lock,
    )
    assert counters == {"updated": 0, "requeued": 0, "processed": 0}
    assert lock.acquisitions == 0

    rid = _insert(db)
    enqueue_requote_reread(rid, "token-abc", delay_s=1.0, now_monotonic=100.0)
    counters = drain_requote_queue(
        db, fetch_book=lambda *a, **k: None, now_monotonic=100.5, now_ts=1.0,
        write_lock=lock,
    )
    assert counters == {"updated": 0, "requeued": 1, "processed": 0}
    assert lock.acquisitions == 0


def test_drain_swallows_fetch_errors(db):
    rid = _insert(db)
    enqueue_requote_reread(rid, "token-abc", delay_s=0.0, now_monotonic=100.0)

    def _boom(token_id, *, now_ts):
        raise RuntimeError("clob down")

    # Must not raise; row stays un-updated.
    counters = drain_requote_queue(
        db, fetch_book=_boom, now_monotonic=101.0, now_ts=1.0
    )
    assert counters["updated"] == 0
    row = db.execute(
        "SELECT requote_ask FROM reject_requotes WHERE id = ?", (rid,)
    ).fetchone()
    assert row["requote_ask"] is None


def test_enqueue_rejects_bad_input():
    assert enqueue_requote_reread(1, "") is False
    assert enqueue_requote_reread(None, "tok") is False


@pytest.mark.parametrize(
    "sleeve,expected",
    [
        ("subminute_5m", 5),
        ("subminute_15m", 15),
        ("baseline_15m", 15),
        ("no_interval_here", None),
        ("", None),
    ],
)
def test_interval_min_from_sleeve(sleeve, expected):
    assert _interval_min_from_sleeve(sleeve) == expected


def test_backfill_fills_resolved_outcome(db):
    # Expired 5m window: start 1000 -> end 1300; now = 2000.
    rid_up = _insert(db, window_start_ts=1000, sleeve="subminute_5m")
    rid_down = _insert(db, window_start_ts=1000, sleeve="subminute_15m",
                       coin="ETH")

    def _resolve(coin, interval_min, window_start_ts):
        won = coin == "BTC"  # BTC up, ETH down
        return types.SimpleNamespace(status=RESOLUTION_RESOLVED, won_up=won)

    n = backfill_reject_requote_outcomes(
        db, polymarket_resolution_fetch=_resolve, now_ts=100_000.0
    )
    assert n == 2
    assert db.execute(
        "SELECT resolved_outcome FROM reject_requotes WHERE id = ?", (rid_up,)
    ).fetchone()["resolved_outcome"] == "UP"
    assert db.execute(
        "SELECT resolved_outcome FROM reject_requotes WHERE id = ?", (rid_down,)
    ).fetchone()["resolved_outcome"] == "DOWN"


def test_backfill_skips_unexpired_and_open_markets(db):
    now = 2000.0
    # Not expired: window_end = 1900 + 300 = 2200 > now.
    rid_future = _insert(db, window_start_ts=1900, sleeve="subminute_5m")
    # Expired but still OPEN on Gamma.
    rid_open = _insert(db, window_start_ts=1000, sleeve="subminute_5m", coin="ETH")

    def _resolve(coin, interval_min, window_start_ts):
        return types.SimpleNamespace(status=RESOLUTION_OPEN, won_up=None)

    n = backfill_reject_requote_outcomes(
        db, polymarket_resolution_fetch=_resolve, now_ts=now
    )
    assert n == 0
    for rid in (rid_future, rid_open):
        assert db.execute(
            "SELECT resolved_outcome FROM reject_requotes WHERE id = ?", (rid,)
        ).fetchone()["resolved_outcome"] is None
