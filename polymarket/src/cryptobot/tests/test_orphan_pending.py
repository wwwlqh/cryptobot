"""U15 orphan PENDING reconciler tests (carry-forward AE7)."""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot.execution.ledger import record_bet
from cryptobot.reconciliation.orphan_pending import reconcile_orphan_pending
from cryptobot.tests.test_slot_state import make_signal


def _expire(db: sqlite3.Connection, end_ts: int) -> None:
    db.execute("UPDATE ledger SET window_end_ts = ?", (end_ts,))


def test_reconcile_settles_orphan_pendings(db: sqlite3.Connection):
    # 3 PENDING rows whose window expired 30 min ago — all settle via mocked
    # Chainlink fetch.
    for i in range(3):
        record_bet(db, make_signal(bet_size=1.0, fill_price=0.55, window_start_ts=1_715_900_000 + i*60))
    _expire(db, end_ts=1_715_900_000)
    counters = reconcile_orphan_pending(
        db,
        now_ts=2_000_000_000.0,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    assert counters["settled"] == 3
    assert counters["cancelled"] == 0
    n_pending = db.execute(
        "SELECT COUNT(*) FROM ledger WHERE outcome='PENDING'"
    ).fetchone()[0]
    assert n_pending == 0


def test_partial_settlement_keeps_unsettled_pending(db: sqlite3.Connection):
    # Mix: 2 settle via Chainlink, 1 fails (upstream down for that row).
    for i in range(3):
        record_bet(db, make_signal(bet_size=1.0, fill_price=0.55, window_start_ts=1_715_900_000 + i*60))
    _expire(db, end_ts=1_715_900_000)

    calls = {"n": 0}
    def chainlink(coin, ts):
        calls["n"] += 1
        # First two rows resolve; the third finds the oracle down.
        if calls["n"] <= 2:
            return 70_000.0
        return None

    counters = reconcile_orphan_pending(
        db,
        now_ts=2_000_000_000.0,
        chainlink_fetch=chainlink,
    )
    assert counters["settled"] == 2
    assert counters["skipped"] == 1
    n_pending = db.execute(
        "SELECT COUNT(*) FROM ledger WHERE outcome='PENDING'"
    ).fetchone()[0]
    assert n_pending == 1


def test_no_expired_pending_does_nothing(db: sqlite3.Connection):
    record_bet(db, make_signal(bet_size=1.0))
    _expire(db, end_ts=3_000_000_000)   # well into the future
    counters = reconcile_orphan_pending(
        db,
        now_ts=2_000_000_000.0,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    # `pruned_signals` is an additive counter added when the sweep gained
    # a signal-retention pruning step (review #13). Assert the sweep-level
    # counters separately so future additive fields don't break this test.
    assert counters["settled"] == 0
    assert counters["cancelled"] == 0
    assert counters["skipped"] == 0
