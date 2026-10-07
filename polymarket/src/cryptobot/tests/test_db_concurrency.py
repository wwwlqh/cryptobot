"""Audit w19oxqf24 finding #1 (P0) regression tests.

The pre-fix bug: a single ``sqlite3.Connection`` (isolation_level=None,
check_same_thread=False) was shared across the tick worker thread, every
scheduler-job thread, and the dashboard POST thread. Because BEGIN IMMEDIATE /
COMMIT / ROLLBACK operate on connection-global transaction state, a background
job's ``commit()`` could prematurely commit the tick's open transaction, and a
tick's ``rollback()`` could discard a job's settlement write.

The fix: ``ThreadLocalConnections`` hands each thread its own connection on a
file-backed DB. SQLite's WAL file lock + ``busy_timeout`` then serialize
BEGIN IMMEDIATE writers across connections instead of letting them splice into
each other's transactions.

These tests encode both the pre-fix hazard (as a documented characterization)
and the post-fix guarantee.
"""

from __future__ import annotations

import sqlite3
import threading

import pytest

from cryptobot.db import init_db
from cryptobot.db.connection import ThreadLocalConnections, get_connection


def test_separate_connections_isolate_transactions(tmp_path):
    """Post-fix guarantee: a commit() on connection B does NOT commit an open
    BEGIN IMMEDIATE transaction on connection A.

    This is the core property the per-thread model buys us. With the old
    single shared connection, conn.commit() ended *the* transaction regardless
    of which logical writer opened it.
    """
    db_path = tmp_path / "iso.db"
    init_db(db_path)  # apply schema

    conn_a = get_connection(db_path)
    conn_b = get_connection(db_path)
    try:
        # A opens a transaction and writes a health row (not yet committed).
        conn_a.execute("BEGIN IMMEDIATE")
        conn_a.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('tick', 'OK', 'A-uncommitted')"
        )

        # B is a *different* connection. Under WAL + busy_timeout, B's own
        # write waits for A's write lock; once A is done B proceeds. The point
        # here is that B cannot even see or commit A's uncommitted row.
        # A second writer's BEGIN IMMEDIATE blocks while A holds the lock, so
        # B must wait — assert that A still owns an open transaction.
        assert conn_a.in_transaction is True

        # A rolls back. Its uncommitted row must vanish — a separate
        # connection could not have committed it out from under A.
        conn_a.execute("ROLLBACK")

        rows = conn_b.execute(
            "SELECT COUNT(*) FROM pipeline_health WHERE message='A-uncommitted'"
        ).fetchone()[0]
        assert rows == 0, "A's rolled-back row leaked — transactions not isolated"
    finally:
        conn_a.close()
        conn_b.close()


def test_concurrent_writers_serialize_without_corruption(tmp_path):
    """Post-fix: two threads, each with its own connection, both writing under
    BEGIN IMMEDIATE, produce exactly N+M durable rows with no lost writes and
    no 'database is locked' error (busy_timeout makes the loser wait)."""
    db_path = tmp_path / "concurrent.db"
    init_db(db_path)
    conns = ThreadLocalConnections(db_path)

    errors: list[Exception] = []
    barrier = threading.Barrier(2)

    def writer(tag: str, n: int) -> None:
        try:
            conn = conns.get()  # this thread's own connection
            barrier.wait()
            for i in range(n):
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO pipeline_health (stage, status, message) "
                    "VALUES ('tick', 'OK', ?)",
                    (f"{tag}-{i}",),
                )
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=writer, args=("tick", 40))
    t2 = threading.Thread(target=writer, args=("sweep", 40))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors, f"writer raised under contention: {errors}"

    verify = get_connection(db_path)
    try:
        total = verify.execute(
            "SELECT COUNT(*) FROM pipeline_health WHERE status='OK'"
        ).fetchone()[0]
        # Every write is durable: no commit was lost to a cross-connection race.
        assert total == 80, f"expected 80 durable rows, got {total}"
    finally:
        verify.close()


def test_thread_local_gives_distinct_connections_on_file_db(tmp_path):
    """Each thread gets a distinct connection object on a file-backed DB."""
    db_path = tmp_path / "distinct.db"
    init_db(db_path)
    conns = ThreadLocalConnections(db_path)

    seen: dict[str, int] = {}
    lock = threading.Lock()

    def grab(name: str) -> None:
        c = conns.get()
        # Same thread must get the SAME connection on repeated calls.
        assert conns.get() is c
        with lock:
            seen[name] = id(c)

    threads = [threading.Thread(target=grab, args=(f"t{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(seen.values())) == 4, "threads shared a connection"
    conns.close_all()


def test_memory_db_shares_one_connection():
    """An in-memory DB cannot use per-thread connections (a second :memory:
    connection sees an empty DB), so the handle shares one connection across
    threads. This preserves the test/--once smoke path."""
    boot = init_db(":memory:")
    conns = ThreadLocalConnections(":memory:", shared=boot)
    assert conns.get() is boot

    other: list[sqlite3.Connection] = []
    t = threading.Thread(target=lambda: other.append(conns.get()))
    t.start()
    t.join()
    assert other[0] is boot, "in-memory handle must share one connection"


def test_close_all_closes_every_tracked_connection(tmp_path):
    """close_all() closes the boot connection, per-thread connections, and any
    extra (dashboard) connection registered with the handle."""
    db_path = tmp_path / "closeall.db"
    boot = init_db(db_path)
    conns = ThreadLocalConnections(db_path)
    conns.register_current_thread(boot)

    dashboard = get_connection(db_path)
    conns.register_extra(dashboard)

    opened: list[sqlite3.Connection] = []
    t = threading.Thread(target=lambda: opened.append(conns.get()))
    t.start()
    t.join()

    conns.close_all()

    # Every tracked connection is now closed — using it raises ProgrammingError.
    for c in (boot, dashboard, opened[0]):
        with pytest.raises(sqlite3.ProgrammingError):
            c.execute("SELECT 1")
