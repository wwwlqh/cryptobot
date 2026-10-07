"""U4 verification: global operator state + audit events.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U4:
  - operator_control_state singleton defaults to STOPPED_PROCESSING (safe).
  - seed_operator_control_state is idempotent.
  - set_operator_state uses optimistic-concurrency `version` tokens.
  - Every transition emits an `operator_control_events` row with
    scope='global' (D-LIVE-25).
  - start_processing / stop_processing / enter_transfer_lock /
    clear_transfer_lock are thin wrappers around set_operator_state.

These tests deliberately do NOT exercise the per-wallet mode surface —
that's covered by test_wallet_mode_state.py.
"""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot.db import init_db
from cryptobot.execution.operator_control import (
    ConcurrentMutationError,
    OperatorControlError,
    STOPPED_PROCESSING,
    TRANSFER_LOCK,
    clear_transfer_lock,
    enter_transfer_lock,
    get_operator_state,
    release_stale_transfer_lock,
    seed_operator_control_state,
    set_operator_state,
    start_processing,
    stop_processing,
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    seed_operator_control_state(db, boot_dry_run=True)
    yield db
    db.close()


class TestSingletonDefaults:
    def test_fresh_db_seeds_stopped_processing(self, conn: sqlite3.Connection) -> None:
        state = get_operator_state(conn)
        assert state.state == "STOPPED_PROCESSING"
        assert state.boot_dry_run is True
        assert state.version == 0

    def test_seed_is_idempotent(self, conn: sqlite3.Connection) -> None:
        # Re-seeding finds the row already present and leaves it alone.
        first = get_operator_state(conn)
        second = seed_operator_control_state(conn, boot_dry_run=False)
        # boot_dry_run on second call is ignored because the row exists.
        assert second.state == first.state
        assert second.version == first.version

    def test_get_operator_state_raises_without_seed(self) -> None:
        """`get_operator_state` raises when no seed has happened. Phase 1
        boot path always seeds; this test pins the failure mode for
        future regression hunts."""
        db = init_db(":memory:")
        try:
            with pytest.raises(OperatorControlError, match="row missing"):
                get_operator_state(db)
        finally:
            db.close()


class TestTransitions:
    def test_start_processing_goes_live(self, conn: sqlite3.Connection) -> None:
        out = start_processing(conn, reason="ready to trade")
        assert out.state == "LIVE"
        assert out.version == 1
        assert out.reason == "ready to trade"
        assert out.processing_enabled is True

    def test_stop_processing_goes_stopped(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        out = stop_processing(conn, reason="manual halt")
        assert out.state == "STOPPED_PROCESSING"
        assert out.version == 2
        assert out.processing_enabled is False

    def test_enter_transfer_lock(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        out = enter_transfer_lock(conn)
        assert out.state == "TRANSFER_LOCK"
        assert out.transfer_locked is True

    def test_clear_transfer_lock_returns_to_live(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        enter_transfer_lock(conn)
        out = clear_transfer_lock(conn)
        assert out.state == "LIVE"

    def test_rejects_unsupported_state(self, conn: sqlite3.Connection) -> None:
        with pytest.raises(OperatorControlError, match="unsupported"):
            set_operator_state(conn, "MAINTENANCE")


class TestOptimisticConcurrency:
    def test_stale_expected_version_raises(self, conn: sqlite3.Connection) -> None:
        # Simulate two writers: writer A reads version=0, writer B flips
        # to LIVE (version=1). Writer A then submits with expected_version=0.
        start_processing(conn)  # bumps to version=1
        with pytest.raises(ConcurrentMutationError, match="version mismatch"):
            set_operator_state(conn, "STOPPED_PROCESSING", expected_version=0)

    def test_matching_expected_version_succeeds(
        self, conn: sqlite3.Connection
    ) -> None:
        out = set_operator_state(conn, "LIVE", expected_version=0)
        assert out.state == "LIVE"
        assert out.version == 1

    def test_no_expected_version_uses_current(self, conn: sqlite3.Connection) -> None:
        # When `expected_version` is omitted, the call reads current and
        # always succeeds (single-writer pattern used by CLI tools).
        out = set_operator_state(conn, "LIVE")
        assert out.state == "LIVE"


class TestAuditEvents:
    def test_each_transition_writes_global_scoped_event(
        self, conn: sqlite3.Connection
    ) -> None:
        start_processing(conn, reason="start")
        stop_processing(conn, reason="stop")
        events = conn.execute(
            "SELECT scope, action, from_state, to_state, actor, reason "
            "FROM operator_control_events ORDER BY id ASC"
        ).fetchall()
        assert [e["scope"] for e in events] == ["global", "global"]
        assert [e["action"] for e in events] == ["set_live", "set_stopped_processing"]
        assert events[0]["from_state"] == "STOPPED_PROCESSING"
        assert events[0]["to_state"] == "LIVE"

    def test_actor_recorded_on_event(self, conn: sqlite3.Connection) -> None:
        start_processing(conn, actor="cli", reason="boot test")
        event = conn.execute(
            "SELECT actor, reason FROM operator_control_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert event["actor"] == "cli"
        assert event["reason"] == "boot test"


class TestPublicPayload:
    def test_to_public_dict_includes_version(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        payload = get_operator_state(conn).to_public_dict()
        assert payload["state"] == "LIVE"
        assert payload["version"] == 1
        assert payload["processingEnabled"] is True
        assert payload["transferLocked"] is False


# ============================================================================
# Stale-state watchdogs
#
# TestReleaseStaleTransferLock arrived here verbatim from
# cryptobot_polymarket/tests/test_transfer.py when its subject moved to core
# (panel verdict D4-1). It only ever used core symbols.
# ============================================================================


class TestReleaseStaleTransferLock:
    def test_no_op_when_not_locked(self, conn: sqlite3.Connection) -> None:
        assert release_stale_transfer_lock(conn) is None

    def test_no_op_when_fresh_lock(self, conn: sqlite3.Connection) -> None:
        enter_transfer_lock(conn)
        assert release_stale_transfer_lock(conn, max_age_minutes=15) is None
        # Still TRANSFER_LOCK.
        assert get_operator_state(conn).state == TRANSFER_LOCK

    def test_transitions_to_stopped_when_stale(
        self, conn: sqlite3.Connection
    ) -> None:
        enter_transfer_lock(conn)
        # Backdate the lock so it's "stale".
        conn.execute(
            "UPDATE operator_control_state SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=1"
        )
        conn.commit()
        reason = release_stale_transfer_lock(conn, max_age_minutes=15)
        assert reason == "transfer_lock_timeout"
        # Safe recovery target is STOPPED, not LIVE.
        assert get_operator_state(conn).state == STOPPED_PROCESSING
