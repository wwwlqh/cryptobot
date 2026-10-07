"""U4 verification: live-action guard table reads.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U4 (D-LIVE-5):

  - require_processing_enabled fails when operator state != LIVE.
  - require_no_transfer_lock fails when state == TRANSFER_LOCK.
  - require_fresh_live_readiness fails on no row / NOT_READY / stale row.
  - require_fresh_wallet_reconciliation fails on no row / stale row.
  - require_wallet_can_go_live composes all four; earliest-failing reason
    wins so dashboard surfaces show the single most actionable error.

Phase 1: live_readiness_reports + wallet_reconciliation_runs writers
don't exist yet — so every test seeds the rows manually to validate the
behavior the Phase 2 writers will produce.
"""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot.db import init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.live_action_guard import (
    require_fresh_live_readiness,
    require_fresh_wallet_reconciliation,
    require_no_transfer_lock,
    require_processing_enabled,
    require_wallet_can_go_live,
)
from cryptobot.execution.operator_control import (
    BlockedError,
    enter_transfer_lock,
    seed_operator_control_state,
    start_processing,
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    seed_operator_control_state(db, boot_dry_run=True)
    # Seed a wallet_mode_state row for the v6_5m wallet used in the
    # composite gate tests. require_wallet_can_go_live now rejects
    # unknown wallet_ids (maintainability finding #12) so the fixture
    # must register the wallet the tests reference.
    db.execute(
        "INSERT OR IGNORE INTO wallet_mode_state "
        "(wallet_id, mode, updated_by, version) "
        "VALUES ('v6_5m', 'DRY', 'fixture', 0)"
    )
    db.commit()
    yield db
    db.close()


def _insert_readiness(
    conn: sqlite3.Connection,
    *,
    status: str = "READY",
    created_at: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO live_readiness_reports "
        "(created_at, status, report_json) VALUES (?, ?, '{}')",
        (created_at or utc_now_sql(), status),
    )
    conn.commit()


def _insert_recon(
    conn: sqlite3.Connection,
    *,
    sampled_at: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, source_status) VALUES (?, 'ok')",
        (sampled_at or utc_now_sql(),),
    )
    conn.commit()


# ============================================================================
# Global state gates
# ============================================================================


class TestRequireProcessingEnabled:
    def test_passes_when_state_live(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        require_processing_enabled(conn)  # no raise

    def test_blocks_when_stopped(self, conn: sqlite3.Connection) -> None:
        # Fresh seed defaults to STOPPED_PROCESSING.
        with pytest.raises(BlockedError) as exc:
            require_processing_enabled(conn)
        assert exc.value.reason == "operator_stopped"

    def test_blocks_when_transfer_lock(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        enter_transfer_lock(conn)
        with pytest.raises(BlockedError) as exc:
            require_processing_enabled(conn)
        assert exc.value.reason == "transfer_lock"


class TestRequireNoTransferLock:
    def test_passes_when_not_transfer_lock(self, conn: sqlite3.Connection) -> None:
        require_no_transfer_lock(conn)
        start_processing(conn)
        require_no_transfer_lock(conn)

    def test_blocks_when_transfer_lock(self, conn: sqlite3.Connection) -> None:
        start_processing(conn)
        enter_transfer_lock(conn)
        with pytest.raises(BlockedError) as exc:
            require_no_transfer_lock(conn)
        assert exc.value.reason == "transfer_lock"


# ============================================================================
# Readiness gate
# ============================================================================


class TestRequireFreshLiveReadiness:
    def test_blocks_when_no_row(self, conn: sqlite3.Connection) -> None:
        with pytest.raises(BlockedError) as exc:
            require_fresh_live_readiness(conn)
        assert exc.value.reason == "no_readiness_report"

    def test_blocks_when_not_ready(self, conn: sqlite3.Connection) -> None:
        _insert_readiness(conn, status="NOT_READY")
        with pytest.raises(BlockedError) as exc:
            require_fresh_live_readiness(conn)
        assert exc.value.reason == "readiness_not_ready"

    def test_blocks_when_stale(self, conn: sqlite3.Connection) -> None:
        _insert_readiness(conn, created_at="2026-01-01 00:00:00")
        with pytest.raises(BlockedError) as exc:
            require_fresh_live_readiness(conn)
        assert exc.value.reason == "readiness_stale"

    def test_passes_when_fresh_ready(self, conn: sqlite3.Connection) -> None:
        _insert_readiness(conn, status="READY")
        require_fresh_live_readiness(conn)

    def test_custom_ttl_respected(self, conn: sqlite3.Connection) -> None:
        # 5-second TTL with a ~zero-second-old row should pass.
        _insert_readiness(conn, status="READY")
        require_fresh_live_readiness(conn, ttl_s=5)


# ============================================================================
# Wallet reconciliation gate
# ============================================================================


class TestRequireFreshWalletReconciliation:
    def test_blocks_when_no_row(self, conn: sqlite3.Connection) -> None:
        with pytest.raises(BlockedError) as exc:
            require_fresh_wallet_reconciliation(conn)
        assert exc.value.reason == "wallet_recon_missing"

    def test_blocks_when_stale(self, conn: sqlite3.Connection) -> None:
        _insert_recon(conn, sampled_at="2026-01-01 00:00:00")
        with pytest.raises(BlockedError) as exc:
            require_fresh_wallet_reconciliation(conn)
        assert exc.value.reason == "wallet_recon_stale"

    def test_passes_when_fresh(self, conn: sqlite3.Connection) -> None:
        _insert_recon(conn)
        require_fresh_wallet_reconciliation(conn)


# ============================================================================
# Composite guard
# ============================================================================


class TestRequireWalletCanGoLive:
    def test_blocks_with_operator_stopped_first(
        self, conn: sqlite3.Connection
    ) -> None:
        # Earliest-failing reason wins — operator_stopped before
        # no_readiness_report.
        with pytest.raises(BlockedError) as exc:
            require_wallet_can_go_live(conn, "v6_5m")
        assert exc.value.reason == "operator_stopped"

    def test_blocks_with_no_readiness_when_state_live(
        self, conn: sqlite3.Connection
    ) -> None:
        start_processing(conn)
        # No live_readiness_reports row → readiness gate fires.
        with pytest.raises(BlockedError) as exc:
            require_wallet_can_go_live(conn, "v6_5m")
        assert exc.value.reason == "no_readiness_report"

    def test_blocks_with_wallet_recon_missing_after_readiness_ok(
        self, conn: sqlite3.Connection
    ) -> None:
        start_processing(conn)
        _insert_readiness(conn, status="READY")
        with pytest.raises(BlockedError) as exc:
            require_wallet_can_go_live(conn, "v6_5m")
        assert exc.value.reason == "wallet_recon_missing"

    def test_blocks_with_transfer_lock_before_readiness_check(
        self, conn: sqlite3.Connection
    ) -> None:
        start_processing(conn)
        enter_transfer_lock(conn)
        # Even with fresh readiness + recon, transfer lock blocks first.
        _insert_readiness(conn, status="READY")
        _insert_recon(conn)
        with pytest.raises(BlockedError) as exc:
            require_wallet_can_go_live(conn, "v6_5m")
        assert exc.value.reason == "transfer_lock"

    def test_passes_when_all_gates_satisfied(
        self, conn: sqlite3.Connection
    ) -> None:
        start_processing(conn)
        _insert_readiness(conn, status="READY")
        _insert_recon(conn)
        require_wallet_can_go_live(conn, "v6_5m")

    def test_blocks_unknown_wallet_id(self, conn: sqlite3.Connection) -> None:
        """Maintainability finding #12: wallet_id is now a real gate. A
        typo or stale wallet_id is rejected before any state / readiness
        / recon checks fire."""
        start_processing(conn)
        _insert_readiness(conn, status="READY")
        _insert_recon(conn)
        with pytest.raises(BlockedError) as exc:
            require_wallet_can_go_live(conn, "v6_5m_TYPO")
        assert exc.value.reason == "unknown_wallet"

    def test_blocks_empty_wallet_id(self, conn: sqlite3.Connection) -> None:
        with pytest.raises(BlockedError) as exc:
            require_wallet_can_go_live(conn, "")
        assert exc.value.reason == "unknown_wallet"
