"""U15a Phase-1 E2E foundation tests.

Per the 2026-05-24-001 per-strategy LIVE deploy plan Â§U15a:

  - boot path: init_db â†’ load_sleeves â†’ backfill_wallet_groups â†’
    seed_operator_control_state â†’ seed_wallet_modes.
  - mode flip audit: DRY â†’ LIVE â†’ DRY preserves immutability of prior
    PENDING ledger rows (D-LIVE-3 + D-LIVE-6).
  - parity: existing DRY ledger rows survive the schema migration; the
    backtest parity invariant (D1 + D14 + D20) is unchanged because
    `unit_pnl` / `cash_pnl` formulas don't read the new `mode` column.

The Phase 2 E2E tests (live order routing, group cap, transfer lock)
land alongside U6 + U10. This file covers ONLY the foundation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    BlockedError,
    boot_operator_state,
    get_operator_state,
    get_wallet_mode,
    seed_operator_control_state,
    seed_wallet_modes,
    set_wallet_mode,
    start_processing,
)
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio


@pytest.fixture
def boot_db(tmp_path: Path) -> sqlite3.Connection:
    """End-to-end boot sequence â€” matches what main.py does."""
    db_path = tmp_path / "boot.db"
    conn = init_db(db_path)
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    yield conn
    conn.close()


def _seed_readiness_and_recon(conn: sqlite3.Connection) -> None:
    """Insert fresh READY rows so live_action_guard.require_wallet_can_go_live
    passes â€” Phase 2 (U7 + U9) will populate these automatically."""
    now = utc_now_sql()
    conn.execute(
        "INSERT INTO live_readiness_reports (created_at, status, report_json) "
        "VALUES (?, 'READY', '{}')",
        (now,),
    )
    conn.execute(
        "INSERT INTO wallet_reconciliation_runs (sampled_at, source_status) "
        "VALUES (?, 'ok')",
        (now,),
    )
    conn.commit()


# ============================================================================
# Boot sequence
# ============================================================================


class TestBootSequence:
    def test_boot_seeds_four_wallets_all_dry(
        self, boot_db: sqlite3.Connection
    ) -> None:
        rows = boot_db.execute(
            "SELECT wallet_id, mode FROM wallet_mode_state ORDER BY wallet_id"
        ).fetchall()
        wallets = {r["wallet_id"]: r["mode"] for r in rows}
        assert wallets == {
            "v6_5m": "DRY",
            "v6_15m": "DRY",
            "pairs_5m": "DRY",
            "pairs_15m": "DRY",
        }

    def test_boot_seeds_operator_state_stopped_processing(
        self, boot_db: sqlite3.Connection
    ) -> None:
        state = get_operator_state(boot_db)
        assert state.state == "STOPPED_PROCESSING"
        assert state.boot_dry_run is True
        assert state.version == 0

    def test_boot_records_audit_event_for_first_seed(
        self, boot_db: sqlite3.Connection
    ) -> None:
        # No events from seed_operator_control_state itself (INSERT OR IGNORE
        # doesn't write events). But explicit transitions afterward do.
        events = boot_db.execute(
            "SELECT COUNT(*) AS n FROM operator_control_events"
        ).fetchone()
        assert events["n"] == 0

    def test_boot_is_idempotent(self, tmp_path: Path) -> None:
        """Re-running boot on the same DB preserves prior state â€” operator
        toggles from the FIRST boot survive the SECOND."""
        db_path = tmp_path / "idem.db"
        portfolio = legacy_portfolio()

        # First boot.
        conn = init_db(db_path)
        boot_operator_state(conn, portfolio, boot_dry_run=True)
        # Operator presses Start.
        start_processing(conn, reason="ready")
        conn.close()

        # Second boot (simulates restart).
        conn = init_db(db_path)
        boot_operator_state(conn, portfolio, boot_dry_run=True)
        try:
            state = get_operator_state(conn)
            assert state.state == "LIVE", (
                "second boot should preserve operator's prior LIVE state"
            )
        finally:
            conn.close()

    def test_boot_writes_no_ledger_rows(self, boot_db: sqlite3.Connection) -> None:
        """Boot is observation-free â€” no ledger rows written without an
        actual signal."""
        n = boot_db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        assert n == 0


# ============================================================================
# Mode flip audit + immutability (D-LIVE-3 + D-LIVE-6)
# ============================================================================


class TestModeFlipAudit:
    def test_dry_to_live_to_dry_audit_trail(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """Three flips â†’ three wallet-scoped audit events with the correct
        from/to states (D-LIVE-25 single audit table)."""
        start_processing(boot_db)
        _seed_readiness_and_recon(boot_db)

        # DRY â†’ LIVE.
        set_wallet_mode(
            boot_db,
            "pairs_15m",
            "LIVE",
            actor="cli",
            reason="canary promote",
        )
        # LIVE â†’ DRY (no cooldown, no guard).
        set_wallet_mode(
            boot_db,
            "pairs_15m",
            "DRY",
            actor="dashboard",
            reason="halt for review",
        )
        # DRY â†’ LIVE again with force (within cooldown).
        set_wallet_mode(
            boot_db,
            "pairs_15m",
            "LIVE",
            actor="cli",
            reason="resume after review",
            force=True,
        )

        events = boot_db.execute(
            "SELECT from_state, to_state, actor, reason "
            "FROM operator_control_events "
            "WHERE scope='wallet:pairs_15m' "
            "ORDER BY id ASC"
        ).fetchall()
        assert len(events) == 3
        assert (events[0]["from_state"], events[0]["to_state"]) == ("DRY", "LIVE")
        assert (events[1]["from_state"], events[1]["to_state"]) == ("LIVE", "DRY")
        assert (events[2]["from_state"], events[2]["to_state"]) == ("DRY", "LIVE")
        assert events[0]["reason"] == "canary promote"
        assert events[1]["actor"] == "dashboard"

    def test_pending_ledger_row_mode_immutable_across_flip(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """D-LIVE-3 + D-LIVE-6: a row written under mode='LIVE' keeps its
        mode column even after the wallet flips back to DRY. Per D-LIVE-6
        the F3 resolution sweep settles all PENDING rows via Polymarket
        Gamma regardless of mode, so the immutable mode column is purely
        forensic â€” but the assertion below is what underwrites that
        invariant for the audit trail."""
        start_processing(boot_db)
        _seed_readiness_and_recon(boot_db)

        # Promote v6_5m to LIVE.
        set_wallet_mode(boot_db, "v6_5m", "LIVE", reason="phase 3 canary")

        # Write a fake LIVE PENDING ledger row (Phase 2 will do this via the
        # pipeline, but the schema invariant is testable now).
        boot_db.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
               window_start_ts, window_end_ts, interval_min, poly_market_id,
               poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
               realized_edge, target_usd, bet_size, entry_top_price,
               config_sha256, mode, wallet_group)
            VALUES
              ('2026-05-24 14:04:00', 'BTC', 'v6_5m', 'v6_5m', 4, 'YES',
               1715923200, 1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55,
               0.55, 0.10, 5.0, 5.0, 0.55, 'sha-stub', 'LIVE', 'group_5m')
            """,
        )
        boot_db.commit()

        # Operator halts the wallet.
        set_wallet_mode(boot_db, "v6_5m", "DRY", reason="halt")

        # The PENDING row's mode is still LIVE â€” the flip does NOT rewrite
        # historical rows.
        row = boot_db.execute(
            "SELECT mode, wallet_group, outcome FROM ledger LIMIT 1"
        ).fetchone()
        assert row["mode"] == "LIVE"
        assert row["wallet_group"] == "group_5m"
        assert row["outcome"] == "PENDING"

    def test_concurrent_dashboard_clicks_one_loses(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """Optimistic-concurrency `version` token â€” when two writers race
        on the same wallet, the stale one raises and must refetch."""
        from cryptobot.execution.operator_control import (
            ConcurrentMutationError,
            DRY,
        )

        start_processing(boot_db)
        _seed_readiness_and_recon(boot_db)

        # Reader A reads version=0.
        snapshot = get_wallet_mode(boot_db, "pairs_15m")
        assert snapshot.version == 0

        # Reader B flips DRY â†’ LIVE. Reads version=0, writes version=1.
        set_wallet_mode(boot_db, "pairs_15m", "LIVE", reason="B")

        # Reader A's stale write at expected_version=0 must fail.
        with pytest.raises(ConcurrentMutationError):
            set_wallet_mode(
                boot_db,
                "pairs_15m",
                "DRY",
                expected_version=snapshot.version,
            )

        # The winning state is what Reader B wrote.
        assert get_wallet_mode(boot_db, "pairs_15m").mode == "LIVE"


# ============================================================================
# Backfill behavior on legacy ledger rows
# ============================================================================


class TestLegacyLedgerBackfill:
    def test_backfill_runs_after_load_sleeves(self, tmp_path: Path) -> None:
        """The plan's D-LIVE-15 sequencing: schema migration is portfolio-free,
        and the JSON-derived walletâ†’group backfill runs AFTER legacy_portfolio()
        once the source of truth exists."""
        db_path = tmp_path / "backfill.db"
        conn = init_db(db_path)
        # Pre-migration row: legacy wallet_id stamp but no wallet_group.
        conn.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
               window_start_ts, window_end_ts, interval_min, poly_market_id,
               poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
               realized_edge, target_usd, bet_size, entry_top_price,
               config_sha256)
            VALUES
              ('2026-05-17 14:04:00', 'BTC', 'v6_5m', 'v6_5m', 4, 'YES',
               1715923200, 1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55,
               0.55, 0.10, 5.0, 5.0, 0.55, 'sha-stub')
            """,
        )
        conn.commit()

        # Verify wallet_group is NULL before backfill.
        before = conn.execute(
            "SELECT wallet_group FROM ledger LIMIT 1"
        ).fetchone()["wallet_group"]
        assert before is None

        portfolio = legacy_portfolio()
        counts = backfill_wallet_groups(conn, portfolio)
        try:
            after = conn.execute(
                "SELECT wallet_group, mode FROM ledger LIMIT 1"
            ).fetchone()
            assert after["wallet_group"] == "group_5m"
            assert after["mode"] == "DRY"  # ALTER TABLE default applied
            assert counts["ledger"] == 1
        finally:
            conn.close()

    def test_backfill_handles_pair_leg_sleeves(self, tmp_path: Path) -> None:
        """Pair-leg rows: sleeve='pairs_5m_long' / pair_component='pairs_5m'
        â€” both must end up in `group_5m` per the wallet_group_for() lookup."""
        db_path = tmp_path / "pair_backfill.db"
        conn = init_db(db_path)
        for sleeve, pair_component in (
            ("pairs_5m_long", "pairs_5m"),
            ("pairs_5m_short", "pairs_5m"),
            ("pairs_15m_long", "pairs_15m"),
            ("pairs_15m_short", "pairs_15m"),
        ):
            conn.execute(
                """
                INSERT INTO ledger
                  (bet_ts, coin, sleeve, pair_component, entry_minute, side,
                   window_start_ts, window_end_ts, interval_min, poly_market_id,
                   poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
                   realized_edge, target_usd, bet_size, entry_top_price,
                   config_sha256)
                VALUES
                  ('2026-05-17 14:04:00', 'BTC', ?, ?, 4, 'YES',
                   1715923200, 1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55,
                   0.55, 0.10, 5.0, 5.0, 0.55, 'sha-stub')
                """,
                (sleeve, pair_component),
            )
        conn.commit()

        portfolio = legacy_portfolio()
        backfill_wallet_groups(conn, portfolio)
        try:
            rows = conn.execute(
                "SELECT sleeve, wallet_group FROM ledger ORDER BY sleeve"
            ).fetchall()
            assert {r["sleeve"]: r["wallet_group"] for r in rows} == {
                "pairs_15m_long": "group_15m",
                "pairs_15m_short": "group_15m",
                "pairs_5m_long": "group_5m",
                "pairs_5m_short": "group_5m",
            }
        finally:
            conn.close()


# ============================================================================
# Backtest parity sanity (DRY rows survive the migration)
# ============================================================================


class TestParitySurvivesMigration:
    """The schema migration adds DEFAULT 'DRY' to ledger.mode, so every
    legacy row reads back as DRY. The backtest parity test (D1 + D14 + D20
    in `test_backtest_parity.py`) operates on formulas, not the ledger,
    so it's unaffected â€” but this test pins the schema invariant that
    underpins it."""

    def test_new_ledger_row_without_mode_kwarg_defaults_to_dry(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """`record_bet` (in Â§U6 of the plan) will add `mode` + `wallet_group`
        kwargs. Until then, the existing `record_bet` writes rows without
        those kwargs â€” the schema DEFAULT must keep the rows valid."""
        boot_db.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
               window_start_ts, window_end_ts, interval_min, poly_market_id,
               poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
               realized_edge, target_usd, bet_size, entry_top_price,
               config_sha256)
            VALUES
              ('2026-05-24 14:04:00', 'BTC', 'v6_5m', 'v6_5m', 4, 'YES',
               1715923200, 1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55,
               0.55, 0.10, 5.0, 5.0, 0.55, 'sha-stub')
            """,
        )
        row = boot_db.execute(
            "SELECT mode, wallet_group, pair_orphan FROM ledger"
        ).fetchone()
        assert row["mode"] == "DRY"
        assert row["pair_orphan"] == 0

    def test_all_existing_ledger_rows_read_as_dry_after_migration(
        self, tmp_path: Path
    ) -> None:
        """Hand-rolled pre-migration schema â†’ INSERT a DRY row â†’
        init_db re-migrates â†’ row reads back as mode='DRY'."""
        path = tmp_path / "legacy.db"
        conn = sqlite3.connect(path)
        try:
            conn.executescript(
                """
                CREATE TABLE ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bet_ts TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    wallet_id TEXT,
                    sleeve TEXT NOT NULL,
                    entry_minute INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    window_start_ts INTEGER NOT NULL,
                    window_end_ts INTEGER NOT NULL,
                    interval_min INTEGER NOT NULL,
                    poly_market_id TEXT NOT NULL,
                    poly_token_id TEXT NOT NULL,
                    fee_rate_bps REAL NOT NULL,
                    model_p REAL NOT NULL,
                    p_market REAL NOT NULL,
                    fill_price REAL NOT NULL,
                    realized_edge REAL NOT NULL,
                    target_usd REAL NOT NULL,
                    bet_size REAL NOT NULL,
                    slot_filled_pre REAL NOT NULL DEFAULT 0,
                    is_topup INTEGER NOT NULL DEFAULT 0,
                    entry_top_price REAL NOT NULL,
                    outcome TEXT NOT NULL DEFAULT 'PENDING',
                    config_sha256 TEXT NOT NULL,
                    ghost_fill INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO ledger
                  (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
                   window_start_ts, window_end_ts, interval_min,
                   poly_market_id, poly_token_id, fee_rate_bps, model_p,
                   p_market, fill_price, realized_edge, target_usd, bet_size,
                   entry_top_price, config_sha256)
                VALUES
                  ('2026-04-01 12:00:00', 'ETH', 'v6_5m', 'v6_5m', 4, 'YES',
                   1715923200, 1715923500, 5, 'M', 'T', 100.0, 0.7, 0.5,
                   0.5, 0.05, 2.5, 2.5, 0.5, 'legacy-sha');
                """,
            )
            conn.commit()
        finally:
            conn.close()

        migrated = init_db(path)
        try:
            row = migrated.execute(
                "SELECT mode, wallet_group FROM ledger LIMIT 1"
            ).fetchone()
            # Pre-migration row, post-ALTER TABLE: mode defaults to DRY.
            assert row["mode"] == "DRY"
            # wallet_group is still NULL â€” the backfill helper hasn't run.
            assert row["wallet_group"] is None
        finally:
            migrated.close()


# ============================================================================
# Live action guard end-to-end gate
# ============================================================================


class TestLiveActionGuardE2E:
    def test_phase1_promote_attempt_blocked_by_no_readiness(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """Phase 1 has no `live_readiness_reports` writer (lands in U9).
        Every DRY â†’ LIVE attempt fails closed â€” operator can't promote
        until Phase 2 deploys the readiness scheduler."""
        start_processing(boot_db)  # state=LIVE
        with pytest.raises(BlockedError) as exc:
            set_wallet_mode(boot_db, "v6_5m", "LIVE")
        assert exc.value.reason == "no_readiness_report"
        # Wallet is still DRY.
        assert get_wallet_mode(boot_db, "v6_5m").mode == "DRY"

    def test_promote_succeeds_when_phase2_seeded_readiness_and_recon(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """Phase 2 simulation: insert fresh READY rows â†’ promote succeeds."""
        start_processing(boot_db)
        _seed_readiness_and_recon(boot_db)
        out = set_wallet_mode(boot_db, "v6_5m", "LIVE", reason="phase 2 sim")
        assert out.mode == "LIVE"
        assert out.version == 1

    def test_halt_always_succeeds_even_with_no_readiness(
        self, boot_db: sqlite3.Connection
    ) -> None:
        """The emergency halt path (LIVE â†’ DRY) bypasses both the guard
        AND the rate limit â€” D-LIVE-5."""
        start_processing(boot_db)
        _seed_readiness_and_recon(boot_db)
        set_wallet_mode(boot_db, "v6_5m", "LIVE", reason="promote")

        # Force the readiness check to fail by NOT_READYing the row.
        boot_db.execute(
            "UPDATE live_readiness_reports SET status='NOT_READY'"
        )
        boot_db.commit()

        # LIVE â†’ DRY still succeeds.
        out = set_wallet_mode(boot_db, "v6_5m", "DRY", reason="halt")
        assert out.mode == "DRY"
