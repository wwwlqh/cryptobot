"""U1 verification: schema + pragmas + slot index + config sanity.

Plan §U1 / §Backtest ↔ Dry-Run Divergence Audit (D6, D20):
  - config.POLY_FEE_THETA must equal backtest/sweep_lib.POLY_FEE_THETA
  - config.SHARED_EXPOSURE_CAP must equal the live dry-run artifact fallback cap
  - schema must include the first-class slot key columns and idx_ledger_slot

Per-strategy LIVE deploy plan §U1 (2026-05-24-001):
  - ledger gains mode + wallet_group + LIVE CLOB linkage + pair_orphan
    + top_ask_at_decision (D-LIVE-3, D-LIVE-22, D-LIVE-28, D-LIVE-29)
  - signals gains mode + wallet_group for skip-audit context
  - new tables: wallet_mode_state, operator_control_state,
    operator_control_events (single audit per D-LIVE-25),
    live_readiness_reports, bankroll_peak, capital_flows,
    wallet_reconciliation_runs/records, live_order_attempts,
    transfer_requests (with source_wallet_id)
  - backfill_wallet_groups(conn, portfolio) stamps legacy rows from the
    portfolio's wallet_group_for(wallet_id) mapping
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pytest

from cryptobot import config
from cryptobot.db import backfill_wallet_groups
# schema.sql travels WITH the package (db/connection.py owns the path), so the
# legacy-migration tests read it from there instead of a CWD-relative literal.
from cryptobot.db.connection import _SCHEMA_PATH


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _indexes(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
        (table,),
    ).fetchall()
    return {row[0] for row in rows}


class TestLedgerSchema:
    def test_slot_key_columns_first_class(self, db: sqlite3.Connection) -> None:
        cols = _columns(db, "ledger")
        for c in ("coin", "sleeve", "entry_minute", "side", "window_start_ts"):
            assert c in cols, f"slot key column {c} missing from ledger"

    def test_new_columns_present(self, db: sqlite3.Connection) -> None:
        cols = _columns(db, "ledger")
        required = {
            "is_topup",
            "config_sha256",
            "add_on_realized_edge_minus_initial_edge",
            "entry_top_price",
            "slot_filled_pre",
            "walked_vwap",
            "yes_pre1_anchor",
            "pair_group_id",
            "pair_role",
            "pair_component",
            "paired_coin",
            "wallet_id",
        }
        assert required.issubset(cols), f"missing: {required - cols}"

    def test_outcome_default_pending(self, db: sqlite3.Connection) -> None:
        db.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, sleeve, entry_minute, side, window_start_ts, window_end_ts,
               interval_min, poly_market_id, poly_token_id, fee_rate_bps,
               model_p, p_market, fill_price, realized_edge,
               target_usd, bet_size, entry_top_price, config_sha256)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-05-17 14:04:00",
                "BTC",
                "v6_5m",
                4,
                "YES",
                1715923200,
                1715923500,
                5,
                "MARKET_ID",
                "TOKEN_ID",
                100.0,
                0.78,
                0.55,
                0.55,
                0.10,
                5.0,
                5.0,
                0.55,
                "sha256-stub",
            ),
        )
        outcome = db.execute("SELECT outcome FROM ledger LIMIT 1").fetchone()[0]
        assert outcome == "PENDING"


class TestSchemaIndexes:
    def test_idx_ledger_slot_exists(self, db: sqlite3.Connection) -> None:
        idx = _indexes(db, "ledger")
        assert "idx_ledger_slot" in idx, "composite slot index missing"
        assert "idx_ledger_pair_group" in idx, "pair group index missing"
        assert "idx_ledger_wallet_outcome" in idx, "wallet outcome index missing"

    def test_signal_wallet_index_exists(self, db: sqlite3.Connection) -> None:
        cols = _columns(db, "signals")
        idx = _indexes(db, "signals")
        assert "wallet_id" in cols
        assert "idx_signals_wallet_created" in idx

    def test_idx_ledger_slot_columns(self, db: sqlite3.Connection) -> None:
        info = db.execute("PRAGMA index_info(idx_ledger_slot)").fetchall()
        cols = [row[2] for row in info]
        assert cols == ["coin", "sleeve", "entry_minute", "side", "window_start_ts", "outcome"]


class TestEntryScanLatches:
    def test_entry_scan_latches_primary_key(self, db: sqlite3.Connection) -> None:
        cols = _columns(db, "entry_scan_latches")
        assert {"created_at", "coin", "sleeve", "entry_minute", "window_start_ts"} <= cols

        db.execute(
            "INSERT INTO entry_scan_latches "
            "(coin, sleeve, entry_minute, window_start_ts) VALUES (?, ?, ?, ?)",
            ("BTC", "exp044_5m", 2, 1_715_923_200),
        )
        db.execute(
            "INSERT OR IGNORE INTO entry_scan_latches "
            "(coin, sleeve, entry_minute, window_start_ts) VALUES (?, ?, ?, ?)",
            ("BTC", "exp044_5m", 2, 1_715_923_200),
        )
        assert db.execute("SELECT COUNT(*) FROM entry_scan_latches").fetchone()[0] == 1


class TestPragmas:
    def test_wal_mode(self, tmp_db: sqlite3.Connection) -> None:
        mode = tmp_db.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"

    def test_foreign_keys_on(self, tmp_db: sqlite3.Connection) -> None:
        fk = tmp_db.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1

    def test_wal_autocheckpoint_raised_off_hot_path(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        # Latency item #4: the default 1000-page autocheckpoint ran the
        # WAL→db fsync inline on whichever writer crossed it (usually the
        # tick's own autocommit). The raised bound keeps the inline trigger
        # effectively unreachable; the periodic PASSIVE job drains instead.
        pages = tmp_db.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
        assert int(pages) == 20000

    def test_passive_wal_checkpoint_drains_wal(self, tmp_path) -> None:
        """The scheduler-job helper must checkpoint committed WAL frames into
        the main db without raising — the fsync happens on the caller's
        (scheduler-pool) thread, never the tick's."""
        from cryptobot.db import init_db
        from cryptobot.db.connection import passive_wal_checkpoint

        conn = init_db(tmp_path / "walck.db")
        try:
            for i in range(50):
                conn.execute(
                    "INSERT INTO pipeline_health (stage, status, message) "
                    "VALUES ('tick', 'OK', ?)",
                    (f"wal-{i}",),
                )
            conn.commit()
            passive_wal_checkpoint(conn)
            # busy=0 (nothing blocked a PASSIVE pass on a quiet db) and every
            # WAL frame checkpointed (log == checkpointed).
            busy, log_frames, ckpt_frames = conn.execute(
                "PRAGMA wal_checkpoint(PASSIVE)"
            ).fetchone()
            assert busy == 0
            assert log_frames == ckpt_frames
        finally:
            conn.close()

    def test_independent_connections(self, tmp_path) -> None:
        from cryptobot.db import init_db

        path = tmp_path / "two.db"
        a = init_db(path)
        b = init_db(path)
        try:
            for conn in (a, b):
                assert {"ledger", "signals", "pipeline_health"}.issubset(
                    {r[0] for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()}
                )
        finally:
            a.close()
            b.close()

    def test_legacy_ledger_migrates_pair_columns_before_pair_index(self, tmp_path) -> None:
        """Existing server DBs lack pair columns, so schema bootstrap must migrate first."""
        from cryptobot.db import init_db

        schema = Path(_SCHEMA_PATH).read_text(encoding="utf-8")
        legacy_schema = "\n".join(
            line
            for line in schema.splitlines()
            if not line.strip().startswith(
                ("pair_group_id", "pair_role", "pair_component", "paired_coin")
            )
        )

        path = tmp_path / "legacy.db"
        conn = sqlite3.connect(path)
        try:
            conn.executescript(legacy_schema)
        finally:
            conn.close()

        migrated = init_db(path)
        try:
            cols = _columns(migrated, "ledger")
            idx = _indexes(migrated, "ledger")
            assert {"pair_group_id", "pair_role", "pair_component", "paired_coin"}.issubset(cols)
            assert "idx_ledger_pair_group" in idx
        finally:
            migrated.close()

    def test_legacy_db_migrates_wallet_columns_and_indexes(self, tmp_path) -> None:
        from cryptobot.db import init_db

        schema = Path(_SCHEMA_PATH).read_text(encoding="utf-8")
        # Simulate the pre-wallet_id schema (before 2026-05-20 feat(live):
        # isolate dry-run strategy wallets). Strip the wallet_id column from
        # ledger and signals + the indexes that reference them; keep the
        # rest of the modern schema (including live_order_attempts, whose
        # `wallet_id NOT NULL` is unrelated to the ledger/signals
        # migration tested here — that table did not exist at the legacy
        # cutoff, but is harmless to include for this assertion).
        in_ledger_or_signals_table = False

        def _legacy_filter(line: str) -> bool:
            nonlocal in_ledger_or_signals_table
            stripped = line.strip()
            if stripped.startswith("CREATE TABLE IF NOT EXISTS ledger") or stripped.startswith(
                "CREATE TABLE IF NOT EXISTS signals"
            ):
                in_ledger_or_signals_table = True
            if in_ledger_or_signals_table and stripped.startswith("wallet_id"):
                return False
            if stripped.startswith(");"):
                in_ledger_or_signals_table = False
            # Indexes referencing wallet_id on ledger or signals are tied to
            # the wallet_id column existing.
            if "ON ledger(wallet_id" in line or "ON signals(wallet_id" in line:
                return False
            return True

        legacy_schema = "\n".join(
            line for line in schema.splitlines() if _legacy_filter(line)
        )

        path = tmp_path / "legacy_wallet.db"
        conn = sqlite3.connect(path)
        try:
            conn.executescript(legacy_schema)
        finally:
            conn.close()

        migrated = init_db(path)
        try:
            assert "wallet_id" in _columns(migrated, "ledger")
            assert "wallet_id" in _columns(migrated, "signals")
            assert "idx_ledger_wallet_outcome" in _indexes(migrated, "ledger")
            assert "idx_signals_wallet_created" in _indexes(migrated, "signals")
        finally:
            migrated.close()


class TestConfigConstantsMatchBacktest:
    """D6 + D20: live constants MUST equal backtest's constants — boot drift detection."""

    def test_poly_fee_theta_matches_backtest(self) -> None:
        from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA as BACKTEST_THETA

        assert config.POLY_FEE_THETA == BACKTEST_THETA, (
            f"POLY_FEE_THETA drift: live={config.POLY_FEE_THETA} "
            f"backtest={BACKTEST_THETA}. Pin cryptobot/config.py to match."
        )

    def test_shared_exposure_cap_matches_live_artifact(self) -> None:
        import json

        best = json.loads(config.V6_CONFIG_PATH.read_text())
        expected = float(best["concurrent_exposure_cap_fraction"])
        assert config.SHARED_EXPOSURE_CAP == expected, (
            f"SHARED_EXPOSURE_CAP drift: live={config.SHARED_EXPOSURE_CAP} "
            f"artifact={expected} ({config.V6_CONFIG_PATH.name}). "
            f"Pin cryptobot/config.py to match the deployed artifact."
        )

    def test_max_drawdown_halt_matches_live_config(self) -> None:
        # 2026-07-31 throttle removal (operator-ordered): 0.30 peak-equity DD
        # halt. The graduated brake is gone (floor 1.0) and both intervals size
        # flat at 1%, so this halt is the ONLY drawdown guard; the deployed
        # both-hold twin's worst-fold backtest maxDD is 26.6%. Must equal the
        # deployed artifact's max_drawdown_halt_frac (loader drift-check).
        import json

        best = json.loads(config.V6_CONFIG_PATH.read_text())
        assert config.MAX_DRAWDOWN_HALT_FRAC == 0.30
        assert config.MAX_DRAWDOWN_HALT_FRAC == float(best["max_drawdown_halt_frac"])

    def test_backtest_bankroll_pinned(self) -> None:
        assert config.BACKTEST_BANKROLL == 100.0
        assert config.INITIAL_BANKROLL == 100.0

    def test_live_initial_bankroll_env_override(self) -> None:
        env = os.environ.copy()
        env["CRYPTOBOT_INITIAL_BANKROLL"] = "120.3838"
        # Import this checkout's package, not whatever is installed.
        env["PYTHONPATH"] = str(Path(config.__file__).resolve().parents[1])
        env.pop("INITIAL_BANKROLL", None)
        out = subprocess.check_output(
            [
                sys.executable,
                "-c",
                "from cryptobot import config; print(config.INITIAL_BANKROLL)",
            ],
            env=env,
            text=True,
        )
        assert float(out.strip()) == pytest.approx(120.3838)

    def test_max_walk_price_override(self) -> None:
        assert config.MAX_WALK_PRICE == 0.05

    def test_bet_size_multiplier(self) -> None:
        # 2026-05-19 v6_btceth_v1 deployment dropped the 2.0x multiplier
        # to 1.0x while the new config bedded in. dry_run_recommended.json
        # confirms; this test pins the default to the live value.
        assert config.BET_SIZE_MULTIPLIER == 1.0

    def test_min_bet_usd(self) -> None:
        assert config.MIN_BET_USD == 1.0

    def test_dashboard_bind_default_loopback(self) -> None:
        assert config.DASHBOARD_BIND_HOST == "127.0.0.1"

    def test_coin_universe(self) -> None:
        # 2026-06-12: SOL has no BASELINE ORB filter, so the minute-only scan
        # excludes it. 2026-07-03 SOL reconnect: the sub-minute flag re-adds
        # SOL (the $170,838 checkpoint backtest is 4-coin); its ORB filter is
        # subminute_only so baseline sleeves still reject it.
        expected = ("BTC", "ETH", "XRP") + (
            ("SOL",) if config.SUBMINUTE_ENABLED else ()
        )
        assert config.COINS == expected
        assert "HYPE" not in config.COINS
        assert "BNB" not in config.COINS
        assert "DOGE" not in config.COINS


@pytest.mark.parametrize(
    "table",
    [
        "ledger",
        "signals",
        "entry_scan_latches",
        "pipeline_health",
        "fee_rate_cache",
        # Per-strategy LIVE deploy plan §U1 — tables seeded by schema.sql /
        # _ensure_per_strategy_live_tables; absent on legacy DBs but added by
        # _migrate_db on first boot under the new code.
        "wallet_mode_state",
        "operator_control_state",
        "operator_control_events",
        "live_readiness_reports",
        "bankroll_peak",
        "capital_flows",
        "wallet_reconciliation_runs",
        "wallet_reconciliation_records",
        "live_order_attempts",
        "transfer_requests",
    ],
)
def test_table_exists(db: sqlite3.Connection, table: str) -> None:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    assert row is not None, f"table {table} missing"


# =========================================================================
# Per-strategy LIVE deploy plan §U1 (2026-05-24-001)
#
# Tests below pin the schema additions: ledger/signals new columns, new
# per-strategy LIVE tables, partial unique indexes that enforce single-
# SUBMITTING transfer / active-slot live_order_attempts,
# and the portfolio-dependent `backfill_wallet_groups` helper.
# =========================================================================


def _insert_minimal_ledger_row(
    db: sqlite3.Connection,
    *,
    wallet_id: Optional[str] = None,
    sleeve: str = "v6_5m",
    pair_component: Optional[str] = None,
    mode: Optional[str] = None,
    wallet_group: Optional[str] = None,
) -> int:
    """Insert one ledger row with the minimal required NOT NULL fields.

    Used across migration / backfill tests to seed legacy-shaped rows.
    Lets each test control only the columns under test.
    """
    db.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
           window_start_ts, window_end_ts, interval_min, poly_market_id,
           poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
           realized_edge, target_usd, bet_size, entry_top_price,
           config_sha256, pair_component, mode, wallet_group)
        VALUES (?, 'BTC', ?, ?, 4, 'YES', 1715923200, 1715923500, 5,
                'MARKET_ID', 'TOKEN_ID', 100.0, 0.78, 0.55, 0.55, 0.10,
                5.0, 5.0, 0.55, 'sha256-stub', ?,
                COALESCE(?, 'DRY'), ?)
        """,
        (
            "2026-05-17 14:04:00",
            wallet_id,
            sleeve,
            pair_component,
            mode,
            wallet_group,
        ),
    )
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


@dataclass
class _StubPortfolioSleeve:
    label: str


@dataclass
class _StubPortfolio:
    """Minimal stub matching `_PortfolioLike` for connection.py helpers.

    Real `V6Portfolio` adds `wallet_group_for` + `wallet_default_modes` in
    plan §U3; until that lands, tests pass this stub directly. The stub
    accepts a wallet_id → group_id dict and a list of sleeve labels.
    """

    groups: dict[str, str]
    sleeve_labels: list[str] = field(default_factory=list)

    def wallet_group_for(self, wallet_id: str) -> Optional[str]:
        return self.groups.get(wallet_id)

    @property
    def market_sleeves(self) -> list[_StubPortfolioSleeve]:
        return [_StubPortfolioSleeve(label=label) for label in self.sleeve_labels]


class TestLedgerLiveColumns:
    """D-LIVE-3 / D-LIVE-22 / D-LIVE-28 / D-LIVE-29 column additions."""

    def test_mode_default_dry_when_omitted_from_insert(self, db: sqlite3.Connection) -> None:
        # Use the legacy INSERT shape (no `mode` column) — schema DEFAULT
        # must stamp 'DRY' so existing call sites keep working.
        db.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, sleeve, entry_minute, side, window_start_ts,
               window_end_ts, interval_min, poly_market_id, poly_token_id,
               fee_rate_bps, model_p, p_market, fill_price, realized_edge,
               target_usd, bet_size, entry_top_price, config_sha256)
            VALUES
              ('2026-05-17 14:04:00', 'BTC', 'v6_5m', 4, 'YES', 1715923200,
               1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55, 0.55, 0.10, 5.0,
               5.0, 0.55, 'sha-stub')
            """,
        )
        row = db.execute("SELECT mode, wallet_group, pair_orphan FROM ledger").fetchone()
        assert row["mode"] == "DRY"
        assert row["wallet_group"] is None
        assert row["pair_orphan"] == 0

    def test_pair_orphan_default_zero(self, db: sqlite3.Connection) -> None:
        _insert_minimal_ledger_row(db)
        assert (
            db.execute("SELECT pair_orphan FROM ledger LIMIT 1").fetchone()[0] == 0
        )

    def test_pair_top_up_used_default_zero(self, db: sqlite3.Connection) -> None:
        _insert_minimal_ledger_row(db)
        assert (
            db.execute("SELECT pair_top_up_used FROM ledger LIMIT 1").fetchone()[0] == 0
        )

    def test_live_clob_columns_nullable(self, db: sqlite3.Connection) -> None:
        _insert_minimal_ledger_row(db)
        row = db.execute(
            "SELECT live_order_id, live_client_order_id, live_filled_size, "
            "live_filled_price, live_fill_ts, top_ask_at_decision FROM ledger"
        ).fetchone()
        for col in (
            "live_order_id",
            "live_client_order_id",
            "live_filled_size",
            "live_filled_price",
            "live_fill_ts",
            "top_ask_at_decision",
        ):
            assert row[col] is None, f"{col} should be NULL when omitted"

    def test_top_ask_at_decision_distinct_from_fill_price(
        self, db: sqlite3.Connection
    ) -> None:
        """D-LIVE-29 forensic column: fill_price stores VWAP; top_ask_at_decision
        captures the scanner-tick top-of-book separately.
        """
        db.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, sleeve, entry_minute, side, window_start_ts,
               window_end_ts, interval_min, poly_market_id, poly_token_id,
               fee_rate_bps, model_p, p_market, fill_price, walked_vwap,
               top_ask_at_decision, realized_edge, target_usd, bet_size,
               entry_top_price, config_sha256, mode)
            VALUES
              ('2026-05-17 14:04:00', 'BTC', 'v6_5m', 4, 'YES', 1715923200,
               1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55, 0.5712, 0.5712,
               0.55, 0.10, 5.0, 5.0, 0.55, 'sha-stub', 'LIVE')
            """,
        )
        row = db.execute(
            "SELECT fill_price, walked_vwap, top_ask_at_decision FROM ledger"
        ).fetchone()
        # D-LIVE-29 invariant: walked_vwap mirrors fill_price on new rows;
        # top_ask_at_decision is the forensic anchor and can differ.
        assert row["fill_price"] == row["walked_vwap"]
        assert row["top_ask_at_decision"] == 0.55
        assert row["fill_price"] == 0.5712


class TestSignalsLiveColumns:
    def test_signals_mode_and_group_columns_present(
        self, db: sqlite3.Connection
    ) -> None:
        cols = _columns(db, "signals")
        assert "mode" in cols
        assert "wallet_group" in cols

    def test_signals_mode_nullable(self, db: sqlite3.Connection) -> None:
        db.execute(
            """
            INSERT INTO signals
              (created_at, coin, sleeve, entry_minute, side, window_start_ts,
               outcome)
            VALUES
              ('2026-05-17 14:04:00', 'BTC', 'v6_5m', 4, 'YES', 1715923200,
               'WOULD_BET')
            """,
        )
        row = db.execute("SELECT mode, wallet_group FROM signals").fetchone()
        assert row["mode"] is None
        assert row["wallet_group"] is None


class TestWalletModeState:
    def test_table_default_mode_dry(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT INTO wallet_mode_state (wallet_id) VALUES ('v6_5m')"
        )
        row = db.execute(
            "SELECT mode, version FROM wallet_mode_state WHERE wallet_id='v6_5m'"
        ).fetchone()
        assert row["mode"] == "DRY"
        assert row["version"] == 0

    def test_wallet_id_primary_key(self, db: sqlite3.Connection) -> None:
        db.execute("INSERT INTO wallet_mode_state (wallet_id) VALUES ('v6_5m')")
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO wallet_mode_state (wallet_id) VALUES ('v6_5m')"
            )


class TestOperatorControl:
    def test_singleton_constraint_blocks_second_row(
        self, db: sqlite3.Connection
    ) -> None:
        db.execute(
            "INSERT INTO operator_control_state (id, state, updated_by) "
            "VALUES (1, 'STOPPED_PROCESSING', 'boot')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO operator_control_state (id, state, updated_by) "
                "VALUES (2, 'LIVE', 'boot')"
            )

    def test_default_state_stopped_processing(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT INTO operator_control_state (id, updated_by) VALUES (1, 'boot')"
        )
        row = db.execute(
            "SELECT state, version FROM operator_control_state WHERE id=1"
        ).fetchone()
        assert row["state"] == "STOPPED_PROCESSING"
        assert row["version"] == 0

    def test_events_scope_defaults_to_global(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT INTO operator_control_events (action, actor) "
            "VALUES ('start', 'dashboard')"
        )
        row = db.execute(
            "SELECT scope FROM operator_control_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["scope"] == "global"

    def test_events_scope_supports_per_wallet_scope(
        self, db: sqlite3.Connection
    ) -> None:
        """D-LIVE-25: single audit table — wallet flips use scope='wallet:<id>'."""
        db.execute(
            "INSERT INTO operator_control_events (scope, action, actor) "
            "VALUES ('wallet:v6_5m', 'mode_flip', 'cli')"
        )
        row = db.execute(
            "SELECT scope FROM operator_control_events WHERE action='mode_flip'"
        ).fetchone()
        assert row["scope"] == "wallet:v6_5m"


class TestLiveOrderAttempts:
    def _insert_attempt(
        self,
        db: sqlite3.Connection,
        *,
        client_order_id: str,
        status: str = "SUBMITTING",
        wallet_id: str = "v6_5m",
        wallet_group: str = "group_5m",
        coin: str = "BTC",
        sleeve: str = "v6_5m",
        entry_minute: int = 4,
        side: str = "YES",
        window_start_ts: int = 1715923200,
        amount_usd: float = 5.0,
        ledger_id: Optional[int] = None,
    ) -> int:
        db.execute(
            """
            INSERT INTO live_order_attempts
              (wallet_id, wallet_group, mode, coin, sleeve, entry_minute,
               side, window_start_ts, client_order_id, amount_usd, status,
               ledger_id)
            VALUES (?, ?, 'LIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wallet_id,
                wallet_group,
                coin,
                sleeve,
                entry_minute,
                side,
                window_start_ts,
                client_order_id,
                amount_usd,
                status,
                ledger_id,
            ),
        )
        return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])

    def test_unique_client_order_id(self, db: sqlite3.Connection) -> None:
        self._insert_attempt(db, client_order_id="coid-1")
        with pytest.raises(sqlite3.IntegrityError):
            self._insert_attempt(db, client_order_id="coid-1")

    def test_active_slot_partial_unique_index_blocks_duplicate_submitting(
        self, db: sqlite3.Connection
    ) -> None:
        """D-LIVE-28: only one SUBMITTING/SUBMITTED attempt per slot."""
        self._insert_attempt(db, client_order_id="coid-A", status="SUBMITTING")
        with pytest.raises(sqlite3.IntegrityError):
            self._insert_attempt(db, client_order_id="coid-B", status="SUBMITTING")

    def test_terminal_attempt_unblocks_active_slot(
        self, db: sqlite3.Connection
    ) -> None:
        """FAILED / LINKED rows do NOT consume the slot — they're terminal."""
        self._insert_attempt(db, client_order_id="coid-X", status="FAILED")
        # A SUBMITTING row for the same slot is allowed because the prior
        # attempt is no longer counted by the partial unique index.
        self._insert_attempt(db, client_order_id="coid-Y", status="SUBMITTING")
        count = db.execute("SELECT COUNT(*) FROM live_order_attempts").fetchone()[0]
        assert count == 2

    def test_group_status_index_supports_group_cap_scan(
        self, db: sqlite3.Connection
    ) -> None:
        idx = _indexes(db, "live_order_attempts")
        assert "idx_loa_status_group" in idx


class TestTransferRequests:
    def test_source_wallet_id_column_present_and_nullable(
        self, db: sqlite3.Connection
    ) -> None:
        cols = _columns(db, "transfer_requests")
        assert "source_wallet_id" in cols
        db.execute(
            """
            INSERT INTO transfer_requests
              (actor, from_wallet, to_wallet, amount_usd, status)
            VALUES ('cli', '0xfunder', '0xreturn', 10.0, 'PREVIEW')
            """,
        )
        row = db.execute(
            "SELECT source_wallet_id FROM transfer_requests"
        ).fetchone()
        assert row["source_wallet_id"] is None

    def test_single_submitting_per_from_wallet(
        self, db: sqlite3.Connection
    ) -> None:
        db.execute(
            """
            INSERT INTO transfer_requests
              (actor, from_wallet, to_wallet, amount_usd, status)
            VALUES ('cli', '0xA', '0xB', 5.0, 'SUBMITTING')
            """,
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO transfer_requests
                  (actor, from_wallet, to_wallet, amount_usd, status)
                VALUES ('cli', '0xA', '0xC', 7.0, 'SUBMITTING')
                """,
            )

    def test_two_submitting_from_different_wallets_allowed(
        self, db: sqlite3.Connection
    ) -> None:
        db.execute(
            "INSERT INTO transfer_requests (actor, from_wallet, to_wallet, "
            "amount_usd, status) VALUES ('cli', '0xA', '0xB', 5.0, 'SUBMITTING')"
        )
        db.execute(
            "INSERT INTO transfer_requests (actor, from_wallet, to_wallet, "
            "amount_usd, status) VALUES ('cli', '0xC', '0xD', 5.0, 'SUBMITTING')"
        )
        count = db.execute(
            "SELECT COUNT(*) FROM transfer_requests WHERE status='SUBMITTING'"
        ).fetchone()[0]
        assert count == 2


class TestMigrationIdempotency:
    def test_migrate_runs_twice_without_error(self, tmp_path: Path) -> None:
        from cryptobot.db import init_db

        path = tmp_path / "idempotent.db"
        a = init_db(path)
        # Second open must not raise on the same file — _migrate_db is
        # designed to be idempotent.
        b = init_db(path)
        try:
            assert "wallet_mode_state" in {
                r[0]
                for r in b.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        finally:
            a.close()
            b.close()


class TestBackfillWalletGroups:
    """`backfill_wallet_groups(conn, portfolio)` runs after `load_sleeves()`
    so the JSON-derived wallet→group mapping is available for the UPDATE
    (D-LIVE-15). Tests use a minimal stub portfolio; the real
    `V6Portfolio.wallet_group_for` lands in plan §U3.
    """

    def test_backfill_stamps_v6_5m_to_group_5m(self, db: sqlite3.Connection) -> None:
        # Pre-migration shape: wallet_group IS NULL on an existing v6_5m row.
        row_id = _insert_minimal_ledger_row(
            db, wallet_id="v6_5m", sleeve="v6_5m", wallet_group=None
        )
        portfolio = _StubPortfolio(
            groups={"v6_5m": "group_5m"}, sleeve_labels=["v6_5m"]
        )
        counts = backfill_wallet_groups(db, portfolio)
        after = db.execute(
            "SELECT wallet_group FROM ledger WHERE id=?", (row_id,)
        ).fetchone()
        assert after["wallet_group"] == "group_5m"
        assert counts["ledger"] >= 1

    def test_backfill_stamps_pairs_short_and_long_sleeves(
        self, db: sqlite3.Connection
    ) -> None:
        # Pair-leg rows: sleeve = '<pair_label>_long' / '<pair_label>_short'
        # AND/OR pair_component = '<pair_label>'.
        long_id = _insert_minimal_ledger_row(
            db,
            wallet_id=None,
            sleeve="pairs_5m_long",
            pair_component="pairs_5m",
        )
        short_id = _insert_minimal_ledger_row(
            db,
            wallet_id=None,
            sleeve="pairs_5m_short",
            pair_component="pairs_5m",
        )
        portfolio = _StubPortfolio(
            groups={"pairs_5m": "group_5m"}, sleeve_labels=["pairs_5m"]
        )
        backfill_wallet_groups(db, portfolio)
        long_grp = db.execute(
            "SELECT wallet_group FROM ledger WHERE id=?", (long_id,)
        ).fetchone()["wallet_group"]
        short_grp = db.execute(
            "SELECT wallet_group FROM ledger WHERE id=?", (short_id,)
        ).fetchone()["wallet_group"]
        assert long_grp == "group_5m"
        assert short_grp == "group_5m"

    def test_backfill_leaves_ungrouped_wallet_null(
        self, db: sqlite3.Connection
    ) -> None:
        row_id = _insert_minimal_ledger_row(
            db, wallet_id="weather_5m", sleeve="weather_5m"
        )
        # `weather_5m` is not in any group → portfolio returns None.
        portfolio = _StubPortfolio(
            groups={"v6_5m": "group_5m"},
            sleeve_labels=["v6_5m", "weather_5m"],
        )
        backfill_wallet_groups(db, portfolio)
        assert (
            db.execute(
                "SELECT wallet_group FROM ledger WHERE id=?", (row_id,)
            ).fetchone()["wallet_group"]
            is None
        )

    def test_backfill_is_idempotent(self, db: sqlite3.Connection) -> None:
        _insert_minimal_ledger_row(
            db, wallet_id="v6_5m", sleeve="v6_5m", wallet_group=None
        )
        portfolio = _StubPortfolio(
            groups={"v6_5m": "group_5m"}, sleeve_labels=["v6_5m"]
        )
        first = backfill_wallet_groups(db, portfolio)
        second = backfill_wallet_groups(db, portfolio)
        # Second run touches zero ledger rows because the first already
        # stamped them.
        assert first["ledger"] >= 1
        assert second["ledger"] == 0

    def test_backfill_signals_table_too(self, db: sqlite3.Connection) -> None:
        db.execute(
            """
            INSERT INTO signals
              (created_at, coin, wallet_id, sleeve, entry_minute, side,
               window_start_ts, outcome)
            VALUES
              ('2026-05-17 14:04:00', 'BTC', 'v6_5m', 'v6_5m', 4, 'YES',
               1715923200, 'WOULD_BET')
            """,
        )
        portfolio = _StubPortfolio(
            groups={"v6_5m": "group_5m"}, sleeve_labels=["v6_5m"]
        )
        counts = backfill_wallet_groups(db, portfolio)
        row = db.execute("SELECT wallet_group FROM signals").fetchone()
        assert row["wallet_group"] == "group_5m"
        assert counts["signals"] >= 1

    def test_backfill_with_empty_portfolio_is_noop(
        self, db: sqlite3.Connection
    ) -> None:
        _insert_minimal_ledger_row(db, wallet_id="v6_5m", sleeve="v6_5m")
        portfolio = _StubPortfolio(groups={}, sleeve_labels=["v6_5m"])
        counts = backfill_wallet_groups(db, portfolio)
        assert counts == {"ledger": 0, "signals": 0}


class TestLegacyDbMigratesLiveColumns:
    """Existing server DBs lack the per-strategy LIVE columns/tables.
    `init_db()` on a legacy file must add them via `_migrate_db()` without
    losing data."""

    # Hand-crafted legacy DDL — the shape of cryptobot's schema BEFORE the
    # 2026-05-24-001 per-strategy LIVE plan landed. Keep this in lockstep
    # with the prior commit's schema.sql so the migration assertion is
    # meaningful. Inlined here rather than derived via string-stripping the
    # current schema to keep the test robust against future comment / index
    # changes.
    _LEGACY_DDL = """
        CREATE TABLE IF NOT EXISTS ledger (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            bet_ts                  TEXT NOT NULL,
            coin                    TEXT NOT NULL,
            wallet_id               TEXT,
            sleeve                  TEXT NOT NULL,
            entry_minute            INTEGER NOT NULL,
            side                    TEXT NOT NULL,
            window_start_ts         INTEGER NOT NULL,
            window_end_ts           INTEGER NOT NULL,
            interval_min            INTEGER NOT NULL,
            poly_market_id          TEXT NOT NULL,
            poly_token_id           TEXT NOT NULL,
            fee_rate_bps            REAL NOT NULL,
            model_p                 REAL NOT NULL,
            p_market                REAL NOT NULL,
            fill_price              REAL NOT NULL,
            walked_vwap             REAL,
            realized_edge           REAL NOT NULL,
            partial_ret_z_1h        REAL,
            lut_p_bucket            INTEGER,
            lut_hit_rate            REAL,
            lut_n_cum               INTEGER,
            target_usd              REAL NOT NULL,
            bet_size                REAL NOT NULL,
            slot_filled_pre         REAL NOT NULL DEFAULT 0,
            is_topup                INTEGER NOT NULL DEFAULT 0,
            entry_top_price         REAL NOT NULL,
            add_on_realized_edge_minus_initial_edge REAL,
            strike_price            REAL,
            window_open_spot        REAL,
            yes_pre1_anchor         REAL,
            outcome                 TEXT NOT NULL DEFAULT 'PENDING',
            won_up                  INTEGER,
            resolution_price        REAL,
            resolution_source       TEXT,
            resolution_ts           TEXT,
            pnl_net                 REAL,
            config_sha256           TEXT NOT NULL,
            polymarket_resolution_price  REAL,
            polymarket_resolution_source TEXT,
            pair_group_id           TEXT,
            pair_role               TEXT,
            pair_component          TEXT,
            paired_coin             TEXT,
            ghost_fill              INTEGER NOT NULL DEFAULT 0,
            created_at              TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS signals (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at              TEXT NOT NULL,
            coin                    TEXT NOT NULL,
            wallet_id               TEXT,
            sleeve                  TEXT NOT NULL,
            entry_minute            INTEGER NOT NULL,
            side                    TEXT NOT NULL,
            window_start_ts         INTEGER NOT NULL,
            model_p                 REAL,
            ask                     REAL,
            edge                    REAL,
            fee_rate_bps            REAL,
            partial_ret_z_1h        REAL,
            lut_p_bucket            INTEGER,
            lut_hit_rate            REAL,
            lut_n_cum               INTEGER,
            outcome                 TEXT NOT NULL,
            skip_reason             TEXT
        );
        CREATE TABLE IF NOT EXISTS pipeline_health (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at              TEXT NOT NULL DEFAULT (datetime('now')),
            stage                   TEXT NOT NULL,
            status                  TEXT NOT NULL,
            coin                    TEXT,
            message                 TEXT,
            duration_ms             INTEGER
        );
        CREATE TABLE IF NOT EXISTS process_health (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at              TEXT NOT NULL DEFAULT (datetime('now')),
            metric                  TEXT NOT NULL,
            value                   REAL NOT NULL,
            status                  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fee_rate_cache (
            market_token_id         TEXT PRIMARY KEY,
            fee_rate_bps            REAL NOT NULL,
            fetched_at              TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """

    def test_legacy_ledger_adds_mode_and_wallet_group(
        self, tmp_path: Path
    ) -> None:
        from cryptobot.db import init_db

        path = tmp_path / "legacy_live.db"
        conn = sqlite3.connect(path)
        try:
            conn.executescript(self._LEGACY_DDL)
        finally:
            conn.close()

        migrated = init_db(path)
        try:
            cols = _columns(migrated, "ledger")
            assert {
                "mode",
                "wallet_group",
                "live_order_id",
                "live_client_order_id",
                "live_filled_size",
                "live_filled_price",
                "live_fill_ts",
                "pair_orphan",
                "pair_top_up_used",
                "top_ask_at_decision",
            }.issubset(cols)
            assert "mode" in _columns(migrated, "signals")
            assert "wallet_group" in _columns(migrated, "signals")
            tables = {
                r[0]
                for r in migrated.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert {
                "wallet_mode_state",
                "operator_control_state",
                "operator_control_events",
                "live_readiness_reports",
                "bankroll_peak",
                "capital_flows",
                "wallet_reconciliation_runs",
                "wallet_reconciliation_records",
                "live_order_attempts",
                "transfer_requests",
            }.issubset(tables)
        finally:
            migrated.close()

    def test_legacy_ledger_default_mode_dry_on_existing_rows(
        self, tmp_path: Path
    ) -> None:
        """After migration, the ALTER TABLE ADD COLUMN with DEFAULT 'DRY'
        stamps every pre-migration ledger row as DRY. This preserves backtest
        parity (every pre-LIVE row was a paper bet) per D-LIVE-3."""
        from cryptobot.db import init_db

        path = tmp_path / "legacy_rows.db"
        conn = sqlite3.connect(path)
        try:
            conn.executescript(self._LEGACY_DDL)
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
        finally:
            conn.close()

        migrated = init_db(path)
        try:
            row = migrated.execute(
                "SELECT mode, wallet_group, pair_orphan FROM ledger LIMIT 1"
            ).fetchone()
            assert row["mode"] == "DRY"
            assert row["wallet_group"] is None  # backfill is a separate step
            assert row["pair_orphan"] == 0
        finally:
            migrated.close()
