"""U7 verification: WalletReconciler.sample() snapshots CLOB + chain state.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U7 / D-LIVE-9:
  - Run row is always inserted, even when CLOB is unreachable
    (source_status='clob_unreachable').
  - Position records match against LIVE PENDING ledger rows by
    (token_id, side); unmatched rows surface as orphan_clob or
    orphan_ledger.
  - Chain ↔ CLOB drift surfaces as a warning in warnings_json, never as
    a halt.
  - bankroll_peak is appended on every successful run.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from cryptobot.db import init_db
from cryptobot.persistence.wallet_reconciliation import (
    ReconciliationRun,
    WalletReconciler,
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    yield db
    db.close()


def _stub_order_client(
    *,
    balance: Optional[float] = 200.0,
    orders: Optional[list[dict[str, Any]]] = None,
    positions: Optional[list[dict[str, Any]]] = None,
    raise_on: Optional[str] = None,
) -> MagicMock:
    """Build a minimal stub mimicking OrderClient + its `_client`."""
    client = MagicMock()
    inner = MagicMock()
    client._client = inner

    def _check_balance() -> Optional[float]:
        if raise_on == "balance":
            raise RuntimeError("clob down")
        return balance

    client.check_balance.side_effect = _check_balance
    inner.get_open_orders.return_value = orders or []
    inner.get_positions.return_value = positions or []
    return client


def _insert_live_pending_row(
    conn: sqlite3.Connection,
    *,
    token_id: str,
    side: str = "YES",
    bet_size: float = 5.0,
    live_filled_size: Optional[float] = None,
    live_filled_price: Optional[float] = None,
    live_order_id: Optional[str] = None,
    wallet_id: str = "v6_5m",
    wallet_group: str = "group_5m",
) -> int:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side, window_start_ts,
           window_end_ts, interval_min, poly_market_id, poly_token_id,
           fee_rate_bps, model_p, p_market, fill_price, realized_edge,
           target_usd, bet_size, entry_top_price, config_sha256, mode,
           wallet_group, live_order_id, live_filled_size, live_filled_price)
        VALUES ('2026-05-24 14:04:00', 'BTC', ?, 'v6_5m', 4, ?, 1715923200,
                1715923500, 5, 'M', ?, 100.0, 0.78, 0.55, 0.55, 0.10,
                ?, ?, 0.55, 'sha-stub', 'LIVE', ?, ?, ?, ?)
        """,
        (
            wallet_id,
            side,
            token_id,
            bet_size,
            bet_size,
            wallet_group,
            live_order_id,
            live_filled_size,
            live_filled_price,
        ),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


# ============================================================================
# Happy path
# ============================================================================


class TestWalletReconcilerHappyPath:
    def test_inserts_run_with_clob_balance(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=199.5)
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        assert run.source_status == "ok"
        assert run.clob_balance_usd == 199.5
        assert run.chain_balance_usd is None
        assert run.warnings == []

    def test_inserts_one_db_row_per_run(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=199.5)
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        reconciler.sample(conn)
        reconciler.sample(conn)
        n = conn.execute(
            "SELECT COUNT(*) FROM wallet_reconciliation_runs"
        ).fetchone()[0]
        assert n == 2

    def test_inserts_balance_record(self, conn: sqlite3.Connection) -> None:
        client = _stub_order_client(balance=200.0)
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        recs = conn.execute(
            "SELECT source_layer, record_type, amount_usd "
            "FROM wallet_reconciliation_records WHERE run_id=?",
            (run.run_id,),
        ).fetchall()
        types = {(r["source_layer"], r["record_type"]) for r in recs}
        assert ("clob", "balance") in types


# ============================================================================
# Position matching
# ============================================================================


class TestPositionMatching:
    def test_matched_position_links_to_ledger(
        self, conn: sqlite3.Connection
    ) -> None:
        ledger_id = _insert_live_pending_row(conn, token_id="token-1")
        client = _stub_order_client(
            balance=100.0,
            positions=[{"token_id": "token-1", "side": "YES", "size": 9.09, "value": 5.0}],
        )
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        assert run.open_positions_count == 1
        matched = conn.execute(
            "SELECT match_status, matched_ledger_id "
            "FROM wallet_reconciliation_records "
            "WHERE record_type='position' AND source_layer='clob' AND run_id=?",
            (run.run_id,),
        ).fetchone()
        assert matched["match_status"] == "matched"
        assert matched["matched_ledger_id"] == ledger_id

    def test_position_size_mismatch_is_not_marked_matched(
        self, conn: sqlite3.Connection
    ) -> None:
        ledger_id = _insert_live_pending_row(
            conn,
            token_id="token-1",
            live_filled_size=10.0,
            live_filled_price=0.50,
        )
        client = _stub_order_client(
            balance=100.0,
            positions=[
                {"token_id": "token-1", "side": "YES", "size": 3.0, "value": 1.5}
            ],
        )
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)

        row = conn.execute(
            "SELECT match_status, matched_ledger_id "
            "FROM wallet_reconciliation_records "
            "WHERE record_type='position' AND source_layer='clob' AND run_id=?",
            (run.run_id,),
        ).fetchone()
        assert row["match_status"] == "mismatch"
        assert row["matched_ledger_id"] == ledger_id

    def test_position_value_mismatch_is_not_marked_matched(
        self, conn: sqlite3.Connection
    ) -> None:
        ledger_id = _insert_live_pending_row(
            conn,
            token_id="token-1",
            live_filled_size=10.0,
            live_filled_price=0.50,
        )
        client = _stub_order_client(
            balance=100.0,
            positions=[{"token_id": "token-1", "side": "YES", "value": 2.0}],
        )
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)

        row = conn.execute(
            "SELECT match_status, matched_ledger_id "
            "FROM wallet_reconciliation_records "
            "WHERE record_type='position' AND source_layer='clob' AND run_id=?",
            (run.run_id,),
        ).fetchone()
        assert row["match_status"] == "mismatch"
        assert row["matched_ledger_id"] == ledger_id

    def test_orphan_clob_when_no_ledger_match(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(
            balance=100.0,
            positions=[{"token_id": "token-X", "side": "YES", "size": 1.0}],
        )
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        row = conn.execute(
            "SELECT match_status FROM wallet_reconciliation_records "
            "WHERE record_type='position' AND source_layer='clob' AND run_id=?",
            (run.run_id,),
        ).fetchone()
        assert row["match_status"] == "orphan_clob"

    def test_orphan_ledger_when_no_clob_position(
        self, conn: sqlite3.Connection
    ) -> None:
        ledger_id = _insert_live_pending_row(conn, token_id="token-2")
        client = _stub_order_client(balance=100.0, positions=[])
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        row = conn.execute(
            "SELECT matched_ledger_id, match_status "
            "FROM wallet_reconciliation_records "
            "WHERE source_layer='ledger' AND record_type='position' AND run_id=?",
            (run.run_id,),
        ).fetchone()
        assert row["match_status"] == "orphan_ledger"
        assert row["matched_ledger_id"] == ledger_id


# ============================================================================
# Chain reconciliation + drift warning
# ============================================================================


class TestChainBalance:
    def test_chain_balance_recorded(self, conn: sqlite3.Connection) -> None:
        client = _stub_order_client(balance=200.0)
        reconciler = WalletReconciler(
            order_client=client,
            funder_address="0xfunder",
            chain_balance_fetcher=lambda: 200.0,
        )
        run = reconciler.sample(conn)
        assert run.chain_balance_usd == 200.0
        assert run.source_status == "ok"

    def test_drift_warning_emitted(self, conn: sqlite3.Connection) -> None:
        client = _stub_order_client(balance=100.0)
        reconciler = WalletReconciler(
            order_client=client,
            funder_address="0xfunder",
            chain_balance_fetcher=lambda: 95.0,
            balance_tolerance_usd=0.5,
        )
        run = reconciler.sample(conn)
        assert any(w.startswith("chain_clob_drift") for w in run.warnings)
        # Warning written to DB
        warnings_json = conn.execute(
            "SELECT warnings_json FROM wallet_reconciliation_runs WHERE id=?",
            (run.run_id,),
        ).fetchone()["warnings_json"]
        warnings = json.loads(warnings_json)
        assert any("chain_clob_drift" in w for w in warnings)

    def test_chain_unreachable_partial_status(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=100.0)
        reconciler = WalletReconciler(
            order_client=client,
            funder_address="0xfunder",
            chain_balance_fetcher=lambda: None,
        )
        run = reconciler.sample(conn)
        assert run.source_status == "partial"
        assert "chain_unreachable" in run.warnings

    def test_drift_does_not_halt(self, conn: sqlite3.Connection) -> None:
        client = _stub_order_client(balance=100.0)
        reconciler = WalletReconciler(
            order_client=client,
            funder_address="0xfunder",
            chain_balance_fetcher=lambda: 95.0,
            balance_tolerance_usd=0.5,
        )
        # No exception raised — drift is warning-only.
        run = reconciler.sample(conn)
        assert run is not None


# ============================================================================
# CLOB unreachable
# ============================================================================


class TestClobUnreachable:
    def test_balance_failure_inserts_unreachable_row(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=None, raise_on="balance")
        # Also kill the orders + positions fetch so all three are None.
        client._client.get_open_orders.side_effect = RuntimeError("clob")
        client._client.get_positions.side_effect = RuntimeError("clob")
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        assert run.source_status == "clob_unreachable"
        assert run.clob_balance_usd is None

    def test_partial_when_orders_succeeds_but_positions_fails(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=100.0)
        client._client.get_positions.side_effect = RuntimeError("data api")
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        run = reconciler.sample(conn)
        assert run.source_status == "partial"


# ============================================================================
# Bankroll peak snapshot
# ============================================================================


class TestBankrollPeak:
    def test_inserts_one_row_per_successful_run(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=200.0)
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        reconciler.sample(conn)
        reconciler.sample(conn)
        n = conn.execute("SELECT COUNT(*) FROM bankroll_peak").fetchone()[0]
        assert n == 2

    def test_records_wallet_realized_and_pending(
        self, conn: sqlite3.Connection
    ) -> None:
        # Seed one PENDING LIVE row and one settled WIN row.
        _insert_live_pending_row(conn, token_id="token-A", bet_size=5.0)
        conn.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=2.5 WHERE id=("
            "SELECT MAX(id) FROM ledger)"
        )
        _insert_live_pending_row(conn, token_id="token-B", bet_size=7.0)
        conn.commit()

        client = _stub_order_client(balance=200.0)
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        reconciler.sample(conn)
        row = conn.execute(
            "SELECT wallet_balance, realized_pnl, pending_exposure "
            "FROM bankroll_peak ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["wallet_balance"] == 200.0
        assert row["realized_pnl"] == 2.5
        assert row["pending_exposure"] == 7.0

    def test_no_bankroll_peak_when_balance_unreachable(
        self, conn: sqlite3.Connection
    ) -> None:
        client = _stub_order_client(balance=None, raise_on="balance")
        reconciler = WalletReconciler(
            order_client=client, funder_address="0xfunder"
        )
        reconciler.sample(conn)
        n = conn.execute("SELECT COUNT(*) FROM bankroll_peak").fetchone()[0]
        assert n == 0
