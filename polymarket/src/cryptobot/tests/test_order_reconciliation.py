"""U7 verification: per-order CLOB ↔ ledger reconciliation + stale recovery.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U7 / D-LIVE-17 +
D-LIVE-28:

  - reconcile_open_orders updates live_filled_size/price on LIVE PENDING
    rows; transitions CANCELLED/EXPIRED with filled=0 to outcome='CANCELLED'.
  - reconcile_stale_attempts releases SUBMITTING attempts the CLOB never
    saw, or surfaces orphan CLOB-without-ledger via CRITICAL log.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cryptobot.db import init_db
from cryptobot.execution.order_attempts import (
    LINKED,
    FAILED,
    get_attempt,
    mark_attempt_submitted,
    reserve_live_order_attempt,
)
from cryptobot.persistence.reconciliation import (
    reconcile_open_orders,
    reconcile_stale_attempts,
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    yield db
    db.close()


def _insert_live_pending(
    conn: sqlite3.Connection,
    *,
    live_order_id: str,
    token_id: str = "token-1",
    side: str = "YES",
    bet_size: float = 5.0,
) -> int:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side, window_start_ts,
           window_end_ts, interval_min, poly_market_id, poly_token_id,
           fee_rate_bps, model_p, p_market, fill_price, realized_edge,
           target_usd, bet_size, entry_top_price, config_sha256, mode,
           wallet_group, live_order_id)
        VALUES ('2026-05-24 14:04:00', 'BTC', 'v6_5m', 'v6_5m', 4, ?, 1715923200,
                1715923500, 5, 'M', ?, 100.0, 0.78, 0.55, 0.55, 0.10,
                ?, ?, 0.55, 'sha-stub', 'LIVE', 'group_5m', ?)
        """,
        (side, token_id, bet_size, bet_size, live_order_id),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


# ============================================================================
# reconcile_open_orders
# ============================================================================


class TestReconcileOpenOrders:
    def test_updates_live_filled_size_on_partial_fill(
        self, conn: sqlite3.Connection
    ) -> None:
        ledger_id = _insert_live_pending(conn, live_order_id="order-1")
        fetcher = lambda oid: {
            "order_id": "order-1",
            "status": "MATCHED_PARTIAL",
            "size_matched": 3.5,
            "avg_fill_price": 0.55,
            "matched_at": "2026-05-24 14:05:00",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        row = conn.execute(
            "SELECT live_filled_size, live_filled_price, outcome FROM ledger "
            "WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["live_filled_size"] == 3.5
        assert row["live_filled_price"] == 0.55
        assert row["outcome"] == "PENDING"

    def test_backfills_subminute_fill_realized_usd(
        self, conn: sqlite3.Connection
    ) -> None:
        """A GTC sub-minute order fills async: record_subminute_fill wrote the
        placement snapshot with NULL fill columns, and reconcile_open_orders
        lands the true fill on the ledger. The sub-minute breaker + rung-1 cap
        read subminute_fills.filled_usd, so the reconciler must mirror the
        realized fill there too (keyed by CLOB order_id) — else fill_rate reads
        0% and the cap never accrues on orders that really filled.
        """
        from cryptobot.execution.fill_rate_breaker import record_subminute_fill

        _insert_live_pending(conn, live_order_id="order-sub", side="YES")
        record_subminute_fill(
            conn,
            coin="BTC",
            sleeve="subminute_5m",
            offset_sec=290,
            window_start_ts=1715923200,
            side="YES",
            requested_usd=1.0,
            quote_ask=0.51,
            limit_price=0.51,
            filled_size=None,   # async GTC — no synchronous fill metadata
            filled_price=None,
            filled_usd=None,
            order_id="order-sub",
            client_order_id="coid-sub",
            outcome="accepted",
        )
        conn.commit()

        fetcher = lambda oid: {
            "order_id": "order-sub",
            "status": "MATCHED",
            "size_matched": 1.960783,
            "avg_fill_price": 0.51,
            "matched_at": "2026-05-24 14:05:00",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        sf = conn.execute(
            "SELECT filled_size, filled_price, filled_usd FROM subminute_fills "
            "WHERE order_id='order-sub'",
        ).fetchone()
        assert sf["filled_size"] == pytest.approx(1.960783)
        assert sf["filled_price"] == pytest.approx(0.51)
        assert sf["filled_usd"] == pytest.approx(1.960783 * 0.51)

        # Idempotent: a second reconcile re-updates the still-PENDING ledger row
        # but the backfill guard (filled_usd already > 0) must leave the
        # subminute_fills row unchanged rather than double-writing.
        reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        sf2 = conn.execute(
            "SELECT filled_usd FROM subminute_fills WHERE order_id='order-sub'",
        ).fetchone()
        assert sf2["filled_usd"] == pytest.approx(1.960783 * 0.51)

    def test_derives_full_fak_price_from_get_order_payload(
        self, conn: sqlite3.Connection
    ) -> None:
        """CLOB get_order may omit avg_fill_price but include a full FAK match:
        size_matched ~= original_size at the order price. That price is the
        only fill price the resolution sweep can safely use for settlement.
        """
        ledger_id = _insert_live_pending(
            conn, live_order_id="order-fak-full", side="NO", bet_size=4.0
        )
        fetcher = lambda oid: {
            "order_id": oid,
            "status": "MATCHED",
            "price": "0.52",
            "original_size": "7.6923",
            "size_matched": "7.692306",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        row = conn.execute(
            "SELECT live_filled_size, live_filled_price FROM ledger WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["live_filled_size"] == pytest.approx(7.692306)
        assert row["live_filled_price"] == pytest.approx(0.52)

    def test_derives_market_buy_price_when_fill_improves_under_cap(
        self, conn: sqlite3.Connection
    ) -> None:
        """Cash-sized FAK buys can fill below their executable cap.

        CLOB may return only the matched share size. If the requested cash
        amount divided by that size is a valid price below the cap, that is the
        average fill and should not strand settlement.
        """
        ledger_id = _insert_live_pending(
            conn,
            live_order_id="order-price-improved",
            side="YES",
            bet_size=4.142162676416512,
        )
        fetcher = lambda oid: {
            "order_id": oid,
            "status": "MATCHED",
            "price": "0.43",
            "size_matched": "9.857141",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        row = conn.execute(
            "SELECT live_filled_size, live_filled_price FROM ledger WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["live_filled_size"] == pytest.approx(9.857141)
        assert row["live_filled_price"] == pytest.approx(
            4.142162676416512 / 9.857141
        )

    def test_partial_fill_falls_back_to_limit_price(
        self, conn: sqlite3.Connection
    ) -> None:
        """Root-cause fix (LIVE row 571): a PARTIAL fill whose payload carries
        only the order limit `price` (no avg_fill_price / filled_value, and an
        implied price above the cap) must persist BOTH live_filled_size AND
        live_filled_price — booking the conservative limit price — rather than
        stranding the row with size-but-no-price for the sweep to skip forever.
        A marketable FAK/limit BUY only fills at/below `price`, so the limit is
        a safe upper bound (over-states cost; never over-books a win)."""
        ledger_id = _insert_live_pending(conn, live_order_id="order-partial")
        fetcher = lambda oid: {
            "order_id": oid,
            "status": "MATCHED_PARTIAL",
            "price": "0.55",
            "original_size": "9.0909",
            "size_matched": "3.0",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        row = conn.execute(
            "SELECT live_filled_size, live_filled_price FROM ledger WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["live_filled_size"] == pytest.approx(3.0)
        assert row["live_filled_price"] == pytest.approx(0.55)

    def test_row_571_partial_fill_persists_price(
        self, conn: sqlite3.Connection
    ) -> None:
        """The exact live bug: order 571 partial-filled 0.33 shares of a 1.097
        USD intended stake at limit 0.76 and the reconciler wrote size WITHOUT
        price (live_filled_price stayed NULL), stranding the sweep. It must now
        persist live_filled_price=0.76 so settlement can proceed."""
        ledger_id = _insert_live_pending(
            conn, live_order_id="order-571", bet_size=1.097
        )
        fetcher = lambda oid: {
            "order_id": oid,
            "status": "MATCHED_PARTIAL",
            "price": "0.76",
            "original_size": "1.4434",  # 1.097 / 0.76
            "size_matched": "0.33",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        row = conn.execute(
            "SELECT live_filled_size, live_filled_price FROM ledger WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["live_filled_size"] == pytest.approx(0.33)
        assert row["live_filled_price"] == pytest.approx(0.76)

    def test_derives_fill_price_from_filled_value(
        self, conn: sqlite3.Connection
    ) -> None:
        ledger_id = _insert_live_pending(conn, live_order_id="order-value")
        fetcher = lambda oid: {
            "order_id": oid,
            "status": "MATCHED_PARTIAL",
            "size_matched": "3.5",
            "filled_value": "1.925",
        }
        reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        row = conn.execute(
            "SELECT live_filled_size, live_filled_price FROM ledger WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["live_filled_size"] == pytest.approx(3.5)
        assert row["live_filled_price"] == pytest.approx(0.55)

    def test_reconciler_overwrite_after_immediate_persist_is_harmless(
        self, conn: sqlite3.Connection
    ) -> None:
        """PURE_STOP_04 immediate-fill fix (2026-07-10): the accept branch now
        persists venue-truth fill columns at accept time. The 60s reconciler
        later re-reads the SAME order and overwrites with venue truth. That must
        stay harmless: the row stays PENDING (reconciler never settles), no
        duplicate row is created, and the fill equals the venue value — the
        reconciler remains the authority without conflicting with the immediate
        write."""
        ledger_id = _insert_live_pending(conn, live_order_id="order-imm")
        # Immediate persist already landed the fill at accept time.
        conn.execute(
            "UPDATE ledger SET live_filled_size=?, live_filled_price=?, "
            "live_fill_ts=? WHERE id=?",
            (5.0, 0.72, "2026-07-10T00:00:00Z", ledger_id),
        )
        conn.commit()
        before = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]

        fetcher = lambda oid: {
            "order_id": "order-imm",
            "status": "MATCHED",
            "size_matched": 5.0,
            "avg_fill_price": 0.72,
            "matched_at": "2026-07-10T00:00:05Z",
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["reconciled"] == 1
        after = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        assert after == before  # no duplicate ledger row

        row = conn.execute(
            "SELECT outcome, live_filled_size, live_filled_price FROM ledger "
            "WHERE id=?",
            (ledger_id,),
        ).fetchone()
        assert row["outcome"] == "PENDING"  # reconciler never settles the row
        assert row["live_filled_size"] == pytest.approx(5.0)
        assert row["live_filled_price"] == pytest.approx(0.72)

    def test_cancelled_with_zero_fill_transitions_to_cancelled(
        self, conn: sqlite3.Connection
    ) -> None:
        """D-LIVE-17: CLOB CANCELLED + filled=0 → outcome='CANCELLED'."""
        ledger_id = _insert_live_pending(conn, live_order_id="order-2")
        fetcher = lambda oid: {
            "order_id": "order-2",
            "status": "CANCELED",
            "size_matched": 0,
        }
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["cancelled"] == 1
        row = conn.execute(
            "SELECT outcome, pnl_net FROM ledger WHERE id=?", (ledger_id,)
        ).fetchone()
        assert row["outcome"] == "CANCELLED"
        assert row["pnl_net"] == 0

    def test_still_pending_when_no_fill(self, conn: sqlite3.Connection) -> None:
        _insert_live_pending(conn, live_order_id="order-3")
        fetcher = lambda oid: {"order_id": "order-3", "status": "OPEN"}
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["still_pending"] == 1

    def test_fetcher_returns_none_counts_unreachable(
        self, conn: sqlite3.Connection
    ) -> None:
        _insert_live_pending(conn, live_order_id="order-down")
        fetcher = lambda oid: None
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts["unreachable"] == 1

    def test_dry_rows_ignored(self, conn: sqlite3.Connection) -> None:
        # DRY row with a live_order_id should still be excluded.
        conn.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, sleeve, entry_minute, side, window_start_ts,
               window_end_ts, interval_min, poly_market_id, poly_token_id,
               fee_rate_bps, model_p, p_market, fill_price, realized_edge,
               target_usd, bet_size, entry_top_price, config_sha256, mode,
               live_order_id)
            VALUES ('2026-05-24', 'BTC', 'v6_5m', 4, 'YES', 1715923200,
                    1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55, 0.55, 0.10,
                    5.0, 5.0, 0.55, 'sha', 'DRY', 'order-dry')
            """,
        )
        conn.commit()
        fetcher = lambda oid: {"order_id": oid, "status": "MATCHED", "size_matched": 1.0}
        counts = reconcile_open_orders(conn, MagicMock(), order_fetcher=fetcher)
        assert counts == {"reconciled": 0, "cancelled": 0, "still_pending": 0, "unreachable": 0}


# ============================================================================
# reconcile_stale_attempts
# ============================================================================


def _make_signal(wallet_id: str = "v6_5m", entry_minute: int = 4):
    from cryptobot.execution.ledger import BetSignal

    return BetSignal(
        coin="BTC",
        sleeve="v6_5m",
        entry_minute=entry_minute,
        side="YES",
        window_start_ts=1715923200,
        window_end_ts=1715923500,
        interval_min=5,
        poly_market_id="M",
        poly_token_id="token-yes",
        fee_rate_bps=100.0,
        model_p=0.78,
        p_market=0.55,
        fill_price=0.55,
        walked_vwap=0.55,
        realized_edge=0.10,
        partial_ret_z_1h=None,
        lut_p_bucket=None,
        lut_hit_rate=None,
        lut_n_cum=None,
        target_usd=5.0,
        bet_size=5.0,
        slot_filled_pre=0.0,
        entry_top_price=0.55,
        yes_pre1_anchor=0.55,
        config_sha256="sha",
        wallet_id=wallet_id,
    )


class TestReconcileStaleAttempts:
    def test_no_stale_returns_zero(self, conn: sqlite3.Connection) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        counts = reconcile_stale_attempts(
            conn, MagicMock(), stale_seconds=600
        )
        assert counts == {
            "recovered": 0,
            "released": 0,
            "still_active": 0,
            "orphaned": 0,
        }

    def test_stale_without_clob_match_released(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        # Backdate the row so it counts as stale.
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=lambda: [],
        )
        assert counts["released"] == 1

        row = get_attempt(conn, attempt.id)
        assert row.status == FAILED
        assert row.error_kind == "not_found_after_timeout"

    def test_stale_with_clob_match_recovers_ledger_row(
        self, conn: sqlite3.Connection
    ) -> None:
        """Audit P0 #2: an orphan CLOB order (a crash between the FAK fill
        and record_bet) must be RECONSTRUCTED into a LIVE PENDING ledger row
        from the attempt's persisted signal_json, then LINKED — not left as
        a manual-only CRITICAL where the position is invisible to caps/PnL."""
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00', "
            "live_order_id='clob-order-orphan' WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        clob_orders = [
            {
                "id": "clob-order-orphan",
                "client_order_id": attempt.client_order_id,
                "status": "MATCHED",
                "size_matched": 9.0,
                "avg_fill_price": 0.55,
                "matched_at": "2026-05-24 14:05:00",
            }
        ]
        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=lambda: clob_orders,
        )
        assert counts["recovered"] == 1
        assert counts["orphaned"] == 0
        assert counts["released"] == 0

        # The attempt is now LINKED to a freshly reconstructed ledger row.
        row = get_attempt(conn, attempt.id)
        assert row.status == LINKED
        assert row.ledger_id is not None

        led = conn.execute(
            "SELECT mode, outcome, live_client_order_id, live_filled_size, "
            "bet_size, wallet_group FROM ledger WHERE id=?",
            (row.ledger_id,),
        ).fetchone()
        assert led["mode"] == "LIVE"
        assert led["outcome"] == "PENDING"
        assert led["live_client_order_id"] == attempt.client_order_id
        assert led["live_filled_size"] == 9.0  # real CLOB fill, not placeholder
        assert led["bet_size"] == 5.0
        assert led["wallet_group"] == "group_5m"

    def test_orphan_links_existing_ledger_row_no_duplicate(
        self, conn: sqlite3.Connection
    ) -> None:
        """Dedup: a crash AFTER record_bet but BEFORE link leaves a ledger
        row already carrying this client_order_id. Recovery must LINK to it,
        never insert a duplicate."""
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        # Simulate record_bet having already run (row exists) but link did not.
        existing_id = _insert_live_pending(conn, live_order_id="clob-dup")
        conn.execute(
            "UPDATE ledger SET live_client_order_id=? WHERE id=?",
            (attempt.client_order_id, existing_id),
        )
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (attempt.id,),
        )
        conn.commit()
        before = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]

        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=lambda: [
                {"id": "clob-dup", "client_order_id": attempt.client_order_id}
            ],
        )
        after = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        assert counts["recovered"] == 1
        assert after == before  # no duplicate ledger row inserted
        row = get_attempt(conn, attempt.id)
        assert row.status == LINKED
        assert row.ledger_id == existing_id

    def test_stale_match_by_live_order_id_when_client_id_missing(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_submitted(conn, attempt.id, "clob-order-id-only")
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=lambda: [
                {
                    "id": "clob-order-id-only",
                    "status": "MATCHED",
                    "size_matched": 9.0,
                    "avg_fill_price": 0.55,
                }
            ],
        )

        assert counts["recovered"] == 1
        row = get_attempt(conn, attempt.id)
        assert row.status == LINKED

    def test_stale_match_by_order_intent_when_ids_missing(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        conn.execute(
            "UPDATE live_order_attempts "
            "SET created_at='2026-05-24 14:04:00', "
            "updated_at='2026-01-01 00:00:00' WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=lambda: [
                {
                    "asset_id": "token-yes",
                    "side": "BUY",
                    "price": 0.55,
                    "size": 5.0 / 0.55,
                    "created_at": "2026-05-24 14:04:30",
                    "status": "MATCHED",
                    "size_matched": 9.0,
                    "avg_fill_price": 0.55,
                }
            ],
        )

        assert counts["recovered"] == 1
        row = get_attempt(conn, attempt.id)
        assert row.status == LINKED

    def test_orphan_unreconstructable_escalates(
        self, conn: sqlite3.Connection
    ) -> None:
        """When the signal can't be reconstructed, escalate (CRITICAL
        pipeline_health row) rather than silently counting it recovered."""
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        # Wipe the persisted signal so reconstruction fails.
        conn.execute(
            "UPDATE live_order_attempts SET signal_json=NULL, "
            "updated_at='2026-01-01 00:00:00' WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=lambda: [
                {"id": "clob-x", "client_order_id": attempt.client_order_id}
            ],
        )
        assert counts["orphaned"] == 1
        assert counts["recovered"] == 0
        health = conn.execute(
            "SELECT COUNT(*) FROM pipeline_health WHERE stage='reconcile' "
            "AND status='CRITICAL'"
        ).fetchone()[0]
        assert health == 1

    def test_fetcher_exception_leaves_attempts_alone(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        def _boom():
            raise RuntimeError("CLOB down")

        counts = reconcile_stale_attempts(
            conn,
            MagicMock(),
            stale_seconds=10,
            funder_orders_fetcher=_boom,
        )
        assert counts["still_active"] == 1
        # Attempt is still SUBMITTING — recoverable on next tick.
        row = get_attempt(conn, attempt.id)
        assert row.status != FAILED

    def test_default_fetcher_exception_leaves_attempt_active(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (attempt.id,),
        )
        conn.commit()

        class _BrokenClob:
            def get_orders(self):
                raise RuntimeError("CLOB unreachable")

        counts = reconcile_stale_attempts(
            conn,
            SimpleNamespace(_client=_BrokenClob()),
            stale_seconds=10,
        )

        assert counts["still_active"] == 1
        row = get_attempt(conn, attempt.id)
        assert row.status != FAILED
