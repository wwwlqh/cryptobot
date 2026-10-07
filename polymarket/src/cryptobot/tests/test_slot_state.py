"""U3 slot_state / record_bet tests.

Covers AE-NEW-3 (top-up anchor) and AE-NEW-7 (slot-full predicate), plus
the cross-window isolation regression guard called out in the plan's test
scenarios for U3 ("Without this test, a regression that drops
``window_start_ts`` from the WHERE clause would silently produce
cross-window over-fills.").
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from cryptobot.execution.ledger import (
    BetSignal,
    record_bet,
    slot_state,
)


def make_signal(
    *,
    coin: str = "BTC",
    sleeve: str = "v6_5m",
    entry_minute: int = 4,
    side: str = "YES",
    window_start_ts: int = 1_715_923_200,
    bet_size: float = 2.0,
    target_usd: float = 5.0,
    slot_filled_pre: float = 0.0,
    entry_top_price: float = 0.55,
    fill_price: float = 0.55,
    walked_vwap: float | None = 0.55,
    realized_edge: float = 0.10,
    strike_price: float | None = 65_000.0,   # spot at decision (review #2)
    window_open_spot: float | None = 65_000.0,  # spot at window_open (divergence #13 fix)
) -> BetSignal:
    return BetSignal(
        coin=coin,
        sleeve=sleeve,
        entry_minute=entry_minute,
        side=side,
        window_start_ts=window_start_ts,
        window_end_ts=window_start_ts + 300,
        interval_min=5 if sleeve == "v6_5m" else 15,
        poly_market_id="MARKET",
        poly_token_id="TOKEN",
        fee_rate_bps=100.0,
        model_p=0.78,
        p_market=fill_price,
        fill_price=fill_price,
        walked_vwap=walked_vwap,
        realized_edge=realized_edge,
        partial_ret_z_1h=2.5,
        lut_p_bucket=7,
        lut_hit_rate=0.85,
        lut_n_cum=200,
        target_usd=target_usd,
        bet_size=bet_size,
        slot_filled_pre=slot_filled_pre,
        entry_top_price=entry_top_price,
        yes_pre1_anchor=fill_price,
        strike_price=strike_price,
        window_open_spot=window_open_spot,
        config_sha256="sha256-stub",
    )


def slot_args() -> dict:
    return dict(
        coin="BTC",
        sleeve="v6_5m",
        entry_minute=4,
        side="YES",
        window_start_ts=1_715_923_200,
    )


class TestSlotState:
    def test_empty_slot_returns_zero_and_none(self, db: sqlite3.Connection) -> None:
        assert slot_state(db, **slot_args()) == (0.0, None)

    def test_topup_anchor_returns_first_fill_price(self, db: sqlite3.Connection) -> None:
        # Covers AE-NEW-3.
        record_bet(db, make_signal(bet_size=2.0, entry_top_price=0.55))
        filled, anchor = slot_state(db, **slot_args())
        assert filled == pytest.approx(2.0)
        assert anchor == pytest.approx(0.55)

    def test_two_rows_summed(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=2.0, entry_top_price=0.55))
        record_bet(
            db,
            make_signal(
                bet_size=1.5, slot_filled_pre=2.0, entry_top_price=0.55, fill_price=0.55,
            ),
        )
        filled, anchor = slot_state(db, **slot_args())
        assert filled == pytest.approx(3.5)
        assert anchor == pytest.approx(0.55)   # anchor stays at first-fill price

    def test_slot_full(self, db: sqlite3.Connection) -> None:
        # Covers AE-NEW-7: insert two rows summing to bet_size=5.0; slot_full.
        record_bet(db, make_signal(bet_size=3.0, entry_top_price=0.55))
        record_bet(
            db,
            make_signal(
                bet_size=2.0, slot_filled_pre=3.0, entry_top_price=0.55,
            ),
        )
        filled, _ = slot_state(db, **slot_args())
        assert filled == pytest.approx(5.0)

    def test_cross_window_isolation(self, db: sqlite3.Connection) -> None:
        # Regression guard: a regression that drops window_start_ts from
        # the WHERE clause would silently produce cross-window over-fills.
        record_bet(db, make_signal(bet_size=2.0, window_start_ts=1_715_923_200))
        record_bet(
            db,
            make_signal(
                bet_size=3.0, window_start_ts=1_715_923_500,  # next window
            ),
        )
        # Querying window 1 returns 2.0 only — NOT the sum across windows.
        args1 = {**slot_args(), "window_start_ts": 1_715_923_200}
        args2 = {**slot_args(), "window_start_ts": 1_715_923_500}
        assert slot_state(db, **args1)[0] == pytest.approx(2.0)
        assert slot_state(db, **args2)[0] == pytest.approx(3.0)

    def test_slot_isolation_across_sleeves(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=2.0, sleeve="v6_5m", entry_minute=4))
        record_bet(db, make_signal(bet_size=4.0, sleeve="v6_15m", entry_minute=7))
        five = slot_state(db, **{**slot_args(), "sleeve": "v6_5m", "entry_minute": 4})
        fifteen = slot_state(
            db, **{**slot_args(), "sleeve": "v6_15m", "entry_minute": 7}
        )
        assert five[0] == pytest.approx(2.0)
        assert fifteen[0] == pytest.approx(4.0)

    def test_cancelled_rows_excluded(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=2.0))
        # promote one row to CANCELLED — slot_state ignores it
        db.execute("UPDATE ledger SET outcome='CANCELLED'")
        assert slot_state(db, **slot_args()) == (0.0, None)

    def test_pending_only_stays_locked(self, db: sqlite3.Connection) -> None:
        # PENDING-only slot stays fully locked even when the row is old, so a
        # top-up cannot double-stake against an in-flight order.
        record_bet(db, make_signal(bet_size=2.0))
        db.execute(
            "UPDATE ledger SET bet_ts = datetime('now', '-12 hours')"
        )
        filled, _ = slot_state(db, **slot_args())
        assert filled == pytest.approx(2.0)

    def test_stale_pending_excluded_when_filled_row_anchors(
        self, db: sqlite3.Connection
    ) -> None:
        # FILLED row + stale PENDING → stale PENDING dropped from the sum.
        record_bet(db, make_signal(bet_size=3.0))
        db.execute("UPDATE ledger SET outcome='WIN', pnl_net=1.0 WHERE id=1")
        record_bet(db, make_signal(bet_size=2.0, slot_filled_pre=3.0))
        db.execute(
            "UPDATE ledger SET bet_ts = datetime('now', '-12 hours') WHERE id=2"
        )
        filled, _ = slot_state(db, **slot_args())
        # Only the WIN row counts toward the sum (stale PENDING dropped).
        assert filled == pytest.approx(3.0)

    # ------------------------------------------------------------------
    # Filled + stale-PENDING combinations, pinned BEFORE the single-pass
    # conditional-aggregation rewrite so the refactor can be proven
    # bit-identical against them (latency item #3).
    # ------------------------------------------------------------------

    def test_filled_plus_fresh_and_stale_pending_mixed(
        self, db: sqlite3.Connection
    ) -> None:
        # FILLED + fresh PENDING + stale PENDING → only the stale PENDING is
        # dropped; the fresh PENDING still locks its share of the slot.
        record_bet(db, make_signal(bet_size=3.0))
        db.execute("UPDATE ledger SET outcome='WIN', pnl_net=1.0 WHERE id=1")
        record_bet(db, make_signal(bet_size=2.0, slot_filled_pre=3.0))
        db.execute(
            "UPDATE ledger SET bet_ts = datetime('now', '-12 hours') WHERE id=2"
        )
        record_bet(db, make_signal(bet_size=1.5, slot_filled_pre=5.0))
        filled, anchor = slot_state(db, **slot_args())
        # 3.0 (WIN) + 1.5 (fresh PENDING); the 2.0 stale PENDING is dropped.
        assert filled == pytest.approx(4.5)
        assert anchor == pytest.approx(0.55)

    def test_multiple_filled_rows_minus_stale_pending(
        self, db: sqlite3.Connection
    ) -> None:
        # Two FILLED rows + one stale PENDING: sum both fills, drop the stale.
        record_bet(db, make_signal(bet_size=2.0))
        db.execute("UPDATE ledger SET outcome='WIN', pnl_net=1.0 WHERE id=1")
        record_bet(db, make_signal(bet_size=1.0, slot_filled_pre=2.0))
        db.execute("UPDATE ledger SET outcome='LOSS', pnl_net=-1.0 WHERE id=2")
        record_bet(db, make_signal(bet_size=4.0, slot_filled_pre=3.0))
        db.execute(
            "UPDATE ledger SET bet_ts = datetime('now', '-12 hours') WHERE id=3"
        )
        filled, _ = slot_state(db, **slot_args())
        assert filled == pytest.approx(3.0)

    def test_stale_pending_alone_stays_locked_without_filled_row(
        self, db: sqlite3.Connection
    ) -> None:
        # The stale-PENDING drop is gated on the slot having at least one
        # FILLED (non-PENDING) row. Two stale PENDINGs with no fill keep the
        # slot fully locked — same invariant as test_pending_only_stays_locked
        # but with multiple rows, so the n_filled guard is what's pinned here.
        record_bet(db, make_signal(bet_size=2.0))
        record_bet(db, make_signal(bet_size=1.0, slot_filled_pre=2.0))
        db.execute("UPDATE ledger SET bet_ts = datetime('now', '-12 hours')")
        filled, anchor = slot_state(db, **slot_args())
        assert filled == pytest.approx(3.0)
        assert anchor == pytest.approx(0.55)

    def test_anchor_survives_stale_pending_branch(
        self, db: sqlite3.Connection
    ) -> None:
        # The anchor is the EARLIEST non-cancelled row's entry_top_price even
        # when that row is the stale PENDING being dropped from the sum.
        record_bet(db, make_signal(bet_size=2.0, entry_top_price=0.61))
        db.execute(
            "UPDATE ledger SET bet_ts = datetime('now', '-12 hours') WHERE id=1"
        )
        record_bet(
            db,
            make_signal(
                bet_size=3.0, slot_filled_pre=2.0, entry_top_price=0.66,
                fill_price=0.66,
            ),
        )
        db.execute("UPDATE ledger SET outcome='WIN', pnl_net=1.0 WHERE id=2")
        filled, anchor = slot_state(db, **slot_args())
        assert filled == pytest.approx(3.0)
        assert anchor == pytest.approx(0.61)


class TestRecordBet:
    def test_is_topup_inferred_from_slot_filled_pre(self, db: sqlite3.Connection) -> None:
        rowid_initial = record_bet(db, make_signal(bet_size=2.0, slot_filled_pre=0.0))
        rowid_topup = record_bet(
            db, make_signal(bet_size=1.5, slot_filled_pre=2.0)
        )
        rows = db.execute(
            "SELECT id, is_topup FROM ledger ORDER BY id"
        ).fetchall()
        assert rows[0]["id"] == rowid_initial and rows[0]["is_topup"] == 0
        assert rows[1]["id"] == rowid_topup and rows[1]["is_topup"] == 1

    def test_pending_first_invariant(self, db: sqlite3.Connection) -> None:
        # outcome must be PENDING immediately after insert (R18 carry-forward).
        record_bet(db, make_signal())
        row = db.execute("SELECT outcome FROM ledger LIMIT 1").fetchone()
        assert row["outcome"] == "PENDING"

    def test_addon_realized_edge_populated_when_topup(
        self, db: sqlite3.Connection
    ) -> None:
        initial_edge = 0.10
        record_bet(db, make_signal(realized_edge=initial_edge))
        # add-on with realized_edge=0.13 → delta=0.03
        record_bet(
            db,
            make_signal(realized_edge=0.13, slot_filled_pre=2.0, bet_size=1.0),
            initial_realized_edge=initial_edge,
        )
        row = db.execute(
            "SELECT add_on_realized_edge_minus_initial_edge FROM ledger "
            "WHERE is_topup=1"
        ).fetchone()
        assert row[0] == pytest.approx(0.03)

    def test_addon_delta_null_when_initial(
        self, db: sqlite3.Connection
    ) -> None:
        record_bet(db, make_signal())
        row = db.execute(
            "SELECT add_on_realized_edge_minus_initial_edge FROM ledger "
            "WHERE is_topup=0"
        ).fetchone()
        assert row[0] is None


class TestQueryPlan:
    """Composite index idx_ledger_slot is the primary access pattern."""

    def test_slot_state_uses_idx_ledger_slot(self, db: sqlite3.Connection) -> None:
        # Seed enough rows that SQLite would prefer the index over a scan.
        for i in range(200):
            record_bet(
                db,
                make_signal(bet_size=1.0, window_start_ts=1_715_923_200 + i * 60),
            )
        plan = db.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT 1 FROM ledger WHERE coin=? AND sleeve=? AND entry_minute=? "
            "AND side=? AND window_start_ts=? AND outcome != 'CANCELLED'",
            ("BTC", "v6_5m", 4, "YES", 1_715_923_200),
        ).fetchall()
        plan_text = " ".join(row["detail"] for row in plan)
        assert "idx_ledger_slot" in plan_text, plan_text


class TestSlotEntryAnchor:
    """slot_state returns (filled, anchor); the second element is the
    earliest non-cancelled row's entry_top_price (the sticky walker anchor)."""

    def test_returns_none_when_empty(self, db: sqlite3.Connection) -> None:
        filled, anchor = slot_state(db, **slot_args())
        assert filled == 0.0
        assert anchor is None

    def test_returns_earliest_anchor(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(entry_top_price=0.55))
        time.sleep(0.01)
        record_bet(
            db,
            make_signal(
                slot_filled_pre=2.0, entry_top_price=0.60, fill_price=0.60,
            ),
        )
        _, anchor = slot_state(db, **slot_args())
        assert anchor == pytest.approx(0.55)
