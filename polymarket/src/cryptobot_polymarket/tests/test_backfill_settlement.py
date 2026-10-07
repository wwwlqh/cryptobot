from __future__ import annotations

import sqlite3

import pytest

from cryptobot_polymarket.gamma import (
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
    PolymarketResolution,
)
from cryptobot.execution.grade_fees import graded_cash_pnl
from cryptobot.execution.ledger import record_bet
from cryptobot_polymarket.backfill_settlement import (
    backfill_polymarket_settlement,
)
from cryptobot.tests.test_slot_state import make_signal


def _settle_with_pyth(
    conn: sqlite3.Connection,
    row_id: int,
    *,
    outcome: str = "LOSS",
    won_up: int = 0,
    pnl_net: float = -10.0,
) -> None:
    conn.execute(
        "UPDATE ledger SET outcome = ?, won_up = ?, resolution_price = ?, "
        "resolution_source = ?, pnl_net = ? WHERE id = ?",
        (outcome, won_up, 64_000.0, "pyth_fallback", pnl_net, row_id),
    )
    conn.commit()


def test_backfill_regrades_pyth_rows_to_polymarket_final_outcome(
    db: sqlite3.Connection,
) -> None:
    row_id = record_bet(
        db,
        make_signal(
            side="YES",
            bet_size=10.0,
            fill_price=0.20,
            walked_vwap=0.20,
            window_start_ts=1_715_923_200,
        ),
    )
    _settle_with_pyth(db, row_id, outcome="LOSS", won_up=0, pnl_net=-10.0)

    summary = backfill_polymarket_settlement(
        db,
        resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_RESOLVED,
            coin=c,
            interval_min=interval,
            window_start_ts=start,
            window_end_ts=start + interval * 60,
            won_up=1,
            up_price=1.0,
            down_price=0.0,
        ),
    )

    row = db.execute(
        "SELECT outcome, won_up, resolution_price, resolution_source, "
        "polymarket_resolution_price, polymarket_resolution_source, pnl_net "
        "FROM ledger WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert summary.scanned == 1
    assert summary.updated == 1
    assert row["outcome"] == "WIN"
    assert row["won_up"] == 1
    assert row["resolution_source"] == "polymarket_gamma"
    assert row["resolution_price"] == pytest.approx(1.0)
    assert row["polymarket_resolution_price"] == pytest.approx(1.0)
    assert row["polymarket_resolution_source"] == "polymarket_gamma"
    # 2026-07-23 calibrated fee grading: the regrade books the calibrated
    # entry-leg fee — S*(1-p) - 0.864*0.07*p*(1-p)*S with S = 10/0.20 = 50 —
    # the same formula the resolution sweep books (no fork).
    expected = (1.0 - 0.20) * 50.0 - 0.864 * 0.07 * 0.20 * 0.80 * 50.0
    assert row["pnl_net"] == pytest.approx(expected, abs=1e-9)
    assert row["pnl_net"] == pytest.approx(
        graded_cash_pnl("YES", 0.20, True, 10.0, venue="polymarket"),
        abs=1e-9,
    )


def test_backfill_leaves_unfinalized_rows_unchanged(db: sqlite3.Connection) -> None:
    row_id = record_bet(db, make_signal(side="YES", bet_size=10.0, fill_price=0.20))
    _settle_with_pyth(db, row_id, outcome="LOSS", won_up=0, pnl_net=-10.0)

    summary = backfill_polymarket_settlement(
        db,
        resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_OPEN,
            coin=c,
            interval_min=interval,
            window_start_ts=start,
            window_end_ts=start + interval * 60,
        ),
    )

    row = db.execute(
        "SELECT outcome, resolution_source, pnl_net FROM ledger WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert summary.updated == 0
    assert summary.unresolved == 1
    assert row["outcome"] == "LOSS"
    assert row["resolution_source"] == "pyth_fallback"
    assert row["pnl_net"] == pytest.approx(-10.0)


def test_backfill_dry_run_does_not_mutate(db: sqlite3.Connection) -> None:
    row_id = record_bet(db, make_signal(side="YES", bet_size=10.0, fill_price=0.20))
    _settle_with_pyth(db, row_id, outcome="LOSS", won_up=0, pnl_net=-10.0)

    summary = backfill_polymarket_settlement(
        db,
        dry_run=True,
        resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_RESOLVED,
            coin=c,
            interval_min=interval,
            window_start_ts=start,
            window_end_ts=start + interval * 60,
            won_up=1,
            up_price=1.0,
            down_price=0.0,
        ),
    )

    row = db.execute(
        "SELECT outcome, resolution_source FROM ledger WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert summary.updated == 1
    assert row["outcome"] == "LOSS"
    assert row["resolution_source"] == "pyth_fallback"
