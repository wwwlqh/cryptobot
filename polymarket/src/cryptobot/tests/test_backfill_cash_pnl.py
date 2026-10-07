from __future__ import annotations

import sqlite3

import pytest

from cryptobot.execution.ledger import record_bet
from cryptobot.reconciliation.backfill_cash_pnl import backfill_cash_pnl
from cryptobot.strategy.parity import cash_pnl
from cryptobot.tests.test_slot_state import make_signal


def _settle(
    conn: sqlite3.Connection,
    row_id: int,
    *,
    outcome: str,
    won_up: int,
    pnl_net: float,
) -> None:
    conn.execute(
        "UPDATE ledger SET outcome = ?, won_up = ?, pnl_net = ? WHERE id = ?",
        (outcome, won_up, pnl_net, row_id),
    )
    conn.commit()


def test_backfill_repairs_old_per_share_loss_math(db: sqlite3.Connection) -> None:
    row_id = record_bet(
        db,
        make_signal(
            side="YES",
            bet_size=5.0,
            fill_price=0.11,
            walked_vwap=0.116666666666667,
        ),
    )
    _settle(db, row_id, outcome="LOSS", won_up=0, pnl_net=-0.857441588010411)

    summary = backfill_cash_pnl(db)

    row = db.execute("SELECT pnl_net, outcome FROM ledger WHERE id = ?", (row_id,)).fetchone()
    assert summary.updated == 1
    assert row["outcome"] == "LOSS"
    assert row["pnl_net"] == pytest.approx(-5.0, abs=1e-9)


def test_backfill_repairs_old_per_share_win_math_and_keeps_theta_fee(
    db: sqlite3.Connection,
) -> None:
    row_id = record_bet(
        db,
        make_signal(
            side="YES",
            bet_size=5.0,
            fill_price=0.22,
            walked_vwap=0.22,
        ),
    )
    _settle(db, row_id, outcome="WIN", won_up=1, pnl_net=3.5541456493224)

    summary = backfill_cash_pnl(db)

    expected = cash_pnl("YES", 0.22, 1, 5.0, slippage_haircut=0.0)
    no_fee = ((1.0 - 0.22) * (5.0 / 0.22))
    row = db.execute("SELECT pnl_net, outcome FROM ledger WHERE id = ?", (row_id,)).fetchone()
    assert summary.updated == 1
    assert row["outcome"] == "WIN"
    assert row["pnl_net"] == pytest.approx(expected, abs=1e-9)
    assert row["pnl_net"] < no_fee


def test_backfill_dry_run_does_not_mutate(db: sqlite3.Connection) -> None:
    row_id = record_bet(db, make_signal(side="YES", bet_size=5.0, fill_price=0.47))
    _settle(db, row_id, outcome="LOSS", won_up=0, pnl_net=-2.62410825467708)

    summary = backfill_cash_pnl(db, dry_run=True)

    row = db.execute("SELECT pnl_net FROM ledger WHERE id = ?", (row_id,)).fetchone()
    assert summary.updated == 1
    assert row["pnl_net"] == pytest.approx(-2.62410825467708, abs=1e-12)
