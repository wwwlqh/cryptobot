"""Settlement backfill for predict.fun ledger rows (venue-scoped, offline)."""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot import config
from cryptobot.venues.types import (
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
    PolymarketResolution,
)
from cryptobot.execution.grade_fees import graded_cash_pnl
from cryptobot.execution.ledger import record_bet
from cryptobot_predictfun.backfill_settlement import (
    backfill_predictfun_settlement,
)
from cryptobot.tests.test_slot_state import make_signal


def _resolved(won_up: int):
    return lambda c, interval, start: PolymarketResolution(
        status=RESOLUTION_RESOLVED,
        coin=c,
        interval_min=interval,
        window_start_ts=start,
        window_end_ts=start + interval * 60,
        won_up=won_up,
        up_price=1.0 if won_up else 0.0,
        down_price=0.0 if won_up else 1.0,
        source="predictfun",
    )


def _open():
    return lambda c, interval, start: PolymarketResolution(
        status=RESOLUTION_OPEN,
        coin=c,
        interval_min=interval,
        window_start_ts=start,
        window_end_ts=start + interval * 60,
    )


def _record_predictfun(db: sqlite3.Connection, monkeypatch, **kw) -> int:
    monkeypatch.setattr(config, "VENUE", "predictfun")
    row_id = record_bet(db, make_signal(**kw))
    monkeypatch.setattr(config, "VENUE", "polymarket")
    return row_id


def test_settles_pending_predictfun_row(db, monkeypatch):
    row_id = _record_predictfun(
        db, monkeypatch,
        side="YES", bet_size=10.0, fill_price=0.20, walked_vwap=0.20,
        window_start_ts=1_715_923_200,
    )
    # Live predict.fun markets carry feeRateBps=200; the grade must read the
    # ROW's value (make_signal stamps 100 — pin the passthrough, not a const).
    db.execute("UPDATE ledger SET fee_rate_bps=200.0 WHERE id=?", (row_id,))
    summary = backfill_predictfun_settlement(db, resolution_fetch=_resolved(1))
    row = db.execute(
        "SELECT outcome, won_up, venue, resolution_source, pnl_net FROM ledger "
        "WHERE id=?",
        (row_id,),
    ).fetchone()
    assert summary.scanned == 1
    assert summary.updated == 1
    assert row["outcome"] == "WIN"
    assert row["won_up"] == 1
    assert row["venue"] == "predictfun"
    assert row["resolution_source"] == "predictfun"
    # 2026-07-23 calibrated fee grading: predict.fun books the TENT off the
    # row's own bps — S*(1-p) - (200/1e4)*min(p,1-p)*S with S = 10/0.20 = 50.
    expected = (1.0 - 0.20) * 50.0 - 0.02 * 0.20 * 50.0
    assert row["pnl_net"] == pytest.approx(expected, abs=1e-9)
    assert row["pnl_net"] == pytest.approx(
        graded_cash_pnl(
            "YES", 0.20, True, 10.0, venue="predictfun", fee_rate_bps=200.0
        ),
        abs=1e-9,
    )


def test_open_market_leaves_row_pending(db, monkeypatch):
    row_id = _record_predictfun(db, monkeypatch, side="YES", bet_size=10.0, fill_price=0.20)
    summary = backfill_predictfun_settlement(db, resolution_fetch=_open())
    row = db.execute("SELECT outcome FROM ledger WHERE id=?", (row_id,)).fetchone()
    assert summary.unresolved == 1
    assert summary.updated == 0
    assert row["outcome"] == "PENDING"


def test_dry_run_does_not_mutate(db, monkeypatch):
    row_id = _record_predictfun(db, monkeypatch, side="YES", bet_size=10.0, fill_price=0.20)
    summary = backfill_predictfun_settlement(
        db, dry_run=True, resolution_fetch=_resolved(1)
    )
    row = db.execute("SELECT outcome FROM ledger WHERE id=?", (row_id,)).fetchone()
    assert summary.updated == 1
    assert row["outcome"] == "PENDING"


def test_ignores_polymarket_venue_rows(db, monkeypatch):
    # A polymarket-venue row (default) must never be touched by the predict.fun
    # backfill even with a RESOLVED fetch.
    monkeypatch.setattr(config, "VENUE", "polymarket")
    poly_id = record_bet(db, make_signal(side="YES", bet_size=10.0, fill_price=0.20))
    summary = backfill_predictfun_settlement(db, resolution_fetch=_resolved(1))
    row = db.execute("SELECT outcome, venue FROM ledger WHERE id=?", (poly_id,)).fetchone()
    assert summary.scanned == 0  # no predictfun rows
    assert row["outcome"] == "PENDING"
    assert row["venue"] == "polymarket"
