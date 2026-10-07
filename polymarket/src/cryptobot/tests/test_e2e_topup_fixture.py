"""U16 fixture-backed top-up determinism test.

Drives the pipeline twice with the same stable book and qualifying
minute — asserts the second tick produces an add-on row (is_topup=True)
with the sticky cap honored.

Per the plan U16 #3: deterministically validates the top-up code path
without relying on live market behavior.
"""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot_polymarket.gamma import PolyMarket
from cryptobot.data.binance import BinanceSpot
from cryptobot.execution.pipeline import TickDeps, run_scanner_tick
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.tests._fixtures import seed_for_tests
from cryptobot.tests._legacy_artifact import legacy_portfolio
from cryptobot.tests._stub_scorer import install_fake_scorer


# 2026-07-31: the stub RF forest + synthetic LUT this fixture used to build fed
# the lut|rf|blend scorer, retired with the pair sleeves. `tests/_stub_scorer`
# reproduces their exact numbers through the pipeline's scorer hook instead.


@pytest.fixture(autouse=True)
def _stub_scorer(monkeypatch):
    install_fake_scorer(monkeypatch)


def _build_stubs(now_ts):
    spots = {
        coin: BinanceSpot(coin=coin, price=p, publish_time=now_ts - 1)
        for coin, p in (
            ("BTC", 67_000.0), ("ETH", 3_100.0), ("SOL", 175.0),
            ("XRP", 0.52),
        )
    }
    window5 = (now_ts // 300) * 300
    markets = {
        (coin, 5, window5): PolyMarket(
            coin=coin, interval_min=5,
            window_start_ts=window5, window_end_ts=window5 + 300,
            condition_id=f"COND-{coin}",
            yes_token_id=f"YES-{coin}", no_token_id=f"NO-{coin}",
            fee_rate_bps=100.0,
        )
        for coin in ("BTC", "ETH", "SOL", "XRP")
    }

    def yes_book_factory(top: float):
        return BookSnapshot(
            token_id="x",
            asks=[{"price": top, "size": 50.0}, {"price": top + 0.01, "size": 50.0}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
    def no_book_factory(top: float):
        return BookSnapshot(
            token_id="x",
            asks=[{"price": top, "size": 50.0}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
    books = {}
    for c in ("BTC", "ETH", "SOL", "XRP"):
        books[f"YES-{c}"] = yes_book_factory(0.53)
        books[f"NO-{c}"] = no_book_factory(0.45)

    return spots, markets, books


def test_topup_lands_on_second_tick(db: sqlite3.Connection, monkeypatch):
    # v6_5m target_bet_size is 0.05 of current capital ($5 from the
    # default $100 starting capital) at BET_SIZE_MULTIPLIER=1.0. Force the
    # multiplier to 5.0 so the test can
    # create a clean initial-fill + top-up split with plenty of room above
    # the dry-run min-bet floor.
    monkeypatch.setattr(config, "BET_SIZE_MULTIPLIER", 5.0)
    # Pick a 5m qualifying timestamp that does not also activate 15m.
    now_ts = 1_715_923_200 + 7 * 60
    spots, markets, books = _build_stubs(now_ts)
    portfolio = legacy_portfolio()

    # New training-parity engine requires ~30+ minutes of warmup before
    # current_z returns a value. Seed state directly so this E2E test
    # exercises the topup path, not the no_feature warmup path.
    eng = PartialRetEngine()
    for coin, spot in spots.items():
        seed_for_tests(eng, coin, now_ts=now_ts, spot=spot.price)

    deps = TickDeps(
        partial_ret_engine=eng,
        config_sha256="stub",
        fetch_spots=lambda *, now_ts, tick_seconds: spots,
        fetch_market=lambda coin, *, interval_min, window_start_ts, conn, now_ts: markets.get((coin, interval_min, window_start_ts)),
        fetch_book=lambda token_id, *, now_ts: books.get(token_id),
        get_disk_free_mb=lambda: 10_000.0,
    )

    # Tick 1 — initial fill lands.
    r1 = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    assert r1.pending_rows_written >= 1
    initial_rows = db.execute(
        "SELECT COUNT(*) FROM ledger WHERE is_topup=0"
    ).fetchone()[0]
    assert initial_rows >= 1

    # Tick 2 — same book one second later; the slot is not full yet.
    # With the test's BET_SIZE_MULTIPLIER=5.0 monkeypatch, the 5m
    # target_bet_size becomes $25. Thin book at 8 shares x 0.53 = $4.24
    # leaves plenty to top up, with both legs clear of the min-bet floor.

    # Reset: thin-book retry. Replace books with depth=8 shares x 0.53=$4.24.
    thin_books = {}
    for c in ("BTC", "ETH", "SOL", "XRP"):
        thin_books[f"YES-{c}"] = BookSnapshot(
            token_id="x",
            asks=[{"price": 0.53, "size": 8.0}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
        thin_books[f"NO-{c}"] = BookSnapshot(
            token_id="x",
            asks=[{"price": 0.45, "size": 8.0}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
    db.execute("DELETE FROM ledger")
    db.execute("DELETE FROM signals")
    db.execute("DELETE FROM entry_scan_latches")

    deps_thin = TickDeps(
        partial_ret_engine=eng,
        config_sha256="stub",
        fetch_spots=lambda *, now_ts, tick_seconds: spots,
        fetch_market=lambda coin, *, interval_min, window_start_ts, conn, now_ts: markets.get((coin, interval_min, window_start_ts)),
        fetch_book=lambda token_id, *, now_ts: thin_books.get(token_id),
        get_disk_free_mb=lambda: 10_000.0,
    )
    # Tick 1: thin book caps the fill at $4.40 per cell.
    r1 = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps_thin)
    # Tick 2 on the SAME slot: walker re-runs and adds another fill
    # because slot_filled_usd ($4.40) < target ($25 under the test multiplier).
    r2 = run_scanner_tick(db, portfolio, now_ts=now_ts + 1, deps=deps_thin)
    topup_rows = db.execute(
        "SELECT COUNT(*) FROM ledger WHERE is_topup=1"
    ).fetchone()[0]
    assert topup_rows >= 1, "expected at least one add-on row on tick 2"

    # Verify sticky cap: every topup row's fill_price <=
    # entry_top_price + MAX_WALK_PRICE.
    rows = db.execute(
        "SELECT fill_price, entry_top_price FROM ledger WHERE is_topup=1"
    ).fetchall()
    for r in rows:
        assert r["fill_price"] <= r["entry_top_price"] + config.MAX_WALK_PRICE + 1e-9
