"""U9 pipeline orchestrator tests — fixture-backed, no network.

Covers AE-NEW-1 / AE-NEW-3 / AE-NEW-4 / AE-NEW-5 / AE-NEW-8 from the
plan, plus the D5 two-phase loop and the D10 market-level dedup.
"""

from __future__ import annotations

import shutil
import sqlite3
import math
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot_polymarket.gamma import (
    PolyMarket,
    _market_cache_put,
    clear_market_cache,
)
from cryptobot.data.binance import BinanceSpot
from cryptobot.execution.pipeline import (
    TickDeps,
    _prefetch_next_window_markets,
    run_scanner_tick,
)
from cryptobot.scheduler.book_prefetch import BookPrefetcher, BookSourceChain
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.tests._fixtures import seed_for_tests
from cryptobot.tests._stub_scorer import install_fake_scorer
from cryptobot.strategy.score import ScoreResult
from cryptobot.strategy.portfolio_loader import V6Portfolio


# ---- Stub artifacts: avoid the slow real checkpoint_v1 bundle ----------
#
# 2026-07-31: the stub RF forest + synthetic LUT this module used to build fed
# the lut|rf|blend scorer, retired with the pair sleeves. One scorer stub
# (`tests/_stub_scorer`) replaces both and reproduces their exact numbers, so
# every edge / side / bet-size assertion below is unchanged.


@pytest.fixture(autouse=True)
def _stub_scorer(monkeypatch):
    install_fake_scorer(monkeypatch)


def _make_portfolio() -> V6Portfolio:
    """Pipeline-mechanic tests are calibrated against the legacy
    `dry_run_recommended.json` entry_minute_sets and exposure caps. The
    chunk-CV all_4 artifact's entry minutes diverge, so anchoring the
    fixture at the legacy artifact keeps these tests stable across
    deploys. All_4 behavior is tested separately in test_portfolio_loader.py
    and the all_4-scoped tests in this module."""
    from cryptobot.tests._legacy_artifact import legacy_portfolio
    return legacy_portfolio()


# ---- Deps factory -----------------------------------------------------


@dataclass
class StubDeps:
    """Closure-friendly stub for TickDeps. Override per-test."""

    market_lookup: dict[tuple[str, int, int], PolyMarket]
    book_lookup: dict[str, BookSnapshot]
    spot_data: dict[str, BinanceSpot]

    def fetch_spots(self, *, now_ts: float, tick_seconds: int) -> Optional[dict]:
        return self.spot_data

    def fetch_market(self, coin: str, *, interval_min: int, window_start_ts: int, conn, now_ts: float) -> Optional[PolyMarket]:
        return self.market_lookup.get((coin, interval_min, window_start_ts))

    def fetch_book(self, token_id: str, *, now_ts: float) -> Optional[BookSnapshot]:
        return self.book_lookup.get(token_id)

    def disk_free(self) -> float:
        return 10_000.0


def _stub_deps(stub: StubDeps) -> TickDeps:
    return TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub-sha256",
        fetch_spots=stub.fetch_spots,
        fetch_market=stub.fetch_market,
        fetch_book=stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
    )


def _spots(now_ts: int = 1_715_923_440) -> dict[str, BinanceSpot]:
    return {
        coin: BinanceSpot(coin=coin, price=p, publish_time=now_ts - 1)
        for coin, p in (
            ("BTC", 67_000.0), ("ETH", 3_100.0), ("SOL", 175.0),
            ("XRP", 0.52),
        )
    }


def _book(token_id: str, top: float, depth: float = 100.0, now_ts: float = 1_715_923_440.0) -> BookSnapshot:
    return BookSnapshot(
        token_id=token_id,
        asks=[{"price": top, "size": depth}],
        bids=[],
        fetched_at_ts=now_ts,
    )


def _build_markets(window_start: int, interval_min: int = 5) -> dict[tuple[str, int, int], PolyMarket]:
    out = {}
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        out[(coin, interval_min, window_start)] = PolyMarket(
            coin=coin,
            interval_min=interval_min,
            window_start_ts=window_start,
            window_end_ts=window_start + interval_min * 60,
            condition_id=f"COND-{coin}",
            yes_token_id=f"YES-{coin}",
            no_token_id=f"NO-{coin}",
            fee_rate_bps=100.0,
        )
    return out


# ---- Tests -----------------------------------------------------------


def test_not_entry_minute_skips_without_api_calls(db: sqlite3.Connection) -> None:
    # Pick a minute that's NOT in either sleeve's entry_minute_set.
    # Current dry-run config fires 5m at mod5=2 and 15m at
    # mod15 in {2,4,6,7,9}.
    portfolio = _make_portfolio()
    class BlowUp:
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(now_ts=now_ts)
        def fetch_market(self, *a, **kw):
            raise AssertionError("fetch_market should not be called at not_entry_minute")
        def fetch_book(self, *a, **kw):
            raise AssertionError("fetch_book should not be called at not_entry_minute")
        def disk_free(self):
            return 10_000.0
    blowup = BlowUp()
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=blowup.fetch_spots,
        fetch_market=blowup.fetch_market,
        fetch_book=blowup.fetch_book,
        get_disk_free_mb=blowup.disk_free,
    )
    # 1_715_923_200 has mod5=0/mod15=5; +21min -> mod5=1/mod15=11.
    bad_minute_ts = 1_715_923_200 + 21 * 60
    result = run_scanner_tick(db, portfolio, now_ts=bad_minute_ts, deps=deps)
    assert result.pending_rows_written == 0
    # Expected skip count scales with the active runtime coin universe.
    assert result.skips_by_reason.get("not_entry_minute") == len(config.COINS) * 3


def test_weekday_schedule_filter_skips_weekend_without_api_calls(
    db: sqlite3.Connection,
) -> None:
    base = _make_portfolio()
    weekday_sleeve = next(s for s in base.sleeves if s.label == "v6_5m")
    assert weekday_sleeve.dow_set == "weekday"
    portfolio = replace(base, sleeves=(weekday_sleeve,))

    class BlowUp:
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(now_ts=now_ts)

        def fetch_market(self, *a, **kw):
            raise AssertionError("fetch_market should not be called for schedule skips")

        def fetch_book(self, *a, **kw):
            raise AssertionError("fetch_book should not be called for schedule skips")

        def disk_free(self):
            return 10_000.0

    blowup = BlowUp()
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=blowup.fetch_spots,
        fetch_market=blowup.fetch_market,
        fetch_book=blowup.fetch_book,
        get_disk_free_mb=blowup.disk_free,
    )
    saturday_entry_ts = int(
        datetime(2026, 5, 23, 8, 2, tzinfo=timezone.utc).timestamp()
    )

    result = run_scanner_tick(db, portfolio, now_ts=saturday_entry_ts, deps=deps)

    assert result.pending_rows_written == 0
    assert result.cells_evaluated == 0
    assert result.skips_by_reason.get("schedule_filter") == len(config.COINS)


def test_no_feature_skip_avoids_polymarket_calls(db: sqlite3.Connection) -> None:
    # At a qualifying entry minute, an un-warmed PartialRetEngine cannot
    # produce the training-parity feature. The pipeline should skip before
    # spending time on market/book calls that cannot affect the decision.
    portfolio = _make_portfolio()
    class BlowUp:
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(now_ts=now_ts)
        def fetch_market(self, *a, **kw):
            raise AssertionError("fetch_market should not be called before no_feature")
        def fetch_book(self, *a, **kw):
            raise AssertionError("fetch_book should not be called before no_feature")
        def disk_free(self):
            return 10_000.0
    blowup = BlowUp()
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=blowup.fetch_spots,
        fetch_market=blowup.fetch_market,
        fetch_book=blowup.fetch_book,
        get_disk_free_mb=blowup.disk_free,
    )
    now_ts = 1_715_923_200 + 7 * 60

    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

    assert result.pending_rows_written == 0
    assert result.skips_by_reason.get("no_feature") == len(config.COINS) * 2


def test_qualifying_tick_evaluates_all_coins(db: sqlite3.Connection) -> None:
    # mod5=2 -> 5m sleeve fires; mod15=12 -> 15m sleeve does not.
    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    # Seed engine state directly so current_z returns a parity z without
    # 60 minutes of simulated warmup ticks (training-formula now requires
    # rv_1h_at_open which the warmup loop did not provide).
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    portfolio = _make_portfolio()
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    # |COINS| x 1 sleeve (5m at entry minute 2) cells evaluated. The 15m sleeve
    # is at "not_entry_minute" so its cells produce signal rows instead.
    assert result.cells_evaluated == len(config.COINS) * 2


def test_gamma_fanout_gives_each_executor_thread_its_own_connection(
    tmp_path,
) -> None:
    """Regression: the Pass-1 Gamma fan-out (>1 cold cells) must NOT let its
    _clob_executor worker closures share the tick's sqlite3 connection.

    Two executor threads touching one connection's prepared-statement cache
    concurrently (the fee-cache SELECT + always-miss INSERT/DELETE on every
    *-updown-* market) raised sqlite3.InterfaceError (SQLITE_MISUSE), which
    the fan-out's blanket ``except`` silently turned into a missed bet. The
    fix hands each worker its OWN thread-local connection via
    ``deps.conn_factory`` (the ThreadLocalConnections handle). This test drives
    a real fan-out and asserts every executor-thread fetch received a
    connection that is (a) NOT the tick's connection and (b) stable per worker
    thread, while results still flow through unchanged.
    """
    import threading

    from cryptobot.db import init_db
    from cryptobot.db.connection import ThreadLocalConnections

    # File-backed DB so ThreadLocalConnections hands each thread a DISTINCT
    # connection (a ``:memory:`` DB is forced into shared-connection mode and
    # could not exhibit — nor test — the per-thread isolation the fix relies on).
    db_path = tmp_path / "fanout.db"
    init_db(db_path).close()  # bootstrap schema once
    conns = ThreadLocalConnections(db_path)
    tick_conn = conns.get()  # the main (tick) thread's connection
    main_ident = threading.get_ident()

    now_ts = 1_715_923_200 + 7 * 60  # mod5=2 -> 5m sleeve fires for every coin
    window5 = (now_ts // 300) * 300
    markets = _build_markets(window5, interval_min=5)

    records: list[tuple[int, int]] = []  # (thread_ident, id(conn))
    rec_lock = threading.Lock()

    def recording_fetch_market(
        coin: str, *, interval_min: int, window_start_ts: int, conn, now_ts: float
    ) -> Optional[PolyMarket]:
        # Prove the handed connection is a live, usable sqlite3 connection —
        # exactly what the real fee-cache SELECT/INSERT needs (a shared conn
        # would raise SQLITE_MISUSE here under concurrency).
        conn.execute("SELECT 1").fetchone()
        with rec_lock:
            records.append((threading.get_ident(), id(conn)))
        return markets.get((coin, interval_min, window_start_ts))

    stub = StubDeps(
        market_lookup=markets,
        book_lookup=(
            {f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts)
             for c in ("BTC", "ETH", "SOL", "XRP")}
            | {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts)
               for c in ("BTC", "ETH", "SOL", "XRP")}
        ),
        spot_data=_spots(now_ts=now_ts),
    )
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=stub.fetch_spots,
        fetch_market=recording_fetch_market,
        fetch_book=stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
        conn_factory=conns.get,
    )
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    portfolio = _make_portfolio()
    try:
        result = run_scanner_tick(tick_conn, portfolio, now_ts=now_ts, deps=deps)
    finally:
        conns.close_all()

    # The sleeve fan-out (both 5m sleeves fire for every live coin) resolves
    # each cold cell on a _clob_executor WORKER thread. Nothing resolves inline
    # any more: the pair sleeves that did were retired 2026-07-31, so
    # `inline_records` is now expected to be EMPTY.
    exec_records = [(tid, cid) for tid, cid in records if tid != main_ident]
    inline_records = [(tid, cid) for tid, cid in records if tid == main_ident]

    # Fan-out actually engaged with >=2 cold cells (one per live coin x sleeve).
    assert len(exec_records) == len(config.COINS) * 2
    assert len(exec_records) >= 2, "need >=2 cold cells to exercise the fan-out"

    # CORE ASSERTION: every executor-thread fetch received a connection that is
    # NOT the tick's connection object (the SQLITE_MISUSE silent-miss fix).
    assert all(cid != id(tick_conn) for _tid, cid in exec_records)

    # Thread-local guarantee: a given worker thread always sees the same conn.
    by_thread: dict[int, set[int]] = {}
    for tid, cid in exec_records:
        by_thread.setdefault(tid, set()).add(cid)
    assert all(len(cids) == 1 for cids in by_thread.values())

    # No market resolution runs inline on the tick thread any more.
    assert inline_records == []

    # Results still flow through the fan-out unchanged (same count as the
    # inline/shared-conn qualifying-tick test).
    assert result.cells_evaluated == len(config.COINS) * 2


# ---- Entry-skip lever (config.ENTRY_SKIP_SECONDS) ---------------------
#
# 2026-06-02 "skip first 10s of every minute" lever. The bot scans every
# second (the partial-ret engine is fed so the window-open snapshot + z-score
# parity stay intact), but places NO bet during the first N seconds of EVERY
# minute — entry minute or not. The guard sits right after the engine update
# and returns the tick early, before any scoring / market / book work.
#
# The conftest autouse fixture pins ENTRY_SKIP_SECONDS=0 for the rest of the
# suite; these tests re-enable it via monkeypatch. All other tick tests run at
# now_ts % 60 == 0, exactly the second the guard suppresses, so the lever and
# the legacy second-0 assertions cannot coexist without this opt-in split.


def _blowup_deps(spots_fn) -> TickDeps:
    """Deps whose market/book fetchers raise — proves a skipped tick makes
    zero Polymarket API calls. fetch_spots still returns spots so the engine
    is fed."""

    class BlowUp:
        def fetch_spots(self, *, now_ts, tick_seconds):
            return spots_fn(now_ts=int(now_ts))

        def fetch_market(self, *a, **kw):
            raise AssertionError("fetch_market must not be called within skip window")

        def fetch_book(self, *a, **kw):
            raise AssertionError("fetch_book must not be called within skip window")

        def disk_free(self):
            return 10_000.0

    blowup = BlowUp()
    return TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=blowup.fetch_spots,
        fetch_market=blowup.fetch_market,
        fetch_book=blowup.fetch_book,
        get_disk_free_mb=blowup.disk_free,
    )


def test_peak_drawdown_halt_blocks_new_entries(db: sqlite3.Connection) -> None:
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests.test_slot_state import make_signal

    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=100.0, "
        "resolution_ts=datetime('now') WHERE id = 1"
    )
    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m", window_start_ts=1_715_923_500))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-110.0, "
        "resolution_ts=datetime('now') WHERE id = 2"
    )
    db.commit()

    now_ts = 1_715_923_200 + 12 * 60

    class BlowUp:
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(now_ts=int(now_ts))
        def fetch_market(self, *a, **kw):
            raise AssertionError("halted tick should not fetch markets")
        def fetch_book(self, *a, **kw):
            raise AssertionError("halted tick should not fetch books")
        def disk_free(self):
            return 10_000.0

    blowup = BlowUp()
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=blowup.fetch_spots,
        fetch_market=blowup.fetch_market,
        fetch_book=blowup.fetch_book,
        get_disk_free_mb=blowup.disk_free,
    )

    base = _make_portfolio()
    halted_sleeve = next(s for s in base.sleeves if s.label == "v6_5m")
    portfolio = replace(base, sleeves=(halted_sleeve,))
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

    assert result.pending_rows_written == 0
    assert result.skips_by_reason.get("dd_halt") == len(config.COINS)
    assert db.execute("SELECT COUNT(*) FROM ledger WHERE outcome='PENDING'").fetchone()[0] == 0
    row = db.execute(
        "SELECT status, message FROM pipeline_health WHERE stage='risk'"
    ).fetchone()
    assert row["status"] == "CRITICAL"
    assert "dd_halt active" in row["message"]


def test_wallet_drawdown_halt_does_not_stop_other_wallets(
    db: sqlite3.Connection,
) -> None:
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests.test_slot_state import make_signal

    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=100.0, "
        "resolution_ts=datetime('now') WHERE id = 1"
    )
    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m", window_start_ts=1_715_923_500))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-110.0, "
        "resolution_ts=datetime('now') WHERE id = 2"
    )
    db.commit()

    now_ts = 1_715_923_200 + 12 * 60
    window5 = (now_ts // 300) * 300
    window15 = (now_ts // 900) * 900
    stub = StubDeps(
        market_lookup=(
            _build_markets(window5, interval_min=5)
            | _build_markets(window15, interval_min=15)
        ),
        book_lookup={
            f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts)
            for c in ("BTC", "ETH", "SOL", "XRP")
        }
        | {
            f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts)
            for c in ("BTC", "ETH", "SOL", "XRP")
        },
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    base = _make_portfolio()
    halted = next(s for s in base.sleeves if s.label == "v6_5m")
    still_live = next(s for s in base.sleeves if s.label == "v6_15m")
    portfolio = replace(base, sleeves=(halted, still_live))
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

    assert result.skips_by_reason.get("dd_halt") == len(config.COINS)
    assert result.pending_rows_written >= 1
    assert db.execute(
        "SELECT COUNT(*) FROM ledger WHERE outcome='PENDING' AND wallet_id='v6_5m'"
    ).fetchone()[0] == 0
    assert db.execute(
        "SELECT COUNT(*) FROM ledger WHERE outcome='PENDING' AND wallet_id='v6_15m'"
    ).fetchone()[0] >= 1


def test_live_target_bet_size_scales_with_realized_capital(
    db: sqlite3.Connection,
) -> None:
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests.test_slot_state import make_signal

    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=32.20, "
        "resolution_ts=datetime('now') WHERE id = 1"
    )
    db.commit()

    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    result = run_scanner_tick(db, _make_portfolio(), now_ts=now_ts, deps=deps)
    assert result.pending_rows_written >= 1
    rows = db.execute(
        "SELECT target_usd, bet_size FROM ledger "
        "WHERE outcome='PENDING' AND wallet_id='v6_5m'"
    ).fetchall()
    assert rows
    for row in rows:
        assert row["target_usd"] == pytest.approx(3.302568148464853)
        assert row["bet_size"] == pytest.approx(3.302568148464853)


def test_live_target_bet_size_applies_per_coin_cap(
    db: sqlite3.Connection,
) -> None:
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests.test_slot_state import make_signal

    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=32.20, "
        "resolution_ts=datetime('now') WHERE id = 1"
    )
    db.commit()

    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    base = _make_portfolio()
    capped = []
    for sleeve in base.sleeves:
        if sleeve.label == "v6_5m":
            capped.append(
                replace(
                    sleeve,
                    max_bet_usd_by_coin={
                        "BTC": 2.00,
                        "ETH": 2.25,
                        "SOL": 2.50,
                        "XRP": 2.75,
                    },
                )
            )
        else:
            capped.append(sleeve)
    portfolio = replace(base, sleeves=tuple(capped))

    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    assert result.pending_rows_written >= 1
    rows = db.execute(
        "SELECT coin, target_usd, bet_size FROM ledger "
        "WHERE outcome='PENDING' AND wallet_id='v6_5m'"
    ).fetchall()
    by_coin = {row["coin"]: row for row in rows}
    expected_caps = {"BTC": 2.00, "ETH": 2.25, "SOL": 2.50, "XRP": 2.75}
    assert set(by_coin) == set(config.COINS)
    assert "SOL" not in by_coin
    for coin in config.COINS:
        assert by_coin[coin]["target_usd"] == pytest.approx(expected_caps[coin])
    for row in by_coin.values():
        assert row["bet_size"] == pytest.approx(row["target_usd"])


def test_no_window_open_spot_skips_bet(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Divergence #13 fix (2026-05-21): when PartialRetEngine returns
    None for window_open_spot — e.g., bot booted mid-window and the
    engine has not yet observed a boundary crossing — the cell must
    SKIP with reason `no_window_open_spot`, NOT record a bet that
    resolution_sweep cannot honestly grade.

    Defense-in-depth note: in the normal flow, a missing
    window_open_spot ALSO breaks the z-score computation in
    score_cell, so the cell hits `no_feature` first and the new skip
    path is never reached. The check still exists because (a) it
    survives any future refactor that decouples z-score from
    window_open_spot, (b) it's an explicit statement of intent at
    the bet-decision site. To exercise it directly here, we
    monkeypatch the engine's accessor to return None while leaving
    the underlying state intact — that keeps score_cell happy and
    forces the cell to reach the new skip.
    """
    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    # Force the new accessor to return None — exercises the defense-in-depth
    # skip without breaking score_cell's z-score computation.
    monkeypatch.setattr(
        deps.partial_ret_engine,
        "window_open_spot",
        lambda coin, interval_min: None,
    )

    portfolio = _make_portfolio()
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

    # Every active-coin × v6_5m cell should have hit the skip path.
    assert result.skips_by_reason.get("no_window_open_spot", 0) == len(config.COINS) * 2, (
        f"expected len(COINS)={len(config.COINS)} no_window_open_spot skips, "
        f"got {result.skips_by_reason}"
    )
    # And NO ledger rows were written — the whole point is "refuse to
    # bet on a window we can't honestly grade".
    n_ledger = db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
    assert n_ledger == 0, "skip path must not write ledger rows"
    # The skip signal rows ARE written for observability.
    n_skip = db.execute(
        "SELECT COUNT(*) FROM signals WHERE skip_reason = 'no_window_open_spot'"
    ).fetchone()[0]
    assert n_skip == len(config.COINS) * 2


def test_exposure_cap_blocks_new_bet(db: sqlite3.Connection) -> None:
    # AE-NEW-5: realized=100, pending=84 is already above the v6_5m
    # per-wallet cap, so no new entries should pass.
    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300

    from cryptobot.execution.ledger import BetSignal, record_bet
    for i in range(21):  # 21 x $4 = $84 PENDING
        sig = BetSignal(
            coin="ETH", sleeve="v6_5m", entry_minute=2, side="YES",
            window_start_ts=window5 - (i+1)*300, window_end_ts=window5 - i*300,
            interval_min=5, poly_market_id=f"PM{i}", poly_token_id=f"T{i}",
            fee_rate_bps=100, model_p=0.7, p_market=0.5, fill_price=0.5,
            walked_vwap=0.5, realized_edge=0.1, partial_ret_z_1h=0.0,
            lut_p_bucket=7, lut_hit_rate=0.7, lut_n_cum=200,
            target_usd=4.0, bet_size=4.0, slot_filled_pre=0.0,
            entry_top_price=0.5, yes_pre1_anchor=0.5, config_sha256="x",
        )
        record_bet(db, sig)
    db.commit()

    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    portfolio = _make_portfolio()
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    # Every active cell at this tick should fire SKIP:exposure_cap because the
    # isolated wallet cap is already consumed.
    assert result.skips_by_reason.get("exposure_cap", 0) >= 1
    assert any(
        d.final_decision == "SKIP:exposure_cap" for d in result.cell_decisions
    )


def test_per_cell_exception_isolated(db: sqlite3.Connection, monkeypatch) -> None:
    # AE-NEW-8: per-cell error should not break the tick; pipeline_health
    # gets the row.
    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    # Monkey-patch evaluate_gates to raise on BTC only.
    from cryptobot.execution import pipeline as pipeline_mod
    orig = pipeline_mod.evaluate_gates
    def boom(inp):
        if inp.coin == "BTC":
            raise RuntimeError("simulated per-cell crash")
        return orig(inp)
    monkeypatch.setattr(pipeline_mod, "evaluate_gates", boom)

    portfolio = _make_portfolio()
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    err_rows = db.execute(
        "SELECT coin, status FROM pipeline_health WHERE stage='tick' AND status='ERROR'"
    ).fetchall()
    assert any(r["coin"] == "BTC" for r in err_rows)
    # Other coins still processed (cells_evaluated counts every coin in
    # config.COINS at this entry minute, including the BTC cell that errored).
    assert result.cells_evaluated == len(config.COINS) * 2


def test_d10_dedup_blocks_second_initial_fill_in_window(db: sqlite3.Connection) -> None:
    """Review #4 — D10 first_qualifying dedup. v6_15m fires at multiple
    entry_minute_set members (e.g. e=2 and e=4) within
    the same 15m window; the backtest treats those as ONE qualifying bet.
    Live must skip the second initial fill with SKIP:market_dedup
    (top-ups inside the FIRST slot are unaffected because they're
    is_topup=True). Test uses e=2 -> e=4, both in the current
    entry_minute_set (2, 4, 6, 7, 9).
    """
    # Seed an existing initial fill for BTC v6_15m e=2 YES at window W.
    from cryptobot.execution.ledger import BetSignal, record_bet
    window15 = 1_715_923_200 - (1_715_923_200 % 900)
    sig_first = BetSignal(
        coin="BTC", sleeve="v6_15m", entry_minute=2, side="YES",
        window_start_ts=window15, window_end_ts=window15 + 900,
        interval_min=15, poly_market_id="M", poly_token_id="T",
        fee_rate_bps=100, model_p=0.7, p_market=0.55, fill_price=0.55,
        walked_vwap=0.55, realized_edge=0.1, partial_ret_z_1h=0.0,
        lut_p_bucket=7, lut_hit_rate=0.7, lut_n_cum=200,
        target_usd=4.0, bet_size=4.0, slot_filled_pre=0.0,
        entry_top_price=0.55, yes_pre1_anchor=0.55,
        strike_price=65_000.0, config_sha256="x",
    )
    record_bet(db, sig_first)
    db.execute(
        "INSERT INTO entry_scan_latches "
        "(coin, sleeve, entry_minute, window_start_ts) VALUES (?, ?, ?, ?)",
        ("BTC", "v6_15m", 2, window15),
    )
    db.commit()

    # Tick at e=4 within the SAME 15m window. With dedup, the second
    # initial fill on BTC YES should SKIP:market_dedup.
    now_ts = window15 + 4 * 60
    stub = StubDeps(
        market_lookup=_build_markets(window15, interval_min=15),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)
    portfolio = _make_portfolio()
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    # At least the BTC YES cell at e=4 must have skipped; there's already a
    # BTC YES e=2 initial fill in this window.
    assert result.skips_by_reason.get("market_dedup", 0) >= 1


def test_every_qualifying_does_not_dedup_within_window(db: sqlite3.Connection) -> None:
    """U2 of the all_4 deploy plan: v6_5m's chunk-CV pick is
    `entry_strategy=every_qualifying`. Each qualifying entry minute in the
    same (coin, sleeve, side, window) must fire its own bet — the
    first_qualifying market_dedup must NOT apply.
    """
    import dataclasses
    from cryptobot.execution.ledger import BetSignal, record_bet

    # Seed an initial fill for BTC v6_15m e=2 YES (mirrors the dedup test
    # above). We flip v6_15m's strategy to every_qualifying for this run so
    # the e=4 attempt should pass the dedup gate.
    base = _make_portfolio()
    new_sleeves = tuple(
        dataclasses.replace(s, entry_strategy="every_qualifying")
        if s.label == "v6_15m" else s
        for s in base.sleeves
    )
    portfolio = dataclasses.replace(base, sleeves=new_sleeves)

    window15 = 1_715_923_200 - (1_715_923_200 % 900)
    sig_first = BetSignal(
        coin="BTC", sleeve="v6_15m", entry_minute=2, side="YES",
        window_start_ts=window15, window_end_ts=window15 + 900,
        interval_min=15, poly_market_id="M", poly_token_id="T",
        fee_rate_bps=100, model_p=0.7, p_market=0.55, fill_price=0.55,
        walked_vwap=0.55, realized_edge=0.1, partial_ret_z_1h=0.0,
        lut_p_bucket=7, lut_hit_rate=0.7, lut_n_cum=200,
        target_usd=4.0, bet_size=4.0, slot_filled_pre=0.0,
        entry_top_price=0.55, yes_pre1_anchor=0.55,
        strike_price=65_000.0, config_sha256="x",
    )
    record_bet(db, sig_first)
    db.execute(
        "INSERT INTO entry_scan_latches "
        "(coin, sleeve, entry_minute, window_start_ts) VALUES (?, ?, ?, ?)",
        ("BTC", "v6_15m", 2, window15),
    )
    db.commit()

    # Tick at e=4 in the same 15m window. Under every_qualifying the
    # second initial fill must NOT be filtered with market_dedup.
    now_ts = window15 + 4 * 60
    stub = StubDeps(
        market_lookup=_build_markets(window15, interval_min=15),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

    # No market_dedup skip should fire — every_qualifying keeps all
    # qualifying entries within (slug_ts, coin, side).
    assert result.skips_by_reason.get("market_dedup", 0) == 0
    btc = next(
        d for d in result.cell_decisions
        if d.coin == "BTC" and d.sleeve.label == "v6_15m"
    )
    assert btc.final_decision == "WOULD_BET"
    assert db.execute(
        "SELECT COUNT(*) FROM ledger "
        "WHERE coin='BTC' AND sleeve='v6_15m' AND entry_minute=4 "
        "  AND window_start_ts=? AND is_topup=0",
        (window15,),
    ).fetchone()[0] == 1


def test_second_scan_latches_after_first_scan_skip(
    db: sqlite3.Connection,
) -> None:
    """The first live scan owns the entry minute, even when it skips.

    Backtest rows are one sample per (coin, sleeve, entry_minute, window).
    A prior latch row means live has already consumed that sample, so later
    1Hz ticks must not rescore or fetch the market just because conditions
    improved inside the same minute.
    """
    now_ts = 1_715_923_200 + 7 * 60
    retry_ts = now_ts + 5
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    base = _make_portfolio()
    portfolio = replace(
        base,
        sleeves=tuple(s for s in base.sleeves if s.label == "v6_5m"),
    )

    first = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    assert first.skips_by_reason.get("no_feature", 0) == len(config.COINS)
    assert db.execute(
        "SELECT COUNT(*) FROM entry_scan_latches "
        "WHERE sleeve='v6_5m' AND entry_minute=2 AND window_start_ts=?",
        (window5,),
    ).fetchone()[0] == len(config.COINS)

    stub.spot_data = _spots(now_ts=retry_ts)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=retry_ts, spot=spot.price)

    def fail_market_fetch(*_args, **_kwargs):
        raise AssertionError("latched entry minute should not fetch market")

    deps.fetch_market = fail_market_fetch

    result = run_scanner_tick(db, portfolio, now_ts=retry_ts, deps=deps)

    assert result.skips_by_reason.get("entry_scan_latched", 0) == len(config.COINS)
    assert db.execute(
        "SELECT COUNT(*) FROM ledger WHERE sleeve='v6_5m' AND window_start_ts=?",
        (window5,),
    ).fetchone()[0] == 0
    assert all(d.final_decision == "SKIP:entry_scan_latched" for d in result.cell_decisions)


def test_real_initial_bet_latches_opposite_side_retry_same_entry(
    db: sqlite3.Connection,
) -> None:
    """A real initial fill locks the coin/sleeve/entry slot across sides."""
    from cryptobot.execution.ledger import BetSignal, record_bet

    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    record_bet(
        db,
        BetSignal(
            coin="BTC",
            sleeve="v6_5m",
            entry_minute=2,
            side="NO",
            window_start_ts=window5,
            window_end_ts=window5 + 300,
            interval_min=5,
            poly_market_id="M",
            poly_token_id="NO-BTC",
            fee_rate_bps=100.0,
            model_p=0.75,
            p_market=0.5,
            fill_price=0.5,
            walked_vwap=0.5,
            realized_edge=0.2,
            partial_ret_z_1h=0.0,
            lut_p_bucket=7,
            lut_hit_rate=0.75,
            lut_n_cum=1000,
            target_usd=5.0,
            bet_size=5.0,
            slot_filled_pre=0.0,
            entry_top_price=0.5,
            yes_pre1_anchor=0.5,
            config_sha256="x",
        ),
    )
    db.commit()

    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup={f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")} |
                    {f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts) for c in ("BTC","ETH","SOL","XRP")},
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    result = run_scanner_tick(db, _make_portfolio(), now_ts=now_ts, deps=deps)

    btc = next(
        d for d in result.cell_decisions
        if d.coin == "BTC" and d.sleeve.label == "v6_5m"
    )
    assert btc.final_decision == "SKIP:entry_scan_latched"
    assert db.execute(
        "SELECT COUNT(*) FROM ledger "
        "WHERE coin='BTC' AND sleeve='v6_5m' AND entry_minute=2 "
        "  AND window_start_ts=?",
        (window5,),
    ).fetchone()[0] == 1


def test_walker_returns_none_on_non_empty_book_writes_pipeline_health_warn(
    db: sqlite3.Connection,
) -> None:
    """Review-pass #22 closeout. The walker can return None on a book
    that has rows but no level clearing min_edge / min_bet_usd / max_walk.
    Without observability this is a silent degraded-CLOB event. A
    pipeline_health WARN row makes it queryable post-hoc.
    """
    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    # Build a book with depth too thin to clear MIN_BET_USD=$1.
    thin_books: dict[str, BookSnapshot] = {}
    for c in ("BTC", "ETH", "SOL", "XRP"):
        thin_books[f"YES-{c}"] = BookSnapshot(
            token_id=f"YES-{c}",
            asks=[{"price": 0.55, "size": 0.5}],   # $0.275 of depth — below MIN_BET_USD
            bids=[],
            fetched_at_ts=float(now_ts),
        )
        thin_books[f"NO-{c}"] = BookSnapshot(
            token_id=f"NO-{c}",
            asks=[{"price": 0.45, "size": 0.5}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup=thin_books,
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)
    portfolio = _make_portfolio()
    run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    warn_rows = db.execute(
        "SELECT coin, message FROM pipeline_health WHERE stage='walk' AND status='WARN'"
    ).fetchall()
    # At least one coin should have surfaced a "walker returned None on
    # a non-empty N-level book" WARN — confirms the silent skip became
    # observable.
    assert warn_rows, "expected pipeline_health 'walk' WARN rows on degraded book"
    assert any("non-empty" in r["message"] for r in warn_rows)


def test_walker_none_on_unbettable_top_ask_writes_no_warn(
    db: sqlite3.Connection,
) -> None:
    """2026-07-03 tick-latency fix: a walk that returns None because even
    the book's TOP level cannot clear the sleeve's edge floor is the
    expected outcome of an unbettable price, not book degradation.
    Sub-minute checkpoint cells hit this on nearly every near-settle tick
    (asks 0.9+ can never clear the floor) and were flooding
    pipeline_health with ~1 WARN/tick plus its DB write. The WARN must
    stay scoped to walks that fail DESPITE a floor-clearing top (depth /
    price-cap / malformed-level failures — the previous test above).
    """
    now_ts = 1_715_923_200 + 7 * 60
    window5 = (now_ts // 300) * 300
    # Deep books (no min_bet shortfall) priced so the max achievable edge
    # (1 − 0.97 − fee ≈ 0.03) sits far below the fixture sleeves' min_edge
    # (0.16-0.25) — every walk fails on the edge floor at the top level.
    expensive_books: dict[str, BookSnapshot] = {}
    for c in ("BTC", "ETH", "SOL", "XRP"):
        for side in ("YES", "NO"):
            expensive_books[f"{side}-{c}"] = BookSnapshot(
                token_id=f"{side}-{c}",
                asks=[{"price": 0.97, "size": 500.0}],
                bids=[],
                fetched_at_ts=float(now_ts),
            )
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup=expensive_books,
        spot_data=_spots(now_ts=now_ts),
    )
    deps = _stub_deps(stub)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)
    run_scanner_tick(db, _make_portfolio(), now_ts=now_ts, deps=deps)
    warn_rows = db.execute(
        "SELECT message FROM pipeline_health WHERE stage='walk' AND status='WARN'"
    ).fetchall()
    assert not warn_rows, [r["message"] for r in warn_rows]


def test_spot_down_aborts_tick(db: sqlite3.Connection) -> None:
    now_ts = 1_715_923_200 + 4 * 60
    stub = StubDeps(market_lookup={}, book_lookup={}, spot_data=None)
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=lambda *, now_ts, tick_seconds: None,
        fetch_market=stub.fetch_market,
        fetch_book=stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
    )
    portfolio = _make_portfolio()
    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    assert result.pending_rows_written == 0
    row = db.execute(
        "SELECT message FROM pipeline_health WHERE stage='fetch'"
    ).fetchone()
    assert row is not None
    assert "spot fetch returned None" in row["message"]


def test_spot_down_health_warns_once_per_fetch_bucket(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "SPOT_FETCH_INTERVAL_SECONDS", 10)
    now_ts = 1_715_923_200
    now_ts = (now_ts // config.SPOT_FETCH_INTERVAL_SECONDS) * config.SPOT_FETCH_INTERVAL_SECONDS
    stub = StubDeps(market_lookup={}, book_lookup={}, spot_data=None)
    deps = TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub",
        fetch_spots=lambda *, now_ts, tick_seconds: None,
        fetch_market=stub.fetch_market,
        fetch_book=stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
    )
    portfolio = _make_portfolio()

    run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    run_scanner_tick(db, portfolio, now_ts=now_ts + 1, deps=deps)
    run_scanner_tick(
        db,
        portfolio,
        now_ts=now_ts + config.SPOT_FETCH_INTERVAL_SECONDS,
        deps=deps,
    )

    n = db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE stage='fetch'"
    ).fetchone()[0]
    assert n == 2


# ---- 5m interval master switch (CRYPTOBOT_INTERVAL_5M_ENABLED) ---------
#
# Operator order 2026-07-25: disable 5m trading. The gate is ENTRY-only and sits
# in the tick loop's sleeve iteration, so a disabled 5m sleeve must cost ZERO
# API calls and write ZERO rows of any kind (at 1Hz a skip row per coin for a
# permanently-off interval would flood the signals table), while the 15m sleeve
# on the SAME artifact is completely untouched.


def _live_subminute_tick(
    db: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
    *,
    label: str,
    interval_5m_enabled: bool = True,
    blow_up_on_fetch: bool = False,
    score_fn=None,
    wire_book_source: bool = False,
    runner=None,
):
    """Run ONE real scanner tick over a single sleeve of the LIVE artifact at a
    qualifying checkpoint second (25s before the window closes, inside both
    sleeves' ``checkpoint_offset_set``).

    ``blow_up_on_fetch`` turns every market/book fetch into an assertion failure,
    which is how a "silent skip" is pinned. ``score_fn`` overrides the scorer
    (default: an in-band YES).

    ``wire_book_source`` attaches a push/cache book source serving the same
    ladders (the fast-trigger rungs are cache-only, so they return immediately
    without one). ``runner`` swaps ``run_scanner_tick`` for another
    ``(conn, portfolio, now_ts, deps) -> Any`` entry point — used by the
    fast-trigger integration tests so the fast path is exercised against the
    REAL ``_resolve_net_cells`` + ``_commit_cell``, not stubs.
    """
    import cryptobot.execution.pipeline as pipeline
    import cryptobot.strategy.portfolio_loader as portfolio_loader
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", False)
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", interval_5m_enabled)
    monkeypatch.setattr(portfolio_loader.config, "SUBMINUTE_ENABLED", True)

    full = load_sleeves(str(config.V6_CONFIG_PATH))
    sleeve = next(s for s in full.sleeves if s.label == label)
    portfolio = replace(full, sleeves=(sleeve,))

    window_s = sleeve.interval_min * 60
    window = (1_715_923_200 // window_s) * window_s
    now_ts = window + window_s - 25

    books: dict[str, BookSnapshot] = {}
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        # v2 fill band is 0.65 < ask <= 0.85. The shadow-WFO outcome is graded
        # through the same gate stack, so keep the NO side in-band too.
        books[f"YES-{coin}"] = _book(f"YES-{coin}", top=0.75, depth=500.0, now_ts=now_ts)
        books[f"NO-{coin}"] = _book(f"NO-{coin}", top=0.70, depth=500.0, now_ts=now_ts)
    stub = StubDeps(
        market_lookup=_build_markets(window, interval_min=sleeve.interval_min),
        book_lookup=books,
        spot_data=_spots(now_ts=now_ts),
    )

    def _blow_up_market(*_a, **_kw):
        raise AssertionError("fetch_market called for a disabled interval")

    def _blow_up_book(*_a, **_kw):
        raise AssertionError("fetch_book called for a disabled interval")

    def _default_score(**_kwargs) -> ScoreResult:
        return ScoreResult(
            model_p=0.82,
            side="YES",
            edge=0.20,
            realized_edge=0.20,
            lut_p_bucket=-1,
            lut_hit_rate=None,
            lut_n_cum=0,
            partial_ret_z_1h=0.1,
            skip_reason=None,
            orb_range_ret=0.01,
            orb_entry_ret=0.01,
        )

    monkeypatch.setattr(pipeline, "score_cell_hgb", score_fn or _default_score)

    eng = PartialRetEngine()
    for coin, spot in stub.spot_data.items():
        seed_for_tests(eng, coin, now_ts=window, spot=spot.price)

    deps = TickDeps(
        partial_ret_engine=eng,
        config_sha256="stub-sha256",
        fetch_spots=stub.fetch_spots,
        # The fast rung's spot access is PEEK-ONLY (never `fetch_spots`); the
        # stub's fixture dict stands in for an already-warm bucket. The tick
        # path never reads this field.
        peek_spots=stub.fetch_spots,
        fetch_market=_blow_up_market if blow_up_on_fetch else stub.fetch_market,
        fetch_book=_blow_up_book if blow_up_on_fetch else stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
        checkpoint_rf_artifact=SimpleNamespace(model_sha1="live"),
        checkpoint_hgb_artifact=SimpleNamespace(),
        book_prefetch=(
            SimpleNamespace(
                note_tokens=lambda _t: None,
                noted_tokens=lambda: tuple(books),
                get=lambda t, *, now_ts: books.get(t),
                get_tagged=lambda t, *, now_ts: (books.get(t), "ws"),
            )
            if wire_book_source
            else None
        ),
    )
    if runner is not None:
        return runner(db, portfolio, now_ts, deps)
    return run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)


def test_5m_disabled_skips_sleeve_silently(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Flag off at a qualifying 5m checkpoint second: nothing is evaluated, no
    # market/book fetch happens (the deps raise if touched), and no signal or
    # ledger row is written.
    result = _live_subminute_tick(
        db, monkeypatch,
        label="subminute_5m", interval_5m_enabled=False, blow_up_on_fetch=True,
    )
    assert result.cells_evaluated == 0
    assert result.signals_written == 0
    assert result.pending_rows_written == 0
    assert result.skips_by_reason == {}
    assert db.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0] == 0


def test_5m_enabled_is_the_unchanged_baseline(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Same tick with the flag ON: the 5m sleeve evaluates every coin. This is
    # what the disabled case above is being compared against — without it a
    # broken fixture would make the skip test vacuously pass.
    result = _live_subminute_tick(db, monkeypatch, label="subminute_5m")
    assert result.cells_evaluated == len(config.COINS)


def test_5m_disable_leaves_15m_untouched(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The gate keys on interval_min, so the surviving 15m sleeve must evaluate
    # normally at its own checkpoint second with the 5m switch off.
    result = _live_subminute_tick(
        db, monkeypatch, label="subminute_15m", interval_5m_enabled=False,
    )
    assert result.cells_evaluated == len(config.COINS)


# ---- Fast-trigger entry path (config.FAST_TRIGGER_*) -------------------
#
# The unit behaviour (guards, cadence clamp, dedup, single-flight, trigger_src)
# lives in test_fast_entry.py. These two run the fast rung through the SAME real
# harness the tick uses, so the shared path is pinned against the REAL
# _resolve_net_cells + _commit_cell signatures rather than stubs — a mismatch
# there would otherwise only surface in production.


def _reset_v2_window_state() -> None:
    """The decider state is a process-global that survives across ticks (and
    across tests). The live artifact's checkpoint sleeves ride the frozen MID
    band, i.e. the shared module-global decider — so a test that deliberately
    consumes an offset MUST reset it or it leaks into the next test.

    The per-label REGISTRY is cleared too, not just the MID instance: another
    module (test_hgb_scoring) builds a `subminute_5m` sleeve with a widened
    ask band, which mints a NON-mid decider cached under that very label. It
    then outlives that test and answers `_v2_decider_for("subminute_5m")` here,
    holding spent offsets for the window these tests use."""
    import cryptobot.execution.pipeline as pipeline

    pipeline._V2_WINDOW_DECIDER.reset()
    for dec in pipeline._V2_WINDOW_DECIDERS.values():
        dec.reset()
    pipeline._V2_WINDOW_DECIDERS.clear()
    pipeline._V2_MARKET_FILLED.clear()


def _fast_scan_runner(monkeypatch: pytest.MonkeyPatch, src: str = "fast_sweep"):
    import cryptobot.execution.pipeline as pipeline

    # The fast rungs are cache-only for market metadata; this harness stubs
    # fetch_market, so the real per-window Gamma cache is cold.
    monkeypatch.setattr(pipeline, "_venue_market_cache_has", lambda *a, **k: True)

    def _run(conn, portfolio, now_ts, deps):
        return pipeline.run_fast_entry_scan(
            conn, portfolio, now_ts=now_ts, deps=deps, trigger_src=src
        )

    return _run


def _skip_reasons(db: sqlite3.Connection) -> dict:
    return {
        row[0]: row[1]
        for row in db.execute(
            "SELECT skip_reason, COUNT(*) FROM signals "
            "WHERE skip_reason IS NOT NULL GROUP BY 1"
        )
    }


def test_fast_entry_scan_decides_identically_to_the_tick(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fast rung is not a second copy of the decision logic — it runs the
    REAL _resolve_net_cells + _commit_cell. Same second, same books, same
    verdict as the tick's own Phase B (this harness's ladders are too thin to
    fill, so the shared verdict here is insufficient_depth)."""
    _reset_v2_window_state()
    try:
        tick_result = _live_subminute_tick(db, monkeypatch, label="subminute_5m")
        tick_reasons = _skip_reasons(db)
        assert tick_result.cells_evaluated == len(config.COINS)
    finally:
        _reset_v2_window_state()
    db.execute("DELETE FROM signals")
    db.execute("DELETE FROM subminute_scan_latches")
    db.commit()
    try:
        counters = _live_subminute_tick(
            db, monkeypatch, label="subminute_5m",
            wire_book_source=True, runner=_fast_scan_runner(monkeypatch),
        )
    finally:
        _reset_v2_window_state()
    # Every coin was considered, scored, left bettable by the rescore, and
    # handed to the real commit machinery.
    assert counters["cells"] == len(config.COINS)
    assert counters["scored"] == len(config.COINS)
    assert counters["bettable"] == len(config.COINS)
    assert counters["committed"] == len(config.COINS)
    assert counters["contended"] == 0
    assert _skip_reasons(db) == tick_reasons


def test_fast_entry_scan_is_a_noop_without_a_book_source(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No push/cache source => the rung returns before any work, and in
    # particular never falls back to a REST book fetch.
    counters = _live_subminute_tick(
        db, monkeypatch, label="subminute_5m",
        wire_book_source=False, runner=_fast_scan_runner(monkeypatch),
    )
    assert counters["cells"] == 0
    assert counters["committed"] == 0
    assert db.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0] == 0


def test_fast_entry_scan_consumes_the_offset_for_the_tick(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The coexistence invariant, end to end: once a fast scan has band-passed
    integer offset S, the 1Hz tick's visit to that SAME second is vetoed by the
    decider (v2_window_consumed) instead of re-deciding it."""
    _reset_v2_window_state()
    try:
        _live_subminute_tick(
            db, monkeypatch, label="subminute_5m",
            wire_book_source=True, runner=_fast_scan_runner(monkeypatch),
        )
        db.execute("DELETE FROM signals")
        db.commit()
        result = _live_subminute_tick(db, monkeypatch, label="subminute_5m")
    finally:
        _reset_v2_window_state()
    assert _skip_reasons(db) == {"v2_window_consumed": len(config.COINS)}
    assert dict(result.skips_by_reason) == {
        "v2_window_consumed": len(config.COINS)
    }
    assert result.pending_rows_written == 0


def test_phase_b_single_flight_lock_covers_the_parallel_post_flush(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P0: under CRYPTOBOT_PHASE_B_PARALLEL_POST — the LIVE config — a cell does
    NOT post inside _commit_cell; it reserves and collects, and the POSTs are
    fired by _flush_parallel_posts. So the single-flight lock has to still be
    held THERE, or a fast rung could interleave its own order POST with the
    tick's fan-out (the per-cell lock covered only the prep)."""
    import cryptobot.execution.pipeline as pipeline

    monkeypatch.setattr(config, "FAST_TRIGGER_ENABLED", True)
    monkeypatch.setattr(config, "PHASE_B_PARALLEL_POST", True)
    cell_held: list[bool] = []
    flush_held: list[bool] = []

    def _commit(conn, d, *a, **k):
        cell_held.append(pipeline._FAST_ENTRY_LOCK.locked())
        posts = k.get("parallel_posts")
        if posts is not None and not posts:
            posts.append("reserved-post-sentinel")

    monkeypatch.setattr(pipeline, "_commit_cell", _commit)
    monkeypatch.setattr(
        pipeline,
        "_flush_parallel_posts",
        lambda conn, prepared: flush_held.append(
            pipeline._FAST_ENTRY_LOCK.locked()
        ),
    )
    _reset_v2_window_state()
    try:
        _live_subminute_tick(db, monkeypatch, label="subminute_5m")
    finally:
        _reset_v2_window_state()
    # >1 placing cell is required for the flag to collect at all (n=1 posts
    # inline), which this 3-coin harness satisfies.
    assert len(cell_held) == len(config.COINS)
    assert all(cell_held)
    assert flush_held == [True]
    assert pipeline._FAST_ENTRY_LOCK.locked() is False


def test_phase_b_takes_no_lock_when_the_fast_trigger_is_off(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Flag OFF must be byte-identical: no lock is taken, so a held lock cannot
    stall the tick."""
    import cryptobot.execution.pipeline as pipeline

    monkeypatch.setattr(config, "FAST_TRIGGER_ENABLED", False)
    held: list[bool] = []
    monkeypatch.setattr(
        pipeline,
        "_commit_cell",
        lambda *a, **k: held.append(pipeline._FAST_ENTRY_LOCK.locked()),
    )
    pipeline._FAST_ENTRY_LOCK.acquire()
    try:
        _reset_v2_window_state()
        _live_subminute_tick(db, monkeypatch, label="subminute_5m")
    finally:
        pipeline._FAST_ENTRY_LOCK.release()
        _reset_v2_window_state()
    assert held and all(held)  # locked() is True only because WE hold it


# ---- A8: next-window book-prefetch token push --------------------------
#
# _prefetch_next_window_markets runs at the END of the tick (off the decision
# path). During the last _MARKET_PREFETCH_TAIL_S of a window it must hand the
# NEXT window's yes/no token ids to the book prefetch chain so the WS mirror
# subscribes / the REST prefetcher warms them BEFORE the boundary — otherwise
# the first qualifying tick of the fresh window pays an inline REST book fetch.
# note_tokens REPLACES the tracked set, so the push must carry the CURRENT
# window forward (the ~2x tail asset set) or the current window stops serving.


def _tail_portfolio(interval_min: int = 5) -> SimpleNamespace:
    return SimpleNamespace(sleeves=[SimpleNamespace(interval_min=interval_min)])


def test_prefetch_notes_next_window_tokens_and_keeps_current_served(monkeypatch):
    clear_market_cache()
    monkeypatch.setattr(config, "COINS", ("BTC",))
    window_start = 1_800_000_000  # divisible by the 300s (5m) span
    now = window_start + 295  # 5s from window end -> inside the 20s tail
    next_start = window_start + 300

    def fetch_market(coin, *, interval_min, window_start_ts, conn, now_ts):
        assert window_start_ts == next_start  # only the NEXT window is warmed
        return PolyMarket(
            coin=coin,
            interval_min=interval_min,
            window_start_ts=window_start_ts,
            window_end_ts=window_start_ts + interval_min * 60,
            condition_id=f"COND-{coin}",
            yes_token_id=f"YES-NEXT-{coin}",
            no_token_id=f"NO-NEXT-{coin}",
            fee_rate_bps=100.0,
        )

    pf = BookPrefetcher(lambda *a, **k: None, lead_ms=150, max_age_ms=700)
    chain = BookSourceChain([pf])
    # A prior qualifying tick of the CURRENT window noted its tokens and the
    # prefetch cache holds fresh current-window books (i.e. currently served).
    chain.note_tokens(["YES-CUR", "NO-CUR"])
    pf._cache = {t: _book(t, top=0.6, now_ts=float(now)) for t in ("YES-CUR", "NO-CUR")}
    assert chain.get("YES-CUR", now_ts=float(now)) is not None

    _prefetch_next_window_markets(
        None, _tail_portfolio(), fetch_market, int(now), book_prefetch=chain
    )

    noted = set(chain.noted_tokens())
    # NEXT window's ids were pushed (WS subscribes / REST prefetcher warms).
    assert {"YES-NEXT-BTC", "NO-NEXT-BTC"} <= noted
    # CURRENT window retained -> the union keeps it served from the cache.
    assert {"YES-CUR", "NO-CUR"} <= noted
    assert chain.get("YES-CUR", now_ts=float(now)) is not None
    # The union fanned out to the underlying prefetcher too.
    assert set(pf.noted_tokens()) == noted
    clear_market_cache()


def test_prefetch_gathers_cached_next_window_tokens_past_budget(monkeypatch):
    # A LATER tail tick finds the next window already Gamma-cached (no GET
    # budget consumed). Tokens must STILL be gathered every tail tick, for all
    # coins, even past _MARKET_PREFETCH_MAX_PER_TICK — else _resolve_net_cells'
    # per-tick REPLACE would drop them after the first tail tick.
    clear_market_cache()
    coins = ("BTC", "ETH", "XRP")  # 3 coins > _MARKET_PREFETCH_MAX_PER_TICK (2)
    monkeypatch.setattr(config, "COINS", coins)
    window_start = 1_800_000_000
    now = window_start + 295
    next_start = window_start + 300

    markets: dict = {}
    for coin in coins:
        m = PolyMarket(
            coin=coin,
            interval_min=5,
            window_start_ts=next_start,
            window_end_ts=next_start + 300,
            condition_id=f"COND-{coin}",
            yes_token_id=f"YES-{coin}",
            no_token_id=f"NO-{coin}",
            fee_rate_bps=100.0,
        )
        markets[(coin, 5, next_start)] = m
        _market_cache_put(m, float(now))  # already cached -> market_cache_has True

    def fetch_market(coin, *, interval_min, window_start_ts, conn, now_ts):
        return markets.get((coin, interval_min, window_start_ts))

    pf = BookPrefetcher(lambda *a, **k: None, lead_ms=150, max_age_ms=700)
    chain = BookSourceChain([pf])

    _prefetch_next_window_markets(
        None, _tail_portfolio(), fetch_market, int(now), book_prefetch=chain
    )

    noted = set(chain.noted_tokens())
    for coin in coins:  # all THREE coins present despite the 2-GET budget
        assert {f"YES-{coin}", f"NO-{coin}"} <= noted
    clear_market_cache()


def test_prefetch_does_not_touch_book_prefetch_outside_tail(monkeypatch):
    clear_market_cache()
    monkeypatch.setattr(config, "COINS", ("BTC",))
    window_start = 1_800_000_000
    now = window_start + 100  # 200s from end of the 5m window -> NOT the tail

    def fetch_market(*a, **k):
        raise AssertionError("fetch_market must not be called outside the tail")

    pf = BookPrefetcher(lambda *a, **k: None, lead_ms=150, max_age_ms=700)
    chain = BookSourceChain([pf])
    chain.note_tokens(["YES-CUR", "NO-CUR"])

    _prefetch_next_window_markets(
        None, _tail_portfolio(), fetch_market, int(now), book_prefetch=chain
    )

    # Noted set untouched -> only the current window is tracked.
    assert set(chain.noted_tokens()) == {"YES-CUR", "NO-CUR"}
    clear_market_cache()
