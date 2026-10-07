"""U6 pipeline LIVE/DRY routing tests.

Covers the per-strategy mode router in `_commit_cell` and
`_commit_pair_candidate`:

  * LIVE wallets reserve a `live_order_attempts` row before calling
    OrderClient.place_gtc_limit_order, then link the row to the
    resulting ledger row (D-LIVE-28).
  * DRY wallets continue on `walk_book_edge_preserving` and never invoke
    the OrderClient (D-LIVE-1).
  * DRY wallets keep firing during operator_control_state.STOPPED
    (D-LIVE-12) â€” only LIVE entries are blocked.
  * Mode is stamped immutably on every ledger row (D-LIVE-3).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

import pytest

from cryptobot.venues.types import BookSnapshot
from cryptobot_polymarket.gamma import PolyMarket
from cryptobot.data.binance import BinanceSpot
from cryptobot.db import backfill_wallet_groups
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    boot_operator_state,
    set_wallet_mode,
    start_processing,
)
from cryptobot.execution.pipeline import TickDeps, run_scanner_tick
from cryptobot.execution.types import OrderResult
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.strategy.portfolio_loader import V6Portfolio
from cryptobot.tests._legacy_artifact import legacy_portfolio
from cryptobot.tests._fixtures import seed_for_tests
from cryptobot.tests._stub_scorer import install_fake_scorer


# ----- Stub scorer (mirrors test_pipeline_tick.py) ------------------------
#
# 2026-07-31: the stub RF forest + synthetic LUT this module used to build fed
# the lut|rf|blend scorer, retired with the pair sleeves. One scorer stub
# (`tests/_stub_scorer`) replaces both and reproduces their exact numbers.


@pytest.fixture(autouse=True)
def _stub_scorer(monkeypatch):
    install_fake_scorer(monkeypatch)


def _spots(now_ts: int = 1_715_923_440) -> dict[str, BinanceSpot]:
    return {
        coin: BinanceSpot(coin=coin, price=p, publish_time=now_ts - 1)
        for coin, p in (
            ("BTC", 67_000.0), ("ETH", 3_100.0), ("SOL", 175.0),
            ("XRP", 0.52),
        )
    }


def _book(
    token_id: str, top: float, depth: float = 100.0, now_ts: float = 1_715_923_440.0
) -> BookSnapshot:
    return BookSnapshot(
        token_id=token_id,
        asks=[{"price": top, "size": depth}],
        bids=[],
        fetched_at_ts=now_ts,
    )


def _build_markets(window_start: int, interval_min: int = 5) -> dict:
    return {
        (coin, interval_min, window_start): PolyMarket(
            coin=coin,
            interval_min=interval_min,
            window_start_ts=window_start,
            window_end_ts=window_start + interval_min * 60,
            condition_id=f"COND-{coin}",
            yes_token_id=f"YES-{coin}",
            no_token_id=f"NO-{coin}",
            fee_rate_bps=100.0,
        )
        for coin in ("BTC", "ETH", "SOL", "XRP")
    }


@dataclass
class StubDeps:
    market_lookup: dict
    book_lookup: dict[str, BookSnapshot]
    spot_data: dict[str, BinanceSpot]

    def fetch_spots(self, *, now_ts, tick_seconds):
        return self.spot_data

    def fetch_market(self, coin, *, interval_min, window_start_ts, conn, now_ts):
        return self.market_lookup.get((coin, interval_min, window_start_ts))

    def fetch_book(self, token_id, *, now_ts):
        return self.book_lookup.get(token_id)

    def disk_free(self):
        return 10_000.0


# Stub OrderClient that records every call and returns deterministic fills.
class StubOrderClient:
    def __init__(
        self,
        *,
        balance: Optional[float] = 200.0,
        always_fail_kind: Optional[str] = None,
        fail_on_short: bool = False,
        cancel_succeeds: bool = True,
        include_fill_metadata: bool = True,
    ) -> None:
        self.balance = balance
        self.calls: list[dict] = []
        self.cancel_calls: list[str] = []
        self.cancel_succeeds = cancel_succeeds
        self._fail_on_short = fail_on_short
        self._always_fail_kind = always_fail_kind
        self._include_fill_metadata = include_fill_metadata

    def check_balance(self) -> Optional[float]:
        return self.balance

    def place_gtc_limit_order(self, signal, *, client_order_id):
        self.calls.append({
            "client_order_id": client_order_id,
                "side": signal.side,
                "size_usd": signal.bet_size,
                "token": signal.poly_token_id,
                "sleeve": signal.sleeve,
                "fill_price": signal.fill_price,
                "walked_vwap": signal.walked_vwap,
                "limit_price": signal.limit_price,
            })
        if self._always_fail_kind is not None:
            return OrderResult(
                success=False,
                error=f"stub forced failure: {self._always_fail_kind}",
                error_kind=self._always_fail_kind,
                client_order_id=client_order_id,
            )
        if self._fail_on_short and signal.side == "NO":
            return OrderResult(
                success=False,
                error="market_closed",
                error_kind="market_closed",
                client_order_id=client_order_id,
            )
        price = float(signal.fill_price)
        limit_price = float(signal.limit_price or signal.fill_price)
        if not self._include_fill_metadata:
            return OrderResult(
                success=True,
                order_id=f"clob-{len(self.calls)}",
                client_order_id=client_order_id,
                limit_price=limit_price,
            )
        size = float(signal.bet_size) / price
        return OrderResult(
            success=True,
            order_id=f"clob-{len(self.calls)}",
            client_order_id=client_order_id,
            limit_price=limit_price,
            filled_size_tokens=size,
            filled_price_vwap=price,
            filled_value_usd=price * size,
            fill_ts=utc_now_sql(),
        )

    def cancel_order(self, order_id: str) -> bool:
        self.cancel_calls.append(order_id)
        return self.cancel_succeeds


def _stub_deps(
    stub: StubDeps,
    *,
    order_client: Optional[StubOrderClient] = None,
) -> TickDeps:
    return TickDeps(
        partial_ret_engine=PartialRetEngine(),
        config_sha256="stub-sha256",
        fetch_spots=stub.fetch_spots,
        fetch_market=stub.fetch_market,
        fetch_book=stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
        order_client=order_client,
        # Always-fresh readiness so DRY â†’ LIVE flips + LIVE gate succeed.
        live_readiness_loader=lambda _conn: True,
        live_balance_fetcher=(
            (lambda: order_client.check_balance()) if order_client else None
        ),
    )


def _seed_live_readiness(conn: sqlite3.Connection) -> None:
    """Phase 2 simulation: insert fresh READY rows so DRY â†’ LIVE flips pass."""
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


def _bootstrap(conn: sqlite3.Connection) -> V6Portfolio:
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    return portfolio


def _qualifying_5m_now() -> int:
    """A tick second that lands in pairs_5m's entry minute set."""
    return 1_715_923_200 + 11 * 60


def _qualifying_v6_5m_now() -> int:
    """A tick second at v6_5m's entry minute (mod5=2)."""
    return 1_715_923_200 + 7 * 60


def _stub_v6_books(now_ts: int) -> dict[str, BookSnapshot]:
    return {
        f"YES-{c}": _book(f"YES-{c}", top=0.53, now_ts=now_ts)
        for c in ("BTC", "ETH", "SOL", "XRP")
    } | {
        f"NO-{c}": _book(f"NO-{c}", top=0.45, now_ts=now_ts)
        for c in ("BTC", "ETH", "SOL", "XRP")
    }


def _stub_v6_walk_books(now_ts: int) -> dict[str, BookSnapshot]:
    books: dict[str, BookSnapshot] = {}
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        books[f"YES-{coin}"] = BookSnapshot(
            token_id=f"YES-{coin}",
            asks=[{"price": 0.50, "size": 1.0}, {"price": 0.53, "size": 100.0}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
        books[f"NO-{coin}"] = BookSnapshot(
            token_id=f"NO-{coin}",
            asks=[{"price": 0.45, "size": 1.0}, {"price": 0.48, "size": 100.0}],
            bids=[],
            fetched_at_ts=float(now_ts),
        )
    return books


# ----- Tests -----------------------------------------------------------


class TestDryWalletStaysOnWalkBook:
    def test_dry_wallet_does_not_call_order_client(
        self, db: sqlite3.Connection
    ) -> None:
        """Wallet stays DRY â†’ no place_gtc_limit_order call."""
        portfolio = _bootstrap(db)
        start_processing(db)
        # All wallets still DRY; no mode flip.

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
        assert result.pending_rows_written > 0

        rows = db.execute("SELECT mode FROM ledger").fetchall()
        assert all(r["mode"] == "DRY" for r in rows), (
            "DRY wallets must stamp every row as mode=DRY"
        )
        # No CLOB calls and no live_order_attempts.
        assert order_client.calls == []
        attempts = db.execute(
            "SELECT COUNT(*) AS n FROM live_order_attempts"
        ).fetchone()
        assert attempts["n"] == 0


class TestLiveWalletRoutesThroughOrderClient:
    def test_live_wallet_reserves_attempt_and_calls_clob(
        self, db: sqlite3.Connection
    ) -> None:
        """Wallet flipped to LIVE â†’ place_gtc_limit_order invoked once per cell."""
        portfolio = _bootstrap(db)
        start_processing(db)
        _seed_live_readiness(db)
        set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
        assert result.pending_rows_written > 0

        # Every LIVE ledger row carries live_order_id + live_client_order_id.
        rows = db.execute(
            "SELECT mode, wallet_group, live_order_id, live_client_order_id "
            "FROM ledger ORDER BY id"
        ).fetchall()
        live_rows = [r for r in rows if r["mode"] == "LIVE"]
        assert live_rows, "expected at least one LIVE ledger row"
        for r in live_rows:
            assert r["wallet_group"] == "group_5m"
            assert r["live_order_id"] is not None
            assert r["live_client_order_id"] is not None

        # OrderClient was actually called.
        assert order_client.calls, "order_client.place_gtc_limit_order not called"
        # And every attempt has been linked (status=LINKED) â€” no in-flight
        # reservations consume group-cap headroom after a tick.
        unlinked = db.execute(
            "SELECT COUNT(*) AS n FROM live_order_attempts "
            "WHERE ledger_id IS NULL AND status IN ('SUBMITTING','SUBMITTED')"
        ).fetchone()
        assert unlinked["n"] == 0

    def test_live_order_uses_walked_limit_price(
        self, db: sqlite3.Connection
    ) -> None:
        portfolio = _bootstrap(db)
        start_processing(db)
        _seed_live_readiness(db)
        set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_walk_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        walked_calls = [
            c for c in order_client.calls
            if c["limit_price"] is not None and c["limit_price"] > c["fill_price"]
        ]
        assert walked_calls, order_client.calls
        for call in walked_calls:
            assert call["walked_vwap"] > call["fill_price"]

        prices = [
            r["price"]
            for r in db.execute(
                "SELECT price FROM live_order_attempts WHERE status='LINKED'"
            ).fetchall()
        ]
        assert any(
            p is not None and abs(float(p) - float(call["limit_price"])) < 1e-9
            for p in prices
            for call in walked_calls
        )

    def test_accepted_live_order_without_fill_metadata_links_pending_row(
        self, db: sqlite3.Connection
    ) -> None:
        portfolio = _bootstrap(db)
        start_processing(db)
        _seed_live_readiness(db)
        set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(
            balance=200.0,
            include_fill_metadata=False,
        )
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        rows = db.execute(
            "SELECT outcome, live_order_id, live_client_order_id, "
            "live_filled_size, live_filled_price "
            "FROM ledger WHERE mode='LIVE'"
        ).fetchall()
        assert rows
        for row in rows:
            assert row["outcome"] == "PENDING"
            assert row["live_order_id"] is not None
            assert row["live_client_order_id"] is not None
            assert row["live_filled_size"] is None
            assert row["live_filled_price"] is None

        linked = db.execute(
            "SELECT COUNT(*) AS n FROM live_order_attempts WHERE status='LINKED'"
        ).fetchone()
        assert linked["n"] == len(rows)


class _FillGetStub:
    """OrderClient stub exposing only fetch_order_fill for the immediate-GET
    fallback (PURE_STOP_04 coverage)."""

    def __init__(self, *, result=(None, None, None), raises=False):
        self._result = result
        self._raises = raises
        self.calls: list = []

    def fetch_order_fill(self, order_id, *, intent_amount_usd=None, intent_price=None):
        self.calls.append((order_id, intent_amount_usd, intent_price))
        if self._raises:
            raise RuntimeError("boom")
        return self._result


class TestImmediateFillForHeldPass:
    """PURE_STOP_04 root-cause fix: persist the realized fill on the ledger at
    accept time via one immediate GET when the FAK response left no fill, so the
    held-position (T3) pass sees the position THIS tick instead of after
    window_end."""

    def test_returns_fill_from_followup_get(self) -> None:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass

        stub = _FillGetStub(result=(6.0, 0.72, "2026-07-10T00:00:00Z"))
        out = _immediate_fill_for_held_pass(
            stub, "ord-1", offset_sec=285, current_size=None,
            intent_amount_usd=4.3, intent_price=0.72,
        )
        assert out == (6.0, 0.72, "2026-07-10T00:00:00Z")
        # intent threaded through so the reconciler price fallbacks can fire.
        assert stub.calls == [("ord-1", 4.3, 0.72)]

    def test_skips_when_response_already_had_fill(self) -> None:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass

        stub = _FillGetStub(result=(6.0, 0.72, None))
        out = _immediate_fill_for_held_pass(
            stub, "ord-1", offset_sec=285, current_size=5.0,
            intent_amount_usd=4.3, intent_price=0.72,
        )
        assert out is None
        assert stub.calls == []  # no GET when the fill was already known

    def test_skips_non_subminute(self) -> None:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass

        stub = _FillGetStub(result=(6.0, 0.72, None))
        out = _immediate_fill_for_held_pass(
            stub, "ord-1", offset_sec=None, current_size=None,
            intent_amount_usd=4.3, intent_price=0.72,
        )
        assert out is None
        assert stub.calls == []  # 5m/15m orders never pay the extra GET

    def test_size_without_price_is_rejected(self) -> None:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass

        stub = _FillGetStub(result=(6.0, None, None))
        out = _immediate_fill_for_held_pass(
            stub, "ord-1", offset_sec=285, current_size=None,
            intent_amount_usd=4.3, intent_price=0.72,
        )
        assert out is None  # never persist size without price

    def test_no_fetch_method_returns_none(self) -> None:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass
        from types import SimpleNamespace

        out = _immediate_fill_for_held_pass(
            SimpleNamespace(), "ord-1", offset_sec=285, current_size=None,
            intent_amount_usd=4.3, intent_price=0.72,
        )
        assert out is None

    def test_exception_swallowed(self) -> None:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass

        stub = _FillGetStub(raises=True)
        out = _immediate_fill_for_held_pass(
            stub, "ord-1", offset_sec=285, current_size=None,
            intent_amount_usd=4.3, intent_price=0.72,
        )
        assert out is None


def _flush_signal(*, coin: str = "BTC", entry_minute: int = 4) -> "BetSignal":
    """Minimal LIVE sub-minute BetSignal for the deferred-fill flush tests."""
    from cryptobot.execution.ledger import BetSignal

    return BetSignal(
        coin=coin,
        sleeve="v2_5m",
        entry_minute=entry_minute,
        side="YES",
        window_start_ts=1_715_923_200,
        window_end_ts=1_715_923_500,
        interval_min=5,
        poly_market_id="market-1",
        poly_token_id="token-yes",
        fee_rate_bps=100.0,
        model_p=0.78,
        p_market=0.72,
        fill_price=0.72,
        walked_vwap=0.72,
        realized_edge=0.05,
        partial_ret_z_1h=None,
        lut_p_bucket=None,
        lut_hit_rate=None,
        lut_n_cum=None,
        target_usd=4.3,
        bet_size=4.3,
        slot_filled_pre=0.0,
        entry_top_price=0.72,
        yes_pre1_anchor=0.72,
        wallet_id="v2_5m",
        offset_sec=285,
        config_sha256="stub-sha",
    )


class TestFlushImmediateFillGets:
    """A11 latency: the immediate-fill GET is queued during Phase B and
    flushed after the loops — the write-back must land the same columns the
    old inline path passed into record_bet (size+price together, fill_ts
    only when the venue reported one) on the already-committed row."""

    def _insert_row(self, db, *, coin: str, fill_ts=None) -> int:
        from cryptobot.execution.ledger import record_bet

        row_id = record_bet(
            db,
            _flush_signal(coin=coin),
            mode="LIVE",
            live_order_id=f"ord-{coin}",
            live_client_order_id=f"coid-{coin}",
            live_fill_ts=fill_ts,
        )
        db.commit()
        return row_id

    def test_single_entry_writes_fill_inline(self, db: sqlite3.Connection) -> None:
        from cryptobot.execution.pipeline import (
            _DeferredFillGet,
            _flush_immediate_fill_gets,
        )

        row_id = self._insert_row(db, coin="BTC")
        stub = _FillGetStub(result=(6.0, 0.72, "2026-07-14 00:00:01"))
        _flush_immediate_fill_gets(db, [
            _DeferredFillGet(
                order_client=stub, ledger_id=row_id, order_id="ord-BTC",
                offset_sec=285, current_size=None,
                intent_amount_usd=4.3, intent_price=0.72,
            ),
        ])
        row = db.execute(
            "SELECT live_filled_size, live_filled_price, live_fill_ts "
            "FROM ledger WHERE id=?",
            (row_id,),
        ).fetchone()
        assert row["live_filled_size"] == 6.0
        assert row["live_filled_price"] == 0.72
        assert row["live_fill_ts"] == "2026-07-14 00:00:01"

    def test_null_ts_keeps_post_response_fill_ts(
        self, db: sqlite3.Connection
    ) -> None:
        """COALESCE precedence: a GET with no fill_ts must not clobber the
        POST response's fill_ts already on the row (inline-path parity)."""
        from cryptobot.execution.pipeline import (
            _DeferredFillGet,
            _flush_immediate_fill_gets,
        )

        row_id = self._insert_row(db, coin="BTC", fill_ts="2026-07-14 00:00:00")
        stub = _FillGetStub(result=(6.0, 0.72, None))
        _flush_immediate_fill_gets(db, [
            _DeferredFillGet(
                order_client=stub, ledger_id=row_id, order_id="ord-BTC",
                offset_sec=285, current_size=None,
                intent_amount_usd=4.3, intent_price=0.72,
            ),
        ])
        row = db.execute(
            "SELECT live_filled_size, live_fill_ts FROM ledger WHERE id=?",
            (row_id,),
        ).fetchone()
        assert row["live_filled_size"] == 6.0
        assert row["live_fill_ts"] == "2026-07-14 00:00:00"

    def test_fanout_batch_writes_fills_and_skips_no_fill(
        self, db: sqlite3.Connection
    ) -> None:
        """>1 entries take the concurrent fan-out path; a no-fill GET leaves
        its row NULL (the 60s reconciler remains the authority)."""
        from cryptobot.execution.pipeline import (
            _DeferredFillGet,
            _flush_immediate_fill_gets,
        )

        filled_id = self._insert_row(db, coin="BTC")
        empty_id = self._insert_row(db, coin="ETH")
        filled_stub = _FillGetStub(result=(6.0, 0.72, "2026-07-14 00:00:01"))
        empty_stub = _FillGetStub(result=(None, None, None))
        _flush_immediate_fill_gets(db, [
            _DeferredFillGet(
                order_client=filled_stub, ledger_id=filled_id,
                order_id="ord-BTC", offset_sec=285, current_size=None,
                intent_amount_usd=4.3, intent_price=0.72,
            ),
            _DeferredFillGet(
                order_client=empty_stub, ledger_id=empty_id,
                order_id="ord-ETH", offset_sec=285, current_size=None,
                intent_amount_usd=4.3, intent_price=0.72,
            ),
        ])
        filled = db.execute(
            "SELECT live_filled_size FROM ledger WHERE id=?", (filled_id,)
        ).fetchone()
        empty = db.execute(
            "SELECT live_filled_size, live_filled_price FROM ledger WHERE id=?",
            (empty_id,),
        ).fetchone()
        assert filled["live_filled_size"] == 6.0
        assert empty["live_filled_size"] is None
        assert empty["live_filled_price"] is None


class TestPhaseBPlaceCandidatesFirst:
    """A9 latency pin: Phase B must commit every PLACE-candidate cell
    (score.skip_reason is None) before any skip cell, so skip cells' signals
    INSERTs never sit between an entry decision and its order POST. Order
    within each partition is stable."""

    def test_place_candidates_commit_before_skip_cells(
        self, db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import cryptobot.execution.pipeline as pipeline_mod

        portfolio = _bootstrap(db)
        start_processing(db)

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        deps = _stub_deps(stub)
        # Seed every coin EXCEPT BTC: its cells score SKIP:no_feature in
        # Phase A (partial-ret warmup) and are appended to cell_decisions
        # BEFORE the qualifying cells — exactly the skips-first build order
        # the Phase B reorder exists to invert.
        for coin, spot in stub.spot_data.items():
            if coin == "BTC":
                continue
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        seen: list = []
        real_commit = pipeline_mod._commit_cell

        def spy(conn, d, *args, **kwargs):
            seen.append(d.score.skip_reason)
            return real_commit(conn, d, *args, **kwargs)

        monkeypatch.setattr(pipeline_mod, "_commit_cell", spy)
        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        # Both partitions must exist for this pin to be meaningful.
        assert any(reason is None for reason in seen), seen
        assert any(reason is not None for reason in seen), seen
        first_skip = next(
            i for i, reason in enumerate(seen) if reason is not None
        )
        assert all(reason is not None for reason in seen[first_skip:]), seen


class TestDryContinuesDuringStoppedProcessing:
    def test_dry_continues_when_operator_stopped(
        self, db: sqlite3.Connection
    ) -> None:
        """D-LIVE-12: STOPPED blocks LIVE only â€” DRY paper bets keep firing
        so parity validation stays active during a Stop."""
        portfolio = _bootstrap(db)
        # Operator is STOPPED (default from boot â€” no start_processing call).
        # All wallets remain DRY.

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
        # DRY rows still write even though operator_state=STOPPED_PROCESSING.
        assert result.pending_rows_written > 0
        rows = db.execute(
            "SELECT mode FROM ledger WHERE outcome='PENDING'"
        ).fetchall()
        assert all(r["mode"] == "DRY" for r in rows)
        # No CLOB calls because no wallet is LIVE.
        assert order_client.calls == []


class TestLiveBlockedWhenOperatorStopped:
    def test_live_skips_with_operator_stopped(self, db: sqlite3.Connection) -> None:
        portfolio = _bootstrap(db)
        start_processing(db)
        _seed_live_readiness(db)
        set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

        # Operator presses Stop AFTER the LIVE flip.
        from cryptobot.execution.operator_control import stop_processing
        stop_processing(db, reason="emergency")

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
        # No LIVE rows; no CLOB calls.
        live_rows = db.execute(
            "SELECT COUNT(*) AS n FROM ledger WHERE mode='LIVE'"
        ).fetchone()
        assert live_rows["n"] == 0
        assert order_client.calls == []
        # The skip reason recorded for the v6_5m wallet is operator_stopped.
        assert result.skips_by_reason.get("operator_stopped", 0) > 0


class TestLiveOrderAuthFailureMarksAttemptFailed:
    def test_clob_auth_failure_skips_and_releases_reservation(
        self, db: sqlite3.Connection
    ) -> None:
        portfolio = _bootstrap(db)
        start_processing(db)
        _seed_live_readiness(db)
        set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(
            balance=200.0,
            always_fail_kind="auth",
        )
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
        # No LIVE ledger row, no in-flight reservations.
        live_rows = db.execute(
            "SELECT COUNT(*) AS n FROM ledger WHERE mode='LIVE'"
        ).fetchone()
        assert live_rows["n"] == 0
        attempts = db.execute(
            "SELECT status FROM live_order_attempts"
        ).fetchall()
        # Exactly one attempt because only the first cell exercised the
        # CLOB path â€” the stub returned an auth failure once, then went
        # back to its happy default.
        assert any(a["status"] == "FAILED" for a in attempts)
        # SKIP reason includes the clob_<kind> suffix.
        assert any(
            k.startswith("clob_") for k in result.skips_by_reason
        ), result.skips_by_reason


class TestModeIsImmutable:
    def test_pending_live_row_unchanged_by_flip_to_dry(
        self, db: sqlite3.Connection
    ) -> None:
        portfolio = _bootstrap(db)
        start_processing(db)
        _seed_live_readiness(db)
        set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        stub = StubDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )
        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        # Operator halts: flip v6_5m back to DRY mid-resolution.
        set_wallet_mode(db, "v6_5m", "DRY", reason="halt")

        # Existing PENDING LIVE rows keep their mode (D-LIVE-3 / D-LIVE-6).
        rows = db.execute(
            "SELECT mode FROM ledger WHERE wallet_id='v6_5m'"
        ).fetchall()
        # At least one row was LIVE; none should have flipped back.
        assert any(r["mode"] == "LIVE" for r in rows)


class TestDiscoveryWarmDeferredToPassTwoTail:
    def test_ensure_warm_runs_after_books_once_per_market(
        self, db: sqlite3.Connection
    ) -> None:
        """A15: the L4 sync-warm fallback is deferred until AFTER the Pass-2
        book fetches (so the Pass-1 async warm overlaps the books RTT instead
        of paying a duplicate inline GET) — and it still fires once per
        discovered market, keeping the warm-before-SIGN guarantee's input set
        unchanged."""
        portfolio = _bootstrap(db)
        start_processing(db)

        now_ts = _qualifying_v6_5m_now()
        window5 = (now_ts // 300) * 300
        events: list[tuple[str, str]] = []

        class _BookRecordingDeps(StubDeps):
            def fetch_book(self, token_id, *, now_ts):
                events.append(("book", token_id))
                return super().fetch_book(token_id, now_ts=now_ts)

        stub = _BookRecordingDeps(
            market_lookup=_build_markets(window5, interval_min=5),
            book_lookup=_stub_v6_books(now_ts),
            spot_data=_spots(now_ts=now_ts),
        )
        order_client = StubOrderClient(balance=200.0)
        order_client.warm_market_info_async = (  # type: ignore[attr-defined]
            lambda cid: events.append(("warm_async", cid))
        )
        order_client.ensure_market_info_warm = (  # type: ignore[attr-defined]
            lambda cid: events.append(("ensure", cid)) or True
        )
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(
                deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price
            )

        result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
        assert result.pending_rows_written > 0

        warm_idx = [i for i, e in enumerate(events) if e[0] == "warm_async"]
        book_idx = [i for i, e in enumerate(events) if e[0] == "book"]
        ensure_idx = [i for i, e in enumerate(events) if e[0] == "ensure"]
        assert warm_idx and ensure_idx
        # Once per discovered market — same set the inline call used to see.
        warm_cids = [cid for k, cid in events if k == "warm_async"]
        ensure_cids = [cid for k, cid in events if k == "ensure"]
        assert sorted(warm_cids) == sorted(ensure_cids)
        # Pass 2 fetches the discovered markets' YES/NO inline books first
        # (later phases may fetch more — only the leading batch is Pass 2's).
        pass2_idx = book_idx[: 2 * len(warm_cids)]
        assert len(pass2_idx) == 2 * len(warm_cids)
        # Async warms fire at Pass-1 discovery — before any Pass-2 book GET…
        assert max(warm_idx) < min(pass2_idx)
        # …and the sync fallback is deferred past the whole books batch.
        assert min(ensure_idx) > max(pass2_idx)
