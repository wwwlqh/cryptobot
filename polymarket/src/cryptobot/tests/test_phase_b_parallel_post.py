"""LEVER 1 — CRYPTOBOT_PHASE_B_PARALLEL_POST flag-ON coverage.

The flag OFF path is covered byte-for-byte by test_pipeline_live_routing.py (the
serial code path is unchanged). These tests exercise ONLY the flag-ON behavior:

  * the order POSTs of >=2 place-candidate LIVE cells fire CONCURRENTLY;
  * post-POST processing (ledger rows) is replayed SERIALLY in the ORIGINAL cell
    order — the parallel run's LIVE rows match the serial run's, same order;
  * the documented conservative-exposure divergence: a reserved cell applies its
    stake to the within-tick exposure delta PESSIMISTICALLY at reserve time, so a
    later cell's gate sees it even when the earlier POST ultimately REJECTS
    (serial only counts ACCEPTED fills);
  * reject handling: a rejected cell writes no LIVE row and its reservation is
    released (FAILED) — no orphaned SUBMITTING/SUBMITTED rows survive the tick.
"""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from cryptobot import config
from cryptobot.db import init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import set_wallet_mode, start_processing
from cryptobot.execution.pipeline import run_scanner_tick
from cryptobot.execution.types import OrderResult
from cryptobot.tests._fixtures import seed_for_tests
from cryptobot.tests._stub_scorer import install_fake_scorer
from cryptobot.tests.test_pipeline_live_routing import (
    StubDeps,
    _bootstrap,
    _build_markets,
    _seed_live_readiness,
    _spots,
    _stub_deps,
    _stub_v6_walk_books,
    _qualifying_v6_5m_now,
)


# The borrowed helpers come from a module whose own autouse scorer stub does
# NOT apply here — install it (see tests/_stub_scorer).
@pytest.fixture(autouse=True)
def _stub_scorer(monkeypatch):
    install_fake_scorer(monkeypatch)



def _fresh_db() -> sqlite3.Connection:
    return init_db(":memory:")


def _live_v6_5m(db: sqlite3.Connection):
    """Bootstrap + flip v6_5m to LIVE; return (portfolio, now_ts, StubDeps)."""
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
    return portfolio, now_ts, stub


# --------------------------------------------------------------------------
# Stubs
# --------------------------------------------------------------------------


class _ConcurrencyStub:
    """Records the MAX number of place_gtc_limit_order calls in flight at once.

    Each call sleeps briefly while holding an in-flight slot. Serial POSTs never
    overlap (max_in_flight == 1); a concurrent fan-out overlaps (>= 2)."""

    def __init__(self, *, balance: float = 200.0) -> None:
        self.balance = balance
        self.calls: list[str] = []
        self._lock = threading.Lock()
        self._in_flight = 0
        self.max_in_flight = 0

    def check_balance(self):
        return self.balance

    def place_gtc_limit_order(self, signal, *, client_order_id):
        with self._lock:
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
            self.calls.append(signal.coin)
            n = len(self.calls)
        time.sleep(0.05)
        with self._lock:
            self._in_flight -= 1
        price = float(signal.fill_price)
        size = float(signal.bet_size) / price
        return OrderResult(
            success=True,
            order_id=f"clob-{n}",
            client_order_id=client_order_id,
            limit_price=float(signal.limit_price or signal.fill_price),
            filled_size_tokens=size,
            filled_price_vwap=price,
            filled_value_usd=price * size,
            fill_ts=utc_now_sql(),
        )


class _RejectOneCoinStub:
    """Accepts every coin except ``reject_coin`` (a terminal CLOB reject)."""

    def __init__(self, reject_coin: str, *, balance: float = 200.0) -> None:
        self.reject_coin = reject_coin
        self.balance = balance
        self.calls: list[str] = []

    def check_balance(self):
        return self.balance

    def place_gtc_limit_order(self, signal, *, client_order_id):
        self.calls.append(signal.coin)
        if signal.coin == self.reject_coin:
            return OrderResult(
                success=False,
                error="market_closed",
                error_kind="market_closed",
                client_order_id=client_order_id,
            )
        price = float(signal.fill_price)
        size = float(signal.bet_size) / price
        return OrderResult(
            success=True,
            order_id=f"clob-{signal.coin}",
            client_order_id=client_order_id,
            limit_price=float(signal.limit_price or signal.fill_price),
            filled_size_tokens=size,
            filled_price_vwap=price,
            filled_value_usd=price * size,
            fill_ts=utc_now_sql(),
        )


class _RejectAllStub:
    """Rejects every POST (so no ledger row is ever written)."""

    def __init__(self, *, balance: float = 200.0) -> None:
        self.balance = balance
        self.calls: list[str] = []

    def check_balance(self):
        return self.balance

    def place_gtc_limit_order(self, signal, *, client_order_id):
        self.calls.append(signal.coin)
        return OrderResult(
            success=False,
            error="market_closed",
            error_kind="market_closed",
            client_order_id=client_order_id,
        )


def _live_rows(db: sqlite3.Connection) -> list[str]:
    return [
        r["coin"]
        for r in db.execute(
            "SELECT coin FROM ledger WHERE mode='LIVE' AND sleeve='v6_5m' "
            "ORDER BY id"
        ).fetchall()
    ]


# --------------------------------------------------------------------------
# Concurrency + result ordering
# --------------------------------------------------------------------------


def test_flag_on_posts_fire_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PHASE_B_PARALLEL_POST", True)
    db = _fresh_db()
    try:
        portfolio, now_ts, stub = _live_v6_5m(db)
        order_client = _ConcurrencyStub()
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        assert len(order_client.calls) >= 2, order_client.calls
        assert order_client.max_in_flight >= 2, (
            "expected concurrent POSTs under the flag, "
            f"max_in_flight={order_client.max_in_flight}"
        )
    finally:
        db.close()


def test_flag_off_posts_are_serial(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PHASE_B_PARALLEL_POST", False)
    db = _fresh_db()
    try:
        portfolio, now_ts, stub = _live_v6_5m(db)
        order_client = _ConcurrencyStub()
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        assert len(order_client.calls) >= 2, order_client.calls
        assert order_client.max_in_flight == 1, (
            "serial path must never overlap POSTs, "
            f"max_in_flight={order_client.max_in_flight}"
        )
    finally:
        db.close()


def test_parallel_finish_matches_serial_row_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-POST processing replays serially in the ORIGINAL cell order — the
    parallel run's LIVE rows equal the serial run's, in the same order."""
    def _run(parallel: bool) -> list[str]:
        monkeypatch.setattr(config, "PHASE_B_PARALLEL_POST", parallel)
        db = _fresh_db()
        try:
            portfolio, now_ts, stub = _live_v6_5m(db)
            order_client = _ConcurrencyStub()
            deps = _stub_deps(stub, order_client=order_client)
            for coin, spot in stub.spot_data.items():
                seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)
            run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
            # Every reservation resolved — no orphaned SUBMITTING/SUBMITTED.
            unlinked = db.execute(
                "SELECT COUNT(*) AS n FROM live_order_attempts "
                "WHERE ledger_id IS NULL AND status IN ('SUBMITTING','SUBMITTED')"
            ).fetchone()
            assert unlinked["n"] == 0
            return _live_rows(db)
        finally:
            db.close()

    serial_rows = _run(False)
    parallel_rows = _run(True)
    assert serial_rows == parallel_rows, (serial_rows, parallel_rows)
    assert len(parallel_rows) >= 2


# --------------------------------------------------------------------------
# Reject handling
# --------------------------------------------------------------------------


def test_parallel_reject_releases_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PHASE_B_PARALLEL_POST", True)
    db = _fresh_db()
    try:
        portfolio, now_ts, stub = _live_v6_5m(db)
        order_client = _RejectOneCoinStub("ETH")
        deps = _stub_deps(stub, order_client=order_client)
        for coin, spot in stub.spot_data.items():
            seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

        run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

        # ETH: no LIVE ledger row, reservation released to FAILED.
        eth_rows = db.execute(
            "SELECT COUNT(*) AS n FROM ledger "
            "WHERE mode='LIVE' AND sleeve='v6_5m' AND coin='ETH'"
        ).fetchone()
        assert eth_rows["n"] == 0
        eth_failed = db.execute(
            "SELECT COUNT(*) AS n FROM live_order_attempts "
            "WHERE coin='ETH' AND status='FAILED'"
        ).fetchone()
        assert eth_failed["n"] >= 1, "rejected cell's reservation must be released"

        # At least one accepted coin wrote a LIVE row.
        live_rows = _live_rows(db)
        assert live_rows and "ETH" not in live_rows, live_rows

        # NO orphaned in-flight reservations survive the tick.
        orphaned = db.execute(
            "SELECT COUNT(*) AS n FROM live_order_attempts "
            "WHERE ledger_id IS NULL AND status IN ('SUBMITTING','SUBMITTED')"
        ).fetchone()
        assert orphaned["n"] == 0
    finally:
        db.close()


# --------------------------------------------------------------------------
# Conservative-exposure divergence
# --------------------------------------------------------------------------


def test_parallel_reserve_applies_pessimistic_exposure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reserved cell's stake hits the within-tick exposure delta at RESERVE
    time, so a LATER cell's gate sees it — even when every POST ultimately
    rejects. Serially, a rejected POST contributes NOTHING, so later cells see
    the unchanged baseline. Captured via the pending_exposure_total each LIVE
    v6_5m cell's gate is handed."""
    import cryptobot.execution.pipeline as pipeline_mod

    real_eval = pipeline_mod.evaluate_gates

    def _run(parallel: bool) -> list[float]:
        monkeypatch.setattr(config, "PHASE_B_PARALLEL_POST", parallel)
        seen: list[float] = []

        def _capture(inp):
            if inp.mode == "LIVE" and getattr(inp.sleeve, "label", "") == "v6_5m":
                seen.append(float(inp.pending_exposure_total))
            return real_eval(inp)

        monkeypatch.setattr(pipeline_mod, "evaluate_gates", _capture)
        db = _fresh_db()
        try:
            portfolio, now_ts, stub = _live_v6_5m(db)
            # All POSTs reject: serially this adds NO within-tick exposure.
            order_client = _RejectAllStub()
            deps = _stub_deps(stub, order_client=order_client)
            for coin, spot in stub.spot_data.items():
                seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)
            run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
            return seen
        finally:
            monkeypatch.setattr(pipeline_mod, "evaluate_gates", real_eval)
            db.close()

    serial_seen = _run(False)
    parallel_seen = _run(True)

    assert len(serial_seen) >= 2 and len(parallel_seen) >= 2, (
        serial_seen, parallel_seen
    )
    # Serial + all-reject: every cell's gate saw the SAME baseline (rejects add
    # no delta), so no within-tick accumulation.
    assert max(serial_seen) == pytest.approx(min(serial_seen)), serial_seen
    # Parallel: reserved stakes accumulated pessimistically BEFORE any POST, so
    # a later cell's gate saw strictly more pending exposure than the first.
    assert max(parallel_seen) > min(parallel_seen), parallel_seen
    assert max(parallel_seen) > max(serial_seen), (parallel_seen, serial_seen)
