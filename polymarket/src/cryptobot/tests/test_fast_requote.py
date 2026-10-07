"""Fix 3b — fast-requote for flicker rejects (config.FAST_REQUOTE_ENABLED).

On a ``no_liquidity`` FAK reject the tick arms ONE bounded fast-requote for the
(coin, interval, window, side) key; a dedicated short-cadence scheduler job
drains it ~FAST_REQUOTE_DELAY_MS later and replays a fresh-book reserve→POST on
its own connection + order_client. This suite pins:

  * Flag defaults + flag-OFF byte-identity (no queue write when off).
  * Arming: only no_liquidity arms; never after window_end; offset cells only;
    the per-side-window bound (MAX_ATTEMPTS).
  * The fresh-book source chain (book_source first, else fetch_book GET).
  * The executor: fill → note_fill + _V2_MARKET_FILLED + ledger row; reject →
    terminal FAILED attempt; the drop guards (window closed / market filled /
    out-of-band / thin book / slot race).
  * The drain (due vs not-ready) + start_scheduler job registration.
"""
from __future__ import annotations

import queue
import sqlite3
from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot.execution import pipeline as P
from cryptobot.execution.order_attempts import reserve_live_order_attempt
from cryptobot.execution.types import OrderResult


# --------------------------------------------------------------------------- fixtures

def _sleeve(label: str = "fr_5m"):
    # Non-MID band (lo=0.55 != 0.65) so _v2_decider_for mints a fresh per-label
    # decider (never the shared module-global MID decider), keeping tests isolated.
    return SimpleNamespace(
        label=label, interval="5m", interval_min=5,
        ask_bucket_ranges=((0.55, 0.85),), threshold=0.03, min_edge=0.02,
        signal_source="hgb", book_consumption_max=None, max_walk_price=0.05,
    )


def _book(asks, *, token="YES-BTC", fetched_at=1_715_923_485.0):
    return BookSnapshot(token_id=token, asks=asks, bids=[], fetched_at_ts=fetched_at)


_THICK = [{"price": 0.70, "size": 1000.0}]     # in-band, fully covers $5
_THIN = [{"price": 0.70, "size": 0.5}]         # in-band, covers only $0.35 (< $1)
_OUT_OF_BAND = [{"price": 0.90, "size": 1000.0}]  # 0.90 > 0.85 => out of band

_WINDOW_START = 1_715_923_200
_WINDOW_END = _WINDOW_START + 300
_NOW_IN_WINDOW = _WINDOW_START + 285


def _cell(**over):
    """CellDecision-shaped stub the arm helper reads."""
    score = SimpleNamespace(
        model_p=0.90, realized_edge=0.05, partial_ret_z_1h=None,
        lut_p_bucket=8, lut_hit_rate=0.88, lut_n_cum=1000,
    )
    d = SimpleNamespace(
        coin="BTC", window_start_ts=_WINDOW_START, window_end_ts=_WINDOW_END,
        poly_market_id="COND-BTC", entry_minute=4, offset_sec=285,
        strike_price=67_000.0, window_open_spot=67_000.0, fee_rate_bps=100.0,
        score=score,
    )
    for k, v in over.items():
        setattr(d, k, v)
    return d


def _arm(sleeve, d, **over):
    kw = dict(
        error_kind="no_liquidity", sleeve=sleeve, d=d, side="YES",
        token_id="YES-BTC", walker_target=5.0, anchor_for_walker=None,
        target_usd=5.0, config_sha256="stub", wallet_id="v2_5m",
        wallet_group="group_5m", now_ts=_NOW_IN_WINDOW,
    )
    kw.update(over)
    return P._maybe_arm_fast_requote(**kw)


def _rq(sleeve, **over):
    kw = dict(
        sleeve=sleeve, coin="BTC", side="YES", token_id="YES-BTC",
        poly_market_id="COND-BTC", entry_minute=4, offset_sec=285,
        window_start_ts=_WINDOW_START, window_end_ts=_WINDOW_END,
        model_p=0.90, realized_edge=0.05, partial_ret_z_1h=None,
        lut_p_bucket=8, lut_hit_rate=0.88, lut_n_cum=1000, fee_rate_bps=100.0,
        strike_price=67_000.0, window_open_spot=67_000.0,
        walker_target=5.0, anchor_for_walker=None, target_usd=5.0,
        wallet_id="v2_5m", wallet_group="group_5m", config_sha256="stub",
        ready_at_monotonic=0.0,
    )
    kw.update(over)
    return P._FastRequote(**kw)


class _OrderStub:
    """Minimal OrderClient: records placements, returns a preset OrderResult."""

    def __init__(self, result: OrderResult):
        self._result = result
        self.calls: list[str] = []

    def place_gtc_limit_order(self, signal, *, client_order_id):
        self.calls.append(client_order_id)
        return self._result


def _fill_result(order_id="clob-1"):
    return OrderResult(
        success=True, order_id=order_id, client_order_id="ignored",
        limit_price=0.70, filled_size_tokens=7.0, filled_price_vwap=0.70,
        filled_value_usd=4.9, fill_ts="2026-07-18T00:00:00Z",
    )


def _reject_result(kind="no_liquidity"):
    return OrderResult(success=False, error=kind, error_kind=kind)


def _fetch_book_for(books: dict):
    def _f(token_id, *, now_ts):
        return books.get(token_id)
    return _f


@pytest.fixture(autouse=True)
def _reset_fast_requote_state():
    def _clear():
        while True:
            try:
                P._FAST_REQUOTE_QUEUE.get_nowait()
            except queue.Empty:
                break
        P._FAST_REQUOTE_ATTEMPTS.clear()
        P._V2_MARKET_FILLED.clear()
        P._V2_WINDOW_DECIDERS.clear()
    _clear()
    yield
    _clear()


# --------------------------------------------------------------------------- flag defaults

def test_flag_defaults_off():
    assert config.FAST_REQUOTE_ENABLED is False
    assert config.FAST_REQUOTE_DELAY_MS == 400
    assert config.FAST_REQUOTE_MAX_ATTEMPTS == 1
    assert config.FAST_REQUOTE_MIN_DELTA == -0.02
    # DRAIN_MS unset => falls back to DELAY_MS (byte-identical cadence).
    assert config.FAST_REQUOTE_DRAIN_MS == config.FAST_REQUOTE_DELAY_MS


# --------------------------------------------------------------------------- flag-OFF byte-identity

def test_flag_off_no_enqueue():
    """Default OFF => the arm helper returns False BEFORE any queue write — the
    hard byte-identity invariant (no queue write, no bound bump)."""
    assert not config.FAST_REQUOTE_ENABLED
    assert _arm(_sleeve(), _cell()) is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 0
    assert P._FAST_REQUOTE_ATTEMPTS == {}


# --------------------------------------------------------------------------- arming

def test_arms_on_no_liquidity(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell()) is True
    assert P._FAST_REQUOTE_QUEUE.qsize() == 1
    assert P._FAST_REQUOTE_ATTEMPTS[("BTC", "5m", _WINDOW_START, "YES")] == 1


@pytest.mark.parametrize(
    "kind",
    ["auth", "market_closed", "insufficient_funds", "venue_unavailable",
     "post_only", "invalid_amount", "network", "unknown", None],
)
def test_never_arms_on_non_flicker(monkeypatch, kind):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell(), error_kind=kind) is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 0


def test_bound_max_attempts_one(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    monkeypatch.setattr(config, "FAST_REQUOTE_MAX_ATTEMPTS", 1)
    sleeve, d = _sleeve(), _cell()
    assert _arm(sleeve, d) is True
    # Second arm on the SAME (coin, interval, window, side) is bounded out.
    assert _arm(sleeve, d) is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 1


def test_bound_respects_higher_max(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    monkeypatch.setattr(config, "FAST_REQUOTE_MAX_ATTEMPTS", 2)
    sleeve, d = _sleeve(), _cell()
    assert _arm(sleeve, d) is True
    assert _arm(sleeve, d) is True
    assert _arm(sleeve, d) is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 2


def test_never_after_window_end(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell(), now_ts=_WINDOW_END) is False
    assert _arm(_sleeve(), _cell(), now_ts=_WINDOW_END + 5) is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 0


def test_offset_none_never_arms(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell(offset_sec=None)) is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 0


def test_empty_token_never_arms(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell(), token_id="") is False
    assert P._FAST_REQUOTE_QUEUE.qsize() == 0


def test_arm_carries_correct_side_token(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell(), side="NO", token_id="NO-BTC") is True
    item = P._FAST_REQUOTE_QUEUE.get_nowait()
    assert item.side == "NO" and item.token_id == "NO-BTC"
    assert P._FAST_REQUOTE_ATTEMPTS[("BTC", "5m", _WINDOW_START, "NO")] == 1


def test_arm_carries_rejected_limit_price(monkeypatch):
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell(), rejected_limit_price=0.71) is True
    item = P._FAST_REQUOTE_QUEUE.get_nowait()
    assert item.rejected_limit_price == 0.71


def test_arm_rejected_limit_defaults_none(monkeypatch):
    # Legacy call sites that don't pass the limit arm a gate-disabled item.
    monkeypatch.setattr(config, "FAST_REQUOTE_ENABLED", True)
    assert _arm(_sleeve(), _cell()) is True
    assert P._FAST_REQUOTE_QUEUE.get_nowait().rejected_limit_price is None


# --------------------------------------------------------------------------- fresh-book chain

def test_fresh_book_prefers_source():
    class _Src:
        def __init__(self, snap):
            self.snap = snap
            self.calls = []

        def get(self, token, *, now_ts):
            self.calls.append((token, now_ts))
            return self.snap

    src = _Src(_book(_THICK))
    fetch = _fetch_book_for({"YES-BTC": _book(_OUT_OF_BAND)})  # must NOT be used
    out = P._fast_requote_fresh_book(src, fetch, "YES-BTC", _NOW_IN_WINDOW)
    assert out.asks == _THICK
    assert src.calls == [("YES-BTC", float(_NOW_IN_WINDOW))]


def test_fresh_book_falls_back_to_fetch():
    # Source miss (None) => one fetch_book GET.
    class _Src:
        def get(self, token, *, now_ts):
            return None

    fetch = _fetch_book_for({"YES-BTC": _book(_THICK)})
    out = P._fast_requote_fresh_book(_Src(), fetch, "YES-BTC", _NOW_IN_WINDOW)
    assert out.asks == _THICK


def test_fresh_book_none_when_nothing_available():
    assert P._fast_requote_fresh_book(None, lambda *_a, **_k: None, "YES-BTC", 1) is None
    assert P._fast_requote_fresh_book(None, None, "YES-BTC", 1) is None
    assert P._fast_requote_fresh_book(None, lambda *_a, **_k: None, "", 1) is None


def test_fresh_book_swallows_source_and_fetch_errors():
    class _Src:
        def get(self, token, *, now_ts):
            raise RuntimeError("boom")

    def _boom_fetch(token_id, *, now_ts):
        raise RuntimeError("boom")

    assert P._fast_requote_fresh_book(_Src(), _boom_fetch, "YES-BTC", 1) is None


# --------------------------------------------------------------------------- executor

def _exec(db, rq, *, order_client, books=None, book_source=None, now_ts=_NOW_IN_WINDOW):
    return P._execute_fast_requote(
        db, rq,
        order_client=order_client,
        fetch_book=_fetch_book_for(books or {}),
        book_source=book_source,
        now_ts=now_ts,
    )


def test_executor_fills_and_consumes_window(db: sqlite3.Connection):
    sleeve = _sleeve()
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(sleeve), order_client=stub, books={"YES-BTC": _book(_THICK)})
    assert status == "filled"
    assert stub.calls, "expected exactly one POST"
    # Ledger row written (LIVE) + attempt LINKED + window consumed + telemetry.
    row = db.execute(
        "SELECT mode, live_order_id, side, coin FROM ledger WHERE mode='LIVE'"
    ).fetchone()
    assert row is not None and row["live_order_id"] == "clob-1"
    linked = db.execute(
        "SELECT COUNT(*) AS n FROM live_order_attempts WHERE status='LINKED'"
    ).fetchone()
    assert linked["n"] == 1
    assert ("BTC", "5m", _WINDOW_START) in P._V2_MARKET_FILLED
    fill = db.execute(
        "SELECT outcome FROM subminute_fills WHERE outcome='accepted'"
    ).fetchone()
    assert fill is not None


def test_executor_reject_is_terminal(db: sqlite3.Connection):
    sleeve = _sleeve()
    stub = _OrderStub(_reject_result())
    status = _exec(db, _rq(sleeve), order_client=stub, books={"YES-BTC": _book(_THICK)})
    assert status == "rejected"
    assert stub.calls  # it DID post
    # No ledger row; the attempt is FAILED; window NOT consumed (no second requote).
    assert db.execute("SELECT COUNT(*) AS n FROM ledger").fetchone()["n"] == 0
    assert db.execute(
        "SELECT status FROM live_order_attempts"
    ).fetchone()["status"] == "FAILED"
    assert ("BTC", "5m", _WINDOW_START) not in P._V2_MARKET_FILLED
    rej = db.execute(
        "SELECT outcome FROM subminute_fills WHERE outcome='rejected'"
    ).fetchone()
    assert rej is not None


def test_executor_out_of_band_drops_without_posting(db: sqlite3.Connection):
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve()), order_client=stub,
        books={"YES-BTC": _book(_OUT_OF_BAND)},
    )
    assert status == "out_of_band"
    assert stub.calls == []  # no POST, no reserve
    assert db.execute("SELECT COUNT(*) AS n FROM live_order_attempts").fetchone()["n"] == 0


def test_executor_adverse_dip_drops_without_posting(db: sqlite3.Connection):
    """Fresh ask dipped >|MIN_DELTA| below the rejected limit => adverse_dip
    drop, no reserve, no POST. Book at 0.70 vs rejected limit 0.75 = -0.05
    dip, past the -0.02 floor."""
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve(), rejected_limit_price=0.75),
        order_client=stub, books={"YES-BTC": _book(_THICK)},
    )
    assert status == "adverse_dip"
    assert stub.calls == []
    assert db.execute("SELECT COUNT(*) AS n FROM live_order_attempts").fetchone()["n"] == 0


def test_executor_edge_above_ceiling_drops_without_posting(db: sqlite3.Connection):
    """V2_EDGE_CAP gap fix (2026-07-23): a requote whose RE-PRICED edge
    (model_p - fresh ask - parabola fee) exceeds sleeve.max_edge drops like
    the entry-path edge_above_ceiling gate. model_p 0.90 @ fresh ask 0.70 =>
    ~0.185 > cap 0.08."""
    sleeve = _sleeve()
    sleeve.max_edge = 0.08
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(sleeve), order_client=stub, books={"YES-BTC": _book(_THICK)})
    assert status == "edge_above_ceiling"
    assert stub.calls == []  # no POST, no reserve
    assert db.execute("SELECT COUNT(*) AS n FROM live_order_attempts").fetchone()["n"] == 0


def test_executor_edge_under_ceiling_posts(db: sqlite3.Connection):
    """A ceiling above the re-priced edge leaves the requote path untouched
    (and sleeves WITHOUT a max_edge attr — every other test here — skip the
    check entirely via the tolerant getattr)."""
    sleeve = _sleeve()
    sleeve.max_edge = 0.50
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(sleeve), order_client=stub, books={"YES-BTC": _book(_THICK)})
    assert status == "filled"


def test_executor_small_dip_within_floor_posts(db: sqlite3.Connection):
    """A dip inside the floor (-0.015 >= -0.02) is the flicker bucket — the
    requote proceeds and fills."""
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve(), rejected_limit_price=0.715),
        order_client=stub, books={"YES-BTC": _book(_THICK)},
    )
    assert status == "filled"
    assert stub.calls


def test_executor_unchanged_or_higher_ask_posts(db: sqlite3.Connection):
    # Ask AT the rejected limit (the +13..16c/$ unchanged-book cohort).
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve(), rejected_limit_price=0.70),
        order_client=stub, books={"YES-BTC": _book(_THICK)},
    )
    assert status == "filled"


def test_executor_none_rejected_limit_skips_gate(db: sqlite3.Connection):
    # Legacy item (no limit recorded) => gate inert, requote proceeds.
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve(), rejected_limit_price=None),
        order_client=stub, books={"YES-BTC": _book(_THICK)},
    )
    assert status == "filled"


def test_executor_dip_gate_respects_env_override(db: sqlite3.Connection, monkeypatch):
    # MIN_DELTA widened to -0.10 => the same -0.05 dip passes.
    monkeypatch.setattr(config, "FAST_REQUOTE_MIN_DELTA", -0.10)
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve(), rejected_limit_price=0.75),
        order_client=stub, books={"YES-BTC": _book(_THICK)},
    )
    assert status == "filled"


def test_executor_thin_book_drops(db: sqlite3.Connection):
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(_sleeve()), order_client=stub, books={"YES-BTC": _book(_THIN)})
    assert status == "insufficient_depth"
    assert stub.calls == []
    assert db.execute("SELECT COUNT(*) AS n FROM live_order_attempts").fetchone()["n"] == 0


def test_executor_window_closed_drops(db: sqlite3.Connection):
    stub = _OrderStub(_fill_result())
    status = _exec(
        db, _rq(_sleeve()), order_client=stub,
        books={"YES-BTC": _book(_THICK)}, now_ts=_WINDOW_END,
    )
    assert status == "window_closed"
    assert stub.calls == []


def test_executor_market_filled_drops(db: sqlite3.Connection):
    P._V2_MARKET_FILLED.add(("BTC", "5m", _WINDOW_START))
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(_sleeve()), order_client=stub, books={"YES-BTC": _book(_THICK)})
    assert status == "market_filled"
    assert stub.calls == []


def test_executor_no_book_drops(db: sqlite3.Connection):
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(_sleeve()), order_client=stub, books={})  # fetch returns None
    assert status == "no_book"
    assert stub.calls == []


def test_executor_no_client_drops(db: sqlite3.Connection):
    status = _exec(db, _rq(_sleeve()), order_client=None, books={"YES-BTC": _book(_THICK)})
    assert status == "no_client"


def test_executor_slot_full_when_reservation_races(db: sqlite3.Connection):
    """A concurrent 1Hz tick already holds an active SUBMITTING reservation on
    this exact slot => reserve_live_order_attempt raises IntegrityError => drop."""
    from cryptobot.execution.ledger import BetSignal

    # Occupy the slot with a SUBMITTING attempt (same wallet/coin/sleeve/
    # entry_minute/side/window as the requote).
    occupying = BetSignal(
        coin="BTC", sleeve="fr_5m", entry_minute=4, side="YES",
        window_start_ts=_WINDOW_START, window_end_ts=_WINDOW_END, interval_min=5,
        poly_market_id="COND-BTC", poly_token_id="YES-BTC", fee_rate_bps=100.0,
        model_p=0.90, p_market=0.70, fill_price=0.70, walked_vwap=0.70,
        limit_price=0.70, realized_edge=0.05, partial_ret_z_1h=None,
        lut_p_bucket=8, lut_hit_rate=0.88, lut_n_cum=1000, target_usd=5.0,
        bet_size=5.0, slot_filled_pre=0.0, entry_top_price=0.70,
        yes_pre1_anchor=0.70, wallet_id="v2_5m", offset_sec=285,
        config_sha256="stub",
    )
    reserve_live_order_attempt(db, occupying, wallet_group="group_5m")

    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(_sleeve()), order_client=stub, books={"YES-BTC": _book(_THICK)})
    assert status == "slot_full"
    assert stub.calls == []  # never posted a second live order on a taken slot


def test_executor_uses_book_source_chain(db: sqlite3.Connection):
    class _Src:
        def __init__(self, snap):
            self.snap = snap
            self.calls = []

        def get(self, token, *, now_ts):
            self.calls.append(token)
            return self.snap

    src = _Src(_book(_THICK))
    stub = _OrderStub(_fill_result())
    status = _exec(db, _rq(_sleeve()), order_client=stub, books={}, book_source=src)
    assert status == "filled"
    assert src.calls == ["YES-BTC"]  # re-read via the chain, not fetch_book


# --------------------------------------------------------------------------- drain

def test_drain_processes_due_item(db: sqlite3.Connection):
    sleeve = _sleeve()
    P._FAST_REQUOTE_QUEUE.put_nowait(_rq(sleeve, ready_at_monotonic=0.0))
    stub = _OrderStub(_fill_result())
    counters = P.drain_fast_requote_queue(
        db, order_client=stub, fetch_book=_fetch_book_for({"YES-BTC": _book(_THICK)}),
        now_monotonic=1_000_000.0, now_ts=float(_NOW_IN_WINDOW),
    )
    assert counters["processed"] == 1
    assert counters["placed"] == 1
    assert counters["filled"] == 1
    assert stub.calls
    assert P._FAST_REQUOTE_QUEUE.qsize() == 0


def test_drain_requeues_not_ready_item(db: sqlite3.Connection):
    sleeve = _sleeve()
    P._FAST_REQUOTE_QUEUE.put_nowait(_rq(sleeve, ready_at_monotonic=1_000_000.0))
    stub = _OrderStub(_fill_result())
    counters = P.drain_fast_requote_queue(
        db, order_client=stub, fetch_book=_fetch_book_for({"YES-BTC": _book(_THICK)}),
        now_monotonic=0.0, now_ts=float(_NOW_IN_WINDOW),
    )
    assert counters["processed"] == 0
    assert counters["requeued"] == 1
    assert stub.calls == []
    assert P._FAST_REQUOTE_QUEUE.qsize() == 1  # still queued for a later drain


def test_drain_dropped_item_counts(db: sqlite3.Connection):
    # A due item whose window has closed => processed but dropped, not placed.
    sleeve = _sleeve()
    P._FAST_REQUOTE_QUEUE.put_nowait(_rq(sleeve, ready_at_monotonic=0.0))
    stub = _OrderStub(_fill_result())
    counters = P.drain_fast_requote_queue(
        db, order_client=stub, fetch_book=_fetch_book_for({"YES-BTC": _book(_THICK)}),
        now_monotonic=1_000_000.0, now_ts=float(_WINDOW_END + 10),
    )
    assert counters["processed"] == 1
    assert counters["placed"] == 0
    assert counters["dropped"] == 1
    assert stub.calls == []


# --------------------------------------------------------------------------- scheduler wiring

def _sched_runner(db):
    from cryptobot.scheduler.tick import TickRunner
    from cryptobot.execution.pipeline import TickResult

    class _StubPortfolio:
        sleeves = ()

    return TickRunner(
        conn_factory=lambda: db,
        portfolio=_StubPortfolio(),
        deps_factory=lambda: None,
        notify=lambda *_a: None,
        on_tick=None,
    )


def test_start_scheduler_registers_fast_requote_when_provided(db: sqlite3.Connection):
    from apscheduler.triggers.interval import IntervalTrigger
    from cryptobot.scheduler.tick import start_scheduler

    runner = _sched_runner(db)
    scheduler = start_scheduler(
        runner, tick_seconds=3600,
        fast_requote_job=lambda: None,
        fast_requote_seconds=0.4,
    )
    try:
        job = scheduler.get_job("fast_requote_drain")
        assert job is not None
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval.total_seconds() == pytest.approx(0.4)
    finally:
        scheduler.shutdown(wait=False)


def test_start_scheduler_omits_fast_requote_when_disabled(db: sqlite3.Connection):
    from cryptobot.scheduler.tick import start_scheduler

    runner = _sched_runner(db)
    # Cadence 0 (flag off) => no job even when a callable is supplied.
    scheduler = start_scheduler(
        runner, tick_seconds=3600,
        fast_requote_job=lambda: None,
        fast_requote_seconds=0.0,
    )
    try:
        assert scheduler.get_job("fast_requote_drain") is None
    finally:
        scheduler.shutdown(wait=False)

    # No job supplied at all => also absent (default byte-identical wiring).
    scheduler = start_scheduler(runner, tick_seconds=3600)
    try:
        assert scheduler.get_job("fast_requote_drain") is None
    finally:
        scheduler.shutdown(wait=False)
