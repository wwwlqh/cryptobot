"""Fast-entry latency: the per-second commit-stage cache and the placement-line
breakdown.

Pins the two things that must hold for the cache to be safe:

  * **Commit-stage staleness.** The cached preamble is dropped by a placement
    (the epoch) and by a fill/settlement (the ledger probe), the operator row is
    re-read on EVERY fire, and master-off never caches at all.
  * **The drawdown halt still binds** when the preamble is served from cache.
"""

from __future__ import annotations

import math
import time
from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.execution import pipeline as P
from cryptobot.scheduler import fast_entry as FE
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.tests._fixtures import seed_for_tests


_WINDOW_START = 1_715_923_200          # 300s-aligned
_OFFSET = 285                          # a step1last checkpoint second
_NOW = _WINDOW_START + _OFFSET
_ANCHOR = 67_000.0                     # the window-open spot the engine stamps
#: Live spot deliberately OFF the window anchor so ``partial_ret`` — and hence z
#: — is non-zero. At spot == anchor the numerator is log(1) == 0 and z is 0 for
#: EVERY rv, which would make the "changing rv must miss" case vacuous (it is
#: genuinely a hit there: rv reaches the score only through the z denominator).
_SPOT = 67_050.0
_ENTRY_MINUTE = _OFFSET // 60
#: The engine, the spot dict and `_score_cell_for` are all keyed by the
#: `config.COINS` spelling in production (run_scanner_tick feeds
#: `partial_ret_engine.update` straight from the spot batch), so the rig uses
#: that spelling too — a lower-cased engine key would make `current_z` return
#: None and silently un-memoise every case below.
_COIN = "BTC"


# --------------------------------------------------------------------------- real-ish scoring rig


def _hgb_sleeve(label: str = "fl_5m"):
    return SimpleNamespace(
        label=label, interval="5m", interval_min=5,
        checkpoint_offset_set=frozenset(range(270, 300)),
        ask_bucket_ranges=((0.55, 0.85),), threshold=0.03, min_edge=0.02,
        signal_source="hgb", slippage_haircut=0.0, min_lut_n=0,
        side_filter="both", book_consumption_max=None, max_walk_price=0.05,
        hour_bucket_set="all", dow_set="all",
    )


def _engine(anchor: float = _ANCHOR) -> PartialRetEngine:
    eng = PartialRetEngine()
    seed_for_tests(eng, _COIN, now_ts=_NOW, spot=anchor)
    return eng


def _rig(**over):
    """(deps, sleeve, None, None) for the commit-stage tests."""
    kw = dict(
        partial_ret_engine=_engine(),
        config_sha256="stub",
        fetch_spots=lambda **k: None,
        fetch_market=lambda *a, **k: None,
        get_disk_free_mb=lambda: 10_000.0,
    )
    kw.update(over)
    return P.TickDeps(**kw), _hgb_sleeve(), None, None


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(config, "FAST_TRIGGER_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", False)
    monkeypatch.setattr(config, "COINS", ("BTC",))
    P.clear_fast_commit_stage()
    yield
    P.clear_fast_commit_stage()
    if P._FAST_ENTRY_LOCK.locked():  # pragma: no cover — regression guard
        P._FAST_ENTRY_LOCK.release()


# --------------------------------------------------------------------------- flag-off byte-identity


def test_flag_off_never_caches_the_commit_stage(monkeypatch, db):
    monkeypatch.setattr(config, "FAST_TRIGGER_ENABLED", False)
    deps, sleeve, _est, _pipe = _rig()
    portfolio = SimpleNamespace(
        sleeves=[sleeve], wallet_groups=(),
        funder_balance_floor_usd=lambda: 0.0,
    )
    tick = P.TickResult(tick_ts=_NOW)
    _s1, hit1 = P._fast_commit_stage(db, portfolio, deps, now_ts=_NOW, tick=tick)
    _s2, hit2 = P._fast_commit_stage(db, portfolio, deps, now_ts=_NOW, tick=tick)
    assert (hit1, hit2) == (False, False)
    assert P._FAST_COMMIT_STAGE == {}


# --------------------------------------------------------------------------- commit-stage cache


def _portfolio(sleeve):
    return SimpleNamespace(
        sleeves=[sleeve], wallet_groups=(),
        funder_balance_floor_usd=lambda: 0.0,
    )


def _stage(db, deps, portfolio, *, now_ts=_NOW):
    return P._fast_commit_stage(
        db, portfolio, deps, now_ts=now_ts, tick=P.TickResult(tick_ts=now_ts)
    )


def test_second_fire_in_the_same_second_reuses_the_preamble(db):
    """The fire that PLACES is usually the 2nd..Nth of its second; it must not
    re-pay the ledger scan an earlier non-placing fire already paid."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    first, hit1 = _stage(db, deps, portfolio)
    second, hit2 = _stage(db, deps, portfolio)
    assert (hit1, hit2) == (False, True)
    assert second is first


def test_a_placement_invalidates_the_preamble(db):
    """⚠️ THE safety clause: the DD-halt / balance / cap view must never be
    served across a placement."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    _stage(db, deps, portfolio)
    P._bump_placement_epoch()
    _again, hit = _stage(db, deps, portfolio)
    assert hit is False


def test_a_bare_reservation_invalidates_the_preamble(db):
    """⚠️ EVERY reservation site, not just the ones in pipeline.py.

    A `live_order_attempts` row (SUBMITTING, `ledger_id IS NULL`) is group
    pending exposure the moment it commits, and NEITHER other signal sees it:
    `ledger_version` aggregates the `ledger` table, and that row does not land
    until the POST returns — never at all if it fails. The T3 scale-in top-up
    (`execution.scale_in`, on its own 1Hz held-position thread) reserves without
    going through this module, so the bump has to live in
    `reserve_live_order_attempt` itself. Reserving through the real function is
    the whole point of this test: it pins the CHOKE POINT, not a call site."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    _stage(db, deps, portfolio)
    P.reserve_live_order_attempt(db, _topup_signal(), wallet_group="group_5m")
    _again, hit = _stage(db, deps, portfolio)
    assert hit is False


def test_each_connection_keeps_its_own_preamble(db, tmp_db):
    """The two fast rungs never share a connection (ThreadLocalConnections: the
    sweep on rotating pool threads, the event rung on its own). A single global
    slot made every cross-rung fire both miss AND evict the other's stage, so
    the advertised saving was realized well under half the time."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    first, _ = _stage(db, deps, portfolio)
    other, hit_other = _stage(tmp_db, deps, portfolio)
    assert hit_other is False                    # a different conn, a different world
    assert other is not first
    # ...and the second connection's load did NOT evict the first's stage.
    again, hit_again = _stage(db, deps, portfolio)
    assert (again is first, hit_again) == (True, True)


def test_a_fill_invalidates_the_preamble(db):
    """A ledger row landing between two fires (a fill linked by the reconciler,
    an F3 settlement, a booked stop) moves ``ledger_version`` => rescan."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    _stage(db, deps, portfolio)
    _seed_ledger_row(db)
    _again, hit = _stage(db, deps, portfolio)
    assert hit is False


def test_a_new_second_invalidates_the_preamble(db):
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    _stage(db, deps, portfolio)
    _again, hit = _stage(db, deps, portfolio, now_ts=_NOW + 1)
    assert hit is False


def test_a_different_deps_bundle_invalidates_the_preamble(db):
    """The bundle carries the balance fetcher and the order client; a different
    one is a different world."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    _stage(db, deps, portfolio)
    _again, hit = _stage(db, P.dataclass_replace(deps), portfolio)
    assert hit is False


def test_the_operator_row_is_re_read_on_every_fire(db):
    """An operator STOP must bind at the next FIRE, not the next second — so the
    one singleton row is never served from the per-second cache."""
    calls: list[int] = []

    def _loader(_conn):
        calls.append(1)
        return SimpleNamespace(state="LIVE")

    deps, sleeve, _e, _p = _rig(operator_state_loader=_loader)
    portfolio = _portfolio(sleeve)
    _stage(db, deps, portfolio)
    n_after_load = len(calls)
    assert P._fresh_operator_state(db, deps, None) is not None
    assert len(calls) == n_after_load + 1


def test_a_failing_operator_read_falls_back_to_the_cached_row(db):
    """`None` maps to `operator_stopped`; a transient SQLite error must not halt
    LIVE entries."""
    def _boom(_conn):
        raise RuntimeError("db busy")

    deps, _sleeve, _e, _p = _rig(operator_state_loader=_boom)
    cached = SimpleNamespace(state="LIVE")
    assert P._fresh_operator_state(db, deps, cached) is cached


def test_assemble_never_reuses_a_within_scan_exposure_delta(db):
    """The context carries a MUTABLE ``tick_group_live_delta``; two scans off one
    cached load must not share it."""
    deps, sleeve, _e, _p = _rig()
    portfolio = _portfolio(sleeve)
    stage, _ = _stage(db, deps, portfolio)
    d1: dict = {}
    ctx1, _ = P._assemble_live_commit_context(
        stage.loaded, deps, tick_group_live_delta=d1
    )
    ctx1.tick_group_live_delta["g"] = 5.0
    d2: dict = {}
    ctx2, _ = P._assemble_live_commit_context(
        stage.loaded, deps, tick_group_live_delta=d2
    )
    assert ctx2.tick_group_live_delta == {}
    assert ctx2.tick_group_live_delta is not ctx1.tick_group_live_delta


def _topup_signal():
    """The minimum `reserve_live_order_attempt` reads off a signal. A plain
    namespace on purpose: `_signal_payload` falls back for a non-dataclass, so
    this exercises the real reservation without importing the BetSignal shape."""
    return SimpleNamespace(
        wallet_id="w", coin=_COIN, sleeve="fl_5m", entry_minute=_ENTRY_MINUTE,
        side="YES", window_start_ts=_WINDOW_START, poly_market_id="cid",
        poly_token_id="tid", limit_price=0.70, fill_price=0.70, bet_size=5.0,
    )


def _seed_ledger_row(db) -> None:
    db.execute(
        "INSERT INTO ledger (bet_ts, coin, sleeve, entry_minute, side, "
        " window_start_ts, window_end_ts, interval_min, poly_market_id, "
        " poly_token_id, fee_rate_bps, model_p, p_market, fill_price, "
        " realized_edge, target_usd, bet_size, entry_top_price, config_sha256, "
        " outcome, pnl_net, wallet_id) "
        "VALUES ('2026-07-30 00:00:00','BTC','fl_5m',4,'YES',?,?,5,'cid','tid',"
        " 0.0,0.6,0.7,0.7,0.02,5.0,5.0,0.7,'sha','PENDING',0.0,'w')",
        (_WINDOW_START, _WINDOW_START + 300),
    )
    db.commit()


# --------------------------------------------------------------------------- resolver-deps memo


def test_resolver_deps_view_is_built_once_per_bundle():
    deps, _sleeve, _e, _p = _rig()
    view = P._fast_resolver_deps(deps)
    assert P._fast_resolver_deps(deps) is view
    assert (view.fetch_book, view.fetch_books, view.book_shadow) == (None, None, None)
    other = P.dataclass_replace(deps)
    assert P._fast_resolver_deps(other) is not view


# --------------------------------------------------------------------------- per-second TickDeps


def test_trigger_builds_one_deps_bundle_per_second():
    """A fresh bundle per fire defeated every identity-keyed per-second cache
    downstream (the resolver view and the commit-stage preamble)."""
    built: list[int] = []

    def _factory():
        built.append(1)
        return _rig()[0]

    tr = FE.FastEntryTrigger(
        conn_factory=lambda: object(),
        portfolio=_portfolio(_hgb_sleeve()),
        deps_factory=_factory,
        now_fn=lambda: float(_NOW),
    )
    first = tr._deps_for(_NOW)
    assert tr._deps_for(_NOW) is first
    assert len(built) == 1
    assert tr._deps_for(_NOW + 1) is not first
    assert len(built) == 2


def _scan_rig(monkeypatch, *, scorer_calls: list, resolved):
    """A fast scan with a counting scorer stub and a stubbed resolver."""
    sleeve = _hgb_sleeve()
    deps, _s, _e, _p = _rig(
        peek_spots=lambda **k: {
            "BTC": SimpleNamespace(price=_SPOT, publish_time=_NOW)
        },
        book_prefetch=SimpleNamespace(
            get=lambda t, *, now_ts: None, noted_tokens=lambda: ()
        ),
    )
    frozen = SimpleNamespace(
        skip_reason=None, side="YES", model_p=0.6, partial_ret_z_1h=0.1,
    )

    def _scorer(**kw):
        scorer_calls.append(kw)
        return frozen

    monkeypatch.setattr(P, "_venue_market_cache_has", lambda *a, **k: True)
    monkeypatch.setattr(P, "_score_cell_for", _scorer)
    monkeypatch.setattr(P, "_resolve_net_cells", resolved)
    return sleeve, deps, _portfolio(sleeve)


def test_the_dd_halt_still_binds_on_a_cached_commit_stage(monkeypatch, db):
    """⚠️ The hard drawdown halt is the ONE gate the cheap in-window predicate
    cannot evaluate, so it binds off the commit stage's wallet snapshot. Serving
    that snapshot from the per-second cache must not weaken it."""
    calls: list = []
    sleeve, deps, portfolio = _scan_rig(
        monkeypatch, scorer_calls=calls,
        resolved=lambda cells, **kw: [
            SimpleNamespace(
                coin="BTC", sleeve=cells[0].sleeve,
                offset_sec=cells[0].offset_sec,
                window_start_ts=cells[0].window_start,
                entry_minute=cells[0].entry_minute,
                trigger_src=cells[0].trigger_src,
                score=SimpleNamespace(skip_reason=None, side="YES"),
            )
        ],
    )
    halted = SimpleNamespace(drawdown_pct_peak_frac=0.99)
    monkeypatch.setattr(
        P, "_load_live_context", lambda *a, **k: ({}, None, {}, None, True)
    )
    monkeypatch.setattr(
        P, "get_wallet_snapshots", lambda *a, **k: {sleeve.label: halted}
    )
    monkeypatch.setattr(P, "_live_pending_exposure_usd", lambda *a, **k: 0.0)
    monkeypatch.setattr(P, "_live_cap_basis_usd", lambda *a, **k: None)
    monkeypatch.setattr(P, "_live_wallet_skip_reason", lambda **k: None)
    monkeypatch.setattr(P, "_fresh_operator_state", lambda *a, **k: None)
    monkeypatch.setattr(P, "evaluate_breaker", lambda conn: SimpleNamespace(tripped=False))
    monkeypatch.setattr(P, "_wallet_drawdown_halted", lambda pf, snap: True)
    monkeypatch.setattr(P, "_commit_cell", lambda *a, **k: pytest.fail(
        "a drawdown-halted wallet placed off-tick"
    ))
    first = P.run_fast_entry_scan(
        db, portfolio, now_ts=_NOW, deps=deps, trigger_src="fast_sweep",
    )
    second = P.run_fast_entry_scan(
        db, portfolio, now_ts=_NOW, deps=deps, trigger_src="fast_event",
    )
    assert (first["dd_halted"], second["dd_halted"]) == (1, 1)
    assert (first["committed"], second["committed"]) == (0, 0)
    # The second fire really was the cached path (nothing placed, ledger still).
    assert P._FAST_COMMIT_STAGE.get(id(db)) is not None


# --------------------------------------------------------------------------- telemetry


def test_walk_time_is_accumulated_on_the_tick_result():
    assert P.TickResult(tick_ts=_NOW).ms_walk == 0.0


def test_the_push_stamp_is_only_taken_for_a_push_that_wakes():
    """``on_ask_update`` runs at ~822 msg/s on the venue recv thread; the added
    monotonic read must sit AFTER both gates so a dropped push pays nothing."""
    clock = [100.0]
    tr = FE.FastEntryTrigger(
        conn_factory=lambda: object(),
        portfolio=_portfolio(_hgb_sleeve()),
        deps_factory=lambda: _rig()[0],
        now_fn=lambda: float(_NOW),
        monotonic_fn=lambda: clock[0],
    )
    tr._universe = frozenset({"YES-BTC"})
    tr._in_window = True
    tr.on_ask_update("OTHER", 1.0)               # off-universe => dropped
    assert tr._push_m == 0.0
    tr._in_window = False
    tr.on_ask_update("YES-BTC", 1.0)             # off-window => dropped
    assert tr._push_m == 0.0
    tr._in_window = True
    tr.on_ask_update("YES-BTC", 1.0)
    assert tr._push_m == 100.0


def test_event_lead_is_logged_only_when_the_scan_places(monkeypatch, caplog):
    """The trigger-side segment (worker wake + the min-gap throttle, up to 62ms
    at the 250ms default) is invisible to the scan's own `pre_ms`. Log it — but
    once per PLACEMENT, never at push rate."""
    placed = [{"orders": 1}, {"orders": 0}]
    monkeypatch.setattr(FE, "run_fast_entry_scan", lambda *a, **k: placed.pop(0))
    monkeypatch.setattr(config, "FAST_TRIGGER_MS", 100)   # min_gap 25ms
    tr = FE.FastEntryTrigger(
        conn_factory=lambda: object(),
        portfolio=_portfolio(_hgb_sleeve()),
        deps_factory=lambda: _rig()[0],
        now_fn=lambda: float(_NOW),
    )
    tr._universe = frozenset({"YES-BTC"})
    tr._in_window = True
    with caplog.at_level("INFO", logger=FE.logger.name):
        tr.start_event_worker()
        try:
            tr.on_ask_update("YES-BTC", 1.0)
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and len(placed) == 2:
                time.sleep(0.01)
            # Second push: the scan reports no order, so no lead line.
            tr.on_ask_update("YES-BTC", 1.0)
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and placed:
                time.sleep(0.01)
        finally:
            tr.stop()
    leads = [m for m in caplog.messages if "event_lead" in m]
    assert len(leads) == 1, caplog.messages
    for field in ("push_to_scan_ms=", "throttle_ms=", "min_gap_ms=25"):
        assert field in leads[0]


def test_placement_line_carries_the_full_breakdown(monkeypatch, caplog):
    """The next forensic must be able to see WHERE the remaining ms live —
    and `trigger_to_post_ms` must keep its name AND its meaning so the
    pre-change 381ms samples stay comparable."""
    sleeve = _hgb_sleeve()
    portfolio = _portfolio(sleeve)
    deps, _s, _e, _p = _rig(
        peek_spots=lambda **k: {
            "BTC": SimpleNamespace(price=_SPOT, publish_time=_NOW)
        },
        book_prefetch=SimpleNamespace(
            get=lambda t, *, now_ts: None, noted_tokens=lambda: ()
        ),
    )
    monkeypatch.setattr(P, "_venue_market_cache_has", lambda *a, **k: True)
    monkeypatch.setattr(
        P, "_score_cell_for",
        lambda **kw: SimpleNamespace(skip_reason=None, side="YES"),
    )
    monkeypatch.setattr(
        P, "_resolve_net_cells",
        lambda cells, **kw: [
            SimpleNamespace(
                coin="BTC", sleeve=cells[0].sleeve, offset_sec=cells[0].offset_sec,
                window_start_ts=cells[0].window_start,
                entry_minute=cells[0].entry_minute,
                trigger_src=cells[0].trigger_src,
                score=SimpleNamespace(skip_reason=None, side="YES"),
            )
        ],
    )
    monkeypatch.setattr(
        P, "_fast_commit_stage",
        lambda *a, **k: (
            P._FastCommitStage(
                second=_NOW, refs=(), epoch=0, ledger_probe=(),
                loaded=P._LiveCtxInputs(
                    mode_state={}, op_state=None, group_state={},
                    live_balance=None, readiness_fresh=True,
                    all_group_caps_sum_usd=0.0, wallet_snaps={},
                ),
                tick_breaker=SimpleNamespace(tripped=False),
                disk_free_mb=10_000.0,
            ),
            True,
        ),
    )
    monkeypatch.setattr(P, "_live_wallet_skip_reason", lambda **k: None)
    monkeypatch.setattr(P, "_fresh_operator_state", lambda *a, **k: None)
    monkeypatch.setattr(P, "_wallet_drawdown_halted", lambda pf, snap: False)

    def _commit(conn, d, *a, **k):
        tick = a[6]                     # positional `tick` in _commit_cell's list
        tick.n_orders = 1
        tick.ms_order_sign = 1.5
        tick.ms_order_post = 302.0
        tick.ms_walk = 0.4

    monkeypatch.setattr(P, "_commit_cell", _commit)
    with caplog.at_level("INFO", logger=P.logger.name):
        counters = P.run_fast_entry_scan(
            object(), portfolio, now_ts=_NOW, deps=deps, trigger_src="fast_event",
        )
    assert counters["orders"] == 1
    line = next(m for m in caplog.messages if m.startswith("fast_entry placed"))
    for field in (
        "trigger_to_post_ms=", "pre_ms=", "ctx_ms=", "ctxhit=1", "walk_ms=0.4",
        "sign_ms=2", "post_ms=302", "db_ms=", "cells=", "bettable=",
    ):
        assert field in line, f"{field!r} missing from {line!r}"
