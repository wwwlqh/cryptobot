"""Fast-trigger entry path (config.FAST_TRIGGER_*, operator 2026-07-30).

The on-chain forensic measured that 84% of our misses were taken by a FASTER
TAKER ~1s before we arrived, and that the only reclaimable latency is
trigger-to-decision (the 1Hz boundary-aligned tick means a mid-second ask refill
waits ~500ms before we look). Two extra rungs — a 250ms sweep job and a WS
ask-update push — converge on ONE evaluation path that reuses the tick's own
scoring, resolver and ``_commit_cell``. This suite pins:

  * the cheap out-of-window return (no spot fetch, no score, no DB);
  * cadence clamp + flag-off byte-identity (no job registered, no WS callback
    registered, no lock taken by the tick);
  * gate PARITY with the tick's sleeve loop — including the hard DD halt, the
    one gate the cheap in-window predicate cannot evaluate;
  * the same-integer-offset dedup that makes tick+fast coexistence safe, both
    before and after the rescore picks a side;
  * first-attempt-only: a fast-path reject spends the side and the TICK sees it;
  * single-flight: a contended scan is abandoned, never queued;
  * ``trigger_src`` carried on the CELL through ``record_subminute_fill``;
  * the WS hook: ask-side frames fire, bid-only deltas do not, and the chain
    fans registration out to whichever sources expose it.
"""
from __future__ import annotations

import sqlite3
import threading
from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.execution import pipeline as P
from cryptobot.execution.fill_rate_breaker import record_subminute_fill
from cryptobot.scheduler import fast_entry as FE


# --------------------------------------------------------------------------- fixtures

_WINDOW_START = 1_715_923_200          # 300s-aligned
_NOW_IN_WINDOW = _WINDOW_START + 285   # offset 285 => a step1last checkpoint
_NOW_OFF_WINDOW = _WINDOW_START + 100  # offset 100 => not a checkpoint


def _sleeve(label: str = "fe_5m"):
    # Non-MID band (lo=0.55) so _v2_decider_for mints a per-label decider and
    # these tests never mutate the shared module-global MID decider.
    return SimpleNamespace(
        label=label, interval="5m", interval_min=5,
        checkpoint_offset_set=frozenset(range(270, 300)),
        ask_bucket_ranges=((0.55, 0.85),), threshold=0.03, min_edge=0.02,
        signal_source="hgb", book_consumption_max=None, max_walk_price=0.05,
        hour_bucket_set="all", dow_set="all",
    )


def _portfolio(*sleeves):
    return SimpleNamespace(
        sleeves=list(sleeves) or [_sleeve()],
        funder_balance_floor_usd=lambda: 0.0,
    )


class _Boom:
    """Any call is a test failure — proves a stage was never reached."""

    def __init__(self, what: str) -> None:
        self._what = what

    def __call__(self, *a, **k):  # pragma: no cover - only on regression
        raise AssertionError(f"{self._what} must not be called")


def _deps(**over):
    kw = dict(
        partial_ret_engine=SimpleNamespace(),
        config_sha256="stub",
        fetch_spots=_Boom("fetch_spots"),
        peek_spots=_Boom("peek_spots"),
        fetch_market=_Boom("fetch_market"),
        fetch_book=_Boom("fetch_book"),
        get_disk_free_mb=lambda: 10_000.0,
        book_prefetch=SimpleNamespace(
            get=lambda t, *, now_ts: None,
            noted_tokens=lambda: ("YES-BTC", "NO-BTC"),
        ),
    )
    kw.update(over)
    return P.TickDeps(**kw)


def _spots(price: float = 67_000.0):
    return {"BTC": SimpleNamespace(price=price, publish_time=_NOW_IN_WINDOW)}


def _scan(portfolio=None, *, now_ts=_NOW_IN_WINDOW, conn=None, src="fast_sweep",
          **deps_over):
    """``run_fast_entry_scan`` with this suite's defaults.

    Every call in this file varies at most the portfolio, the second, the
    connection stand-in and one or two deps; spelling all six kwargs out a dozen
    times hid those differences.
    """
    return P.run_fast_entry_scan(
        conn if conn is not None else object(),
        portfolio if portfolio is not None else _portfolio(),
        now_ts=now_ts,
        deps=_deps(**deps_over),
        trigger_src=src,
    )


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    # The fast rungs are cache-only for market metadata; pretend the tick has
    # already warmed the per-window cache (the gating itself is pinned by
    # test_scan_skips_a_cold_market_rather_than_paying_a_gamma_get).
    monkeypatch.setattr(P, "_venue_market_cache_has", lambda *a, **k: True)
    monkeypatch.setattr(config, "COINS", ("BTC",))
    monkeypatch.setattr(config, "FAST_TRIGGER_ENABLED", True)
    monkeypatch.setattr(config, "FAST_TRIGGER_EVENT_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", False)
    P._V2_WINDOW_DECIDERS.pop("fe_5m", None)
    yield
    P._V2_WINDOW_DECIDERS.pop("fe_5m", None)
    # A leaked lock would deadlock every later test's tick commit.
    if P._FAST_ENTRY_LOCK.locked():  # pragma: no cover - regression guard
        P._FAST_ENTRY_LOCK.release()


# --------------------------------------------------------------------------- cadence

@pytest.mark.parametrize(
    "ms,expected",
    [(250, 0.25), (1000, 1.0), (100, 0.1), (10, 0.1), (0, 0.1), (-5, 0.1)],
)
def test_cadence_clamped_to_floor(ms, expected):
    assert FE.fast_trigger_seconds(ms) == pytest.approx(expected)


def test_cadence_garbage_falls_back_to_floor():
    assert FE.fast_trigger_seconds("nope") == pytest.approx(0.1)  # type: ignore[arg-type]


def test_cadence_defaults_to_config(monkeypatch):
    monkeypatch.setattr(config, "FAST_TRIGGER_MS", 500)
    assert FE.fast_trigger_seconds() == pytest.approx(0.5)


def test_event_min_gap_has_20ms_floor(monkeypatch):
    monkeypatch.setattr(config, "FAST_TRIGGER_MS", 100)
    assert FE.event_min_gap_seconds() == pytest.approx(0.025)
    monkeypatch.setattr(config, "FAST_TRIGGER_MS", 40)  # clamps to 100ms
    assert FE.event_min_gap_seconds() == pytest.approx(0.025)


# --------------------------------------------------------------------------- window guard

def test_candidate_sleeves_in_window():
    got = P.fast_entry_candidate_sleeves(_portfolio(), _NOW_IN_WINDOW)
    assert [off for _s, off in got] == [285]


def test_candidate_sleeves_empty_off_checkpoint():
    assert P.fast_entry_candidate_sleeves(_portfolio(), _NOW_OFF_WINDOW) == []


def test_candidate_sleeves_empty_for_non_checkpoint_sleeve():
    baseline = _sleeve("baseline")
    baseline.checkpoint_offset_set = ()
    assert P.fast_entry_candidate_sleeves(_portfolio(baseline), _NOW_IN_WINDOW) == []


def test_candidate_sleeves_respects_kill_switch(monkeypatch):
    monkeypatch.setattr(config, "SUBMINUTE_KILL", True)
    assert P.fast_entry_candidate_sleeves(_portfolio(), _NOW_IN_WINDOW) == []


def test_candidate_sleeves_respects_master_flag(monkeypatch):
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", False)
    assert P.fast_entry_candidate_sleeves(_portfolio(), _NOW_IN_WINDOW) == []


def test_candidate_sleeves_respects_interval_disable(monkeypatch):
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", False)
    assert P.fast_entry_candidate_sleeves(_portfolio(), _NOW_IN_WINDOW) == []


def test_candidate_sleeves_respects_schedule_filter():
    s = _sleeve()
    s.dow_set = "nonsense"  # any unknown value is a hard schedule_filter
    assert P.fast_entry_candidate_sleeves(_portfolio(s), _NOW_IN_WINDOW) == []


# --------------------------------------------------------------------------- cheap return

def test_scan_out_of_window_touches_nothing():
    """The cadence rests on this: outside a qualifying offset the scan must
    return before the spot fetch, the score, or any DB access."""
    counters = _scan(conn=_Boom("conn"), now_ts=_NOW_OFF_WINDOW)
    assert counters["cells"] == 0
    assert counters["scored"] == 0


def test_scan_without_book_source_returns_early():
    # No push/cache source => nothing to read without a REST call the fast rung
    # is not allowed to make.
    counters = _scan(conn=_Boom("conn"), book_prefetch=None)
    assert counters == {
        "cells": 0, "scored": 0, "bettable": 0, "committed": 0,
        "orders": 0, "contended": 0, "dd_halted": 0, "spot_cold": 0,
    }


# --------------------------------------------------------------------------- same-offset dedup

def _seed_attempted(sleeve, *sides, offset=285, coin="BTC"):
    dec = P._v2_decider_for(sleeve)
    for side in sides:
        key = (coin, sleeve.interval, _WINDOW_START, side)
        dec._attempted_offsets.setdefault(key, set()).add(offset)
    return dec


def test_offset_spent_needs_both_sides():
    s = _sleeve()
    dec = _seed_attempted(s, "YES")
    assert P._fast_offset_spent_for_all_sides(
        dec, "BTC", "5m", _WINDOW_START, 285
    ) is False
    _seed_attempted(s, "NO")
    assert P._fast_offset_spent_for_all_sides(
        dec, "BTC", "5m", _WINDOW_START, 285
    ) is True


def test_offset_spent_is_per_offset_and_per_window():
    s = _sleeve()
    dec = _seed_attempted(s, "YES", "NO", offset=285)
    # A different second of the same window is still open...
    assert P._fast_offset_spent_for_all_sides(
        dec, "BTC", "5m", _WINDOW_START, 286
    ) is False
    # ...and so is the same second of the next window.
    assert P._fast_offset_spent_for_all_sides(
        dec, "BTC", "5m", _WINDOW_START + 300, 285
    ) is False


def test_scan_skips_offset_the_tick_already_fired():
    """The tick's own band-passing look at second S adds S to the decider's
    per-side set; the sub-second fires that follow must not re-score it."""
    s = _sleeve()
    _seed_attempted(s, "YES", "NO")
    counters = _scan(_portfolio(s), conn=_Boom("conn"))
    assert counters["cells"] == 1      # the cell was considered...
    assert counters["scored"] == 0     # ...and dropped before any spot/score work


def test_scan_still_runs_when_only_one_side_fired(monkeypatch):
    s = _sleeve()
    _seed_attempted(s, "YES")
    seen: list = []
    monkeypatch.setattr(
        P, "_score_cell_for",
        lambda **kw: seen.append(kw) or SimpleNamespace(skip_reason="stop_here"),
    )
    counters = _scan(_portfolio(s), peek_spots=lambda **kw: _spots())
    assert counters["scored"] == 1
    assert len(seen) == 1


def test_scan_drops_the_chosen_side_before_taking_the_lock(monkeypatch):
    """Post-rescore dedup: only ONE side matters once the rescore has picked
    one, so a cell whose own side already fired this integer offset is dropped
    BEFORE the shared lock — the lock is the tick's too, and a fire that can only
    reach a guaranteed SKIP must never block it."""
    s = _sleeve()
    _seed_attempted(s, "YES")          # NO still open => stage 2 lets it through
    _stub_commit_stage(monkeypatch, _Boom("_commit_cell"))
    counters = _scan(_portfolio(s), peek_spots=lambda **kw: _spots())
    assert counters["scored"] == 1      # it WAS scored and rescored...
    assert counters["bettable"] == 0    # ...then dropped on the side's own dedup
    assert counters["contended"] == 0   # never even reached the lock


def test_scan_skips_a_cold_market_rather_than_paying_a_gamma_get(monkeypatch):
    """Cache-only for market metadata: a cold window is skipped, so the fast
    rungs can never multiply the tick's retry load on a failing endpoint."""
    monkeypatch.setattr(P, "_venue_market_cache_has", lambda *a, **k: False)
    monkeypatch.setattr(P, "_score_cell_for", _Boom("score_cell"))
    monkeypatch.setattr(P, "_resolve_net_cells", _Boom("_resolve_net_cells"))
    counters = _scan(peek_spots=lambda **kw: _spots())
    assert counters["cells"] == 1
    assert counters["scored"] == 0


def test_scan_abstains_on_a_cold_spot_bucket_instead_of_fetching(monkeypatch):
    """PEEK-ONLY spot (regression 2026-07-30). Stage 3 reads `peek_spots`, and a
    cold bucket ABSTAINS — it must never fall back to `fetch_spots`.

    A fetching fast rung both paid ~50-100ms inline at 4Hz AND evicted the
    SpotPrefetcher's next-bucket entry out of what was then a single-slot
    adapter cache, taking the 1Hz tick's own p50 from 115ms to 315ms with
    stale_spot skips x6.9. `_deps()` leaves `fetch_spots=_Boom(...)`, so any
    reintroduced fallback fails here loudly.
    """
    monkeypatch.setattr(P, "_score_cell_for", _Boom("score_cell"))
    monkeypatch.setattr(P, "_resolve_net_cells", _Boom("_resolve_net_cells"))
    counters = _scan(peek_spots=lambda **kw: None)
    assert counters["cells"] == 1
    assert counters["scored"] == 0
    assert counters["orders"] == 0
    assert counters["spot_cold"] == 1


def test_scan_without_a_peek_reader_wired_cannot_read_spot_at_all():
    """`peek_spots=None` (a bundle with no cache-only reader) abstains rather
    than reaching for the tick's fetch-on-miss `fetch_spots`."""
    counters = _scan(peek_spots=None)
    assert counters["cells"] == 1
    assert counters["scored"] == 0


def test_scan_scores_at_the_integer_offset(monkeypatch):
    """Checkpoint bundles are keyed by INTEGER offset_sec — a mid-second fire
    must score second S, not a fraction of it."""
    s = _sleeve()
    s.signal_source = "hgb"
    captured: dict = {}
    monkeypatch.setattr(
        P, "_score_cell_for",
        lambda **kw: captured.update(kw) or SimpleNamespace(skip_reason="stop"),
    )
    _scan(_portfolio(s), peek_spots=lambda **kw: _spots(), src="fast_event")
    assert captured["offset_sec"] == 285
    assert captured["entry_minute"] == 4
    assert captured["window_start_ts"] == _WINDOW_START


def test_scan_drops_cells_the_rescore_left_unbettable(monkeypatch):
    """No signals rows at 4Hz: a non-bettable cell is dropped, not recorded."""
    s = _sleeve()
    monkeypatch.setattr(
        P, "_score_cell_for", lambda **kw: SimpleNamespace(skip_reason=None, side="YES")
    )
    monkeypatch.setattr(
        P, "_resolve_net_cells",
        lambda cells, **kw: [
            SimpleNamespace(score=SimpleNamespace(skip_reason="out_of_band"))
        ],
    )
    monkeypatch.setattr(P, "_commit_cell", _Boom("_commit_cell"))
    counters = _scan(_portfolio(s), peek_spots=lambda **kw: _spots())
    assert counters["bettable"] == 0
    assert counters["committed"] == 0


# --------------------------------------------------------------------------- commit stage

def _stub_commit_stage(monkeypatch, commit, *, wallet_snaps=None):
    """Neutralize every DB read the commit stage makes, then install ``commit``."""
    monkeypatch.setattr(
        P, "_score_cell_for", lambda **kw: SimpleNamespace(skip_reason=None, side="YES")
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
        P, "_load_live_context", lambda *a, **k: ({}, None, {}, None, True)
    )
    monkeypatch.setattr(
        P, "get_wallet_snapshots", lambda *a, **k: dict(wallet_snaps or {})
    )
    monkeypatch.setattr(P, "_live_pending_exposure_usd", lambda *a, **k: 0.0)
    monkeypatch.setattr(P, "_live_cap_basis_usd", lambda *a, **k: None)
    monkeypatch.setattr(P, "_live_wallet_skip_reason", lambda **k: None)
    monkeypatch.setattr(
        P, "evaluate_breaker", lambda conn: SimpleNamespace(tripped=False)
    )
    monkeypatch.setattr(P, "_commit_cell", commit)


def test_scan_stamps_the_trigger_src_on_the_committed_cell(monkeypatch):
    """Provenance rides on the CELL, not a thread-local: under
    PHASE_B_PARALLEL_POST the fill row is written by the POST flush, which need
    not be the thread that decided the cell."""
    srcs: list[str] = []
    _stub_commit_stage(monkeypatch, lambda conn, d, *a, **k: srcs.append(d.trigger_src))
    counters = _scan(peek_spots=lambda **kw: _spots(), src="fast_event")
    assert counters["committed"] == 1
    assert srcs == ["fast_event"]


def test_scan_releases_the_lock_after_commit(monkeypatch):
    _stub_commit_stage(monkeypatch, lambda *a, **k: None)
    _scan(peek_spots=lambda **kw: _spots())
    assert P._FAST_ENTRY_LOCK.locked() is False


def test_scan_releases_the_lock_when_a_cell_raises(monkeypatch):
    def _raise(*a, **k):
        raise RuntimeError("boom")

    _stub_commit_stage(monkeypatch, _raise)
    counters = _scan(peek_spots=lambda **kw: _spots())
    assert counters["committed"] == 0
    assert P._FAST_ENTRY_LOCK.locked() is False


def test_scan_abandons_when_single_flight_is_contended(monkeypatch):
    """A skipped scan is free; a double order is not. Never queue behind the
    tick — the next fire is 250ms away."""
    _stub_commit_stage(monkeypatch, _Boom("_commit_cell"))
    P._FAST_ENTRY_LOCK.acquire()
    try:
        counters = _scan(peek_spots=lambda **kw: _spots())
    finally:
        P._FAST_ENTRY_LOCK.release()
    assert counters["contended"] == 1
    assert counters["committed"] == 0


# --------------------------------------------------------------------------- gate parity

def test_scan_refuses_to_place_for_a_drawdown_halted_wallet(monkeypatch):
    """P0 gate parity. The tick's sleeve loop refuses a wallet past the HARD
    drawdown halt; the fast rung's cheap in-window predicate cannot see that (it
    holds no wallet snapshot), so the gate is re-run against the snapshot the
    commit stage loads. Without it the fast rung places while HALTED."""
    halted = SimpleNamespace(drawdown_pct_peak_frac=0.99)
    monkeypatch.setattr(P, "_wallet_drawdown_halted", lambda pf, snap: True)
    _stub_commit_stage(
        monkeypatch, _Boom("_commit_cell"), wallet_snaps={"fe_5m": halted}
    )
    counters = _scan(peek_spots=lambda **kw: _spots())
    assert counters["bettable"] == 1     # the rescore did leave it bettable...
    assert counters["dd_halted"] == 1    # ...and the halt gate dropped it
    assert counters["committed"] == 0
    assert P._FAST_ENTRY_LOCK.locked() is False


def test_scan_places_for_a_wallet_that_is_not_halted(monkeypatch):
    """The other half: the halt gate must not veto a healthy wallet."""
    ok = SimpleNamespace(drawdown_pct_peak_frac=0.0)
    monkeypatch.setattr(P, "_wallet_drawdown_halted", lambda pf, snap: False)
    _stub_commit_stage(monkeypatch, lambda *a, **k: None, wallet_snaps={"fe_5m": ok})
    counters = _scan(peek_spots=lambda **kw: _spots())
    assert (counters["dd_halted"], counters["committed"]) == (0, 1)


@pytest.mark.parametrize(
    "gate,setup",
    [
        ("interval_disabled", lambda mp, s: mp.setattr(config, "INTERVAL_5M_ENABLED", False)),
        ("subminute_disabled", lambda mp, s: mp.setattr(config, "SUBMINUTE_KILL", True)),
        ("schedule_filter", lambda mp, s: setattr(s, "dow_set", "nonsense")),
    ],
)
def test_the_shared_gate_names_every_blocker(monkeypatch, gate, setup):
    """One gate helper, one reason string — the tick writes it onto its skip row
    and the fast predicate reads it as a boolean."""
    s = _sleeve()
    setup(monkeypatch, s)
    assert P._sleeve_entry_gate(s, _NOW_IN_WINDOW) == gate


def test_the_shared_gate_passes_a_qualifying_second():
    assert P._sleeve_entry_gate(_sleeve(), _NOW_IN_WINDOW) is None
    assert P._sleeve_entry_gate(_sleeve(), _NOW_OFF_WINDOW) == "not_qualifying_offset"


# --------------------------------------------------------------------------- single-flight wrapper

def test_phase_b_takes_no_lock_when_flag_off(monkeypatch):
    """Flag OFF must be byte-identical: the tick pays nothing, so even a HELD
    lock cannot stall its Phase B."""
    monkeypatch.setattr(config, "FAST_TRIGGER_ENABLED", False)
    P._FAST_ENTRY_LOCK.acquire()
    try:
        with P._phase_b_single_flight():
            pass          # would deadlock if the flag-off path took the lock
    finally:
        P._FAST_ENTRY_LOCK.release()


def test_phase_b_holds_the_lock_when_flag_on():
    with P._phase_b_single_flight():
        assert P._FAST_ENTRY_LOCK.locked() is True
    assert P._FAST_ENTRY_LOCK.locked() is False


def test_phase_b_blocks_until_a_fast_scan_releases():
    """The tick is never SKIPPED — it waits (bounded by one Phase B, well inside
    TICK_HARD_DEADLINE_MS)."""
    done = threading.Event()

    def _tick_phase_b():
        with P._phase_b_single_flight():
            done.set()

    P._FAST_ENTRY_LOCK.acquire()
    t = threading.Thread(target=_tick_phase_b, daemon=True)
    t.start()
    assert done.wait(timeout=0.2) is False   # blocked on the held lock
    P._FAST_ENTRY_LOCK.release()
    assert done.wait(timeout=5.0) is True
    t.join(timeout=5.0)


# --------------------------------------------------------------------------- trigger_src column

def test_trigger_src_defaults_to_tick(db: sqlite3.Connection):
    record_subminute_fill(
        db, coin="BTC", sleeve="s", offset_sec=285,
        window_start_ts=_WINDOW_START, side="YES", requested_usd=5.0,
        quote_ask=0.7, limit_price=0.71, filled_size=None, filled_price=None,
        filled_usd=None, order_id="o1", client_order_id="c1",
        outcome="accepted",
    )
    row = db.execute(
        "SELECT trigger_src FROM subminute_fills WHERE client_order_id='c1'"
    ).fetchone()
    assert row[0] == "tick"


@pytest.mark.parametrize("src", ["fast_sweep", "fast_event"])
def test_trigger_src_stamped_on_a_fast_rung_fill(db: sqlite3.Connection, src):
    record_subminute_fill(
        db, coin="BTC", sleeve="s", offset_sec=285,
        window_start_ts=_WINDOW_START, side="YES", requested_usd=5.0,
        quote_ask=0.7, limit_price=0.71, filled_size=None,
        filled_price=None, filled_usd=None, order_id="o2",
        client_order_id="c2", outcome="rejected",
        trigger_src=src,
    )
    row = db.execute(
        "SELECT trigger_src, outcome FROM subminute_fills "
        "WHERE client_order_id='c2'"
    ).fetchone()
    # The forensics classifier keys on outcome='rejected' + the
    # client_order_id join, so a fast reject stays classifiable.
    assert (row[0], row[1]) == (src, "rejected")


def test_cell_decision_defaults_to_the_tick_rung():
    """Every non-fast caller keeps writing 'tick' with no signature churn."""
    assert P._NetCell(
        sleeve=_sleeve(), entry_minute=4, window_start=_WINDOW_START,
        window_end=_WINDOW_START + 300, coin="BTC", spot=None, spot_age_s=0.0,
        score=None,
    ).trigger_src == "tick"


# --------------------------------------------------------------------------- scheduler wiring

def test_no_sweep_job_when_not_wired(start_scheduler_jobs):
    assert "fast_trigger_sweep" not in start_scheduler_jobs()


def test_no_sweep_job_at_zero_cadence(start_scheduler_jobs):
    assert "fast_trigger_sweep" not in start_scheduler_jobs(
        fast_trigger_job=lambda: None, fast_trigger_seconds=0.0
    )


def test_sweep_job_registered_at_positive_cadence(start_scheduler_jobs):
    assert "fast_trigger_sweep" in start_scheduler_jobs(
        fast_trigger_job=lambda: None, fast_trigger_seconds=0.25
    )


# --------------------------------------------------------------------------- event rung

def _trigger(**over):
    kw = dict(
        conn_factory=lambda: object(),
        portfolio=_portfolio(),
        deps_factory=lambda: _deps(),
        book_source=SimpleNamespace(noted_tokens=lambda: ("YES-BTC",)),
        now_fn=lambda: float(_NOW_IN_WINDOW),
    )
    kw.update(over)
    return FE.FastEntryTrigger(**kw)


@pytest.mark.parametrize(
    "universe,in_window,token,wakes,why",
    [
        # A push for a token outside the current entry universe is not ours.
        (frozenset({"YES-BTC"}), True, "OTHER", False, "off-universe"),
        # Outside a qualifying offset second there is nothing to react to.
        (frozenset({"YES-BTC"}), False, "YES-BTC", False, "off-window"),
        (frozenset({"YES-BTC"}), True, "YES-BTC", True, "ours, in window"),
        # An EMPTY noted set means "unknown", not "nothing qualifies" — the
        # window gate still bounds it and the worker re-derives the real gate.
        (frozenset(), True, "ANY", True, "unknown universe does not filter"),
    ],
)
def test_ask_update_gates(universe, in_window, token, wakes, why):
    tr = _trigger()
    tr._universe = universe
    tr._in_window = in_window
    tr.on_ask_update(token, 1.0)
    assert tr._wake.is_set() is wakes, why
    assert tr.events_seen == 1
    assert tr.events_dropped == (0 if wakes else 1)


def test_sweep_refreshes_window_flag_and_universe(monkeypatch):
    seen: list[dict] = []

    def _fake_scan(*a, **k):
        seen.append(k)
        return {"cells": 1}

    monkeypatch.setattr(FE, "run_fast_entry_scan", _fake_scan)
    tr = _trigger()
    assert tr.run_sweep() == {"cells": 1}
    assert tr._in_window is True
    assert tr._universe == frozenset({"YES-BTC"})
    assert [k["trigger_src"] for k in seen] == ["fast_sweep"]
    # The guard already resolved the candidate list — the scan must not pay for
    # it a second time.
    assert [s for s, _off in seen[0]["candidates"]] == list(tr.portfolio.sleeves)


def test_sweep_skips_the_universe_read_when_the_event_rung_is_off(monkeypatch):
    """The noted-token set exists ONLY to filter WS pushes, so with the event
    rung disabled reading it 4x/s is pure cost."""
    monkeypatch.setattr(config, "FAST_TRIGGER_EVENT_ENABLED", False)
    monkeypatch.setattr(FE, "run_fast_entry_scan", lambda *a, **k: {})
    tr = _trigger(book_source=SimpleNamespace(noted_tokens=_Boom("noted_tokens")))
    tr.run_sweep()
    assert tr._universe == frozenset()


def test_sweep_rebuilds_the_universe_only_when_the_noted_tuple_changes(monkeypatch):
    """The tick re-notes the SAME tuple almost every second; rebuilding the
    frozenset off an unchanged tuple is work for nothing."""
    monkeypatch.setattr(FE, "run_fast_entry_scan", lambda *a, **k: {})
    noted = ("YES-BTC", "NO-BTC")
    tr = _trigger(book_source=SimpleNamespace(noted_tokens=lambda: noted))
    tr.run_sweep()
    first = tr._universe
    tr.run_sweep()
    assert tr._universe is first          # same tuple -> same frozenset object
    noted = ("YES-ETH",)                  # a fresh tuple DOES rebuild
    tr.run_sweep()
    assert tr._universe == frozenset({"YES-ETH"})


def test_sweep_is_free_outside_the_window(monkeypatch):
    monkeypatch.setattr(FE, "run_fast_entry_scan", _Boom("run_fast_entry_scan"))
    tr = _trigger(now_fn=lambda: float(_NOW_OFF_WINDOW))
    assert tr.run_sweep() == {}
    assert tr._in_window is False
    assert tr.sweep_scans == 0


def test_sweep_swallows_errors(monkeypatch):
    def _raise(*a, **k):
        raise RuntimeError("venue down")

    monkeypatch.setattr(FE, "run_fast_entry_scan", _raise)
    assert _trigger().run_sweep() == {}


def test_event_worker_runs_a_scan_on_a_push(monkeypatch):
    ran = threading.Event()
    srcs: list[str] = []

    def _fake_scan(*a, **k):
        srcs.append(k["trigger_src"])
        ran.set()
        return {}

    monkeypatch.setattr(FE, "run_fast_entry_scan", _fake_scan)
    monkeypatch.setattr(config, "FAST_TRIGGER_MS", 100)
    tr = _trigger()
    tr._universe = frozenset({"YES-BTC"})
    tr._in_window = True
    tr.start_event_worker()
    try:
        tr.on_ask_update("YES-BTC", 1.0)
        assert ran.wait(timeout=5.0) is True
        assert srcs == ["fast_event"]
    finally:
        tr.stop()


def test_stop_joins_the_event_worker():
    tr = _trigger()
    tr.start_event_worker()
    worker = tr._worker
    assert worker is not None and worker.is_alive()
    tr.stop()
    assert worker.is_alive() is False
    assert tr._worker is None


def test_start_event_worker_is_idempotent():
    tr = _trigger()
    tr.start_event_worker()
    first = tr._worker
    tr.start_event_worker()
    try:
        assert tr._worker is first
    finally:
        tr.stop()


# --------------------------------------------------------------------------- hook registration
#
# The chain is the ONLY reachable path to a mirror's ask-update hook: it defines
# no __getattr__, so once the sources are wrapped their own methods are invisible
# to the tick path. Registration therefore fans out over the sources, exactly
# like note_tokens / start / stop.


def _chain(*sources):
    from cryptobot.scheduler.book_prefetch import BookSourceChain

    return BookSourceChain(list(sources))


def test_chain_registers_the_hook_on_the_sources_that_have_one():
    got: list = []
    mirror = SimpleNamespace(
        set_ask_update_callback=got.append,
        note_tokens=lambda _t: None, get=lambda t, *, now_ts: None,
    )
    plain = SimpleNamespace(note_tokens=lambda _t: None, get=lambda t, *, now_ts: None)
    assert _chain(mirror, plain).set_ask_update_callback("cb") == 1
    assert got == ["cb"]


def test_chain_with_no_hooked_source_reports_zero_rather_than_failing():
    # predict.fun's market-keyed mirror has no hook yet: the sweep rung must
    # still arm rather than failing the boot.
    plain = SimpleNamespace(note_tokens=lambda _t: None, get=lambda t, *, now_ts: None)
    assert _chain(plain).set_ask_update_callback("cb") == 0
    assert _chain().set_ask_update_callback("cb") == 0


def test_chain_swallows_a_raising_setter():
    def _raise(_cb):
        raise RuntimeError("nope")

    mirror = SimpleNamespace(
        set_ask_update_callback=_raise,
        note_tokens=lambda _t: None, get=lambda t, *, now_ts: None,
    )
    assert _chain(mirror).set_ask_update_callback("cb") == 0
