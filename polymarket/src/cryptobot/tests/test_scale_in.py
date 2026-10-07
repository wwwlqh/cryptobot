"""T3 scale-in taker top-up tests (research WFO pick 2026-07-15).

Covers the flag-gated per-second scale-in that rides inside the held-position
loop (execution/scale_in.py, driven from execution/held_position_pass.py):

  * no-dip disarm (permanent — never re-arms after the first dip);
  * crossed-book observation skip;
  * walked-ask cap (ii), spread gate (iii);
  * max-N fills (iv), 1.5x exposure budget (v);
  * 0.5x sizing with the $1 floor, DD-halt + available-cash gates (vi);
  * unanchored +0.01 pad (own walk, decoupled from V2_FAK_PAD);
  * distinguishable is_topup=1 + scalein_topup=1 rows (one-bet-per-market
    exemption);
  * the model-deterioration predicate permanently disarming further top-ups;
  * flag-off inertness (no writes at all).

Hermetic: in-memory SQLite via ``tmp_db``, fake OrderClient, and (for the
run_held_position_pass integration cases) ``score_side_hgb`` monkeypatched.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot.execution import scale_in, held_position_pass
from cryptobot.execution.ledger import BetSignal, record_bet
from cryptobot.execution.types import OrderResult

WINDOW_START = 1_715_923_200  # divisible by 300 (5m boundary)
assert WINDOW_START % 300 == 0
NOW_TS = WINDOW_START + 285  # offset 285, inside the last-30s window, > end-buffer
WINDOW_END = WINDOW_START + 300


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
def _make_book(token_id, *, top_ask=None, top_bid=None, ask_size=1000.0):
    asks = () if top_ask is None else ({"price": float(top_ask), "size": ask_size},)
    bids = () if top_bid is None else (
        {"price": float(top_bid) - 0.01, "size": 1000.0},
        {"price": float(top_bid), "size": 1000.0},
    )
    return BookSnapshot(
        token_id=token_id, asks=asks, bids=bids, fetched_at_ts=float(NOW_TS),
        total_ask_depth_usd=float(ask_size),
    )


class FakeClient:
    """Records place_gtc_limit_order (FAK top-up BUY) calls; returns scripted
    OrderResults.

    Deliberately has NO sell entry point: the stop-loss SELL was retired
    2026-08-05, so any code path still trying to sell raises AttributeError
    here instead of passing silently."""

    def __init__(self, *, buy_fills=True):
        self.buy_fills = buy_fills
        self.buy_calls: list = []
        self._n = 0

    def place_gtc_limit_order(self, signal, *, client_order_id):
        self._n += 1
        self.buy_calls.append(
            SimpleNamespace(
                bet_size=float(signal.bet_size),
                limit_price=float(signal.limit_price),
                side=signal.side,
                token_id=signal.poly_token_id,
                slot_filled_pre=float(signal.slot_filled_pre),
                client_order_id=client_order_id,
            )
        )
        if not self.buy_fills:
            return OrderResult(
                success=True, order_id=f"buy-{self._n}",
                client_order_id=client_order_id, limit_price=float(signal.limit_price),
            )
        shares = round(float(signal.bet_size) / float(signal.limit_price), 4)
        return OrderResult(
            success=True, order_id=f"buy-{self._n}",
            client_order_id=client_order_id, limit_price=float(signal.limit_price),
            filled_size_tokens=shares, filled_price_vwap=float(signal.limit_price),
            filled_value_usd=shares * float(signal.limit_price),
            fill_ts="2026-07-16T00:00:00Z",
        )


def _deps(order_client, *, held_ask, held_bid, other_ask=0.50):
    yes_book = _make_book("Y", top_ask=held_ask, top_bid=held_bid)
    no_book = _make_book("N", top_ask=other_ask, top_bid=0.45)
    books = {"Y": yes_book, "N": no_book}

    def fetch_market(coin, *, interval_min, window_start_ts, conn, now_ts):
        return SimpleNamespace(
            yes_token_id="Y", no_token_id="N", condition_id="c", fee_rate_bps=0.0,
        )

    def fetch_book(token_id, *, now_ts):
        return books.get(token_id)

    return SimpleNamespace(
        order_client=order_client, notify=None, fetch_market=fetch_market,
        fetch_book=fetch_book, book_prefetch=None, partial_ret_engine=object(),
        checkpoint_rf_artifact=object(), checkpoint_hgb_artifact=object(),
        twap_mirror=None, twap_hgb_artifact=object(),
    )


def _portfolio():
    return SimpleNamespace(
        sleeves=[SimpleNamespace(label="subminute_5m", interval="5m", interval_min=5)],
        market_sleeves=[SimpleNamespace(label="subminute_5m")],
        max_drawdown_halt_frac=0.40,
        wallet_group_for=lambda w: "group_live",
        wallet_groups=(),
    )


def _insert_entry(
    conn, *, entry_fill=0.70, ask0=0.70, size_tokens=5.0, bet_size=None,
    side="YES", token="Y", window_open_spot=None,
) -> int:
    bs = bet_size if bet_size is not None else size_tokens * entry_fill
    sig = BetSignal(
        coin="BTC", sleeve="subminute_5m", entry_minute=4, side=side,
        window_start_ts=WINDOW_START, window_end_ts=WINDOW_END,
        interval_min=5, poly_market_id="c", poly_token_id=token,
        fee_rate_bps=0.0, model_p=0.75, p_market=ask0,
        fill_price=ask0, walked_vwap=ask0, realized_edge=0.05,
        partial_ret_z_1h=0.0, lut_p_bucket=None, lut_hit_rate=None,
        lut_n_cum=None, target_usd=bs, bet_size=bs,
        slot_filled_pre=0.0, entry_top_price=ask0, yes_pre1_anchor=ask0,
        offset_sec=280, config_sha256="x", window_open_spot=window_open_spot,
    )
    rid = record_bet(
        conn, sig, mode="LIVE", wallet_group="group_live",
        live_order_id="e1", live_client_order_id="c1",
        live_filled_size=size_tokens, live_filled_price=entry_fill,
        live_fill_ts="2026-07-16T00:00:00Z",
    )
    conn.commit()
    return rid


def _insert_topup_sibling(conn, *, filled_size, filled_price, coid, oid):
    """Seed a filled T3 top-up row sharing the entry's slot (for count/exposure)."""
    sig = BetSignal(
        coin="BTC", sleeve="subminute_5m", entry_minute=4, side="YES",
        window_start_ts=WINDOW_START, window_end_ts=WINDOW_END,
        interval_min=5, poly_market_id="c", poly_token_id="Y",
        fee_rate_bps=0.0, model_p=0.75, p_market=0.72,
        fill_price=0.72, walked_vwap=0.72, realized_edge=0.05,
        partial_ret_z_1h=None, lut_p_bucket=None, lut_hit_rate=None,
        lut_n_cum=None, target_usd=filled_size * filled_price,
        bet_size=filled_size * filled_price,
        slot_filled_pre=3.5, entry_top_price=0.70, yes_pre1_anchor=0.70,
        wallet_id="subminute_5m", offset_sec=283, config_sha256="x",
    )
    rid = record_bet(
        conn, sig, is_topup=True, scalein_topup=True, mode="LIVE",
        wallet_group="group_live", live_order_id=oid, live_client_order_id=coid,
        live_filled_size=filled_size, live_filled_price=filled_price,
        live_fill_ts="2026-07-16T00:00:00Z",
    )
    conn.commit()
    return rid


def _entry_row(conn, rid):
    return conn.execute("SELECT id FROM ledger WHERE id=?", (rid,)).fetchone()


_UNSET = object()


def _scan(
    conn, rid, held_book, *, bid, held_ask, now_ts=NOW_TS,
    deps=None, wallet_snaps=_UNSET, budget=None, triggered=False,
):
    if wallet_snaps is _UNSET:
        wallet_snaps = {"subminute_5m": SimpleNamespace(drawdown_pct_peak_frac=0.0)}
    if budget is None:
        budget = [None]
    return scale_in.scan_topup(
        conn, _entry_row(conn, rid), held_book, bid=bid, held_ask=held_ask,
        now_ts=now_ts, deps=deps, wallet_snaps=wallet_snaps, budget=budget,
        triggered=triggered,
    )


@pytest.fixture(autouse=True)
def _t3_defaults(monkeypatch):
    monkeypatch.setattr(config, "SCALEIN_T3_ENABLED", True)
    monkeypatch.setattr(config, "SCALEIN_T3_MAX_N", 3)
    monkeypatch.setattr(config, "SCALEIN_T3_FRAC", 0.5)
    monkeypatch.setattr(config, "SCALEIN_T3_NO_DIP", 0.0)
    monkeypatch.setattr(config, "SCALEIN_T3_CAP", 0.83)
    monkeypatch.setattr(config, "SCALEIN_T3_SPREAD_MAX", 0.02)
    monkeypatch.setattr(config, "SCALEIN_T3_EXPOSURE_CAP", 1.5)
    monkeypatch.setattr(config, "SCALEIN_T3_CADENCE_S", 1)
    monkeypatch.setattr(config, "SCALEIN_T3_FAK_PAD", 0.01)
    monkeypatch.setattr(config, "SCALEIN_T3_END_BUFFER_S", 4)
    monkeypatch.setattr(config, "MAX_DRAWDOWN_HALT_FRAC", 0.40)
    monkeypatch.setattr(config, "MIN_BET_USD", 1.0)


def _state(conn, rid):
    return conn.execute(
        "SELECT ask0, running_min_ask, disarmed, last_attempt_ts "
        "FROM scalein_state WHERE ledger_id=?", (rid,),
    ).fetchone()


def _topup_rows(conn):
    return conn.execute(
        "SELECT id, side, coin, window_start_ts, is_topup, scalein_topup, "
        "bet_size, live_filled_size, live_filled_price, offset_sec "
        "FROM ledger WHERE scalein_topup=1 ORDER BY id",
    ).fetchall()


# ---------------------------------------------------------------------------
# no-dip disarm (permanent)
# ---------------------------------------------------------------------------
def test_no_dip_disarm_is_permanent(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.65, held_bid=0.64)
    # Tick 1: ask 0.65 < ask0 0.70 → dip → permanent disarm, no top-up.
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.65, top_bid=0.64),
          bid=0.64, held_ask=0.65, deps=deps)
    assert client.buy_calls == []
    assert _state(tmp_db, rid)["disarmed"] == 1
    # Tick 2: ask recovered to 0.75 → but disarmed is permanent → still no top-up.
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.75, top_bid=0.74),
          bid=0.74, held_ask=0.75, now_ts=NOW_TS + 1, deps=deps)
    assert client.buy_calls == []
    assert _state(tmp_db, rid)["disarmed"] == 1
    assert _topup_rows(tmp_db) == []


def test_no_dip_within_no_dip_band_stays_armed(tmp_db, monkeypatch):
    # no_dip=0.05: an ask 0.66 (>= 0.70 - 0.05) does NOT disarm.
    monkeypatch.setattr(config, "SCALEIN_T3_NO_DIP", 0.05)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.66, held_bid=0.655)
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.66, top_bid=0.655),
                  bid=0.655, held_ask=0.66, deps=deps)
    assert _state(tmp_db, rid)["disarmed"] == 0
    assert len(client.buy_calls) == 1  # armed + gates pass → filled
    assert spent > 0


# ---------------------------------------------------------------------------
# crossed-book observation skip
# ---------------------------------------------------------------------------
def test_crossed_book_skips_observation_and_placement(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.75)  # bid >= ask → crossed
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.75),
          bid=0.75, held_ask=0.72, deps=deps)
    # Crossed: no observation (running_min stays NULL), no disarm, no placement.
    st = _state(tmp_db, rid)
    assert st["disarmed"] == 0
    assert st["running_min_ask"] is None
    assert client.buy_calls == []
    # A later non-crossed tick then observes + fills.
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
                  bid=0.71, held_ask=0.72, now_ts=NOW_TS + 1, deps=deps)
    assert len(client.buy_calls) == 1
    assert spent > 0


# ---------------------------------------------------------------------------
# ask cap (ii) — uses the WALKED ask for the stake
# ---------------------------------------------------------------------------
def test_walked_cap_rejects_above_cap(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    # ask 0.90 > cap 0.83 (armed since 0.90 >= ask0 0.70) → reject, no buy.
    deps = _deps(client, held_ask=0.90, held_bid=0.89)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.90, top_bid=0.89),
          bid=0.89, held_ask=0.90, deps=deps)
    assert client.buy_calls == []
    assert _state(tmp_db, rid)["disarmed"] == 0  # not a dip — still armed


def test_walked_cap_allows_at_cap(tmp_db, monkeypatch):
    # This test pins gate (ii)'s boundary ONLY. Its 0.80-ask fixture prices
    # the add-on edge negative (entry model_p 0.75), so float the
    # TOPUP_EDGE_FLOOR out of the way — the floor has its own tests below.
    monkeypatch.setattr(config, "TOPUP_EDGE_FLOOR", -1.0)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.80, held_bid=0.79)  # walked 0.80 <= 0.83
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.80, top_bid=0.79),
                  bid=0.79, held_ask=0.80, deps=deps)
    assert len(client.buy_calls) == 1
    assert spent > 0


def test_walked_cap_uses_deep_book(tmp_db):
    # A thin top level forces the walk deeper; the DEEP price (0.90) breaches cap
    # even though the best ask (0.80) is under it.
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.80, held_bid=0.79)
    book = BookSnapshot(
        token_id="Y",
        asks=({"price": 0.80, "size": 0.5}, {"price": 0.90, "size": 100.0}),
        bids=({"price": 0.78, "size": 100.0}, {"price": 0.79, "size": 100.0}),
        fetched_at_ts=float(NOW_TS), total_ask_depth_usd=100.0,
    )
    _scan(tmp_db, rid, book, bid=0.79, held_ask=0.80, deps=deps)
    assert client.buy_calls == []  # walked limit 0.90 > cap 0.83


# ---------------------------------------------------------------------------
# spread gate (iii)
# ---------------------------------------------------------------------------
def test_spread_gate_rejects_wide(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.80, held_bid=0.70)  # spread 0.10 > 0.02
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.80, top_bid=0.70),
          bid=0.70, held_ask=0.80, deps=deps)
    assert client.buy_calls == []


def test_spread_gate_allows_tight(tmp_db, monkeypatch):
    # Pins gate (iii) only; the 0.80-ask fixture's add-on edge is negative
    # (entry model_p 0.75) — float the TOPUP_EDGE_FLOOR out of the way.
    monkeypatch.setattr(config, "TOPUP_EDGE_FLOOR", -1.0)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.80, held_bid=0.785)  # spread 0.015 <= 0.02
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.80, top_bid=0.785),
                  bid=0.785, held_ask=0.80, deps=deps)
    assert len(client.buy_calls) == 1
    assert spent > 0


# ---------------------------------------------------------------------------
# max-N fills (iv)
# ---------------------------------------------------------------------------
def test_max_n_fills_blocks_fourth(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    # Three prior filled top-ups → 4th blocked at max_n=3. Keep each tiny so the
    # exposure cap (1.5 x 3.5 = 5.25) is not the binding rule.
    for i in range(3):
        _insert_topup_sibling(
            tmp_db, filled_size=0.5, filled_price=0.72, coid=f"t{i}", oid=f"o{i}",
        )
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert client.buy_calls == []


# ---------------------------------------------------------------------------
# 1.5x exposure budget (v)
# ---------------------------------------------------------------------------
def test_exposure_cap_blocks_over_budget(tmp_db):
    # entry filled cash = 5*0.70 = 3.5 → budget 1.5x = 5.25. One prior top-up of
    # 4.0 cash + this stake 1.75 = 5.75 > 5.25 → blocked.
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    _insert_topup_sibling(tmp_db, filled_size=5.0, filled_price=0.80, coid="t", oid="o")
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert client.buy_calls == []


def test_exposure_cap_allows_within_budget(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    # prior top-up of 1.0 cash + stake 1.75 = 2.75 <= 5.25 → allowed.
    _insert_topup_sibling(tmp_db, filled_size=1.25, filled_price=0.80, coid="t", oid="o")
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
                  bid=0.71, held_ask=0.72, deps=deps)
    assert len(client.buy_calls) == 1
    assert spent > 0


# ---------------------------------------------------------------------------
# 0.5x sizing + $1 floor (vi)
# ---------------------------------------------------------------------------
def test_stake_is_half_entry_requested(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0, bet_size=3.50)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert len(client.buy_calls) == 1
    assert client.buy_calls[0].bet_size == pytest.approx(1.75)  # 0.5 * 3.50


def test_stake_floors_at_one_dollar(tmp_db):
    # entry requested 1.00 → 0.5x = 0.50 → floored UP to MIN_BET_USD 1.00.
    rid = _insert_entry(
        tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=1.4286, bet_size=1.00,
    )
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert len(client.buy_calls) == 1
    assert client.buy_calls[0].bet_size == pytest.approx(1.00)


def test_stake_cents_floored(tmp_db):
    # entry requested 3.33 → 0.5x = 1.665 → cents-floored DOWN to 1.66.
    rid = _insert_entry(
        tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=4.757, bet_size=3.33,
    )
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert client.buy_calls[0].bet_size == pytest.approx(1.66)


# ---------------------------------------------------------------------------
# DD-halt + available-cash gates (vi)
# ---------------------------------------------------------------------------
def test_dd_halt_blocks(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    snaps = {"subminute_5m": SimpleNamespace(drawdown_pct_peak_frac=0.5)}  # >= 0.40
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps, wallet_snaps=snaps)
    assert client.buy_calls == []


def test_snapshot_unavailable_fails_closed(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps, wallet_snaps=None)
    assert client.buy_calls == []  # no snapshot → DD gate fails closed


def test_available_cash_blocks_when_short(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, bet_size=3.50)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    # stake 1.75 > remaining 1.00 → cash gate blocks.
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps, budget=[1.00])
    assert client.buy_calls == []


def test_available_cash_decremented_on_fill(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, bet_size=3.50)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    budget = [10.0]
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
                  bid=0.71, held_ask=0.72, deps=deps, budget=budget)
    assert spent > 0
    assert budget[0] == pytest.approx(10.0 - spent)


# ---------------------------------------------------------------------------
# unanchored +0.01 pad (decoupled from V2_FAK_PAD)
# ---------------------------------------------------------------------------
def test_pad_is_own_walk_plus_one_cent(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.05)  # entry-scoped pad must be ignored
    # Pins the pad arithmetic only; the 0.80-ask fixture's add-on edge is
    # negative (entry model_p 0.75) — float the TOPUP_EDGE_FLOOR out of the way.
    monkeypatch.setattr(config, "TOPUP_EDGE_FLOOR", -1.0)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.80, held_bid=0.79)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.80, top_bid=0.79),
          bid=0.79, held_ask=0.80, deps=deps)
    assert len(client.buy_calls) == 1
    # walked_limit 0.80 + SCALEIN_T3_FAK_PAD 0.01 = 0.81 (NOT +0.05).
    assert client.buy_calls[0].limit_price == pytest.approx(0.81)


def test_pad_capped_at_99_cents(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "SCALEIN_T3_CAP", 0.99)
    # Pins the 0.99 pad clamp only; the 0.99-ask fixture's add-on edge is deep
    # negative (entry model_p 0.75) — float the TOPUP_EDGE_FLOOR out of the way.
    monkeypatch.setattr(config, "TOPUP_EDGE_FLOOR", -1.0)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.99, held_bid=0.98)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.99, top_bid=0.98),
          bid=0.98, held_ask=0.99, deps=deps)
    assert client.buy_calls[0].limit_price == pytest.approx(0.99)  # min(1.00, 0.99)


# ---------------------------------------------------------------------------
# distinguishable ledger row (one-bet-per-market exemption)
# ---------------------------------------------------------------------------
def test_topup_row_tagged_and_same_slot(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    rows = _topup_rows(tmp_db)
    assert len(rows) == 1
    r = rows[0]
    assert r["is_topup"] == 1 and r["scalein_topup"] == 1
    # Same coin / side / window as the entry (the one-bet-per-market exemption).
    assert r["coin"] == "BTC" and r["side"] == "YES"
    assert r["window_start_ts"] == WINDOW_START
    assert r["live_filled_size"] > 0
    # The entry row is untouched (still its own PENDING position).
    entry = tmp_db.execute(
        "SELECT outcome, scalein_topup FROM ledger WHERE id=?", (rid,)
    ).fetchone()
    assert entry["outcome"] == "PENDING" and entry["scalein_topup"] == 0


def test_topup_edge_cap_blocks_above_ceiling(tmp_db, monkeypatch):
    """Operator order 2026-07-23: T3 top-ups obey V2_EDGE_CAP. Entry model_p
    0.75 @ walked 0.72 => add-on edge ~0.0155; cap 0.01 below it => reject,
    no POST, no top-up row."""
    monkeypatch.setattr(config, "V2_EDGE_CAP", 0.01)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert client.buy_calls == []
    assert _topup_rows(tmp_db) == []


def test_topup_edge_cap_passes_below_ceiling(tmp_db, monkeypatch):
    """Add-on edge ~0.0155 under the armed 0.08 cap => top-up proceeds
    (and cap=0.0 default keeps every other test in this file byte-identical)."""
    monkeypatch.setattr(config, "V2_EDGE_CAP", 0.08)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert len(client.buy_calls) == 1
    assert len(_topup_rows(tmp_db)) == 1


def test_topup_row_records_addon_edge(tmp_db):
    """Forensics fix 2026-07-23: the T3 row's realized_edge is the RE-PRICED
    add-on edge (frozen entry model_p - walked_limit - parabola fee), not the
    entry's edge echoed forward; add_on_realized_edge_minus_initial_edge is
    populated (it was NULL on every live T3 row before this)."""
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    r = tmp_db.execute(
        "SELECT realized_edge, add_on_realized_edge_minus_initial_edge "
        "FROM ledger WHERE scalein_topup=1",
    ).fetchone()
    assert r is not None
    wl = 0.72  # thick book: walked limit == top ask
    expected = 0.75 - wl - config.POLY_FEE_THETA * wl * (1.0 - wl)  # entry model_p 0.75
    assert r["realized_edge"] == pytest.approx(expected)
    # entry realized_edge fixture = 0.05
    assert r["add_on_realized_edge_minus_initial_edge"] == pytest.approx(expected - 0.05)


# ---------------------------------------------------------------------------
# top-up edge floor (config.TOPUP_EDGE_FLOOR — 2026-07-23 live id7)
# ---------------------------------------------------------------------------
def test_topup_edge_floor_default_zero_is_armed():
    # 0.0 is ACTIVE, not off: it blocks exactly the negative-edge cohort.
    assert config.TOPUP_EDGE_FLOOR == 0.0


def test_topup_edge_floor_default_blocks_negative_edge(tmp_db):
    """The id7 class at the DEFAULT floor 0.0: entry model_p 0.75 @ walked
    0.80 => add-on edge ~-0.0615 while cap (0.80 <= 0.83), spread, no-dip and
    every other gate pass => reject, no POST, no top-up row — and NOT a
    disarm (the next second's scan re-prices freely)."""
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.80, held_bid=0.79)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.80, top_bid=0.79),
          bid=0.79, held_ask=0.80, deps=deps)
    assert client.buy_calls == []
    assert _topup_rows(tmp_db) == []
    assert _state(tmp_db, rid)["disarmed"] == 0


def test_topup_edge_floor_exactly_zero_edge_passes(tmp_db, monkeypatch):
    """Strict-< boundary at the DEFAULT floor 0.0: theta pinned 0.0 and a
    walked ask equal to the frozen entry model_p (0.75) make the add-on edge
    EXACTLY 0.0 — 0.0 < 0.0 is False, so the top-up places (and the row
    records the gated 0.0 verbatim)."""
    monkeypatch.setattr(config, "POLY_FEE_THETA", 0.0)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.75, held_bid=0.74)
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.75, top_bid=0.74),
                  bid=0.74, held_ask=0.75, deps=deps)
    assert len(client.buy_calls) == 1
    assert spent > 0
    r = tmp_db.execute(
        "SELECT realized_edge FROM ledger WHERE scalein_topup=1"
    ).fetchone()
    assert r["realized_edge"] == 0.0


def test_topup_edge_floor_armed_blocks_positive_edge_below_it(tmp_db, monkeypatch):
    """A true floor, not a sign gate: walked 0.72 => add-on edge ~+0.0155;
    floor 0.05 sits above it => reject (the SAME fixture places at the 0.0
    default — test_topup_edge_cap_passes_below_ceiling)."""
    monkeypatch.setattr(config, "TOPUP_EDGE_FLOOR", 0.05)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, deps=deps)
    assert client.buy_calls == []
    assert _topup_rows(tmp_db) == []


# ---------------------------------------------------------------------------
# cadence throttle
# ---------------------------------------------------------------------------
def test_cadence_throttles_within_window(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "SCALEIN_T3_CADENCE_S", 3)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    book = _make_book("Y", top_ask=0.72, top_bid=0.71)
    _scan(tmp_db, rid, book, bid=0.71, held_ask=0.72, now_ts=NOW_TS, deps=deps)
    assert len(client.buy_calls) == 1
    # +1s < cadence 3 → throttled.
    _scan(tmp_db, rid, book, bid=0.71, held_ask=0.72, now_ts=NOW_TS + 1, deps=deps)
    assert len(client.buy_calls) == 1
    # +3s >= cadence 3 → allowed.
    _scan(tmp_db, rid, book, bid=0.71, held_ask=0.72, now_ts=NOW_TS + 3, deps=deps)
    assert len(client.buy_calls) == 2


# ---------------------------------------------------------------------------
# end-of-window buffer
# ---------------------------------------------------------------------------
def test_end_buffer_blocks_near_resolution(tmp_db):
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    # WINDOW_END - end_buffer(4) = offset 296. now at 297 (>= 296) → blocked.
    _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
          bid=0.71, held_ask=0.72, now_ts=WINDOW_START + 297, deps=deps)
    assert client.buy_calls == []


# ---------------------------------------------------------------------------
# flag-off inertness
# ---------------------------------------------------------------------------
def test_flag_off_scan_is_inert(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "SCALEIN_T3_ENABLED", False)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    spent = _scan(tmp_db, rid, _make_book("Y", top_ask=0.72, top_bid=0.71),
                  bid=0.71, held_ask=0.72, deps=deps)
    assert spent == 0.0
    assert client.buy_calls == []
    # No scalein_state row written, no top-up row.
    assert _state(tmp_db, rid) is None
    assert _topup_rows(tmp_db) == []


# ---------------------------------------------------------------------------
# record_bet invariant: scalein_topup ⇒ is_topup / slot_filled_pre > 0
# ---------------------------------------------------------------------------
def test_record_bet_scalein_requires_topup(tmp_db):
    sig = BetSignal(
        coin="BTC", sleeve="subminute_5m", entry_minute=4, side="YES",
        window_start_ts=WINDOW_START, window_end_ts=WINDOW_END, interval_min=5,
        poly_market_id="c", poly_token_id="Y", fee_rate_bps=0.0, model_p=0.75,
        p_market=0.70, fill_price=0.70, walked_vwap=0.70, realized_edge=0.05,
        partial_ret_z_1h=None, lut_p_bucket=None, lut_hit_rate=None, lut_n_cum=None,
        target_usd=1.0, bet_size=1.0, slot_filled_pre=0.0, entry_top_price=0.70,
        yes_pre1_anchor=0.70, offset_sec=280, config_sha256="x",
    )
    with pytest.raises(ValueError):
        record_bet(tmp_db, sig, is_topup=False, scalein_topup=True, mode="LIVE")
    # Default scalein_topup=False leaves existing callers writing scalein_topup=0.
    rid = record_bet(tmp_db, sig, mode="LIVE", live_filled_size=1.0, live_filled_price=0.70)
    row = tmp_db.execute("SELECT scalein_topup FROM ledger WHERE id=?", (rid,)).fetchone()
    assert row["scalein_topup"] == 0


# ---------------------------------------------------------------------------
# INTEGRATION: run_held_position_pass — model-deterioration disarm
# ---------------------------------------------------------------------------
#
# The stop-loss EXIT was retired 2026-08-05, but its trigger predicate survives
# as T3's disarm signal: when the re-scored model says the position is
# deteriorating (bid >= model_p + band, underwater), scan_topup is called with
# triggered=True, which permanently disarms further top-ups. Nothing is sold and
# the ledger rows stay PENDING to settlement.
def test_deterioration_disarms_t3_after_topups(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "SCALEIN_T3_DISARM_BAND", 0.04)
    monkeypatch.setattr(held_position_pass, "score_side_hgb", lambda **k: 0.44)

    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0, bet_size=3.50)
    client = FakeClient()
    spots = {"BTC": SimpleNamespace(price=100.0)}
    port = _portfolio()

    # Passes 1 & 2: armed (ask 0.72 >= ask0 0.70, bid 0.71 -> winner, no
    # deterioration) -> one top-up each.
    for i in range(2):
        deps = _deps(client, held_ask=0.72, held_bid=0.71)
        held_position_pass.run_held_position_pass(
            tmp_db, port, now_ts=NOW_TS + i, deps=deps, spots=spots,
        )
    topups = _topup_rows(tmp_db)
    assert len(topups) == 2
    assert _state(tmp_db, rid)["disarmed"] == 0

    # Pass 3: book craters (bid 0.50 < entry 0.70, and >= model 0.44 + 0.04) ->
    # deterioration fires. T3 permanently disarms; NOTHING is sold and every row
    # stays PENDING.
    deps = _deps(client, held_ask=0.55, held_bid=0.50)
    held_position_pass.run_held_position_pass(
        tmp_db, port, now_ts=NOW_TS + 2, deps=deps, spots=spots,
    )
    assert _state(tmp_db, rid)["disarmed"] == 1
    assert len(_topup_rows(tmp_db)) == 2  # no new top-up on the trigger second

    entry = tmp_db.execute(
        "SELECT outcome, exit_kind, exit_price, pnl_net FROM ledger WHERE id=?", (rid,)
    ).fetchone()
    assert entry["outcome"] == "PENDING"
    assert entry["exit_kind"] is None and entry["exit_price"] is None
    for r in _topup_rows(tmp_db):
        row = tmp_db.execute(
            "SELECT outcome, exit_kind FROM ledger WHERE id=?", (r["id"],)
        ).fetchone()
        assert row["outcome"] == "PENDING" and row["exit_kind"] is None

    # Pass 4: even if the book recovers into an arming shape, the disarm holds.
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    held_position_pass.run_held_position_pass(
        tmp_db, port, now_ts=NOW_TS + 3, deps=deps, spots=spots,
    )
    assert len(_topup_rows(tmp_db)) == 2


def test_twap_t3_uses_persisted_open_after_mirror_restart(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "TWAP_SYMMETRIC_ENABLED", True)
    seen = {}

    def _score(**kwargs):
        seen["open_twap"] = kwargs["open_twap"]
        return 0.75

    monkeypatch.setattr(held_position_pass, "score_side_twap", _score)
    rid = _insert_entry(
        tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0,
        window_open_spot=67_123.45,
    )
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    # A fresh mirror has no replay for a pre-restart boundary. The persisted
    # exact open on the entry row must keep held-position rescoring alive.
    deps.twap_mirror = SimpleNamespace(
        get_opening_twap=lambda *args: pytest.fail("mirror must not be queried")
    )

    held_position_pass.run_held_position_pass(
        tmp_db, _portfolio(), now_ts=NOW_TS, deps=deps,
        spots={"BTC": SimpleNamespace(price=67_200.0)},
    )

    assert seen["open_twap"] == pytest.approx(67_123.45)
    assert rid > 0


def test_deterioration_disarms_without_topups(tmp_db, monkeypatch):
    monkeypatch.setattr(held_position_pass, "score_side_hgb", lambda **k: 0.44)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    client = FakeClient()
    # First observed book already dipped (0.55 < 0.70) AND deteriorates -> no
    # top-up ever, position untouched, T3 disarmed.
    deps = _deps(client, held_ask=0.55, held_bid=0.50)
    held_position_pass.run_held_position_pass(
        tmp_db, _portfolio(), now_ts=NOW_TS, deps=deps,
        spots={"BTC": SimpleNamespace(price=100.0)},
    )
    assert client.buy_calls == []
    assert _topup_rows(tmp_db) == []
    row = tmp_db.execute("SELECT outcome FROM ledger WHERE id=?", (rid,)).fetchone()
    assert row["outcome"] == "PENDING"
    assert _state(tmp_db, rid)["disarmed"] == 1


def test_disarmed_position_short_circuits_before_rescore(tmp_db, monkeypatch):
    """topup_gate_open replaces the retired stop_loss_attempts pre-filter: an
    already-disarmed position must not pay for a market/book fetch or an HGB
    re-score (both downstream consumers already no-op on disarmed)."""
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    scale_in.disarm(tmp_db, rid)

    def _blow_up(**_k):
        raise AssertionError("re-score must not run for a disarmed position")

    monkeypatch.setattr(held_position_pass, "score_side_hgb", _blow_up)
    client = FakeClient()
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    held_position_pass.run_held_position_pass(
        tmp_db, _portfolio(), now_ts=NOW_TS, deps=deps,
        spots={"BTC": SimpleNamespace(price=100.0)},
    )
    assert client.buy_calls == []


def test_topup_gate_open_fails_safe_on_query_error(tmp_db):
    """Fail-safe shape inherited from the retired attempts_used gate: a broken
    state query parks the position rather than proceeding blindly."""
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    assert held_position_pass.topup_gate_open(tmp_db, rid) is True

    class _BoomConn:
        def execute(self, *_a, **_k):
            raise sqlite3.OperationalError("no such table: scalein_state")

    assert held_position_pass.topup_gate_open(_BoomConn(), rid) is False


def test_no_dip_observed_when_rescore_bails(tmp_db, monkeypatch):
    """Tick-exactness (both reviewers): a held-side dip on a pass whose model
    re-score bails (model_p None, or the other-side book is missing) must STILL
    permanently disarm T3. The observation rides BEFORE the re-score early
    returns via observe_no_dip; without it live would keep topping up on ticks
    where the backtest cummin disarms (adverse-selected dip-buys)."""
    monkeypatch.setattr(config, "SCALEIN_T3_DISARM_BAND", 0.04)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    client = FakeClient()
    spots = {"BTC": SimpleNamespace(price=100.0)}
    port = _portfolio()

    # Pass 1: held side dips to 0.65 (< ask0 0.70) but the model re-score returns
    # None → _evaluate_position bails BEFORE the scan_topup call. The early
    # observe_no_dip must still fold the dip and permanently disarm.
    monkeypatch.setattr(held_position_pass, "score_side_hgb", lambda **k: None)
    deps = _deps(client, held_ask=0.65, held_bid=0.64)
    held_position_pass.run_held_position_pass(tmp_db, port, now_ts=NOW_TS, deps=deps, spots=spots)
    st = _state(tmp_db, rid)
    assert st is not None and st["disarmed"] == 1  # dip observed despite the bail
    assert client.buy_calls == []

    # Pass 2: ask recovers to 0.75 and the re-score now succeeds & does NOT
    # deteriorate (winner bid 0.74 >= entry 0.70) → still no top-up: disarm is
    # permanent.
    monkeypatch.setattr(held_position_pass, "score_side_hgb", lambda **k: 0.60)
    deps = _deps(client, held_ask=0.75, held_bid=0.74)
    held_position_pass.run_held_position_pass(
        tmp_db, port, now_ts=NOW_TS + 1, deps=deps, spots=spots
    )
    assert client.buy_calls == []
    assert _topup_rows(tmp_db) == []


# ---------------------------------------------------------------------------
# same-second "blind" fresh gate (config.TOPUP_FRESH_GATE_MODE)
# ---------------------------------------------------------------------------
#
# A top-up placed in the SAME SECOND as its parent entry buys a book that can
# predate the parent's POST (live: n=50 -> -$5.81 vs n=117 >=1s later -> +$4.28).
# The gate rejects when now_ts <= parent_fill_epoch. _insert_entry stamps
# live_fill_ts in 2026 while NOW_TS is a 2024 epoch, so these tests pin the
# parent timestamp to a controlled second relative to NOW_TS.
_PASSING_BOOK = {"top_ask": 0.72, "top_bid": 0.71}  # armed + every other gate OK


def _sql_ts(epoch: int) -> str:
    """Ledger SQL text format (db.connection.utc_now_sql) for an epoch second."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _set_col(conn, rid, column, value):
    conn.execute(f"UPDATE ledger SET {column}=? WHERE id=?", (value, rid))
    conn.commit()


def _scan_passing(conn, rid, client, *, now_ts):
    """One scan against a book on which every OTHER T3 gate passes, so the fresh
    gate is the only thing that can suppress the top-up."""
    deps = _deps(client, held_ask=_PASSING_BOOK["top_ask"], held_bid=_PASSING_BOOK["top_bid"])
    return _scan(
        conn, rid, _make_book("Y", **_PASSING_BOOK),
        bid=_PASSING_BOOK["top_bid"], held_ask=_PASSING_BOOK["top_ask"],
        now_ts=now_ts, deps=deps,
    )


def test_fresh_gate_defaults_off():
    assert config.TOPUP_FRESH_GATE_MODE == "off"


def test_fresh_gate_live_rejects_same_second_then_next_second_places(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "TOPUP_FRESH_GATE_MODE", "live")
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    _set_col(tmp_db, rid, "live_fill_ts", _sql_ts(NOW_TS))  # parent filled THIS second
    client = FakeClient()

    assert _scan_passing(tmp_db, rid, client, now_ts=NOW_TS) == 0.0
    assert client.buy_calls == []  # blind same-second top-up suppressed
    st = _state(tmp_db, rid)
    # A skip is NOT an attempt and NOT a disarm — the next second must be free.
    assert st is not None
    assert st["last_attempt_ts"] is None  # no cadence throttle consumed
    assert st["disarmed"] == 0

    # +1s: now strictly after the parent fill second -> proceeds and places.
    spent = _scan_passing(tmp_db, rid, client, now_ts=NOW_TS + 1)
    assert len(client.buy_calls) == 1
    assert spent > 0


def test_fresh_gate_live_allows_one_second_after_parent(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "TOPUP_FRESH_GATE_MODE", "live")
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    _set_col(tmp_db, rid, "live_fill_ts", _sql_ts(NOW_TS - 1))  # filled 1s ago
    client = FakeClient()
    spent = _scan_passing(tmp_db, rid, client, now_ts=NOW_TS)
    assert len(client.buy_calls) == 1
    assert spent > 0


def test_fresh_gate_shadow_places_and_logs(tmp_db, monkeypatch, caplog):
    monkeypatch.setattr(config, "TOPUP_FRESH_GATE_MODE", "shadow")
    scale_in._FRESH_GATE_SHADOW_COUNTS["would_skip"] = 0
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    _set_col(tmp_db, rid, "live_fill_ts", _sql_ts(NOW_TS))
    client = FakeClient()

    with caplog.at_level(logging.INFO, logger="cryptobot.execution.scale_in"):
        spent = _scan_passing(tmp_db, rid, client, now_ts=NOW_TS)

    # Shadow NEVER suppresses — the top-up is placed exactly as today.
    assert len(client.buy_calls) == 1
    assert spent > 0
    assert "topup fresh-gate shadow" in caplog.text
    assert f"ledger_id={rid}" in caplog.text
    assert scale_in._FRESH_GATE_SHADOW_COUNTS["would_skip"] == 1
    scale_in._FRESH_GATE_SHADOW_COUNTS["would_skip"] = 0


def test_fresh_gate_off_same_second_still_places(tmp_db, monkeypatch):
    """Regression guard: with the flag off a same-second top-up behaves exactly
    as it does today (this is the cohort the gate exists to remove)."""
    monkeypatch.setattr(config, "TOPUP_FRESH_GATE_MODE", "off")
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    _set_col(tmp_db, rid, "live_fill_ts", _sql_ts(NOW_TS))
    client = FakeClient()
    spent = _scan_passing(tmp_db, rid, client, now_ts=NOW_TS)
    assert len(client.buy_calls) == 1
    assert spent > 0


def test_fresh_gate_falls_back_to_bet_ts_when_fill_ts_null(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "TOPUP_FRESH_GATE_MODE", "live")
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    _set_col(tmp_db, rid, "live_fill_ts", None)
    _set_col(tmp_db, rid, "bet_ts", _sql_ts(NOW_TS))  # placement THIS second
    client = FakeClient()
    assert _scan_passing(tmp_db, rid, client, now_ts=NOW_TS) == 0.0
    assert client.buy_calls == []  # bet_ts fallback still catches the same second


def test_fresh_gate_fails_open_when_no_timestamp_parses(tmp_db, monkeypatch):
    """No parseable parent timestamp -> ALLOW. T3 is +EV overall; a data gap must
    never silently kill it (bet_ts/created_at are NOT NULL, so they are set to
    unparseable text rather than NULL)."""
    monkeypatch.setattr(config, "TOPUP_FRESH_GATE_MODE", "live")
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70)
    _set_col(tmp_db, rid, "live_fill_ts", None)
    _set_col(tmp_db, rid, "bet_ts", "not-a-timestamp")
    _set_col(tmp_db, rid, "created_at", "also-not-a-timestamp")
    client = FakeClient()
    spent = _scan_passing(tmp_db, rid, client, now_ts=NOW_TS)
    assert len(client.buy_calls) == 1
    assert spent > 0


def test_fresh_gate_parses_iso_and_sql_timestamps():
    """live_fill_ts is an ISO-8601 CLOB payload; bet_ts/created_at are SQL text.
    Both must parse to the same UTC epoch, and junk must yield None (fail-open)."""
    assert scale_in._parse_ts_epoch("2026-07-16 00:00:00") == pytest.approx(1_784_160_000)
    assert scale_in._parse_ts_epoch("2026-07-16T00:00:00Z") == pytest.approx(1_784_160_000)
    assert scale_in._parse_ts_epoch("2026-07-16T00:00:00+00:00") == pytest.approx(1_784_160_000)
    assert scale_in._parse_ts_epoch("2026-07-16T00:00:00.500Z") == pytest.approx(1_784_160_000.5)
    for junk in (None, "", "   ", "not-a-timestamp", "2026-13-45 99:99:99"):
        assert scale_in._parse_ts_epoch(junk) is None


def test_run_pass_flag_off_no_scalein_writes(tmp_db, monkeypatch):
    monkeypatch.setattr(config, "SCALEIN_T3_ENABLED", False)
    monkeypatch.setattr(held_position_pass, "score_side_hgb", lambda **k: 0.44)
    rid = _insert_entry(tmp_db, entry_fill=0.70, ask0=0.70, size_tokens=5.0)
    client = FakeClient()
    # A book that WOULD arm a top-up if the flag were on (armed + winner bid).
    deps = _deps(client, held_ask=0.72, held_bid=0.71)
    held_position_pass.run_held_position_pass(
        tmp_db, _portfolio(), now_ts=NOW_TS, deps=deps,
        spots={"BTC": SimpleNamespace(price=100.0)},
    )
    # Flag off: no top-up buy, no scalein_state row, no top-up ledger row.
    assert client.buy_calls == []
    assert _topup_rows(tmp_db) == []
    assert _state(tmp_db, rid) is None
    # Winner (bid 0.71 >= entry 0.70) → held.
    row = tmp_db.execute("SELECT outcome FROM ledger WHERE id=?", (rid,)).fetchone()
    assert row["outcome"] == "PENDING"
