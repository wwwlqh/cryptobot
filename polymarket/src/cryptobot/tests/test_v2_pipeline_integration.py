"""Focused tests for the v2 step1last pipeline-integration helpers.

Covers the two net-new pieces the integration adds to ``pipeline.py``:
  * ``_record_v2_brake_health`` — graduated DD-brake CRITICAL telemetry
    (engaged below 0.75 vs at-floor, each deduped 6h; shallow throttle
    in [0.75, 1.0) is INFO-only, no CRITICAL row),
  * ``_v2_prune_window_decider`` — bounded-memory eviction of the module-global
    WindowDecider's committed-window set.

The WindowDecider decision semantics themselves live in ``spec_v2`` and are
covered by ``test_spec_v2_parity``; here we only assert the pipeline's wiring
(module-global identity + prune) behaves.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.execution import pipeline as P
from cryptobot.strategy import spec_v2

_V2_CONFIG = str(config.V6_CONFIG_PATH)  # resolved: runtime/ -> legacy -> polymarket/live/config/


def _load_v2_sleeves() -> dict:
    """Real MID checkpoint sleeves from the deployed v2 config (subminute_5m /
    subminute_15m — the only sleeves the live artifact carries)."""
    from cryptobot.strategy.portfolio_loader import load_sleeves

    prev = config.SUBMINUTE_ENABLED
    config.SUBMINUTE_ENABLED = True
    try:
        port = load_sleeves(_V2_CONFIG)
    finally:
        config.SUBMINUTE_ENABLED = prev
    return {s.label: s for s in port.sleeves}


# ---------------------------------------------------------------------------
# _v2_sizing_equity — real-balance vs ledger-capital selection
# ---------------------------------------------------------------------------


def test_sizing_equity_prefers_live_balance() -> None:
    live_ctx = SimpleNamespace(live_balance=113.69)
    # Cached CLOB balance present + fresh -> size off it, not the $93 ledger.
    assert P._v2_sizing_equity(live_ctx, 93.0) == pytest.approx(113.69)


def test_sizing_equity_falls_back_when_balance_none() -> None:
    live_ctx = SimpleNamespace(live_balance=None)
    # Stale/unavailable cache (get() failed closed to None) -> ledger capital.
    assert P._v2_sizing_equity(live_ctx, 93.0) == pytest.approx(93.0)


def test_sizing_equity_falls_back_when_no_live_ctx() -> None:
    # DRY / no live context -> ledger capital (never blocks on a fetch).
    assert P._v2_sizing_equity(None, 93.0) == pytest.approx(93.0)


# ---------------------------------------------------------------------------
# _v2_decider_cell — key shape the decider bookkeeping reads
# ---------------------------------------------------------------------------


def test_v2_decider_cell_shape() -> None:
    cell = P._v2_decider_cell("BTC", "5m", 1000, "YES")
    assert cell == {
        "coin": "BTC", "interval": "5m", "window_start": 1000, "side": "YES",
    }
    # matches the keys spec_v2.WindowDecider._key / _market_key read.
    assert spec_v2.WindowDecider._key(cell) == ("BTC", "5m", 1000, "YES")
    assert spec_v2.WindowDecider._market_key(cell) == ("BTC", "5m", 1000)


def _critical_messages(conn) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT message FROM pipeline_health "
            "WHERE stage='risk' AND status='CRITICAL' ORDER BY id"
        ).fetchall()
    ]


def test_brake_health_noop_when_healthy(db) -> None:
    P._record_v2_brake_health(db, brake=1.0, floor=0.50, dd=0.0)
    assert _critical_messages(db) == []


def test_brake_health_shallow_throttle_is_info_only(db) -> None:
    # Brake in [0.75, 1.0) is a routine shallow throttle -> INFO, no CRITICAL.
    # (scale 0.60 / floor 0.50 rule: dd metadata = 0.60*(1-brake).)
    P._record_v2_brake_health(db, brake=0.90, floor=0.50, dd=0.06)
    P._record_v2_brake_health(db, brake=0.75, floor=0.50, dd=0.15)
    assert _critical_messages(db) == []


def test_brake_health_engaged_writes_once_deduped(db) -> None:
    # Two engaged (crossed below 0.75, above floor) calls -> one CRITICAL row.
    P._record_v2_brake_health(db, brake=0.60, floor=0.50, dd=0.24)
    P._record_v2_brake_health(db, brake=0.55, floor=0.50, dd=0.27)
    msgs = _critical_messages(db)
    assert len(msgs) == 1
    assert "ENGAGED" in msgs[0]


def test_brake_health_engaged_fires_only_below_warn_threshold(db) -> None:
    # 0.80 is a shallow throttle (INFO); crossing to 0.60 fires the CRITICAL.
    P._record_v2_brake_health(db, brake=0.80, floor=0.50, dd=0.12)
    assert _critical_messages(db) == []
    P._record_v2_brake_health(db, brake=0.60, floor=0.50, dd=0.24)
    msgs = _critical_messages(db)
    assert len(msgs) == 1 and "ENGAGED" in msgs[0]


def test_brake_health_floor_is_a_distinct_alert(db) -> None:
    P._record_v2_brake_health(db, brake=0.60, floor=0.50, dd=0.24)  # engaged
    P._record_v2_brake_health(db, brake=0.50, floor=0.50, dd=0.30)  # at floor
    msgs = _critical_messages(db)
    assert len(msgs) == 2
    joined = " ".join(msgs)
    assert "ENGAGED" in joined and "AT FLOOR" in joined


def test_prune_window_decider_evicts_only_stale(db=None) -> None:
    dec = P._V2_WINDOW_DECIDER
    dec.reset()
    try:
        now = 2_000_000
        stale_ws = now - (P._V2_DECIDER_RETAIN_SECONDS + 1)
        # Push past the prune threshold with a mix of stale and fresh windows.
        for i in range(P._V2_DECIDER_PRUNE_ABOVE + 20):
            ws = stale_ws if i % 2 == 0 else now
            dec._committed.add((f"btc{i}", "5m", ws, "YES"))
        assert len(dec._committed) > P._V2_DECIDER_PRUNE_ABOVE
        P._v2_prune_window_decider(now)
        # Every surviving key is inside the retention window; stale ones gone.
        assert dec._committed
        assert all(
            k[2] >= now - P._V2_DECIDER_RETAIN_SECONDS for k in dec._committed
        )
    finally:
        dec.reset()


def test_prune_window_decider_noop_below_threshold() -> None:
    dec = P._V2_WINDOW_DECIDER
    dec.reset()
    try:
        now = 3_000_000
        # A single very old key: under the size threshold it is retained
        # (prune only triggers once the set grows large).
        dec._committed.add(("btc", "5m", now - 10_000_000, "YES"))
        P._v2_prune_window_decider(now)
        assert len(dec._committed) == 1
    finally:
        dec.reset()


# ---------------------------------------------------------------------------
# _v2_decider_for — per-sleeve band-aware decider registry
# ---------------------------------------------------------------------------


def test_v2_decider_for_live_sleeves_uses_their_band() -> None:
    P._V2_WINDOW_DECIDERS.clear()
    sleeves = _load_v2_sleeves()
    d5 = P._v2_decider_for(sleeves["subminute_5m"])
    # The live band differs from the default, so each sleeve gets its own.
    assert d5 is not P._V2_WINDOW_DECIDER
    assert (d5.ask_lo, d5.ask_hi) == (0.55, 0.85)
    assert d5.gate == 0.10
    assert P._v2_decider_for(sleeves["subminute_5m"]) is d5
    assert P._v2_decider_for(sleeves["subminute_15m"]) is not d5


def test_v2_decider_for_non_mid_band_mints_own_decider() -> None:
    # The deployed artifact is MID-only, but the band-aware decider registry
    # stays in src (any sleeve owning a different band/gate reuses it) — cover
    # it with a synthetic sleeve built off the real MID sleeve.
    from dataclasses import replace

    P._V2_WINDOW_DECIDERS.clear()
    sleeves = _load_v2_sleeves()
    alt_sleeve = replace(
        sleeves["subminute_5m"],
        label="alt_5m",
        ask_bucket_ranges=((0.85, 0.92),),
        threshold=0.01,
    )
    alt5 = P._v2_decider_for(alt_sleeve)
    # The non-MID sleeve gets its OWN band-aware decider: (0.85, 0.92], gate 0.01.
    assert alt5 is not P._V2_WINDOW_DECIDER
    assert (alt5.ask_lo, alt5.ask_hi) == (0.85, 0.92)
    assert alt5.gate == 0.01
    # cached: a second lookup returns the same instance.
    assert P._v2_decider_for(alt_sleeve) is alt5
    # The two deciders hold PRIVATE _filled sets (cross-sleeve dedup is NOT via
    # the decider — it is the shared _V2_MARKET_FILLED set + durable ledger query).
    mid = P._v2_decider_for(sleeves["subminute_5m"])
    assert alt5._filled is not mid._filled


def test_mid_band_with_60s_offsets_does_not_reuse_legacy_30s_decider() -> None:
    from dataclasses import replace

    P._V2_WINDOW_DECIDERS.clear()
    sleeve = replace(
        _load_v2_sleeves()["subminute_5m"],
        label="mid_final_minute_5m",
        checkpoint_offset_set=tuple(range(240, 300)),
    )
    decider = P._v2_decider_for(sleeve)

    assert decider is not P._V2_WINDOW_DECIDER
    assert decider.checkpoint_offsets == frozenset(range(240, 300))


def test_prune_evicts_market_filled_set() -> None:
    P._V2_MARKET_FILLED.clear()
    now = 4_000_000
    stale_ws = now - (P._V2_DECIDER_RETAIN_SECONDS + 1)
    for i in range(P._V2_DECIDER_PRUNE_ABOVE + 20):
        ws = stale_ws if i % 2 == 0 else now
        P._V2_MARKET_FILLED.add((f"btc{i}", "5m", ws))
    assert len(P._V2_MARKET_FILLED) > P._V2_DECIDER_PRUNE_ABOVE
    P._v2_prune_window_decider(now)
    assert P._V2_MARKET_FILLED  # fresh keys survive
    assert all(
        k[2] >= now - P._V2_DECIDER_RETAIN_SECONDS for k in P._V2_MARKET_FILLED
    )
    P._V2_MARKET_FILLED.clear()


# ---------------------------------------------------------------------------
# _v2_apply_coin_cap — per-coin cap clamp (INERT in the deployed artifact)
#
# OPERATOR OVERRIDE 2026-07-21 ("follow backtest, don't look at live
# performance"): the SOL $1-floor probe was REMOVED — both MID sleeves ship
# max_bet_usd_by_coin {} and all four coins run full equity-fraction size. The
# pipeline helper stays wired (a future per-coin cap re-uses it), so the clamp
# semantics are covered with a SYNTHETIC cap and the artifact is pinned to the
# empty-map pass-through.
# ---------------------------------------------------------------------------


def test_v2_apply_coin_cap_artifact_maps_empty_all_coins_pass_through() -> None:
    """The deployed artifact carries NO per-coin cap (max_bet_usd_by_coin {})
    on either sleeve — the clamp is a no-op for every coin in the 4-coin
    universe and the sized stake passes through untouched."""
    sleeves = _load_v2_sleeves()
    for label in ("subminute_5m", "subminute_15m"):
        sleeve = sleeves[label]
        assert sleeve.max_bet_usd_by_coin == {}
        for coin in ("BTC", "ETH", "SOL", "XRP"):
            assert sleeve.max_bet_usd_for(coin) is None
            assert P._v2_apply_coin_cap(sleeve, coin, 3.75) == 3.75


def test_v2_apply_coin_cap_synthetic_cap_clamps_including_tier_a() -> None:
    """Clamp semantics (kept for a future per-coin cap): with a synthetic
    {"SOL": 1.0} cap the clamp runs AFTER sized_usd's $1 floor-up, the tier-A
    1.5x boost and the ramp re-floor, so a capped entry — tier-A firsts
    included — goes out at EXACTLY the cap, and other coins are untouched."""
    from dataclasses import replace

    from cryptobot.execution import sizing_v2

    sleeves = _load_v2_sleeves()
    for label in ("subminute_5m", "subminute_15m"):
        sleeve = replace(sleeves[label], max_bet_usd_by_coin={"SOL": 1.0})
        assert sleeve.max_bet_usd_for("SOL") == 1.0
        sized = sizing_v2.sized_usd(100.0, sleeve.interval, 0.0, k=1)
        # sized_usd floors up to MIN_BET_USD, so min(target, 1.0) == 1.0 exactly.
        assert sized >= 1.0
        assert P._v2_apply_coin_cap(sleeve, "SOL", sized) == 1.0
        # Tier-A boosted first fill (the $1.50-bet hazard the clamp placement
        # after the boost specifically prevents).
        assert P._v2_apply_coin_cap(sleeve, "SOL", sized * 1.5) == 1.0
        # Coins without a cap entry still pass through unchanged.
        assert P._v2_apply_coin_cap(sleeve, "BTC", 3.75) == 3.75


# ---------------------------------------------------------------------------
# subminute_market_other_sleeve — durable cross-sleeve one-bet-per-market
# ---------------------------------------------------------------------------


def test_subminute_market_other_sleeve_cross_sleeve(db) -> None:
    from cryptobot.execution.ledger import (
        record_bet,
        subminute_market_other_sleeve,
    )
    from cryptobot.tests.test_subminute import make_subminute_signal

    window = 1_800_000
    # MID (subminute_5m) opens a real position on BTC's 5m market.
    record_bet(
        db,
        make_subminute_signal(
            coin="BTC", sleeve="subminute_5m", side="YES",
            offset_sec=275, window_start_ts=window,
        ),
        mode="DRY",
    )
    db.commit()

    # A second sleeve (other_5m) on the SAME market sees the subminute_5m row ->
    # blocked (first fill wins).
    assert subminute_market_other_sleeve(
        db, coin="BTC", interval_min=5, window_start_ts=window, sleeve="other_5m"
    ) is True
    # The owner itself is not "another sleeve".
    assert subminute_market_other_sleeve(
        db, coin="BTC", interval_min=5, window_start_ts=window, sleeve="subminute_5m"
    ) is False
    # A different coin / interval / window is a DIFFERENT market -> not blocked.
    assert subminute_market_other_sleeve(
        db, coin="ETH", interval_min=5, window_start_ts=window, sleeve="other_5m"
    ) is False
    assert subminute_market_other_sleeve(
        db, coin="BTC", interval_min=15, window_start_ts=window, sleeve="other_15m"
    ) is False
    assert subminute_market_other_sleeve(
        db, coin="BTC", interval_min=5, window_start_ts=window + 300, sleeve="other_5m"
    ) is False
