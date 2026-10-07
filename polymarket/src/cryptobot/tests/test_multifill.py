"""Multifill — independent entries per resolution above an equity floor.

Operator order 2026-07-10: "if there is a 2nd entry you can have a 2nd bet for
BTC if all conditions are satisfied, applied for all coins" — capacity scaling
for a grown bankroll. The per-resolution entry COUNT is unbounded (the fixed
ceiling was removed by operator order 2026-07-11 — "the number-of-bets cap = 5,
no need"); capacity is bounded instead by the equity gate, the same-side rule,
the shared exposure cap and the DD brake/halt.

Two production primitives carry the feature and are unit-covered here:
  * ``ledger.count_window_entries`` — the durable, restart-safe, cross-sleeve
    non-topup entry tally the gate reads (count >= 1 == a prior entry filled);
  * ``spec_v2.WindowDecider.on_second(..., allow_refill=)`` — the consume-on-fill
    veto bypass (covered in test_spec_v2_parity).

``_simulate_window`` below mirrors the pipeline's one-per-window gate block
(execution/pipeline.py) call-for-call, so the whole feature is asserted
end-to-end: a filled first entry admits further same-side entries with no count
ceiling, and the same-side rule still refuses the opposite leg of the binary.

⚠️ The equity floor was dropped 5000 -> 0 by operator order 2026-07-25
("every qualifying"), so multifill is now ARMED at live equity and the old
INERTNESS BAR (flag-on == flag-off at ~$130) no longer describes what ships. The
two tests that pin a 5000 floor now stand for the DISARM path; the
"EVERY-QUALIFYING" section at the bottom pins the shipped configuration.
"""
from __future__ import annotations

import sqlite3

import pytest

from cryptobot import config
from cryptobot.execution.ledger import (
    count_window_entries,
    record_bet,
    subminute_window_existing_side_any_sleeve,
)
from cryptobot.strategy import spec_v2
from cryptobot.tests.test_subminute import make_subminute_signal, WINDOW_START


# ---------------------------------------------------------------------------
# count_window_entries — durable cross-sleeve non-topup entry tally
# ---------------------------------------------------------------------------


def _record(conn, *, sleeve="subminute_5m", side="YES", offset_sec=275,
            slot_filled_pre=0.0, window_start_ts=WINDOW_START, mode="DRY",
            is_topup=None):
    rid = record_bet(
        conn,
        make_subminute_signal(
            coin="BTC", sleeve=sleeve, side=side, offset_sec=offset_sec,
            window_start_ts=window_start_ts, slot_filled_pre=slot_filled_pre,
        ),
        mode=mode,
        is_topup=is_topup,
    )
    conn.commit()
    return rid


def test_count_window_entries_empty_is_zero(db) -> None:
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 0


def test_count_window_entries_counts_non_topup_entries(db) -> None:
    _record(db, sleeve="subminute_5m", offset_sec=275)
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 1
    # a SECOND independent (is_topup=0) entry on the same market -> 2.
    _record(db, sleeve="subminute_5m", offset_sec=280)
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 2


def test_count_window_entries_is_cross_sleeve(db) -> None:
    # Two sleeves taking the same market count as TWO entries (the "N entries
    # per resolution" contract arbitrates across sleeves).
    _record(db, sleeve="subminute_5m", offset_sec=275)
    _record(db, sleeve="other_5m", offset_sec=280)
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 2


def test_count_window_entries_excludes_topups(db) -> None:
    _record(db, sleeve="subminute_5m", offset_sec=275)              # entry
    _record(db, sleeve="subminute_5m", offset_sec=280,
            slot_filled_pre=2.0, is_topup=True)                     # top-up
    # Top-ups complete ONE position — they are not independent entries.
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 1


def test_count_window_entries_excludes_cancelled(db) -> None:
    rid = _record(db, sleeve="subminute_5m", offset_sec=275)
    db.execute("UPDATE ledger SET outcome='CANCELLED' WHERE id=?", (rid,))
    db.commit()
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 0


def test_count_window_entries_scopes_market(db) -> None:
    _record(db, sleeve="subminute_5m", offset_sec=275, window_start_ts=WINDOW_START)
    # different window / coin / interval are different markets.
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START + 300
    ) == 0
    assert count_window_entries(
        db, coin="ETH", interval_min=5, window_start_ts=WINDOW_START
    ) == 0
    assert count_window_entries(
        db, coin="BTC", interval_min=15, window_start_ts=WINDOW_START
    ) == 0


# ---------------------------------------------------------------------------
# Decision-flow simulation — mirrors the pipeline one-per-window gate block
# (execution/pipeline.py) so the inertness bar is asserted end-to-end.
# ---------------------------------------------------------------------------


def _simulate_window(conn, decider, *, equity, seconds, coin="BTC",
                     interval="5m", interval_min=5, side="YES",
                     sleeve="subminute_5m", window_start_ts=WINDOW_START):
    """Replay a window's qualifying seconds through the exact gate the pipeline
    runs (multifill allowance -> decider.on_second(allow_refill) -> cross-sleeve
    first-fill-wins veto -> cross-sleeve SAME-SIDE enforcement), recording a real
    ledger fill whenever it PLACEs.

    Returns the list of offsets that PLACED (i.e. opened a ledger row).
    """
    placed = []
    for offset in seconds:
        # -- multifill allowance (pipeline.py: the _multifill_extra block) -----
        multifill_extra = False
        if config.V2_MULTIFILL_ENABLED and equity >= config.V2_MULTIFILL_MIN_EQUITY:
            existing = count_window_entries(
                conn, coin=coin, interval_min=interval_min,
                window_start_ts=window_start_ts,
            )
            # Per-resolution cap: 1 initial + MAX_EXTRA extras. Over it the
            # pipeline skips as `multifill_cap` before touching the decider.
            if existing > config.V2_MULTIFILL_MAX_EXTRA:
                continue
            multifill_extra = existing >= 1

        cell = {
            "coin": coin, "interval": interval, "window_start": window_start_ts,
            "side": side, "ask": 0.70, "offset_sec": offset, "pred_edge": 0.50,
        }
        verdict = decider.on_second(cell, allow_refill=multifill_extra)
        if verdict != "PLACE":
            continue
        # -- cross-sleeve first-fill-wins veto (bypassed under multifill) ------
        # (single-sleeve sim: the durable other-sleeve query is vacuously False;
        #  the in-memory _V2_MARKET_FILLED analogue is the decider's own _filled,
        #  already consulted above.)
        # -- cross-sleeve SAME-SIDE enforcement (multifill only) ---------------
        # Multifill bypasses the first-fill-wins veto, so an extra entry could
        # otherwise open the OPPOSITE leg of the binary on a resolution another
        # sleeve already filled. Reject any side that disagrees with the
        # resolution's already-open side across ALL sleeves (pipeline.py).
        if multifill_extra:
            existing_side = subminute_window_existing_side_any_sleeve(
                conn, coin=coin, interval_min=interval_min,
                window_start_ts=window_start_ts,
            )
            if existing_side is not None and existing_side != side:
                continue
        # -- fill: write the ledger row + tell the decider ---------------------
        _record(conn, sleeve=sleeve, side=side, offset_sec=offset,
                window_start_ts=window_start_ts)
        decider.note_fill(cell)
        placed.append(offset)
    return placed


@pytest.fixture(autouse=True)
def _pin_multifill_config(monkeypatch: pytest.MonkeyPatch) -> None:
    # Only ENABLED is pinned. The equity floor is per-test: the tests below run
    # at $6000, which clears both the shipped 0 and the retired 5000 identically,
    # and the two that genuinely exercise a floor pin it themselves.
    monkeypatch.setattr(config, "V2_MULTIFILL_ENABLED", True)


def test_below_floor_flag_on_equals_flag_off(db, monkeypatch) -> None:
    # DISARM path: below the floor the decision flow is byte-identical whether
    # the multifill flag is on or off — exactly ONE entry per resolution. This is
    # what raising CRYPTOBOT_V2_MULTIFILL_MIN_EQUITY back above live equity
    # restores.
    monkeypatch.setattr(config, "V2_MULTIFILL_MIN_EQUITY", 5000.0)
    seconds = [292, 293, 294, 295]

    monkeypatch.setattr(config, "V2_MULTIFILL_ENABLED", True)
    dec_on = spec_v2.WindowDecider(consume_on_fill=True)
    placed_on = _simulate_window(db, dec_on, equity=130.0, seconds=seconds)

    # Fresh DB for the OFF replay.
    db.execute("DELETE FROM ledger")
    db.commit()
    monkeypatch.setattr(config, "V2_MULTIFILL_ENABLED", False)
    dec_off = spec_v2.WindowDecider(consume_on_fill=True)
    placed_off = _simulate_window(db, dec_off, equity=130.0, seconds=seconds)

    assert placed_on == placed_off == [292]  # one bet, first qualifying second


def test_second_entry_allowed_up_to_the_cap_at_6k(db) -> None:
    # Above the floor, each subsequent qualifying second opens an independent
    # entry until the per-resolution cap binds: 1 initial + MAX_EXTRA extras.
    # Six qualifying seconds are offered; only the first four may fill.
    seconds = [292, 293, 294, 295, 296, 297]
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(db, dec, equity=6000.0, seconds=seconds)

    assert placed == [292, 293, 294, 295]  # initial + 3 extras, then capped
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 1 + config.V2_MULTIFILL_MAX_EXTRA


def test_no_second_entry_just_below_equity_floor(db, monkeypatch) -> None:
    # $4999 is below a $5000 floor -> feature inert, one bet only.
    monkeypatch.setattr(config, "V2_MULTIFILL_MIN_EQUITY", 5000.0)
    seconds = [292, 293, 294]
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(db, dec, equity=4999.0, seconds=seconds)
    assert placed == [292]


# ---------------------------------------------------------------------------
# EVERY-QUALIFYING — the SHIPPED configuration (operator order 2026-07-25)
# ---------------------------------------------------------------------------


def test_shipped_defaults_are_armed_and_capped() -> None:
    # Guards the arming AND its bound together. If the floor drifts back up,
    # every-qualifying silently reverts to one-bet-per-resolution at live equity;
    # if MAX_EXTRA drifts up, a single resolution can absorb most of the
    # bankroll (30 qualifying seconds x ~$3.25 at ~$130 equity).
    assert config.V2_MULTIFILL_ENABLED is True
    assert config.V2_MULTIFILL_MIN_EQUITY == 0.0
    assert config.V2_MULTIFILL_MAX_EXTRA == 3  # 1 initial + 3 = 4 rows max


def test_every_qualifying_armed_at_live_equity(db) -> None:
    # At the SHIPPED floor and today's ~$130 equity, the first qualifying second
    # fills and each later qualifying second opens an independent entry that is
    # re-scored on that second's own book/prob — up to the cap.
    seconds = [292, 293, 294, 295]
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(db, dec, equity=130.0, seconds=seconds)

    assert placed == seconds
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == len(seconds)


def test_cap_binds_at_exactly_four_rows(db) -> None:
    # The boundary an off-by-one would hide: with 4 rows already on the
    # resolution the NEXT qualifying second must open nothing, and with 3 it
    # must still be admitted. Rows are pre-seeded rather than simulated so the
    # tally is unambiguous.
    for i, off in enumerate((280, 281, 282)):
        _record(db, sleeve="subminute_5m", side="YES", offset_sec=off)
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 3

    # 3 existing (initial + 2 extras) -> the 4th is still allowed.
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    assert _simulate_window(db, dec, equity=130.0, seconds=[292]) == [292]
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 4

    # 4 existing -> capped, nothing more opens however many seconds qualify.
    dec2 = spec_v2.WindowDecider(consume_on_fill=True)
    assert _simulate_window(db, dec2, equity=130.0, seconds=[293, 294, 295]) == []
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 4


def test_cap_zero_restores_one_entry_per_resolution(db, monkeypatch) -> None:
    # MAX_EXTRA=0 is the disarm path: no extras, i.e. the pre-multifill
    # one-bet-per-resolution semantic, with the floor still at 0.
    monkeypatch.setattr(config, "V2_MULTIFILL_MAX_EXTRA", 0)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(db, dec, equity=130.0, seconds=[292, 293, 294])
    assert placed == [292]


def test_cap_counts_across_sleeves(db) -> None:
    # count_window_entries is cross-sleeve, so the cap is a property of the
    # RESOLUTION, not of one sleeve: 4 rows opened by subminute_5m must block a
    # 5th from the other sleeve even though that sleeve has opened nothing.
    for off in (280, 281, 282, 283):
        _record(db, sleeve="subminute_5m", side="YES", offset_sec=off)

    dec_other = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(
        db, dec_other, equity=130.0, seconds=[292, 293],
        sleeve="other_5m", side="YES",
    )
    assert placed == []
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 4


def test_every_qualifying_still_refuses_the_opposite_leg(db) -> None:
    # Arming at $0 must NOT weaken the same-side rule: an extra entry whose side
    # disagrees with the resolution's already-open side is still rejected, so the
    # bot can never hold both legs of one binary.
    _record(db, sleeve="subminute_5m", side="YES", offset_sec=280)

    dec_other = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(
        db, dec_other, equity=130.0, seconds=[292, 293],
        sleeve="other_5m", side="NO",
    )
    assert placed == []
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 1


# ---------------------------------------------------------------------------
# Cross-sleeve SAME-SIDE enforcement (adversarial review, money-path)
# ---------------------------------------------------------------------------
#
# Multifill deliberately bypasses the cross-sleeve first-fill-wins guard so a
# SECOND independent entry can open on an already-filled resolution — the same
# sleeve OR another one. The per-sleeve opposite-side dedup (WHERE sleeve=?) is
# BLIND to the other sleeve, so above the equity floor one sleeve could fill
# YES, the price reverse, a second sleeve qualify NO, and the bot open BOTH legs
# of one binary (a guaranteed-losing hedge). The multifill branch re-enforces
# same-side ACROSS ALL SLEEVES; these tests pin that.


def test_multifill_cross_sleeve_opposite_side_rejected_at_6k(db) -> None:
    # subminute_5m already FILLED YES on this resolution. At $6k a second sleeve
    # (other_5m) qualifies on the SAME market but the OPPOSITE side (NO) —
    # price reversed. Buying both legs is a guaranteed-losing hedge, so the
    # extra entry MUST be rejected and NO ledger row opens.
    _record(db, sleeve="subminute_5m", side="YES", offset_sec=280)
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 1

    dec_other = spec_v2.WindowDecider(consume_on_fill=True)  # fresh 2nd-sleeve decider
    placed = _simulate_window(
        db, dec_other, equity=6000.0, seconds=[292, 293],
        sleeve="other_5m", side="NO",
    )
    assert placed == []  # opposite side across sleeves: rejected
    # Still exactly the one subminute_5m entry — no NO leg opened on the binary.
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 1
    assert subminute_window_existing_side_any_sleeve(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == "YES"


def test_multifill_cross_sleeve_same_side_admitted_at_6k(db) -> None:
    # subminute_5m already FILLED YES. At $6k a second sleeve (other_5m)
    # qualifies on the SAME side (YES) of the SAME market — a legit cross-sleeve
    # extra: it is admitted and opens a second same-side entry (2 total).
    _record(db, sleeve="subminute_5m", side="YES", offset_sec=280)

    dec_other = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(
        db, dec_other, equity=6000.0, seconds=[292],
        sleeve="other_5m", side="YES",
    )
    assert placed == [292]  # same side across sleeves: the extra entry opens
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 2


def test_multifill_same_sleeve_side_gate_unchanged(db, monkeypatch) -> None:
    # Same-sleeve behavior is unchanged by the cross-sleeve guard:
    #  * same side -> a second entry still opens;
    #  * opposite side -> still rejected (no both-legs position).
    # Same-side admission (subminute_5m, YES): a second same-side entry opens.
    seconds = [292, 293]
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    placed = _simulate_window(db, dec, equity=6000.0, seconds=seconds)
    assert placed == [292, 293]
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=WINDOW_START
    ) == 2

    # Opposite side within the SAME sleeve, on a FRESH resolution: the first
    # entry fills YES, a later NO extra is rejected (the broadened same-side
    # query catches an intra-sleeve flip too).
    ws2 = WINDOW_START + 300
    _record(db, sleeve="subminute_5m", side="YES", offset_sec=280,
            window_start_ts=ws2)
    dec2 = spec_v2.WindowDecider(consume_on_fill=True)
    placed2 = _simulate_window(
        db, dec2, equity=6000.0, seconds=[292, 293],
        sleeve="subminute_5m", side="NO", window_start_ts=ws2,
    )
    assert placed2 == []
    assert count_window_entries(
        db, coin="BTC", interval_min=5, window_start_ts=ws2
    ) == 1
