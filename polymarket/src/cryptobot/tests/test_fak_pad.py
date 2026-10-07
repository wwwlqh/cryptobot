"""Anchored FAK pad (config.V2_FAK_PAD) — operator-approved 2026-07-11.

Lifts the executable limit_price of every LIVE sub-minute BUY FAK by PAD, clamped
to ``first_limit_of_this_side_window + PAD`` and clipped to ``<= 0.99``, so an
order crosses a book that ticked up between the score and the sign WITHOUT
chasing a runaway book. Two production primitives carry the feature:

  * ``execution.pipeline._fak_padded_limit`` — the pure pad math + anchor cap +
    0.99 clip (the exact function ``_commit_cell`` calls);
  * ``strategy.spec_v2.WindowDecider.anchor_limit`` — the in-memory per-side-window
    first-limit store (the exact anchor ``_commit_cell`` reads).

``_padded_placement_limit`` below mirrors ``_commit_cell``'s sub-minute FAK-pad
guard block (execution/pipeline.py, just above the BetSignal build) call-for-call
— guard -> ``anchor_limit`` -> ``_fak_padded_limit`` — so the scope guards (flag
on, LIVE, sub-minute, walked present) and the anchor cap are asserted exactly as
the live path runs them. Keep it in lockstep with that block. Default 0.0 = OFF
must be byte-identical.

Hermetic: no network.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cryptobot import config
import cryptobot.execution.pipeline as pipeline
from cryptobot.execution.pipeline import _fak_padded_limit, _v2_decider_cell
from cryptobot.strategy import spec_v2

_WS = 1_715_923_200  # divisible by 300 (5m boundary); window_start_ts anchor key


def _padded_placement_limit(
    decider, *, walked_limit, wallet_mode, offset_sec,
    coin="BTC", interval="5m", window_start_ts=_WS, side="YES",
):
    """Mirror execution/pipeline.py's sub-minute FAK-pad guard block
    call-for-call (guard -> WindowDecider.anchor_limit -> _fak_padded_limit).

    Kept in lockstep with the block just above the ``_commit_cell`` BetSignal
    build so the four scope guards and the anchored cap are asserted exactly as
    the live placement path applies them.
    """
    limit_price_out = walked_limit
    if (
        config.V2_FAK_PAD > 0.0
        and wallet_mode == "LIVE"
        and offset_sec is not None
        and walked_limit is not None
    ):
        anchor = None if config.V2_RETRY_REPRICE else decider.anchor_limit(
            _v2_decider_cell(coin, interval, window_start_ts, side),
            walked_limit,
        )
        limit_price_out = _fak_padded_limit(
            walked_limit, anchor=anchor, pad=config.V2_FAK_PAD
        )
    return limit_price_out


# ---------------------------------------------------------------------------
# Flag default — OFF is byte-identical
# ---------------------------------------------------------------------------
def test_flag_defaults_off() -> None:
    assert config.V2_FAK_PAD == 0.0


# ---------------------------------------------------------------------------
# _fak_padded_limit — pure pad math + anchor cap + clip
# ---------------------------------------------------------------------------
def test_pad_first_attempt_adds_pad() -> None:
    # anchor == walked on the first attempt -> walked + pad.
    assert _fak_padded_limit(0.80, anchor=0.80, pad=0.01) == pytest.approx(0.81)


def test_pad_retry_above_first_is_capped() -> None:
    # a later second's fresh walk ran ABOVE the anchor -> capped at first + pad.
    assert _fak_padded_limit(0.85, anchor=0.80, pad=0.01) == pytest.approx(0.81)


# ---------------------------------------------------------------------------
# V2_RETRY_REPRICE — every-qualifying retries (operator order 2026-07-25)
# ---------------------------------------------------------------------------
#
# The anchor clamp is what made a 2nd/3rd attempt execute at the FIRST
# qualifying second's price even though it re-qualified on a new second. With
# the reprice on, attempt 2..N pays its own fresh walked limit; attempt 1 is
# identical in both modes.


def test_retry_reprice_defaults_off() -> None:
    # Default OFF keeps the anchored behavior byte-identical.
    assert config.V2_RETRY_REPRICE is False


def test_reprice_anchor_none_pays_own_walk() -> None:
    # The exact case the audit found: fresh walk ABOVE the first attempt. Anchored
    # it is sent at 0.81 — BELOW the 0.85 the book is actually showing, i.e.
    # structurally unfillable. Repriced it is sent at 0.86 and can cross.
    assert _fak_padded_limit(0.85, anchor=0.80, pad=0.01) == pytest.approx(0.81)
    assert _fak_padded_limit(0.85, anchor=None, pad=0.01) == pytest.approx(0.86)


def test_reprice_still_clips_at_099() -> None:
    # Dropping the anchor must NOT drop the hard 0.99 clip.
    assert _fak_padded_limit(0.995, anchor=None, pad=0.01) == pytest.approx(0.99)


def test_reprice_does_not_change_a_cheaper_retry() -> None:
    # A retry whose fresh walk fell BELOW the anchor already paid its own price;
    # the anchor was never binding there, so both modes agree.
    assert _fak_padded_limit(0.70, anchor=0.80, pad=0.01) == pytest.approx(0.71)
    assert _fak_padded_limit(0.70, anchor=None, pad=0.01) == pytest.approx(0.71)


def test_reprice_leaves_first_attempt_identical(monkeypatch) -> None:
    # End-to-end through the mirrored placement block: the FIRST attempt in a
    # side window sends the same limit with the reprice on or off, because its
    # anchor IS its own walked limit.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)

    monkeypatch.setattr(config, "V2_RETRY_REPRICE", False)
    anchored = _padded_placement_limit(
        spec_v2.WindowDecider(consume_on_fill=True),
        walked_limit=0.80, wallet_mode="LIVE", offset_sec=292,
    )
    monkeypatch.setattr(config, "V2_RETRY_REPRICE", True)
    repriced = _padded_placement_limit(
        spec_v2.WindowDecider(consume_on_fill=True),
        walked_limit=0.80, wallet_mode="LIVE", offset_sec=292,
    )
    assert anchored == repriced == pytest.approx(0.81)


def test_reprice_frees_the_second_attempt(monkeypatch) -> None:
    # End-to-end: attempt 1 walks 0.80, the book runs up, attempt 2 walks 0.85.
    # Anchored, attempt 2 is pinned to 0.81; repriced it pays 0.86.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)

    monkeypatch.setattr(config, "V2_RETRY_REPRICE", False)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    assert _padded_placement_limit(
        dec, walked_limit=0.80, wallet_mode="LIVE", offset_sec=292
    ) == pytest.approx(0.81)
    assert _padded_placement_limit(
        dec, walked_limit=0.85, wallet_mode="LIVE", offset_sec=293
    ) == pytest.approx(0.81)  # pinned to the FIRST qualifying second's price

    monkeypatch.setattr(config, "V2_RETRY_REPRICE", True)
    dec2 = spec_v2.WindowDecider(consume_on_fill=True)
    assert _padded_placement_limit(
        dec2, walked_limit=0.80, wallet_mode="LIVE", offset_sec=292
    ) == pytest.approx(0.81)
    assert _padded_placement_limit(
        dec2, walked_limit=0.85, wallet_mode="LIVE", offset_sec=293
    ) == pytest.approx(0.86)  # prices off its OWN second


def test_reprice_inert_when_pad_is_off(monkeypatch) -> None:
    # The reprice lives INSIDE the pad block, so with the pad off it cannot
    # change anything — the bare walked limit is still what goes out.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.0)
    monkeypatch.setattr(config, "V2_RETRY_REPRICE", True)
    assert _padded_placement_limit(
        spec_v2.WindowDecider(consume_on_fill=True),
        walked_limit=0.85, wallet_mode="LIVE", offset_sec=292,
    ) == pytest.approx(0.85)


def test_reprice_does_not_touch_dry(monkeypatch) -> None:
    # Scope guard unchanged: DRY paper rows still send the bare walked limit, so
    # research parity is preserved.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)
    monkeypatch.setattr(config, "V2_RETRY_REPRICE", True)
    assert _padded_placement_limit(
        spec_v2.WindowDecider(consume_on_fill=True),
        walked_limit=0.85, wallet_mode="DRY", offset_sec=292,
    ) == pytest.approx(0.85)


def test_pad_retry_below_first_not_capped() -> None:
    # fresh walk BELOW the anchor -> the cap is not binding, pay walked + pad.
    assert _fak_padded_limit(0.75, anchor=0.80, pad=0.01) == pytest.approx(0.76)


def test_pad_clips_at_099() -> None:
    assert _fak_padded_limit(0.995, anchor=0.995, pad=0.01) == pytest.approx(0.99)
    # clip wins even when the anchor cap would allow higher.
    assert _fak_padded_limit(0.99, anchor=0.99, pad=0.05) == pytest.approx(0.99)


# ---------------------------------------------------------------------------
# WindowDecider.anchor_limit — first-set / get lifecycle
# ---------------------------------------------------------------------------
def test_anchor_sets_on_first_and_holds() -> None:
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    cell = _v2_decider_cell("BTC", "5m", _WS, "YES")
    # first call RECORDS walked as the anchor and returns it.
    assert dec.anchor_limit(cell, 0.80) == pytest.approx(0.80)
    # every later call returns the STORED first limit, ignoring the new walked.
    assert dec.anchor_limit(cell, 0.85) == pytest.approx(0.80)
    assert dec.anchor_limit(cell, 0.60) == pytest.approx(0.80)


def test_anchor_independent_per_side_key() -> None:
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    yes = _v2_decider_cell("BTC", "5m", _WS, "YES")
    no = _v2_decider_cell("BTC", "5m", _WS, "NO")
    other_window = _v2_decider_cell("BTC", "5m", _WS + 300, "YES")
    assert dec.anchor_limit(yes, 0.80) == pytest.approx(0.80)
    # opposite side of the same market is a distinct key.
    assert dec.anchor_limit(no, 0.30) == pytest.approx(0.30)
    # a different window is a distinct key.
    assert dec.anchor_limit(other_window, 0.70) == pytest.approx(0.70)
    # the original YES anchor is unchanged by the sibling keys.
    assert dec.anchor_limit(yes, 0.99) == pytest.approx(0.80)


def test_anchor_cleared_by_reset() -> None:
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    cell = _v2_decider_cell("BTC", "5m", _WS, "YES")
    dec.anchor_limit(cell, 0.80)
    dec.reset()
    # after reset the next placement re-seeds the anchor (restart semantics).
    assert dec.anchor_limit(cell, 0.85) == pytest.approx(0.85)


# ---------------------------------------------------------------------------
# Placement guard block — money path (mirrors _commit_cell call-for-call)
# ---------------------------------------------------------------------------
def test_flag_off_is_byte_identical() -> None:
    # (1) default 0.0 -> the guard short-circuits -> the bare walked_limit is
    #     sent, exactly as before this feature (no float op touches it).
    assert config.V2_FAK_PAD == 0.0
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    out = _padded_placement_limit(
        dec, walked_limit=0.80, wallet_mode="LIVE", offset_sec=285
    )
    assert out == 0.80  # exact identity, not approx
    assert dec._first_limit == {}  # no anchor recorded while OFF


def test_live_subminute_first_then_retries(monkeypatch) -> None:
    # (2)/(3)/(4) end-to-end through the guard, sharing one side-window anchor.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    # (2) first placement -> walked + pad, and the anchor is set to 0.80.
    assert _padded_placement_limit(
        dec, walked_limit=0.80, wallet_mode="LIVE", offset_sec=285
    ) == pytest.approx(0.81)
    # (3) retry a second later with a HIGHER fresh walk -> capped at first + pad.
    assert _padded_placement_limit(
        dec, walked_limit=0.90, wallet_mode="LIVE", offset_sec=286
    ) == pytest.approx(0.81)
    # (4) retry with a LOWER fresh walk -> cap not binding, pay the lower + pad.
    assert _padded_placement_limit(
        dec, walked_limit=0.70, wallet_mode="LIVE", offset_sec=287
    ) == pytest.approx(0.71)


def test_clip_through_placement(monkeypatch) -> None:
    # (5) clip at 0.99 through the live guard (first attempt, anchor == walked).
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.05)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    assert _padded_placement_limit(
        dec, walked_limit=0.97, wallet_mode="LIVE", offset_sec=285
    ) == pytest.approx(0.99)


def test_dry_mode_never_padded(monkeypatch) -> None:
    # Scope guard: DRY paper rows send the bare walked_limit and record NO anchor.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    assert _padded_placement_limit(
        dec, walked_limit=0.80, wallet_mode="DRY", offset_sec=285
    ) == 0.80
    assert dec._first_limit == {}


def test_minute_boundary_never_padded(monkeypatch) -> None:
    # Scope guard: a minute-boundary cell (offset_sec is None) is not sub-minute.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    assert _padded_placement_limit(
        dec, walked_limit=0.80, wallet_mode="LIVE", offset_sec=None
    ) == 0.80
    assert dec._first_limit == {}


def test_missing_walked_limit_passthrough(monkeypatch) -> None:
    # Scope guard: no executable limit (DRY/legacy walk) -> pass None through.
    monkeypatch.setattr(config, "V2_FAK_PAD", 0.01)
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    assert _padded_placement_limit(
        dec, walked_limit=None, wallet_mode="LIVE", offset_sec=285
    ) is None
    assert dec._first_limit == {}


# ---------------------------------------------------------------------------
# Pruning — the new _first_limit map is evicted with the rest of the state
# ---------------------------------------------------------------------------
def test_prune_drops_stale_first_limit() -> None:
    dec = spec_v2.WindowDecider(consume_on_fill=True)
    now_ts = 2_000_000_000
    cutoff = now_ts - pipeline._V2_DECIDER_RETAIN_SECONDS
    stale_ws = cutoff - 10_000
    fresh_ws = now_ts
    # Exceed the prune threshold so _v2_prune_window_decider actually runs.
    for i in range(pipeline._V2_DECIDER_PRUNE_ABOVE + 5):
        dec._first_limit[("BTC", "5m", stale_ws, f"S{i}")] = 0.80
    dec._first_limit[("BTC", "5m", fresh_ws, "YES")] = 0.81

    # Register the decider so the pipeline prune pass sweeps it (identity-deduped
    # against the MID global, which is empty here), then remove it afterwards.
    pipeline._V2_WINDOW_DECIDERS["__fak_pad_prune_test__"] = dec
    try:
        pipeline._v2_prune_window_decider(now_ts)
    finally:
        pipeline._V2_WINDOW_DECIDERS.pop("__fak_pad_prune_test__", None)

    assert all(k[2] >= cutoff for k in dec._first_limit)  # stale windows evicted
    assert ("BTC", "5m", fresh_ws, "YES") in dec._first_limit  # fresh retained
