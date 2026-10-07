"""First-attempt-only (config.V2_FIRST_ATTEMPT_ONLY, default ON).

Operator order 2026-07-29: a venue no_liquidity reject of a non-top-up
sub-minute attempt is FINAL — the side window is spent and later qualifying
seconds skip instead of retrying (realized retry fills ran -6.2c/bet vs -2.3c
first-try; the dip-refill cohort is WR 52.2% / -33.7c/$1). These tests pin:
(a) flag OFF = byte-identical (records nothing, never skips); (b) flag ON
spends exactly the fast-requote cohort (no_liquidity, non-top-up, offset) and
only the rejected SIDE; (c) the spent set is pruned with the decider maps.
"""
from __future__ import annotations

import pytest

from cryptobot import config
from cryptobot.execution import pipeline


@pytest.fixture(autouse=True)
def _clean_state():
    pipeline._V2_FIRST_ATTEMPT_REJECTED.clear()
    yield
    pipeline._V2_FIRST_ATTEMPT_REJECTED.clear()


def _flag(monkeypatch, on: bool) -> None:
    monkeypatch.setattr(config, "V2_FIRST_ATTEMPT_ONLY", on)


KEY = ("BTC", "5m", 100, "UP")


def _record(coin="BTC", interval="5m", ws=100, side="UP", *,
            error_kind="no_liquidity", is_topup=False, offset_sec=270):
    pipeline._record_first_attempt_reject(
        coin, interval, ws, side,
        error_kind=error_kind, is_topup=is_topup, offset_sec=offset_sec,
    )


def _skip(coin="BTC", interval="5m", ws=100, side="UP", *,
          is_topup=False, offset_sec=270) -> bool:
    return pipeline._first_attempt_consumed_skip(
        coin, interval, ws, side, is_topup=is_topup, offset_sec=offset_sec,
    )


# --- flag OFF => byte-identical (record nothing, never skip) -----------------

def test_flag_off_records_nothing(monkeypatch):
    _flag(monkeypatch, False)
    _record()
    assert pipeline._V2_FIRST_ATTEMPT_REJECTED == set()


def test_flag_off_never_skips_even_with_stale_entry(monkeypatch):
    _flag(monkeypatch, False)
    pipeline._V2_FIRST_ATTEMPT_REJECTED.add(KEY)
    assert _skip() is False


# --- flag ON: recording is scoped to the fast-requote cohort -----------------

def test_records_no_liquidity_offset_reject(monkeypatch):
    _flag(monkeypatch, True)
    _record()
    assert KEY in pipeline._V2_FIRST_ATTEMPT_REJECTED


def test_topup_reject_records_nothing(monkeypatch):
    _flag(monkeypatch, True)
    _record(is_topup=True)
    assert pipeline._V2_FIRST_ATTEMPT_REJECTED == set()


def test_non_offset_reject_records_nothing(monkeypatch):
    _flag(monkeypatch, True)
    _record(offset_sec=None)
    assert pipeline._V2_FIRST_ATTEMPT_REJECTED == set()


def test_transient_error_kind_records_nothing(monkeypatch):
    # Infra/transient rejects carry no price information: retry stays allowed.
    _flag(monkeypatch, True)
    _record(error_kind="rate_limited")
    _record(error_kind=None)
    assert pipeline._V2_FIRST_ATTEMPT_REJECTED == set()


# --- flag ON: the skip decision ----------------------------------------------

def test_spent_side_skips_and_opposite_side_does_not(monkeypatch):
    _flag(monkeypatch, True)
    _record(side="UP")
    assert _skip(side="UP") is True
    assert _skip(side="DOWN") is False


def test_other_window_not_spent(monkeypatch):
    _flag(monkeypatch, True)
    _record(ws=100)
    assert _skip(ws=200) is False


def test_topup_never_skips(monkeypatch):
    # Top-ups of an already-filled position are out of scope even when the
    # side key is spent (they are not reattempts of a missed entry).
    _flag(monkeypatch, True)
    pipeline._V2_FIRST_ATTEMPT_REJECTED.add(KEY)
    assert _skip(is_topup=True) is False


def test_skip_applies_at_any_later_offset(monkeypatch):
    # The spend is per side-WINDOW, not per offset second: every later
    # qualifying second of the same window skips.
    _flag(monkeypatch, True)
    _record(offset_sec=270)
    assert _skip(offset_sec=271) is True
    assert _skip(offset_sec=299) is True


# --- pruning ----------------------------------------------------------------

def test_prune_evicts_resolved_windows(monkeypatch):
    _flag(monkeypatch, True)
    monkeypatch.setattr(pipeline, "_V2_DECIDER_PRUNE_ABOVE", 0)
    now_ts = 1_000_000
    old_ws = now_ts - pipeline._V2_DECIDER_RETAIN_SECONDS - 10
    pipeline._V2_FIRST_ATTEMPT_REJECTED.add(("BTC", "5m", old_ws, "UP"))
    pipeline._V2_FIRST_ATTEMPT_REJECTED.add(("BTC", "5m", now_ts, "UP"))
    pipeline._v2_prune_window_decider(now_ts)
    assert ("BTC", "5m", now_ts, "UP") in pipeline._V2_FIRST_ATTEMPT_REJECTED
    assert ("BTC", "5m", old_ws, "UP") not in pipeline._V2_FIRST_ATTEMPT_REJECTED
