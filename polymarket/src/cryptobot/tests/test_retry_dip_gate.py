"""Retry adverse-dip gate (config.RETRY_DIP_GATE_ENABLED, default OFF).

Ports the proven fast-requote FAST_REQUOTE_MIN_DELTA fence onto the per-second
FAK retry (V2_REJECT_RETRY), which re-enters a rejected offset window with no
adverse-dip guard. The ground-truth "maker-pull chase" loss (-$6.26 era
2026-07-21..24) is that retry filling the cheap-refill cohort BELOW the rejected
limit. These tests pin: (a) flag OFF = byte-identical (records nothing, never
skips); (b) flag ON skips the adverse dip but keeps the +EV flicker-back.
"""
from __future__ import annotations

import pytest

from cryptobot import config
from cryptobot.execution import pipeline


@pytest.fixture(autouse=True)
def _clean_state():
    pipeline._V2_RETRY_DIP_LIMIT.clear()
    yield
    pipeline._V2_RETRY_DIP_LIMIT.clear()


def _flag(monkeypatch, on: bool, min_delta: float = -0.02) -> None:
    monkeypatch.setattr(config, "RETRY_DIP_GATE_ENABLED", on)
    monkeypatch.setattr(config, "FAST_REQUOTE_MIN_DELTA", min_delta)


KEY = ("BTC", "5m", 100, "UP")


# --- flag OFF => byte-identical (record nothing, never skip) -----------------

def test_flag_off_records_nothing(monkeypatch):
    _flag(monkeypatch, False)
    pipeline._record_retry_dip_limit(
        "BTC", "5m", 100, "UP",
        error_kind="no_liquidity", is_topup=False, offset_sec=270,
        limit_price=0.69,
    )
    assert pipeline._V2_RETRY_DIP_LIMIT == {}


def test_flag_off_never_skips_even_with_stale_entry(monkeypatch):
    _flag(monkeypatch, False)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69  # would be adverse if flag were on
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.55, is_topup=False, offset_sec=270,
        )
        is False
    )


# --- flag ON: recording is scoped to the fast-requote cohort -----------------

def test_records_no_liquidity_offset_reject(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._record_retry_dip_limit(
        "BTC", "5m", 100, "UP",
        error_kind="no_liquidity", is_topup=False, offset_sec=270,
        limit_price=0.69,
    )
    assert pipeline._V2_RETRY_DIP_LIMIT[KEY] == pytest.approx(0.69)


def test_topup_reject_records_nothing(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._record_retry_dip_limit(
        "ETH", "5m", 200, "UP",
        error_kind="no_liquidity", is_topup=True, offset_sec=270,
        limit_price=0.70,
    )
    assert ("ETH", "5m", 200, "UP") not in pipeline._V2_RETRY_DIP_LIMIT


def test_non_offset_reject_records_nothing(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._record_retry_dip_limit(
        "ETH", "5m", 200, "UP",
        error_kind="no_liquidity", is_topup=False, offset_sec=None,
        limit_price=0.70,
    )
    assert ("ETH", "5m", 200, "UP") not in pipeline._V2_RETRY_DIP_LIMIT


def test_non_flicker_error_records_nothing(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._record_retry_dip_limit(
        "BTC", "5m", 100, "UP",
        error_kind="rate_limited", is_topup=False, offset_sec=270,
        limit_price=0.69,
    )
    assert pipeline._V2_RETRY_DIP_LIMIT == {}


def test_latest_reject_wins(monkeypatch):
    _flag(monkeypatch, True)
    for lim in (0.69, 0.71):
        pipeline._record_retry_dip_limit(
            "BTC", "5m", 100, "UP",
            error_kind="no_liquidity", is_topup=False, offset_sec=270,
            limit_price=lim,
        )
    assert pipeline._V2_RETRY_DIP_LIMIT[KEY] == pytest.approx(0.71)


# --- flag ON: the gate decision ----------------------------------------------

def test_skips_adverse_dip(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69
    # fresh ask 0.55 < 0.69 - 0.02 = 0.67 -> adverse cheap re-fill -> skip
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.55, is_topup=False, offset_sec=270,
        )
        is True
    )


def test_flicker_back_still_fills(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69
    # 0.68 >= 0.67 -> flicker-back (the +EV cohort) -> no skip
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.68, is_topup=False, offset_sec=270,
        )
        is False
    )
    # exactly at the threshold boundary passes (strict `<`)
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.67, is_topup=False, offset_sec=270,
        )
        is False
    )


def test_topup_never_gated(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.55, is_topup=True, offset_sec=270,
        )
        is False
    )


def test_non_offset_never_gated(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.55, is_topup=False, offset_sec=None,
        )
        is False
    )


def test_nan_ask_no_skip(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", float("nan"), is_topup=False, offset_sec=270,
        )
        is False
    )


def test_no_prior_reject_no_skip(monkeypatch):
    _flag(monkeypatch, True)
    # first entry (nothing recorded) is never gated
    assert (
        pipeline._retry_adverse_dip_skip(
            "BTC", "5m", 100, "UP", 0.10, is_topup=False, offset_sec=270,
        )
        is False
    )


def test_side_and_window_scoped(monkeypatch):
    _flag(monkeypatch, True)
    pipeline._V2_RETRY_DIP_LIMIT[KEY] = 0.69
    # different side / window / coin share no state
    assert not pipeline._retry_adverse_dip_skip(
        "BTC", "5m", 100, "DOWN", 0.55, is_topup=False, offset_sec=270,
    )
    assert not pipeline._retry_adverse_dip_skip(
        "BTC", "5m", 999, "UP", 0.55, is_topup=False, offset_sec=270,
    )
    assert not pipeline._retry_adverse_dip_skip(
        "ETH", "5m", 100, "UP", 0.55, is_topup=False, offset_sec=270,
    )


def test_round_trip_reject_then_gate(monkeypatch):
    """End-to-end helper flow: record a reject, then the next-tick gate fires."""
    _flag(monkeypatch, True)
    pipeline._record_retry_dip_limit(
        "SOL", "15m", 500, "DOWN",
        error_kind="no_liquidity", is_topup=False, offset_sec=870,
        limit_price=0.73,
    )
    # ask collapsed to 0.54 (the verified ETH-style knife) -> skip
    assert pipeline._retry_adverse_dip_skip(
        "SOL", "15m", 500, "DOWN", 0.54, is_topup=False, offset_sec=870,
    )
    # a later flicker back to 0.72 would fill
    assert not pipeline._retry_adverse_dip_skip(
        "SOL", "15m", 500, "DOWN", 0.72, is_topup=False, offset_sec=870,
    )


def test_prune_bounds_the_map(monkeypatch):
    _flag(monkeypatch, True)
    # fill above the prune threshold with old windows, plus a few fresh
    old_ws = 10
    fresh_ws = 10_000_000_000
    for i in range(pipeline._V2_DECIDER_PRUNE_ABOVE + 50):
        pipeline._V2_RETRY_DIP_LIMIT[("BTC", "5m", old_ws + i, "UP")] = 0.5
    pipeline._V2_RETRY_DIP_LIMIT[("BTC", "5m", fresh_ws, "UP")] = 0.5
    pipeline._v2_prune_window_decider(now_ts=fresh_ws + 1)
    # old windows evicted (window_start < cutoff), the fresh one kept
    assert ("BTC", "5m", fresh_ws, "UP") in pipeline._V2_RETRY_DIP_LIMIT
    assert ("BTC", "5m", old_ws, "UP") not in pipeline._V2_RETRY_DIP_LIMIT
