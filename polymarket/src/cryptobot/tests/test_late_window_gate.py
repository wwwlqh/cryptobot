"""Feature 1 — late-window entry skip gate (config.LATE_WINDOW_SKIP).

Two pure surfaces, tested directly (the repo idiom — see test_prepost_recheck):
  * ``config._parse_late_window_skip`` — the COIN:SECONDS env parser, incl.
    malformed-token fail-open (a typo must never wipe the map or suppress an
    order).
  * ``pipeline._late_window_skip`` — the per-cell skip predicate (now the single
    authority for the gate's whole scope: default (empty dict) is byte-identical
    zero-work; only a LIVE offset cell whose configured coin lands inside its
    last-N-seconds cutoff skips; DRY, minute-boundary (offset None), and
    unconfigured coins never skip).
"""
from __future__ import annotations

from cryptobot import config
from cryptobot.execution import pipeline as P


# --------------------------------------------------------------------------- parser

def test_parse_empty_is_empty_dict():
    assert config._parse_late_window_skip("") == {}
    assert config._parse_late_window_skip("   ") == {}
    assert config._parse_late_window_skip(None) == {}  # defensive: None -> {}


def test_parse_single_entry():
    assert config._parse_late_window_skip("BTC:20") == {"BTC": 20}


def test_parse_multi_and_upper_cases_coins():
    # Lower-case coin is upper-cased; whitespace tolerated; multiple entries.
    assert config._parse_late_window_skip("btc:20,ETH:10") == {"BTC": 20, "ETH": 10}
    assert config._parse_late_window_skip(" btc : 20 , eth : 10 ") == {
        "BTC": 20, "ETH": 10,
    }


def test_parse_zero_cutoff_is_valid():
    # 0 is a legitimate cutoff (skip only the exact final second).
    assert config._parse_late_window_skip("BTC:0") == {"BTC": 0}


def test_parse_malformed_tokens_ignored_keep_rest():
    # "BTC" (no colon) is dropped; "ETH:x" (non-int) is dropped; the valid
    # "XRP:5" survives — one typo can neither wipe the map nor suppress orders.
    assert config._parse_late_window_skip("BTC") == {}
    assert config._parse_late_window_skip("BTC:x") == {}
    assert config._parse_late_window_skip("BTC:20,ETH:x,XRP:5") == {
        "BTC": 20, "XRP": 5,
    }
    # Blank coin and negative cutoff are both ignored.
    assert config._parse_late_window_skip(":20") == {}
    assert config._parse_late_window_skip("BTC:-5") == {}
    # Trailing / empty tokens ignored.
    assert config._parse_late_window_skip("BTC:20,,") == {"BTC": 20}


def test_module_default_is_empty():
    # Unpinned deploy => empty (falsy) dict => byte-identical.
    assert config.LATE_WINDOW_SKIP == {}


# --------------------------------------------------------------------------- predicate

def test_predicate_default_empty_never_skips():
    # Falsy dict short-circuits before any lookup — the byte-identical guard.
    assert config.LATE_WINDOW_SKIP == {}
    assert P._late_window_skip(
        "BTC", offset_sec=299, interval_min=5, wallet_mode="LIVE"
    ) is False


def test_predicate_skips_inside_cutoff(monkeypatch):
    # 5m window = 300s. offset 285 => 300-285 = 15s to close <= 20 => SKIP.
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    assert P._late_window_skip(
        "BTC", offset_sec=285, interval_min=5, wallet_mode="LIVE"
    ) is True


def test_predicate_boundary_exactly_at_cutoff(monkeypatch):
    # 300-280 = 20 <= 20 => SKIP (inclusive boundary).
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    assert P._late_window_skip(
        "BTC", offset_sec=280, interval_min=5, wallet_mode="LIVE"
    ) is True
    # 300-279 = 21 > 20 => proceed.
    assert P._late_window_skip(
        "BTC", offset_sec=279, interval_min=5, wallet_mode="LIVE"
    ) is False


def test_predicate_other_coin_not_configured(monkeypatch):
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    # ETH has no cutoff -> never skips, even deep in the late window.
    assert P._late_window_skip(
        "ETH", offset_sec=299, interval_min=5, wallet_mode="LIVE"
    ) is False


def test_predicate_coin_case_insensitive(monkeypatch):
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    assert P._late_window_skip(
        "btc", offset_sec=290, interval_min=5, wallet_mode="LIVE"
    ) is True


def test_predicate_minute_boundary_offset_none(monkeypatch):
    # A minute-boundary cell (no sub-second offset) is never a late-window skip.
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    assert P._late_window_skip(
        "BTC", offset_sec=None, interval_min=5, wallet_mode="LIVE"
    ) is False


def test_predicate_dry_never_skips(monkeypatch):
    # Research-parity invariant: even a configured coin deep in the late window
    # NEVER skips in DRY (the gate is LIVE-only; DRY paper rows are untouched).
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    assert P._late_window_skip(
        "BTC", offset_sec=290, interval_min=5, wallet_mode="DRY"
    ) is False


ARMED_20260801 = "BTC:5,ETH:5,SOL:5,XRP:5"


def test_armed_20260801_all_coin_five_second_map(monkeypatch):
    """The value install_remote.sh force-pins from 2026-08-01.

    All four coins at 5s: the robust half of the LAST-SECONDS forensic (the
    <=10s cohort is bigger but outlier-sensitive and stays unarmed). The
    backtest twin mirrors the same boundary — see
    polymarket/backtest/tests/test_tick_late_skip.py.
    """
    parsed = config._parse_late_window_skip(ARMED_20260801)
    assert parsed == {"BTC": 5, "ETH": 5, "SOL": 5, "XRP": 5}
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", parsed)

    for coin in ("BTC", "ETH", "SOL", "XRP"):
        # 5m window = 300s: the last five seconds (295..299) are skipped...
        for offset in (295, 296, 297, 298, 299):
            assert P._late_window_skip(
                coin, offset_sec=offset, interval_min=5, wallet_mode="LIVE"
            ) is True
        # ...and 294 (6s to close) still trades.
        assert P._late_window_skip(
            coin, offset_sec=294, interval_min=5, wallet_mode="LIVE"
        ) is False

        # 15m window = 900s: same boundary, 895..899 skipped, 894 trades.
        for offset in (895, 896, 897, 898, 899):
            assert P._late_window_skip(
                coin, offset_sec=offset, interval_min=15, wallet_mode="LIVE"
            ) is True
        assert P._late_window_skip(
            coin, offset_sec=894, interval_min=15, wallet_mode="LIVE"
        ) is False


def test_armed_20260801_still_live_only(monkeypatch):
    """Arming the gate does not change the DRY invariant — paper keeps booking."""
    monkeypatch.setattr(
        config, "LATE_WINDOW_SKIP", config._parse_late_window_skip(ARMED_20260801)
    )
    assert P._late_window_skip(
        "SOL", offset_sec=299, interval_min=5, wallet_mode="DRY"
    ) is False


def test_predicate_early_window_proceeds(monkeypatch):
    # 15m window = 900s. offset 30 => 900-30 = 870s to close >> 20 => proceed.
    monkeypatch.setattr(config, "LATE_WINDOW_SKIP", {"BTC": 20})
    assert P._late_window_skip(
        "BTC", offset_sec=30, interval_min=15, wallet_mode="LIVE"
    ) is False


# ------------------------------------------------------------- early-window gate

def test_early_window_default_off_never_skips(monkeypatch):
    monkeypatch.setattr(config, "ENTRY_LAST_SECS_ONLY", 0)
    assert not P._early_window_skip(
        offset_sec=270, interval_min=5, wallet_mode="LIVE"
    )


def test_early_window_skips_outside_last_n(monkeypatch):
    monkeypatch.setattr(config, "ENTRY_LAST_SECS_ONLY", 15)
    # 5m: offset 284 -> 16s to close -> too early; 285 -> exactly 15s -> allowed.
    assert P._early_window_skip(offset_sec=284, interval_min=5, wallet_mode="LIVE")
    assert not P._early_window_skip(offset_sec=285, interval_min=5, wallet_mode="LIVE")
    assert not P._early_window_skip(offset_sec=294, interval_min=5, wallet_mode="LIVE")
    # 15m: offset 884 -> 16s -> too early; 885 -> 15s -> allowed.
    assert P._early_window_skip(offset_sec=884, interval_min=15, wallet_mode="LIVE")
    assert not P._early_window_skip(offset_sec=885, interval_min=15, wallet_mode="LIVE")


def test_early_window_live_only_and_offset_none(monkeypatch):
    monkeypatch.setattr(config, "ENTRY_LAST_SECS_ONLY", 15)
    # DRY paper rows keep research parity (mirrors the late gate).
    assert not P._early_window_skip(offset_sec=270, interval_min=5, wallet_mode="DRY")
    # Minute-boundary cell with no sub-second offset is never a skip.
    assert not P._early_window_skip(offset_sec=None, interval_min=5, wallet_mode="LIVE")
