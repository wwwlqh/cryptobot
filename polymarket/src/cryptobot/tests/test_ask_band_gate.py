"""Ask-band entry skip gate (config.ASK_BAND_SKIP).

Two pure surfaces, tested directly (the repo idiom — see test_late_window_gate):
  * ``config._parse_ask_band_skip`` — the COIN:SLEEVE:LO-HI env parser, incl.
    malformed-rule fail-open + WARNING (a typo must never wipe the spec, never
    fail closed into "skip everything", and never pass silently).
  * ``pipeline._ask_band_skip`` — the per-cell skip predicate (the single
    authority for the gate's whole scope: default (empty tuple) is
    byte-identical zero-work; only a configured (coin, sleeve-prefix, band)
    match skips; DRY-invariant by construction).

Motivating measurement (2026-07-20 live ledger): SOL clean fills in the
0.65-0.85 band went WR 87.3% -> 69.5% / +6.85c/$ -> -20.44c/$ across the 07-17
halt while ETH and BTC HELD in the same band on the same days. Damage is
localized to SOL's sub-minute sleeves inside that band — SOL ask >= 0.85 (100%
WR) must keep trading, which is what the sleeve and band selectors below pin.
"""
from __future__ import annotations

import logging

from cryptobot import config
from cryptobot.execution import pipeline as P

RULE = config.AskBandSkipRule
SOL_SUBMINUTE = (RULE(coin="SOL", sleeve="subminute", lo=0.65, hi=0.85),)


# --------------------------------------------------------------------------- parser

def test_parse_empty_is_empty_tuple():
    assert config._parse_ask_band_skip("") == ()
    assert config._parse_ask_band_skip("   ") == ()
    assert config._parse_ask_band_skip(None) == ()  # defensive: None -> ()


def test_parse_single_rule():
    assert config._parse_ask_band_skip("SOL:subminute:0.65-0.85") == SOL_SUBMINUTE


def test_parse_normalizes_case_and_whitespace():
    # Coin upper-cased, sleeve lower-cased, whitespace tolerated everywhere.
    assert config._parse_ask_band_skip(" sol : SubMinute : 0.65 - 0.85 ") == (
        RULE(coin="SOL", sleeve="subminute", lo=0.65, hi=0.85),
    )


def test_parse_multi_rule():
    assert config._parse_ask_band_skip(
        "SOL:subminute:0.65-0.85,ETH:alt_5m:0.70-0.80"
    ) == (
        RULE(coin="SOL", sleeve="subminute", lo=0.65, hi=0.85),
        RULE(coin="ETH", sleeve="alt_5m", lo=0.70, hi=0.80),
    )


def test_parse_wildcard_sleeve():
    assert config._parse_ask_band_skip("SOL:*:0.65-0.85") == (
        RULE(coin="SOL", sleeve="*", lo=0.65, hi=0.85),
    )


def test_parse_malformed_rules_ignored_keep_rest(caplog):
    """A typo drops ONLY its own rule — never the spec, never an order."""
    with caplog.at_level(logging.WARNING, logger="cryptobot.config"):
        got = config._parse_ask_band_skip(
            "SOL:subminute,"          # too few fields
            "SOL:subminute:0.65:0.85,"  # too many fields
            "SOL:subminute:0.65,"     # band missing the "-"
            "SOL:subminute:lo-hi,"    # non-numeric band
            ":subminute:0.65-0.85,"   # blank coin
            "SOL::0.65-0.85,"         # blank sleeve
            "SOL:subminute:0.85-0.65,"  # inverted band
            "SOL:subminute:0.65-1.5,"   # band above 1.0
            "SOL:subminute:-0.1-0.85,"  # band below 0.0
            "ETH:alt_5m:0.70-0.80"     # the one valid rule survives
        )
    assert got == (RULE(coin="ETH", sleeve="alt_5m", lo=0.70, hi=0.80),)
    # Every dropped rule warns — a silently-ignored typo would leave the
    # operator believing an adverse band is gated while it still trades.
    assert len(caplog.records) == 9


def test_parse_blank_tokens_are_not_warned(caplog):
    # A trailing/empty comma is punctuation, not a typo: dropped without noise.
    with caplog.at_level(logging.WARNING, logger="cryptobot.config"):
        assert config._parse_ask_band_skip("SOL:subminute:0.65-0.85,,") == SOL_SUBMINUTE
    assert caplog.records == []


def test_parse_degenerate_band_lo_equals_hi_is_valid():
    # lo == hi is legal: with the (lo, hi] convention it matches exactly nothing,
    # which is a harmless no-op rather than an operator error.
    assert config._parse_ask_band_skip("SOL:subminute:0.65-0.65") == (
        RULE(coin="SOL", sleeve="subminute", lo=0.65, hi=0.65),
    )


def test_module_default_is_empty():
    # Unpinned deploy => empty (falsy) tuple => byte-identical.
    assert config.ASK_BAND_SKIP == ()


# --------------------------------------------------------------------------- predicate

def test_predicate_default_empty_never_skips():
    # Falsy tuple short-circuits before any comparison — the byte-identical
    # regression guard: with the flag unset the gate is a pure no-op for every
    # coin/sleeve/ask combination.
    assert config.ASK_BAND_SKIP == ()
    for coin in ("SOL", "ETH", "BTC"):
        for sleeve in ("subminute_5m", "subminute_15m", "alt_5m", "alt_15m"):
            for ask in (0.60, 0.65, 0.75, 0.85, 0.90):
                assert P._ask_band_skip(coin, sleeve, ask) is False


def test_predicate_skips_sol_subminute_in_band(monkeypatch):
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("SOL", "subminute_5m", 0.75) is True


def test_predicate_sleeve_prefix_matches_both_subminute_sleeves(monkeypatch):
    # "subminute" must cover BOTH sleeves the study found damaged.
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("SOL", "subminute_5m", 0.75) is True
    assert P._ask_band_skip("SOL", "subminute_15m", 0.75) is True


def test_predicate_non_subminute_sleeve_not_skipped(monkeypatch):
    # A sleeve whose label does not start with the configured prefix is out of
    # the rule's scope — the sleeve selector must spare it.
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("SOL", "alt_5m", 0.75) is False
    assert P._ask_band_skip("SOL", "alt_15m", 0.75) is False


def test_predicate_above_band_not_skipped(monkeypatch):
    # SOL ask >= 0.85 measured 100% WR — the band ceiling must spare it.
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("SOL", "subminute_5m", 0.90) is False


def test_predicate_other_coin_not_skipped(monkeypatch):
    # ETH/BTC HELD in the same band on the same days — coin selector spares them.
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("ETH", "subminute_5m", 0.75) is False
    assert P._ask_band_skip("BTC", "subminute_5m", 0.75) is False


def test_predicate_band_boundaries_open_low_closed_high(monkeypatch):
    """Convention pinned to spec_v2.WindowDecider._in_band: ``lo < ask <= hi``.

    The strategy's MID band is documented ``(0.65, 0.85]``, so an ask of exactly
    0.65 is BELOW the band (trades) and exactly 0.85 is INSIDE it (skips).
    """
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("SOL", "subminute_5m", 0.65) is False   # open low
    assert P._ask_band_skip("SOL", "subminute_5m", 0.85) is True    # closed high
    # Just inside / just outside either edge.
    assert P._ask_band_skip("SOL", "subminute_5m", 0.6501) is True
    assert P._ask_band_skip("SOL", "subminute_5m", 0.8501) is False


def test_predicate_coin_and_sleeve_case_insensitive(monkeypatch):
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("sol", "SUBMINUTE_5M", 0.75) is True


def test_predicate_wildcard_sleeve_matches_all(monkeypatch):
    monkeypatch.setattr(
        config, "ASK_BAND_SKIP",
        (RULE(coin="SOL", sleeve="*", lo=0.65, hi=0.85),),
    )
    for sleeve in ("subminute_5m", "subminute_15m", "alt_5m", "alt_15m"):
        assert P._ask_band_skip("SOL", sleeve, 0.75) is True
    # Still scoped to the coin and the band.
    assert P._ask_band_skip("ETH", "alt_5m", 0.75) is False
    assert P._ask_band_skip("SOL", "alt_5m", 0.90) is False


def test_predicate_exact_sleeve_name_matches_only_itself(monkeypatch):
    monkeypatch.setattr(
        config, "ASK_BAND_SKIP",
        (RULE(coin="SOL", sleeve="subminute_5m", lo=0.65, hi=0.85),),
    )
    assert P._ask_band_skip("SOL", "subminute_5m", 0.75) is True
    assert P._ask_band_skip("SOL", "subminute_15m", 0.75) is False


def test_predicate_multi_rule_matches_any(monkeypatch):
    monkeypatch.setattr(
        config, "ASK_BAND_SKIP",
        config._parse_ask_band_skip(
            "SOL:subminute:0.65-0.85,ETH:alt_5m:0.70-0.80"
        ),
    )
    assert P._ask_band_skip("SOL", "subminute_15m", 0.75) is True
    assert P._ask_band_skip("ETH", "alt_5m", 0.75) is True
    # Neither rule's cross-product leaks.
    assert P._ask_band_skip("ETH", "subminute_5m", 0.75) is False
    assert P._ask_band_skip("SOL", "alt_5m", 0.75) is False


def test_predicate_missing_or_nonfinite_ask_fails_open(monkeypatch):
    # `top_ask` is float("nan") when the book has no ask — an unknown price must
    # never suppress an order.
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    assert P._ask_band_skip("SOL", "subminute_5m", None) is False
    assert P._ask_band_skip("SOL", "subminute_5m", float("nan")) is False
    assert P._ask_band_skip("SOL", "subminute_5m", float("inf")) is False


def test_predicate_malformed_spec_ignored_entry_proceeds(monkeypatch):
    # End-to-end fail-open: a fully malformed env parses to () and the gate is a
    # no-op — it must NEVER fail closed into "skip everything".
    monkeypatch.setattr(
        config, "ASK_BAND_SKIP", config._parse_ask_band_skip("SOL-subminute-0.65"),
    )
    assert config.ASK_BAND_SKIP == ()
    assert P._ask_band_skip("SOL", "subminute_5m", 0.75) is False


def test_predicate_dry_invariant(monkeypatch):
    """DRY-invariance is STRUCTURAL: the predicate takes no wallet_mode.

    This gate removes a cohort whose EDGE was refuted on the live ledger (not an
    execution-timing tweak like `_late_window_skip`, which is deliberately
    LIVE-only), so the DRY paper stream must reach the identical verdict rather
    than keep booking edge the live book no longer has.
    """
    import inspect

    params = inspect.signature(P._ask_band_skip).parameters
    assert "wallet_mode" not in params
    monkeypatch.setattr(config, "ASK_BAND_SKIP", SOL_SUBMINUTE)
    # Same inputs, one verdict — there is no mode dimension to diverge on.
    assert P._ask_band_skip("SOL", "subminute_5m", 0.75) is True
