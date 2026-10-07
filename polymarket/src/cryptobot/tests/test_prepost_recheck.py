"""Fix 3a — pre-POST freshness recheck (config.PREPOST_RECHECK_ENABLED).

The seam lives in ``_commit_cell`` right before the LIVE reserve/POST; the
net-new decision logic + shadow-validation counters are the module-level pure
helpers ``_prepost_fresh_cover`` / ``_prepost_recheck`` / ``_prepost_shadow_*``,
tested here directly (the repo's v2-pipeline test idiom — see
``test_v2_pipeline_integration`` — pins the helpers, not a full tick drive).

Coverage mirrors the spec test plan:
  * Flag-OFF byte-identity: the verdict short-circuits to the constant
    ``(False, False, "", None)`` with NO ``book_source`` read.
  * Flag-ON stubbed source: covers -> proceed; doesn't cover -> skip verdict;
    no fresh frame / raise -> fail-open proceed.
  * Correct-side token: the recheck re-reads whatever correct-side token it is
    handed (NO-side reads the NO token/book).
  * Shadow counters: would_skip classified against the real fill/reject.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot.execution import pipeline as P


# --------------------------------------------------------------------------- fixtures

def _sleeve():
    # Attribute surface _execution_min_edge / _execution_max_walk_price read.
    return SimpleNamespace(
        label="v2_5m", threshold=None, min_edge=0.02,
        signal_source="hgb", book_consumption_max=None, max_walk_price=0.05,
    )


def _book(asks, *, token="YES-BTC", fetched_at=1_000.0):
    return BookSnapshot(token_id=token, asks=asks, bids=[], fetched_at_ts=fetched_at)


# Thick cheap book fully covers a $5 stake (walk filled_usd == 5.0); a thin/gone
# book covers 0 (walk None -> 0.0); a partial book covers $3.01.
_THICK = [{"price": 0.70, "size": 1000.0}]
_PARTIAL = [{"price": 0.70, "size": 4.3}]
_GONE: list = []


class _SourceStub:
    """book_prefetch-shaped source: records get() calls, serves a preset snap."""

    def __init__(self, snap, *, raises=False):
        self._snap = snap
        self._raises = raises
        self.get_calls: list[tuple] = []

    def get(self, token, *, now_ts):
        self.get_calls.append((token, now_ts))
        if self._raises:
            raise RuntimeError("boom")
        return self._snap


def _recheck(source, **over):
    """Call _prepost_recheck with sensible LIVE-offset defaults; override any."""
    kw = dict(
        wallet_mode="LIVE",
        offset_sec=285,
        book_source=source,
        order_client=object(),
        token_id="YES-BTC",
        now_ts=1_000,
        ask_book=_book(_THICK),
        walker_target=5.0,
        model_p=0.90,
        sleeve=_sleeve(),
        anchor_for_walker=None,
        proposed_bet_size=5.0,
    )
    kw.update(over)
    return P._prepost_recheck(**kw)


@pytest.fixture(autouse=True)
def _reset_shadow_counts():
    for d in (P._PREPOST_SHADOW_COUNTS, P._PREPOST_DROP_SHADOW_COUNTS):
        for k in d:
            d[k] = 0
    yield
    for d in (P._PREPOST_SHADOW_COUNTS, P._PREPOST_DROP_SHADOW_COUNTS):
        for k in d:
            d[k] = 0


# --------------------------------------------------------------------------- flag defaults

def test_flag_defaults_off():
    assert config.PREPOST_RECHECK_ENABLED is False
    assert config.PREPOST_RECHECK_MIN_COVER_FRAC == 1.0
    # Drop-guard extension: default OFF, damage-floor threshold.
    assert config.PREPOST_DROP_GUARD_MODE == "off"
    assert config.PREPOST_DROP_GUARD_THRESH == 0.02


# --------------------------------------------------------------------------- flag-OFF byte-identity

def test_flag_off_no_book_source_read():
    """Default OFF (recheck disabled + WS shadow off) -> the constant
    (False, False, "", None) and the book source is NEVER consulted — the hard
    byte-identity invariant."""
    assert not config.PREPOST_RECHECK_ENABLED
    src = _SourceStub(_book(_GONE))  # would say "skip" if it were ever read
    assert _recheck(src) == (False, False, "", None)
    assert src.get_calls == []


def test_shadow_inactive_when_ws_shadow_off(monkeypatch):
    # WS enabled but SHADOW off => not shadow-active => no read while recheck off.
    monkeypatch.setattr(config, "BOOK_WS_ENABLED", True)
    monkeypatch.setattr(config, "BOOK_WS_SHADOW", False)
    src = _SourceStub(_book(_GONE))
    assert _recheck(src) == (False, False, "", None)
    assert src.get_calls == []


# --------------------------------------------------------------------------- enabled: covers / skips

def test_enabled_covers_proceeds(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    src = _SourceStub(_book(_THICK))
    # fresh book still covers -> go; the fresh best ask (0.70) rides the 4-tuple.
    assert _recheck(src) == (False, False, "", pytest.approx(0.70))
    assert src.get_calls == [("YES-BTC", 1_000.0)]  # correct-side token re-read


def test_enabled_does_not_cover_skips(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    # Fresh book gone -> coverable 0.0 < 5.0 -> real skip (cover reason); empty
    # ask side => fresh ask None.
    assert _recheck(_SourceStub(_book(_GONE))) == (True, False, "cover", None)
    # Partial fresh book (covers $3.01 < $5) -> real skip at MIN_COVER_FRAC 1.0.
    assert _recheck(_SourceStub(_book(_PARTIAL))) == (
        True, False, "cover", pytest.approx(0.70)
    )


def test_min_cover_frac_tolerates_partial(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    monkeypatch.setattr(config, "PREPOST_RECHECK_MIN_COVER_FRAC", 0.5)
    # Partial covers $3.01 >= 5.0*0.5=2.5 -> proceed.
    assert _recheck(_SourceStub(_book(_PARTIAL))) == (
        False, False, "", pytest.approx(0.70)
    )
    # Gone covers 0.0 < 2.5 -> skip.
    assert _recheck(_SourceStub(_book(_GONE))) == (True, False, "cover", None)


# --------------------------------------------------------------------------- fail-open

def test_fail_open_no_source(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    # nothing wired -> proceed
    assert _recheck(None) == (False, False, "", None)


def test_fail_open_no_fresh_frame(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    src = _SourceStub(None)  # get() returns None (no fresh frame)
    assert _recheck(src) == (False, False, "", None)
    assert src.get_calls == [("YES-BTC", 1_000.0)]  # it DID try, then failed open


def test_fail_open_source_raises(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    assert _recheck(_SourceStub(None, raises=True)) == (False, False, "", None)


# --------------------------------------------------------------------------- scope guards

def test_dry_never_rechecks(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    src = _SourceStub(_book(_GONE))
    assert _recheck(src, wallet_mode="DRY") == (False, False, "", None)
    assert src.get_calls == []  # DRY paper path is untouched


def test_minute_boundary_never_rechecks(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    src = _SourceStub(_book(_GONE))
    # not sub-minute
    assert _recheck(src, offset_sec=None) == (False, False, "", None)
    assert src.get_calls == []


def test_no_order_client_never_rechecks(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    src = _SourceStub(_book(_GONE))
    # A LIVE wallet with no order_client falls to the DRY paper path (no POST).
    assert _recheck(src, order_client=None) == (False, False, "", None)
    assert src.get_calls == []


# --------------------------------------------------------------------------- correct-side token

def test_no_side_reads_no_token(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    # A NO-side cell passes its OWN (NO) token; the recheck re-reads the NO book.
    src = _SourceStub(_book(_GONE, token="NO-BTC"))
    assert _recheck(src, token_id="NO-BTC") == (True, False, "cover", None)
    assert src.get_calls == [("NO-BTC", 1_000.0)]


# --------------------------------------------------------------------------- shadow mode

def test_shadow_would_skip_does_not_suppress(monkeypatch):
    # Recheck DISABLED but WS shadow ON: a would-skip must NOT skip (proceed),
    # only flag shadow_would_skip for post-POST classification.
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", False)
    monkeypatch.setattr(config, "BOOK_WS_ENABLED", True)
    monkeypatch.setattr(config, "BOOK_WS_SHADOW", True)
    # would-skip (gone book -> fresh ask None)
    assert _recheck(_SourceStub(_book(_GONE))) == (False, True, "cover", None)
    # would-not-skip (thick book covers -> fresh ask 0.70)
    assert _recheck(_SourceStub(_book(_THICK))) == (
        False, False, "", pytest.approx(0.70)
    )


# --------------------------------------------------------------------------- _prepost_fresh_cover

def test_fresh_cover_walkable_returns_cover():
    src = _SourceStub(_book(_THICK))
    cover, fresh_ask = P._prepost_fresh_cover(
        src, "YES-BTC", 1_000, ask_book=_book(_THICK), walker_target=5.0,
        model_p=0.90, sleeve=_sleeve(), anchor_for_walker=None,
    )
    assert cover == pytest.approx(5.0)
    assert fresh_ask == pytest.approx(0.70)  # fresh book best ask surfaced too


def test_fresh_cover_gone_book_returns_zero_not_none():
    # A fresh frame that is present but un-walkable => cover 0.0 (a genuine
    # "gone"), NOT None — only a missing frame fails open. An empty ask side
    # has no best ask => fresh_ask None.
    cover, fresh_ask = P._prepost_fresh_cover(
        _SourceStub(_book(_GONE)), "YES-BTC", 1_000, ask_book=_book(_GONE),
        walker_target=5.0, model_p=0.90, sleeve=_sleeve(), anchor_for_walker=None,
    )
    assert cover == 0.0
    assert fresh_ask is None


def test_fresh_cover_fail_open_paths():
    kw = dict(
        token_id="YES-BTC", now_ts=1_000, ask_book=_book(_THICK),
        walker_target=5.0, model_p=0.90, sleeve=_sleeve(), anchor_for_walker=None,
    )
    assert P._prepost_fresh_cover(None, **kw) == (None, None)            # no source
    assert P._prepost_fresh_cover(_SourceStub(None), **kw) == (None, None)  # no frame
    assert P._prepost_fresh_cover(  # source raises
        _SourceStub(None, raises=True), **kw
    ) == (None, None)
    # empty token id fails open too
    assert P._prepost_fresh_cover(
        _SourceStub(_book(_THICK)), **{**kw, "token_id": ""}
    ) == (None, None)


# --------------------------------------------------------------------------- shadow counters

def test_shadow_classify_true_and_false_positive():
    # A would-skip that really REJECTED = true positive.
    P._prepost_shadow_classify(filled=False, coin="BTC", sleeve="v2_5m", offset_sec=285)
    # A would-skip that really FILLED = false positive (the gate metric).
    P._prepost_shadow_classify(filled=True, coin="ETH", sleeve="v2_5m", offset_sec=286)
    assert P._PREPOST_SHADOW_COUNTS == {
        "would_skip": 2,
        "would_skip_rejected": 1,
        "would_skip_filled": 1,
    }


def test_shadow_active_predicate(monkeypatch):
    monkeypatch.setattr(config, "BOOK_WS_ENABLED", True)
    monkeypatch.setattr(config, "BOOK_WS_SHADOW", True)
    assert P._prepost_shadow_active() is True
    monkeypatch.setattr(config, "BOOK_WS_SHADOW", False)
    assert P._prepost_shadow_active() is False


# --------------------------------------------------------------------------- drop guard
#
# Feature 2: the TOXIC direction — the fresh best ask DROPPED below the decision
# ask (book collapsing mid-flight). Fresh books below use a THICK ladder at the
# target price so the COVER guard never fires (cover == 5.0) — that isolates the
# drop verdict. Decision ask is 0.70; thresh default 0.02 (the damage floor).

def _fresh_ask_book(price):
    """A thick fresh book whose best ask is `price` (covers the $5 stake fully,
    so only the drop guard can trip)."""
    return _book([{"price": price, "size": 1000.0}])


def test_drop_guard_default_off_no_book_read():
    # Byte-identity: with ONLY defaults (cover off, WS shadow off, drop off) a
    # would-be 10c drop is never even read — the guard short-circuits.
    assert config.PREPOST_DROP_GUARD_MODE == "off"
    src = _SourceStub(_fresh_ask_book(0.60))  # huge drop, would trip if read
    assert _recheck(src, decision_top_ask=0.70) == (False, False, "", None)
    assert src.get_calls == []


def test_drop_guard_live_aborts_on_2c_drop(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "live")
    # 0.70 -> 0.68 == 2c drop == thresh -> LIVE abort, reason "drop".
    src = _SourceStub(_fresh_ask_book(0.68))
    assert _recheck(src, decision_top_ask=0.70) == (
        True, False, "drop", pytest.approx(0.68)
    )


def test_drop_guard_no_abort_just_below_thresh(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "live")
    # 0.70 -> 0.681 == 1.9c drop < 2c -> proceed (do NOT block 1c wiggles).
    src = _SourceStub(_fresh_ask_book(0.681))
    assert _recheck(src, decision_top_ask=0.70) == (
        False, False, "", pytest.approx(0.681)
    )


def test_drop_guard_shadow_flags_without_suppressing(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "shadow")
    src = _SourceStub(_fresh_ask_book(0.68))
    # Shadow: never suppresses, only flags the drop verdict.
    assert _recheck(src, decision_top_ask=0.70) == (
        False, True, "drop", pytest.approx(0.68)
    )


def test_drop_guard_fail_open_when_no_fresh_ask(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "live")
    # Gone book -> fresh_top_ask None -> no drop verdict (cover guard is OFF so
    # the under-cover does not abort either) -> proceed.
    assert _recheck(_SourceStub(_book(_GONE)), decision_top_ask=0.70) == (
        False, False, "", None,
    )


def test_drop_guard_none_decision_ask_no_verdict(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "live")
    src = _SourceStub(_fresh_ask_book(0.60))  # big drop, but no decision ask
    # No verdict (decision ask None), but the fresh ask still rides the tuple.
    assert _recheck(src, decision_top_ask=None) == (
        False, False, "", pytest.approx(0.60)
    )


def test_drop_guard_pad_direction_never_triggers(monkeypatch):
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "live")
    # Fresh ask ROSE (0.72 > 0.70) -> negative drop -> never trips.
    src = _SourceStub(_fresh_ask_book(0.72))
    assert _recheck(src, decision_top_ask=0.70) == (
        False, False, "", pytest.approx(0.72)
    )


def test_drop_guard_returns_fresh_ask(monkeypatch):
    # The 4th tuple element carries the observed fresh best ask so the caller can
    # log it in the drop-guard shadow line (replaces the old capture-list side
    # channel — no redundant second book read).
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "shadow")
    result = P._prepost_recheck(
        wallet_mode="LIVE", offset_sec=285,
        book_source=_SourceStub(_fresh_ask_book(0.68)), order_client=object(),
        token_id="YES-BTC", now_ts=1_000, ask_book=_book(_THICK),
        walker_target=5.0, model_p=0.90, sleeve=_sleeve(), anchor_for_walker=None,
        proposed_bet_size=5.0, decision_top_ask=0.70,
    )
    assert result == (False, True, "drop", pytest.approx(0.68))


def test_cover_precedence_over_drop(monkeypatch):
    # A book that BOTH under-covers ($2.40 < $5) AND dropped (0.60 << 0.70):
    # cover guard ENABLED + drop LIVE both fire -> cover wins.
    monkeypatch.setattr(config, "PREPOST_RECHECK_ENABLED", True)
    monkeypatch.setattr(config, "PREPOST_DROP_GUARD_MODE", "live")
    src = _SourceStub(_book([{"price": 0.60, "size": 4.0}]))
    assert _recheck(src, decision_top_ask=0.70) == (
        True, False, "cover", pytest.approx(0.60)
    )


def test_drop_shadow_classify_uses_drop_dict():
    # guard="drop" writes the drop ledger with NEUTRAL outcome labels
    # (filled/rejected), not the cover ledger's true_pos/false_pos framing.
    P._prepost_shadow_classify(
        filled=True, coin="BTC", sleeve="v2_5m", offset_sec=285,
        guard="drop", decision_ask=0.70, fresh_ask=0.68,
    )
    assert P._PREPOST_DROP_SHADOW_COUNTS == {
        "would_skip": 1, "filled": 1, "rejected": 0,
    }
    # The cover ledger is untouched.
    assert P._PREPOST_SHADOW_COUNTS == {
        "would_skip": 0, "would_skip_rejected": 0, "would_skip_filled": 0,
    }
