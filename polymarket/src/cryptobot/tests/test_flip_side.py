"""SUBMINUTE_FLIP_SIDE (operator order 2026-08-07): bet against the model.

Covers the two seams the flag touches:
  * score_cell_hgb — selection runs on the model's side, only the RETURNED
    side is inverted; model_p/edge stay the model side's numbers.
  * _execution_max_walk_price_for — returns the walker's RELATIVE increment;
    under the flip it is min(sleeve leash, ceiling - flip_top_ask) with
    ceiling = (1 - model_side_ask) + SUBMINUTE_FLIP_PAD, and the -1.0
    SENTINEL (caller skips, fail-closed) when either book is unreadable or
    the flip top already prices above the ceiling. The pre-2026-08-08 code
    compared the ABSOLUTE ceiling against the relative 5c leash, which left
    the cap permanently inert — these tests pin the converted semantics.
"""

from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from cryptobot import config
from cryptobot.execution.pipeline import (
    _execution_max_walk_price,
    _execution_max_walk_price_for,
)
from cryptobot.strategy.score import score_cell_hgb
from cryptobot.strategy.portfolio_loader import V6Portfolio, load_sleeves
from cryptobot.tests._artifact_skips import needs_checkpoint
from cryptobot.tests.test_hgb_scoring import (  # constants + engine helper only
    _OFF5,
    _SEED_TS,
    _V2_CONFIG,
    _warm_engine,
)


# Fixtures mirror test_hgb_scoring's (pytest fixtures don't import across
# modules) — same checkpoint bundle, same deployed v2 config.
@pytest.fixture(scope="module")
def _ckpt_models():
    from cryptobot.strategy.checkpoint_loader import load_checkpoint_models

    return load_checkpoint_models()


@pytest.fixture(scope="module")
def rf(_ckpt_models):
    return _ckpt_models[0]


@pytest.fixture(scope="module")
def hgb(_ckpt_models):
    return _ckpt_models[1]


@pytest.fixture(scope="module")
def s5():
    prev = config.SUBMINUTE_ENABLED
    config.SUBMINUTE_ENABLED = True
    try:
        portfolio: V6Portfolio = load_sleeves(_V2_CONFIG)
    finally:
        config.SUBMINUTE_ENABLED = prev
    return next(s for s in portfolio.sleeves if s.interval == "5m")


@pytest.fixture()
def flip_on(monkeypatch):
    monkeypatch.setattr(config, "SUBMINUTE_FLIP_SIDE", True)


class TestScorerFlip:
    @needs_checkpoint
    def test_flip_inverts_side_only(self, rf, hgb, s5, monkeypatch) -> None:
        kwargs = dict(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.75, no_ask=0.70,
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        monkeypatch.setattr(config, "SUBMINUTE_FLIP_SIDE", False)
        base = score_cell_hgb(partial_ret_engine=_warm_engine(), **kwargs)
        assert base.skip_reason is None

        monkeypatch.setattr(config, "SUBMINUTE_FLIP_SIDE", True)
        flipped = score_cell_hgb(partial_ret_engine=_warm_engine(), **kwargs)
        assert flipped.skip_reason is None
        assert flipped.side == ("NO" if base.side == "YES" else "YES")
        # Provenance trap by design: model numbers describe the MODEL side.
        assert flipped.model_p == pytest.approx(base.model_p, abs=1e-12)
        assert flipped.edge == pytest.approx(base.edge, abs=1e-12)
        assert flipped.realized_edge == pytest.approx(
            base.realized_edge, abs=1e-12
        )

    @needs_checkpoint
    def test_flip_does_not_touch_skip_sentinels(self, rf, hgb, s5, flip_on) -> None:
        # A skip row (no candidates) must come out exactly as with the flag off.
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.20, no_ask=0.95,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason == "ask_bucket"
        assert r.side == "YES"


def _decision(yes_ask, no_ask):
    def _book(ask):
        return None if ask is None else SimpleNamespace(top_ask=ask)
    return SimpleNamespace(yes_book=_book(yes_ask), no_book=_book(no_ask))


class TestFlipWalkCap:
    def test_off_returns_sleeve_cap(self, s5, monkeypatch) -> None:
        monkeypatch.setattr(config, "SUBMINUTE_FLIP_SIDE", False)
        d = _decision(0.30, 0.72)
        assert _execution_max_walk_price_for(
            d, "YES", s5
        ) == _execution_max_walk_price(s5)

    def test_on_returns_increment_to_ceiling(self, s5, monkeypatch, flip_on) -> None:
        monkeypatch.setattr(config, "SUBMINUTE_FLIP_PAD", 0.04)
        # Executing YES (the flip) with top 0.30; model side is NO at ask
        # 0.72 -> absolute ceiling 0.32 -> increment 0.02 over the 0.30
        # anchor. The sleeve leash (5c) is looser, so the ceiling wins.
        d = _decision(0.30, 0.72)
        cap = _execution_max_walk_price_for(d, "YES", s5)
        assert cap == pytest.approx(min(_execution_max_walk_price(s5), 0.02))
        assert cap == pytest.approx(0.02)

    def test_on_sleeve_leash_still_binds_when_tighter(
        self, s5, monkeypatch, flip_on
    ) -> None:
        # Ceiling far above the top (cheap flip book): increment to ceiling
        # (0.30) is looser than the 5c sleeve leash -> the leash wins.
        monkeypatch.setattr(config, "SUBMINUTE_FLIP_PAD", 0.02)
        d = _decision(0.10, 0.62)  # ceiling 0.40, top 0.10 -> inc 0.30
        cap = _execution_max_walk_price_for(d, "YES", s5)
        assert cap == pytest.approx(_execution_max_walk_price(s5))

    def test_on_top_at_ceiling_walks_top_only(self, s5, monkeypatch, flip_on) -> None:
        monkeypatch.setattr(config, "SUBMINUTE_FLIP_PAD", 0.02)
        d = _decision(0.30, 0.72)  # ceiling 0.30 == top -> increment 0.0
        assert _execution_max_walk_price_for(d, "YES", s5) == pytest.approx(0.0)

    def test_on_top_above_ceiling_is_skip_sentinel(
        self, s5, monkeypatch, flip_on
    ) -> None:
        monkeypatch.setattr(config, "SUBMINUTE_FLIP_PAD", 0.02)
        # ceiling = 1 - 0.75 + 0.02 = 0.27 < top 0.30 -> unpriceable flip.
        d = _decision(0.30, 0.75)
        assert _execution_max_walk_price_for(d, "YES", s5) == -1.0

    def test_on_fails_closed_without_model_ask(self, s5, flip_on) -> None:
        for bad in (None, float("nan")):
            d = _decision(0.30, bad)
            assert _execution_max_walk_price_for(d, "YES", s5) == -1.0
        d = SimpleNamespace(yes_book=SimpleNamespace(top_ask=0.30), no_book=None)
        assert _execution_max_walk_price_for(d, "YES", s5) == -1.0

    def test_on_fails_closed_without_flip_top(self, s5, flip_on) -> None:
        for bad in (None, float("nan"), 0.0):
            d = _decision(bad, 0.72)
            assert _execution_max_walk_price_for(d, "YES", s5) == -1.0
        d = SimpleNamespace(yes_book=None, no_book=SimpleNamespace(top_ask=0.72))
        assert _execution_max_walk_price_for(d, "YES", s5) == -1.0

    def test_math_isfinite_guard_accepts_int_ask(self, s5, flip_on) -> None:
        # model ask == 1 (int) -> ceiling = 0 + PAD < top 0.30 -> sentinel;
        # degenerate but must not raise on the int input.
        d = _decision(0.30, 1)
        assert _execution_max_walk_price_for(d, "YES", s5) == -1.0


# --------------------------------------------------------- crossed-book guard

class TestCrossedBookSkip:
    """Phantom-book guard (2026-08-08, shipped with the EDGE_CAP disarm)."""

    def test_sub_dollar_sum_skips(self) -> None:
        from cryptobot.execution.pipeline import _crossed_book_skip
        assert _crossed_book_skip(0.01, 0.69)     # the era's phantom shape
        assert _crossed_book_skip(0.30, 0.60)

    def test_real_books_pass(self) -> None:
        from cryptobot.execution.pipeline import _crossed_book_skip
        assert not _crossed_book_skip(0.30, 0.72)   # normal overround
        assert not _crossed_book_skip(0.30, 0.70)   # sums to exactly 1.00
        assert not _crossed_book_skip(0.25, 0.749)  # 0.999 boundary passes

    def test_unreadable_sides_never_skip(self) -> None:
        # Only POSITIVE evidence of a broken book fires the guard — missing /
        # NaN books keep their ordinary no-book handling.
        from cryptobot.execution.pipeline import _crossed_book_skip
        assert not _crossed_book_skip(None, 0.70)
        assert not _crossed_book_skip(0.30, None)
        assert not _crossed_book_skip(float("nan"), 0.30)
