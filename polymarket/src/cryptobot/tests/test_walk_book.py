"""U2 walker tests — characterization-first port of hightempbot's coverage
plus the AE-NEW-2 acceptance example.

Pinned to ``config.POLY_FEE_THETA = 0.07`` (D6) rather than hightempbot's
0.05; expected values were recomputed for the new theta.
"""

from __future__ import annotations

import math

import pytest

from cryptobot.config import POLY_FEE_THETA
from cryptobot.execution.walk_book import walk_book_edge_preserving


FEE = POLY_FEE_THETA  # 0.07
MIN_EDGE = 0.05


class TestEdgePreservingWalker:
    def test_fills_full_target_when_book_is_deep_and_flat(self):
        # edge at 0.20 = 0.30 - 0.20 - 0.07*0.20*0.80 = 0.30 - 0.20 - 0.0112 = 0.0888
        book = {"asks": [{"price": "0.20", "size": "100000"}]}
        result = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        assert result is not None
        filled_usd, _, vwap, limit_price, realized_edge = result
        assert filled_usd == pytest.approx(10.0)
        assert vwap == pytest.approx(0.20)
        assert limit_price == pytest.approx(0.20)
        assert realized_edge == pytest.approx(0.0888, abs=1e-4)

    def test_returns_none_on_empty_asks(self):
        assert walk_book_edge_preserving(
            {"asks": []}, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        ) is None

    def test_returns_none_when_no_fill_reaches_min_bet_usd(self):
        # L1 0.20 @ $0.50 depth alone is below MIN_BET_USD; L2 0.40 would
        # break edge floor → walker returns None.
        book = {"asks": [
            {"price": "0.20", "size": "2.5"},
            {"price": "0.40", "size": "100"},
        ]}
        assert walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        ) is None

    def test_caps_at_target_usd_when_book_has_more_depth(self):
        book = {"asks": [{"price": "0.20", "size": "100000"}]}
        result = walk_book_edge_preserving(
            book, 5.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        assert result is not None
        filled_usd, *_ = result
        assert filled_usd == pytest.approx(5.0)


class TestMaxWalkPriceCap:
    def test_walker_stops_at_price_walk_cap(self):
        # Top=0.20, second=0.24 (under cap), third=0.26 (above cap=0.25).
        # Edge floor disabled (min_edge=-1) so the cap is the only break.
        book = {"asks": [
            {"price": "0.20", "size": "2000"},
            {"price": "0.24", "size": "5000"},
            {"price": "0.26", "size": "9999"},
        ]}
        result = walk_book_edge_preserving(
            book, 10000.0,
            prob_safe_floor=0.95, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.05,
        )
        assert result is not None
        filled_usd, _, vwap, limit_price, _ = result
        assert filled_usd == pytest.approx(1600.0)   # $400 + $1200
        assert 0.20 <= vwap <= 0.24
        assert limit_price == pytest.approx(0.24)

    def test_level_exactly_at_cap_is_consumable(self):
        # Cap is strict-greater; 0.25 = top(0.20) + 0.05 → consumable.
        book = {"asks": [
            {"price": "0.20", "size": "100"},
            {"price": "0.25", "size": "100"},
        ]}
        result = walk_book_edge_preserving(
            book, 100.0,
            prob_safe_floor=0.95, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.05,
        )
        assert result is not None
        filled_usd, _, _, limit_price, _ = result
        assert filled_usd == pytest.approx(45.0)
        assert limit_price == pytest.approx(0.25)

    def test_ae_new_2_btc_book(self):
        # Plan AE-NEW-2. Book [(0.55, 50), (0.57, 30), (0.58, 40), (0.62, 80)],
        # cap=0.05 → walker may consume 0.55/0.57/0.58 but stops before 0.62.
        # min_edge=-1 to isolate the cap break.
        book = {"asks": [
            {"price": "0.55", "size": "50"},
            {"price": "0.57", "size": "30"},
            {"price": "0.58", "size": "40"},
            {"price": "0.62", "size": "80"},
        ]}
        result = walk_book_edge_preserving(
            book, 200.0,
            prob_safe_floor=0.99, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.05,
        )
        assert result is not None
        filled_usd, _, _, limit_price, _ = result
        # 50*0.55 + 30*0.57 + 40*0.58 = 27.5 + 17.1 + 23.2 = 67.8
        assert filled_usd == pytest.approx(67.8)
        assert limit_price == pytest.approx(0.58)

    def test_max_walk_price_zero_fills_only_top_level(self):
        book = {"asks": [
            {"price": "0.20", "size": "50"},
            {"price": "0.21", "size": "10000"},
        ]}
        result = walk_book_edge_preserving(
            book, 10000.0,
            prob_safe_floor=0.95, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.0,
        )
        assert result is not None
        filled_usd, _, _, limit_price, _ = result
        assert filled_usd == pytest.approx(10.0)
        assert limit_price == pytest.approx(0.20)

    def test_negative_max_walk_price_returns_none(self):
        book = {"asks": [{"price": "0.20", "size": "10000"}]}
        assert walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0, max_walk_price=-0.05,
        ) is None

    @pytest.mark.parametrize("bad", [float("inf"), float("nan")])
    def test_non_finite_max_walk_price_returns_none(self, bad: float):
        book = {"asks": [{"price": "0.20", "size": "10000"}]}
        assert walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0, max_walk_price=bad,
        ) is None

    def test_non_finite_top_does_not_silently_disable_cap(self):
        # NaN-priced poisoned top — walker must anchor cap on the first
        # finite ask, not silently let the 0.30 level through.
        book = {"asks": [
            {"price": "nan", "size": "10000"},
            {"price": "0.20", "size": "50"},
            {"price": "0.30", "size": "10000"},
        ]}
        result = walk_book_edge_preserving(
            book, 10000.0,
            prob_safe_floor=0.95, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.05,
        )
        assert result is not None
        filled_usd, _, _, limit_price, _ = result
        assert filled_usd == pytest.approx(10.0)
        assert limit_price == pytest.approx(0.20)

    def test_walk_anchor_sticks_across_book_drift(self):
        # R-NEW-9 add-on top-up: original top was 0.55 (anchor=0.55,
        # cap=0.60). Book has drifted — fresh top 0.61 would otherwise
        # set cap=0.66 and let 0.62 through. Sticky anchor must reject
        # both levels (both > 0.60).
        book = {"asks": [
            {"price": "0.62", "size": "100"},
            {"price": "0.61", "size": "100"},
        ]}
        result_anchored = walk_book_edge_preserving(
            book, 100.0,
            prob_safe_floor=0.95, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.05, walk_anchor_price=0.55,
        )
        assert result_anchored is None

        result_fresh = walk_book_edge_preserving(
            book, 100.0,
            prob_safe_floor=0.95, fee_theta=FEE, min_edge=-1.0,
            min_bet_usd=1.0, max_walk_price=0.05,
        )
        assert result_fresh is not None

    def test_max_walk_price_none_is_back_compat(self):
        book = {"asks": [{"price": "0.20", "size": "100000"}]}
        a = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        b = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0, max_walk_price=None,
        )
        assert a == b


class TestEdgeFloorBreak:
    def test_stops_before_level_that_would_break_edge_floor(self):
        # Level 1: 0.20 @ $1 → edge 0.0885 (pass). Level 2: 0.40 with
        # min_edge=0.05 → adding L2 would drop VWAP edge below 0.05.
        book = {"asks": [
            {"price": "0.20", "size": "5"},      # $1 depth
            {"price": "0.40", "size": "100"},     # breaks edge
        ]}
        result = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        assert result is not None
        filled_usd, _, vwap, limit_price, realized_edge = result
        assert filled_usd == pytest.approx(1.0)
        assert vwap == pytest.approx(0.20)
        assert limit_price == pytest.approx(0.20)
        assert realized_edge >= MIN_EDGE

    @pytest.mark.parametrize("book", [
        {"asks": [{"price": "0.10", "size": "200"}]},
        {"asks": [{"price": "0.10", "size": "10"}, {"price": "0.12", "size": "50"}]},
        {"asks": [{"price": "0.18", "size": "80"}, {"price": "0.22", "size": "100"}]},
    ])
    def test_realized_edge_always_ge_min_edge_when_filled(self, book):
        result = walk_book_edge_preserving(
            book, 8.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        if result is not None:
            *_, realized_edge = result
            assert realized_edge >= MIN_EDGE - 1e-9


class TestInputValidation:
    @pytest.mark.parametrize(
        "kwargs",
        [
            dict(prob_safe_floor=float("nan")),
            dict(target_usd=float("nan")),
            dict(fee_theta=float("inf")),
            dict(min_edge=float("nan")),
            dict(min_bet_usd=float("inf")),
        ],
    )
    def test_non_finite_inputs_return_none(self, kwargs):
        base = dict(
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE, min_bet_usd=1.0,
        )
        base.update(kwargs)
        book = {"asks": [{"price": "0.20", "size": "100"}]}
        target = base.pop("target_usd", 5.0)
        assert walk_book_edge_preserving(book, target, **base) is None

    def test_non_positive_target_usd_returns_none(self):
        book = {"asks": [{"price": "0.20", "size": "100"}]}
        assert walk_book_edge_preserving(
            book, 0.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        ) is None

    def test_non_finite_walk_anchor_returns_none(self):
        book = {"asks": [{"price": "0.20", "size": "100"}]}
        for bad in (float("nan"), float("inf"), -0.5, 0.0):
            assert walk_book_edge_preserving(
                book, 5.0,
                prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
                min_bet_usd=1.0, max_walk_price=0.05, walk_anchor_price=bad,
            ) is None


class TestReturnLevels:
    def test_return_levels_appends_fill_ladder(self):
        # Top 0.12 fills exactly $10, second level 0.13 takes the remaining $5.
        # Walker accepts both because edge stays >= MIN_EDGE.
        book = {"asks": [
            {"price": "0.12", "size": "83.3333333333"},
            {"price": "0.13", "size": "1000"},
        ]}
        result = walk_book_edge_preserving(
            book, 15.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0, return_levels=True,
        )
        assert result is not None
        filled_usd, _, vwap, limit_price, _, levels = result
        assert filled_usd == pytest.approx(15.0)
        assert vwap == pytest.approx(15.0 / (83.3333333333 + 5.0 / 0.13))
        assert limit_price == pytest.approx(0.13)
        assert levels[0]["price"] == pytest.approx(0.12)
        assert levels[0]["usd"] == pytest.approx(10.0)
        assert levels[1]["price"] == pytest.approx(0.13)
        assert levels[1]["usd"] == pytest.approx(5.0)


class TestGhostFillRegressions:
    """Divergence #12 (2026-05-18 audit): ghost-fill handling currently
    has 0 occurrences in the live ledger, but the walker is the only
    line of defence if a Polymarket book vanishes between snapshot and
    take. These tests pin the no-fill paths so a future walker rewrite
    cannot silently emit a malformed fill on a degraded book.
    """

    def test_book_with_zero_size_top_ask_returns_none(self):
        # Top of book exists but has zero depth — analogue of a ghost ask
        # left in the snapshot after the resting order was cancelled.
        book = {"asks": [{"price": "0.20", "size": "0"}]}
        result = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        assert result is None

    def test_book_with_missing_asks_field_returns_none(self):
        # /book response that came back with no asks field at all
        # (Polymarket's CLOB returns {} in some edge cases).
        assert walk_book_edge_preserving(
            {}, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        ) is None

    def test_book_with_only_invalid_price_string_returns_none(self):
        # Polymarket has on rare occasions returned price strings that
        # don't parse as floats — when EVERY level is malformed there is
        # no executable book; walker must refuse rather than synthesise
        # a fill or raise to caller.
        book = {"asks": [{"price": "NaN", "size": "100"}]}
        result = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        assert result is None

    def test_book_with_invalid_price_string_skips_level_cleanly(self):
        # When a malformed level sits in front of a valid level the
        # walker must skip the bad level and fill from the valid one.
        # The resulting VWAP must be well-formed (finite, sane).
        book = {"asks": [
            {"price": "NaN", "size": "100"},
            {"price": "0.20", "size": "100"},
        ]}
        result = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        # Walker either returns None (refused) or a well-formed fill at
        # the valid level only. A silent malformed fill that includes
        # the NaN level is the unacceptable outcome.
        if result is None:
            return
        _, _, vwap, _, _ = result
        assert math.isfinite(vwap)
        assert vwap == pytest.approx(0.20, abs=1e-6)

    def test_full_walk_through_evaporating_book_preserves_edge(self):
        # First level 0.20 with $5 depth; second level 0.50 — way above
        # what edge tolerates. Walker must take only the first level
        # and stop walking BEFORE the edge constraint breaks; never
        # produce a malformed fill that spans both.
        book = {"asks": [
            {"price": "0.20", "size": "25"},   # 25 shares × 0.20 = $5
            {"price": "0.50", "size": "100"},  # edge here would be 0.30 - 0.50 - ... < 0
        ]}
        result = walk_book_edge_preserving(
            book, 10.0,
            prob_safe_floor=0.30, fee_theta=FEE, min_edge=MIN_EDGE,
            min_bet_usd=1.0,
        )
        if result is None:
            # Acceptable: walker may refuse if it can't reach the $10
            # target without breaking edge. The key property is that no
            # corrupt fill is produced.
            return
        filled_usd, _, vwap, limit_price, realized_edge = result
        # If walker produced a fill, it must NOT include level 2.
        assert limit_price <= 0.20 + 1e-9
        assert filled_usd <= 5.0 + 1e-9
        assert realized_edge >= MIN_EDGE - 1e-9
