"""U6 gate stack tests.

Covers:
  - first-fail short-circuit + deterministic ordering
  - AE-NEW-5 shared exposure cap (binary skip per D4 parity)
  - AE-NEW-7 slot_full
  - brainstorm AE1 (fee_unknown), AE8 (edge_above_ceiling)
  - D7 haircut applied BEFORE the min_edge gate
"""

from __future__ import annotations


import pytest

from cryptobot import config
from cryptobot.execution.gates import (
    GATE_NAMES,
    GateInput,
    evaluate_gates,
)
from cryptobot.strategy.portfolio_loader import Sleeve


def make_sleeve(
    *,
    min_edge: float = 0.05,
    max_edge: float = 0.50,
    min_fill_price: float = 0.10,
    max_fill_price: float = 0.90,
    slippage_haircut: float = 0.05,
    partial_ret_z_floor: float | None = None,
    partial_ret_z_ceiling: float | None = None,
) -> Sleeve:
    return Sleeve(
        label="v6_5m",
        alloc=0.4,
        interval="5m",
        interval_min=5,
        side_strategy="model_signed",
        entry_minute_set=(4,),
        entry_strategy="first_qualifying",
        min_edge=min_edge,
        max_edge=max_edge,
        min_fill_price=min_fill_price,
        max_fill_price=max_fill_price,
        min_agreement=1,
        capital_frac=0.11,
        max_walk_price=0.05,
        slippage_haircut=slippage_haircut,
        min_lut_n=50,
        signal_source="hgb",
        hour_bucket_set="all",
        dow_set="all",
        partial_ret_z_floor=partial_ret_z_floor,
        partial_ret_z_ceiling=partial_ret_z_ceiling,
    )


def make_input(**overrides) -> GateInput:
    defaults: dict = dict(
        coin="BTC",
        sleeve=make_sleeve(),
        entry_minute=4,
        side="YES",
        realized_edge=0.10,
        fill_price=0.55,
        fee_rate_bps=100.0,
        spot_age_s=1.0,
        proposed_bet_size=2.0,
        pending_exposure_total=0.0,
        realized_capital=100.0,
        slot_filled_usd=0.0,
        slot_target_usd=5.0,
        disk_free_mb=10000.0,
        book_top_ask_age_s=0.5,
        is_topup=False,
        # Pin the exposure-cap fraction so gate-mechanic tests stay stable
        # across artifact deploys. The all_4 cap (0.5919) and prior caps
        # (0.2877) both differ; this anchors the cap-related test math.
        exposure_cap_fraction=0.28767857660247903,
    )
    defaults.update(overrides)
    return GateInput(**defaults)


class TestGateOrdering:
    def test_gate_stack_in_documented_order(self) -> None:
        # 12-gate stack (ORB retired 2026-07-09; topup_edge_floor added
        # 2026-07-23). `threshold` and `book_consumption` are inert for
        # non-hgb sleeves; `topup_edge_floor` is inert for entries.
        assert GATE_NAMES == (
            "disk",
            "freshness",
            "fee",
            "threshold",
            "edge",
            "topup_edge_floor",
            "partial_ret_z",
            "capital",
            "book_consumption",
            "exposure",
            "group_exposure",
            "slot_full",
        )

    def test_disk_fails_before_freshness(self) -> None:
        # Both would fail; gate stack must report disk first.
        r = evaluate_gates(make_input(disk_free_mb=1.0, spot_age_s=60.0))
        assert r.skip_reason == "disk_low"

    def test_happy_path_passes(self) -> None:
        r = evaluate_gates(make_input())
        assert r.passed is True
        assert r.skip_reason is None


class TestIndividualGates:
    def test_disk_low(self) -> None:
        assert evaluate_gates(make_input(disk_free_mb=10.0)).skip_reason == "disk_low"

    def test_stale_spot(self) -> None:
        assert evaluate_gates(make_input(spot_age_s=30.0)).skip_reason == "stale_spot"

    def test_stale_book(self) -> None:
        assert evaluate_gates(make_input(book_top_ask_age_s=30.0)).skip_reason == "stale_book"

    def test_fee_unknown_none(self) -> None:
        # AE1.
        assert evaluate_gates(make_input(fee_rate_bps=None)).skip_reason == "fee_unknown"

    @pytest.mark.parametrize("bad", [float("nan"), 0.0, -1.0])
    def test_fee_unknown_invalid(self, bad: float) -> None:
        assert evaluate_gates(make_input(fee_rate_bps=bad)).skip_reason == "fee_unknown"

    def test_edge_below_floor(self) -> None:
        r = evaluate_gates(make_input(realized_edge=0.02))
        assert r.skip_reason == "edge_below_floor"

    def test_edge_above_ceiling(self) -> None:
        # AE8.
        r = evaluate_gates(make_input(realized_edge=0.80))
        assert r.skip_reason == "edge_above_ceiling"

    def test_fill_band_too_cheap(self) -> None:
        r = evaluate_gates(make_input(fill_price=0.05))
        assert r.skip_reason == "fill_band"

    def test_fill_band_too_expensive(self) -> None:
        r = evaluate_gates(make_input(fill_price=0.95))
        assert r.skip_reason == "fill_band"

    def test_insufficient_capital(self) -> None:
        r = evaluate_gates(make_input(proposed_bet_size=config.MIN_BET_USD - 0.01))
        assert r.skip_reason == "insufficient_capital"


class TestTopupEdgeFloorGate:
    """config.TOPUP_EDGE_FLOOR (2026-07-23 incident): top-up-only floor on the
    decision edge, DEFAULT 0.0 = no NEGATIVE-edge top-ups. The sleeve edge
    band owns entries; a min_edge of -1.0 in these tests opens the
    [min_edge, floor) window so the NEW gate is the one that fires."""

    def test_default_zero_is_armed(self) -> None:
        assert config.TOPUP_EDGE_FLOOR == 0.0

    def test_negative_edge_topup_skipped_at_default(self) -> None:
        r = evaluate_gates(make_input(
            sleeve=make_sleeve(min_edge=-1.0), is_topup=True,
            realized_edge=-0.0119,  # the live id7 recorded edge
        ))
        assert r.skip_reason == "topup_edge_below_floor"

    def test_negative_edge_entry_not_touched(self) -> None:
        # Inert for entries: the identical input with is_topup=False passes
        # (the sleeve band, min_edge -1.0 here, owns entry edge policy).
        r = evaluate_gates(make_input(
            sleeve=make_sleeve(min_edge=-1.0), is_topup=False,
            realized_edge=-0.0119,
        ))
        assert r.passed is True

    def test_edge_exactly_at_floor_passes(self) -> None:
        # Strict <: an edge of exactly 0.0 at the 0.0 default passes.
        r = evaluate_gates(make_input(
            sleeve=make_sleeve(min_edge=-1.0), is_topup=True,
            realized_edge=0.0,
        ))
        assert r.passed is True

    def test_positive_edge_topup_unaffected_at_default(self) -> None:
        r = evaluate_gates(make_input(is_topup=True, realized_edge=0.10))
        assert r.passed is True

    def test_armed_floor_blocks_positive_edge_below_it(self, monkeypatch) -> None:
        monkeypatch.setattr(config, "TOPUP_EDGE_FLOOR", 0.05)
        r = evaluate_gates(make_input(
            sleeve=make_sleeve(min_edge=-1.0), is_topup=True,
            realized_edge=0.02,
        ))
        assert r.skip_reason == "topup_edge_below_floor"

    def test_sleeve_edge_band_fires_first(self) -> None:
        # Attribution precedence: an edge under BOTH the sleeve min_edge and
        # the top-up floor keeps its existing 'edge_below_floor' reason.
        r = evaluate_gates(make_input(is_topup=True, realized_edge=-0.0119))
        assert r.skip_reason == "edge_below_floor"


class TestExposureGate:
    def test_under_cap_passes(self) -> None:
        # realized=100, cap~=28.77, pending=24, new=4 -> 28 <= cap -> passes
        r = evaluate_gates(make_input(pending_exposure_total=24.0, proposed_bet_size=4.0))
        assert r.passed is True

    def test_ae_new_5_exposure_breach(self) -> None:
        # AE-NEW-5: realized=100, pending=27, new=3 -> 30 > cap -> skip.
        r = evaluate_gates(make_input(pending_exposure_total=27.0, proposed_bet_size=3.0))
        assert r.skip_reason == "exposure_cap"

    def test_exposure_cap_is_binary_not_partial(self) -> None:
        # D4 parity: exposure cap MUST be skip-entirely. No partial-fill.
        # If the proposed exceeds the cap by even a cent, skip — never fill
        # only the remaining headroom. pending=49.50, new=1.01 → 50.51 > 50.
        pending = config.INITIAL_BANKROLL * config.SHARED_EXPOSURE_CAP - 0.5
        r = evaluate_gates(
            make_input(pending_exposure_total=pending, proposed_bet_size=1.01)
        )
        assert r.skip_reason == "exposure_cap"

    def test_cap_denominator_tracks_realized_capital(self) -> None:
        # Operator override 2026-05-21: exposure is a percentage of current
        # total capital. With realized=$200, cap=$100, so pending+new=$51 passes.
        r = evaluate_gates(
            make_input(
                realized_capital=200.0,
                pending_exposure_total=48.0,
                proposed_bet_size=3.0,
            )
        )
        assert r.passed is True


class TestSlotFullGate:
    def test_slot_full_within_tolerance(self) -> None:
        # AE-NEW-7.
        r = evaluate_gates(make_input(slot_filled_usd=5.0, slot_target_usd=5.0))
        assert r.skip_reason == "slot_full"

    def test_partial_slot_passes(self) -> None:
        r = evaluate_gates(make_input(slot_filled_usd=2.0, slot_target_usd=5.0))
        assert r.passed is True

    def test_slot_target_zero_disables(self) -> None:
        # Defensive: target=0 should not trigger slot_full.
        r = evaluate_gates(make_input(slot_filled_usd=0.0, slot_target_usd=0.0))
        # Capital gate will still fire on bet_size, but slot_full does not.
        assert r.skip_reason != "slot_full"


class TestShortCircuit:
    def test_first_failure_short_circuits(self) -> None:
        # disk fails first; downstream gates would also fail but we
        # never reach them.
        r = evaluate_gates(
            make_input(
                disk_free_mb=1.0,
                fee_rate_bps=None,
                realized_edge=-1.0,
                proposed_bet_size=0.0,
            )
        )
        assert r.skip_reason == "disk_low"

    def test_fee_unknown_before_edge(self) -> None:
        # fee gate (rank 3) precedes edge (rank 4).
        r = evaluate_gates(make_input(fee_rate_bps=None, realized_edge=0.80))
        assert r.skip_reason == "fee_unknown"

    def test_edge_before_capital(self) -> None:
        r = evaluate_gates(
            make_input(realized_edge=0.01, proposed_bet_size=0.1)
        )
        assert r.skip_reason == "edge_below_floor"


class TestPartialRetZGate:
    """U4 of the all_4 deploy plan: chunk-CV partial_ret_z gate."""

    def test_inert_when_both_bounds_none(self) -> None:
        sleeve = make_sleeve()  # both bounds default to None
        r = evaluate_gates(make_input(sleeve=sleeve, partial_ret_z_1h=None))
        # Passes — gate is no-op when sleeve has no z bound.
        assert r.skip_reason is None

    def test_floor_rejects_below(self) -> None:
        sleeve = make_sleeve(partial_ret_z_floor=-1.5)
        r = evaluate_gates(make_input(sleeve=sleeve, partial_ret_z_1h=-2.0))
        assert r.skip_reason == "partial_ret_z_below_floor"

    def test_floor_passes_at_or_above(self) -> None:
        sleeve = make_sleeve(partial_ret_z_floor=-1.5)
        r = evaluate_gates(make_input(sleeve=sleeve, partial_ret_z_1h=-1.0))
        assert r.skip_reason is None

    def test_ceiling_rejects_above(self) -> None:
        sleeve = make_sleeve(partial_ret_z_ceiling=2.0)
        r = evaluate_gates(make_input(sleeve=sleeve, partial_ret_z_1h=3.0))
        assert r.skip_reason == "partial_ret_z_above_ceiling"

    def test_z_unavailable_when_bound_set(self) -> None:
        sleeve = make_sleeve(partial_ret_z_floor=-1.5)
        r = evaluate_gates(make_input(sleeve=sleeve, partial_ret_z_1h=None))
        assert r.skip_reason == "partial_ret_z_unavailable"

    def test_both_bounds_passes_inside_window(self) -> None:
        sleeve = make_sleeve(partial_ret_z_floor=-1.5, partial_ret_z_ceiling=2.0)
        r = evaluate_gates(make_input(sleeve=sleeve, partial_ret_z_1h=0.5))
        assert r.skip_reason is None
