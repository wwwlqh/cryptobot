"""W4 sizing_v2 unit tests.

Pins the v2 step1last sizing math to the design's acceptance vectors
(throttle removal + resize, operator-ordered 2026-07-31: FLAT 1% of equity on
both intervals, brake floor 1.0 = graduated throttle REMOVED, paired with the
0.30 hard halt):
  equity=$1000 / dd=0 / k=1 -> $10.00 (15m and 5m — same flat frac)
  any dd                    -> brake x1.00 (the throttle is off by default)
  k=4                       -> conc x0.5
  sub-$1                    -> floored UP to exactly $1.00

The brake FUNCTION is still live code (the floor is an env knob), so its ramp
math is exercised under ``armed_throttle``, which restores the pre-07-31
scale 0.60 / floor 0.50 shape:
  dd=0.30 (floor knee)      -> brake x0.50 (half size)
  dd>=0.30                  -> brake x0.50 (floor)

Also guards the env-overridability of every knob and the shipped
"no cw/cw.mean() renorm" property of ``concurrency_weight``.
"""

from __future__ import annotations

import math

import pytest

from cryptobot.execution import sizing_v2


@pytest.fixture()
def armed_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-arm the graduated DD brake (pre-2026-07-31 shape: scale 0.60 / floor 0.50).

    The shipped default is floor 1.0 (throttle removed), so every assertion about
    the ramp/knee has to say which throttle it is talking about. Re-arming is a
    one-knob change live, and these vectors are what it would restore.
    """
    monkeypatch.setenv("CRYPTOBOT_V2_BRAKE_SCALE", "0.60")
    monkeypatch.setenv("CRYPTOBOT_V2_BRAKE_FLOOR", "0.50")


# ---------------------------------------------------------------------------
# base_fraction
# ---------------------------------------------------------------------------


def test_base_fraction_defaults() -> None:
    # FLAT 1% on both intervals since 2026-07-31 (was 0.025 / 0.0125): the 2:1
    # 5m/15m asymmetry is gone, so neither interval rides at "half weight".
    assert sizing_v2.base_fraction("15m") == pytest.approx(0.010)
    assert sizing_v2.base_fraction("5m") == pytest.approx(0.010)


def test_base_fraction_non_5m_uses_15m() -> None:
    # Mirrors research np.where(interval == "5m", ...): anything not "5m" -> 15m frac.
    assert sizing_v2.base_fraction("anything-else") == pytest.approx(0.010)


def test_base_fraction_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRYPTOBOT_V2_BASE_FRAC_15M", "0.05")
    monkeypatch.setenv("CRYPTOBOT_V2_BASE_FRAC_5M", "0.03")
    assert sizing_v2.base_fraction("15m") == pytest.approx(0.05)
    assert sizing_v2.base_fraction("5m") == pytest.approx(0.03)


# ---------------------------------------------------------------------------
# brake_multiplier
# ---------------------------------------------------------------------------


def test_brake_multiplier_healthy() -> None:
    assert sizing_v2.brake_multiplier(0.0) == pytest.approx(1.0)


def test_brake_multiplier_is_inert_at_shipped_defaults() -> None:
    # THE 2026-07-31 CONTRACT: the graduated throttle is REMOVED. Floor 1.0 puts
    # the knee at dd >= 0, so max(floor, 1 - dd/scale) == 1.0 at every drawdown
    # depth — the sleeve never downsizes on drawdown, it only ever HALTS
    # (config.MAX_DRAWDOWN_HALT_FRAC = 0.30).
    assert sizing_v2.DEFAULT_BRAKE_FLOOR == pytest.approx(1.0)
    for dd in (0.0, 0.05, 0.15, 0.30, 0.45, 0.60, 0.99):
        assert sizing_v2.brake_multiplier(dd) == pytest.approx(1.0)


def test_brake_multiplier_linear_ramp_between_full_and_floor(
    armed_throttle: None,
) -> None:
    # Re-armed throttle: linear region 1 - dd/0.60 for dd in (0, 0.30) — full
    # size ramps down to the half-size floor.
    assert sizing_v2.brake_multiplier(0.10) == pytest.approx(1.0 - 0.10 / 0.60)
    assert sizing_v2.brake_multiplier(0.20) == pytest.approx(1.0 - 0.20 / 0.60)


def test_brake_multiplier_floors_at_knee_0_30(armed_throttle: None) -> None:
    # Floor knee = scale*(1-floor) = 0.60*0.50 = 0.30: half size from
    # >=30% DD (1 - 0.30/0.60 = 0.50 == the floor).
    assert sizing_v2.brake_multiplier(0.30) == pytest.approx(0.50)
    assert sizing_v2.brake_multiplier(0.36) == pytest.approx(0.50)


def test_brake_multiplier_floors_at_scale(armed_throttle: None) -> None:
    # dd == scale -> 1 - 1 = 0 -> clamped to floor 0.50
    assert sizing_v2.brake_multiplier(0.60) == pytest.approx(0.50)


def test_brake_multiplier_floors_beyond_knee(armed_throttle: None) -> None:
    # past the floor knee (dd >= 0.30) -> still clamped to floor 0.50
    assert sizing_v2.brake_multiplier(0.45) == pytest.approx(0.50)
    assert sizing_v2.brake_multiplier(0.99) == pytest.approx(0.50)


def test_brake_multiplier_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRYPTOBOT_V2_BRAKE_SCALE", "0.50")
    monkeypatch.setenv("CRYPTOBOT_V2_BRAKE_FLOOR", "0.10")
    # 1 - 0.25/0.50 = 0.5
    assert sizing_v2.brake_multiplier(0.25) == pytest.approx(0.5)
    # deep drawdown clamps to the overridden floor
    assert sizing_v2.brake_multiplier(0.90) == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# concurrency_weight
# ---------------------------------------------------------------------------


def test_concurrency_weight_single_is_unity() -> None:
    assert sizing_v2.concurrency_weight(1) == pytest.approx(1.0)


def test_concurrency_weight_k4_halves() -> None:
    assert sizing_v2.concurrency_weight(4) == pytest.approx(0.5)


def test_concurrency_weight_is_raw_1_over_sqrt_k_no_renorm() -> None:
    # Documents the shipped semantics: raw 1/sqrt(k), NO cw/cw.mean() renorm.
    for k in (1, 2, 3, 4, 9, 16):
        assert sizing_v2.concurrency_weight(k) == pytest.approx(1.0 / math.sqrt(k))


def test_concurrency_weight_guards_nonpositive_k() -> None:
    # k should be >= 1 in practice; clamp defensively so we never divide by zero.
    assert sizing_v2.concurrency_weight(0) == pytest.approx(1.0)
    assert sizing_v2.concurrency_weight(-3) == pytest.approx(1.0)


def test_concurrency_weight_toggle_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRYPTOBOT_V2_CONC_DOWNWEIGHT", "0")
    assert sizing_v2.concurrency_weight(4) == pytest.approx(1.0)
    assert sizing_v2.concurrency_weight(9) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# sized_usd — acceptance vectors
# ---------------------------------------------------------------------------


def test_sized_usd_healthy_base_vectors() -> None:
    # Flat 1%: both intervals size identically now.
    assert sizing_v2.sized_usd(1000.0, "15m", 0.0, 1) == pytest.approx(10.0)
    assert sizing_v2.sized_usd(1000.0, "5m", 0.0, 1) == pytest.approx(10.0)


def test_sized_usd_ignores_drawdown_at_shipped_defaults() -> None:
    # Throttle removed: a 36% drawdown sizes exactly like a healthy sleeve.
    # (Placement stops at the 0.30 hard halt upstream — this module never sees
    # a halted sleeve, so its job is only to NOT double-count the risk.)
    assert sizing_v2.sized_usd(1000.0, "15m", 0.36, 1) == pytest.approx(10.0)


def test_sized_usd_brake_applies(armed_throttle: None) -> None:
    # dd=0.15 -> brake 1 - 0.15/0.60 = 0.75; $1000 equity so the $1 floor does
    # not mask it: 1000 * 0.010 * 0.75 = 7.5
    assert sizing_v2.sized_usd(1000.0, "15m", 0.15, 1) == pytest.approx(7.5)


def test_sized_usd_brake_floor_at_030(armed_throttle: None) -> None:
    # dd>=0.30 -> brake x0.50 (floor). $1000 equity so the $1 floor does not
    # mask the brake: 1000 * 0.010 * 0.50 = 5.0
    assert sizing_v2.sized_usd(1000.0, "15m", 0.36, 1) == pytest.approx(5.0)


def test_sized_usd_concurrency_applies() -> None:
    # k=4 -> conc x0.5 -> 10.00 * 0.5 = 5.00
    assert sizing_v2.sized_usd(1000.0, "15m", 0.0, 4) == pytest.approx(5.0)


def test_sized_usd_floors_up_to_one_dollar() -> None:
    # 5m, k=4 (conc 0.5): 100 * 0.010 * 0.5 = 0.50 -> $1.00
    assert sizing_v2.sized_usd(100.0, "5m", 0.30, 4) == pytest.approx(1.0)


def test_sized_usd_floor_is_exactly_min_bet_not_the_raw() -> None:
    # any sub-$1 raw lands on exactly the floor, never the raw value
    out = sizing_v2.sized_usd(10.0, "5m", 0.0, 1)  # 10 * 0.010 = 0.10
    assert out == pytest.approx(1.0)


def test_sized_usd_above_floor_not_clamped() -> None:
    # 5m at $1000 equity, healthy: 1000 * 0.010 = 10.0, well above the floor
    assert sizing_v2.sized_usd(1000.0, "5m", 0.0, 1) == pytest.approx(10.0)


def test_sized_usd_full_composition(armed_throttle: None) -> None:
    # equity=$800, 15m, dd=0.30 (brake 0.50 at the knee), k=4 (conc 0.5):
    # 800 * 0.010 * 0.5 * 0.5 = 2.0
    assert sizing_v2.sized_usd(800.0, "15m", 0.30, 4) == pytest.approx(2.0)


def test_sized_usd_default_k_is_one() -> None:
    assert sizing_v2.sized_usd(1000.0, "15m", 0.0) == pytest.approx(10.0)


def test_sized_usd_min_bet_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRYPTOBOT_MIN_BET_USD", "5.0")
    # 100 * 0.010 = 1.0 < 5.0 -> floored up to 5.0
    assert sizing_v2.sized_usd(100.0, "5m", 0.0, 1) == pytest.approx(5.0)


def test_sized_usd_conc_toggle_off_removes_downweight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CRYPTOBOT_V2_CONC_DOWNWEIGHT", "0")
    # k=4 no longer halves: 1000 * 0.010 = 10.00
    assert sizing_v2.sized_usd(1000.0, "15m", 0.0, 4) == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# no depth/fill cap — stake follows bet size at any equity (operator 2026-07-11)
# ---------------------------------------------------------------------------


def test_no_depth_cap_high_equity_follows_bet_size() -> None:
    # The per-coin depth/fill cap was removed (operator order 2026-07-11): at
    # high equity the stake is the RAW equity-fraction size, never clamped to
    # book depth. 5000 * 0.010 = 50; 20000 * 0.010 = 200 — both far above the
    # old per-coin ceilings (xrp 53, btc 313), which no longer bind.
    assert sizing_v2.sized_usd(5000.0, "15m", 0.0, 1) == pytest.approx(50.0)
    assert sizing_v2.sized_usd(20000.0, "15m", 0.0, 1) == pytest.approx(200.0)
    # 5m sizes the same since the flat-1% resize: 5000 * 0.010 = 50, uncapped.
    assert sizing_v2.sized_usd(5000.0, "5m", 0.0, 1) == pytest.approx(50.0)


def test_sized_usd_detail_reports_stake_only() -> None:
    # SizedBet now carries only the final placeable stake — the cap telemetry
    # (cap_usd / cap_binding) was removed along with the depth cap.
    detail = sizing_v2.sized_usd_detail(5000.0, "15m", 0.0, 1)
    assert detail.stake == pytest.approx(50.0)
    assert not hasattr(detail, "cap_usd")
    assert not hasattr(detail, "cap_binding")
