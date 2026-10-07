"""v2 step1last sizing: equity-fraction stake with DD brake + concurrency downweight.

Pure, dependency-light re-implementation of the research sizing model for the
live tick path. (That model — ``pool_study.SIZING_SPEC`` / ``stake_weights`` —
was deleted 2026-08-03 as dead research code once live had diverged from it;
recover it with ``git show
<the 2026-08-03 lib-census commit>^:polymarket/backtest/lib/pool_study.py``.
THIS module is the authority now.) The integrator (``pipeline.py``) wires these functions into the sizing
choke point; this module owns NO live state, does NO I/O, and reads its knobs straight
from the environment so it can be exercised in isolation and retuned without a restart.

    stake = equity
            * base_fraction(interval)
            * brake_multiplier(dd)   # INERT since 2026-07-31 (floor 1.0 => always 1.0)
            * concurrency_weight(k)

then floored UP to the $1 Polymarket minimum (research floors to $1; it does NOT
skip sub-$1 bets — see ``sized_usd``). There is NO per-coin depth/fill cap: per
operator order 2026-07-11 ("delete fill cap for each coin — as long as each coin
satisfied condition and follows bet size, just fill") the stake is the raw
equity-fraction size, never clamped to book depth.

Knobs (all env-overridable, read at call time):
  CRYPTOBOT_V2_BASE_FRAC_15M   default 0.010   15m stake = 1% of equity
  CRYPTOBOT_V2_BASE_FRAC_5M    default 0.010   5m stake = 1% of equity (flat since 07-31)
  CRYPTOBOT_V2_BRAKE_SCALE     default 0.60    linear DD-throttle scale (inert at floor 1.0)
  CRYPTOBOT_V2_BRAKE_FLOOR     default 1.0     THROTTLE REMOVED 2026-07-31 — always full size
  CRYPTOBOT_V2_CONC_DOWNWEIGHT default 1       enable the 1/sqrt(k) same-minute downweight
  CRYPTOBOT_MIN_BET_USD        default 1.0     Polymarket $1 floor (shared with config.py)

The HARD drawdown halt lives in the pipeline (``config.MAX_DRAWDOWN_HALT_FRAC``),
NOT here: this module always returns a placeable (floored) stake; the caller decides
whether the sleeve is halted.

2026-07-31 (operator order): the graduated throttle is REMOVED — brake floor 1.0
pins ``brake_multiplier`` at 1.0 for every drawdown depth, and both intervals size
flat at 1% of equity. Per-$ edge was measured flat across drawdown depth, so the
brake bought no edge and only dragged the recovery; the drawdown budget it used to
defend is carried by the smaller flat size plus the 0.30 hard halt. BRAKE_SCALE is
left at 0.60 so re-arming the throttle is a one-knob change. See
``polymarket/backtest/studies/tick_latency/THROTTLE_REMOVAL_SIZE_20260731.md``.
"""

from __future__ import annotations

import math
import os
from typing import NamedTuple

# Defaults ONCE mirrored research SIZING_SPEC (pool_study.py, deleted 2026-08-03);
# they no longer do — see the 07-31 flat-1% line below. Kept as module constants so
# telemetry and tests have a canonical reference even when the environment is unset.
# 2026-07-31 (operator order): flat 1% on BOTH intervals (was 0.025 / 0.0125).
DEFAULT_BASE_FRAC_15M: float = 0.010
DEFAULT_BASE_FRAC_5M: float = 0.010
# 2026-07-16 (operator decision): reverted the F2 0.30/0.25 retune back to
# 0.50/0.50. 2026-07-21 (operator override, risk retune): scale 0.60 with the
# floor kept at 0.50 — half size at the knee scale*(1-floor) = 30% DD.
# 2026-07-31 (operator order): THROTTLE REMOVED — floor 0.50 -> 1.0 makes
# max(floor, 1 - dd/scale) == 1.0 at every dd >= 0. SCALE stays 0.60 so the
# throttle can be re-armed by moving the floor alone.
DEFAULT_BRAKE_SCALE: float = 0.60
DEFAULT_BRAKE_FLOOR: float = 1.0
DEFAULT_CONC_DOWNWEIGHT: bool = True
DEFAULT_MIN_BET_USD: float = 1.0


class SizedBet(NamedTuple):
    """Sizing breakdown for one v2 bet.

    ``stake``       final placeable USD stake (post $1 floor-up). There is no
                    per-coin depth/fill cap (removed by operator order
                    2026-07-11), so this is the raw equity-fraction size floored
                    up to the $1 Polymarket minimum.
    """

    stake: float


def _env_float(key: str, default: float) -> float:
    """Read a float env var, falling back to ``default`` on unset/blank/malformed.

    Sizing runs on the live tick path, so a stray non-numeric override must never
    raise here — it degrades to the safe default rather than crashing the tick.
    """
    raw = os.environ.get(key)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def base_fraction(interval: str) -> float:
    """Base stake fraction of equity for a sleeve's interval.

    Mirrors research ``np.where(interval == "5m", base_frac_5m, base_frac_15m)``: the
    5m sleeve rides at half weight; anything else (i.e. ``"15m"``) uses the 15m fraction.
    """
    if interval == "5m":
        return _env_float("CRYPTOBOT_V2_BASE_FRAC_5M", DEFAULT_BASE_FRAC_5M)
    return _env_float("CRYPTOBOT_V2_BASE_FRAC_15M", DEFAULT_BASE_FRAC_15M)


def brake_multiplier(dd: float) -> float:
    """Linear drawdown throttle: ``max(floor, 1 - dd / scale)``.

    ``dd`` is peak-equity drawdown as a fraction
    (``WalletSnapshot.drawdown_pct_peak_frac``, always >= 0). At dd=0 the multiplier is
    1.0; it falls linearly to the floor at the knee ``dd >= scale*(1-floor)`` and never
    drops below it. This is a *throttle* distinct from the hard HALT enforced upstream
    in the pipeline. Kept uncapped above 1.0 to match research bet-for-bet (dd >= 0
    keeps ``1 - dd/scale <= 1`` in practice).

    INERT AT THE 2026-07-31 DEFAULTS: floor 1.0 puts the knee at dd >= 0, so this
    returns 1.0 for every drawdown. The function is retained (not deleted) because the
    floor is an env knob — set CRYPTOBOT_V2_BRAKE_FLOOR < 1.0 to re-arm the throttle.
    """
    scale = _env_float("CRYPTOBOT_V2_BRAKE_SCALE", DEFAULT_BRAKE_SCALE)
    floor = _env_float("CRYPTOBOT_V2_BRAKE_FLOOR", DEFAULT_BRAKE_FLOOR)
    if scale <= 0.0:
        # Degenerate scale: throttle is undefined, fall to the floor (fail-safe small).
        return floor
    return max(floor, 1.0 - dd / scale)


def concurrency_weight(k: int) -> float:
    """Same-minute concurrency downweight ``1/sqrt(k)`` (k = concurrent bets this minute).

    RENORMALIZATION CAVEAT — shipped WITHOUT research's ``cw / cw.mean()`` step, by design:

    Research (the deleted ``pool_study.stake_weights``) computed ``k`` as ``value_counts`` over
    ``entry_ts // 60`` across the ENTIRE backtest bet frame, then renormalizes the raw
    weights by their mean over that whole frame (``cw / cw.mean()``). That mean is a
    GLOBAL quantity — it depends on every bet in the minute (and, via the frame, the
    whole run) — so a real-time per-second loop cannot reproduce it: at the instant a bet
    is placed, live does not yet know how many more bets the minute will hold. Any live
    proxy for the mean would be a different, path-dependent number, so matching research
    bet-for-bet on the renormalized scale is impossible.

    We therefore ship the raw ``1/sqrt(k)`` (<= 1 for all k >= 1). Consequence: the live
    absolute scale is slightly SMALLER than research on low-concurrency minutes (research
    can push ``cw/cw.mean()`` above 1), i.e. we err conservative. The brake and base
    fraction dominate the risk budget, so the missing renorm is a second-order scale
    effect, not a sign/edge change. Set ``CRYPTOBOT_V2_CONC_DOWNWEIGHT=0`` to disable the
    downweight entirely (weight == 1.0).
    """
    if not _env_bool("CRYPTOBOT_V2_CONC_DOWNWEIGHT", DEFAULT_CONC_DOWNWEIGHT):
        return 1.0
    k_eff = k if k >= 1 else 1
    return 1.0 / math.sqrt(k_eff)


def sized_usd_detail(
    equity: float,
    interval: str,
    dd: float,
    k: int = 1,
) -> SizedBet:
    """Full sizing breakdown for one v2 bet.

        raw   = equity * base_fraction(interval) * brake_multiplier(dd)
                * concurrency_weight(k)
        stake = floor_up(raw, MIN_BET_USD)

    The raw equity-fraction stake is floored UP to the $1 Polymarket minimum.
    There is NO per-coin depth/fill cap (removed by operator order 2026-07-11):
    the stake is never clamped to book depth. The hard 40% drawdown halt is gated
    separately upstream — this always returns a placeable stake.
    """
    raw = (
        equity
        * base_fraction(interval)
        * brake_multiplier(dd)
        * concurrency_weight(k)
    )
    min_bet = _env_float("CRYPTOBOT_MIN_BET_USD", DEFAULT_MIN_BET_USD)
    stake = raw if raw >= min_bet else min_bet
    return SizedBet(stake=stake)


def sized_usd(
    equity: float,
    interval: str,
    dd: float,
    k: int = 1,
) -> float:
    """USD stake for one v2 bet: equity-fraction stake, floored $1.

        stake = equity * base_fraction(interval) * brake_multiplier(dd)
                * concurrency_weight(k)

    then floored UP to ``MIN_BET_USD`` (research floors to $1 rather than skipping
    the bet); the caller must NOT then re-reject the floored $1 in the capital
    gate. There is no per-coin depth/fill cap (removed by operator order
    2026-07-11) — the stake follows bet size only.
    """
    return sized_usd_detail(equity, interval, dd, k=k).stake
