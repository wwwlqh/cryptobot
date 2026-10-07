"""Stake sizing for sub-minute bets: a fraction of equity, with an optional
drawdown brake and a same-minute concurrency weight.

Pure functions with no state or I/O; settings are read from the environment on
every call, so they can be tested in isolation and changed without a restart.
``pipeline.py`` calls them at its single sizing point.

    stake = equity
            * base_fraction(interval)
            * brake_multiplier(dd)   # 1.0 at the default floor
            * concurrency_weight(k)

then raised to the $1 Polymarket minimum (a sub-$1 stake is floored up, not
skipped; see ``sized_usd``). The stake is never capped by book depth; the
book-consumption gate handles depth.

Settings (env, read at call time):

  CRYPTOBOT_V2_BASE_FRAC_15M   default 0.010   15m stake = 1% of equity
  CRYPTOBOT_V2_BASE_FRAC_5M    default 0.010   5m stake = 1% of equity
  CRYPTOBOT_V2_BRAKE_SCALE     default 0.60    drawdown brake scale
  CRYPTOBOT_V2_BRAKE_FLOOR     default 1.0     1.0 turns the brake off
  CRYPTOBOT_V2_CONC_DOWNWEIGHT default 1       apply the 1/sqrt(k) weight
  CRYPTOBOT_MIN_BET_USD        default 1.0     $1 minimum (shared with config.py)

The hard drawdown halt (``config.MAX_DRAWDOWN_HALT_FRAC``) is enforced by the
pipeline, not here; this module always returns a placeable stake.

With the brake floor at 1.0, ``brake_multiplier`` is 1.0 at every drawdown.
Edge per dollar was measured to be flat across drawdown depth, so the brake
only slowed recovery; the small flat size plus the hard halt cover the risk
instead. Lower CRYPTOBOT_V2_BRAKE_FLOOR to turn it back on.
"""

from __future__ import annotations

import math
import os
from typing import NamedTuple

# Defaults, kept as constants so telemetry and tests have a reference when the
# environment is unset. Both intervals use a flat 1%.
DEFAULT_BASE_FRAC_15M: float = 0.010
DEFAULT_BASE_FRAC_5M: float = 0.010
# Brake defaults. A floor of 1.0 makes max(floor, 1 - dd/scale) == 1.0 for
# every dd >= 0; the scale is kept so the brake can be re-armed by changing the
# floor alone.
DEFAULT_BRAKE_SCALE: float = 0.60
DEFAULT_BRAKE_FLOOR: float = 1.0
DEFAULT_CONC_DOWNWEIGHT: bool = True
DEFAULT_MIN_BET_USD: float = 1.0


class SizedBet(NamedTuple):
    """Sizing breakdown for one bet.

    ``stake``       final USD stake: the equity-fraction size raised to the $1
    minimum. Not capped by book depth.
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
    """Linear drawdown brake: ``max(floor, 1 - dd / scale)``.

    ``dd`` is drawdown from peak equity as a fraction
    (``WalletSnapshot.drawdown_pct_peak_frac``, always >= 0). At dd=0 the
    multiplier is 1.0; it falls linearly to the floor at ``dd >=
    scale*(1-floor)`` and stays there. This is a size throttle, separate from
    the hard halt in the pipeline. Not capped above 1.0, matching the backtest
    (dd >= 0 keeps it <= 1 anyway).

    At the default floor of 1.0 it returns 1.0 for every drawdown; set
    CRYPTOBOT_V2_BRAKE_FLOOR below 1.0 to enable it.
    """
    scale = _env_float("CRYPTOBOT_V2_BRAKE_SCALE", DEFAULT_BRAKE_SCALE)
    floor = _env_float("CRYPTOBOT_V2_BRAKE_FLOOR", DEFAULT_BRAKE_FLOOR)
    if scale <= 0.0:
        # Degenerate scale: throttle is undefined, fall to the floor (fail-safe small).
        return floor
    return max(floor, 1.0 - dd / scale)


def concurrency_weight(k: int) -> float:
    """Same-minute concurrency weight ``1/sqrt(k)``, k = bets in the same
    minute.

    The backtest also divided the weights by their mean over the whole bet
    frame (``cw / cw.mean()``). Live cannot do that: when a bet is placed it is
    not known how many more will land in that minute, so the mean does not
    exist yet. The raw ``1/sqrt(k)`` (<= 1) is used instead, which makes live
    stakes slightly smaller than the backtest's on quiet minutes. That changes
    scale only, not edge or sign. Set ``CRYPTOBOT_V2_CONC_DOWNWEIGHT=0`` to
    turn the weight off (1.0).
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
    """Full sizing breakdown for one bet.

        raw   = equity * base_fraction(interval) * brake_multiplier(dd)
                * concurrency_weight(k)
        stake = floor_up(raw, MIN_BET_USD)

    Not capped by book depth. The hard drawdown halt is checked by the caller;
    this always returns a placeable stake.
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
    """USD stake for one bet:

        stake = equity * base_fraction(interval) * brake_multiplier(dd)
                * concurrency_weight(k)

    raised to ``MIN_BET_USD`` if smaller (the backtest floors to $1 rather than
    skipping). The caller must not then reject that $1 in the capital gate.
    """
    return sized_usd_detail(equity, interval, dd, k=k).stake
