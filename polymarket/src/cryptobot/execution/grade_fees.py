"""Grade-time entry fee: realized P&L booking for settled bets.

Polymarket charges its taker fee on every entry fill, win or lose. A
flat-to-flat wallet audit measured realized fees at 0.864 x the published
``theta * p * (1 - p)`` curve (theta = 0.07), so the booked fee is
``POLY_FEE_CALIBRATION * POLY_FEE_THETA * p * (1 - p) * shares``.

This applies only where realized ``pnl_net`` is booked. Entry decisions
(edge gates, the book walker, ``strategy.parity``) use the raw published theta.
"""

from __future__ import annotations

from cryptobot.venues.fees import fee_per_share


def entry_fee_usd(*, entry_price: float, shares: float) -> float:
    """USD entry-leg taker fee for ``shares`` bought at ``entry_price``.

    ``entry_price`` is clipped to ``[1e-6, 0.999]``, the same clip
    ``strategy.parity`` applies, so bad prices cannot produce a negative or
    runaway fee.
    """
    from cryptobot import config

    if float(shares) <= 0.0:
        return 0.0
    p = min(max(float(entry_price), 1e-6), 0.999)
    shape = fee_per_share(p, theta=float(config.POLY_FEE_THETA))
    return float(config.POLY_FEE_CALIBRATION) * shape * float(shares)


def graded_cash_pnl(
    side: str,
    fill_price: float,
    won_up: bool | int,
    stake_usd: float,
    *,
    slippage_haircut: float = 0.0,
) -> float:
    """Dollar realized P&L for a cash-sized bet.

    With ``S = stake_usd / p`` shares (p clipped like ``strategy.parity``):

      WIN  = S*(1-p) - entry_fee_usd(...)
      LOSS = -S*p    - entry_fee_usd(...)

    Accepts YES/NO and Up/Down side names.
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    shares = float(stake_usd) / effective
    won_side = (str(side).upper() in ("YES", "UP")) == bool(won_up)
    base = (1.0 - effective) * shares if won_side else -effective * shares
    return base - entry_fee_usd(entry_price=effective, shares=shares)
