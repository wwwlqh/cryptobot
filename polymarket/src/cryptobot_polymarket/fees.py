"""Polymarket taker fee — the PARABOLA.

``fee = POLY_FEE_THETA * p * (1 - p)`` with ``POLY_FEE_THETA = 0.07``
(``= feeRateBps/1000`` on those markets, but the decision math uses theta
directly; feeRateBps is decorative there).

Split out of the old dual-venue ``cryptobot/predictfun/fees.py`` in Phase 2; the
dispatcher now lives in ``cryptobot.venues.fees`` and the predict.fun TENT in
``cryptobot_predictfun.fees``.

HARD CONSTRAINT (carried over verbatim): this is a venue-layer MIRROR of the
shape ``walk_book`` / the strategy compute inline. It is NOT imported by
``strategy/spec_v2.py``, ``strategy/parity.py`` or ``strategy/hgb_features.py``
— the parity chain (``config.POLY_FEE_THETA == backtest.sweep_lib.POLY_FEE_THETA
== strategy.parity.POLY_FEE_THETA == 0.07``) stays frozen and keeps its own
inline formula.
"""

from __future__ import annotations


def poly_fee_per_share(ask: float, theta: float) -> float:
    """PARABOLA fee-per-share ``theta * ask * (1 - ask)``.

    Reproduced with the identical float-op order (``theta * ask * (1.0 - ask)``)
    so routing the Polymarket path through the venue dispatch is byte-identical.
    """
    return theta * ask * (1.0 - ask)


__all__ = ["poly_fee_per_share"]
