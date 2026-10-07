"""predict.fun taker fee — the TENT.

``fee = (feeRateBps / 10000) * min(p, 1 - p)`` with a PER-MARKET ``feeRateBps``
(200 observed => 2%). Symmetric about p=0.5, linear on each side, peaking at the
middle — a genuinely different curve from Polymarket's parabola, so a favorite
bought at p=0.80 pays 0.02*0.20 = 0.004 here vs 0.072*0.80*0.20 = 0.01152 there.

Phase 2: this file used to be ``cryptobot/predictfun/fees.py`` and held BOTH
venues' math plus the dispatcher, which meant every Polymarket fee call imported
the predict.fun package. The dispatcher now lives in ``cryptobot.venues.fees``
and the parabola in ``cryptobot_polymarket.fees``.

HARD CONSTRAINT (unchanged): the tent only reaches decision math through the
venue-dispatched call sites (``cryptobot.venues.fees.walk_fee_fn`` /
``fee_per_share`` from ``execution/pipeline.py`` and ``execution/walk_book.py``),
and only when the active venue is predict.fun. It must never be imported by
``strategy/spec_v2.py``, ``strategy/parity.py`` or ``strategy/hgb_features.py``.
"""

from __future__ import annotations


def _default_fee_rate_bps() -> float:
    """The env-overridable fallback bps, read lazily.

    Sourced from ``config.PREDICTFUN_DEFAULT_FEE_RATE_BPS`` (default 200 bps =
    2%, the value observed on every probed predict.fun market, 2026-07-21) —
    ONE source of truth. Imported inside the function so this module keeps its
    deliberate no-config-import-at-module-scope design. The REAL bps is read
    per-market in ``markets.py`` and threaded to the call sites where available.
    """
    from cryptobot import config

    return float(config.PREDICTFUN_DEFAULT_FEE_RATE_BPS)


def __getattr__(name: str):
    if name == "PREDICTFUN_DEFAULT_FEE_RATE_BPS":
        return _default_fee_rate_bps()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def predictfun_fee_per_share(ask: float, fee_rate_bps: float) -> float:
    """TENT fee-per-share ``(fee_rate_bps / 10000) * min(ask, 1-ask)``.

    ``fee_rate_bps`` is the per-market ``feeRateBps`` (200 => 2%). Symmetric
    about 0.5. No clamping of ``ask`` here — callers pass a book price in
    (0, 1); ``min(ask, 1-ask)`` is correct across the whole open interval and
    naturally goes to 0 at the p=0/p=1 boundaries.
    """
    return (fee_rate_bps / 10000.0) * min(ask, 1.0 - ask)


__all__ = ["predictfun_fee_per_share"]
