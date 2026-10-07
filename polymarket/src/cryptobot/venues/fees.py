"""Venue-dispatched taker fee — the CORE entry point.

Before Phase 2 this module was ``cryptobot/predictfun/fees.py``: a file inside
the predict.fun package that actually held BOTH venues' fee math plus the
dispatcher, so every Polymarket fee call imported the predict.fun package. The
split is now honest:

  * the dispatcher + the two strictness levels live HERE, in core;
  * the Polymarket PARABOLA lives in ``cryptobot_polymarket.fees``;
  * the predict.fun TENT lives in ``cryptobot_predictfun.fees``;
  * the registry wires them.

The two venues charge taker fees with DIFFERENT SHAPES, and that difference is
load-bearing for edge-after-fee — it must stay a dispatched function, never a
constant swap:

  * Polymarket — PARABOLA:  ``fee = POLY_FEE_THETA * p * (1 - p)`` with
    ``POLY_FEE_THETA = 0.07``.
  * predict.fun — TENT:     ``fee = (feeRateBps / 10000) * min(p, 1 - p)`` with
    a PER-MARKET ``feeRateBps`` (200 observed => 2%).

A favorite bought at p=0.80 therefore pays 0.02*0.20 = 0.004 on predict.fun vs
0.07*0.80*0.20 = 0.0112 on Polymarket.

Two strictness levels — do not mix them up
------------------------------------------
``fee_per_share`` / ``walk_fee_fn`` are the DECISION path and are FAIL-CLOSED:
an unknown venue raises, because a wrong fee shape on the live path is a
real-money edge error.

``row_fee_per_share`` is the GRADE-TIME path and is PERMISSIVE: it dispatches on
a LEDGER ROW's ``venue`` column, which is ``NOT NULL DEFAULT 'polymarket'``
across years of mixed history. Any value we cannot price — legacy blank, a
garbage string, or a registered venue with no fee shape implemented yet
(kalshi) — grades as polymarket, matching the column default. A raise there
would strand rows PENDING and wedge the settlement sweep.

The permissiveness has exactly ONE carve-out (2026-08-01): a row whose venue is
``config.VENUE`` — this process's OWN venue — raises when its adapter will not
load. The fallback's whole justification is cross-venue LEGACY history; applied
to our own venue it would book the wrong fee shape against live money on every
row, forever, with nothing in the P&L surface saying so.

HARD CONSTRAINT (unchanged from the pre-split module): the parity chain
(``config.POLY_FEE_THETA == backtest.sweep_lib.POLY_FEE_THETA ==
strategy.parity.POLY_FEE_THETA == 0.07``) stays untouched. This module is NOT
imported by ``strategy/spec_v2.py``, ``strategy/parity.py`` or
``strategy/hgb_features.py``; those keep their own inline formula.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from cryptobot.venues.registry import (
    KNOWN_VENUES,
    UnknownVenueError,
    get_adapter,
)

logger = logging.getLogger(__name__)

# Venue string constants — the single source of truth the config VENUE knob and
# every call-site dispatch compare against. Kept in the venue layer rather than
# in config so importing the fee helper never drags in the whole config module.
VENUE_POLYMARKET = "polymarket"
VENUE_PREDICTFUN = "predictfun"

# NOTE: the predict.fun default-bps fallback does NOT live here. It is one
# venue's knob, and this module is the venue-NEUTRAL dispatcher — it owns which
# adapter answers, never any adapter's arithmetic. It belongs to (and lives in)
# ``cryptobot_predictfun.fees``, which the PF adapter reads directly.


# ---------------------------------------------------------------------------
# DECISION path — fail-closed
# ---------------------------------------------------------------------------


def fee_per_share(
    ask: float,
    *,
    venue: str,
    fee_rate_bps: Optional[float] = None,
    theta: Optional[float] = None,
) -> float:
    """Dispatch fee-per-share on ``venue``.

    * ``polymarket`` -> PARABOLA (``theta`` REQUIRED; pass
      ``config.POLY_FEE_THETA`` so the Polymarket call site is byte-identical).
    * ``predictfun`` -> TENT (``fee_rate_bps`` falls back to
      ``cryptobot_predictfun.fees.PREDICTFUN_DEFAULT_FEE_RATE_BPS`` — the
      adapter's own knob — when the per-market value was not threaded to the
      call site).

    Fail-closed: an unknown venue raises ``UnknownVenueError`` (a ``ValueError``)
    rather than silently picking a shape, and a registered-but-unimplemented
    venue raises ``NotImplementedError`` from its own adapter.
    """
    return get_adapter(venue).fee_per_share(
        ask, fee_rate_bps=fee_rate_bps, theta=theta
    )


def walk_fee_fn(
    venue: str,
    *,
    fee_rate_bps: Optional[float] = None,
) -> Optional[Callable[[float], float]]:
    """Return the per-share fee function ``walk_book`` should use, or ``None``.

    ``None`` means "use the built-in parabola with the supplied ``fee_theta``" —
    i.e. the Polymarket path is unchanged and byte-identical. Only the
    predict.fun venue returns a callable (a TENT closure over the market's
    ``fee_rate_bps``), which ``walk_book_edge_preserving`` applies at each
    candidate VWAP in place of the parabola.

    This is the ONE seam through which the predict.fun fee reaches decision
    math, and it is inert (returns ``None``) on Polymarket, so an unpinned
    deploy never changes the walker's arithmetic.
    """
    return get_adapter(venue).walk_fee_fn(fee_rate_bps=fee_rate_bps)


# ---------------------------------------------------------------------------
# GRADE-TIME path — row-permissive
# ---------------------------------------------------------------------------

#: Venues whose fee SHAPE is implemented, OTHER than polymarket (the fallback).
#: A ledger row naming anything else grades as polymarket — see the module
#: docstring. This is a hand-maintained list on purpose: membership means "we
#: know this venue's fee curve and historic rows must be priced with it", which
#: is a stronger claim than "an adapter exists". kalshi is registered but its
#: curve is unknown, so it is deliberately absent and grades as polymarket.
#: Add a venue here in the same change that implements its fee shape.
_ROW_PRICEABLE = (VENUE_PREDICTFUN,)

_warned_unpriceable: set[str] = set()
#: Separate seen-set for the BROKEN-ADAPTER case. Not shared with
#: ``_warned_unpriceable``: "kalshi has no curve" is an expected, benign
#: WARNING, while "predictfun's package will not import" is an ERROR about a
#: box that is mis-provisioned, and one must never suppress the other.
_errored_unimportable: set[str] = set()


def row_fee_per_share(
    ask: float,
    *,
    venue: Optional[str],
    fee_rate_bps: Optional[float] = None,
    theta: Optional[float] = None,
) -> float:
    """Fee-per-share for a LEDGER ROW's venue — permissive on a bad value.

    Grade-time must keep settling. ``None``, ``""``, a legacy/garbage venue
    string, or a registered venue with no fee shape yet (``kalshi``) all take
    the Polymarket parabola, which matches the ledger column's
    ``NOT NULL DEFAULT 'polymarket'``.

    ONE exception (2026-08-01): a row whose venue equals ``config.VENUE`` — the
    venue this process actually trades — RAISES when its adapter cannot price
    it. Falling back there would book Polymarket-parabola fiction against our
    own live P&L, silently and permanently. Cross-venue legacy rows keep the
    loud-but-permissive fallback.
    """
    key = (venue or "").strip().lower()
    if key in _ROW_PRICEABLE:
        try:
            return get_adapter(key).fee_per_share(
                ask, fee_rate_bps=fee_rate_bps, theta=theta
            )
        except (UnknownVenueError, NotImplementedError, ImportError) as exc:
            # A venue we KNOW how to price, whose adapter would not load on
            # this box. The fallback stays — grading must never crash, and a
            # raise here strands rows PENDING and wedges the settlement sweep —
            # but it is NOT benign and must not be silent.
            #
            # Why ERROR and not WARNING: the caller is
            # ``execution.grade_fees.entry_fee_usd``, and that function keys its
            # calibration on the ROW's venue string, not on the shape it
            # actually got back (grade_fees.py:~97: `if key ==
            # VENUE_PREDICTFUN` skips POLY_FEE_CALIBRATION, "the tent is
            # venue-measured exact"). So when this fallback fires for
            # predictfun, the row books an UNCALIBRATED Polymarket parabola —
            # wrong shape AND the k factor deliberately withheld — with nothing
            # else in the P&L surface to say so. Every booked dollar downstream
            # of this line is suspect until the package is installed.
            _error_once(key, exc)
            # ...EXCEPT for OUR OWN venue. The permissive fallback exists for
            # CROSS-VENUE LEGACY rows: a Polymarket box holding historic
            # predict.fun rows must keep settling rather than wedge the sweep
            # (see the module docstring). But when the row's venue IS the venue
            # this process trades, a fallback would book poly-parabola fiction
            # against the live P&L of the money we are placing right now — and
            # do it silently, on every row, forever. A predict.fun box whose
            # own adapter will not import is mis-provisioned and must fail
            # loudly at the first graded row, not quietly mis-price its ledger.
            from cryptobot import config

            if key == (config.VENUE or "").strip().lower():
                raise
    elif key and key != VENUE_POLYMARKET:
        _warn_once(key)
    return get_adapter(VENUE_POLYMARKET).fee_per_share(ask, theta=theta)


def _warn_once(venue: str) -> None:
    if venue in _warned_unpriceable:
        return
    _warned_unpriceable.add(venue)
    logger.warning(
        "grade-time fee: venue %r has no fee shape here — grading as "
        "'polymarket' (ledger column default). Known venues: %s",
        venue,
        ", ".join(sorted(KNOWN_VENUES)),
    )


def _error_once(venue: str, exc: BaseException) -> None:
    if venue in _errored_unimportable:
        return
    _errored_unimportable.add(venue)
    logger.error(
        "grade-time fee: venue %r IS priceable but its adapter would not load "
        "(%s: %s) — falling back to the polymarket parabola so the settlement "
        "sweep keeps running. These rows book the WRONG fee shape, and "
        "grade_fees.entry_fee_usd keys POLY_FEE_CALIBRATION on the row's venue "
        "string, so a %r row also loses its calibration exemption. Install the "
        "venue package on this box. Known venues: %s",
        venue,
        type(exc).__name__,
        exc,
        venue,
        ", ".join(sorted(KNOWN_VENUES)),
    )


__all__ = [
    "VENUE_POLYMARKET",
    "VENUE_PREDICTFUN",
    "fee_per_share",
    "walk_fee_fn",
    "row_fee_per_share",
]
