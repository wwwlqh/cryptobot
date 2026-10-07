"""Calibrated grade-time entry-leg fee — venue-dispatched realized-PnL booking.

WHY (2026-07-23 fee audits): both venues charge their taker fee on EVERY
entry fill, but the ledger booked fees on WIN rows only — and predict.fun
rows booked the WRONG venue's fee entirely:

  * Old convention: WIN pnl_net = S*(1-p) - S*0.072*p*(1-p); LOSS = -S*p
    exactly (no fee); stop CANCELLED = S*(exit-entry) exactly (no fee either
    leg). S = shares, p = entry price.
  * Polymarket: a flat-to-flat wallet-tape audit (2026-07-21 22:11 ->
    2026-07-23 ~11:43 UTC; both endpoints zero positions + zero unsettled,
    transfers empty) measured realized venue friction $1.73 vs $2.18 modeled
    by theta*p*(1-p) per fill at the then-assumed theta=0.072 — the venue
    extracts ~0.84x of THAT parabola, i.e. an effective coefficient
    k*theta = 0.06048. When the published theta was corrected to 0.07
    (2026-08-05) the calibration was re-based to 0.864 so the product — the
    quantity the audit actually measured — is unchanged. The entry-leg fee is
    booked as ``POLY_FEE_CALIBRATION * POLY_FEE_THETA * p * (1-p) * S`` on
    every row class (booking the raw parabola on losers overcorrects ~14%). Stop
    EXIT sells are NOT fee-charged by the venue (verified: adding an exit
    fee degraded a wallet-tape reconciliation from $0.04 residual to $2.83),
    so only the entry leg is ever booked.
  * predict.fun: the real fee is the TENT — ``(fee_rate_bps/1e4) *
    min(p, 1-p)`` per share, taken IN SHARES on every fill (measured on real
    validation orders 2026-07-23, cross-check rel=2.0%; live markets carry
    feeRateBps=200 and every ledger row stores its market's
    ``fee_rate_bps``). The calibration knob does NOT apply — the 2% share
    fee is venue-measured exact. Before this module, PF WINs booked the
    POLYMARKET parabola (verified on live row id=1: 5.1594 sh @0.69 ->
    pnl 1.51996 = S(1-p) - S*0.072*p*(1-p)) — the wrong venue's fee.

Dispatch is on the ROW's ``venue`` column, never ``config.VENUE`` — a
mixed-history DB must grade each row under the venue that actually charged
it. Fee SHAPES come from ``cryptobot.venues.fees.row_fee_per_share``, the
ROW-PERMISSIVE half of the one venue-dispatch source; this module adds only the
grade-time calibration and the payout base — no formula is forked here.

Scope: GRADE-TIME BOOKING ONLY, forward-only. The pre-bet decision gates
(edge formula, walkers, the ``predictfun/fees.py`` walk seam, and the frozen
``strategy/parity.py`` <-> ``backtest.sweep_lib`` chain) are untouched; every
caller books through a PENDING-guarded write, so already-graded rows are
never rewritten and the pnl_net convention simply changes at the deploy
boundary.

Historical note (stop exits): the retired PURE_STOP_04 stop-loss booked the
ENTRY-leg fee only on both venues, because Polymarket verifiably does not
charge a taker fee on the exit SELL and predict.fun's exit-leg status was
never measured. The stop-loss was deleted 2026-08-05, so no new row can take
that path; already-booked stop rows keep the entry-leg-only convention.
"""

from __future__ import annotations

from cryptobot.venues.fees import VENUE_PREDICTFUN, row_fee_per_share


def entry_fee_usd(
    *,
    venue: str | None,
    entry_price: float,
    shares: float,
    fee_rate_bps: float | None = None,
) -> float:
    """USD entry-leg taker fee for ``shares`` bought at ``entry_price``.

    * ``predictfun`` -> TENT: ``(fee_rate_bps/1e4) * min(p, 1-p) * shares``
      with the ROW's ``fee_rate_bps`` (``None`` falls back to
      ``config.PREDICTFUN_DEFAULT_FEE_RATE_BPS``; an explicit 0.0 means a
      genuinely fee-free market and is honored). The calibration knob does
      NOT apply — the tent is venue-measured exact.
    * everything else -> calibrated PARABOLA:
      ``POLY_FEE_CALIBRATION * POLY_FEE_THETA * p * (1-p) * shares``. The
      ledger ``venue`` column is NOT NULL DEFAULT 'polymarket', so a legacy /
      unknown value grades as polymarket (matching the column DEFAULT) rather
      than raising and wedging the settlement sweep with rows stuck PENDING —
      grade-time must keep settling; the decision-path dispatch
      (``venues.fees.fee_per_share``) stays fail-closed.

    ``entry_price`` is clipped to ``[1e-6, 0.999]`` — the same clip
    ``strategy.parity`` applies — so garbage prices cannot produce a negative
    or runaway fee. Polymarket rows' decorative ``fee_rate_bps`` (72) is
    deliberately ignored: the poly fee comes from theta * calibration.
    """
    from cryptobot import config

    if float(shares) <= 0.0:
        return 0.0
    p = min(max(float(entry_price), 1e-6), 0.999)
    key = (venue or "").strip().lower()
    # row_fee_per_share owns the ROW-PERMISSIVE fallback: predictfun gets the
    # tent, and anything else — legacy blank, garbage string, or a registered
    # venue with no fee shape yet (kalshi) — gets the polymarket parabola,
    # matching the ledger column's NOT NULL DEFAULT 'polymarket'.
    shape = row_fee_per_share(
        p,
        venue=key,
        fee_rate_bps=fee_rate_bps,
        theta=float(config.POLY_FEE_THETA),
    )
    if key == VENUE_PREDICTFUN:
        # The 2% share fee is venue-measured exact — k must not scale the tent.
        #
        # NOTE the asymmetry: this branch keys on the ROW's venue STRING, while
        # `shape` above is whatever row_fee_per_share could actually price. On a
        # box where cryptobot_predictfun will not import, row_fee_per_share
        # falls back to the polymarket PARABOLA and lands here anyway — so the
        # row books a parabola with the calibration deliberately withheld, i.e.
        # the wrong shape AND the wrong k. That is why the fallback logs at
        # ERROR (venues/fees.py::_error_once) instead of passing quietly; there
        # is no other signal in the P&L surface that it happened.
        return shape * float(shares)
    return float(config.POLY_FEE_CALIBRATION) * shape * float(shares)


def graded_cash_pnl(
    side: str,
    fill_price: float,
    won_up: bool | int,
    stake_usd: float,
    *,
    venue: str | None,
    fee_rate_bps: float | None = None,
    slippage_haircut: float = 0.0,
) -> float:
    """Dollar realized P&L for a cash-sized bet under the calibrated convention.

    ``S = stake_usd / p`` shares (p clipped to ``[1e-6, 0.999]`` exactly like
    ``strategy.parity.cash_pnl``, so the payout base is unchanged vs the
    pre-calibration formula):

      WIN  = S*(1-p) - entry_fee_usd(...)
      LOSS = -S*p    - entry_fee_usd(...)   (loses the stake PLUS the fee)

    The entry-leg fee is charged on BOTH outcomes — the venue takes it on the
    fill, not on the win (module docstring has the measured derivations).
    Accepts YES/NO and Up/Down side names like the parity helpers.
    """
    effective = min(max(float(fill_price) + float(slippage_haircut), 1e-6), 0.999)
    shares = float(stake_usd) / effective
    won_side = (str(side).upper() in ("YES", "UP")) == bool(won_up)
    base = (1.0 - effective) * shares if won_side else -effective * shares
    return base - entry_fee_usd(
        venue=venue,
        entry_price=effective,
        shares=shares,
        fee_rate_bps=fee_rate_bps,
    )
