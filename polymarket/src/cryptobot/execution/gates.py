"""Pre-trade gate stack; the first failing gate decides the skip reason.

Order:

  1. disk             disk_free_mb >= MIN_DISK_FREE_MB
  2. freshness        spot quote age <= SPOT_STALENESS_S
  3. fee              fee_rate_bps known and > 0
  3b. threshold       pred_edge >= sleeve.threshold (if set)
  4. edge             sleeve.min_edge <= realized_edge <= sleeve.max_edge
  4b. topup_edge_floor  top-ups only: realized_edge >= TOPUP_EDGE_FLOOR
  5. partial_ret_z    optional z bounds (off when both are None)
  6. capital          bet_size >= MIN_BET_USD
  6b. book_consumption  bet_size <= book_depth * book_consumption_max (if set)
  7. exposure         pending + bet_size <= wallet cap
  8. group_exposure   live only: per-group cap
  9. slot_full        slot_filled_usd < target_usd

Reason strings match the dashboard's labels exactly, e.g. SKIP:disk_low,
SKIP:stale_spot, SKIP:stale_book, SKIP:fee_unknown, SKIP:edge_below_threshold,
SKIP:edge_below_floor, SKIP:edge_above_ceiling, SKIP:topup_edge_below_floor,
SKIP:insufficient_capital, SKIP:book_consumption_cap, SKIP:exposure_cap,
SKIP:slot_full, SKIP:insufficient_depth, SKIP:fill_band.

The edge gate checks ``realized_edge = fee_adjusted_edge -
sleeve.slippage_haircut``: the haircut is subtracted before the ``min_edge <=
edge <= max_edge`` check, in the same order as the backtest.

The exposure cap is ``wallet_realized_capital * exposure_cap_fraction``,
scaling with current capital like the bet size does.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from cryptobot import config
from cryptobot.strategy.portfolio_loader import Sleeve


@dataclass(frozen=True)
class GateInput:
    """Everything the gate stack needs for one cell evaluation."""

    coin: str
    sleeve: Sleeve
    entry_minute: int
    side: str
    # decision-time observations
    realized_edge: float          # fee-adjusted edge minus slippage_haircut (D7)
    fill_price: float             # top_ask used as the fill_price for parity (D2)
    fee_rate_bps: Optional[float]  # None / NaN / <=0 → SKIP:fee_unknown
    spot_age_s: float
    # capital / exposure
    proposed_bet_size: float
    pending_exposure_total: float
    realized_capital: float
    slot_filled_usd: float
    slot_target_usd: float
    # disk + book
    disk_free_mb: float
    book_top_ask_age_s: float
    # Total ask-side notional ($) at decision time, sum(price * size) over the
    # ask levels of the side being bought. Used by the book_consumption gate;
    # None disables it.
    book_total_ask_depth_usd: Optional[float] = None
    is_topup: bool = False
    exposure_cap_fraction: float = config.SHARED_EXPOSURE_CAP
    # Optional sleeve bounds on partial_ret_z. With no bound on the sleeve the
    # gate is off; with a bound but no z here it emits
    # partial_ret_z_unavailable.
    partial_ret_z_1h: Optional[float] = None
    # Group exposure inputs. All optional; with group_cap_usd None the
    # group_exposure gate does nothing (DRY paths, ungrouped wallets, tests).
    mode: str = "DRY"
    group_cap_usd: Optional[float] = None
    group_pending_usd: float = 0.0
    live_wallet_balance_usd: Optional[float] = None
    # Defaults to live_wallet_balance_usd for legacy callers. Live routing sets
    # this to cash plus already-funded LIVE pending exposure.
    live_cap_basis_usd: Optional[float] = None
    all_group_caps_sum_usd: float = 0.0
    # When True (WalletGroup.cap_tracks_equity), the group's exposure ceiling
    # is live equity (cash plus funded pending) instead of the static cap_usd,
    # so it grows with the account as in the backtest. False keeps the fixed
    # cap.
    group_cap_tracks_equity: bool = False


@dataclass(frozen=True)
class GateResult:
    passed: bool
    skip_reason: Optional[str]  # None when passed


MIN_DISK_FREE_MB = 200.0   # carry-forward R39
BOOK_STALENESS_S = 5.0     # carry-forward R8 for top-of-book ages


def _gate_disk(inp: GateInput) -> Optional[str]:
    if inp.disk_free_mb < MIN_DISK_FREE_MB:
        return "disk_low"
    return None


def _gate_freshness(inp: GateInput) -> Optional[str]:
    if inp.spot_age_s > config.SPOT_STALENESS_S:
        return "stale_spot"
    if inp.book_top_ask_age_s > BOOK_STALENESS_S:
        return "stale_book"
    return None


def _gate_fee(inp: GateInput) -> Optional[str]:
    # Unknown, NaN or non-positive fee: refuse.
    fee = inp.fee_rate_bps
    if fee is None:
        return "fee_unknown"
    try:
        f = float(fee)
    except (TypeError, ValueError):
        return "fee_unknown"
    if not (f > 0.0) or f != f:  # NaN guard
        return "fee_unknown"
    return None


def _gate_threshold(inp: GateInput) -> Optional[str]:
    """Threshold gate: ``pred_edge >= sleeve.threshold``.

    Off when the sleeve has no threshold. For HGB sleeves ``realized_edge``
    equals ``pred_edge`` (no haircut), so this is the backtest's ``pred_edge >=
    threshold`` filter.
    """
    thr = getattr(inp.sleeve, "threshold", None)
    if thr is None:
        return None
    if inp.realized_edge < thr:
        return "edge_below_threshold"
    return None


def _gate_edge(inp: GateInput) -> Optional[str]:
    # Band check: edge ceiling and min_edge floor. The edge already includes
    # the haircut.
    sleeve = inp.sleeve
    if inp.realized_edge < sleeve.min_edge:
        return "edge_below_floor"
    if inp.realized_edge > sleeve.max_edge:
        return "edge_above_ceiling"
    # Fill-price band — carry-forward (matches backtest's _apply_filters).
    if inp.fill_price < sleeve.min_fill_price or inp.fill_price > sleeve.max_fill_price:
        return "fill_band"
    return None


def _gate_topup_edge_floor(inp: GateInput) -> Optional[str]:
    """Edge floor for top-ups (config.TOPUP_EDGE_FLOOR).

    A top-up re-prices its edge, and placing one whose recomputed edge is below
    the floor contradicts the sizing logic; the ceiling check alone cannot stop
    a negative-edge top-up. First entries are not affected (the sleeve min_edge
    and threshold gates handle those). Strict ``<``, so an edge exactly at the
    floor passes; the 0.0 default only blocks negative-edge top-ups.
    """
    if not inp.is_topup:
        return None
    if inp.realized_edge < config.TOPUP_EDGE_FLOOR:
        return "topup_edge_below_floor"
    return None


def _gate_partial_ret_z(inp: GateInput) -> Optional[str]:
    """Optional ``partial_ret_z_floor`` / ``partial_ret_z_ceiling`` gate; off
    when both are None on the sleeve.
    """
    sleeve = inp.sleeve
    floor = getattr(sleeve, "partial_ret_z_floor", None)
    ceiling = getattr(sleeve, "partial_ret_z_ceiling", None)
    if floor is None and ceiling is None:
        return None
    z = inp.partial_ret_z_1h
    if z is None:
        return "partial_ret_z_unavailable"
    if floor is not None and z < floor:
        return "partial_ret_z_below_floor"
    if ceiling is not None and z > ceiling:
        return "partial_ret_z_above_ceiling"
    return None


def _gate_capital(inp: GateInput) -> Optional[str]:
    # The walker returns None when depth is insufficient, giving bet_size=0;
    # any bet below MIN_BET_USD is insufficient_capital.
    if inp.proposed_bet_size < config.MIN_BET_USD:
        return "insufficient_capital"
    return None


def _gate_book_consumption(inp: GateInput) -> Optional[str]:
    """Book-consumption cap: bet notional <= depth * frac.

    Off when the sleeve has no ``book_consumption_max``. Otherwise the bet may
    not take more than ``frac`` of the total ask-side depth at this tick.
    Missing or non-positive depth means the bet cannot be sized against the
    book, so fail with ``insufficient_depth``.
    """
    frac = getattr(inp.sleeve, "book_consumption_max", None)
    if frac is None:
        return None
    depth = inp.book_total_ask_depth_usd
    if depth is None or depth <= 0:
        return "insufficient_depth"
    if inp.proposed_bet_size > depth * frac + 1e-9:
        return "book_consumption_cap"
    return None


def _gate_exposure(inp: GateInput) -> Optional[str]:
    # Exposure cap is binary: if wallet pending + bet would exceed the cap, skip
    # entirely. Never partial-fill into the cap.
    cap_usd = max(0.0, float(inp.realized_capital)) * inp.exposure_cap_fraction
    if inp.pending_exposure_total + inp.proposed_bet_size > cap_usd + 1e-9:
        return "exposure_cap"
    return None


def _gate_slot_full(inp: GateInput) -> Optional[str]:
    # Relative tolerance so float dust cannot leave a slot permanently a
    # fraction of a cent from full.
    target = inp.slot_target_usd
    if target <= 0:
        return None
    if inp.slot_filled_usd + 1e-9 >= target * (1.0 - config.CLOSE_SIZE_REL_TOLERANCE):
        return "slot_full"
    return None


def _gate_group_exposure(inp: GateInput) -> Optional[str]:
    """Live-only group cap.

    DRY rows skip this gate, since a dry run uses no real money. Ungrouped
    wallets (group_cap_usd None) skip it too.

    The cap shrinks proportionally when the live cap basis falls below the sum
    of all group caps: each group gets ``cap_usd / SUM(all_group_caps) *
    live_cap_basis``. The basis is cash plus already-funded live pending
    exposure, falling back to the raw funder balance for older callers.
    """
    if inp.mode != "LIVE":
        return None
    if inp.group_cap_usd is None:
        return None

    static_cap = float(inp.group_cap_usd)
    live_basis = (
        inp.live_cap_basis_usd
        if inp.live_cap_basis_usd is not None
        else inp.live_wallet_balance_usd
    )
    if inp.group_cap_tracks_equity:
        # Equity-tracking mode: the ceiling is live equity, with no
        # proportional shrink. static_cap is only a fallback if the balance
        # fetch failed, and funder_balance_unknown already blocks that tick.
        effective_cap = max(0.0, float(live_basis)) if live_basis is not None else static_cap
    else:
        effective_cap = static_cap
        # A cap_tracks_equity group contributes 0 to all_group_caps_sum_usd, so
        # this shrink never applies to it.
        if live_basis is not None and inp.all_group_caps_sum_usd > 0:
            share = static_cap / float(inp.all_group_caps_sum_usd)
            scaled = max(0.0, float(live_basis)) * share
            if scaled < effective_cap:
                effective_cap = scaled

    if inp.group_pending_usd + inp.proposed_bet_size > effective_cap + 1e-9:
        return "group_exposure_cap"
    return None


# Gate stack in evaluation order. Each entry is (name, predicate_fn).
_GATE_STACK = (
    ("disk", _gate_disk),
    ("freshness", _gate_freshness),
    ("fee", _gate_fee),
    ("threshold", _gate_threshold),
    ("edge", _gate_edge),
    ("topup_edge_floor", _gate_topup_edge_floor),
    ("partial_ret_z", _gate_partial_ret_z),
    ("capital", _gate_capital),
    ("book_consumption", _gate_book_consumption),
    ("exposure", _gate_exposure),
    ("group_exposure", _gate_group_exposure),
    ("slot_full", _gate_slot_full),
)

GATE_NAMES = tuple(name for name, _ in _GATE_STACK)


def evaluate_gates(inp: GateInput) -> GateResult:
    """Walk the gate stack; return on first failure.

    Gate ordering is deterministic across runs — `_GATE_STACK` is a tuple,
    not a dict. ``disk`` fires before ``freshness`` even when both would
    fail, so post-hoc analysis can attribute skips correctly.
    """
    for _name, fn in _GATE_STACK:
        reason = fn(inp)
        if reason is not None:
            return GateResult(passed=False, skip_reason=reason)
    return GateResult(passed=True, skip_reason=None)
