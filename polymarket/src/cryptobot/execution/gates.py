"""7-gate stack with first-failure short-circuit.

Order (carry-forward of brainstorm gate stack plus R-NEW additions):

  1. disk         — disk_free_mb >= MIN_DISK_FREE_MB (R39)
  2. freshness    — spot quote age <= SPOT_STALENESS_S (R8)
  3. fee          — fee_rate_bps known + > 0 (R9 fail-closed)
  3b. threshold   — hgb: realized_edge(==pred_edge) >= sleeve.threshold
                    (inert when sleeve.threshold is None)
  4. edge         — sleeve.min_edge <= realized_edge <= sleeve.max_edge (D7 order)
  4b. topup_edge_floor — is_topup only: realized_edge >= TOPUP_EDGE_FLOOR
                    (2026-07-23 incident; inert for entries)
  5. partial_ret_z — chunk-CV z bound (inert when both bounds None)
  6. capital      — bet_size >= MIN_BET_USD (R13 + R-NEW-8 partial-fill case)
  6b. book_consumption — exp044 hgb: bet_size <= book_depth * book_consumption_max
                    (inert when sleeve.book_consumption_max is None)
  7. exposure     — pending + bet_size <= wallet cap (R-NEW-5)
  8. group_exposure — LIVE-only per-group cap (D-LIVE-2 / D-LIVE-4)
  9. slot_full    — slot_filled_usd < target_usd (R-NEW-10)

Reason strings match the dashboard vocabulary verbatim per R-NEW-13:
  SKIP:disk_low / SKIP:stale_spot / SKIP:stale_book / SKIP:fee_unknown /
  SKIP:edge_below_threshold / SKIP:edge_below_floor / SKIP:edge_above_ceiling /
  SKIP:topup_edge_below_floor /
  SKIP:insufficient_capital / SKIP:book_consumption_cap / SKIP:exposure_cap /
  SKIP:slot_full / SKIP:insufficient_depth /
  SKIP:fee_band / SKIP:fill_band / SKIP:lut_missing / SKIP:not_entry_minute

D7 invariant: the edge gate evaluates ``realized_edge = fee_adjusted_edge
- sleeve.slippage_haircut`` — haircut subtracted BEFORE the
``min_edge <= edge <= max_edge`` band check. Same order as backtest.

Current live cap denominator: ``wallet_realized_capital * exposure_cap_fraction``.
This matches dry-run bet sizing, which scales from current realized capital
instead of a fixed $100 notional.
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
    # Total ask-side notional ($) at decision time — sum(price*size) across all
    # ask levels of the side being bought. Drives the exp044 book_consumption
    # gate; None on legacy lut|rf|blend ticks (gate inert there).
    book_total_ask_depth_usd: Optional[float] = None
    is_topup: bool = False
    exposure_cap_fraction: float = config.SHARED_EXPOSURE_CAP
    # U4 of the all_4 deploy plan: chunk-CV v6 sleeves may set
    # partial_ret_z_floor/ceiling. None on the sleeve = gate inert.
    # None on the input z (when the sleeve sets a bound) = emit
    # partial_ret_z_unavailable.
    partial_ret_z_1h: Optional[float] = None
    # Per-strategy LIVE deploy group exposure (D-LIVE-2 / D-LIVE-4 / D-LIVE-14).
    # All optional; when group_cap_usd is None the group_exposure gate is a
    # no-op (legacy DRY-only paths, ungrouped wallets, test fixtures).
    mode: str = "DRY"
    group_cap_usd: Optional[float] = None
    group_pending_usd: float = 0.0
    live_wallet_balance_usd: Optional[float] = None
    # Defaults to live_wallet_balance_usd for legacy callers. Live routing sets
    # this to cash plus already-funded LIVE pending exposure.
    live_cap_basis_usd: Optional[float] = None
    all_group_caps_sum_usd: float = 0.0
    # 2026-07-05 backtest-exposure mode (WalletGroup.cap_tracks_equity). When
    # True, the group's concurrent-exposure ceiling = live equity (cash + funded
    # pending) instead of the static cap_usd / D-LIVE-4 shrink — so it grows with
    # the account, matching the backtest. Default False = fixed-cap behavior.
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
    # R9 fail-closed. Unknown / NaN / <=0 fee → refuse to bet.
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
    """exp044 HGB threshold gate: ``pred_edge >= sleeve.threshold``.

    Inert for non-hgb sleeves (``threshold`` is None there). For hgb sleeves
    the score's ``realized_edge`` == ``pred_edge`` (slippage_haircut == 0.0),
    so this reproduces the backtest's ``test["pred_edge"] >= threshold`` filter.
    """
    thr = getattr(inp.sleeve, "threshold", None)
    if thr is None:
        return None
    if inp.realized_edge < thr:
        return "edge_below_threshold"
    return None


def _gate_edge(inp: GateInput) -> Optional[str]:
    # Band check: AE8 (edge_above_ceiling) + min_edge floor. D7 order: edge
    # is already haircut-adjusted before this gate runs.
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
    """Top-up-only edge floor (config.TOPUP_EDGE_FLOOR, 2026-07-23 incident).

    A top-up re-prices this tick's edge, and one placed while that recomputed
    edge sits under the floor contradicts the sizing logic's own claim (live
    id7: a scale-in top-up filled at realized_edge -0.0119 — the ceiling-only
    cap could never stop it). Inert for entries (``is_topup`` False) — the
    sleeve min_edge/threshold gates own those. Strict ``<``, so an edge of
    exactly the floor passes; the 0.0 default blocks only NEGATIVE-edge
    top-ups. Venue-independent by construction (no venue input).
    """
    if not inp.is_topup:
        return None
    if inp.realized_edge < config.TOPUP_EDGE_FLOOR:
        return "topup_edge_below_floor"
    return None


def _gate_partial_ret_z(inp: GateInput) -> Optional[str]:
    """U4: chunk-CV `partial_ret_z_floor` / `partial_ret_z_ceiling` gate.
    Inert when both bounds are None on the sleeve."""
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
    # R-NEW-8: walker returns None on insufficient depth → bet_size=0.
    # Treat any proposed bet < MIN_BET_USD as insufficient_capital.
    if inp.proposed_bet_size < config.MIN_BET_USD:
        return "insufficient_capital"
    return None


def _gate_book_consumption(inp: GateInput) -> Optional[str]:
    """exp044 per-tick book-consumption cap: bet notional <= depth * frac.

    Inert for non-hgb sleeves (``book_consumption_max`` is None there). For
    hgb sleeves the proposed bet may not consume more than ``frac`` of the
    total ask-side depth on this tick. A missing / non-positive depth means we
    cannot size against the book — fail closed with ``insufficient_depth``.
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
    # R-NEW-10 + AE-NEW-7. Compare with relative tolerance so float dust
    # doesn't keep the slot perpetually 0.001 away from full.
    target = inp.slot_target_usd
    if target <= 0:
        return None
    if inp.slot_filled_usd + 1e-9 >= target * (1.0 - config.CLOSE_SIZE_REL_TOLERANCE):
        return "slot_full"
    return None


def _gate_group_exposure(inp: GateInput) -> Optional[str]:
    """LIVE-only group cap (D-LIVE-2 / D-LIVE-4 / D-LIVE-14).

    DRY rows skip this gate — the parity shadow never consumes real-money
    headroom. Ungrouped wallets (group_cap_usd None) also skip.

    Effective cap shrinks proportionally when the live cap basis drops below
    SUM(all_group_caps): each group's share of the available basis is
    `cap_usd / SUM(all_group_caps) x live_cap_basis`. In live routing, the
    basis includes cash plus already-funded LIVE pending exposure; legacy
    callers fall back to raw live funder balance.
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
        # Backtest-exposure mode: ceiling = live equity (grows with the account);
        # no D-LIVE-4 shrink. static_cap is only the fallback if the balance fetch
        # failed — and funder_balance_unknown already blocks that tick.
        effective_cap = max(0.0, float(live_basis)) if live_basis is not None else static_cap
    else:
        effective_cap = static_cap
        # NOTE: a cap_tracks_equity group sets all_group_caps_sum_usd to 0
        # (funder floor retired), so this shrink stays inert for it by design.
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
