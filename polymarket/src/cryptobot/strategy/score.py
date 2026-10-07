"""Glue: per (coin, sleeve, checkpoint-second) compute (model_p, side, edge).

Pipeline per cell (``score_cell_hgb``):
  1. compute partial_ret_z_1h (PartialRetEngine.current_z) at the FRACTIONAL
     minute ``offset_sec / 60``
  2. checkpoint RF.predict_proba slice → model_p_v1
  3. build the 11-set feature row per executable side and serve the
     per-interval checkpoint HGB → model_prob
  4. compute the fee-adjusted pred_edge from the side-specific ask
  5. side = the higher-edge side, matching the backtest's _dedupe_best_side
  6. selected edge minus `sleeve.slippage_haircut` (D7 ordering)

2026-07-31 (exp044 phase 2): the legacy lut|rf|blend scorers
(``score_probability`` / ``score_pair_edges`` / ``score_cell``) went with the
pair-sleeve subsystem and the decision_table/LUT chain. This module no longer
imports ``lut_lookup`` at all; ``ScoreResult``'s ``lut_*`` fields survive only
because they are ``signals``/``ledger`` DB columns, and the checkpoint path
always writes the -1/None/0 sentinels.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from cryptobot import config
from cryptobot.strategy.hgb_features import build_feature_vector
from cryptobot.strategy.hgb_loader import HgbArtifact, predict_model_prob
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.strategy.rf_loader import RfArtifact, predict_proba
from cryptobot.strategy.portfolio_loader import Sleeve
from cryptobot.strategy.twap_loader import (
    twap_execution_params,
    twap_feature_params,
)
from cryptobot.venues.fees import fee_per_share


@dataclass(frozen=True)
class ScoreResult:
    model_p: float
    side: str            # "YES" or "NO"
    edge: float          # fee-adjusted edge minus slippage_haircut (D7)
    realized_edge: float  # alias for clarity downstream
    # Retired-LUT sentinels (-1 / None / 0). Kept because `signals` and
    # `ledger` carry them as columns; the checkpoint scorer never populates them.
    lut_p_bucket: int
    lut_hit_rate: Optional[float]
    lut_n_cum: int
    partial_ret_z_1h: Optional[float]
    skip_reason: Optional[str]   # populated when score() returns a SKIP-row reason
    # ORB live port. Unsigned window-anchored log-returns from PartialRetEngine,
    # carried to the ORB gate which signs them by the chosen side and checks the
    # per-coin filter. None on cells where the opening range is not yet closed →
    # the ORB gate fails closed (orb_no_feature). entry_z for ORB is the raw
    # ``partial_ret_z_1h`` above.
    orb_range_ret: Optional[float] = None
    orb_entry_ret: Optional[float] = None


def _fee_adjusted_edge(
    model_p: float,
    ask: float,
    *,
    fee_rate_bps: Optional[float] = None,
) -> float:
    """Model edge net of the taker fee: ``p - ask - theta * ask * (1 - ask)``.

    Same float-op order as ``backtest.engine.sweep_lib.edge`` and
    ``strategy.parity.edge``; ``tests/test_audit_parity.py`` pins the three.
    """
    return model_p - ask - fee_per_share(ask, theta=config.POLY_FEE_THETA)


def score_side_hgb(
    *,
    coin: str,
    sleeve: Sleeve,
    offset_sec: int,
    side: str,
    live_spot: float,
    held_ask: float,
    other_ask: float,
    window_start_ts: int,
    partial_ret_engine: PartialRetEngine,
    checkpoint_rf: Optional[RfArtifact],
    checkpoint_hgb: Optional[HgbArtifact],
) -> Optional[float]:
    """Re-score ONE specific (held) side at a checkpoint second → model_p_side.

    This is the per-second held-position re-scorer (operator 2026-07-09,
    execution/held_position_pass.py). It reuses the EXACT checkpoint feature/model path
    that ``score_cell_hgb`` runs for the sub-minute entry — same per-checkpoint
    ``checkpoint_rf``/``checkpoint_hgb`` artifacts, same ``build_feature_vector``
    inputs, same ``predict_model_prob`` — but for the caller-specified held side
    rather than the best-edge side, and WITHOUT the ask-band bucket filter (a
    held position is re-valued regardless of where its ask has drifted). Cost is
    one HGB predict per open position per second.

    Returns ``model_p`` for ``side`` (YES→Up, NO→Down), or ``None`` on any
    missing input (no feature, no RF slice, no HGB, un-buildable feature row) so
    the caller simply skips the stop evaluation this tick and holds.
    """
    if checkpoint_rf is None or checkpoint_hgb is None:
        return None
    if not (isinstance(held_ask, (int, float)) and math.isfinite(float(held_ask))):
        return None
    if not (isinstance(other_ask, (int, float)) and math.isfinite(float(other_ask))):
        return None
    a = float(held_ask)
    if not (0.0 < a < 1.0):
        return None

    em_value: float = offset_sec / 60.0
    z = partial_ret_engine.current_z(
        coin,
        interval_min=sleeve.interval_min,
        live_spot=live_spot,
        entry_minute=em_value,
    )
    if z is None:
        return None

    rf_proba = predict_proba(
        checkpoint_rf,
        coin=coin,
        interval=sleeve.interval,
        entry_minute=offset_sec,
        partial_ret_z_1h=z,
    )
    if rf_proba is None:
        return None

    feat_side = "Up" if side == "YES" else "Down"
    feat = build_feature_vector(
        side=feat_side,
        coin=coin,
        interval=sleeve.interval,
        interval_min=sleeve.interval_min,
        entry_minute=em_value,
        window_start_ts=window_start_ts,
        model_p_up=rf_proba,
        ask=a,
        other_ask=float(other_ask),
        partial_ret_z_1h=z,
        feature_columns=checkpoint_hgb.columns_for(sleeve.interval),
    )
    if feat is None:
        return None
    return float(
        predict_model_prob(
            checkpoint_hgb, interval=sleeve.interval, feature_row_values=feat
        )
    )


def _twap_feature_vector(
    *,
    side: str,
    coin: str,
    sleeve: Sleeve,
    entry_minute: float,
    feature_ts: int,
    ask: float,
    other_ask: float,
    z_up: float,
    twap_hgb: HgbArtifact,
) -> Optional[list[float]]:
    """Build the direct TWAP model row in its manifest-pinned column order."""
    return build_feature_vector(
        side="Up" if side == "YES" else "Down",
        coin=coin,
        interval=sleeve.interval,
        interval_min=sleeve.interval_min,
        entry_minute=entry_minute,
        # The direct WFO derives hour/day from the decision row's entry_ts.
        # Reuse the shared builder with that timestamp, not the market open.
        window_start_ts=feature_ts,
        # The direct model does not request RF-derived columns. A finite dummy
        # satisfies the shared builder without entering the returned vector.
        model_p_up=0.5,
        ask=ask,
        other_ask=other_ask,
        partial_ret_z_1h=z_up,
        feature_columns=twap_hgb.columns_for(sleeve.interval),
    )


def score_side_twap(
    *,
    coin: str,
    sleeve: Sleeve,
    offset_sec: int,
    side: str,
    live_spot: float,
    open_twap: float,
    held_ask: float,
    other_ask: float,
    window_start_ts: int,
    partial_ret_engine: PartialRetEngine,
    twap_hgb: Optional[HgbArtifact],
) -> Optional[float]:
    """Re-score one already-held side with the direct TWAP model."""
    if twap_hgb is None:
        return None
    if not all(
        isinstance(value, (int, float)) and math.isfinite(float(value))
        for value in (held_ask, other_ask, open_twap)
    ):
        return None
    if not (0.0 < float(held_ask) < 1.0 and float(open_twap) > 0.0):
        return None
    entry_minute = offset_sec / 60.0
    try:
        basis_center_log, basis_residual_clip_log = twap_feature_params(
            twap_hgb.manifest,
            coin=coin,
            interval=sleeve.interval,
        )
    except ValueError:
        return None
    z_up = partial_ret_engine.current_z_from_open(
        coin,
        interval_min=sleeve.interval_min,
        live_spot=live_spot,
        open_price=float(open_twap),
        entry_minute=entry_minute,
        basis_center_log=basis_center_log,
        basis_residual_clip_log=basis_residual_clip_log,
    )
    if z_up is None:
        return None
    features = _twap_feature_vector(
        side=side,
        coin=coin,
        sleeve=sleeve,
        entry_minute=entry_minute,
        feature_ts=window_start_ts + offset_sec,
        ask=float(held_ask),
        other_ask=float(other_ask),
        z_up=z_up,
        twap_hgb=twap_hgb,
    )
    if features is None:
        return None
    return float(
        predict_model_prob(
            twap_hgb,
            interval=sleeve.interval,
            feature_row_values=features,
        )
    )


def _ask_in_bucket(ask: float, ranges: tuple[tuple[float, float], ...]) -> bool:
    """True when `ask` falls in any (lo, hi) range using lo < ask <= hi.

    Mirrors the frozen research evaluator's ask-band filter
    (`pool_study.build_bets`: `ask > lo & ask <= hi`) so the src decision path
    is bet-for-bet identical: the low bound is EXCLUDED, the high bound INCLUDED
    (e.g. band 0.65-0.85 rejects ask==0.65 and accepts ask==0.85). An empty
    `ranges` tuple means no bucket constraint, so every ask is in-bucket.
    """
    if not ranges:
        return True
    for lo, hi in ranges:
        if lo < ask <= hi:
            return True
    return False


def score_cell_twap(
    *,
    coin: str,
    sleeve: Sleeve,
    window_start_ts: int,
    live_spot: float,
    open_twap: Optional[float],
    yes_ask: float,
    no_ask: float,
    partial_ret_engine: PartialRetEngine,
    offset_sec: Optional[int],
    twap_hgb: Optional[HgbArtifact],
    fee_rate_bps: Optional[float] = None,
) -> ScoreResult:
    """Score the best executable trend side using a Chainlink TWAP anchor."""
    missing = offset_sec is None or twap_hgb is None
    if missing:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
            skip_reason="twap_model_missing",
        )
    chainlink_windows = twap_hgb.manifest.get("chainlink_window_seconds") or {}
    if chainlink_windows.get(sleeve.interval) != 60:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
            skip_reason="twap_window_contract_mismatch",
        )
    if not (
        isinstance(open_twap, (int, float))
        and math.isfinite(float(open_twap))
        and float(open_twap) > 0.0
    ):
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
            skip_reason="twap_open_missing",
        )

    entry_minute = offset_sec / 60.0
    try:
        basis_center_log, basis_residual_clip_log = twap_feature_params(
            twap_hgb.manifest,
            coin=coin,
            interval=sleeve.interval,
        )
    except ValueError:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
            skip_reason="twap_feature_contract_missing",
        )
    try:
        shared_ask_ranges, _shared_threshold = twap_execution_params(
            twap_hgb.manifest,
            interval=sleeve.interval,
        )
    except ValueError:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
            skip_reason="twap_execution_contract_missing",
        )
    z_up = partial_ret_engine.current_z_from_open(
        coin,
        interval_min=sleeve.interval_min,
        live_spot=live_spot,
        open_price=float(open_twap),
        entry_minute=entry_minute,
        basis_center_log=basis_center_log,
        basis_residual_clip_log=basis_residual_clip_log,
    )
    if z_up is None:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
            skip_reason="no_feature",
        )

    candidates: list[tuple[float, str, float]] = []
    for side, ask, other_ask in (
        ("YES", yes_ask, no_ask),
        ("NO", no_ask, yes_ask),
    ):
        if sleeve.side_filter != "both":
            allowed = "up" if side == "YES" else "down"
            if sleeve.side_filter != allowed:
                continue
        if not (isinstance(ask, (int, float)) and math.isfinite(float(ask))):
            continue
        ask_f = float(ask)
        if not (0.0 < ask_f < 1.0) or not _ask_in_bucket(
            ask_f, shared_ask_ranges
        ):
            continue
        features = _twap_feature_vector(
            side=side,
            coin=coin,
            sleeve=sleeve,
            entry_minute=entry_minute,
            feature_ts=window_start_ts + offset_sec,
            ask=ask_f,
            other_ask=(
                float(other_ask)
                if isinstance(other_ask, (int, float))
                else float("nan")
            ),
            z_up=z_up,
            twap_hgb=twap_hgb,
        )
        if features is None:
            continue
        model_prob = predict_model_prob(
            twap_hgb,
            interval=sleeve.interval,
            feature_row_values=features,
        )
        candidates.append(
            (
                _fee_adjusted_edge(
                    model_prob, ask_f, fee_rate_bps=fee_rate_bps
                ),
                side,
                model_prob,
            )
        )

    if not candidates:
        asks_finite = any(
            isinstance(ask, (int, float)) and math.isfinite(float(ask))
            for ask in (yes_ask, no_ask)
        )
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"), lut_p_bucket=-1,
            lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=z_up,
            skip_reason="ask_bucket" if asks_finite else None,
        )

    best_edge, best_side, best_prob = max(
        candidates, key=lambda candidate: (candidate[0], candidate[2])
    )
    return ScoreResult(
        model_p=best_prob,
        side=best_side,
        edge=best_edge,
        realized_edge=best_edge - sleeve.slippage_haircut,
        lut_p_bucket=-1,
        lut_hit_rate=None,
        lut_n_cum=0,
        partial_ret_z_1h=z_up,
        skip_reason=None,
    )


def score_cell_hgb(
    *,
    coin: str,
    sleeve: Sleeve,
    entry_minute: int,
    window_start_ts: int,
    live_spot: float,
    yes_ask: float,
    no_ask: float,
    partial_ret_engine: PartialRetEngine,
    offset_sec: Optional[int] = None,
    checkpoint_rf: Optional[RfArtifact] = None,
    checkpoint_hgb: Optional[HgbArtifact] = None,
    fee_rate_bps: Optional[float] = None,
) -> ScoreResult:
    """Score one (coin, sleeve, checkpoint-second) cell for a checkpoint sleeve.

    THE scorer — since the 2026-07-31 exp044 retirement there is no other. The
    sleeve's edge is the model's fee-adjusted ``pred_edge`` on the best of the
    two (YES/NO) executable sides, after the ask-bucket filter is applied to
    BOTH sides (matching the backtest's per-side filtering before best-side
    dedup). ``slippage_haircut`` is 0.0 on the live sleeves, so ``realized_edge``
    equals ``edge``; the ``threshold`` gate (``pred_edge >= threshold``) does
    the qualification downstream in the gate stack.

    Pre-book contract: when neither ask is finite this returns a
    ``skip_reason=None`` sentinel (carrying ``model_p=rf_proba`` and the z) so
    the pipeline knows the signal path exists and fetches the book.

    ``fee_rate_bps`` is the resolved market's per-market predict.fun
    ``feeRateBps``, threaded to :func:`_fee_adjusted_edge`. IGNORED on
    Polymarket (whose fee is the theta parabola), and ``None`` on the cheap
    pre-score — where no market is resolved yet — so the predict.fun tent falls
    back to ``config.PREDICTFUN_DEFAULT_FEE_RATE_BPS``. The AUTHORITATIVE
    rescore in ``_resolve_net_cells`` always carries the real value.
    """
    # 2026-07-05 parity: a sub-minute checkpoint cell scores against the
    # per-checkpoint-second checkpoint_v1 models with a FRACTIONAL entry_minute
    # (offset_sec/60), reproducing training.checkpoint's basis instead of the
    # ``offset//60`` integer-minute quantization the parity audit found.
    #
    # 2026-07-31 exp044 retirement: this is now the ONLY path. The baseline
    # (minute-entry) arm scored against the frozen rf_v1/hgb_v1 artifacts, which
    # no longer load — a cell without ``offset_sec`` (or without the checkpoint
    # bundle) fails closed here instead of silently reaching for them.
    if offset_sec is None or checkpoint_rf is None or checkpoint_hgb is None:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"),
            lut_p_bucket=-1, lut_hit_rate=None, lut_n_cum=0,
            partial_ret_z_1h=None, skip_reason="checkpoint_models_missing",
        )
    rf = checkpoint_rf
    hgb = checkpoint_hgb
    em_value: float = offset_sec / 60.0
    rf_key: int = offset_sec

    z = partial_ret_engine.current_z(
        coin,
        interval_min=sleeve.interval_min,
        live_spot=live_spot,
        entry_minute=em_value,
    )
    if z is None:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"),
            lut_p_bucket=-1, lut_hit_rate=None, lut_n_cum=0,
            partial_ret_z_1h=None, skip_reason="no_feature",
        )

    # ORB features (unsigned window-anchored log-returns). entry_partial_ret is
    # the SAME numerator current_z used above, so it inherits the proven
    # z-parity; range_partial_ret is None until this window's first minute has
    # closed (the ORB gate then fails closed). Both stay computed once here and
    # ride on the successful ScoreResult only — the skip/pre-book sentinels
    # below never reach the ORB gate.
    orb_entry_ret = partial_ret_engine.entry_partial_ret(
        coin, interval_min=sleeve.interval_min, live_spot=live_spot,
    )
    orb_range_ret = partial_ret_engine.range_partial_ret(
        coin, interval_min=sleeve.interval_min,
        # Sub-minute checkpoint cells use the backtest's 5s opening range
        # (step5/step1last range_sec=5). The retired baseline arm used 60s.
        range_sec=5,
    )

    rf_proba = predict_proba(
        rf,
        coin=coin,
        interval=sleeve.interval,
        entry_minute=rf_key,
        partial_ret_z_1h=z,
    )
    if rf_proba is None:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"),
            lut_p_bucket=-1, lut_hit_rate=None, lut_n_cum=0,
            partial_ret_z_1h=z, skip_reason="rf_slice_missing",
        )

    feature_columns = hgb.columns_for(sleeve.interval)

    # (res_side, ask, other_ask, feat_side) — feat_side is the build_feature_vector
    # vocabulary (Up/Down); res_side is the ScoreResult vocabulary (YES/NO).
    side_specs = (
        ("YES", yes_ask, no_ask, "Up"),
        ("NO", no_ask, yes_ask, "Down"),
    )
    # audit w19oxqf24 (P2): mirror the backtest's pre-serve side_filter drop
    # (training.checkpoint._score_rows_with_model, which builds ONLY the
    # permitted side's frame before concat — re-pointed 2026-08-01 from
    # training.residual_matrix, deleted in phase 6; checkpoint is the surviving
    # implementation of the same drop).
    # When a sleeve restricts to up|down, the forbidden side's row is dropped
    # BEFORE the model serves, so max(candidates) can never pick it. Without
    # this, live always built both Up/Down candidates and would bet the
    # opposite higher-edge side the backtest forbade — a parity break.
    # The feat_side vocabulary (Up/Down) matches the backtest's lower-cased
    # ``side`` column; side_filter is loader-validated to {both,up,down}.
    side_filter = sleeve.side_filter
    candidates: list[tuple[float, str, float]] = []  # (pred_edge, res_side, model_prob)
    for res_side, ask, other_ask, feat_side in side_specs:
        if side_filter != "both" and feat_side.lower() != side_filter:
            continue
        if not (isinstance(ask, (int, float)) and math.isfinite(float(ask))):
            continue
        a = float(ask)
        if not (0.0 < a < 1.0):
            continue
        # Ask-bucket filter on BOTH sides (matches backtest pre-dedup filter).
        if not _ask_in_bucket(a, sleeve.ask_bucket_ranges):
            continue
        feat = build_feature_vector(
            side=feat_side,
            coin=coin,
            interval=sleeve.interval,
            interval_min=sleeve.interval_min,
            entry_minute=em_value,
            window_start_ts=window_start_ts,
            model_p_up=rf_proba,
            ask=a,
            other_ask=other_ask,
            partial_ret_z_1h=z,
            feature_columns=feature_columns,
        )
        if feat is None:
            continue
        model_prob = predict_model_prob(
            hgb, interval=sleeve.interval, feature_row_values=feat
        )
        pred_edge = _fee_adjusted_edge(model_prob, a, fee_rate_bps=fee_rate_bps)
        candidates.append((pred_edge, res_side, model_prob))

    if not candidates:
        yes_finite = isinstance(yes_ask, (int, float)) and math.isfinite(float(yes_ask))
        no_finite = isinstance(no_ask, (int, float)) and math.isfinite(float(no_ask))
        if not yes_finite and not no_finite:
            # PRE-BOOK sentinel: signal path exists, fetch the book next.
            return ScoreResult(
                model_p=rf_proba, side="YES", edge=float("nan"),
                realized_edge=float("nan"),
                lut_p_bucket=-1, lut_hit_rate=None, lut_n_cum=0,
                partial_ret_z_1h=z, skip_reason=None,
            )
        # Asks were finite but none qualified the bucket / valid-price filter.
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"),
            lut_p_bucket=-1, lut_hit_rate=None, lut_n_cum=0,
            partial_ret_z_1h=z, skip_reason="ask_bucket",
        )

    # Best-side selection MUST match the backtest's _dedupe_best_side
    # (polymarket.backtest.training.modeling): sort_values(["pred_edge",
    # "model_prob"], ascending=[False, False]).drop_duplicates(keep="first").
    # i.e. on an exact pred_edge tie the higher model_prob wins. Keying on
    # pred_edge alone would deterministically keep the first-built side (YES,
    # per side_specs order) and could pick the opposite side from the backtest.
    # The tie is float-unreachable on continuous live asks, so this is
    # parity-hardening — not a live bugfix — but it removes the only remaining
    # selection-key divergence and lets the parity test guard it exactly.
    best_edge, best_side, best_prob = max(candidates, key=lambda c: (c[0], c[2]))
    realized_edge = best_edge - sleeve.slippage_haircut
    # SUBMINUTE_FLIP_SIDE (operator order 2026-08-07): selection above ran
    # untouched on the model's side — same firing seconds, same gates — and
    # only the RETURNED side is inverted, so every downstream consumer
    # (book routing, walker, ledger.side, grading) follows the flip natively.
    # model_p/edge stay the MODEL side's numbers on purpose: the threshold and
    # edge gates must pass exactly when they pass today. See config.py for the
    # provenance trap this creates in signals/ledger.
    if config.SUBMINUTE_FLIP_SIDE:
        best_side = "NO" if best_side == "YES" else "YES"
    return ScoreResult(
        model_p=best_prob,
        side=best_side,
        edge=best_edge,
        realized_edge=realized_edge,
        lut_p_bucket=-1,
        lut_hit_rate=None,
        lut_n_cum=0,
        partial_ret_z_1h=z,
        skip_reason=None,
        orb_range_ret=orb_range_ret,
        orb_entry_ret=orb_entry_ret,
    )
