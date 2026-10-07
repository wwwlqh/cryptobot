"""Score one cell (coin, sleeve, second): model probability, side and edge.

``score_cell_twap``:

  1. compute the TWAP z-score (PartialRetEngine.current_z_from_open) at the
     fractional minute ``offset_sec / 60``;
  2. build the feature row for each tradable side and get the interval
     model's probability;
  3. compute the fee-adjusted edge from that side's ask;
  4. pick the higher-edge side, as the backtest's _dedupe_best_side does;
  5. subtract ``sleeve.slippage_haircut`` from the chosen edge.

``ScoreResult``'s ``lut_*`` fields are always -1/None/0; they remain because
``signals`` and ``ledger`` still have those columns.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from cryptobot import config
from cryptobot.strategy.hgb_features import build_feature_vector
from cryptobot.strategy.hgb_loader import HgbArtifact, predict_model_prob
from cryptobot.strategy.partial_ret import PartialRetEngine
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
    # Unused placeholders (-1 / None / 0), kept because ``signals`` and
    # ``ledger`` have these columns.
    lut_p_bucket: int
    lut_hit_rate: Optional[float]
    lut_n_cum: int
    partial_ret_z_1h: Optional[float]
    skip_reason: Optional[str]   # populated when score() returns a SKIP-row reason


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

