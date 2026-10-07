"""B4/B5: sub-minute fill-rate circuit breaker + staged sizing ramp.

The sub-minute checkpoint add-on (step5+step1last port — see
backtest/optimize_runs/checkpoint_grid_combo_wfo/checkpoint_grid_combo_digest.md)
is deployed WITHOUT a prior shadow probe, so this module is the safety net
that lets reality assert itself cheaply:

  * every LIVE sub-minute order's requested vs realized fill is recorded in
    ``subminute_fills`` (`record_subminute_fill`);
  * the BREAKER (`evaluate_breaker`) trips when the rolling fill rate is
    low (ghost/stale quotes returning unfilled through Polymarket's 250ms
    taker delay) OR realized EV over settled sub-minute bets is clearly
    negative — and a tripped breaker blocks every further sub-minute
    placement while the baseline 60s sleeves keep running untouched;
  * the RAMP (`ramp_state`) keeps stakes at min-bet under a hard cumulative
    dollar cap until enough settled bets prove the breaker green, then
    scales toward the pe0.045/stake0.47 target in rungs.

Everything here is read/derive-from-DB — no in-memory state that a process
restart could clear, and no network. Callers wrap every entry point in
try/except so a bug here can never slow or crash the live tick (the
fail-closed default on ANY internal error is "block sub-minute placement").

Operator controls:
  * env ``CRYPTOBOT_SUBMINUTE_KILL=1`` — hard kill-switch, checked before
    every placement, wins over everything.
  * env ``CRYPTOBOT_SUBMINUTE_BREAKER_RESET=<ISO-8601 UTC>`` — after
    investigating a trip, only orders created AFTER this timestamp feed the
    breaker window (re-arms the ramp without deleting history).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from cryptobot import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fill recording
# ---------------------------------------------------------------------------


def record_subminute_fill(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    offset_sec: int,
    window_start_ts: int,
    side: str,
    requested_usd: float,
    quote_ask: Optional[float],
    limit_price: Optional[float],
    filled_size: Optional[float],
    filled_price: Optional[float],
    filled_usd: Optional[float],
    order_id: Optional[str],
    client_order_id: Optional[str],
    outcome: str,
    attempt: int = 1,
    trigger_src: str = "tick",
) -> None:
    """Persist one LIVE sub-minute order's requested-vs-realized fill.

    ``outcome`` is the placement-level result ('accepted' / 'rejected' /
    'error'); realized fill columns may be NULL when the CLOB response
    carried no fill metadata (the U7 reconciler back-fills the ledger, but
    THIS row keeps the placement-time snapshot the breaker reads). The
    caller commits.

    ``attempt`` is 1 for the first FAK and 2 for the v2 one-shot post-reject
    retry (operator 2026-07-09) — the breaker's fill-rate/EV arms treat both
    the same (each is a real requested-vs-realized order); the column only lets
    telemetry split first-vs-retry fills.

    ``trigger_src`` is the trigger rung that produced this placement (operator
    2026-07-30): 'tick' for the 1Hz boundary-aligned scanner — the default, and
    what every legacy row is — 'fast_sweep' for the sub-second sweep job and
    'fast_event' for a WS book-update push. Purely descriptive: the breaker's
    arms are src-blind (each row is a real requested-vs-realized order), the
    column exists so fast-vs-tick realized c/$ is separable downstream.
    """
    conn.execute(
        """
        INSERT INTO subminute_fills
          (coin, sleeve, offset_sec, window_start_ts, side, requested_usd,
           quote_ask, limit_price, filled_size, filled_price, filled_usd,
           order_id, client_order_id, outcome, attempt, trigger_src)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            coin,
            sleeve,
            int(offset_sec),
            int(window_start_ts),
            side,
            float(requested_usd),
            quote_ask,
            limit_price,
            filled_size,
            filled_price,
            filled_usd,
            order_id,
            client_order_id,
            outcome,
            int(attempt),
            str(trigger_src or "tick"),
        ),
    )


# ---------------------------------------------------------------------------
# Breaker
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BreakerState:
    tripped: bool
    reason: Optional[str]           # 'fill_rate' | 'realized_ev' | 'error' | None
    n_orders: int                   # orders in the fill-rate window
    fill_rate: Optional[float]      # Σfilled/Σrequested over the window
    n_settled: int                  # settled sub-minute ledger bets considered
    settled_pnl: Optional[float]    # Σ pnl_net over those
    settled_stake: Optional[float]  # Σ bet_size over those


def _reset_clause() -> tuple[str, tuple]:
    """WHERE-fragment applying the operator breaker-reset horizon.

    Review finding (major): the docs instruct the T-separated ISO form
    ("2026-07-03T00:00:00") but rows are stored space-separated
    ("2026-07-03 00:00:00"); a raw lexicographic compare would wrongly
    exclude every same-day row from the breaker window, EV arm, ramp
    counting, AND the rung-1 hard cap ('T' sorts after ' '). Normalize in
    Python via datetime.fromisoformat → space form so both documented
    spellings are equivalent. An UNPARSEABLE reset is ignored (full
    history) — that is the fail-SAFE direction: the breaker/cap then see
    every historical order rather than none (an SQL-side datetime(?)
    normalization would instead yield `created_at > NULL` ≡ empty
    history, silently disarming the breaker).
    """
    reset = config.SUBMINUTE_BREAKER_RESET
    if not reset:
        return " ", ()
    try:
        normalized = datetime.fromisoformat(reset).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error(
            "CRYPTOBOT_SUBMINUTE_BREAKER_RESET %r is not ISO-8601 — "
            "IGNORING it (breaker/ramp use FULL history, the safe side)",
            reset,
        )
        return " ", ()
    return " AND created_at > ? ", (normalized,)


def evaluate_breaker(conn: sqlite3.Connection) -> BreakerState:
    """Compute the breaker from the DB. Fail-closed: any internal error
    returns tripped=True/reason='error' — callers then block placement."""
    try:
        clause, params = _reset_clause()
        rows = conn.execute(
            "SELECT requested_usd, filled_usd FROM subminute_fills "
            "WHERE outcome != 'error'" + clause +
            "ORDER BY id DESC LIMIT ?",
            (*params, int(config.SUBMINUTE_BREAKER_WINDOW)),
        ).fetchall()
        n_orders = len(rows)
        fill_rate: Optional[float] = None
        if n_orders:
            req = sum(float(r["requested_usd"] or 0.0) for r in rows)
            fil = sum(float(r["filled_usd"] or 0.0) for r in rows)
            fill_rate = (fil / req) if req > 0 else None

        # Realized-EV arm: settled LIVE sub-minute ledger rows (offset_sec is
        # only ever written by the sub-minute path). mode='LIVE' is REQUIRED:
        # DRY paper bets fill at the observed book by construction and would
        # both mask a ghost-fill reality and advance the ramp on fake fills.
        bet_clause, bet_params = _reset_clause()
        settled = conn.execute(
            "SELECT COALESCE(SUM(pnl_net), 0.0) AS pnl, "
            "       COALESCE(SUM(bet_size), 0.0) AS stake, "
            "       COUNT(*) AS n "
            "FROM ledger WHERE offset_sec IS NOT NULL AND mode = 'LIVE' "
            "AND outcome IN ('WIN', 'LOSS')" + bet_clause.replace(
                "created_at", "bet_ts"
            ),
            bet_params,
        ).fetchone()
        n_settled = int(settled["n"] or 0)
        settled_pnl = float(settled["pnl"] or 0.0)
        settled_stake = float(settled["stake"] or 0.0)

        if (
            n_orders >= int(config.SUBMINUTE_BREAKER_MIN_ORDERS)
            and fill_rate is not None
            and fill_rate < float(config.SUBMINUTE_BREAKER_MIN_FILL_RATE)
        ):
            return BreakerState(
                True, "fill_rate", n_orders, fill_rate,
                n_settled, settled_pnl, settled_stake,
            )
        if (
            n_settled >= int(config.SUBMINUTE_BREAKER_EV_MIN_SETTLED)
            and settled_stake > 0
            and settled_pnl
            < -(float(config.SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC) * settled_stake)
        ):
            return BreakerState(
                True, "realized_ev", n_orders, fill_rate,
                n_settled, settled_pnl, settled_stake,
            )
        return BreakerState(
            False, None, n_orders, fill_rate,
            n_settled, settled_pnl, settled_stake,
        )
    except Exception:  # noqa: BLE001 — fail CLOSED
        logger.exception("subminute breaker evaluation failed — failing closed")
        return BreakerState(True, "error", 0, None, 0, None, None)


# ---------------------------------------------------------------------------
# Staged sizing ramp
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RampState:
    rung: int                    # 1 (min-bet) / 2 (50% target) / 3 (full)
    stake_multiplier: float      # applied to sleeve target (rung 1 overrides)
    min_bet_override: bool       # rung 1: clamp stake to MIN_BET_USD
    cumulative_filled_usd: float # rung-1 hard-cap accounting
    cap_blocked: bool            # rung-1 cumulative cap reached → block
    n_settled: int


def ramp_state(conn: sqlite3.Connection) -> RampState:
    """Derive the ramp rung from settled sub-minute bets + cumulative fills.

    Fail-closed: any internal error returns a cap_blocked rung-1 state so
    placement is refused rather than mis-sized.

    When ``config.SUBMINUTE_RAMP_ENABLED`` is off (the default — see that flag
    for the rationale), the staged ramp is skipped: this returns a full-size
    rung-3 state from the first bet and the 35% drawdown halt is the sole
    sizing control.
    """
    if not config.SUBMINUTE_RAMP_ENABLED:
        return RampState(
            rung=3,
            stake_multiplier=1.0,
            min_bet_override=False,
            cumulative_filled_usd=0.0,
            cap_blocked=False,
            n_settled=0,
        )
    try:
        clause, params = _reset_clause()
        # mode='LIVE' is REQUIRED: the ramp exists to prove REAL fills are
        # possible before scaling real money — DRY paper bets (which "fill"
        # at the observed book by construction) must never advance a rung.
        settled_row = conn.execute(
            "SELECT COUNT(*) AS n FROM ledger "
            "WHERE offset_sec IS NOT NULL AND mode = 'LIVE' "
            "AND outcome IN ('WIN', 'LOSS')"
            + clause.replace("created_at", "bet_ts"),
            params,
        ).fetchone()
        n_settled = int(settled_row["n"] or 0)

        green = int(config.SUBMINUTE_RAMP_GREEN_BETS)
        if n_settled >= 2 * green:
            return RampState(3, 1.0, False, 0.0, False, n_settled)
        if n_settled >= green:
            return RampState(2, 0.5, False, 0.0, False, n_settled)

        # Rung 1: min-bet stakes under the hard cumulative dollar cap. The
        # cap counts REALIZED fill value (plan: SUM(filled_usd)) — unfilled
        # ghost FAKs cost nothing and are the breaker's business, not the
        # cap's. A failed query fails closed via the except below.
        fclause, fparams = _reset_clause()
        cum_row = conn.execute(
            "SELECT COALESCE(SUM(filled_usd), 0.0) AS s FROM subminute_fills "
            "WHERE 1=1" + fclause,
            fparams,
        ).fetchone()
        cumulative = float(cum_row["s"] or 0.0)
        cap_blocked = cumulative >= float(config.SUBMINUTE_RAMP_CAP_USD)
        return RampState(1, 1.0, True, cumulative, cap_blocked, n_settled)
    except Exception:  # noqa: BLE001 — fail CLOSED
        logger.exception("subminute ramp evaluation failed — failing closed")
        return RampState(1, 1.0, True, 0.0, True, 0)
