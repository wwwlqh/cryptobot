"""Fill-rate circuit breaker for sub-minute orders.

  * Every live sub-minute order's requested and realized fill is recorded in
    ``subminute_fills`` (``record_subminute_fill``).
  * The breaker (``evaluate_breaker``) trips when the rolling fill rate is
    low (stale quotes coming back unfilled after Polymarket's 250ms taker
    delay) or realized EV on settled sub-minute bets is clearly negative.
    A tripped breaker blocks further sub-minute orders.

Everything is derived from the database (no in-memory state a restart could
lose, and no network). Callers wrap every entry point in try/except so a bug
here cannot slow or crash the tick; any internal error blocks sub-minute orders
(fail closed).

Operator controls:

  * ``CRYPTOBOT_SUBMINUTE_KILL=1``: hard kill switch, checked before every
    order, overrides everything.
  * ``CRYPTOBOT_SUBMINUTE_BREAKER_RESET=<ISO-8601 UTC>``: after investigating
    a trip, only orders created after this time count toward the breaker
    (history is kept).
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
    """Store one live sub-minute order's requested and realized fill.

    ``outcome`` is the placement result ('accepted', 'rejected', 'error').
    Realized fill columns may be NULL when the CLOB response had no fill data;
    the reconciler later fixes the ledger row, but this row keeps the
    placement-time view the breaker reads. The caller commits.

    ``attempt`` is 1 for the first FAK and 2+ for retries after a reject; the
    breaker treats them alike, and the column lets telemetry separate them.

    ``trigger_src`` says what triggered the order: 'tick' (the 1 Hz scanner,
    also the default for old rows), 'fast_sweep' (the sub-second sweep job) or
    'fast_event' (a WS book update). Descriptive only; the breaker ignores it.
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
    """WHERE fragment applying the breaker-reset time.

    Rows are stored as "2026-07-03 00:00:00" but the reset may be given in ISO
    form ("2026-07-03T00:00:00"); a raw string compare would drop every row
    from that day ('T' sorts after ' '). So the reset is parsed with
    datetime.fromisoformat and rewritten in the space form. An unparseable
    reset is ignored, so the breaker sees the full history; that is the safe
    direction (normalizing in SQL would instead give ``created_at > NULL``,
    which matches nothing and disarms the breaker).
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

        # Realized-EV check: settled live sub-minute ledger rows (only the
        # sub-minute path writes offset_sec). mode='LIVE' is required: DRY bets
        # "fill" at the observed book by construction and would hide
        # stale-quote misses.
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
