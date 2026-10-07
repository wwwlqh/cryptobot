"""Resolution sweep: settles PENDING rows whose market window has ended.

Runs every 5 minutes. Each run:

  1. selects ledger rows with outcome='PENDING' and window_end_ts <= now;
  2. fetches each market's final outcome from Polymarket (Gamma);
  3. computes pnl_net from the actual fill or VWAP, charging the calibrated
     entry fee (execution.grade_fees) on wins and losses alike;
  4. updates outcome, won_up, resolution_price, resolution_source,
     resolution_ts and pnl_net.

Failure handling:

  - Gamma reports the market open, missing, malformed or unreachable: the
    row stays PENDING for the next run (no guessing from a spot price);
  - the legacy oracle resolver returns None: the row stays PENDING;
  - the resolver raises ResolutionError (out_of_bounds or
    excessive_deviation): the row is CANCELLED with pnl_net=0 and a CRITICAL
    pipeline_health row is written.

Idempotent: only PENDING rows are selected, so running twice settles a row
once.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Callable, Optional

from cryptobot import config
from cryptobot.venues.types import (
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
    PolymarketResolution,
)
from cryptobot.execution.resolution import (
    PriceFetcher,
    ResolutionError,
    fetch_resolution_price,
)

logger = logging.getLogger(__name__)

PolymarketResolutionFetcher = Callable[[str, int, int], Optional[PolymarketResolution]]


def _graded_cash_pnl(
    side: str,
    fill_price: float,
    won_up: bool,
    stake_usd: float,
) -> float:
    """Settlement dollar P&L with the calibrated entry-leg fee on WIN and LOSS.

    See ``cryptobot.execution.grade_fees`` for the fee derivation.
    """
    from cryptobot.execution.grade_fees import graded_cash_pnl

    return graded_cash_pnl(side, fill_price, won_up, stake_usd, slippage_haircut=0.0)


def _implied_live_fill_price(
    row: sqlite3.Row,
    *,
    live_size: float,
    cap_price: float,
) -> Optional[float]:
    """Infer the average fill for a cash-sized FAK buy that got price
    improvement.
    """
    if live_size <= 0:
        return None
    try:
        bet_size = float(row["bet_size"] or 0.0)
        cap = float(cap_price)
        implied = bet_size / live_size
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if not (math.isfinite(implied) and 0.0 < implied <= 1.0):
        return None
    if not (math.isfinite(cap) and cap > 0.0):
        return None
    tolerance = max(0.0005, 0.002 * max(abs(cap), 1.0))
    if implied <= cap + tolerance:
        return implied
    return None


def _decide_won_up(coin: str, strike: float, resolution_price: float) -> bool:
    """For the 5m/15m up-or-down markets, YES wins when the resolution price is
    strictly above the strike.

    ``strike`` must be the spot at window open, the anchor the model trains on
    and Polymarket settles against. Callers pass the row's
    ``window_open_spot``; only old rows without it fall back to
    ``strike_price`` (the spot at decision time, which biases outcomes).

    For rows older still, callers may use ``yes_pre1_anchor`` only when it is
    clearly a spot price (far outside [0, 1]); values inside that range are
    probabilities and must not be compared with ``resolution_price``.
    """
    return resolution_price > strike


def _log_pipeline(conn: sqlite3.Connection, *, stage: str, status: str, message: str, coin: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO pipeline_health (stage, status, coin, message) "
        "VALUES (?, ?, ?, ?)",
        (stage, status, coin, message),
    )


def _live_pending_skip_health(now_ts: float, window_end_ts: int) -> tuple[str, float]:
    """Return ``(health_status, age_s)`` for a live PENDING row the sweep has
    to skip because it cannot settle yet (no reconciled fill or no usable
    price).

    Past ``MAX_PENDING_SETTLE_AGE_S`` such a row keeps its stake inside the
    group exposure cap and shows as a phantom open position, so it is escalated
    to CRITICAL (which alerts) instead of logging WARN forever. Never
    auto-cancelled: a real on-chain fill may just be missing its price.
    """
    age_s = float(now_ts) - float(window_end_ts)
    health = "CRITICAL" if age_s > config.MAX_PENDING_SETTLE_AGE_S else "WARN"
    return health, age_s


def run_resolution_sweep(
    conn: sqlite3.Connection,
    *,
    now_ts: float | None = None,
    polymarket_resolution_fetch: PolymarketResolutionFetcher | None = None,
    chainlink_fetch: PriceFetcher | None = None,
    reference_spot_fetch: PriceFetcher | None = None,
    slippage_haircut_by_sleeve: Optional[dict[str, float]] = None,
) -> dict:
    """Run one sweep. Returns counters for observability."""
    import time as _time

    if now_ts is None:
        now_ts = _time.time()

    # now_ts is fixed for the whole sweep, so format resolution_ts once.
    resolution_ts = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    rows = conn.execute(
        "SELECT id, coin, sleeve, side, fill_price, walked_vwap, p_market, "
        "       yes_pre1_anchor, strike_price, window_open_spot, bet_size, "
        "       interval_min, window_start_ts, window_end_ts, "
        "       mode, live_filled_size, live_filled_price, "
        "       venue, fee_rate_bps "
        "FROM ledger "
        "WHERE outcome = 'PENDING' AND window_end_ts <= ? "
        "ORDER BY window_end_ts ASC, id ASC",
        (int(now_ts),),
    ).fetchall()

    settled = 0
    cancelled = 0
    skipped = 0

    for row in rows:
        coin = row["coin"]
        win_end = int(row["window_end_ts"])
        result_price: float
        result_source: str
        polymarket_resolution_price: float | None = None
        polymarket_resolution_source: str | None = None

        if polymarket_resolution_fetch is not None:
            interval_min = int(row["interval_min"])
            window_start_ts = int(row["window_start_ts"])
            try:
                poly_result = polymarket_resolution_fetch(
                    coin, interval_min, window_start_ts
                )
            except Exception as exc:
                _log_pipeline(
                    conn,
                    stage="resolve",
                    status="WARN",
                    coin=coin,
                    message=(
                        f"polymarket resolution fetch failed for "
                        f"row id={row['id']}: {exc}"
                    ),
                )
                skipped += 1
                continue

            if (
                poly_result is not None
                and poly_result.status == RESOLUTION_RESOLVED
                and poly_result.won_up is not None
            ):
                won_up = bool(poly_result.won_up)
                result_source = poly_result.source
                result_price = (
                    float(poly_result.up_price)
                    if poly_result.up_price is not None
                    else float(poly_result.won_up)
                )
                polymarket_resolution_price = result_price
                polymarket_resolution_source = poly_result.source
            else:
                status = poly_result.status if poly_result is not None else "none"
                age_s = float(now_ts) - float(win_end)
                stale_unresolvable = (
                    status != RESOLUTION_OPEN
                    and age_s > config.MAX_PENDING_SETTLE_AGE_S
                )
                if stale_unresolvable and str(row["mode"]).upper() == "LIVE":
                    # A live PENDING row Polymarket can no longer resolve
                    # (permanent 404, HTTP error, bad payload) holds its stake
                    # in the group exposure cap with nothing to release it.
                    # Escalate to CRITICAL so an operator settles or cancels
                    # it. Not auto-cancelled: the market may have resolved and
                    # only the fetch is failing.
                    health = "CRITICAL"
                    message = (
                        f"row id={row['id']} LIVE PENDING unresolvable "
                        f"(status={status}) for {age_s / 3600:.1f}h "
                        f"(> {config.MAX_PENDING_SETTLE_AGE_S / 3600:.1f}h) — "
                        f"bet_size stranded in the group cap; manual "
                        f"resolve/cancel required"
                    )
                else:
                    health = "OK" if status == RESOLUTION_OPEN else "WARN"
                    message = (
                        f"row id={row['id']} polymarket resolution "
                        f"status={status}; leaving PENDING"
                    )
                _log_pipeline(
                    conn,
                    stage="resolve",
                    status=health,
                    coin=coin,
                    message=message,
                )
                skipped += 1
                continue
        else:
            # Legacy and test path: resolve from an oracle spot price and
            # compare it to the recorded window-open strike.
            reference_spot = None
            if reference_spot_fetch is not None:
                reference_spot = reference_spot_fetch(coin, win_end)

            try:
                result = fetch_resolution_price(
                    coin,
                    win_end,
                    reference_spot=reference_spot,
                    chainlink_fetch=chainlink_fetch,
                )
            except ResolutionError as exc:
                _log_pipeline(
                    conn,
                    stage="resolve",
                    status="CRITICAL",
                    coin=coin,
                    message=f"resolution rejected ({exc.code}): {exc}",
                )
                conn.execute(
                    "UPDATE ledger SET outcome='CANCELLED', pnl_net=0, "
                    "resolution_ts=? WHERE id = ?",
                    (resolution_ts, row["id"]),
                )
                cancelled += 1
                continue

            if result is None:
                # Upstream down: the row stays PENDING for the next run.
                skipped += 1
                continue

            # Strike: prefer window_open_spot, the anchor the model trains on
            # and Polymarket settles against. Old rows without it fall back to
            # strike_price so they can still settle.
            window_open_raw = row["window_open_spot"]
            strike_raw = row["strike_price"]
            if window_open_raw is not None:
                strike = float(window_open_raw)
            elif strike_raw is not None:
                strike = float(strike_raw)
            else:
                legacy_anchor = row["yes_pre1_anchor"]
                if legacy_anchor is not None and (
                    float(legacy_anchor) < 0.0 or float(legacy_anchor) > 1.0
                ):
                    strike = float(legacy_anchor)
                else:
                    _log_pipeline(
                        conn,
                        stage="resolve",
                        status="WARN",
                        coin=coin,
                        message=(
                            f"row id={row['id']} has no window_open_spot or "
                            "strike_price and yes_pre1_anchor is in probability "
                            "range - skipping"
                        ),
                    )
                    skipped += 1
                    continue
            won_up = _decide_won_up(coin, strike, result.price)
            if result.price == strike:
                # Exact tie on the legacy oracle path. _decide_won_up uses
                # strict ``>``, so a tie books YES=LOSS / NO=WIN with no
                # refund, while the venue might settle a flat close
                # differently. Log it so the rare case can be checked by hand.
                # The Polymarket-resolved branch above is unaffected (it uses
                # poly_result.won_up).
                _log_pipeline(
                    conn,
                    stage="resolve",
                    status="WARN",
                    coin=coin,
                    message=(
                        f"row id={row['id']} resolution_price == strike "
                        f"({result.price}) — exact tie booked as won_up=False "
                        f"(no PUSH path); verify against venue settlement"
                    ),
                )
            result_price = result.price
            result_source = result.source
        side = row["side"]
        # Use the multi-level VWAP actually paid rather than the top-of-book
        # ask at decision time; many bets walk more than one level, and using
        # top_ask would understate losses. ``fill_price`` itself is unchanged
        # (the backtest-parity test feeds its own values); only pnl_net uses
        # walked_vwap.
        fill_price_topask = float(row["fill_price"])
        walked = row["walked_vwap"]
        effective_fill = (
            float(walked) if walked is not None else fill_price_topask
        )
        # Log every NULL walked_vwap so the fallback's frequency is visible in
        # pipeline_health. Rows written before the column existed are expected
        # to be NULL; a new row with NULL means the order path skipped the VWAP
        # and is worth investigating. The row alone cannot tell which case it
        # is, so always warn.
        if walked is None:
            _log_pipeline(
                conn,
                stage="resolve",
                status="WARN",
                coin=coin,
                message=(
                    f"row id={row['id']} has walked_vwap=NULL; "
                    f"using fill_price={fill_price_topask}"
                ),
            )
        bet_size = float(row["bet_size"] or 0.0)
        # For a live row, bet_size is the intended stake and walked_vwap is the
        # walker's estimate. The order can fill partially or at another price,
        # and the reconciler (persistence/reconciliation.py) records the real
        # values in live_filled_size / live_filled_price. Settle from those:
        # stake = live_filled_size * live_filled_price, price =
        # live_filled_price.
        row_mode = (row["mode"] or "DRY").upper()
        live_fill_price_for_write: float | None = None
        if row_mode == "LIVE":
            live_size = row["live_filled_size"]
            live_price = row["live_filled_price"]
            live_size_f = float(live_size) if live_size is not None else 0.0
            live_price_f = float(live_price) if live_price is not None else 0.0
            if live_size_f <= 0.0:
                # No reconciled fill yet. Do not settle against the
                # placeholder; leave the row PENDING so order reconciliation
                # can record the real fill first. reconcile_open_orders only
                # cancels a row when the CLOB reports the order
                # CANCELLED/CANCELED/EXPIRED with zero fill. Live entries are
                # FAK, so the CLOB resolves them quickly; a row still stuck
                # here after MAX_PENDING_SETTLE_AGE_S (order unreachable, or
                # reconciler stuck) escalates to CRITICAL. Never
                # auto-cancelled: a real fill may just be missing its price.
                health, age_s = _live_pending_skip_health(now_ts, win_end)
                message = (
                    f"LIVE row id={row['id']} has no reconciled fill "
                    f"(live_filled_size={live_size}, "
                    f"live_filled_price={live_price}); leaving PENDING for "
                    f"the order-reconciliation job before settling"
                )
                if health == "CRITICAL":
                    message += (
                        f" — unreconciled for {age_s / 3600:.1f}h "
                        f"(> {config.MAX_PENDING_SETTLE_AGE_S / 3600:.1f}h); "
                        f"bet_size stranded in the group cap, manual resolve "
                        f"required"
                    )
                _log_pipeline(
                    conn,
                    stage="resolve",
                    status=health,
                    coin=coin,
                    message=message,
                )
                skipped += 1
                continue
            if live_price_f <= 0.0:
                # The reconciler records live_filled_price on partial fills,
                # but older rows may have a size and no price. Fall back to the
                # price actually paid that is already on the row: walked_vwap,
                # else fill_price. Inferring price from bet_size /
                # live_filled_size is wrong for a partial fill (bet_size is the
                # intended stake, so the ratio exceeds 1.0 and is rejected), so
                # that is only a last resort when neither recorded price is
                # usable.
                recorded_price: float | None = None
                if walked is not None and float(walked) > 0.0:
                    recorded_price = float(walked)
                elif fill_price_topask > 0.0:
                    recorded_price = fill_price_topask

                if recorded_price is not None:
                    live_price_f = recorded_price
                    live_fill_price_for_write = recorded_price
                    _log_pipeline(
                        conn,
                        stage="resolve",
                        status="WARN",
                        coin=coin,
                        message=(
                            f"LIVE row id={row['id']} missing live_filled_price; "
                            f"settling from recorded taker price "
                            f"{recorded_price:.10f} "
                            f"({'walked_vwap' if walked is not None else 'fill_price'})"
                        ),
                    )
                else:
                    implied_price = _implied_live_fill_price(
                        row, live_size=live_size_f, cap_price=effective_fill
                    )
                    if implied_price is None:
                        # No reconciled price and no recorded price to fall
                        # back on: escalate a row stuck past
                        # MAX_PENDING_SETTLE_AGE_S to CRITICAL instead of
                        # skipping forever. No auto-cancel: a real fill may
                        # just be missing its price.
                        health, age_s = _live_pending_skip_health(now_ts, win_end)
                        message = (
                            f"LIVE row id={row['id']} has reconciled size but "
                            f"no usable fill price "
                            f"(live_filled_size={live_size}, "
                            f"live_filled_price={live_price}, "
                            f"walked_vwap={walked}, "
                            f"fill_price={fill_price_topask}); leaving PENDING "
                            f"for the order-reconciliation job before settling"
                        )
                        if health == "CRITICAL":
                            message += (
                                f" — stuck for {age_s / 3600:.1f}h "
                                f"(> {config.MAX_PENDING_SETTLE_AGE_S / 3600:.1f}h); "
                                f"bet_size stranded in the group cap, manual "
                                f"resolve required"
                            )
                        _log_pipeline(
                            conn,
                            stage="resolve",
                            status=health,
                            coin=coin,
                            message=message,
                        )
                        skipped += 1
                        continue
                    live_price_f = implied_price
                    live_fill_price_for_write = implied_price
                    _log_pipeline(
                        conn,
                        stage="resolve",
                        status="WARN",
                        coin=coin,
                        message=(
                            f"LIVE row id={row['id']} missing live_filled_price; "
                            f"using implied cash-sized FAK fill price "
                            f"{implied_price:.10f}"
                        ),
                    )
            effective_fill = live_price_f
            # USD actually spent = shares * price per share.
            bet_size = live_size_f * live_price_f
        # PnL is cash-sized: bet_size is filled USD while the payoff formula is
        # per share, so convert through effective_fill. A losing $10 bet then
        # loses $10 plus its entry fee. The fill/VWAP is already the real price
        # paid, so the strategy's slippage_haircut is not applied again.
        pnl_net = _graded_cash_pnl(side, effective_fill, won_up, bet_size)
        outcome = "WIN" if pnl_net > 0 else "LOSS"

        conn.execute(
            "UPDATE ledger SET outcome = ?, won_up = ?, resolution_price = ?, "
            "resolution_source = ?, resolution_ts = ?, pnl_net = ?, "
            "live_filled_price = COALESCE(live_filled_price, ?), "
            "polymarket_resolution_price = ?, polymarket_resolution_source = ? "
            "WHERE id = ?",
            (
                outcome,
                int(won_up),
                result_price,
                result_source,
                resolution_ts,
                pnl_net,
                live_fill_price_for_write,
                polymarket_resolution_price,
                polymarket_resolution_source,
                row["id"],
            ),
        )
        settled += 1

    # Reject re-quote telemetry: fill in resolved_outcome (UP/DOWN) for
    # rejected FAK rows whose market has now settled, using the same Gamma
    # fetcher as settlement (closed=true finds expired 5m markets). Observation
    # only; a failure here must not abort the sweep.
    reject_requotes_resolved = 0
    if polymarket_resolution_fetch is not None:
        try:
            from cryptobot.execution.reject_requote import (
                backfill_reject_requote_outcomes,
            )

            reject_requotes_resolved = backfill_reject_requote_outcomes(
                conn,
                polymarket_resolution_fetch=polymarket_resolution_fetch,
                now_ts=now_ts,
            )
        except Exception:  # noqa: BLE001 — telemetry backfill is best-effort
            logger.exception("reject_requote outcome backfill raised")

    # Fill in the market outcome on stop-loss rows, for display. A stop-loss
    # exit was booked as outcome='CANCELLED' with its realized pnl_net, so the
    # settlement loop above never sees it and its won_up / resolution price
    # stay NULL. This pass sets only won_up and resolution price/source;
    # pnl_net, outcome, exit_* and resolution_ts are never touched (see
    # backfill_stop_loss_resolutions). Bounded per sweep, idempotent (won_up IS
    # NULL), best-effort.
    stop_resolutions_backfilled = 0
    if polymarket_resolution_fetch is not None:
        try:
            stop_resolutions_backfilled = backfill_stop_loss_resolutions(
                conn,
                polymarket_resolution_fetch=polymarket_resolution_fetch,
                now_ts=now_ts,
            )
        except Exception:  # noqa: BLE001 — display-only backfill is best-effort
            logger.exception("stop-loss resolution backfill raised")

    # The signals table gets one row per non-qualifying evaluation, about a
    # million rows a day at 1 Hz. Prune rows older than SIGNALS_RETENTION_DAYS
    # on every sweep so observational rows never trip the disk gate.
    pruned = prune_signals(conn, retention_days=config.SIGNALS_RETENTION_DAYS, now_ts=now_ts)
    # Bound pipeline_health too; the per-tick latency traces grow it quickly.
    pruned_health = prune_pipeline_health(
        conn, retention_days=config.PIPELINE_HEALTH_RETENTION_DAYS, now_ts=now_ts
    )
    # Per-offset scan latches (~130k rows/day with sub-minute sleeves on); only
    # the current window's latches are ever read.
    pruned_latches = prune_subminute_scan_latches(
        conn, retention_days=3, now_ts=now_ts
    )

    conn.commit()
    return {
        "settled": settled,
        "cancelled": cancelled,
        "skipped": skipped,
        "reject_requotes_resolved": reject_requotes_resolved,
        "stop_resolutions_backfilled": stop_resolutions_backfilled,
        "pruned_signals": pruned,
        "pruned_pipeline_health": pruned_health,
        "pruned_subminute_latches": pruned_latches,
    }


def backfill_stop_loss_resolutions(
    conn: sqlite3.Connection,
    *,
    polymarket_resolution_fetch: PolymarketResolutionFetcher,
    now_ts: float,
    max_rows: int = 128,
) -> int:
    """Write the market outcome onto stop-loss ledger rows, for display only.

    A stop-loss exit updated the position row to ``outcome='CANCELLED'`` with
    its realized ``pnl_net``, and the settlement sweep skips non-PENDING rows,
    so the market's actual result was never recorded: ``won_up`` and the
    resolution price stay NULL and the dashboard cannot show the "if held"
    result.

    This pass fetches the resolution for expired stop-loss rows and writes only
    ``won_up``, ``resolution_price`` / ``resolution_source`` and the
    ``polymarket_resolution_*`` fields. It never touches ``pnl_net``,
    ``outcome`` or the ``exit_*`` columns.

    ``resolution_ts`` is left NULL. capital.py orders stop-loss rows by their
    ``exit_ts`` when replaying equity for the drawdown peak
    (``_equity_change_sort_key``), so the peak would not change either way, but
    leaving it NULL keeps backfilled and untouched rows identical. The
    dashboard shows stop rows from ``exit_kind`` and ``won_up`` and does not
    need resolution_ts.

    Only rows with ``exit_kind='stop_loss'`` are touched, so voided CANCELLED
    rows (``exit_kind IS NULL``) are left alone. At most ``max_rows`` per
    sweep; idempotent (``won_up IS NULL`` on both the SELECT and the UPDATE);
    best-effort (a failed fetch leaves the row for a later sweep). The caller
    commits.
    """
    try:
        rows = conn.execute(
            "SELECT id, coin, interval_min, window_start_ts "
            "FROM ledger "
            "WHERE outcome = 'CANCELLED' AND exit_kind = 'stop_loss' "
            "  AND won_up IS NULL AND window_end_ts <= ? "
            "ORDER BY id ASC LIMIT ?",
            (int(now_ts), int(max_rows)),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    updated = 0
    for row in rows:
        interval_min = row["interval_min"]
        window_start_ts = row["window_start_ts"]
        if interval_min is None or window_start_ts is None:
            continue
        try:
            poly_result = polymarket_resolution_fetch(
                row["coin"], int(interval_min), int(window_start_ts)
            )
        except Exception:  # noqa: BLE001 — leave NULL, retry next sweep
            logger.debug(
                "stop-loss resolution backfill fetch failed for row %s",
                row["id"], exc_info=True,
            )
            continue
        if (
            poly_result is None
            or poly_result.status != RESOLUTION_RESOLVED
            or poly_result.won_up is None
        ):
            continue
        won_up = bool(poly_result.won_up)
        result_price = (
            float(poly_result.up_price)
            if poly_result.up_price is not None
            else float(won_up)
        )
        # The UPDATE repeats the exit_kind and won_up IS NULL conditions so it
        # cannot overwrite a row changed in the meantime, and sets no pnl_net,
        # outcome, exit_* or resolution_ts column.
        conn.execute(
            "UPDATE ledger SET won_up = ?, resolution_price = ?, "
            "resolution_source = ?, "
            "polymarket_resolution_price = ?, polymarket_resolution_source = ? "
            "WHERE id = ? AND outcome = 'CANCELLED' "
            "  AND exit_kind = 'stop_loss' AND won_up IS NULL",
            (
                int(won_up),
                result_price,
                poly_result.source,
                result_price,
                poly_result.source,
                row["id"],
            ),
        )
        updated += 1
    return updated


def prune_signals(
    conn: sqlite3.Connection,
    *,
    retention_days: int,
    now_ts: float | None = None,
) -> int:
    """Delete ``signals`` rows older than ``retention_days``; returns the
    count.

    Called by the sweep. Safe to call on its own; runs in its own transaction
    so a later sweep failure does not roll back the delete.
    """
    import time as _time
    if retention_days <= 0:
        return 0
    if now_ts is None:
        now_ts = _time.time()
    cutoff = datetime.fromtimestamp(now_ts, tz=timezone.utc) \
        .replace(microsecond=0)
    cur = conn.execute(
        "DELETE FROM signals WHERE created_at < datetime(?, ?)",
        (
            cutoff.strftime("%Y-%m-%d %H:%M:%S"),
            f"-{int(retention_days)} days",
        ),
    )
    return int(cur.rowcount or 0)


def prune_pipeline_health(
    conn: sqlite3.Connection,
    *,
    retention_days: int,
    now_ts: float | None = None,  # noqa: ARG001 — kept for call-site symmetry
) -> int:
    """Delete ``pipeline_health`` rows older than ``retention_days`` (<= 0
    disables).

    Unlike ``prune_signals`` the cutoff uses the database's own clock
    (``datetime('now')``), because rows are stamped by that column default. A
    synthetic ``now_ts`` (tests and backfills use future times) would otherwise
    delete rows the same sweep just wrote.

    Boot rows (the config-hash log used to attribute ledger rows) and CRITICAL
    rows (drawdown halts, breaker trips) are kept; retention is for the
    high-volume rows (latency traces, WARN noise).
    """
    if retention_days <= 0:
        return 0
    cur = conn.execute(
        "DELETE FROM pipeline_health WHERE created_at < datetime('now', ?) "
        "AND stage != 'boot' AND status != 'CRITICAL'",
        (f"-{int(retention_days)} days",),
    )
    return int(cur.rowcount or 0)


def prune_subminute_scan_latches(
    conn: sqlite3.Connection,
    *,
    retention_days: int,
    now_ts: float | None = None,
) -> int:
    """Delete ``subminute_scan_latches`` rows for long-settled windows.

    With sub-minute sleeves on, claim_subminute_scan writes ~130k latch rows a
    day. A latch is only read for its own window, so cutting off on
    ``window_start_ts`` (epoch seconds, caller clock) is exact and safe.
    ``subminute_fills`` is not pruned; the fill-rate breaker and the dashboard
    read it.
    """
    import time as _time
    if retention_days <= 0:
        return 0
    if now_ts is None:
        now_ts = _time.time()
    cutoff_ts = int(now_ts) - int(retention_days) * 86400
    try:
        cur = conn.execute(
            "DELETE FROM subminute_scan_latches WHERE window_start_ts < ?",
            (cutoff_ts,),
        )
        return int(cur.rowcount or 0)
    except sqlite3.OperationalError:
        # Old database without the table.
        return 0
