"""F3 resolution sweep — settles PENDING rows whose window has expired.

Runs every 5 minutes (Cron `0,5,10,...,55`); each invocation:
  1. SELECT id, coin, sleeve, side, fill_price, window_start_ts, window_end_ts
     FROM ledger WHERE outcome='PENDING' AND window_end_ts <= now
  2. For each row, fetch Polymarket/Gamma final outcome when wired
  3. Compute pnl_net using cash-sized P&L from the actual ledger fill/VWAP,
     charging the calibrated entry-leg fee dispatched on the ROW's venue
     column (execution.grade_fees — 2026-07-23 calibrated fee grading; fees
     are booked on WIN and LOSS alike, never config.VENUE-dispatched, so a
     mixed-history DB grades every row under the venue that charged it)
  4. UPDATE outcome/won_up/resolution_price/resolution_source/resolution_ts/pnl_net

Failure modes:
  - Gamma says open/not found/bad payload/http error -> row stays PENDING for
    the next sweep cycle rather than guessing from an oracle spot price
  - legacy oracle resolver returns None (Chainlink down) -> row
    stays PENDING for next sweep cycle
  - resolver raises ResolutionError(out_of_bounds / excessive_deviation) ->
    row marked CANCELLED with pnl_net=0; CRITICAL log row written to
    pipeline_health (security defense per U7a)

Idempotency: the sweep predicates on outcome='PENDING', so running it
twice on the same row produces one settlement, not two.
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


def _unit_pnl(side: str, fill_price: float, won_up: bool, slippage_haircut: float = 0.0) -> float:
    """Per-share P&L for one Polymarket binary share — D2 parity.

    Thin wrapper over ``cryptobot.strategy.parity.unit_pnl``. The U16
    parity test asserts this runtime copy agrees with ``backtest.sweep_lib``.
    """
    from cryptobot.strategy.parity import unit_pnl as _shared_unit_pnl

    return _shared_unit_pnl(side, fill_price, won_up, slippage_haircut=slippage_haircut)


def _cash_pnl(
    side: str,
    fill_price: float,
    won_up: bool,
    stake_usd: float,
    slippage_haircut: float = 0.0,
) -> float:
    """Uncalibrated dollar P&L — the U16 backtest-parity surface ONLY.

    Since the 2026-07-23 calibrated fee grading, settlement books through
    ``_graded_cash_pnl`` below; this wrapper stays so the parity test
    (test_backtest_parity) can keep pinning the frozen
    ``strategy.parity`` <-> ``backtest.sweep_lib`` chain byte-for-byte.
    """
    from cryptobot.strategy.parity import cash_pnl as _shared_cash_pnl

    return _shared_cash_pnl(
        side, fill_price, won_up, stake_usd, slippage_haircut=slippage_haircut
    )


def _graded_cash_pnl(
    side: str,
    fill_price: float,
    won_up: bool,
    stake_usd: float,
    *,
    venue: str | None,
    fee_rate_bps: float | None,
) -> float:
    """Settlement dollar P&L — calibrated entry-leg fee, ROW-venue dispatched.

    2026-07-23 calibrated fee grading: the venue charges its taker fee on
    every fill, so WIN and LOSS both book the entry-leg fee — polymarket rows
    pay ``POLY_FEE_CALIBRATION * theta * p * (1-p)`` per share (flat-to-flat
    audit: k*theta = 0.06048 measured, re-based to k=0.864 at theta=0.07),
    predictfun
    rows pay the exact tent off the row's own ``fee_rate_bps``. Formula and
    measured derivations live in ``cryptobot.execution.grade_fees``.
    """
    from cryptobot.execution.grade_fees import graded_cash_pnl

    return graded_cash_pnl(
        side,
        fill_price,
        won_up,
        stake_usd,
        venue=venue,
        fee_rate_bps=fee_rate_bps,
        slippage_haircut=0.0,
    )


def _implied_live_fill_price(
    row: sqlite3.Row,
    *,
    live_size: float,
    cap_price: float,
) -> Optional[float]:
    """Infer average fill for cash-sized FAK buys with price improvement."""
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
    """For the 5m/15m up-or-down crypto markets, YES wins when the
    resolution price strictly exceeds the strike (window-open spot).

    ``strike`` MUST be the live spot AT WINDOW-OPEN — the same anchor
    the model trains against and Polymarket Chainlink settles on. Callers
    are expected to pass ``window_open_spot`` from the ledger row; only
    legacy rows (pre-divergence-#13-fix) fall back to ``strike_price``,
    which captured spot at decision time and produced systematically
    biased outcomes (the bug this comparator now closes).

    For even-older rows that pre-date both columns, callers MAY fall
    back to ``yes_pre1_anchor`` only when the value is clearly a spot
    (far outside ``[0, 1]``) — values inside that band are Polymarket
    probabilities and unsafe to compare against ``resolution_price``.
    """
    return resolution_price > strike


def _log_pipeline(conn: sqlite3.Connection, *, stage: str, status: str, message: str, coin: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO pipeline_health (stage, status, coin, message) "
        "VALUES (?, ?, ?, ?)",
        (stage, status, coin, message),
    )


def _live_pending_skip_health(now_ts: float, window_end_ts: int) -> tuple[str, float]:
    """Return ``(health_status, age_s)`` for a LIVE PENDING row the sweep must
    skip because it cannot yet settle (no reconciled fill / no usable price).

    Mirrors the unresolvable-market escalation (Audit P1 #12): past
    ``MAX_PENDING_SETTLE_AGE_S`` a stuck LIVE row holds its bet_size in the
    group exposure cap with NO auto relief and shows as a phantom open
    position, so escalate to CRITICAL (surfaced by Telegram/systemd alerting)
    rather than re-logging WARN and skipping forever. We never auto-cancel: a
    real on-chain fill may just be missing its price.
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
    """Run one F3 sweep cycle. Returns counters for observability."""
    import time as _time

    if now_ts is None:
        now_ts = _time.time()

    # now_ts is fixed for the whole sweep — format the SQL resolution_ts once.
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
                    # Audit P1 #12: a LIVE PENDING row Polymarket can no longer
                    # resolve (permanent 404 / http_error / bad_payload) holds
                    # its bet_size in the group exposure cap indefinitely with
                    # no auto relief. Escalate to CRITICAL so an operator
                    # resolves/cancels it. We do NOT auto-cancel: the market
                    # may actually have resolved and only the fetch is failing,
                    # so an auto-cancel could discard a real WIN/LOSS.
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
            # Legacy/test path: resolve from an oracle spot price, then compare
            # to the recorded window-open strike.
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
                # Upstream down - row stays PENDING for next cycle.
                skipped += 1
                continue

            # Resolve strike. Divergence #13 fix (2026-05-21) elevates
            # window_open_spot to the primary anchor: same anchor the model
            # trains against and Polymarket settles on. For legacy rows pre-#13
            # we fall back to strike_price (decision-time spot) so old PENDING
            # rows can still settle without manual repair.
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
                # Exact strike tie on the legacy oracle-spot fallback path.
                # _decide_won_up uses strict `>` so a tie books YES=LOSS /
                # NO=WIN with NO PUSH/refund path here. Real prints rarely tie,
                # but the venue's on-chain rule may settle a flat as DOWN/refund,
                # so a tie can mis-settle vs Polymarket. Log it so the rare event
                # is observable for manual reconciliation. The Polymarket-resolved
                # branch above is unaffected (it trusts poly_result.won_up).
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
        # Divergence #6 (2026-05-18 audit): for honest realized PnL,
        # prefer the multi-level VWAP we actually paid over the top_ask
        # snapshot at decision time. ~46% of live bets walked the book
        # by an average ~52 bps, undercounting losses on the dashboard.
        # `fill_price` is retained (column unchanged, used by the
        # backtest-parity test that feeds its own fill values) — only
        # the realized `pnl_net` becomes walked_vwap-aware.
        fill_price_topask = float(row["fill_price"])
        walked = row["walked_vwap"]
        effective_fill = (
            float(walked) if walked is not None else fill_price_topask
        )
        # Audit hook: log every NULL walked_vwap so operators can scan
        # pipeline_health for the frequency of the fallback path. Two
        # populations land here:
        #   * legacy rows recorded before the `walked_vwap` column existed
        #     (these stay NULL forever and are expected)
        #   * rows created after the schema migration that ended up with
        #     NULL because the take-side code wrote the top-ask only (this
        #     SHOULD be investigated; the live ledger is supposed to write
        #     the walked VWAP on every fresh tick post-#6).
        # We can't distinguish the two from the row alone without a
        # creation-timestamp lookup, so emit the WARN unconditionally and
        # let operators correlate against the schema-migration ts.
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
        # Audit w19oxqf24 finding #7 (P1): for a LIVE row, bet_size is the
        # INTENDED stake and walked_vwap is the DRY walker's ESTIMATED VWAP. The
        # GTC limit order can fill partially or at a different VWAP, and the
        # reconciler (persistence/reconciliation.py) writes the realized values
        # into live_filled_size / live_filled_price. Settle a LIVE row from the
        # ACTUAL fill — stake = live_filled_size * live_filled_price, fill price
        # = live_filled_price — not the requested stake/estimated price.
        row_mode = (row["mode"] or "DRY").upper()
        live_fill_price_for_write: float | None = None
        if row_mode == "LIVE":
            live_size = row["live_filled_size"]
            live_price = row["live_filled_price"]
            live_size_f = float(live_size) if live_size is not None else 0.0
            live_price_f = float(live_price) if live_price is not None else 0.0
            if live_size_f <= 0.0:
                # Not yet reconciled to a real fill. Refuse to settle against
                # the optimistic placeholder — leave PENDING so the
                # order-reconciliation job (D-LIVE-17) can land the actual
                # fill first. reconcile_open_orders flips to outcome='CANCELLED'
                # only when the CLOB reports the order CANCELLED/CANCELED/EXPIRED
                # with zero fill. A live entry that stays OPEN forever, or whose
                # status fetch is unreachable, would NOT auto-cancel and would
                # re-skip here every cycle. In the current config this is bounded
                # because live entries are FAK (the CLOB resolves them promptly
                # to filled-or-cancelled). FIX 3 (2026-07-09): a row that DOES
                # strand here past MAX_PENDING_SETTLE_AGE_S (unreachable order or
                # a wedged reconciler) now escalates to CRITICAL like the
                # unresolvable-market path above, instead of re-logging WARN and
                # skipping forever with its bet_size stuck in the group cap. We
                # do NOT auto-cancel: a real on-chain fill may just be un-priced.
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
                # FIX 2 (2026-07-09): the reconciler now persists
                # live_filled_price on partial fills, but rows already stranded
                # with size-but-no-price must still heal. Fall back to the taker
                # price ACTUALLY PAID, already recorded on the row: prefer
                # walked_vwap (multi-level VWAP), else fill_price (top-ask).
                # Both are real prices paid. The legacy bet_size/live_filled_size
                # inference is WRONG for a PARTIAL fill — bet_size is the
                # INTENDED stake, not cash spent, so the ratio exceeds 1.0, is
                # rejected, and the row skips forever (LIVE row 571). Keep the
                # implied path only as a last resort when BOTH recorded prices
                # are unusable.
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
                        # FIX 3: no reconciled price AND no recorded price to fall
                        # back on — escalate a genuinely stuck LIVE row past
                        # MAX_PENDING_SETTLE_AGE_S to CRITICAL (bet_size stranded
                        # in the group cap) instead of skipping forever. No
                        # auto-cancel: a real fill may just be un-priced.
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
            # Realized USD actually spent = shares * price-per-share.
            bet_size = live_size_f * live_price_f
        # Per-bet P&L is cash-sized: bet_size is filled USD, while the
        # venue payoff formula is per share. Convert cash to shares
        # through effective_fill so a losing $10 bet loses $10 plus its
        # entry-leg fee, not fill_price * $10. The ledger fill/VWAP is
        # already the actual price paid, so do not apply the strategy
        # slippage_haircut again here. The fee is dispatched on the ROW's
        # venue + fee_rate_bps (2026-07-23 calibrated fee grading) so DRY /
        # paper rows grade byte-identically to LIVE ones on the same venue.
        pnl_net = _graded_cash_pnl(
            side,
            effective_fill,
            won_up,
            bet_size,
            venue=row["venue"],
            fee_rate_bps=row["fee_rate_bps"],
        )
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

    # Reject re-quote telemetry: back-fill resolved_outcome (UP/DOWN) for
    # rejected-FAK rows whose market has now settled. Uses the SAME Gamma
    # resolution fetcher the ledger settlement uses (closed=true finds expired
    # 5m markets Gamma otherwise hides). Observation only — never places an
    # order; a failure here must not abort the ledger sweep.
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

    # Display-only backfill of the market outcome onto stop-closed rows. A
    # stop-loss fill books the position outcome='CANCELLED' with a realized
    # (round-trip) pnl_net the instant it fires, so the settlement loop above
    # (WHERE outcome='PENDING') never touches it and its won_up / resolution
    # price stay NULL — the market's actual outcome is unknown on the row. This
    # pass fills ONLY the resolution display fields (won_up + resolution
    # price/source); pnl_net, outcome, the exit_* columns, AND resolution_ts are
    # NEVER touched (booked-once is sacred, and stamping resolution_ts would
    # perturb capital.py's order-sensitive DD peak replay — see
    # backfill_stop_loss_resolutions). Uses the SAME Gamma fetcher as ledger
    # settlement; bounded per sweep, idempotent (won_up IS NULL), best-effort.
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

    # Disk hygiene: signals table grows ~12 rows/tick (one per non-qualifying
    # coin × sleeve evaluation). At 1Hz this is ~1M rows/day; prune anything
    # older than SIGNALS_RETENTION_DAYS on every sweep cycle so the disk gate
    # never fires from observational rows alone.
    pruned = prune_signals(conn, retention_days=config.SIGNALS_RETENTION_DAYS, now_ts=now_ts)
    # A6 hygiene: pipeline_health previously had NO retention; the A0
    # stage='latency' trace rows make bounding it worthwhile.
    pruned_health = prune_pipeline_health(
        conn, retention_days=config.PIPELINE_HEALTH_RETENTION_DAYS, now_ts=now_ts
    )
    # B1 hygiene: per-offset scan latches (~130k rows/day when the
    # sub-minute flag is on) — only ever consulted for the current window.
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
    """Backfill the market outcome onto stop-closed ledger rows — display only.

    A successful stop-loss sell UPDATEs the SINGLE position row to
    ``outcome='CANCELLED'`` with a realized round-trip ``pnl_net`` (booked
    exactly once), and the F3 settlement sweep skips it because it predicates on
    ``outcome='PENDING'``. As a result the market's actual resolution is never
    recorded on the row: ``won_up`` and the resolution price stay NULL, so the
    dashboard cannot show the "if held" counterfactual.

    This pass fetches that resolution for EXPIRED stop rows and writes ONLY the
    resolution display fields — ``won_up`` + ``resolution_price`` /
    ``resolution_source`` + the ``polymarket_resolution_*`` mirror. It NEVER
    touches ``pnl_net``, ``outcome``, or the ``exit_*`` columns; the booked-once
    loss and its sacred invariants are untouched.

    ``resolution_ts`` is left NULL here (defense in depth). capital.py replays
    each wallet's equity curve in equity-change order to find the PEAK that gates
    the LIVE drawdown halt + graduated brake (``get_wallet_snapshots`` /
    ``get_capital_snapshot``); realized capital is order-independent but the peak
    is NOT. A booked-once stop loss already realized its ``pnl_net`` at
    ``exit_ts`` (before market resolution), so a stop row's equity-change moment
    is ``exit_ts``, not ``resolution_ts``. The load-bearing fix is in capital.py:
    its replay sort key (``_equity_change_sort_key``) keys stop rows on the
    immutable ``exit_ts``, so the peak is invariant whether ``resolution_ts`` is
    NULL or already backfilled — defending the gate at the consumer means no
    writer here (or elsewhere) can perturb it. We still avoid stamping
    resolution_ts so the two states stay visibly identical; the dashboard renders
    the stop row off ``exit_kind`` + ``won_up`` and streams it every SSE delta
    (``outcome != 'PENDING'``), so it never needs resolution_ts.

    Discriminates strictly on ``exit_kind='stop_loss'`` so legacy voided
    CANCELLED rows (cutover resets / orphan closes, ``exit_kind IS NULL``) are
    left alone. Bounded to ``max_rows`` per sweep, idempotent (the ``won_up IS
    NULL`` predicate excludes already-backfilled rows on both the SELECT and the
    guarded UPDATE), and best-effort (a per-row fetch failure leaves the row for
    a later sweep). The caller commits."""
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
        # Guarded UPDATE re-asserts the discriminator + idempotency predicate so
        # a concurrent stop-book or a prior backfill can never be clobbered, and
        # NO pnl_net/outcome/exit_*/resolution_ts column is in the SET list
        # (resolution_ts stays NULL — defense in depth; capital.py already keys
        # the DD peak replay on exit_ts for stop rows, see the docstring).
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
    """Delete `signals` rows older than `retention_days`. Returns row count.

    Called by the F3 sweep cycle. Safe to invoke standalone; runs in its own
    transaction so a partial sweep failure doesn't roll back the deletion.
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
    """Delete `pipeline_health` rows older than `retention_days` (A6).

    Called by the F3 sweep cycle; retention_days<=0 disables.

    Unlike `prune_signals`, the cutoff uses the DB's OWN clock
    (``datetime('now')``), not the caller's ``now_ts``: pipeline_health rows
    are stamped by the column default ``datetime('now')``, so a synthetic
    ``now_ts`` (tests/backfills sweep with future timestamps) would compute a
    future cutoff and delete rows the very same sweep just wrote. Retention
    is a wall-clock policy — keep both sides on the same clock.

    Scope (review finding): boot rows (config-sha generation log used for
    post-hoc ledger attribution) and CRITICAL rows (dd_halt / breaker-trip
    forensics) are EXEMPT — the retention exists for the high-volume
    observational rows (stage='latency' traces, WARN spam), not the audit
    trail that previously persisted forever.
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
    """Delete `subminute_scan_latches` rows for long-settled windows.

    Review finding: with the sub-minute flag on, claim_subminute_scan
    inserts ~130k latch rows/day and nothing deleted them — the exact
    disk-creep class the A6 work fixes elsewhere. A latch is only ever
    consulted for its own (current) window, so keying the cutoff on
    ``window_start_ts`` (epoch seconds, caller clock) is exact and safe.
    NOTE: ``subminute_fills`` is deliberately NOT pruned — ramp_state
    enforces the rung-1 cumulative cap via a lifetime SUM over it.
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
        # Legacy DB that pre-dates the table and never booted the new code.
        return 0
