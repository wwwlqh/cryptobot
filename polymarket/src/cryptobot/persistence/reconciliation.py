"""Per-order CLOB fill ↔ ledger row reconciliation + stale-attempt recovery.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U7:

  * `reconcile_open_orders(conn, order_client)` — for each LIVE PENDING
    ledger row with a `live_order_id`, query the CLOB and update
    `live_filled_size` / `live_filled_price` / `live_fill_ts`.
    Per D-LIVE-17, when the CLOB reports CANCELLED / EXPIRED with
    `live_filled_size=0`, the row transitions to `outcome='CANCELLED'`
    with `pnl_net=0` so the group exposure cap releases immediately on
    the reconciler tick.

  * `reconcile_stale_attempts(conn, order_client, *, stale_seconds)` —
    scan `live_order_attempts` rows in SUBMITTING/SUBMITTED with
    `ledger_id IS NULL` older than `stale_seconds`. For each:
        - Query the CLOB by `client_order_id` (when supported) or by
          (funder, token, side, price, size, time-window). On match,
          create/link the missing LIVE ledger row.
        - On no match after `stale_seconds`, mark the attempt FAILED with
          `error_kind='not_found_after_timeout'` and release the
          reservation.

Both helpers are idempotent: re-running on the same DB is a no-op for
rows already in their terminal state.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from cryptobot.execution.ledger import BetSignal, record_bet
from cryptobot.execution.order_attempts import (
    LiveOrderAttempt,
    get_attempt_signal_json,
    link_attempt_to_ledger,
    mark_attempt_failed,
    stale_active_attempts,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Order ↔ ledger reconciliation (D-LIVE-17)
# ---------------------------------------------------------------------------


def reconcile_open_orders(
    conn: sqlite3.Connection,
    order_client: object,
    *,
    order_fetcher: Optional[Callable[[str], Optional[dict[str, Any]]]] = None,
) -> dict[str, int]:
    """For each LIVE PENDING row with a `live_order_id`, query the CLOB and
    update the fill columns.

    `order_fetcher(order_id) -> dict | None` is the injection point for
    tests; the production caller passes a closure that wraps
    `order_client._client.get_order(order_id)`.

    Returns counts: `{'reconciled': N, 'cancelled': K, 'still_pending': M,
    'unreachable': U}`.
    """
    counts = {"reconciled": 0, "cancelled": 0, "still_pending": 0, "unreachable": 0}
    rows = conn.execute(
        """
        SELECT id, live_order_id, bet_size, fill_price, walked_vwap
          FROM ledger
         WHERE outcome = 'PENDING'
           AND mode = 'LIVE'
           AND live_order_id IS NOT NULL
        """,
    ).fetchall()

    fetcher = order_fetcher or _make_default_order_fetcher(order_client)

    for row in rows:
        ledger_id = int(row["id"])
        order_id = str(row["live_order_id"])
        payload = fetcher(order_id)
        if payload is None:
            counts["unreachable"] += 1
            continue

        intent_price = (
            row["walked_vwap"]
            if row["walked_vwap"] is not None
            else row["fill_price"]
        )
        status, fill_size, fill_price, fill_ts = _read_clob_order_payload(
            payload,
            intent_amount_usd=_coerce_float(row["bet_size"]),
            intent_price=_coerce_float(intent_price),
        )

        if status in {"CANCELLED", "CANCELED", "EXPIRED"} and (fill_size or 0) <= 0:
            # Unfilled and cancelled: outcome=CANCELLED, pnl_net=0.
            conn.execute(
                """
                UPDATE ledger
                   SET outcome='CANCELLED', pnl_net=0,
                       live_filled_size=COALESCE(?, 0),
                       live_fill_ts=COALESCE(?, datetime('now')),
                       resolution_ts=COALESCE(resolution_ts, datetime('now'))
                 WHERE id=?
                """,
                (fill_size, fill_ts, ledger_id),
            )
            counts["cancelled"] += 1
        elif fill_size is not None and fill_size > 0:
            conn.execute(
                """
                UPDATE ledger
                   SET live_filled_size=?,
                       live_filled_price=COALESCE(?, live_filled_price),
                       live_fill_ts=COALESCE(?, live_fill_ts)
                 WHERE id=?
                """,
                (fill_size, fill_price, fill_ts, ledger_id),
            )
            _backfill_subminute_fill(conn, order_id, fill_size, fill_price)
            counts["reconciled"] += 1
        else:
            counts["still_pending"] += 1

    conn.commit()
    return counts


# ---------------------------------------------------------------------------
# Stale-attempt recovery (D-LIVE-28)
# ---------------------------------------------------------------------------


def reconcile_stale_attempts(
    conn: sqlite3.Connection,
    order_client: object,
    *,
    stale_seconds: int = 300,
    funder_orders_fetcher: Optional[
        Callable[[], list[dict[str, Any]]]
    ] = None,
) -> dict[str, int]:
    """Recover or release `live_order_attempts` reservations the bot can no
    longer reason about.

    For each stale SUBMITTING/SUBMITTED row with `ledger_id IS NULL`:

      1. Query the CLOB's recent funder orders for a match by
         (token, side, price, size, time-window). When the CLOB supports
         `client_order_id` echo, prefer that.
      2. On match WITHOUT a ledger row (audit P0 #2): RECONSTRUCT the LIVE
         PENDING ledger row from the attempt's persisted `signal_json` and
         LINK it. A crash between the FAK fill and `record_bet` otherwise
         leaves real on-chain USDC exposure invisible to the exposure cap,
         PnL, and redeem accounting. Double-insert is prevented by a
         `live_client_order_id` dedup query (a crash AFTER record_bet but
         before link LINKs to the existing row). When the signal cannot be
         reconstructed, escalate via a CRITICAL pipeline_health row + notify.
      3. On NO match after `stale_seconds`: mark the attempt FAILED
         with `error_kind='not_found_after_timeout'` so the group cap
         reservation is released.

    Returns `{'recovered': N, 'released': K, 'still_active': M, 'orphaned': O}`,
    where `orphaned` counts matches that could not be auto-reconstructed and
    were escalated for manual resolution.
    """
    counts = {"recovered": 0, "released": 0, "still_active": 0, "orphaned": 0}
    stale = stale_active_attempts(conn, older_than_seconds=stale_seconds)
    if not stale:
        return counts

    fetcher = funder_orders_fetcher or _make_default_funder_orders_fetcher(
        order_client
    )
    try:
        clob_orders = fetcher() or []
    except Exception:  # noqa: BLE001
        logger.warning(
            "reconcile_stale_attempts: funder_orders fetcher raised — leaving "
            "stale rows alone for next tick",
            exc_info=True,
        )
        counts["still_active"] = len(stale)
        return counts

    clob_by_coid: dict[str, dict[str, Any]] = {}
    clob_by_order_id: dict[str, dict[str, Any]] = {}
    for order in clob_orders:
        coid = (
            order.get("client_order_id")
            or order.get("clientOrderID")
            or order.get("clientOrderId")
        )
        if coid:
            clob_by_coid[str(coid)] = order
        order_id = _clob_order_id(order)
        if order_id:
            clob_by_order_id[order_id] = order

    for attempt in stale:
        match = _find_stale_attempt_match(
            attempt,
            clob_orders,
            clob_by_coid=clob_by_coid,
            clob_by_order_id=clob_by_order_id,
        )
        if match is not None:
            # The CLOB has the order but our ledger row is missing (audit
            # P0 #2): a crash between the FAK fill and record_bet. Recover
            # the ledger row instead of leaving a manual-only CRITICAL.
            try:
                recovered_id = _recover_orphan_ledger_row(conn, attempt, match)
            except Exception:  # noqa: BLE001
                logger.critical(
                    "reconcile_stale_attempts: orphan recovery raised for "
                    "attempt id=%s client_order_id=%s",
                    attempt.id,
                    attempt.client_order_id,
                    exc_info=True,
                )
                recovered_id = None
            if recovered_id is not None:
                counts["recovered"] += 1
            else:
                _escalate_orphan(conn, attempt, match)
                counts["orphaned"] += 1
            continue
        # No CLOB match found within the stale window — release the slot.
        try:
            mark_attempt_failed(
                conn,
                attempt.id,
                error_kind="not_found_after_timeout",
                error=f"no CLOB match for client_order_id={attempt.client_order_id} "
                f"after {stale_seconds}s",
            )
            counts["released"] += 1
        except Exception:  # noqa: BLE001
            logger.warning(
                "reconcile_stale_attempts: failed to release attempt id=%s",
                attempt.id,
                exc_info=True,
            )
            counts["still_active"] += 1

    return counts


def _backfill_subminute_fill(
    conn: sqlite3.Connection,
    order_id: str,
    fill_size: Optional[float],
    fill_price: Optional[float],
) -> None:
    """Backfill the realized fill into the matching `subminute_fills` row.

    `record_subminute_fill` writes the PLACEMENT-time snapshot, and a GTC
    limit that fills asynchronously carries no fill metadata in the placement
    response — so `filled_size/price/usd` are NULL there until this reconciler
    lands the true fill on the ledger. The sub-minute fill-rate BREAKER and the
    rung-1 cumulative CAP both read `subminute_fills.filled_usd`; leaving it
    NULL makes them read $0 realized (breaker fill_rate collapses toward 0 and
    would false-trip on "nothing fills" when orders actually filled and lost;
    the $20 cap never accrues). Mirror the ledger fill here, keyed by the CLOB
    order_id (== ledger.live_order_id == subminute_fills.order_id). Idempotent:
    the guard only touches rows not yet backfilled. Best-effort — an
    observability write must never break reconciliation."""
    if fill_size is None or fill_size <= 0 or fill_price is None:
        return
    try:
        conn.execute(
            """
            UPDATE subminute_fills
               SET filled_size=?,
                   filled_price=?,
                   filled_usd=?
             WHERE order_id=?
               AND (filled_usd IS NULL OR filled_usd = 0)
            """,
            (fill_size, fill_price, fill_size * fill_price, order_id),
        )
    except sqlite3.DatabaseError:
        logger.warning(
            "reconcile_open_orders: subminute_fills backfill failed for "
            "order_id=%s",
            order_id,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Orphan ledger-row recovery (audit P0 #2)
# ---------------------------------------------------------------------------


def _recover_orphan_ledger_row(
    conn: sqlite3.Connection,
    attempt: LiveOrderAttempt,
    match: dict[str, Any],
) -> Optional[int]:
    """Reconstruct + link the LIVE PENDING ledger row for an orphaned
    attempt. Returns the ledger id, or None when it cannot be reconstructed
    (the caller then escalates for manual resolution).

    Safe against double-insert: if `record_bet` already wrote a row for this
    `client_order_id` (a crash AFTER record_bet but BEFORE link), LINK to
    that existing row rather than inserting a duplicate.
    """
    existing = conn.execute(
        "SELECT id FROM ledger WHERE live_client_order_id = ? LIMIT 1",
        (attempt.client_order_id,),
    ).fetchone()
    if existing is not None:
        ledger_id = int(existing["id"])
        link_attempt_to_ledger(conn, attempt.id, ledger_id)
        logger.critical(
            "reconcile_stale_attempts: LINKED orphan attempt id=%s to existing "
            "ledger row id=%s (record_bet ran but link did not)",
            attempt.id,
            ledger_id,
        )
        return ledger_id

    signal_json = get_attempt_signal_json(conn, attempt.id)
    if not signal_json:
        return None
    try:
        signal = BetSignal(**json.loads(signal_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        logger.warning(
            "reconcile_stale_attempts: could not reconstruct BetSignal for "
            "attempt id=%s",
            attempt.id,
            exc_info=True,
        )
        return None

    # Use the REAL fill from the CLOB order when present; otherwise leave the
    # fill columns NULL so the resolution sweep defers settlement (its
    # live_size<=0/NULL guard) until reconcile_open_orders lands the true
    # fill — never settle against an optimistic placeholder.
    _status, fill_size, fill_price, fill_ts = _read_clob_order_payload(match)
    live_order_id = (
        attempt.live_order_id
        or str(match.get("id") or match.get("order_id") or "")
        or None
    )
    ledger_id = record_bet(
        conn,
        signal,
        mode="LIVE",
        wallet_group=attempt.wallet_group,
        live_order_id=live_order_id,
        live_client_order_id=attempt.client_order_id,
        live_filled_size=fill_size,
        live_filled_price=fill_price,
        live_fill_ts=fill_ts,
    )
    link_attempt_to_ledger(conn, attempt.id, ledger_id)
    conn.commit()
    logger.critical(
        "reconcile_stale_attempts: RECOVERED orphan CLOB order without ledger "
        "row — reconstructed ledger id=%s from attempt id=%s client_order_id=%s "
        "(filled_size=%s)",
        ledger_id,
        attempt.id,
        attempt.client_order_id,
        fill_size,
    )
    return ledger_id


def _escalate_orphan(
    conn: sqlite3.Connection,
    attempt: LiveOrderAttempt,
    match: dict[str, Any],
) -> None:
    """Last resort when an orphan CLOB order cannot be auto-reconstructed:
    write a CRITICAL pipeline_health row and dispatch a notify so the
    stranded position is visible rather than buried in a log line."""
    clob_order = match.get("id") or match.get("order_id")
    msg = (
        f"orphan CLOB order without ledger row AND unreconstructable signal — "
        f"attempt id={attempt.id} client_order_id={attempt.client_order_id} "
        f"clob_order={clob_order}; manual resolution required"
    )
    logger.critical("reconcile_stale_attempts: %s", msg)
    try:
        conn.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('reconcile', 'CRITICAL', ?)",
            (msg,),
        )
        conn.commit()
    except sqlite3.DatabaseError:
        logger.warning(
            "reconcile_stale_attempts: failed to write CRITICAL health row",
            exc_info=True,
        )
    try:
        from cryptobot.execution.notify import send_event

        send_event("orphan_order_unreconstructable", msg, critical=True)
    except Exception:  # noqa: BLE001 — notify dispatch is best-effort
        logger.warning(
            "reconcile_stale_attempts: notify dispatch for orphan failed",
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# CLOB response parsing helpers
# ---------------------------------------------------------------------------


def _read_clob_order_payload(
    payload: dict[str, Any],
    *,
    intent_amount_usd: Optional[float] = None,
    intent_price: Optional[float] = None,
) -> tuple[str, Optional[float], Optional[float], Optional[str]]:
    """Extract (status, filled_size_tokens, filled_price_vwap, fill_ts).

    Polymarket's V2 CLOB returns variations on the field naming — handle
    the common ones. Float coercion is defensive against string fields.
    """
    status = str(
        payload.get("status")
        or payload.get("state")
        or payload.get("order_status")
        or "UNKNOWN"
    ).upper()
    fill_size = _coerce_float(
        payload.get("size_matched")
        or payload.get("matched_size")
        or payload.get("filled_size")
        or payload.get("filled")
    )
    fill_price = _read_fill_price(
        payload,
        fill_size,
        intent_amount_usd=intent_amount_usd,
        intent_price=intent_price,
    )
    fill_ts = payload.get("matched_at") or payload.get("filled_at")
    if fill_ts is not None:
        fill_ts = str(fill_ts)
    return status, fill_size, fill_price, fill_ts


def _read_fill_price(
    payload: dict[str, Any],
    fill_size: Optional[float],
    *,
    intent_amount_usd: Optional[float] = None,
    intent_price: Optional[float] = None,
) -> Optional[float]:
    """Extract a realized fill price for a matched CLOB order.

    Polymarket's `get_order` can return a FAK market-buy order with
    `size_matched` and `price`, but no `avg_fill_price`. The precise paths are
    tried first, most-accurate first: an explicit VWAP field, then
    `filled_value / size_matched`, then (for a cash-sized buy that spent the
    full stake) `intent_amount_usd / size_matched` when the implied price is at
    or below the executable cap.

    Root-cause fix for LIVE row 571 (2026-07-09): a PARTIAL fill that did NOT
    spend the full cash amount has an implied price above the cap (bet_size is
    the INTENDED stake, not cash spent), so every precise path is rejected. The
    order still has a KNOWN conservative fill price — the order's executable
    limit `price`. A marketable FAK/limit BUY only matches book levels AT OR
    BELOW that limit, so booking it over-states cost (never over-books a win,
    never under-books a loss) while guaranteeing we never persist
    `live_filled_size` WITHOUT a `live_filled_price` again — the exact state
    that stranded the resolution sweep on every 5-min cycle, forever.
    """
    explicit = _first_value(
        payload,
        (
            "avg_fill_price",
            "average_price",
            "averagePrice",
            "matched_price",
            "filled_price",
            "vwap",
        ),
    )
    price = _valid_binary_price(_coerce_float(explicit))
    if price is not None:
        return price

    if fill_size is not None and fill_size > 0:
        filled_value = _positive_float(
            _first_value(
                payload,
                (
                    "filled_value",
                    "filledValue",
                    "filled_value_usd",
                    "filledValueUsd",
                    "matched_value",
                    "matchedValue",
                    "amount_usd",
                    "amountUsd",
                    "notional",
                    "value",
                ),
            )
        )
        if filled_value is not None:
            price = _valid_binary_price(filled_value / fill_size)
            if price is not None:
                return price

        if intent_amount_usd is not None and intent_amount_usd > 0:
            implied = _valid_binary_price(intent_amount_usd / fill_size)
            if (
                implied is not None
                and intent_price is not None
                and implied <= intent_price + _price_tolerance(intent_price)
            ):
                return implied

        order_price = _valid_binary_price(
            _coerce_float(
                _first_value(payload, ("price", "limit_price", "limitPrice"))
            )
        )
        original_size = _positive_float(
            _first_value(payload, ("original_size", "originalSize", "size"))
        )
        if (
            order_price is not None
            and original_size is not None
            and _close_enough(fill_size, original_size, rel_tol=0.001, abs_tol=0.001)
        ):
            return order_price

        if intent_amount_usd is not None and intent_amount_usd > 0:
            if (
                order_price is not None
                and _close_enough(
                    fill_size * order_price,
                    intent_amount_usd,
                    rel_tol=0.005,
                    abs_tol=0.02,
                )
            ):
                return order_price

        # Root-cause fix (LIVE row 571): a PARTIAL fill with no explicit VWAP,
        # no filled_value, and an implied price above the cap still has a KNOWN
        # conservative upper bound — the order's executable limit `price`. A
        # marketable FAK/limit BUY only fills at levels AT OR BELOW that limit,
        # so book it rather than persist size-without-price and strand the row.
        if order_price is not None:
            return order_price

    return None


def _price_tolerance(price: float) -> float:
    return max(0.0005, 0.002 * max(abs(price), 1.0))


def _first_value(payload: dict[str, Any], keys: tuple[str, ...]) -> object:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _positive_float(value: object) -> Optional[float]:
    out = _coerce_float(value)
    if out is None or out <= 0.0:
        return None
    return out


def _valid_binary_price(value: Optional[float]) -> Optional[float]:
    if value is None or value <= 0.0 or value > 1.0:
        return None
    return value


def _make_default_order_fetcher(
    order_client: object,
) -> Callable[[str], Optional[dict[str, Any]]]:
    """Build a default `(order_id) -> dict | None` closure that wraps the
    OrderClient's CLOB get_order/get_order_status method."""

    def _fetch(order_id: str) -> Optional[dict[str, Any]]:
        client = getattr(order_client, "_client", None)
        if client is None:
            return None
        for attr in ("get_order", "order", "get_order_status"):
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            try:
                return fn(order_id)
            except Exception:  # noqa: BLE001
                logger.debug(
                    "reconcile_open_orders: %s failed for %s",
                    attr,
                    order_id,
                    exc_info=True,
                )
                return None
        return None

    return _fetch


def _make_default_funder_orders_fetcher(
    order_client: object,
) -> Callable[[], list[dict[str, Any]]]:
    def _fetch() -> list[dict[str, Any]]:
        client = getattr(order_client, "_client", None)
        if client is None:
            return []
        for attr in ("get_orders", "get_all_orders", "orders"):
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            rows = fn() or []
            return [r for r in rows if isinstance(r, dict)]
        return []

    return _fetch


def _find_stale_attempt_match(
    attempt: LiveOrderAttempt,
    clob_orders: list[dict[str, Any]],
    *,
    clob_by_coid: dict[str, dict[str, Any]],
    clob_by_order_id: dict[str, dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Match a stale local attempt to a CLOB order.

    Prefer explicit correlation ids. When the CLOB payload omits the local
    `client_order_id`, fall back to the durable order intent that was persisted
    before submission: token, side, executable limit price, requested notional,
    and the attempt/order timestamps. The fallback deliberately requires at
    least two pieces of non-id evidence beyond token+side to avoid linking an
    unrelated open position in the same market.
    """
    match = clob_by_coid.get(attempt.client_order_id)
    if match is not None:
        return match
    if attempt.live_order_id:
        match = clob_by_order_id.get(str(attempt.live_order_id))
        if match is not None:
            return match

    scored: list[tuple[int, dict[str, Any]]] = []
    for order in clob_orders:
        if not _same_text(_clob_token_id(order), attempt.poly_token_id):
            continue
        if not _clob_outcome_side_matches_attempt(order, attempt):
            continue

        evidence = 0
        price = _clob_price(order)
        if price is not None and _close_enough(price, attempt.price, abs_tol=0.0001):
            evidence += 1

        if _clob_amount_matches_attempt(order, attempt):
            evidence += 1

        if _clob_time_matches_attempt(order, attempt):
            evidence += 1

        if evidence >= 2:
            scored.append((evidence, order))

    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def _clob_order_id(order: dict[str, Any]) -> Optional[str]:
    value = (
        order.get("id")
        or order.get("order_id")
        or order.get("orderID")
        or order.get("orderId")
    )
    return str(value) if value else None


def _clob_token_id(order: dict[str, Any]) -> Optional[str]:
    value = (
        order.get("asset_id")
        or order.get("token_id")
        or order.get("tokenID")
        or order.get("tokenId")
    )
    return str(value) if value else None


def _clob_side(order: dict[str, Any]) -> Optional[str]:
    side = order.get("side") or order.get("outcome")
    return str(side).upper() if side else None


def _clob_outcome_side_matches_attempt(
    order: dict[str, Any], attempt: LiveOrderAttempt
) -> bool:
    side = _clob_side(order)
    if side is None or side in {"BUY", "SELL"}:
        return True
    if side == "UP":
        side = "YES"
    elif side == "DOWN":
        side = "NO"
    return _same_text(side, attempt.side)


def _clob_price(order: dict[str, Any]) -> Optional[float]:
    return _coerce_float(
        order.get("price")
        or order.get("limit_price")
        or order.get("limitPrice")
        or order.get("avg_fill_price")
        or order.get("matched_price")
    )


def _clob_amount_usd(order: dict[str, Any]) -> Optional[float]:
    return _coerce_float(
        order.get("amount_usd")
        or order.get("amountUsd")
        or order.get("notional")
        or order.get("value")
        or order.get("filled_value")
        or order.get("filledValue")
    )


def _clob_size_tokens(order: dict[str, Any]) -> Optional[float]:
    return _coerce_float(
        order.get("size")
        or order.get("original_size")
        or order.get("originalSize")
        or order.get("size_matched")
        or order.get("matched_size")
        or order.get("filled_size")
        or order.get("filled")
    )


def _clob_amount_matches_attempt(
    order: dict[str, Any], attempt: LiveOrderAttempt
) -> bool:
    amount_usd = _clob_amount_usd(order)
    if amount_usd is not None:
        return _close_enough(amount_usd, attempt.amount_usd, rel_tol=0.02, abs_tol=0.05)

    price = _clob_price(order) or attempt.price
    size_tokens = _clob_size_tokens(order)
    if price is None or size_tokens is None or price <= 0:
        return False
    expected_tokens = float(attempt.amount_usd) / float(price)
    return _close_enough(size_tokens, expected_tokens, rel_tol=0.02, abs_tol=0.0001)


def _clob_time_matches_attempt(
    order: dict[str, Any], attempt: LiveOrderAttempt
) -> bool:
    order_ts = _parse_time(
        order.get("created_at")
        or order.get("createdAt")
        or order.get("created")
        or order.get("timestamp")
        or order.get("matched_at")
        or order.get("filled_at")
    )
    attempt_ts = _parse_time(attempt.created_at)
    if order_ts is None or attempt_ts is None:
        return False
    return abs((order_ts - attempt_ts).total_seconds()) <= 300


def _parse_time(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except (OSError, ValueError):
            return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                parsed = None  # type: ignore[assignment]
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _same_text(left: Optional[str], right: Optional[str]) -> bool:
    return bool(left) and bool(right) and str(left).lower() == str(right).lower()


def _close_enough(
    left: Optional[float],
    right: Optional[float],
    *,
    rel_tol: float = 0.001,
    abs_tol: float = 0.0,
) -> bool:
    if left is None or right is None:
        return False
    try:
        l_val = float(left)
        r_val = float(right)
    except (TypeError, ValueError):
        return False
    if l_val != l_val or r_val != r_val:
        return False
    return abs(l_val - r_val) <= max(abs_tol, rel_tol * max(abs(r_val), 1.0))
