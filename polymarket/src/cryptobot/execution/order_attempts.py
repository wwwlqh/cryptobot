"""Durable LIVE order-attempt reservation (D-LIVE-28).

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U5 / §U6:

A LIVE order is NEVER submitted to Polymarket until the bot has durably
written a local `live_order_attempts` row carrying a unique
`client_order_id`. The CLOB call happens OUTSIDE the SQLite write
transaction so a crash between the SUBMITTING write and the network
return leaves a recoverable artifact for the reconciler (U7).

Lifecycle:

  1. `reserve_live_order_attempt(conn, signal, wallet_group)` — INSERT row
     with `status='SUBMITTING'`, `client_order_id=uuid4().hex`,
     `ledger_id=NULL`. The active-slot partial unique index prevents two
     concurrent attempts on the same slot — a parallel writer surfaces
     as an `IntegrityError`.

  2. `mark_attempt_submitted(conn, attempt_id, live_order_id)` — after the
     CLOB accepts the order. Carries the CLOB-side `order_id` forward.

  3. `link_attempt_to_ledger(conn, attempt_id, ledger_id)` — after
     `record_bet` writes the LIVE row. Status transitions to `LINKED` and
     the active-slot reservation is released (LINKED is outside the
     partial index predicate).

  4. `mark_attempt_failed(conn, attempt_id, error_kind, error)` — terminal
     failure path (auth, insufficient_funds, market_closed). Also releases
     the active-slot reservation.

The group-cap gate (D-LIVE-14) counts `SUBMITTING` + `SUBMITTED` rows
where `ledger_id IS NULL` toward pending group exposure, so a reserved
attempt blocks duplicate slot submission across ticks.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from typing import Optional, TYPE_CHECKING

from cryptobot.db.connection import utc_now_sql

if TYPE_CHECKING:
    from cryptobot.execution.ledger import BetSignal


# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------


SUBMITTING = "SUBMITTING"
SUBMITTED = "SUBMITTED"
LINKED = "LINKED"
FAILED = "FAILED"

ALLOWED_STATUSES: frozenset[str] = frozenset({SUBMITTING, SUBMITTED, LINKED, FAILED})

# Statuses that still count toward group exposure — they're active reservations
# waiting on the CLOB or the ledger row. LINKED and FAILED are terminal.
ACTIVE_STATUSES: frozenset[str] = frozenset({SUBMITTING, SUBMITTED})


# ---------------------------------------------------------------------------
# Placement epoch — the "something was reserved/placed" invalidation signal.
#
# Read by `execution.pipeline._FastCommitStage` (clause 1): ANY reservation in
# this process invalidates every cached commit-stage preamble, because a
# SUBMITTING row with `ledger_id IS NULL` counts toward group pending exposure
# (`capital.get_group_state`) while the OTHER invalidation signal —
# `capital.ledger_version` — aggregates the `ledger` table only, so it does not
# move until the bet's own `record_bet` lands one venue round trip later, and
# never at all when the POST fails.
#
# It lives HERE, in the leaf module every reservation funnels through, and is
# bumped by `reserve_live_order_attempt` itself, so a new reservation site
# cannot forget it. That is not hypothetical: the T3 scale-in top-up
# (`execution.scale_in`) reserves without going through pipeline.py and cannot
# import it (pipeline -> held_position_pass -> scale_in is already a chain), so a bump
# written at each CALL SITE left exactly one path consuming group-cap pending
# while every cached commit-stage view still said it had not.
#
# `itertools.count` instead of `+= 1` because `next()` on it is atomic in
# CPython: a lost read-modify-write update would be a MISSED invalidation,
# which is the one failure mode this counter exists to prevent.
# ---------------------------------------------------------------------------


_PLACEMENT_COUNTER = itertools.count(1)
_PLACEMENT_EPOCH: int = 0


def bump_placement_epoch() -> None:
    """Invalidate every cached commit-stage view. Cheap enough to be
    unconditional — two atomic ops on a path that is already POSTing."""
    global _PLACEMENT_EPOCH
    _PLACEMENT_EPOCH = next(_PLACEMENT_COUNTER)


def placement_epoch() -> int:
    """The current epoch.

    A FUNCTION, not a module value to import: ``from ... import
    _PLACEMENT_EPOCH`` binds the integer ONCE at import time and would never
    observe a bump again — a silently permanent stale serve.
    """
    return _PLACEMENT_EPOCH


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveOrderAttempt:
    """In-memory view of one `live_order_attempts` row."""

    id: int
    created_at: str
    updated_at: str
    wallet_id: str
    wallet_group: Optional[str]
    mode: str
    coin: str
    sleeve: str
    entry_minute: int
    side: str
    window_start_ts: int
    client_order_id: str
    poly_market_id: Optional[str]
    poly_token_id: Optional[str]
    price: Optional[float]
    amount_usd: float
    status: str
    ledger_id: Optional[int]
    live_order_id: Optional[str]
    error_kind: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_client_order_id() -> str:
    """Generate a unique local correlation key. UUID4 hex (32 chars)
    matches what most CLOB clients accept as a custom client_order_id."""
    return uuid.uuid4().hex


def _signal_payload(signal: "BetSignal") -> str:
    """Serialize a BetSignal to JSON for crash-recovery.

    Stored on the attempt row so the reconciler (U7) can reconstruct the
    bet's intent when the process crashed between CLOB submit and ledger
    write. We use `dataclasses.asdict` for a stable representation;
    Optional fields are preserved as JSON `null`.
    """
    try:
        return json.dumps(asdict(signal), sort_keys=True, default=str)
    except (TypeError, ValueError):
        # Best-effort fallback for test stubs whose dataclasses aren't
        # JSON-serializable.
        return json.dumps({"sleeve": getattr(signal, "sleeve", "")})


def _row_to_attempt(row: sqlite3.Row) -> LiveOrderAttempt:
    """Map a `live_order_attempts` SELECT row to a LiveOrderAttempt."""
    return LiveOrderAttempt(
        id=row["id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        wallet_id=row["wallet_id"],
        wallet_group=row["wallet_group"],
        mode=row["mode"],
        coin=row["coin"],
        sleeve=row["sleeve"],
        entry_minute=row["entry_minute"],
        side=row["side"],
        window_start_ts=row["window_start_ts"],
        client_order_id=row["client_order_id"],
        poly_market_id=row["poly_market_id"],
        poly_token_id=row["poly_token_id"],
        price=row["price"],
        amount_usd=row["amount_usd"],
        status=row["status"],
        ledger_id=row["ledger_id"],
        live_order_id=row["live_order_id"],
        error_kind=row["error_kind"],
        error=row["error"],
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def reserve_live_order_attempt(
    conn: sqlite3.Connection,
    signal: "BetSignal",
    *,
    wallet_group: Optional[str] = None,
    client_order_id: Optional[str] = None,
) -> LiveOrderAttempt:
    """Insert a `SUBMITTING` row BEFORE invoking the CLOB.

    Raises `sqlite3.IntegrityError` when an active (SUBMITTING/SUBMITTED)
    row already exists for the same slot — the active-slot partial unique
    index (uniq_loa_active_slot) catches the race so the caller can SKIP
    cleanly rather than placing a duplicate LIVE order.

    Bumps the placement epoch on success, so every caller invalidates the fast
    rungs' cached commit-stage views by construction (see
    :func:`bump_placement_epoch`) — the reservation it just committed is group
    pending exposure that no other invalidation signal reflects yet.
    """
    coid = client_order_id or make_client_order_id()
    wallet_id = signal.wallet_id or ""
    if not wallet_id:
        raise ValueError(
            "reserve_live_order_attempt: signal.wallet_id is required for the "
            "active-slot reservation"
        )

    now = utc_now_sql()
    order_price = (
        signal.limit_price
        if getattr(signal, "limit_price", None) is not None
        else signal.fill_price
    )
    cur = conn.execute(
        """
        INSERT INTO live_order_attempts
          (created_at, updated_at, wallet_id, wallet_group, mode, coin,
           sleeve, entry_minute, side, window_start_ts, client_order_id,
           poly_market_id, poly_token_id, price, amount_usd, status,
           signal_json)
        VALUES (?, ?, ?, ?, 'LIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            now,
            wallet_id,
            wallet_group,
            signal.coin,
            signal.sleeve,
            signal.entry_minute,
            signal.side,
            signal.window_start_ts,
            coid,
            signal.poly_market_id,
            signal.poly_token_id,
            order_price,
            signal.bet_size,
            SUBMITTING,
            _signal_payload(signal),
        ),
    )
    conn.commit()
    # AFTER the commit, never before: the epoch is sampled by readers BEFORE
    # they load, so bumping ahead of the write would let a load that misses this
    # row still be cached under the post-bump epoch.
    bump_placement_epoch()
    attempt_id = cur.lastrowid
    if attempt_id is None:
        raise RuntimeError("reserve_live_order_attempt INSERT did not produce a rowid")
    return LiveOrderAttempt(
        id=int(attempt_id),
        created_at=now,
        updated_at=now,
        wallet_id=wallet_id,
        wallet_group=wallet_group,
        mode="LIVE",
        coin=signal.coin,
        sleeve=signal.sleeve,
        entry_minute=signal.entry_minute,
        side=signal.side,
        window_start_ts=signal.window_start_ts,
        client_order_id=coid,
        poly_market_id=signal.poly_market_id,
        poly_token_id=signal.poly_token_id,
        price=order_price,
        amount_usd=signal.bet_size,
        status=SUBMITTING,
        ledger_id=None,
        live_order_id=None,
    )


def mark_attempt_submitted(
    conn: sqlite3.Connection,
    attempt_id: int,
    live_order_id: str,
    *,
    commit: bool = True,
) -> None:
    """SUBMITTING → SUBMITTED. Records the CLOB-returned order_id so the
    reconciler can match the in-flight order back to this attempt.

    ``commit=False`` lets the pipeline fold this transition into the ledger
    write's own BEGIN IMMEDIATE (one commit on the order path, not two). A
    rollback there reverts the attempt to SUBMITTING — still recoverable:
    reconcile_stale_attempts matches SUBMITTING rows by client_order_id /
    order intent from the persisted signal_json, so no crash checkpoint is
    lost by deferring the commit."""
    if not live_order_id:
        raise ValueError("mark_attempt_submitted: live_order_id is required")
    cur = conn.execute(
        """
        UPDATE live_order_attempts
           SET status=?, live_order_id=?, updated_at=?
         WHERE id=? AND status=?
        """,
        (SUBMITTED, live_order_id, utc_now_sql(), attempt_id, SUBMITTING),
    )
    if int(cur.rowcount or 0) == 0:
        raise RuntimeError(
            f"mark_attempt_submitted: attempt {attempt_id} not in SUBMITTING state"
        )
    if commit:
        conn.commit()


def link_attempt_to_ledger(
    conn: sqlite3.Connection,
    attempt_id: int,
    ledger_id: int,
) -> None:
    """SUBMITTING/SUBMITTED → LINKED. Releases the active-slot reservation
    (LINKED is outside the partial unique index predicate) so the group
    cap gate no longer counts this row as in-flight exposure."""
    cur = conn.execute(
        """
        UPDATE live_order_attempts
           SET status=?, ledger_id=?, updated_at=?
         WHERE id=?
        """,
        (LINKED, ledger_id, utc_now_sql(), attempt_id),
    )
    if int(cur.rowcount or 0) == 0:
        raise RuntimeError(
            f"link_attempt_to_ledger: attempt {attempt_id} not found"
        )
    conn.commit()


def mark_attempt_failed(
    conn: sqlite3.Connection,
    attempt_id: int,
    error_kind: str,
    error: Optional[str] = None,
) -> None:
    """Any → FAILED. Records the coarse error_kind so the reconciler can
    skip retry on terminal failures (auth, insufficient_funds, market_closed)."""
    cur = conn.execute(
        """
        UPDATE live_order_attempts
           SET status=?, error_kind=?, error=?, updated_at=?
         WHERE id=?
        """,
        (FAILED, error_kind, error, utc_now_sql(), attempt_id),
    )
    if int(cur.rowcount or 0) == 0:
        raise RuntimeError(f"mark_attempt_failed: attempt {attempt_id} not found")
    conn.commit()


def get_attempt(conn: sqlite3.Connection, attempt_id: int) -> Optional[LiveOrderAttempt]:
    row = conn.execute(
        "SELECT id, created_at, updated_at, wallet_id, wallet_group, mode, coin, sleeve, "
        "entry_minute, side, window_start_ts, client_order_id, amount_usd, "
        "poly_market_id, poly_token_id, price, status, ledger_id, "
        "live_order_id, error_kind, error "
        "FROM live_order_attempts WHERE id=?",
        (attempt_id,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_attempt(row)


def get_attempt_signal_json(
    conn: sqlite3.Connection, attempt_id: int
) -> Optional[str]:
    """Return the raw `signal_json` persisted on the attempt row, or None.

    The reconciler (U7 / audit P0 #2) deserializes this back into a
    BetSignal to reconstruct the LIVE ledger row when the process crashed
    between the CLOB fill and `record_bet`."""
    row = conn.execute(
        "SELECT signal_json FROM live_order_attempts WHERE id=?",
        (attempt_id,),
    ).fetchone()
    if row is None:
        return None
    value = row["signal_json"]
    return str(value) if value else None


def stale_active_attempts(
    conn: sqlite3.Connection, *, older_than_seconds: int
) -> list[LiveOrderAttempt]:
    """Return SUBMITTING/SUBMITTED rows older than `older_than_seconds`
    with `ledger_id IS NULL`. Reconciler (U7) feeds these to a CLOB
    lookup-by-client-order-id or by-funder/token recovery query."""
    rows = conn.execute(
        f"""
        SELECT id, created_at, updated_at, wallet_id, wallet_group, mode, coin, sleeve,
               entry_minute, side, window_start_ts, client_order_id,
               poly_market_id, poly_token_id, price, amount_usd, status,
               ledger_id, live_order_id, error_kind, error
          FROM live_order_attempts
         WHERE status IN ('SUBMITTING','SUBMITTED')
           AND ledger_id IS NULL
           AND updated_at < datetime('now', ?)
        """,
        (f"-{int(older_than_seconds)} seconds",),
    ).fetchall()
    return [_row_to_attempt(row) for row in rows]
