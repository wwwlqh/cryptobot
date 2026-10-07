"""Reject re-quote TELEMETRY (observation only — never places an order).

When a LIVE FAK is rejected at the ``_commit_cell`` order-placement site we
learn nothing today about *why*: was the quoted ask a ghost that vanished, or
did the book move against us in the ~250ms taker delay? This module answers
that cheaply and off the decision path:

  1. At the reject site we ``insert_reject_requote`` a row capturing the
     placement intent (coin/sleeve/offset/window/side/limit/size) and
     ``enqueue_requote_reread`` a lightweight job on a module-level queue.
  2. ~1-2s later a fast-cadence (2s) scheduler job drains the queue via
     ``drain_requote_queue``, re-reads the book ONCE, and UPDATEs
     ``requote_ask`` + ``requote_age_ms`` (how the top-of-book looked shortly
     after the reject, and how long after).
  3. Once the market settles, the resolution sweep calls
     ``backfill_reject_requote_outcomes`` to fill ``resolved_outcome`` (UP/DOWN)
     so a reject can be scored against what actually happened.

No order is ever placed, cancelled, or gated from this path. Every entry point
is best-effort: any failure is swallowed so the live order path is untouched.
The queue is bounded and drops on overflow — telemetry must never back-pressure
the tick.
"""

from __future__ import annotations

import logging
import queue
import re
import sqlite3
import time
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Bounded so a reject storm can never grow memory without limit; overflow just
# drops the re-read (the row is already persisted with NULL requote_ask).
_MAX_QUEUE = 4096
_REQUOTE_QUEUE: "queue.Queue[_PendingRequote]" = queue.Queue(maxsize=_MAX_QUEUE)

# Wait at least this long before re-reading the book after a reject; we want
# the book a moment later, not at the same instant. The real delay is this plus
# the drain cadence and must stay far below the 30-second entry window,
# otherwise ``requote_ask`` captures the near-final book (the outcome) rather
# than whether the order could have filled. ``requote_age_ms`` records the
# actual delay; treat rows with large ages with caution.
_REREAD_DELAY_S = 1.0

BookFetch = Callable[..., object]  # (token_id, *, now_ts) -> BookSnapshot|None


@dataclass(frozen=True)
class _PendingRequote:
    row_id: int
    token_id: str
    enqueue_monotonic: float
    ready_at_monotonic: float


# ---------------------------------------------------------------------------
# Reject-site write + enqueue
# ---------------------------------------------------------------------------


def insert_reject_requote(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    offset_sec: Optional[int],
    window_start_ts: int,
    side: str,
    limit_price: Optional[float],
    requested_usd: float,
) -> Optional[int]:
    """Persist one reject's placement intent. Returns the new row id (for the
    deferred re-quote) or ``None`` on failure. The caller commits."""
    cur = conn.execute(
        """
        INSERT INTO reject_requotes
          (coin, sleeve, offset_sec, window_start_ts, side, limit_price,
           requested_usd)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            coin,
            sleeve,
            None if offset_sec is None else int(offset_sec),
            int(window_start_ts),
            side,
            limit_price,
            float(requested_usd),
        ),
    )
    return int(cur.lastrowid) if cur.lastrowid is not None else None


def enqueue_requote_reread(
    row_id: int,
    token_id: str,
    *,
    delay_s: float = _REREAD_DELAY_S,
    now_monotonic: Optional[float] = None,
) -> bool:
    """Queue a reject row for a ~``delay_s``-later book re-read. Best-effort:
    returns False (and drops) if the queue is full or inputs are unusable."""
    if not token_id or row_id is None:
        return False
    now = time.monotonic() if now_monotonic is None else float(now_monotonic)
    item = _PendingRequote(
        row_id=int(row_id),
        token_id=str(token_id),
        enqueue_monotonic=now,
        ready_at_monotonic=now + max(0.0, float(delay_s)),
    )
    try:
        _REQUOTE_QUEUE.put_nowait(item)
        return True
    except queue.Full:
        logger.debug("reject_requote queue full — dropping re-read for row %s", row_id)
        return False


# ---------------------------------------------------------------------------
# Deferred drain (fast-cadence scheduler job)
# ---------------------------------------------------------------------------


def drain_requote_queue(
    conn: sqlite3.Connection,
    *,
    fetch_book: BookFetch,
    now_monotonic: Optional[float] = None,
    now_ts: Optional[float] = None,
    max_items: int = 512,
    write_lock: Optional[AbstractContextManager] = None,
) -> dict:
    """Re-read the book for every queued reject whose re-read time has arrived.

    Items not yet ready are re-queued. Each ready item re-reads the token's
    book ONCE; the results are then written as one batch — ``requote_ask``
    (top ask, or NULL if the book is gone) + ``requote_age_ms`` (real elapsed
    since the reject). ``write_lock`` (main.py's db_write_lock) is held only
    around that batched write, never across a ``fetch_book`` HTTP call, and is
    not touched at all when there is nothing to write — the drain fires every
    ~2s and its idle path must stay contention-free (REL-08 pattern). Bounded
    by ``max_items`` per call so a backlog can't monopolize the scheduler
    thread. Best-effort: a per-item error is logged and skipped. The caller
    need not commit — this commits its own batch.
    """
    now_m = time.monotonic() if now_monotonic is None else float(now_monotonic)
    now_s = time.time() if now_ts is None else float(now_ts)
    processed = 0
    requeued = 0
    ready: list[_PendingRequote] = []
    not_ready: list[_PendingRequote] = []
    while len(ready) < max_items:
        try:
            item = _REQUOTE_QUEUE.get_nowait()
        except queue.Empty:
            break
        if item.ready_at_monotonic > now_m:
            not_ready.append(item)
            continue
        ready.append(item)
    # Book re-reads run lock-free: fetch_book is a network call.
    updates: list[tuple[Optional[float], int, int]] = []
    for item in ready:
        processed += 1
        try:
            snap = fetch_book(item.token_id, now_ts=now_s)
            requote_ask = getattr(snap, "top_ask", None) if snap is not None else None
            age_ms = int(max(0.0, (now_m - item.enqueue_monotonic) * 1000.0))
            updates.append((requote_ask, age_ms, item.row_id))
        except Exception:  # noqa: BLE001 — telemetry only; never break the job
            logger.debug("reject_requote re-read failed for row %s", item.row_id,
                         exc_info=True)
    # Put back the not-yet-ready items for a later drain.
    for item in not_ready:
        try:
            _REQUOTE_QUEUE.put_nowait(item)
            requeued += 1
        except queue.Full:
            break
    updated = 0
    if updates:
        try:
            with write_lock if write_lock is not None else nullcontext():
                conn.executemany(
                    "UPDATE reject_requotes SET requote_ask = ?, "
                    "requote_age_ms = ? WHERE id = ?",
                    updates,
                )
                conn.commit()
            updated = len(updates)
        except Exception:  # noqa: BLE001
            logger.debug("reject_requote drain write failed", exc_info=True)
    return {"updated": updated, "requeued": requeued, "processed": processed}


# ---------------------------------------------------------------------------
# Settlement backfill (resolution sweep)
# ---------------------------------------------------------------------------

_INTERVAL_RE = re.compile(r"(\d+)m")


def _interval_min_from_sleeve(sleeve: str) -> Optional[int]:
    """Derive interval minutes from a sleeve label (e.g. ``subminute_5m`` → 5).

    Returns None when no ``<n>m`` token is present so the caller can skip the
    row rather than guess."""
    if not sleeve:
        return None
    m = _INTERVAL_RE.search(str(sleeve))
    if not m:
        return None
    try:
        val = int(m.group(1))
    except ValueError:
        return None
    return val if val > 0 else None


def backfill_reject_requote_outcomes(
    conn: sqlite3.Connection,
    *,
    polymarket_resolution_fetch: Callable[[str, int, int], object],
    now_ts: Optional[float] = None,
    max_rows: int = 256,
) -> int:
    """Fill ``resolved_outcome`` (``UP``/``DOWN``) for settled reject rows.

    Only rows whose window has expired AND whose interval is recoverable from
    the sleeve label are queried. Markets not yet resolved (or unfetchable) are
    left NULL for a later sweep. Returns the number of rows updated. Best-effort
    per row; the caller commits (or we commit our own batch)."""
    from cryptobot.venues.types import RESOLUTION_RESOLVED

    now_s = time.time() if now_ts is None else float(now_ts)
    try:
        rows = conn.execute(
            "SELECT id, coin, sleeve, window_start_ts FROM reject_requotes "
            "WHERE resolved_outcome IS NULL "
            "ORDER BY id ASC LIMIT ?",
            (int(max_rows),),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    updated = 0
    for row in rows:
        sleeve = row["sleeve"]
        interval_min = _interval_min_from_sleeve(sleeve)
        if interval_min is None:
            continue
        window_start_ts = int(row["window_start_ts"])
        window_end_ts = window_start_ts + interval_min * 60
        if window_end_ts > now_s:
            continue  # not expired yet
        try:
            result = polymarket_resolution_fetch(
                row["coin"], interval_min, window_start_ts
            )
        except Exception:  # noqa: BLE001 — leave NULL, retry next sweep
            logger.debug("reject_requote resolution fetch failed for row %s",
                         row["id"], exc_info=True)
            continue
        status = getattr(result, "status", None)
        won_up = getattr(result, "won_up", None)
        if result is None or status != RESOLUTION_RESOLVED or won_up is None:
            continue
        outcome = "UP" if bool(won_up) else "DOWN"
        conn.execute(
            "UPDATE reject_requotes SET resolved_outcome = ? WHERE id = ?",
            (outcome, row["id"]),
        )
        updated += 1
    return updated
