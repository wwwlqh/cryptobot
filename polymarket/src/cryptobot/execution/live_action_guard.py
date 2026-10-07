"""Guards for real-money actions: read-only checks over
``live_readiness_reports``, ``wallet_reconciliation_runs`` and
``operator_control_state``.

  * ``require_processing_enabled``: ``operator_control_state.state == LIVE``.
  * ``require_no_transfer_lock``: state is not ``TRANSFER_LOCK``.
  * ``require_wallet_can_go_live``: state, readiness and reconciliation
    freshness together; used by every DRY -> LIVE switch.

Each raises ``BlockedError`` with a stable ``.reason`` the dashboard switches
on (``'operator_stopped'``, ``'transfer_lock'``, ``'readiness_stale'``,
``'no_readiness_report'``, ``'wallet_recon_stale'``,
``'wallet_recon_missing'``).

Until the readiness and reconciliation jobs have written their first rows,
every DRY -> LIVE attempt fails closed with ``no_readiness_report`` or
``wallet_recon_missing``.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from cryptobot.execution.operator_control import (
    LIVE,
    TRANSFER_LOCK,
    BlockedError,
    get_operator_state,
)
from cryptobot.execution.strategy_constants import (
    LIVE_READINESS_CACHE_TTL_S,
    WALLET_SNAPSHOT_FRESHNESS_TTL_S,
)


from cryptobot.execution._time_utils import (
    seconds_since as _seconds_since,
)


# ---------------------------------------------------------------------------
# Global state gates
# ---------------------------------------------------------------------------


def require_processing_enabled(conn: sqlite3.Connection) -> None:
    """Raise ``BlockedError(reason='operator_stopped'|'transfer_lock')`` when
    the global state blocks live orders.

    DRY wallets are not subject to this (a stop blocks live trading only);
    callers route by ``wallet_mode_state`` first.
    """
    state = get_operator_state(conn)
    if state.state == LIVE:
        return
    if state.state == TRANSFER_LOCK:
        raise BlockedError(
            "transfer_lock",
            "operator state is TRANSFER_LOCK; LIVE entries refused",
        )
    raise BlockedError(
        "operator_stopped",
        f"operator state is {state.state}; LIVE entries refused",
    )


# ---------------------------------------------------------------------------
# Readiness gate
# ---------------------------------------------------------------------------


def _latest_readiness_report(
    conn: sqlite3.Connection,
) -> Optional[dict[str, object]]:
    """Return the latest ``live_readiness_reports`` row as a dict, or None if
    there is none, so the guard can fail with ``no_readiness_report`` rather
    than ``readiness_stale``. Wraps
    ``live_readiness.latest_readiness_report`` so the query lives in one
    place.
    """
    from cryptobot.execution.live_readiness import latest_readiness_report

    row = latest_readiness_report(conn)
    if row is None:
        return None
    return {
        "id": row.get("id"),
        "created_at": row.get("created_at"),
        "status": row.get("status"),
        "mode": row.get("mode"),
        "funder": row.get("funder"),
        "signer": row.get("signer"),
        "clob_balance_usd": row.get("clob_balance_usd"),
        "chain_balance_usd": row.get("chain_balance_usd"),
    }


def require_fresh_live_readiness(
    conn: sqlite3.Connection,
    *,
    ttl_s: int = LIVE_READINESS_CACHE_TTL_S,
) -> None:
    """Refuse when no fresh `live_readiness_reports.status='READY'` row
    exists within `ttl_s` seconds."""
    latest = _latest_readiness_report(conn)
    if latest is None:
        raise BlockedError(
            "no_readiness_report",
            "no live_readiness_reports row found; cannot promote to LIVE",
        )
    if latest["status"] != "READY":
        raise BlockedError(
            "readiness_not_ready",
            f"latest readiness status is {latest['status']!r}, expected 'READY'",
        )
    elapsed = _seconds_since(str(latest["created_at"] or ""))
    if elapsed is None or elapsed > ttl_s:
        raise BlockedError(
            "readiness_stale",
            f"latest readiness row is {elapsed}s old (ttl={ttl_s}s)",
        )


# ---------------------------------------------------------------------------
# Wallet reconciliation freshness gate
# ---------------------------------------------------------------------------


def require_fresh_wallet_reconciliation(
    conn: sqlite3.Connection,
    *,
    ttl_s: int = WALLET_SNAPSHOT_FRESHNESS_TTL_S,
) -> None:
    """Refuse when there is no recent ``wallet_reconciliation_runs`` row.

    Raises ``wallet_recon_missing`` before the reconciler has ever run, and
    ``wallet_recon_stale`` when the latest run is too old.
    """
    try:
        row = conn.execute(
            "SELECT sampled_at, source_status FROM wallet_reconciliation_runs "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is None:
        raise BlockedError(
            "wallet_recon_missing",
            "no wallet_reconciliation_runs row found; cannot promote to LIVE",
        )
    elapsed = _seconds_since(str(row["sampled_at"] or ""))
    if elapsed is None or elapsed > ttl_s:
        raise BlockedError(
            "wallet_recon_stale",
            f"latest wallet reconciliation row is {elapsed}s old (ttl={ttl_s}s)",
        )


# ---------------------------------------------------------------------------
# Composite gate — what set_wallet_mode(target=LIVE) calls
# ---------------------------------------------------------------------------


def require_wallet_can_go_live(
    conn: sqlite3.Connection,
    wallet_id: str,
    *,
    readiness_ttl_s: int = LIVE_READINESS_CACHE_TTL_S,
    recon_ttl_s: int = WALLET_SNAPSHOT_FRESHNESS_TTL_S,
) -> None:
    """Fail closed unless every precondition for live orders holds.

    Checks, in order (the first failure wins):

      1. wallet_id is non-empty and present in ``wallet_mode_state``
         ('unknown_wallet'), so a typo cannot pass;
      2. global state is LIVE ('operator_stopped' or 'transfer_lock');
      3. a recent ``live_readiness_reports`` row with status 'READY'
         ('no_readiness_report' / 'readiness_not_ready' / 'readiness_stale');
      4. a recent ``wallet_reconciliation_runs`` row
         ('wallet_recon_missing' / 'wallet_recon_stale').

    Step 2 also covers TRANSFER_LOCK, so ``require_no_transfer_lock`` is not
    called separately.
    """
    if not wallet_id or not isinstance(wallet_id, str):
        raise BlockedError(
            "unknown_wallet",
            f"wallet_id must be a non-empty string; got {wallet_id!r}",
        )
    try:
        row = conn.execute(
            "SELECT 1 FROM wallet_mode_state WHERE wallet_id=? LIMIT 1",
            (wallet_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is None:
        raise BlockedError(
            "unknown_wallet",
            f"wallet_mode_state has no row for wallet_id={wallet_id!r}; "
            f"refusing LIVE promotion of an unknown wallet",
        )
    require_processing_enabled(conn)
    require_fresh_live_readiness(conn, ttl_s=readiness_ttl_s)
    require_fresh_wallet_reconciliation(conn, ttl_s=recon_ttl_s)
