"""Live-money action guards — read-only gates over `live_readiness_reports`,
`wallet_reconciliation_runs`, and `operator_control_state`.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U4 / D-LIVE-5:

  * `require_processing_enabled` — `operator_control_state.state == LIVE`.
  * `require_no_transfer_lock`   — state must not be `TRANSFER_LOCK`.
  * `require_wallet_can_go_live` — composite gate (state + readiness +
    reconciliation freshness) used by every DRY → LIVE flip attempt.

Each raises a `BlockedError` whose `.reason` is a stable token the
dashboard surface switches on (`'operator_stopped'`, `'transfer_lock'`,
`'readiness_stale'`, `'no_readiness_report'`, `'wallet_recon_stale'`,
`'wallet_recon_missing'`).

Phase 1 reads from durable tables only — the writers
(`evaluate_readiness` in §U9, `WalletReconciler.sample` in §U7) ship in
Phase 2. Until those land, every DRY → LIVE attempt fails closed with
`no_readiness_report` / `wallet_recon_missing` — which is exactly the
desired behavior: the operator can't promote a wallet to LIVE on a
Phase 1 deploy because no live machinery exists yet.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
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
    parse_iso as _parse_iso,
    seconds_since as _seconds_since,
)


# ---------------------------------------------------------------------------
# Global state gates
# ---------------------------------------------------------------------------


def require_processing_enabled(conn: sqlite3.Connection) -> None:
    """Raise `BlockedError(reason='operator_stopped'|'transfer_lock')` when
    the global state blocks LIVE order placement.

    DRY wallets bypass this gate entirely (D-LIVE-12 — STOPPED blocks
    LIVE only). Callers route by `wallet_mode_state` before invoking.
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
# Readiness gate (D-LIVE-5 / D-LIVE-9)
# ---------------------------------------------------------------------------


def _latest_readiness_report(
    conn: sqlite3.Connection,
) -> Optional[dict[str, object]]:
    """Return the most-recent `live_readiness_reports` row as a dict, or
    None if no row exists. Phase 1 deploys never have one — `evaluate_readiness`
    (U9) is the writer. Returning None lets the guard fail closed with
    a distinct `no_readiness_report` reason vs `readiness_stale`.

    Kieran-python finding #7: thin wrapper around the canonical
    `live_readiness.latest_readiness_report` so a single schema change
    only updates one query site. The public function returns the same
    row shape; we filter to the fields this guard cares about.
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
# Wallet reconciliation freshness gate (D-LIVE-9)
# ---------------------------------------------------------------------------


def require_fresh_wallet_reconciliation(
    conn: sqlite3.Connection,
    *,
    ttl_s: int = WALLET_SNAPSHOT_FRESHNESS_TTL_S,
) -> None:
    """Refuse when no fresh `wallet_reconciliation_runs` row exists.

    Phase 1 deploys lack the writer; raises `wallet_recon_missing`. Phase
    2's `WalletReconciler.sample` (U7) starts populating the table; after
    that, stale rows raise `wallet_recon_stale`.
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
    """Fail closed unless every precondition for LIVE order placement holds.

    Order of checks (earliest-failing reason wins):
      1. wallet_id is non-empty and exists in `wallet_mode_state`
         (raises 'unknown_wallet')
      2. global state == LIVE (raises 'operator_stopped' or 'transfer_lock')
      3. fresh `live_readiness_reports.status='READY'` within TTL
         (raises 'no_readiness_report' / 'readiness_not_ready' / 'readiness_stale')
      4. fresh `wallet_reconciliation_runs` within TTL
         (raises 'wallet_recon_missing' / 'wallet_recon_stale')

    Maintainability finding #12: the per-wallet existence check is the
    minimum meaningful use of the `wallet_id` parameter. Previously the
    argument was discarded (`_ = wallet_id  # reserved for U6/U7`), so a
    typo or stale wallet_id silently passed every check. The on-chain
    funder-balance >= SUM(group_caps) check is still a deferred U6/U7
    item but the typo case is now caught at the gate.

    Maintainability finding #43: `require_no_transfer_lock(conn)` used
    to be called right after `require_processing_enabled(conn)`; the
    latter already raises BlockedError('transfer_lock') when state ==
    TRANSFER_LOCK, so the second call was unreachable and the operator
    paid for an extra `get_operator_state` round-trip on every check.
    Removed.
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
