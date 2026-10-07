"""Durable operator control state for the dashboard's live controls.

  * Global state: the ``operator_control_state`` singleton row, one of
    ``LIVE``, ``STOPPED_PROCESSING`` or ``TRANSFER_LOCK``. A fresh deploy
    starts in ``STOPPED_PROCESSING``, so an operator must press Start before
    any LIVE wallet places orders. Stopping blocks LIVE wallets only; DRY
    wallets keep producing paper bets.

  * Per-wallet mode: one ``wallet_mode_state`` row per wallet_id, with
    ``mode`` (DRY or LIVE) and an optimistic-concurrency ``version``. Flips
    are audited in ``operator_control_events`` with
    ``scope='wallet:<id>'``. DRY -> LIVE has a cooldown
    (``WALLET_MODE_FLIP_RATE_LIMIT_S``); LIVE -> DRY is always allowed.

  * ``set_wallet_mode(target='LIVE')`` calls
    ``live_action_guard.require_wallet_can_go_live(...)`` before saving, so
    the readiness, reconciliation and balance checks run for every caller
    (dashboard, CLI, internal).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import weakref
from dataclasses import dataclass
from typing import Callable, Optional, TYPE_CHECKING

from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.strategy_constants import WALLET_MODE_FLIP_RATE_LIMIT_S

if TYPE_CHECKING:
    from cryptobot.strategy.portfolio_loader import V6Portfolio

logger = logging.getLogger(__name__)


# -- State constants ---------------------------------------------------------

LIVE = "LIVE"
STOPPED_PROCESSING = "STOPPED_PROCESSING"
TRANSFER_LOCK = "TRANSFER_LOCK"
ALLOWED_OPERATOR_STATES: frozenset[str] = frozenset(
    {LIVE, STOPPED_PROCESSING, TRANSFER_LOCK}
)

DRY = "DRY"
ALLOWED_WALLET_MODES: frozenset[str] = frozenset({DRY, LIVE})


# -- Exceptions --------------------------------------------------------------


class OperatorControlError(RuntimeError):
    """Raised when an operator-control mutation violates a guardrail."""


class ConcurrentMutationError(OperatorControlError):
    """A version mismatch lost the optimistic-concurrency race; caller must retry."""


class BlockedError(OperatorControlError):
    """The state machine refused the transition (readiness stale, operator
    stopped, transfer lock active, ...). ``reason`` is a stable token the
    dashboard can switch on.
    """

    def __init__(self, reason: str, message: str | None = None) -> None:
        super().__init__(message or reason)
        self.reason = reason


class RateLimitError(BlockedError):
    """DRY -> LIVE flip inside the cooldown without ``force=True``."""

    def __init__(self, message: str) -> None:
        super().__init__(reason="rate_limited", message=message)


# -- Dataclasses -------------------------------------------------------------


@dataclass(frozen=True)
class OperatorState:
    """Mirror of the `operator_control_state` singleton row."""

    state: str
    boot_dry_run: bool
    reason: str
    updated_by: str
    updated_at: str
    version: int = 0

    @property
    def processing_enabled(self) -> bool:
        return self.state == LIVE

    @property
    def transfer_locked(self) -> bool:
        return self.state == TRANSFER_LOCK

    def to_public_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "bootDryRun": self.boot_dry_run,
            "reason": self.reason,
            "updatedBy": self.updated_by,
            "updatedAt": self.updated_at,
            "processingEnabled": self.processing_enabled,
            "transferLocked": self.transfer_locked,
            "version": self.version,
        }


@dataclass(frozen=True)
class WalletModeState:
    """One ``wallet_mode_state`` row.

    ``version`` is the optimistic-concurrency token: a write reads it, updates
    WHERE version=?, and bumps it on success. An UPDATE that touches no rows
    means another writer got there first; the caller raises
    ``ConcurrentMutationError`` and the dashboard refetches.
    """

    wallet_id: str
    mode: str
    updated_by: str
    updated_at: str
    version: int = 0


# -- Schema verification cache ---------------------------------------------

_verified_conns: weakref.WeakSet[sqlite3.Connection] = weakref.WeakSet()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Check once per connection that the tables exist, so a caller cannot
    silently do nothing when the migration did not run. schema.sql and
    ``_migrate_db()`` define the tables.
    """
    if conn in _verified_conns:
        return
    required = (
        "wallet_mode_state",
        "operator_control_state",
        "operator_control_events",
    )
    for table in required:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None:
            raise OperatorControlError(
                f"operator_control table {table!r} is missing; run init_db "
                "(cryptobot.db.connection.init_db) to apply schema.sql"
            )
    try:
        _verified_conns.add(conn)
    except TypeError:
        # Mock connections in tests may not support weak references; repeating
        # the check for them is harmless.
        pass


# -- Audit event writer ------------------------------------------------------


def _write_event(
    conn: sqlite3.Connection,
    *,
    scope: str,
    action: str,
    from_state: Optional[str],
    to_state: Optional[str],
    actor: str,
    reason: str,
    detail: Optional[dict[str, object]] = None,
) -> None:
    """Insert one row into ``operator_control_events``.

    Global and per-wallet changes share the table and are told apart by
    ``scope`` (``'global'`` or ``'wallet:<id>'``).
    """
    conn.execute(
        """
        INSERT INTO operator_control_events
        (scope, action, from_state, to_state, actor, reason, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scope,
            action,
            from_state,
            to_state,
            actor,
            reason,
            json.dumps(detail or {}, sort_keys=True) if detail else None,
        ),
    )


# -- Global operator state ---------------------------------------------------


def seed_operator_control_state(
    conn: sqlite3.Connection,
    *,
    boot_dry_run: bool = True,
) -> OperatorState:
    """INSERT OR IGNORE the singleton row.

    main.py calls this after ``init_db()``; later boots find it already there.
    ``boot_dry_run`` records the first boot's setting.
    """
    _ensure_schema(conn)
    conn.execute(
        """
        INSERT OR IGNORE INTO operator_control_state
        (id, state, boot_dry_run, reason, updated_by, updated_at, version)
        VALUES (1, 'STOPPED_PROCESSING', ?, '', 'boot', ?, 0)
        """,
        (1 if boot_dry_run else 0, utc_now_sql()),
    )
    conn.commit()
    return get_operator_state(conn)


def get_operator_state(conn: sqlite3.Connection) -> OperatorState:
    _ensure_schema(conn)
    row = conn.execute(
        "SELECT state, boot_dry_run, reason, updated_by, updated_at, "
        "COALESCE(version, 0) AS version "
        "FROM operator_control_state WHERE id=1"
    ).fetchone()
    if row is None:
        raise OperatorControlError(
            "operator_control_state row missing — call seed_operator_control_state(conn)"
        )
    return OperatorState(
        state=row["state"],
        boot_dry_run=bool(row["boot_dry_run"]),
        reason=row["reason"] or "",
        updated_by=row["updated_by"] or "",
        updated_at=row["updated_at"] or "",
        version=int(row["version"] or 0),
    )


def set_operator_state(
    conn: sqlite3.Connection,
    new_state: str,
    *,
    actor: str = "dashboard",
    reason: str = "",
    expected_version: Optional[int] = None,
    detail: Optional[dict[str, object]] = None,
) -> OperatorState:
    """Update the singleton row with optimistic concurrency.

    With ``expected_version=None`` the current version is read and used (single
    writer). When given (e.g. by the dashboard POST handler), a mismatch raises
    ``ConcurrentMutationError`` and the caller refetches before retrying.
    """
    _ensure_schema(conn)
    if new_state not in ALLOWED_OPERATOR_STATES:
        raise OperatorControlError(f"unsupported operator state: {new_state!r}")

    current = get_operator_state(conn)
    if expected_version is not None and expected_version != current.version:
        raise ConcurrentMutationError(
            f"operator_control_state version mismatch "
            f"(expected={expected_version}, current={current.version})"
        )

    new_version = current.version + 1
    now = utc_now_sql()
    # The connection is in autocommit mode, so without an explicit transaction
    # the UPDATE would commit before _write_event runs, and a failure in
    # between would change live state with no audit row. BEGIN IMMEDIATE makes
    # the two atomic.
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            """
            UPDATE operator_control_state
            SET state=?, reason=?, updated_by=?, updated_at=?, version=?
            WHERE id=1 AND COALESCE(version, 0) = ?
            """,
            (new_state, reason, actor, now, new_version, current.version),
        )
        if int(cur.rowcount or 0) == 0:
            conn.rollback()
            raise ConcurrentMutationError(
                "concurrent state change detected on operator_control_state; retry"
            )

        _write_event(
            conn,
            scope="global",
            action=f"set_{new_state.lower()}",
            from_state=current.state,
            to_state=new_state,
            actor=actor,
            reason=reason,
            detail=detail,
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    return get_operator_state(conn)


def start_processing(
    conn: sqlite3.Connection,
    *,
    actor: str = "dashboard",
    reason: str = "",
    expected_version: Optional[int] = None,
) -> OperatorState:
    return set_operator_state(
        conn,
        LIVE,
        actor=actor,
        reason=reason or "operator start processing",
        expected_version=expected_version,
    )


def stop_processing(
    conn: sqlite3.Connection,
    *,
    actor: str = "dashboard",
    reason: str = "",
    expected_version: Optional[int] = None,
) -> OperatorState:
    return set_operator_state(
        conn,
        STOPPED_PROCESSING,
        actor=actor,
        reason=reason or "operator stop processing",
        expected_version=expected_version,
    )


def enter_transfer_lock(
    conn: sqlite3.Connection,
    *,
    actor: str = "dashboard",
    reason: str = "",
    expected_version: Optional[int] = None,
) -> OperatorState:
    return set_operator_state(
        conn,
        TRANSFER_LOCK,
        actor=actor,
        reason=reason or "operator transfer lock",
        expected_version=expected_version,
    )


def clear_transfer_lock(
    conn: sqlite3.Connection,
    *,
    actor: str = "dashboard",
    reason: str = "",
    expected_version: Optional[int] = None,
) -> OperatorState:
    return set_operator_state(
        conn,
        LIVE,
        actor=actor,
        reason=reason or "operator clear transfer lock",
        expected_version=expected_version,
    )


# -- Per-wallet mode ---------------------------------------------------------


def seed_wallet_modes(
    conn: sqlite3.Connection, portfolio: "V6Portfolio"
) -> dict[str, str]:
    """INSERT OR IGNORE one ``wallet_mode_state`` row per wallet in the loaded
    portfolio.

    Uses ``portfolio.wallet_default_modes[wallet_id]`` when present, else
    'DRY'. Existing rows are kept, so the operator's toggles survive a restart.
    Rows for wallets no longer in the portfolio are deleted so retired
    strategies stop showing up.

    Returns the resolved {wallet_id: mode} dict for the boot log.
    """
    _ensure_schema(conn)
    resolved: dict[str, str] = {}
    active_wallet_ids: list[str] = []
    now = utc_now_sql()
    for sleeve in portfolio.market_sleeves:
        wallet_id = getattr(sleeve, "label", None)
        if not isinstance(wallet_id, str):
            continue
        active_wallet_ids.append(wallet_id)
        mode = portfolio.default_mode_for(wallet_id)
        if mode not in ALLOWED_WALLET_MODES:
            mode = DRY
        conn.execute(
            """
            INSERT OR IGNORE INTO wallet_mode_state
            (wallet_id, mode, updated_by, updated_at, version)
            VALUES (?, ?, 'boot', ?, 0)
            """,
            (wallet_id, mode, now),
        )
        resolved[wallet_id] = mode

    if active_wallet_ids:
        placeholders = ",".join("?" for _ in active_wallet_ids)
        stale_rows = conn.execute(
            f"""
            SELECT wallet_id, mode
            FROM wallet_mode_state
            WHERE wallet_id NOT IN ({placeholders})
            ORDER BY wallet_id
            """,
            tuple(active_wallet_ids),
        ).fetchall()
        for row in stale_rows:
            _write_event(
                conn,
                scope=f"wallet:{row['wallet_id']}",
                action="prune_stale_wallet",
                from_state=row["mode"],
                to_state=None,
                actor="boot",
                reason="wallet not in loaded portfolio",
            )
        conn.execute(
            f"DELETE FROM wallet_mode_state WHERE wallet_id NOT IN ({placeholders})",
            tuple(active_wallet_ids),
        )
    else:
        stale_rows = conn.execute(
            "SELECT wallet_id, mode FROM wallet_mode_state ORDER BY wallet_id"
        ).fetchall()
        for row in stale_rows:
            _write_event(
                conn,
                scope=f"wallet:{row['wallet_id']}",
                action="prune_stale_wallet",
                from_state=row["mode"],
                to_state=None,
                actor="boot",
                reason="no wallets in loaded portfolio",
            )
        conn.execute("DELETE FROM wallet_mode_state")
    conn.commit()
    return resolved


def get_wallet_mode(
    conn: sqlite3.Connection, wallet_id: str
) -> WalletModeState:
    _ensure_schema(conn)
    row = conn.execute(
        "SELECT wallet_id, mode, updated_by, updated_at, "
        "COALESCE(version, 0) AS version "
        "FROM wallet_mode_state WHERE wallet_id=?",
        (wallet_id,),
    ).fetchone()
    if row is None:
        raise OperatorControlError(
            f"wallet_mode_state row missing for wallet_id={wallet_id!r}"
        )
    return WalletModeState(
        wallet_id=row["wallet_id"],
        mode=row["mode"],
        updated_by=row["updated_by"] or "",
        updated_at=row["updated_at"] or "",
        version=int(row["version"] or 0),
    )


def load_wallet_mode_state(conn: sqlite3.Connection) -> dict[str, str]:
    """Load every wallet_id -> mode into a dict.

    Called once per tick and passed to both phases of ``run_scanner_tick`` so
    the whole tick routes consistently. Missing wallets are not added here
    (``seed_wallet_modes`` does that); ``run_scanner_tick`` logs a WARN if a
    portfolio wallet has no entry.
    """
    _ensure_schema(conn)
    rows = conn.execute(
        "SELECT wallet_id, mode FROM wallet_mode_state"
    ).fetchall()
    return {row["wallet_id"]: row["mode"] for row in rows}


from cryptobot.execution._time_utils import (
    seconds_since as _seconds_since,
)


def set_wallet_mode(
    conn: sqlite3.Connection,
    wallet_id: str,
    target: str,
    *,
    actor: str = "dashboard",
    reason: str = "",
    expected_version: Optional[int] = None,
    force: bool = False,
    rate_limit_s: int = WALLET_MODE_FLIP_RATE_LIMIT_S,
    guard_live_action_context: Optional[
        Callable[[sqlite3.Connection, str], None]
    ] = None,
) -> WalletModeState:
    """Switch one wallet between DRY and LIVE.

      * ``target='DRY'``: always allowed, no cooldown or readiness check. This
        is the emergency stop.
      * ``target='LIVE'``: runs ``live_action_guard.require_wallet_can_go_live``
        through ``guard_live_action_context`` before saving (boot seeding and
        tests can pass None and a passing guard). A second flip within
        ``rate_limit_s`` raises ``RateLimitError`` unless ``force=True``.

    ``expected_version`` gives optimistic concurrency for multiple writers (the
    dashboard). With None the row's current version is used.

    ``guard_live_action_context`` is injected so the dashboard can pass its own
    prepared ``require_wallet_can_go_live`` closure and so this module can be
    tested without the readiness machinery; by default the module's guard is
    imported and called.
    """
    _ensure_schema(conn)
    target = (target or "").upper()
    if target not in ALLOWED_WALLET_MODES:
        raise OperatorControlError(
            f"wallet mode must be DRY or LIVE; got {target!r}"
        )

    current = get_wallet_mode(conn, wallet_id)
    if expected_version is not None and expected_version != current.version:
        raise ConcurrentMutationError(
            f"wallet_mode_state version mismatch for {wallet_id!r} "
            f"(expected={expected_version}, current={current.version})"
        )

    # Same-mode flip is a no-op: no event row, no version bump.
    if current.mode == target:
        return current

    # LIVE -> DRY is always allowed, and the first DRY -> LIVE after seeding
    # (version 0) skips the cooldown. Later DRY -> LIVE flips within
    # ``rate_limit_s`` raise RateLimitError unless ``force=True``.
    if (
        target == LIVE
        and not force
        and current.mode == DRY
        and current.version > 0
    ):
        elapsed = _seconds_since(current.updated_at)
        if elapsed is not None and elapsed < rate_limit_s:
            raise RateLimitError(
                f"DRY → LIVE for {wallet_id!r} cooldown {rate_limit_s}s "
                f"unmet (elapsed={elapsed:.1f}s); pass --force to override"
            )

    # Real-money guard: DRY -> LIVE must pass readiness and reconciliation.
    if target == LIVE:
        if guard_live_action_context is None:
            # Default: the module's own guard.
            from cryptobot.execution.live_action_guard import (
                require_wallet_can_go_live,
            )
            require_wallet_can_go_live(conn, wallet_id)
        else:
            guard_live_action_context(conn, wallet_id)

    new_version = current.version + 1
    now = utc_now_sql()
    cur = conn.execute(
        """
        UPDATE wallet_mode_state
        SET mode=?, updated_by=?, updated_at=?, version=?
        WHERE wallet_id=? AND COALESCE(version, 0) = ?
        """,
        (target, actor, now, new_version, wallet_id, current.version),
    )
    if int(cur.rowcount or 0) == 0:
        try:
            conn.rollback()
        except Exception:
            pass
        raise ConcurrentMutationError(
            f"concurrent state change detected on wallet_mode_state[{wallet_id!r}]; retry"
        )

    _write_event(
        conn,
        scope=f"wallet:{wallet_id}",
        action="mode_flip",
        from_state=current.mode,
        to_state=target,
        actor=actor,
        reason=reason,
        detail={"force": bool(force)} if force else None,
    )
    conn.commit()
    return get_wallet_mode(conn, wallet_id)


# -- Convenience boot helpers -----------------------------------------------


def boot_operator_state(
    conn: sqlite3.Connection,
    portfolio: "V6Portfolio",
    *,
    boot_dry_run: bool = True,
) -> tuple[OperatorState, dict[str, str]]:
    """Called once from main.py after ``load_sleeves()``.

    Returns (operator_state, wallet_mode_state_dict) for the boot log.
    """
    op_state = seed_operator_control_state(conn, boot_dry_run=boot_dry_run)
    seed_wallet_modes(conn, portfolio)
    return op_state, load_wallet_mode_state(conn)


# -- Stale-state watchdog ----------------------------------------------------
#
# Pure SQLite housekeeping over ``operator_control_state``; main.py runs it
# from an always-on watchdog job. The name is used by main.py and in the
# pipeline_health rows it writes.


def release_stale_transfer_lock(
    conn: sqlite3.Connection,
    *,
    max_age_minutes: int = 15,
    actor: str = "transfer_watchdog",
) -> Optional[str]:
    """If operator state has been TRANSFER_LOCK for longer than
    ``max_age_minutes``, move it to STOPPED_PROCESSING and return the
    reason; otherwise return None.

    STOPPED_PROCESSING rather than LIVE, so an operator investigates before
    trading resumes.
    """
    op_state = get_operator_state(conn)
    if op_state.state != TRANSFER_LOCK:
        return None
    age = _seconds_since(op_state.updated_at)
    if age is None or age < max_age_minutes * 60:
        return None
    stop_processing(conn, actor=actor, reason="transfer_lock_timeout")
    return "transfer_lock_timeout"


