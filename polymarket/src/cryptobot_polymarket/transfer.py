"""Per-strategy realized-PnL extraction via the Polymarket relayer.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U10 / D-LIVE-21:

  * `preview_transfer(amount_usd, source_wallet_id)` — persist a short-
    lived PREVIEW row carrying the idempotency key, return a
    `TransferPreview` for the dashboard to confirm. Refuses if the
    requested amount exceeds the source wallet's extractable PnL.
  * `submit_transfer(idempotency_key)` — bind the dashboard confirmation
    to the prior preview. Verifies `(amount, to_wallet, source_wallet_id)`
    match before transitioning to SUBMITTING + entering TRANSFER_LOCK.
    Mismatch → `preview_mismatch` error.
  * The `transfer_requests` partial unique index allows at most one
    SUBMITTING row per `from_wallet`; concurrent submits raise
    `IntegrityError` which the wrapper surfaces as `transfer_in_progress`.

The relayer client is injected — production wraps Polymarket's relayer
endpoint, tests pass a stub callable. The whole flow is idempotent;
re-running on a CONFIRMED key is a no-op (the unique index blocks).
"""

from __future__ import annotations

import json
import logging
import secrets
import sqlite3
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FuturesTimeoutError,
)
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.capital import (
    extractable_pnl_for_wallet,
)
from cryptobot.execution.operator_control import (
    LIVE,
    TRANSFER_LOCK,
    OperatorControlError,
    clear_transfer_lock,
    enter_transfer_lock,
    get_operator_state,
)


# Reliability finding #5: bound every relayer round-trip so a hung
# Polymarket relayer cannot freeze the PostRouter._lock (which serializes
# every dashboard mutation endpoint). The pool is shared so concurrent
# transfer/redemption calls don't each spawn a one-shot executor.
_RELAYER_EXECUTOR = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="cryptobot-relayer"
)
RELAYER_TIMEOUT_S = 30.0


def _call_relayer_with_timeout(
    relayer_submitter: Callable[[dict[str, Any]], dict[str, Any]],
    payload: dict[str, Any],
    *,
    timeout_s: Optional[float] = None,
) -> dict[str, Any]:
    # Resolve the timeout at call time so test monkeypatching of
    # `RELAYER_TIMEOUT_S` takes effect (defaulting at signature-time
    # captures the import-time value).
    effective_timeout = timeout_s if timeout_s is not None else RELAYER_TIMEOUT_S
    future = _RELAYER_EXECUTOR.submit(relayer_submitter, payload)
    try:
        return future.result(timeout=effective_timeout)
    except FuturesTimeoutError as exc:
        raise TransferError(
            "relayer_timeout",
            f"relayer call exceeded {effective_timeout}s",
        ) from exc


logger = logging.getLogger(__name__)


_PREVIEW_TTL_S: int = 300  # 5 minutes per D-LIVE-21


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TransferError(RuntimeError):
    """Base for transfer-flow errors with a stable `reason` token."""

    def __init__(self, reason: str, message: str | None = None) -> None:
        super().__init__(message or reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransferPreview:
    """Returned by `preview_transfer`; consumed by the dashboard's
    confirm step + `submit_transfer`."""

    preview_id: int
    idempotency_key: str
    from_wallet: str
    to_wallet: str
    source_wallet_id: Optional[str]
    amount_usd: float
    current_balance_usd: Optional[float]
    post_balance_usd: Optional[float]
    extractable_pnl_usd: float
    est_gas_usd: float = 0.0
    expires_at: str = ""

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "previewId": self.preview_id,
            "idempotencyKey": self.idempotency_key,
            "fromWallet": self.from_wallet,
            "toWallet": self.to_wallet,
            "sourceWalletId": self.source_wallet_id,
            "amountUsd": self.amount_usd,
            "currentBalanceUsd": self.current_balance_usd,
            "postBalanceUsd": self.post_balance_usd,
            "extractablePnlUsd": self.extractable_pnl_usd,
            "estGasUsd": self.est_gas_usd,
            "expiresAt": self.expires_at,
        }


@dataclass(frozen=True)
class TransferResult:
    """Outcome of `submit_transfer`."""

    request_id: int
    status: str  # 'CONFIRMED' / 'SUBMITTED' / 'FAILED'
    tx_hash: Optional[str] = None
    relayer_tx_id: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Transfer flow
# ---------------------------------------------------------------------------


def make_idempotency_key() -> str:
    """32-char hex token — generated client-side and echoed in the submit."""
    return secrets.token_hex(16)


def preview_transfer(
    conn: sqlite3.Connection,
    *,
    from_wallet: str,
    to_wallet: str,
    amount_usd: float,
    source_wallet_id: Optional[str] = None,
    actor: str = "dashboard",
    current_balance_usd: Optional[float] = None,
    ttl_s: int = _PREVIEW_TTL_S,
    to_wallet_allowlist: Optional[str] = None,
) -> TransferPreview:
    """Stage a transfer request; the dashboard surfaces the preview to the
    operator before any TRANSFER_LOCK fires.

    Per-strategy attribution: `source_wallet_id` is REQUIRED. The preview
    refuses if `amount_usd > extractable_pnl_for_wallet(source_wallet_id)`.
    The legacy whole-funder path (`source_wallet_id=None`) is disabled
    (audit P0 #1): a CONFIRMED NULL-source transfer was never subtracted
    from extractable PnL, so the same realized profit could be withdrawn
    repeatedly. Callers must name the wallet the withdrawal debits.

    Security finding #2 / #23: `to_wallet_allowlist`, when set, restricts
    the destination address to a single operator-configured value
    (`POLY_RETURN_WALLET`). The check fires BEFORE any DB write so a
    rogue POST cannot pollute `transfer_requests` with an arbitrary
    destination. When `to_wallet_allowlist` is None / empty, every
    preview is rejected with `to_wallet_not_configured` -- transfers
    are opt-in via the env var.

    No state machine changes here — the row is `status='PREVIEW'`. Submit
    transitions it to `SUBMITTING`.
    """
    if not (isinstance(amount_usd, (int, float)) and amount_usd > 0):
        raise TransferError("invalid_amount", f"amount_usd must be > 0, got {amount_usd}")
    if not from_wallet or not to_wallet:
        raise TransferError(
            "invalid_addresses", "from_wallet and to_wallet are required"
        )
    if not to_wallet_allowlist:
        raise TransferError(
            "to_wallet_not_configured",
            "POLY_RETURN_WALLET is unset; transfers are disabled until "
            "the operator configures the destination address",
        )
    if to_wallet.strip().lower() != to_wallet_allowlist.strip().lower():
        raise TransferError(
            "to_wallet_not_allowed",
            f"to_wallet={to_wallet!r} does not match POLY_RETURN_WALLET; "
            "configured allowlist is the only permitted destination",
        )
    if not source_wallet_id:
        raise TransferError(
            "source_wallet_id_required",
            "whole-funder (NULL source_wallet_id) transfers are disabled "
            "(audit P0 #1): specify the source wallet so the withdrawal is "
            "debited against that wallet's extractable PnL. Repeated "
            "NULL-source withdrawals of the same realized profit were "
            "possible because CONFIRMED NULL-source transfers were never "
            "subtracted from the extractable total.",
        )

    extractable = _resolve_extractable(
        conn, source_wallet_id=source_wallet_id
    )
    if amount_usd > extractable + 1e-9:
        raise TransferError(
            "insufficient_extractable_pnl",
            f"amount_usd={amount_usd:.4f} exceeds extractable_pnl={extractable:.4f} "
            f"for source_wallet_id={source_wallet_id!r}",
        )

    # Correctness finding: when the caller supplies the real on-chain pUSD
    # balance, refuse a preview the deposit wallet cannot actually cover. The
    # extractable-PnL gate above is realized-accounting and can diverge from
    # the on-chain balance (unredeemed wins, gas, prior manual moves); without
    # this the operator UI would show a positive postBalance for a transfer
    # the chain will reject only after the TRANSFER_LOCK + relayer round-trip.
    if (
        current_balance_usd is not None
        and amount_usd > current_balance_usd + 1e-9
    ):
        raise TransferError(
            "insufficient_onchain_balance",
            f"amount_usd={amount_usd:.4f} exceeds current on-chain "
            f"balance={current_balance_usd:.4f} for the deposit wallet",
        )

    post_balance = (
        current_balance_usd - amount_usd if current_balance_usd is not None else None
    )

    idempotency_key = make_idempotency_key()
    expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=ttl_s)
    ).strftime("%Y-%m-%d %H:%M:%S")
    preview_payload = {
        "from_wallet": from_wallet,
        "to_wallet": to_wallet,
        "source_wallet_id": source_wallet_id,
        "amount_usd": float(amount_usd),
        "current_balance_usd": current_balance_usd,
        "post_balance_usd": post_balance,
        "extractable_pnl_usd": extractable,
        "expires_at": expires_at,
    }

    cur = conn.execute(
        """
        INSERT INTO transfer_requests
          (actor, from_wallet, to_wallet, source_wallet_id, amount_usd,
           status, idempotency_key, preview_json)
        VALUES (?, ?, ?, ?, ?, 'PREVIEW', ?, ?)
        """,
        (
            actor,
            from_wallet,
            to_wallet,
            source_wallet_id,
            float(amount_usd),
            idempotency_key,
            json.dumps(preview_payload, sort_keys=True),
        ),
    )
    conn.commit()
    preview_id = int(cur.lastrowid or 0)
    if preview_id == 0:
        raise TransferError(
            "preview_insert_failed", "transfer_requests INSERT did not produce a rowid"
        )

    return TransferPreview(
        preview_id=preview_id,
        idempotency_key=idempotency_key,
        from_wallet=from_wallet,
        to_wallet=to_wallet,
        source_wallet_id=source_wallet_id,
        amount_usd=float(amount_usd),
        current_balance_usd=current_balance_usd,
        post_balance_usd=post_balance,
        extractable_pnl_usd=extractable,
        expires_at=expires_at,
    )


def submit_transfer(
    conn: sqlite3.Connection,
    *,
    idempotency_key: str,
    relayer_submitter: Callable[[dict[str, Any]], dict[str, Any]],
    actor: str = "dashboard",
    to_wallet_allowlist: Optional[str] = None,
) -> TransferResult:
    """Bind the dashboard confirmation to a prior `preview_transfer` row
    and submit through the relayer.

    Steps:
      1. Fetch the PREVIEW row by idempotency_key; refuse if expired
         (`preview_expired`) or already promoted (`preview_consumed`).
      2. Refuse if operator state != LIVE.
      3. Transition operator state → TRANSFER_LOCK (D-LIVE-21).
      4. UPDATE the row to status='SUBMITTING'; the partial unique index
         enforces single-SUBMITTING per from_wallet — concurrent submits
         raise `transfer_in_progress`.
      5. Call the relayer; on success → CONFIRMED with tx_hash, on
         failure → FAILED with error_kind.
      6. Restore operator state → LIVE (clear TRANSFER_LOCK).
    """
    if not idempotency_key:
        raise TransferError("missing_idempotency_key")

    row = conn.execute(
        """
        SELECT id, from_wallet, to_wallet, source_wallet_id, amount_usd,
               status, preview_json
          FROM transfer_requests
         WHERE idempotency_key = ?
         ORDER BY id DESC LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise TransferError("preview_not_found", f"idempotency_key={idempotency_key}")
    if row["status"] != "PREVIEW":
        raise TransferError(
            "preview_consumed",
            f"transfer {row['id']} already in status={row['status']}",
        )

    # Security finding #2 / #23 defense-in-depth: re-verify the PREVIEW
    # row's stored to_wallet against the current allowlist. Catches the
    # case where the allowlist was loosened after preview, or where a
    # preview row was inserted via a pre-allowlist code path.
    if not to_wallet_allowlist:
        raise TransferError(
            "to_wallet_not_configured",
            "POLY_RETURN_WALLET is unset; transfers are disabled",
        )
    if str(row["to_wallet"] or "").strip().lower() != to_wallet_allowlist.strip().lower():
        raise TransferError(
            "to_wallet_not_allowed",
            f"PREVIEW to_wallet={row['to_wallet']!r} does not match the "
            f"configured POLY_RETURN_WALLET allowlist",
        )

    try:
        preview_payload = json.loads(row["preview_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        preview_payload = {}
    expires_at = preview_payload.get("expires_at") or ""
    if expires_at and _is_expired(expires_at):
        conn.execute(
            "UPDATE transfer_requests SET status='FAILED', error='preview_expired', "
            "updated_at=? WHERE id=?",
            (utc_now_sql(), row["id"]),
        )
        conn.commit()
        raise TransferError("preview_expired", f"key={idempotency_key} past TTL")

    # Audit w19oxqf24 finding #13 (P2): re-validate extractable PnL at SUBMIT,
    # not just at PREVIEW. extractable_pnl_for_wallet subtracts only CONFIRMED
    # prior transfers and the SUBMITTING unique index only blocks CONCURRENT
    # submits, so an operator could stage two PREVIEWs each at the full
    # extractable amount and submit them SEQUENTIALLY: after the first CONFIRMs
    # and clears SUBMITTING, the second would pass every other gate and drain
    # past the wallet's realized PnL. Re-running the extractable check here
    # closes that gap (the just-CONFIRMED first transfer is now subtracted).
    amount_usd = float(row["amount_usd"] or 0.0)
    if not row["source_wallet_id"]:
        # Reject any pre-fix PREVIEW row with a NULL source (audit P0 #1).
        # New previews are blocked at preview_transfer, but a row staged
        # before this fix could still be submitted; fail it closed.
        conn.execute(
            "UPDATE transfer_requests SET status='FAILED', "
            "error='source_wallet_id_required', updated_at=? WHERE id=?",
            (utc_now_sql(), row["id"]),
        )
        conn.commit()
        raise TransferError(
            "source_wallet_id_required",
            f"transfer {row['id']} has no source_wallet_id; whole-funder "
            "transfers are disabled (audit P0 #1)",
        )
    extractable_now = _resolve_extractable(
        conn, source_wallet_id=row["source_wallet_id"]
    )
    if amount_usd > extractable_now + 1e-9:
        raise TransferError(
            "insufficient_extractable_pnl",
            f"amount_usd={amount_usd:.4f} exceeds current extractable_pnl="
            f"{extractable_now:.4f} for source_wallet_id="
            f"{row['source_wallet_id']!r} at submit time (a prior transfer may "
            f"have consumed it since preview)",
        )

    op_state = get_operator_state(conn)
    if op_state.state != LIVE:
        raise TransferError(
            "operator_not_live",
            f"operator state must be LIVE; got {op_state.state}",
        )

    try:
        enter_transfer_lock(conn, actor=actor, reason="transfer submit")
    except OperatorControlError as exc:
        raise TransferError("operator_state_changed", str(exc)) from exc

    try:
        cur = conn.execute(
            """
            UPDATE transfer_requests
               SET status='SUBMITTING', actor=?, updated_at=?
             WHERE id=? AND status='PREVIEW'
            """,
            (actor, utc_now_sql(), row["id"]),
        )
        if int(cur.rowcount or 0) == 0:
            # Correctness finding #25 (CORR-001): previously raised
            # `preview_consumed` WITHOUT releasing TRANSFER_LOCK, so a
            # racing submit between the row-status check above and this
            # UPDATE left the operator stuck in TRANSFER_LOCK until the
            # watchdog fired STOPPED_PROCESSING 15 minutes later. Release
            # the lock before propagating so a benign race resolves
            # cleanly.
            _restore_live(conn, actor=actor)
            raise TransferError(
                "preview_consumed",
                f"transfer {row['id']} no longer PREVIEW (race)",
            )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        # Single-SUBMITTING per from_wallet partial unique index violation.
        _restore_live(conn, actor=actor)
        raise TransferError("transfer_in_progress", str(exc)) from exc

    # ---- Relayer submission ------------------------------------------
    request_payload = {
        "from_wallet": row["from_wallet"],
        "to_wallet": row["to_wallet"],
        "source_wallet_id": row["source_wallet_id"],
        "amount_usd": float(row["amount_usd"]),
        "idempotency_key": idempotency_key,
    }
    try:
        response = _call_relayer_with_timeout(relayer_submitter, request_payload) or {}
    except TransferError as exc:
        if exc.reason == "relayer_timeout":
            # A timeout is NOT a definitive failure: the worker thread may
            # still complete the on-chain submission after our local timeout
            # expires. Keep the row SUBMITTING so the partial unique index
            # blocks another transfer from this funder.
            #
            # Audit P1 #7: do NOT restore LIVE here. The bot cannot know the
            # funder balance is stable while an in-flight transfer may still
            # execute, so resuming trading risks placing bets against a
            # balance about to drop by the transfer amount. Hold the operator
            # in TRANSFER_LOCK (the stale-lock watchdog escalates it to
            # STOPPED_PROCESSING) until an operator reconciles the in-flight
            # request by relayer_tx_id. Escalate so the stuck state is loud.
            conn.execute(
                """
                UPDATE transfer_requests
                   SET status='SUBMITTING', error=?, updated_at=?
                 WHERE id=?
                """,
                (exc.reason, utc_now_sql(), row["id"]),
            )
            conn.commit()
            _escalate_in_flight_timeout(conn, request_id=int(row["id"]))
            raise
        # Definitive failure (not a timeout): the transfer did not execute,
        # so funds did not move — release the lock and resume trading.
        conn.execute(
            """
            UPDATE transfer_requests
               SET status='FAILED', error=?, updated_at=?
             WHERE id=?
            """,
            (exc.reason, utc_now_sql(), row["id"]),
        )
        conn.commit()
        _restore_live(conn, actor=actor)
        raise
    except Exception as exc:  # noqa: BLE001
        # Leakage finding: this is the real-money pUSD withdrawal path. The
        # relayer/signing stack (eth-account / py_builder_relayer_client) can
        # echo the offending hex input — including the private key, API key, or
        # signed payload — inside ValueError/parse-error messages. Route the
        # exception text through the repo's secret-scrubber BEFORE it is
        # persisted to transfer_requests.error or surfaced to the dashboard/CLI
        # via TransferError.detail. (Local import keeps module-load ordering
        # simple; this is the rare error path.)
        from cryptobot.execution.live_readiness import redact_operator_text

        redacted = redact_operator_text(str(exc))
        conn.execute(
            """
            UPDATE transfer_requests
               SET status='FAILED', error=?, updated_at=?
             WHERE id=?
            """,
            (redacted, utc_now_sql(), row["id"]),
        )
        conn.commit()
        _restore_live(conn, actor=actor)
        raise TransferError("relayer_error", redacted) from exc

    tx_hash = response.get("tx_hash") or response.get("txHash")
    relayer_tx_id = (
        response.get("relayer_tx_id")
        or response.get("relayerTxId")
        or response.get("transactionID")
        or response.get("transactionId")
    )
    success = bool(response.get("success", True))

    if not success:
        error_text = str(response.get("error") or "relayer rejected")
        conn.execute(
            """
            UPDATE transfer_requests
               SET status='FAILED', error=?, updated_at=?
             WHERE id=?
            """,
            (error_text, utc_now_sql(), row["id"]),
        )
        conn.commit()
        _restore_live(conn, actor=actor)
        return TransferResult(
            request_id=int(row["id"]),
            status="FAILED",
            error=error_text,
        )

    if not tx_hash and relayer_tx_id:
        conn.execute(
            """
            UPDATE transfer_requests
               SET status='SUBMITTED', relayer_tx_id=?, updated_at=?
             WHERE id=?
            """,
            (relayer_tx_id, utc_now_sql(), row["id"]),
        )
        conn.commit()
        _restore_live(conn, actor=actor)
        return TransferResult(
            request_id=int(row["id"]),
            status="SUBMITTED",
            relayer_tx_id=relayer_tx_id,
        )

    if not tx_hash:
        error_text = str(response.get("error") or "relayer rejected")
        conn.execute(
            """
            UPDATE transfer_requests
               SET status='FAILED', error=?, updated_at=?
             WHERE id=?
            """,
            (error_text, utc_now_sql(), row["id"]),
        )
        conn.commit()
        _restore_live(conn, actor=actor)
        return TransferResult(
            request_id=int(row["id"]),
            status="FAILED",
            error=error_text,
        )

    conn.execute(
        """
        UPDATE transfer_requests
           SET status='CONFIRMED', tx_hash=?, relayer_tx_id=?, updated_at=?
         WHERE id=?
        """,
        (tx_hash, relayer_tx_id, utc_now_sql(), row["id"]),
    )
    conn.commit()
    _restore_live(conn, actor=actor)
    return TransferResult(
        request_id=int(row["id"]),
        status="CONFIRMED",
        tx_hash=tx_hash,
        relayer_tx_id=relayer_tx_id,
    )


# ---------------------------------------------------------------------------
# Watchdog — release stuck TRANSFER_LOCK
#
# MOVED TO CORE (panel verdict D4-1, Phase-2 follow-up):
# `release_stale_transfer_lock` now lives in
# `cryptobot.execution.operator_control`, next to the state machine it drives.
# It never touched Polymarket — it is pure SQLite hygiene over
# `operator_control_state`, a core-owned table — and main.py runs it from an
# ALWAYS-ON watchdog on every venue, so keeping it here made core's
# unconditional path depend on this package being installed. Nothing in this
# module used it; there is deliberately no re-export.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_extractable(
    conn: sqlite3.Connection, *, source_wallet_id: Optional[str]
) -> float:
    # Whole-funder (NULL source_wallet_id) transfers are DISABLED (audit
    # P0 #1). extractable_pnl_by_wallet subtracts prior withdrawals only
    # `WHERE source_wallet_id IS NOT NULL`, so a CONFIRMED NULL-source
    # transfer was never debited from the aggregate — the same realized
    # PnL could be previewed+submitted repeatedly and drained past the
    # wallet's actual profit. Every transfer must name its source wallet,
    # which extractable_pnl_for_wallet correctly debits per CONFIRMED
    # prior transfer. Fail closed here so no caller can reopen the hole.
    if not source_wallet_id:
        raise TransferError(
            "source_wallet_id_required",
            "whole-funder (NULL source_wallet_id) transfers are disabled "
            "(audit P0 #1)",
        )
    return extractable_pnl_for_wallet(conn, source_wallet_id)


def _escalate_in_flight_timeout(
    conn: sqlite3.Connection, *, request_id: int
) -> None:
    """A relayer timeout left a transfer in-flight with unknown on-chain
    state (audit P1 #7). The operator is intentionally NOT restored to LIVE;
    surface a CRITICAL pipeline_health row + notify so the held state is
    investigated and reconciled rather than silently sitting locked."""
    msg = (
        f"transfer {request_id} relayer_timeout: in-flight on-chain state "
        f"unknown; operator HELD in TRANSFER_LOCK (not restored to LIVE) "
        f"until reconciled by relayer_tx_id. Verify on-chain before any retry."
    )
    logger.critical("polymarket_transfer: %s", msg)
    try:
        conn.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('transfer', 'CRITICAL', ?)",
            (msg,),
        )
        conn.commit()
    except sqlite3.DatabaseError:
        logger.warning(
            "polymarket_transfer: failed to write CRITICAL health row for "
            "transfer timeout",
            exc_info=True,
        )
    try:
        from cryptobot.execution.notify import send_event

        send_event("transfer_in_flight_timeout", msg, critical=True)
    except Exception:  # noqa: BLE001 — notify dispatch is best-effort
        logger.warning(
            "polymarket_transfer: notify dispatch for transfer timeout failed",
            exc_info=True,
        )


def _restore_live(conn: sqlite3.Connection, *, actor: str) -> None:
    """Restore operator state from TRANSFER_LOCK back to LIVE after a
    relayer call completes.

    Reliability finding #6: previously the OperatorControlError catch
    only logged WARN. The common cause is the watchdog firing
    STOPPED_PROCESSING mid-relayer-call (raising ConcurrentMutationError
    on the post-relayer clear). When that happens the operator state is
    stuck STOPPED_PROCESSING with no Telegram alert — the bot fails
    silently. Now: write a CRITICAL pipeline_health row AND dispatch a
    CRITICAL notify via the shared notify path. Failures inside notify
    itself are swallowed so this function never raises (callers run it
    in finally-style sequences)."""
    try:
        clear_transfer_lock(
            conn, actor=actor, reason="transfer submit complete"
        )
    except OperatorControlError as exc:
        logger.warning(
            "polymarket_transfer: failed to restore LIVE after transfer submit",
            exc_info=True,
        )
        try:
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('transfer', 'CRITICAL', ?)",
                (
                    f"failed to restore LIVE after transfer "
                    f"({type(exc).__name__}: {exc}); operator state may be "
                    f"stuck — investigate operator_control_state",
                ),
            )
            conn.commit()
        except sqlite3.DatabaseError:
            pass
        try:
            from cryptobot.execution.notify import send_event

            send_event(
                "transfer_restore_failed",
                f"polymarket_transfer: clear_transfer_lock raised "
                f"{type(exc).__name__}; operator state may be stuck. Check "
                f"operator_control_state.",
                critical=True,
            )
        except Exception:  # noqa: BLE001 — notify dispatch is best-effort
            logger.warning("notify dispatch in _restore_live failed", exc_info=True)


from cryptobot.execution._time_utils import (
    parse_iso as _parse_iso,
)


def _is_expired(expires_at: str) -> bool:
    when = _parse_iso(expires_at)
    if when is None:
        return False
    return datetime.now(timezone.utc) > when
