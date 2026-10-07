"""Live readiness check, run before any wallet goes live.

``evaluate_readiness(conn, config, order_client)`` runs the checks that gate
every DRY -> LIVE switch:

  1. ``_validate_wallet_config(sig, funder)`` passes.
  2. ``order_client.check_balance()`` >= the sum of group caps.
  3. The on-chain balance (when available) is within
     ``LIVE_BALANCE_TOLERANCE_USD`` of the CLOB balance.
  4. A recent ``wallet_reconciliation_runs`` row exists.
  5. No ``pipeline_health(stage='boot', status='CRITICAL')`` rows in the
     last hour.
  6. ``operator_control_state.state`` is LIVE or STOPPED_PROCESSING (not
     TRANSFER_LOCK).

``live_readiness_reports.report_json`` is built from an explicit field
whitelist, never ``Config.model_dump()``, which would write secrets into the
database.

``ReadinessReport.status`` is 'READY' only when every check passes.
"""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3

# Upper sanity bound (USD) for an RPC-derived funder balance. The LIVE
# funder for this bot holds well under this; a value above it indicates a
# wrong/corrupt/hostile RPC response, not a real balance.
_MAX_PLAUSIBLE_BALANCE: float = 1_000_000.0
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import LIVE, STOPPED_PROCESSING
from cryptobot.execution.strategy_constants import (
    LIVE_BALANCE_TOLERANCE_USD,
    WALLET_SNAPSHOT_FRESHNESS_TTL_S,
)
from cryptobot.execution.wallet_topology import _validate_wallet_config

# pUSD (Polymarket collateral) has 6 decimals.
_DEFAULT_ERC20_DECIMALS = 6

if TYPE_CHECKING:
    from cryptobot.runtime_secrets import Config
    from cryptobot.strategy.portfolio_loader import V6Portfolio


logger = logging.getLogger(__name__)

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

_RE_URL = re.compile(r"https?://[^\s'\"<>),]+")
_RE_SECRET_QUERY_VALUE = re.compile(
    r"(?i)\b([A-Za-z0-9_.-]*(?:key|token|secret|pass|auth)[A-Za-z0-9_.-]*=)"
    r"[^&\s'\"),;]+"
)
_RE_ADDR = re.compile(r"0x[0-9a-fA-F]{40,}")
_RE_HEX64 = re.compile(r"\b[0-9a-fA-F]{64}\b")
_RE_OPAQUE_TOKEN = re.compile(r"\b[A-Za-z0-9_-]{40,}\b")


def _redact_url(match: re.Match[str]) -> str:
    raw = match.group(0)
    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return "<URL_REDACTED>"
    netloc = parsed.hostname or parsed.netloc.rsplit("@", 1)[-1]
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is not None:
        netloc = f"{netloc}:{port}"
    path = ""
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        path = "/<REDACTED>"
    return urlunsplit((parsed.scheme, netloc, path, "", ""))


def redact_operator_text(text: str) -> str:
    """Remove URL secrets and opaque tokens from operator-visible text."""
    if not text:
        return text
    redacted = _RE_URL.sub(_redact_url, text)
    redacted = _RE_SECRET_QUERY_VALUE.sub(r"\1<REDACTED>", redacted)
    redacted = _RE_ADDR.sub("0x<REDACTED>", redacted)
    redacted = _RE_HEX64.sub("<HEX64_REDACTED>", redacted)
    redacted = _RE_OPAQUE_TOKEN.sub("<TOKEN_REDACTED>", redacted)
    return redacted


def _exception_detail(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {redact_operator_text(str(exc))}"


def is_address(value: str | None) -> bool:
    return bool(value and ADDRESS_RE.match(value.strip()))


def normalize_address(value: str | None) -> str:
    return (value or "").strip().lower()


def mask_address(value: str | None) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if len(value) <= 12:
        return value
    return f"{value[:6]}...{value[-4:]}"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReadinessCheck:
    name: str
    passed: bool
    detail: str = ""


@dataclass(frozen=True)
class ReadinessReport:
    """In-memory view of one `live_readiness_reports` row."""

    id: Optional[int]
    status: str  # 'READY' / 'NOT_READY' / 'ERROR'
    mode: Optional[str]
    funder: Optional[str]
    signer: Optional[str]
    clob_balance_usd: Optional[float]
    chain_balance_usd: Optional[float]
    checks: list[ReadinessCheck]
    report_payload: dict[str, Any] = field(default_factory=dict)

    @property
    def is_ready(self) -> bool:
        return self.status == "READY"


# ---------------------------------------------------------------------------
# Report field whitelist
# ---------------------------------------------------------------------------


_REPORT_FIELD_WHITELIST: tuple[str, ...] = (
    "funder_address",
    "signer_address",
    "clob_balance_usd",
    "chain_balance_usd",
    "clob_chain_delta_usd",
    "readiness_status",
    "readiness_check_results",
    "wallet_balance_freshness_s",
    "sample_timestamp",
)


def _build_report_payload(
    *,
    funder_address: str,
    signer_address: Optional[str],
    clob_balance_usd: Optional[float],
    chain_balance_usd: Optional[float],
    readiness_status: str,
    check_results: list[ReadinessCheck],
    wallet_balance_freshness_s: Optional[float],
    sample_timestamp: str,
) -> dict[str, Any]:
    """Build the ``report_json`` payload from the field whitelist.

    Never serializes Config itself: ``Config.model_dump(mode='json')`` would
    write POLY_PRIVATE_KEY and every other secret into a database file any
    local process can read.
    """
    clob_chain_delta: Optional[float] = None
    if (
        clob_balance_usd is not None
        and chain_balance_usd is not None
        and math.isfinite(clob_balance_usd)
        and math.isfinite(chain_balance_usd)
    ):
        clob_chain_delta = clob_balance_usd - chain_balance_usd

    payload = {
        "funder_address": funder_address,
        "signer_address": signer_address,
        "clob_balance_usd": clob_balance_usd,
        "chain_balance_usd": chain_balance_usd,
        "clob_chain_delta_usd": clob_chain_delta,
        "readiness_status": readiness_status,
        "readiness_check_results": [
            {"name": c.name, "passed": c.passed, "detail": c.detail}
            for c in check_results
        ],
        "wallet_balance_freshness_s": wallet_balance_freshness_s,
        "sample_timestamp": sample_timestamp,
    }
    # Defense-in-depth: assert no field outside the whitelist landed in
    # the payload.
    extra = set(payload.keys()) - set(_REPORT_FIELD_WHITELIST)
    if extra:
        raise RuntimeError(
            f"live_readiness report_json built with non-whitelisted fields: {extra}"
        )
    return payload


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _PrefetchedReadinessState:
    """Network results for ``evaluate_readiness``, fetched by the caller
    outside ``db_write_lock`` so the lock is not held during a CLOB call
    that can take up to 30s.
    """

    clob_balance: Optional[float]
    clob_unreachable_detail: Optional[str]
    chain_balance: Optional[float]
    chain_unreachable_detail: Optional[str] = None


def read_erc20_balance(
    *,
    rpc_url: str,
    token_address: str,
    wallet_address: str,
    decimals: int = _DEFAULT_ERC20_DECIMALS,
    timeout_s: float = 15.0,
) -> float:
    """Read an ERC-20 balance with raw JSON-RPC, avoiding a web3 dependency."""
    if not is_address(token_address):
        raise ValueError("token_address must be a 0x-prefixed 40-byte address")
    if not is_address(wallet_address):
        raise ValueError("wallet_address must be a 0x-prefixed 40-byte address")
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError("requests is not installed") from exc

    wallet_hex = wallet_address[2:].lower().rjust(64, "0")
    data = "0x70a08231" + wallet_hex
    response = requests.post(
        rpc_url,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": token_address, "data": data}, "latest"],
        },
        timeout=timeout_s,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(f"RPC error: {payload['error']!r}")
    raw = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected RPC balance response: {payload!r}")
    balance = int(raw, 16) / (10 ** decimals)
    # This value feeds the readiness drift check straight from a single RPC (by
    # default a public one), so bound it: an implausible balance is treated as
    # an error (reported as chain_unreachable). For live trading prefer a
    # dedicated authenticated RPC over the default polygon-rpc.com.
    if not math.isfinite(balance) or balance < 0 or balance > _MAX_PLAUSIBLE_BALANCE:
        raise RuntimeError(
            f"RPC balance out of plausible bounds: {balance!r}"
        )
    return balance


def build_venue_balance_fetcher(config: "Config") -> Callable[[], Optional[float]]:
    """On-chain balance fetcher for live readiness (Polygon pUSD of POLY_FUNDER)."""
    from cryptobot.venues import get_adapter

    return get_adapter().build_balance_fetcher(config)


def prefetch_readiness_state(
    *,
    order_client: Optional[object] = None,
    chain_balance_fetcher: Optional[Callable[[], Optional[float]]] = None,
) -> _PrefetchedReadinessState:
    """Run the network-bound part of `evaluate_readiness`. No DB access;
    safe to invoke without `db_write_lock`."""
    clob_balance: Optional[float] = None
    clob_unreachable_detail: Optional[str] = None
    if order_client is not None:
        try:
            clob_balance = order_client.check_balance()
        except Exception as exc:  # noqa: BLE001
            clob_unreachable_detail = f"clob_unreachable: {_exception_detail(exc)}"
    chain_balance, chain_unreachable_detail = _safe_call_fetcher(chain_balance_fetcher)
    return _PrefetchedReadinessState(
        clob_balance=clob_balance,
        clob_unreachable_detail=clob_unreachable_detail,
        chain_balance=chain_balance,
        chain_unreachable_detail=chain_unreachable_detail,
    )


def _secret_value(value: object) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        return str(getter() or "")
    return str(value or "")


def _derive_topology(config: "Config") -> object | None:
    """Derive the signer/deposit wallet pair for the wallet_topology_derived gate."""
    from cryptobot.venues import get_adapter

    return get_adapter().derive_wallet_topology(config)


def evaluate_readiness(
    conn: sqlite3.Connection,
    *,
    config: "Config",
    order_client: Optional[object] = None,
    portfolio: Optional["V6Portfolio"] = None,
    chain_balance_fetcher: Optional[Callable[[], Optional[float]]] = None,
    topology_provider: Optional[Callable[["Config"], object | None]] = None,
    balance_tolerance_usd: float = LIVE_BALANCE_TOLERANCE_USD,
    snapshot_freshness_ttl_s: int = WALLET_SNAPSHOT_FRESHNESS_TTL_S,
    prefetched: Optional[_PrefetchedReadinessState] = None,
) -> ReadinessReport:
    """Run every readiness check, store the result and return the report.

    The readiness job runs it every ``LIVE_READINESS_CACHE_TTL_S / 2``; the
    dashboard and ``live_action_guard.require_fresh_live_readiness`` read the
    latest row.

    With ``prefetched=None`` this makes the CLOB and chain calls itself. A
    caller holding ``db_write_lock`` should pass
    ``prefetched=prefetch_readiness_state(...)`` so the lock only covers the
    database work.
    """
    onchain_required = bool(getattr(config, "live_onchain_verify_enabled", True))
    if chain_balance_fetcher is None and onchain_required:
        chain_balance_fetcher = build_venue_balance_fetcher(config)

    if prefetched is None:
        prefetched = prefetch_readiness_state(
            order_client=order_client,
            chain_balance_fetcher=chain_balance_fetcher,
        )

    checks: list[ReadinessCheck] = []

    # ---- 1. Wallet topology -------------------------------------------
    funder_address = (config.poly_funder or "").strip()
    signature_type = int(getattr(config, "poly_signature_type", 0) or 0)
    try:
        _validate_wallet_config(signature_type, funder_address)
        checks.append(
            ReadinessCheck(
                name="wallet_topology",
                passed=True,
                detail=f"sig={signature_type} funder={mask_address(funder_address)}",
            )
        )
    except ValueError as exc:
        checks.append(
            ReadinessCheck(
                name="wallet_topology",
                passed=False,
                detail=redact_operator_text(str(exc)),
            )
        )

    require_deposit = bool(getattr(config, "live_require_deposit_wallet", True))
    if require_deposit:
        checks.append(
            ReadinessCheck(
                name="signature_type",
                passed=signature_type == 3,
                detail=(
                    "POLY_SIGNATURE_TYPE=3 deposit wallet path"
                    if signature_type == 3
                    else f"configured={signature_type}; expected=3"
                ),
            )
        )

    topology = None
    signer_address: Optional[str] = None
    if require_deposit and _secret_value(getattr(config, "poly_private_key", "")):
        try:
            provider = topology_provider or _derive_topology
            topology = provider(config)
            if topology is None:
                checks.append(
                    ReadinessCheck(
                        name="wallet_topology_derived",
                        passed=False,
                        detail="could_not_derive_wallet_topology",
                    )
                )
            else:
                signer_address = str(getattr(topology, "signer", "") or "")
                derived_deposit = str(getattr(topology, "deposit", "") or "")
                checks.append(
                    ReadinessCheck(
                        name="wallet_topology_derived",
                        passed=True,
                        detail=(
                            f"signer={mask_address(signer_address)} "
                            f"deposit={mask_address(derived_deposit)}"
                        ),
                    )
                )
                checks.append(
                    ReadinessCheck(
                        name="funder_matches_deposit",
                        passed=normalize_address(funder_address)
                        == normalize_address(derived_deposit),
                        detail=(
                            f"configured={mask_address(funder_address)} "
                            f"expected={mask_address(derived_deposit)}"
                        ),
                    )
                )
                relayer_owner = (
                    getattr(config, "relayer_api_key_address", "") or ""
                ).strip()
                checks.append(
                    ReadinessCheck(
                        name="relayer_owner",
                        passed=normalize_address(relayer_owner)
                        == normalize_address(signer_address),
                        detail=(
                            f"configured={mask_address(relayer_owner)} "
                            f"expected={mask_address(signer_address)}"
                        ),
                    )
                )
        except Exception as exc:  # noqa: BLE001
            checks.append(
                ReadinessCheck(
                    name="wallet_topology_derived",
                    passed=False,
                    detail=_exception_detail(exc),
                )
            )
    elif require_deposit:
        # Deposit-wallet verification is REQUIRED but no private key is
        # configured to derive the signer/deposit topology. Skipping the
        # block (the prior behavior) left `funder_matches_deposit` and
        # `relayer_owner` entirely unappended, so `all(c.passed)` was
        # satisfied vacuously and readiness could report READY without ever
        # verifying the deposit wallet. Fail closed instead.
        checks.append(
            ReadinessCheck(
                name="wallet_topology_derived",
                passed=False,
                detail="no_private_key_for_required_deposit_wallet",
            )
        )

    # ---- 2. CLOB balance (prefetched outside db_write_lock) ----------- The
    # prefetched result is the single source; the fallback only runs when no
    # order_client is wired, so a failing check_balance cannot add a second
    # row.
    clob_balance = prefetched.clob_balance
    required_balance = _required_balance(portfolio)
    if prefetched.clob_unreachable_detail is not None:
        checks.append(
            ReadinessCheck(
                name="clob_balance",
                passed=False,
                detail=prefetched.clob_unreachable_detail,
            )
        )
    elif clob_balance is None:
        checks.append(
            ReadinessCheck(
                name="clob_balance",
                passed=False,
                detail="clob_unreachable" if order_client is not None else "no_order_client",
            )
        )
    else:
        passed = clob_balance >= required_balance
        checks.append(
            ReadinessCheck(
                name="clob_balance",
                passed=passed,
                detail=(
                    f"balance={clob_balance:.4f} required={required_balance:.4f}"
                ),
            )
        )

    # ---- 3. Chain balance / drift (prefetched outside db_write_lock) -
    chain_balance = prefetched.chain_balance
    if onchain_required and chain_balance is None:
        checks.append(
            ReadinessCheck(
                name="chain_balance",
                passed=False,
                detail=(
                    prefetched.chain_unreachable_detail
                    or "chain_balance_missing"
                ),
            )
        )
    elif not onchain_required:
        checks.append(
            ReadinessCheck(
                name="chain_balance",
                passed=True,
                detail="on_chain_verification_disabled",
            )
        )
    elif chain_balance is not None:
        # Apply the same floor as the CLOB balance, so a drained on-chain
        # wallet fails here directly rather than only through the drift check.
        passed = chain_balance >= required_balance
        checks.append(
            ReadinessCheck(
                name="chain_balance",
                passed=passed,
                detail=(
                    f"balance={chain_balance:.4f} required={required_balance:.4f}"
                ),
            )
        )

    if (
        clob_balance is not None
        and chain_balance is not None
        and math.isfinite(clob_balance)
        and math.isfinite(chain_balance)
    ):
        delta = abs(clob_balance - chain_balance)
        passed = delta <= balance_tolerance_usd
        checks.append(
            ReadinessCheck(
                name="chain_clob_drift",
                passed=passed,
                detail=(
                    f"delta={delta:.4f} tolerance={balance_tolerance_usd:.4f}"
                ),
            )
        )

    # ---- 4. Wallet reconciliation freshness ---------------------------
    recon_age = _latest_recon_age_seconds(conn)
    if recon_age is None:
        checks.append(
            ReadinessCheck(
                name="wallet_reconciliation",
                passed=False,
                detail="no_recon_row",
            )
        )
    else:
        passed = recon_age <= snapshot_freshness_ttl_s
        checks.append(
            ReadinessCheck(
                name="wallet_reconciliation",
                passed=passed,
                detail=f"age_s={recon_age:.0f} ttl={snapshot_freshness_ttl_s}",
            )
        )

    # ---- 5. No recent CRITICAL boot row -------------------------------
    critical_count = _recent_critical_boot_count(conn)
    checks.append(
        ReadinessCheck(
            name="no_recent_boot_critical",
            passed=critical_count == 0,
            detail=f"count_1h={critical_count}",
        )
    )

    # ---- 6. Operator state not TRANSFER_LOCK --------------------------
    operator_state = _read_operator_state(conn)
    checks.append(
        ReadinessCheck(
            name="operator_state",
            passed=operator_state in {LIVE, STOPPED_PROCESSING},
            detail=f"state={operator_state}",
        )
        )

    # ---- Compose final status -----------------------------------------
    status = "READY" if all(c.passed for c in checks) else "NOT_READY"
    sampled_at = utc_now_sql()
    payload = _build_report_payload(
        funder_address=funder_address,
        signer_address=signer_address,
        clob_balance_usd=clob_balance,
        chain_balance_usd=chain_balance,
        readiness_status=status,
        check_results=checks,
        wallet_balance_freshness_s=recon_age,
        sample_timestamp=sampled_at,
    )

    cur = conn.execute(
        """
        INSERT INTO live_readiness_reports
          (created_at, status, mode, funder, signer, clob_balance_usd,
           chain_balance_usd, report_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sampled_at,
            status,
            None,
            funder_address,
            signer_address,
            clob_balance,
            chain_balance,
            json.dumps(payload, sort_keys=True),
        ),
    )
    conn.commit()

    return ReadinessReport(
        id=int(cur.lastrowid or 0) or None,
        status=status,
        mode=None,
        funder=funder_address,
        signer=signer_address,
        clob_balance_usd=clob_balance,
        chain_balance_usd=chain_balance,
        checks=checks,
        report_payload=payload,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_call_fetcher(
    fetcher: Optional[Callable[[], Optional[float]]],
) -> tuple[Optional[float], Optional[str]]:
    if fetcher is None:
        return None, None
    try:
        return fetcher(), None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "live_readiness: chain balance fetcher raised", exc_info=True
        )
        return None, _exception_detail(exc)


def _required_balance(portfolio: Optional["V6Portfolio"]) -> float:
    """Minimum CLOB balance for readiness.

    Grouped portfolios use the sum of group caps. Isolated wallets have no
    groups and use the portfolio's funder floor when present.
    """
    if portfolio is None:
        return 0.0
    try:
        floor_fn = getattr(portfolio, "funder_balance_floor_usd", None)
        if callable(floor_fn):
            return float(floor_fn())
        return float(portfolio.all_group_caps_sum_usd())
    except Exception:  # noqa: BLE001
        # Fail CLOSED. A portfolio that cannot compute its own floor must
        # not authorize a LIVE flip. Returning 0.0 here would make the
        # downstream `clob_balance >= required_balance` gate pass for ANY
        # non-negative balance (including a near-empty wallet) — defeating
        # the exact under-funding check this gate exists to enforce. Return
        # +inf so the CLOB-balance check fails until the floor is computable.
        logger.warning(
            "live_readiness: _required_balance computation raised; failing "
            "closed (required=inf)",
            exc_info=True,
        )
        return float("inf")


def _latest_recon_age_seconds(conn: sqlite3.Connection) -> Optional[float]:
    """Age in seconds of the latest ``wallet_reconciliation_runs`` row.

    Returns None when the table is empty (the reconciler has not run yet); the
    readiness check then fails closed.
    """
    row = conn.execute(
        """
        SELECT (julianday('now') - julianday(sampled_at)) * 86400 AS age_s
          FROM wallet_reconciliation_runs
         ORDER BY id DESC LIMIT 1
        """,
    ).fetchone()
    if row is None or row["age_s"] is None:
        return None
    try:
        return float(row["age_s"])
    except (TypeError, ValueError):
        return None


def _recent_critical_boot_count(
    conn: sqlite3.Connection, *, window_minutes: int = 60
) -> int:
    """Count CRITICAL pipeline_health rows from the last ``window_minutes``.

    The interval is passed as a bound parameter, like every other query here,
    and a non-int ``window_minutes`` is rejected before it reaches SQL.
    """
    if not isinstance(window_minutes, int) or isinstance(window_minutes, bool):
        raise TypeError(
            f"window_minutes must be int, got {type(window_minutes).__name__}"
        )
    if window_minutes < 0:
        raise ValueError(f"window_minutes must be >= 0, got {window_minutes}")
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM pipeline_health
         WHERE stage='boot' AND status='CRITICAL'
           AND created_at >= datetime('now', ?)
        """,
        (f"-{window_minutes} minutes",),
    ).fetchone()
    if row is None:
        return 0
    return int(row["n"] or 0)


def _read_operator_state(conn: sqlite3.Connection) -> str:
    """Return the singleton operator state, or 'UNKNOWN' when the row is
    missing (e.g., boot hasn't seeded yet)."""
    try:
        row = conn.execute(
            "SELECT state FROM operator_control_state WHERE id=1"
        ).fetchone()
    except sqlite3.OperationalError:
        return "UNKNOWN"
    if row is None:
        return "UNKNOWN"
    return str(row["state"] or "UNKNOWN")


# ---------------------------------------------------------------------------
# Read helpers — consumed by live_action_guard (and U11a CLIs)
# ---------------------------------------------------------------------------


def latest_readiness_report(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    """Return the latest live_readiness_reports row as a dict, or None when
    there is none; the guard treats None as 'no_readiness_report'.
    """
    row = conn.execute(
        """
        SELECT id, created_at, status, funder, clob_balance_usd,
               chain_balance_usd, report_json
          FROM live_readiness_reports
         ORDER BY id DESC LIMIT 1
        """,
    ).fetchone()
    if row is None:
        return None
    payload: dict[str, Any] = {
        "id": row["id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "funder": row["funder"],
        "clob_balance_usd": row["clob_balance_usd"],
        "chain_balance_usd": row["chain_balance_usd"],
    }
    try:
        payload["report_json"] = json.loads(row["report_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        payload["report_json"] = {}
    return payload
