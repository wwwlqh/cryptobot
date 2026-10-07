"""Capital and drawdown accounting for the live scanner.

The single source of truth for:

  - realized_capital (INITIAL_BANKROLL + net capital flows + net PnL on
    settled rows)
  - peak_realized_capital (high-water mark of that flow-inclusive series)
  - pending_exposure (USD on PENDING rows)
  - drawdown_ratio (denominator is the constant INITIAL_BANKROLL, not
    realized_capital, to match the backtest's drawdown shape)
  - per-sleeve pending breakdown for the dashboard

Capital flows: ``INITIAL_BANKROLL`` is the account balance at
``WALLET_START_AT``. Every later deposit, withdrawal or rebate is a
``capital_flows`` row that moves equity at its own timestamp. Folding a later
deposit into the seed would backdate the money and overstate the peak that the
drawdown halt reads. Flows belong to the whole account (a deposit cannot be
attributed to one sleeve), so the same series is merged into every wallet's
replay.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from cryptobot import config
from cryptobot.config import INITIAL_BANKROLL

logger = logging.getLogger(__name__)

# The four ``capital_flows.kind`` values, matching the schema CHECK. The sign
# of ``amount_usd`` is fixed per kind so the replay can just add it: money in
# is positive, money out negative, and 'adjustment' (either sign) is for manual
# corrections.
CAPITAL_FLOW_KINDS: tuple[str, ...] = (
    "deposit",
    "withdrawal",
    "rebate",
    "adjustment",
)
CAPITAL_FLOW_POSITIVE_KINDS: tuple[str, ...] = ("deposit", "rebate")
CAPITAL_FLOW_NEGATIVE_KINDS: tuple[str, ...] = ("withdrawal",)


# ---------------------------------------------------------------------------
# Per-connection cache of the ledger scan (hot decision path).
#
# ``get_wallet_snapshots`` reads the whole ledger, and it runs every tick. The
# rows only change when the ledger does, so they are cached per connection and
# reused until then. The aggregation (which depends on the clock for the
# rolling 24h fields) is recomputed on every call, so results are exactly what
# a fresh scan would return.
#
# The cache key is a probe of the ledger's contents (``ledger_version``), not a
# connection change counter: the tick's own signals inserts and other tables'
# writes change those counters every second and would invalidate the cache on
# every tick. The probe aggregates the ledger in one narrow query (MAX(id),
# COUNT(*), settled-row count, SUM(pnl_net), latest settle time). Writes to
# other tables never move it, while every ledger change that matters does:
#
#  * INSERT / DELETE: MAX(id) or COUNT(*) moves.
#  * settlement UPDATE: the settled count moves (PENDING -> WIN/LOSS/
#    CANCELLED), plus SUM(pnl_net) and MAX(resolution_ts). MAX(id) alone would
#    miss this, and the drawdown halt's peak replay depends on catching it
#    (test_capital covers it).
#  * stop-loss booking (historical rows): settled count, SUM(pnl_net) and
#    exit_ts move.
#
# Not covered: rewriting only bet_ts, which no production code does. The probe
# is read before the scan (see _cached_ledger_rows), so a concurrent write can
# only cause a later rescan, never a stale result.
# ---------------------------------------------------------------------------

# Keyed by the connection object itself (sqlite3.Connection cannot be weakly
# referenced), so rows are never served for a different database. Production
# uses a few long-lived connections; the cap only matters for tests that create
# many short-lived ones.
_SCAN_CACHE_MAX_CONNS = 64


@dataclass
class _LedgerScanEntry:
    signal: tuple[int, int, int, float, str]
    since_sql: Optional[str]
    rows: list[Any]


_SNAPSHOT_SCAN_CACHE: "dict[sqlite3.Connection, _LedgerScanEntry]" = {}
_SNAPSHOT_SCAN_LOCK = threading.Lock()


def clear_snapshot_scan_cache() -> None:
    """Drop every cached ledger scan. For tests and ops; the cache otherwise
    invalidates itself.
    """
    with _SNAPSHOT_SCAN_LOCK:
        _SNAPSHOT_SCAN_CACHE.clear()


def ledger_version(conn: sqlite3.Connection) -> tuple[int, int, int, float, str]:
    """Ledger-content version: one narrow aggregate query instead of the full
    fetch it guards. Any change that can alter the cached rows moves at
    least one component (see the note above); writes to other tables
    (signals, pipeline_health, dashboard state) move none.

    The snapshot builder's KPI cache uses it too, so both caches agree on when
    the ledger changed.
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS max_id, "
        "       COUNT(*) AS n, "
        # Settlement flips outcome away from PENDING in place, which MAX(id)
        # and COUNT(*) cannot see.
        "       COALESCE(SUM(CASE WHEN outcome != 'PENDING' THEN 1 ELSE 0 END), 0) AS settled_n, "
        # Catches a pnl correction that does not change the outcome.
        "       COALESCE(SUM(pnl_net), 0.0) AS pnl_sum, "
        # Catches a new settle time; exit_ts covers stop-loss rows whose
        # resolution_ts is NULL.
        "       COALESCE(MAX(COALESCE(resolution_ts, exit_ts)), '') AS max_settle_ts "
        "FROM ledger"
    ).fetchone()
    return (
        int(row["max_id"] or 0),
        int(row["n"] or 0),
        int(row["settled_n"] or 0),
        round(float(row["pnl_sum"] or 0.0), 6),
        str(row["max_settle_ts"] or ""),
    )


def _cached_ledger_rows(
    conn: sqlite3.Connection,
    since_sql: Optional[str],
    *,
    version: Optional[tuple] = None,
) -> list[Any]:
    """Return the ledger rows for wallet snapshots, from the per-connection
    cache when the ledger is unchanged. The version is read before the scan,
    so a concurrent write can only cause a later rescan, never a stale
    result. Pass ``version`` if you already read ledger_version() in this
    pass.
    """
    signal = ledger_version(conn) if version is None else version
    with _SNAPSHOT_SCAN_LOCK:
        entry = _SNAPSHOT_SCAN_CACHE.get(conn)
        if (
            entry is not None
            and entry.signal == signal
            and entry.since_sql == since_sql
        ):
            return entry.rows

    where = ""
    params: tuple[str, ...] = ()
    if since_sql is not None:
        where = "WHERE bet_ts >= ?"
        params = (since_sql,)
    rows = conn.execute(
        "SELECT id, bet_ts, sleeve, wallet_id, pair_component, outcome, pnl_net, "
        "       bet_size, resolution_ts, exit_ts, exit_kind, is_topup "
        f"FROM ledger {where} ORDER BY id ASC",
        params,
    ).fetchall()

    with _SNAPSHOT_SCAN_LOCK:
        if len(_SNAPSHOT_SCAN_CACHE) >= _SCAN_CACHE_MAX_CONNS and conn not in _SNAPSHOT_SCAN_CACHE:
            _SNAPSHOT_SCAN_CACHE.clear()
        _SNAPSHOT_SCAN_CACHE[conn] = _LedgerScanEntry(signal, since_sql, rows)
    return rows


@dataclass(frozen=True)
class WalletSnapshot:
    wallet_id: str
    label: str
    kind: str
    ledger_sleeves: tuple[str, ...]
    initial_bankroll: float
    realized_capital: float
    peak_realized_capital: float
    pending_exposure: float
    drawdown_ratio: float
    drawdown_pct_peak_frac: float
    pnl_total: float
    pnl_24h: float
    wins_24h: int
    settled_24h: int
    n_pending: int
    n_settled: int
    n_topups_24h: int
    # Live-trading fields. All have defaults so older callers and test fixtures
    # still work.
    mode: str = "DRY"
    wallet_group: Optional[str] = None
    group_cap_usd: Optional[float] = None
    group_pending_usd: Optional[float] = None
    live_wallet_balance: Optional[float] = None
    # Stop-loss exits booked in the last 24h (exit_kind='stop_loss' with a real
    # pnl_net; voided cancels excluded). Only historical rows have these;
    # win_rate_24h counts them as losses.
    stops_24h: int = 0
    # Net capital flows since the cutover, included in realized_capital and the
    # peak. Account-level, so every wallet reports the same value.
    net_flow_usd: float = 0.0

    @property
    def win_rate_24h(self) -> Optional[float]:
        """Win rate counting stop-loss exits as losses: ``wins / (settled_24h +
        stops_24h)``, where ``settled_24h`` is the WIN/LOSS count.
        """
        denom = self.settled_24h + self.stops_24h
        if denom <= 0:
            return None
        return self.wins_24h / denom


def _equity_change_sort_key(
    exit_kind: Any, exit_ts: Any, resolution_ts: Any
) -> str:
    """Sort key for the peak/drawdown equity replay.

    Realized capital does not depend on order, but the running peak (which the
    drawdown halt and brake read) does, so rows are ordered by when equity
    actually changed. For a normal WIN/LOSS/PUSH row that is ``resolution_ts``.
    For a stop-loss row (``exit_kind='stop_loss'``, ``outcome='CANCELLED'``) it
    is ``exit_ts``, the sell fill time, which is set once at booking and never
    changes; its ``resolution_ts`` may be NULL or may have been backfilled
    later for display. Using ``exit_ts`` gives the true time and keeps the peak
    the same whether or not the backfill ran.

    Timestamps come in two spellings: ISO (``2026-07-11T00:00:00Z``) in some
    ``exit_ts`` values and SQL text (``2026-07-11 00:00:00``) in
    ``resolution_ts``. A raw string compare would misorder them ('T' sorts
    after ' '), so both are normalized to the space form. This is the Python
    version of ``_equity_change_order_sql``.
    """
    raw = exit_ts if str(exit_kind or "") == "stop_loss" else resolution_ts
    return str(raw or "").replace("T", " ").replace("Z", "")


def _equity_change_order_sql(fallback: str) -> str:
    """SQL expression for the equity-change ordering key, shared by both SQL
    replays (``get_capital_snapshot`` here and the dashboard's
    ``_ledger_equity_series``) so the drawdown halt and the equity chart
    cannot drift apart.

    Orders a stop-loss row (``exit_kind='stop_loss'``) by its ``exit_ts`` and
    every other row by ``resolution_ts``, with both spellings normalized to the
    SQL form ('T' -> ' ', drop 'Z'). ``fallback`` is used when both are NULL
    and is the one intended difference between callers: ``''`` for the capital
    snapshot, ``bet_ts`` for the equity chart. Otherwise matches
    ``_equity_change_sort_key``.
    """
    return (
        "replace(replace(COALESCE("
        "CASE WHEN exit_kind = 'stop_loss' THEN exit_ts ELSE resolution_ts END, "
        f"{fallback}), 'T', ' '), 'Z', '')"
    )


def load_capital_flows(
    conn: sqlite3.Connection, since_sql: Optional[str]
) -> list[tuple[str, int, str, float]]:
    """Account-level capital flows as ``(occurred_at, id, kind, amount_usd)``,
    ready to merge into an equity replay.

    ``since_sql`` is the cutover (``WALLET_START_AT`` in SQL-text form).
    ``INITIAL_BANKROLL`` is the balance at that moment, so a flow dated before
    it is already in the seed and would be counted twice. Such rows are dropped
    here (with a debug log) rather than refused at write time, so the full
    funding history can be recorded without distorting current equity.

    Returns [] when the table is missing (an old schema), so every replay falls
    back to PnL only. Not cached: the table holds a handful of rows and the
    read is one index scan.
    """
    try:
        rows = conn.execute(
            "SELECT id, occurred_at, kind, amount_usd FROM capital_flows "
            "ORDER BY occurred_at ASC, id ASC"
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    out: list[tuple[str, int, str, float]] = []
    for row in rows:
        occurred_at = str(row["occurred_at"] or "").replace("T", " ").replace("Z", "")
        if since_sql is not None and occurred_at < since_sql:
            logger.debug(
                "capital_flow id=%s occurred_at=%s ignored: before the "
                "WALLET_START_AT cutover %s (already inside INITIAL_BANKROLL)",
                row["id"], occurred_at, since_sql,
            )
            continue
        out.append(
            (
                occurred_at,
                int(row["id"]),
                str(row["kind"] or "adjustment"),
                float(row["amount_usd"] or 0.0),
            )
        )
    return out


class CapitalFlowError(ValueError):
    """A ``capital_flows`` write was refused (bad kind, sign or timestamp, or a
    duplicate). ``reason`` is machine-readable, like TransferError, so
    callers can branch without matching strings.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def normalize_flow_ts(raw: str) -> str:
    """Validate a flow timestamp and normalize it to the ledger's SQL-text UTC
    form.

    Accepts the SQL-text and ISO spellings. Anything else is refused, as is a
    future time, which would sit past the end of the equity replay and do
    nothing until the clock caught up.
    """
    text = str(raw or "").strip()
    if text.endswith(" UTC"):
        text = text[:-4]
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CapitalFlowError(
            "bad_timestamp",
            f"occurred_at {raw!r} is not 'YYYY-MM-DD HH:MM:SS' UTC",
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if parsed > datetime.now(timezone.utc):
        raise CapitalFlowError(
            "future_timestamp",
            f"occurred_at {raw!r} is in the future",
        )
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def validate_flow_amount(kind: str, amount_usd: float) -> tuple[str, float]:
    """Check ``(kind, amount_usd)`` against the schema's sign rules.

    A wrong-signed amount is refused, never flipped: "withdrawal 50" could be a
    typo or a misunderstanding, and guessing wrong would move the halt metric
    by twice the amount.
    """
    kind_norm = str(kind or "").strip().lower()
    if kind_norm not in CAPITAL_FLOW_KINDS:
        raise CapitalFlowError(
            "bad_kind",
            f"kind must be one of {', '.join(CAPITAL_FLOW_KINDS)}; got {kind!r}",
        )
    try:
        amount = float(amount_usd)
    except (TypeError, ValueError) as exc:
        raise CapitalFlowError(
            "bad_amount", f"amount {amount_usd!r} is not a number"
        ) from exc
    if amount == 0.0:
        raise CapitalFlowError("bad_amount", "amount must be non-zero")
    if kind_norm in CAPITAL_FLOW_POSITIVE_KINDS and amount < 0:
        raise CapitalFlowError(
            "bad_sign", f"{kind_norm} amount must be POSITIVE; got {amount}"
        )
    if kind_norm in CAPITAL_FLOW_NEGATIVE_KINDS and amount > 0:
        raise CapitalFlowError(
            "bad_sign",
            f"{kind_norm} amount must be NEGATIVE (money leaving the "
            f"account); got {amount}",
        )
    return kind_norm, amount


def record_capital_flow(
    conn: sqlite3.Connection,
    *,
    kind: str,
    amount_usd: float,
    occurred_at: str,
    note: Optional[str] = None,
    source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    """Insert one validated ``capital_flows`` row and return it as a dict.

    Refuses a duplicate (same kind, amount and occurred_at) unless ``force``.
    Re-running the backfill's printed commands is the easy way to record a
    deposit twice, and a doubled deposit inflates the peak the drawdown halt
    reads.
    """
    kind_norm, amount = validate_flow_amount(kind, amount_usd)
    ts = normalize_flow_ts(occurred_at)

    if not force:
        dupe = conn.execute(
            "SELECT id FROM capital_flows "
            "WHERE kind = ? AND occurred_at = ? AND ABS(amount_usd - ?) < 1e-6",
            (kind_norm, ts, amount),
        ).fetchone()
        if dupe is not None:
            raise CapitalFlowError(
                "duplicate",
                f"a {kind_norm} of {amount} at {ts} is already recorded "
                f"(id={int(dupe['id'])}); pass --force to record it anyway",
            )

    cur = conn.execute(
        "INSERT INTO capital_flows (occurred_at, kind, amount_usd, note, source) "
        "VALUES (?, ?, ?, ?, ?)",
        (ts, kind_norm, amount, note or None, str(source or "manual")),
    )
    conn.commit()
    return {
        "id": int(cur.lastrowid),
        "occurred_at": ts,
        "kind": kind_norm,
        "amount_usd": amount,
        "note": note or None,
        "source": str(source or "manual"),
    }


def list_capital_flows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """All recorded flows, oldest first.

    Rows before ``WALLET_START_AT`` are listed for history but excluded from
    the equity replay. Returns [] when the table is missing.
    """
    try:
        rows = conn.execute(
            "SELECT id, occurred_at, kind, amount_usd, note, source, created_at "
            "FROM capital_flows ORDER BY occurred_at ASC, id ASC"
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        {
            "id": int(r["id"]),
            "occurred_at": r["occurred_at"],
            "kind": r["kind"],
            "amount_usd": float(r["amount_usd"] or 0.0),
            "note": r["note"],
            "source": r["source"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def _replay_equity(
    settled_rows: list[tuple[str, int, float]],
    flows: list[tuple[str, int, str, float]],
) -> tuple[float, float, float]:
    """Replay the equity series and return ``(peak, pnl_peak, net_flow)``.

    Every caller (the per-wallet replay, the shared-equity pass and
    ``get_capital_snapshot``) uses this one merge, so they always agree on the
    peak.

    One ordered pass yields two high-water marks:

      * ``peak``: the flow-inclusive series (seed + pnl + flows). This is what
        ``drawdown_pct_peak_frac`` (the drawdown halt and brake) reads. A deposit
        raises equity at its timestamp without counting as profit; a withdrawal
        resets the peak to the remaining balance, so moving cash out cannot
        trigger a halt. Later trading losses are measured from the new peak.
      * ``pnl_peak``: the PnL-only series (seed + pnl), ignoring flows, for the
        backtest-comparable ``drawdown_ratio``.

    Both start at ``INITIAL_BANKROLL``, so with no flows they are identical.

    Ordering: rows sort by the equity-change key, and at an identical key a
    settlement sorts before a flow (rank 0 vs 1). Flow timestamps are entered
    by hand and coarse, while settlement times are exact.
    """
    events = [(ts, 0, row_id, pnl, None) for ts, row_id, pnl in settled_rows]
    events.extend(
        (ts, 1, flow_id, amount, kind)
        for ts, flow_id, kind, amount in flows
    )
    events.sort(key=lambda event: event[:3])

    running = peak = pnl_running = pnl_peak = INITIAL_BANKROLL
    net_flow = 0.0
    for _ts, rank, _event_id, delta, flow_kind in events:
        running += delta
        if running > peak:
            peak = running
        if rank == 0:
            pnl_running += delta
            if pnl_running > pnl_peak:
                pnl_peak = pnl_running
        else:
            net_flow += delta
            if flow_kind == "withdrawal":
                # A withdrawal changes the funded balance, not trading equity.
                # Start a fresh high-water mark at the balance that remains.
                peak = running
    return peak, pnl_peak, net_flow


def _compute_drawdown_metrics(
    peak: float,
    realized: float,
    *,
    pnl_peak: Optional[float] = None,
    pnl_realized: Optional[float] = None,
) -> tuple[float, float]:
    """Return (drawdown_ratio, drawdown_pct_peak_frac) for a peak/realized
    pair.

    ``drawdown_ratio`` divides by the constant INITIAL_BANKROLL, not
    realized_capital, to match the backtest's drawdown shape.

    The two metrics deliberately differ on capital flows.
    ``drawdown_pct_peak_frac``, which the drawdown halt and brake read, uses
    the flow-inclusive ``peak``/``realized`` pair, because risk is measured
    against the money actually in the account. ``drawdown_ratio`` uses the
    PnL-only ``pnl_peak``/``pnl_realized`` pair over the constant seed, so
    deposits never change it. Both default to the flow-inclusive pair, which is
    the same thing when there are no flows.
    """
    ratio_peak = peak if pnl_peak is None else pnl_peak
    ratio_realized = realized if pnl_realized is None else pnl_realized
    drawdown_ratio = (
        max(0.0, (ratio_peak - ratio_realized) / INITIAL_BANKROLL)
        if INITIAL_BANKROLL > 0 else 0.0
    )
    drawdown_pct_peak_frac = (
        max(0.0, (peak - realized) / peak) if peak > 0 else 0.0
    )
    return drawdown_ratio, drawdown_pct_peak_frac


@dataclass(frozen=True)
class GroupCapState:
    """Per-group live exposure state for the group exposure gate.

    ``pending_usd`` sums live PENDING ledger rows plus unresolved
    ``live_order_attempts`` (SUBMITTING/SUBMITTED with ``ledger_id IS NULL``)
    across all member wallets. DRY rows are excluded so a dry-run shadow can
    never block live entries.
    """

    group_id: str
    cap_usd: float
    pending_usd: float
    member_wallets: tuple[str, ...]


def wallet_id_for_sleeve(sleeve: str, pair_component: Optional[str] = None) -> str:
    """Map a ledger or signal label to its strategy wallet id.

    Older rows may lack ``wallet_id``, so every reader keeps this fallback.
    """
    if pair_component:
        return str(pair_component)
    value = str(sleeve)
    for suffix in ("_long", "_short"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _wallet_start_sql(raw: Optional[str] = None) -> Optional[str]:
    text = (config.WALLET_START_AT if raw is None else raw) or ""
    text = str(text).strip()
    if not text:
        return None
    if text.endswith(" UTC"):
        text = text[:-4]
    try:
        iso = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        # SQLite datetime comparisons expect this form; input already in
        # SQL-text form is used as is.
        return text.replace("T", " ")[:19]


def wallet_specs_for_portfolio(portfolio: Any) -> list[dict[str, Any]]:
    """One wallet spec per market sleeve.

    ``ledger_sleeves`` is a tuple holding just the wallet's own label; callers
    iterate it.
    """
    specs: list[dict[str, Any]] = []
    for component in getattr(portfolio, "market_sleeves", ()):
        label = str(component.label)
        specs.append({
            "wallet_id": label,
            "label": label,
            "kind": "v6",
            "ledger_sleeves": (label,),
            "component": component,
        })
    return specs


def get_wallet_snapshots(
    conn: sqlite3.Connection,
    portfolio: Any,
    *,
    since_ts: Optional[str] = None,
    mode_state: Optional[dict[str, str]] = None,
    group_state: Optional[dict[str, "GroupCapState"]] = None,
    live_wallet_balance: Optional[float] = None,
    version: Optional[tuple] = None,
) -> dict[str, WalletSnapshot]:
    """Compute isolated capital state for each strategy wallet.

    Optional keyword arguments carry per-wallet mode and group exposure into
    the snapshots so the pipeline routes consistently within a tick:

    - ``mode_state``: ``{wallet_id: 'LIVE'|'DRY'}`` from ``wallet_mode_state``.
      Missing wallets default to 'DRY'.
    - ``group_state``: ``{group_id: GroupCapState}`` from ``get_group_state``,
      so each snapshot carries the group cap and pending amount the gate needs.
    - ``live_wallet_balance``: latest CLOB funder balance; it shrinks the
      effective cap.
    - ``version``: a ledger_version() the caller already read this pass (the
      snapshot builder shares one across its caches). If omitted, the scan reads
      its own, still before scanning.
    """
    specs = wallet_specs_for_portfolio(portfolio)
    state: dict[str, dict[str, Any]] = {
        spec["wallet_id"]: {
            "spec": spec,
            "pnl_total": 0.0,
            "pending_exposure": 0.0,
            "n_pending": 0,
            "n_settled": 0,
            "pnl_24h": 0.0,
            "wins_24h": 0,
            "settled_24h": 0,
            "stops_24h": 0,
            "n_topups_24h": 0,
            "settled_rows": [],
        }
        for spec in specs
    }
    since_sql = _wallet_start_sql(since_ts)
    one_day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    rows = _cached_ledger_rows(conn, since_sql, version=version)
    # Account-level cash events, using the same cutover as the ledger scan (the
    # seed is the balance at that moment). Loaded once and merged into every
    # wallet's replay: a deposit funds the whole account, not one sleeve.
    flows = load_capital_flows(conn, since_sql)

    for row in rows:
        wallet_id = row["wallet_id"] or wallet_id_for_sleeve(
            row["sleeve"], row["pair_component"]
        )
        if wallet_id not in state:
            continue
        bucket = state[wallet_id]
        outcome = str(row["outcome"])
        bet_ts = str(row["bet_ts"])
        pnl_raw = row["pnl_net"]
        if outcome == "PENDING":
            bucket["pending_exposure"] += float(row["bet_size"] or 0.0)
            bucket["n_pending"] += 1
        elif pnl_raw is not None:
            pnl = float(pnl_raw or 0.0)
            bucket["pnl_total"] += pnl
            bucket["n_settled"] += 1
            bucket["settled_rows"].append((
                _equity_change_sort_key(
                    row["exit_kind"], row["exit_ts"], row["resolution_ts"]
                ),
                int(row["id"]),
                pnl,
            ))
            if bet_ts >= one_day_ago:
                bucket["pnl_24h"] += pnl
                if outcome in ("WIN", "LOSS"):
                    bucket["settled_24h"] += 1
                    if outcome == "WIN":
                        bucket["wins_24h"] += 1
                elif str(row["exit_kind"] or "") == "stop_loss":
                    # Stop-loss exit (CANCELLED with a real pnl_net; this
                    # branch already requires pnl_raw IS NOT NULL, so voided
                    # cancels never land here). Counted separately so
                    # win_rate_24h can count it as a loss.
                    bucket["stops_24h"] += 1
        if int(row["is_topup"] or 0) == 1 and bet_ts >= one_day_ago:
            bucket["n_topups_24h"] += 1

    wallet_group_lookup = getattr(portfolio, "wallet_group_for", None)

    snapshots: dict[str, WalletSnapshot] = {}
    for wallet_id, bucket in state.items():
        spec = bucket["spec"]
        peak, pnl_peak, net_flow = _replay_equity(bucket["settled_rows"], flows)
        pnl_realized = INITIAL_BANKROLL + float(bucket["pnl_total"])
        realized = pnl_realized + net_flow
        drawdown_ratio, drawdown_pct_peak_frac = _compute_drawdown_metrics(
            peak, realized, pnl_peak=pnl_peak, pnl_realized=pnl_realized
        )
        # Mode and group come from optional arguments so callers that do not
        # supply them (tests, the dashboard snapshot builder) still work.
        mode = "DRY"
        if mode_state is not None:
            mode = str(mode_state.get(wallet_id, "DRY") or "DRY").upper()
        group_id: Optional[str] = None
        if callable(wallet_group_lookup):
            try:
                group_id = wallet_group_lookup(wallet_id)
            except Exception:  # noqa: BLE001 — defensive against fake portfolios
                group_id = None
        group_cap_usd: Optional[float] = None
        group_pending_usd: Optional[float] = None
        if group_state is not None and group_id is not None:
            gs = group_state.get(group_id)
            if gs is not None:
                group_cap_usd = float(gs.cap_usd)
                group_pending_usd = float(gs.pending_usd)

        snapshots[wallet_id] = WalletSnapshot(
            wallet_id=wallet_id,
            label=str(spec["label"]),
            kind=str(spec["kind"]),
            ledger_sleeves=tuple(spec["ledger_sleeves"]),
            initial_bankroll=INITIAL_BANKROLL,
            realized_capital=realized,
            peak_realized_capital=peak,
            pending_exposure=float(bucket["pending_exposure"]),
            drawdown_ratio=drawdown_ratio,
            drawdown_pct_peak_frac=drawdown_pct_peak_frac,
            pnl_total=float(bucket["pnl_total"]),
            pnl_24h=float(bucket["pnl_24h"]),
            wins_24h=int(bucket["wins_24h"]),
            settled_24h=int(bucket["settled_24h"]),
            stops_24h=int(bucket["stops_24h"]),
            n_pending=int(bucket["n_pending"]),
            n_settled=int(bucket["n_settled"]),
            n_topups_24h=int(bucket["n_topups_24h"]),
            net_flow_usd=net_flow,
            mode=mode,
            wallet_group=group_id,
            group_cap_usd=group_cap_usd,
            group_pending_usd=group_pending_usd,
            live_wallet_balance=live_wallet_balance,
        )

    # ---- Shared-equity pass ---- The sleeves in a ``shared_equity`` wallet
    # group compound on one equity, share one peak (for the drawdown halt) and
    # one exposure cap, as in the single-wallet backtest. Compute the combined
    # capital across members and override only the shared fields on each
    # member's snapshot. Isolated wallets and groups without ``shared_equity``
    # are left alone.
    groups = getattr(portfolio, "wallet_groups", ()) or ()
    for group in groups:
        if not getattr(group, "shared_equity", False):
            continue
        member_ids = [m for m in group.members if m in state]
        if not member_ids:
            continue

        # combined_pnl_total = sum of the members' pnl_total.
        combined_pnl_total = sum(
            float(state[m]["pnl_total"]) for m in member_ids
        )

        # combined_peak: merge every member's settled rows with the account's
        # capital flows, replay combined equity in equity-change order and take
        # the maximum. That is the peak of the combined curve, not the sum or
        # max of per-sleeve peaks. The drawdown halt reads this peak, which is
        # why the flows must be merged here and not only in the per-wallet
        # replay.
        merged_rows: list[tuple[str, int, float]] = []
        for m in member_ids:
            merged_rows.extend(state[m]["settled_rows"])
        combined_peak, combined_pnl_peak, combined_net_flow = _replay_equity(
            merged_rows, flows
        )
        # The shared wallet has one bankroll and one funding stream: add
        # INITIAL_BANKROLL and the flows once, not once per member.
        combined_pnl_realized = INITIAL_BANKROLL + combined_pnl_total
        combined_realized = combined_pnl_realized + combined_net_flow

        # combined_pending = sum of the members' pending_exposure.
        combined_pending = sum(
            float(state[m]["pending_exposure"]) for m in member_ids
        )

        combined_dd_ratio, combined_dd_peak_frac = _compute_drawdown_metrics(
            combined_peak,
            combined_realized,
            pnl_peak=combined_pnl_peak,
            pnl_realized=combined_pnl_realized,
        )

        # Override only the shared capital fields on each member. Per-member
        # pnl_total, pnl_24h, wins_24h, settled_24h, stops_24h, n_pending,
        # n_settled, n_topups_24h, mode, wallet_group, group_cap_usd,
        # group_pending_usd and live_wallet_balance stay as they are for the
        # dashboard.
        for m in member_ids:
            snapshots[m] = replace(
                snapshots[m],
                initial_bankroll=INITIAL_BANKROLL,
                realized_capital=combined_realized,
                peak_realized_capital=combined_peak,
                pending_exposure=combined_pending,
                drawdown_ratio=combined_dd_ratio,
                drawdown_pct_peak_frac=combined_dd_peak_frac,
                net_flow_usd=combined_net_flow,
            )

    return snapshots


def get_group_state(
    conn: sqlite3.Connection,
    portfolio: Any,
) -> dict[str, "GroupCapState"]:
    """Compute per-group live pending exposure.

    ``pending_usd`` adds up:

      1. live PENDING ledger rows for any wallet in the group;
      2. ``live_order_attempts`` in SUBMITTING/SUBMITTED with
         ``ledger_id IS NULL`` whose ``wallet_group`` is this group.

    DRY rows are excluded because a dry run uses no real money. Wallets outside
    any group get no row.
    """
    groups = getattr(portfolio, "wallet_groups", ()) or ()
    out: dict[str, GroupCapState] = {}
    if not groups:
        return out

    # 1. Live PENDING ledger exposure per wallet_id.
    ledger_rows = conn.execute(
        """
        SELECT wallet_id, COALESCE(SUM(bet_size), 0.0) AS pending
          FROM ledger
         WHERE outcome = 'PENDING' AND mode = 'LIVE'
        GROUP BY wallet_id
        """,
    ).fetchall()
    ledger_pending_by_wallet: dict[str, float] = {
        str(r["wallet_id"] or ""): float(r["pending"] or 0.0) for r in ledger_rows
    }

    # 2. Active live_order_attempts not yet linked to a ledger row count their
    # reserved amount. The active-slot unique index allows one row per (wallet,
    # coin, sleeve, em, side, window), so nothing is counted twice.
    try:
        attempt_rows = conn.execute(
            """
            SELECT wallet_group, COALESCE(SUM(amount_usd), 0.0) AS reserved
              FROM live_order_attempts
             WHERE status IN ('SUBMITTING', 'SUBMITTED')
               AND ledger_id IS NULL
               AND wallet_group IS NOT NULL
            GROUP BY wallet_group
            """,
        ).fetchall()
    except sqlite3.OperationalError:
        attempt_rows = []
    attempt_pending_by_group: dict[str, float] = {
        str(r["wallet_group"]): float(r["reserved"] or 0.0) for r in attempt_rows
    }

    for group in groups:
        members = tuple(group.members)
        ledger_total = sum(
            ledger_pending_by_wallet.get(member, 0.0) for member in members
        )
        attempt_total = attempt_pending_by_group.get(group.group_id, 0.0)
        out[group.group_id] = GroupCapState(
            group_id=group.group_id,
            cap_usd=float(group.cap_usd),
            pending_usd=float(ledger_total + attempt_total),
            member_wallets=members,
        )
    return out


def extractable_pnl_for_wallet(
    conn: sqlite3.Connection, wallet_id: str
) -> float:
    """Realized PnL a strategy wallet can transfer out.

      extractable = SUM(pnl_net for live settled rows on the wallet)
                  - SUM(amount_usd for in-flight or CONFIRMED transfers out)

    In-flight transfers (SUBMITTING, SUBMITTED) are subtracted along with
    CONFIRMED ones. Nothing promotes SUBMITTED to CONFIRMED, so counting only
    CONFIRMED would let an in-flight withdrawal be spent twice. If a transfer
    later FAILs it drops out of the sum and extractable rises again.

    "Settled" uses the same test as realized_capital in get_capital_snapshot:
    ``outcome != 'PENDING' AND pnl_net IS NOT NULL``. A CANCELLED row can carry
    a real pnl_net, so filtering on WIN/LOSS/PUSH would let extractable
    overstate what can be withdrawn.

    The transfer preview refuses any transfer that would make extractable
    negative. Withdrawals come from one source wallet, so winners can be
    extracted without freezing losers.

    For every wallet at once, use ``extractable_pnl_by_wallet`` (two queries in
    total instead of two per wallet).
    """
    pnl = conn.execute(
        """
        SELECT COALESCE(SUM(pnl_net), 0.0) AS realized
          FROM ledger
         WHERE mode = 'LIVE'
           AND wallet_id = ?
           AND outcome != 'PENDING'
           AND pnl_net IS NOT NULL
        """,
        (wallet_id,),
    ).fetchone()
    prior = conn.execute(
        """
        SELECT COALESCE(SUM(amount_usd), 0.0) AS prior
          FROM transfer_requests
         WHERE status IN ('CONFIRMED', 'SUBMITTED', 'SUBMITTING')
           AND source_wallet_id = ?
        """,
        (wallet_id,),
    ).fetchone()
    return float(pnl["realized"] or 0.0) - float(prior["prior"] or 0.0)


def extractable_pnl_by_wallet(conn: sqlite3.Connection) -> dict[str, float]:
    """Bulk variant: two queries in total instead of two per wallet. Returns
    ``{wallet_id: extractable}`` for every wallet with at least one live
    settled row or a CONFIRMED transfer.

    A wallet missing from the map has zero PnL and zero transfers; callers
    default missing keys to 0.0.
    """
    pnl_rows = conn.execute(
        """
        SELECT wallet_id, COALESCE(SUM(pnl_net), 0.0) AS realized
          FROM ledger
         WHERE mode = 'LIVE'
           AND wallet_id IS NOT NULL
           AND outcome != 'PENDING'
           AND pnl_net IS NOT NULL
         GROUP BY wallet_id
        """
    ).fetchall()
    prior_rows = conn.execute(
        """
        SELECT source_wallet_id, COALESCE(SUM(amount_usd), 0.0) AS prior
          FROM transfer_requests
         WHERE status IN ('CONFIRMED', 'SUBMITTED', 'SUBMITTING')
           AND source_wallet_id IS NOT NULL
         GROUP BY source_wallet_id
        """
    ).fetchall()
    out: dict[str, float] = {}
    for r in pnl_rows:
        out[r["wallet_id"]] = float(r["realized"] or 0.0)
    for r in prior_rows:
        wid = r["source_wallet_id"]
        out[wid] = out.get(wid, 0.0) - float(r["prior"] or 0.0)
    return out


