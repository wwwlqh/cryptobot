"""Capital + drawdown accounting for the live scanner.

Mirrors `hightempbot/execution/capital.py` but adapted for cryptobot's
two-sleeve ledger. The single source of truth for:
  - realized_capital (= INITIAL_BANKROLL + net capital flows + cumulative net
    PnL on settled rows)
  - peak_realized_capital (high-water mark of the FLOW-INCLUSIVE series)
  - pending_exposure (USD on PENDING rows)
  - drawdown_ratio (per D14: denominator MUST be INITIAL_BANKROLL constant,
    NOT realized_capital — otherwise diverges from backtest's drawdown shape)
  - per-sleeve pending breakdown for the dashboard's StatStrip

CAPITAL FLOWS. ``INITIAL_BANKROLL`` is the account balance AT
``WALLET_START_AT``; every deposit / withdrawal / rebate after it is a
``capital_flows`` row that moves equity at its OWN timestamp instead of being
folded into the seed (folding a later deposit into the seed backdates the money
and overstates the peak, which is what the hard DD halt gates on). Flows are
account-level — the account is one funded pot and a deposit cannot be
attributed to a sleeve — so the same series is merged into EVERY wallet's
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

# The four `capital_flows.kind` values, mirroring the schema CHECK. The sign of
# `amount_usd` is pinned per kind so the replay can add it blindly: money in is
# positive, money out is negative, and 'adjustment' is the either-way escape
# hatch for a manual correction.
CAPITAL_FLOW_KINDS: tuple[str, ...] = (
    "deposit",
    "withdrawal",
    "rebate",
    "adjustment",
)
CAPITAL_FLOW_POSITIVE_KINDS: tuple[str, ...] = ("deposit", "rebate")
CAPITAL_FLOW_NEGATIVE_KINDS: tuple[str, ...] = ("withdrawal",)


# ---------------------------------------------------------------------------
# Per-tick ledger-scan cache (hot decision path).
#
# ``get_wallet_snapshots`` ran an unbounded ``SELECT ... FROM ledger ORDER BY
# id`` on EVERY tick (pipeline ~1160), and the scan cost grows with ledger
# size. The rows are identical between ledger writes, so we cache the fetched
# rows per connection and reuse them until the ledger changes — the aggregation
# (which depends on wall-clock for the rolling-24h fields) is recomputed fresh
# on every call, so the returned snapshots are EXACTLY what a fresh scan would
# produce.
#
# Invalidation is a LEDGER-CONTENT probe (``ledger_version``), not a
# connection-level change counter. The previous (data_version, total_changes)
# signal was safe but useless as a cache key at 1 Hz: the tick's own signals
# INSERT autocommits bump ``total_changes`` every tick, and worker commits bump
# ``data_version`` on the scheduler connection — so BOTH per-tick call sites
# (pipeline pre_score_wallets + snapshot _kpis_block) rescanned the full
# 12-column ledger every tick (~5-20ms each). The probe instead aggregates the
# ledger itself in one narrow SQL pass (MAX(id), COUNT(*), settled-row count,
# Σpnl_net, MAX settle-ts), so unrelated-table writes never invalidate while
# every ledger mutation that can change the cached rows does:
#   * INSERT / DELETE            → MAX(id) / COUNT(*) move.
#   * F3 sweep settlement UPDATE → the settled-row count moves (PENDING →
#     WIN/LOSS/CANCELLED) plus Σpnl_net / MAX(resolution_ts). This is the case
#     MAX(id) alone would miss — and the one the LIVE hard-DD-halt
#     (config.MAX_DRAWDOWN_HALT_FRAC) equity-peak replay depends on catching
#     (test_capital pins it).
#   * stop-loss booking          → settled count + Σpnl_net + exit_ts max.
# Deliberately NOT covered: a bet_ts-only rewrite (no production writer ever
# mutates bet_ts after insert; tests that rewrite it do so before the first
# snapshot read). The probe is read BEFORE the scan (see _cached_ledger_rows)
# so a concurrent write can only force a later rescan, never a stale serve.
# ---------------------------------------------------------------------------

# Keyed by the connection object itself (sqlite3.Connection is not weak-
# referenceable) so cached rows can never be served across a different DB with
# a coincidentally-equal id. Bounded: production uses a handful of long-lived
# connections; the cap only trips under a test's churn of throwaway connections.
_SCAN_CACHE_MAX_CONNS = 64


@dataclass
class _LedgerScanEntry:
    signal: tuple[int, int, int, float, str]
    since_sql: Optional[str]
    rows: list[Any]


_SNAPSHOT_SCAN_CACHE: "dict[sqlite3.Connection, _LedgerScanEntry]" = {}
_SNAPSHOT_SCAN_LOCK = threading.Lock()


def clear_snapshot_scan_cache() -> None:
    """Drop every cached ledger scan. Test/ops hook — the cache is otherwise
    self-invalidating via the connection change signal."""
    with _SNAPSHOT_SCAN_LOCK:
        _SNAPSHOT_SCAN_CACHE.clear()


def ledger_version(conn: sqlite3.Connection) -> tuple[int, int, int, float, str]:
    """Ledger-content version probe — one narrow aggregate SQL instead of the
    12-column full fetch it guards. Any mutation that can change the cached
    wallet-snapshot rows moves at least one component (see the invalidation
    note above); unrelated-table writes (the tick's per-second signals INSERT,
    pipeline_health, dashboard state) move NONE, which is the whole point.

    Also exported for the snapshot builder's KPI TTL cache so both caches
    invalidate off one definition of "the ledger changed".
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS max_id, "
        "       COUNT(*) AS n, "
        # Settled-row discriminator: the F3 sweep's settlement UPDATE flips
        # outcome away from PENDING in place (MAX(id)/COUNT(*) both blind).
        "       COALESCE(SUM(CASE WHEN outcome != 'PENDING' THEN 1 ELSE 0 END), 0) AS settled_n, "
        # Catches a pnl correction that does not flip outcome.
        "       COALESCE(SUM(pnl_net), 0.0) AS pnl_sum, "
        # Catches a settle-time (re)stamp; exit_ts covers booked-once stop rows
        # whose resolution_ts is NULL at booking.
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
    """Return the wallet-snapshot ledger scan, served from the per-connection
    cache when the ledger is unchanged. The change signal is read BEFORE the
    scan so a concurrent write can only cause a later rescan, never a stale
    serve. ``version`` lets a caller that already read ledger_version() this
    pass hand it in (skips the re-read); omitted, this reads its own — still
    before the scan either way."""
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
    # Per-strategy LIVE deploy fields (D-LIVE-1 / D-LIVE-2 / D-LIVE-4).
    # All default so legacy callers and test fixtures keep working.
    mode: str = "DRY"
    wallet_group: Optional[str] = None
    group_cap_usd: Optional[float] = None
    group_pending_usd: Optional[float] = None
    live_wallet_balance: Optional[float] = None
    # Booked stop-loss exits in the last 24h (exit_kind='stop_loss' with a
    # real pnl_net; voided cancels excluded). Defaulted so legacy fixtures
    # keep constructing; counted as LOSSES by win_rate_24h per the
    # 2026-07-21 operator W·L·SL order.
    stops_24h: int = 0
    # Net post-cutover capital flows folded into realized_capital + the
    # halt-facing peak. Account-level, so every wallet reports the same value.
    net_flow_usd: float = 0.0

    @property
    def win_rate_24h(self) -> Optional[float]:
        """W·L·SL win rate (operator order 2026-07-21): stop-loss exits count
        as losses — ``wins / (settled_24h + stops_24h)`` where ``settled_24h``
        is the decided (WIN/LOSS) count."""
        denom = self.settled_24h + self.stops_24h
        if denom <= 0:
            return None
        return self.wins_24h / denom


def _equity_change_sort_key(
    exit_kind: Any, exit_ts: Any, resolution_ts: Any
) -> str:
    """Chronological sort key for the peak/drawdown equity replay.

    The replay orders settled rows by the moment equity ACTUALLY changed and
    tracks the running peak — realized capital is order-independent but the
    peak (which gates the LIVE hard DD halt, config.MAX_DRAWDOWN_HALT_FRAC,
    plus the DD brake) is NOT. For a normal
    WIN/LOSS/PUSH row that moment is ``resolution_ts`` (market settlement). For
    a booked-once PURE_STOP_04 stop-loss row (``exit_kind='stop_loss'``,
    ``outcome='CANCELLED'``) the pnl is realized at ``exit_ts`` — the stop-sell
    fill, set ONCE at booking, never NULL, never mutated — while its
    ``resolution_ts`` is NULL until a *display-only* telemetry backfill may (on
    older rows) stamp the sweep clock onto it.

    Ordering a stop row by ``resolution_ts`` is therefore both (a) the WRONG
    timestamp (it is not when equity moved) and (b) NON-invariant: a backfill
    could re-sort the row and silently move the live gate. Keying on
    ``exit_ts`` fixes both — it is the true equity-change moment and it is
    immutable, so the peak is identical whether ``resolution_ts`` is NULL or
    already backfilled (the live box carries a handful of stop rows in each
    state). Defending the invariant HERE, at the replay, means no future writer
    of ``resolution_ts`` on a stop row can perturb the gate.

    The two timestamp spellings in the codebase are normalized so they
    interleave chronologically: some ``exit_ts`` values are ISO
    (``2026-07-11T00:00:00Z``) while ``resolution_ts`` is the SQL-text form
    (``2026-07-11 00:00:00``). A raw lexicographic compare would misorder them
    ('T' sorts after ' '; see fill_rate_breaker._reset_clause for the same
    hazard), so both are folded to the space form. This is the per-row Python
    form of the shared SQL fragment ``_equity_change_order_sql``.
    """
    raw = exit_ts if str(exit_kind or "") == "stop_loss" else resolution_ts
    return str(raw or "").replace("T", " ").replace("Z", "")


def _equity_change_order_sql(fallback: str) -> str:
    """SQL expression for the equity-change ordering key — the SINGLE source of
    truth shared by both SQL replays (``get_capital_snapshot`` here and
    observability ``_ledger_equity_series``), so a change to the stop-row key
    can't silently desync the LIVE hard-DD-halt gate
    (config.MAX_DRAWDOWN_HALT_FRAC) from the equity chart.

    Orders a settled row by the moment equity ACTUALLY changed: a booked-once
    PURE_STOP_04 stop-loss row (``exit_kind='stop_loss'``) by its immutable
    ``exit_ts``, every other row by ``resolution_ts``; the two timestamp
    spellings are folded to the SQL-space form ('T'→' ', drop 'Z') so ISO and
    SQL-text values interleave chronologically. ``fallback`` is the COALESCE
    tail when both are NULL — the ONE intended difference between the two
    callers: ``''`` for the capital snapshot, ``bet_ts`` for the equity chart.
    Matches ``_equity_change_sort_key`` (the per-row Python form) apart from it.
    """
    return (
        "replace(replace(COALESCE("
        "CASE WHEN exit_kind = 'stop_loss' THEN exit_ts ELSE resolution_ts END, "
        f"{fallback}), 'T', ' '), 'Z', '')"
    )


def load_capital_flows(
    conn: sqlite3.Connection, since_sql: Optional[str]
) -> list[tuple[str, int, str, float]]:
    """Account-level capital-flow events as
    ``(occurred_at, id, kind, amount_usd)``,
    ready to merge into an equity replay.

    ``since_sql`` is the cutover (``WALLET_START_AT`` in SQL-text form).
    ``INITIAL_BANKROLL`` is defined as the balance AT that moment, so a flow
    dated BEFORE it is ALREADY inside the seed — admitting it would count the
    same dollars twice. Those rows are dropped here with a debug note rather
    than rejected at write time, so an operator can record the full funding
    history without corrupting the era's equity.

    Returns [] when the table is missing (legacy DB opened by an old-schema
    reader) so every replay degrades to the pure-P&L behaviour.

    Not cached: the table holds a handful of operator-recorded rows and the
    read is one covering-index scan, unlike the 12-column ledger fetch that
    ``_cached_ledger_rows`` exists to eliminate.
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
    """A `capital_flows` write was refused (bad kind / sign / timestamp, or a
    duplicate). Carries a machine-readable ``reason`` like the transfer flow's
    TransferError so callers can branch without string-matching."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def normalize_flow_ts(raw: str) -> str:
    """Validate + normalize a flow timestamp to the ledger's SQL-text UTC form.

    Accepts the SQL-text form and the ISO spelling (both appear in this
    codebase's timestamp columns); anything else — or a moment in the future,
    which would sit past the equity replay's right edge and silently do nothing
    until the clock caught up — is refused.
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
    """Check ``(kind, amount_usd)`` against the schema's sign contract.

    A wrong-signed amount is REFUSED, never silently flipped: "withdrawal 50"
    is ambiguous between a typo and a misunderstanding, and guessing would move
    the halt metric the wrong way by twice the amount.
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
    """Insert one validated `capital_flows` row; return its public dict.

    Refuses a duplicate (same kind + amount + occurred_at) unless ``force`` —
    re-running the backfill proposer's emitted commands is the expected way to
    double-record a deposit, and a doubled deposit silently inflates the peak
    the DD halt gates on.
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
    """All recorded flows oldest-first.

    Rows before ``WALLET_START_AT`` remain visible for audit history but are
    excluded from the current equity replay. This table is a hand-entered
    record of a few account events, not a stream. Returns [] when missing.
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

    ONE merge, shared by every caller (the per-wallet Python replay, the
    shared-equity post-pass and the SQL-sourced ``get_capital_snapshot``), so
    the halt-facing peak can never desync between them.

    Two high-water marks come out of the single ordered pass:
      * ``peak`` — the FLOW-INCLUSIVE series (seed + pnl + flows). This is what
        ``drawdown_pct_peak_frac`` (the hard DD halt + the DD brake) reads: a
        deposit raises equity at its timestamp WITHOUT counting as profit, and
        a withdrawal re-anchors the peak at the post-withdrawal balance so the
        cash transfer itself cannot trigger a halt. Trading losses after that
        withdrawal are measured from the new peak.
      * ``pnl_peak`` — the PURE-P&L series (seed + pnl), untouched by flows,
        preserving the D14 ``drawdown_ratio`` backtest-parity shape.

    Both are seeded at ``INITIAL_BANKROLL``, so with no flow rows the two are
    identical and every existing number is byte-for-byte unchanged.

    Ordering: rows sort by the equity-change timestamp key, and a settlement
    sorts BEFORE a flow at an identical key (rank 0 vs 1) — flows carry a
    coarse operator-observed timestamp while a settlement's is exact, so within
    the same second the market event is the earlier one.
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
    """Return (drawdown_ratio, drawdown_pct_peak_frac) for a peak/realized pair.

    D14 pin: drawdown_ratio denominator MUST be INITIAL_BANKROLL (constant),
    NOT realized_capital, to preserve parity with the backtest drawdown shape.

    DELIBERATE ASYMMETRY (capital flows): ``drawdown_pct_peak_frac`` — the
    metric the live hard DD halt and the DD brake gate on — runs on the
    FLOW-INCLUSIVE ``peak``/``realized`` pair, because risk must be measured
    against the money actually funding the account. ``drawdown_ratio`` stays
    PURE P&L: it reads the ``pnl_peak``/``pnl_realized`` pair (flows excluded)
    over the constant seed, so a deposit can never flatter or worsen the
    backtest-comparable drawdown shape. Both default to the flow-inclusive pair
    so callers with no flows are unaffected.
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
    """Per-group LIVE exposure state for the U6 group_exposure gate.

    `pending_usd` sums LIVE PENDING ledger rows + unresolved
    `live_order_attempts` (SUBMITTING/SUBMITTED with `ledger_id IS NULL`)
    across all member wallets. DRY rows are excluded — the parity shadow
    cannot block LIVE entries (D-LIVE-14).
    """

    group_id: str
    cap_usd: float
    pending_usd: float
    member_wallets: tuple[str, ...]


def wallet_id_for_sleeve(sleeve: str, pair_component: Optional[str] = None) -> str:
    """Map ledger/signal labels to the dry-run strategy wallet id.

    Pair ledger rows are stored as ``pairs_5m_long``/``pairs_5m_short`` for
    leg auditability, while the wallet is the component label (``pairs_5m``).
    Legacy rows may not have ``wallet_id`` populated, so every reader keeps
    this inference path.
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
        # SQLite datetime comparisons expect this format; trust operator input
        # when it is already in SQL-text form.
        return text.replace("T", " ")[:19]


def wallet_specs_for_portfolio(portfolio: Any) -> list[dict[str, Any]]:
    """One wallet spec per market sleeve.

    ``ledger_sleeves`` stays a TUPLE even though it now always holds exactly the
    wallet's own label: the pair sleeves (retired 2026-07-31) were the only
    components that fanned one wallet out across two ledger sleeve names
    (``{label}_long`` / ``{label}_short``), and every consumer already iterates
    the tuple.
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
    """Compute isolated capital state for each dry-run strategy wallet.

    Optional U6 keyword args propagate per-wallet LIVE/DRY mode + group
    exposure context into the returned snapshots so the pipeline can route
    consistently within a tick:

    - `mode_state`: bulk-loaded `{wallet_id: 'LIVE'|'DRY'}` from
      `wallet_mode_state` (D-LIVE-20). Wallets missing from the dict default
      to 'DRY'.
    - `group_state`: `{group_id: GroupCapState}` computed by
      `get_group_state` so the snapshot exposes the group cap/pending pair
      the gate needs.
    - `live_wallet_balance`: latest CLOB funder balance — drives the
      D-LIVE-4 effective-cap shrinkage.
    - `version`: a ledger_version() probe the caller already read this pass
      (the snapshot builder shares ONE probe across its KPI caches); passed
      through to the scan cache to skip the re-read. Omitted, the scan reads
      its own — still before the scan, so the freshness contract holds.
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
    # Account-level cash events, gated on the SAME cutover as the ledger scan
    # (the seed is the balance at that moment). Loaded once and merged into
    # every wallet's replay below — a deposit funds the whole account, not a
    # sleeve.
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
                    # Booked stop-loss exit (CANCELLED outcome, real pnl_net —
                    # this branch already requires pnl_raw IS NOT NULL, so a
                    # voided cancel never lands here). Counted separately so
                    # win_rate_24h can count it as a loss (W·L·SL contract).
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
        # Per-strategy LIVE plumbing: mode + group come from the optional
        # kwargs so legacy callers (tests, dashboard snapshot builder) keep
        # working without supplying them.
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

    # ---- Phase 5 shared-equity post-pass (plan 2026-05-29-001 option B1) ----
    # For each `shared_equity` wallet_group, the member sleeves compound on
    # ONE shared equity, share ONE peak (drawdown halt), and share ONE
    # exposure cap — faithful to exp044's single-wallet backtest. We compute
    # the COMBINED capital across members and OVERRIDE only the shared fields
    # on each member snapshot. Isolated wallets and non-shared groups are
    # untouched (the loop body skips groups whose `shared_equity` is False).
    groups = getattr(portfolio, "wallet_groups", ()) or ()
    for group in groups:
        if not getattr(group, "shared_equity", False):
            continue
        member_ids = [m for m in group.members if m in state]
        if not member_ids:
            continue

        # combined_pnl_total = SUM of member buckets' pnl_total.
        combined_pnl_total = sum(
            float(state[m]["pnl_total"]) for m in member_ids
        )

        # combined_peak: MERGE all member buckets' settled_rows into one
        # list plus the account-level capital flows, replay the running
        # combined equity in equity-change order, track the max. This is the
        # peak of the COMBINED equity curve — NOT the sum (or max) of
        # per-sleeve peaks. This is the peak the LIVE hard DD halt reads: the
        # deployed sleeves live in a shared_equity group, so the flows have to
        # land HERE, not only on the per-wallet replay above.
        merged_rows: list[tuple[str, int, float]] = []
        for m in member_ids:
            merged_rows.extend(state[m]["settled_rows"])
        combined_peak, combined_pnl_peak, combined_net_flow = _replay_equity(
            merged_rows, flows
        )
        # The shared wallet has ONE bankroll and ONE funding stream — add
        # INITIAL_BANKROLL (and the flows) ONCE, NOT once per member (summing
        # per-member realized double-counts both).
        combined_pnl_realized = INITIAL_BANKROLL + combined_pnl_total
        combined_realized = combined_pnl_realized + combined_net_flow

        # combined_pending = SUM of member buckets' pending_exposure.
        combined_pending = sum(
            float(state[m]["pending_exposure"]) for m in member_ids
        )

        combined_dd_ratio, combined_dd_peak_frac = _compute_drawdown_metrics(
            combined_peak,
            combined_realized,
            pnl_peak=combined_pnl_peak,
            pnl_realized=combined_pnl_realized,
        )

        # Override ONLY the shared capital fields on each member snapshot via
        # dataclasses.replace. KEEP per-member pnl_total / pnl_24h / wins_24h /
        # settled_24h / stops_24h / n_pending / n_settled / n_topups_24h /
        # mode / wallet_group / group_cap_usd / group_pending_usd /
        # live_wallet_balance for dashboard attribution.
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
    """Compute per-group LIVE pending exposure (D-LIVE-14).

    Sources contributing to `pending_usd`:
      1. LIVE PENDING ledger rows for any wallet in the group.
      2. `live_order_attempts` in SUBMITTING/SUBMITTED with `ledger_id IS NULL`
         and `wallet_group` matching the group.

    DRY rows are excluded — the parity shadow does not consume real-money
    headroom. Wallets with no group membership get no row.
    """
    groups = getattr(portfolio, "wallet_groups", ()) or ()
    out: dict[str, GroupCapState] = {}
    if not groups:
        return out

    # 1. LIVE PENDING ledger exposure aggregated per wallet_id.
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

    # 2. Active live_order_attempts that have not yet been linked to a
    # ledger row contribute their reserved amount toward group pending
    # exposure. The active-slot partial unique index guarantees one row
    # per (wallet, coin, sleeve, em, side, window) — no double-counting.
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
    """Per-strategy realized PnL available for transfer (D-LIVE-21).

    Formula:
      extractable = SUM(pnl_net for LIVE settled rows on wallet)
                  - SUM(amount_usd for in-flight/CONFIRMED transfers from wallet)

    Audit P1 #6: prior transfers in CONFIRMED *and* the in-flight states
    SUBMITTED/SUBMITTING are subtracted. A SUBMITTED transfer (relayer
    accepted, awaiting on-chain confirmation) is a real withdrawal in
    progress; nothing in the repo promotes SUBMITTED->CONFIRMED, so counting
    only CONFIRMED let an in-flight withdrawal be double-spent. Treating
    in-flight as committed is conservative and self-corrects: if the transfer
    later FAILs, the row leaves the subtracted set and extractable rises.

    "Settled" is the SAME canonical predicate get_capital_snapshot uses for
    realized_capital: ``outcome != 'PENDING' AND pnl_net IS NOT NULL``. Audit
    P1 #5: the prior ``outcome IN ('WIN','LOSS','PUSH')`` filter EXCLUDED
    CANCELLED rows, but a CANCELLED row can still carry a REAL realized
    round-trip PnL in ``pnl_net`` (historically the retired pair-orphan close
    wrote one). realized_capital counted that loss while extractable did not,
    so extractable overstated withdrawable funds. Using one predicate keeps the
    two views in lockstep.

    The transfer flow's preview blocks any submission that would push
    extractable negative. v1 attributes withdrawals to a single source
    wallet so the operator can extract winners without freezing losers.

    Performance finding #8 (PERF-04): for the per-tick snapshot builder
    that needs values for every wallet, prefer `extractable_pnl_by_wallet`
    (one aggregate query covering all wallets) over N invocations of
    this per-wallet helper (2N queries).
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
    """Bulk variant: 2 queries total instead of 2*N for the snapshot
    builder. Returns `{wallet_id: extractable}` for every wallet that
    has at least one LIVE settled row or a CONFIRMED transfer.

    Wallets absent from the map have zero realized PnL and zero prior
    transfers; callers default missing keys to 0.0.
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


