"""Ledger writer + slot_state predicate for the live scanner.

Ports `slot_state` and PENDING-first `record_bet` from
`hightempbot/execution/ledger.py:318-565`, adapted for cryptobot's slot key:

  (coin, sleeve, entry_minute, side, window_start_ts)

Where hightempbot read the slot key out of `event_detail` JSON, cryptobot
queries first-class indexed columns (U1's `idx_ledger_slot` composite).
This keeps `slot_state()` under p95 5ms at 1Hz × 12 slots/tick.

PENDING-first invariant (R18 carry-forward): every bet is inserted with
outcome='PENDING' before any other write; settlement (WIN/LOSS/CANCELLED)
happens later via the F3 resolution sweep (U7a).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

from cryptobot import config
from cryptobot.config import MAX_PENDING_AGE_MINUTES
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.capital import wallet_id_for_sleeve


@dataclass(frozen=True)
class BetSignal:
    """All the fields needed to write a PENDING ledger row.

    The pipeline (U9) builds a BetSignal from the scoring + walker outputs
    and passes it to ``record_bet``. Top-up vs initial-fill is determined
    by ``slot_filled_pre > 0``.
    """

    # slot key
    coin: str
    sleeve: str
    entry_minute: int
    side: str
    window_start_ts: int
    window_end_ts: int
    # market identity
    interval_min: int
    poly_market_id: str
    poly_token_id: str
    fee_rate_bps: float
    # decision-time snapshot
    model_p: float
    p_market: float          # top_ask at scan (used as fill_price for parity, D2)
    fill_price: float        # = p_market on writes (D2)
    walked_vwap: Optional[float]  # multi-level VWAP from walker (forensic)
    realized_edge: float
    partial_ret_z_1h: Optional[float]
    lut_p_bucket: Optional[int]
    lut_hit_rate: Optional[float]
    lut_n_cum: Optional[int]
    # sizing
    target_usd: float
    bet_size: float
    slot_filled_pre: float   # cumulative slot fill BEFORE this row
    entry_top_price: float   # sticky cap anchor (R-NEW-9)
    yes_pre1_anchor: Optional[float]  # top_ask at entry_ts - 60s (D3)
    # Underlying spot at decision time. Resolution sweep computes
    # `won_up = (resolution_price > strike_price)` against this value —
    # NOT against yes_pre1_anchor (a Polymarket probability in [0,1]
    # that's the wrong magnitude for a spot comparison; review #2).
    strike_price: Optional[float] = None
    # Divergence #13 fix (2026-05-21): live spot at window_open — the
    # same anchor the model trains against and Polymarket settles on.
    # Read from PartialRetEngine.window_open_spot() at bet time; consumed
    # by resolution_sweep._decide_won_up. When non-None this is the
    # authoritative anchor; sweep falls back to strike_price only on
    # legacy rows where window_open_spot is NULL.
    window_open_spot: Optional[float] = None
    # Audit w19oxqf24 (P2): the executable limit price for a LIVE GTC order —
    # the most-expensive book level the walker consumed (walker WalkResult
    # limit_price). The GTC BUY limit must be priced here, NOT at fill_price
    # (= top_ask): the order is SIZED for multi-level walked notional, so a
    # limit at top_ask only crosses top-of-book depth and systematically
    # under-fills versus the recorded bet_size. None for DRY rows / legacy
    # callers, where place_gtc_limit_order falls back to fill_price.
    limit_price: Optional[float] = None
    pair_group_id: Optional[str] = None
    pair_role: Optional[str] = None
    pair_component: Optional[str] = None
    paired_coin: Optional[str] = None
    wallet_id: Optional[str] = None
    # B1 sub-minute checkpoint add-on: seconds from window open at which the
    # bet was admitted. None on every baseline (minute-boundary) bet — the
    # sub-minute path is the only writer.
    offset_sec: Optional[int] = None
    # forensic
    config_sha256: str = ""

    @property
    def is_topup(self) -> bool:
        return self.slot_filled_pre > 0.0


def _slot_where() -> str:
    """WHERE clause selecting one slot's non-cancelled rows."""
    return (
        "coin = ? AND sleeve = ? AND entry_minute = ? AND side = ? "
        "AND window_start_ts = ? AND outcome != 'CANCELLED'"
    )


def _slot_params(
    coin: str, sleeve: str, entry_minute: int, side: str, window_start_ts: int
) -> tuple:
    return (coin, sleeve, entry_minute, side, window_start_ts)


def slot_state(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    entry_minute: int,
    side: str,
    window_start_ts: int,
) -> tuple[float, Optional[float]]:
    """Return ``(slot_filled_usd, slot_entry_top_price)`` in one SQL.

    Hot-path call; runs once per (coin, sleeve, entry_minute, side) per
    qualifying tick. The composite ``idx_ledger_slot`` index handles the
    predicate in O(log N).

    Honors the same stale-PENDING semantics as ``slot_filled_usd``.
    """
    where = _slot_where()
    params = _slot_params(coin, sleeve, entry_minute, side, window_start_ts)
    age = int(max(0, MAX_PENDING_AGE_MINUTES))

    # Single pass (latency item #3): the stale-PENDING sum is folded into the
    # main aggregate as a CASE on the same datetime('now', ?) age predicate the
    # old second query used, so a filled slot no longer pays a second indexed
    # scan every qualifying tick. The stale sum is only APPLIED when the slot
    # has at least one non-PENDING row and age > 0 — identical to the old
    # two-query gating, pinned by the filled+stale-PENDING cases in
    # test_slot_state.py.
    row = conn.execute(
        f"SELECT COALESCE(SUM(bet_size), 0) AS filled, "
        f"SUM(CASE WHEN outcome != 'PENDING' THEN 1 ELSE 0 END) AS n_filled, "
        f"COALESCE(SUM(CASE WHEN outcome = 'PENDING' "
        f"                   AND bet_ts < datetime('now', ?) "
        f"                  THEN bet_size ELSE 0 END), 0) AS stale, "
        f"(SELECT entry_top_price FROM ledger WHERE {where} "
        f" ORDER BY bet_ts ASC, id ASC LIMIT 1) AS anchor "
        f"FROM ledger WHERE {where}",
        (f"-{age} minutes", *params, *params),
    ).fetchone()
    if row is None:
        return 0.0, None
    n_filled = int(row["n_filled"] or 0)
    total = float(row["filled"] or 0.0)
    if n_filled > 0 and age > 0:
        total = max(0.0, total - float(row["stale"] or 0.0))
    raw = row["anchor"]
    if raw is None:
        return total, None
    try:
        return total, float(raw)
    except (TypeError, ValueError):
        return total, None


def record_bet(
    conn: sqlite3.Connection,
    signal: BetSignal,
    *,
    is_topup: bool | None = None,
    scalein_topup: bool = False,
    initial_realized_edge: Optional[float] = None,
    mode: str = "DRY",
    wallet_group: Optional[str] = None,
    live_order_id: Optional[str] = None,
    live_client_order_id: Optional[str] = None,
    live_filled_size: Optional[float] = None,
    live_filled_price: Optional[float] = None,
    live_fill_ts: Optional[str] = None,
    top_ask_at_decision: Optional[float] = None,
    pair_orphan: int = 0,
) -> int:
    """PENDING-first insert. Returns the new row id.

    Caller-side responsibility: the orchestrator wraps this call in a
    ``BEGIN IMMEDIATE`` transaction with the slot_state() probe so that
    concurrent ticks evaluating the same slot are serialized (R-NEW-9).

    ``is_topup`` defaults to ``signal.is_topup`` (slot_filled_pre > 0). When
    is_topup is True and ``initial_realized_edge`` is supplied, the row's
    ``add_on_realized_edge_minus_initial_edge`` column is populated.

    Per-strategy LIVE plumbing (D-LIVE-1 / D-LIVE-3 / D-LIVE-28):
    ``mode`` stamps the row's immutable LIVE/DRY column, ``wallet_group``
    routes the group cap audit, and the ``live_*`` kwargs carry the CLOB
    fill metadata when the pipeline took the LIVE path.
    """
    now = utc_now_sql()
    resolved_topup = signal.is_topup if is_topup is None else bool(is_topup)
    resolved_wallet_id = signal.wallet_id or wallet_id_for_sleeve(
        signal.sleeve, signal.pair_component
    )

    # is_topup ⇔ slot_filled_pre > 0 — invariant enforced application-side
    # because SQLite cannot add CHECK constraints via ALTER on existing DBs.
    # Mismatch indicates a caller bug that would silently break the parity
    # test (U16 filters on is_topup=False).
    expected_topup = signal.slot_filled_pre > 0.0
    if resolved_topup != expected_topup:
        raise ValueError(
            f"record_bet: is_topup={resolved_topup} disagrees with "
            f"slot_filled_pre={signal.slot_filled_pre} for slot "
            f"({signal.coin}, {signal.sleeve}, {signal.entry_minute}, "
            f"{signal.side}, {signal.window_start_ts})"
        )

    # A T3 scale-in top-up is a strict subset of top-ups: scalein_topup ⇒
    # is_topup (slot_filled_pre > 0). Enforced application-side (SQLite cannot
    # add CHECK via ALTER) so a caller that tags scalein_topup without the
    # cumulative-fill anchor fails loudly instead of writing a mislabelled row.
    resolved_scalein_topup = bool(scalein_topup)
    if resolved_scalein_topup and not resolved_topup:
        raise ValueError(
            "record_bet: scalein_topup=True requires is_topup / "
            f"slot_filled_pre > 0 (got slot_filled_pre={signal.slot_filled_pre})"
        )

    addon_delta: Optional[float] = None
    if resolved_topup and initial_realized_edge is not None:
        addon_delta = signal.realized_edge - initial_realized_edge

    resolved_mode = (mode or "DRY").upper()
    if resolved_mode not in ("DRY", "LIVE"):
        raise ValueError(f"record_bet: mode must be 'DRY' or 'LIVE'; got {mode!r}")

    cur = conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side, window_start_ts, window_end_ts,
           interval_min, poly_market_id, poly_token_id, fee_rate_bps,
           model_p, p_market, fill_price, walked_vwap, realized_edge,
           partial_ret_z_1h, lut_p_bucket, lut_hit_rate, lut_n_cum,
           target_usd, bet_size, slot_filled_pre, is_topup, scalein_topup, entry_top_price,
           add_on_realized_edge_minus_initial_edge, yes_pre1_anchor, strike_price,
           window_open_spot, pair_group_id, pair_role, pair_component, paired_coin,
           mode, wallet_group, live_order_id, live_client_order_id,
           live_filled_size, live_filled_price, live_fill_ts,
           top_ask_at_decision, pair_orphan, offset_sec, edge_semantics_version,
           outcome, config_sha256, venue)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 2, 'PENDING', ?, ?)
        """,
        (
            now,
            signal.coin,
            resolved_wallet_id,
            signal.sleeve,
            signal.entry_minute,
            signal.side,
            signal.window_start_ts,
            signal.window_end_ts,
            signal.interval_min,
            signal.poly_market_id,
            signal.poly_token_id,
            signal.fee_rate_bps,
            signal.model_p,
            signal.p_market,
            signal.fill_price,
            signal.walked_vwap,
            signal.realized_edge,
            signal.partial_ret_z_1h,
            signal.lut_p_bucket,
            signal.lut_hit_rate,
            signal.lut_n_cum,
            signal.target_usd,
            signal.bet_size,
            signal.slot_filled_pre,
            int(resolved_topup),
            int(resolved_scalein_topup),
            signal.entry_top_price,
            addon_delta,
            signal.yes_pre1_anchor,
            signal.strike_price,
            signal.window_open_spot,
            signal.pair_group_id,
            signal.pair_role,
            signal.pair_component,
            signal.paired_coin,
            resolved_mode,
            wallet_group,
            live_order_id,
            live_client_order_id,
            live_filled_size,
            live_filled_price,
            live_fill_ts,
            top_ask_at_decision,
            int(pair_orphan),
            signal.offset_sec,
            signal.config_sha256,
            # Deliverable 5: every new bet is stamped with the active venue.
            # Read dynamically (never bound at import) so a monkeypatched
            # config.VENUE / a per-process pin is honored. Absent-in-DB legacy
            # rows read back as 'polymarket' via the column DEFAULT.
            config.VENUE,
        ),
    )
    rowid = cur.lastrowid
    if rowid is None:
        raise RuntimeError("record_bet INSERT did not produce a rowid")
    return rowid


def record_signal_skip(
    conn: sqlite3.Connection,
    *,
    created_at: str,
    coin: str,
    sleeve: str,
    entry_minute: int,
    side: str,
    window_start_ts: int,
    model_p: Optional[float],
    ask: Optional[float],
    edge: Optional[float],
    fee_rate_bps: Optional[float],
    partial_ret_z_1h: Optional[float],
    lut_p_bucket: Optional[int],
    lut_hit_rate: Optional[float],
    lut_n_cum: Optional[int],
    skip_reason: str,
    wallet_id: Optional[str] = None,
    mode: Optional[str] = None,
    wallet_group: Optional[str] = None,
    offset_sec: Optional[int] = None,
) -> None:
    """Write a SKIP:<reason> signals row. Observational only (D19)."""
    resolved_wallet_id = wallet_id or wallet_id_for_sleeve(sleeve)
    conn.execute(
        """
        INSERT INTO signals
          (created_at, coin, wallet_id, sleeve, entry_minute, side, window_start_ts,
           model_p, ask, edge, fee_rate_bps,
           partial_ret_z_1h, lut_p_bucket, lut_hit_rate, lut_n_cum,
           mode, wallet_group, offset_sec, outcome, skip_reason)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            created_at, coin, resolved_wallet_id, sleeve, entry_minute, side, window_start_ts,
            model_p, ask, edge, fee_rate_bps,
            partial_ret_z_1h, lut_p_bucket, lut_hit_rate, lut_n_cum,
            mode, wallet_group, offset_sec,
            f"SKIP:{skip_reason}", skip_reason,
        ),
    )


def record_signal_would_bet(
    conn: sqlite3.Connection,
    *,
    created_at: str,
    coin: str,
    sleeve: str,
    entry_minute: int,
    side: str,
    window_start_ts: int,
    model_p: float,
    ask: float,
    edge: float,
    fee_rate_bps: Optional[float],
    partial_ret_z_1h: Optional[float],
    lut_p_bucket: Optional[int],
    lut_hit_rate: Optional[float],
    lut_n_cum: Optional[int],
    wallet_id: Optional[str] = None,
    mode: Optional[str] = None,
    wallet_group: Optional[str] = None,
    offset_sec: Optional[int] = None,
) -> None:
    """Write a WOULD_BET signals row paired with a PENDING ledger row."""
    resolved_wallet_id = wallet_id or wallet_id_for_sleeve(sleeve)
    conn.execute(
        """
        INSERT INTO signals
          (created_at, coin, wallet_id, sleeve, entry_minute, side, window_start_ts,
           model_p, ask, edge, fee_rate_bps,
           partial_ret_z_1h, lut_p_bucket, lut_hit_rate, lut_n_cum,
           mode, wallet_group, offset_sec, outcome, skip_reason)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WOULD_BET', NULL)
        """,
        (
            created_at, coin, resolved_wallet_id, sleeve, entry_minute, side, window_start_ts,
            model_p, ask, edge, fee_rate_bps,
            partial_ret_z_1h, lut_p_bucket, lut_hit_rate, lut_n_cum,
            mode, wallet_group, offset_sec,
        ),
    )


# ---------------------------------------------------------------------------
# B1: sub-minute checkpoint slot helpers (per-offset semantics)
# ---------------------------------------------------------------------------


def subminute_window_state(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    side: str,
    window_start_ts: int,
) -> tuple[float, Optional[float]]:
    """One-entry-per-resolution state for the sub-minute add-on.

    Returns ``(slot_filled_usd, slot_entry_top_price)`` aggregated across ALL
    offsets/minutes of this (coin, sleeve, side, window) — unlike ``slot_state``
    which keys on a single entry_minute. Used when
    ``config.SUBMINUTE_ONE_PER_WINDOW`` is on so later checkpoints TOP UP the
    window's first entry instead of stacking a fresh bet each second. Mirrors
    ``slot_state``'s stale-PENDING handling (drops PENDING rows older than
    MAX_PENDING_AGE_MINUTES so a stuck order cannot permanently wedge the slot).
    """
    where = (
        "coin = ? AND sleeve = ? AND side = ? "
        "AND window_start_ts = ? AND outcome != 'CANCELLED'"
    )
    params = (coin, sleeve, side, int(window_start_ts))
    age = int(max(0, MAX_PENDING_AGE_MINUTES))
    row = conn.execute(
        f"SELECT COALESCE(SUM(bet_size), 0) AS filled, "
        f"SUM(CASE WHEN outcome != 'PENDING' THEN 1 ELSE 0 END) AS n_filled, "
        f"(SELECT entry_top_price FROM ledger WHERE {where} "
        f" ORDER BY bet_ts ASC, id ASC LIMIT 1) AS anchor "
        f"FROM ledger WHERE {where}",
        (*params, *params),
    ).fetchone()
    if row is None:
        return 0.0, None
    n_filled = int(row["n_filled"] or 0)
    total = float(row["filled"] or 0.0)
    if n_filled > 0 and age > 0:
        stale_row = conn.execute(
            f"SELECT COALESCE(SUM(bet_size), 0) AS stale FROM ledger WHERE {where} "
            f"AND outcome = 'PENDING' "
            f"AND bet_ts < datetime('now', ?)",
            (*params, f"-{age} minutes"),
        ).fetchone()
        stale_total = (
            float(stale_row["stale"] or 0.0) if stale_row is not None else 0.0
        )
        total = max(0.0, total - stale_total)
    raw = row["anchor"]
    if raw is None:
        return total, None
    try:
        return total, float(raw)
    except (TypeError, ValueError):
        return total, None


def subminute_window_existing_side(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    window_start_ts: int,
) -> Optional[str]:
    """First non-cancelled side already opened for this sub-minute market.

    Backtest checkpoint combos dedupe at final resolution-market level, not at
    side level. The same-side state above is still needed for top-ups, but an
    opposite-side entry in the same window must be rejected before it can open
    a second position.
    """
    row = conn.execute(
        "SELECT side FROM ledger "
        "WHERE coin = ? AND sleeve = ? "
        "  AND window_start_ts = ? AND outcome != 'CANCELLED' "
        "ORDER BY bet_ts ASC, id ASC LIMIT 1",
        (coin, sleeve, int(window_start_ts)),
    ).fetchone()
    if row is None:
        return None
    raw = row["side"] if isinstance(row, sqlite3.Row) else row[0]
    return str(raw) if raw is not None else None


def subminute_window_existing_side_any_sleeve(
    conn: sqlite3.Connection,
    *,
    coin: str,
    interval_min: int,
    window_start_ts: int,
) -> Optional[str]:
    """First non-cancelled side already opened on this resolution market, ACROSS
    ALL sleeves.

    The cross-sleeve twin of ``subminute_window_existing_side`` (which filters
    ``WHERE sleeve = ?`` and so only sees one band's entries). Scoped exactly
    like ``count_window_entries`` — (coin, interval_min, window_start_ts) — which
    identifies the single binary market that every loaded sleeve scores
    (``interval_min`` separates 5m vs 15m markets sharing a
    ``window_start_ts``). The multifill gate (operator 2026-07-10) uses this to
    enforce that every extra entry on a resolution is the SAME side as whatever
    already opened it (any sleeve): buying both YES and NO on one binary is a
    guaranteed-losing hedge, so an opposite-side candidate is rejected even
    though multifill deliberately bypasses the first-fill-wins guard.
    """
    row = conn.execute(
        "SELECT side FROM ledger "
        "WHERE coin = ? AND interval_min = ? "
        "  AND window_start_ts = ? AND outcome != 'CANCELLED' "
        "ORDER BY bet_ts ASC, id ASC LIMIT 1",
        (coin, int(interval_min), int(window_start_ts)),
    ).fetchone()
    if row is None:
        return None
    raw = row["side"] if isinstance(row, sqlite3.Row) else row[0]
    return str(raw) if raw is not None else None


def subminute_slot_seen(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    offset_sec: int,
    window_start_ts: int,
) -> bool:
    """Whether this (coin, sleeve, window, offset) checkpoint already opened
    a bet. Per-offset — every checkpoint in a window is its own admissible
    slot (the checkpoint-grid combo stacks multiple offsets per market);
    sub-minute slots do NOT top up (one shot per checkpoint)."""
    row = conn.execute(
        "SELECT 1 FROM ledger "
        "WHERE coin = ? AND sleeve = ? AND offset_sec = ? "
        "  AND window_start_ts = ? AND outcome != 'CANCELLED' "
        "LIMIT 1",
        (coin, sleeve, int(offset_sec), int(window_start_ts)),
    ).fetchone()
    return row is not None


def subminute_market_other_sleeve(
    conn: sqlite3.Connection,
    *,
    coin: str,
    interval_min: int,
    window_start_ts: int,
    sleeve: str,
) -> bool:
    """True if ANY OTHER sleeve already opened a non-cancelled position on this
    exact resolution market (coin, interval, window).

    Cross-sleeve one-bet-per-market (first fill wins): two sleeves can score the
    SAME market, but at most ONE may take a position on it. The in-memory
    ``_V2_MARKET_FILLED`` set
    is the fast path within a process; this durable query is the restart-safe
    complement (the per-sleeve ``subminute_window_*`` dedups all filter
    ``WHERE sleeve = ?`` and so do NOT arbitrate across sleeves). The market is
    identified by (coin, interval_min, window_start_ts) — ``interval_min``
    scopes 5m vs 15m markets that can share a ``window_start_ts``.
    """
    row = conn.execute(
        "SELECT 1 FROM ledger "
        "WHERE coin = ? AND interval_min = ? AND window_start_ts = ? "
        "  AND sleeve != ? AND outcome != 'CANCELLED' LIMIT 1",
        (coin, int(interval_min), int(window_start_ts), sleeve),
    ).fetchone()
    return row is not None


def count_window_entries(
    conn: sqlite3.Connection,
    *,
    coin: str,
    interval_min: int,
    window_start_ts: int,
) -> int:
    """Number of INDEPENDENT non-cancelled entries on this resolution market.

    Counts top-level entries (``is_topup = 0``) across ALL sleeves and sides on
    this exact (coin, interval_min, window) market. This is the durable,
    restart-safe entry tally the multifill gate reads (operator 2026-07-10): a
    further INDEPENDENT entry is admissible once this count is >= 1. Only FILLS
    write a ledger row (a reject records no row), so a count >= 1 proves a prior
    entry actually filled — the sole requirement now that the fixed per-resolution
    count ceiling is removed (operator 2026-07-11). Cross-sleeve by design — two
    sleeves taking the same market count as two entries.
    """
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM ledger "
        "WHERE coin = ? AND interval_min = ? AND window_start_ts = ? "
        "  AND is_topup = 0 AND outcome != 'CANCELLED'",
        (coin, int(interval_min), int(window_start_ts)),
    ).fetchone()
    if row is None:
        return 0
    n = row["n"] if isinstance(row, sqlite3.Row) else row[0]
    return int(n or 0)


def claim_subminute_scan(
    conn: sqlite3.Connection,
    *,
    coin: str,
    sleeve: str,
    offset_sec: int,
    window_start_ts: int,
) -> bool:
    """Return True only for the first scan of this checkpoint offset
    (mirrors ``_claim_entry_scan_for_key`` at per-offset granularity)."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO subminute_scan_latches "
        "(coin, sleeve, offset_sec, window_start_ts) "
        "VALUES (?, ?, ?, ?)",
        (coin, sleeve, int(offset_sec), int(window_start_ts)),
    )
    return (cur.rowcount or 0) > 0


def utc_now_iso() -> str:
    """Back-compat alias for ``cryptobot.db.connection.utc_now_sql``.

    The whole module now uses the SQL-text format (``YYYY-MM-DD HH:MM:SS``)
    so cross-table datetime comparisons (signals vs ledger) match exactly.
    Kept under the old name so existing callers continue to work; new code
    should import ``utc_now_sql`` from ``cryptobot.db`` directly.
    """
    return utc_now_sql()
