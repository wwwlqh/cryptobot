"""Read-only proposer for missing `capital_flows` rows.

The live box records a `bankroll_peak` sample (real CLOB balance + lifetime
LIVE settled pnl + pending notional) on every reconciliation run. Between two
samples where the account is FLAT — nothing pending, no bet open across the
gap — the balance can only move by settled P&L. Anything left over is cash that
entered or left the account: a deposit, a withdrawal or a rebate.

This module reconstructs those residuals and PROPOSES `record-flow` commands.
It never inserts: the samples are 10 minutes apart so the timestamp is a
midpoint estimate, and a mis-dated flow moves the peak the DD halt gates on.
The operator confirms each line against their own funding record.
"""

from __future__ import annotations

import sqlite3
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

# Residual below which a gap is treated as reconciliation noise rather than a
# cash event (fee dust, a sub-cent balance read, a redemption booked a beat
# after its sample). Real funding events are two orders of magnitude larger.
FLOW_RESIDUAL_MIN_USD = 2.0

# Far-future sentinel for a row that never settled — sorts after every real
# SQL-text timestamp, so an unresolved bet keeps its gap disqualified forever.
_OPEN_INTERVAL_END = "9999-12-31 23:59:59"


@dataclass(frozen=True)
class FlowProposal:
    """One proposed `capital_flows` row, with the evidence that produced it."""

    occurred_at: str
    kind: str
    amount_usd: float
    window_start: str
    window_end: str
    balance_delta: float
    settled_pnl_delta: float
    residual_usd: float


def _sql_ts(raw: object) -> str:
    return str(raw or "").replace("T", " ").replace("Z", "")


def _midpoint(start: str, end: str) -> str:
    """Midpoint of the bracketing samples — the best estimate available for a
    flow that landed somewhere inside a ~10-minute reconciliation gap. Falls
    back to the window start if either endpoint is unparseable."""
    try:
        a = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        b = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    except ValueError:
        return start
    mid = a + timedelta(seconds=(b - a).total_seconds() / 2.0)
    return mid.strftime("%Y-%m-%d %H:%M:%S")


def _busy_intervals(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Merged [open, settled] intervals of every LIVE ledger row.

    A gap overlapping one of these is NOT flat even when both endpoint samples
    read pending_exposure=0: a bet placed and resolved between the samples
    moves cash on its own schedule (stake out at fill, payout back at
    redemption), so its residual is execution transit, not funding.
    """
    rows = conn.execute(
        "SELECT bet_ts, outcome, exit_kind, exit_ts, resolution_ts "
        "FROM ledger WHERE mode = 'LIVE'"
    ).fetchall()
    raw: list[tuple[str, str]] = []
    for r in rows:
        start = _sql_ts(r["bet_ts"])
        if not start:
            continue
        if str(r["outcome"]) == "PENDING":
            end = _OPEN_INTERVAL_END
        else:
            settled = (
                r["exit_ts"] if str(r["exit_kind"] or "") == "stop_loss"
                else r["resolution_ts"]
            )
            end = _sql_ts(settled) or _OPEN_INTERVAL_END
        raw.append((start, max(start, end)))

    raw.sort()
    merged: list[tuple[str, str]] = []
    for start, end in raw:
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def _overlaps(merged: list[tuple[str, str]], starts: list[str], t0: str, t1: str) -> bool:
    """True when any merged busy interval intersects the open gap (t0, t1).

    Merging leaves the intervals disjoint and start-sorted, so their ends are
    strictly increasing too: only the LAST interval beginning before ``t1`` can
    still be running at ``t0``, and one bisect settles the whole question.
    """
    idx = bisect_left(starts, t1)
    return idx > 0 and merged[idx - 1][1] > t0


def propose_capital_flows(
    conn: sqlite3.Connection,
    *,
    min_residual_usd: float = FLOW_RESIDUAL_MIN_USD,
    since: Optional[str] = None,
) -> tuple[list[FlowProposal], dict[str, int]]:
    """Scan `bankroll_peak` for unexplained cash moves. Returns
    ``(proposals, stats)``; never writes."""
    rows = conn.execute(
        "SELECT sampled_at, wallet_balance, realized_pnl, pending_exposure "
        "FROM bankroll_peak ORDER BY sampled_at ASC, id ASC"
    ).fetchall()
    if since:
        rows = [r for r in rows if _sql_ts(r["sampled_at"]) >= since]
    flat = [r for r in rows if abs(float(r["pending_exposure"] or 0.0)) < 1e-9]

    merged = _busy_intervals(conn)
    starts = [iv[0] for iv in merged]

    proposals: list[FlowProposal] = []
    skipped_busy = 0
    for prev, cur in zip(flat, flat[1:]):
        t0 = _sql_ts(prev["sampled_at"])
        t1 = _sql_ts(cur["sampled_at"])
        balance_delta = float(cur["wallet_balance"]) - float(prev["wallet_balance"])
        pnl_delta = float(cur["realized_pnl"]) - float(prev["realized_pnl"])
        residual = balance_delta - pnl_delta
        if abs(residual) < min_residual_usd:
            continue
        if _overlaps(merged, starts, t0, t1):
            skipped_busy += 1
            continue
        # Cents is the unit real cash moves in; the exact residual rides along
        # on the proposal so the operator can see what was rounded away.
        amount = round(residual, 2)
        proposals.append(
            FlowProposal(
                occurred_at=_midpoint(t0, t1),
                kind="deposit" if amount > 0 else "withdrawal",
                amount_usd=amount,
                window_start=t0,
                window_end=t1,
                balance_delta=balance_delta,
                settled_pnl_delta=pnl_delta,
                residual_usd=residual,
            )
        )
    stats = {
        "samples": len(rows),
        "flat_samples": len(flat),
        "skipped_busy_gaps": skipped_busy,
    }
    return proposals, stats


def format_proposals(
    proposals: list[FlowProposal],
    stats: dict[str, int],
    *,
    min_residual_usd: float = FLOW_RESIDUAL_MIN_USD,
    db_path: Optional[str] = None,
) -> str:
    """Human-readable report ending in ready-to-run `record-flow` command
    lines. Nothing here mutates anything - the operator pastes the lines they
    recognize.

    Output is deliberately plain ASCII: the operator runs this over an ssh
    session on a Windows console, whose cp1252 stdout raises on box-drawing and
    arrow glyphs.
    """
    out: list[str] = [
        "capital-flow backfill proposal (READ-ONLY - nothing was written)",
    ]
    if db_path:
        out.append(f"db: {db_path}")
    out.append(
        f"bankroll_peak samples: {stats['samples']} | "
        f"flat (pending_exposure=0): {stats['flat_samples']} | "
        f"gaps skipped for an open bet: {stats['skipped_busy_gaps']} | "
        f"residual threshold: ${min_residual_usd:.2f}"
    )
    out.append("")
    if not proposals:
        out.append("no unexplained residual - no flow proposed.")
        return "\n".join(out)

    for p in proposals:
        out.append(
            f"  {p.occurred_at}  {p.kind:<10} {p.amount_usd:+10.2f}   "
            f"(balance {p.balance_delta:+.4f} | settled pnl "
            f"{p.settled_pnl_delta:+.4f} | residual {p.residual_usd:+.4f})"
        )
        out.append(f"      window {p.window_start} .. {p.window_end}")
    out.append("")
    out.append(
        f"{len(proposals)} proposal(s). Confirm each against your funding "
        "record, then run:"
    )
    out.append("")
    for p in proposals:
        note = f"backfill: bankroll_peak residual {p.window_start}..{p.window_end}"
        out.append(
            "python -m cryptobot.cli.operator record-flow "
            f"--kind {p.kind} --amount {p.amount_usd} "
            f'--at "{p.occurred_at}" --note "{note}"'
        )
    return "\n".join(out)
