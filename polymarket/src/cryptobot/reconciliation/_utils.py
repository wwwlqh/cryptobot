"""Shared helpers for the reconciliation backfill scripts."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from cryptobot.venues.types import PolymarketResolution

ResolutionFetcher = Callable[[str, int, int], "Optional[PolymarketResolution]"]


def _effective_fill(
    fill_price: float | None, walked_vwap: float | None
) -> float | None:
    """Pick the fill price to size shares from: walked VWAP wins over the
    quoted fill_price. Returns None when neither is a usable positive number."""
    fill = walked_vwap if walked_vwap is not None else fill_price
    if fill is None:
        return None
    fill = float(fill)
    if fill <= 0.0:
        return None
    return fill


@dataclass(frozen=True)
class SettlementBackfillSummary:
    """Row counters and PnL totals for one settlement backfill run."""

    scanned: int
    updated: int
    skipped: int
    unresolved: int
    old_total: float
    new_total: float

    @property
    def delta(self) -> float:
        return self.new_total - self.old_total


def run_settlement_backfill(
    conn: sqlite3.Connection,
    *,
    where_sql: str,
    params: tuple = (),
    resolution_fetch: ResolutionFetcher,
    stage: str,
    dry_run: bool = False,
) -> SettlementBackfillSummary:
    """Shared settle/regrade loop for settlement backfills.

    Selects ledger rows matching ``where_sql``, re-resolves each via
    ``resolution_fetch(coin, interval_min, window_start_ts)`` and books
    WIN/LOSS with ``execution.grade_fees.graded_cash_pnl``, the same function
    the resolution sweep uses, so a backfilled row and a sweep-settled row
    grade identically. A row whose market is not decisively RESOLVED is left
    alone and counted ``unresolved`` (a PENDING row is never settled on an
    ambiguous payload); rows with no usable fill or size are counted
    ``skipped``. Unless ``dry_run``, the UPDATEs and a ``pipeline_health`` OK
    row (under ``stage``) commit together.

    These are regrade tools: callers' WHERE clauses can select WIN/LOSS rows
    too, which are then re-booked with the calibrated fee. Make sure that is
    intended for older rows.
    """
    from cryptobot.venues.types import RESOLUTION_RESOLVED
    from cryptobot.execution.grade_fees import graded_cash_pnl

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, coin, interval_min, window_start_ts, side, fill_price, "
        "       walked_vwap, bet_size, outcome, won_up, pnl_net, resolution_source, "
        "       venue, fee_rate_bps "
        "FROM ledger "
        f"WHERE {where_sql} "
        "ORDER BY id ASC",
        params,
    ).fetchall()

    scanned = 0
    updated = 0
    skipped = 0
    unresolved = 0
    old_total = 0.0
    new_total = 0.0
    updates: list[tuple[str, int, float, str, float, float, str, int]] = []

    for row in rows:
        scanned += 1
        old_pnl = float(row["pnl_net"] or 0.0)
        old_total += old_pnl

        fill = _effective_fill(row["fill_price"], row["walked_vwap"])
        bet_size = float(row["bet_size"] or 0.0)
        if fill is None or bet_size <= 0.0:
            skipped += 1
            new_total += old_pnl
            continue

        result = resolution_fetch(
            str(row["coin"]),
            int(row["interval_min"]),
            int(row["window_start_ts"]),
        )
        if (
            result is None
            or result.status != RESOLUTION_RESOLVED
            or result.won_up is None
        ):
            unresolved += 1
            new_total += old_pnl
            continue

        won_up = bool(int(result.won_up))
        result_price = (
            float(result.up_price)
            if result.up_price is not None
            else float(int(won_up))
        )
        source = result.source
        pnl_net = graded_cash_pnl(
            str(row["side"]),
            fill,
            won_up,
            bet_size,
            slippage_haircut=0.0,
        )
        outcome = "WIN" if pnl_net > 0 else "LOSS"
        new_total += float(pnl_net)

        needs_update = (
            str(row["outcome"]) != outcome
            or int(row["won_up"] or 0) != int(won_up)
            or abs(float(pnl_net) - old_pnl) > 1e-9
            or str(row["resolution_source"] or "") != source
        )
        if needs_update:
            updated += 1
            updates.append(
                (
                    outcome,
                    int(won_up),
                    result_price,
                    source,
                    float(pnl_net),
                    result_price,
                    source,
                    int(row["id"]),
                )
            )

    if not dry_run and updates:
        at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with conn:
            conn.executemany(
                "UPDATE ledger SET outcome = ?, won_up = ?, resolution_price = ?, "
                "resolution_source = ?, pnl_net = ?, "
                "polymarket_resolution_price = ?, polymarket_resolution_source = ? "
                "WHERE id = ?",
                updates,
            )
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) VALUES (?, ?, ?)",
                (
                    stage,
                    "OK",
                    (
                        f"updated={updated} scanned={scanned} skipped={skipped} "
                        f"unresolved={unresolved} old_total={old_total:.8f} "
                        f"new_total={new_total:.8f} delta={new_total - old_total:.8f} "
                        f"at={at}"
                    ),
                ),
            )

    return SettlementBackfillSummary(
        scanned=scanned,
        updated=updated,
        skipped=skipped,
        unresolved=unresolved,
        old_total=old_total,
        new_total=new_total,
    )
