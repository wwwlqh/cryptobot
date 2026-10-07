"""Regrade settled ledger rows against Polymarket's finalized outcome.

The backtest's ``won_up_polymarket`` label comes from Gamma's finalized
``outcomePrices`` for markets that Polymarket settled from Chainlink Data
Streams. This backfill repairs legacy live rows that were settled from the
now-deleted oracle-spot fallback path (``resolution_source='pyth_fallback'``)
so the ledger uses the same settlement label family.

The row loop / UPDATE / pipeline_health bookkeeping is shared with the
predict.fun twin via :func:`cryptobot.reconciliation._utils.run_settlement_backfill`;
only the WHERE predicate and the resolution fetcher are venue-specific.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Callable, Optional

from cryptobot_polymarket.gamma import (
    PolymarketResolution,
    fetch_resolved_outcome,
)
from cryptobot.reconciliation._utils import (
    SettlementBackfillSummary,
    run_settlement_backfill,
)

PolymarketResolutionFetcher = Callable[[str, int, int], Optional[PolymarketResolution]]

# Back-compat alias — the summary shape is shared across the venue twins.
PolymarketSettlementBackfillSummary = SettlementBackfillSummary


def backfill_polymarket_settlement(
    conn: sqlite3.Connection,
    *,
    resolution_fetch: PolymarketResolutionFetcher | None = None,
    dry_run: bool = False,
    include_already_polymarket: bool = False,
) -> SettlementBackfillSummary:
    """Recompute WIN/LOSS rows from finalized Polymarket outcomes.

    By default this targets rows whose ``resolution_source`` is not already
    ``polymarket_gamma``. Rows that Gamma has not finalized stay unchanged and
    are counted as ``unresolved``.
    """
    if resolution_fetch is None:
        resolution_fetch = lambda coin, interval, start: fetch_resolved_outcome(
            coin,
            interval_min=interval,
            window_start_ts=start,
        )

    source_predicate = ""
    if not include_already_polymarket:
        source_predicate = (
            "AND (resolution_source IS NULL OR resolution_source != 'polymarket_gamma') "
        )
    return run_settlement_backfill(
        conn,
        where_sql=f"outcome IN ('WIN', 'LOSS') {source_predicate}",
        resolution_fetch=resolution_fetch,
        stage="backfill_polymarket_settlement",
        dry_run=dry_run,
    )


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Path to cryptobot SQLite DB")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--include-already-polymarket",
        action="store_true",
        help="also recheck rows already marked resolution_source=polymarket_gamma",
    )
    args = parser.parse_args()

    with _connect(Path(args.db)) as conn:
        summary = backfill_polymarket_settlement(
            conn,
            dry_run=args.dry_run,
            include_already_polymarket=args.include_already_polymarket,
        )

    mode = "DRY RUN" if args.dry_run else "APPLIED"
    print(
        f"{mode}: scanned={summary.scanned} updated={summary.updated} "
        f"skipped={summary.skipped} unresolved={summary.unresolved} "
        f"old_total={summary.old_total:.8f} new_total={summary.new_total:.8f} "
        f"delta={summary.delta:.8f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
