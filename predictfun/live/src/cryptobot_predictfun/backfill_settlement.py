"""Settle predict.fun ledger rows against the markets API resolved outcome.

The predict.fun counterpart of
:mod:`cryptobot_polymarket.backfill_settlement`. It settles/regrades
predict.fun ledger rows (``venue='predictfun'``) from
:func:`cryptobot_predictfun.markets.fetch_resolved_outcome` — the API-outcome
path (the published UP/DOWN winner). Rows the API has not finalized stay
untouched and are counted as ``unresolved`` (PENDING-preserving, fail-closed,
same semantics as Polymarket's backfill).

The Binance-1s-candle administrator fallback is NOT implemented here — only the
API-outcome path (see the candle-fallback comment block at the end of
``cryptobot_predictfun.markets`` for the documented, deliberately-unbuilt
candle rule).

The row loop / UPDATE / pipeline_health bookkeeping is shared with the
Polymarket twin via :func:`cryptobot.reconciliation._utils.run_settlement_backfill`;
only the WHERE predicate and the resolution fetcher are venue-specific.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Callable, Optional

import httpx

from cryptobot.venues.types import PolymarketResolution
from cryptobot_predictfun.markets import fetch_resolved_outcome
from cryptobot.reconciliation._utils import (
    SettlementBackfillSummary,
    run_settlement_backfill,
)

PredictFunResolutionFetcher = Callable[[str, int, int], Optional[PolymarketResolution]]


def backfill_predictfun_settlement(
    conn: sqlite3.Connection,
    *,
    resolution_fetch: PredictFunResolutionFetcher | None = None,
    dry_run: bool = False,
) -> SettlementBackfillSummary:
    """Settle/regrade ``venue='predictfun'`` rows from the API resolved outcome.

    Targets PENDING/WIN/LOSS predict.fun rows. A row whose market the API reports
    RESOLVED is booked WIN/LOSS with the calibrated venue-dispatched grade
    formula (``execution.grade_fees.graded_cash_pnl`` — predict.fun rows book
    the TENT off the row's own ``fee_rate_bps``, the same formula the
    resolution sweep books; 2026-07-23 calibrated fee grading); every other
    case (still trading / unmappable / HTTP error => ``fetch_resolved_outcome``
    returns OPEN) is left unchanged and counted ``unresolved`` — a PENDING row
    is NEVER mis-settled on an ambiguous payload.

    The default fetcher opens ONE httpx client for the whole run and memoizes
    the resolution per (coin, interval, window): many ledger rows share a
    settlement window, and a per-row client + re-fetch would hammer the
    240 req/min API-key budget for identical answers.
    """
    owned_client: Optional[httpx.Client] = None
    if resolution_fetch is None:
        owned_client = httpx.Client(timeout=3.0)
        cache: dict[tuple[str, int, int], Optional[PolymarketResolution]] = {}
        run_client = owned_client

        def resolution_fetch(coin: str, interval: int, start: int):
            key = (str(coin), int(interval), int(start))
            if key not in cache:
                cache[key] = fetch_resolved_outcome(
                    coin,
                    interval_min=interval,
                    window_start_ts=start,
                    client=run_client,
                )
            return cache[key]

    try:
        return run_settlement_backfill(
            conn,
            # Only predict.fun rows. The venue column defaults 'polymarket', so
            # a polymarket row is never touched here even on a shared DB.
            where_sql="outcome IN ('PENDING', 'WIN', 'LOSS') AND venue = 'predictfun'",
            resolution_fetch=resolution_fetch,
            stage="backfill_predictfun_settlement",
            dry_run=dry_run,
        )
    finally:
        if owned_client is not None:
            owned_client.close()


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Path to cryptobot SQLite DB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with _connect(Path(args.db)) as conn:
        summary = backfill_predictfun_settlement(conn, dry_run=args.dry_run)

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
