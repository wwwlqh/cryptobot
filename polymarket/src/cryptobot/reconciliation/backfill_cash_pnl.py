"""Backfill settled ledger rows to cash-sized realized PnL.

Repairs rows written by the old per-share math, where ``pnl_net`` was
``unit_pnl * bet_size``. Correct PnL treats ``bet_size`` as cash spent,
converts it to shares through the actual fill or VWAP, and applies the theta
fee on winning shares.

This re-books the pre-calibration convention (fee on wins only, via
``parity.cash_pnl``). Do not run it over rows graded with
``execution.grade_fees``: it would remove their fees on losses. Restrict any
run to the old rows with an id or timestamp condition.

It fixes magnitude only, not direction. When ``won_up`` is NULL,
``_infer_won_up`` picks the direction consistent with the stored outcome (WIN
-> the bet's side, LOSS -> the other), so the existing WIN/LOSS is always kept
and a wrongly graded row is never flipped. To fix direction, run
``backfill_polymarket_settlement`` first (it re-derives won_up and outcome from
Gamma's final result), then this script.
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cryptobot.reconciliation._utils import _effective_fill
from cryptobot.strategy.parity import cash_pnl


@dataclass(frozen=True)
class BackfillSummary:
    scanned: int
    updated: int
    skipped: int
    old_total: float
    new_total: float

    @property
    def delta(self) -> float:
        return self.new_total - self.old_total


def _side_is_up(side: str) -> bool:
    return side.upper() in {"YES", "UP"}


def _infer_won_up(side: str, outcome: str, won_up: int | None) -> bool | None:
    # When won_up is NULL we TRUST the stored outcome and derive a
    # self-consistent direction. This intentionally cannot re-grade a
    # mis-settled row — run backfill_polymarket_settlement first to fix
    # won_up/outcome from Gamma, then this script re-scales magnitude.
    if won_up is not None:
        return bool(int(won_up))
    if outcome == "WIN":
        return _side_is_up(side)
    if outcome == "LOSS":
        return not _side_is_up(side)
    return None


def backfill_cash_pnl(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> BackfillSummary:
    """Recompute ``pnl_net`` for all settled WIN/LOSS rows.

    Uses actual ledger fill/VWAP with zero extra slippage haircut. Theta fee is
    preserved through ``cryptobot.strategy.parity.cash_pnl``.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, side, fill_price, walked_vwap, bet_size, outcome, won_up, pnl_net "
        "FROM ledger "
        "WHERE outcome IN ('WIN', 'LOSS') AND pnl_net IS NOT NULL "
        "ORDER BY id ASC"
    ).fetchall()

    scanned = 0
    updated = 0
    skipped = 0
    old_total = 0.0
    new_total = 0.0
    updates: list[tuple[float, str, int, int]] = []

    for row in rows:
        scanned += 1
        old_pnl = float(row["pnl_net"] or 0.0)
        old_total += old_pnl

        fill = _effective_fill(row["fill_price"], row["walked_vwap"])
        won_up = _infer_won_up(str(row["side"]), str(row["outcome"]), row["won_up"])
        bet_size = float(row["bet_size"] or 0.0)
        if fill is None or won_up is None or bet_size <= 0.0:
            skipped += 1
            new_total += old_pnl
            continue

        new_pnl = cash_pnl(
            str(row["side"]),
            fill,
            won_up,
            bet_size,
            slippage_haircut=0.0,
        )
        new_outcome = "WIN" if new_pnl > 0 else "LOSS"
        new_total += float(new_pnl)

        if abs(float(new_pnl) - old_pnl) > 1e-9 or new_outcome != row["outcome"]:
            updated += 1
            updates.append((float(new_pnl), new_outcome, int(won_up), int(row["id"])))

    if not dry_run and updates:
        with conn:
            conn.executemany(
                "UPDATE ledger SET pnl_net = ?, outcome = ?, won_up = ? WHERE id = ?",
                updates,
            )
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) VALUES (?, ?, ?)",
                (
                    "backfill_cash_pnl",
                    "OK",
                    (
                        f"updated={updated} scanned={scanned} skipped={skipped} "
                        f"old_total={old_total:.8f} new_total={new_total:.8f} "
                        f"delta={new_total - old_total:.8f} "
                        f"at={datetime.now(timezone.utc).isoformat(timespec='seconds')}"
                    ),
                ),
            )

    return BackfillSummary(
        scanned=scanned,
        updated=updated,
        skipped=skipped,
        old_total=old_total,
        new_total=new_total,
    )


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
        summary = backfill_cash_pnl(conn, dry_run=args.dry_run)

    mode = "DRY RUN" if args.dry_run else "APPLIED"
    print(
        f"{mode}: scanned={summary.scanned} updated={summary.updated} "
        f"skipped={summary.skipped} old_total={summary.old_total:.8f} "
        f"new_total={summary.new_total:.8f} delta={summary.delta:.8f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
