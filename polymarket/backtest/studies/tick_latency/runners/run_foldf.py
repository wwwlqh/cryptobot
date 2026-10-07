"""Fold-F runner: patch te.FOLDS to admit 'F', then delegate to the topup driver.

UNION universe (no band filter) -- kept for the union-vs-MID reference arm only.
For the live twin use run_midonly.py / run_size.py instead;
this file replays fold B n=7,386 where the MID-only twin is n=5,842.
Fold F needs --score-path .../optimize_runs/scored_foldEF_20260721.parquet.
"""
import sys

from polymarket.backtest.engine import tick_exit004 as te

te.FOLDS = ("B", "C", "D", "E", "F")

from polymarket.backtest.scripts import tick_topup_foldBE as m  # noqa: E402

if __name__ == "__main__":
    # Guarded: without this the module ran a full replay on *import*, and with no
    # --tag the driver writes foldBE_stop_report_400.json -- straight over the
    # audit-pinned report that audit/live_artifacts.py:127 checks.
    raise SystemExit(m.main(sys.argv[1:]))
