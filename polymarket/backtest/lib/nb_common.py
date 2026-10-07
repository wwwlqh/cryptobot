"""Shared fee / P&L / interval maths for the research scripts.

WHAT THIS IS NOW. Four helpers and one constant, imported by
``scripts/report_best_hold.py``. That is the whole surface. Keep it
dependency-light — pandas / numpy only — so a script can import the fee model
without dragging in sklearn (``pool_study`` does).

WHAT IT WAS. A ~660-line "thin-notebook substrate": root resolution, trap-safe
live-snapshot loaders (``load_ledger`` / ``load_signals`` / ``boot_eras``),
grading tables (``reliability_table`` / ``edge_bucket_table`` /
``population_table``), memory-bounded parquet I/O (``slug_index`` /
``parquet_frames``) and a re-export of the raw-L2 replay arms. It was written
for ``polymarket/backtest/{5m,15m}/notebooks/`` — a planned tree that was
never created. The repo
has carried ZERO ``.ipynb`` files the entire time, so no caller ever appeared:
an import census on 2026-08-03 found the only importer of this module was
``tests/test_raw_replay.py``, i.e. it was kept alive solely by a test of another
orphan. All of it was cut that day along with ``lib/raw_replay.py``.

Recover any of it with
``git show <this commit>^:polymarket/backtest/lib/nb_common.py``. The traps
those loaders encoded are NOT lost — they are recorded in ``SOURCES.md`` and in
``lib/pool_study``'s frozen-evaluator block, which is the surviving contract.
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np

__all__ = ["THETA", "fee", "breakeven_wr", "flat_pnl", "wilson"]

# Live edge model: edge = model_p - ask - THETA * ask * (1 - ask). `signals.edge`
# and `ledger.realized_edge` are both computed with this exact constant.
THETA = 0.07  # 2026-08-05: published theta corrected 0.072 -> 0.07


def fee(price: Any) -> Any:
    """Polymarket crypto fee at a binary price: THETA * p * (1 - p)."""
    p = np.asarray(price, dtype=float)
    return THETA * p * (1.0 - p)


def breakeven_wr(price: Any) -> Any:
    """WR* that breaks even at price p after fee: p / (p + (1-p)(1 - THETA*p))."""
    p = np.asarray(price, dtype=float)
    return p / (p + (1.0 - p) * (1.0 - THETA * p))


def flat_pnl(won: Any, price: Any) -> np.ndarray:
    """Net P&L per $1 of cash held to resolution, uniform fee on BOTH sides.

    Uniform theta is a MODEL: the ledger books fee on wins only before 07-24 and
    at k=0.864 on both sides after, so a modelled margin never equals the cash
    basis exactly. Headlines quote the cash basis.
    """
    w, p = np.asarray(won, dtype=float), np.asarray(price, dtype=float)
    return w * (1.0 / p - 1.0) - (1.0 - w) - fee(p)


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval — the right CI for the small-n cohorts here."""
    if not n:
        return (float("nan"), float("nan"))
    p, d = k / n, 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (centre - half, centre + half)
