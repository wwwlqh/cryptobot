"""Settle orphaned PENDING rows at boot.

If the previous process crashed, PENDING rows whose ``window_end_ts`` has
passed would wait for the next 5-minute sweep. Running one sweep at boot makes
the dashboard's numbers correct from the first tick.

A thin wrapper around ``run_resolution_sweep``. With a Polymarket resolver it
settles from Gamma's final outcome; with only price fetchers it uses the legacy
oracle path.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Optional

from cryptobot.execution.resolution import PriceFetcher
from cryptobot.scheduler.resolution_sweep import (
    PolymarketResolutionFetcher,
    run_resolution_sweep,
)

logger = logging.getLogger(__name__)


def reconcile_orphan_pending(
    conn: sqlite3.Connection,
    *,
    polymarket_resolution_fetch: PolymarketResolutionFetcher | None = None,
    chainlink_fetch: PriceFetcher | None = None,
    reference_spot_fetch: PriceFetcher | None = None,
    slippage_haircut_by_sleeve: Optional[dict[str, float]] = None,
    now_ts: float | None = None,
) -> dict:
    """One pass over PENDING rows whose window has expired.

    Returns counters: {settled, cancelled, skipped}. Failed rows stay
    PENDING for the next F3 sweep cycle.
    """
    counters = run_resolution_sweep(
        conn,
        now_ts=now_ts if now_ts is not None else time.time(),
        polymarket_resolution_fetch=polymarket_resolution_fetch,
        chainlink_fetch=chainlink_fetch,
        reference_spot_fetch=reference_spot_fetch,
        slippage_haircut_by_sleeve=slippage_haircut_by_sleeve,
    )
    logger.info(
        "orphan reconcile: settled=%d cancelled=%d skipped=%d",
        counters["settled"], counters["cancelled"], counters["skipped"],
    )
    return counters
