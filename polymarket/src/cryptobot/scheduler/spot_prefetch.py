"""Spot prefetch thread.

The tick otherwise makes a ~50-100ms spot fetch at the start of every tick. The
spot adapter (``data.binance``) caches one batch per time bucket and accepts an
explicit ``now_ts``, so a background thread can fill exactly the bucket the
next tick will read.

The thread wakes ``lead_ms`` before each second boundary and fetches with
``now_ts = next_boundary``; the second-aligned tick then reads the cached
batch. The spot is at most ``lead_ms`` older than an inline fetch would return,
and its ``publish_time`` ages normally, so stale_spot works as before.

A failed prefetch marks the bucket failed in the adapter cache, exactly like a
failed inline fetch, and the tick behaves as during any spot outage. The thread
is a daemon, stopped with ``stop()`` at shutdown.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class SpotPrefetcher:
    """Daemon thread that pre-warms the spot bucket cache ahead of each tick.

    ``fetch_spots`` must be the SAME callable (same underlying adapter +
    ``tick_seconds`` bucket math) the tick's ``deps.fetch_spots`` uses, so the
    bucket keys line up.
    """

    def __init__(
        self,
        fetch_spots: Callable[..., Optional[dict]],
        *,
        tick_seconds: int,
        lead_ms: int,
    ) -> None:
        self._fetch_spots = fetch_spots
        self._tick_seconds = max(1, int(tick_seconds))
        self._lead_s = max(0.02, min(0.9, float(lead_ms) / 1000.0))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="cryptobot-spot-prefetch", daemon=True
        )
        self._thread.start()
        logger.info(
            "spot prefetch-ahead started (lead=%.0fms, bucket=%ds)",
            self._lead_s * 1000.0,
            self._tick_seconds,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            next_boundary = math.floor(now) + 1.0
            wake_at = next_boundary - self._lead_s
            if wake_at <= now:
                # Inside the lead window already (e.g. a slow previous fetch)
                # — target the boundary after next rather than fetching late.
                next_boundary += 1.0
                wake_at += 1.0
            # Event.wait doubles as an interruptible sleep.
            if self._stop.wait(timeout=wake_at - now):
                return
            try:
                self._fetch_spots(
                    now_ts=float(next_boundary),
                    tick_seconds=self._tick_seconds,
                )
            except Exception:  # noqa: BLE001 — prefetch is best-effort
                logger.debug("spot prefetch failed", exc_info=True)
