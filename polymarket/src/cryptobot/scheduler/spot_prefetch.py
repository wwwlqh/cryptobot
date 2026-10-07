"""A7 latency: spot prefetch-ahead thread.

The scanner tick pays a ~50-100ms inline spot GET at the top of every
network-active tick (``deps.fetch_spots`` at ``pipeline.run_scanner_tick``).
The spot adapter (`data.binance`) caches one batch per
``now_ts // tick_seconds`` bucket behind a lock, and accepts an explicit
``now_ts`` — so a background thread can populate the EXACT bucket the
upcoming tick will read, turning the tick's fetch into a zero-network cache
hit.

Timing: the thread wakes ``lead_ms`` before each wall-clock second boundary
and fetches with ``now_ts = next_boundary``. With the A6 boundary-aligned
scheduler the tick then fires at the boundary and reads the cached batch.
The scored spot is at most ``lead_ms`` older than the inline fetch would
have produced — comparable to the in-flight time of the inline GET itself —
and the freshness stamp (`publish_time`) still ages normally, so the
stale_spot gate semantics are unchanged.

Failure containment: a prefetch failure marks the bucket failed in the
adapter cache exactly as an inline failure would (same function, same
arguments); the tick then behaves as it does today on a spot outage. The
thread is a daemon and is stopped via ``stop()`` on shutdown, but an
un-stopped thread cannot keep the process alive.
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
