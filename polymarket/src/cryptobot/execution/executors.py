"""Process-wide worker pools shared by the engine and the venue adapters.

``_clob_executor`` was defined at module scope in ``execution/walker.py``, and
BOTH the Polymarket order client and the core pipeline's Pass-1 cold-cell Gamma
fan-out submitted to it. Phase 2 moved the walker into ``cryptobot_polymarket``,
which would have made core import a venue package for a thread pool — so the
pool itself lives here now and the walker imports it.

It is still ONE pool with the same size and thread-name prefix, so concurrency
behavior is unchanged; only its address moved.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

#: Bounded pool for blocking venue HTTP calls issued off the tick thread:
#: the order client's parallel POSTs and market-info warmups, and the
#: pipeline's threaded fan-out for cold cells.
clob_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="clob")

__all__ = ["clob_executor"]
