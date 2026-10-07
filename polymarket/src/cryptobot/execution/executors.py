"""Process-wide worker pools shared by the engine and the venue adapter.

``_clob_executor`` is used by both the Polymarket order client and the
pipeline's parallel market lookups. It lives here so the core package does not
have to import the venue package to reach it.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

#: Bounded pool for blocking venue HTTP calls issued off the tick thread:
#: the order client's parallel POSTs and market-info warmups, and the
#: pipeline's threaded fan-out for cold cells.
clob_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="clob")

__all__ = ["clob_executor"]
