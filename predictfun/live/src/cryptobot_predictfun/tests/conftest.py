"""Fixture bridge for the predict.fun venue test suite.

These tests moved out of ``core/src/cryptobot/tests/`` in Phase 2 and left
their fixtures behind. Rather than fork them (two copies of the schema-applied
``db`` fixture drift), the shared ones are re-exported from the core conftest —
pytest treats a fixture imported into a conftest namespace as defined there,
autouse ones included.
"""

from __future__ import annotations

from cryptobot.tests.conftest import (  # noqa: F401  re-exported fixtures
    _clear_gamma_market_cache,
    _neutralize_entry_skip,
    db,
    tmp_db,
)
