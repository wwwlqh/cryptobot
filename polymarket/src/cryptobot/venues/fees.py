"""Polymarket taker fee.

Polymarket charges ``theta * p * (1 - p)`` per share on crypto markets, with
``theta = config.POLY_FEE_THETA = 0.07``. The same theta is pinned in
``backtest.engine.sweep_lib`` and ``strategy.parity``;
``tests/test_audit_parity.py`` fails if they drift apart.
"""

from __future__ import annotations


def fee_per_share(ask: float, *, theta: float) -> float:
    """Taker fee per share bought at ``ask``."""
    return theta * ask * (1.0 - ask)


__all__ = ["fee_per_share"]
