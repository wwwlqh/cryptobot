"""Live-trading settings.

Kept out of ``config.py`` because they only concern live execution and have
nothing to do with backtest parity.

Settings with an env override get it through
``cryptobot.runtime_secrets.Config``; the constants here are the defaults.
"""

from __future__ import annotations


# DRY -> LIVE switches on the same wallet within this window need ``--force``
# (CLI only; the dashboard does not accept it). LIVE -> DRY always works.
WALLET_MODE_FLIP_RATE_LIMIT_S: int = 60


# Allowed difference between the on-chain and CLOB balances. Also available as
# ``cryptobot.runtime_secrets.Config.live_balance_tolerance_usd``.
LIVE_BALANCE_TOLERANCE_USD: float = 0.25


# A READY ``live_readiness_reports`` row older than this is stale and DRY ->
# LIVE switches are refused. The readiness job runs every TTL/2, so a fresh row
# is normally available.
LIVE_READINESS_CACHE_TTL_S: int = 120


# The reconciliation job writes ``wallet_reconciliation_runs`` every
# WALLET_SNAPSHOT_INTERVAL_MINUTES; DRY -> LIVE switches are refused when the
# latest run is older than this.
WALLET_SNAPSHOT_FRESHNESS_TTL_S: int = 900
