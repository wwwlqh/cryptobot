"""Per-strategy LIVE deploy knobs — live-only tunables.

Lives in `execution/` rather than `config.py` because these values are
specific to the LIVE execution surface (D-LIVE-1..D-LIVE-29) and bear no
relation to backtest parity. Keeping them out of `config.py` makes the
dry-run-only test surface importable without touching live-execution
internals.

Every knob carries an env override via `cryptobot.runtime_secrets.Config`
when one exists; module-level constants below are the boot defaults.
"""

from __future__ import annotations


# D-LIVE-19: asymmetric mode-flip rate limit. DRY → LIVE flips on the same
# wallet within this window require `--force` (CLI only — the dashboard
# endpoint does NOT accept a `force` field, defense-in-depth).
# LIVE → DRY flips ignore this cooldown entirely (emergency halt is
# always allowed).
WALLET_MODE_FLIP_RATE_LIMIT_S: int = 60


# D-LIVE-4: chain ↔ CLOB balance drift tolerance. When the live funder
# balance falls more than this much below SUM(group_caps), every LIVE
# wallet halts immediately (require_wallet_can_go_live raises) and a
# CRITICAL notify fires. Mirrors `cryptobot.runtime_secrets.Config
# .live_balance_tolerance_usd` for callers that want the constant alone.
LIVE_BALANCE_TOLERANCE_USD: float = 0.25


# D-LIVE-5 readiness TTL: time after which a `live_readiness_reports.status
# = READY` row is considered stale and DRY → LIVE flips refuse. The
# scheduler (U9 in Phase 2) re-evaluates readiness every TTL/2 so a fresh
# row is always available when the operator presses promote.
LIVE_READINESS_CACHE_TTL_S: int = 120


# Wallet reconciliation freshness — the scheduler (U7 in Phase 2) writes
# `wallet_reconciliation_runs` every WALLET_SNAPSHOT_INTERVAL_MINUTES; the
# guard rejects DRY → LIVE flips when the latest run is older than this.
WALLET_SNAPSHOT_FRESHNESS_TTL_S: int = 900


# D-LIVE-28 pair-leg post-fill imbalance tolerance. Both filled legs of a
# pair entry must end within this ratio (0.03 = 3%) of each other in USD,
# or the bot attempts ONE rebalance pass. Raise to 0.05 if real Polymarket
# fills routinely exceed 3% on the operator's coin universe.
PAIR_LEG_MAX_IMBALANCE_PCT: float = 0.03
