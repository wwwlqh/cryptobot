"""Operator CLI surface for cryptobot live execution.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U11a/U11b:

  * LIVE controls: `operator`, `live_go_no_go`, `set_wallet_mode`.
  * Operator convenience: `env_parity`.

The venue-specific entry points (`cbot-poly-wallet-diagnostics`,
`cbot-derive-poly-creds`, and the predict.fun equivalents) moved to their
venue packages in the Phase-2 restructure and are NOT in this package —
see `[project.scripts]` in `pyproject.toml` for the frozen console-script
names and their current module paths.

§U4 owns `set_wallet_mode` — the scripted DRY/LIVE flip with audit
events. Phase 1 ships it so the operator can flip a wallet to LIVE
the moment Phase 2 deploys the readiness writers.
"""
