#!/usr/bin/env bash
# Forwarding shim — the deploy moved to polymarket/infra/deploy.sh in the
# 2026-07-28 restructure (PATH_MAP §Phase-1). THIS path stays alive because it
# is the operator's allowlisted deploy entrypoint (.claude/settings.local.json)
# and operator muscle memory. Keep it a pure exec; all logic lives downstream.
exec bash "$(dirname "$0")/../polymarket/infra/deploy.sh" "$@"
