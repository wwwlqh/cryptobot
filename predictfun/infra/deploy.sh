#!/usr/bin/env bash
# predictfun/infra/deploy.sh — deploy driver for the PREDICT.FUN box.
#
# THE PREDICT.FUN BOX IS PREDICT.FUN-ONLY.
# ap-northeast-1 (Tokyo), Amazon Linux 2023, ec2-user. It runs the predict.fun
# scanner as REAL MONEY on its own SQLite ledger and serves its own operator
# console on loopback :8801. The POLYMARKET scanner unit (cryptobot.service)
# must NEVER run here: there is no Polymarket signing key on this box, no
# Polymarket ledger, and a second live consumer of the shared strategy would be
# a split brain. CRYPTOBOT_BOX_ROLE=predictfun is what enforces that —
# install_remote.sh does not even lay the poly unit file down under this role.
#
# ⚠️ SINGLE-KEY-CONSUMER RULE: predict.fun allows ~240 requests/min per API key.
# Two boxes scanning with the SAME key is ~480/min AND two processes trading the
# same wallet. STOP any other predict.fun units BEFORE arming this box.
#
# This is a thin wrapper: everything real lives in polymarket/infra/deploy.sh
# (the ONE deploy driver — staging, test gate, tarball, remote install). It only
# pins the role and this box's defaults.
#
# Usage, from the repo root:
#
#   bash predictfun/infra/deploy.sh
#
# Every default below is env-overridable, e.g.:
#
#   EC2_HOST=my-other-host PEM_PATH=~/keys/pf.pem bash predictfun/infra/deploy.sh
#
# All of the parent driver's flags still apply (SKIP_TESTS + CONFIRM_UNSAFE_DEPLOY,
# RESET_RECORDS, EC2_USER, SERVICE_USER, PROJECT_DIR) — pass them the same way.
# ⚠️ RESET_RECORDS=1 here cuts over the PREDICT.FUN accounting record.

set -euo pipefail

# predictfun/infra/deploy.sh -> two levels up is the repo root.
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

export CRYPTOBOT_BOX_ROLE="predictfun"
: "${EC2_HOST:=<your-ec2-host>}"
: "${PEM_PATH:=$REPO_ROOT/predictfun/predict.fun.pem}"
export EC2_HOST PEM_PATH

echo "==> predict.fun (Tokyo) deploy — role=$CRYPTOBOT_BOX_ROLE host=$EC2_HOST"
exec bash "$REPO_ROOT/polymarket/infra/deploy.sh" "$@"
