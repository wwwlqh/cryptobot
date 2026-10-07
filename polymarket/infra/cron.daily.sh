#!/usr/bin/env bash
# Daily WAL checkpoint + disk-free probe for cryptobot.
#
# Invoked by:
#   1. systemd timer `cryptobot-wal-checkpoint.timer` (the actual fire
#      path on AL2023, which has no cron daemon). See
#      deploy/cryptobot-wal-checkpoint.{service,timer}.
#   2. /etc/cron.daily/cryptobot — installed for forward-compat with any
#      future Ubuntu host that does have a cron daemon. Idempotent
#      either way. NOTE: only installed on the POLYMARKET box — /etc/cron.daily
#      runs with no environment, so the script could not learn its box role
#      there (see CRYPTOBOT_BOX_ROLE below); AL2023 has no cron daemon, so the
#      systemd timer is the only real fire path on either box.
#
# 1. PRAGMA wal_checkpoint(TRUNCATE) — without this the WAL can hover near
#    the 200 MB cap (journal_size_limit) and never reclaim the space on disk.
# 2. Disk-free probe — log to journald if free space dips under 1 GB, so
#    CloudWatch can alarm before the bot's own R39 disk gate fires at 200 MB.
#
# This file is NOT a substitute for the in-bot signals pruning (#13) which
# runs every 5 min via the F3 sweep. It's belt-and-suspenders only.
#
# Note (2026-07-31): the decision_table LUT it used to warn about is retired.
# The live bot no longer loads it (or rf_v1) at boot, so there is nothing
# model-side for this timer to refresh — the live scorer is checkpoint_v1,
# rebuilt on the operator's laptop and shipped by deploy.sh.

set -euo pipefail
exec 1> >(logger -t cryptobot-cron)
exec 2>&1

PROJECT_DIR="${PROJECT_DIR:-/home/ec2-user/cryptobot}"

# BOX ROLE (2026-08-01 predict.fun Tokyo cutover). ONE script, two boxes — the
# DB it checkpoints and the unit it defers to both follow the role:
#
#   polymarket (DEFAULT) — data/cryptobot.db          + cryptobot.service
#   predictfun           — data/predictfun_live/...db + cryptobot-predictfun.service
#
# Supplied by install_remote.sh through a systemd drop-in on
# cryptobot-wal-checkpoint.service (this script is `install`ed verbatim and the
# oneshot carries no EnvironmentFile, so the drop-in is the only channel).
# UNSET == polymarket == byte-identical to the pre-role script.
#
# ⚠️ CRYPTOBOT_DB_PATH is deliberately NOT consulted on the predictfun role:
# the shared /etc/cryptobot.env force-pins it to the POLYMARKET DB on every
# box, so honouring it there would checkpoint a database that does not exist
# and the predict.fun WAL would grow forever.
BOX_ROLE="${CRYPTOBOT_BOX_ROLE:-polymarket}"
if [ "$BOX_ROLE" = "predictfun" ]; then
    DB_PATH="${CRYPTOBOT_CRON_DB_PATH:-$PROJECT_DIR/data/predictfun_live/cryptobot_predictfun_live.db}"
    BOT_UNIT="cryptobot-predictfun.service"
else
    DB_PATH="${CRYPTOBOT_CRON_DB_PATH:-${CRYPTOBOT_DB_PATH:-/home/ec2-user/cryptobot/data/cryptobot.db}}"
    BOT_UNIT="cryptobot.service"
fi

# audit #10 (w19oxqf24): the wal-checkpoint timer's unit drops
# `Requires=cryptobot.service` so it can no longer force-start a
# deliberately-stopped real-money bot. Belt-and-suspenders here: if the
# operator has `systemctl stop`ped the bot to inspect the DB / pause
# trading, skip the checkpoint+prune entirely so we never touch the DB
# behind their back. wal_checkpoint is a safe no-op on a stopped DB, but
# skipping it makes the intent explicit and audit-visible in journald.
# (systemctl may be absent on a non-systemd host — there we fall through
# and run as before, matching the cron.daily forward-compat path.)
if command -v systemctl >/dev/null 2>&1 \
        && ! systemctl is-active --quiet "$BOT_UNIT"; then
    echo "SKIP: $BOT_UNIT is not active — operator likely paused trading; skipping WAL checkpoint + audit prune (audit #10)"
    BOT_INACTIVE=1
else
    BOT_INACTIVE=0
fi

if [ "$BOT_INACTIVE" != "1" ] && [ -f "$DB_PATH" ]; then
    echo "wal_checkpoint(TRUNCATE) on $DB_PATH"
    # PRAGMA wal_checkpoint returns 3 ints: busy, log, checkpointed.
    sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(TRUNCATE);" || \
        echo "WARN: wal_checkpoint failed (DB may be locked by running scanner; will retry tomorrow)"
fi

# Per-strategy LIVE deploy plan §U14a: prune the new audit tables to
# 30 days. Driven by the per-strategy LIVE rollout — these tables are
# debugging snapshots that grow unbounded otherwise. Keep transfer_requests
# SUBMITTING rows (active workflow); prune only terminal statuses.
if [ "$BOT_INACTIVE" != "1" ] && [ -f "$DB_PATH" ]; then
    echo "pruning per-strategy LIVE audit tables (>30 days)"
    sqlite3 "$DB_PATH" <<'SQL' || echo "WARN: audit-table prune failed (DB may be locked); retry tomorrow"
DELETE FROM operator_control_events
 WHERE created_at < datetime('now', '-30 days');

DELETE FROM live_readiness_reports
 WHERE created_at < datetime('now', '-30 days');

-- bankroll_peak is high-volume (one row per recon cycle = ~144/day at
-- the default 10-min interval). Keep last 30 days only.
DELETE FROM bankroll_peak
 WHERE sampled_at < datetime('now', '-30 days');

DELETE FROM wallet_reconciliation_records
 WHERE run_id IN (
   SELECT id FROM wallet_reconciliation_runs
    WHERE sampled_at < datetime('now', '-30 days')
 );

DELETE FROM wallet_reconciliation_runs
 WHERE sampled_at < datetime('now', '-30 days');

-- Don't prune SUBMITTING rows — they're active workflow.
DELETE FROM transfer_requests
 WHERE created_at < datetime('now', '-30 days')
   AND status IN ('CONFIRMED', 'FAILED');

-- live_order_attempts is short-lived (linked to a ledger row or terminal
-- FAILED within minutes). Prune terminal/linked rows after 30 days.
DELETE FROM live_order_attempts
 WHERE created_at < datetime('now', '-30 days')
   AND status IN ('LINKED', 'FAILED');
SQL
fi

# Disk-free probe — report when free space crosses warning thresholds.
DISK_FREE_MB=$(df -BM --output=avail "$PROJECT_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -z "$DISK_FREE_MB" ]; then
    echo "WARN: could not read disk free for $PROJECT_DIR — skipping disk probe"
elif [ "$DISK_FREE_MB" -lt 1024 ]; then
    echo "CRITICAL: disk free=${DISK_FREE_MB}MB on $(df -h "$PROJECT_DIR" | tail -1 | awk '{print $1}') — bot's R39 disk gate fires at 200 MB"
elif [ "$DISK_FREE_MB" -lt 3072 ]; then
    echo "WARN: disk free=${DISK_FREE_MB}MB on $(df -h "$PROJECT_DIR" | tail -1 | awk '{print $1}')"
else
    echo "OK: disk free=${DISK_FREE_MB}MB"
fi
