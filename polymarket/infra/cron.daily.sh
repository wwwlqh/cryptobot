#!/usr/bin/env bash
# Daily WAL checkpoint, audit-table pruning and disk-free check.
#
# Fired by the systemd timer cryptobot-wal-checkpoint.timer (Amazon Linux 2023
# has no cron daemon); also installed in /etc/cron.daily for hosts that do.
#
# 1. PRAGMA wal_checkpoint(TRUNCATE): reclaims WAL space on disk, which
#    otherwise can sit near the 200 MB journal_size_limit.
# 2. Prunes audit tables older than 30 days.
# 3. Logs when free disk drops under 3 GB / 1 GB, well before the bot's own
#    disk gate stops trading at 200 MB.
#
# The bot prunes the signals table itself on every resolution sweep; this is
# an extra safety net.

set -euo pipefail
exec 1> >(logger -t cryptobot-cron)
exec 2>&1

PROJECT_DIR="${PROJECT_DIR:-/home/ec2-user/cryptobot}"
DB_PATH="${CRYPTOBOT_CRON_DB_PATH:-${CRYPTOBOT_DB_PATH:-$PROJECT_DIR/data/cryptobot.db}}"

# If the operator stopped the bot (to inspect the DB or pause trading), leave
# the database alone.
if command -v systemctl >/dev/null 2>&1         && ! systemctl is-active --quiet cryptobot.service; then
    echo "SKIP: cryptobot.service is not active; skipping WAL checkpoint and prune"
    BOT_INACTIVE=1
else
    BOT_INACTIVE=0
fi

if [ "$BOT_INACTIVE" != "1" ] && [ -f "$DB_PATH" ]; then
    echo "wal_checkpoint(TRUNCATE) on $DB_PATH"
    sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(TRUNCATE);" ||         echo "WARN: wal_checkpoint failed (DB may be locked); will retry tomorrow"
fi

# Prune audit tables to 30 days. SUBMITTING transfers are an active workflow
# and are kept; only terminal statuses are pruned.
if [ "$BOT_INACTIVE" != "1" ] && [ -f "$DB_PATH" ]; then
    echo "pruning audit tables (>30 days)"
    sqlite3 "$DB_PATH" <<'SQL' || echo "WARN: audit-table prune failed (DB may be locked); retry tomorrow"
DELETE FROM operator_control_events
 WHERE created_at < datetime('now', '-30 days');

DELETE FROM live_readiness_reports
 WHERE created_at < datetime('now', '-30 days');

-- bankroll_peak gets ~144 rows/day at the default 10-minute interval.
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

-- live_order_attempts rows are linked or FAILED within minutes.
DELETE FROM live_order_attempts
 WHERE created_at < datetime('now', '-30 days')
   AND status IN ('LINKED', 'FAILED');
SQL
fi

# Disk-free check.
DISK_FREE_MB=$(df -BM --output=avail "$PROJECT_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -z "$DISK_FREE_MB" ]; then
    echo "WARN: could not read disk free for $PROJECT_DIR — skipping disk probe"
elif [ "$DISK_FREE_MB" -lt 1024 ]; then
    echo "CRITICAL: disk free=${DISK_FREE_MB}MB on $(df -h "$PROJECT_DIR" | tail -1 | awk '{print $1}') (the bot stops trading at 200 MB)"
elif [ "$DISK_FREE_MB" -lt 3072 ]; then
    echo "WARN: disk free=${DISK_FREE_MB}MB on $(df -h "$PROJECT_DIR" | tail -1 | awk '{print $1}')"
else
    echo "OK: disk free=${DISK_FREE_MB}MB"
fi
