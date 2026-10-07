#!/usr/bin/env bash
# install_remote.sh - runs on the EC2 instance (called by deploy.sh).
#
# Idempotent; safe to re-run on every deploy:
#   * creates the venv if missing and installs the pinned runtime deps
#   * lays down the systemd units and seeds /etc/cryptobot.env on first install
#   * pins the deployment settings below in /etc/cryptobot.env
#   * restarts the bot and waits for "boot OK"

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/cryptobot}"
SERVICE_USER="${SERVICE_USER:-$(whoami)}"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="/var/log/cryptobot"
ENV_FILE="/etc/cryptobot.env"

echo "==> install_remote: PROJECT_DIR=$PROJECT_DIR  USER=$SERVICE_USER"

# ---- 1. system packages -----------------------------------------------------
if [ -f /etc/amazon-linux-release ] || grep -q "Amazon Linux" /etc/os-release 2>/dev/null; then
    PKG_INSTALL="sudo dnf install -y"
    PYTHON_PKG="python3.11 python3.11-pip"
    PYTHON_BIN="python3.11"
    SQLITE_PKG="sqlite"
elif grep -q "Ubuntu" /etc/os-release 2>/dev/null; then
    PKG_INSTALL="sudo apt-get install -y"
    sudo apt-get update -q
    PYTHON_PKG="python3 python3-venv python3-pip"
    PYTHON_BIN="python3"
    SQLITE_PKG="sqlite3"
else
    echo "ERROR: unsupported OS (Amazon Linux 2023 and Ubuntu are supported)."
    cat /etc/os-release 2>/dev/null || true
    exit 1
fi

echo "==> installing system packages"
$PKG_INSTALL $PYTHON_PKG $SQLITE_PKG gcc tar gzip

# ---- 2. venv + runtime deps -------------------------------------------------
if [ ! -d "$VENV_DIR" ]; then
    echo "==> creating venv at $VENV_DIR"
    $PYTHON_BIN -m venv "$VENV_DIR"
fi
echo "==> installing runtime deps"
"$VENV_DIR/bin/pip" install --quiet --upgrade pip wheel
"$VENV_DIR/bin/pip" install --quiet -r "$PROJECT_DIR/deploy/requirements.runtime.txt"

# ---- 3. data + log directories ----------------------------------------------
mkdir -p \
    "$PROJECT_DIR/data" \
    "$PROJECT_DIR/data/models" \
    "$PROJECT_DIR/runtime/config" \
    "$PROJECT_DIR/runtime/data"
sudo mkdir -p "$LOG_DIR"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$LOG_DIR"
sudo install -m 0644 "$PROJECT_DIR/deploy/cryptobot-scanner.logrotate" \
    /etc/logrotate.d/cryptobot-scanner
find "$PROJECT_DIR/dashboard" -type d -name "__pycache__" -prune -exec rm -rf -- {} +

# ---- 4. systemd units + env file --------------------------------------------
install_unit() {
    sudo sed "s|^User=ec2-user|User=$SERVICE_USER|; s|^Group=ec2-user|Group=$SERVICE_USER|; s|/home/ec2-user/cryptobot|$PROJECT_DIR|g" \
        "$PROJECT_DIR/deploy/$1" \
        | sudo tee "/etc/systemd/system/$1" > /dev/null
}

echo "==> installing systemd units"
for unit in cryptobot.service cryptobot-wal-checkpoint.service \
            cryptobot-wal-checkpoint.timer cryptobot-failure-alert.service; do
    install_unit "$unit"
done
# The daily job learns the project dir from this drop-in (the script itself is
# installed verbatim).
sudo mkdir -p /etc/systemd/system/cryptobot-wal-checkpoint.service.d
printf '[Service]\nEnvironment=PROJECT_DIR=%s\n' "$PROJECT_DIR" \
    | sudo tee /etc/systemd/system/cryptobot-wal-checkpoint.service.d/project.conf > /dev/null
# AL2023 has no cron daemon (the systemd timer is what fires); this copy is
# for hosts that do.
sudo install -m 0755 "$PROJECT_DIR/deploy/cron.daily.sh" /etc/cron.daily/cryptobot

if [ ! -f "$ENV_FILE" ]; then
    echo "==> seeding $ENV_FILE from the example (first install)"
    sudo sed "s|/home/ec2-user/cryptobot|$PROJECT_DIR|g" \
        "$PROJECT_DIR/deploy/cryptobot.env.example" \
        | sudo tee "$ENV_FILE" > /dev/null
    sudo chown root:"$SERVICE_USER" "$ENV_FILE"
    sudo chmod 0640 "$ENV_FILE"
else
    echo "==> $ENV_FILE exists - keeping operator values"
fi

# Set KEY=VALUE, replacing any existing line.
set_env_line() {
    if sudo grep -q "^$1=" "$ENV_FILE"; then
        sudo sed -i "s|^$1=.*|$1=$2|" "$ENV_FILE"
    else
        echo "$1=$2" | sudo tee -a "$ENV_FILE" > /dev/null
    fi
}

# Set KEY=VALUE only if the key is missing or empty.
ensure_env_line() {
    if sudo grep -q "^$1=" "$ENV_FILE"; then
        current="$(sudo grep "^$1=" "$ENV_FILE" | tail -n 1 | cut -d= -f2-)"
        if [ -z "$current" ]; then
            sudo sed -i "s|^$1=.*|$1=$2|" "$ENV_FILE"
        fi
    else
        echo "$1=$2" | sudo tee -a "$ENV_FILE" > /dev/null
    fi
}

# Deployment layout.
set_env_line PYTHONPATH "$PROJECT_DIR/src"
# The deployed bundle has no pyproject.toml for config.py to find the root by,
# so pin it; a wrong root would put the SQLite ledger somewhere else.
set_env_line CRYPTOBOT_ROOT "$PROJECT_DIR"
set_env_line PYTHONWARNINGS "ignore::UserWarning:sklearn.utils.parallel"
set_env_line CRYPTOBOT_DB_PATH "$PROJECT_DIR/data/cryptobot.db"
set_env_line CRYPTOBOT_V6_CONFIG "$PROJECT_DIR/runtime/config/twap_symmetric_live.json"
set_env_line CRYPTOBOT_DASHBOARD_DIR "$PROJECT_DIR/dashboard"

# Strategy and risk. These are pinned on every deploy so the box always runs
# the tested contract; edit them here, not in $ENV_FILE. The exposure cap and
# drawdown halt must equal the strategy config's values or boot fails.
set_env_line CRYPTOBOT_SUBMINUTE_ENABLED "1"
set_env_line CRYPTOBOT_COINS "BTC,ETH,SOL,XRP"
set_env_line CRYPTOBOT_INTERVAL_5M_ENABLED "1"
set_env_line CRYPTOBOT_ENABLE_TOPUPS "0"
set_env_line CRYPTOBOT_MIN_BET_USD "1.0"
set_env_line CRYPTOBOT_SHARED_EXPOSURE_CAP "1.0"
set_env_line CRYPTOBOT_MAX_DD_HALT_FRAC "0.40"
set_env_line CRYPTOBOT_V2_BASE_FRAC_15M "0.010"
set_env_line CRYPTOBOT_V2_BASE_FRAC_5M "0.010"
# Floor 1.0 turns the drawdown brake off; the hard halt above is the guard.
set_env_line CRYPTOBOT_V2_BRAKE_SCALE "0.60"
set_env_line CRYPTOBOT_V2_BRAKE_FLOOR "1.0"
set_env_line CRYPTOBOT_V2_CONC_DOWNWEIGHT "1"

# Execution. A rejected FAK leaves the market open, so each later qualifying
# second retries on a fresh book at its own price plus a $0.01 pad.
set_env_line CRYPTOBOT_V2_REJECT_RETRY "1"
set_env_line CRYPTOBOT_V2_FAK_PAD "0.01"
set_env_line CRYPTOBOT_V2_RETRY_REPRICE "1"
set_env_line CRYPTOBOT_PHASE_B_PARALLEL_POST "1"

# Fill-rate breaker: disarmed (a minimum fill rate of 0 and a loss limit of
# 999x can never trip). CRYPTOBOT_SUBMINUTE_KILL=1 remains the emergency stop.
# To arm it, set a recent reset time and a minimum fill rate such as 0.2.
set_env_line CRYPTOBOT_SUBMINUTE_BREAKER_RESET "2026-07-08T13:45:00Z"
set_env_line CRYPTOBOT_SUBMINUTE_BREAKER_MIN_FILL_RATE "0.0"
set_env_line CRYPTOBOT_SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC "999"

# Market data.
set_env_line CRYPTOBOT_TWAP_RTDS_URL "wss://ws-live-data.polymarket.com"
set_env_line CRYPTOBOT_SPOT_SOURCE "binance"
set_env_line CRYPTOBOT_SPOT_FETCH_INTERVAL_SECONDS "1"
set_env_line CRYPTOBOT_SPOT_USE_1S_KLINES "1"
set_env_line CRYPTOBOT_SPOT_STALENESS_S "5"
set_env_line CRYPTOBOT_BINANCE_BASE_URL "https://data-api.binance.vision"
# A Binance fetch from the box takes ~200ms, so the prefetch starts 450ms
# before each second to land before the tick.
set_env_line CRYPTOBOT_SPOT_PREFETCH_LEAD_MS "450"
# The REST book batch takes ~230ms; starting 400ms early lands it before the
# tick, still well inside the 700ms freshness limit.
set_env_line CRYPTOBOT_BOOK_PREFETCH_ENABLED "true"
set_env_line CRYPTOBOT_BOOK_PREFETCH_LEAD_MS "400"
# The WebSocket book mirror is read first. Book age here reflects a quiet
# market rather than feed lag, so the age cap is effectively off (10 minutes
# outlives every window); the stall watchdog covers a subscription that stops
# updating. Set ENABLED to 0 to fall back to REST books.
set_env_line CRYPTOBOT_BOOK_WS_ENABLED "1"
set_env_line CRYPTOBOT_BOOK_WS_SHADOW "0"
set_env_line CRYPTOBOT_BOOK_WS_MAX_AGE_MS "600000"

# Accounting. INITIAL_BANKROLL is the account balance at WALLET_START_AT; both
# are only seeded when missing, so normal deploys keep the operator's values.
# RESET_RECORDS=1 moves the cutover to now: the dashboard record (journal, P&L,
# peak, max drawdown) and the drawdown halt restart from this point. Ledger
# rows are never deleted. Set INITIAL_BANKROLL to the balance at the new
# cutover when you reset.
ensure_env_line CRYPTOBOT_INITIAL_BANKROLL "100"
WALLET_START_NOW="$(date -u '+%Y-%m-%d %H:%M:%S')"
if [ "${RESET_RECORDS:-0}" = "1" ]; then
    PREV_START="$(sudo grep '^CRYPTOBOT_WALLET_START_AT=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true)"
    echo "==> RESET_RECORDS=1: accounting cutover moves to $WALLET_START_NOW UTC (was ${PREV_START:-unset})"
    set_env_line CRYPTOBOT_WALLET_START_AT "$WALLET_START_NOW"
else
    ensure_env_line CRYPTOBOT_WALLET_START_AT "$WALLET_START_NOW"
fi

# ---- 5. enable + restart ----------------------------------------------------
sudo systemctl daemon-reload
sudo systemctl enable cryptobot
sudo systemctl enable --now cryptobot-wal-checkpoint.timer

# Fail before restarting if the pinned contract did not land.
for expected in \
    'CRYPTOBOT_COINS=BTC,ETH,SOL,XRP' \
    'CRYPTOBOT_V2_BASE_FRAC_15M=0.010' \
    'CRYPTOBOT_V2_BASE_FRAC_5M=0.010' \
    'CRYPTOBOT_MAX_DD_HALT_FRAC=0.40' \
    'CRYPTOBOT_V2_FAK_PAD=0.01' \
    'CRYPTOBOT_ENABLE_TOPUPS=0'; do
    sudo grep -Fqx "$expected" "$ENV_FILE" || {
        echo "ERROR: $ENV_FILE does not contain: $expected" >&2
        exit 1
    }
done
# Check the Chainlink TWAP feed answers before trading on it.
sudo -u "$SERVICE_USER" env \
    PYTHONPATH="$PROJECT_DIR/src" \
    CRYPTOBOT_COINS=BTC,ETH,SOL,XRP \
    "$VENV_DIR/bin/python" \
    "$PROJECT_DIR/deploy/check_twap_rtds.py"

BOOT_WATCH_SINCE="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
BOOT_LOG="$LOG_DIR/scanner.log"
BOOT_ERR="$LOG_DIR/scanner.err"
ERR_OFFSET=$(sudo stat -c %s "$BOOT_ERR" 2>/dev/null || echo 0)
LOG_OFFSET=$(sudo stat -c %s "$BOOT_LOG" 2>/dev/null || echo 0)
sudo systemctl restart cryptobot

boot_ok_after_offset() {
    [ -f "$1" ] && sudo tail -c "+$(($2 + 1))" "$1" 2>/dev/null | grep -q "boot OK"
}

# Boot loads the models and seeds features from Binance, which can take
# ~90s; override with BOOT_OK_TIMEOUT_SECONDS.
BOOT_OK_TIMEOUT_SECONDS="${BOOT_OK_TIMEOUT_SECONDS:-120}"
echo "==> waiting up to ${BOOT_OK_TIMEOUT_SECONDS}s for boot OK"
for i in $(seq 1 "$BOOT_OK_TIMEOUT_SECONDS"); do
    if systemctl is-active --quiet cryptobot && {
        journalctl -u cryptobot --since "$BOOT_WATCH_SINCE" -n 200 --no-pager 2>/dev/null | grep -q "boot OK" ||
            boot_ok_after_offset "$BOOT_ERR" "$ERR_OFFSET" ||
            boot_ok_after_offset "$BOOT_LOG" "$LOG_OFFSET"
    }; then
        echo "==> SUCCESS: cryptobot is running (took ${i}s)"
        sudo systemctl status cryptobot --no-pager | head -12
        exit 0
    fi
    sleep 1
done

echo "WARN: no 'boot OK' within ${BOOT_OK_TIMEOUT_SECONDS}s. Recent logs:"
journalctl -u cryptobot --since "$BOOT_WATCH_SINCE" -n 30 --no-pager
sudo tail -n 30 "$BOOT_ERR" 2>/dev/null || true
sudo tail -n 30 "$BOOT_LOG" 2>/dev/null || true
exit 1
