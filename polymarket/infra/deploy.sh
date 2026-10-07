#!/usr/bin/env bash
# deploy.sh - build a runtime bundle locally and install it on the EC2 box.
# Run from the repo root:
#
#   EC2_HOST=ec2-x-x-x-x.compute.amazonaws.com \
#   PEM_PATH=polymarket/cryptobot.pem \
#   bash polymarket/infra/deploy.sh
#
# Optional env vars:
#   EC2_USER        SSH login user (default: ec2-user)
#   SERVICE_USER    Linux user the bot runs as (default: EC2_USER)
#   PROJECT_DIR     remote project dir (default: /home/$EC2_USER/cryptobot)
#   PYTHON_BIN      Python for the local test gate and build (default: python)
#   SKIP_TESTS      1 skips the local test gate. Also requires
#                   CONFIRM_UNSAFE_DEPLOY=1, so shipping untested code to a
#                   real-money bot is always a deliberate choice.
#   RESET_RECORDS   1 moves the accounting cutover (CRYPTOBOT_WALLET_START_AT)
#                   to this deploy: the dashboard record and the drawdown halt
#                   restart from now; ledger rows are kept. Pass it inline for
#                   one deploy; do not export it.

set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"

# ---- 0. inputs ----------------------------------------------------------------
: "${EC2_HOST:?EC2_HOST is required (public DNS or IP of the instance)}"
: "${PEM_PATH:?PEM_PATH is required (path to the .pem file)}"
EC2_USER="${EC2_USER:-ec2-user}"
SERVICE_USER="${SERVICE_USER:-$EC2_USER}"
PROJECT_DIR_REMOTE="${PROJECT_DIR:-/home/$EC2_USER/cryptobot}"

if [ ! -f "$PEM_PATH" ]; then
    echo "ERROR: PEM not found at $PEM_PATH"
    exit 1
fi
PEM_PERMS=$(stat -c "%a" "$PEM_PATH" 2>/dev/null || stat -f "%Lp" "$PEM_PATH" 2>/dev/null || echo "")
case "$PEM_PERMS" in
    600|400) : ;;
    *)
        echo "==> tightening PEM permissions ($PEM_PERMS -> 600)"
        chmod 600 "$PEM_PATH" 2>/dev/null || echo "WARN: chmod failed (Windows filesystem?)"
        ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
echo "==> repo root: $REPO_ROOT"
echo "==> target:    $EC2_USER@$EC2_HOST:$PROJECT_DIR_REMOTE"

# ---- 1. local test gate -------------------------------------------------------
if [ "${SKIP_TESTS:-0}" != "1" ]; then
    echo "==> running the test suite"
    # A fresh temp dir avoids stale-permission problems with pytest's default
    # temp root on Windows.
    PYTEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/cryptobot-pytest.XXXXXX")"
    PYTEST_TMP_ARG="$PYTEST_TMP"
    if [[ "$PYTHON_BIN" == *.exe ]] && command -v wslpath >/dev/null 2>&1; then
        PYTEST_TMP_ARG="$(wslpath -w "$PYTEST_TMP")"
    fi
    (cd "$REPO_ROOT" && "$PYTHON_BIN" -m pytest --basetemp "$PYTEST_TMP_ARG" -q --tb=line --no-header) || {
        echo "ERROR: tests failed - refusing to deploy."
        echo "       For an emergency hotfix set BOTH: SKIP_TESTS=1 CONFIRM_UNSAFE_DEPLOY=1"
        exit 1
    }
    rm -rf -- "$PYTEST_TMP" 2>/dev/null || true
else
    if [ "${CONFIRM_UNSAFE_DEPLOY:-0}" != "1" ]; then
        echo "ERROR: SKIP_TESTS=1 also needs CONFIRM_UNSAFE_DEPLOY=1."
        echo "       Skipping tests ships untested code to the live bot."
        exit 1
    fi
    echo "################################################################"
    echo "# TEST GATE SKIPPED - untested code is going to the live bot"
    echo "#   when: $(date -u '+%Y-%m-%dT%H:%M:%SZ') UTC"
    echo "#   who:  ${USER:-${LOGNAME:-unknown}}@$(hostname 2>/dev/null || echo unknown)"
    echo "################################################################"
fi

if [ "${RESET_RECORDS:-0}" = "1" ]; then
    echo "################################################################"
    echo "# RESET_RECORDS=1: the accounting record restarts at this deploy"
    echo "# (journal, P&L, peak, max drawdown and the drawdown halt)."
    echo "# Ctrl-C now if you did not mean to reset."
    echo "################################################################"
fi

echo "==> rebuilding the dashboard's static snapshot"
(cd "$REPO_ROOT" && PYTHONPATH="polymarket/src" "$PYTHON_BIN" polymarket/dashboard/build_snapshot.py)

# ---- 2. stage the runtime bundle ----------------------------------------------
TARBALL="/tmp/cryptobot-deploy-$(date +%Y%m%d-%H%M%S).tar.gz"
STAGING_DIR="$(mktemp -d /tmp/cryptobot-stage-XXXXXX)"
cleanup() {
    rm -rf "$STAGING_DIR"
    rm -f "$TARBALL"
}
trap cleanup EXIT

echo "==> staging runtime bundle: $STAGING_DIR"
mkdir -p \
    "$STAGING_DIR/src" \
    "$STAGING_DIR/dashboard/assets" \
    "$STAGING_DIR/data/models/twap_symmetric_v1" \
    "$STAGING_DIR/runtime/config" \
    "$STAGING_DIR/runtime/data" \
    "$STAGING_DIR/deploy"

# Code: the two runtime packages, flattened into src/ on the box, without tests.
cp -a "$REPO_ROOT/polymarket/src/cryptobot" "$STAGING_DIR/src/"
cp -a "$REPO_ROOT/polymarket/src/cryptobot_polymarket" "$STAGING_DIR/src/"
rm -rf "$STAGING_DIR/src/cryptobot/tests" "$STAGING_DIR/src/cryptobot_polymarket/tests"
find "$STAGING_DIR/src" -type d -name "__pycache__" -exec rm -rf -- {} +
find "$STAGING_DIR/src" -type f -name "*.pyc" -delete

# Strategy config.
CONFIG_SOURCE="$REPO_ROOT/polymarket/live/config/twap_symmetric_live.json"
cp "$CONFIG_SOURCE" "$STAGING_DIR/runtime/config/twap_symmetric_live.json"

# Model (gitignored; built by polymarket.backtest.training.export_twap_symmetric_60s).
MODEL_DIR="$REPO_ROOT/data/models/twap_symmetric_v1"
for artifact in twap_hgb_v1.joblib twap_symmetric_v1_manifest.json; do
    if [ ! -f "$MODEL_DIR/$artifact" ]; then
        echo "ERROR: missing model artifact data/models/twap_symmetric_v1/$artifact" >&2
        echo "       Build it with: python -m polymarket.backtest.training.export_twap_symmetric_60s" >&2
        exit 1
    fi
    cp "$MODEL_DIR/$artifact" "$STAGING_DIR/data/models/twap_symmetric_v1/"
done
echo "==> staged twap_symmetric_v1 model"

# The model directory is gitignored, so check the shipped model against the
# tracked release pin (polymarket/live/config/twap_symmetric_release.json):
# same model hash, coins, feature contract, windows, thresholds and execution
# settings as the approved release. Any mismatch stops the deploy.
"$PYTHON_BIN" - "$MODEL_DIR/twap_symmetric_v1_manifest.json" \
    "$CONFIG_SOURCE" "$REPO_ROOT/polymarket/live/config/twap_symmetric_release.json" <<'PY'
import hashlib, json, pathlib, sys

manifest = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
config = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
release = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))
model = pathlib.Path(sys.argv[1]).with_name("twap_hgb_v1.joblib")
with model.open("rb") as handle:
    actual_hash = hashlib.file_digest(handle, "sha256").hexdigest()
if actual_hash != manifest.get("model_sha256") or actual_hash != release.get("model_sha256"):
    raise SystemExit("TWAP release/model SHA256 mismatch")
coins = sorted(str(x).upper() for x in config.get("coin_universe", []))
if coins != sorted(release.get("live_coins", [])):
    raise SystemExit("TWAP release/config coin-universe mismatch")
if coins != sorted(str(x).upper() for x in manifest.get("live_coins", [])):
    raise SystemExit("TWAP release/manifest coin-universe mismatch")
for key in ("live_feature_contract", "chainlink_window_seconds",
            "decision_window_seconds", "execution_by_interval", "validation"):
    if release.get(key) != manifest.get(key):
        raise SystemExit(f"TWAP release/manifest {key} mismatch")
entry_validation = release.get("entry_window_validation")
if not entry_validation or entry_validation != manifest.get("entry_window_validation"):
    raise SystemExit("TWAP release/manifest entry-window validation mismatch")
if entry_validation.get("model_sha256") != actual_hash:
    raise SystemExit("TWAP entry-window validation used a different model")
if entry_validation.get("model_refits") != 0:
    raise SystemExit("TWAP entry-window validation refit the frozen model")
if entry_validation.get("selected_window_seconds") != release.get("decision_window_seconds"):
    raise SystemExit("TWAP entry-window validation does not cover the live windows")
if entry_validation.get("topups_enabled") is not False:
    raise SystemExit("TWAP entry-window validation must be base-only")
if manifest.get("contract_version") != 3 or release.get("contract_version") != 3:
    raise SystemExit("TWAP manifest is not contract version 3")
if manifest.get("model_count") != 2 or release.get("model_count") != 2:
    raise SystemExit("TWAP release must contain exactly two models")
expected_routing = {"5m": "pooled_all4", "15m": "pooled_all4"}
if manifest.get("model_routing") != expected_routing or release.get("model_routing") != expected_routing:
    raise SystemExit("TWAP model routing is not pooled across all four coins")
expected_categorical = {"5m": ["coin_id"], "15m": ["coin_id"]}
if manifest.get("categorical_features") != expected_categorical or release.get("categorical_features") != expected_categorical:
    raise SystemExit("TWAP model coin_id is not categorical")
sleeves = {s["config"]["interval"]: s["config"] for s in config.get("sleeves", [])}
for interval, threshold in release.get("thresholds", {}).items():
    if float(sleeves[interval]["threshold"]) != float(threshold):
        raise SystemExit(f"TWAP release/config threshold mismatch: {interval}")
    if float(manifest["sleeves"][interval]["threshold"]) != float(threshold):
        raise SystemExit(f"TWAP release/manifest threshold mismatch: {interval}")
if set(sleeves) != set(release.get("live_intervals", [])):
    raise SystemExit("TWAP release/config interval mismatch")
if set(sleeves) != set(manifest.get("intervals", [])):
    raise SystemExit("TWAP release/manifest interval mismatch")
execution = release.get("execution", {})
if float(config.get("max_drawdown_halt_frac")) != float(execution.get("max_drawdown_halt_frac")):
    raise SystemExit("TWAP release/config max drawdown halt mismatch")
for interval, sleeve in sleeves.items():
    if float(sleeve["capital_frac"]) != float(execution.get("base_equity_fraction")):
        raise SystemExit(f"TWAP release/config base fraction mismatch: {interval}")
    contract = manifest["sleeves"][interval]
    for key in ("ask_bucket", "side_filter", "book_consumption_max", "checkpoint_offset_set"):
        if sleeve[key] != contract[key]:
            raise SystemExit(f"TWAP release/config {key} mismatch: {interval}")
    if sleeve["checkpoint_offset_set"] != release["checkpoint_offset_sets"][interval]:
        raise SystemExit(f"TWAP release/config checkpoint offsets mismatch: {interval}")
required_execution = {
    "trend_following": True, "side_flip": False, "t3_enabled": False,
    "max_drawdown_halt_frac": 0.40,
    "tier_a_multiplier": 1.0, "fak_pad": 0.01,
    "retry_until_first_fill": True, "base_equity_fraction": 0.01,
}
if execution != required_execution:
    raise SystemExit("TWAP release execution contract mismatch")
print(f"TWAP release verified: {actual_hash[:12]} coins={','.join(coins)}")
PY

# Dashboard: every file index.html loads, plus the static snapshot.
cp "$REPO_ROOT/polymarket/dashboard/index.html" "$STAGING_DIR/dashboard/"
cp "$REPO_ROOT/polymarket/dashboard/styles.css" "$STAGING_DIR/dashboard/"
cp "$REPO_ROOT/polymarket/dashboard/time.js" "$STAGING_DIR/dashboard/"
cp "$REPO_ROOT/polymarket/dashboard/live-bridge.js" "$STAGING_DIR/dashboard/"
cp "$REPO_ROOT/polymarket/dashboard/api.js" "$STAGING_DIR/dashboard/"
for jsx in data charts shell pages-overview pages-live pages-analytics pages-config pages-latency pages-subminute app; do
    cp "$REPO_ROOT/polymarket/dashboard/$jsx.jsx" "$STAGING_DIR/dashboard/"
done
cp "$REPO_ROOT/polymarket/dashboard/snapshot.json" "$STAGING_DIR/dashboard/"
cp "$REPO_ROOT/polymarket/dashboard/build_snapshot.py" "$STAGING_DIR/dashboard/"
cp -a "$REPO_ROOT/polymarket/dashboard/assets/." "$STAGING_DIR/dashboard/assets/"

# Cache-bust: stamp index.html's ?v= tags with a hash of the shipped dashboard
# files, so browsers refetch exactly when something changed.
_dash_hash=""
if _h="$(cat "$STAGING_DIR/dashboard/"*.jsx "$STAGING_DIR/dashboard/styles.css" "$STAGING_DIR/dashboard/time.js" "$STAGING_DIR/dashboard/api.js" "$STAGING_DIR/dashboard/live-bridge.js" 2>/dev/null | git hash-object --stdin 2>/dev/null)"; then
    _dash_hash="$(printf '%s' "$_h" | cut -c1-10)"
fi
[ -n "$_dash_hash" ] || _dash_hash="$(date -u +%Y%m%d%H%M%S)"
sed -i "s#?v=v4-[0-9a-z]*#?v=v4-$_dash_hash#g" "$STAGING_DIR/dashboard/index.html"
echo "==> dashboard cache-buster -> v4-$_dash_hash"

# Units, installer and env template (all land in deploy/ on the box).
for src in \
    polymarket/infra/cron.daily.sh \
    polymarket/infra/cryptobot.env.example \
    polymarket/infra/check_twap_rtds.py \
    polymarket/infra/install_remote.sh \
    polymarket/infra/requirements.runtime.txt \
    polymarket/live/service/cryptobot.service \
    polymarket/live/service/cryptobot-scanner.logrotate \
    polymarket/live/service/cryptobot-wal-checkpoint.service \
    polymarket/live/service/cryptobot-wal-checkpoint.timer \
    polymarket/live/service/cryptobot-failure-alert.service
do
    cp "$REPO_ROOT/$src" "$STAGING_DIR/deploy/"
done

echo "==> packing tarball: $TARBALL"
tar --create --gzip --file="$TARBALL" --directory="$STAGING_DIR" .
echo "==> tarball size: $(du -h "$TARBALL" | cut -f1)"

# ---- 3. upload and extract ----------------------------------------------------
SSH_OPTS="-i $PEM_PATH -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o IdentitiesOnly=yes"
echo "==> probing SSH"
if ! ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" "echo OK && uname -a"; then
    echo "ERROR: SSH failed. Check EC2_HOST, EC2_USER, and that the security"
    echo "       group allows port 22 from your current IP."
    exit 1
fi

REMOTE_TAR="/tmp/$(basename "$TARBALL")"
echo "==> uploading"
scp $SSH_OPTS "$TARBALL" "$EC2_USER@$EC2_HOST:$REMOTE_TAR"

echo "==> extracting into $PROJECT_DIR_REMOTE"
ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" \
    "PROJECT_DIR_REMOTE=$(printf '%q' "$PROJECT_DIR_REMOTE") REMOTE_TAR=$(printf '%q' "$REMOTE_TAR") SERVICE_USER=$(printf '%q' "$SERVICE_USER") bash -s" <<'REMOTE_EXTRACT'
set -euo pipefail

case "$PROJECT_DIR_REMOTE" in
    ""|"/"|"/home"|"/home/"|"/home/ec2-user"|"/root")
        echo "ERROR: refusing unsafe PROJECT_DIR_REMOTE=$PROJECT_DIR_REMOTE" >&2
        exit 1
        ;;
esac
if [ "$PROJECT_DIR_REMOTE" = "$HOME" ]; then
    echo "ERROR: refusing to clean the login user's home directory" >&2
    exit 1
fi

tmp_extract="$(mktemp -d /tmp/cryptobot-extract-XXXXXX)"
trap 'rm -rf "$tmp_extract"' EXIT

mkdir -p "$PROJECT_DIR_REMOTE"
tar -xzf "$REMOTE_TAR" -C "$tmp_extract"
test -d "$tmp_extract/src/cryptobot"
test -d "$tmp_extract/src/cryptobot_polymarket"
test -f "$tmp_extract/deploy/install_remote.sh"
test -f "$tmp_extract/runtime/config/twap_symmetric_live.json"
test -f "$tmp_extract/data/models/twap_symmetric_v1/twap_symmetric_v1_manifest.json"

mkdir -p "$PROJECT_DIR_REMOTE/data"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$PROJECT_DIR_REMOTE"

# Replace everything except the venv and data/. Inside data/ only the models
# are replaced; the SQLite ledger and anything else there is kept.
find "$PROJECT_DIR_REMOTE" -mindepth 1 -maxdepth 1 \
    ! -name ".venv" \
    ! -name "data" \
    -exec rm -rf -- {} +
rm -rf "$PROJECT_DIR_REMOTE/data/models"

cp -a "$tmp_extract"/. "$PROJECT_DIR_REMOTE"/
rm -f "$REMOTE_TAR"
chmod +x "$PROJECT_DIR_REMOTE/deploy/"*.sh
REMOTE_EXTRACT

# ---- 4. remote install --------------------------------------------------------
echo "==> running install_remote.sh on the instance"
ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" \
    "PROJECT_DIR='$PROJECT_DIR_REMOTE' SERVICE_USER='$SERVICE_USER' \
     RESET_RECORDS='${RESET_RECORDS:-0}' \
     bash '$PROJECT_DIR_REMOTE/deploy/install_remote.sh'"

rm -f "$TARBALL"
rm -rf "$STAGING_DIR"
trap - EXIT

cat <<EOF

================================================================
DEPLOY COMPLETE
================================================================
Live log:   ssh -i $PEM_PATH $EC2_USER@$EC2_HOST 'sudo journalctl -fu cryptobot'
Dashboard:  ssh -L 8800:127.0.0.1:8800 -i $PEM_PATH $EC2_USER@$EC2_HOST
            then open http://127.0.0.1:8800/
Stop:       ssh -i $PEM_PATH $EC2_USER@$EC2_HOST 'sudo systemctl stop cryptobot'
================================================================
EOF
