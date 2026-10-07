#!/usr/bin/env bash
# deploy.sh - local driver. Run from the repo root:
#
#   EC2_HOST=ec2-x-x-x-x.compute.amazonaws.com \
#   PEM_PATH=polymarket/cryptobot.pem \
#   bash polymarket/infra/deploy.sh
#
# Optional env vars:
#   EC2_USER       - SSH login user (default: ec2-user)
#   SERVICE_USER   - Linux user the bot runs as (default: same as EC2_USER)
#   PROJECT_DIR    - remote project dir (default: /home/$EC2_USER/cryptobot)
#   SKIP_TESTS     - set to 1 to skip the local test gate. REQUIRES
#                    CONFIRM_UNSAFE_DEPLOY=1 as a second ack (audit #10,
#                    w19oxqf24) — shipping untested code to a live money
#                    bot must be a deliberate two-flag decision.
#   CONFIRM_UNSAFE_DEPLOY - set to 1 to acknowledge SKIP_TESTS=1
#   PYTHON_BIN      - Python executable for the local gate/build (default:
#                    python). Useful on Windows Git Bash when the repo venv is
#                    available only as a Windows executable path.
#   PYTHON_JSON_BIN - Python executable for the stdlib-only config conversion
#                    (default: PYTHON_BIN). Set to a WSL/POSIX Python when
#                    PYTHON_BIN is a Windows executable and paths are /mnt/c.
#   CRYPTOBOT_BOX_ROLE - which BOX this deploy targets:
#                    'polymarket' (DEFAULT — eu-west-1 Ireland, the Polymarket
#                    scanner + its recorder) or 'predictfun' (ap-northeast-1
#                    Tokyo, the predict.fun scanner ONLY, REAL money). Passed
#                    through to install_remote.sh, which installs/enables only
#                    that role's units. Unset ⇒ polymarket ⇒ byte-identical to
#                    the pre-role deploy. Prefer the thin wrapper
#                    predictfun/infra/deploy.sh, which pins the role + the
#                    Tokyo host + the predict.fun PEM.
#   RESET_RECORDS  - set to 1 to cut the accounting record over at THIS deploy:
#                    force-pins CRYPTOBOT_WALLET_START_AT to the deploy instant
#                    (UTC). Resets the dashboard record (journal, PnL, peak
#                    equity, max-DD%) AND the runtime DD state — the configured
#                    DD halt and DD-brake re-anchor, so a halted/throttled bot
#                    resumes at full size. Ledger history is preserved in
#                    SQLite, just excluded from the record window. The LIVE
#                    group-exposure cap and fill-rate breaker are unaffected.
#                    Deliberate one-shot flag: pass it inline on the command
#                    line, do NOT export it (an exported value silently resets
#                    the record on every later deploy from that shell).

set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
PYTHON_JSON_BIN="${PYTHON_JSON_BIN:-$PYTHON_BIN}"
PYTEST_BASETEMP=""
PYTEST_BASETEMP_POSIX=""

# ---- 0. Inputs + sanity ------------------------------------------------------
: "${EC2_HOST:?EC2_HOST is required (public DNS or IP of the instance)}"
: "${PEM_PATH:?PEM_PATH is required (path to the .pem file)}"
EC2_USER="${EC2_USER:-ec2-user}"
SERVICE_USER="${SERVICE_USER:-$EC2_USER}"
PROJECT_DIR_REMOTE="${PROJECT_DIR:-/home/$EC2_USER/cryptobot}"
BOX_ROLE="${CRYPTOBOT_BOX_ROLE:-polymarket}"
case "$BOX_ROLE" in
    polymarket|predictfun) : ;;
    *)
        echo "ERROR: unknown CRYPTOBOT_BOX_ROLE='$BOX_ROLE' (want polymarket|predictfun)" >&2
        exit 1
        ;;
esac

if [ ! -f "$PEM_PATH" ]; then
    echo "ERROR: PEM not found at $PEM_PATH"
    exit 1
fi

PEM_PERMS=$(stat -c "%a" "$PEM_PATH" 2>/dev/null || stat -f "%Lp" "$PEM_PATH" 2>/dev/null || echo "")
case "$PEM_PERMS" in
    600|400) : ;;
    *)
        echo "==> Tightening PEM permissions ($PEM_PERMS -> 600)"
        chmod 600 "$PEM_PATH" 2>/dev/null || {
            echo "WARN: chmod failed (Windows fs?) - continuing with IdentitiesOnly"
        }
        ;;
esac

# polymarket/infra/deploy.sh -> two levels up is the repo root.
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# THREE local src roots for FOUR packages: polymarket/src holds BOTH the shared
# engine and the Polymarket adapter (the two collapsed into one src root on
# 2026-08-04 — single src per venue); predict.fun and kalshi keep their own.
# The separator is OS-dependent and getting it wrong fails
# SILENTLY: Windows Python splits PYTHONPATH on ';', so a ':'-joined list
# arrives as one nonexistent directory and every venue import dies with a
# confusing ModuleNotFoundError. This script is normally run from Git Bash on
# the operator's Windows laptop.
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) PY_PATH_SEP=";" ;;
    *)                    PY_PATH_SEP=":" ;;
esac
PY_SRC_PATH="polymarket/src${PY_PATH_SEP}predictfun/live/src${PY_PATH_SEP}kalshi/live/src"
echo "==> repo root: $REPO_ROOT"
echo "==> target:    $EC2_USER@$EC2_HOST:$PROJECT_DIR_REMOTE"
echo "==> box role:  $BOX_ROLE"

if [ "${SKIP_TESTS:-0}" != "1" ]; then
    echo "==> running local test gate"
    # Venue-seam import gate (panel verdict L). deploy.sh stages ALL the venue
    # packages into one flat $PROJECT_DIR/src, so on the box every
    # `import cryptobot_polymarket` resolves whether or not core was supposed
    # to make it — a direction violation is INVISIBLE at runtime and shows up
    # only as an unbootable tree the day a venue package stops being staged
    # (cryptobot_kalshi already is not). The test suite cannot see it either: a
    # green suite is perfectly compatible with core importing a venue again.
    # So it is checked here, before anything is staged.
    (cd "$REPO_ROOT" && "$PYTHON_BIN" tools/importcheck.py) || {
        echo "ERROR: venue-seam import violation - refusing to deploy."
        echo "       Fix the import, or add it to ALLOWLIST in"
        echo "       tools/importcheck.py WITH a reason."
        exit 1
    }
    # Target src/cryptobot/ (the whole package tree). All unit tests now live in
    # the single src/cryptobot/tests/ root. This is what runs the W6 v2 PARITY
    # GATE (tests/test_v2_parity_foldE.py — bet-for-bet vs research build_bets on
    # fold E: n=2503/+0.0428 pure, n=2366/+0.0407 live-legal ceiling), the
    # spec_v2 parity unit test, and the sizing_v2 tests. A broken v2 decision or
    # sizing path therefore BLOCKS the deploy. The 92 MB parity fixture is read
    # from its repo path; the module skips cleanly if it is absent (CI without
    # the research data). polymarket/backtest/training/tests/ guards the LIVE model
    # exporter (test_export_models.py; was polymarket/backtest/scripts/tests/ before the
    # 2026-07-12 research fold-in — the old path made pytest collect NOTHING
    # and abort every deploy).
    # --basetemp: pytest's default %TEMP%/pytest-of-<user> dir and a reused
    # repo-local directory can both rot into a permission-denied state on
    # Windows/OneDrive. A fresh mktemp directory makes every gate independent
    # of stale ACLs while keeping the fixture root explicit.
    # Phase 2 (2026-07-28): several src roots and two extra suites — the venue
    # adapters were extracted into cryptobot_{polymarket,predictfun,kalshi} and
    # their dedicated tests travelled with them. Dropping either venue suite
    # here would let a broken order client or fee shape reach the box.
    PYTEST_BASETEMP_POSIX="$(mktemp -d "${TMPDIR:-/tmp}/cryptobot-pytest-gate.XXXXXX")"
    PYTEST_BASETEMP="$PYTEST_BASETEMP_POSIX"
    if [[ "$PYTHON_BIN" == *.exe ]] && command -v wslpath >/dev/null 2>&1; then
        PYTEST_BASETEMP="$(wslpath -w "$PYTEST_BASETEMP")"
    fi
    (cd "$REPO_ROOT" && PYTHONPATH="$PY_SRC_PATH" "$PYTHON_BIN" -m pytest \
        polymarket/src/cryptobot/ \
        polymarket/src/cryptobot_polymarket/tests/ \
        predictfun/live/src/cryptobot_predictfun/tests/ \
        polymarket/dashboard/tests/ polymarket/backtest/tests/ polymarket/backtest/training/tests/ \
        --basetemp "$PYTEST_BASETEMP" \
        -q --tb=line --no-header) || {
        echo "ERROR: tests failed - refusing to deploy."
        echo "       Fix the failures, or to ship a deliberate emergency"
        echo "       hotfix set BOTH: SKIP_TESTS=1 CONFIRM_UNSAFE_DEPLOY=1"
        exit 1
    }
    rm -rf -- "$PYTEST_BASETEMP_POSIX" 2>/dev/null ||
        echo "WARN: could not remove pytest basetemp: $PYTEST_BASETEMP_POSIX" >&2
    PYTEST_BASETEMP_POSIX=""
else
    # audit #10 (w19oxqf24): the test gate is the ONLY safeguard between a
    # broken commit and a live money bot, and it was fully skippable via a
    # single env var with no confirmation and no audit trail. Require a
    # second explicit ack so skipping tests can never be accidental, and
    # emit a loud journald-captured banner recording who/when when it IS
    # honored.
    if [ "${CONFIRM_UNSAFE_DEPLOY:-0}" != "1" ]; then
        echo
        echo "################################################################"
        echo "# REFUSING UNSAFE DEPLOY                                        #"
        echo "################################################################"
        echo "# SKIP_TESTS=1 was set but CONFIRM_UNSAFE_DEPLOY is not 1.      #"
        echo "# Skipping the test gate ships UNTESTED code to the LIVE money  #"
        echo "# trading bot. This must be a deliberate, acknowledged choice.  #"
        echo "#                                                              #"
        echo "# To proceed anyway (emergency hotfix only), re-run with BOTH: #"
        echo "# SKIP_TESTS=1 CONFIRM_UNSAFE_DEPLOY=1 bash polymarket/infra/deploy.sh #"
        echo "################################################################"
        echo
        exit 1
    fi
    echo
    echo "################################################################"
    echo "# !! TEST GATE SKIPPED — UNTESTED CODE -> LIVE MONEY BOT !!     #"
    echo "################################################################"
    echo "#   SKIP_TESTS=1 + CONFIRM_UNSAFE_DEPLOY=1 acknowledged."
    echo "#   when:  $(date -u '+%Y-%m-%dT%H:%M:%SZ') (UTC)"
    echo "#   who:   ${USER:-${LOGNAME:-unknown}}@$(hostname 2>/dev/null || echo unknown-host)"
    echo "#   target: $EC2_USER@$EC2_HOST:$PROJECT_DIR_REMOTE"
    echo "################################################################"
    echo
fi

if [ "${RESET_RECORDS:-0}" = "1" ]; then
    echo
    echo "################################################################"
    echo "# RESET_RECORDS=1 — ACCOUNTING RECORD RESET THIS DEPLOY         #"
    echo "################################################################"
    echo "# CRYPTOBOT_WALLET_START_AT will be force-pinned to the deploy #"
    echo "# instant: journal/PnL/peak/max-DD% reset AND configured DD halt#"
    echo "# + DD-brake re-anchor (a halted bot resumes at full size).    #"
    echo "# If you did not intend a reset, Ctrl-C now and unset the flag.#"
    echo "################################################################"
    echo
fi

echo "==> rebuilding dashboard static snapshot (venue=$BOX_ROLE)"
# The static snapshot is the dashboard's pre-SSE fallback and now carries a
# `venue` stamp: the console is ONE app parameterized by venue (the cross-venue
# view was retired 2026-08-01), so the fallback must describe the box it ships
# to. The box role IS the venue — polymarket box => polymarket console,
# predictfun box => predict.fun console.
(cd "$REPO_ROOT" && PYTHONPATH="$PY_SRC_PATH" "$PYTHON_BIN" polymarket/dashboard/build_snapshot.py --venue "$BOX_ROLE")

# ---- 0b. Resolve local artifact sources (legacy-first) ---------------------
# The venue-tree restructure (2026-07-28) moved tracked CODE but deliberately
# left the gitignored ~20 GB research tree physically at backtest/. So
# v2_step1last_live.json -- a LIVE contract, not a research output -- lives at
# polymarket/live/config/ and only falls back to its old research-tree path for
# older checkouts.
#
# 2026-07-31 (exp044 phase 2): decision_table.parquet resolution, its .lutcache
# sidecar warm, and the rf_v1 staging further below are GONE. Nothing in the
# live boot path reads either artifact any more -- the pair sleeves and the
# lut|rf|blend scorer that consumed them were retired -- so shipping them was
# ~90 MB of tarball and a pandas groupby of pure deploy cost.
pick_first() {
    for candidate in "$@"; do
        if [ -e "$candidate" ]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    return 1
}
if [ "$BOX_ROLE" = "polymarket" ]; then
    V6_SOURCE="$REPO_ROOT/polymarket/live/config/twap_symmetric_live.json"
    if [ ! -f "$V6_SOURCE" ]; then
        echo "ERROR: TWAP live config missing: $V6_SOURCE" >&2
        exit 1
    fi
else
    V6_SOURCE="$(pick_first \
        "$REPO_ROOT/polymarket/live/config/v2_step1last_live.json" \
        "$REPO_ROOT/backtest/optimize_runs/v2_step1last_live.json")" || {  # legacy-path-ok: pre-restructure checkouts
        echo "ERROR: v2_step1last_live.json not found (polymarket/live/config/)" >&2
        exit 1
    }
fi
echo "==> v6 config source:      $V6_SOURCE"

# ---- 1. Stage a runtime-only bundle -----------------------------------------
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
    "$STAGING_DIR/data/models" \
    "$STAGING_DIR/runtime/config" \
    "$STAGING_DIR/runtime/data" \
    "$STAGING_DIR/deploy"

# Runtime package only. EC2 does not need local tests, notebooks, backtest
# drivers, historical SQLite datasets, or research feature stores.
# All unit tests live in the single src/cryptobot/tests/ root (no nested
# strategy/tests or execution/tests), so this one strip removes every test.
# NOTE: the REMOTE layout is deliberately UNCHANGED ($PROJECT_DIR/src/
# cryptobot). The local package moved to polymarket/src in the venue-tree
# restructure (and again on 2026-08-04, when the venue collapsed to ONE src
# root), so staging maps new-local -> old-remote and the box, its unit files
# and /etc/cryptobot.env need no cutover.
cp -a "$REPO_ROOT/polymarket/src/cryptobot" "$STAGING_DIR/src/"
rm -rf "$STAGING_DIR/src/cryptobot/tests"  # legacy-path-ok: staged AS src/ for the frozen remote layout
# Phase 2 (2026-07-28): the venue adapters are their own top-level packages
# now, staged ALONGSIDE cryptobot in the same src/ dir. The remote PYTHONPATH
# stays exactly $PROJECT_DIR/src, so install_remote.sh and all 12 unit files
# need no change for this — the packages simply appear next to cryptobot.
#
# BOTH venues ship to BOTH boxes on purpose. Core resolves the ACTIVE venue
# lazily, but grade-time fee booking dispatches on each LEDGER ROW's venue,
# so a Polymarket box with predict.fun rows in its history must still be able
# to price the tent.
cp -a "$REPO_ROOT/polymarket/src/cryptobot_polymarket" "$STAGING_DIR/src/"
cp -a "$REPO_ROOT/predictfun/live/src/cryptobot_predictfun" "$STAGING_DIR/src/"
rm -rf "$STAGING_DIR/src/cryptobot_polymarket/tests"  # legacy-path-ok: staged AS src/ for the frozen remote layout
rm -rf "$STAGING_DIR/src/cryptobot_predictfun/tests"  # legacy-path-ok: staged AS src/ for the frozen remote layout
# cryptobot_kalshi is deliberately NOT staged: it is a stub whose every
# member raises, and CRYPTOBOT_VENUE=kalshi must fail at boot either way
# (config's allowlist is the registry map, and an unimportable registered
# venue raises just as loudly as an unimplemented one).
find "$STAGING_DIR/src" -type d -name "__pycache__" -exec rm -rf -- {} +
find "$STAGING_DIR/src" -type f -name "*.pyc" -delete

# Runtime artifacts. Local research still owns the source copy (resolved into
# $V6_SOURCE above); the server reads the deployed copy from runtime/.
# v2 "step1last" is the ACTIVE (and only) live config: checkpoint-only
# sub-minute sleeves at 100% of live — ask band 0.65-0.85, gate
# pred_edge>=0.03, last-30s per-second, one bet per market. install_remote.sh
# force-pins CRYPTOBOT_V6_CONFIG at this file every deploy. We must ship the
# things the bot reads at boot:
#   - v2_step1last_live.json       (the v6 config / checkpoint sleeves)
#   - data/models/checkpoint_v1/*  (the ONLY scoring artifact — staged below)
# ORB/exp044 retired 2026-07-09: exp044_live.json is no longer built or shipped.
cp "$V6_SOURCE" "$STAGING_DIR/runtime/config/v2_step1last_live.json"
# predict.fun DRY paper instance's V6 artifact: GENERATED from the live
# artifact at deploy time with every wallet_default_modes value rewritten to
# "DRY" (nothing else changes). Rationale: seed_wallet_modes seeds a FRESH DB
# from the artifact — DRY_RUN=true only sets boot_dry_run, NOT wallet modes —
# so pointing the paper instance at the live artifact would seed its wallets
# LIVE (silently bet-less with order_client=None, or LIVE-armed the day a
# predict.fun key+SDK appear). Generating (not hand-copying) means strategy
# amendments to the live artifact propagate automatically and the copy can
# never drift; the drift-check trio (exposure cap / DD halt) is inherited
# unchanged so load_sleeves passes.
"$PYTHON_JSON_BIN" - "$V6_SOURCE" \
    "$STAGING_DIR/runtime/config/v2_step1last_predictfun.json" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
with open(src, "r", encoding="utf-8") as fh:
    cfg = json.load(fh)
modes = cfg.get("wallet_default_modes")
if not isinstance(modes, dict) or not modes:
    raise SystemExit("v2_step1last_live.json has no wallet_default_modes - refusing")
cfg["wallet_default_modes"] = {k: "DRY" for k in modes}
with open(dst, "w", encoding="utf-8") as fh:
    json.dump(cfg, fh, indent=2)
    fh.write("\n")
print(f"  generated {dst} (wallet_default_modes -> all DRY)")
PY
# ---- ROLLBACK ---------------------------------------------------------------
# The env-flip rollback to exp044 is retired with the ORB/exp044 code removal.
# Rollback = git revert <this change> + redeploy.
#
#   EMERGENCY STOP (halt all v2 placement immediately, no redeploy):
#     grep -q '^CRYPTOBOT_SUBMINUTE_KILL=' /etc/cryptobot.env \
#       && sudo sed -i 's/^CRYPTOBOT_SUBMINUTE_KILL=.*/CRYPTOBOT_SUBMINUTE_KILL=1/' /etc/cryptobot.env \
#       || echo 'CRYPTOBOT_SUBMINUTE_KILL=1' | sudo tee -a /etc/cryptobot.env
#     sudo systemctl restart cryptobot
# ----------------------------------------------------------------------------
# checkpoint_v1: the v2 "step1last" LIVE sub-minute models (11-feature HGB per
# interval + per-second RF slices), produced by
#   python -m polymarket.backtest.training.export_models
# Since v2 is now 100% of live trading, these are REQUIRED — main.py loads them
# at boot for the checkpoint sleeves (CRYPTOBOT_SUBMINUTE_ENABLED=1), and the v2
# config has NO other sleeves. Shipping a bundle without them would boot a bot
# whose only sleeves cannot score, so FAIL the deploy loudly here instead. (The
# 200-tree backup checkpoint_rf_v1_200trees.joblib, the staged training parquet,
# and export.log are research-only and NOT shipped.)
CKPT_V1_DIR="$REPO_ROOT/data/models/checkpoint_v1"
for artifact in checkpoint_rf_v1.joblib checkpoint_hgb_v1.joblib checkpoint_v1_manifest.json; do
    if [ ! -f "$CKPT_V1_DIR/$artifact" ]; then
        echo "ERROR: missing v2 model artifact data/models/checkpoint_v1/$artifact" >&2
        echo "       Build the v2 step1last models before deploying:" >&2
        echo "         python -m polymarket.backtest.training.export_models" >&2
        echo "       v2 is 100% of live trading; the checkpoint sleeves cannot" >&2
        echo "       score without these — refusing to ship a dead bundle." >&2
        exit 1
    fi
done
mkdir -p "$STAGING_DIR/data/models/checkpoint_v1"
cp "$CKPT_V1_DIR/checkpoint_rf_v1.joblib" \
    "$STAGING_DIR/data/models/checkpoint_v1/checkpoint_rf_v1.joblib"
cp "$CKPT_V1_DIR/checkpoint_hgb_v1.joblib" \
    "$STAGING_DIR/data/models/checkpoint_v1/checkpoint_hgb_v1.joblib"
cp "$CKPT_V1_DIR/checkpoint_v1_manifest.json" \
    "$STAGING_DIR/data/models/checkpoint_v1/checkpoint_v1_manifest.json"
echo "==> staged checkpoint_v1 v2 sub-minute models ($(du -h "$STAGING_DIR/data/models/checkpoint_v1/checkpoint_rf_v1.joblib" | cut -f1) RF)"

# Direct TWAP-symmetric HGB bundle. The live scorer has no RF stage; 5m uses
# the shared four-coin 60s refit and 15m routes to its corrected per-coin model.
TWAP_V1_DIR="$REPO_ROOT/data/models/twap_symmetric_v1"
for artifact in twap_hgb_v1.joblib twap_symmetric_v1_manifest.json; do
    if [ ! -f "$TWAP_V1_DIR/$artifact" ]; then
        echo "ERROR: missing TWAP model artifact data/models/twap_symmetric_v1/$artifact" >&2
        echo "       Build it with:" >&2
        echo "         python -m polymarket.backtest.training.export_twap_symmetric" >&2
        exit 1
    fi
done
mkdir -p "$STAGING_DIR/data/models/twap_symmetric_v1"
cp "$TWAP_V1_DIR/twap_hgb_v1.joblib" \
    "$STAGING_DIR/data/models/twap_symmetric_v1/twap_hgb_v1.joblib"
cp "$TWAP_V1_DIR/twap_symmetric_v1_manifest.json" \
    "$STAGING_DIR/data/models/twap_symmetric_v1/twap_symmetric_v1_manifest.json"
echo "==> staged twap_symmetric_v1 model"

# Tracked release pin: the gitignored model directory cannot by itself prove
# that the artifact being shipped is the exact one whose forward holdout was
# approved.  Fail closed on any model/config/release drift.
TWAP_RELEASE="$REPO_ROOT/polymarket/live/config/twap_symmetric_release.json"
if [ "$BOX_ROLE" = "polymarket" ]; then
    if [ ! -f "$TWAP_RELEASE" ]; then
        echo "ERROR: missing tracked TWAP release pin: $TWAP_RELEASE" >&2
        exit 1
    fi
    "$PYTHON_BIN" - "$TWAP_V1_DIR/twap_symmetric_v1_manifest.json" \
        "$V6_SOURCE" "$TWAP_RELEASE" <<'PY'
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
if release.get("live_feature_contract") != manifest.get("live_feature_contract"):
    raise SystemExit("TWAP release/manifest live-feature-contract mismatch")
if release.get("chainlink_window_seconds") != manifest.get("chainlink_window_seconds"):
    raise SystemExit("TWAP release/manifest Chainlink-window mismatch")
if release.get("decision_window_seconds") != manifest.get("decision_window_seconds"):
    raise SystemExit("TWAP release/manifest decision-window mismatch")
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
if release.get("execution_by_interval") != manifest.get("execution_by_interval"):
    raise SystemExit("TWAP release/manifest shared execution mismatch")
if manifest.get("contract_version") != 3 or release.get("contract_version") != 3:
    raise SystemExit("TWAP manifest is not the pooled fixed-model contract version")
if manifest.get("model_count") != 2 or release.get("model_count") != 2:
    raise SystemExit("TWAP release must contain exactly two models")
expected_routing = {"5m": "pooled_all4", "15m": "pooled_all4"}
if manifest.get("model_routing") != expected_routing or release.get("model_routing") != expected_routing:
    raise SystemExit("TWAP release/model routing is not pooled across all four coins")
expected_categorical = {"5m": ["coin_id"], "15m": ["coin_id"]}
if manifest.get("categorical_features") != expected_categorical or release.get("categorical_features") != expected_categorical:
    raise SystemExit("TWAP release/model coin_id is not categorical")
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
base_fraction = float(release.get("execution", {}).get("base_equity_fraction"))
dd_halt_fraction = float(release.get("execution", {}).get("max_drawdown_halt_frac"))
if float(config.get("max_drawdown_halt_frac")) != dd_halt_fraction:
    raise SystemExit("TWAP release/config max drawdown halt mismatch")
for interval, sleeve in sleeves.items():
    if float(sleeve["capital_frac"]) != base_fraction:
        raise SystemExit(f"TWAP release/config base fraction mismatch: {interval}")
    contract = manifest["sleeves"][interval]
    for key in (
        "ask_bucket", "side_filter", "book_consumption_max",
        "checkpoint_offset_set",
    ):
        if sleeve[key] != contract[key]:
            raise SystemExit(f"TWAP release/config {key} mismatch: {interval}")
    if sleeve["checkpoint_offset_set"] != release["checkpoint_offset_sets"][interval]:
        raise SystemExit(f"TWAP release/config checkpoint offsets mismatch: {interval}")
execution = release.get("execution", {})
required_execution = {
    "trend_following": True, "side_flip": False, "t3_enabled": False,
    "max_drawdown_halt_frac": 0.40,
    "tier_a_multiplier": 1.0, "fak_pad": 0.01,
    "retry_until_first_fill": True, "base_equity_fraction": 0.01,
}
if execution != required_execution:
    raise SystemExit("TWAP release execution contract mismatch")
if release.get("validation") != manifest.get("validation"):
    raise SystemExit("TWAP release/manifest validation mismatch")
print(f"TWAP release verified: {actual_hash[:12]} coins={','.join(coins)}")
PY
fi

# Live dashboard surface (light multi-page journal). The bundle must carry
# every JSX module index.html loads, in load order, plus the stylesheet and
# the SSE bridge. colors_and_type.css + preview/ were retired with the
# 2026-05-30 redesign; styles.css is the live theme now.
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

# ---- auto cache-bust ---------------------------------------------------------
# Stamp the SHIPPED index.html's ?v= tag with a content hash of the staged
# dashboard bundle, so browsers refetch JSX EXACTLY when it changes and never
# serve a stale module. Replaces the manual ?v= bump that kept being forgotten:
# 4974ef8 shipped a new data.jsx with an UNCHANGED buster, so the operator's
# browser rendered stale paper (+$1.43) while the server served cash (+$9.31).
# Hash covers every shipped .jsx + the stylesheet + the plain-JS dashboard
# helpers + the SSE bridge (index.html is refetched every load, so it needn't
# be in the hash); falls back to a UTC
# timestamp if git hashing is unavailable. Staging-only — never touches the
# working tree or the trading path.
_dash_hash=""
if _h="$(cat "$STAGING_DIR/dashboard/"*.jsx "$STAGING_DIR/dashboard/styles.css" "$STAGING_DIR/dashboard/time.js" "$STAGING_DIR/dashboard/api.js" "$STAGING_DIR/dashboard/live-bridge.js" 2>/dev/null | git hash-object --stdin 2>/dev/null)"; then
    _dash_hash="$(printf '%s' "$_h" | cut -c1-10)"
fi
[ -n "$_dash_hash" ] || _dash_hash="$(date -u +%Y%m%d%H%M%S)"
sed -i "s#?v=v4-[0-9a-z]*#?v=v4-$_dash_hash#g" "$STAGING_DIR/dashboard/index.html"
echo "==> dashboard cache-buster -> v4-$_dash_hash ($(grep -o "?v=v4-$_dash_hash" "$STAGING_DIR/dashboard/index.html" | wc -l | tr -d ' ') refs)"

# Unit files / installers / env contract. Sources are now per-venue
# (polymarket/{live,recorder}/service, polymarket/infra, predictfun/...),
# but they all still land in ONE flat $STAGING_DIR/deploy/ because the
# REMOTE layout ($PROJECT_DIR/deploy/) is unchanged and every unit file's
# ExecStart= and install_remote.sh's own paths point at it.
for src in \
    polymarket/infra/cron.daily.sh \
    polymarket/live/service/cryptobot-scanner.logrotate \
    polymarket/infra/cryptobot.env.example \
    polymarket/infra/check_twap_rtds.py \
    polymarket/live/service/cryptobot.service \
    polymarket/live/service/cryptobot-wal-checkpoint.service \
    polymarket/live/service/cryptobot-wal-checkpoint.timer \
    polymarket/live/service/cryptobot-failure-alert.service \
    predictfun/live/service/cryptobot-predictfun.service \
    polymarket/infra/deploy.sh \
    polymarket/infra/install_remote.sh \
    polymarket/infra/requirements.runtime.txt
do
    cp "$REPO_ROOT/$src" "$STAGING_DIR/deploy/"
done
# BOTH roles' units ship in EVERY bundle on purpose (same reasoning as staging
# both venue packages): install_remote.sh picks which ones to lay down from
# CRYPTOBOT_BOX_ROLE, so one tarball can provision either box and a role flip
# needs no re-stage.
# RETIRED 2026-08-01 with the cross-venue dashboard view — do NOT re-add:
#   cryptobot-snapshot-refresh.{service,timer}      (predictfun_paper.json)
#   cryptobot-predictfun-snapshot.{service,timer}   (predictfun_live.json)
#   refresh_dashboard_snapshots.sh                  (their producer)
# Each console renders only its own deployment's SSE payload now, so those
# dumps have no consumer; install_remote.sh disables + removes any units left
# behind on a box.

echo "==> packing tarball: $TARBALL"
tar --create --gzip --file="$TARBALL" --directory="$STAGING_DIR" .
TARBALL_SIZE=$(du -h "$TARBALL" | cut -f1)
echo "==> tarball size: $TARBALL_SIZE"
TARBALL_BYTES=$(stat -c "%s" "$TARBALL" 2>/dev/null || stat -f "%z" "$TARBALL" 2>/dev/null || echo 0)
if [ "$TARBALL_BYTES" -gt 209715200 ]; then
    echo "WARN: tarball >200 MB - runtime bundle may have grown unexpectedly."
    echo "      Largest 10 files in tarball:"
    tar -tzvf "$TARBALL" | sort -k3 -n -r | head -10
fi

# ---- 2. SSH connectivity probe ----------------------------------------------
SSH_OPTS="-i $PEM_PATH -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o IdentitiesOnly=yes"
echo "==> probing SSH"
if ! ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" "echo OK && uname -a"; then
    echo "ERROR: SSH probe failed. Check:"
    echo "  - EC2_HOST is the public DNS/IP"
    echo "  - Security group inbound rule allows port 22 from your operator IP"
    echo "  - EC2_USER is correct (ec2-user for Amazon Linux, ubuntu for Ubuntu)"
    exit 1
fi

# ---- 3. Upload + extract -----------------------------------------------------
REMOTE_TAR="/tmp/$(basename "$TARBALL")"
echo "==> scp tarball"
scp $SSH_OPTS "$TARBALL" "$EC2_USER@$EC2_HOST:$REMOTE_TAR"

echo "==> extracting into $PROJECT_DIR_REMOTE"
REMOTE_PROJECT_ESCAPED=$(printf '%q' "$PROJECT_DIR_REMOTE")
REMOTE_TAR_ESCAPED=$(printf '%q' "$REMOTE_TAR")
REMOTE_SERVICE_USER_ESCAPED=$(printf '%q' "$SERVICE_USER")
ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" \
    "PROJECT_DIR_REMOTE=$REMOTE_PROJECT_ESCAPED REMOTE_TAR=$REMOTE_TAR_ESCAPED SERVICE_USER=$REMOTE_SERVICE_USER_ESCAPED bash -s" <<'REMOTE_EXTRACT'
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
cleanup_extract() {
    rm -rf "$tmp_extract"
}
trap cleanup_extract EXIT

mkdir -p "$PROJECT_DIR_REMOTE"
tar -xzf "$REMOTE_TAR" -C "$tmp_extract"
test -d "$tmp_extract/src/cryptobot"  # legacy-path-ok: remote layout
# Phase 2: the venue adapters must be in the bundle or the scanner cannot
# boot (get_adapter imports them) and grade-time cannot price a PF row.
test -d "$tmp_extract/src/cryptobot_polymarket"  # legacy-path-ok: remote layout
test -d "$tmp_extract/src/cryptobot_predictfun"  # legacy-path-ok: remote layout
test -f "$tmp_extract/deploy/install_remote.sh"  # legacy-path-ok: remote layout
# v2 active config + its models are now boot-critical (V6_CONFIG points here and
# the only sleeves are the checkpoint sub-minute ones).
test -f "$tmp_extract/runtime/config/v2_step1last_live.json"
# predict.fun DRY paper instance's generated all-DRY artifact (its unit's
# V6_CONFIG points here; missing => the paper instance can never boot).
test -f "$tmp_extract/runtime/config/v2_step1last_predictfun.json"
test -f "$tmp_extract/data/models/checkpoint_v1/checkpoint_v1_manifest.json"
test -f "$tmp_extract/data/models/twap_symmetric_v1/twap_symmetric_v1_manifest.json"

mkdir -p "$PROJECT_DIR_REMOTE/data"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$PROJECT_DIR_REMOTE"

# Replace the deployed runtime tree while preserving the venv and the live
# SQLite ledger sidecars. This removes stale backtest/research/docs/scripts
# files left by older additive deploys.
find "$PROJECT_DIR_REMOTE" -mindepth 1 -maxdepth 1 \
    ! -name ".venv" \
    ! -name "data" \
    -exec rm -rf -- {} +
# book_recorder/predictfun_book_recorder held irreplaceable append-only COLLECTED
# DATA, not deployed artifacts: the recorders needed weeks of continuous history
# to be usable (book parity study ~3-4 weeks). They were never staged into the
# tarball, so without these exclusions every deploy silently restarted collection
# from zero — the 2026-07-19 deploy destroyed 07-16→07-19 of recorder history
# that way. The recorders were RETIRED 2026-08-02 and their code deleted
# 2026-08-04; both exclusions are KEPT deliberately as fail-safes, so that a box
# still holding un-pulled parquet/JSONL is not wiped by a deploy. Nothing writes
# there any more (install_remote.sh no longer even creates the dirs).
# RTDS recorder retired 2026-08-01, prereg abandoned ungraded by operator order:
# its `! -name "rtds_recorder"` exclusion was REMOVED deliberately, so the next
# deploy reclaims data/rtds_recorder on the box. That is the intent, not a gap.
# predictfun_paper/ + predictfun_live/ are the predict.fun instance's OWN
# runtime DB dirs (never staged into the tarball) — same b70a95f lesson as the
# recorder dirs: without the exclusion every deploy would DELETE the DB.
# ⚠️ predictfun_live/ is the REAL-MONEY live DB (ledger + operator state +
# wallet modes); wiping it on 2026-07-23 reset the live bot to
# STOPPED_PROCESSING mid-session and lost the settled-bet accounting. NEVER
# remove this exclusion, and add any NEW data/ runtime dir here too.
find "$PROJECT_DIR_REMOTE/data" -mindepth 1 -maxdepth 1 \
    ! -name "cryptobot.db" \
    ! -name "cryptobot.db-wal" \
    ! -name "cryptobot.db-shm" \
    ! -name "predictfun_paper" \
    ! -name "predictfun_live" \
    ! -name "book_recorder" \
    ! -name "predictfun_book_recorder" \
    -exec rm -rf -- {} +

cp -a "$tmp_extract"/. "$PROJECT_DIR_REMOTE"/
rm -f "$REMOTE_TAR"
chmod +x "$PROJECT_DIR_REMOTE/deploy/"*.sh
REMOTE_EXTRACT

# ---- 4. Remote install -------------------------------------------------------
echo "==> running install_remote.sh on the instance"
ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" \
    "PROJECT_DIR='$PROJECT_DIR_REMOTE' SERVICE_USER='$SERVICE_USER' \
     RESET_RECORDS='${RESET_RECORDS:-0}' \
     CRYPTOBOT_BOX_ROLE='$BOX_ROLE' \
     bash '$PROJECT_DIR_REMOTE/deploy/install_remote.sh'"  # legacy-path-ok: remote layout

# ---- 5. Wrap-up --------------------------------------------------------------
rm -f "$TARBALL"
rm -rf "$STAGING_DIR"
trap - EXIT

if [ "$BOX_ROLE" = "predictfun" ]; then
    DEPLOY_UNIT="cryptobot-predictfun"
    DEPLOY_PORT="8801"
    DEPLOY_DRIVER="bash predictfun/infra/deploy.sh"
else
    DEPLOY_UNIT="cryptobot"
    DEPLOY_PORT="8800"
    DEPLOY_DRIVER="bash polymarket/infra/deploy.sh"
fi
echo
echo "================================================================"
echo "DEPLOY COMPLETE ($BOX_ROLE box)"
echo "================================================================"
echo "To watch the live scanner log:"
echo "  ssh -i $PEM_PATH $EC2_USER@$EC2_HOST 'sudo journalctl -fu $DEPLOY_UNIT'"
echo
echo "To open the dashboard (SSH tunnel):"
echo "  ssh -L $DEPLOY_PORT:127.0.0.1:$DEPLOY_PORT -i $PEM_PATH $EC2_USER@$EC2_HOST"
echo "  then browse http://127.0.0.1:$DEPLOY_PORT/"
echo
echo "To stop the bot:"
echo "  ssh -i $PEM_PATH $EC2_USER@$EC2_HOST 'sudo systemctl stop $DEPLOY_UNIT'"
echo
echo "To redeploy (after editing code locally):"
echo "  EC2_HOST=$EC2_HOST PEM_PATH=$PEM_PATH $DEPLOY_DRIVER"
echo "================================================================"
