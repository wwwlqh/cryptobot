#!/usr/bin/env bash
# install_remote.sh - runs on the EC2 instance.
#
# Idempotent installer. Safe to re-run on every deploy:
#   * creates the venv if missing
#   * upgrades runtime deps in-place
#   * lays down/refreshes the systemd unit + env file
#   * restarts the service so the new code takes effect

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/cryptobot}"
SERVICE_USER="${SERVICE_USER:-$(whoami)}"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="/var/log/cryptobot"

# ---- 0. BOX ROLE ------------------------------------------------------------
# ONE installer, TWO boxes (predict.fun Tokyo cutover, 2026-08-01):
#
#   polymarket (DEFAULT) — eu-west-1 Ireland: the Polymarket scanner, its book
#                          recorder and the poly WAL/audit job. UNSET == this,
#                          and the whole polymarket path below is byte-identical
#                          to the pre-role installer.
#   predictfun           — ap-northeast-1 Tokyo: the predict.fun scanner ONLY
#                          (REAL money). The Polymarket scanner unit is never
#                          installed, enabled or restarted here, the poly book
#                          recorder is pinned OFF, and the daily WAL/audit job
#                          targets the predict.fun LIVE DB.
#
# Plumbed from polymarket/infra/deploy.sh (which reads CRYPTOBOT_BOX_ROLE from
# the operator env; predictfun/infra/deploy.sh pins it) through the remote
# install invocation.
BOX_ROLE="${CRYPTOBOT_BOX_ROLE:-polymarket}"
case "$BOX_ROLE" in
    polymarket|predictfun) : ;;
    *)
        echo "ERROR: unknown CRYPTOBOT_BOX_ROLE='$BOX_ROLE' (want polymarket|predictfun)" >&2
        exit 1
        ;;
esac

echo "==> install_remote: PROJECT_DIR=$PROJECT_DIR  USER=$SERVICE_USER  ROLE=$BOX_ROLE"

# ---- 1. OS detection + system packages --------------------------------------
if [ -f /etc/amazon-linux-release ] || grep -q "Amazon Linux" /etc/os-release 2>/dev/null; then
    OS_FAMILY="al2023"
    PKG_INSTALL="sudo dnf install -y"
    PYTHON_PKG="python3.11 python3.11-pip"
    PYTHON_BIN="python3.11"
    SQLITE_PKG="sqlite"
elif grep -q "Ubuntu" /etc/os-release 2>/dev/null; then
    OS_FAMILY="ubuntu"
    PKG_INSTALL="sudo apt-get install -y"
    sudo apt-get update -q
    PYTHON_PKG="python3 python3-venv python3-pip"
    PYTHON_BIN="python3"
    SQLITE_PKG="sqlite3"
else
    echo "ERROR: unsupported OS - only Amazon Linux 2023 and Ubuntu detected."
    cat /etc/os-release 2>/dev/null || true
    exit 1
fi
echo "==> OS family: $OS_FAMILY"

echo "==> installing system packages"
$PKG_INSTALL $PYTHON_PKG $SQLITE_PKG gcc tar gzip

# ---- 2. venv + runtime deps -------------------------------------------------
if [ ! -d "$VENV_DIR" ]; then
    echo "==> creating venv at $VENV_DIR"
    $PYTHON_BIN -m venv "$VENV_DIR"
fi
echo "==> upgrading pip + installing runtime deps"
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

# Retired-dashboard-asset cleanup. The deploy tarball overlays files in-place,
# so a file deleted locally would otherwise be served forever off the box; when
# an asset is retired, add an explicit `rm -f` for it here.
# mock-payload.js and assets/coins/doge.svg were pruned from this list
# 2026-08-01 (neither has existed in the repo for months, so the removal had
# long since taken effect on the box). bnb.svg came off the list 2026-07-21 —
# the predict.fun venue port re-added it (BNB is in the predictfun universe)
# and this cleanup was wiping it on every deploy; it must STAY off the list.
# 2026-08-01: the cross-venue dashboard view was retired, so the two static
# JSON dumps it polled have no producer and no allowlist entry any more. They
# are deployed-dir artifacts, so remove them explicitly or the box keeps a
# stale trading-row dump on disk forever.
sudo rm -f "$PROJECT_DIR/dashboard/predictfun_paper.json" \
    "$PROJECT_DIR/dashboard/predictfun_live.json" \
    "$PROJECT_DIR/dashboard/predictfun_paper.json.tmp" \
    "$PROJECT_DIR/dashboard/predictfun_live.json.tmp"
find "$PROJECT_DIR/dashboard" -type d -name "__pycache__" -prune -exec rm -rf -- {} +

# ---- 4. systemd unit + env file ---------------------------------------------
# ROLE: the Polymarket scanner unit is a polymarket-box artifact. On a
# predict.fun box it is never written, never enabled and never restarted — the
# poly scanner must not exist there (no poly signing key, no poly DB, and a
# second consumer of the shared strategy would be a split brain).
if [ "$BOX_ROLE" = "polymarket" ]; then
    echo "==> installing systemd unit (cryptobot.service)"
    sudo sed "s|^User=ec2-user|User=$SERVICE_USER|; s|^Group=ec2-user|Group=$SERVICE_USER|; s|/home/ec2-user/cryptobot|$PROJECT_DIR|g" \
        "$PROJECT_DIR/deploy/cryptobot.service" \
        | sudo tee /etc/systemd/system/cryptobot.service > /dev/null
else
    echo "==> ROLE=predictfun — SKIPPING cryptobot.service (Polymarket scanner)"
fi

if [ ! -f /etc/cryptobot.env ]; then
    echo "==> seeding /etc/cryptobot.env from example (first install)"
    sudo sed "s|/home/ec2-user/cryptobot|$PROJECT_DIR|g" \
        "$PROJECT_DIR/deploy/cryptobot.env.example" \
        | sudo tee /etc/cryptobot.env > /dev/null
    sudo chown root:"$SERVICE_USER" /etc/cryptobot.env
    sudo chmod 0640 /etc/cryptobot.env
else
    echo "==> /etc/cryptobot.env exists - preserving operator overrides"
fi

set_env_line() {
    key="$1"
    value="$2"
    if sudo grep -q "^${key}=" /etc/cryptobot.env; then
        sudo sed -i "s|^${key}=.*|${key}=${value}|" /etc/cryptobot.env
    else
        echo "${key}=${value}" | sudo tee -a /etc/cryptobot.env > /dev/null
    fi
}

# Scrub a retired knob from the live env so a stale key never shadows a future
# re-introduction of the same name. No-op when the key is absent.
remove_env_line() {
    key="$1"
    sudo sed -i "/^${key}=/d" /etc/cryptobot.env
}

ensure_env_line() {
    key="$1"
    value="$2"
    if sudo grep -q "^${key}=" /etc/cryptobot.env; then
        current="$(sudo grep "^${key}=" /etc/cryptobot.env | tail -n 1 | cut -d= -f2-)"
        if [ -z "$current" ]; then
            sudo sed -i "s|^${key}=.*|${key}=${value}|" /etc/cryptobot.env
        fi
    else
        echo "${key}=${value}" | sudo tee -a /etc/cryptobot.env > /dev/null
    fi
}

# These are deployment-layout facts, not operator tuning knobs. Keep dashboard
# host/port, top-up, sweep cadence, and sizing overrides intact.
set_env_line PYTHONPATH "$PROJECT_DIR/src"
# Belt-and-braces after the 2026-07-28 venue-tree restructure: config.py
# resolves the project root by walking for a pyproject.toml / polymarket/src
# marker, and the deployed bundle ships NEITHER (it is a flat src/ tree).
# Pinning CRYPTOBOT_ROOT means the box never depends on that fallback —
# a mis-resolved root would relocate the live SQLite ledger.
set_env_line CRYPTOBOT_ROOT "$PROJECT_DIR"
set_env_line PYTHONWARNINGS "ignore::UserWarning:sklearn.utils.parallel"
set_env_line CRYPTOBOT_DB_PATH "$PROJECT_DIR/data/cryptobot.db"
# 2026-07-08 v2 "step1last" promotion: V6_CONFIG points at v2_step1last_live.json
# (checkpoint-only sub-minute sleeves at 100% of live) and the exposure cap + DD
# halt MUST equal its drift-check fields or the loader fails closed at boot. The
# three move together as a trio — see polymarket/infra/cryptobot.env.example
# (staged to $PROJECT_DIR/deploy/cryptobot.env.example on the box).
# ORB/exp044 retired 2026-07-09: rollback = git revert the removal + redeploy
# (the env-flip-to-exp044 path is gone; exp044_live.json is no longer shipped).
# set_env_line force-pins these on every deploy, so they live here, not just in
# the operator-edited /etc/cryptobot.env.
set_env_line CRYPTOBOT_V6_CONFIG "$PROJECT_DIR/runtime/config/v2_step1last_live.json"
set_env_line CRYPTOBOT_DASHBOARD_DIR "$PROJECT_DIR/dashboard"
set_env_line CRYPTOBOT_MIN_BET_USD "1.0"
set_env_line CRYPTOBOT_SHARED_EXPOSURE_CAP "1.0"
# Risk retune (operator-ordered 2026-07-21): hard halt 0.35 -> 0.40, paired
# with the brake retune below (V2_BRAKE_SCALE 0.60 = half size at >=30% DD).
# THROTTLE REMOVAL (operator-ordered 2026-07-31): hard halt 0.40 -> 0.30. The
# graduated brake below is now inert (floor 1.0) and both intervals size flat at
# 1%, so this halt is the ONLY drawdown guard. The deployed both-hold twin's
# worst-fold backtest maxDD is 26.6% (fold C), so 0.30 clears every measured
# drawdown at the new size — see
# polymarket/backtest/studies/tick_latency/THROTTLE_REMOVAL_SIZE_20260731.md.
# Must equal config.py MAX_DRAWDOWN_HALT_FRAC + the artifact's
# max_drawdown_halt_frac or the loader drift-check fails closed at boot.
# This is the shared/predict.fun default; the Polymarket TWAP block below
# deliberately overrides it to the operator-selected 0.40 release contract.
set_env_line CRYPTOBOT_MAX_DD_HALT_FRAC "0.30"
# v2 "step1last" promotion (2026-07-08) — force-pinned every deploy so the
# checkpoint-only config actually loads and topups stay off:
#   * SUBMINUTE_ENABLED=1 is REQUIRED — with it off the loader drops the
#     checkpoint sleeves and a checkpoint-only config has nothing to run.
#   * ENABLE_TOPUPS=0 enforces one order per market/resolution.
#   * COINS pins BTC,ETH ONLY (operator order 2026-08-02: fresh era, trade
#     only ETH+BTC — supersedes the 2026-07-21 4-coin override; SOL+XRP
#     dropped from the live scan set). Pinned explicitly so it is independent
#     of the flag's coin-set side effect.
#   * The V2_* knobs are the equity-fraction sizing (flat 2% base frac on both
#     intervals, brake inert at floor 1.0, 1/sqrt(k) concurrency); see config.py.
# 2026-07-08 operator decision: v2 "step1last" is a NEW strategy — do not judge
# it on the old sub-minute add-on's live record. (a) RESET the breaker window to
# the v2 go-live instant so only v2's own orders/settled bets feed it; (b) DISABLE
# both breaker trip arms (fill-rate floor -> 0.0 can never trip; realized-EV max
# loss -> 999x can never trip). The old add-on's -21% / n=30 no longer blocks v2.
# CRYPTOBOT_SUBMINUTE_KILL is still the emergency stop; the fail-closed DB-error
# arm remains. These are PINNED so a redeploy cannot silently re-arm the breaker
# and halt trading. To restore the guard: delete these 3 lines (or set RESET to a
# fresh ts + fill-rate 0.2) and redeploy.
set_env_line CRYPTOBOT_SUBMINUTE_BREAKER_RESET "2026-07-08T13:45:00Z"
set_env_line CRYPTOBOT_SUBMINUTE_BREAKER_MIN_FILL_RATE "0.0"
set_env_line CRYPTOBOT_SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC "999"
set_env_line CRYPTOBOT_SUBMINUTE_ENABLED "1"
set_env_line CRYPTOBOT_ENABLE_TOPUPS "0"
# Universe (operator-ordered 2026-08-04): SOL+XRP re-enabled — 4-coin scan set
# again (supersedes the 2026-08-02 BTC,ETH-only cut), paired with the 1% resize
# below as the reduced-risk re-entry experiment; forward-OOS grades it.
set_env_line CRYPTOBOT_COINS "BTC,ETH,SOL,XRP"
# Bet size (operator-ordered 2026-08-04, with the SOL+XRP re-enable): back to
# FLAT 1% of equity on BOTH intervals — supersedes the 2026-08-02 2%/2%
# (which had superseded the 2026-07-31 1%/1%; the 07-30 study's case against
# the 2:1 asymmetry still stands — FLAT across intervals stays).
# 2026-08-08 operator order (flip era): 1% -> 3% BOTH intervals — the
# ENTRY_LAST_SECS_ONLY=15 gate cut volume ~80% and the operator sized up the
# remaining band. Halt 0.30 + fail-closed trio unchanged.
# 2026-08-09 operator order: FLIP EXPERIMENT CLOSED at 53/100 trades (era
# +$18.23 but one +$38 outlier deep; bootstrap P(<=0)=39%; operator chose the
# BT verdict — flip -EV under both label regimes). Size restored 3% -> 1%
# with the rest of the pre-flip config. Same day, operator orders: 1% -> 2%
# flat BOTH intervals, then (after the ~13:15 UTC flip re-arm, full-window)
# back to 1% flat. (THIS env pair is THE live sizing knob —
# sizing_v2.sized_usd reads config.V2_BASE_FRAC_*; the artifact
# capital_frac is the loader mirror, updated in lockstep.)
set_env_line CRYPTOBOT_V2_BASE_FRAC_15M "0.010"
set_env_line CRYPTOBOT_V2_BASE_FRAC_5M "0.010"
# DD-brake throttle (risk retune, operator-ordered 2026-07-21): scale 0.60 /
# floor 0.50 — full size at 0% DD, ramping linearly to HALF size at >=30% DD.
# THROTTLE REMOVED (operator-ordered 2026-07-31): floor 0.50 -> 1.0 pins the
# brake multiplier max(floor, 1-dd/scale) at 1.0 for every drawdown depth, i.e.
# the graduated throttle is OFF. Per-$ edge was measured flat across drawdown
# depth, so the brake bought no edge and only dragged the recovery; its risk
# job is now carried by the 1%/1% resize plus the 0.30 hard halt. SCALE stays
# 0.60 so the throttle can be re-armed by moving the FLOOR alone.
# Force-pinned so a redeploy cannot silently revert; MUST match config.py
# V2_BRAKE_* / execution/sizing_v2.py DEFAULT_BRAKE_* / v2_step1last_live.json.
set_env_line CRYPTOBOT_V2_BRAKE_SCALE "0.60"
set_env_line CRYPTOBOT_V2_BRAKE_FLOOR "1.0"
# F2 tier-A size boost (operator-ordered 2026-07-14): 1.5x stake when the PLACE
# is the market's FIRST qualifying second (spec_v2 WindowDecider
# market_place_rank == 1) AND the score edge >= 0.0465. Backtest E-holdout:
# A-tier first fills earn ~2x per-$ vs the rest. MULT=1.0 disables.
# 2026-08-08 operator order: MULT 1.5 -> 1.0 while SUBMINUTE_FLIP_SIDE is ON.
# The 1.5x evidence base is MODEL-side tier-A performance, which the flip
# inverts — under the flip it concentrated 41% of stake on the cohort where
# the model most disagrees with the executed side.
# 2026-08-09: flip CLOSED — 1.5 restored; ~13:00 UTC flip RE-ARMED — back
# to 1.0 (the 1.5x evidence is model-side, inverted under the flip).
set_env_line CRYPTOBOT_V2_TIER_A_MULT "1.0"
set_env_line CRYPTOBOT_V2_TIER_A_EDGE "0.0465"
set_env_line CRYPTOBOT_V2_CONC_DOWNWEIGHT "1"
# v2 reject-retry (operator-ordered 2026-07-09, upgraded same day to
# consume-on-fill): a LIVE sub-minute FAK rejection RELEASES the window — every
# subsequent second re-evaluates via the normal per-second path (fresh book,
# fresh score, band + gate at that second's ask). Only a FILL (or window end)
# consumes the market; ONE_PER_WINDOW / one-bet-per-resolution is preserved.
set_env_line CRYPTOBOT_V2_REJECT_RETRY "1"
# v2 FAK pay-up pad (operator-ordered 2026-07-11): add this many dollars to the
# walked FAK BUY limit so a marketable-limit taker crosses the touch instead of
# resting and getting rejected. ANCHORED — capped at the window's FIRST walked
# limit + pad (spec_v2 WindowDecider._first_limit), so a reject-retry later in the
# window can never chase a moved-up book; also capped at 0.99. INERT at 0.0
# (config.py default = byte-identical send of the bare walked_limit); the operator
# pins the flat +0.01 rung (the only marginally +EV pad — reject_pad research
# 2026-07-11, n=15). Sub-minute single-BUY path only (pair-path BetSignals are
# untouched). Force-pinned so a redeploy cannot silently drop it; to disable,
# delete this line (or set 0.0) and redeploy.
set_env_line CRYPTOBOT_V2_FAK_PAD "0.01"
# EVERY-QUALIFYING RETRIES — ARMED (operator order 2026-07-25). Drops the anchor
# clamp above for attempt 2..N, so a retry that re-qualified on a NEW second pays
# its OWN fresh walked limit + pad instead of the FIRST qualifying second's
# price. Operator: "even with 2nd 3rd attempt, it still gets caught with first
# qualifying condition." Confirmed on clean ticks: 41% of retry attempts (88/213)
# were sent BELOW the ask the bot itself displayed — structurally unfillable, the
# anchor rather than absent liquidity. Attempt 1 is unchanged in both modes.
# ⚠️ This is the chase axis and the research is NOT a green light: PRE-era chase
# negative in 7/7 cells, RESET-era UNIDENTIFIED (two simulators on opposite sides
# of zero, $26-wide band). Armed by operator order over that finding. Every entry
# gate still binds the retry BEFORE its price is formed (band 0.65-0.85, edge
# >=0.03 re-scored at that second's ask, walker min_edge + MAX_WALK_PRICE,
# V2_EDGE_CAP 0.08, RETRY_DIP_GATE), so it pays up only as far as the edge floor
# allows inside the band. Revert = 0 (env-only, no redeploy).
# GRADE: subminute_fills WHERE attempt > 1 — fill rate + realized c/$1 of the
# repriced cohort vs the anchored history.
set_env_line CRYPTOBOT_V2_RETRY_REPRICE "1"
# Fast-requote (operator armed live 2026-07-22, 29c8674) — force-pinned so a
# redeploy cannot silently drop the lever or, worse, keep the lever while
# dropping its adverse-dip fence. MIN_DELTA -0.02: drop the requote when the
# fresh ask dipped >2c below the rejected limit — that cohort is -20c/$ in
# reject_requotes ground truth (negative on all 4 coins, 10/11 days) while the
# unchanged-book cohort is +13..16c/$; -0.02 is the empirical sign crossover
# (workflow-verified 2026-07-22, 3 independent re-derivations). DRAIN_MS 100
# decouples the drain cadence from the 400ms item delay so effective re-fire is
# ~400-500ms instead of uniform 400-800ms (hygiene, default-preserving).
set_env_line CRYPTOBOT_FAST_REQUOTE_ENABLED "1"
set_env_line CRYPTOBOT_FAST_REQUOTE_MIN_DELTA "-0.02"
set_env_line CRYPTOBOT_FAST_REQUOTE_DRAIN_MS "100"
# Preserve the checkpoint strategy's adverse-dip retry fence by default.
set_env_line CRYPTOBOT_RETRY_DIP_GATE_ENABLED "1"
# Parallel Phase-B POSTs (verified 2026-07-22): 5.7% of ticks carry >1 order and
# serial POSTs cost ~350ms each; position-2 orders underperform position-1 by
# ~16c/$ via adverse CHEAP fills (fills-below-limit 37.6% vs 24.7%), not misses.
# Grade post-hoc on pos-2 fill quality (cheap-fill fraction / WR / pnl per $);
# EXPECT pos-2 no_liquidity to RISE toward ~38% — a falling miss rate here is
# the failure mode, not the win.
set_env_line CRYPTOBOT_PHASE_B_PARALLEL_POST "1"
set_env_line CRYPTOBOT_V2_EDGE_CAP "0.15"
# Multifill entry-count cap REMOVED (operator order 2026-07-11): above the
# equity floor a resolution could take ANY number of independent same-side
# entries. That ceiling is BACK as an extras-keyed cap below (operator order
# 2026-07-25). The old CRYPTOBOT_V2_MULTIFILL_MAX pin stays dead — scrub it so a
# stale value can never be misread as the new key, whose semantics differ
# (extras on top of the initial entry, not a total).
remove_env_line CRYPTOBOT_V2_MULTIFILL_MAX
# EVERY-QUALIFYING ENTRIES — ARMED (operator order 2026-07-25). Dropping the
# multifill equity floor from $5000 to $0 makes multifill live at the current
# ~$130 equity: after the first qualifying second FILLS, every later qualifying
# second in the same window is a fresh independent entry, re-scored on a fresh
# book/prob and sized normally. Entry semantics go from "the first qualifying
# bet owns the resolution" to "every qualifying bet".
#
# MAX_EXTRA 3 = 1 initial entry + 3 extras = at most 4 non-topup ledger rows per
# (coin, interval, window). REQUIRED alongside the 0 floor: a 15m window offers
# 30 qualifying seconds, so uncapped it could stack ~30 entries ~= $97 at ~$130
# equity — ~75% of the bankroll on ONE binary. The exposure cap does NOT bound
# that (cap_tracks_equity: its ceiling IS equity, so it caps TOTAL exposure, not
# per-market concentration). Capped: 4 x ~$3.25 ~= $13, ~10%. When it binds the
# tick records skip_reason='multifill_cap' so the regrade can see it.
# Revert = MIN_EQUITY back to 5000, MAX_EXTRA to 0 (one entry per resolution), or
# CRYPTOBOT_V2_MULTIFILL_ENABLED=0 to disarm (all env-only, no redeploy).
# ⛔ MULTIFILL OFF (operator correction 2026-07-25, same day it was armed).
# Multifill is the WRONG mechanism for "every qualifying": it opens INDEPENDENT
# extra ENTRIES after a fill, so one resolution carried 4 entries + 2 T3 top-ups
# = 6 orders (BTC 15m ws=1784981700, ledger ids 3480-3485). The intended shape
# is ONE entry — retried every qualifying second until it FILLS — and then up to
# 3 top-ups on that single position:
#     attempt 1 rejected -> attempt 2 -> attempt 3 ... (NO ledger row until a
#     fill, so retries never multiply positions)  ==> 1 entry row
#     then T3 scale-in, SCALEIN_T3_MAX_N=3         ==> <= 3 top-up rows
#     TOTAL <= 4 orders per resolution.
# That shape needs NO multifill: V2_REJECT_RETRY=1 already gives the
# retry-until-fill cadence, V2_RETRY_REPRICE=1 lets each retry price off its own
# second, and note_fill consumes the window the moment one lands.
# MAX_EXTRA stays pinned as a belt if multifill is ever re-armed — it is inert
# while ENABLED=0 (the `and` short-circuits before the ledger query).
set_env_line CRYPTOBOT_V2_MULTIFILL_ENABLED "0"
set_env_line CRYPTOBOT_V2_MULTIFILL_MIN_EQUITY "0"
set_env_line CRYPTOBOT_V2_MULTIFILL_MAX_EXTRA "3"
set_env_line CRYPTOBOT_SCALEIN_T3_ENABLED "1"
set_env_line CRYPTOBOT_SCALEIN_T3_MAX_N "3"
# Both WFO-valid intervals remain enabled with every-qualifying entry selection.
# History for the record: 5m was DISABLED 2026-07-25 as the bleeder (at-ask
# -4.19c/$1 RESET / negative BOTH eras — a strategy result, not an execution
# one), and this re-enable is an explicit operator override of that data. The
# 5m model stop stays armed (+$5.88 same-sign alpha, n=75). Disable again =
# set to 0 (deploy-durable here; hand-edits to /etc/cryptobot.env survive only
# until the next deploy).
set_env_line CRYPTOBOT_INTERVAL_5M_ENABLED "1"
# Historical TWAP-era reset (operator order 2026-08-06; superseded below):
# Polymarket switches *-updown-* settlement to TWAP at 2026-08-07 00:00 UTC
# (5m=30s / 15m=60s at that historical cutover). New accounting era starts
# exactly there. Both products use 60s from the 2026-08-14 live refit onward. The
# bankroll anchor is the 2026-08-06 04:40:39 UTC reconciliation sample
# (clob == chain == 195.449861, 0 open orders, 0 open positions, status ok);
# the bot has been in STOPPED_PROCESSING since 2026-08-03, so the balance
# cannot move before the cutover — sampling early is exact, not approximate.
# Pre-cutover ledger rows/flows are ERA-FILTERED, never deleted (they are the
# pre-TWAP baseline for the fill-quality comparison under the fixed WS
# mirror). Supersedes the 2026-08-02 00:32:08 / 194.777193 era.
# WALLET_START_AT is deliberately owned by the record-cutover block below.
# Normal deploys preserve it; RESET_RECORDS=1 advances it.  Do not set it here:
# doing so silently reverted every operator reset to the old 2026-08-07 era.
# The T-150 live era was reset at 2026-08-15 02:44:37 UTC. Its reconciled
# CLOB and chain cash balances both equal 58.575353, with zero open orders and
# zero open positions. INITIAL_BANKROLL must equal that cutover balance so the
# equity graph and DD denominator start from real cash rather than the prior
# TWAP era's 91.526787 anchor. Normal deploys preserve WALLET_START_AT above.
set_env_line CRYPTOBOT_INITIAL_BANKROLL "58.575353"
# TWAP symmetric deployment (operator order 2026-08-12): Polymarket follows
# the selected model side on both intervals. Exact RTDS and causal Binance TWAP
# anchors are mandatory; no cross-source point-open fallback is allowed. Keep
# the separate predict.fun box on its existing checkpoint scorer and flip policy.
if [ "$BOX_ROLE" = "polymarket" ]; then
    # Exact 60s TWAP release (operator override 2026-08-15): all four coins,
    # shared T-150 entry window on 5m + 15m, causal Binance opening TWAP, 1%
    # base stake, top-ups + T3 off. These overrides are deliberately
    # Polymarket-only; predict.fun is not part of this cutover.
    set_env_line CRYPTOBOT_COINS "BTC,ETH,SOL,XRP"
    set_env_line CRYPTOBOT_V2_BASE_FRAC_15M "0.010"
    set_env_line CRYPTOBOT_V2_BASE_FRAC_5M "0.010"
    set_env_line CRYPTOBOT_MAX_DD_HALT_FRAC "0.40"
    set_env_line CRYPTOBOT_SCALEIN_T3_ENABLED "0"
    set_env_line CRYPTOBOT_V2_FIRST_ATTEMPT_ONLY "0"
    set_env_line CRYPTOBOT_FAST_REQUOTE_ENABLED "0"
    set_env_line CRYPTOBOT_RETRY_DIP_GATE_ENABLED "0"
    set_env_line CRYPTOBOT_V2_EDGE_CAP "0"
    set_env_line CRYPTOBOT_TWAP_SYMMETRIC_ENABLED "1"
    remove_env_line CRYPTOBOT_TWAP_SAME_DIRECTION_RESOLUTION_CAP
    set_env_line CRYPTOBOT_TWAP_RTDS_URL "wss://ws-live-data.polymarket.com"
    remove_env_line CRYPTOBOT_TWAP_RTDS_MAX_BOUNDARY_AGE_S
    set_env_line CRYPTOBOT_SUBMINUTE_FLIP_SIDE "0"
else
    set_env_line CRYPTOBOT_TWAP_SYMMETRIC_ENABLED "0"
    set_env_line CRYPTOBOT_SUBMINUTE_FLIP_SIDE "1"
fi
set_env_line CRYPTOBOT_SUBMINUTE_FLIP_PAD "0.02"
# Per-coin depth cap RETIRED (operator order 2026-07-11): stakes are pure
# equity-fraction sizing, no per-coin USD ceiling. One prefix pass scrubs any
# hand-added CRYPTOBOT_V2_DEPTH_CAP_<COIN>_<INTERVAL> override so a stale key
# can never shadow a future knob of the same name. (The old block also had a
# CRYPTOBOT_V2_DEPTH_CAP_ENABLED flag; that key is gone too and is covered by
# the same prefix.)
sudo sed -i '/^CRYPTOBOT_V2_DEPTH_CAP_/d' /etc/cryptobot.env
# STOP-LOSS RETIRED (operator order 2026-08-05). The PURE_STOP_04 model
# stop-loss EXIT is DELETED from the engine (execution/stop_loss.py is gone).
# It was already inert in production: CRYPTOBOT_STOPLOSS_EXCLUDE_INTERVALS was
# pinned "5,15", exempting BOTH traded intervals, so no position could be sold
# by it. Every filled position now HOLDs to binary settlement, which is exactly
# what the "5,15" pin already produced.
#
# The per-second held-position loop SURVIVES — it is the T3 scale-in driver
# (execution/held_position_pass.py, armed by CRYPTOBOT_SCALEIN_T3_ENABLED above)
# — so its two book knobs are RENAMED rather than dropped, keeping their live
# values. The old model-stop band lives on as the T3 model-deterioration DISARM
# band (CRYPTOBOT_SCALEIN_T3_DISARM_BAND, code default 0.04 = the value that was
# live, never pinned here).
#
# One prefix pass scrubs every retired key so a stale hand-added pin can never
# shadow a future knob of the same name (the CRYPTOBOT_V2_DEPTH_CAP_ precedent
# above). Covers ENABLED / BAND / MAX_ATTEMPTS / SELL_CHASE / EXCLUDE_INTERVALS
# / BOOK_CHAIN / BATCH_BOOKS / FAST_CADENCE_MS / MIN_EXIT_VALUE_USD, both
# CRYPTOBOT_STOP_SLIP_EXEMPT keys, and the retired entry-side floor.
sudo sed -i '/^CRYPTOBOT_STOPLOSS_/d' /etc/cryptobot.env
sudo sed -i '/^CRYPTOBOT_STOP_SLIP_EXEMPT/d' /etc/cryptobot.env
sudo sed -i '/^CRYPTOBOT_MIN_STOPPABLE_STAKE_USD=/d' /etc/cryptobot.env
# Held-position book-source chain (was CRYPTOBOT_STOPLOSS_BOOK_CHAIN, pinned "1"
# since 2026-07-11): route the pass's per-tick book reads through the SAME WS ->
# REST-prefetch -> inline fallback chain the entry path uses, instead of raw
# deps.fetch_book REST GETs. Fail-closed: with no chain wired
# (deps.book_prefetch is None) the reads are byte-identical. Force-pinned; to
# disable, delete this line (or set 0) and redeploy.
set_env_line CRYPTOBOT_HELD_PASS_BOOK_CHAIN "1"
# WS-mirror freshness gate EFFECTIVELY REMOVED (operator orders 2026-08-06:
# first 700 -> 2500ms for speed, then "remove the cap" once it was clear that
# snapshot age = market quietness, not feed lag — the venue pushes changes
# with ~1ms inter-frame gaps, so the mirror is current whenever anything
# moves). 600000ms (10 min) outlives every 5m/15m market window, so within a
# market's life the mirror always serves and the ~119-200ms REST batch never
# fires on the entry path. Residual risk accepted by the operator: a
# stalled-but-CONNECTED subscription would price a dead ladder until the
# window rolls (connection death still clears the mirror, orphan eviction
# still removes rolled-off tokens, and a FAK cannot fill worse than its
# ask+pad limit — a dead ladder mostly causes misses, not overpays).
set_env_line CRYPTOBOT_BOOK_WS_MAX_AGE_MS "600000"
# Late-window entry skip gate ARMED at 5s on ALL FOUR coins (operator order
# 2026-08-01) — supersedes the 2026-07-23 removal order and its no-re-add
# guardrail, and widens the retired 2026-07-21 BTC-only BTC:20 pin.
# Rationale: the LAST-SECONDS forensic (project_late_window_loss_20260801)
# found entries with <=10s left carry 87% of the era loss (−$37.63 over 214
# bets, p≈1e-7 against the pre-era base rate); mechanism is model
# overconfidence exactly as the market becomes informative, not
# execution/stop/top-ups. BT evidence is mixed: the cohort is -EV per flat
# dollar in the fill-corrected twin (-7.7 c/$ 5m) and the skip improves maxDD,
# but a TRUE re-run shows the skip costs end equity in hot regimes (fold C
# -37.6%) — so this pin is armed primarily on the LIVE two-era evidence, with
# a pre-registered grade on the skipped cohort. <=10s is larger but
# outlier-sensitive and stays OFF.
# ⚠ raw at-ask backtests rank this cohort BACKWARDS (ghost fills) — grade this
# against the fill-corrected pool only. Gate is LIVE-only by construction, so
# DRY paper rows deliberately diverge from live while it is armed.
# Force-pinned so a redeploy cannot silently drop it (config.py default is ""
# = OFF; this pin is the ONLY thing arming the gate). Kill switch = set the
# value to "" here and redeploy — a hand-edited /etc/cryptobot.env line is
# overwritten by this pin on the next deploy.
# ROLE: POLYMARKET-ONLY TRADING POLICY. The gate was armed on Polymarket's live
# two-era evidence (poly 4-coin universe, poly books, poly fills); nothing about
# it has been measured on predict.fun. Operator decision 2026-08-01: predict.fun
# TRADES THE FINAL 5 SECONDS. On the predict.fun role the shared key is pinned
# EMPTY (= config.py default = gate OFF) rather than left to whatever the
# cryptobot.env.example seed carried — the example ships the armed 4-coin value,
# so "just don't pin it" would have silently armed a poly policy on Tokyo.
# /etc/cryptobot-predictfun.env pins the same empty value a second time (it is
# loaded later, so it wins per-key) — belt and braces.
if [ "$BOX_ROLE" = "polymarket" ]; then
    # 2026-08-08 operator order (flip era): DISARMED — trade through the final
    # 5 seconds. The 5s arming rested on MODEL-side late-loss evidence, which
    # the flip inverts (same logic as the tier-A mult removal).
    # 2026-08-09: flip CLOSED — 5s skip RE-ARMED; ~13:00 UTC flip RE-ARMED —
    # DISARMED again (the late-loss forensic is model-side evidence, inverted
    # under the flip; flip trades through the final 5 seconds).
    set_env_line CRYPTOBOT_LATE_WINDOW_SKIP ""
    # Early-window gate: OFF (operator order 2026-08-09 flip re-arm — "no need
    # limit as last 15s"): unlike the 08-08 flip era, the re-armed flip trades
    # the FULL window, not just the final 15s. 0 disables.
    set_env_line CRYPTOBOT_ENTRY_LAST_SECS_ONLY "0"
else
    set_env_line CRYPTOBOT_LATE_WINDOW_SKIP ""
    set_env_line CRYPTOBOT_ENTRY_LAST_SECS_ONLY "0"
fi
# Ask-band skip gate: NOT pinned (operator decision 2026-07-21 — no SOL gate
# live; the 4-coin full-size override bets the backtest face numbers). Scrub
# any hand-added key on every deploy so a stale band can never linger and
# silently suppress a live cohort after the override.
remove_env_line CRYPTOBOT_ASK_BAND_SKIP
# NL15 retired (sleeve removed 2026-07-21, nl_* purged 2026-07-29): the
# CRYPTOBOT_NL15_MIN_ASK_DEPTH3_USD scrub that lived here was dropped 2026-08-01.
set_env_line CRYPTOBOT_SPOT_SOURCE "binance"
set_env_line CRYPTOBOT_SPOT_FETCH_INTERVAL_SECONDS "1"
set_env_line CRYPTOBOT_SPOT_USE_1S_KLINES "1"
set_env_line CRYPTOBOT_SPOT_STALENESS_S "5"
set_env_line CRYPTOBOT_BINANCE_BASE_URL "https://data-api.binance.vision"
# A7 spot prefetch lead — the prefetch daemon fetches the spot batch this many ms
# before each second boundary. Measured Binance fetch from the box is ~204ms, so
# the config default 200ms lead LOSES the race every tick (trace 2026-07-10); the
# prefetched bucket lands after the boundary and the tick pays the inline fetch.
# 450ms wins it. Spot is a 1s bucket, so the fetched VALUE is unchanged — only
# the race timing moves (SPOT_PREFETCH_ENABLED stays default-on). Force-pinned.
set_env_line CRYPTOBOT_SPOT_PREFETCH_LEAD_MS "450"
# A8 REST book prefetch-ahead — a daemon re-fetches the previous tick's
# POST /books token set before each second boundary; the tick reads those
# snapshots (served only if younger than 700ms) instead of paying the inline
# batch round-trip. Falls through to the inline fetch on any miss, so the
# failure mode is the legacy behavior. Validated (test_book_prefetch).
# LEAD 400ms (2026-07-10 trace): the config default 150ms LOSES the race — the
# POST /books takes ~230ms, lands ~80ms after the boundary and the tick BLOCKS
# ~170-200ms on WS-uncovered tokens. 400ms wins it; the prefetched book is
# ~170-400ms old at read (inside the 700ms freshness bound, dwarfed by the
# venue's 250ms taker delay) and the FAK fires earlier in the second.
set_env_line CRYPTOBOT_BOOK_PREFETCH_ENABLED "true"
set_env_line CRYPTOBOT_BOOK_PREFETCH_LEAD_MS "400"
# WS book mirror — venue-timestamped CLOB book off the market channel, read
# FIRST in the book source chain (real staleness, audit #12). AUTHORITATIVE since
# 2026-07-10 (operator order, after a ~2.5h shadow run: px/depth divergence ~5%
# = the WS-is-fresher signature at 1-2c, ws_missing ~64% falls through to the
# REST prefetch harmlessly — fail-closed chain WS -> REST prefetch -> inline).
# Kill switch: set ENABLED to "0" and redeploy; the tick falls straight back to
# the REST prefetch + inline batch.
set_env_line CRYPTOBOT_BOOK_WS_ENABLED "1"
set_env_line CRYPTOBOT_BOOK_WS_SHADOW "0"
# Self-hosted pmdata-mode book recorder (SEPARATE service, off the trading path)
# ⛔ RETIRED 2026-08-02, CODE DELETED 2026-08-04 (operator order: "no need book
# recorder"). Both venues' recorder modules, unit files and tests are gone from
# the repo; the units are torn down idempotently in the retired-unit convergence
# block below, so no flag is pinned here any more. Scrub any surviving
# CRYPTOBOT_BOOK_RECORDER_* / CRYPTOBOT_PREDICTFUN_RECORDER_* line from
# /etc/cryptobot.env by hand — they are inert now.
# WHY this is coherent rather than a loss of substrate: the recorder's only
# downstream consumer was the book-execution pool rebuild, which the operator
# killed on 2026-07-31 along with book_depth_index and the pmdata ledgers, and
# both venues' collected book history was deleted on 2026-08-02.
# NOT deleted and still load-bearing: the book_depth.py module, the
# CRYPTOBOT_BOOK_WS_* live-trading book feed pinned just above (a DIFFERENT
# thing — that is the execution path, not the recorder), and the local
# consolidated pools (poly_tick_last30.db, poly_l2_raw, snapshot_features_full).
# Recover from git history if it is ever wanted back.
# RTDS recorder retired 2026-08-01, prereg abandoned ungraded by operator order.
# Record window cutover. Default: seed only when missing/empty (first deploy).
# RESET_RECORDS=1 (passed through deploy.sh) FORCE-pins the cutover to this
# deploy instant. Ledger rows are never deleted; everything before the cutover
# just leaves the record window (bet_ts >= CRYPTOBOT_WALLET_START_AT). What
# resets: the dashboard record (journal, PnL, peak equity, max-DD%) AND —
# because the runtime DD replay reads the SAME cutover-scoped wallet snapshots
# (pipeline -> capital.get_wallet_snapshots) — the 0.30 max-DD hard halt and the
# DD-brake sizing multiplier re-anchor to a clean slate. A reset while halted /
# deep in drawdown RESUMES betting at full brake-free size; that is the point
# of the flag, but it must be a deliberate operator decision. NOT scoped by
# this timestamp: the LIVE group-exposure cap (real PENDING rows), the
# fill-rate breaker window, and CRYPTOBOT_SUBMINUTE_KILL.
WALLET_START_NOW="$(date -u '+%Y-%m-%d %H:%M:%S')"
if [ "${RESET_RECORDS:-0}" = "1" ]; then
    PREV_START="$(sudo grep '^CRYPTOBOT_WALLET_START_AT=' /etc/cryptobot.env | tail -n 1 | cut -d= -f2- || true)"
    echo "==> RESET_RECORDS=1 — cutting the accounting record over at $WALLET_START_NOW UTC"
    echo "    previous cutover was: ${PREV_START:-<unset>} (recorded here for forensics)"
    set_env_line CRYPTOBOT_WALLET_START_AT "$WALLET_START_NOW"
else
    ensure_env_line CRYPTOBOT_WALLET_START_AT "$WALLET_START_NOW"
fi

# ---- 4b. predict.fun instance env file (paper DRY | LIVE) -------------------
# The second scanner's per-key OVERRIDES of the shared /etc/cryptobot.env
# (systemd: a later EnvironmentFile wins per-key; inline Environment= lines
# would LOSE to EnvironmentFile values, hence a second file). Regenerated on
# every deploy — these are venue-seam layout facts, not operator tuning knobs;
# operator overrides belong in /etc/cryptobot.env (shared) or nowhere.
#
# MODE SWITCH (operator-armed; lives in the SHARED env so it SURVIVES
# deploys): CRYPTOBOT_PREDICTFUN_LIVE=1 in /etc/cryptobot.env flips the
# generated file from the paper-DRY variant to the REAL-MONEY variant.
# ⚠️ ARMING PREREQUISITE: the staged
# validation CLI (cbot-predictfun-live-validation) must have passed
# preflight/auth/build/reject/order on this box first — the submit wire was
# implemented 2026-07-23 from docs and is only PROVEN by that protocol.
#
# ROLE: predict.fun-box only. The Polymarket box stopped hosting predict.fun on
# 2026-08-01 (Tokyo cutover) — its PF units are disabled + removed by the
# convergence block near the end of this script, so regenerating their env file
# there would only leave a misleading artifact. The existing
# /etc/cryptobot-predictfun.env on a poly box is deliberately LEFT IN PLACE (it
# carries the retired paper era's accounting cutover; nothing reads it).
# The body below is left un-indented on purpose so this guard shows up as a
# two-line diff rather than re-indenting ~130 lines of load-bearing heredoc.
if [ "$BOX_ROLE" = "predictfun" ]; then
PF_LIVE_FLAG="$(sudo grep '^CRYPTOBOT_PREDICTFUN_LIVE=' /etc/cryptobot.env | tail -n 1 | cut -d= -f2- || true)"
if [ "$PF_LIVE_FLAG" = "1" ]; then
    # ---------------- LIVE variant (operator order 2026-07-23) --------------
    #   * DRY_RUN=false + the LIVE artifact (wallet_default_modes LIVE) on a
    #     FRESH live DB dir. The paper DB stays in place UNTOUCHED as the
    #     calibration baseline — never point live at it, never RESET over it.
    #   * PREDICTFUN_PRIVATE_KEY / PREDICTFUN_PREDICT_ACCOUNT /
    #     NOTIFY_TELEGRAM_TOKEN are NOT overridden here => inherited from the
    #     shared env (signing key present + live-money alerts ON — the point).
    #   * CRYPTOBOT_V2_BASE_FRAC_{5M,15M}=0.010 — THE sizing knob (operator
    #     order 2026-08-02: predict.fun bets 1% of equity per bet on BOTH
    #     intervals; superseded the 08-01 2%/2%, accepting that ~$2 stakes sit
    #     below the ~$3.5-4 stop-salvage floor).
    #     sizing_v2.base_fraction reads these from the env at CALL
    #     time, so the pin is live without a code change. This REPLACED
    #     CRYPTOBOT_BET_SIZE_MULTIPLIER=2, which was inert on the sub-minute
    #     path — pipeline.py's v2 sizing overwrites the portfolio target with
    #     sizing_v2's equity fraction, so the multiplier only ever moved
    #     portfolio_loader's (unused) target and made the dashboard's sizing
    #     panel lie about the real stake.
    #   * SIZING SCANS THE DEPOSIT (operator order 2026-07-23): every bet
    #     sizes off the live on-chain collateral balance (BalanceCache ->
    #     check_balance -> predict-account USDT, pipeline EQUITY SOURCE) —
    #     NEVER off a pinned constant; deposits/withdrawals take effect
    #     with no config change. CRYPTOBOT_PREDICTFUN_LIVE_BANKROLL
    #     (optional, default 100) seeds ONLY the DD brake/halt accounting
    #     basis + dashboard display + the stale-balance-cache sizing
    #     fallback — it is deliberately NOT re-read from the chain at boot
    #     (a restart with stakes locked would corrupt the drawdown series).
    #   * CRYPTOBOT_WALLET_START_AT: stamped at FIRST live arm, preserved
    #     across redeploys (per-venue accounting cutover).
    PF_BANKROLL="$(sudo grep '^CRYPTOBOT_PREDICTFUN_LIVE_BANKROLL=' /etc/cryptobot.env | tail -n 1 | cut -d= -f2- || true)"
    PF_BANKROLL="${PF_BANKROLL:-100}"
    PREV_PF_START="$(sudo grep '^CRYPTOBOT_WALLET_START_AT=' /etc/cryptobot-predictfun.env 2>/dev/null | tail -n 1 | cut -d= -f2- || true)"
    PF_START="${PREV_PF_START:-$WALLET_START_NOW}"
    echo "==> writing /etc/cryptobot-predictfun.env (⚠️ LIVE overrides; start=$PF_START bankroll=$PF_BANKROLL)"
    sudo tee /etc/cryptobot-predictfun.env > /dev/null <<EOF
CRYPTOBOT_VENUE=predictfun
CRYPTOBOT_TWAP_SYMMETRIC_ENABLED=0
DRY_RUN=false
CRYPTOBOT_INITIAL_BANKROLL=$PF_BANKROLL
CRYPTOBOT_DB_PATH=$PROJECT_DIR/data/predictfun_live/cryptobot_predictfun_live.db
CRYPTOBOT_DASHBOARD_PORT=8801
CRYPTOBOT_COINS=BTC,ETH
CRYPTOBOT_V6_CONFIG=$PROJECT_DIR/runtime/config/v2_step1last_live.json
# 1% of equity per bet on BOTH intervals (operator order 2026-08-02,
# superseding the 08-01 2%/2%). ⚠️ At the current ~$195 bankroll this is
# ~$1.95/bet — BELOW the ~$3.5-4 stop-salvage floor (venue $0.90 order min),
# so the stop-loss covers only shallow drift; deep decays ride to zero.
# Operator accepted that trade-off with the resize. The shared
# /etc/cryptobot.env force-pins the POLYMARKET 1%/1% resize; this file is
# loaded second, so these win per-key for the predict.fun unit only.
CRYPTOBOT_V2_BASE_FRAC_5M=0.010
CRYPTOBOT_V2_BASE_FRAC_15M=0.010
# 5m interval ON (both intervals trade). Pinned EXPLICITLY here so the
# predict.fun unit never depends on the Polymarket interval decision in the
# shared env — and because this heredoc rewrites the whole file on every
# deploy, a hand-added override would be silently wiped.
CRYPTOBOT_INTERVAL_5M_ENABLED=1
# NO late-window gate on predict.fun (operator order 2026-08-01: PF trades
# through the final 5 seconds). The shared env force-pins the armed poly value
# on a POLYMARKET box; pinned EMPTY here so a mixed/legacy shared env can never
# leak that poly-only policy onto this venue. Empty == config.py default == OFF.
CRYPTOBOT_LATE_WINDOW_SKIP=
# LIVE-READINESS UNBLOCK — CRITICAL, do not drop either line.
# execution/live_readiness.py evaluates POLYMARKET wallet topology on EVERY
# venue. With LIVE_REQUIRE_DEPOSIT_WALLET=true (the shipped
# cryptobot.env.example default) and no Polymarket private key on the box, the
# derived-topology block fails closed with
# 'no_private_key_for_required_deposit_wallet' => readiness is never READY =>
# pipeline._live_block_reason returns readiness_stale and EVERY live cell is
# skipped. A predict.fun-only box has no poly key BY DESIGN, so the check is
# meaningless here; POLY_SIGNATURE_TYPE=0 (EOA) likewise keeps the topology
# validator off the proxy/safe path it cannot satisfy without a poly funder.
# This is an ENV fix on purpose — live_readiness.py is NOT venue-forked.
LIVE_REQUIRE_DEPOSIT_WALLET=false
POLY_SIGNATURE_TYPE=0
# Dashboard auth OFF on the predict.fun console. main.py refuses to boot when
# CRYPTOBOT_DASHBOARD_PASS is empty and auth is not explicitly disabled, and
# this box has no shared dashboard password. The security boundary is the
# LOOPBACK bind + the operator's SSH tunnel (main.py re-checks that and still
# refuses to boot if the bind is not loopback).
CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1
# PF WS book mirror now EXISTS (predictfun/ws_mirror.py, official
# predictOrderbook/{marketId} channel) — enable it, but SHADOW-FIRST:
# REST stays authoritative while the shadow comparator logs WS-vs-REST
# best-ask/depth divergence (and confirms WS subscribe/heartbeat do NOT
# breach the 240/min REST key). Operator flips SHADOW=0 to go authoritative
# after a divergence run looks clean (same rollout the Polymarket mirror had).
CRYPTOBOT_BOOK_WS_ENABLED=1
CRYPTOBOT_BOOK_WS_SHADOW=1
# Stop-loss fast cadence — decouples the stop pass from the ~2s inline entry
# fill-polls onto its OWN scheduler job at this interval (the pipeline skips
# the inline stop pass whenever this is >0). 1000ms keeps the stop's book
# reads (chain-first; REST only on a cache miss) inside the split decision
# bucket while the WS mirror is still in SHADOW; tighten toward 750 only
# after the operator flips the SHADOW line above to 0 (WS authoritative).
CRYPTOBOT_STOPLOSS_FAST_CADENCE_MS=1000
# Venue-min stop-exit floor: predict.fun rejects POST /v1/orders under a
# 0.90 USD minimum order value (measured 2026-07-23:
# create_order_min_order_value_not_met — live id6's three crash-salvage FOK
# sells, quoted 0.29/0.27/0.10 for notionals 0.42/0.39/0.15 USD, all got
# HTTP 400 invisibly and the position rode to -100%). With the floor armed
# the stop pass never posts a sub-min sell: it records
# outcome='below_venue_min' and fires ONE critical alert per un-stoppable
# position instead of silent-400 spam.
CRYPTOBOT_STOPLOSS_MIN_EXIT_VALUE_USD=0.90
# Restart-gate policy (operator decides at restart — mechanism only, no
# default): with the 0.90 USD venue min, pick ONE of
#   (a) min-stoppable stake ~3.5-4 USD (uncomment + set the line below):
#       every LIVE stake sized so its crash-salvage sell still clears the
#       venue min at crashed quotes;
#   (b) an earlier/wider stop band (CRYPTOBOT_STOPLOSS_BAND) so stops fire
#       before the quote falls under the venue min; or
#   (c) stops OFF (CRYPTOBOT_STOPLOSS_ENABLED=0) with entry edge priced for
#       the -100% tail.
# CRYPTOBOT_MIN_STOPPABLE_STAKE_USD=
CRYPTOBOT_WALLET_START_AT=$PF_START
EOF
    # Real-money unit MUST page on failure: drop-in adds the OnFailure the
    # base unit deliberately omits for paper (removed again on paper revert).
    sudo mkdir -p /etc/systemd/system/cryptobot-predictfun.service.d
    sudo tee /etc/systemd/system/cryptobot-predictfun.service.d/live.conf > /dev/null <<'EOF'
[Unit]
Description=cryptobot predict.fun LIVE scanner (BTC/ETH, REAL MONEY, own DB)
OnFailure=cryptobot-failure-alert.service
EOF
else
    # ---------------- paper-DRY variant (default) ---------------------------
    # SAFETY INVARIANTS (do not weaken):
    #   * CRYPTOBOT_DB_PATH points at the instance's OWN DB — the live bot's
    #     latches/accounting/operator state must never be shared with paper.
    #   * PREDICTFUN_PRIVATE_KEY is FORCED EMPTY => order_client=None => LIVE
    #     placement structurally impossible even if the operator later adds a
    #     signing key to the shared env for diagnostics.
    #   * NOTIFY_TELEGRAM_TOKEN is FORCED EMPTY — paper alerts must never be
    #     mistakable for live-money alerts.
    #   * CRYPTOBOT_BOOK_WS_ENABLED=1 + SHADOW=1 — the PF WS book mirror now
    #     exists (predictfun/ws_mirror.py, official predictOrderbook channel);
    #     shadow-first so REST stays authoritative on paper too while the
    #     comparator logs WS-vs-REST divergence (order_client=None keeps this
    #     read-only regardless).
    #   * NO CRYPTOBOT_V2_EDGE_CAP override (operator order 2026-07-23,
    #     supersedes the 07-22 uncapped-calibration stance): the paper unit
    #     inherits the shared env's cap so BOTH venues obey the same 0.08
    #     ceiling on entries AND top-ups. Calibration consequence (accepted):
    #     the paper funnel no longer samples the >=cap claimed-edge tail.
    #   * The V6 artifact is the deploy-generated DRY-modes copy: the live
    #     artifact seeds wallet_default_modes LIVE, and a fresh DB seeded LIVE
    #     with order_client=None would run silently bet-less (or worse, be
    #     LIVE-armed the day a key+SDK appear).
    #   * CRYPTOBOT_WALLET_START_AT is PRESERVE-ONCE here too (2026-08-01).
    #     It used to be absent from this heredoc, so a revert-to-paper (flip
    #     CRYPTOBOT_PREDICTFUN_LIVE off + redeploy) DESTROYED the venue's
    #     accounting cutover: the file was rewritten without the key, and the
    #     next live arm stamped a brand-new anchor over the era. Same
    #     preserve-once read the live branch does.
    PREV_PF_START="$(sudo grep '^CRYPTOBOT_WALLET_START_AT=' /etc/cryptobot-predictfun.env 2>/dev/null | tail -n 1 | cut -d= -f2- || true)"
    PF_START="${PREV_PF_START:-$WALLET_START_NOW}"
    echo "==> writing /etc/cryptobot-predictfun.env (paper-instance overrides; start=$PF_START)"
    sudo tee /etc/cryptobot-predictfun.env > /dev/null <<EOF
CRYPTOBOT_VENUE=predictfun
CRYPTOBOT_TWAP_SYMMETRIC_ENABLED=0
DRY_RUN=true
CRYPTOBOT_INITIAL_BANKROLL=100
CRYPTOBOT_DB_PATH=$PROJECT_DIR/data/predictfun_paper/cryptobot_predictfun.db
CRYPTOBOT_DASHBOARD_PORT=8801
CRYPTOBOT_COINS=BTC,ETH
CRYPTOBOT_V6_CONFIG=$PROJECT_DIR/runtime/config/v2_step1last_predictfun.json
# 1% of equity per bet on BOTH intervals (operator order 2026-08-02) — the
# SAME pin the live branch uses, so a paper run calibrates the stakes the
# live unit would actually place.
# (This replaced CRYPTOBOT_BET_SIZE_MULTIPLIER=2, which was inert on the
# sub-minute path: pipeline's v2 sizing overwrites portfolio_loader's target
# with sizing_v2's equity fraction, so the multiplier never moved a real stake
# and only made the dashboard's sizing panel misreport it.)
CRYPTOBOT_V2_BASE_FRAC_5M=0.010
CRYPTOBOT_V2_BASE_FRAC_15M=0.010
# NO late-window gate on predict.fun — same operator decision as the live
# branch; pinned here so paper and live sample the SAME entry window.
CRYPTOBOT_LATE_WINDOW_SKIP=
# PF WS book mirror shadow-first (see the SAFETY INVARIANTS note above): the
# mirror runs but REST stays authoritative; on paper order_client=None keeps
# the whole path read-only.
CRYPTOBOT_BOOK_WS_ENABLED=1
CRYPTOBOT_BOOK_WS_SHADOW=1
# Stop-loss fast cadence — decouples the stop pass from the ~2s inline entry
# fill-polls onto its OWN scheduler job at this interval (the pipeline skips
# the inline stop pass whenever this is >0). 1000ms keeps the stop's book
# reads (chain-first; REST only on a cache miss) inside the split decision
# bucket while the WS mirror is still in SHADOW; tighten toward 750 only
# after the operator flips the SHADOW line above to 0 (WS authoritative).
CRYPTOBOT_STOPLOSS_FAST_CADENCE_MS=1000
# Venue-min stop-exit floor: predict.fun rejects POST /v1/orders under a
# 0.90 USD minimum order value (measured 2026-07-23:
# create_order_min_order_value_not_met — the live unit's id6 crash-salvage
# sells all 400'd invisibly and the position rode to -100%). Pinned on paper
# too so the funnel mirrors the live unit's stop mechanics: a sub-min sell
# is never posted — outcome='below_venue_min' + one critical alert per
# un-stoppable position instead of silent-400 spam (inert while
# order_client=None, but the pin must not drift from the live block).
CRYPTOBOT_STOPLOSS_MIN_EXIT_VALUE_USD=0.90
# 5m ENABLED on the paper unit (operator order 2026-07-28 14:47). NOTE: this
# no longer differs from the shared env — the shared /etc/cryptobot.env pinned
# CRYPTOBOT_INTERVAL_5M_ENABLED=0 when 5m was the LIVE bleeder, but 5m was
# RE-ENABLED live on 2026-07-29 and the shared pin is now =1, so both units run
# 5m. The line is kept explicit so the paper funnel's 5m sampling never depends
# on the live interval decision: this EnvironmentFile is loaded second, so if
# the shared pin ever flips back to 0 the paper unit stays 5m-ON. Pinned HERE
# because this heredoc REWRITES the whole file on every deploy — a hand-added
# override is silently wiped (it was, by the 2026-07-29 05:09 deploy, which
# stopped the paper 5m funnel until it was restored). Revert = drop this line.
CRYPTOBOT_INTERVAL_5M_ENABLED=1
# Dashboard auth OFF on the predict.fun console (loopback + SSH tunnel is the
# boundary). Same pin as the live branch — main.py refuses to boot with an
# empty CRYPTOBOT_DASHBOARD_PASS unless this is set, and it independently
# re-checks that the bind really is loopback.
CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1
CRYPTOBOT_WALLET_START_AT=$PF_START
PREDICTFUN_PRIVATE_KEY=
NOTIFY_TELEGRAM_TOKEN=
EOF
    # Revert path: a stale live drop-in must not survive a paper regeneration.
    sudo rm -f /etc/systemd/system/cryptobot-predictfun.service.d/live.conf
fi
sudo chown root:"$SERVICE_USER" /etc/cryptobot-predictfun.env
sudo chmod 0640 /etc/cryptobot-predictfun.env
else
    echo "==> ROLE=polymarket — SKIPPING /etc/cryptobot-predictfun.env (predict.fun moved to its own box 2026-08-01)"
fi

# Pre-create BOTH instance DB dirs. The tightened cryptobot-predictfun.service
# sandbox (2026-07-21 safety audit) ReadWritePaths EXACTLY these dirs instead
# of the whole data/ tree — data/ (and the live Polymarket cryptobot.db inside
# it) is read-only in that unit's mount namespace, so cross-DB writes are
# structurally impossible. Each DB lives in its OWN SUBDIRECTORY: SQLite's WAL
# mode needs directory-level create/unlink freedom (file-level bind mounts of
# db/-wal/-shm broke PRAGMA journal_mode=WAL with "unable to open database
# file" on first boot, 2026-07-22), and a dir bind mount gives it that while
# data/ itself stays read-only. Both are pre-created unconditionally (the unit
# uses '-' prefixed paths, so the unused one is simply bind-mounted empty).
mkdir -p "$PROJECT_DIR/data/predictfun_paper" "$PROJECT_DIR/data/predictfun_live"

# ---- 5. WAL checkpoint + audit-prune systemd timer --------------------------
# The dashboard-snapshot timers (cryptobot-snapshot-refresh.{service,timer},
# cryptobot-predictfun-snapshot.{service,timer}) were RETIRED 2026-08-01 with
# the cross-venue dashboard view: each console now renders ONLY its own
# deployment's SSE payload, so predictfun_paper.json / predictfun_live.json had
# no consumer left. Idempotent teardown lives in the convergence block below.
echo "==> installing WAL-checkpoint + audit-prune systemd timer"

# Common units + the role's own. cryptobot.service is installed in §4 above.
UNITS="cryptobot-wal-checkpoint.service cryptobot-wal-checkpoint.timer cryptobot-failure-alert.service"
if [ "$BOX_ROLE" = "polymarket" ]; then
    # AL2023 has no cron daemon (the timer above is the real fire path); this
    # install is forward-compat for a hypothetical Ubuntu host. It is NOT laid
    # down on a predict.fun box: /etc/cron.daily runs with no environment, so
    # the script could not learn its role and would checkpoint the (absent)
    # Polymarket DB. The systemd unit gets the role via its drop-in below.
    sudo install -m 0755 "$PROJECT_DIR/deploy/cron.daily.sh" /etc/cron.daily/cryptobot  # legacy-path-ok: remote layout
else
    UNITS="$UNITS cryptobot-predictfun.service"
    sudo rm -f /etc/cron.daily/cryptobot
fi
for unit in $UNITS; do
    sudo sed "s|^User=ec2-user|User=$SERVICE_USER|; s|^Group=ec2-user|Group=$SERVICE_USER|; s|/home/ec2-user/cryptobot|$PROJECT_DIR|g" \
        "$PROJECT_DIR/deploy/$unit" \
        | sudo tee "/etc/systemd/system/$unit" > /dev/null
done

# Role + layout for the daily job. cron.daily.sh is `install`ed verbatim (no
# sed pass), and the oneshot unit carries no EnvironmentFile, so the ONLY way
# it learns which DB/unit to act on is this drop-in. On the polymarket role the
# values are exactly the script's own built-in defaults, so the poly box's
# behavior is unchanged.
sudo mkdir -p /etc/systemd/system/cryptobot-wal-checkpoint.service.d
printf '[Service]\nEnvironment=CRYPTOBOT_BOX_ROLE=%s\nEnvironment=PROJECT_DIR=%s\n' \
    "$BOX_ROLE" "$PROJECT_DIR" \
    | sudo tee /etc/systemd/system/cryptobot-wal-checkpoint.service.d/role.conf > /dev/null

# ---- 6. enable + restart the service ----------------------------------------
sudo systemctl daemon-reload

# Idempotent convergence for RETIRED units. Both roles drop the dashboard
# snapshot timers (the cross-venue view is gone, 2026-08-01); the polymarket
# role additionally drops every predict.fun unit, because predict.fun moved to
# its own Tokyo box that day and two boxes sharing ONE predict.fun API key
# would be a 480/min rate-limit breach AND a split brain over the same wallet.
# `|| true` everywhere: a unit that was never installed must not fail the run.
# Both roles also drop BOTH book-recorder units: the book recorder was retired
# 2026-08-02 and its code deleted from the repo 2026-08-04, so the unit files are
# no longer staged and a box that still has one installed would otherwise keep an
# orphaned ExecStart pointing at a module that is gone.
RETIRED_UNITS="cryptobot-snapshot-refresh.timer cryptobot-snapshot-refresh.service \
cryptobot-predictfun-snapshot.timer cryptobot-predictfun-snapshot.service \
book-recorder.service predictfun-book-recorder.service"
if [ "$BOX_ROLE" = "polymarket" ]; then
    RETIRED_UNITS="$RETIRED_UNITS cryptobot-predictfun.service"
fi
for unit in $RETIRED_UNITS; do
    if [ -f "/etc/systemd/system/$unit" ]; then
        echo "==> converging retired unit: $unit (disable --now + remove)"
    fi
    sudo systemctl disable --now "$unit" 2>/dev/null || true
    sudo rm -f "/etc/systemd/system/$unit" || true
done
if [ "$BOX_ROLE" = "polymarket" ]; then
    sudo rm -rf /etc/systemd/system/cryptobot-predictfun.service.d || true
fi
sudo systemctl daemon-reload

if [ "$BOX_ROLE" = "polymarket" ]; then
    sudo systemctl enable cryptobot

    # One-time PERF-03 index retirement (2026-07-14): drop the dead
    # idx_signals_created_at_skip_reason OUT-OF-BAND with the service stopped, so
    # the page-walk of a large signals index is never charged against the 60s
    # boot-OK window (init_db also carries an idempotent DROP as backstop for
    # other installs — instant once this has run). Best-effort: a failure here
    # just defers the drop to the in-boot backstop.
    # POLYMARKET-ONLY: it opens the Polymarket DB, which does not exist on a
    # predict.fun box (and whose scanner unit is not installed there anyway).
    sudo systemctl stop cryptobot 2>/dev/null || true
    sudo -u "$SERVICE_USER" env CRYPTOBOT_DB="$PROJECT_DIR/data/cryptobot.db" \
        "$PROJECT_DIR/.venv/bin/python" - <<'PY' || echo "WARN: out-of-band index drop failed; boot-path backstop will retry"
import os, sqlite3
db = os.environ["CRYPTOBOT_DB"]
if os.path.exists(db):
    conn = sqlite3.connect(db, timeout=120)
    conn.execute("DROP INDEX IF EXISTS idx_signals_created_at_skip_reason")
    conn.commit()
    conn.close()
PY
fi

BOOT_WATCH_SINCE="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
# Which unit + log pair the boot-OK watcher at the end of this script tails.
if [ "$BOX_ROLE" = "polymarket" ]; then
    BOOT_UNIT="cryptobot"
    BOOT_LOG="$LOG_DIR/scanner.log"
    BOOT_ERR="$LOG_DIR/scanner.err"
else
    BOOT_UNIT="cryptobot-predictfun.service"
    BOOT_LOG="$LOG_DIR/predictfun-scanner.log"
    BOOT_ERR="$LOG_DIR/predictfun-scanner.err"
fi
SCANNER_ERR_OFFSET=$(sudo stat -c %s "$BOOT_ERR" 2>/dev/null || echo 0)
SCANNER_LOG_OFFSET=$(sudo stat -c %s "$BOOT_LOG" 2>/dev/null || echo 0)
if [ "$BOX_ROLE" = "polymarket" ]; then
    # Final installed-control assertion immediately before restart. A stale or
    # hand-edited env must fail closed rather than silently changing the exact
    # forward-tested execution contract.
    for expected in \
        'CRYPTOBOT_COINS=BTC,ETH,SOL,XRP' \
        'CRYPTOBOT_V2_BASE_FRAC_15M=0.010' \
        'CRYPTOBOT_V2_BASE_FRAC_5M=0.010' \
        'CRYPTOBOT_MAX_DD_HALT_FRAC=0.40' \
        'CRYPTOBOT_SCALEIN_T3_ENABLED=0' \
        'CRYPTOBOT_V2_TIER_A_MULT=1.0' \
        'CRYPTOBOT_V2_FAK_PAD=0.01' \
        'CRYPTOBOT_V2_FIRST_ATTEMPT_ONLY=0' \
        'CRYPTOBOT_FAST_REQUOTE_ENABLED=0' \
        'CRYPTOBOT_RETRY_DIP_GATE_ENABLED=0' \
        'CRYPTOBOT_V2_EDGE_CAP=0' \
        'CRYPTOBOT_SUBMINUTE_FLIP_SIDE=0' \
        'CRYPTOBOT_TWAP_SYMMETRIC_ENABLED=1'; do
        sudo grep -Fqx "$expected" /etc/cryptobot.env || {
            echo "ERROR: installed TWAP control mismatch: $expected" >&2
            exit 1
        }
    done
    sudo -u "$SERVICE_USER" env \
        PYTHONPATH="$PROJECT_DIR/src" \
        CRYPTOBOT_COINS=BTC,ETH,SOL,XRP \
        "$PROJECT_DIR/.venv/bin/python" \
        "$PROJECT_DIR/deploy/check_twap_rtds.py"
    sudo systemctl restart cryptobot
fi
sudo systemctl enable --now cryptobot-wal-checkpoint.timer

# Flag-gated SEPARATE units. Each is always installed + daemon-reloaded above,
# but only enabled/started when the operator has explicitly pinned its flag =1 in
# /etc/cryptobot.env. When unpinned/0 we leave it stopped AND stop any running
# instance so a flag flip-off actually takes effect on redeploy.
# restart, not `enable --now`: start is a no-op on an already-running unit, so a
# flag-gated unit would keep running stale code through later deploys (caught
# 2026-07-15 on the since-retired book recorder). enable + restart picks up new
# code.
arm_flag_gated_unit() {
    flag="$1"; unit="$2"
    if sudo grep -Eq "^${flag}=(1|true|yes|on)$" /etc/cryptobot.env; then
        echo "==> $unit ARMED ($flag pinned) — enabling"
        sudo systemctl enable "$unit"
        sudo systemctl restart "$unit"
    else
        echo "==> $unit disabled (flag unpinned) — installed but not started"
        sudo systemctl disable --now "$unit" 2>/dev/null || true
    fi
}
if [ "$BOX_ROLE" != "polymarket" ]; then
    # book-recorder.service was armed on the polymarket role and
    # predictfun-book-recorder.service here until 2026-08-02 — retired by
    # operator order, code deleted 2026-08-04, torn down in the retired-unit
    # convergence block above. rtds-recorder.service went the same way
    # 2026-08-01 (prereg abandoned ungraded).
    # predict.fun scanner (dashboard :8801). ⚠️ NAMING TRAP: the flag still
    # reads *_DRY_ENABLED but it is the UNIT gate in BOTH modes — the unit does
    # not start in LIVE mode either unless CRYPTOBOT_PREDICTFUN_DRY_ENABLED=1.
    # ⚠️ Gating is UNIT-LEVEL ONLY (main.py has no self-exit on this flag — a
    # manual `systemctl start` runs it). ⚠️ MEMORY: the instance loads the full
    # artifact stack; the unit asks for 3300M, so this box must be >= 8 GB.
    arm_flag_gated_unit CRYPTOBOT_PREDICTFUN_DRY_ENABLED cryptobot-predictfun.service
fi
# The predict.fun FULL-snapshot timer (predictfun_live.json for the :8800
# cross-venue view) was armed here until 2026-08-01. Retired with that view —
# each console renders only its own deployment now.

file_has_boot_ok_after_offset() {
    path="$1"
    offset="$2"
    if [ ! -f "$path" ]; then
        return 1
    fi
    sudo tail -c "+$((offset + 1))" "$path" 2>/dev/null | grep -q "boot OK"
}

BOOT_OK_TIMEOUT_SECONDS="${BOOT_OK_TIMEOUT_SECONDS:-120}"
echo "==> waiting up to ${BOOT_OK_TIMEOUT_SECONDS}s for boot OK ($BOOT_UNIT)"
for i in $(seq 1 "$BOOT_OK_TIMEOUT_SECONDS"); do
    if systemctl is-active --quiet "$BOOT_UNIT" && {
        journalctl -u "$BOOT_UNIT" --since "$BOOT_WATCH_SINCE" -n 200 --no-pager 2>/dev/null | grep -q "boot OK" ||
            file_has_boot_ok_after_offset "$BOOT_ERR" "$SCANNER_ERR_OFFSET" ||
            file_has_boot_ok_after_offset "$BOOT_LOG" "$SCANNER_LOG_OFFSET"
    }; then
        echo "==> SUCCESS: $BOOT_UNIT is running (took ${i}s)"
        sudo systemctl status "$BOOT_UNIT" --no-pager | head -12
        exit 0
    fi
    sleep 1
done

# A predict.fun box whose scanner flag is deliberately unpinned has nothing to
# wait for — the unit is installed-but-stopped BY DESIGN, not a failed boot.
if [ "$BOX_ROLE" = "predictfun" ] \
        && ! sudo grep -Eq '^CRYPTOBOT_PREDICTFUN_DRY_ENABLED=(1|true|yes|on)$' /etc/cryptobot.env; then
    echo "==> $BOOT_UNIT is not armed (CRYPTOBOT_PREDICTFUN_DRY_ENABLED unpinned) — install complete, scanner intentionally stopped"
    exit 0
fi

echo "WARN: did not see 'boot OK' within ${BOOT_OK_TIMEOUT_SECONDS}s. Last 30 log lines:"
journalctl -u "$BOOT_UNIT" --since "$BOOT_WATCH_SINCE" -n 30 --no-pager
echo "WARN: last 30 $BOOT_ERR lines:"
sudo tail -n 30 "$BOOT_ERR" 2>/dev/null || true
echo "WARN: last 30 $BOOT_LOG lines:"
sudo tail -n 30 "$BOOT_LOG" 2>/dev/null || true
exit 1
