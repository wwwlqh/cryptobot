"""Runtime configuration knobs for the live scanner.

All values here are tuning surfaces — change them, restart the process.
Live retraining is not supported (R6/R7 from origin brainstorm).

Defaults are chosen to match the deployed artifact
`polymarket/live/config/v2_step1last_live.json` (2026-07-09 v2 step1last
deploy, which retired the earlier exp044 residual-edge HGB config; the file
moved out of the research tree on 2026-07-28 — it is a LIVE contract).
`SHARED_EXPOSURE_CAP` and `MAX_DRAWDOWN_HALT_FRAC` must equal the
deployed artifact's `concurrent_exposure_cap_fraction` and
`max_drawdown_halt_frac` (1.00 exposure / 0.30 DD halt) or the loader's
drift-check fails closed at boot.

Env-override caution: `os.environ` overrides win over these defaults,
so any `CRYPTOBOT_V6_CONFIG` / `CRYPTOBOT_SHARED_EXPOSURE_CAP` /
`CRYPTOBOT_MAX_DD_HALT_FRAC` set on the live box must be reconciled to
v2_step1last_live.json / 1.00 / 0.30 before redeploy (see
`polymarket/infra/cryptobot.env.example`). To revert all three together the
operator must point V6_CONFIG at a matching artifact and set the cap +
halt to that artifact's fields — the drift-check (portfolio_loader
`load_sleeves`) requires the trio to agree.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def _resolve_repo_root() -> Path:
    """Project root, resilient to BOTH tree layouts.

    Local dev puts the package at ``<root>/polymarket/src/cryptobot``; the DEPLOYED
    box puts it at ``$PROJECT_DIR/src/cryptobot`` — legacy-path-ok: deploy.sh
    maps the local layout onto the unchanged remote one, so the server
    never learns about ``polymarket/`` until the Phase 3 cutover.

    A fixed ``parents[N]`` cannot satisfy both, and getting it wrong
    silently relocates the live SQLite ledger — so:

      1. ``CRYPTOBOT_ROOT`` if set (install_remote.sh pins it on the box),
      2. else walk up for a marker directory (``pyproject.toml`` or
         ``polymarket/src/cryptobot``) — matches any local checkout,
      3. else ``parents[2]`` — the deployed layout, where neither marker ships.
    """
    raw = os.environ.get("CRYPTOBOT_ROOT")
    if raw:
        return Path(raw).resolve()
    here = Path(__file__).resolve()
    for candidate in here.parents:
        if (candidate / "pyproject.toml").is_file():
            return candidate
        if (candidate / "polymarket" / "src" / "cryptobot").is_dir():
            return candidate
    return here.parents[2]  # legacy-path-ok: THE sanctioned root anchor (deployed src/cryptobot layout)


# Repo root resolved from this file. Tests can override via env vars.
_REPO_ROOT = _resolve_repo_root()

# All runtime-writable state hangs off DATA_ROOT (DBs, recorder output, models)
# and LOGS_DIR. Both are env-overridable so a box can put them on another
# volume without patching code — and so nothing is ever CWD-relative again.
# (Defined with a raw os.environ read rather than `_path`, which is declared
# further down; keep them here so every later default can build on them.)
DATA_ROOT: Path = Path(os.environ.get("CRYPTOBOT_DATA_ROOT") or (_REPO_ROOT / "data"))
LOGS_DIR: Path = Path(os.environ.get("CRYPTOBOT_LOGS_DIR") or (_REPO_ROOT / "logs"))

# Records how each shipped artifact resolved, for boot logging / the platform
# console: env_key -> (path, "env" | "runtime" | "legacy" | "new" | "... (missing)").
ARTIFACT_RESOLUTION: dict[str, tuple[Path, str]] = {}


def _resolve_artifact(env_key: str, *candidates: tuple[str, Path]) -> Path:
    """First existing candidate wins; env override always wins outright.

    Candidates are ordered ``runtime`` (the deployed copy) -> ``legacy`` (the
    pre-restructure research-tree path, still real in every existing checkout)
    -> ``new`` (the post-restructure home, and the preferred default for a
    fresh clone where none of them exist yet).
    """
    raw = os.environ.get(env_key)
    if raw:
        chosen = Path(raw)
        ARTIFACT_RESOLUTION[env_key] = (chosen, "env")
        return chosen
    for why, candidate in candidates:
        if candidate.exists():
            ARTIFACT_RESOLUTION[env_key] = (candidate, why)
            return candidate
    why, candidate = candidates[-1]
    ARTIFACT_RESOLUTION[env_key] = (candidate, f"{why} (missing)")
    return candidate


def _path(env_key: str, default: Path) -> Path:
    raw = os.environ.get(env_key)
    return Path(raw) if raw else default


def _float_env(env_key: str, default: float, *, legacy_env_key: str | None = None) -> float:
    raw = os.environ.get(env_key)
    if raw is None and legacy_env_key is not None:
        raw = os.environ.get(legacy_env_key)
    return float(raw) if raw not in (None, "") else float(default)


# Every ledger row records the venue it traded on.
VENUE: str = "polymarket"


# -- Scheduler cadence --------------------------------------------------------

# 1Hz default per the operator decision (2026-05-17). Bump to 2 before smoke
# if measured tick p95 > 900ms per U16's pre-smoke latency probe.
TICK_SECONDS: int = int(os.environ.get("CRYPTOBOT_TICK_SECONDS", "1"))
TICK_SOFT_DEADLINE_MS: int = 900
TICK_HARD_DEADLINE_MS: int = 5000

# Latency item #4: periodic PASSIVE WAL checkpoint cadence (seconds). Pairs
# with db/connection.py's wal_autocheckpoint=20000 — the raised bound keeps
# SQLite's inline checkpoint off the tick's commit path, and this scheduler
# job (main.py → db.connection.passive_wal_checkpoint) drains the WAL on a
# background thread instead. PASSIVE never blocks readers/writers, and
# journal_size_limit=200MB bounds WAL disk regardless. 0 disables the job
# (not recommended on the live box: the WAL then only drains at the 20000-
# page inline threshold or on clean close).
WAL_CHECKPOINT_SECONDS: int = int(
    os.environ.get("CRYPTOBOT_WAL_CHECKPOINT_SECONDS", "60")
)

# Latency finding #8 (connection-warmth): cadence (seconds) of the clob_ping
# scheduler job — a lightweight unauthenticated GET /ok on the CLOB SIGNING
# pool (the module-level httpx client the order POST reuses). Between orders
# that pool's only steady traffic is the 60s readiness balance read vs the
# pool's keepalive_expiry=65s — a 5s margin — and a readiness fire delayed
# past the scheduler's misfire_grace_time=2 is DROPPED outright (~120s gap),
# guaranteeing the socket expires so the next order POST pays fresh
# DNS + TCP + full TLS inside post_ms (httpcore never resumes TLS sessions).
# A ~20s ping keeps the single hot connection permanently warm and drains
# buffered GOAWAY/FIN frames off the decision path. Wired in main.py only
# when an OrderClient exists; the job skips itself when ANY pool response
# landed within SKIP_IF_ACTIVE (no redundant GET right after order/readiness
# traffic) and overrides the pool's read=30s with a short per-request timeout
# so a hung /ok can't pin a scheduler-pool worker. 0 disables the job AND the
# pool response hook feeding the skip guard — byte-identical runtime.
CLOB_PING_SECONDS: int = int(os.environ.get("CRYPTOBOT_CLOB_PING_SECONDS", "20"))
# Skip window + per-request timeout for the ping (seconds). Not env-tunable —
# review F4: the 40s window means the ping only fires after ≥40s of pool
# silence, OUTSIDE any active-trading band (a 15s window let the ping occupy
# the single warm keepalive connection exactly when an order POST fired —
# the cold-second-connection cost the feature exists to remove), while the
# 20s cadence still lands a ping safely inside keepalive_expiry=65s. The
# 3s timeout mirrors the pool's connect=3.0 fail-fast bound.
CLOB_PING_SKIP_IF_ACTIVE_S: float = 40.0
CLOB_PING_TIMEOUT_S: float = 3.0

# Latency item #5: read/write/pool timeout (seconds) for the shared upstream
# read clients (spot / gamma / clob) built in main.py. Default 3.0 is
# the long-standing hardcoded httpx.Timeout value, so an unpinned deploy is
# behavior-identical; the knob exists so the operator can pin a tighter
# fail-fast timeout on decision-path reads WITHOUT a code change (e.g. 1.5s
# so a hung venue read can never eat most of the 900ms soft deadline plus
# retry budget). Documented here rather than polymarket/infra/cryptobot.env.example
# (that file is owned by a parallel workstream). connect stays pinned at 2.0s
# in main.py — this knob covers read/write/pool only.
DECISION_READ_TIMEOUT_S: float = _float_env(
    "CRYPTOBOT_DECISION_READ_TIMEOUT_S", 3.0
)

# Phase B parallel order POST (latency lever). When ON and >=2 place-candidate
# LIVE cells reach the order-POST stage in one tick, their
# `place_gtc_limit_order` venue round trips fire CONCURRENTLY on a transient
# thread pool instead of serially. The CLOB taker delay is uncancellable
# (~250ms), so N serial POSTs cost ~N*250ms of decision->fill latency while a
# fan-out costs ~250ms — the saving is ~250ms per EXTRA cell in the tick.
#
# Pre-POST (gates -> walk_book -> sizing -> reserve_live_order_attempt) stays
# STRICTLY serial in cell order, and post-POST result processing (mark_submitted
# / record_bet / note_fill / requote telemetry) is serial in the ORIGINAL cell
# order; only the network POST is parallel. DEFAULT OFF (=0) => the Phase B loop
# takes the exact existing serial code path — the guard is at the loop level in
# execution/pipeline.py's run_scanner_tick, and _commit_cell's serial branch is
# byte-identical to today. The flag introduces ONE documented behavioral
# divergence: reserved cells apply their stake to the within-tick exposure
# deltas PESSIMISTICALLY at reserve time (a later-rejected POST still consumed
# within-tick headroom — conservative, never over-exposes). See
# execution/pipeline.py `_commit_cell` / `_flush_parallel_posts`.
PHASE_B_PARALLEL_POST: bool = os.environ.get(
    "CRYPTOBOT_PHASE_B_PARALLEL_POST", "0"
).strip().lower() in ("1", "true", "yes")

# -- Walk-book + sizing -------------------------------------------------------

# Absolute cents added to top_ask. NOT a percentage. See R-NEW-6.
# Overrides v6's optimized 0.288 (5m) / 0.206 (15m) values; operator preference.
MAX_WALK_PRICE: float = 0.05

# Research artifact base. The deployed artifact (`v2_step1last_live.json`)
# must keep this $100 provenance anchor so backtest-derived stake fractions
# and config fingerprints remain auditable.
BACKTEST_BANKROLL: float = 100.0

# Live accounting seed. Sizing scales from realized capital
# (INITIAL_BANKROLL + post-cutover PnL), so live deploys can anchor the scanner
# to the actual funded pUSD balance without rewriting the research artifact.
# `INITIAL_BANKROLL` is accepted as a legacy alias because old env examples
# advertised that key, but the CRYPTOBOT_* name wins.
INITIAL_BANKROLL: float = _float_env(
    "CRYPTOBOT_INITIAL_BANKROLL",
    BACKTEST_BANKROLL,
    legacy_env_key="INITIAL_BANKROLL",
)
if INITIAL_BANKROLL <= 0:
    raise ValueError("CRYPTOBOT_INITIAL_BANKROLL must be > 0")

# Dry-run wallet accounting can be cut over from a deployment timestamp without
# deleting historical ledger rows. Empty means include the full ledger.
WALLET_START_AT: str = os.environ.get("CRYPTOBOT_WALLET_START_AT", "").strip()

# 2026-05-23: keep multiplier at 1.0; the latest dry-run artifact folds the
# free-size portfolio backtest scales directly into per-sleeve capital_frac.
BET_SIZE_MULTIPLIER: float = float(os.environ.get("CRYPTOBOT_BET_SIZE_MULTIPLIER", "1.0"))

# 2026-05-23 free-size portfolio backtest still leaves the smallest deployed
# base target (v6_15m ~= $1.89 on $100 base) above the $1 floor.
MIN_BET_USD: float = float(os.environ.get("CRYPTOBOT_MIN_BET_USD", "1.0"))
CLOSE_SIZE_REL_TOLERANCE: float = 1e-6

# Polymarket's published crypto-category peak taker fee (theta-shape:
# fee_per_share = theta * p * (1 - p)). 2026-08-05 operator correction: the
# published value is 0.07, not 0.072 — the latter was entered in error on
# 2026-05-16 and carried since. Pinned to match
# `backtest/engine/sweep_lib.py` (POLY_FEE_THETA) and
# `strategy/parity.py`; the U1 test (tests/test_audit_parity.py) asserts
# equality across all three to fail boot on silent drift (D6).
POLY_FEE_THETA: float = 0.07

# Default fee-rate-bps used when Polymarket Gamma returns feeRateBps=None on
# the *-updown-* markets. Verified 2026-05-17: every BTC/ETH/SOL/BNB/XRP/DOGE
# updown market exposes feeRateBps as null on Gamma. The actual P&L math uses
# POLY_FEE_THETA via the theta-shape formula (sweep_lib.unit_pnl) regardless
# of this value — feeRateBps is decorative on these markets and only needs to
# be a positive sentinel to satisfy the R9 fail-closed presence check.
# 70 bps = θ × 10000, surfaced as a forensic value the operator can trace
# back to POLY_FEE_THETA = 0.07.
POLY_DEFAULT_FEE_RATE_BPS: float = 70.0

# Grade-time calibration multiplier on the Polymarket ENTRY-leg taker fee.
# Consumed ONLY where realized pnl_net is booked (execution.grade_fees);
# decision math (edge gates, walkers, strategy.parity / spec_v2) keeps the
# raw published theta, so bet selection is unchanged.
#
# Derivation (flat-to-flat wallet-tape audit, 2026-07-23): over
# 2026-07-21 22:11 -> 2026-07-23 ~11:43 UTC — both endpoints flat (zero
# positions, zero unsettled, transfers empty) — realized venue friction was
# $1.73 vs $2.18 modeled by theta*p*(1-p) per entry fill at the then-assumed
# theta = 0.072, i.e. the venue extracted ~0.84x of that parabola. Booking the
# raw parabola on every fill (losers included) would overcorrect ~16%.
#
# 2026-08-05 RE-BASING (no new measurement): the audit measured a PHYSICAL
# quantity — the effective extracted coefficient k * theta:
#     k_old * theta_old = 0.84 * 0.072 = 0.06048
# The published theta was then corrected 0.072 -> 0.07. The venue's behaviour
# did not change, so the product must be held fixed and only the basis moves:
#     k_new = k_old * theta_old / theta_new = 0.84 * 0.072 / 0.07 = 0.864
#     check: 0.864 * 0.07 = 0.06048  (identical booked fee)
# Booked fees are therefore byte-unchanged by the theta correction; only the
# DECISION-side edge gate (which uses the raw published theta) got cheaper.
# When re-measured with the same flat-to-flat method, retune with this one env
# line. Does NOT apply to predict.fun (2% tent share-fee is venue-measured).
POLY_FEE_CALIBRATION: float = _float_env("CRYPTOBOT_POLY_FEE_CALIBRATION", 0.864)

# Default concurrent-exposure cap (R-NEW-5; D20). The live artifact can
# override this per strategy wallet; this value is the top-level fallback.
# 2026-05-25 (all_4 deploy plan): bumped to the chunk-CV winner's
# exposure_cap_frac (0.5919). Under per-strategy isolated wallets every
# wallet's wallet_exposure_cap_fracs entry is also set to 0.5919, so the
# top-level fallback and per-wallet values are deliberately identical.
# 2026-05-29 (exp044 deploy): set to exp044's exposure_cap_frac (0.30). This
# replaces the prior all_4 chunk-CV value (0.5919). The loader drift-check
# (portfolio_loader) requires the deployed artifact's concurrent_exposure_cap_fraction
# to equal this constant — exp044_live.json is built with 0.30 to match.
# 2026-06-07 (operator decision): raised 0.30 -> 0.80 to capture the ORB+HGB
# clustered windows. The 6/2-6/6 forward window saturates at ~0.75 concurrent
# exposure (24 stacked entries, 0 skips) for +$71.5 vs +$26.5 at 0.30; 0.80
# leaves a small margin above saturation. Realized max-DD stayed ~26% (under
# the 0.35 halt) at every exposure level in that window, BUT that is settlement
# drawdown on a window where the big cluster WON — open-position paper risk is
# larger and not charged here. DRY shadow only. The trio (this constant,
# exp044_live.json concurrent_exposure_cap_fraction, and the box's
# CRYPTOBOT_SHARED_EXPOSURE_CAP env) must all read 0.80 or the loader fails
# closed at boot.
# 2026-06-09 (operator decision): raised 0.80 -> 1.0 as part of FULL PARITY with
# the +$13,721 WFO backtest (stake x4, book x2, per-coin weights, no max-bet cap).
# This deploys the selection-optimistic in-sample upper bound to the DRY shadow;
# the 0.35 DD halt below is now the only remaining guard. The trio (this constant,
# exp044_live.json concurrent_exposure_cap_fraction, and the box's
# CRYPTOBOT_SHARED_EXPOSURE_CAP env) must all read 1.0 or the loader fails closed.
SHARED_EXPOSURE_CAP: float = float(os.environ.get(
    "CRYPTOBOT_SHARED_EXPOSURE_CAP",
    "1.0",
))

# Hard live trading halt. This is peak-equity drawdown, not the legacy
# initial-bankroll denominator used by drawdown_ratio. 2026-05-26:
# restored to the backtest-recommended 0.50 (v2_recommended.md all_4
# winner). The 2026-05-25 operator override to 0.40 was rolled back
# after the chunk-CV reopt (ae428c0) reconfirmed the original 0.50
# threshold as the optimizer's choice.
# 2026-06-06 ORB/HGB top-up research setting: 0.40 hard stop. This tolerates
# the measured ~0.37 exp044-style peak drawdown while keeping less headroom
# than the prior 0.50 deployment guard.
# 2026-07-16 (operator decision): tightened 0.40 -> 0.35 hard stop.
# 2026-07-21 (operator override, risk retune): back to 0.40 hard stop, paired
# with the brake retune (V2_BRAKE_SCALE 0.60 — half size at >=30% DD).
# 2026-07-31 (operator order): the graduated throttle is REMOVED (brake floor
# 1.0 below) and both intervals resize to 1% of equity, so the halt is now the
# ONLY drawdown guard — tightened 0.40 -> 0.30. The deployed both-hold twin's
# worst-fold backtest maxDD is 26.6% (fold C), i.e. 0.30 sits above every
# measured drawdown at the new size and never fires in that replay. See
# polymarket/backtest/studies/tick_latency/THROTTLE_REMOVAL_SIZE_20260731.md.
# The trio (this constant, v2_step1last_live.json max_drawdown_halt_frac, and
# the box's CRYPTOBOT_MAX_DD_HALT_FRAC env) must all read 0.30 or the loader
# fails closed.
MAX_DRAWDOWN_HALT_FRAC: float = float(os.environ.get(
    "CRYPTOBOT_MAX_DD_HALT_FRAC",
    "0.30",
))

# UI threshold for amber warning on the exposure tile (R-NEW-18).
EXPOSURE_WARN_RATIO: float = 0.80

# -- Add-on / top-up control --------------------------------------------------

# Operator can disable add-ons mid-soak if the profitability gate trips
# (Success Criteria: add-on edge ≥ initial-fill edge within 200 bps).
ENABLE_TOPUPS: bool = os.environ.get("CRYPTOBOT_ENABLE_TOPUPS", "1") == "1"

# Stale PENDING age beyond which slot_state ignores the row (R3 reconcile path
# will re-settle it). Matches hightempbot.
MAX_PENDING_AGE_MINUTES: int = 300

# -- Resolution sweep cadence -------------------------------------------------

# F3 sweep runs every N minutes via APScheduler. Plan U7a / docstring step 7.
RESOLUTION_SWEEP_MINUTES: int = int(os.environ.get("CRYPTOBOT_SWEEP_MINUTES", "5"))

# Audit P1 #12: a LIVE PENDING row whose Polymarket market is permanently
# unresolvable (404 / http_error / bad_payload) holds its bet_size in the
# group exposure cap indefinitely. Past this age (seconds since window_end)
# the sweep escalates the stuck row to CRITICAL so an operator resolves or
# cancels it manually. Default 6h: long enough that a transient Gamma outage
# self-heals first, short enough that stranded capital is surfaced same-day.
MAX_PENDING_SETTLE_AGE_S: int = int(
    os.environ.get("CRYPTOBOT_MAX_PENDING_SETTLE_AGE_S", str(6 * 3600))
)

# -- Resolution sanity bounds (R-NEW-19 / U7a; security) ---------------------

# Per-coin (lo, hi) sanity bounds on resolution price; out-of-bounds → CANCELLED.
RESOLUTION_PRICE_BOUNDS: dict[str, tuple[float, float]] = {
    "BTC": (1_000.0, 10_000_000.0),
    "ETH": (10.0, 1_000_000.0),
    "SOL": (1.0, 10_000.0),
    "BNB": (10.0, 10_000.0),
    "XRP": (0.01, 100.0),
    "DOGE": (0.001, 10.0),
}
MAX_RESOLUTION_PRICE_MOVE_PCT: float = 0.50

# -- File paths ---------------------------------------------------------------

DB_PATH: Path = _path("CRYPTOBOT_DB_PATH", DATA_ROOT / "cryptobot.db")
# The CRYPTOBOT_V6_CONFIG env name is legacy and deliberately unchanged: it is
# pinned in /etc/cryptobot.env on the live box and names the v6-format config
# schema (v2_step1last_live.json). The loader module was renamed to
# portfolio_loader, but this env var name must stay stable.
#
# Resolution chain (see _resolve_artifact): the deployed runtime/ copy first —
# that is what the box actually reads and install_remote pins it explicitly —
# then the pre-restructure research-tree path (still real in older checkouts),
# then the post-2026-07-28 home. v2_step1last_live.json is a LIVE contract, so
# its new home is the venue's live config dir, not the research tree.
V6_CONFIG_PATH: Path = _resolve_artifact(
    "CRYPTOBOT_V6_CONFIG",
    ("runtime", _REPO_ROOT / "runtime" / "config" / "v2_step1last_live.json"),
    ("legacy", _REPO_ROOT / "backtest" / "optimize_runs" / "v2_step1last_live.json"),  # legacy-path-ok: pre-restructure checkouts
    ("new", _REPO_ROOT / "polymarket" / "live" / "config" / "v2_step1last_live.json"),
)
# RETIRED 2026-07-31 (exp044). Phases 1-2 took them off the live boot path;
# phase 3 removed the last research readers, so all three are now artifacts with
# ZERO consumers anywhere in the tree:
#   * the baseline minute-entry HGB — data/models/hgb_v1.joblib +
#     hgb_v1_manifest.json (CRYPTOBOT_HGB_MODEL / CRYPTOBOT_HGB_MANIFEST). Its
#     last reader, the checkpoint-grid WFO, now carries the exp044 sleeve recipe
#     inline (training/checkpoint.py EXP044_SLEEVES).
#   * the baseline RF — data/models/rf_v1.joblib + rf_v1_manifest.json
#     (CRYPTOBOT_RF_MODEL / CRYPTOBOT_RF_MANIFEST). Its only live consumer was
#     the lut|rf|blend scorer, which went with the pair sleeves; the joblib and
#     its research loader were deleted in phase 3.
#   * the LUT — decision_table.parquet + its .lutcache.pkl sidecar
#     (CRYPTOBOT_DECISION_TABLE). Only the pair sleeves and that same scorer
#     ever looked a bucket up; the parquet was deleted in phase 3 with its whole
#     producer chain.
# Every live hgb sleeve is a checkpoint sleeve served by checkpoint_v1 below.
# checkpoint_v1: frozen per-checkpoint-second sub-minute models (RF forests keyed
# by (coin, interval, checkpoint_sec) + a per-interval HGB), built by
# polymarket/backtest/training/export_models.py to give the sub-minute sleeves live/backtest
# DECISION parity (2026-07-05 parity audit). Loaded only when SUBMINUTE_ENABLED.
_CHECKPOINT_DIR = DATA_ROOT / "models" / "checkpoint_v1"
CHECKPOINT_RF_MODEL_PATH: Path = _path(
    "CRYPTOBOT_CHECKPOINT_RF_MODEL", _CHECKPOINT_DIR / "checkpoint_rf_v1.joblib"
)
CHECKPOINT_HGB_MODEL_PATH: Path = _path(
    "CRYPTOBOT_CHECKPOINT_HGB_MODEL", _CHECKPOINT_DIR / "checkpoint_hgb_v1.joblib"
)
CHECKPOINT_MANIFEST_PATH: Path = _path(
    "CRYPTOBOT_CHECKPOINT_MANIFEST", _CHECKPOINT_DIR / "checkpoint_v1_manifest.json"
)

# Chainlink-TWAP symmetric model. When enabled, the sub-minute sleeves keep
# Binance for the decision-time spot and realized-volatility denominator, but
# re-anchor the return numerator to Chainlink's exact 60s TWAP sample at
# the market boundary. The scorer fails closed if that RTDS anchor is absent.
TWAP_SYMMETRIC_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_TWAP_SYMMETRIC_ENABLED", "0"
).strip().lower() in {"1", "true", "yes", "on"}
_TWAP_SYMMETRIC_DIR = DATA_ROOT / "models" / "twap_symmetric_v1"
TWAP_SYMMETRIC_MODEL_PATH: Path = _path(
    "CRYPTOBOT_TWAP_SYMMETRIC_MODEL",
    _TWAP_SYMMETRIC_DIR / "twap_hgb_v1.joblib",
)
TWAP_SYMMETRIC_MANIFEST_PATH: Path = _path(
    "CRYPTOBOT_TWAP_SYMMETRIC_MANIFEST",
    _TWAP_SYMMETRIC_DIR / "twap_symmetric_v1_manifest.json",
)
TWAP_RTDS_URL: str = os.environ.get(
    "CRYPTOBOT_TWAP_RTDS_URL", "wss://ws-live-data.polymarket.com"
).strip()
# The dashboard tree moved under the venue on 2026-08-04 (`polymarket/dashboard`).
# The DEPLOYED box layout is UNCHANGED — `$PROJECT_DIR/dashboard`, which
# install_remote.sh pins explicitly via CRYPTOBOT_DASHBOARD_DIR — so this
# default only ever serves LOCAL dev. Prefer the venue home, then fall back to
# the flat deployed shape so a box that somehow lost the env pin still resolves.
_DASHBOARD_DEFAULT = _REPO_ROOT / "polymarket" / "dashboard"
if not _DASHBOARD_DEFAULT.is_dir():
    _DASHBOARD_DEFAULT = _REPO_ROOT / "dashboard"
DASHBOARD_DIR: Path = _path("CRYPTOBOT_DASHBOARD_DIR", _DASHBOARD_DEFAULT)

# -- Dashboard / SSE server ---------------------------------------------------

DASHBOARD_HTTP_PORT: int = int(os.environ.get("CRYPTOBOT_DASHBOARD_PORT", "8800"))

# Security: never default-bind to 0.0.0.0. Operator override required for
# remote access; live-mode boot guard (U14) refuses non-loopback bind when
# real-money mode is on.
DASHBOARD_BIND_HOST: str = os.environ.get("CRYPTOBOT_DASHBOARD_HOST", "127.0.0.1")

MAX_SSE_CLIENTS: int = 10

# Per-client write deadline — beyond this the broadcast skips the slow client
# rather than blocking the broadcast thread on TCP backpressure (#50).
SSE_CLIENT_WRITE_TIMEOUT_S: float = 2.0

# Audit w19oxqf24 (P3): per-connection request-line/header read deadline on the
# HTTP handler. Bounds a pre-auth slowloris client that dribbles header bytes to
# hold a worker thread. 15s is generous for a legitimate request yet closes a
# stalled connection promptly.
SSE_HEADER_READ_TIMEOUT_S: float = 15.0

# Bootstrap and delta limits for the ledger block in the SSE payload (#46).
# Bootstrap raised 50 -> 500 (2026-07-12 ledger-window alignment): the initial
# snapshot now hydrates the FULL v2-only era (~500 rows today) so the Journal's
# "All" filter and every client-side ledger analytic cover the whole deployed
# history on a fresh load, not a noisy ~50-row session slice. Payload cost: the
# ledger block is the initial snapshot's bulk — a settled row serializes to
# ~0.8 KB of compact JSON, so 500 rows is ~0.4 MB of one-time bootstrap (up from
# ~40 KB) over the single-operator SSH tunnel; forward deltas are unchanged
# (still LEDGER_DELTA_LIMIT). Hundreds of KB is acceptable here; multi-MB is not.
LEDGER_BOOTSTRAP_LIMIT: int = 500
LEDGER_DELTA_LIMIT: int = 500
# Depth of the v2 "step1last" resolution/journal tab streamed per SSE tick
# (recent per-bet rows + scored rejects). v2 went live 2026-07-08 at a few
# hundred rows/day and the ledger was purged to v2-only on 2026-07-09, so these
# bound the tab to the FULL v2-only history while keeping the per-tick payload
# finite; the tab paginates client-side so the DOM stays bounded regardless.
# Raised from the original 24/20 display caps (which hard-limited the tab to the
# last ~day) — operator wants the whole v2-only journal. Tune down if the
# per-tick payload grows uncomfortable.
V2_BETS_LIMIT: int = 500
V2_REJECTS_LIMIT: int = 500
# Max points in the server-side lifetime equity series (downsampled, first +
# last always preserved) so the dashboard equity curve shows the FULL active-
# strategy history without an unbounded SSE payload.
LEDGER_EQUITY_MAX_POINTS: int = 240

# Operator override required to bind the SSE server beyond loopback.
# When DASHBOARD_BIND_HOST is non-loopback and this is not set to "1", boot
# fails fast (plan U14 — "refuse non-loopback bind").
ALLOW_REMOTE_DASHBOARD: bool = os.environ.get(
    "CRYPTOBOT_ALLOW_REMOTE_DASHBOARD", "0"
) == "1"

# Audit w19oxqf24 finding #4 (P1): a non-loopback bind must ALSO assert TLS
# termination (D-LIVE-13 spec), otherwise the operator-control session cookie
# travels in cleartext and is set without the Secure attribute (the cookie
# helper in sse_server only emits Secure when this is set). Same env var the
# cookie helper reads (CRYPTOBOT_DASHBOARD_TLS_TERMINATED) so the boot gate and
# the cookie flag stay in lockstep.
DASHBOARD_TLS_TERMINATED: bool = os.environ.get(
    "CRYPTOBOT_DASHBOARD_TLS_TERMINATED", "0"
) == "1"

# Signals retention (R39 / disk hygiene). Plan trims signals rows older than
# this on every resolution-sweep cycle to keep the table bounded — at 1Hz with
# 12 non-qualifying signal inserts per tick the unbounded growth would breach
# the disk gate within a week.
SIGNALS_RETENTION_DAYS: int = int(os.environ.get("CRYPTOBOT_SIGNALS_RETENTION_DAYS", "7"))

# pipeline_health retention (A6 hygiene). The table previously grew unbounded;
# the A0 stage='latency' trace rows (one per network-active tick) make bounding
# it worthwhile. Pruned on the same resolution-sweep cycle as signals.
PIPELINE_HEALTH_RETENTION_DAYS: int = int(
    os.environ.get("CRYPTOBOT_PIPELINE_HEALTH_RETENTION_DAYS", "14")
)

# -- Sub-minute checkpoint add-on (B: step5+step1last port) -------------------
#
# The checkpoint-grid combo strategy ($BT_RUNS_DIR/
# checkpoint_grid_combo_wfo/checkpoint_grid_combo_digest.md — "$170,838"
# archive figure) adds sub-minute entry checkpoints (5s cadence + 1s cadence
# over the last 30s) at pred_edge>=0.045 / stake_mult 0.47 ON TOP of the
# unchanged baseline 60s sleeves. ⚠️ The backtest edge is a documented
# stale-ask/ghost-fill artifact (0/256 stability pass, flat fold-B ≈ 0,
# capacity ~$16-18k/chunk) gated on whether frozen near-settle quotes
# actually fill through Polymarket's 250ms taker delay — which only live
# fills can answer. The deployment is therefore RAMPED: min-bet stakes under
# a hard cumulative cap until the fill-rate breaker (execution/
# fill_rate_breaker.py) proves real fills, with an independent kill-switch.
#
# SUBMINUTE_ENABLED: master flag. Default OFF — a deploy is a no-op until the
# operator flips it; the v6 loader drops the checkpoint sleeves (and their
# wallet group) entirely when off, so the baseline path is byte-identical.
SUBMINUTE_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_ENABLED", "0"
) == "1"
# SUBMINUTE_KILL: independent kill-switch checked immediately before every
# sub-minute placement — refuses regardless of every other flag/state.
SUBMINUTE_KILL: bool = os.environ.get("CRYPTOBOT_SUBMINUTE_KILL", "0") == "1"
# SUBMINUTE_ONE_PER_WINDOW (operator change 2026-07-05): when ON, sub-minute
# places ONE initial entry per market resolution (window) and TOPS UP that same
# side on later checkpoints instead of stacking independent bets at every offset
# second. Default ON matches the retained combo backtest's resolution-level
# dedupe; set to "0" only for an explicit per-offset stacking experiment.
# Rationale: same-window stacking doubled exposure on losing resolutions with no
# diversification (see the 2026-07-05 fill analysis). Reversible via env — no
# redeploy needed to flip back.
SUBMINUTE_ONE_PER_WINDOW: bool = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_ONE_PER_WINDOW", "1"
) == "1"
# INTERVAL_5M_ENABLED (operator order 2026-07-25): master switch for the 5m
# interval. When off, NO 5m sleeve may open a new entry — the tick loop skips
# every 5m sleeve silently (no signal rows, no API calls, exactly like
# SUBMINUTE_KILL), the per-cell decision path refuses defensively, and an armed
# fast-requote for a 5m market is dropped instead of posted.
#
# Deliberately an ENTRY-ONLY gate, not a sleeve removal: `subminute_5m` stays in
# the loaded portfolio, so open 5m positions keep their held-position/T3
# coverage (held_position_pass.py looks the sleeve up by label) and the dashboard's
# active-sleeve ledger filter keeps showing 5m history and PnL. Dropping the
# sleeve from the artifact would silently do both of those harms.
#
# Default "1" = byte-identical to the pre-flag bot (backtest parity, test suite,
# any legacy artifact). install_remote.sh force-pins "0" for the LIVE deploy.
# Rationale: subminute_5m is the bleeder — apple-to-apple regrade of the clean
# post-RESET era put it at -6.4c/$1 carrying 82% of the mass, while subminute_15m
# earned +12.5c/$1 (project_regrade_0721_era_20260724).
INTERVAL_5M_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_INTERVAL_5M_ENABLED", "1"
) == "1"
# Fill-rate breaker: over the last BREAKER_WINDOW LIVE sub-minute orders,
# trip when n >= BREAKER_MIN_ORDERS and (Σfilled/Σrequested) <
# BREAKER_MIN_FILL_RATE. Ghost/stale quotes show up as FAK orders returning
# unfilled/under-filled through the 250ms taker delay — exactly this
# signature.
SUBMINUTE_BREAKER_WINDOW: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_WINDOW", "20")
)
SUBMINUTE_BREAKER_MIN_ORDERS: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_MIN_ORDERS", "10")
)
SUBMINUTE_BREAKER_MIN_FILL_RATE: float = float(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_MIN_FILL_RATE", "0.5")
)
# Realized-EV breaker arm: with >= BREAKER_EV_MIN_SETTLED settled sub-minute
# bets in the window, trip when total pnl_net < -(BREAKER_EV_MAX_LOSS_FRAC ×
# Σ stake) — i.e. worse than a -15% ROI, clearly beyond fee noise.
SUBMINUTE_BREAKER_EV_MIN_SETTLED: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_EV_MIN_SETTLED", "30")
)
SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC: float = float(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC", "0.15")
)
# Operator breaker reset: ISO-8601 UTC timestamp ("2026-07-03T00:00:00");
# only sub-minute orders created AFTER it feed the breaker window. Set this
# after investigating a trip to re-arm the ramp; empty = full history.
SUBMINUTE_BREAKER_RESET: str = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_BREAKER_RESET", ""
).strip()

# -- v2 "step1last" equity-fraction sizing (promotion to 100% live) -----------
#
# The v2 promotion sizes each sub-minute bet off live equity rather than the
# old ramp/rung/$20-cap machinery (which stays inert with RAMP_ENABLED=0):
#
#   stake_usd = equity * base_frac(interval) * brake(dd) * conc_weight(k)
#              then floored UP to MIN_BET_USD (never skipped for being < $1).
#
# These knobs originally mirrored the research SIZING_SPEC
# (polymarket/backtest/lib/pool_study.py — deleted 2026-08-03, recoverable from
# git history; live had long since diverged): base_frac_15m=0.025,
# base_frac_5m=0.0125, brake = max(0.50, 1 - dd/scale), concurrency = 1/sqrt(k).
# `dd` is the drawdown-from-peak fraction (capital.py drawdown_pct_peak_frac).
# 2026-07-31 (operator order): both intervals resize to a FLAT 1% of equity.
# The old 2:1 5m/15m asymmetry amplified the 15m era loss (the 07-30 era study
# measured -$66.25 live vs -$6.49 at flat stake), and the tick twin's maxDD
# scales ~linearly in size while end equity scales super-linearly, so the size
# is a pure risk budget: 1%/1% keeps the worst fold near 21-22% (both-stop) /
# 26.6% (the deployed both-hold arm) where the old 1.25/2.5 sizing ran 45-48%.
# 2026-08-08 (operator order, flip era): 1% -> 3% flat on BOTH intervals — the
# ENTRY_LAST_SECS_ONLY=15 gate cut trade volume ~80% and the operator sized up
# the remaining late band. Halt 0.30 unchanged (DD reaches it ~3x faster).
# 2026-08-09 (operator orders, same day): flip CLOSED — restored to 0.010;
# resized to 0.020; flip RE-ARMED full-window ~13:15 UTC; then back to 0.010
# flat. Default kept in lockstep with the install_remote.sh pin and the
# artifact capital_frac, as with the 07-31 resize above.
V2_BASE_FRAC_15M: float = _float_env("CRYPTOBOT_V2_BASE_FRAC_15M", 0.010)
V2_BASE_FRAC_5M: float = _float_env("CRYPTOBOT_V2_BASE_FRAC_5M", 0.010)
# Brake multiplier = max(V2_BRAKE_FLOOR, 1 - dd / V2_BRAKE_SCALE).
# 2026-07-16 (operator decision): reverted the F2 0.30/0.25 retune back to
# scale 0.50 / floor 0.50.
# 2026-07-21 (operator override, risk retune): scale 0.50 -> 0.60 with the
# floor kept at 0.50 — half size at the knee scale*(1-floor) = 30% DD.
# 2026-07-31 (operator order): THROTTLE REMOVED — floor 0.50 -> 1.0, which
# pins the multiplier at 1.0 for every drawdown depth (max(1.0, 1 - dd/scale)
# == 1.0 for dd >= 0). Per-$ edge was measured flat across drawdown depth, so
# the brake bought no edge and only dragged the recovery; the drawdown budget
# it used to defend is now carried by the 1%/1% resize plus the 0.30 hard halt.
# SCALE is left at 0.60 so re-arming the throttle is a one-knob change.
V2_BRAKE_SCALE: float = _float_env("CRYPTOBOT_V2_BRAKE_SCALE", 0.60)
V2_BRAKE_FLOOR: float = _float_env("CRYPTOBOT_V2_BRAKE_FLOOR", 1.0)
# Concurrency downweight toggle: when on (default), scale each same-minute
# candidate's stake by 1/sqrt(k). Set to "0" to disable the 1/sqrt(k) factor
# and size on base_frac * brake alone.
V2_CONC_DOWNWEIGHT: bool = os.environ.get(
    "CRYPTOBOT_V2_CONC_DOWNWEIGHT", "1"
) == "1"
# v2 reject-retry (operator order 2026-07-09): on a LIVE sub-minute FAK
# rejection, re-read the book ONCE and place ONE more shot iff the window is
# still open, the fresh ask is still in (0.65, 0.85], and pred_edge at the fresh
# ask still clears the 0.03 gate. One retry max per (market, side) window;
# ONE_PER_WINDOW semantics are preserved (a filled retry consumes the window
# exactly like a filled first attempt). Default ON per operator order. Consumed
# by execution/pipeline.py at the FAK reject site.
V2_REJECT_RETRY: bool = os.environ.get(
    "CRYPTOBOT_V2_REJECT_RETRY", "1"
) == "1"

# -- Anchored FAK pad (operator-approved 2026-07-11) --------------------------
#
# Lift the executable limit_price of every LIVE sub-minute BUY FAK by PAD dollars
# so the order crosses a book that ticked up between the score and the sign,
# WITHOUT chasing a runaway book. For each placement:
#     padded_limit = min(walked_limit + PAD, first_limit_of_this_side_window + PAD)
#   clipped to <= 0.99,
# where first_limit_of_this_side_window is the walked_limit of the FIRST placed
# attempt for (sleeve, coin, interval, window_start_ts, side) — captured in-memory
# on the WindowDecider (strategy/spec_v2.py, alongside the per-side-window attempt
# state). On the first attempt the two terms are equal (walked_limit + PAD); a
# later retry can therefore never pay more than the first attempt's cap + PAD.
# The stake (MarketOrderArgs amount) is CASH and UNCHANGED, so a higher fill
# price simply buys fewer shares (intended: constant stake). Counterfactual over
# live history: +28.6% PnL.
#
# Scope: LIVE + sub-minute FAK BUYs ONLY. It does NOT touch the pair-sleeve
# path or DRY rows,
# and it NEVER feeds band/gate/selection (those read the OBSERVED ask, never the
# padded limit). The padded value is what is SENT, so subminute_fills.limit_price
# and ledger.limit_price forensically record it.
#
# ⚠️ DEFAULT 0.0 = OFF = byte-identical runtime. The whole pad block short-circuits
# on `V2_FAK_PAD > 0.0`, so an unpinned deploy sends the bare walked_limit exactly
# as before. Consumed by execution/pipeline.py at the sub-minute BetSignal build.
V2_FAK_PAD: float = _float_env("CRYPTOBOT_V2_FAK_PAD", 0.0)

# -- V2_RETRY_REPRICE: price a RETRY off its own second (operator 2026-07-25) --
#
# The retry CADENCE has been every-qualifying since 2026-07-09 (V2_REJECT_RETRY:
# a FAK reject does not consume the window, so every later qualifying second
# re-scores on a fresh book and a fresh prob). The retry PRICE was not: the
# anchored pad above clamps attempt 2..N to `first_attempt_walked_limit + PAD`,
# so a retry that re-qualified on new conditions was still executed at the FIRST
# qualifying second's price. Operator framing: "even with 2nd 3rd attempt, it
# still gets caught with first qualifying condition."
#
# When ON, attempt 2..N drops that clamp and pays its OWN fresh walked_limit +
# PAD. Attempt 1 is unaffected in both modes (its anchor IS its walked_limit, so
# the min() is a no-op there).
#
# ⚠️ This is the "chase" axis, and the research is NOT a green light — record it
# honestly. Clean per-tick re-test 2026-07-25: PRE-era chase negative in 7/7
# cells (-0.5..-1.9c/$1); RESET-era UNIDENTIFIED — two independent simulators
# landed on OPPOSITE sides of zero across a $26-wide band. Armed by operator
# order over that finding. What the study measured is also STRICTLY MORE
# aggressive than what this ships: it modelled chasing to the fresh ask, whereas
# here every existing entry gate still binds the retry, and they bind BEFORE the
# price is formed --
#   * the band gate — a second only qualifies while 0.65 < ask <= 0.85, so a
#     book that runs above the band stops producing attempts at all;
#   * the gate — pred_edge >= 0.03 re-scored at THAT second's ask, so a run-up
#     that eats the edge fails the gate and sends nothing;
#   * the edge-preserving walker's own min_edge + MAX_WALK_PRICE ceiling;
#   * V2_EDGE_CAP (0.08) on claimed edge, and RETRY_DIP_GATE on the dip side.
# So the reprice can only pay UP to what still clears the edge floor inside the
# band — it is not an unbounded chase.
#
# ⚠️ DEFAULT OFF = byte-identical runtime (the anchor is still applied). Grade
# it on subminute_fills where attempt > 1: fill rate and realized c/$1 of the
# repriced cohort vs the anchored history.
V2_RETRY_REPRICE: bool = os.environ.get(
    "CRYPTOBOT_V2_RETRY_REPRICE", "0"
) == "1"

# -- MISS_FORENSICS: on-chain verdict for every rejected FAK -------------------
#
# Operator order 2026-07-30: every missed trade must be classified from CHAIN
# evidence — faster TAKER vs MAKER cancel — and shown on the dashboard. The
# ~1s post-reject book re-read (reject_requotes) cannot distinguish those two:
# "the ask is gone" reads identically whether it traded away or was pulled,
# which is how the "68% maker-cancel" inference got made and then refuted. The
# trade tape can, so a scheduler job walks each new `no_liquidity` reject and
# writes a verdict to `miss_forensics` (see scheduler/miss_forensics.py for the
# method: complement-token mint-match + settlement-lag correction).
#
# Strictly off the decision path — reads subminute_fills/live_order_attempts,
# writes only miss_forensics, every exception swallowed with a WARN, and the
# per-run HTTP/RPC call counts are hard-capped so a venue outage costs a
# bounded number of calls and then leaves rows PENDING for the next fire.
# Polymarket-only: the same core runs a predict.fun paper unit whose fills are
# not on Polygon, and the job refuses to arm there (main.py gates on VENUE).
# Needs POLY_FUNDER (to exclude our own prints); POLYGON_RPC_URL is optional
# and only adds the rival maker address to a FASTER_TAKER verdict.
# Kill switch: CRYPTOBOT_MISS_FORENSICS_ENABLED=0 (no job registered at all).
MISS_FORENSICS_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_MISS_FORENSICS_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")
MISS_FORENSICS_SECONDS: int = int(
    os.environ.get("CRYPTOBOT_MISS_FORENSICS_SECONDS", "300")
)

# -- FAST_TRIGGER: sub-second entry trigger (operator order 2026-07-30) -------
#
# The on-chain forensic (n=45 misses, 2026-07-30) measured WHY we miss: 84% of
# rejects were consumed by a FASTER TAKER, median 1.0s before our arrival, ~5x
# our size, at exactly our quoted ask. Our POST path is already at the venue
# floor (sign ~1ms, POST ~300ms of which 250ms is the taker delay every taker
# pays), so the only reclaimable latency is TRIGGER-TO-DECISION: the scanner
# samples once per second on a boundary-aligned tick, so a fillable ask that
# appears mid-second waits ~500ms on average before we even look at it.
#
# Two additional trigger rungs, both converging on the SAME single-cell
# evaluation the tick uses (execution.pipeline.run_fast_entry_scan -> the same
# scoring, the same `_commit_cell`, therefore the same band / gate /
# V2_EDGE_CAP / sizing / caps / DD-brake / dedup / first-attempt-only guards):
#
#   * SWEEP rung  — an APScheduler job at FAST_TRIGGER_MS (default 250ms).
#     Cheap-guard-first: outside a qualifying offset window it returns after
#     pure integer math on the sleeve offset sets, so it is ~free ~90% of the
#     time. Mean sampling delay 500ms -> ~125ms.
#   * EVENT rung  — the WS book mirror fires a narrow ask-side callback; a
#     dedicated coalescing worker thread runs the same scan within ~20ms of the
#     push. Separately killable (FAST_TRIGGER_EVENT_ENABLED) because it is the
#     rung that touches the venue WS reader thread.
#
# Coexistence with the 1Hz tick is safe by construction, not by timing: the
# WindowDecider's `_attempted_offsets` set means the SAME integer offset_sec
# never places twice, `subminute_slot_seen` is the durable backstop, and the
# `uniq_loa_active_slot` partial index turns any residual race into a clean
# slot_full drop. A fast scan and the tick's Phase B are additionally
# single-flighted through one process-wide lock (the fast rung SKIPS when
# contended — a skipped scan is free, a double order is not).
#
# MASTER OFF (CRYPTOBOT_FAST_TRIGGER_ENABLED=0) => no job registered, no WS
# callback registered, and the tick takes no lock: byte-identical runtime.
FAST_TRIGGER_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_FAST_TRIGGER_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")
# Sweep cadence. Clamped to >=100ms by scheduler.fast_entry.fast_trigger_seconds
# — the 100ms fast-requote drain is the proven floor for an APScheduler job on
# this box, and below that the guard itself starts to cost more than it saves.
FAST_TRIGGER_MS: int = int(os.environ.get("CRYPTOBOT_FAST_TRIGGER_MS", "250"))
# Event rung kill switch, independent of the sweep. OFF => the ask-update
# callback is never registered on the mirror and no worker thread is started;
# the sweep rung keeps running.
FAST_TRIGGER_EVENT_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_FAST_TRIGGER_EVENT_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")

# -- Top-up edge floor (2026-07-23 incident, venue-independent) ---------------
#
# Live ledger id7 (predict.fun): a T3 scale-in top-up walked/padded 5c above
# its 0.75 entry anchor and filled at 0.81 with recorded realized_edge
# -0.0119. The top-up paths re-price their edge but only enforced the
# V2_EDGE_CAP CEILING — nothing stopped a top-up whose recomputed edge had
# gone NEGATIVE, an order that contradicts the sizing logic's own claim.
#
# A top-up whose recomputed realized edge is < this floor is SKIPPED, using
# the SAME edge quantity each path already computes and records:
#   * T3 scale-in (execution/scale_in.py): the re-priced add-on edge (frozen
#     entry model_p - walked_limit - parabola fee, the ledger row's
#     realized_edge) — reject reason 'edge_below_floor', the
#     'edge_above_ceiling' twin;
#   * generic within-minute top-ups (execution/gates.py, is_topup cells):
#     the decision edge the gate stack already evaluates — skip reason
#     'topup_edge_below_floor' (distinct from the sleeve-min_edge
#     'edge_below_floor').
# Strict <, so an edge of exactly the floor passes.
#
# ⚠️ DEFAULT 0.0 is ACTIVE, not off: it blocks exactly the NEGATIVE-edge
# top-ups current logic would place (the id7 class) and nothing else — every
# top-up with edge >= 0 is byte-identical. Applies to BOTH venues; entries
# are never touched (sleeve min_edge/threshold gates own those).
TOPUP_EDGE_FLOOR: float = _float_env("CRYPTOBOT_TOPUP_EDGE_FLOOR", 0.0)

# -- Coin universe ------------------------------------------------------------

# 2026-06-12: live scanner narrowed to the effective ORB/HGB universe. SOL has
# no BASELINE ORB filter in exp044_live.json, so scanning it for the minute
# bot only creates per-second signal/log noise.
# 2026-07-03 SOL reconnect: the $170,838 checkpoint-grid backtest ran the
# 4-coin universe and SOL's flat EV is positive in every fold, so SOL rejoins
# the scan set when the sub-minute add-on is on. Its exp044_live.json ORB
# filter is marked subminute_only — baseline sleeves still reject it
# (orb_no_filter); only checkpoint sleeves can bet it.
# CRYPTOBOT_COINS (comma-separated) pins the scan universe explicitly and wins
# over the flag-derived default. The v2 promotion pins BTC,ETH,SOL,XRP so the
# 4-coin universe survives independent of SUBMINUTE_ENABLED's coin-set side
# effect. Empty/unset falls back to the flag-derived default below (SOL added
# only when the sub-minute sleeves are enabled).
_COINS_ENV = os.environ.get("CRYPTOBOT_COINS", "").strip()
if _COINS_ENV:
    COINS: tuple[str, ...] = tuple(
        c.strip().upper() for c in _COINS_ENV.split(",") if c.strip()
    )
else:
    COINS = ("BTC", "ETH", "XRP") + (("SOL",) if SUBMINUTE_ENABLED else ())


# -- Spot source + data freshness (carry-forward R8) --------------------------

# Live strategy spot source. ``binance`` is the only supported value: the
# current RF/LUT backtest features were built from Binance klines, so the live
# partial-return input must come from the same venue family. The knob is kept
# so main.py fails loud (and env parity still pins it) rather than silently
# accepting an unimplemented source.
SPOT_SOURCE: str = os.environ.get("CRYPTOBOT_SPOT_SOURCE", "binance").strip().lower()

# Public market-data endpoints have per-IP request weights. Keep the scheduler
# at 1Hz for entry timing, but reuse one batched spot response for this many
# seconds.
SPOT_FETCH_INTERVAL_SECONDS: int = int(os.environ.get(
    "CRYPTOBOT_SPOT_FETCH_INTERVAL_SECONDS", "1"
))

# Use the same Binance 1-second kline close contract as the forward backtest.
# The legacy 24h-ticker endpoint is batched and cheap, but its last-trade quote
# is not the historical ``close`` field used by ``binance_1s_forward``.  Keep
# this opt-in locally; the Polymarket deploy pins it on explicitly.
SPOT_USE_1S_KLINES: bool = os.environ.get(
    "CRYPTOBOT_SPOT_USE_1S_KLINES", "0"
).strip().lower() in {"1", "true", "yes", "on"}

# Allow a small buffer above the fetch interval so a quote published slightly
# before the bucket boundary is not rejected immediately on the final cached
# tick.
SPOT_STALENESS_S: float = float(os.environ.get(
    "CRYPTOBOT_SPOT_STALENESS_S",
    str(max(5.0, float(SPOT_FETCH_INTERVAL_SECONDS) + 2.0)),
))

# A7 latency: spot prefetch-ahead. A daemon thread fetches the spot batch
# ~LEAD_MS before each wall-clock second boundary into the same bucket cache
# the tick reads, so the tick's inline spot fetch becomes a zero-network cache
# hit (~50-100ms off every decision tick). The data the tick scores on is at
# most LEAD_MS older than the inline fetch would have returned — comparable to
# the in-flight time of the inline GET itself. Kill switch: set ENABLED=false
# to restore the legacy inline fetch exactly.
SPOT_PREFETCH_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_SPOT_PREFETCH_ENABLED", "true"
).strip().lower() in ("1", "true", "yes")
SPOT_PREFETCH_LEAD_MS: int = int(os.environ.get(
    "CRYPTOBOT_SPOT_PREFETCH_LEAD_MS", "200"
))

# A8 latency: order-book prefetch-ahead. A daemon thread re-fetches the batch
# POST /books for the token set the previous tick actually used, LEAD_MS before
# each second boundary; the tick then reads those snapshots from the cache
# instead of paying the inline ~30-50ms batch round-trip. A cached snapshot is
# served only if younger than MAX_AGE_MS at decision time; tokens not covered
# (first tick of a window, cache miss, stale) fall through to the inline batch
# exactly as today. Default OFF — flip on after shadow telemetry.
BOOK_PREFETCH_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_PREFETCH_ENABLED", "false"
).strip().lower() in ("1", "true", "yes")
BOOK_PREFETCH_LEAD_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_PREFETCH_LEAD_MS", "150"
))
BOOK_PREFETCH_MAX_AGE_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_PREFETCH_MAX_AGE_MS", "700"
))

# WS book mirror: a daemon thread mirrors the Polymarket CLOB *market*
# WebSocket channel into a snapshot cache the tick reads before the inline
# POST /books batch. Unlike the REST prefetch (A8), the WS book carries a
# server timestamp, so BookSnapshot.fetched_at_ts is stamped from the VENUE
# clock — making book staleness real (audit #12) and letting a snapshot be
# served only if younger than MAX_AGE_MS at decision time. On any WS
# error/disconnect the mirror serves nothing and the tick falls through to the
# REST batch exactly as today. Default OFF — enable via env pin only after
# live shadow validation.
BOOK_WS_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_ENABLED", "false"
).strip().lower() in ("1", "true", "yes")
BOOK_WS_MAX_AGE_MS: int = int(os.environ.get(
    # Matches the REST prefetch freshness bound (BOOK_PREFETCH_MAX_AGE_MS): a
    # book older than ~700ms can price a limit that FAK-rejects by decision time.
    "CRYPTOBOT_BOOK_WS_MAX_AGE_MS", "700"
))
BOOK_WS_URL: str = os.environ.get(
    "CRYPTOBOT_BOOK_WS_URL",
    "wss://ws-subscriptions-clob.polymarket.com/ws/market",
).strip()
# WS shadow-compare mode (W3). When BOOK_WS_ENABLED *and* this are both true the
# WS mirror runs but is NEVER authoritative: every tick compares its snapshot
# against the authoritative REST book for the same token (top_ask diff, depth
# relative diff), counts + rate-limit-logs divergences, and REST still drives
# ALL decisions. Lets the operator watch WS/REST agreement for a day before
# flipping the mirror authoritative (shadow off). Default OFF.
BOOK_WS_SHADOW: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_SHADOW", "false"
).strip().lower() in ("1", "true", "yes")
# -- WS orphan fix (2026-07-25 latency audit) ---------------------------------
# Measured from the live box: the venue market channel pushes ~822 msg/s with a
# p50 1ms inter-frame gap on the open *-updown-* windows, while this mirror was
# serving decision books with a p50 age of 6,935ms (max 114,498ms) and only ~3%
# of one core on its recv thread — i.e. the feed is a firehose and the consumer
# was barely reading it. Cause: cache entries for tokens that rolled out of the
# window were NEVER evicted, and get()'s only gate is connection-level
# recv-liveness (re-stamped by any frame, incl. pings/other tokens' acks).
#
# EVICT default ON: it can only remove stale data and the fallback is the
# well-tested REST batch. Set 0 to restore pre-fix behavior.
BOOK_WS_EVICT_UNSUBSCRIBED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_EVICT_UNSUBSCRIBED", "true"
).strip().lower() in ("1", "true", "yes")
# Per-token venue-book age gate, ms. 0 = OFF = byte-identical to pre-fix.
# SAFETY NET, not the fix — with eviction on, ages collapse and this rarely
# fires. ⚠️ Do NOT arm this alone at ~400ms while ages are still seconds-scale:
# it would reject essentially every WS hit, which is equivalent to (and slower
# than) simply setting CRYPTOBOT_BOOK_WS_ENABLED=0.
# ⚠️ SECOND, INDEPENDENT REASON NOT TO ARM THIS SUB-SECOND: the gate compares
# the tick's `now_ts` — an INTEGER second (run_scanner_tick(now_ts: int)) —
# against a millisecond venue timestamp, so the computed age carries up to
# ~1000ms of floor quantization and can even go slightly negative. The same bias
# sits in the `ws_age` telemetry this audit was built on; immaterial at the
# measured ~6.9s, material the moment anyone tunes against it sub-second. Fix the
# clock before arming the gate below ~2000ms.
BOOK_WS_MAX_BOOK_AGE_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_MAX_BOOK_AGE_MS", "0"
))

# -- WS mirror STALL WATCHDOG (operator order, 2026-08-06) ---------------------
# Lineage: the freshness cap was REMOVED on the live box
# (CRYPTOBOT_BOOK_WS_MAX_AGE_MS pinned 600000 in infra/install_remote.sh) —
# snapshot age on these books is market QUIETNESS, not feed lag, so capping it
# only spent REST round-trips re-learning the same ladder. The accepted residual
# is one gray zone: a STALLED-BUT-CONNECTED subscription (socket alive, so
# recv-liveness keeps being re-stamped by pings/other tokens, while updates for
# one subscribed token silently stop) would price a dead ladder until the window
# rolls. Connection death already clears the mirror; rolled-off tokens are
# evicted (2026-07-25 orphan fix). This watchdog is the mechanical detector for
# what is left.
#
# CONSTRAINT: it runs ONLY on the scheduler pool — never on the tick/order path
# — so the ~0ms WS hot path is unchanged. It cross-checks only SUSPECT tokens
# against REST, fails open on any error (never evicts on a failed verify), and
# with ENABLED=0 nothing is constructed and no job is registered (byte-identical).
# Default ON wherever the mirror is authoritative (BOOK_WS_ENABLED and not
# SHADOW): the detector is the whole reason removing the cap was safe.
BOOK_WS_WATCHDOG_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_ENABLED", "true"
).strip().lower() in ("1", "true", "yes")
# Cross-check cadence. Bounds the REST budget: ONE batch POST /books per pass =
# 12/min at 5s, covering at most MAX_TOKENS_PER_PASS tokens.
BOOK_WS_WATCHDOG_INTERVAL_S: float = float(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_INTERVAL_S", "5"
))
# A ladder older than this is SUSPECT — not stale, just worth one REST question.
# Must stay well above the normal inter-frame gap on a busy book (p50 1ms) and
# well below a window's life, or every quiet book becomes a suspect.
BOOK_WS_WATCHDOG_STALL_SUSPECT_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_STALL_SUSPECT_MS", "5000"
))
# Rate limit: oldest-first cap on tokens cross-checked per pass. One batch
# request regardless, but a small cap keeps the response (and the compare work)
# bounded on the scheduler pool.
BOOK_WS_WATCHDOG_MAX_TOKENS_PER_PASS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_MAX_TOKENS_PER_PASS", "4"
))
# A token that VERIFIED against REST is not re-asked until this elapses (or new
# ws activity lands and the book goes quiet again). Without it a quiet-but-
# correct book would be re-fetched every interval, re-creating in the background
# exactly the REST cost the cap removal eliminated.
BOOK_WS_WATCHDOG_VERIFY_TTL_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_VERIFY_TTL_MS", "30000"
))
# Top-of-book comparison: price must match EXACTLY (cent ticks — any real
# difference is >= 0.01), size within this RELATIVE tolerance because the batch
# and delta paths round sizes differently.
BOOK_WS_WATCHDOG_SIZE_REL_TOL: float = float(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_SIZE_REL_TOL", "0.02"
))

# -- RF step-function LUT (2026-07-25 latency audit) ---------------------------
# The checkpoint RF forests are univariate (feature_columns ==
# ['partial_ret_z_1h']), so each forest is exactly a step function of z and can be
# served by a searchsorted instead of 200 tree traversals — 0.003ms vs ~6.8ms
# measured, and score_cell_hgb calls predict_proba once per cell
# (~36ms/order tick). LUTs are built on a daemon thread; a slice serves the
# unchanged sklearn path until its LUT lands. (Unrelated to the retired
# decision_table LUT — this is a per-forest step function, not a hit-rate table.)
# Default ON, but kill-switchable WITHOUT a redeploy: this is the one change in
# the pass that alters a number the decision reads, so the operator keeps a lever.
# Set 0 to force every slice back onto sklearn.
RF_STEP_LUT_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_RF_STEP_LUT", "true"
).strip().lower() in ("1", "true", "yes")

# Binance market-data-only base URL recommended by Binance for public market
# data. Operators can switch to https://api.binance.com or api1-api4 if needed.
BINANCE_BASE_URL: str = os.environ.get(
    "CRYPTOBOT_BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

# -- Notify -------------------------------------------------------------------

CONSECUTIVE_FAILURE_THRESHOLD: int = 5
