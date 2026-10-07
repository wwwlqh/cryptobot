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
from typing import NamedTuple

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


def _optional_float_env(env_key: str) -> float | None:
    """A float knob that is UNSET by default (returns None on unset/blank).

    Unlike ``_float_env`` there is no numeric default: None means "no override",
    which callers use to stay byte-identical to the pre-knob behavior."""
    raw = os.environ.get(env_key)
    if raw in (None, ""):
        return None
    return float(raw)


def _tristate_env(env_name: str) -> str:
    """Shared parser for a shadow-first rollout flag -> {"off","shadow","live"}.

    "1"/"true"/"yes"/"live" -> "live"; "shadow" -> "shadow"; everything else
    (incl. "0", unset, blanks, typos) -> "off" (fail-open). The ONE tri-state
    mapping behind PREPOST_DROP_GUARD_MODE — an off/shadow/live gate with this
    env surface (see the
    ``_float_env`` env-name idiom)."""
    key = (os.environ.get(env_name) or "").strip().lower()
    return {
        "1": "live", "true": "live", "yes": "live", "live": "live",
        "shadow": "shadow",
    }.get(key, "off")


# -- Venue ---------------------------------------------------------------------

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

# Latency finding #6 (gc-pause): flag-gated CPython GC tuning. Boot pins a
# large permanently-live tracked heap (RF/HGB/checkpoint artifacts + the LUT
# dict + the pandas/sklearn import graphs); at the default thresholds
# (700, 10, 10) every gen-2 collection re-scans ALL of it WITH THE GIL HELD
# (plausibly 30-150ms on the small EC2 vCPU), landing at an arbitrary point on
# the tick/stop path — including between a decision and its order POST (part
# of the tick.py "other" residue class). When ON, main.py — after every boot
# artifact load, immediately before start_scheduler — runs gc.collect() then
# gc.freeze() so the boot heap moves into the permanent generation and leaves
# every future pass, raises the generation thresholds to the values below, and
# registers a periodic explicit gc.collect() on the scheduler pool (the
# wal_checkpoint_job off-tick pattern) to bound memory creep from the raised
# thresholds; GC_COLLECT_SECONDS=0 disables just that job. Tradeoffs are
# memory-only — watch RSS on the 2GB box after arming (see
# polymarket/infra/cryptobot.env.example).
# ⚠️ DEFAULT OFF = zero gc calls anywhere = byte-identical runtime.
GC_TUNING: bool = os.environ.get(
    "CRYPTOBOT_GC_TUNING", "0"
).strip().lower() in ("1", "true", "yes")
# gc.set_threshold(gen0, gen1, gen2) applied when GC_TUNING is on. 50000/20/20
# per the finding: young-gen passes stop tripping on the tick's ~1-5k
# containers/tick steady churn (json parses, dataclasses, sqlite rows) and
# full passes become rare — the explicit off-tick collect job covers the gap.
GC_GEN0_THRESHOLD: int = int(os.environ.get("CRYPTOBOT_GC_GEN0_THRESHOLD", "50000"))
GC_GEN1_THRESHOLD: int = int(os.environ.get("CRYPTOBOT_GC_GEN1_THRESHOLD", "20"))
GC_GEN2_THRESHOLD: int = int(os.environ.get("CRYPTOBOT_GC_GEN2_THRESHOLD", "20"))
# Cadence of the explicit off-tick gc.collect() scheduler job (seconds).
# Only wired when GC_TUNING is on; 0 disables the job while keeping the
# freeze + thresholds.
GC_COLLECT_SECONDS: int = int(os.environ.get("CRYPTOBOT_GC_COLLECT_SECONDS", "60"))
# GC pause telemetry: a gc.callbacks hook logs any collection pause >= 10ms
# ("gc_pause ..." in scanner logs). Deliberately INDEPENDENT of GC_TUNING so
# the operator can measure a before (telemetry on, tuning off) vs after (both
# on) on the live box. DEFAULT OFF.
GC_PAUSE_TELEMETRY: bool = os.environ.get(
    "CRYPTOBOT_GC_PAUSE_TELEMETRY", "0"
).strip().lower() in ("1", "true", "yes")

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

# Entry-timing skip ("skip first N seconds of every minute" lever). The scanner
# ticks at 1Hz; a positive N suppresses ALL bet placement during the first N
# seconds of EVERY minute. The bot still SCANS those seconds — the partial-ret
# z-score engine is fed every tick so its window-open snapshot and minute-
# boundary returns stay parity-correct — it just does not TRADE until second N.
# Because the per-window ledger dedup admits one bet per window on the first
# passing tick, a positive N lands each window's fill at ~second N instead of
# ~second 0, reading the live ask ~N seconds later.
#
# DEFAULT 0 = no skip (2026-06-07 operator decision). This ALIGNS live entry
# timing with the ORB+HGB backtest, which fills from the pmdata.dev book
# snapshot AT entry_ts (the entry-minute boundary = second 0) with NO offset —
# see polymarket/backtest/training/pmdata_target.py::_raw_book_payload
# (searchsorted(entry_ts) picks the last snapshot at/before the boundary).
# Both live and backtest now read the book at the minute boundary.
#
# Setting N>0 (e.g. CRYPTOBOT_ENTRY_SKIP_SECONDS=10) re-introduces the live skip
# and DIVERGES from the backtest — the backtest would fill ~N seconds earlier
# than live. Do not do this unless the backtest book read is given a matching
# +N second offset so the two stay aligned.
ENTRY_SKIP_SECONDS: int = int(os.environ.get("CRYPTOBOT_ENTRY_SKIP_SECONDS", "0"))

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
# Rung-1 hard cumulative dollar cap: total realized sub-minute fill value
# (SUM(filled_usd) over subminute_fills) must stay under this before any
# rung-1 placement; the query failing counts as the cap being hit
# (fail-closed).
# 2026-07-05 operator decision: the staged ramp (min-bet rungs under the $20
# cumulative cap) is REMOVED — the checkpoint parity port (checkpoint_v1 models)
# means live now actually runs the backtest strategy, and the operator wants the
# 0.30 drawdown halt (MAX_DRAWDOWN_HALT_FRAC) to be the SOLE sizing/risk control.
# When RAMP_ENABLED is off (default), ramp_state() returns full-target sizing
# from the first sub-minute bet (no rungs, no cumulative cap). The fill-rate/EV
# BREAKER (evaluate_breaker) is INDEPENDENT and still runs. Set this env to "1"
# to restore the old probation ramp.
SUBMINUTE_RAMP_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_RAMP_ENABLED", "0"
) == "1"
SUBMINUTE_RAMP_CAP_USD: float = float(
    os.environ.get("CRYPTOBOT_SUBMINUTE_RAMP_CAP_USD", "20")
)
# Ramp rungs (settled LIVE sub-minute bets required to advance): rung 1
# (min-bet) until GREEN_BETS settle with the breaker green; rung 2 (50%
# target, floored at MIN_BET_USD) until 2×GREEN_BETS; rung 3 = full
# pe0.045/stake0.47 target (also floored at MIN_BET_USD).
# ⚠️ Arithmetic constraint (review finding): rung-1 fills are ~$1 each and
# the RUNG-1 cumulative cap is RAMP_CAP_USD — GREEN_BETS × MIN_BET_USD must
# stay comfortably UNDER the cap or rung 2 is unreachable (the original 25
# deadlocked against the $20 cap at ~20 fills). 15 × $1 = $15 leaves
# headroom for partial fills.
SUBMINUTE_RAMP_GREEN_BETS: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_RAMP_GREEN_BETS", "15")
)
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
# F2 tier-A size boost (operator 2026-07-14): multiply the sized stake by
# V2_TIER_A_MULT when BOTH (a) this is the market's FIRST qualifying second
# this window (WindowDecider market_place_rank == 1) and (b) the score edge
# clears V2_TIER_A_EDGE. Backtest E-holdout: A-tier first fills earn ~2x the
# per-$ return of the rest. Set MULT=1.0 to disable.
V2_TIER_A_MULT: float = _float_env("CRYPTOBOT_V2_TIER_A_MULT", 1.5)
V2_TIER_A_EDGE: float = _float_env("CRYPTOBOT_V2_TIER_A_EDGE", 0.0465)
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

# -- Multifill: independent entries per resolution (operator 2026-07-10) -------
#
# Capacity scaling for a GROWN bankroll. Above V2_MULTIFILL_MIN_EQUITY the
# one-bet-per-resolution rule is relaxed to allow additional INDEPENDENT entries
# on the SAME (coin, interval, window) market — each fully re-qualified through
# the normal per-second path (fresh book / band / gate / reachability) and each
# sized independently (equity-fraction × DD-brake × conc). Operator framing: "if
# there is a 2nd entry you can have a 2nd BTC bet if all conditions are
# satisfied, applied for all coins". The entry COUNT is UNBOUNDED (the fixed
# per-resolution ceiling was removed by operator order 2026-07-11 — "the
# number-of-bets cap = 5, no need"); capacity is bounded instead by the equity
# gate, the same-side rule, the shared exposure cap, and the DD brake/halt, all
# of which still bind globally.
#
# A 2nd..Nth entry is permitted ONLY when ALL hold: the flag is on; equity >=
# MIN_EQUITY; a PRIOR entry on this resolution already FILLED (only fills write a
# ledger row — a reject keeps normal retry semantics and opens no extra slot).
# Consumed by execution/pipeline.py (the sub-minute one-per-window decision
# block).
#
# EVERY-QUALIFYING (operator order 2026-07-25): MIN_EQUITY 5000 -> 0, so multifill
# is ARMED AT THE CURRENT ~$130 EQUITY. Entry semantics move from "the first
# qualifying second that fills owns the resolution" to "every qualifying second
# is a fresh entry" — after the first attempt FILLS, later qualifying seconds
# re-score on a fresh book/prob and place independent 2nd..Nth entries. The
# equity gate is no longer the binding constraint; capacity is bounded instead by
# V2_MULTIFILL_MAX_EXTRA (below), the same-side rule, the shared exposure cap,
# and the DD brake/halt.
# ⚠️ Two consequences of a 0 floor, both accepted by the operator:
#   * `count_window_entries` (one indexed ledger COUNT) now runs on EVERY
#     sub-minute qualifying second instead of short-circuiting — a small fixed
#     add on the hot path.
#   * the CLOSED <~$250/resolution multi-entry research regime
#     (project_multi_entry_per_resolution) is no longer held out.
# Revert = set CRYPTOBOT_V2_MULTIFILL_MIN_EQUITY back to 5000 (env-only), or
# CRYPTOBOT_V2_MULTIFILL_ENABLED=0 to disarm entirely.
V2_MULTIFILL_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_V2_MULTIFILL_ENABLED", "1"
) == "1"
V2_MULTIFILL_MIN_EQUITY: float = _float_env(
    "CRYPTOBOT_V2_MULTIFILL_MIN_EQUITY", 0.0
)
# Per-resolution entry cap — RE-INTRODUCED (operator order 2026-07-25), keyed on
# EXTRAS so the total is unambiguous: 1 initial entry + MAX_EXTRA extras, i.e. at
# most 4 non-topup ledger rows on one (coin, interval_min, window_start_ts).
#
# The old CRYPTOBOT_V2_MULTIFILL_MAX ceiling was removed 2026-07-11 ("the
# number-of-bets cap = 5, no need") — safe only because the $5000 equity floor
# meant multifill never actually ran. Dropping that floor to 0 re-exposed the
# surface: a 15m window offers 30 qualifying seconds (offsets 870-899), so an
# uncapped resolution could take ~30 entries. At ~$130 equity and a ~$3.25 stake
# that is ~$97 — roughly 75% of the bankroll on ONE binary outcome. Nothing else
# bounds that: the exposure cap is cap_tracks_equity, so its ceiling IS equity —
# it bounds TOTAL exposure, never concentration in a single market — and the DD
# brake only scales the per-bet stake. The cap does: 4 x ~$3.25 = ~$13, ~10%.
#
# Enforced on the DURABLE ledger tally (``count_window_entries``), so it survives
# a process restart mid-window and counts cross-sleeve + cross-side exactly like
# that tally. 0 = no extras (one entry per resolution, the pre-multifill
# semantic). Consumed by the pipeline's sub-minute one-per-window block, which
# emits skip_reason='multifill_cap' when it binds — the cap must never be silent.
# The retired CRYPTOBOT_V2_MULTIFILL_MAX key stays dead and scrubbed; this is a
# NEW key with different (extras-not-total) semantics, so a stale value of the
# old one can never be misread as this one.
V2_MULTIFILL_MAX_EXTRA: int = max(
    0, int(os.environ.get("CRYPTOBOT_V2_MULTIFILL_MAX_EXTRA", "3"))
)

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

# -- V2_FIRST_ATTEMPT_ONLY: a venue reject ends the side window ---------------
#
# Operator order 2026-07-29: "when first attempt not filled, when being filled
# 2nd 3rd time, the trade normally loses" — first attempt only. The realized
# data agrees with the instinct: retry fills ran -6.2c/bet vs -2.3c first-try
# (n=89); the reappeared book comes back repriced to fair (price-honesty
# z=0.34 over 186 reappeared sides — realized WR statistically equals the
# price paid), and the dip-refill cohort is WR 52.2% / -33.7c/$1. A blanket
# retry-cancel was previously rejected as day-unstable; this ships it anyway
# per explicit operator override — grade it on subminute_fills.attempt (the
# >1 cohort should go to ~zero) and the forward-OOS weekly re-grade.
#
# When ON, a venue `no_liquidity` FAK reject of a NON-top-up sub-minute
# (offset) attempt CONSUMES that (coin, interval, window_start, side): every
# later qualifying second skips with skip_reason='first_attempt_consumed'
# instead of retrying, and the fast-requote never arms. Scope is deliberately
# narrow:
#   * entry SELECTION is unchanged — every qualifying second remains eligible
#     to be the FIRST attempt. This is NOT first-commit (V2_REJECT_RETRY=0),
#     where a gate-FAILED second consumes the window before any order is sent;
#     here only a real venue reject consumes.
#   * only `no_liquidity` consumes — infra/transient error kinds carry no
#     price information and keep the per-second retry semantics.
#   * top-ups are untouched. Multifill extras after a
#     FILL stay allowed, but a rejected extra consumes the side like any
#     other reject (same adverse cohort).
#   * the opposite SIDE of the window stays independently eligible.
# State is in-memory, keyed like the decider maps (window_start at index 2)
# and pruned by _v2_prune_window_decider; a process restart mid-window
# forgets rejects <=30s old — acceptable, same class as the dip gate.
# Revert = CRYPTOBOT_V2_FIRST_ATTEMPT_ONLY=0 (env-only, no code change).
# The RETRY_DIP_GATE stays an independent (now largely shadowed) fence.
V2_FIRST_ATTEMPT_ONLY: bool = os.environ.get(
    "CRYPTOBOT_V2_FIRST_ATTEMPT_ONLY", "1"
) == "1"

# -- SUBMINUTE_FLIP_SIDE: bet AGAINST the model (operator order 2026-08-07) ----
#
# Operator order 2026-08-07 ("u flip in live", full size), over advisor
# objection recorded in the session: TWAP day-1 ran 13/26 our-side (fills
# -$11.18, no_liquidity misses graded -$5.04) and the operator judged the live
# edge inverted vs the TWAP BT. Prior large-sample flip/contrarian studies
# (2026-07-19/20) graded -EV; n=26 cannot reject a market-fair 0.73. This flag
# exists to run the operator's inversion experiment anyway, reversibly.
#
# When ON, score_cell_hgb selects the best side EXACTLY as today (same model,
# same ask-band on the model side, same threshold/edge gates, same sizing, same
# firing seconds) and then returns the OPPOSITE side. Everything downstream —
# book routing, walker, token, ledger.side, resolution grading — follows the
# flipped side natively.
#
# ⚠️ PROVENANCE TRAP (deliberate): with the flag ON, `model_p` / `edge` /
# `realized_edge` in signals+ledger describe the MODEL's side, while `side` /
# asks / fill/limit prices describe the EXECUTED (flipped) side. model_p is NOT
# the model's win probability of the recorded side (it is ~1-that). Grade the
# flip cohort by outcome/pnl_net only.
#
# SUBMINUTE_FLIP_PAD caps what the flip may pay: the walker's price ceiling on
# the flip book is min(sleeve max_walk_price, (1 - model_side_ask) + PAD), i.e.
# the mirror price plus PAD. A flip book quoted wider than that is NOT taken
# (venue no_liquidity-style skip) — without this cap a wide complement book
# could fill 20c+ over mirror and the experiment measures spread, not signal.
# PAD default 0.02 = operator 2026-08-07 ("cap 0.02 instead of 0.01, since
# there is spread if u flip") — 2c spread allowance over mirror.
#
# ⚠️ DEFAULT OFF = byte-identical runtime. Revert = drop the env pin.
SUBMINUTE_FLIP_SIDE: bool = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_FLIP_SIDE", "0"
) == "1"
SUBMINUTE_FLIP_PAD: float = _float_env("CRYPTOBOT_SUBMINUTE_FLIP_PAD", 0.02)

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

# -- V2_EDGE_CAP: pred_edge upper cap (registered candidate 2026-07-22) -------
#
# Claimed edge is anti-calibrated at the top: the bigger the model's edge claim,
# the more the MARKET is right. Repaired-ledger live buckets (2,451 decided):
# edge 0.08-0.10 = -3.1c/$1 (n=47), 0.10+ = -44.1c/$1 (realized WR 36% vs
# claimed 87%, n=11). Backtest at-ask replay agrees — fold F: 0.05-0.06 -1.7,
# 0.06-0.08 -6.2, 0.08-0.10 -16.0, 0.10+ -23.7 c/$1 (WR 77%->56% while claimed
# rises 80%->87%); fold E has the same toxic >=0.08 tail. Sizing is flat
# equity-fraction, so without a cap the most-wrong bets get full stake.
#
# Mechanism: when > 0, the v6 loader clamps every sleeve's max_edge to
# min(artifact max_edge, cap) at load; the existing edge_above_ceiling gate and
# its signals telemetry then enforce/record it (no new gate path).
#
# ⚠️ DEFAULT 0.0 = OFF = byte-identical runtime (live artifact ships
# max_edge=1.0 on both MID sleeves). ARMED 0.08 in production by operator
# order 2026-07-22 (install_remote.sh pin; supersedes the 2-week-grade
# pre-registration): 0.08 is negative in live AND fold E AND fold F AND 6/7
# weekly chunks; cap-sweep ranked 0.08 most robust (0.07 = backtest-max
# +2596c E+F, 0.06 rejected — flips fold E negative). Operator order
# 2026-07-23: the cap governs entries AND T3 scale-in top-ups on BOTH venues
# (scale_in re-checks the re-priced add-on edge; the paper unit's =0
# override was removed so it inherits this shared pin).
V2_EDGE_CAP: float = _float_env("CRYPTOBOT_V2_EDGE_CAP", 0.0)

# -- Held-position pass (T3 scale-in driver) ---------------------------------
#
# After a sub-minute position FILLS, a per-second pass re-reads it, folds the
# held-side ask into T3's no-dip running min and places at most one FAK top-up.
# Consumed by execution/held_position_pass.py. The pass self-gates on
# SCALEIN_T3_ENABLED (below) + a wired OrderClient — there is no separate
# enable knob.
#
# HISTORY: this pass was execution/stop_loss.py (PURE_STOP_04). The stop-loss
# EXIT was retired by operator order 2026-08-05 and every knob that fed only
# the sell — CRYPTOBOT_STOPLOSS_{ENABLED,BAND,MAX_ATTEMPTS,SELL_CHASE,
# EXCLUDE_INTERVALS,MIN_EXIT_VALUE_USD}, CRYPTOBOT_STOP_SLIP_EXEMPT{,_THRESH}
# and CRYPTOBOT_MIN_STOPPABLE_STAKE_USD — is DELETED. The deployed pin was
# EXCLUDE_INTERVALS="5,15", i.e. the stop was already exempt on both traded
# intervals, so the deletion is a runtime no-op. install_remote.sh scrubs the
# retired CRYPTOBOT_STOPLOSS_* / CRYPTOBOT_STOP_SLIP_EXEMPT* keys from
# /etc/cryptobot.env so a stale pin can never shadow a future knob.

# Route the pass's per-tick book reads (the held + other token) through the
# shared WS-mirror/prefetch BookSourceChain (TickDeps.book_prefetch) instead of
# two raw serial REST /book GETs per open candidate per second. This is
# POST-ORDER TICK HYGIENE, NOT an order-latency change: the pass runs strictly
# AFTER Phase B, so it never sits between an entry decision and its order POST.
# The win is that its ~2 GETs/candidate/second (each ~25ms serial) scale away as
# the candidate count grows — the same WS/prefetch snapshots the entry scan
# already warmed serve the re-score for free. A chain snapshot is used for the
# HELD side ONLY when it carries an executable bid (the held-side bid is
# load-bearing for the no-dip crossed-book check and the T3 disarm predicate; an
# ask-only snapshot would make the pass bail before the re-score and silently
# skip T3 for that tick — the 73c6153 silent-blindness class); on any chain miss
# / stale / bid-missing / exception the token falls back to the authoritative
# REST GET. DEFAULT OFF: with this unset — or when no chain is wired
# (deps.book_prefetch is None) — the book reads go straight to REST: one batched
# deps.fetch_books POST for the chain-missed tokens when HELD_PASS_BATCH_BOOKS
# (below, default ON) allows it; per-token deps.fetch_book GETs when the batch is
# disabled, unwired, fails, or omits a token.
# (Renamed from CRYPTOBOT_STOPLOSS_BOOK_CHAIN, 2026-08-05; live pin was "1".)
HELD_PASS_BOOK_CHAIN: bool = os.environ.get(
    "CRYPTOBOT_HELD_PASS_BOOK_CHAIN", "false"
).strip().lower() in ("1", "true", "yes")
# Kill switch for the pass's batched POST /books (the REST tier above).
# DEFAULT ON = keep the latency win: chain-missed tokens arrive in ONE
# fetch_books POST (one TokenBucket draw) instead of two serial per-token GETs.
# Parse semantics are identical either way (the batch POST and the per-token GET
# share _snapshot_from_payload). Set CRYPTOBOT_HELD_PASS_BATCH_BOOKS=0 to skip
# the batch attempt entirely and restore the per-token deps.fetch_book GET path
# byte-identically (the fallback ladder IS that path). Consumed by
# execution/held_position_pass.py::_books_for_pass.
# (Renamed from CRYPTOBOT_STOPLOSS_BATCH_BOOKS, 2026-08-05; was never pinned.)
HELD_PASS_BATCH_BOOKS: bool = os.environ.get(
    "CRYPTOBOT_HELD_PASS_BATCH_BOOKS", "true"
).strip().lower() in ("1", "true", "yes")
# Held-position pass cadence (latency lever). DEFAULT 0 = the pass runs INLINE
# in the 1Hz scanner tick, strictly AFTER Phase B (so a top-up never sits
# between an entry decision and its order POST). When > 0 the value is the
# desired cadence in MILLISECONDS, clamped UP to a 250ms floor — the Polymarket
# 250ms uncancellable taker delay makes a tighter loop pointless. At >0 the
# inline run_held_position_pass call in run_scanner_tick is SKIPPED and main.py
# wires a DEDICATED APScheduler job at this cadence (IntervalTrigger takes float
# seconds; max_instances=1 + coalesce) that runs the pass on its OWN
# ThreadLocalConnections connection (the wal_checkpoint_job / requote_drain_job
# pattern), decoupling T3 top-ups from the entry tick.
#
# Known, accepted race: a fast-job fire that lands BETWEEN an entry's ledger
# commit and its deferred fill write-back sees live_filled_size NULL for that
# position and simply skips it until the next cycle (the _open_held_positions
# query already filters `live_filled_size IS NOT NULL AND > 0`, so a NULL-fill
# row is never a candidate — no crash). DEFAULT 0 => no dedicated job, inline
# pass exactly as today.
# (Renamed from CRYPTOBOT_STOPLOSS_FAST_CADENCE_MS, 2026-08-05. NOT pinned in
# the shared Polymarket env — the predict.fun unit pins its own 1000ms.)
_held_pass_cadence_raw = int(
    os.environ.get("CRYPTOBOT_HELD_PASS_CADENCE_MS", "0")
)
HELD_PASS_CADENCE_MS: int = (
    0 if _held_pass_cadence_raw <= 0 else max(250, _held_pass_cadence_raw)
)


# -- T3 scale-in (no-dip taker top-ups; research WFO pick 2026-07-15) ----------
#
# After a sub-minute ENTRY fills, scale into the SAME side with small
# frozen-at-entry taker (FAK) top-ups while the market "has not dipped". Rides
# inside the per-second held-position loop (execution/scale_in.py, driven from
# execution/held_position_pass.py). The reference semantics are the
# research driver polymarket/backtest/scripts/tick_topup_foldBE.py::_simulate_topups;
# the pre-registered WFO pick is config **T3** (SCALEIN_GRID_DIGEST.md; digest
# removed in a6df8ed, recover via `git show
# a6df8ed^:polymarket/backtest/studies/tick_latency/SCALEIN_GRID_DIGEST.md`):
#   * every 1s from entry to resolution, attempt ONE top-up while ALL hold:
#       (i)  no-dip: the side's best ask has NEVER been observed below the entry
#            ask (ask0 = ledger p_market); first violation permanently disarms M;
#       (ii) the walked ask for the top-up stake <= CAP (0.83, the binding rule);
#       (iii) spread (best ask - best bid) <= SPREAD_MAX (0.02);
#       (iv) fewer than MAX_N (3) top-up FILLS so far for M;
#       (v)  cumulative top-up fill cash + this stake <= EXPOSURE_CAP (1.5x) of
#            the entry's realized filled cash;
#       (vi) all existing entry risk checks (40% DD halt, available cash,
#            $1 MIN_BET floor) — the top-up cash flows through the same ledger
#            accounting (a normal PENDING LIVE row tagged scalein_topup=1).
#   * order: FAK buy, stake = FRAC (0.5x) the entry's requested stake (cents-
#     floored, $1 floor), limit = walked_limit + FAK_PAD (0.01) capped at 0.99.
#     Each top-up pads from its OWN walk — NOT the entry's anchored FAK pad
#     (which would auto-reject a deliberate higher-ask top-up).
#   * the held-position pass never sells; every position remains held through
#     resolution, while deterioration permanently disarms further top-ups for M.
#
# One-bet-per-market is UNCHANGED except that these same-market/same-side
# top-ups are its ONLY exemption, via this path only.
#
# DEFAULT OFF in code. With CRYPTOBOT_SCALEIN_T3_ENABLED unset the whole scan is
# skipped and every path is byte-identical to the no-scale-in held path. Prod arms it
# by pinning CRYPTOBOT_SCALEIN_T3_ENABLED=1 in /etc/cryptobot.env (see
# polymarket/infra/cryptobot.env.example — the enable pin is added to the env file by the
# operator, NOT rewritten by a code default). All params are env-overridable.
SCALEIN_T3_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_SCALEIN_T3_ENABLED", "0"
) == "1"
SCALEIN_T3_MAX_N: int = int(os.environ.get("CRYPTOBOT_SCALEIN_T3_MAX_N", "3"))
# Model-deterioration DISARM band. Renamed from CRYPTOBOT_STOPLOSS_BAND
# (2026-08-05) and kept at the same FROZEN 0.04 — the § C/D locked scalp-WFO
# value, which was never pinned in any env, so the default IS the live value.
#
# This is the original PURE_STOP_04 trigger band, and it is NOT vestigial: the
# predicate (held_position_pass._deterioration_disarm) fires when
# `bid >= model_p + band` AND the position is underwater, and its result is
# passed to scan_topup as `triggered=`, where True PERMANENTLY disarms further
# top-ups for that market. On the deployed configuration the retired stop never
# sold on this signal (both intervals were exempt), but it always drove this
# disarm — so the band still governs live T3 behavior. Do not tune without
# re-running the scalp WFO delay-stress.
SCALEIN_T3_DISARM_BAND: float = _float_env(
    "CRYPTOBOT_SCALEIN_T3_DISARM_BAND", 0.04
)
SCALEIN_T3_FRAC: float = _float_env("CRYPTOBOT_SCALEIN_T3_FRAC", 0.5)
SCALEIN_T3_NO_DIP: float = _float_env("CRYPTOBOT_SCALEIN_T3_NO_DIP", 0.0)
SCALEIN_T3_CAP: float = _float_env("CRYPTOBOT_SCALEIN_T3_CAP", 0.83)
SCALEIN_T3_SPREAD_MAX: float = _float_env("CRYPTOBOT_SCALEIN_T3_SPREAD_MAX", 0.02)
SCALEIN_T3_EXPOSURE_CAP: float = _float_env(
    "CRYPTOBOT_SCALEIN_T3_EXPOSURE_CAP", 1.5
)
SCALEIN_T3_CADENCE_S: int = int(os.environ.get("CRYPTOBOT_SCALEIN_T3_CADENCE_S", "1"))
# The "+0.01" order pad on each top-up's OWN walked limit. Kept a dedicated knob
# so it never couples to the entry-scoped V2_FAK_PAD.
SCALEIN_T3_FAK_PAD: float = _float_env("CRYPTOBOT_SCALEIN_T3_FAK_PAD", 0.01)
# End-of-window buffer: stop attempting top-ups this many seconds before the
# window closes (memory notes recording stops ~4s pre-resolution).
SCALEIN_T3_END_BUFFER_S: int = int(
    os.environ.get("CRYPTOBOT_SCALEIN_T3_END_BUFFER_S", "4")
)

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

# -- T3 same-second "blind" top-up fresh gate (flag-gated, DEFAULT "0" = OFF) ---
#
# Evidence (live ledger 2026-07-20): a T3 top-up placed in the SAME SECOND as its
# parent ENTRY is "blind" — the stop pass drives scan_topup within the same 1s
# loop tick as the entry, against a held-side book that can PREDATE the parent's
# POST. When the book crashes onto the parent in-flight (the 250ms uncancellable
# taker delay), the same-second top-up sails into the same crater; the no-dip
# check can't see it because the dip is not in the observed book yet (real case:
# ETH parent + top-up both stamped 14:34:54, decision ask 0.69, both filled 0.60).
# Live since 07-14: same-second top-ups n=50 -> -$5.81 total; top-ups placed >=1s
# after the parent n=117 -> +$4.28. This gate kills only the blind same-second
# cohort and leaves the later cohort untouched.
#
# The gate requires the book used for the top-up to be demonstrably fresher than
# the parent's POST/fill moment. The /book REST snapshot carries no trustworthy
# server timestamp (BookSnapshot.fetched_at_ts is the LOCAL fetch time, see
# data/polymarket_clob.py audit #12), so the implementable rule is TIME-BASED:
# the scan second must be STRICTLY at least 1s after the parent's fill second —
# i.e. reject when now_epoch <= parent_fill_epoch. The parent moment is parsed
# from live_fill_ts (the CLOB fill payload), falling back to bet_ts then
# created_at when live_fill_ts is NULL. This ALSO restores live<->backtest parity:
# the T3 reference (polymarket/backtest/scripts/tick_topup_foldBE._simulate_topups)
# starts its FIRST top-up at t = ceil(entry_fill_ts) + 1 — strictly after the
# fill second — a same-second top-up it never simulates.
#
# Tri-state, parsed to TOPUP_FRESH_GATE_MODE in {"off","shadow","live"}:
#   "0"      -> off    : byte-identical — scan_topup never evaluates the gate.
#   "shadow" -> shadow : log-only — each would-skip is logged + counted
#                        (_FRESH_GATE_SHADOW_COUNTS) but the top-up PROCEEDS
#                        exactly as today (no behavior change; grade offline by
#                        joining the logged ledger_id to its settlement pnl_net).
#   "1"      -> live   : the same-second top-up is SKIPPED this second (reason
#                        'fresh_gate') — NOT an attempt (no cadence throttle
#                        consumed / last_attempt_ts unwritten) and NOT a disarm;
#                        the next second's scan proceeds normally (delay, not kill).
# Anything else -> off (fail-open).
#
# FAIL-OPEN on missing data: if NO timestamp (live_fill_ts / bet_ts / created_at)
# can be parsed, the top-up is ALLOWED (with a debug log). T3 overall is +EV and
# must never be silently killed by a data gap; the gate targets one narrow cohort.
# Parsed by the shared ``_tristate_env`` (identical off/shadow/live surface as the
# stop slip-exempt + pre-POST drop guard gates above).
TOPUP_FRESH_GATE_MODE: str = _tristate_env("CRYPTOBOT_TOPUP_FRESH_GATE")

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


# -- Late-window entry skip gate (flag-gated, default {} = OFF) ----------------
#
# Per-coin cutoff (seconds before window close) below which a LIVE sub-minute
# entry is dropped as a validated -EV cohort. Format: comma list of
# COIN:SECONDS, e.g. "BTC:20" (skip BTC entries whose fill would land in the
# last 20s of the window) or "BTC:20,ETH:10". Coins upper-cased; SECONDS a
# non-negative int.
#
# Validated 2026-07-19 study: BTC entries in the last 20s of a window net
# +$34.93/era when SKIPPED (split-half validated in BOTH directions). The gate
# lives ONLY on the LIVE offset path (execution/pipeline.py _commit_cell) —
# DRY paper rows + minute-boundary cells are never affected (research parity).
def _parse_late_window_skip(raw: str) -> dict[str, int]:
    """Parse CRYPTOBOT_LATE_WINDOW_SKIP ("COIN:SECONDS,...") into a dict.

    Upper-cases coins; SILENTLY drops malformed tokens (missing colon, blank
    coin, non-int or negative seconds) so a single typo can neither wipe the
    whole map nor — worse — suppress a live order. Empty/blank -> {}. Mirrors
    the fail-open _COINS_ENV / _float_env idiom (silent fallback, no
    import-time raise).
    """
    out: dict[str, int] = {}
    for tok in (raw or "").split(","):
        tok = tok.strip()
        if not tok or ":" not in tok:
            continue  # blank or missing "COIN:SECONDS" separator -> ignore
        coin, _, secs = tok.partition(":")
        coin = coin.strip().upper()
        secs = secs.strip()
        if not coin:
            continue  # blank coin -> ignore
        try:
            val = int(secs)
        except ValueError:
            continue  # non-int seconds -> ignore this entry, keep the rest
        if val < 0:
            continue  # negative cutoff is meaningless -> ignore
        out[coin] = val
    return out


# ⚠️ DEFAULT "" => {} => byte-identical: the empty dict is falsy, so the
# pipeline predicate (_late_window_skip) short-circuits before any lookup and an
# unpinned deploy takes the exact existing path. Documented in
# polymarket/infra/cryptobot.env.example.
LATE_WINDOW_SKIP: dict[str, int] = _parse_late_window_skip(
    os.environ.get("CRYPTOBOT_LATE_WINDOW_SKIP", "")
)

# -- Early-window entry skip gate (operator order 2026-08-08, flip era) -------
# N > 0 => LIVE entries are allowed ONLY within the final N seconds of the
# window (seconds_to_close <= N); everything earlier skips with reason
# `early_window_skip` (same end-of-decision-path placement as the late gate, so
# the skipped cohort = exactly the bets the ungated bot would have placed —
# gradeable from signals). All coins, both intervals, seconds-based by operator
# instruction ("bet last 15s only"). Composes with LATE_WINDOW_SKIP: with
# LATE=5s and this=15, the live entry band is seconds-to-close 6..15.
# DEFAULT 0 => OFF => byte-identical path.
ENTRY_LAST_SECS_ONLY: int = int(_float_env("CRYPTOBOT_ENTRY_LAST_SECS_ONLY", 0))


# -- Ask-band entry skip gate (flag-gated, default () = OFF) ------------------
#
# Per-(coin, sleeve, ask-band) rules that drop an entry whose decision-time ask
# lands inside a band measured to have gone adverse. Format: comma list of
# COIN:SLEEVE:LO-HI, e.g. "SOL:subminute:0.65-0.85".
#
# Validated 2026-07-20 live-ledger study: SOL clean fills (slip >= -0.01) in the
# 0.65-0.85 marginal-favorite band broke with a sharp boundary at the 07-17
# halt — WR 87.3% -> 69.5%, +6.85c/$ -> -20.44c/$, +$38.77 -> -$15.26 (the
# single largest loss driver of the window) — while ETH (86.0 -> 88.4%) and BTC
# (84.2 -> 90.9%) HELD in the same band on the same days. Holding ask fixed,
# SOL's same-price WR fell 84.1% -> 61.0% vs ETH 81.4 -> 83.8%, so this is not
# a price-mix artifact. Damage is localized to SOL `subminute_5m` (n35, WR
# 68.6%) + `subminute_15m` (n6, WR 16.7%); SOL ask >= 0.85 (100% WR) is HEALTHY
# and must stay live — hence the gate is keyed on (coin, sleeve, band) rather
# than a blanket per-coin kill.
class AskBandSkipRule(NamedTuple):
    """One parsed CRYPTOBOT_ASK_BAND_SKIP rule.

    ``coin`` upper-cased; ``sleeve`` lower-cased and matched as a PREFIX of the
    sleeve label (so "subminute" covers both ``subminute_5m`` and
    ``subminute_15m``, while "subminute_5m" covers only itself) or ``"*"`` for
    every sleeve; ``lo``/``hi`` the ask band.
    """

    coin: str
    sleeve: str
    lo: float
    hi: float


def _parse_ask_band_skip(raw: str) -> tuple[AskBandSkipRule, ...]:
    """Parse CRYPTOBOT_ASK_BAND_SKIP ("COIN:SLEEVE:LO-HI,...") into rules.

    Mirrors the ``_parse_late_window_skip`` fail-open contract: a malformed rule
    is DROPPED (never raised, never widened) so one typo can neither wipe the
    spec nor — far worse — fail closed into "skip everything". Unlike that
    parser this one also WARNS on each dropped rule: a silently-ignored typo
    here would leave the operator believing a measured-adverse band is gated
    when it is in fact still trading real money.

    Empty/blank -> ``()`` (falsy => the pipeline predicate short-circuits).
    """
    def _drop(rule: str, why: str) -> None:
        logger.warning(
            "CRYPTOBOT_ASK_BAND_SKIP: ignoring rule %r (%s)", rule, why,
        )

    out: list[AskBandSkipRule] = []
    for tok in (raw or "").split(","):
        tok = tok.strip()
        if not tok:
            continue  # blank / trailing comma -> punctuation, not a typo
        parts = tok.split(":")
        if len(parts) != 3:
            _drop(tok, "expected COIN:SLEEVE:LO-HI")
            continue
        coin = parts[0].strip().upper()
        sleeve = parts[1].strip().lower()
        lo_raw, sep, hi_raw = parts[2].strip().partition("-")
        if not coin or not sleeve:
            _drop(tok, "blank coin/sleeve")
            continue
        if not sep:
            _drop(tok, "band must be LO-HI")
            continue
        try:
            lo, hi = float(lo_raw.strip()), float(hi_raw.strip())
        except ValueError:
            _drop(tok, "non-numeric band")
            continue
        # An inverted or out-of-[0,1] band is a typo, not a wider gate: drop it
        # rather than let e.g. "0.85-0.65" silently match nothing.
        if not (0.0 <= lo <= hi <= 1.0):
            _drop(tok, "need 0 <= LO <= HI <= 1")
            continue
        out.append(AskBandSkipRule(coin=coin, sleeve=sleeve, lo=lo, hi=hi))
    return tuple(out)


# ⚠️ DEFAULT "" => () => byte-identical: the empty tuple is falsy, so the
# pipeline predicate (_ask_band_skip) short-circuits before any comparison and
# an unpinned deploy takes the exact existing path. Documented in
# polymarket/infra/cryptobot.env.example.
ASK_BAND_SKIP: tuple[AskBandSkipRule, ...] = _parse_ask_band_skip(
    os.environ.get("CRYPTOBOT_ASK_BAND_SKIP", "")
)

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

# -- Fix 3a: pre-POST freshness recheck (flag-gated, default OFF) --------------
#
# The FAK limit is priced from the correct-side book read ONCE per tick in
# _resolve_net_cells Pass 2 and is never re-read before the order POST. Between
# that read and the POST the priced level can vanish (a maker pull, intra-tick
# serial Phase-B work, or a 700ms-old prefetch/WS frame), so the FAK crosses a
# book that is already gone and rejects (no_liquidity). When ENABLED,
# _commit_cell re-reads the correct-side token from the SAME shared WS/prefetch
# source (TickDeps.book_prefetch) immediately BEFORE reserving the LIVE order and,
# if a fresh re-walk (identical walk params to the decision walk) no longer
# covers MIN_COVER_FRAC of the proposed stake, SKIPS the POST: it writes a
# `prepost_recheck_skip` signal row, reserves nothing, POSTs nothing, and does
# NOT consume the window — the next qualifying second re-tries, exactly the
# existing per-second retry (CRYPTOBOT_V2_REJECT_RETRY). SKIP variant only.
#
# FAIL-OPEN: with no book source wired, or when the source has no fresh frame
# for the token, the order proceeds EXACTLY as today — a missing WS/prefetch
# frame must never suppress a live order. LIVE sub-minute (offset) cells only;
# DRY paper rows, minute-boundary cells, and the pair path are
# untouched.
#
# ⚠️ DEFAULT OFF = byte-identical runtime: the recheck block short-circuits
# before any book-source read, so an unpinned deploy takes the exact existing
# reserve->POST path. Validate first in SHADOW mode (recheck OFF + WS shadow on:
# CRYPTOBOT_BOOK_WS_ENABLED=1 + CRYPTOBOT_BOOK_WS_SHADOW=1) — each would-POST
# then counts + logs what the gate WOULD have decided vs the real fill/reject
# (true-positive = a would-skip that really rejected) without changing behavior.
# Enable-live gate: >=90% true-positive over >=3 days. Consumed by
# execution/pipeline.py; documented in polymarket/infra/cryptobot.env.example.
PREPOST_RECHECK_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_PREPOST_RECHECK_ENABLED", "0"
).strip().lower() in ("1", "true", "yes")
# Fresh-book coverage floor: skip the POST when the re-walked coverable notional
# is < proposed_bet_size * MIN_COVER_FRAC. 1.0 (default) = require the fresh book
# to still FULLY cover the sized stake; lower it (e.g. 0.5) to tolerate a
# partially-thinned book.
PREPOST_RECHECK_MIN_COVER_FRAC: float = _float_env(
    "CRYPTOBOT_PREPOST_RECHECK_MIN_COVER_FRAC", 1.0
)
# Deferred variant, deliberately NOT a knob (removed 2026-07-28, was
# PREPOST_RECHECK_REPRICE — defined but read by no code path): repricing the FAK
# limit to the fresh book instead of skipping is future work gated on the SKIP
# variant validating in shadow; it risks fill-price parity and needs a band
# re-check plus the _fak_padded_limit anchor cap. No config surface until built.

# -- Fix 3a (drop-guard extension): pre-POST TOXIC-drop guard (default OFF) -----
#
# The cover recheck above only catches the MISS direction (the priced level
# THINNED so the FAK rejects). This extension catches the complementary TOXIC
# direction: between the Pass-2 book read and the POST the fresh best ask has
# DROPPED well below the decision-time ask — the book is collapsing mid-flight
# and the FAK would fill BELOW the decision ask (a validated -EV "toxic" fill).
# The 2026-07-19 study puts the damage floor at 0.02 below the decision ask; the
# -0.02..-0.01 slip bucket is actually +EV, so the default thresh is 0.02, NOT
# 0.01 (do not block 1c wiggles).
#
# Tri-state, parsed to PREPOST_DROP_GUARD_MODE in {"off","shadow","live"}:
#   "0"      -> off    : byte-identical — the guard never reads a book.
#   "shadow" -> shadow : count-only — each would-abort is logged + classified
#                         against the real fill/reject (with decision + fresh ask
#                         for offline ledger grading) but NEVER suppresses.
#   "1"      -> live   : abort the POST when the fresh ask dropped >= thresh.
# Anything else -> off (fail-open). The cover guard takes precedence if BOTH
# fire on the same POST.
#
# ⚠️ DEFAULT "0" => "off" = byte-identical runtime: with the cover recheck AND
# WS shadow AND this guard all off, _prepost_recheck short-circuits before ANY
# book-source read. Shadow-first enable path: run "shadow" for >=3 days, grade
# the true-abort rate offline (join the logged decision+fresh ask to the
# ledger), then flip to "1". Consumed by execution/pipeline.py; documented in
# polymarket/infra/cryptobot.env.example. Parsed by the shared ``_tristate_env`` (identical
# off/shadow/live surface as the stop slip-exempt gate above).
PREPOST_DROP_GUARD_MODE: str = _tristate_env("CRYPTOBOT_PREPOST_DROP_GUARD")
# Minimum drop (decision_ask - fresh_ask, in price units) that trips the guard.
# 0.02 (default) = the validated damage floor; the -0.02..-0.01 slip bucket is
# actually +EV, so do NOT lower this below ~0.02 without re-validating.
PREPOST_DROP_GUARD_THRESH: float = _float_env(
    "CRYPTOBOT_PREPOST_DROP_GUARD_THRESH", 0.02
)

# -- Fix 3b: fast-requote for flicker rejects (flag-gated, default OFF) --------
#
# 3a (above) SKIPS a doomed POST when the priced level is already gone at
# recheck time; it cannot win the fill. 3b targets the complementary ~8%
# "flicker" bucket: a no_liquidity FAK reject where the book flickers BACK a
# few hundred ms later. On such a reject _process_live_post enqueues ONE bounded
# fast-requote for that (coin, interval, window_start_ts, side); a dedicated
# short-cadence scheduler job (main.py wires it on the HELD_PASS_CADENCE_MS
# dedicated-job pattern — its OWN connection + order_client) drains due requotes
# ~DELAY_MS after the reject: re-read the freshest book, re-run the sleeve BAND
# check on the fresh ask, re-derive walked_limit + the same anchored FAK pad,
# reserve a fresh attempt (new client_order_id; the uniq_loa_active_slot index
# still serializes any race → slot_full drop), POST ONCE, and run the standard
# accept/reject handling (a fill consumes the window via note_fill + the ledger
# row; a reject is terminal and arms NO second requote). NO inline sleep ever
# runs on the tick thread. Only no_liquidity rejects arm — never
# auth/market_closed/insufficient_funds/venue_unavailable. Never fires after
# window_end.
#
# ⚠️ DEFAULT OFF = byte-identical runtime: with CRYPTOBOT_FAST_REQUOTE_ENABLED
# unset _process_live_post enqueues nothing (the arm helper short-circuits before
# any queue write) and main.py registers NO drain job. Consumed by
# execution/pipeline.py; documented in polymarket/infra/cryptobot.env.example.
FAST_REQUOTE_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_FAST_REQUOTE_ENABLED", "0"
).strip().lower() in ("1", "true", "yes")
# Milliseconds to wait after a reject before the fast-requote fires. Drives BOTH
# the queued item's ready-delay AND the dedicated drain job's cadence.
FAST_REQUOTE_DELAY_MS: int = int(
    os.environ.get("CRYPTOBOT_FAST_REQUOTE_DELAY_MS", "400")
)
# Bound: at most this many fast-requotes per (coin, interval, window, side).
# 1 (default) = a single extra shot per side-window.
FAST_REQUOTE_MAX_ATTEMPTS: int = int(
    os.environ.get("CRYPTOBOT_FAST_REQUOTE_MAX_ATTEMPTS", "1")
)
# Adverse-dip gate: drop the requote (no POST) when the fresh top ask has
# DIPPED more than |MIN_DELTA| below the rejected limit. Ground truth
# (reject_requotes 07-11..07-21, first-reject dedup): dip < -0.02 cohort is
# -20c/$ (negative all 4 coins, 10/11 days) while [-0.02, -0.005) is +4.8c/$
# and the unchanged-book cohort +13..16c/$ — a cheap re-availability after a
# reject is adverse selection, not opportunity. -0.02 is the empirical sign
# crossover. Set to a large negative (e.g. -1.0) to disable the gate.
FAST_REQUOTE_MIN_DELTA: float = float(
    os.environ.get("CRYPTOBOT_FAST_REQUOTE_MIN_DELTA", "-0.02")
)
# Retry adverse-dip gate (operator 2026-07-24, DEFAULT OFF = byte-identical).
# The per-second FAK retry (V2_REJECT_RETRY) re-enters a rejected offset window
# with NO adverse-dip fence, so it fills the cheap-refill cohort that the
# fast-requote's FAST_REQUOTE_MIN_DELTA gate blocks — ground truth: that cohort
# is the -EV maker-pull "chase" (14 next-tick fills, -$6.26, era 07-21..07-24;
# worst losers filled well BELOW limit, e.g. BTC 0.58 vs limit 0.69). When ON,
# the entry path skips re-entry on a rejected offset window whose FRESH ask has
# dipped below rejected_limit + FAST_REQUOTE_MIN_DELTA (reuses that proven
# threshold); a flicker-back at/above the limit still fills (+EV cohort kept).
# OFF => the reject site records nothing and the entry gate short-circuits on an
# empty map, so runtime is byte-identical (backtest parity preserved). Consumed
# by execution/pipeline.py; documented in polymarket/infra/cryptobot.env.example.
RETRY_DIP_GATE_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_RETRY_DIP_GATE_ENABLED", "0"
).strip().lower() in ("1", "true", "yes")
# Drain-job cadence in ms, decoupled from the per-item ready delay above.
# Default = FAST_REQUOTE_DELAY_MS (byte-identical: cadence has always equalled
# the delay, making effective re-fire latency uniform delay..2*delay). Pin
# lower (e.g. 100) to tighten re-fire toward the delay itself.
FAST_REQUOTE_DRAIN_MS: int = int(
    os.environ.get("CRYPTOBOT_FAST_REQUOTE_DRAIN_MS", "0")
) or FAST_REQUOTE_DELAY_MS

# Binance market-data-only base URL recommended by Binance for public market
# data. Operators can switch to https://api.binance.com or api1-api4 if needed.
BINANCE_BASE_URL: str = os.environ.get(
    "CRYPTOBOT_BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

# -- Notify -------------------------------------------------------------------

CONSECUTIVE_FAILURE_THRESHOLD: int = 5
