"""Runtime settings for the live scanner.

Every value can be overridden by an environment variable of the same name with
a ``CRYPTOBOT_`` prefix; change it and restart the process.

Defaults match the deployed strategy config
``polymarket/live/config/twap_symmetric_live.json``. ``SHARED_EXPOSURE_CAP``
and ``MAX_DRAWDOWN_HALT_FRAC`` must equal that file's
``concurrent_exposure_cap_fraction`` and ``max_drawdown_halt_frac``, or the
loader's drift check refuses to boot. If you point ``CRYPTOBOT_V6_CONFIG`` at
another config, set ``CRYPTOBOT_SHARED_EXPOSURE_CAP`` and
``CRYPTOBOT_MAX_DD_HALT_FRAC`` to that file's values too.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def _resolve_repo_root() -> Path:
    """Project root, for both the local and the deployed layout.

    Locally the package lives at ``<root>/polymarket/src/cryptobot``; on the
    server deploy.sh flattens it to ``$PROJECT_DIR/src/cryptobot``. Guessing
    wrong would silently move the SQLite ledger, so the root is resolved as:

      1. ``CRYPTOBOT_ROOT`` if set (install_remote.sh sets it on the server),
      2. else the nearest parent holding ``pyproject.toml`` or
         ``polymarket/src/cryptobot`` (any local checkout),
      3. else ``parents[2]`` (the deployed layout, which ships neither marker).
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


# Repo root resolved from this file. Tests override paths through env vars.
_REPO_ROOT = _resolve_repo_root()

# All writable state lives under DATA_ROOT (databases, models) and LOGS_DIR.
# Both are env-overridable so a server can put them on another volume, and
# nothing depends on the working directory. They read os.environ directly
# because ``_path`` is defined further down.
DATA_ROOT: Path = Path(os.environ.get("CRYPTOBOT_DATA_ROOT") or (_REPO_ROOT / "data"))
LOGS_DIR: Path = Path(os.environ.get("CRYPTOBOT_LOGS_DIR") or (_REPO_ROOT / "logs"))

# How each shipped artifact was resolved, for the boot log: env_key -> (path,
# "env" | "runtime" | "repo" | "... (missing)").
ARTIFACT_RESOLUTION: dict[str, tuple[Path, str]] = {}


def _resolve_artifact(env_key: str, *candidates: tuple[str, Path]) -> Path:
    """Return the first candidate path that exists; an env override always
    wins.

    Candidates are ordered deployed copy first, then the repo copy. If none
    exists the last candidate is returned (and recorded as missing).
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

# One scanner tick per second.
TICK_SECONDS: int = int(os.environ.get("CRYPTOBOT_TICK_SECONDS", "1"))
TICK_SOFT_DEADLINE_MS: int = 900
TICK_HARD_DEADLINE_MS: int = 5000

# Seconds between PASSIVE WAL checkpoints on a background thread.
# db/connection.py raises wal_autocheckpoint to 20000 pages so SQLite never
# checkpoints inline on the tick's commit; this job drains the WAL instead.
# PASSIVE never blocks readers or writers, and journal_size_limit caps the WAL
# on disk. 0 disables the job, leaving the WAL to drain only at the inline
# threshold or on clean close.
WAL_CHECKPOINT_SECONDS: int = int(
    os.environ.get("CRYPTOBOT_WAL_CHECKPOINT_SECONDS", "60")
)

# Seconds between ``GET /ok`` pings on the CLOB signing connection pool (the
# httpx client the order POST uses). Between orders that pool's only traffic is
# the 60s balance read, against a 65s keepalive expiry; one dropped scheduler
# fire lets the socket expire, and the next order then pays DNS, TCP and a full
# TLS handshake. A 20s ping keeps the connection warm. The job only runs when
# an OrderClient exists and skips itself if the pool saw any response within
# ``CLOB_PING_SKIP_IF_ACTIVE_S``. 0 disables the job and the response hook that
# feeds the skip check.
CLOB_PING_SECONDS: int = int(os.environ.get("CRYPTOBOT_CLOB_PING_SECONDS", "20"))
# Skip window and per-request timeout for the ping, in seconds. The 40s window
# keeps the ping away from active trading, so it never holds the one warm
# connection when an order needs it; the 20s cadence still lands inside the 65s
# keepalive. The 3s timeout matches the pool's connect timeout.
CLOB_PING_SKIP_IF_ACTIVE_S: float = 40.0
CLOB_PING_TIMEOUT_S: float = 3.0

# Read/write/pool timeout (seconds) for the shared spot, Gamma and CLOB read
# clients built in main.py. Lower it to make a hung venue read fail fast inside
# the 900ms tick budget. The connect timeout is fixed at 2.0s in main.py.
DECISION_READ_TIMEOUT_S: float = _float_env(
    "CRYPTOBOT_DECISION_READ_TIMEOUT_S", 3.0
)

# Parallel order POST. When on and two or more cells reach the order stage in
# one tick, their order POSTs run concurrently on a short-lived thread pool
# instead of one after another. The venue's ~250ms taker delay cannot be
# avoided, so N serial POSTs cost ~N*250ms while a fan-out costs ~250ms.
#
# Everything before the POST (gates, book walk, sizing, attempt reservation)
# stays serial in cell order, and results are processed serially in the
# original order; only the network call is parallel. One side effect: a
# reserved cell counts its stake against this tick's exposure when it is
# reserved, so a POST that is later rejected still used up headroom. That is
# conservative and can never over-expose. Off by default.
PHASE_B_PARALLEL_POST: bool = os.environ.get(
    "CRYPTOBOT_PHASE_B_PARALLEL_POST", "0"
).strip().lower() in ("1", "true", "yes")

# -- Walk-book + sizing -------------------------------------------------------

# Absolute price added to top_ask when walking the book (dollars, not a
# percentage).
MAX_WALK_PRICE: float = 0.05

# Bankroll the strategy config was built against. Stake fractions and config
# fingerprints are relative to this $100 base.
BACKTEST_BANKROLL: float = 100.0

# Live accounting seed. Sizing scales from realized capital (this plus PnL
# since the cutover), so the scanner can be anchored to the actual funded
# balance without editing the strategy config. ``INITIAL_BANKROLL`` is still
# read as an alias; the ``CRYPTOBOT_`` name wins.
INITIAL_BANKROLL: float = _float_env(
    "CRYPTOBOT_INITIAL_BANKROLL",
    BACKTEST_BANKROLL,
    legacy_env_key="INITIAL_BANKROLL",
)
if INITIAL_BANKROLL <= 0:
    raise ValueError("CRYPTOBOT_INITIAL_BANKROLL must be > 0")

# Start wallet accounting from this timestamp without deleting older ledger
# rows. Empty means use the whole ledger.
WALLET_START_AT: str = os.environ.get("CRYPTOBOT_WALLET_START_AT", "").strip()

# Global stake multiplier. Stake fractions are set per sleeve in the strategy
# config, so this stays at 1.0.
BET_SIZE_MULTIPLIER: float = float(os.environ.get("CRYPTOBOT_BET_SIZE_MULTIPLIER", "1.0"))

# Smallest stake the bot will place, in USD.
MIN_BET_USD: float = float(os.environ.get("CRYPTOBOT_MIN_BET_USD", "1.0"))
CLOSE_SIZE_REL_TOLERANCE: float = 1e-6

# Polymarket's published peak taker fee for the crypto category: fee_per_share
# = theta * p * (1 - p). Must match POLY_FEE_THETA in
# ``backtest/engine/sweep_lib.py`` and ``strategy/parity.py``;
# tests/test_audit_parity.py fails on any drift.
POLY_FEE_THETA: float = 0.07

# Fee rate used when Gamma returns ``feeRateBps=None``, which it does for every
# *-updown-* market. PnL math uses POLY_FEE_THETA regardless; this only needs
# to be a positive value to pass the fee-presence check. 70 bps = theta *
# 10000.
POLY_DEFAULT_FEE_RATE_BPS: float = 70.0

# Calibration multiplier on the entry taker fee, applied only when realized
# pnl_net is booked (execution.grade_fees). Decision math (edge gates, book
# walker, strategy.parity, spec_v2) uses the raw published theta, so it does
# not change which bets are taken.
#
# A flat-to-flat wallet audit (no open positions at either end) found the venue
# charged 0.06048 * p * (1 - p) per entry fill, i.e. 0.864 of the published
# theta = 0.07. Booking the full parabola on every fill would overstate fees by
# ~14%. Re-measure the same way and retune with this one env var.
POLY_FEE_CALIBRATION: float = _float_env("CRYPTOBOT_POLY_FEE_CALIBRATION", 0.864)

# Maximum fraction of equity that may be at risk across open positions at once.
# The strategy config can set a per-wallet value; this is the fallback. This
# constant, the config's ``concurrent_exposure_cap_fraction`` and the server's
# ``CRYPTOBOT_SHARED_EXPOSURE_CAP`` must agree, or the loader refuses to boot.
SHARED_EXPOSURE_CAP: float = float(os.environ.get(
    "CRYPTOBOT_SHARED_EXPOSURE_CAP",
    "1.0",
))

# Hard trading halt, measured as drawdown from peak equity. With no graduated
# size throttle (V2_BRAKE_FLOOR = 1.0) this is the only drawdown guard. This
# constant, the config's ``max_drawdown_halt_frac`` and the server's
# ``CRYPTOBOT_MAX_DD_HALT_FRAC`` must agree, or the loader refuses to boot.
MAX_DRAWDOWN_HALT_FRAC: float = float(os.environ.get(
    "CRYPTOBOT_MAX_DD_HALT_FRAC",
    "0.40",
))

# Exposure ratio at which the dashboard tile turns amber.
EXPOSURE_WARN_RATIO: float = 0.80

# -- Add-on / top-up control --------------------------------------------------

# Allows top-up orders on a market already bet. install_remote.sh pins it to 0
# so live trading places one order per market.
ENABLE_TOPUPS: bool = os.environ.get("CRYPTOBOT_ENABLE_TOPUPS", "1") == "1"

# PENDING rows older than this are ignored by slot_state; the reconcile pass
# settles them.
MAX_PENDING_AGE_MINUTES: int = 300

# -- Resolution sweep cadence -------------------------------------------------

# Minutes between resolution sweeps. Lower values settle PnL sooner at the cost
# of more Gamma calls.
RESOLUTION_SWEEP_MINUTES: int = int(os.environ.get("CRYPTOBOT_SWEEP_MINUTES", "5"))

# A live PENDING row whose market cannot be resolved (404, HTTP error, bad
# payload) keeps its stake inside the exposure cap. After this many seconds
# past window_end the sweep raises it to CRITICAL so an operator settles or
# cancels it by hand. Six hours lets a transient Gamma outage clear first and
# still surfaces stranded capital the same day.
MAX_PENDING_SETTLE_AGE_S: int = int(
    os.environ.get("CRYPTOBOT_MAX_PENDING_SETTLE_AGE_S", str(6 * 3600))
)

# -- Resolution sanity bounds -------------------------------------------------

# Per-coin (lo, hi) bounds on the resolution price; anything outside is
# CANCELLED rather than settled.
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
# Strategy config. The env name ``CRYPTOBOT_V6_CONFIG`` is kept for
# compatibility with the server's /etc/cryptobot.env. The deployed copy under
# runtime/ wins over the repo copy.
V6_CONFIG_PATH: Path = _resolve_artifact(
    "CRYPTOBOT_V6_CONFIG",
    ("runtime", _REPO_ROOT / "runtime" / "config" / "twap_symmetric_live.json"),
    ("repo", _REPO_ROOT / "polymarket" / "live" / "config" / "twap_symmetric_live.json"),
)
# The TWAP model: one HistGradientBoosting classifier per interval, trained on
# Chainlink 60-second TWAP settlement labels (polymarket/backtest/training/
# export_twap_symmetric_60s.py).
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
# Dashboard assets live at ``polymarket/dashboard`` locally and at
# ``$PROJECT_DIR/dashboard`` on the server (install_remote.sh also sets
# CRYPTOBOT_DASHBOARD_DIR there).
_DASHBOARD_DEFAULT = _REPO_ROOT / "polymarket" / "dashboard"
if not _DASHBOARD_DEFAULT.is_dir():
    _DASHBOARD_DEFAULT = _REPO_ROOT / "dashboard"
DASHBOARD_DIR: Path = _path("CRYPTOBOT_DASHBOARD_DIR", _DASHBOARD_DEFAULT)

# -- Dashboard / SSE server ---------------------------------------------------

DASHBOARD_HTTP_PORT: int = int(os.environ.get("CRYPTOBOT_DASHBOARD_PORT", "8800"))

# Never bind to 0.0.0.0 by default. A non-loopback bind needs an explicit
# override, which main.py checks at boot.
DASHBOARD_BIND_HOST: str = os.environ.get("CRYPTOBOT_DASHBOARD_HOST", "127.0.0.1")

MAX_SSE_CLIENTS: int = 10

# Per-client write deadline. A slow client is skipped rather than blocking the
# broadcast thread on TCP backpressure.
SSE_CLIENT_WRITE_TIMEOUT_S: float = 2.0

# Deadline for reading a request line and headers. Stops a slowloris client
# from holding a worker thread before authentication.
SSE_HEADER_READ_TIMEOUT_S: float = 15.0

# Ledger rows sent in the initial snapshot and in each delta. 500 rows (~0.4 MB
# once, over the SSH tunnel) covers the full deployed history so the Journal's
# "All" filter and the client-side analytics see every bet.
LEDGER_BOOTSTRAP_LIMIT: int = 500
LEDGER_DELTA_LIMIT: int = 500
# Recent bets and scored rejects streamed per SSE tick for the journal tab. The
# tab paginates client-side, so the DOM stays small regardless.
V2_BETS_LIMIT: int = 500
V2_REJECTS_LIMIT: int = 500
# Maximum points in the lifetime equity series (downsampled; first and last
# points always kept), so the equity curve covers the whole history with a
# bounded payload.
LEDGER_EQUITY_MAX_POINTS: int = 240

# Must be "1" to bind the dashboard beyond loopback; otherwise boot fails.
ALLOW_REMOTE_DASHBOARD: bool = os.environ.get(
    "CRYPTOBOT_ALLOW_REMOTE_DASHBOARD", "0"
) == "1"

# A non-loopback bind must also declare TLS termination. Without TLS the
# operator session cookie would travel in cleartext, and sse_server only sets
# the cookie's Secure flag when this is on. Boot check and cookie read the same
# env var so they cannot disagree.
DASHBOARD_TLS_TERMINATED: bool = os.environ.get(
    "CRYPTOBOT_DASHBOARD_TLS_TERMINATED", "0"
) == "1"

# Days of signals rows to keep. Older rows are trimmed on every resolution
# sweep; at 1 Hz the table would otherwise fill the disk within weeks.
SIGNALS_RETENTION_DAYS: int = int(os.environ.get("CRYPTOBOT_SIGNALS_RETENTION_DAYS", "7"))

# Days of pipeline_health rows to keep (the per-tick latency traces make it
# grow quickly). Trimmed on the same sweep.
PIPELINE_HEALTH_RETENTION_DAYS: int = int(
    os.environ.get("CRYPTOBOT_PIPELINE_HEALTH_RETENTION_DAYS", "14")
)

# -- Sub-minute entries -------------------------------------------------------
#
# The live sleeves enter on a per-second grid inside each market's final
# window. SUBMINUTE_ENABLED is the master flag: when off, the loader drops the
# sub-minute sleeves and their wallet group entirely, and the bot places
# nothing. Off by default; the live deploy turns it on.
SUBMINUTE_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_ENABLED", "0"
) == "1"
# Independent kill switch, checked right before every sub-minute order. Blocks
# placement regardless of any other setting.
SUBMINUTE_KILL: bool = os.environ.get("CRYPTOBOT_SUBMINUTE_KILL", "0") == "1"
# When on, a sleeve places at most one entry per market and side. Turning it
# off lets every qualifying second stack its own bet, which doubles exposure on
# a losing market with no diversification.
SUBMINUTE_ONE_PER_WINDOW: bool = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_ONE_PER_WINDOW", "1"
) == "1"
# Master switch for 5m markets. When off, no 5m sleeve opens a new entry: the
# tick skips 5m sleeves silently (no signal rows, no API calls) and the
# decision path refuses defensively.
#
# This only gates entries; the sleeve stays loaded, so open 5m positions still
# settle normally and the dashboard keeps showing 5m history.
INTERVAL_5M_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_INTERVAL_5M_ENABLED", "1"
) == "1"
# Fill-rate breaker: over the last BREAKER_WINDOW live sub-minute orders, trip
# when n >= BREAKER_MIN_ORDERS and filled / requested < BREAKER_MIN_FILL_RATE.
# Stale quotes show up as FAK orders that come back unfilled or partly filled
# after the 250ms taker delay.
SUBMINUTE_BREAKER_WINDOW: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_WINDOW", "20")
)
SUBMINUTE_BREAKER_MIN_ORDERS: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_MIN_ORDERS", "10")
)
SUBMINUTE_BREAKER_MIN_FILL_RATE: float = float(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_MIN_FILL_RATE", "0.5")
)
# Realized-EV breaker: with at least BREAKER_EV_MIN_SETTLED settled sub-minute
# bets in the window, trip when total pnl_net < -(BREAKER_EV_MAX_LOSS_FRAC *
# total stake), i.e. a loss well beyond fee noise.
SUBMINUTE_BREAKER_EV_MIN_SETTLED: int = int(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_EV_MIN_SETTLED", "30")
)
SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC: float = float(
    os.environ.get("CRYPTOBOT_SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC", "0.15")
)
# Breaker reset: an ISO-8601 UTC timestamp ("2026-07-03T00:00:00"). Only orders
# created after it count toward the breaker. Set it after investigating a trip;
# empty means use the full history.
SUBMINUTE_BREAKER_RESET: str = os.environ.get(
    "CRYPTOBOT_SUBMINUTE_BREAKER_RESET", ""
).strip()

# -- Equity-fraction sizing ---------------------------------------------------
#
# Each sub-minute bet is sized off live equity:
#
#   stake_usd = equity * base_frac(interval) * brake(dd) * conc_weight(k)
#
# raised to MIN_BET_USD if smaller. ``dd`` is the drawdown from peak
# (capital.py drawdown_pct_peak_frac), brake = max(V2_BRAKE_FLOOR, 1 - dd /
# V2_BRAKE_SCALE) and conc_weight = 1/sqrt(k) for k same-minute candidates.
# Both intervals use a flat 1% of equity. Drawdown grows roughly linearly with
# size, so the base fraction is the risk budget; keep it in step with the
# strategy config's capital_frac and the install_remote.sh pin.
V2_BASE_FRAC_15M: float = _float_env("CRYPTOBOT_V2_BASE_FRAC_15M", 0.010)
V2_BASE_FRAC_5M: float = _float_env("CRYPTOBOT_V2_BASE_FRAC_5M", 0.010)
# Drawdown brake: multiplier = max(V2_BRAKE_FLOOR, 1 - dd / V2_BRAKE_SCALE).
# FLOOR = 1.0 pins the multiplier at 1.0 for every drawdown, which turns the
# throttle off; the hard halt above is the drawdown guard instead. SCALE is
# kept at 0.60 so the throttle can be re-armed by changing FLOOR alone.
V2_BRAKE_SCALE: float = _float_env("CRYPTOBOT_V2_BRAKE_SCALE", 0.60)
V2_BRAKE_FLOOR: float = _float_env("CRYPTOBOT_V2_BRAKE_FLOOR", 1.0)
# When on (default), scale each same-minute candidate's stake by 1/sqrt(k). Set
# to "0" to size on base_frac * brake alone.
V2_CONC_DOWNWEIGHT: bool = os.environ.get(
    "CRYPTOBOT_V2_CONC_DOWNWEIGHT", "1"
) == "1"
# Reject retry: when a live sub-minute FAK order is rejected, the market stays
# open for that sleeve, so a later qualifying second can try again on a fresh
# book and a fresh score. A filled retry uses up the market just like a filled
# first attempt. Read by execution/pipeline.py at the reject site.
V2_REJECT_RETRY: bool = os.environ.get(
    "CRYPTOBOT_V2_REJECT_RETRY", "1"
) == "1"

# -- Anchored FAK pad ---------------------------------------------------------
#
# Raises the limit price of every live sub-minute FAK buy by PAD dollars, so
# the order still crosses a book that ticked up between scoring and signing,
# without chasing a book that keeps running:
#
#     padded_limit = min(walked_limit + PAD, first_limit + PAD), capped at 0.99
#
# ``first_limit`` is the walked limit of the first attempt for that (sleeve,
# coin, interval, window, side), held in memory on the WindowDecider. A later
# retry can therefore never pay more than the first attempt's cap plus PAD. The
# stake is a cash amount, so a higher fill price buys fewer shares.
#
# Only the price sent to the venue changes; band, gate and selection logic
# always read the observed ask. The padded value is what ledger.limit_price and
# subminute_fills.limit_price record. 0.0 (default) turns the pad off.
V2_FAK_PAD: float = _float_env("CRYPTOBOT_V2_FAK_PAD", 0.0)

# -- Retry reprice ------------------------------------------------------------
#
# With the anchored pad, attempts 2..N are capped at the first attempt's price.
# When this is on, each retry pays its own fresh walked_limit + PAD instead.
# Attempt 1 is the same either way.
#
# Research did not show this helps: replays came out negative in one period and
# inconclusive in another. Every entry gate still applies to a retry before its
# price is formed, so it cannot chase without bound:
#
#   * the band gate: a second only qualifies while the ask is inside the band;
#   * the edge gate, re-scored at that second's ask;
#   * the book walker's own min_edge and MAX_WALK_PRICE ceiling;
#   * V2_EDGE_CAP on claimed edge.
#
# Off by default. Evaluate it on subminute_fills rows with attempt > 1.
V2_RETRY_REPRICE: bool = os.environ.get(
    "CRYPTOBOT_V2_RETRY_REPRICE", "0"
) == "1"

# -- Miss forensics -----------------------------------------------------------
#
# Classifies every rejected FAK order from on-chain evidence: did a faster
# taker take the liquidity, or did the maker cancel? Re-reading the book a
# second after the reject cannot tell these apart, because the ask is gone
# either way; the trade tape can. A scheduler job walks each new
# ``no_liquidity`` reject and writes a verdict to ``miss_forensics`` (method in
# scheduler/miss_forensics.py).
#
# The job never touches the decision path: it reads subminute_fills and
# live_order_attempts, writes only miss_forensics, logs and swallows every
# exception, and caps HTTP/RPC calls per run so a venue outage costs a bounded
# number of calls. Needs POLY_FUNDER (to exclude our own trades);
# POLYGON_RPC_URL is optional and adds the rival maker's address to a
# FASTER_TAKER verdict. Set CRYPTOBOT_MISS_FORENSICS_ENABLED=0 to skip
# registering the job.
MISS_FORENSICS_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_MISS_FORENSICS_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")
MISS_FORENSICS_SECONDS: int = int(
    os.environ.get("CRYPTOBOT_MISS_FORENSICS_SECONDS", "300")
)

# -- Fast trigger: sub-second entry scans -------------------------------------
#
# On-chain forensics showed most misses were taken by a faster taker about a
# second before our order arrived, at our quoted ask. Signing and POSTing are
# already at the venue floor (the 250ms taker delay applies to everyone), so
# the latency left to cut is between a fillable ask appearing and our decision.
# A 1 Hz tick sees an ask that appears mid-second ~500ms late on average.
#
# Two extra triggers run the same single-cell evaluation as the tick
# (execution.pipeline.run_fast_entry_scan, which uses the same scoring and
# ``_commit_cell`` and therefore every gate, cap and dedup check):
#
#   * SWEEP: a scheduler job every FAST_TRIGGER_MS (default 250ms). Outside a
#     qualifying entry window it returns after a few integer comparisons.
#     Average sampling delay drops from ~500ms to ~125ms.
#   * EVENT: the WS book mirror calls back on ask-side updates and a
#     coalescing worker thread runs the scan within ~20ms. It has its own
#     switch (FAST_TRIGGER_EVENT_ENABLED) because it runs off the WS reader.
#
# Running alongside the 1 Hz tick cannot double-place: the WindowDecider never
# places the same offset second twice, ``subminute_slot_seen`` is the durable
# backstop, and the ``uniq_loa_active_slot`` index turns any remaining race
# into a slot_full drop. Fast scans and the tick's order phase also share one
# lock; a fast scan that finds it held simply skips.
#
# Off (CRYPTOBOT_FAST_TRIGGER_ENABLED=0): no job, no WS callback, no lock.
FAST_TRIGGER_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_FAST_TRIGGER_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")
# Sweep cadence, clamped to >= 100ms by scheduler.fast_entry.
# fast_trigger_seconds; below that the guard costs more than it saves.
FAST_TRIGGER_MS: int = int(os.environ.get("CRYPTOBOT_FAST_TRIGGER_MS", "250"))
# Event trigger switch, independent of the sweep. Off means the callback is
# never registered and no worker thread starts.
FAST_TRIGGER_EVENT_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_FAST_TRIGGER_EVENT_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")

# -- Top-up edge floor --------------------------------------------------------
#
# Skips a top-up whose recomputed edge is below this floor (strictly less). The
# top-up paths re-price their edge but otherwise only enforce the V2_EDGE_CAP
# ceiling, so without this a top-up whose edge had turned negative could still
# be placed. Within-minute top-ups (execution/gates.py) use the decision edge
# the gate stack already evaluates; the skip reason is
# ``topup_edge_below_floor``.
#
# The default 0.0 is active: it blocks negative-edge top-ups and nothing else.
# First entries are never affected.
TOPUP_EDGE_FLOOR: float = _float_env("CRYPTOBOT_TOPUP_EDGE_FLOOR", 0.0)

# -- Coin universe ------------------------------------------------------------

# CRYPTOBOT_COINS (comma-separated) sets the scan universe explicitly; the live
# deploy pins BTC,ETH,SOL,XRP. When unset, the default is BTC, ETH and XRP,
# plus SOL when sub-minute sleeves are enabled.
_COINS_ENV = os.environ.get("CRYPTOBOT_COINS", "").strip()
if _COINS_ENV:
    COINS: tuple[str, ...] = tuple(
        c.strip().upper() for c in _COINS_ENV.split(",") if c.strip()
    )
else:
    COINS = ("BTC", "ETH", "XRP") + (("SOL",) if SUBMINUTE_ENABLED else ())


# -- Spot source and data freshness -------------------------------------------

# Live spot source. ``binance`` is the only supported value, because the model
# features were built from Binance klines. The setting exists so main.py fails
# loudly on anything else.
SPOT_SOURCE: str = os.environ.get("CRYPTOBOT_SPOT_SOURCE", "binance").strip().lower()

# Binance's public endpoints have per-IP request weights, so one batched spot
# response is reused for this many seconds.
SPOT_FETCH_INTERVAL_SECONDS: int = int(os.environ.get(
    "CRYPTOBOT_SPOT_FETCH_INTERVAL_SECONDS", "1"
))

# Use Binance 1-second kline closes, the same field the backtest uses. The
# 24h-ticker endpoint is cheaper but returns the last trade, not the kline
# close. Off locally; the deploy turns it on.
SPOT_USE_1S_KLINES: bool = os.environ.get(
    "CRYPTOBOT_SPOT_USE_1S_KLINES", "0"
).strip().lower() in {"1", "true", "yes", "on"}

# Small buffer above the fetch interval so a quote published just before the
# bucket boundary is not rejected on the last cached tick.
SPOT_STALENESS_S: float = float(os.environ.get(
    "CRYPTOBOT_SPOT_STALENESS_S",
    str(max(5.0, float(SPOT_FETCH_INTERVAL_SECONDS) + 2.0)),
))

# Spot prefetch: a daemon thread fetches the spot batch LEAD_MS before each
# second boundary into the cache the tick reads, so the tick gets a cache hit
# instead of a 50-100ms fetch. The data is at most LEAD_MS older than an inline
# fetch would return. Set ENABLED=false to fetch inline.
SPOT_PREFETCH_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_SPOT_PREFETCH_ENABLED", "true"
).strip().lower() in ("1", "true", "yes")
SPOT_PREFETCH_LEAD_MS: int = int(os.environ.get(
    "CRYPTOBOT_SPOT_PREFETCH_LEAD_MS", "200"
))

# Book prefetch: a daemon thread re-fetches ``POST /books`` for the tokens the
# previous tick used, LEAD_MS before each second boundary, and the tick reads
# from that cache instead of paying a 30-50ms round trip. A cached book is only
# used if younger than MAX_AGE_MS at decision time; anything else falls through
# to the inline batch. Off by default.
BOOK_PREFETCH_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_PREFETCH_ENABLED", "false"
).strip().lower() in ("1", "true", "yes")
BOOK_PREFETCH_LEAD_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_PREFETCH_LEAD_MS", "150"
))
BOOK_PREFETCH_MAX_AGE_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_PREFETCH_MAX_AGE_MS", "700"
))

# WS book mirror: a daemon thread mirrors Polymarket's CLOB market WebSocket
# into a cache the tick reads before the inline ``POST /books`` batch. WS books
# carry a venue timestamp, so a book's age is real and a book is only served if
# younger than MAX_AGE_MS. On any WS error or disconnect the mirror serves
# nothing and the tick uses the REST batch. Off by default; the live deploy
# keeps it off because its books were seconds stale with no latency saved.
BOOK_WS_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_ENABLED", "false"
).strip().lower() in ("1", "true", "yes")
BOOK_WS_MAX_AGE_MS: int = int(os.environ.get(
    # Same freshness bound as the REST prefetch: a book older than ~700ms can
    # price a limit that FAK-rejects by the time it is sent.
    "CRYPTOBOT_BOOK_WS_MAX_AGE_MS", "700"
))
BOOK_WS_URL: str = os.environ.get(
    "CRYPTOBOT_BOOK_WS_URL",
    "wss://ws-subscriptions-clob.polymarket.com/ws/market",
).strip()
# Shadow mode: with BOOK_WS_ENABLED and this both on, the mirror runs but never
# drives a decision. Each tick compares its book with the REST book for the
# same token (top-ask difference, relative depth difference) and logs
# divergences, rate-limited. Off by default.
BOOK_WS_SHADOW: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_SHADOW", "false"
).strip().lower() in ("1", "true", "yes")
# -- WS stale-token eviction --------------------------------------------------
#
# The venue pushes ~800 msg/s, yet mirrored books were measured seconds old.
# Cache entries for tokens that had rolled out of the subscription were never
# evicted, and get() only checked connection-level liveness, which any frame
# refreshes. Eviction (default on) can only remove stale data; the fallback is
# the REST batch. Set 0 to disable.
BOOK_WS_EVICT_UNSUBSCRIBED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_EVICT_UNSUBSCRIBED", "true"
).strip().lower() in ("1", "true", "yes")
# Per-token book age limit in ms; 0 turns it off. This is a safety net, not the
# fix: with eviction on it rarely fires. Do not set it sub-second. The tick
# time ``now_ts`` is a whole second, so the computed age can be off by up to
# ~1000ms (and even go negative); fix that before arming this below ~2000ms.
BOOK_WS_MAX_BOOK_AGE_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_MAX_BOOK_AGE_MS", "0"
))

# -- WS stall watchdog --------------------------------------------------------
#
# A book's age mostly reflects how quiet the market is, not feed lag, so the
# live deploy sets no age cap. That leaves one gap: a subscription that stays
# connected (pings keep the connection marked live) while updates for one token
# silently stop, which would price a dead ladder until the window rolls. A
# dropped connection already clears the mirror and rolled-off tokens are
# evicted; this watchdog covers the rest.
#
# It runs only on the scheduler pool, never on the tick or order path. It
# cross-checks only suspect tokens against REST and never evicts on a failed
# check. On by default, but it only runs when the mirror is authoritative
# (BOOK_WS_ENABLED and not SHADOW). With ENABLED=0 nothing is built and no job
# is registered.
BOOK_WS_WATCHDOG_ENABLED: bool = os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_ENABLED", "true"
).strip().lower() in ("1", "true", "yes")
# Seconds between cross-checks. Each pass is one batch ``POST /books`` (12 per
# minute at 5s), covering at most MAX_TOKENS_PER_PASS tokens.
BOOK_WS_WATCHDOG_INTERVAL_S: float = float(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_INTERVAL_S", "5"
))
# A book with no update for this long is a suspect worth one REST check. Keep
# it well above the normal inter-frame gap and well below a window's length.
BOOK_WS_WATCHDOG_STALL_SUSPECT_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_STALL_SUSPECT_MS", "5000"
))
# Maximum tokens checked per pass, oldest first, to keep the response and the
# comparison work small.
BOOK_WS_WATCHDOG_MAX_TOKENS_PER_PASS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_MAX_TOKENS_PER_PASS", "4"
))
# A token that matched REST is not checked again until this long has passed (or
# it updates and goes quiet again). Without this a quiet but correct book would
# be re-fetched every pass.
BOOK_WS_WATCHDOG_VERIFY_TTL_MS: int = int(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_VERIFY_TTL_MS", "30000"
))
# Top-of-book check: price must match exactly (prices move in whole cents);
# size may differ by this relative tolerance because the batch and delta feeds
# round sizes differently.
BOOK_WS_WATCHDOG_SIZE_REL_TOL: float = float(os.environ.get(
    "CRYPTOBOT_BOOK_WS_WATCHDOG_SIZE_REL_TOL", "0.02"
))

# Binance's market-data-only base URL. api.binance.com or api1-api4 also work.
BINANCE_BASE_URL: str = os.environ.get(
    "CRYPTOBOT_BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

# -- Notify -------------------------------------------------------------------

CONSECUTIVE_FAILURE_THRESHOLD: int = 5
