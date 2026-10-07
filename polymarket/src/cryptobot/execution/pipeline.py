"""Tick orchestrator — one F1 tick end-to-end.

`run_scanner_tick(conn, sleeves, now_ts, ...)` produces:
  - zero or more PENDING ledger rows (initial fills + add-on top-ups)
  - one `signals` row per evaluated `(coin, sleeve, entry_minute, side)`
  - one `TickResult` for the SSE snapshot builder

Two-phase loop (D5):
  Phase A — score all (coin × sleeve at this minute); produce per-cell
            (model_p, edge, side) decisions. NO ledger writes yet.
  Phase B — apply min_agreement cross-coin gate to Phase A's full set,
            then per-survivor: slot_state → gates → walk_book → record_bet
            inside a BEGIN IMMEDIATE slot lock so concurrent ticks
            evaluating the same slot are serialized.

D10 market-level dedup: tracks per (coin, sleeve, side, window_start_ts)
whether any initial fill has already landed. The 15m sleeve sees both
e=7 and e=13 within one market window; the backtest treats them as ONE
qualifying bet — we suppress subsequent initial fills, but top-ups
within the slot that already fired are unaffected.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace as dataclass_replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional
from zoneinfo import ZoneInfo

from cryptobot import config
from cryptobot.venues.fees import fee_per_share
from cryptobot.venues.types import BookSnapshot
from cryptobot.execution.capital import (
    GroupCapState,
    WalletSnapshot,
    get_group_state,
    get_wallet_snapshots,
    ledger_version,
    wallet_id_for_sleeve,
)
from cryptobot.execution.fill_rate_breaker import (
    BreakerState,
    evaluate_breaker,
    record_subminute_fill,
)
from cryptobot.execution.gates import GateInput, evaluate_gates
from cryptobot.execution.ledger import (
    BetSignal,
    claim_subminute_scan,
    record_bet,
    record_signal_skip,
    record_signal_would_bet,
    slot_state,
    subminute_market_other_sleeve,
    subminute_slot_seen,
    subminute_window_existing_side,
    subminute_window_state,
    utc_now_iso,
)
from cryptobot.execution.order_attempts import (
    bump_placement_epoch,
    link_attempt_to_ledger,
    mark_attempt_failed,
    mark_attempt_submitted,
    placement_epoch,
    reserve_live_order_attempt,
)
from cryptobot.execution.operator_control import (
    LIVE,
    TRANSFER_LOCK,
    OperatorState,
    get_operator_state,
    load_wallet_mode_state,
)
from cryptobot.execution.strategy_constants import (
    LIVE_BALANCE_TOLERANCE_USD,
    LIVE_READINESS_CACHE_TTL_S,
)
from cryptobot.execution.walk_book import walk_book_edge_preserving
from cryptobot.execution import sizing_v2
from cryptobot.strategy import spec_v2
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.strategy.score import ScoreResult, score_cell_twap
from cryptobot.strategy.portfolio_loader import Sleeve, V6Portfolio

if TYPE_CHECKING:  # avoid runtime import cycle but keep type information
    from cryptobot.strategy.hgb_loader import HgbArtifact

logger = logging.getLogger(__name__)
_ET = ZoneInfo("America/New_York")


def _venue_market_cache_has(
    coin: str, interval_min: int, window_start_ts: int
) -> bool:
    """Whether market metadata for this window is already cached."""
    from cryptobot.venues import get_adapter

    return get_adapter(config.VENUE).market_cache_has(
        coin, interval_min, window_start_ts
    )

# v2 step1last live decision state machine. The pipeline is a per-tick FUNCTION
# (no long-lived orchestrator object), so the "commit the window at the first
# in-band second and gate THAT second" rule (spec_v2.WindowDecider) needs a home
# that survives across the 1 Hz ticks of one process. This module-global is that
# home: it persists for the process lifetime and resets on reboot. The durable
# DB dedup (subminute_window_* / ledger) remains the hard guard against
# double-betting an already-PLACED window; the decider adds the R1 refinement
# that a gate-FAILED first in-band second still consumes the window (so a later,
# higher-edge second in the same window does not sneak a bet in — this is the
# n=2366 < n=2503 reachability shortfall the parity gate pins).
# consume-on-fill is the live default (config.V2_REJECT_RETRY=1, pinned). The
# mode is re-synced from the flag each tick at the consult site so the flag is a
# true master switch (=0 restores first-commit / one-attempt-per-window).
_V2_WINDOW_DECIDER = spec_v2.WindowDecider(consume_on_fill=config.V2_REJECT_RETRY)
# Per-sleeve decider registry. Each checkpoint sleeve gets its OWN band-aware
# decider, so a sleeve that owns a different band/gate arbitrates its own window
# state independently of the others. The frozen MID band (0.65, 0.85] / gate 0.03
# reuses the module-global _V2_WINDOW_DECIDER above (identity preserved for the
# prune tests + byte-identical MID behavior); ANY sleeve whose band or gate
# differs mints its own instance keyed by sleeve.label.
_V2_WINDOW_DECIDERS: dict[str, spec_v2.WindowDecider] = {}
# Cross-sleeve one-bet-per-market (first fill wins). Per-sleeve deciders each
# hold a PRIVATE _filled set, so this shared market-key set is what arbitrates
# ACROSS sleeves in-memory: once ANY sleeve fills (coin, interval, window), every
# other sleeve's PLACE on that market is vetoed here. The restart-durable
# complement is ledger.subminute_market_other_sleeve.
_V2_MARKET_FILLED: set[tuple] = set()
# Bound the decider's per-window state so a long-lived process cannot leak memory.
_V2_DECIDER_PRUNE_ABOVE = 5000
_V2_DECIDER_RETAIN_SECONDS = 3600


def _v2_decider_for(sleeve: Sleeve) -> spec_v2.WindowDecider:
    """Return the band-aware WindowDecider that owns ``sleeve``.

    The frozen MID band (0.65, 0.85] / gate 0.03 reuses the module-global
    ``_V2_WINDOW_DECIDER`` (so both subminute_5m and subminute_15m share it,
    exactly as before this change); ANY sleeve whose band or gate differs gets
    its own instance keyed by ``sleeve.label``. ``consume_on_fill`` is re-synced
    from the flag each call so the master switch stays live.
    """
    label = sleeve.label
    dec = _V2_WINDOW_DECIDERS.get(label)
    if dec is None:
        ranges = tuple(getattr(sleeve, "ask_bucket_ranges", ()) or ())
        if len(ranges) == 1:
            lo, hi = float(ranges[0][0]), float(ranges[0][1])
        else:
            # Multi-range or missing band: fall back to the frozen MID band.
            # Both live checkpoint sleeves ship a single contiguous range.
            lo, hi = float(spec_v2.V2_SPEC["ask_lo"]), float(spec_v2.V2_SPEC["ask_hi"])
        gate = (
            float(sleeve.threshold)
            if getattr(sleeve, "threshold", None) is not None
            else float(spec_v2.V2_SPEC["gate"])
        )
        checkpoint_offsets = tuple(
            getattr(sleeve, "checkpoint_offset_set", ()) or ()
        )
        legacy_offsets = {
            "5m": tuple(range(270, 300)),
            "15m": tuple(range(870, 900)),
        }.get(sleeve.interval)
        is_mid = (
            abs(lo - float(spec_v2.V2_SPEC["ask_lo"])) < 1e-12
            and abs(hi - float(spec_v2.V2_SPEC["ask_hi"])) < 1e-12
            and abs(gate - float(spec_v2.V2_SPEC["gate"])) < 1e-12
            and (not checkpoint_offsets or checkpoint_offsets == legacy_offsets)
        )
        if is_mid:
            dec = _V2_WINDOW_DECIDER
        else:
            dec = spec_v2.WindowDecider(
                gate=gate,
                consume_on_fill=config.V2_REJECT_RETRY,
                ask_lo=lo,
                ask_hi=hi,
                checkpoint_offsets=checkpoint_offsets or None,
            )
        _V2_WINDOW_DECIDERS[label] = dec
    dec.consume_on_fill = config.V2_REJECT_RETRY
    return dec


def _v2_prune_window_decider(now_ts: int) -> None:
    """Drop per-window decider state older than an hour once it grows large.

    Every structure is keyed with ``window_start`` at index 2 (side keys
    ``(coin, interval, window_start, side)`` and market keys
    ``(coin, interval, window_start)``), so a single cutoff evicts resolved
    windows across all of them. A 5m/15m window is at most 900s, so a 3600s
    retention never evicts a still-open window. Prunes the module-global MID
    decider, every per-sleeve decider, and the cross-sleeve market-filled set.
    """
    global _V2_MARKET_FILLED
    cutoff = int(now_ts) - _V2_DECIDER_RETAIN_SECONDS
    # De-dup by identity: the MID label(s) alias _V2_WINDOW_DECIDER.
    deciders = {id(_V2_WINDOW_DECIDER): _V2_WINDOW_DECIDER}
    for _d in _V2_WINDOW_DECIDERS.values():
        deciders[id(_d)] = _d
    for dec in deciders.values():
        if len(dec._committed) > _V2_DECIDER_PRUNE_ABOVE:
            dec._committed = {k for k in dec._committed if k[2] >= cutoff}
        if len(dec._filled) > _V2_DECIDER_PRUNE_ABOVE:
            dec._filled = {k for k in dec._filled if k[2] >= cutoff}
        if len(dec._attempts) > _V2_DECIDER_PRUNE_ABOVE:
            dec._attempts = {k: v for k, v in dec._attempts.items() if k[2] >= cutoff}
        if len(dec._attempted_offsets) > _V2_DECIDER_PRUNE_ABOVE:
            dec._attempted_offsets = {
                k: v for k, v in dec._attempted_offsets.items() if k[2] >= cutoff
            }
        if len(dec._first_limit) > _V2_DECIDER_PRUNE_ABOVE:
            dec._first_limit = {
                k: v for k, v in dec._first_limit.items() if k[2] >= cutoff
            }
    if len(_V2_MARKET_FILLED) > _V2_DECIDER_PRUNE_ABOVE:
        _V2_MARKET_FILLED = {k for k in _V2_MARKET_FILLED if k[2] >= cutoff}


def _v2_decider_cell(
    coin: str, interval: str, window_start_ts: int, side: str
) -> dict:
    """Minimal cell mapping for the decider's attempt/fill bookkeeping (the
    keys spec_v2.WindowDecider._key / _market_key read)."""
    return {
        "coin": coin,
        "interval": interval,
        "window_start": window_start_ts,
        "side": side,
    }


def _fak_padded_limit(
    walked_limit: float, *, anchor: Optional[float], pad: float
) -> float:
    """FAK pad (``config.V2_FAK_PAD``) for a LIVE sub-minute BUY.

    ``padded = min(walked_limit + pad, anchor + pad)`` clipped to ``<= 0.99``,
    where ``anchor`` is the walked_limit of the FIRST placed attempt in this side
    window (``spec_v2.WindowDecider.anchor_limit``). On the first attempt
    ``anchor == walked_limit`` so the result is ``walked_limit + pad``; a retry
    whose fresh walked_limit ran ABOVE the anchor is capped at ``anchor + pad``
    (never chase a runaway book), while a retry BELOW the anchor pays the lower
    ``walked_limit + pad`` (the cap is not binding). The cash stake is unchanged,
    so a higher fill just buys fewer shares (stake constant, by design).

    ``anchor=None`` drops the first-attempt clamp entirely, so this attempt
    prices off its OWN fresh walked_limit (``config.V2_RETRY_REPRICE`` —
    every-qualifying retries, operator order 2026-07-25). Note that on attempt 1
    the anchor term is a no-op either way, so ``None`` only ever changes a RETRY.
    """
    padded = (
        walked_limit + pad if anchor is None
        else min(walked_limit + pad, anchor + pad)
    )
    return min(padded, 0.99)


def _v2_sizing_equity(
    live_ctx: Optional["_LiveCommitContext"], realized_capital: float
) -> float:
    """Current available equity for v2 stake sizing (operator 2026-07-09).

    Prefer the cached live CLOB balance (``live_ctx.live_balance``, populated
    from the BalanceCache via TickDeps.live_balance_fetcher) when it is present
    — the cache's ``get()`` already fails closed to None past its staleness
    bound, so a non-None value here is fresh. Fall back to the wallet-ledger
    realized capital (the $100 paper anchor + ledger PnL) only when the balance
    is unavailable/stale or the wallet is DRY, so placement is NEVER blocked on
    a balance fetch. The DD-brake keeps sizing on ledger-dd by design (see the
    call site) — this only swaps the base equity the fractions size against.
    """
    if live_ctx is not None and live_ctx.live_balance is not None:
        return float(live_ctx.live_balance)
    return float(realized_capital)


def _v2_apply_coin_cap(sleeve: Sleeve, coin: str, target_usd: float) -> float:
    """Clamp the FINAL sized v2 checkpoint stake to the sleeve's per-coin cap.

    Rationale for the placement: for checkpoint sleeves ``sizing_v2.sized_usd``
    OVERWRITES the target from ``target_bet_size_for`` (which is the only
    consumer of ``max_bet_usd_by_coin``), so an artifact per-coin cap is
    inert on the live path unless re-applied here, AFTER the tier-A boost and
    ramp multiplier. ``sized_usd`` floors up to $1 and the ramp re-floors at
    MIN_BET_USD, so a $1.00 cap yields EXACTLY $1.00 for every capped entry —
    including tier-A firsts. No cap configured -> no-op.

    STATUS 2026-07-21 (operator override — "follow backtest, don't look at
    live performance"): the deployed artifact ships ``max_bet_usd_by_coin {}``
    on both sleeves (the FOLDF SOL $1-floor probe was removed), so this clamp
    is currently INERT. It stays wired so a future per-coin cap is a pure
    artifact change; semantics are covered by synthetic-cap tests.
    """
    cap = sleeve.max_bet_usd_for(coin)
    if cap is None:
        return target_usd
    return min(target_usd, float(cap))


@dataclass
class CellDecision:
    """Phase A output — one (coin, sleeve, entry_minute) score."""

    coin: str
    sleeve: Sleeve
    entry_minute: int
    window_start_ts: int
    window_end_ts: int
    score: ScoreResult
    yes_book: Optional[BookSnapshot]
    no_book: Optional[BookSnapshot]
    spot_age_s: float
    fee_rate_bps: Optional[float]
    poly_market_id: Optional[str]
    poly_token_id_yes: Optional[str]
    poly_token_id_no: Optional[str]
    fee_unknown: bool = False            # Gamma had no feeRateBps + cache miss
    # Underlying spot at decision time — strike for the up/down market
    # resolution (review #2). Stored on the ledger row so the F3 sweep
    # compares apples to apples (resolution_price vs strike_price).
    strike_price: Optional[float] = None
    # Divergence #13 fix (2026-05-21): live spot at window_open — the
    # same anchor the model trains against and Polymarket Chainlink
    # settles on. Captured from PartialRetEngine.window_open_spot() at
    # the same tick as strike_price. When None, the cell is skipped
    # with skip_reason='no_window_open_spot' (we refuse to bet on a
    # window we cannot honestly grade).
    window_open_spot: Optional[float] = None
    # B1 sub-minute checkpoint add-on: seconds from window open for this
    # cell's checkpoint. None on every baseline (minute-boundary) cell.
    offset_sec: Optional[int] = None
    # Final post-gate outcome for the dashboard market block. ``score``
    # only says whether the RF/LUT signal exists; the cell can still fail
    # agreement, edge, fill-band, exposure, depth, or dedup gates.
    final_decision: Optional[str] = None
    final_skip_reason: Optional[str] = None
    # Which trigger rung produced this cell: 'tick' (the 1Hz scanner and the
    # fast-requote replay) or 'fast_sweep' / 'fast_event'. Stamped onto the
    # cell's `subminute_fills` row so an operator can grade the sub-second
    # latency lever with `WHERE trigger_src IN ('fast_sweep','fast_event')`.
    # Carried on the CELL rather than a thread-local because under
    # CRYPTOBOT_PHASE_B_PARALLEL_POST the POST — and its fill row — is written
    # by `_flush_parallel_posts`, i.e. not necessarily on the thread that
    # decided the cell.
    trigger_src: str = "tick"


@dataclass
class TickResult:
    """One tick's full output for SSE + observability."""

    tick_ts: int
    cells_evaluated: int = 0
    pending_rows_written: int = 0
    topup_rows_written: int = 0
    signals_written: int = 0
    skips_by_reason: dict[str, int] = field(default_factory=dict)
    cell_decisions: list[CellDecision] = field(default_factory=list)
    duration_ms_total: float = 0.0
    # Monotonic tick start, set by run_tick. Lets _do_post stamp ms_pre_post
    # (decision latency) without threading t0 through every call site.
    _t0_monotonic: float = 0.0
    error: Optional[str] = None
    # A0 latency instrumentation — per-phase wall-clock within this tick.
    # Only network-touching phases are decomposed; the residual
    # (duration_ms_total − Σ phases) is compute + SQLite. Written to a
    # stage='latency' pipeline_health row for network-active ticks only
    # (see _record_latency_trace) so the table doesn't grow at 1Hz.
    ms_spot: float = 0.0        # batched spot fetch (Binance)
    ms_live_ctx: float = 0.0    # _load_live_context incl. balance fetch
    ms_gamma: float = 0.0       # Σ fetch_market (Gamma metadata) calls
    ms_book: float = 0.0        # Σ fetch_book (CLOB /book) calls
    ms_order_sign: float = 0.0  # Σ create_market_order (EIP-712 + cache GETs)
    ms_order_post: float = 0.0  # Σ post_order round trips
    ms_order_total: float = 0.0 # Σ whole place_gtc_limit_order envelopes
    n_gamma_calls: int = 0
    n_book_calls: int = 0
    n_orders: int = 0
    # W2 book-source attribution: how many token books this tick was served by
    # each source (ws mirror / REST prefetch cache / inline REST batch+GET), so
    # the operator can SEE whether the WS mirror actually serves books or
    # silently degrades to REST. book_ws_age_ms_max = oldest WS snapshot age
    # (venue clock) served this tick — a live staleness signal.
    n_book_ws: int = 0
    n_book_prefetch: int = 0
    n_book_rest: int = 0
    book_ws_age_ms_max: float = 0.0
    # 2026-07-25 latency audit — ACCEPT vs REJECT cohorts, kept apart on purpose.
    # ms_order_post/n_orders above stay ACCEPT-ONLY so the pre-fix 48h baseline
    # remains comparable. A venue-refused FAK is a real, completed POST round
    # trip (measured ~286ms vs ~415ms for an accept: a 400 skips matching), and
    # it is ~55% of all live orders, so averaging it in as a zero — which is what
    # the dashboard did — reported order post p50 ~= 0.0ms.
    ms_order_post_rej: float = 0.0
    n_orders_rej: int = 0
    # Decision latency: tick start -> first order POST leaving the box. This is
    # the ONLY number that a language/runtime change could move; everything after
    # it is venue-side (250ms uncancellable taker delay) or post-POST tail.
    ms_pre_post: float = 0.0
    # Post-POST tail: the held-position (T3) pass, checkpoint-WFO shadow, next-window
    # market prefetch, and the trace write. Inside the tick's wall clock but
    # delays no fill.
    # ⚠️ UNDER-STATES the true tail: _tail_t0 is stamped AFTER
    # _flush_parallel_posts and AFTER _flush_immediate_fill_gets, so the
    # immediate-fill GETs (~50-260ms/order) are NOT counted here. Treat ms_tail
    # as a lower bound when deciding whether the tail is harmless.
    ms_tail: float = 0.0
    # Σ walk_book_edge_preserving inside `_commit_cell` (2026-07-30 fast-entry
    # latency decomposition). The fast rung's placement line reported ONE number
    # for the whole commit loop, so "80ms of controllable cost" was
    # unattributable between the ladder walk, the SQLite writes and the venue
    # round trip. Only the `_commit_cell` walk is counted — the pre-POST
    # freshness recheck's own walk (`_prepost_fresh_cover`, flag-off by default)
    # is deliberately NOT, so this stays "the walk the placed order was sized
    # by".
    ms_walk: float = 0.0


@dataclass
class _NetCell:
    """A2 latency: a Phase-A cell that passed the cheap feature/model score
    and still needs market metadata + books. Buffered so every qualifying
    cell's books ride ONE ``POST /books`` round trip instead of 2 sequential
    GETs per cell (see ``_resolve_net_cells``)."""

    sleeve: Sleeve
    entry_minute: int
    window_start: int
    window_end: int
    coin: str
    spot: object
    spot_age_s: float
    score: ScoreResult
    # Exact Chainlink opening anchor captured before the cheap score.  The same
    # value must feed the authoritative rescore and the ledger; a late RTDS
    # frame must never change the recorded strike after the model decided.
    twap_open: Optional[float] = None
    # B1: checkpoint offset (None for baseline minute-boundary cells).
    offset_sec: Optional[int] = None
    # Trigger provenance, carried through to the CellDecision (see its field).
    trigger_src: str = "tick"


@dataclass
class _LiveCommitContext:
    """Per-tick context object shared between every `_commit_*` call.

    Holds the LIVE routing state (mode + group + balance + readiness) loaded
    ONCE at tick start so cells 2..N see the same view phase A used. The
    `tick_group_live_delta` accumulates within-tick LIVE bets so the group
    cap gate sees the up-to-date exposure on subsequent cells (parallel to
    `tick_pending_delta` for per-wallet exposure).
    """

    mode_state: dict[str, str]
    operator_state: Optional[OperatorState]
    group_state: dict[str, GroupCapState]
    live_balance: Optional[float]
    live_pending_exposure_usd: float
    live_cap_basis_usd: Optional[float]
    all_group_caps_sum_usd: float
    live_skip_reason: Optional[str]
    order_client: Optional[object]
    notify: Optional[Callable[..., None]]
    tick_group_live_delta: dict[str, float]

def _qualifying_entry_minute(sleeve: Sleeve, current_minute: int) -> Optional[int]:
    """Return the matching entry_minute if this tick is at a qualifying
    minute for the sleeve, else None.
    """
    if sleeve.interval == "5m":
        m = current_minute % 5
    else:
        m = current_minute % 15
    return m if m in sleeve.entry_minute_set else None


def _qualifying_offset_sec(sleeve: Sleeve, now_ts: int) -> Optional[int]:
    """B1: return this tick's offset-from-window-open when it is one of the
    sleeve's checkpoint offsets, else None.

    The 1Hz tick visits each wall-clock second once, so a 1s-granular
    checkpoint set (step1last) is fully reachable; a tick lost to scheduler
    jitter simply drops that checkpoint (the next one is 1-5s later) —
    matching the drop-don't-queue misfire policy.
    """
    offsets = getattr(sleeve, "checkpoint_offset_set", ())
    if not offsets:
        return None
    window_start = _window_start_ts(now_ts, sleeve.interval_min)
    off = int(now_ts) - int(window_start)
    return off if off in offsets else None


def _schedule_filter_reason(sleeve: Sleeve, now_ts: int) -> Optional[str]:
    """Mirror backtest hour/dow filters in live runtime.

    V6 optimization expresses these filters in US/Eastern; keep live on the
    same clock even though the dashboard renders operator-facing time in MYT.
    Pair sleeves currently do not carry schedule filters and therefore pass.
    """
    hour_bucket_set = getattr(sleeve, "hour_bucket_set", "all")
    dow_set = getattr(sleeve, "dow_set", "all")
    if hour_bucket_set == "all" and dow_set == "all":
        return None

    dt_et = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(_ET)
    if hour_bucket_set == "peak" and not 12 <= dt_et.hour <= 17:
        return "schedule_filter"
    if hour_bucket_set == "off" and not 0 <= dt_et.hour <= 5:
        return "schedule_filter"
    if hour_bucket_set not in ("all", "peak", "off"):
        return "schedule_filter"

    is_weekend = dt_et.weekday() >= 5
    if dow_set == "weekday" and is_weekend:
        return "schedule_filter"
    if dow_set == "weekend" and not is_weekend:
        return "schedule_filter"
    if dow_set not in ("all", "weekday", "weekend"):
        return "schedule_filter"
    return None


def _window_start_ts(now_ts: int, interval_min: int) -> int:
    """Window start aligned to the interval boundary (D8 entry_ts pin)."""
    win_seconds = interval_min * 60
    return (now_ts // win_seconds) * win_seconds


def _initial_entry_bet_seen(conn, d: CellDecision) -> bool:
    """Whether this coin/sleeve/window/entry-minute already opened a bet."""
    return _initial_entry_bet_seen_for_key(
        conn,
        coin=d.coin,
        sleeve_label=d.sleeve.label,
        entry_minute=d.entry_minute,
        window_start_ts=d.window_start_ts,
    )


def _initial_entry_bet_seen_for_key(
    conn,
    *,
    coin: str,
    sleeve_label: str,
    entry_minute: int,
    window_start_ts: int,
) -> bool:
    """Whether this coin/sleeve/window/entry-minute already opened a bet."""
    row = conn.execute(
        "SELECT 1 FROM ledger "
        "WHERE coin = ? AND sleeve = ? AND entry_minute = ? "
        "  AND side IN ('YES', 'NO') "
        "  AND window_start_ts = ? "
        "  AND is_topup = 0 AND outcome != 'CANCELLED' "
        "LIMIT 1",
        (coin, sleeve_label, entry_minute, window_start_ts),
    ).fetchone()
    return row is not None


def _claim_entry_scan_for_key(
    conn,
    *,
    coin: str,
    sleeve_label: str,
    entry_minute: int,
    window_start_ts: int,
) -> bool:
    """Return True only for the first live scan of this entry-minute."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO entry_scan_latches "
        "(coin, sleeve, entry_minute, window_start_ts) "
        "VALUES (?, ?, ?, ?)",
        (coin, sleeve_label, entry_minute, window_start_ts),
    )
    return (cur.rowcount or 0) > 0


@dataclass
class TickDeps:
    """Dependency injection bundle so the orchestrator stays testable
    without monkey-patching globals."""

    partial_ret_engine: PartialRetEngine
    config_sha256: str
    fetch_spots: Callable[..., Optional[dict]]
    fetch_market: Callable[..., Optional[object]]
    get_disk_free_mb: Callable[[], float]
    # CACHE-ONLY spot read (``data.binance.peek_all_spots``).
    # The fast-trigger rungs' ONLY spot access: it returns the already-cached
    # batch for the requested bucket or None, and NEVER performs a GET — see
    # `_fast_entry_scan` stage 3 and the regression note on the adapters'
    # ``_MAX_BUCKETS``. `fetch_spots` (fetch-on-miss) stays the 1Hz tick's, and
    # the fast path must never reach for it. None => the fast rungs cannot read
    # spot at all and abstain; the tick is unaffected.
    peek_spots: Optional[Callable[..., Optional[dict]]] = None
    # Per-token book GET (CLOB /book). Optional and None-guarded at every call
    # site, exactly like `fetch_books` below: the fast-trigger rungs pass None
    # because they are never allowed to spend a REST call — a fast trigger only
    # fires because a snapshot ALREADY arrived on the WS push or is already
    # cached, and a 50-260ms GET would cost more than the whole sampling delay
    # this path removes while spending, at 4Hz, the shared per-IP key budget the
    # 1Hz tick depends on. The tick remains the REST-backstopped path.
    fetch_book: Optional[Callable[..., Optional[BookSnapshot]]] = None
    # A2 latency: batch book fetcher (POST /books — one round trip for every
    # qualifying cell's tokens). Optional: when None (tests / legacy
    # fixtures) the pipeline falls back to per-token fetch_book GETs.
    fetch_books: Optional[Callable[..., Optional[dict]]] = None
    # U6 per-strategy LIVE routing (all optional — DRY-only fixtures and
    # tests don't supply them; pipeline falls through to legacy DRY behavior).
    #
    # `order_client` — the OrderClient instance from execution/walker.py.
    # When None, every LIVE wallet skips with `clob_unavailable`.
    # `notify` — callback `(event_type, message, *, wallet_id, critical)`
    # used to surface CRITICAL funder-balance shortfalls + venue degradation.
    order_client: Optional[object] = None
    notify: Optional[Callable[..., None]] = None
    # Per-tick caches the orchestrator reads ONCE (D-LIVE-20). When None
    # the orchestrator calls the default loaders from operator_control /
    # capital so callers don't have to wire them.
    mode_state_loader: Optional[Callable[..., dict[str, str]]] = None
    operator_state_loader: Optional[Callable[..., OperatorState]] = None
    group_state_loader: Optional[Callable[..., dict[str, GroupCapState]]] = None
    live_balance_fetcher: Optional[Callable[[], Optional[float]]] = None
    # Stub-able for tests; defaults to time.monotonic-anchored real-clock
    # readiness lookup against `live_readiness_reports`.
    live_readiness_loader: Optional[Callable[..., bool]] = None
    # The TWAP HGB model and the Chainlink TWAP mirror that supplies each
    # window's opening price.
    twap_hgb_artifact: Optional["HgbArtifact"] = None
    twap_mirror: Optional[object] = None
    # A8 latency: boundary book prefetcher (scheduler/book_prefetch.py). When
    # present, pass 2 serves fresh-enough cached snapshots before paying the
    # inline batch, and reports its token set back via note_tokens(). None =
    # legacy inline fetch exactly.
    book_prefetch: Optional[object] = None
    # W3 shadow-compare: a ShadowBookComparator wrapping the WS mirror. When
    # present, pass 2 hands each AUTHORITATIVE REST book to observe() so the WS
    # snapshot can be compared off the decision path. REST stays authoritative;
    # this NEVER changes `books`. None = no shadow comparison.
    book_shadow: Optional[object] = None
    # Executor-thread DB access for the Pass-1 Gamma fan-out. The
    # ``ThreadLocalConnections.get`` handle (``conns.get``): returns the CALLING
    # thread's own sqlite3 connection. When >1 cold cells fan out over the
    # shared CLOB executor, each worker closure calls this to get ITS OWN
    # connection instead of sharing the tick's — a shared connection's
    # prepared-statement cache raised sqlite3.InterfaceError (SQLITE_MISUSE)
    # under the concurrent fee-cache read/write, which the fan-out's blanket
    # except silently mapped to a missed bet. None in tests / legacy fixtures
    # that stub ``fetch_market`` and never touch the DB → the closure falls back
    # to the tick conn (no DB access ⇒ no MISUSE).
    conn_factory: Optional[Callable[[], sqlite3.Connection]] = None


def _bump(d: dict, key: str) -> None:
    d[key] = d.get(key, 0) + 1


def _bucket_or_none(b: Optional[int]) -> Optional[int]:
    """Translate the `-1` LUT-bucket sentinel to None for downstream rows.

    The retired lut|rf|blend scorer returned `lut_p_bucket=-1` when the LUT
    lookup didn't run, and `score_cell_hgb` still writes that sentinel today.
    Persisting `-1` in the signals table corrupts gate-skip analytics — the
    snapshot builder's GROUP BY treats it as a real bucket. Use this helper at
    every record_signal_* call so the wire format stays NULL-only.
    """
    if b is None or b < 0:
        return None
    return b


def _score_cell_for(
    *,
    coin: str,
    sleeve: Sleeve,
    entry_minute: int,
    window_start_ts: int,
    live_spot: float,
    yes_ask: float,
    no_ask: float,
    offset_sec: Optional[int],
    deps: TickDeps,
    twap_open: Optional[float],
    fee_rate_bps: Optional[float] = None,
) -> "ScoreResult":
    """Score one cell with the TWAP model.

    ONE dispatcher shared by Phase A's cheap pre-score (NaN asks = "no book
    yet"), the authoritative rescore in ``_resolve_net_cells`` and the fast
    rung's pre-score, so the three can never score against different inputs.
    ``entry_minute`` and ``fee_rate_bps`` are accepted for the test scorer stub.
    """
    if sleeve.signal_source != "hgb":
        raise ValueError(
            f"pipeline: sleeve {sleeve.label!r} signal_source="
            f"{sleeve.signal_source!r} has no scorer"
        )
    return score_cell_twap(
        coin=coin,
        sleeve=sleeve,
        window_start_ts=window_start_ts,
        live_spot=live_spot,
        open_twap=None if twap_open is None else float(twap_open),
        yes_ask=yes_ask,
        no_ask=no_ask,
        partial_ret_engine=deps.partial_ret_engine,
        offset_sec=offset_sec,
        twap_hgb=deps.twap_hgb_artifact,
    )


def _window_open_price(
    deps: TickDeps,
    *,
    coin: str,
    sleeve: Sleeve,
    window_start_ts: int,
) -> Optional[float]:
    """The window's opening Chainlink TWAP, or None when it is not known yet.

    This is the price Polymarket settles against, so it is both the model's
    anchor and the strike recorded on the ledger row.
    """
    if deps.twap_mirror is None:
        return None
    sample = deps.twap_mirror.get_opening_twap(
        coin, sleeve.interval, window_start_ts
    )
    return None if sample is None else float(sample.value)


def _mark_skip(d: CellDecision, reason: str) -> None:
    d.final_skip_reason = reason
    d.final_decision = f"SKIP:{reason}"


def _mark_would_bet(d: CellDecision) -> None:
    d.final_skip_reason = None
    d.final_decision = "WOULD_BET"


def _interval_entry_disabled(interval_min: Optional[int]) -> bool:
    """Interval entry-disable predicate (config.INTERVAL_5M_ENABLED, default on).

    THE single authority for this gate's whole scope — the caller just tests the
    boolean. Returns True when ``interval_min`` names an interval the operator
    has switched off for NEW entries (today only 5m; operator order 2026-07-25).
    Pure so it unit-tests directly (mirrors _late_window_skip / _ask_band_skip).

    ENTRY-only by design: the sleeve stays in the loaded portfolio, so open
    positions on a disabled interval keep their held-position/T3 coverage
    (``held_position_pass.py`` looks sleeves up by label) and the dashboard keeps showing
    their history and PnL. Every path that can open NEW exposure tests this —
    the tick loop's sleeve iteration, the per-cell commit, the
    fast-requote replay, and the scale-in top-up (which is driven off open
    LEDGER rows, not the sleeve loop, so it would otherwise bypass the gate).

    Byte-identical when the interval is enabled: the flag short-circuits before
    any comparison.
    """
    return not config.INTERVAL_5M_ENABLED and interval_min == 5


#: Per-sleeve gate reasons the tick's loop acts on WITHOUT writing a signals
#: row. At 1Hz a row per sleeve per coin for a permanently-off interval (or for
#: every non-checkpoint second) would flood the table, so these three stay
#: silent — see the tick's sleeve loop.
_SILENT_SLEEVE_GATES = frozenset(
    {"interval_disabled", "subminute_disabled", "not_qualifying_offset"}
)


def _sleeve_entry_gate(
    sleeve: Sleeve,
    now_ts: int,
    *,
    current_minute: Optional[int] = None,
    portfolio: Optional[V6Portfolio] = None,
    wallet_snap: Optional[WalletSnapshot] = None,
) -> Optional[str]:
    """THE definition of "may this sleeve open a new entry at ``now_ts``?".

    Returns None when the sleeve is clear, else the blocking reason. Evaluated
    in the tick's own order, because the FIRST blocker is the one the tick
    records:

      1. ``interval_disabled``       — operator interval entry-disable (silent)
      2. ``subminute_disabled``      — master flag / kill switch (silent)
      3. ``not_qualifying_offset``   — not one of this sleeve's checkpoint
                                       seconds (silent)
      4. ``not_entry_minute``        — baseline (minute-boundary) sleeves only
      5. ``schedule_filter``         — hour/dow filter
      6. ``dd_halt``                 — HARD per-wallet drawdown halt

    ONE function because there are two callers that must never disagree: the
    tick's sleeve loop (which needs the reason string for its skip rows) and the
    fast-trigger rungs (which need the same verdict as a boolean). The fast rung
    previously reproduced 1-3 and 5 and silently DROPPED the dd-halt gate, so a
    drawdown-HALTED wallet could still place off-tick — the exact class of bug
    a duplicated gate list produces.

    ``current_minute`` None skips gate 4 (checkpoint-only callers); it is inert
    for a checkpoint sleeve either way. ``wallet_snap``/``portfolio`` None skips
    gate 6 — a caller with no snapshot in hand has no drawdown data, which has
    always meant "do not halt" (the tick's own ``wallet_snap is not None``
    guard). The fast rungs' cheap in-window predicate deliberately passes None
    (a snapshot costs one ledger scan, which a 4Hz guard must never pay) and
    re-runs this gate WITH the snapshot it loads before committing.
    """
    if _interval_entry_disabled(sleeve.interval_min):
        return "interval_disabled"
    if getattr(sleeve, "checkpoint_offset_set", ()):
        if not config.SUBMINUTE_ENABLED or config.SUBMINUTE_KILL:
            return "subminute_disabled"
        if _qualifying_offset_sec(sleeve, now_ts) is None:
            return "not_qualifying_offset"
    elif current_minute is not None:
        if _qualifying_entry_minute(sleeve, current_minute) is None:
            return "not_entry_minute"
    schedule_reason = _schedule_filter_reason(sleeve, now_ts)
    if schedule_reason is not None:
        return schedule_reason
    if (
        wallet_snap is not None
        and portfolio is not None
        and _wallet_drawdown_halted(portfolio, wallet_snap)
    ):
        return "dd_halt"
    return None


def _execution_min_edge(sleeve: Sleeve) -> float:
    """Minimum realized VWAP edge required by the execution walker."""
    threshold = getattr(sleeve, "threshold", None)
    if threshold is not None:
        return max(float(sleeve.min_edge), float(threshold))
    return float(sleeve.min_edge)


def _topup_effective_min_bet(target_usd: float) -> float:
    """Avoid dust top-ups once the slot is effectively filled."""
    return max(float(config.MIN_BET_USD), 0.01 * max(0.0, float(target_usd)))


def _execution_max_walk_price(sleeve: Sleeve) -> Optional[float]:
    """Optional execution price leash.

    In the HGB edge-preserving top-up regime, ``book_consumption_max=None``
    means realized VWAP edge is the execution boundary, so the old sticky
    price leash is disabled.
    """
    if getattr(sleeve, "signal_source", None) == "hgb" and getattr(
        sleeve, "book_consumption_max", None
    ) is None:
        return None
    return sleeve.max_walk_price


# ---------------------------------------------------------------------------
# Fast-trigger entry path (config.FAST_TRIGGER_*, operator 2026-07-30)
# ---------------------------------------------------------------------------
#
# The measured problem (on-chain forensic, n=45): 84% of our rejects were
# consumed by a FASTER TAKER, median 1.0s before our arrival, ~5x our size, at
# exactly our quoted ask. The POST path is already at the venue floor, so the
# only reclaimable latency is TRIGGER-TO-DECISION — the 1Hz boundary-aligned
# tick means a mid-second ask refill waits ~500ms on average before we look.
#
# `run_fast_entry_scan` is the ONE evaluation path both extra rungs (the 250ms
# sweep job and the WS ask-update event) converge on. It is deliberately NOT a
# second copy of the decision logic: it reuses the tick's own scoring
# (`score_cell_hgb` at the same INTEGER offset_sec the checkpoint models are
# keyed by), the tick's own market+book+rescore resolver
# (`_resolve_net_cells`), and the tick's own commit machinery (`_commit_cell`),
# so band / gate / V2_EDGE_CAP / sizing / caps / DD-brake / one-per-window dedup
# / `_attempted_offsets` / first-attempt-only / multifill all bind unchanged.

# Single-flight across the tick's Phase B commit stage and the fast rungs. Held
# for the duration of the WHOLE commit stage, which under
# CRYPTOBOT_PHASE_B_PARALLEL_POST=1 (the live config) is the only correct
# boundary: in that mode the order POSTs do not happen inside `_commit_cell` at
# all, they are collected and fired by `_flush_parallel_posts` AFTER the cell
# loop — so a per-`_commit_cell` lock covered the prep and left every POST
# outside it. Worst case held ≈ one tick's Phase B (its skip-row INSERTs plus
# one fan-out of POSTs, ~350ms), well inside TICK_HARD_DEADLINE_MS=5000.
#   * the tick acquires it BLOCKING — a tick must never be skipped;
#   * a fast rung acquires it NON-BLOCKING and abandons the scan when contended
#     (a skipped scan is free; a double order is not).
# Correctness does not rest on this lock alone — `_attempted_offsets` (in
# memory) and the `uniq_loa_active_slot` partial index (durable, IntegrityError
# => slot_full) already make a duplicate order impossible. The lock removes the
# read-modify-write window on the shared in-memory decider state and keeps two
# order POSTs for one resolution from ever being in flight together.
_FAST_ENTRY_LOCK = threading.Lock()


def _phase_b_single_flight():
    """The lock context for the tick's Phase B commit stage (blocking).

    Byte-identical to no lock at all when the master flag is off, so a
    deployment with CRYPTOBOT_FAST_TRIGGER_ENABLED=0 pays literally nothing.
    """
    if not config.FAST_TRIGGER_ENABLED:
        return contextlib.nullcontext()
    return _FAST_ENTRY_LOCK


def _fast_offset_spent_for_all_sides(
    dec: "spec_v2.WindowDecider",
    coin: str,
    interval: str,
    window_start_ts: int,
    offset_sec: int,
) -> bool:
    """True iff BOTH sides of this resolution already fired this integer offset.

    The free pre-filter that makes 4Hz affordable. It is provably non-lossy:
    ``WindowDecider._on_second_consume_on_fill`` checks ``offset in seen``
    UNCONDITIONALLY (multifill's ``allow_refill`` bypasses only the market-level
    fill short-circuit), so a cell whose offset is already in both sides' sets
    could not place no matter what the fresh book says — the scan would burn a
    model inference and a book read to reach a guaranteed SKIP. In practice this
    is what silences the 3 sub-second fires that follow the tick's own
    band-passing look at second S.
    """
    for side in ("YES", "NO"):
        seen = dec._attempted_offsets.get(
            (coin, interval, int(window_start_ts), side)
        )
        if seen is None or offset_sec not in seen:
            return False
    return True


def fast_entry_candidate_sleeves(
    portfolio: V6Portfolio, now_ts: int
) -> list[tuple[Sleeve, int]]:
    """The ``(sleeve, offset_sec)`` pairs eligible for a fast scan at ``now_ts``.

    Pure integer math over the sleeves' checkpoint offset sets plus the tick's
    own per-sleeve gates via :func:`_sleeve_entry_gate` — the ONE gate list, so
    this predicate cannot drift from the tick's loop. Empty list => the caller
    returns immediately, which is the ~free outside-the-window path the 250ms
    cadence rests on. Exported so ``scheduler.fast_entry`` can use the SAME
    predicate for its cheap guard and its WS-callback gate.

    The DD-halt gate is the one member of that list this predicate cannot
    evaluate: it needs a wallet snapshot, i.e. one full ledger scan, which a 4Hz
    guard must never pay. ``run_fast_entry_scan`` re-runs the gate WITH the
    snapshot it already loads before committing, so the halt still binds.
    """
    out: list[tuple[Sleeve, int]] = []
    for sleeve in portfolio.sleeves:
        if not getattr(sleeve, "checkpoint_offset_set", ()):
            continue
        if _sleeve_entry_gate(sleeve, now_ts) is not None:
            continue
        offset_sec = _qualifying_offset_sec(sleeve, now_ts)
        if offset_sec is None:  # pragma: no cover — the gate already vetoed it
            continue
        out.append((sleeve, offset_sec))
    return out


def _fast_side_offset_spent(dec: "spec_v2.WindowDecider", d: CellDecision) -> bool:
    """True iff THIS cell's chosen side already fired this integer offset.

    The post-rescore tightening of :func:`_fast_offset_spent_for_all_sides`.
    Before the rescore only "BOTH sides spent" is provable; afterwards the ONE
    side that matters is known, so a cell the decider is certain to veto is
    dropped BEFORE the single-flight lock instead of after it.
    """
    seen = dec._attempted_offsets.get(
        (d.coin, d.sleeve.interval, int(d.window_start_ts), d.score.side)
    )
    return seen is not None and d.offset_sec in seen


#: One-entry memo for the fast rungs' resolver view of ``deps``. The view is a
#: pure function of the bundle (books cache-only, no shadow comparator), and
#: ``dataclasses.replace`` on a 25-field dataclass is not free at ~16 fires/s —
#: with `scheduler.fast_entry` prebuilding ONE TickDeps per second, this becomes
#: one construction per second instead of one per fire.
_FAST_RESOLVER_DEPS: "tuple[Optional[TickDeps], Optional[TickDeps]]" = (None, None)


def _fast_resolver_deps(deps: TickDeps) -> TickDeps:
    """The cache-only resolver view of ``deps`` (see :data:`_FAST_RESOLVER_DEPS`).

    ``fetch_book`` / ``fetch_books`` are dropped because the fast rungs are never
    allowed to spend a REST call, and ``book_shadow`` because the comparator
    expects AUTHORITATIVE REST books to compare the WS mirror against — a
    cache-served scan has none to give it. Keyed on the SOURCE bundle's identity;
    a different bundle rebuilds.
    """
    global _FAST_RESOLVER_DEPS
    src, view = _FAST_RESOLVER_DEPS
    if src is deps and view is not None:
        return view
    view = dataclass_replace(
        deps, fetch_book=None, fetch_books=None, book_shadow=None
    )
    _FAST_RESOLVER_DEPS = (deps, view)
    return view


def _fresh_operator_state(
    conn, deps: TickDeps, fallback: Optional[OperatorState]
) -> Optional[OperatorState]:
    """Re-read the operator singleton row, falling back to the cached value.

    The ONE piece of the commit-stage context the per-second cache must not
    serve: it is the operator's emergency brake, and one indexed read of a
    single-row table is cheap enough to pay on every fire. A read failure falls
    back to the cached row rather than inventing ``None`` (which
    ``_live_wallet_skip_reason`` maps to ``operator_stopped``) so a transient
    SQLite error cannot silently halt LIVE entries.
    """
    loader = deps.operator_state_loader or get_operator_state
    try:
        return loader(conn)
    except Exception:  # noqa: BLE001 — parity with _load_live_context
        return fallback


def run_fast_entry_scan(
    conn,
    portfolio: V6Portfolio,
    *,
    now_ts: int,
    deps: TickDeps,
    trigger_src: str,
    candidates: Optional[list[tuple[Sleeve, int]]] = None,
) -> dict:
    """Re-evaluate this instant's qualifying sub-minute cells off-tick.

    Called by BOTH fast rungs (250ms sweep job / WS ask-update worker) on their
    own scheduler-pool connection. Returns a counters dict for the caller's log
    line; never raises for an expected drop. ``candidates`` lets a caller that
    already ran :func:`fast_entry_candidate_sleeves` (the sweep's own in-window
    guard does) hand the list straight in instead of paying for it twice.

    Shape — cheapest stage first, so a fire that cannot possibly place costs
    almost nothing:

      1. ``fast_entry_candidate_sleeves`` — integer math, no I/O.
      2. ``_fast_offset_spent_for_all_sides`` — in-memory set lookups.
      3. ``peek_spots`` — a PEEK of the 1s-bucket spot cache the 1Hz tick keeps
         warm; a cold bucket abstains rather than fetching (``spot_cold``). The
         fast rung deliberately does NOT call ``partial_ret_engine.update``: the
         tick owns that state and feeding it several times per second would
         corrupt the per-second return features both paths score from.
      4. the tick's own cheap score (same functions, same integer offset).
      5. ``_resolve_net_cells`` — market metadata (per-window cache, warm by
         construction 4.5 minutes into a window) + books from the WS/prefetch
         chain ONLY + the authoritative rescore, exactly as the tick resolves.
      5b. ``_fast_side_offset_spent`` — the now-known side's own dedup, BEFORE
         the lock (see below).
      6. ``_commit_cell`` for cells the rescore left bettable, under the
         single-flight lock.

    Three policies that differ from the tick, all deliberate:

    * **No skip rows.** Cells the rescore left non-bettable are DROPPED here
      instead of written to ``signals``. At 4Hz the tick's per-cell skip row
      would be pure duplication of a row the 1Hz tick already wrote for the same
      second — the "rows only when something places or rejects" policy. The
      counters + the one INFO line per placement are the observability.
    * **Nothing enters the lock that the decider will veto.** Stage 5b matters
      because the lock is shared with the tick: at 250ms + WS pushes a hot second
      can fire ~20 times, and without it every one of those fires would take the
      lock and run a full live-context preamble (one ledger scan) to reach a
      guaranteed SKIP — blocking the tick each time.

    Latency (operator 2026-07-30). Two things happen SOONER than they used to,
    and nothing happens differently:

    * stages 4/5 are served by the per-second score memo (see
      :data:`_SCORE_MEMO`), so the boundary tick's own second-S scoring IS this
      rung's precompute — the fire only pays for what the fresh ask changed;
    * stage 6's preamble is served by :class:`_FastCommitStage`, so the fire that
      actually places usually does not re-pay the ledger scan an earlier
      non-placing fire in the same second already paid.

    Every guard still binds at FIRE time against the FRESH ask: band, gate,
    V2_EDGE_CAP, `_attempted_offsets` + the durable slot index, first-attempt-only
    and the hard DD halt are all evaluated below or inside ``_commit_cell``,
    untouched.
    """
    t_scan = time.monotonic()
    counters: dict[str, int] = {
        "cells": 0, "scored": 0, "bettable": 0, "committed": 0,
        "orders": 0, "contended": 0, "dd_halted": 0, "spot_cold": 0,
    }
    if candidates is None:
        candidates = fast_entry_candidate_sleeves(portfolio, now_ts)
    if not candidates:
        return counters
    if deps.book_prefetch is None:
        # No push/cache book source wired => nothing this rung could read
        # without a REST call it is not allowed to make (fetch_book=None below).
        return counters

    # Stage 2 — free in-memory dedup against whatever the tick (or an earlier
    # fast fire) already fired for this integer second.
    work: list[tuple[Sleeve, int, int, str]] = []
    for sleeve, offset_sec in candidates:
        window_start = _window_start_ts(now_ts, sleeve.interval_min)
        dec = _v2_decider_for(sleeve)
        for coin in config.COINS:
            counters["cells"] += 1
            if _fast_offset_spent_for_all_sides(
                dec, coin, sleeve.interval, window_start, offset_sec
            ):
                continue
            work.append((sleeve, offset_sec, window_start, coin))
    if not work:
        return counters

    # Stage 3 — spot from the current 1s bucket, PEEK-ONLY (`deps.peek_spots`,
    # never `deps.fetch_spots`). This rung may not spend a GET here any more than
    # it may for a book or for market metadata: a cold bucket ABSTAINS from the
    # fire. See the regression note on the adapters' `_MAX_BUCKETS` — a fetching
    # fast path both paid ~50-100ms inline on the 4Hz rung AND evicted the
    # prefetcher's next-bucket warm entry, taking the 1Hz tick's own p50 from
    # 115ms to 315ms. Abstaining costs one fast fire, which the boundary tick
    # re-evaluates ~immediately.
    peek_spots = deps.peek_spots
    if peek_spots is None:
        # No cache-only reader wired => this rung has no legal spot access.
        return counters
    spots = peek_spots(
        now_ts=float(now_ts), tick_seconds=config.SPOT_FETCH_INTERVAL_SECONDS
    )
    if not spots:
        counters["spot_cold"] = 1
        return counters

    # Stage 4 — the tick's cheap score, at the SAME integer offset_sec the
    # checkpoint bundles are keyed by (a mid-second fire scores second S).
    tick = TickResult(tick_ts=now_ts)  # local only — never persisted
    net_cells: list[_NetCell] = []
    for sleeve, offset_sec, window_start, coin in work:
        spot = spots.get(coin)
        if spot is None:
            continue
        # The fast rungs never spend a venue round trip — books are push/cache
        # only (fetch_book=None below) and market metadata is cache-only here. A
        # resolution whose Gamma/discovery metadata is not already in the
        # per-window cache is skipped rather than paying a GET at 4Hz. That also
        # stops these rungs from multiplying the tick's retry load on a FAILING
        # metadata endpoint: the cache only fills on success, so during an outage
        # the fast rungs stay silent while the 1Hz tick keeps retrying. By the
        # final decision minute the cache has been warm for minutes (the tick
        # resolves every qualifying second, and _prefetch_next_window_markets
        # pre-warms the window before it even opens).
        if not _venue_market_cache_has(coin, sleeve.interval_min, window_start):
            continue
        counters["scored"] += 1
        entry_minute = offset_sec // 60
        nan = float("nan")
        # Same as the 1Hz tick's cheap pre-score: no market resolved yet, so no
        # fee_rate_bps. Stage 5 below runs `_resolve_net_cells`, which rescores
        # with the real per-market fee before anything can place.
        twap_open = _window_open_price(
            deps, coin=coin, sleeve=sleeve, window_start_ts=window_start
        )
        score = _score_cell_for(
            coin=coin,
            sleeve=sleeve,
            entry_minute=entry_minute,
            window_start_ts=window_start,
            live_spot=spot.price,
            yes_ask=nan,
            no_ask=nan,
            offset_sec=offset_sec,
            deps=deps,
            twap_open=twap_open,
        )
        if score.skip_reason is not None:
            continue
        net_cells.append(
            _NetCell(
                sleeve=sleeve,
                entry_minute=entry_minute,
                window_start=window_start,
                window_end=window_start + sleeve.interval_min * 60,
                coin=coin,
                spot=spot,
                spot_age_s=max(0.0, float(now_ts) - float(spot.publish_time)),
                score=score,
                twap_open=twap_open,
                offset_sec=offset_sec,
                trigger_src=trigger_src,
            )
        )
    if not net_cells:
        return counters

    # Stage 5 — the tick's own resolver, over a deps view that can only read
    # pushed/cached books (no per-token GET, no batch POST) and does not re-note
    # the WS subscription set. `note_tokens=False` is load-bearing: note_tokens
    # is a WHOLESALE REPLACE that drives the WS subscription, and the 1Hz tick
    # owns that set (current window PLUS the next window's pre-warm), so letting
    # a 4Hz scan re-note its own narrower subset would churn the subscription
    # several times a second and throw the pre-warm away.
    fast_deps = _fast_resolver_deps(deps)
    decisions = _resolve_net_cells(
        net_cells,
        conn=conn,
        deps=fast_deps,
        portfolio=portfolio,
        now_ts=now_ts,
        tick=tick,
        note_tokens=False,
    )
    placing = [d for d in decisions if d.score.skip_reason is None]
    # Stage 5b — the chosen side's own offset dedup, still OUTSIDE the lock.
    placing = [
        d for d in placing
        if not _fast_side_offset_spent(_v2_decider_for(d.sleeve), d)
    ]
    counters["bettable"] = len(placing)
    if not placing:
        return counters

    # Stage 6 — commit. Everything below is the tick's Phase B, minus the
    # cross-coin min_agreement bookkeeping the sub-minute sleeves do not use
    # (same_side_counts is still built over this scan's own set so the gate
    # reads a structurally-identical map).
    deferred_fill_gets: list[_DeferredFillGet] = []
    if not _FAST_ENTRY_LOCK.acquire(blocking=False):
        counters["contended"] = 1
        return counters
    # The LIVE-context + wallet-snapshot reads below run INSIDE the lock even
    # though they are pure reads, and that is deliberate: taken outside it, a
    # tick commit landing in between would leave this scan sizing against a
    # pre-order exposure view and a per-wallet/group cap could be exceeded by one
    # bet. The cost is the same ledger scan the tick already pays every second,
    # and it is only reached when a cell is genuinely about to place — so the
    # tick's worst-case wait here is that scan plus one POST (~350ms), inside
    # TICK_HARD_DEADLINE_MS=5000. A fresh cap view is worth the wait.
    #
    # `_fast_commit_stage` serves that load from the per-second cache when — and
    # only when — nothing placed and no ledger row moved since it was taken (see
    # the class docstring's five-clause contract). The operator row is re-read
    # FRESH on every fire regardless, so a STOP binds at the next fire.
    try:
        t_ctx = time.monotonic()
        stage, ctx_hit = _fast_commit_stage(
            conn, portfolio, deps, now_ts=now_ts, tick=tick
        )
        tick_group_live_delta: dict[str, float] = {}
        live_ctx, wallet_snaps = _assemble_live_commit_context(
            stage.loaded, deps,
            tick_group_live_delta=tick_group_live_delta,
            op_state=_fresh_operator_state(conn, deps, stage.loaded.op_state),
        )
        ctx_ms = (time.monotonic() - t_ctx) * 1000.0
        # HARD DD-halt parity with the tick's sleeve loop. This is the ONE gate
        # in `_sleeve_entry_gate` the 4Hz in-window predicate cannot evaluate (it
        # needs a wallet snapshot = one ledger scan), so it binds HERE — the
        # first point at which this scan holds a fresh snapshot. A halted wallet
        # placing off-tick would bypass the drawdown halt entirely.
        n_before = len(placing)
        placing = [
            d for d in placing
            if _sleeve_entry_gate(
                d.sleeve, now_ts,
                portfolio=portfolio,
                wallet_snap=wallet_snaps.get(d.sleeve.label),
            ) is None
        ]
        counters["dd_halted"] = n_before - len(placing)
        if not placing:
            return counters  # the finally below still releases the lock
        same_side_counts: dict[tuple, int] = {}
        for d in placing:
            key = (
                d.sleeve.label, d.window_start_ts, d.entry_minute, d.score.side
            )
            same_side_counts[key] = same_side_counts.get(key, 0) + 1
        tick_pending_delta: dict[str, float] = {}
        t_trigger = time.monotonic()
        for d in placing:
            try:
                _commit_cell(
                    conn, d, portfolio, wallet_snaps, tick_pending_delta,
                    stage.disk_free_mb, same_side_counts, deps.config_sha256,
                    tick, now_ts,
                    live_ctx=live_ctx,
                    tick_breaker=stage.tick_breaker,
                    deferred_fill_gets=deferred_fill_gets,
                    # Never fan out: a fast scan carries at most a couple of
                    # cells and must hold the lock for as short as possible.
                    parallel_posts=None,
                )
                counters["committed"] += 1
            except Exception:  # noqa: BLE001 — per-cell isolation
                logger.warning(
                    "fast_entry cell error for %s/%s off=%s",
                    d.coin, d.sleeve.label, d.offset_sec, exc_info=True,
                )
        counters["orders"] = int(tick.n_orders)
        if tick.n_orders:
            # LATENCY DECOMPOSITION (operator 2026-07-30). `trigger_to_post_ms`
            # keeps BOTH its name and its exact meaning — wall time of the commit
            # loop — so the pre-change 381ms samples stay comparable. Everything
            # else is new and additive:
            #   pre_ms   scan entry -> commit-loop start (stages 1-5b + the lock
            #            wait + the context assemble). This segment was entirely
            #            INVISIBLE before, which is why "~80ms controllable" was
            #            never attributable.
            #   ctx_ms   the commit-stage preamble leg alone, and whether the
            #            per-second cache served it (`ctxhit`).
            #   walk_ms  Σ walk_book_edge_preserving inside `_commit_cell`.
            #   sign_ms  Σ EIP-712 signing the OrderClient measured.
            #   post_ms  Σ venue round trip (accept + reject cohorts summed —
            #            here we want the wall clock, not the ACCEPT-only series).
            #   db_ms    the residual: SQLite (BEGIN IMMEDIATE, the reserve's own
            #            commit, the post-POST ledger/fill writes) + the gate
            #            stack. This is the number to attack next if it is large.
            _post_ms = tick.ms_order_post + tick.ms_order_post_rej
            _total_ms = (time.monotonic() - t_trigger) * 1000.0
            logger.info(
                "fast_entry placed src=%s n=%d trigger_to_post_ms=%.0f "
                "pre_ms=%.0f ctx_ms=%.1f ctxhit=%d walk_ms=%.1f sign_ms=%.0f "
                "post_ms=%.0f db_ms=%.0f cells=%d bettable=%d",
                trigger_src, tick.n_orders, _total_ms,
                (t_trigger - t_scan) * 1000.0,
                ctx_ms, int(ctx_hit), tick.ms_walk, tick.ms_order_sign,
                _post_ms,
                max(0.0, _total_ms - tick.ms_walk - tick.ms_order_sign - _post_ms),
                counters["cells"], counters["bettable"],
            )
    finally:
        _FAST_ENTRY_LOCK.release()

    if deferred_fill_gets:
        # Same contract as the tick: the immediate fill GET must land before the
        # held-position pass can read it. Outside the lock — it POSTs nothing.
        try:
            _flush_immediate_fill_gets(conn, deferred_fill_gets)
        except Exception:  # noqa: BLE001 — the 60s reconciler is the backstop
            logger.warning("fast_entry immediate-fill flush failed", exc_info=True)
    return counters


def _record_fetch_abort(conn, now_ts: int) -> None:
    """Record one spot-fetch abort per configured fetch bucket."""
    interval = max(1, int(config.SPOT_FETCH_INTERVAL_SECONDS))
    bucket_start = (int(now_ts) // interval) * interval
    message = (
        f"spot fetch returned None source={config.SPOT_SOURCE} "
        f"bucket={bucket_start}"
    )
    row = conn.execute(
        "SELECT 1 FROM pipeline_health "
        "WHERE stage = 'fetch' "
        "AND message = ? "
        "LIMIT 1",
        (message,),
    ).fetchone()
    if row is not None:
        return
    conn.execute(
        "INSERT INTO pipeline_health (stage, status, message) "
        "VALUES ('fetch', 'WARN', ?)",
        (message,),
    )


def _wallet_drawdown_halted(
    portfolio: V6Portfolio, wallet_snap: WalletSnapshot
) -> bool:
    limit = max(0.0, float(portfolio.max_drawdown_halt_frac))
    return limit > 0.0 and wallet_snap.drawdown_pct_peak_frac >= limit


def _record_subminute_breaker_trip(
    conn, live_ctx: Optional["_LiveCommitContext"], breaker
) -> None:
    """B4: one-shot CRITICAL health row + operator alert when the sub-minute
    fill-rate breaker trips. Deduped over 10 minutes so a tripped breaker
    doesn't spam a row per checkpoint; the breaker itself keeps blocking
    placements regardless."""
    fill_rate = (
        f"{breaker.fill_rate:.3f}" if breaker.fill_rate is not None else "n/a"
    )
    message = (
        f"subminute breaker TRIPPED ({breaker.reason}): "
        f"fill_rate={fill_rate} over n_orders={breaker.n_orders}; "
        f"settled n={breaker.n_settled} pnl={breaker.settled_pnl} "
        f"stake={breaker.settled_stake} — sub-minute placements blocked "
        f"(baseline sleeves unaffected). Investigate subminute_fills, then "
        f"set CRYPTOBOT_SUBMINUTE_BREAKER_RESET to re-arm."
    )
    try:
        row = conn.execute(
            "SELECT 1 FROM pipeline_health "
            "WHERE stage='subminute' AND status='CRITICAL' "
            "AND created_at >= datetime('now', '-10 minutes') LIMIT 1",
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('subminute', 'CRITICAL', ?)",
                (message,),
            )
            if live_ctx is not None and live_ctx.notify is not None:
                live_ctx.notify(
                    "SUBMINUTE_BREAKER", message, wallet_id=None, critical=True
                )
    except Exception:  # noqa: BLE001 — alerting must never fail the tick
        logger.warning("subminute breaker trip record failed", exc_info=True)


def _record_dd_halt_health(
    conn, *, wallet_snap: WalletSnapshot, portfolio: V6Portfolio
) -> None:
    message = (
        "dd_halt active "
        f"wallet={wallet_snap.wallet_id} "
        f"drawdown_pct_peak_frac={wallet_snap.drawdown_pct_peak_frac:.6f} "
        f"limit={portfolio.max_drawdown_halt_frac:.6f}"
    )
    row = conn.execute(
        "SELECT 1 FROM pipeline_health "
        "WHERE stage='risk' AND status='CRITICAL' "
        "AND message = ? "
        "AND created_at >= datetime('now', '-60 seconds') "
        "LIMIT 1",
        (message,),
    ).fetchone()
    if row is not None:
        return
    conn.execute(
        "INSERT INTO pipeline_health (stage, status, message) "
        "VALUES ('risk', 'CRITICAL', ?)",
        (message,),
    )


# v2 DD-brake CRITICAL only fires at two operator-actionable thresholds; a
# shallow throttle (brake in [0.75, 1.0)) is routine noise and logs at INFO.
_V2_BRAKE_WARN_THRESHOLD: float = 0.75
# Dedup CRITICALs to at most one per message per 6h (was 10 min — a
# persistently-throttled sleeve fired ~52 CRITICALs in 12h live).
_V2_BRAKE_DEDUP_WINDOW = "-6 hours"


def _record_v2_brake_health(
    conn, *, brake: float, floor: float, dd: float
) -> None:
    """v2 sizing DD-brake telemetry: surface graduated throttling BEFORE the
    hard 40% halt via the existing pipeline_health CRITICAL path.

    CRITICAL fires only at the two operator-actionable thresholds, each
    deduped to at most one row per 6h via the pipeline_health lookback:
      * brake first crosses below 0.75 (meaningful DD throttle engaged),
      * brake reaches the floor (throttle maxed, approaching the hard halt).
    A shallow throttle (brake in [0.75, 1.0)) is routine and logs at INFO
    only — no pipeline_health row — so a mildly-drawn-down sleeve no longer
    spams the operator (it fired ~52 CRITICALs in 12h before this change).
    The CRITICAL messages carry NO varying numeric so the message-keyed
    dedup holds; the live dd/brake values are on the sizing rows and the
    dashboard.
    """
    if brake >= 1.0:
        return
    at_floor = brake <= floor + 1e-9
    if not at_floor and brake >= _V2_BRAKE_WARN_THRESHOLD:
        # Shallow throttle — informational only, no CRITICAL row.
        logger.info(
            "v2 sizing brake shallow: brake=%.3f (dd=%.3f) — throttle "
            "engaged but above the %.0f%% CRITICAL threshold.",
            brake,
            dd,
            _V2_BRAKE_WARN_THRESHOLD * 100,
        )
        return
    if at_floor:
        message = (
            "v2 sizing brake AT FLOOR: DD throttle maxed, stake pinned at "
            f"{floor:.0%} of base — approaching the 40% hard halt."
        )
    else:
        message = (
            "v2 sizing brake ENGAGED: DD throttle active, stake reduced "
            f"below {_V2_BRAKE_WARN_THRESHOLD:.0%} of base (graduated "
            "throttle before the 40% hard halt)."
        )
    try:
        row = conn.execute(
            "SELECT 1 FROM pipeline_health "
            "WHERE stage='risk' AND status='CRITICAL' "
            "AND message = ? "
            "AND created_at >= datetime('now', ?) "
            "LIMIT 1",
            (message, _V2_BRAKE_DEDUP_WINDOW),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('risk', 'CRITICAL', ?)",
                (message,),
            )
    except Exception:  # noqa: BLE001 — telemetry must never fail the tick
        logger.warning("v2 brake health record failed", exc_info=True)


def _emit_pre_gate_skip(
    conn,
    d: CellDecision,
    reason: str,
    tick: TickResult,
    *,
    mode: Optional[str] = None,
    wallet_group: Optional[str] = None,
) -> None:
    """Record a LIVE-only pre-gate skip (operator_stopped, transfer_lock,
    readiness_stale, funder_balance_low, clob_unavailable).

    These fire before the gate stack, so we don't have a top_ask or
    walked_vwap yet. The signals row uses the cheapest available context
    so the dashboard's skip breakdown still attributes the cell.
    """
    side = d.score.side or "YES"
    ask = _pick_ask(d, side)
    _mark_skip(d, reason)
    record_signal_skip(
        conn,
        created_at=utc_now_iso(),
        coin=d.coin, sleeve=d.sleeve.label, entry_minute=d.entry_minute,
        side=side, window_start_ts=d.window_start_ts,
        model_p=None if d.score.skip_reason is not None else d.score.model_p,
        ask=ask,
        edge=d.score.realized_edge if d.score.skip_reason is None else None,
        fee_rate_bps=d.fee_rate_bps,
        partial_ret_z_1h=d.score.partial_ret_z_1h,
        lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
        lut_hit_rate=d.score.lut_hit_rate,
        lut_n_cum=d.score.lut_n_cum,
        skip_reason=reason,
        mode=mode,
        wallet_group=wallet_group,
        offset_sec=getattr(d, "offset_sec", None),
    )
    _bump(tick.skips_by_reason, reason)
    tick.signals_written += 1


def _load_live_context(
    conn,
    portfolio: V6Portfolio,
    deps: TickDeps,
) -> tuple[
    dict[str, str],
    Optional[OperatorState],
    dict[str, GroupCapState],
    Optional[float],
    bool,
]:
    """Load all LIVE-routing context ONCE per tick (D-LIVE-20).

    Returns a 5-tuple of:
      * `mode_state` — `{wallet_id: 'LIVE'|'DRY'}` (empty when wallet_mode_state
        table is unreachable; pipeline falls back to DRY everywhere).
      * `operator_state` — singleton row, or None when the table is missing
        (Phase 1 pre-boot fixtures, never in production).
      * `group_state` — `{group_id: GroupCapState}` for cap evaluation.
      * `live_balance` — latest CLOB funder balance USD, or None when no
        OrderClient is wired.
      * `readiness_fresh` — True when a `live_readiness_reports.status=READY`
        row is within the cache TTL. LIVE wallets that fail this gate emit
        SKIP:readiness_stale.
    """
    # Mode state.
    mode_loader = deps.mode_state_loader or load_wallet_mode_state
    try:
        mode_state = mode_loader(conn)
    except Exception:  # noqa: BLE001
        logger.warning("pipeline: mode_state load failed", exc_info=True)
        mode_state = {}

    # Operator state.
    op_loader = deps.operator_state_loader or get_operator_state
    op_state: Optional[OperatorState]
    try:
        op_state = op_loader(conn)
    except Exception:  # noqa: BLE001
        op_state = None

    # Group state.
    group_loader = deps.group_state_loader or get_group_state
    try:
        group_state = group_loader(conn, portfolio)
    except Exception:  # noqa: BLE001
        logger.warning("pipeline: group_state load failed", exc_info=True)
        group_state = {}

    # Live wallet balance — fetched only when any wallet is LIVE AND we have
    # an OrderClient. Skip the network call otherwise so DRY-only deploys
    # stay zero-cost.
    live_balance: Optional[float] = None
    any_live = any(m == "LIVE" for m in mode_state.values())
    if any_live and deps.order_client is not None:
        if deps.live_balance_fetcher is not None:
            try:
                live_balance = deps.live_balance_fetcher()
            except Exception:  # noqa: BLE001
                logger.warning("pipeline: live_balance fetch raised", exc_info=True)
                live_balance = None
        else:
            try:
                live_balance = deps.order_client.check_balance()
            except Exception:  # noqa: BLE001
                logger.warning(
                    "pipeline: order_client.check_balance() raised", exc_info=True
                )
                live_balance = None

    # Readiness freshness — only consulted by LIVE-wallet skip gates.
    if deps.live_readiness_loader is not None:
        try:
            readiness_fresh = bool(deps.live_readiness_loader(conn))
        except Exception:  # noqa: BLE001
            readiness_fresh = False
    else:
        readiness_fresh = _default_readiness_fresh(conn)

    return mode_state, op_state, group_state, live_balance, readiness_fresh


def _default_readiness_fresh(conn) -> bool:
    """Default readiness loader: latest READY row within LIVE_READINESS_CACHE_TTL_S."""
    try:
        row = conn.execute(
            """
            SELECT status,
                   (julianday('now') - julianday(created_at)) * 86400 AS age_s
              FROM live_readiness_reports
             ORDER BY id DESC LIMIT 1
            """,
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    if row is None or row["status"] != "READY":
        return False
    try:
        age = float(row["age_s"])
    except (TypeError, ValueError):
        return False
    return age <= LIVE_READINESS_CACHE_TTL_S


def _live_pending_exposure_usd(
    group_state: dict[str, GroupCapState],
    wallet_snapshots: Optional[dict[str, WalletSnapshot]] = None,
) -> float:
    """LIVE exposure already funded from the CLOB wallet."""
    if not group_state and wallet_snapshots is not None:
        return sum(
            max(0.0, float(s.pending_exposure))
            for s in wallet_snapshots.values()
            if s.mode == LIVE
        )
    return sum(max(0.0, float(s.pending_usd)) for s in group_state.values())


def _live_cap_basis_usd(
    live_balance: Optional[float],
    live_pending_exposure_usd: float,
) -> Optional[float]:
    """Cash plus already-funded LIVE exposure for runtime cap accounting."""
    if live_balance is None:
        return None
    return max(0.0, float(live_balance)) + max(
        0.0, float(live_pending_exposure_usd)
    )


def _live_wallet_skip_reason(
    *,
    op_state: Optional[OperatorState],
    readiness_fresh: bool,
    live_balance: Optional[float],
    live_pending_exposure_usd: float = 0.0,
    all_group_caps_sum_usd: float,
    has_order_client: bool,
) -> Optional[str]:
    """LIVE-only gate set — returns SKIP reason or None.

    Order: clob_unavailable → operator_stopped → transfer_lock →
    readiness_stale → funder_balance_low. The order matches D-LIVE-12
    (STOPPED blocks LIVE only) and surfaces the earliest-blocking signal.
    """
    if not has_order_client:
        return "clob_unavailable"
    if op_state is None:
        return "operator_stopped"
    if op_state.state == TRANSFER_LOCK:
        return "transfer_lock"
    if op_state.state != LIVE:
        return "operator_stopped"
    if not readiness_fresh:
        return "readiness_stale"
    # Financial finding: a None live_balance with an order_client wired means
    # the CLOB balance fetch FAILED/timed out this tick (see
    # _load_live_context: it only sets live_balance when any wallet is LIVE
    # AND order_client is present, then None on exception). Previously the
    # funder_balance_low gate was guarded by `if live_balance is not None`,
    # so a failed fetch SKIPPED the floor check and the cap-scaling shrink —
    # LIVE orders fired against static caps with zero on-chain verification
    # (fail OPEN). Fail CLOSED instead: block LIVE placement for this tick.
    if has_order_client and live_balance is None:
        return "funder_balance_unknown"
    live_cap_basis = _live_cap_basis_usd(
        live_balance, live_pending_exposure_usd
    )
    # all_group_caps_sum_usd is 0 for a cap_tracks_equity group (funder floor
    # retired, funder_balance_floor_usd -> 0) as well as for ungrouped/isolated
    # topologies; this guard keeps the fixed floor inert in both cases.
    if live_cap_basis is not None and all_group_caps_sum_usd > 0:
        floor = all_group_caps_sum_usd - LIVE_BALANCE_TOLERANCE_USD
        if live_cap_basis < floor:
            return "funder_balance_low"
    return None


@dataclass(frozen=True)
class _LiveCtxInputs:
    """Everything :func:`_build_live_commit_context` LOADS, before assembly.

    Split out so the fast rungs can cache the loads (one ledger scan + the
    routing SELECTs + the wallet aggregation) across fires inside one second
    while still ASSEMBLING a fresh ``_LiveCommitContext`` per fire — the context
    carries a MUTABLE ``tick_group_live_delta``, and reusing that object across
    scans would carry a previous scan's within-scan exposure into the next one.
    """

    mode_state: dict[str, str]
    op_state: Optional[OperatorState]
    group_state: dict[str, GroupCapState]
    live_balance: Optional[float]
    readiness_fresh: bool
    all_group_caps_sum_usd: float
    wallet_snaps: dict[str, WalletSnapshot]


def _load_live_ctx_inputs(
    conn,
    portfolio: V6Portfolio,
    deps: TickDeps,
    *,
    tick: Optional[TickResult] = None,
) -> _LiveCtxInputs:
    """The LOAD half of :func:`_build_live_commit_context` (see that docstring).

    ``tick`` (when given) is stamped with ``ms_live_ctx`` for the
    ``_load_live_context`` leg only, so the metric keeps meaning what its
    docstring says (routing state + balance fetch, not the ledger scan below).
    """
    _t_ctx = time.monotonic()
    mode_state, op_state, group_state, live_balance, readiness_fresh = (
        _load_live_context(conn, portfolio, deps)
    )
    if tick is not None:
        tick.ms_live_ctx = (time.monotonic() - _t_ctx) * 1000.0
    # Correctness finding #22 (CORR-006): isolated_4 topology has
    # wallet_groups=() so the prior `all_group_caps_sum_usd()` returned 0 and
    # the funder_balance_low gate never fired. `funder_balance_floor_usd` picks
    # the structurally-correct formula per topology.
    all_group_caps_sum_usd = portfolio.funder_balance_floor_usd()
    wallet_snaps = get_wallet_snapshots(
        conn,
        portfolio,
        mode_state=mode_state,
        group_state=group_state,
        live_wallet_balance=live_balance,
    )
    return _LiveCtxInputs(
        mode_state=mode_state,
        op_state=op_state,
        group_state=group_state,
        live_balance=live_balance,
        readiness_fresh=readiness_fresh,
        all_group_caps_sum_usd=all_group_caps_sum_usd,
        wallet_snaps=wallet_snaps,
    )


def _assemble_live_commit_context(
    loaded: _LiveCtxInputs,
    deps: TickDeps,
    *,
    tick_group_live_delta: dict[str, float],
    op_state: Optional[OperatorState] = None,
) -> tuple[_LiveCommitContext, dict[str, WalletSnapshot]]:
    """The ASSEMBLE half — pure arithmetic over ``loaded``, no I/O.

    ``op_state`` overrides the loaded operator row: the fast rungs re-read that
    ONE singleton row fresh on every fire even when the rest of the context is
    served from the per-second cache, so an operator STOP still binds at the
    next fire rather than at the next second.
    """
    op = loaded.op_state if op_state is None else op_state
    live_pending_exposure_usd = _live_pending_exposure_usd(
        loaded.group_state, loaded.wallet_snaps
    )
    live_ctx = _LiveCommitContext(
        mode_state=loaded.mode_state,
        operator_state=op,
        group_state=loaded.group_state,
        live_balance=loaded.live_balance,
        live_pending_exposure_usd=live_pending_exposure_usd,
        live_cap_basis_usd=_live_cap_basis_usd(
            loaded.live_balance, live_pending_exposure_usd
        ),
        all_group_caps_sum_usd=loaded.all_group_caps_sum_usd,
        # Computed ONCE so every LIVE wallet sees the same blocker within this
        # tick/scan (D-LIVE-20).
        live_skip_reason=_live_wallet_skip_reason(
            op_state=op,
            readiness_fresh=loaded.readiness_fresh,
            live_balance=loaded.live_balance,
            live_pending_exposure_usd=live_pending_exposure_usd,
            all_group_caps_sum_usd=loaded.all_group_caps_sum_usd,
            has_order_client=deps.order_client is not None,
        ),
        order_client=deps.order_client,
        notify=deps.notify,
        tick_group_live_delta=tick_group_live_delta,
    )
    return live_ctx, loaded.wallet_snaps


def _build_live_commit_context(
    conn,
    portfolio: V6Portfolio,
    deps: TickDeps,
    *,
    tick_group_live_delta: dict[str, float],
    tick: Optional[TickResult] = None,
) -> tuple[_LiveCommitContext, dict[str, WalletSnapshot]]:
    """Load the LIVE routing context ONCE and build the shared commit context.

    Returns ``(live_ctx, wallet_snaps)``. THE single assembly point for both
    committers — the tick's Phase B and the fast rungs — which had already
    drifted after one release: the fast rung passed a throwaway empty
    ``tick_group_live_delta`` (so its within-scan group-cap accounting was dead)
    and called ``funder_balance_floor_usd()`` twice.

    ``tick`` (when given) is stamped with ``ms_live_ctx`` for the
    ``_load_live_context`` leg only, so the metric keeps meaning what its
    docstring says (routing state + balance fetch, not the ledger scan below).
    """
    loaded = _load_live_ctx_inputs(conn, portfolio, deps, tick=tick)
    return _assemble_live_commit_context(
        loaded, deps, tick_group_live_delta=tick_group_live_delta
    )


# ---------------------------------------------------------------------------
# Placement epoch — the "something was placed" invalidation signal.
#
# Moves at every point this process can change the exposure/cap/DD view a commit
# stage sizes against:
#   * a LIVE `live_order_attempts` reservation — the row that consumes group-cap
#     pending and, via `_process_live_post`, writes the `subminute_fills` row the
#     fill-rate breaker reads. Bumped INSIDE
#     `order_attempts.reserve_live_order_attempt`, so EVERY reservation site
#     invalidates by construction — including `execution.scale_in`'s T3 top-up,
#     which reserves on its own 1Hz held-position thread and cannot import this
#     module (pipeline -> held_position_pass -> scale_in is already a chain).
#   * the within-tick exposure application (the DRY + LIVE-accept ledger write),
#     bumped by `_apply_within_tick_exposure` below.
#
# The counter itself lives in the leaf module (see `order_attempts`); these are
# the names the rest of this module and the tests use.
_bump_placement_epoch = bump_placement_epoch
#: Current epoch. Called, never copied into a local at import time — see
#: :func:`order_attempts.placement_epoch`.
_placement_epoch = placement_epoch


@dataclass
class _FastCommitStage:
    """One integer second's cached commit-stage preamble for the fast rungs.

    A qualifying second fires up to ~16x (250ms sweep + WS pushes) and each fire
    that reaches stage 6 re-paid the whole preamble — the routing SELECTs, the
    wallet-snapshot aggregation over the full ledger, the fill-rate breaker, the
    ramp row and a `shutil.disk_usage` syscall — only to reach a `_commit_cell`
    that usually SKIPS (out of band, dedup, first-attempt-consumed). On the live
    box that preamble measures ~4-5ms; the fire that finally PLACES is typically
    the 2nd..Nth of its second, so it is exactly the placement path that pays it.

    ⚠️ INVALIDATION CONTRACT — all five must hold to serve, and the FIRST TWO are
    the ones a reviewer must check, because a stale cap/DD/balance view is how a
    halted wallet places:
      1. ``epoch`` — ANY reservation or ledger write in this process since the
         load (see :func:`_bump_placement_epoch`). "The DD-halt and balance must
         never go stale ACROSS a placement" is exactly this clause. Sampled AFTER
         the probe SQL on the serve path — see :func:`_fast_commit_stage`.
      2. ``ledger_probe`` — ``capital.ledger_version(conn)``: any ledger mutation
         from anywhere (an F3 settlement, a historical stop-loss booking, the order
         reconciler linking a fill) moves it. Costs one narrow aggregate SQL
         (~0.5-1ms) against the ~4-5ms it guards.
      3. same integer second — bounds staleness of the out-of-process routing
         rows (`wallet_mode_state`, `live_readiness_reports`) to < 1s, which is
         the granularity the 1Hz tick already has.
      4. same ``conn`` / ``portfolio`` / ``deps`` OBJECTS (identity, not id()) —
         a different connection sees a different snapshot, and the deps bundle
         carries the balance fetcher + order client.
      5. ``config.FAST_TRIGGER_ENABLED`` — master off never caches.

    The operator row is deliberately NOT covered: :func:`_fast_commit_stage`'s
    caller re-reads that ONE singleton row on every fire so an operator STOP
    binds at the next fire, not at the next second.
    """

    second: int
    refs: tuple           # (conn, portfolio, deps) — re-checked by identity
    epoch: int
    ledger_probe: tuple
    loaded: _LiveCtxInputs
    tick_breaker: BreakerState
    disk_free_mb: float


#: One stage PER CONNECTION, keyed on ``id(conn)``. The two fast rungs never
#: share a connection — the 250ms sweep runs on rotating APScheduler pool threads
#: and the event rung on its own dedicated thread, all served by
#: `ThreadLocalConnections` — so a single global slot made every cross-rung fire
#: both MISS and EVICT the other rung's stage, which is precisely the ~3-4ms the
#: cache exists to save on the fire that PLACES. Bounded by the number of
#: connections alive in ONE second: entries from an older second are dropped on
#: store, and clause 4's identity re-check makes a recycled ``id()`` a miss
#: rather than a wrong serve.
_FAST_COMMIT_STAGE: "dict[int, _FastCommitStage]" = {}
_FAST_COMMIT_STAGE_LOCK = threading.Lock()


def clear_fast_commit_stage() -> None:
    """Drop every cached commit-stage preamble. Test/ops hook — clears ALL
    connections' entries, so an operator hook is not per-thread."""
    with _FAST_COMMIT_STAGE_LOCK:
        _FAST_COMMIT_STAGE.clear()


def _fast_commit_stage(
    conn,
    portfolio: V6Portfolio,
    deps: TickDeps,
    *,
    now_ts: int,
    tick: TickResult,
) -> tuple[_FastCommitStage, bool]:
    """``(stage, cache_hit)`` for a fast rung's commit stage.

    Called INSIDE the single-flight lock (like the load it replaces), so the
    probe below cannot race a tick commit. See :class:`_FastCommitStage` for the
    invalidation contract.
    """
    refs = (conn, portfolio, deps)
    cached = _FAST_COMMIT_STAGE.get(id(conn)) if config.FAST_TRIGGER_ENABLED else None
    if (
        cached is not None
        and cached.second == int(now_ts)
        and len(cached.refs) == len(refs)
        and all(a is b for a, b in zip(cached.refs, refs))
    ):
        try:
            probe = ledger_version(conn)
        except Exception:  # noqa: BLE001 — an unreadable probe must never serve
            probe = None
        # ⚠️ ORDER: the epoch is sampled AFTER the probe SQL, never before. The
        # bumps that exist are precisely the ones the probe CANNOT see (a bare
        # `live_order_attempts` reservation — the attempts table is not in the
        # aggregate), so an epoch read taken before a ~0.5-1ms SQL would let a
        # reservation land invisibly in between and serve a stage whose
        # `group_state.pending_usd` omits it — one bet over the group cap.
        if (
            probe is not None
            and probe == cached.ledger_probe
            and cached.epoch == _placement_epoch()
        ):
            return cached, True

    # Both invalidation signals are read BEFORE the load, exactly like
    # `capital._cached_ledger_rows`: a write landing DURING the load then moves
    # them past what we stored, so the next fire rescans. Read them AFTER and a
    # concurrent write would be recorded as "already included" — a stale serve.
    epoch_before = _placement_epoch()
    try:
        probe = ledger_version(conn)
    except Exception:  # noqa: BLE001 — no probe => this load is not cacheable
        probe = None
    loaded = _load_live_ctx_inputs(conn, portfolio, deps, tick=tick)
    stage = _FastCommitStage(
        second=int(now_ts),
        refs=refs,
        epoch=epoch_before,
        ledger_probe=probe,
        loaded=loaded,
        tick_breaker=evaluate_breaker(conn),
        disk_free_mb=deps.get_disk_free_mb(),
    )
    if config.FAST_TRIGGER_ENABLED and probe is not None:
        with _FAST_COMMIT_STAGE_LOCK:
            # Evict every OTHER connection's stale second while we hold the lock:
            # those can never serve again (clause 3), so this keeps the dict at
            # the rungs actually live in this second.
            for key in [
                k for k, v in _FAST_COMMIT_STAGE.items() if v.second != stage.second
            ]:
                del _FAST_COMMIT_STAGE[key]
            _FAST_COMMIT_STAGE[id(conn)] = stage
    return stage, False


# A5 latency: next-window Gamma prefetch (see _prefetch_next_window_markets).
_MARKET_PREFETCH_TAIL_S = 20
_MARKET_PREFETCH_MAX_PER_TICK = 2


def _prefetch_next_window_markets(
    conn,
    portfolio: V6Portfolio,
    fetch_market: Callable,
    now_ts: int,
    order_client=None,
    book_prefetch=None,
) -> None:
    """A5 latency: warm the Gamma per-window market cache for the NEXT window
    during the last ``_MARKET_PREFETCH_TAIL_S`` seconds of the current one.

    Runs at the END of the tick — strictly off the decision path — so the
    first qualifying tick of a fresh window (including a second-0 entry)
    finds market metadata already cached instead of paying an inline Gamma
    GET between decision and order POST. fetch_active_market caches only
    SUCCESSFUL resolutions, so a market not listed yet simply retries on the
    next tail tick, and the per-tick budget bounds worst-case added network
    to ``_MARKET_PREFETCH_MAX_PER_TICK`` GETs. Called with the UNWRAPPED
    fetcher so prefetch time never pollutes the ms_gamma/n_gamma
    decision-path latency stats.

    A8 latency: also pushes the NEXT window's yes/no token ids to the book
    prefetch chain so the WS mirror subscribes / the REST prefetcher warms
    them BEFORE the boundary. Without this the chain first learns those ids
    from ``_resolve_net_cells`` on the first QUALIFYING tick of the fresh
    window, so that tick pays an inline REST book fetch (~50-90ms) at the
    entry second — exactly what the prefetch chain exists to avoid.
    """
    from cryptobot.venues.types import FeeUnknown as _FeeUnknown

    budget = _MARKET_PREFETCH_MAX_PER_TICK
    next_tokens: list[str] = []
    for interval_min in sorted({s.interval_min for s in portfolio.sleeves}):
        # A disabled interval keeps its sleeve loaded (for T3 + dashboard)
        # but can never be entered, so warming its next-window markets is pure
        # waste — and worse, `sorted()` puts 5 first, so at a 15m boundary (which
        # is always a 5m boundary too) the dead interval would spend the shared
        # per-tick GET budget before the interval that can actually trade.
        if _interval_entry_disabled(int(interval_min)):
            continue
        span = int(interval_min) * 60
        if span - (int(now_ts) % span) > _MARKET_PREFETCH_TAIL_S:
            continue
        next_window_start = (int(now_ts) // span) * span + span
        for coin in config.COINS:
            already_cached = _venue_market_cache_has(
                coin, int(interval_min), next_window_start
            )
            # Fetch (network) only cache misses, and only within the per-tick
            # GET budget. A cache HIT still needs its tokens gathered below —
            # a cached fetch_market call is a cheap in-process return, and
            # _resolve_net_cells re-notes the CURRENT window on every
            # qualifying tick, so the next window's ids must be re-asserted on
            # EVERY tail tick or they vanish after the first. Hence we no
            # longer `continue` on a cache hit the way the pre-A8 code did.
            if not already_cached:
                if budget <= 0:
                    continue
                budget -= 1
            try:
                market = fetch_market(
                    coin,
                    interval_min=int(interval_min),
                    window_start_ts=next_window_start,
                    conn=conn,
                    now_ts=float(now_ts),
                )
            except Exception:  # noqa: BLE001 — prefetch is best-effort
                logger.debug("next-window market prefetch failed", exc_info=True)
                continue
            if market is None or isinstance(market, _FeeUnknown):
                continue
            # A1 warm for the NEXT window too, only on the FRESH resolution (a
            # cached market was already warmed on the tick that resolved it):
            # pre-warm the CLOB client's static market-info caches (tick size /
            # neg-risk) at prefetch time so the window's FIRST
            # create_market_order is pure local signing + one POST (Pass 1's
            # warm only fires after a cell already qualified). Fire-and-forget;
            # test stubs lack the method.
            if (
                not already_cached
                and order_client is not None
                and getattr(market, "condition_id", None)
            ):
                _warm = getattr(order_client, "warm_market_info_async", None)
                if _warm is not None:
                    try:
                        _warm(market.condition_id)
                    except Exception:  # noqa: BLE001 — warm is best-effort
                        logger.debug("next-window warm raised", exc_info=True)
            # A8: gather this market's yes/no ids for the book prefetch push.
            yes_t = getattr(market, "yes_token_id", None)
            no_t = getattr(market, "no_token_id", None)
            if yes_t:
                next_tokens.append(yes_t)
            if no_t:
                next_tokens.append(no_t)

    # A8 latency: hand the book prefetch chain the UNION of its currently-noted
    # (CURRENT window) set and the NEXT window's ids. note_tokens REPLACES the
    # tracked set wholesale on each source (the WS mirror re-subscribes on the
    # SAME socket; the REST prefetcher re-targets), and _resolve_net_cells
    # re-notes the CURRENT window on every qualifying tick — so we MUST include
    # the current set here or the current window stops being served for the
    # ~20s tail. The prefetch/WS asset set therefore roughly DOUBLES (current +
    # next windows) for the tail ~20s; that is intentional and off the decision
    # path (this runs at the END of the tick). Best-effort: a bookkeeping
    # failure must never fail the tick.
    if book_prefetch is not None and next_tokens:
        try:
            _noted = getattr(book_prefetch, "noted_tokens", None)
            current = list(_noted()) if _noted is not None else []
            book_prefetch.note_tokens(current + next_tokens)
        except Exception:  # noqa: BLE001 — prefetch bookkeeping is best-effort
            logger.debug(
                "next-window book_prefetch note_tokens failed", exc_info=True
            )


def run_scanner_tick(
    conn,
    portfolio: V6Portfolio,
    *,
    now_ts: int,
    deps: TickDeps,
) -> TickResult:
    """Execute one tick. Per-cell errors are caught + logged; the tick
    continues to give downstream cells a chance.
    """
    t0 = time.monotonic()
    tick = TickResult(tick_ts=now_ts)
    tick._t0_monotonic = t0

    # A0 latency instrumentation: wrap the two per-cell network fetchers so
    # EVERY call site accumulates into the tick's per-phase counters without
    # touching each site. `deps` is rebound
    # to a shallow copy so callers' TickDeps instances are never mutated
    # (tests reuse fixtures across ticks).
    _orig_fetch_market = deps.fetch_market
    _orig_fetch_book = deps.fetch_book
    _orig_fetch_books = deps.fetch_books

    def _timed_fetch_market(*args, **kwargs):
        _s = time.monotonic()
        try:
            return _orig_fetch_market(*args, **kwargs)
        finally:
            tick.ms_gamma += (time.monotonic() - _s) * 1000.0
            tick.n_gamma_calls += 1

    def _timed_fetch_book(*args, **kwargs):
        _s = time.monotonic()
        try:
            return _orig_fetch_book(*args, **kwargs)
        finally:
            tick.ms_book += (time.monotonic() - _s) * 1000.0
            tick.n_book_calls += 1

    def _timed_fetch_books(*args, **kwargs):
        _s = time.monotonic()
        try:
            return _orig_fetch_books(*args, **kwargs)
        finally:
            tick.ms_book += (time.monotonic() - _s) * 1000.0
            tick.n_book_calls += 1

    deps = dataclass_replace(
        deps,
        fetch_market=_timed_fetch_market,
        # Both book fetchers are Optional: wrap only what is actually wired, so
        # `None` still reads as "no fetcher" at the call sites downstream rather
        # than becoming a wrapper that raises on call.
        fetch_book=(
            _timed_fetch_book if _orig_fetch_book is not None else None
        ),
        fetch_books=(
            _timed_fetch_books if _orig_fetch_books is not None else None
        ),
    )

    # ------------------------------------------------------------------
    # Phase A — fetch + score
    # ------------------------------------------------------------------
    current_minute = (now_ts // 60) % 60
    # Spot fetch is batched on a slower cadence than the 1Hz scheduler so the
    # public market-data endpoint stays below its per-IP weight limit while the
    # scanner still evaluates entry timing every second.
    _t_spot = time.monotonic()
    spots = deps.fetch_spots(
        now_ts=float(now_ts),
        tick_seconds=config.SPOT_FETCH_INTERVAL_SECONDS,
    )
    tick.ms_spot = (time.monotonic() - _t_spot) * 1000.0
    if spots is None:
        # Spot source is down; we won't be able to score. Don't write signal rows
        # (no per-cell context yet). Record one health row per fetch bucket so
        # the dashboard is honest without growing pipeline_health at 1Hz.
        _record_fetch_abort(conn, now_ts)
        conn.commit()
        tick.duration_ms_total = (time.monotonic() - t0) * 1000.0
        return tick

    # Stamp per-coin partial-ret engine + record live spot for D3
    # walk-mask anchor (yes_pre1_anchor at entry_ts-60s).
    for coin, spot in spots.items():
        deps.partial_ret_engine.update(
            coin, live_spot=spot.price, now_ts=now_ts,
        )

    # D-LIVE-20: load LIVE routing context ONCE per tick and build the shared
    # commit context from it (see _build_live_commit_context — the same call the
    # fast rungs make, so the two committers cannot drift).
    # `tick_group_live_delta` is the per-group running LIVE delta the group-cap
    # gate reads as cells 2..N commit; created here so the context can carry it
    # (parallel to `tick_pending_delta` in Phase B).
    tick_group_live_delta: dict[str, float] = {}
    live_ctx, pre_score_wallets = _build_live_commit_context(
        conn,
        portfolio,
        deps,
        tick_group_live_delta=tick_group_live_delta,
        tick=tick,
    )

    cell_decisions: list[CellDecision] = []
    # A2 latency: qualifying cells buffered for the batched market/book
    # resolution after the scoring loop (see _resolve_net_cells).
    net_cells: list[_NetCell] = []
    for sleeve in portfolio.sleeves:
        # ONE shared gate list (`_sleeve_entry_gate`) so this loop and the
        # fast-trigger rungs can never disagree about whether the sleeve may
        # open an entry this second — the fast rung used to reproduce four of
        # these five gates and silently drop the hard DD halt.
        #
        # The three SILENT reasons make no API calls and write no signals row:
        # at 1Hz a row per sleeve per coin for a permanently-off interval (or
        # for every non-checkpoint second) would flood the signals table. The
        # v6 loader already drops checkpoint sleeves entirely when
        # SUBMINUTE_ENABLED is off; the gate's config re-check is
        # defense-in-depth for the kill-switch, which must work without a
        # reboot-triggering config reload.
        wallet_snap = pre_score_wallets.get(sleeve.label)
        gate_reason = _sleeve_entry_gate(
            sleeve,
            now_ts,
            current_minute=current_minute,
            portfolio=portfolio,
            wallet_snap=wallet_snap,
        )
        if gate_reason in _SILENT_SLEEVE_GATES:
            continue
        # B1: sub-minute checkpoint sleeves qualify by offset-from-window-open,
        # not by entry minute. The gate above already vetoed a non-qualifying
        # second, so this re-derivation always resolves.
        cell_offset_sec: Optional[int] = None
        if getattr(sleeve, "checkpoint_offset_set", ()):
            cell_offset_sec = _qualifying_offset_sec(sleeve, now_ts)
            em = int(cell_offset_sec or 0) // 60
        else:
            em = _qualifying_entry_minute(sleeve, current_minute)

        window_start = _window_start_ts(now_ts, sleeve.interval_min)
        window_end = window_start + sleeve.interval_min * 60

        if gate_reason == "not_entry_minute":
            # Not at a qualifying entry-minute — DO NOT make API calls.
            # A6 hot-path hygiene: persist the not_entry_minute rows ONCE
            # per minute instead of at 1Hz. The 1Hz repetition was pure
            # duplication (~70k rows/day toward the multi-GB signals table /
            # EC2 disk creep) — the once-a-minute row remains the liveness
            # marker, and the in-tick SSE skip counters below still bump
            # every tick. Keyed to the FIRST second that reaches the sleeve
            # loop (review finding): the ENTRY_SKIP_SECONDS early-return
            # consumes seconds [0, N), so a literal second-0 key would write
            # ZERO rows whenever that lever is re-enabled. A tick lost to
            # scheduler jitter drops that minute's row — the gap IS the
            # observability, same policy as the skip lever itself.
            if now_ts % 60 == 0:
                for coin in config.COINS:
                    record_signal_skip(
                        conn,
                        created_at=utc_now_iso(),
                        coin=coin, sleeve=sleeve.label, entry_minute=em or -1,
                        side="YES", window_start_ts=window_start,
                        model_p=None, ask=None, edge=None, fee_rate_bps=None,
                        partial_ret_z_1h=None, lut_p_bucket=None,
                        lut_hit_rate=None, lut_n_cum=None,
                        skip_reason="not_entry_minute",
                    )
                    tick.signals_written += 1
            for coin in config.COINS:
                _bump(tick.skips_by_reason, "not_entry_minute")
            continue

        if gate_reason is not None:
            # The two LOUD gates — schedule_filter (hour/dow) and the hard
            # dd_halt — write one skip row per coin under the gate's own reason
            # string. dd_halt additionally emits its health row.
            if gate_reason == "dd_halt":
                _record_dd_halt_health(
                    conn, wallet_snap=wallet_snap, portfolio=portfolio
                )
            for coin in config.COINS:
                record_signal_skip(
                    conn,
                    created_at=utc_now_iso(),
                    coin=coin, sleeve=sleeve.label, entry_minute=em,
                    side="YES", window_start_ts=window_start,
                    model_p=None, ask=None, edge=None, fee_rate_bps=None,
                    partial_ret_z_1h=None, lut_p_bucket=None,
                    lut_hit_rate=None, lut_n_cum=None,
                    skip_reason=gate_reason,
                    offset_sec=cell_offset_sec,
                )
                _bump(tick.skips_by_reason, gate_reason)
                tick.signals_written += 1
            continue

        # v2 sizing DD-brake telemetry. Past the hard-halt check the sleeve is
        # NOT halted (0 <= dd < the hard DD halt,
        # config.MAX_DRAWDOWN_HALT_FRAC), but the graduated brake may already be
        # throttling stake. Only checkpoint (v2) sleeves size through the brake,
        # and this loop reaches here for a checkpoint sleeve ONLY at a qualifying
        # configured checkpoint second, so the alert fires exactly when the brake is live.
        if (
            wallet_snap is not None
            and getattr(sleeve, "checkpoint_offset_set", ())
        ):
            _dd = wallet_snap.drawdown_pct_peak_frac
            _brake = sizing_v2.brake_multiplier(_dd)
            if _brake < 1.0:
                _record_v2_brake_health(
                    conn,
                    brake=_brake,
                    floor=config.V2_BRAKE_FLOOR,
                    dd=_dd,
                )

        for coin in config.COINS:
            tick.cells_evaluated += 1
            spot = spots[coin]
            spot_age_s = max(0.0, float(now_ts) - float(spot.publish_time))
            twap_open = _window_open_price(
                deps, coin=coin, sleeve=sleeve, window_start_ts=window_start
            )

            # Backtest samples each entry minute once. Live still runs at
            # 1Hz for top-ups, but only after a real initial fill exists.
            # B1: sub-minute cells use the per-OFFSET latch + one-shot slot
            # (no top-ups) — every checkpoint in a window is its own
            # admissible entry.
            if cell_offset_sec is not None:
                _latched = not claim_subminute_scan(
                    conn,
                    coin=coin,
                    sleeve=sleeve.label,
                    offset_sec=cell_offset_sec,
                    window_start_ts=window_start,
                ) and not subminute_slot_seen(
                    conn,
                    coin=coin,
                    sleeve=sleeve.label,
                    offset_sec=cell_offset_sec,
                    window_start_ts=window_start,
                )
            else:
                _latched = not _claim_entry_scan_for_key(
                    conn,
                    coin=coin,
                    sleeve_label=sleeve.label,
                    entry_minute=em,
                    window_start_ts=window_start,
                ) and not _initial_entry_bet_seen_for_key(
                    conn,
                    coin=coin,
                    sleeve_label=sleeve.label,
                    entry_minute=em,
                    window_start_ts=window_start,
                )
            if _latched:
                score = ScoreResult(
                    model_p=float("nan"), side="YES", edge=float("nan"),
                    realized_edge=float("nan"), lut_p_bucket=-1,
                    lut_hit_rate=None, lut_n_cum=0, partial_ret_z_1h=None,
                    skip_reason="entry_scan_latched",
                )
                cell_decisions.append(
                    CellDecision(
                        coin=coin,
                        sleeve=sleeve,
                        entry_minute=em,
                        window_start_ts=window_start,
                        window_end_ts=window_end,
                        score=score,
                        yes_book=None,
                        no_book=None,
                        spot_age_s=spot_age_s,
                        fee_rate_bps=None,
                        poly_market_id=None,
                        poly_token_id_yes=None,
                        poly_token_id_no=None,
                        offset_sec=cell_offset_sec,
                        strike_price=spot.price,
                        window_open_spot=twap_open,
                    )
                )
                continue

            # First run the cheap feature/RF/LUT path. During post-boot
            # partial-ret warmup this returns SKIP:no_feature, so we avoid
            # unnecessary Polymarket market/book calls on cells that cannot
            # possibly bet.
            # No fee_rate_bps here BY CONSTRUCTION: the market is not resolved
            # yet at this point, so the predict.fun tent uses its config
            # default. Nothing bets off this score — `_resolve_net_cells`
            # rescores every surviving cell with the market's real fee.
            yes_ask = no_ask = float("nan")
            score = _score_cell_for(
                coin=coin,
                sleeve=sleeve,
                entry_minute=em,
                window_start_ts=window_start,
                live_spot=spot.price,
                yes_ask=yes_ask,
                no_ask=no_ask,
                offset_sec=cell_offset_sec,
                deps=deps,
                twap_open=twap_open,
            )
            if score.skip_reason is None:
                # A2 latency: DEFER the market + book fetch. Qualifying cells
                # are buffered so every cell's books ride ONE POST /books
                # round trip (plus per-window-cached Gamma metadata) in
                # _resolve_net_cells below, instead of 2 sequential GETs per
                # cell here. The CellDecision is built there with identical
                # rescore semantics.
                net_cells.append(
                    _NetCell(
                        sleeve=sleeve,
                        entry_minute=em,
                        window_start=window_start,
                        window_end=window_end,
                        coin=coin,
                        spot=spot,
                        spot_age_s=spot_age_s,
                        score=score,
                        twap_open=twap_open,
                        offset_sec=cell_offset_sec,
                    )
                )
                continue

            # Cheap-path skip: no network context exists — build the decision
            # inline exactly as before (books/tokens/fee all None).
            decision = CellDecision(
                coin=coin,
                sleeve=sleeve,
                entry_minute=em,
                window_start_ts=window_start,
                window_end_ts=window_end,
                score=score,
                yes_book=None,
                no_book=None,
                spot_age_s=spot_age_s,
                fee_rate_bps=None,
                poly_market_id=None,
                poly_token_id_yes=None,
                poly_token_id_no=None,
                fee_unknown=False,
                offset_sec=cell_offset_sec,
                strike_price=spot.price,
                window_open_spot=twap_open,
            )
            cell_decisions.append(decision)

    # A2 latency: resolve every buffered qualifying cell — Gamma metadata
    # (per-window cached) + ONE batched POST /books + rescore — and append
    # the resulting decisions. Bettable-cell relative order is preserved
    # (buffered cells keep their sleeve×coin iteration order; inline-built
    # decisions above are all non-bettable skips).
    if net_cells:
        cell_decisions.extend(
            _resolve_net_cells(
                net_cells, conn=conn, deps=deps, portfolio=portfolio,
                now_ts=now_ts, tick=tick,
            )
        )

    tick.cell_decisions = list(cell_decisions)

    # ------------------------------------------------------------------
    # Phase B — min_agreement gate + per-cell commit
    # ------------------------------------------------------------------
    same_side_counts: dict[tuple, int] = {}
    for d in cell_decisions:
        if d.score.skip_reason is not None:
            continue
        key = (d.sleeve.label, d.window_start_ts, d.entry_minute, d.score.side)
        same_side_counts[key] = same_side_counts.get(key, 0) + 1

    disk_free_mb = deps.get_disk_free_mb()
    # Performance finding #5 (PERF-02): a second `get_wallet_snapshots`
    # call here ran the same unbounded ledger scan + per-wallet
    # aggregation that Phase A (line ~611) already produced. Phase B's
    # baseline equals Phase A's because pending exposure is tracked
    # separately via `tick_pending_delta` / `tick_group_live_delta`
    # within the tick. Reuse the Phase A snapshot to halve the ledger
    # scan cost at 1 Hz (~86,400 scans/day eliminated).
    wallet_snaps = pre_score_wallets
    # Running pending-exposure delta within this tick. The pre-tick snapshot
    # is stale for cells 2..N after the first record_bet — without this
    # the exposure gate sees pre-tick exposure for every cell, allowing
    # multi-cell ticks to jointly exceed the wallet cap (review finding #5).
    tick_pending_delta: dict[str, float] = {}

    # A9 latency: commit PLACE-candidates before skip cells. cell_decisions
    # is built skips-first (Phase A appends its cheap inline skips as it
    # scores; _resolve_net_cells extends the qualifying cells last), so
    # iterating it directly makes every skip cell's signals INSERT run
    # BEFORE the first order POST of the tick. Reordering is safe by
    # construction: same_side_counts is precomputed above over the FULL set,
    # and a skip cell (score.skip_reason set) returns from _commit_cell
    # after writing only its own signals row — no exposure delta, no slot
    # state, nothing a placing cell's gates read. Order WITHIN each
    # partition is preserved (stable), and tick.cell_decisions keeps the
    # original order for the SSE snapshot.
    placing, skips = [], []
    for d in cell_decisions:
        (placing if d.score.skip_reason is None else skips).append(d)
    phase_b_cells = placing + skips

    # A10 latency: hoist the sub-minute breaker + ramp derivations to ONCE
    # per tick. Both read only subminute_fills + settled ledger rows —
    # inputs that change on a POST or a settlement, never between the cells
    # of one tick's decision set — yet they ran per qualifying sub-minute
    # cell INSIDE the BEGIN IMMEDIATE (up to 4 SELECTs on the order path).
    # The trip decision for the FIRST placing cell is identical (nothing has
    # POSTed yet this tick); cells 2..N now see the tick-start state instead
    # of re-reading after each POST — bounded by one tick's orders against a
    # SUBMINUTE_BREAKER_WINDOW-deep rolling window.
    tick_breaker: Optional[BreakerState] = None
    if any(
        d.offset_sec is not None and d.score.skip_reason is None
        for d in cell_decisions
    ):
        tick_breaker = evaluate_breaker(conn)

    # A11 latency: per-entry immediate-fill GETs queued by _commit_cell.
    # Flushed concurrently AFTER the cell loop (and BEFORE the held-position
    # pass, which reads live_filled_size) — see the flush below.
    deferred_fill_gets: list[_DeferredFillGet] = []

    # LEVER 1 (CRYPTOBOT_PHASE_B_PARALLEL_POST): the fan-out collector. The
    # GUARD IS HERE AT THE LOOP LEVEL, not inside _commit_cell — flag OFF passes
    # `parallel_posts=None`, so every cell (DRY, skip, and LIVE) takes the exact
    # existing serial path (POST inline). Flag ON passes the list: a LIVE cell
    # that reaches the POST stage reserves + collects instead of POSTing, and
    # _flush_parallel_posts (below) fires the POSTs concurrently then replays
    # each cell's post-POST finish serially in cell order.
    #
    # 2026-07-25 latency audit: ALSO require >1 placing cell. `_parallel_collect`
    # runs a single call INLINE on the tick thread (`if len(calls) <= 1`), so at
    # n=1 the flag buys ZERO parallelism while still deferring an already-signed,
    # already-reserved POST behind every remaining skip cell's signals INSERT —
    # partially undoing the A9 reordering directly above. Measured: ~95% of
    # order-bearing ticks carry exactly one order (avg n_orders 1.02/1.06), so
    # this removes the deferral on almost every order tick and keeps the genuine
    # multi-order fan-out untouched.
    parallel_posts: Optional[list[_PreparedPost]] = (
        [] if (config.PHASE_B_PARALLEL_POST and len(placing) > 1) else None
    )

    # Fast-trigger single-flight (config.FAST_TRIGGER_ENABLED): ONE blocking
    # acquire around the WHOLE commit stage — the cell loop AND the parallel-post
    # flush below — so a fast rung's in-flight commit can never interleave with
    # this tick's. The boundary must include the flush: under
    # CRYPTOBOT_PHASE_B_PARALLEL_POST the order POSTs do not happen inside
    # `_commit_cell` at all, they are collected there and fired by
    # `_flush_parallel_posts`, so a per-cell lock protected the prep and left
    # every POST outside it. Flag off => no lock, byte-identical.
    with _phase_b_single_flight():
        for d in phase_b_cells:
            try:
                _commit_cell(
                    conn, d, portfolio, wallet_snaps, tick_pending_delta,
                    disk_free_mb, same_side_counts, deps.config_sha256, tick,
                    now_ts,
                    live_ctx=live_ctx,
                    tick_breaker=tick_breaker,
                    deferred_fill_gets=deferred_fill_gets,
                    parallel_posts=parallel_posts,
                )
            except Exception as exc:   # noqa: BLE001 — per-cell isolation
                _mark_skip(d, "error")
                logger.exception("pipeline: per-cell error for %s/%s e=%d", d.coin, d.sleeve.label, d.entry_minute)
                conn.execute(
                    "INSERT INTO pipeline_health (stage, status, coin, message) "
                    "VALUES ('tick', 'ERROR', ?, ?)",
                    (d.coin, f"{type(exc).__name__}: {exc}"),
                )
                conn.commit()

        # LEVER 1: fan the collected LIVE POSTs out concurrently, then finish
        # each serially in the original cell order (ledger writes + tick counters
        # stay single-threaded). Runs BEFORE the immediate-fill flush so the
        # deferred fill-GETs it queues are drained with the rest below. Any cell already reserved is guaranteed to POST or be released —
        # no orphaned SUBMITTING rows survive the tick. (None when the flag is
        # off => no-op.)
        if parallel_posts:
            _flush_parallel_posts(conn, parallel_posts)

    # A11 latency: flush the deferred immediate-fill GETs now that every
    # order POST is done — one GET (~50-260ms) per accepted
    # no-fill-metadata sub-minute order no longer stalls the NEXT cell's
    # POST inside the Phase B loop. MUST run before run_held_position_pass and
    # commit its writes: the pass only considers rows carrying a
    # realized live_filled_size, and these fills must be visible THIS tick
    # (the whole reason the immediate GET exists — see
    # _immediate_fill_for_held_pass). Best-effort: the 60s reconciler remains
    # the authority on any failure here.
    if deferred_fill_gets:
        try:
            _flush_immediate_fill_gets(conn, deferred_fill_gets)
        except Exception:  # noqa: BLE001 — reconciler is the backstop
            logger.warning("deferred immediate-fill flush failed", exc_info=True)

    _tail_t0 = time.monotonic()

    # A5 latency: off-decision-path warm of the NEXT window's Gamma market
    # metadata + CLOB market-info caches (uses the unwrapped fetcher — see
    # helper docstring).
    _prefetch_next_window_markets(
        conn, portfolio, _orig_fetch_market, now_ts,
        order_client=deps.order_client,
        book_prefetch=deps.book_prefetch,
    )

    tick.ms_tail = (time.monotonic() - _tail_t0) * 1000.0
    tick.duration_ms_total = (time.monotonic() - t0) * 1000.0
    _record_latency_trace(conn, tick)
    return tick


def _immediate_fill_for_held_pass(
    order_client,
    order_id: Optional[str],
    *,
    offset_sec: Optional[int],
    current_size: Optional[float],
    intent_amount_usd: Optional[float],
    intent_price: Optional[float],
) -> Optional[tuple[float, float, Optional[str]]]:
    """Return an immediate ``(size, price, ts)`` realized fill for a just-posted
    sub-minute LIVE order, or None to leave the ledger fill columns NULL.

    Root-cause fix (2026-07-10): a FAK ``post_order`` response usually carries
    NO fill metadata, so ``place_gtc_limit_order`` returns ``filled_* = None``
    and the ledger row's ``live_filled_size`` stays NULL until the 60s
    reconciler (``reconcile_open_orders``) — which, for a last-30s sub-minute
    entry, lands AFTER ``window_end``. The held-position (T3) pass only
    considers still-open windows that carry a realized fill, so the position was
    NEVER a candidate for its whole life (13h armed / 20 losses / 0 stop
    attempts). One immediate GET (issued by the accept branch AFTER the order
    posted — OFF the decision→fill critical path, bounded by the client timeout,
    failures swallowed) makes the fill visible to the stop within THIS tick.

    Scoped to sub-minute entries (``offset_sec is not None``) — the only
    population the stop manages — so 5m/15m order placement is untouched. Only
    fires when the response left no fill (``current_size`` None/<=0). Requires
    BOTH a positive size AND a price from the venue (never persist
    ``live_filled_size`` without ``live_filled_price`` — the invariant
    reconciliation.py enforces); otherwise returns None. The 60s reconciler
    remains the authority and later overwrites with venue truth."""
    if offset_sec is None or not order_id:
        return None
    if current_size is not None and float(current_size) > 0:
        return None
    fetch = getattr(order_client, "fetch_order_fill", None)
    if fetch is None:
        return None
    try:
        size, price, ts = fetch(
            order_id,
            intent_amount_usd=intent_amount_usd,
            intent_price=intent_price,
        )
    except Exception:  # noqa: BLE001 — reconciler is the backstop
        logger.debug(
            "immediate fill GET raised for order %s", order_id, exc_info=True
        )
        return None
    if size is None or float(size) <= 0 or price is None:
        return None
    return float(size), float(price), (str(ts) if ts is not None else None)


@dataclass(frozen=True)
class _DeferredFillGet:
    """One queued post-POST immediate-fill GET (A11 latency).

    _commit_cell used to run _immediate_fill_for_held_pass INLINE between the
    order POST and the ledger write — one GET (~50-260ms) that stalled the
    NEXT cell's POST in the Phase B loop. The accept branch now records the
    GET's inputs plus the committed ledger row id here, and run_scanner_tick
    flushes the whole batch concurrently after the loops (see
    _flush_immediate_fill_gets)."""

    order_client: object
    ledger_id: int
    order_id: str
    offset_sec: int
    current_size: Optional[float]
    intent_amount_usd: float
    intent_price: Optional[float]


# Phase-B fan-out pool for the post-loop flushes (_flush_immediate_fill_gets +
# _flush_parallel_posts). Long-lived + module-level: per-tick pool
# creation/teardown was itself hot-path waste, and this being a SEPARATE pool
# from walker._clob_executor preserves the deadlock isolation — a flush task's
# inner network call submits into _clob_executor via _with_timeout, so fanning
# out on _clob_executor's OWN workers could park every worker on an outer waiter
# and deadlock the inner calls queued behind it. 8 workers is ample: the two
# flushes never run concurrently within a tick (called sequentially in
# run_scanner_tick) and ticks don't overlap under APScheduler max_instances=1.
_PHASEB_FLUSH_POOL = ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="phaseb-flush"
)


def _parallel_collect(
    calls: list[Callable[[], Any]], *, label: str
) -> list[Optional[Any]]:
    """Run `calls` and return results index-aligned to `calls`, mapping any
    raised exception to None (order-preserving) and warning under `label`.

    ≤1 call: run inline on THIS thread — no fan-out benefit (mirrors the
    cold-cell fan-out threshold in _resolve_net_cells). Otherwise fan out on
    _PHASEB_FLUSH_POOL. Both branches swallow→None identically so a single
    caller can't behave differently at n=1 vs n>1."""
    results: list[Optional[Any]] = [None] * len(calls)
    if len(calls) <= 1:
        for i, call in enumerate(calls):
            try:
                results[i] = call()
            except Exception:  # noqa: BLE001 — caller/reconciler is the backstop
                logger.warning("%s raised", label, exc_info=True)
        return results
    futures = [_PHASEB_FLUSH_POOL.submit(call) for call in calls]
    for i, future in enumerate(futures):
        try:
            results[i] = future.result()
        except Exception:  # noqa: BLE001 — caller/reconciler is the backstop
            logger.warning("%s raised", label, exc_info=True)
    return results


def _flush_immediate_fill_gets(
    conn, pending: list[_DeferredFillGet]
) -> None:
    """Run the queued immediate-fill GETs concurrently and land the fills.

    Each result writes back onto the ALREADY-COMMITTED ledger row exactly the
    columns the inline path used to pass into record_bet: live_filled_size +
    live_filled_price together (the reconciliation.py invariant — never one
    without the other; _immediate_fill_for_held_pass returns both or None) and
    live_fill_ts only when the venue reported one (COALESCE keeps the POST
    response's fill_ts otherwise — same precedence as the inline path). One
    commit lands the batch; the caller runs this BEFORE run_held_position_pass
    so the pass sees the fills THIS tick. Failures are swallowed per entry — the
    60s reconciler remains the authority and later overwrites venue truth.
    """
    if not pending:
        return

    def _one(p: _DeferredFillGet) -> Callable[[], Any]:
        return lambda: _immediate_fill_for_held_pass(
            p.order_client,
            p.order_id,
            offset_sec=p.offset_sec,
            current_size=p.current_size,
            intent_amount_usd=p.intent_amount_usd,
            intent_price=p.intent_price,
        )

    results = _parallel_collect(
        [_one(p) for p in pending], label="deferred fill GET"
    )

    wrote = False
    for p, res in zip(pending, results):
        if res is None:
            continue
        size, price, ts = res
        try:
            conn.execute(
                "UPDATE ledger SET live_filled_size=?, live_filled_price=?, "
                "live_fill_ts=COALESCE(?, live_fill_ts) WHERE id=?",
                (size, price, ts, p.ledger_id),
            )
            wrote = True
        except Exception:  # noqa: BLE001 — reconciler is the backstop
            logger.warning(
                "deferred fill write-back failed for ledger id=%s",
                p.ledger_id, exc_info=True,
            )
    if wrote:
        conn.commit()


@dataclass(frozen=True)
class _PreparedPost:
    """One place-candidate LIVE cell whose order POST is deferred for the Phase
    B parallel-POST lever (CRYPTOBOT_PHASE_B_PARALLEL_POST).

    `_commit_cell` runs the pre-POST stages (gates -> walk -> sizing -> reserve)
    STRICTLY serially in cell order, then — instead of POSTing inline — appends
    this record and returns. `_flush_parallel_posts` fires every `do_post`
    concurrently (the ~250ms venue round trips overlap) and then replays each
    `finish` SERIALLY in the ORIGINAL cell order, so all ledger writes + tick
    counter mutations stay single-threaded.

      * `do_post()` -> `(order_result, order_ms)` — the ONLY parallel step; it
        touches no sqlite connection and no `tick` counters, just the network.
      * `finish(order_result, order_ms)` — the serial post-POST processing
        (mark_attempt_submitted / record_bet / note_fill / requote telemetry).
      * `attempt_id` — so a POST that RAISES (network throw) releases its
        reservation via mark_attempt_failed rather than orphaning a SUBMITTING
        row (the requirement: reserved cells must POST or be released)."""

    do_post: Callable[[], tuple]
    finish: Callable[[object, float], None]
    attempt_id: int


def _flush_parallel_posts(conn, prepared: list[_PreparedPost]) -> None:
    """Fire the deferred LIVE order POSTs concurrently, then finish serially.

    The concurrent POST fan-out (on the dedicated _PHASEB_FLUSH_POOL, NOT
    walker._clob_executor — see _parallel_collect / that pool's rationale) maps
    a POST that RAISES to None; this function's own tail then does the release +
    finish. Post-POST `finish` callbacks run SERIALLY on THIS thread in the
    original cell order — they own every sqlite write + `tick` counter update,
    so those stay single-threaded exactly as in the serial path. A POST that
    RAISES (network throw, not a returned failure OrderResult) releases its
    reservation with mark_attempt_failed so no orphaned SUBMITTING row survives
    the tick; the reconciler would eventually recover it, but we release
    eagerly."""
    if not prepared:
        return

    results = _parallel_collect(
        [p.do_post for p in prepared], label="phase-b parallel POST"
    )

    for p, res in zip(prepared, results):
        if res is None:
            # The POST threw before returning an OrderResult. Release the
            # SUBMITTING reservation so it never orphans the group cap.
            try:
                mark_attempt_failed(
                    conn, p.attempt_id, "post_error",
                    "phase-b parallel POST raised",
                )
            except Exception:  # noqa: BLE001 — reconciler is the backstop
                logger.warning(
                    "phase-b parallel post-error release failed for attempt %s",
                    p.attempt_id, exc_info=True,
                )
            continue
        order_result, order_ms = res
        try:
            p.finish(order_result, order_ms)
        except Exception:  # noqa: BLE001 — per-cell isolation (mirrors the loop)
            logger.exception("phase-b parallel finish failed")
            try:
                conn.execute(
                    "INSERT INTO pipeline_health (stage, status, message) "
                    "VALUES ('tick', 'ERROR', ?)",
                    (f"phase-b parallel finish failed for attempt {p.attempt_id}",),
                )
                conn.commit()
            except Exception:  # noqa: BLE001
                logger.warning("phase-b finish health-row write failed", exc_info=True)


def _record_latency_trace(conn, tick: TickResult) -> None:
    """A0 latency instrumentation: persist one stage='latency' row per
    NETWORK-active tick.

    Most 1Hz ticks make zero API calls (not at an entry checkpoint) — those
    are skipped so pipeline_health doesn't grow at 1Hz (same bloat rationale
    as the ENTRY_SKIP_SECONDS no-row policy). The message is compact JSON so
    the dashboard Latency page can decompose fetch/sign/post without a schema
    migration. Failures are swallowed: observability must never fail a tick.
    """
    # WS/prefetch cache hits serve books with ZERO inline fetch (n_book_calls
    # stays 0), so a fully cache-served tick would otherwise write no trace and
    # hide exactly the WS/prefetch hit-rate the operator is arming to watch.
    if (
        tick.n_gamma_calls == 0
        and tick.n_book_calls == 0
        and tick.n_orders == 0
        and tick.n_book_ws == 0
        and tick.n_book_prefetch == 0
    ):
        return
    try:
        payload = json.dumps(
            {
                "spot": round(tick.ms_spot, 1),
                "ctx": round(tick.ms_live_ctx, 1),
                "gamma": round(tick.ms_gamma, 1),
                "book": round(tick.ms_book, 1),
                "sign": round(tick.ms_order_sign, 1),
                "post": round(tick.ms_order_post, 1),
                "order": round(tick.ms_order_total, 1),
                "total": round(tick.duration_ms_total, 1),
                "n_gamma": tick.n_gamma_calls,
                "n_book": tick.n_book_calls,
                "n_orders": tick.n_orders,
                # 2026-07-25: accept/reject cohorts + the decision/tail split.
                # `post` above is ACCEPT-ONLY (baseline-comparable); `post_rej`
                # is the venue-refused cohort, which is ~55% of live orders and
                # used to be recorded as a silent zero.
                "post_rej": round(tick.ms_order_post_rej, 1),
                "n_rej": tick.n_orders_rej,
                "pre_post": round(tick.ms_pre_post, 1),
                "tail": round(tick.ms_tail, 1),
                # W2 book-source attribution + WS staleness.
                "bk_ws": tick.n_book_ws,
                "bk_pf": tick.n_book_prefetch,
                "bk_rest": tick.n_book_rest,
                "ws_age": round(tick.book_ws_age_ms_max, 0),
            },
            separators=(",", ":"),
        )
        conn.execute(
            "INSERT INTO pipeline_health (stage, status, message, duration_ms) "
            "VALUES ('latency', 'OK', ?, ?)",
            (payload, int(tick.duration_ms_total)),
        )
        conn.commit()
    except Exception:  # noqa: BLE001 — observability must never fail the tick
        logger.warning("latency trace write failed", exc_info=True)


def _resolve_net_cells(
    net_cells: list[_NetCell],
    *,
    conn,
    deps: TickDeps,
    portfolio: V6Portfolio,
    now_ts: int,
    tick: TickResult,
    note_tokens: bool = True,
) -> list[CellDecision]:
    """A2 latency: resolve buffered qualifying cells in three passes.

    Pass 1 — Gamma market metadata per cell (the per-window in-process cache
    in polymarket_gamma makes within-window repeats free) + fire-and-forget
    CLOB market-info warm (A1).
    Pass 2 — books: ONE ``POST /books`` batch for every token via
    ``deps.fetch_books``; falls back to the per-token ``fetch_book`` GETs
    when the batch fetcher is unwired (tests/legacy) or the batch call
    fails. A SUCCESSFUL batch is authoritative: a token absent from the
    response ≡ the venue returned no/empty asks ≡ ``fetch_top_book``'s
    None. Both fetchers are Optional and None-guarded — a caller that is not
    allowed to spend a REST call (the fast-trigger rungs) passes neither and is
    served purely from the push/prefetch cache.
    Pass 3 — rescore with live asks + build each CellDecision with the
    exact same semantics the old inline block had (rescore runs even when
    the market is missing, with NaN asks — unchanged).

    ``note_tokens=False`` suppresses the Pass-2 ``note_tokens`` push on the
    fast rungs: it is a WHOLESALE REPLACE driving the WS subscription and the
    1Hz tick owns that set.
    """
    from cryptobot.venues.types import FeeUnknown

    # Pass 1 — market metadata + A1 cache warm.
    #
    # A3 latency: warm cells resolve from the in-process per-window market
    # cache with zero overhead and are served inline (no thread hop). Cold
    # cells each pay a Gamma GET (~30-80ms); fan those out over the shared CLOB
    # executor so N cold cells cost ~one GET of wall time instead of N
    # sequential. fetch_active_market caches only SUCCESSES, so concurrent
    # fan-out over distinct slugs is idempotent-safe.
    #
    # DB safety (SQLITE_MISUSE fix): fetch_active_market runs a fee-cache
    # SELECT and — on the always-miss updown path (feeRateBps is null on every
    # *-updown-* market) — an INSERT ON CONFLICT + prune DELETE. A
    # sqlite3.Connection's per-connection prepared-statement cache is NOT safe
    # to touch from two threads at once, so two executor workers sharing the
    # tick's connection raised sqlite3.InterfaceError ("bad parameter or other
    # API misuse", SQLITE_MISUSE) on a fraction of multi-cold-cell boundaries —
    # which the blanket except below silently mapped to a missed bet (permanent
    # at the final checkpoint offset). Each executor closure therefore resolves
    # ITS OWN thread-local connection via deps.conn_factory (the
    # ThreadLocalConnections handle); SQLite's WAL file lock + busy_timeout
    # serialize the rare concurrent fee-cache writes across those connections.
    # The inline path stays on the main thread with the tick conn.
    conn_factory = deps.conn_factory

    def _fetch_with(cell: "_NetCell", fetch_conn) -> object:
        return deps.fetch_market(
            cell.coin, interval_min=cell.sleeve.interval_min,
            window_start_ts=cell.window_start, conn=fetch_conn,
            now_ts=float(now_ts),
        )

    def _fetch_inline(cell: "_NetCell") -> object:
        # Main thread — the tick conn is safe (no other thread touches it here).
        return _fetch_with(cell, conn)

    def _fetch_threaded(cell: "_NetCell") -> object:
        # Runs on a _clob_executor worker thread. Resolve THIS thread's own
        # connection so the fee-cache read/write never shares the tick conn's
        # statement cache. conn_factory is None only in tests / legacy fixtures
        # that stub fetch_market and never touch the DB, so falling back to the
        # tick conn there cannot trigger SQLITE_MISUSE.
        thread_conn = conn_factory() if conn_factory is not None else conn
        return _fetch_with(cell, thread_conn)

    cold_idx = [
        i for i, c in enumerate(net_cells)
        if not _venue_market_cache_has(
            c.coin, c.sleeve.interval_min, c.window_start
        )
    ]
    markets: list[object] = [None] * len(net_cells)
    if len(cold_idx) > 1:
        from cryptobot.execution.executors import clob_executor

        futures = {
            i: clob_executor.submit(_fetch_threaded, net_cells[i])
            for i in cold_idx
        }
        cold_set = set(cold_idx)
        for i in range(len(net_cells)):
            if i in cold_set:
                try:
                    markets[i] = futures[i].result()
                except Exception as exc:  # noqa: BLE001 — failed GET ⇒ no market
                    # Log the concrete exception class so a swallowed
                    # programming error (e.g. SQLITE_MISUSE) is distinguishable
                    # from an expected network failure in the health logs.
                    logger.warning(
                        "parallel fetch_market raised (%s) — treating as no "
                        "market",
                        type(exc).__name__,
                        exc_info=True,
                    )
                    markets[i] = None
            else:
                markets[i] = _fetch_inline(net_cells[i])
    else:
        # 0-1 cold cells: no fan-out benefit — resolve everything inline.
        for i in range(len(net_cells)):
            markets[i] = _fetch_inline(net_cells[i])

    warm_cids: list[str] = []
    for market in markets:
        if (
            market is not None
            and not isinstance(market, FeeUnknown)
            and deps.order_client is not None
            and getattr(market, "condition_id", None)
        ):
            # A1 latency: pre-warm the CLOB client's static market-info
            # caches at discovery time (fire-and-forget) so a later
            # create_market_order is pure local signing + one POST.
            # getattr-guarded: test stubs lack the method.
            _warm = getattr(deps.order_client, "warm_market_info_async", None)
            if _warm is not None:
                try:
                    _warm(market.condition_id)
                except Exception:  # noqa: BLE001 — warm is best-effort
                    logger.debug("warm_market_info_async raised", exc_info=True)
            # A14/A15 latency: the L4 SYNC warm fallback is DEFERRED to the
            # Pass-2 tail (below) so the async warm above overlaps the books
            # RTT instead of paying a duplicate inline GET here while that
            # warm is still in flight. Collect the cid; still paid during
            # the fetch/scoring phase, off the reserve→POST gap.
            warm_cids.append(market.condition_id)

    # Pass 2 — books.
    tokens: list[str] = []
    for market in markets:
        if market is not None and not isinstance(market, FeeUnknown):
            tokens.append(market.yes_token_id)
            tokens.append(market.no_token_id)
    all_tokens = list(tokens)  # W3: full window set for the shadow subscription

    def _inline_book(token: str) -> Optional[BookSnapshot]:
        """Per-token REST GET, or None when no per-token fetcher is wired."""
        if deps.fetch_book is None:
            return None
        return deps.fetch_book(token, now_ts=float(now_ts))

    books: dict[str, BookSnapshot] = {}
    if tokens and deps.book_prefetch is not None:
        # A8: serve fresh-enough prefetched snapshots first; whatever remains
        # pays the inline batch below. note_tokens() tells the prefetcher what
        # to fetch ahead of the NEXT boundary (token set is window-stable).
        try:
            if note_tokens:
                deps.book_prefetch.note_tokens(tokens)
            # W2: attribute each cache hit to its source (ws / prefetch) via the
            # tagged accessor when the chain exposes it; fall back to plain get.
            get_tagged = getattr(deps.book_prefetch, "get_tagged", None)
            for t in tokens:
                if get_tagged is not None:
                    snap, src = get_tagged(t, now_ts=float(now_ts))
                else:
                    snap, src = deps.book_prefetch.get(t, now_ts=float(now_ts)), None
                if snap is not None:
                    books[t] = snap
                    if src == "ws":
                        tick.n_book_ws += 1
                        age_ms = (
                            float(now_ts)
                            - float(getattr(snap, "fetched_at_ts", now_ts) or now_ts)
                        ) * 1000.0
                        if age_ms > tick.book_ws_age_ms_max:
                            tick.book_ws_age_ms_max = age_ms
                    elif src == "prefetch":
                        tick.n_book_prefetch += 1
        except Exception:  # noqa: BLE001 — prefetch cache must never block
            logger.warning("book_prefetch cache read failed", exc_info=True)
            books = {}
        tokens = [t for t in tokens if t not in books]
    _pre_inline = len(books)  # W2: books added below all came from inline REST
    if tokens:
        batch = None
        if deps.fetch_books is not None:
            try:
                batch = deps.fetch_books(tokens, now_ts=float(now_ts))
            except Exception:  # noqa: BLE001 — fall back to per-token GETs
                logger.warning(
                    "fetch_books batch raised — falling back to per-token GETs",
                    exc_info=True,
                )
                batch = None
        if batch is not None:
            books.update(batch)  # keep any A8-prefetched entries
            # Review finding (flag-off parity): a 200 batch response can
            # OMIT requested tokens (venue-side partial response) — treating
            # that as "no asks" would silently no-bet a baseline cell the
            # old per-token path would have fetched independently. Fall back
            # to per-token GETs for exactly the missing ids. Cost: one GET
            # per genuinely-empty book (fetch_top_book also returns None for
            # those), only on partial responses — the fast path stays 1 RTT.
            missing = [t for t in tokens if t not in books]
            if missing:
                logger.warning(
                    "POST /books omitted %d/%d requested tokens — issuing "
                    "per-token fallback GETs",
                    len(missing),
                    len(tokens),
                )
                for t in missing:
                    snap = _inline_book(t)
                    if snap is not None:
                        books[t] = snap
        else:
            for t in tokens:
                snap = _inline_book(t)
                if snap is not None:
                    books[t] = snap
    tick.n_book_rest += len(books) - _pre_inline  # W2: inline-REST-served count

    # A15 latency: deferred L4 SYNC warm fallback — runs AFTER the books RTT
    # so the Pass-1 async warms above had the whole Pass-2 round trip to
    # finish. ensure_market_info_warm JOINS the retained in-flight Future
    # when one exists (typically already done ⇒ no GET, no block) and only
    # falls back to its own bounded sync GET when the async warm is absent
    # or already failed — the duplicate inline GET A14 used to pay at
    # discovery is gone. Blocks at most ONCE per condition_id either way
    # (the _warm_sync_attempted one-shot — a down venue cannot stall every
    # tick's scoring), and the _commit_cell call before the order sign stays
    # as the final guard. getattr-guarded: test stubs lack the method.
    if warm_cids:  # non-empty only when Pass 1 saw a non-None order_client
        _ensure = getattr(deps.order_client, "ensure_market_info_warm", None)
        if _ensure is not None:
            for _cid in warm_cids:
                try:
                    _ensure(_cid)
                except Exception:  # noqa: BLE001 — warm is best-effort
                    logger.debug(
                        "ensure_market_info_warm raised at discovery",
                        exc_info=True,
                    )

    # W3 shadow-compare: hand each AUTHORITATIVE REST book to the WS mirror
    # comparator. Off the decision path — `books` is already final and is never
    # mutated here; a failure must never fail the tick.
    if deps.book_shadow is not None and all_tokens:
        try:
            deps.book_shadow.note_tokens(all_tokens)
            for t, snap in books.items():
                deps.book_shadow.observe(t, snap, now_ts=float(now_ts))
        except Exception:  # noqa: BLE001 — shadow compare is observability only
            logger.debug("book shadow compare failed", exc_info=True)

    # Pass 3 — rescore + decisions.
    out: list[CellDecision] = []
    for c, market in zip(net_cells, markets):
        fee_unknown_flag = isinstance(market, FeeUnknown)
        yes_book: Optional[BookSnapshot] = None
        no_book: Optional[BookSnapshot] = None
        yes_token: Optional[str] = None
        no_token: Optional[str] = None
        poly_market_id: Optional[str] = None
        fee_rate_bps: Optional[float] = None
        yes_ask = no_ask = float("nan")
        if market is not None and not fee_unknown_flag:
            yes_token = market.yes_token_id
            no_token = market.no_token_id
            poly_market_id = market.condition_id
            fee_rate_bps = market.fee_rate_bps
            yes_book = books.get(yes_token)
            no_book = books.get(no_token)
            if yes_book is not None and yes_book.top_ask is not None:
                yes_ask = yes_book.top_ask
            if no_book is not None and no_book.top_ask is not None:
                no_ask = no_book.top_ask

        score = _score_cell_for(
            coin=c.coin,
            sleeve=c.sleeve,
            entry_minute=c.entry_minute,
            window_start_ts=c.window_start,
            live_spot=c.spot.price,
            yes_ask=yes_ask,
            no_ask=no_ask,
            offset_sec=c.offset_sec,
            deps=deps,
            # THE authoritative rescore — the one score a bet is actually built
            # from — so it carries the resolved market's real per-market fee.
            # The venue-dispatched gate fee (score._fee_adjusted_edge) reads it
            # on predict.fun; Polymarket ignores it (theta parabola).
            fee_rate_bps=fee_rate_bps,
            twap_open=c.twap_open,
        )

        # Divergence #13 fix: capture the window-open spot the model trains
        # against (see CellDecision.window_open_spot). Engine state is
        # stamped once at tick start, so this matches the old inline timing.
        decision = CellDecision(
            coin=c.coin,
            sleeve=c.sleeve,
            entry_minute=c.entry_minute,
            window_start_ts=c.window_start,
            window_end_ts=c.window_end,
            score=score,
            yes_book=yes_book,
            no_book=no_book,
            spot_age_s=c.spot_age_s,
            fee_rate_bps=fee_rate_bps,
            poly_market_id=poly_market_id,
            poly_token_id_yes=yes_token,
            poly_token_id_no=no_token,
            fee_unknown=fee_unknown_flag,
            offset_sec=c.offset_sec,
            strike_price=c.spot.price,
            window_open_spot=c.twap_open,
            trigger_src=c.trigger_src,
        )
        out.append(decision)
    return out


def _commit_cell(
    conn,
    d: CellDecision,
    portfolio: V6Portfolio,
    wallet_snaps: dict[str, WalletSnapshot],
    tick_pending_delta: dict[str, float],
    disk_free_mb: float,
    same_side_counts: dict[tuple[str, int, int, str], int],
    config_sha256: str,
    tick: TickResult,
    now_ts: int,
    *,
    live_ctx: Optional[_LiveCommitContext] = None,
    tick_breaker: Optional[BreakerState] = None,
    deferred_fill_gets: Optional[list["_DeferredFillGet"]] = None,
    parallel_posts: Optional[list["_PreparedPost"]] = None,
) -> None:
    """Commit one cell: gate → slot_state → walk_book/CLOB → record_bet.

    ``tick_pending_delta`` tracks same-tick exposure by wallet. The exposure
    gate sees ``wallet.pending_exposure + tick_pending_delta[wallet_id]`` so
    multi-cell ticks honor each wallet cap on the SECOND cell, not just the
    first.

    ``tick_breaker`` is the once-per-tick hoisted breaker state (A10); when
    None (legacy caller) it is derived here.
    ``deferred_fill_gets`` collects the accept branch's immediate-fill GET
    so run_scanner_tick can flush the batch off the Phase B loop (A11); when
    None the GET runs inline as before.

    ``parallel_posts`` (CRYPTOBOT_PHASE_B_PARALLEL_POST lever) is the Phase B
    order-POST fan-out collector. DEFAULT None = the serial path: a LIVE cell
    reserves, POSTs, and processes the result inline exactly as before. When a
    collector is passed AND this cell reaches the LIVE POST stage, the pre-POST
    stages (through reserve_live_order_attempt + ensure-warm) run serially here,
    then this cell's stake is applied to the within-tick exposure deltas
    PESSIMISTICALLY (see the parallel branch), a ``_PreparedPost`` is appended,
    and the function RETURNS without POSTing — run_scanner_tick fans the POSTs
    out and replays each cell's post-POST ``finish`` serially in cell order.
    DRY cells and skip cells never touch the collector: they complete inline
    whether or not the collector is present, so the serial code they run is
    unchanged.

    Per U6: when ``wallet_snap.mode == 'LIVE'`` AND ``live_ctx`` is wired,
    the path forks before record_bet — the orchestrator reserves a durable
    ``live_order_attempts`` row, places the order via
    ``walker.OrderClient.place_gtc_limit_order``, then writes the ledger row
    with the CLOB-side metadata. DRY wallets follow the existing
    ``walk_book_edge_preserving`` paper path.
    """

    sleeve = d.sleeve
    wallet_id = wallet_id_for_sleeve(sleeve.label)
    wallet_snap = wallet_snaps[wallet_id]
    wallet_mode = wallet_snap.mode
    wallet_group = wallet_snap.wallet_group

    # LIVE-only pre-gate skip — when global state / readiness / balance
    # blocks LIVE entries, DRY wallets continue normally (D-LIVE-12). The
    # skip reason is the same across the whole tick because it's computed
    # ONCE at tick start.
    if (
        wallet_mode == "LIVE"
        and live_ctx is not None
        and live_ctx.live_skip_reason is not None
    ):
        _emit_pre_gate_skip(
            conn, d, live_ctx.live_skip_reason, tick,
            mode=wallet_mode, wallet_group=wallet_group,
        )
        return

    # Fee unknown → SKIP:fee_unknown (review #52). Distinct from stale_book —
    # the market exists but feeRateBps is absent. Fee gate (R9) is fail-closed.
    if d.fee_unknown:
        _mark_skip(d, "fee_unknown")
        record_signal_skip(
            conn,
            created_at=utc_now_iso(),
            coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
            side="YES", window_start_ts=d.window_start_ts,
            model_p=None, ask=None, edge=None, fee_rate_bps=None,
            partial_ret_z_1h=None, lut_p_bucket=None,
            lut_hit_rate=None, lut_n_cum=None,
            skip_reason="fee_unknown",
            offset_sec=d.offset_sec,
        )
        _bump(tick.skips_by_reason, "fee_unknown")
        tick.signals_written += 1
        return

    # Score failure → SKIP signal row, no walker.
    if d.score.skip_reason is not None:
        _mark_skip(d, d.score.skip_reason)
        # A6 hot-path hygiene: entry_scan_latched re-fires on EVERY 1Hz tick
        # of a qualifying minute after the first tick claimed the scan latch
        # — the single largest signals-table flood (~200k rows/day; the live
        # DB hit 1.5GB mostly on these). The rows carry zero information
        # beyond "the latch is held" (model_p is NULL, nothing grades them);
        # the claim-tick's fully-scored row each minute is the liveness
        # marker. Suppress the DB row, keep the SSE decision + skip counter
        # so the dashboard live view is unchanged.
        if d.score.skip_reason == "entry_scan_latched":
            _bump(tick.skips_by_reason, d.score.skip_reason)
            return
        record_signal_skip(
            conn,
            created_at=utc_now_iso(),
            coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
            side="YES", window_start_ts=d.window_start_ts,
            model_p=None, ask=None, edge=None, fee_rate_bps=d.fee_rate_bps,
            partial_ret_z_1h=d.score.partial_ret_z_1h,
            lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
            lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
            skip_reason=d.score.skip_reason,
            offset_sec=d.offset_sec,
        )
        _bump(tick.skips_by_reason, d.score.skip_reason)
        tick.signals_written += 1
        return

    # min_agreement gate (D5 cross-coin): require >= min_agreement OTHER coins
    # agree on the same side at the same window. v6_5m min_agreement=1, v6_15m=0.
    side = d.score.side
    key = (sleeve.label, d.window_start_ts, d.entry_minute, side)
    same_side = same_side_counts.get(key, 0)
    # "min_agreement" in the backtest is the count of OTHER coins agreeing;
    # i.e., (same_side - 1) >= min_agreement.
    if (same_side - 1) < sleeve.min_agreement:
        _mark_skip(d, "agreement_below_floor")
        record_signal_skip(
            conn,
            created_at=utc_now_iso(),
            coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
            side=side, window_start_ts=d.window_start_ts,
            model_p=d.score.model_p, ask=_pick_ask(d, side),
            edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
            partial_ret_z_1h=d.score.partial_ret_z_1h,
            lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
            lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
            skip_reason="agreement_below_floor",
            offset_sec=d.offset_sec,
        )
        _bump(tick.skips_by_reason, "agreement_below_floor")
        tick.signals_written += 1
        return

    # Divergence #13 fix (2026-05-21): refuse to bet on a window we can
    # not honestly grade. window_open_spot is None when the bot booted
    # mid-window and PartialRetEngine has not yet observed a boundary
    # crossing for this (coin, interval). Without it, resolution_sweep
    # would have to fall back to the decision-time strike — exactly the
    # measurement bug this fix exists to close. The skip is cheap
    # (under one window per restart per coin), and the resulting hole
    # in the ledger is the correct behavior.
    if d.window_open_spot is None:
        _mark_skip(d, "no_window_open_spot")
        record_signal_skip(
            conn,
            created_at=utc_now_iso(),
            coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
            side=side, window_start_ts=d.window_start_ts,
            model_p=d.score.model_p, ask=_pick_ask(d, side),
            edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
            partial_ret_z_1h=d.score.partial_ret_z_1h,
            lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
            lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
            skip_reason="no_window_open_spot",
            offset_sec=d.offset_sec,
        )
        _bump(tick.skips_by_reason, "no_window_open_spot")
        tick.signals_written += 1
        return

    # Begin slot lock + slot_state probe. SQLite serializes writes via
    # BEGIN IMMEDIATE — the single-process invariant is the primary
    # defense; the lock is belt-and-suspenders for future parallelization.
    # NOTE (audit P1 #3): on the LIVE path this lock is intentionally released
    # by reserve_live_order_attempt()'s internal commit (the CLOB network call
    # must not run inside a SQLite txn). LIVE slot dedup is then the
    # uniq_loa_active_slot partial index, and the LIVE ledger INSERT + link run
    # in their OWN BEGIN IMMEDIATE below. The DRY path stays wholly inside this
    # transaction and is committed/rolled back by the handler at slot end.
    conn.execute("BEGIN IMMEDIATE")
    try:
        # A12 latency: for offset cells under SUBMINUTE_ONE_PER_WINDOW the
        # minute-keyed probe below is dead work — the ONE_PER_WINDOW state
        # machine unconditionally overwrites (slot_filled, slot_anchor)
        # with its own window-level values (multifill fresh-entry 0.0/None
        # or subminute_window_state) before anything reads them, and every
        # early exit on that path returns without reading them either. Skip
        # the two ledger SELECTs there; every other path still probes here.
        if d.offset_sec is None or not config.SUBMINUTE_ONE_PER_WINDOW:
            slot_filled, slot_anchor = slot_state(
                conn,
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
            )
        else:
            slot_filled, slot_anchor = 0.0, None
        target_usd = sleeve.target_bet_size_for(
            wallet_snap.realized_capital, coin=d.coin
        )

        # ------------------------------------------------------------------
        # B1/B3/B4/B5: sub-minute checkpoint safety stack. Order matters:
        # kill-switch → one-shot per-offset slot → fill-rate breaker →
        # staged ramp sizing. Every check fails CLOSED (skip, not bet).
        # ------------------------------------------------------------------
        if d.offset_sec is not None:
            def _sub_skip(reason: str) -> None:
                _mark_skip(d, reason)
                record_signal_skip(
                    conn,
                    created_at=utc_now_iso(),
                    coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                    side=side, window_start_ts=d.window_start_ts,
                    model_p=d.score.model_p, ask=_pick_ask(d, side),
                    edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                    partial_ret_z_1h=d.score.partial_ret_z_1h,
                    lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                    lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                    skip_reason=reason,
                    mode=wallet_mode, wallet_group=wallet_group,
                    offset_sec=d.offset_sec,
                )
                _bump(tick.skips_by_reason, reason)
                tick.signals_written += 1
                conn.commit()

            # Kill-switch / master flag (defense-in-depth: the tick loop and
            # the loader already gate; a stale decision must still refuse).
            if config.SUBMINUTE_KILL or not config.SUBMINUTE_ENABLED:
                _sub_skip("subminute_disabled")
                return

            # Interval entry-disable (defense-in-depth, exactly like the
            # kill-switch above): the tick loop already skipped this sleeve, so
            # reaching here would mean a decision that outlived its gate. Writes
            # a signal row rather than skipping silently — it cannot fire at 1Hz,
            # so if it ever fires that is worth seeing.
            if _interval_entry_disabled(sleeve.interval_min):
                _sub_skip("interval_disabled")
                return

            # v2 step1last equity-fraction sizing. sizing_v2 OWNS
            # base_fraction × DD-brake × concurrency × $1 floor-up for
            # checkpoint sleeves; the config capital_frac used by
            # target_bet_size_for above is decorative for v2 and is NOT
            # re-applied here → no double-application. The staged-ramp block
            # below only MULTIPLIES this by the rung multiplier (1.0 while the
            # ramp is disabled), so the brake composed here is never undone.
            # k = same-minute, same-side concurrent candidates observed this
            # tick — the live proxy for pool_study.stake_weights' per-minute
            # 1/sqrt(k) downweight (same_side counts this cell, so k >= 1).
            #
            # EQUITY SOURCE (operator 2026-07-09): size off CURRENT AVAILABLE
            # EQUITY — the cached live CLOB balance (BalanceCache via
            # TickDeps.live_balance_fetcher, surfaced on live_ctx.live_balance)
            # — NOT the wallet-ledger realized capital (the $100 paper anchor +
            # ledger PnL, which double-counts the retired strategy's losses).
            # live_ctx.live_balance is None when the balance cache is
            # unavailable/stale (the cache's get() fails closed to None past its
            # staleness bound) OR on a DRY wallet; fall back to realized_capital
            # then so placement is NEVER blocked on a balance fetch.
            #
            # DD-brake divergence (documented, not a bug): the brake's dd input
            # stays wallet_snap.drawdown_pct_peak_frac — the ledger-equity
            # peak-drawdown series that ALSO drives the hard DD halt
            # (config.MAX_DRAWDOWN_HALT_FRAC, _wallet_drawdown_halted). Live
            # equity has no persisted per-tick
            # peak series to draw down from, so re-deriving the brake off the
            # real balance is not trivially possible without a new peak tracker;
            # per operator order we keep the halt untouched and leave the brake
            # on ledger-dd while the STAKE base sizes off the real balance.
            sizing_equity = _v2_sizing_equity(
                live_ctx, wallet_snap.realized_capital
            )
            # No per-coin depth/fill cap (removed by operator order 2026-07-11):
            # the stake is the raw equity-fraction size (base_frac × brake × conc)
            # floored up to $1 — "as long as each coin satisfied condition and
            # follows bet size, just fill." Sizing never affects bet SELECTION
            # (parity), only its USD size.
            target_usd = sizing_v2.sized_usd(
                sizing_equity,
                sleeve.interval,
                wallet_snap.drawdown_pct_peak_frac,
                k=same_side,
            )

            # Slot state for this checkpoint. Two modes:
            #  * SUBMINUTE_ONE_PER_WINDOW off:
            #    one-shot per-offset slot — checkpoint cells never top up, and
            #    the minute-keyed slot_state above may show OTHER offsets' fills
            #    in the same minute, so override to size/dedup this cell as its
            #    own initial fill (per-offset stacking, the checkpoint-grid design).
            #  * SUBMINUTE_ONE_PER_WINDOW on (operator 2026-07-05): aggregate
            #    exposure at market-resolution level. The first side to open
            #    the window owns it; later same-side checkpoints may TOP UP an
            #    under-filled initial, while the opposite side is rejected as
            #    market_dedup. This matches the combo backtest's
            #    _dedup_resolution_with_stats one-row-per-resolution semantics.
            #    A full same-side slot short-circuits to "slot_full" via the
            #    top-up-min-bet guard below.
            if config.SUBMINUTE_ONE_PER_WINDOW:
                # v2 dedup-BEFORE-gate window state machine. Two policies,
                # selected by CRYPTOBOT_V2_REJECT_RETRY (re-synced each tick so
                # the flag is a true master switch):
                #   * =1 (default, operator 2026-07-09): CONSUME-ON-FILL-ONLY.
                #     A FAK rejection does NOT consume the window; only a FILL
                #     (note_fill below) or window end does. Every qualifying
                #     second is an independent attempt (fresh book / fresh score
                #     / band+gate at THAT second's ask) — live aggression now
                #     exceeds the backtest first-commit ceiling BY DESIGN, and
                #     subminute_fills.attempt measures the extra attempts.
                #   * =0: legacy first-commit — commit the window at the FIRST
                #     in-band second and gate THAT second only (a gate-FAILED
                #     first second still consumes it); pins live to the frozen
                #     fold-E n=2366 ceiling (the no-retry baseline).
                # Once ANY side fills, note_fill consumes the window for BOTH
                # sides. The durable DB checks below stay as the hard
                # already-PLACED guard (and survive a process restart that
                # resets the in-memory decider).
                _v2_dec = _v2_decider_for(sleeve)  # per-sleeve, band-aware
                _ask_now = _pick_ask(d, side)
                _v2_cell = {
                    "coin": d.coin,
                    "interval": sleeve.interval,
                    "window_start": d.window_start_ts,
                    "side": side,
                    "ask": _ask_now if _ask_now is not None else 0.0,
                    "offset_sec": d.offset_sec,
                    "pred_edge": (
                        d.score.realized_edge
                        if d.score.realized_edge is not None
                        else float("-inf")
                    ),
                }
                _verdict = _v2_dec.on_second(_v2_cell)
                _v2_prune_window_decider(now_ts)
                if _verdict != "PLACE":
                    _sub_skip("v2_window_consumed")
                    return
                # Cross-sleeve one-bet-per-market (first fill wins). Each sleeve
                # has its OWN decider (private _filled), so a fill by a
                # DIFFERENT sleeve on this exact (coin, interval, window) market
                # is not visible to this decider — enforce it explicitly here.
                # In-memory set = fast path within a process; the ledger query =
                # restart-durable guard (the per-sleeve subminute_window_* dedups
                # all filter WHERE sleeve=? and do NOT arbitrate across sleeves).
                _mkt_key = (d.coin, sleeve.interval, d.window_start_ts)
                if (
                    _mkt_key in _V2_MARKET_FILLED
                    or subminute_market_other_sleeve(
                        conn,
                        coin=d.coin,
                        interval_min=sleeve.interval_min,
                        window_start_ts=d.window_start_ts,
                        sleeve=sleeve.label,
                    )
                ):
                    _sub_skip("market_dedup_cross_sleeve")
                    return
                existing_side = subminute_window_existing_side(
                    conn,
                    coin=d.coin,
                    sleeve=sleeve.label,
                    window_start_ts=d.window_start_ts,
                )
                if existing_side is not None and existing_side != side:
                    # Never bet both up and down on one resolution.
                    _sub_skip("market_dedup")
                    return
                slot_filled, slot_anchor = subminute_window_state(
                    conn,
                    coin=d.coin,
                    sleeve=sleeve.label,
                    side=side,
                    window_start_ts=d.window_start_ts,
                )
            else:
                if subminute_slot_seen(
                    conn,
                    coin=d.coin,
                    sleeve=sleeve.label,
                    offset_sec=d.offset_sec,
                    window_start_ts=d.window_start_ts,
                ):
                    _sub_skip("slot_full")
                    return
                slot_filled, slot_anchor = 0.0, None

            # B4 fill-rate breaker (evaluate is fail-closed: internal error
            # returns tripped). Trip ⇒ block + one-shot CRITICAL + alert;
            # the baseline sleeves are untouched. A10: the hoisted
            # once-per-tick state is used when the orchestrator supplied it
            # (its inputs only change on a POST/settlement); the inline
            # derive remains for legacy callers.
            breaker = (
                tick_breaker if tick_breaker is not None
                else evaluate_breaker(conn)
            )
            if breaker.tripped:
                _record_subminute_breaker_trip(conn, live_ctx, breaker)
                _sub_skip("subminute_breaker_tripped")
                return

            # Per-coin cap clamp — MUST be last: after brake/conc (sized_usd),
            # so the cap bounds what is actually sent. INERT since 2026-07-21 (operator override: empty
            # max_bet_usd_by_coin maps); see _v2_apply_coin_cap.
            target_usd = _v2_apply_coin_cap(sleeve, d.coin, target_usd)

        is_topup_attempt = slot_filled > 0.0
        # B1: the bet-seen guard below keys on ENTRY MINUTE, but sub-minute
        # checkpoints deliberately stack multiple bets per minute (step1last
        # = offsets 270-299 all share minute 4). Their one-bet-per-OFFSET
        # dedup already ran in the safety stack above (subminute_slot_seen),
        # so the minute-keyed guard must not fire for offset cells — it
        # would falsely latch every tail offset after the first.
        if (
            d.offset_sec is None
            and not is_topup_attempt
            and _initial_entry_bet_seen(conn, d)
        ):
            # Live can retry after diagnostic skips inside the entry minute,
            # but once a real initial bet exists, later seconds may only top
            # up that started slot.
            _mark_skip(d, "entry_scan_latched")
            record_signal_skip(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=_pick_ask(d, side),
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                skip_reason="entry_scan_latched",
                offset_sec=d.offset_sec,
            )
            _bump(tick.skips_by_reason, "entry_scan_latched")
            tick.signals_written += 1
            conn.commit()
            return

        # Walker setup. For top-ups, anchor the cap on the slot's first-fill
        # price (sticky cap from R-NEW-9). Otherwise anchor on this tick's
        # top_ask via book.top_ask (the walker default).
        ask_book = _book_for_side(d, side)
        if ask_book is None:
            _mark_skip(d, "stale_book")
            record_signal_skip(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=None,
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                skip_reason="stale_book",
                offset_sec=d.offset_sec,
            )
            _bump(tick.skips_by_reason, "stale_book")
            tick.signals_written += 1
            conn.commit()
            return

        # The walker is called with the FULL target_usd on initial fill so
        # the gate stack can see the intended exposure for the cap check;
        # on top-ups we walk `remaining = target_usd - slot_filled` per
        # R-NEW-9 step 3.
        walker_target = target_usd if not is_topup_attempt else (target_usd - slot_filled)
        anchor_for_walker = slot_anchor if is_topup_attempt else None
        if is_topup_attempt and walker_target < _topup_effective_min_bet(target_usd):
            _mark_skip(d, "slot_full")
            record_signal_skip(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=ask_book.top_ask,
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                skip_reason="slot_full",
                offset_sec=d.offset_sec,
            )
            _bump(tick.skips_by_reason, "slot_full")
            tick.signals_written += 1
            conn.commit()
            return

        # D10 market-level dedup. The backtest treats e=7 and e=13 within
        # the same 15m window as ONE qualifying bet under `first_qualifying`
        # (keyed on (slug_ts, coin, side) at backtest/optimize.py:418-428).
        # Under `every_qualifying` (v6_5m chunk-CV pick) each qualifying
        # entry minute fires a separate bet — the dedup must NOT apply.
        # `single` is implicit at the artifact level (single-element
        # entry_minute_set) and needs no runtime branch.
        # Top-ups within the slot that DID anchor first are unaffected —
        # is_topup_attempt is True for those, so they bypass this guard
        # regardless of strategy.
        entry_strategy = getattr(sleeve, "entry_strategy", "first_qualifying")
        if not is_topup_attempt and entry_strategy == "first_qualifying":
            prior_anchor = conn.execute(
                "SELECT id FROM ledger "
                "WHERE coin = ? AND sleeve = ? AND side = ? "
                "  AND window_start_ts = ? AND is_topup = 0 "
                "  AND outcome != 'CANCELLED' AND entry_minute != ? "
                "LIMIT 1",
                (
                    d.coin, sleeve.label, side,
                    d.window_start_ts, d.entry_minute,
                ),
            ).fetchone()
            if prior_anchor is not None:
                _mark_skip(d, "market_dedup")
                record_signal_skip(
                    conn,
                    created_at=utc_now_iso(),
                    coin=d.coin, sleeve=sleeve.label,
                    entry_minute=d.entry_minute,
                    side=side, window_start_ts=d.window_start_ts,
                    model_p=d.score.model_p,
                    ask=ask_book.top_ask,
                    edge=d.score.realized_edge,
                    fee_rate_bps=d.fee_rate_bps,
                    partial_ret_z_1h=d.score.partial_ret_z_1h,
                    lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                    lut_hit_rate=d.score.lut_hit_rate,
                    lut_n_cum=d.score.lut_n_cum,
                    skip_reason="market_dedup",
                    offset_sec=d.offset_sec,
                )
                _bump(tick.skips_by_reason, "market_dedup")
                tick.signals_written += 1
                conn.commit()
                return

        # Skip add-on attempts when the operator has disabled top-ups.
        if is_topup_attempt and not config.ENABLE_TOPUPS:
            _mark_skip(d, "topups_disabled")
            record_signal_skip(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=ask_book.top_ask,
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                skip_reason="topups_disabled",
                offset_sec=d.offset_sec,
            )
            _bump(tick.skips_by_reason, "topups_disabled")
            tick.signals_written += 1
            conn.commit()
            return

        _t_walk = time.monotonic()
        max_walk_price_cell = _execution_max_walk_price(sleeve)
        walked = walk_book_edge_preserving(
            {"asks": ask_book.asks},
            walker_target,
            prob_safe_floor=d.score.model_p,
            fee_theta=config.POLY_FEE_THETA,
            # Deliverable 8: per-market predict.fun feeRateBps (None on Polymarket).
            min_edge=_execution_min_edge(sleeve),
            min_bet_usd=config.MIN_BET_USD,
            max_walk_price=max_walk_price_cell,
            walk_anchor_price=anchor_for_walker,
        )
        # A0 latency: the ladder walk is the one pure-compute step between the
        # fresh ask and the POST, so the fast rung's placement line can name it
        # (see TickResult.ms_walk).
        tick.ms_walk += (time.monotonic() - _t_walk) * 1000.0

        # Review-pass #22 closeout: walk_book.py's `_sorted_asks` now
        # returns [] for empty/null/missing-`asks` bodies instead of
        # raising TypeError. The TypeError was a loud "CLOB degraded"
        # signal; the new silent-skip path muffles it. When the walker
        # returns None on a NON-EMPTY book — i.e. ``ask_book.asks`` had
        # rows but none cleared min_edge / min_bet_usd / max_walk_price
        # — surface a pipeline_health WARN so degraded-book events stay
        # observable. An empty book that gets normalized to [] by the
        # walker is its own legitimate signal (no top-of-book) and is
        # not warned on here.
        #
        # Scope (2026-07-03 tick-latency fix): when even the book's TOP
        # level fails the walker's edge floor — model_p − top_ask − fee <
        # min_edge — a None walk is the EXPECTED outcome of an unbettable
        # price, not book degradation. Sub-minute checkpoint cells hit
        # this on nearly every near-settle tick (asks 0.9+ can never
        # clear a 0.075 floor), which was flooding pipeline_health with
        # ~1 WARN/tick of pure noise plus its DB write. Only warn when
        # the top level DID clear the floor and the walk still failed
        # (depth below min_bet_usd, price-cap break, malformed levels).
        _warn_top = ask_book.top_ask
        _top_clears_floor = False
        if _warn_top is not None and math.isfinite(_warn_top):
            # Venue-dispatched fee (byte-identical parabola on Polymarket; TENT
            # on predict.fun, per-market feeRateBps threaded like every other
            # venue-fee call site). Observability-only: this warn-fee decides
            # whether a None walk on a non-empty book gets a pipeline_health
            # WARN.
            _warn_fee = fee_per_share(_warn_top, theta=config.POLY_FEE_THETA)
            _top_clears_floor = (
                d.score.model_p - _warn_top - _warn_fee
                >= _execution_min_edge(sleeve)
            )
        if walked is None and ask_book.asks and (
            _top_clears_floor or _warn_top is None
            or not math.isfinite(_warn_top)
        ):
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, coin, message) "
                "VALUES ('walk', 'WARN', ?, ?)",
                (
                    d.coin,
                    f"walker returned None on a non-empty {len(ask_book.asks)}-level "
                    f"book (sleeve={sleeve.label} em={d.entry_minute} "
                    f"top_ask={ask_book.top_ask})",
                ),
            )

        proposed_bet_size = walked[0] if walked is not None else 0.0
        walked_vwap = walked[2] if walked is not None else None
        # Audit w19oxqf24 (P2): the most-expensive consumed level — the
        # executable limit price for the LIVE GTC order, so it reaches the
        # depth bet_size was sized against. (WalkResult position 3.)
        walked_limit = walked[3] if walked is not None else None

        # Build gate input using top_ask (NOT walked VWAP — D2 parity).
        top_ask = ask_book.top_ask if ask_book.top_ask is not None else float("nan")
        # Total ask-side notional ($) on this side's book — feeds the exp044
        # book_consumption gate (inert for non-hgb sleeves, which leave
        # book_consumption_max None). The CLOB adapter now carries the full
        # sorted ask book plus this explicit scalar so the gate uses the same
        # full-book quantity the backtest validated. A 0.0 depth fails closed.
        book_total_ask_depth_usd = float(getattr(ask_book, "total_ask_depth_usd", 0.0) or 0.0)
        # D-LIVE-14 group-cap inputs — LIVE-only; DRY rows pass through
        # (`_gate_group_exposure` returns None when mode != LIVE).
        group_pending_baseline = wallet_snap.group_pending_usd or 0.0
        group_cap_usd = wallet_snap.group_cap_usd
        group_pending_in_tick = (
            live_ctx.tick_group_live_delta.get(wallet_snap.wallet_group or "", 0.0)
            if live_ctx is not None and wallet_snap.wallet_group is not None
            else 0.0
        )
        # Phase 5 shared-equity within-tick pending: for a member of a
        # shared_equity group the exposure gate must see the in-tick bets of
        # ALL group members against the SHARED cap (so sleeve B's gate counts
        # sleeve A's in-tick bet). `wallet_snap.pending_exposure` is already
        # the combined snapshot pending (capital.py post-pass), so we add ONLY
        # the in-tick deltas here — do NOT double-count the snapshot pending.
        # Non-shared / ungrouped wallets keep the per-wallet delta unchanged.
        shared_group = None
        group_tracks_equity = False
        if wallet_snap.wallet_group is not None:
            grp = portfolio.group_by_id(wallet_snap.wallet_group)
            if grp is not None:
                # Backtest-exposure mode: ceiling tracks live equity (gates.py).
                group_tracks_equity = getattr(grp, "cap_tracks_equity", False)
                if getattr(grp, "shared_equity", False):
                    shared_group = grp
        if shared_group is not None:
            pending_within_tick = sum(
                tick_pending_delta.get(m, 0.0) for m in shared_group.members
            )
        else:
            pending_within_tick = tick_pending_delta.get(wallet_id, 0.0)
        gate_input = GateInput(
            coin=d.coin,
            sleeve=sleeve,
            entry_minute=d.entry_minute,
            side=side,
            realized_edge=d.score.realized_edge,
            fill_price=top_ask,
            fee_rate_bps=d.fee_rate_bps,
            spot_age_s=d.spot_age_s,
            proposed_bet_size=proposed_bet_size,
            # Include the running within-tick delta so cells 2..N honor the
            # wallet cap against the up-to-date pending exposure (#5). For a
            # shared_equity member, `pending_within_tick` aggregates all
            # group members' in-tick deltas (Phase 5).
            pending_exposure_total=(
                wallet_snap.pending_exposure + pending_within_tick
            ),
            realized_capital=wallet_snap.realized_capital,
            slot_filled_usd=slot_filled,
            slot_target_usd=target_usd,
            disk_free_mb=disk_free_mb,
            book_top_ask_age_s=max(0.0, float(now_ts) - float(ask_book.fetched_at_ts)),
            book_total_ask_depth_usd=book_total_ask_depth_usd,
            is_topup=is_topup_attempt,
            exposure_cap_fraction=portfolio.exposure_cap_fraction_for(wallet_id),
            mode=wallet_mode,
            group_cap_usd=group_cap_usd,
            group_pending_usd=group_pending_baseline + group_pending_in_tick,
            live_wallet_balance_usd=(
                live_ctx.live_balance if live_ctx is not None else None
            ),
            live_cap_basis_usd=(
                live_ctx.live_cap_basis_usd if live_ctx is not None else None
            ),
            all_group_caps_sum_usd=(
                live_ctx.all_group_caps_sum_usd if live_ctx is not None else 0.0
            ),
            group_cap_tracks_equity=group_tracks_equity,
            partial_ret_z_1h=d.score.partial_ret_z_1h,
        )
        gate = evaluate_gates(gate_input)
        if not gate.passed:
            _mark_skip(d, gate.skip_reason or "unknown")
            record_signal_skip(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=top_ask,
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                skip_reason=gate.skip_reason or "unknown",
                mode=wallet_mode, wallet_group=wallet_group,
                offset_sec=d.offset_sec,
            )
            _bump(tick.skips_by_reason, gate.skip_reason or "unknown")
            tick.signals_written += 1
            conn.commit()
            return

        if walked is None or walked[0] < config.MIN_BET_USD:
            _mark_skip(d, "insufficient_depth")
            record_signal_skip(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=top_ask,
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                skip_reason="insufficient_depth",
                mode=wallet_mode, wallet_group=wallet_group,
                offset_sec=d.offset_sec,
            )
            _bump(tick.skips_by_reason, "insufficient_depth")
            tick.signals_written += 1
            conn.commit()
            return

        # Build BetSignal + record. fill_price uses top_ask (D2); walked_vwap
        # goes in the forensic column.
        token_id = (d.poly_token_id_yes if side == "YES" else d.poly_token_id_no) or ""
        entry_top_for_row = slot_anchor if (is_topup_attempt and slot_anchor is not None) else top_ask

        # Anchored FAK pad (config.V2_FAK_PAD, operator-approved 2026-07-11;
        # default 0.0 = OFF => byte-identical). For a LIVE sub-minute BUY FAK,
        # lift the executable limit by PAD to cross a book that ticked up between
        # the score and the sign, clamped to first_limit_of_this_side_window + PAD
        # so a retry never chases a runaway book, and clipped to <= 0.99. Scope
        # guards: flag on; wallet_mode LIVE (DRY paper rows send the bare
        # walked_limit); sub-minute only (offset_sec is not None) — the
        # SELL paths are untouched;
        # walked_limit present. Gates
        # ran above on the OBSERVED ask (top_ask), so the padded limit never feeds
        # band/gate/selection. The FIRST placed attempt's walked_limit is captured
        # on the decider as the anchor (a multifill extra entry shares that anchor
        # — inert below the $5k equity floor today). The padded value is what is
        # SENT, so subminute_fills/ledger.limit_price forensically record it.
        limit_price_out = walked_limit
        if (
            config.V2_FAK_PAD > 0.0
            and wallet_mode == "LIVE"
            and d.offset_sec is not None
            and walked_limit is not None
        ):
            # V2_RETRY_REPRICE: with the clamp dropped, attempt 2..N pays its own
            # fresh walked_limit + PAD instead of the first attempt's. Attempt 1
            # is identical either way (its anchor IS its walked_limit).
            _fak_anchor = (
                None if config.V2_RETRY_REPRICE
                else _v2_decider_for(sleeve).anchor_limit(
                    _v2_decider_cell(
                        d.coin, sleeve.interval, d.window_start_ts, side
                    ),
                    walked_limit,
                )
            )
            limit_price_out = _fak_padded_limit(
                walked_limit, anchor=_fak_anchor, pad=config.V2_FAK_PAD
            )

        signal = BetSignal(
            coin=d.coin,
            sleeve=sleeve.label,
            entry_minute=d.entry_minute,
            side=side,
            window_start_ts=d.window_start_ts,
            window_end_ts=d.window_end_ts,
            interval_min=sleeve.interval_min,
            poly_market_id=d.poly_market_id or "",
            poly_token_id=token_id,
            fee_rate_bps=d.fee_rate_bps or 0.0,
            model_p=d.score.model_p,
            p_market=top_ask,
            fill_price=top_ask,
            walked_vwap=walked_vwap,
            limit_price=limit_price_out,
            realized_edge=d.score.realized_edge,
            partial_ret_z_1h=d.score.partial_ret_z_1h,
            lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
            lut_hit_rate=d.score.lut_hit_rate,
            lut_n_cum=d.score.lut_n_cum,
            target_usd=target_usd,
            bet_size=proposed_bet_size,
            slot_filled_pre=slot_filled,
            entry_top_price=entry_top_for_row,
            yes_pre1_anchor=top_ask,
            strike_price=d.strike_price,
            window_open_spot=d.window_open_spot,
            wallet_id=wallet_id,
            offset_sec=d.offset_sec,
            config_sha256=config_sha256,
        )
        # For top-ups, look up the slot's first-fill realized_edge so we
        # can populate add_on_realized_edge_minus_initial_edge (R-NEW-9).
        initial_edge = None
        if is_topup_attempt:
            r = conn.execute(
                "SELECT realized_edge FROM ledger "
                "WHERE coin=? AND sleeve=? AND entry_minute=? AND side=? "
                "  AND window_start_ts=? AND outcome != 'CANCELLED' "
                "  AND is_topup=0 "
                "ORDER BY bet_ts ASC, id ASC LIMIT 1",
                (d.coin, sleeve.label, d.entry_minute, side, d.window_start_ts),
            ).fetchone()
            if r is not None:
                initial_edge = float(r["realized_edge"])

        live_order_id: Optional[str] = None
        live_client_order_id: Optional[str] = None
        live_attempt_id: Optional[int] = None
        live_filled_size: Optional[float] = None
        live_filled_price: Optional[float] = None
        live_fill_ts: Optional[str] = None
        _imm_intent_price: Optional[float] = None
        # LEVER 1 (CRYPTOBOT_PHASE_B_PARALLEL_POST): when this cell's order POST
        # is deferred for the parallel fan-out, its within-tick exposure is
        # applied PESSIMISTICALLY at reserve time (see the parallel branch), so
        # the shared finalize below must NOT re-apply it. DRY + serial-LIVE leave
        # this False and apply the delta on accept exactly as before.
        _exposure_pre_applied = False
        def _apply_within_tick_exposure() -> None:
            """Add this cell's stake to the running within-tick exposure deltas
            so the NEXT cell's exposure gate sees the up-to-date total (#5).

            ONE copy shared by the serial-accept path (_finalize_bet_row, under
            `if not _exposure_pre_applied`) and the parallel-defer branch (which
            applies it PESSIMISTICALLY at reserve time, then sets the flag). The
            LIVE-group guard makes it a no-op on the group delta for DRY cells
            (live_ctx is None) — the parallel branch is LIVE-only so the guard is
            always true there, preserving that branch's behavior exactly."""
            tick_pending_delta[wallet_id] = (
                tick_pending_delta.get(wallet_id, 0.0) + proposed_bet_size
            )
            if (
                wallet_mode == "LIVE"
                and live_ctx is not None
                and wallet_snap.wallet_group is not None
            ):
                gid = wallet_snap.wallet_group
                live_ctx.tick_group_live_delta[gid] = (
                    live_ctx.tick_group_live_delta.get(gid, 0.0) + proposed_bet_size
                )
            # Exposure just moved => every cached commit-stage view is stale
            # (_FastCommitStage clause 1). Covers the DRY paper fill and the
            # LIVE accept; the reservation itself bumps separately below.
            _bump_placement_epoch()

        def _finalize_bet_row() -> None:
            """Shared post-POST tail (DRY paper fill + LIVE accept).

            record_bet (+ the LIVE attempt SUBMITTED/LINK transition),
            consume-on-fill note_fill, would-bet telemetry, counters, and the
            within-tick exposure deltas. ONE copy shared by the serial path
            (called inline right after the inline POST) and the parallel finish
            (called by _flush_parallel_posts). Assigns only its OWN locals, so it
            needs no ``nonlocal`` — it READS the live_* / _imm_intent_price cells
            _process_live_post set on accept."""
            _record_bet_kwargs = dict(
                initial_realized_edge=initial_edge,
                mode=wallet_mode,
                wallet_group=wallet_group,
                live_order_id=live_order_id,
                live_client_order_id=live_client_order_id,
                live_filled_size=live_filled_size,
                live_filled_price=live_filled_price,
                live_fill_ts=live_fill_ts,
                top_ask_at_decision=top_ask,
            )
            _live_ledger_path = (
                wallet_mode == "LIVE"
                and live_ctx is not None
                and live_ctx.order_client is not None
                and live_client_order_id is not None
            )
            if _live_ledger_path:
                # Audit P1 #3: reserve_live_order_attempt() already committed (the
                # CLOB call MUST run outside any SQLite txn), so the outer
                # BEGIN IMMEDIATE opened at the top of this slot is no longer held
                # here and its rollback handler cannot undo the placed order. Wrap
                # the SUBMITTED transition (A13 fold — one commit, not two) + the
                # ledger INSERT + attempt-link in their OWN short BEGIN IMMEDIATE
                # so they commit ATOMICALLY: a failure rolls back the ledger row
                # and leaves the SUBMITTING attempt for reconcile_stale_attempts
                # to rebuild from signal_json (audit P0 #2). LIVE slot
                # serialization is the uniq_loa_active_slot partial index (the
                # dissolved outer lock never protected this path).
                conn.execute("BEGIN IMMEDIATE")
                try:
                    mark_attempt_submitted(
                        conn, live_attempt_id, live_order_id or "", commit=False
                    )
                    row_id = record_bet(conn, signal, **_record_bet_kwargs)
                    # Link the attempt to the freshly-written ledger row — releases
                    # the active-slot partial unique index AND drops the group-cap
                    # reservation contribution (LINKED rows are excluded from the
                    # pending sum). link_attempt_to_ledger commits, flushing the
                    # SUBMITTED transition + INSERT + UPDATE atomically; the
                    # reconciler picks up SUBMITTED-without-link rows on its next
                    # pass.
                    cur = conn.execute(
                        "SELECT id FROM live_order_attempts WHERE client_order_id=?",
                        (live_client_order_id,),
                    ).fetchone()
                    if cur is not None:
                        link_attempt_to_ledger(conn, int(cur["id"]), row_id)
                    else:
                        conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                # A11: queue the immediate-fill GET now that the row is committed
                # (the write-back needs the ledger id). Same eligibility the
                # inline GET enforces (sub-minute, order id present, POST left no
                # fill); _immediate_fill_for_held_pass re-checks at flush time.
                if (
                    deferred_fill_gets is not None
                    and d.offset_sec is not None
                    and live_order_id
                    and (live_filled_size is None or float(live_filled_size) <= 0)
                ):
                    deferred_fill_gets.append(
                        _DeferredFillGet(
                            order_client=live_ctx.order_client,
                            ledger_id=row_id,
                            order_id=live_order_id,
                            offset_sec=d.offset_sec,
                            current_size=live_filled_size,
                            intent_amount_usd=float(signal.bet_size),
                            intent_price=_imm_intent_price,
                        )
                    )
            else:
                # DRY path: record_bet runs inside the outer BEGIN IMMEDIATE and is
                # committed (or rolled back) by the handler at the end of the slot.
                row_id = record_bet(conn, signal, **_record_bet_kwargs)
            # v2 consume-on-fill: a bet was recorded (LIVE fill or DRY paper fill) —
            # consume the window for BOTH sides so no later second / opposite side
            # re-bets it. No-op unless the decider is in consume-on-fill mode
            # (CRYPTOBOT_V2_REJECT_RETRY=1); the durable subminute_window / ledger
            # dedup remains the restart-proof hard guard.
            if d.offset_sec is not None and config.SUBMINUTE_ONE_PER_WINDOW:
                _v2_decider_for(sleeve).note_fill(
                    _v2_decider_cell(
                        d.coin, sleeve.interval, d.window_start_ts, side
                    )
                )
                # Cross-sleeve one-bet-per-market: this market is now owned. Record
                # it in the shared set so EVERY OTHER sleeve's decider (each holding
                # a private _filled) is vetoed on this (coin, interval, window). The
                # durable ledger row backs this up across a process restart.
                _V2_MARKET_FILLED.add((d.coin, sleeve.interval, d.window_start_ts))
            record_signal_would_bet(
                conn,
                created_at=utc_now_iso(),
                coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                side=side, window_start_ts=d.window_start_ts,
                model_p=d.score.model_p, ask=top_ask,
                edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                partial_ret_z_1h=d.score.partial_ret_z_1h,
                lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                mode=wallet_mode, wallet_group=wallet_group,
                offset_sec=d.offset_sec,
            )
            _mark_would_bet(d)
            tick.pending_rows_written += 1
            if is_topup_attempt:
                tick.topup_rows_written += 1
            tick.signals_written += 1
            # Add this row's notional to the running pending-exposure delta so
            # the next cell's exposure gate sees the up-to-date total (#5).
            # LEVER 1: skip when the parallel fan-out already applied this stake
            # PESSIMISTICALLY at reserve time — re-applying here would double it.
            if not _exposure_pre_applied:
                _apply_within_tick_exposure()
            conn.commit()

        def _process_live_post(order_result, order_ms: float) -> None:
            """LIVE post-POST processing: latency accounting + subminute-fill
            telemetry + reject/accept, then the shared finalize on accept.

            Runs inline (serial path) or via _flush_parallel_posts (parallel
            path) — ALWAYS serially, so every sqlite write + ``tick`` counter
            update stays single-threaded. ``order_ms`` is the POST round-trip
            wall time measured by _do_post (in the worker thread under the
            parallel flag) so ms_order_total is still attributable."""
            nonlocal live_order_id, live_client_order_id, live_attempt_id
            nonlocal live_filled_size, live_filled_price, live_fill_ts
            nonlocal _imm_intent_price
            # A0 latency instrumentation: accumulate the whole placement
            # envelope + the sign/post split the OrderClient measured.
            tick.ms_order_total += order_ms
            tick.n_orders += 1
            if getattr(order_result, "sign_ms", None) is not None:
                tick.ms_order_sign += float(order_result.sign_ms)
            # ACCEPT vs REJECT kept in separate accumulators (2026-07-25): a
            # venue-refused FAK now reports a real post_ms, but folding it into
            # ms_order_post would silently redefine the metric mid-series. The
            # dashboard reads both and shows them side by side.
            _post_ms = getattr(order_result, "post_ms", None)
            if _post_ms is not None:
                if order_result.success:
                    tick.ms_order_post += float(_post_ms)
                else:
                    tick.ms_order_post_rej += float(_post_ms)
                    tick.n_orders_rej += 1
            elif not order_result.success:
                tick.n_orders_rej += 1
            # B4: every LIVE sub-minute order's requested-vs-realized fill
            # feeds the fill-rate breaker — record accepted AND rejected
            # placements (ghost quotes show up as accepted-but-unfilled or
            # venue-rejected FAKs). Best-effort: a record failure must not
            # affect the order path.
            if d.offset_sec is not None:
                # Attempt number within the (market, side) window. In
                # consume-on-fill mode (CRYPTOBOT_V2_REJECT_RETRY=1) each
                # qualifying second is a fresh placement, so the attempt count
                # advances 1, 2, 3… per real order; legacy/stacking modes place
                # once per key so attempt stays 1.
                if config.SUBMINUTE_ONE_PER_WINDOW and config.V2_REJECT_RETRY:
                    _sub_attempt = _v2_decider_for(sleeve).next_attempt(
                        _v2_decider_cell(
                            d.coin, sleeve.interval, d.window_start_ts, side
                        )
                    )
                else:
                    _sub_attempt = 1
                try:
                    record_subminute_fill(
                        conn,
                        coin=d.coin,
                        sleeve=sleeve.label,
                        offset_sec=d.offset_sec,
                        window_start_ts=d.window_start_ts,
                        side=side,
                        requested_usd=float(signal.bet_size),
                        quote_ask=top_ask,
                        limit_price=signal.limit_price,
                        filled_size=order_result.filled_size_tokens,
                        filled_price=order_result.filled_price_vwap,
                        filled_usd=order_result.filled_value_usd,
                        order_id=order_result.order_id,
                        client_order_id=attempt.client_order_id,
                        outcome=(
                            "accepted" if order_result.success else "rejected"
                        ),
                        attempt=_sub_attempt,
                        # Fast-trigger provenance, carried on the cell (see
                        # CellDecision.trigger_src) so the parallel-post flush
                        # stamps the right rung even off the deciding thread.
                        trigger_src=d.trigger_src,
                    )
                except Exception:  # noqa: BLE001 — observability only
                    logger.warning(
                        "subminute fill record failed", exc_info=True
                    )
            if not order_result.success:
                # Terminal CLOB failures land here; mark attempt FAILED so
                # the slot reservation releases and another tick can retry
                # the same slot key cleanly. Group-cap pending reservation
                # also disappears as a side effect.
                mark_attempt_failed(
                    conn,
                    attempt.id,
                    order_result.error_kind or "unknown",
                    order_result.error,
                )
                # Wake the operator if the CLOB is failing placements in a
                # burst (HTTP 425 / post-only). Silent 425 bursts on
                # 2026-06-19 / 06-24 lost ~7 bet-windows undetected.
                _maybe_alert_venue_degradation(
                    conn,
                    live_ctx,
                    error_kind=order_result.error_kind,
                    coin=d.coin,
                    sleeve=sleeve.label,
                    wallet_id=signal.wallet_id,
                )

                # v2 consume-on-fill (operator 2026-07-09): a FAK rejection does
                # NOT consume the window — the decider was NOT told note_fill, so
                # the NEXT second is re-evaluated by the normal per-second path
                # (fresh book / fresh score / band+gate at that second's ask).
                # There is no special re-quote placement; the tick loop IS the
                # retry. Finalize this attempt's reject.
                skip_reason = f"clob_{order_result.error_kind or 'unknown'}"
                _mark_skip(d, skip_reason)
                record_signal_skip(
                    conn,
                    created_at=utc_now_iso(),
                    coin=d.coin, sleeve=sleeve.label,
                    entry_minute=d.entry_minute,
                    side=side, window_start_ts=d.window_start_ts,
                    model_p=d.score.model_p, ask=top_ask,
                    edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                    partial_ret_z_1h=d.score.partial_ret_z_1h,
                    lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                    lut_hit_rate=d.score.lut_hit_rate,
                    lut_n_cum=d.score.lut_n_cum,
                    skip_reason=skip_reason,
                    mode=wallet_mode, wallet_group=wallet_group,
                    offset_sec=d.offset_sec,
                )
                # Reject re-quote TELEMETRY (observation only): persist this
                # rejection's placement intent + queue the ~1s-later book
                # re-read (the ghost-vs-adverse-move probe). Fires on EVERY
                # reject now. Best-effort — never break the order path.
                try:
                    from cryptobot.execution.reject_requote import (
                        enqueue_requote_reread,
                        insert_reject_requote,
                    )

                    _rr_id = insert_reject_requote(
                        conn,
                        coin=d.coin,
                        sleeve=sleeve.label,
                        offset_sec=d.offset_sec,
                        window_start_ts=d.window_start_ts,
                        side=side,
                        limit_price=signal.limit_price,
                        requested_usd=float(signal.bet_size),
                    )
                    if _rr_id is not None and token_id:
                        enqueue_requote_reread(_rr_id, token_id)
                except Exception:  # noqa: BLE001 — telemetry only
                    logger.debug(
                        "reject_requote enqueue failed", exc_info=True
                    )
                _bump(tick.skips_by_reason, skip_reason)
                tick.signals_written += 1
                conn.commit()
                return

            # A13 latency: the SUBMITTING → SUBMITTED transition now rides
            # the ledger BEGIN IMMEDIATE below (mark_attempt_submitted with
            # commit=False) — one fsync'd commit instead of two on the order
            # path. Crash-recovery is unchanged in kind: a crash between the
            # POST and that commit now leaves the attempt SUBMITTING (was:
            # SUBMITTED), and reconcile_stale_attempts recovers BOTH via the
            # client_order_id / order-intent match on the persisted
            # signal_json (audit P0 #2 — the SUBMITTING-without-ids path is
            # test-pinned).
            live_order_id = order_result.order_id
            live_client_order_id = attempt.client_order_id
            live_attempt_id = attempt.id
            live_filled_size = order_result.filled_size_tokens
            live_filled_price = order_result.filled_price_vwap
            live_fill_ts = order_result.fill_ts

            # Held-position/T3 coverage (2026-07-10 root-cause fix): when
            # the FAK response left no fill (the common case), issue ONE
            # immediate GET so the realized fill is persisted on THIS row at
            # accept time — otherwise the 60s reconciler is the first writer and,
            # for a last-30s sub-minute entry, it lands after window_end so the
            # pass never sees the position as a candidate. Sub-minute
            # only; the reconciler still corrects within 60s if the GET finds
            # nothing. A11: with a deferred_fill_gets queue wired (the
            # run_scanner_tick path) the GET is queued AFTER the ledger row
            # commits below and flushed once the Phase B loops finish — off
            # the next cell's POST path, writes landed before the held pass.
            # Legacy callers without the queue keep the inline GET so the
            # fill still reaches record_bet directly.
            _imm_intent_price = (
                signal.walked_vwap
                if signal.walked_vwap is not None
                else signal.fill_price
            )
            if deferred_fill_gets is None:
                _imm_fill = _immediate_fill_for_held_pass(
                    live_ctx.order_client,
                    live_order_id,
                    offset_sec=d.offset_sec,
                    current_size=live_filled_size,
                    intent_amount_usd=float(signal.bet_size),
                    intent_price=_imm_intent_price,
                )
                if _imm_fill is not None:
                    live_filled_size, live_filled_price, _imm_ts = _imm_fill
                    if _imm_ts is not None:
                        live_fill_ts = _imm_ts

            # Accept path: write the ledger row + telemetry via the shared tail.
            _finalize_bet_row()

        if wallet_mode == "LIVE" and live_ctx is not None and live_ctx.order_client is not None:
            # Reserve attempt OUTSIDE the slot lock — the attempt commit
            # bumps the SQLite snapshot we are working on, but the
            # active-slot partial unique index serializes any racing
            # reservation. On reservation failure (IntegrityError), we
            # treat it as `slot_full` because another tick is already
            # mid-flight on the same slot.
            # The reserve bumps the placement epoch itself: a reservation
            # consumes group-cap pending AND leads to the `subminute_fills` row
            # the fill-rate breaker reads, so every cached commit-stage view is
            # stale from the moment the row commits — BEFORE the POST, so a crash
            # mid-flight still leaves the next fire re-reading the world.
            try:
                attempt = reserve_live_order_attempt(
                    conn, signal, wallet_group=wallet_group
                )
            except sqlite3.IntegrityError:
                _mark_skip(d, "slot_full")
                record_signal_skip(
                    conn,
                    created_at=utc_now_iso(),
                    coin=d.coin, sleeve=sleeve.label, entry_minute=d.entry_minute,
                    side=side, window_start_ts=d.window_start_ts,
                    model_p=d.score.model_p, ask=top_ask,
                    edge=d.score.realized_edge, fee_rate_bps=d.fee_rate_bps,
                    partial_ret_z_1h=d.score.partial_ret_z_1h,
                    lut_p_bucket=_bucket_or_none(d.score.lut_p_bucket),
                    lut_hit_rate=d.score.lut_hit_rate, lut_n_cum=d.score.lut_n_cum,
                    skip_reason="slot_full",
                    mode=wallet_mode, wallet_group=wallet_group,
                    offset_sec=d.offset_sec,
                )
                _bump(tick.skips_by_reason, "slot_full")
                tick.signals_written += 1
                conn.commit()
                return

            # L4 warm-guarantee, FINAL guard: make sure the CLOB client's
            # static market-info cache is populated for this condition_id
            # BEFORE the FAK signs, so the SIGN phase never pays 2 cache-miss
            # GETs on the hot path. A14 moved the expected sync-warm cost to
            # Pass-1 discovery in _resolve_net_cells (paid during scoring,
            # off the reserve→POST gap), so this is a no-op whenever that
            # ran; it still covers a cell whose market resolved outside
            # Pass 1 (once per condition_id — a down venue can't stall every
            # order). getattr-guarded: test stubs lack the method.
            _ensure_warm = getattr(
                live_ctx.order_client, "ensure_market_info_warm", None
            )
            if _ensure_warm is not None and signal.poly_market_id:
                try:
                    _ensure_warm(signal.poly_market_id)
                except Exception:  # noqa: BLE001 — warm is best-effort
                    logger.debug("ensure_market_info_warm raised", exc_info=True)

            def _do_post():
                # The ONLY step the parallel lever fans out — pure network, no
                # sqlite connection and no `tick` counter touched, so it is safe
                # to run on a transient worker thread. Times the venue round trip
                # so the SERIAL finish can still attribute ms_order_total. The
                # CLOB call happens OUTSIDE the slot-lock commit
                # (reserve_live_order_attempt committed) so a crash mid-flight
                # leaves the SUBMITTING row for the reconciler.
                _t_order = time.monotonic()
                # DECISION LATENCY (2026-07-25): tick start -> the first order
                # POST of this tick leaving the box. Everything before this is
                # client-controlled (feeds, scoring, gates, sizing, reserve);
                # everything after is the venue's 250ms uncancellable taker delay
                # + matching. This is the only figure a runtime/language change
                # could ever move, and nothing measured it before. Plain float
                # write, first-writer-wins; under the parallel lever the fan-out
                # is >1 order and the earliest stamp is the one we want.
                if tick.ms_pre_post <= 0.0 and tick._t0_monotonic:
                    tick.ms_pre_post = (_t_order - tick._t0_monotonic) * 1000.0
                _result = live_ctx.order_client.place_gtc_limit_order(
                    signal, client_order_id=attempt.client_order_id,
                )
                return _result, (time.monotonic() - _t_order) * 1000.0

            if parallel_posts is not None:
                # LEVER 1 (CRYPTOBOT_PHASE_B_PARALLEL_POST): DEFER the POST so
                # run_scanner_tick can fan every reserved cell's ~250ms venue
                # round trip out concurrently. Apply this reserved cell's stake
                # to the within-tick exposure deltas NOW so the REMAINING cells'
                # pre-POST gates (still running serially) honor it —
                # PESSIMISTICALLY: pre-POST cannot know this POST's outcome, so a
                # later-rejected POST still consumed the within-tick headroom.
                # This DIVERGES from the serial path (which applies the delta
                # only on ACCEPT, in _finalize_bet_row) but is strictly
                # conservative and never over-exposes. `_exposure_pre_applied`
                # tells _finalize_bet_row not to double-apply on accept. The
                # POST + post-POST processing replays via _flush_parallel_posts:
                # POSTs concurrent, `finish` serial in cell order.
                _apply_within_tick_exposure()
                _exposure_pre_applied = True
                parallel_posts.append(
                    _PreparedPost(
                        do_post=_do_post,
                        finish=_process_live_post,
                        attempt_id=attempt.id,
                    )
                )
                return

            # Serial path (flag off): POST inline, then process — byte-identical
            # to the pre-lever code.
            _order_result, _order_ms = _do_post()
            _process_live_post(_order_result, _order_ms)
            return

        # DRY path (and any LIVE wallet with no order_client wired): no POST —
        # run the shared finalize directly. record_bet then runs inside the
        # outer BEGIN IMMEDIATE and is committed by the handler at slot end.
        _finalize_bet_row()
    except Exception:
        conn.rollback()
        raise


def _notify(
    live_ctx: _LiveCommitContext,
    event_type: str,
    message: str,
    *,
    wallet_id: Optional[str] = None,
    critical: bool = False,
) -> None:
    """Best-effort notify dispatch via the injected callback. Failures are
    swallowed so notify infrastructure can never break the pipeline."""
    if live_ctx.notify is None:
        return
    try:
        live_ctx.notify(event_type, message, wallet_id=wallet_id, critical=critical)
    except Exception:  # noqa: BLE001
        logger.warning("pipeline: notify dispatch failed", exc_info=True)


# Venue-degradation alert (audit: on 2026-06-19 / 06-24 the Polymarket CLOB
# returned HTTP 425 "order manager not ready" in multi-minute bursts; ~79
# order attempts failed and NO operator signal fired, so ~7 bet-windows were
# silently lost). `venue_unavailable` (425) and `post_only` (503) are the
# transient venue kinds; a repeated burst is worth waking the operator.
_VENUE_ERROR_KINDS: frozenset[str] = frozenset({"venue_unavailable", "post_only"})
_VENUE_ALERT_WINDOW_MIN: int = 10
_VENUE_ALERT_THRESHOLD: int = 3
_VENUE_ALERT_COOLDOWN_S: float = 600.0
# Per-process cooldown bookmark so a long outage yields ~1 CRITICAL per
# cooldown, not one per retry. In-memory (resets on restart — re-alerting
# after a restart during an ongoing outage is acceptable).
_LAST_VENUE_ALERT_MONO: dict[str, float] = {}


def _maybe_alert_venue_degradation(
    conn: sqlite3.Connection,
    live_ctx: _LiveCommitContext,
    *,
    error_kind: Optional[str],
    coin: str,
    sleeve: str,
    wallet_id: Optional[str],
) -> None:
    """Fire a CRITICAL operator alert when LIVE order placement is REPEATEDLY
    failing on a transient venue error (HTTP 425 order-manager-not-ready /
    503 post-only). Threshold is stateless — a COUNT over the recent window
    of `live_order_attempts` — so it survives process restarts; the in-process
    cooldown dedups the CRITICAL fire. Best-effort: never raises."""
    if error_kind not in _VENUE_ERROR_KINDS:
        return
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM live_order_attempts "
            "WHERE status='FAILED' "
            "AND error_kind IN ('venue_unavailable','post_only') "
            "AND created_at >= datetime('now', ?)",
            (f"-{_VENUE_ALERT_WINDOW_MIN} minutes",),
        ).fetchone()
    except sqlite3.Error:
        return
    count = int(row[0]) if row and row[0] is not None else 0
    if count < _VENUE_ALERT_THRESHOLD:
        return
    now = time.monotonic()
    last = _LAST_VENUE_ALERT_MONO.get("venue")
    if last is not None and (now - last) < _VENUE_ALERT_COOLDOWN_S:
        return
    _LAST_VENUE_ALERT_MONO["venue"] = now
    _notify(
        live_ctx,
        "clob_venue_degraded",
        (
            f"LIVE order placement failing: {count} venue errors "
            f"({error_kind}) in {_VENUE_ALERT_WINDOW_MIN}min "
            f"(latest {coin}/{sleeve}). Polymarket CLOB likely degraded — "
            f"qualifying bets are being missed until it recovers."
        ),
        wallet_id=wallet_id,
        critical=True,
    )


def _pick_ask(d: CellDecision, side: str) -> Optional[float]:
    book = _book_for_side(d, side)
    return book.top_ask if book is not None else None


def _book_for_side(d: CellDecision, side: str) -> Optional[BookSnapshot]:
    if side == "YES":
        return d.yes_book
    return d.no_book
