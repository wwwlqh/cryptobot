"""One scanner tick, end to end.

``run_scanner_tick(conn, portfolio, now_ts=..., deps=...)`` produces:

  - zero or more PENDING ledger rows (one per placed or paper bet)
  - one ``signals`` row per evaluated (coin, sleeve, offset, side)
  - a ``TickResult`` for the dashboard snapshot

It runs in two phases:

  Phase A  score every (coin x sleeve) cell due this second. No writes.
           Cells that pass the cheap pre-score are resolved together:
           market metadata, ONE batched order-book fetch, then a rescore
           with live asks.
  Phase B  per surviving cell: dedup -> sizing -> gates -> book walk ->
           order -> ledger row, inside a BEGIN IMMEDIATE transaction.

The same Phase B commit path also serves the sub-second fast-entry scan
(``run_fast_entry_scan``), so both paths apply identical gates.
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

# Per-window entry state machine (``spec_v2.WindowDecider``). The pipeline is a
# per-tick function, so this module-level object carries window state across
# the 1 Hz ticks of one process (it resets on restart; the ledger is the
# durable guard against betting an already-placed window). In consume-on-fill
# mode a rejected order leaves the window open and only a fill closes it.
_V2_WINDOW_DECIDER = spec_v2.WindowDecider(consume_on_fill=config.V2_REJECT_RETRY)
# One decider per sleeve when the sleeve's band or gate differs from the
# default; sleeves on the default band share ``_V2_WINDOW_DECIDER``.
_V2_WINDOW_DECIDERS: dict[str, spec_v2.WindowDecider] = {}
# One bet per market across sleeves: once any sleeve fills a (coin, interval,
# window), other sleeves are vetoed here. The ledger check
# ``subminute_market_other_sleeve`` covers the same rule across restarts.
_V2_MARKET_FILLED: set[tuple] = set()
# Bound the decider state so a long-running process cannot leak memory.
_V2_DECIDER_PRUNE_ABOVE = 5000
_V2_DECIDER_RETAIN_SECONDS = 3600


def _v2_decider_for(sleeve: Sleeve) -> spec_v2.WindowDecider:
    """Return the WindowDecider that owns ``sleeve``.

    ``consume_on_fill`` is re-read from config on every call so the flag acts
    immediately.
    """
    label = sleeve.label
    dec = _V2_WINDOW_DECIDERS.get(label)
    if dec is None:
        ranges = tuple(getattr(sleeve, "ask_bucket_ranges", ()) or ())
        if len(ranges) == 1:
            lo, hi = float(ranges[0][0]), float(ranges[0][1])
        else:
            # Multi-range or missing band: fall back to the default band.
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
    """Drop decider state for windows older than an hour once it grows large.

    Every key holds ``window_start`` at index 2, so one cutoff evicts finished
    windows from every structure. Windows last at most 900s, so a 3600s
    retention never evicts an open one.
    """
    global _V2_MARKET_FILLED
    cutoff = int(now_ts) - _V2_DECIDER_RETAIN_SECONDS
    # De-dup by identity: default-band sleeves share _V2_WINDOW_DECIDER.
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
    """Minimal cell mapping with the keys ``WindowDecider`` reads."""
    return {
        "coin": coin,
        "interval": interval,
        "window_start": window_start_ts,
        "side": side,
    }


def _fak_padded_limit(
    walked_limit: float, *, anchor: Optional[float], pad: float
) -> float:
    """Limit price for a live FAK buy, lifted by ``pad``
    (``config.V2_FAK_PAD``).

    ``padded = min(walked_limit + pad, anchor + pad)``, capped at 0.99, where
    ``anchor`` is the walked limit of the first attempt in this side window, so
    a retry never chases a book that ran away. ``anchor=None`` drops that clamp
    and prices the retry off its own walk (``config.V2_RETRY_REPRICE``). The
    cash stake is unchanged; a higher price just buys fewer shares.
    """
    padded = (
        walked_limit + pad if anchor is None
        else min(walked_limit + pad, anchor + pad)
    )
    return min(padded, 0.99)


def _v2_sizing_equity(
    live_ctx: Optional["_LiveCommitContext"], realized_capital: float
) -> float:
    """Equity to size stakes from.

    Uses the cached exchange balance when it is fresh, else the wallet's
    ledger-derived capital, so placement never waits on a balance fetch.
    """
    if live_ctx is not None and live_ctx.live_balance is not None:
        return float(live_ctx.live_balance)
    return float(realized_capital)


def _v2_apply_coin_cap(sleeve: Sleeve, coin: str, target_usd: float) -> float:
    """Clamp the final stake to the sleeve's per-coin cap, if one is
    configured.

    ``sizing_v2.sized_usd`` replaces the target from ``target_bet_size_for``,
    so the per-coin cap has to be applied again here, after sizing.
    """
    cap = sleeve.max_bet_usd_for(coin)
    if cap is None:
        return target_usd
    return min(target_usd, float(cap))


@dataclass
class CellDecision:
    """Phase A output: one scored (coin, sleeve, offset) cell."""

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
    # Underlying spot at decision time, stored on the ledger row.
    strike_price: Optional[float] = None
    # The window's opening price (Chainlink TWAP): the model's anchor and the
    # strike Polymarket settles on. None skips the cell with
    # ``no_window_open_spot``, since a bet we cannot grade honestly is refused.
    window_open_spot: Optional[float] = None
    # Seconds since the window opened (None for minute-boundary sleeves).
    offset_sec: Optional[int] = None
    # Final outcome after every gate, shown on the dashboard. ``score`` alone
    # only says whether the model produced a signal.
    final_decision: Optional[str] = None
    final_skip_reason: Optional[str] = None
    # Which trigger produced the cell: 'tick' (the 1 Hz scanner) or
    # 'fast_sweep' / 'fast_event'. Stored on the ``subminute_fills`` row.
    # Carried on the cell because with parallel posting the fill row may be
    # written on another thread.
    trigger_src: str = "tick"


@dataclass
class TickResult:
    """One tick's output for the dashboard and latency telemetry."""

    tick_ts: int
    cells_evaluated: int = 0
    pending_rows_written: int = 0
    topup_rows_written: int = 0
    signals_written: int = 0
    skips_by_reason: dict[str, int] = field(default_factory=dict)
    cell_decisions: list[CellDecision] = field(default_factory=list)
    duration_ms_total: float = 0.0
    # Monotonic tick start; lets the order path stamp ``ms_pre_post``.
    _t0_monotonic: float = 0.0
    error: Optional[str] = None
    # Per-phase wall-clock timings. Only network phases are split out; the rest
    # of ``duration_ms_total`` is compute and SQLite. Written as a
    # stage='latency' pipeline_health row on network-active ticks only.
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
    # How many books each source served this tick (WebSocket mirror, REST
    # prefetch cache, inline REST), plus the oldest WebSocket book served.
    n_book_ws: int = 0
    n_book_prefetch: int = 0
    n_book_rest: int = 0
    book_ws_age_ms_max: float = 0.0
    # Accepted and rejected orders are timed separately. A rejected FAK is
    # still a full round trip (~286ms vs ~415ms for an accept), and mixing the
    # two would hide both numbers.
    ms_order_post_rej: float = 0.0
    n_orders_rej: int = 0
    # Decision latency: tick start to the first order leaving the box.
    # Everything after it is the exchange's 250ms taker delay and matching.
    ms_pre_post: float = 0.0
    # Post-order tail: next-window prefetch and the trace write. It delays no
    # fill. Lower bound only: the immediate-fill lookups run before
    # ``_tail_t0``.
    ms_tail: float = 0.0
    # Total time spent in ``walk_book_edge_preserving`` inside
    # ``_commit_cell``.
    ms_walk: float = 0.0


@dataclass
class _NetCell:
    """A Phase A cell that passed the cheap pre-score and still needs market
    metadata and books. Buffered so every cell's books come back in ONE
    ``POST /books`` call (see ``_resolve_net_cells``).
    """

    sleeve: Sleeve
    entry_minute: int
    window_start: int
    window_end: int
    coin: str
    spot: object
    spot_age_s: float
    score: ScoreResult
    # Opening TWAP captured before the cheap score. The same value feeds the
    # rescore and the ledger, so a late WebSocket frame cannot change the
    # strike after the model decided.
    twap_open: Optional[float] = None
    # Seconds since window open (None for minute-boundary sleeves).
    offset_sec: Optional[int] = None
    # Trigger provenance, copied onto the CellDecision.
    trigger_src: str = "tick"


@dataclass
class _LiveCommitContext:
    """Live routing state shared by every commit in one tick.

    Loaded once at tick start (mode, operator state, group caps, balance,
    readiness) so every cell sees the same view. ``tick_group_live_delta``
    accumulates this tick's live bets so later cells' group-cap checks see
    them.
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
    """The sleeve's entry minute if this minute qualifies, else None."""
    if sleeve.interval == "5m":
        m = current_minute % 5
    else:
        m = current_minute % 15
    return m if m in sleeve.entry_minute_set else None


def _qualifying_offset_sec(sleeve: Sleeve, now_ts: int) -> Optional[int]:
    """This tick's offset from window open if it is one of the sleeve's entry
    seconds, else None.

    The 1 Hz tick visits every second once; a tick lost to scheduler jitter
    just skips that second.
    """
    offsets = getattr(sleeve, "checkpoint_offset_set", ())
    if not offsets:
        return None
    window_start = _window_start_ts(now_ts, sleeve.interval_min)
    off = int(now_ts) - int(window_start)
    return off if off in offsets else None


def _schedule_filter_reason(sleeve: Sleeve, now_ts: int) -> Optional[str]:
    """Apply the sleeve's hour/day-of-week filters (US/Eastern, as in the
    backtest).
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
    """Start of the window containing ``now_ts``."""
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
    """True only for the first scan of this entry minute."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO entry_scan_latches "
        "(coin, sleeve, entry_minute, window_start_ts) "
        "VALUES (?, ?, ?, ?)",
        (coin, sleeve_label, entry_minute, window_start_ts),
    )
    return (cur.rowcount or 0) > 0


@dataclass
class TickDeps:
    """Everything one tick needs from the outside world, injected so tests can
    stub it.
    """

    partial_ret_engine: PartialRetEngine
    config_sha256: str
    fetch_spots: Callable[..., Optional[dict]]
    fetch_market: Callable[..., Optional[object]]
    get_disk_free_mb: Callable[[], float]
    # Cache-only spot read for the fast-entry scans: returns the cached batch
    # for this second or None, and never fetches. The 1 Hz tick uses
    # ``fetch_spots``.
    peek_spots: Optional[Callable[..., Optional[dict]]] = None
    # Per-token book GET. The fast-entry scans pass None: they only read books
    # already pushed or cached, never spending a REST call.
    fetch_book: Optional[Callable[..., Optional[BookSnapshot]]] = None
    # Batched book fetch (``POST /books``). None falls back to per-token GETs.
    fetch_books: Optional[Callable[..., Optional[dict]]] = None
    # Live routing (optional; tests and DRY runs leave these unset).
    #
    # ``order_client`` places orders; when None every LIVE wallet skips with
    # ``clob_unavailable``. ``notify`` sends operator alerts.
    order_client: Optional[object] = None
    notify: Optional[Callable[..., None]] = None
    # Loaders for the per-tick routing state; None uses the defaults.
    mode_state_loader: Optional[Callable[..., dict[str, str]]] = None
    operator_state_loader: Optional[Callable[..., OperatorState]] = None
    group_state_loader: Optional[Callable[..., dict[str, GroupCapState]]] = None
    live_balance_fetcher: Optional[Callable[[], Optional[float]]] = None
    # Readiness check; defaults to the latest ``live_readiness_reports`` row.
    live_readiness_loader: Optional[Callable[..., bool]] = None
    # The TWAP HGB model and the Chainlink TWAP mirror that supplies each
    # window's opening price.
    twap_hgb_artifact: Optional["HgbArtifact"] = None
    twap_mirror: Optional[object] = None
    # Book read-ahead chain (WebSocket mirror, then REST prefetch). Cells read
    # fresh snapshots from it before paying for the inline batch. None = inline
    # fetch only.
    book_prefetch: Optional[object] = None
    # WebSocket-vs-REST shadow comparator. It observes the REST books for
    # comparison only and never changes the books used to decide.
    book_shadow: Optional[object] = None
    # Returns the calling thread's own SQLite connection. The parallel market
    # lookups use it because sharing one connection across threads raised
    # SQLITE_MISUSE. None in tests that never touch the DB.
    conn_factory: Optional[Callable[[], sqlite3.Connection]] = None


def _bump(d: dict, key: str) -> None:
    d[key] = d.get(key, 0) + 1


def _bucket_or_none(b: Optional[int]) -> Optional[int]:
    """Map the ``-1`` "no bucket" sentinel to None before it reaches a DB row.
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

    One function shared by Phase A's cheap pre-score (NaN asks mean "no book
    yet"), the rescore in ``_resolve_net_cells`` and the fast-entry scan, so
    all three score the same inputs. ``entry_minute`` and ``fee_rate_bps`` are
    only used by the test stub.
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
    """True when ``interval_min`` is switched off for new entries
    (``config.INTERVAL_5M_ENABLED=0`` disables 5m).

    Entry-only: open positions and history on that interval are unaffected.
    """
    return not config.INTERVAL_5M_ENABLED and interval_min == 5


# Sleeve gate reasons that write no signals row. At 1 Hz they would flood the
# table with rows that carry no information.
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
    """May this sleeve open a new entry at ``now_ts``? None if yes, else the
    reason.

    Checked in this order, and the first blocker is the one recorded:

      1. ``interval_disabled``      interval switched off (silent)
      2. ``subminute_disabled``     master flag or kill switch (silent)
      3. ``not_qualifying_offset``  not one of the sleeve's entry seconds (silent)
      4. ``not_entry_minute``       minute-boundary sleeves only
      5. ``schedule_filter``        hour/day-of-week filter
      6. ``dd_halt``                hard drawdown halt

    The tick loop and the fast-entry scan both call this, so they cannot
    disagree. ``current_minute=None`` skips check 4. Without ``wallet_snap``
    and ``portfolio`` check 6 is skipped; the fast scan passes None in its
    cheap pre-check and re-runs the gate with a snapshot before committing.
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
    """Do not top up by dust once the slot is effectively full."""
    return max(float(config.MIN_BET_USD), 0.01 * max(0.0, float(target_usd)))


def _execution_max_walk_price(sleeve: Sleeve) -> Optional[float]:
    """Optional price leash for the book walk.

    An HGB sleeve with ``book_consumption_max=None`` relies on the walker's
    edge floor instead, so the leash is off.
    """
    if getattr(sleeve, "signal_source", None) == "hgb" and getattr(
        sleeve, "book_consumption_max", None
    ) is None:
        return None
    return sleeve.max_walk_price


# ---------------------------------------------------------------------------
# Fast-entry scan
# ---------------------------------------------------------------------------
#
# Most rejected orders were beaten by a faster taker by about a second. The
# order path is already at the exchange's floor, so the reclaimable latency is
# between an ask appearing and us looking at it: a 1 Hz tick waits ~500ms on
# average. Two extra triggers (a 250ms sweep job and the WebSocket ask-update
# event) call ``run_fast_entry_scan``, which reuses the tick's scoring,
# ``_resolve_net_cells`` and ``_commit_cell``, so every gate binds unchanged.

# Single-flight lock across the tick's Phase B and the fast scans, held for the
# whole commit stage (with parallel posting the orders are sent after the cell
# loop, so a per-cell lock would not cover them). The tick acquires it
# blocking; a fast scan acquires it non-blocking and gives up if contended. A
# duplicate order is already impossible (``_attempted_offsets`` plus the
# ``uniq_loa_active_slot`` index); the lock also keeps two orders for one
# market from being in flight together.
_FAST_ENTRY_LOCK = threading.Lock()


def _phase_b_single_flight():
    """Lock context for the tick's Phase B. No lock when fast entry is off."""
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
    """True if BOTH sides of this market already fired this offset.

    Cheap pre-filter: the decider rejects an offset it has already seen, so
    such a cell could never place and scoring it would be wasted work.
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
    """The ``(sleeve, offset_sec)`` pairs eligible for a fast scan at
    ``now_ts``.

    Integer math plus ``_sleeve_entry_gate``, so it cannot drift from the tick
    loop. An empty list makes the scan return immediately. The drawdown-halt
    check needs a wallet snapshot, so ``run_fast_entry_scan`` applies it later.
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
    """True if this cell's chosen side already fired this offset."""
    seen = dec._attempted_offsets.get(
        (d.coin, d.sleeve.interval, int(d.window_start_ts), d.score.side)
    )
    return seen is not None and d.offset_sec in seen


# One-entry cache of the fast scans' resolver view of ``deps``.
_FAST_RESOLVER_DEPS: "tuple[Optional[TickDeps], Optional[TickDeps]]" = (None, None)


def _fast_resolver_deps(deps: TickDeps) -> TickDeps:
    """The cache-only resolver view of ``deps``.

    Drops the REST book fetchers (fast scans never spend a REST call) and the
    shadow comparator (it needs REST books to compare against).
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
    """Re-read the operator state row, falling back to the cached value.

    This is the operator's emergency stop, so it is never served from the
    per-second cache. On a read error the cached row is kept (``None`` would
    mean "stopped"), so a transient SQLite error cannot halt live entries.
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
    """Re-evaluate this second's qualifying cells between ticks.

    Called by both fast triggers (the 250ms sweep job and the WebSocket
    ask-update worker) on their own DB connection. Returns counters for the
    caller's log line. ``candidates`` can be passed in if the caller already
    computed ``fast_entry_candidate_sleeves``.

    Stages run cheapest first, so a fire that cannot place costs almost
    nothing:

      1. ``fast_entry_candidate_sleeves``: integer math, no I/O.
      2. ``_fast_offset_spent_for_all_sides``: in-memory lookups.
      3. ``peek_spots``: read the spot cache the 1 Hz tick keeps warm; a cold
         cache abstains instead of fetching. The fast scan never feeds the
         partial-return engine; the tick owns that state.
      4. the tick's cheap pre-score.
      5. ``_resolve_net_cells`` with cached market metadata and books from the
         WebSocket/prefetch chain only, then the authoritative rescore.
      5b. ``_fast_side_offset_spent``: drop cells the decider will veto, before
          taking the lock.
      6. ``_commit_cell`` for bettable cells, under the single-flight lock.

    Unlike the tick, cells that end up not bettable are dropped without a
    signals row; the 1 Hz tick already writes one for that second. The commit
    preamble may be served from ``_FastCommitStage``. Every gate still applies
    at fire time against the fresh ask.
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
        # No pushed or cached book source: nothing to read without a REST call.
        return counters

    # Stage 2: in-memory dedup against offsets already fired this second.
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

    # Stage 3: spot from the current one-second bucket, cache only. A fetching
    # fast path once added 50-100ms inline and evicted the prefetcher's cached
    # bucket, slowing the 1 Hz tick; abstaining costs one fast fire.
    peek_spots = deps.peek_spots
    if peek_spots is None:
        # No cache-only spot reader wired: this scan cannot read spot at all.
        return counters
    spots = peek_spots(
        now_ts=float(now_ts), tick_seconds=config.SPOT_FETCH_INTERVAL_SECONDS
    )
    if not spots:
        counters["spot_cold"] = 1
        return counters

    # Stage 4: the tick's cheap score at the same integer offset.
    tick = TickResult(tick_ts=now_ts)  # local only — never persisted
    net_cells: list[_NetCell] = []
    for sleeve, offset_sec, window_start, coin in work:
        spot = spots.get(coin)
        if spot is None:
            continue
        # Fast scans never spend a network round trip, so a market whose
        # metadata is not already cached is skipped. This also keeps them from
        # adding retries to a failing metadata endpoint. By the last seconds of
        # a window the cache has been warm for minutes.
        if not _venue_market_cache_has(coin, sleeve.interval_min, window_start):
            continue
        counters["scored"] += 1
        entry_minute = offset_sec // 60
        nan = float("nan")
        # No market resolved yet, so no fee rate; stage 5 rescores before
        # anything can place.
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

    # Stage 5: the tick's resolver over a cache-only view of ``deps``.
    # ``note_tokens=False`` matters: noting tokens replaces the WebSocket
    # subscription set, which the 1 Hz tick owns (current window plus the next
    # window's pre-warm).
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
    # Stage 5b: the chosen side's offset dedup, still outside the lock.
    placing = [
        d for d in placing
        if not _fast_side_offset_spent(_v2_decider_for(d.sleeve), d)
    ]
    counters["bettable"] = len(placing)
    if not placing:
        return counters

    # Stage 6: commit, as in the tick's Phase B.
    deferred_fill_gets: list[_DeferredFillGet] = []
    if not _FAST_ENTRY_LOCK.acquire(blocking=False):
        counters["contended"] = 1
        return counters
    # The live-context and wallet-snapshot reads run inside the lock on
    # purpose: outside it, a tick commit landing in between could leave this
    # scan sizing against stale exposure and exceed a cap by one bet.
    #
    # ``_fast_commit_stage`` serves that load from the per-second cache only
    # when nothing was placed and no ledger row changed since it was taken. The
    # operator row is always re-read.
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
        # The drawdown halt is the one gate the cheap pre-check cannot evaluate
        # (it needs a wallet snapshot), so it is applied here.
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
                    # Never fan out: a fast scan has at most a couple of cells
                    # and must hold the lock briefly.
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
            # Placement log fields:
            #  trigger_to_post_ms  wall time of the commit loop
            #  pre_ms              scan start to commit-loop start (stages 1-5b, lock
            #                      wait, context assembly)
            #  ctx_ms              commit preamble alone; ``ctxhit`` = served from cache
            #  walk_ms             book walk time
            #  sign_ms             order signing time
            #  post_ms             exchange round trips (accepted and rejected)
            #  db_ms               the remainder: SQLite writes and the gate stack
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
        # Same as the tick: land the immediate fill lookups. Outside the lock,
        # since they send no orders.
        try:
            _flush_immediate_fill_gets(conn, deferred_fill_gets)
        except Exception:  # noqa: BLE001 — the 60s reconciler is the backstop
            logger.warning("fast_entry immediate-fill flush failed", exc_info=True)
    return counters


def _record_fetch_abort(conn, now_ts: int) -> None:
    """Record one spot-fetch failure per fetch bucket."""
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
    """CRITICAL health row and operator alert when the fill-rate breaker trips.
    Deduped over 10 minutes; the breaker keeps blocking placements
    regardless.
    """
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


# Drawdown-brake alerts fire only at two actionable levels; a shallow throttle
# (brake in [0.75, 1.0)) is logged at INFO.
_V2_BRAKE_WARN_THRESHOLD: float = 0.75
# At most one CRITICAL per message per 6 hours.
_V2_BRAKE_DEDUP_WINDOW = "-6 hours"


def _record_v2_brake_health(
    conn, *, brake: float, floor: float, dd: float
) -> None:
    """Surface drawdown-brake throttling before the hard halt.

    CRITICAL rows (deduped to one per 6h) fire when the brake first drops below
    0.75 and when it reaches its floor; a shallower throttle is logged at INFO.
    The messages carry no numbers so the dedup by message text holds.
    """
    if brake >= 1.0:
        return
    at_floor = brake <= floor + 1e-9
    if not at_floor and brake >= _V2_BRAKE_WARN_THRESHOLD:
        # Shallow throttle: log only.
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
    """Record a live-only skip that fires before the gate stack
    (operator_stopped, transfer_lock, readiness_stale, funder_balance_low,
    clob_unavailable).

    There is no walked price yet, so the row carries what context exists.
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
    """Load the live routing context once per tick.

    Returns ``(mode_state, operator_state, group_state, live_balance,
    readiness_fresh)``:

      * ``mode_state``: ``{wallet_id: 'LIVE'|'DRY'}``; empty if unreadable, which
        makes every wallet DRY.
      * ``operator_state``: the operator row, or None if the table is missing.
      * ``group_state``: ``{group_id: GroupCapState}`` for cap checks.
      * ``live_balance``: exchange balance in USD, or None with no order client.
      * ``readiness_fresh``: whether a READY readiness report is within its TTL;
        LIVE wallets otherwise skip with ``readiness_stale``.
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

    # Fetch the balance only when a wallet is LIVE and an order client exists,
    # so DRY-only runs make no network call.
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

    # Readiness is only consulted by the live-wallet skip gates.
    if deps.live_readiness_loader is not None:
        try:
            readiness_fresh = bool(deps.live_readiness_loader(conn))
        except Exception:  # noqa: BLE001
            readiness_fresh = False
    else:
        readiness_fresh = _default_readiness_fresh(conn)

    return mode_state, op_state, group_state, live_balance, readiness_fresh


def _default_readiness_fresh(conn) -> bool:
    """Default readiness check: latest READY row within
    LIVE_READINESS_CACHE_TTL_S.
    """
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
    """Live exposure already funded from the exchange wallet."""
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
    """Cash plus funded live exposure, the basis for runtime cap checks."""
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
    """Live-only skip reason, or None.

    Checked in order: clob_unavailable, operator_stopped, transfer_lock,
    readiness_stale, funder_balance_unknown, funder_balance_low.
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
    # A missing balance with an order client wired means the balance fetch
    # failed this tick. Fail closed: no live orders without a verified balance.
    if has_order_client and live_balance is None:
        return "funder_balance_unknown"
    live_cap_basis = _live_cap_basis_usd(
        live_balance, live_pending_exposure_usd
    )
    # ``all_group_caps_sum_usd`` is 0 when caps track equity, which disables
    # the fixed balance floor.
    if live_cap_basis is not None and all_group_caps_sum_usd > 0:
        floor = all_group_caps_sum_usd - LIVE_BALANCE_TOLERANCE_USD
        if live_cap_basis < floor:
            return "funder_balance_low"
    return None


@dataclass(frozen=True)
class _LiveCtxInputs:
    """Everything ``_build_live_commit_context`` loads, before assembly.

    Split out so the fast scans can cache the loads within a second while still
    assembling a fresh context per fire (the context carries a mutable
    ``tick_group_live_delta`` that must not leak between scans).
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
    """The load half of ``_build_live_commit_context``. ``tick.ms_live_ctx``
    times the routing-state and balance load only.
    """
    _t_ctx = time.monotonic()
    mode_state, op_state, group_state, live_balance, readiness_fresh = (
        _load_live_context(conn, portfolio, deps)
    )
    if tick is not None:
        tick.ms_live_ctx = (time.monotonic() - _t_ctx) * 1000.0
    # ``funder_balance_floor_usd`` picks the right floor for each wallet
    # topology.
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
    """The assemble half: pure arithmetic over ``loaded``.

    ``op_state`` overrides the loaded operator row, which the fast scans
    re-read on every fire so an operator stop applies immediately.
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
        # Computed once so every LIVE wallet sees the same blocker in this
        # tick.
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
    """Load the live routing context and build the shared commit context.

    Returns ``(live_ctx, wallet_snaps)``. Used by both the tick's Phase B and
    the fast scans, so the two committers build the context the same way.
    """
    loaded = _load_live_ctx_inputs(conn, portfolio, deps, tick=tick)
    return _assemble_live_commit_context(
        loaded, deps, tick_group_live_delta=tick_group_live_delta
    )


# ---------------------------------------------------------------------------
# Placement epoch: a counter that moves whenever this process changes the
# exposure a commit stage sizes against. It is bumped inside
# ``order_attempts.reserve_live_order_attempt`` (every live reservation) and by
# ``_apply_within_tick_exposure`` (every ledger write). The counter lives in
# ``order_attempts``; these are local aliases.
_bump_placement_epoch = bump_placement_epoch
# Current epoch (call it; never cache the value).
_placement_epoch = placement_epoch


@dataclass
class _FastCommitStage:
    """One second's cached commit-stage preamble for the fast scans.

    A busy second fires up to ~16 times, and each fire that reaches the commit
    stage used to repeat the routing reads, the wallet aggregation over the
    whole ledger, the breaker check and a disk-usage call (~4-5ms), usually
    only to skip.

    A cached stage is served only when all of these hold:

      1. ``epoch`` unchanged: no reservation or ledger write in this process
         since the load. Sampled after the probe query on the serve path.
      2. ``ledger_probe`` unchanged: no ledger change from anywhere (settlement,
         reconciler), via ``capital.ledger_version``.
      3. same integer second, which bounds staleness of the routing rows.
      4. same ``conn``, ``portfolio`` and ``deps`` objects.
      5. fast entry is enabled.

    The operator row is not cached; the caller re-reads it on every fire.
    """

    second: int
    refs: tuple           # (conn, portfolio, deps) — re-checked by identity
    epoch: int
    ledger_probe: tuple
    loaded: _LiveCtxInputs
    tick_breaker: BreakerState
    disk_free_mb: float


# One cached stage per connection (keyed by ``id(conn)``): the two fast
# triggers run on different connections, and a single shared slot would make
# them evict each other. Stale seconds are dropped on store, and check 4 makes
# a recycled ``id()`` a miss.
_FAST_COMMIT_STAGE: "dict[int, _FastCommitStage]" = {}
_FAST_COMMIT_STAGE_LOCK = threading.Lock()


def clear_fast_commit_stage() -> None:
    """Drop every cached commit-stage preamble (test and ops hook)."""
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
    """``(stage, cache_hit)`` for a fast scan's commit stage. Called inside the
    single-flight lock; see ``_FastCommitStage`` for when a cached stage is
    served.
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
        # Sample the epoch after the probe query, never before: a bare
        # reservation moves the epoch but not the probe, so reading the epoch
        # first could miss one landing in between and serve a stage that is one
        # bet over the cap.
        if (
            probe is not None
            and probe == cached.ledger_probe
            and cached.epoch == _placement_epoch()
        ):
            return cached, True

    # Read both invalidation signals before the load, so a write landing during
    # the load forces a reload next time instead of being counted as included.
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
            # Evict other connections' stale seconds while holding the lock.
            for key in [
                k for k, v in _FAST_COMMIT_STAGE.items() if v.second != stage.second
            ]:
                del _FAST_COMMIT_STAGE[key]
            _FAST_COMMIT_STAGE[id(conn)] = stage
    return stage, False


# Next-window market prefetch (see _prefetch_next_window_markets).
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
    """Warm the market cache for the NEXT window during the last
    ``_MARKET_PREFETCH_TAIL_S`` seconds of the current one.

    Runs at the end of the tick, off the decision path, so the first qualifying
    tick of a new window finds market metadata cached. Only successful lookups
    are cached, so an unlisted market retries next tick; at most
    ``_MARKET_PREFETCH_MAX_PER_TICK`` fetches per tick. Uses the unwrapped
    fetcher so it does not count toward decision-path latency.

    Also hands the next window's token ids to the book prefetch chain, so the
    WebSocket mirror subscribes before the boundary instead of the first entry
    second paying an inline REST fetch.
    """
    from cryptobot.venues.types import FeeUnknown as _FeeUnknown

    budget = _MARKET_PREFETCH_MAX_PER_TICK
    next_tokens: list[str] = []
    for interval_min in sorted({s.interval_min for s in portfolio.sleeves}):
        # A disabled interval can never be entered; skip it so it does not use
        # the shared per-tick fetch budget.
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
            # Fetch only on a cache miss and within budget. Cached markets
            # still contribute their token ids below: the current window is
            # re-noted every qualifying tick, so the next window's ids must be
            # re-asserted every tail tick too.
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
            # Pre-warm the order client's market-info cache for a newly
            # resolved market, so the window's first order is local signing
            # plus one POST.
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
            # Collect the token ids for the book prefetch chain.
            yes_t = getattr(market, "yes_token_id", None)
            no_t = getattr(market, "no_token_id", None)
            if yes_t:
                next_tokens.append(yes_t)
            if no_t:
                next_tokens.append(no_t)

    # ``note_tokens`` replaces the tracked set, so pass the current window's
    # tokens plus the next window's; otherwise the current window stops being
    # served for the last ~20 seconds. Best-effort.
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
    """Execute one tick. A failing cell is logged and the rest continue."""
    t0 = time.monotonic()
    tick = TickResult(tick_ts=now_ts)
    tick._t0_monotonic = t0

    # Wrap the network fetchers so every call adds to this tick's latency
    # counters. ``deps`` is rebound to a copy so the caller's object is never
    # mutated.
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
        # Wrap only the fetchers that are wired, so None still means "no
        # fetcher".
        fetch_book=(
            _timed_fetch_book if _orig_fetch_book is not None else None
        ),
        fetch_books=(
            _timed_fetch_books if _orig_fetch_books is not None else None
        ),
    )

    # ------------------------------------------------------------------
    # Phase A: fetch and score
    # ------------------------------------------------------------------
    current_minute = (now_ts // 60) % 60
    # Spot prices are fetched once per bucket and reused, keeping the public
    # endpoint under its per-IP limit while entries are still evaluated every
    # second.
    _t_spot = time.monotonic()
    spots = deps.fetch_spots(
        now_ts=float(now_ts),
        tick_seconds=config.SPOT_FETCH_INTERVAL_SECONDS,
    )
    tick.ms_spot = (time.monotonic() - _t_spot) * 1000.0
    if spots is None:
        # Spot source down: nothing can be scored. Record one health row per
        # fetch bucket rather than one per second.
        _record_fetch_abort(conn, now_ts)
        conn.commit()
        tick.duration_ms_total = (time.monotonic() - t0) * 1000.0
        return tick

    # Feed this second's spot into the partial-return engine.
    for coin, spot in spots.items():
        deps.partial_ret_engine.update(
            coin, live_spot=spot.price, now_ts=now_ts,
        )

    # Load the live routing context once per tick (the same call the fast scans
    # make). ``tick_group_live_delta`` accumulates live bets within the tick
    # for the group-cap gate.
    tick_group_live_delta: dict[str, float] = {}
    live_ctx, pre_score_wallets = _build_live_commit_context(
        conn,
        portfolio,
        deps,
        tick_group_live_delta=tick_group_live_delta,
        tick=tick,
    )

    cell_decisions: list[CellDecision] = []
    # Cells that pass the cheap score, resolved together in _resolve_net_cells.
    net_cells: list[_NetCell] = []
    for sleeve in portfolio.sleeves:
        # One shared gate function (``_sleeve_entry_gate``), so this loop and
        # the fast-entry scan always agree. The silent reasons make no API
        # calls and write no signals row.
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
        # Sub-minute sleeves qualify by offset from window open, not by minute.
        # The gate above already rejected non-qualifying seconds.
        cell_offset_sec: Optional[int] = None
        if getattr(sleeve, "checkpoint_offset_set", ()):
            cell_offset_sec = _qualifying_offset_sec(sleeve, now_ts)
            em = int(cell_offset_sec or 0) // 60
        else:
            em = _qualifying_entry_minute(sleeve, current_minute)

        window_start = _window_start_ts(now_ts, sleeve.interval_min)
        window_end = window_start + sleeve.interval_min * 60

        if gate_reason == "not_entry_minute":
            # Not an entry minute: no API calls. Write the skip row once per
            # minute (at second 0) rather than every second; the in-memory
            # counters still update every tick.
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
            # The schedule filter and the drawdown halt write one skip row per
            # coin; the halt also writes a health row.
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

        # Past the hard halt, the graduated drawdown brake may still be
        # reducing stakes; alert when it is active.
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

            # Each scan slot is claimed once: per offset for sub-minute cells,
            # per entry minute otherwise.
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

            # Cheap pre-score first, with NaN asks. During the partial-return
            # warm-up it returns SKIP:no_feature, saving market and book calls
            # on cells that cannot bet. Nothing bets off this score;
            # ``_resolve_net_cells`` rescores every surviving cell with live
            # asks.
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
                # Defer the market and book fetch: buffered cells get their
                # books in one ``POST /books`` call in _resolve_net_cells.
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

            # Cheap-path skip: no market context, so build the decision inline.
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

    # Resolve the buffered cells (cached market metadata, one batched book
    # fetch, rescore). Their order is preserved; the inline decisions above are
    # all skips.
    if net_cells:
        cell_decisions.extend(
            _resolve_net_cells(
                net_cells, conn=conn, deps=deps, portfolio=portfolio,
                now_ts=now_ts, tick=tick,
            )
        )

    tick.cell_decisions = list(cell_decisions)

    # ------------------------------------------------------------------
    # Phase B: cross-coin agreement gate, then commit each cell
    # ------------------------------------------------------------------
    same_side_counts: dict[tuple, int] = {}
    for d in cell_decisions:
        if d.score.skip_reason is not None:
            continue
        key = (d.sleeve.label, d.window_start_ts, d.entry_minute, d.score.side)
        same_side_counts[key] = same_side_counts.get(key, 0) + 1

    disk_free_mb = deps.get_disk_free_mb()
    # Reuse Phase A's wallet snapshots: exposure added during this tick is
    # tracked in ``tick_pending_delta`` / ``tick_group_live_delta``, so a
    # second full ledger scan would add nothing.
    wallet_snaps = pre_score_wallets
    # This tick's running pending exposure per wallet, so the exposure gate on
    # cells 2..N sees the bets placed earlier in the same tick.
    tick_pending_delta: dict[str, float] = {}

    # Commit placing cells before skip cells, so no skip-row INSERT runs before
    # the tick's first order. Safe because ``same_side_counts`` is computed
    # over the full set and a skip cell writes only its own signals row.
    # ``tick.cell_decisions`` keeps the original order.
    placing, skips = [], []
    for d in cell_decisions:
        (placing if d.score.skip_reason is None else skips).append(d)
    phase_b_cells = placing + skips

    # Evaluate the fill-rate breaker once per tick. Its inputs change only on
    # an order or a settlement, so per-cell re-reads inside the transaction
    # were wasted queries.
    tick_breaker: Optional[BreakerState] = None
    if any(
        d.offset_sec is not None and d.score.skip_reason is None
        for d in cell_decisions
    ):
        tick_breaker = evaluate_breaker(conn)

    # Immediate fill lookups queued by _commit_cell, run together after the
    # cell loop (see the flush below).
    deferred_fill_gets: list[_DeferredFillGet] = []

    # Parallel order posting (``CRYPTOBOT_PHASE_B_PARALLEL_POST``). With a
    # collector, a LIVE cell reserves its order and defers the POST;
    # ``_flush_parallel_posts`` then sends them concurrently and finishes each
    # cell serially in order. Only used with more than one placing cell: with
    # one order there is nothing to parallelize, and deferring it would only
    # delay it.
    parallel_posts: Optional[list[_PreparedPost]] = (
        [] if (config.PHASE_B_PARALLEL_POST and len(placing) > 1) else None
    )

    # Hold the single-flight lock across the whole commit stage, including the
    # parallel-post flush, so a fast scan's commit never interleaves with this
    # tick's. No lock when fast entry is off.
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

        # Send the collected LIVE orders concurrently, then finish each
        # serially in cell order. Every reserved cell is posted or released, so
        # no SUBMITTING row is left behind.
        if parallel_posts:
            _flush_parallel_posts(conn, parallel_posts)

    # Run the deferred immediate fill lookups now that every order is sent, so
    # none of them delays a later cell's order. Best-effort: the 60s reconciler
    # remains the authority.
    if deferred_fill_gets:
        try:
            _flush_immediate_fill_gets(conn, deferred_fill_gets)
        except Exception:  # noqa: BLE001 — reconciler is the backstop
            logger.warning("deferred immediate-fill flush failed", exc_info=True)

    _tail_t0 = time.monotonic()

    # Off the decision path: warm the next window's market metadata.
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
    """Immediate ``(size, price, ts)`` fill for a just-posted sub-minute LIVE
    order, or None.

    A FAK response usually carries no fill details, and the 60s reconciler
    would only fill them in after the window has closed. One lookup right after
    the order (off the decision path, errors swallowed) records the fill on the
    ledger row within the same tick. Only for sub-minute entries whose response
    had no fill, and only when the exchange reports both size and price (the
    two are never stored separately). The reconciler later overwrites with the
    exchange's final numbers.
    """
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
    """One queued post-order fill lookup.

    The accept branch records the lookup's inputs and the committed ledger id
    here instead of calling the exchange inline (~50-260ms) between orders;
    ``run_scanner_tick`` runs the batch after the cell loop.
    """

    order_client: object
    ledger_id: int
    order_id: str
    offset_sec: int
    current_size: Optional[float]
    intent_amount_usd: float
    intent_price: Optional[float]


# Thread pool for the post-loop flushes (fill lookups and parallel orders).
# Kept separate from ``walker._clob_executor``: the flushed tasks submit work
# to that executor themselves, and sharing its workers could deadlock. Eight
# workers is plenty since the two flushes run one after the other.
_PHASEB_FLUSH_POOL = ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="phaseb-flush"
)


def _parallel_collect(
    calls: list[Callable[[], Any]], *, label: str
) -> list[Optional[Any]]:
    """Run ``calls`` and return their results in order, with any exception
    mapped to None (and logged under ``label``). One call runs inline; more
    fan out on _PHASEB_FLUSH_POOL.
    """
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
    """Run the queued fill lookups concurrently and write the fills back.

    Each result updates the already-committed ledger row: size and price
    together, and ``live_fill_ts`` only when the exchange reported one. One
    commit for the batch. Failures are skipped; the reconciler corrects later.
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
    """A LIVE cell whose order POST is deferred for parallel posting.

    ``_commit_cell`` runs the pre-order stages serially and appends this
    record; ``_flush_parallel_posts`` runs every ``do_post`` concurrently and
    then each ``finish`` serially in the original order, so all SQLite writes
    and counter updates stay single-threaded.

      * ``do_post()`` -> ``(order_result, order_ms)``: network only.
      * ``finish(order_result, order_ms)``: the post-order processing.
      * ``attempt_id``: lets a POST that raises release its reservation.
    """

    do_post: Callable[[], tuple]
    finish: Callable[[object, float], None]
    attempt_id: int


def _flush_parallel_posts(conn, prepared: list[_PreparedPost]) -> None:
    """Send the deferred LIVE orders concurrently, then finish each serially.

    A POST that raises (rather than returning a failed result) releases its
    reservation with mark_attempt_failed, so no SUBMITTING row survives the
    tick.
    """
    if not prepared:
        return

    results = _parallel_collect(
        [p.do_post for p in prepared], label="phase-b parallel POST"
    )

    for p, res in zip(prepared, results):
        if res is None:
            # The POST raised before returning a result: release the
            # reservation.
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
    """Write one stage='latency' pipeline_health row per network-active tick.

    Most ticks make no API calls and are skipped, so the table does not grow at
    1 Hz. The message is compact JSON for the dashboard's latency page.
    Failures are swallowed.
    """
    # Cache-served books involve no fetch, so count WebSocket and prefetch hits
    # as activity too; otherwise their hit rate would never be recorded.
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
                # ``post`` is accepted orders only; ``post_rej`` is rejected
                # ones.
                "post_rej": round(tick.ms_order_post_rej, 1),
                "n_rej": tick.n_orders_rej,
                "pre_post": round(tick.ms_pre_post, 1),
                "tail": round(tick.ms_tail, 1),
                # Book source counts and WebSocket book age.
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
    """Resolve buffered cells in three passes.

      Pass 1  market metadata per cell (cached per window), plus a
              fire-and-forget warm of the order client's market-info cache.
      Pass 2  books: one ``POST /books`` call for every token, falling back
              to per-token GETs if the batch fetcher is missing or fails. A
              token missing from a successful batch has no asks. The fast scans
              pass neither fetcher and read only cached books.
      Pass 3  rescore with live asks and build each CellDecision (also when
              the market is missing, with NaN asks).

    ``note_tokens=False`` (fast scans) leaves the WebSocket subscription set
    alone; the 1 Hz tick owns it.
    """
    from cryptobot.venues.types import FeeUnknown

    # Pass 1: market metadata.
    #
    # Cached markets resolve inline. Uncached ones each cost a Gamma request
    # (~30-80ms), so with two or more they run in parallel on the shared
    # executor. Each worker uses its own SQLite connection from
    # ``deps.conn_factory``: the lookup reads and writes a fee cache, and
    # sharing the tick's connection across threads raised SQLITE_MISUSE.
    conn_factory = deps.conn_factory

    def _fetch_with(cell: "_NetCell", fetch_conn) -> object:
        return deps.fetch_market(
            cell.coin, interval_min=cell.sleeve.interval_min,
            window_start_ts=cell.window_start, conn=fetch_conn,
            now_ts=float(now_ts),
        )

    def _fetch_inline(cell: "_NetCell") -> object:
        # Main thread: the tick's connection is safe here.
        return _fetch_with(cell, conn)

    def _fetch_threaded(cell: "_NetCell") -> object:
        # Worker thread: use this thread's own connection. conn_factory is None
        # only in tests that stub fetch_market and never touch the DB.
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
                    # Log the exception class so a programming error is
                    # distinguishable from a network failure.
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
        # Zero or one uncached cell: resolve inline.
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
            # Pre-warm the order client's market-info cache (fire-and-forget)
            # so a later order is local signing plus one POST.
            _warm = getattr(deps.order_client, "warm_market_info_async", None)
            if _warm is not None:
                try:
                    _warm(market.condition_id)
                except Exception:  # noqa: BLE001 — warm is best-effort
                    logger.debug("warm_market_info_async raised", exc_info=True)
            # The synchronous warm fallback runs after the book fetch (below),
            # so the async warm above has time to finish first.
            warm_cids.append(market.condition_id)

    # Pass 2: books.
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
        # Serve fresh prefetched snapshots first; the rest go to the inline
        # batch. note_tokens() tells the prefetcher what to fetch before the
        # next second.
        try:
            if note_tokens:
                deps.book_prefetch.note_tokens(tokens)
            # Attribute each cache hit to its source (WebSocket or prefetch).
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
            # A successful batch response can omit tokens. Fetch exactly the
            # missing ones individually rather than treating them as empty
            # books.
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

    # Synchronous warm fallback, after the book fetch so the async warms had
    # time to finish. ``ensure_market_info_warm`` waits on an in-flight warm
    # when there is one and blocks at most once per market.
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

    # Shadow comparison: hand each REST book to the WebSocket comparator. Off
    # the decision path; ``books`` is never changed here.
    if deps.book_shadow is not None and all_tokens:
        try:
            deps.book_shadow.note_tokens(all_tokens)
            for t, snap in books.items():
                deps.book_shadow.observe(t, snap, now_ts=float(now_ts))
        except Exception:  # noqa: BLE001 — shadow compare is observability only
            logger.debug("book shadow compare failed", exc_info=True)

    # Pass 3: rescore and build decisions.
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
            # The authoritative rescore, the one a bet is built from.
            fee_rate_bps=fee_rate_bps,
            twap_open=c.twap_open,
        )

        # Every decision carries the opening TWAP captured before scoring.
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
    """Commit one cell: dedup, sizing, gates, book walk, order, ledger row.

    ``tick_pending_delta`` tracks exposure added earlier in this tick per
    wallet, so the exposure gate applies each wallet cap across all of the
    tick's cells.

    ``tick_breaker`` is the breaker state evaluated once per tick (derived here
    if None). ``deferred_fill_gets`` collects the accept branch's fill lookup
    for the batched flush (run inline if None).

    ``parallel_posts`` is the parallel-posting collector. With None, a LIVE
    cell reserves, posts and processes the result inline. With a collector, the
    pre-order stages run here, the stake is added to this tick's exposure up
    front (conservatively, before the outcome is known), a ``_PreparedPost`` is
    appended and the function returns; DRY and skip cells are unaffected.

    LIVE wallets reserve a durable ``live_order_attempts`` row, place the order
    through the order client and then write the ledger row with the exchange's
    details. DRY wallets record a paper fill at the walked price.
    """

    sleeve = d.sleeve
    wallet_id = wallet_id_for_sleeve(sleeve.label)
    wallet_snap = wallet_snaps[wallet_id]
    wallet_mode = wallet_snap.mode
    wallet_group = wallet_snap.wallet_group

    # Live-only skip (operator stop, readiness, balance). DRY wallets continue.
    # The reason is computed once per tick.
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

    # Market exists but its fee rate is unknown: skip (fail closed).
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

    # Score failed: write a skip row, no walk.
    if d.score.skip_reason is not None:
        _mark_skip(d, d.score.skip_reason)
        # ``entry_scan_latched`` repeats every second of a claimed minute and
        # the row carries no information, so only the counter is bumped.
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

    # Cross-coin agreement: require at least ``min_agreement`` OTHER coins on
    # the same side in the same window.
    side = d.score.side
    key = (sleeve.label, d.window_start_ts, d.entry_minute, side)
    same_side = same_side_counts.get(key, 0)
    # (same_side - 1) is the number of other coins agreeing.
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

    # No opening price yet (e.g. booted mid-window): skip rather than bet on a
    # window we cannot grade.
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

    # Serialize writes with BEGIN IMMEDIATE. On the LIVE path this transaction
    # is committed by reserve_live_order_attempt() (no network call may run
    # inside a SQLite transaction); live slot dedup is then the
    # ``uniq_loa_active_slot`` index, and the ledger write runs in its own
    # transaction below. The DRY path stays inside this one.
    conn.execute("BEGIN IMMEDIATE")
    try:
        # Sub-minute cells with one-entry-per-window get their slot state from
        # the window-level check below, so the minute-keyed probe is skipped.
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
        # Sub-minute safety stack: kill switch, window dedup, fill-rate
        # breaker. Every check fails closed (skip, not bet).
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

            # Kill switch and master flag, re-checked for a decision made
            # before they were flipped.
            if config.SUBMINUTE_KILL or not config.SUBMINUTE_ENABLED:
                _sub_skip("subminute_disabled")
                return

            # Interval disable, re-checked like the kill switch. Writes a row
            # because reaching this point means a decision outlived its gate.
            if _interval_entry_disabled(sleeve.interval_min):
                _sub_skip("interval_disabled")
                return

            # Stake = equity x base fraction x drawdown brake x 1/sqrt(k),
            # floored up to $1 (``sizing_v2.sized_usd``). ``k`` is the number
            # of same-side candidates this tick, counting this one.
            #
            # Equity is the cached exchange balance when fresh, else the
            # wallet's ledger capital, so placement never waits on a balance
            # fetch. The brake's drawdown input stays the ledger-equity
            # drawdown that also drives the hard halt, since there is no
            # persisted peak series for the live balance.
            sizing_equity = _v2_sizing_equity(
                live_ctx, wallet_snap.realized_capital
            )
            # Sizing only changes how much is bet, never whether a bet is
            # placed.
            target_usd = sizing_v2.sized_usd(
                sizing_equity,
                sleeve.interval,
                wallet_snap.drawdown_pct_peak_frac,
                k=same_side,
            )

            # Window dedup. With one entry per window (the default), the first
            # side to open the window owns it: later seconds on the same side
            # see the slot as filled, and the opposite side is rejected as
            # ``market_dedup``. Without it, each offset is its own one-shot
            # slot.
            if config.SUBMINUTE_ONE_PER_WINDOW:
                # Per-window state machine, applied before the gates. In
                # consume-on-fill mode (``CRYPTOBOT_V2_REJECT_RETRY=1``, the
                # default) a rejected order does not close the window: every
                # qualifying second is a fresh attempt with a fresh book and
                # score, and only a fill or the window's end closes it. With
                # the flag off, the first in-band second commits the window
                # whether or not it passes the gate. A fill on either side
                # closes the window for both. The ledger checks below are the
                # restart-proof guard.
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
                # One bet per market across sleeves: the in-memory set covers
                # this process; the ledger query covers restarts.
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

            # Fill-rate breaker (fails closed: an internal error counts as
            # tripped). Uses the once-per-tick state when the caller supplied
            # it.
            breaker = (
                tick_breaker if tick_breaker is not None
                else evaluate_breaker(conn)
            )
            if breaker.tripped:
                _record_subminute_breaker_trip(conn, live_ctx, breaker)
                _sub_skip("subminute_breaker_tripped")
                return

            # Per-coin cap, applied last so it bounds the amount actually sent.
            target_usd = _v2_apply_coin_cap(sleeve, d.coin, target_usd)

        is_topup_attempt = slot_filled > 0.0
        # The entry-minute guard below does not apply to sub-minute cells:
        # several offsets share one minute, and their dedup already ran above.
        if (
            d.offset_sec is None
            and not is_topup_attempt
            and _initial_entry_bet_seen(conn, d)
        ):
            # Once a real initial bet exists in this minute, later seconds may
            # only top up that slot.
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

        # Walker setup. A top-up anchors its price cap on the slot's first
        # fill; otherwise the cap anchors on this tick's top ask.
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

        # Walk the full target on an initial fill (so the gates see the
        # intended exposure) and only the remainder on a top-up.
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

        # Market-level dedup for ``first_qualifying`` sleeves: one initial bet
        # per (coin, sleeve, side, window) even across entry minutes, matching
        # the backtest. ``every_qualifying`` sleeves bet every qualifying
        # minute. Top-ups are not affected.
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

        # Skip top-up attempts when top-ups are disabled.
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
            min_edge=_execution_min_edge(sleeve),
            min_bet_usd=config.MIN_BET_USD,
            max_walk_price=max_walk_price_cell,
            walk_anchor_price=anchor_for_walker,
        )
        # Time the book walk for the fast-entry placement log (``ms_walk``).
        tick.ms_walk += (time.monotonic() - _t_walk) * 1000.0

        # Warn when the walker returns None on a non-empty book whose top level
        # cleared the edge floor (thin depth, price cap, malformed levels). A
        # top level that cannot clear the floor is just an unbettable price,
        # which happens on most near-settle ticks, so that case is not warned
        # on.
        _warn_top = ask_book.top_ask
        _top_clears_floor = False
        if _warn_top is not None and math.isfinite(_warn_top):
            # Only decides whether to log the warning.
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
        # The most expensive level consumed: the limit price for the live
        # order.
        walked_limit = walked[3] if walked is not None else None

        # Gates use the top ask, not the walked VWAP (as in the backtest).
        top_ask = ask_book.top_ask if ask_book.top_ask is not None else float("nan")
        # Total ask-side notional on this book, for the book-consumption gate.
        # Zero depth fails closed.
        book_total_ask_depth_usd = float(getattr(ask_book, "total_ask_depth_usd", 0.0) or 0.0)
        # Group-cap inputs (live only; DRY rows pass this gate).
        group_pending_baseline = wallet_snap.group_pending_usd or 0.0
        group_cap_usd = wallet_snap.group_cap_usd
        group_pending_in_tick = (
            live_ctx.tick_group_live_delta.get(wallet_snap.wallet_group or "", 0.0)
            if live_ctx is not None and wallet_snap.wallet_group is not None
            else 0.0
        )
        # For a shared-equity group, the exposure gate counts this tick's bets
        # by every member against the shared cap.
        # ``wallet_snap.pending_exposure`` already combines the members'
        # pending exposure, so only this tick's deltas are added. Other wallets
        # use their own delta.
        shared_group = None
        group_tracks_equity = False
        if wallet_snap.wallet_group is not None:
            grp = portfolio.group_by_id(wallet_snap.wallet_group)
            if grp is not None:
                # Cap tracks live equity (see gates.py).
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
            # Include bets placed earlier this tick, so later cells respect the
            # cap.
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

        # fill_price is the top ask; walked_vwap goes in its own column.
        token_id = (d.poly_token_id_yes if side == "YES" else d.poly_token_id_no) or ""
        entry_top_for_row = slot_anchor if (is_topup_attempt and slot_anchor is not None) else top_ask

        # FAK pad (``config.V2_FAK_PAD``): for a LIVE sub-minute buy, raise the
        # limit by PAD to cross a book that ticked up between scoring and
        # signing (capped at 0.99). The gates already ran on the observed ask,
        # so the pad never affects selection. The padded value is what is sent
        # and recorded.
        limit_price_out = walked_limit
        if (
            config.V2_FAK_PAD > 0.0
            and wallet_mode == "LIVE"
            and d.offset_sec is not None
            and walked_limit is not None
        ):
            # With ``V2_RETRY_REPRICE`` a retry prices off its own fresh walk
            # instead of the first attempt's limit; the first attempt is the
            # same either way.
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
        # For a top-up, look up the first fill's edge for
        # ``add_on_realized_edge_minus_initial_edge``.
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
        # With parallel posting the stake is added to this tick's exposure at
        # reserve time, so the finalize step must not add it again.
        _exposure_pre_applied = False
        def _apply_within_tick_exposure() -> None:
            """Add this cell's stake to the tick's running exposure so later
            cells see it.

            Called on a serial accept (from _finalize_bet_row) or, with
            parallel posting, at reserve time before the outcome is known.
            """
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
            # Exposure moved, so cached commit-stage views are stale.
            _bump_placement_epoch()

        def _finalize_bet_row() -> None:
            """Shared post-order tail for a DRY paper fill or a LIVE accept.

            Writes the ledger row (and, for LIVE, marks the attempt submitted
            and links it), closes the window on fill, records the would-bet
            signal, updates counters and adds the stake to this tick's
            exposure. Called inline on the serial path and from
            _flush_parallel_posts on the parallel path.
            """
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
                # reserve_live_order_attempt() already committed (the order
                # must be sent outside any SQLite transaction), so the
                # transaction opened above no longer covers this. Write the
                # SUBMITTED transition, the ledger row and the attempt link in
                # their own transaction so they commit together; on failure the
                # attempt stays SUBMITTING and reconcile_stale_attempts
                # rebuilds it.
                conn.execute("BEGIN IMMEDIATE")
                try:
                    mark_attempt_submitted(
                        conn, live_attempt_id, live_order_id or "", commit=False
                    )
                    row_id = record_bet(conn, signal, **_record_bet_kwargs)
                    # Link the attempt to the new ledger row, which frees the
                    # active-slot index and removes the attempt from pending
                    # group-cap exposure. Commits.
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
                # Queue the immediate fill lookup now that the row exists.
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
                # DRY path: record_bet runs inside the outer transaction.
                row_id = record_bet(conn, signal, **_record_bet_kwargs)
            # A bet was recorded: close this window for both sides.
            if d.offset_sec is not None and config.SUBMINUTE_ONE_PER_WINDOW:
                _v2_decider_for(sleeve).note_fill(
                    _v2_decider_cell(
                        d.coin, sleeve.interval, d.window_start_ts, side
                    )
                )
                # Mark the market as filled for every other sleeve too.
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
            # Skip if parallel posting already added this stake at reserve
            # time.
            if not _exposure_pre_applied:
                _apply_within_tick_exposure()
            conn.commit()

        def _process_live_post(order_result, order_ms: float) -> None:
            """Process a LIVE order result: latency counters, the fill-rate
            record, reject or accept, then the shared finalize on accept.

            Always runs serially (inline, or from _flush_parallel_posts), so
            SQLite writes and counters stay single-threaded. ``order_ms`` is
            the round trip measured in ``_do_post``.
            """
            nonlocal live_order_id, live_client_order_id, live_attempt_id
            nonlocal live_filled_size, live_filled_price, live_fill_ts
            nonlocal _imm_intent_price
            # Order timing, including the signing/posting split the client
            # measured.
            tick.ms_order_total += order_ms
            tick.n_orders += 1
            if getattr(order_result, "sign_ms", None) is not None:
                tick.ms_order_sign += float(order_result.sign_ms)
            # Accepted and rejected round trips are summed separately.
            _post_ms = getattr(order_result, "post_ms", None)
            if _post_ms is not None:
                if order_result.success:
                    tick.ms_order_post += float(_post_ms)
                else:
                    tick.ms_order_post_rej += float(_post_ms)
                    tick.n_orders_rej += 1
            elif not order_result.success:
                tick.n_orders_rej += 1
            # Record every LIVE sub-minute order (accepted or rejected) for the
            # fill-rate breaker. Best-effort.
            if d.offset_sec is not None:
                # Attempt number within the (market, side) window: with
                # consume-on-fill each qualifying second is a new attempt.
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
                        # Trigger provenance from the cell (see
                        # CellDecision.trigger_src).
                        trigger_src=d.trigger_src,
                    )
                except Exception:  # noqa: BLE001 — observability only
                    logger.warning(
                        "subminute fill record failed", exc_info=True
                    )
            if not order_result.success:
                # Order failed: mark the attempt FAILED, which releases the
                # slot and its group-cap reservation so a later second can
                # retry.
                mark_attempt_failed(
                    conn,
                    attempt.id,
                    order_result.error_kind or "unknown",
                    order_result.error,
                )
                # Alert the operator if the exchange is failing orders in a
                # burst.
                _maybe_alert_venue_degradation(
                    conn,
                    live_ctx,
                    error_kind=order_result.error_kind,
                    coin=d.coin,
                    sleeve=sleeve.label,
                    wallet_id=signal.wallet_id,
                )

                # A rejected order does not close the window; the next
                # qualifying second is evaluated normally. Record the reject.
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
                # Reject telemetry: store the order intent and queue a book
                # re-read about a second later. Observation only, best-effort.
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

            # The SUBMITTING -> SUBMITTED transition rides the ledger
            # transaction below (one commit instead of two). A crash in between
            # leaves the attempt SUBMITTING, which reconcile_stale_attempts
            # recovers from the stored signal.
            live_order_id = order_result.order_id
            live_client_order_id = attempt.client_order_id
            live_attempt_id = attempt.id
            live_filled_size = order_result.filled_size_tokens
            live_filled_price = order_result.filled_price_vwap
            live_fill_ts = order_result.fill_ts

            # A FAK response usually has no fill details. With a
            # ``deferred_fill_gets`` queue, the immediate fill lookup is queued
            # after the ledger row commits; without one (direct callers) it
            # runs inline here.
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

            # Accept: write the ledger row via the shared tail.
            _finalize_bet_row()

        if wallet_mode == "LIVE" and live_ctx is not None and live_ctx.order_client is not None:
            # Reserve the attempt outside the transaction. The
            # ``uniq_loa_active_slot`` index serializes racing reservations, so
            # an IntegrityError means another order is in flight for this slot:
            # ``slot_full``. The reservation also bumps the placement epoch.
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

            # Make sure the order client's market-info cache is warm before
            # signing, so signing never waits on network lookups. Usually
            # already done in Pass 1.
            _ensure_warm = getattr(
                live_ctx.order_client, "ensure_market_info_warm", None
            )
            if _ensure_warm is not None and signal.poly_market_id:
                try:
                    _ensure_warm(signal.poly_market_id)
                except Exception:  # noqa: BLE001 — warm is best-effort
                    logger.debug("ensure_market_info_warm raised", exc_info=True)

            def _do_post():
                # The only step that runs on a worker thread under parallel
                # posting: network only, no SQLite and no counters. The order
                # is sent after the reservation committed, so a crash leaves a
                # SUBMITTING row for the reconciler.
                _t_order = time.monotonic()
                # Decision latency: tick start to the first order leaving the
                # box. First writer wins, which under parallel posting is the
                # earliest order.
                if tick.ms_pre_post <= 0.0 and tick._t0_monotonic:
                    tick.ms_pre_post = (_t_order - tick._t0_monotonic) * 1000.0
                _result = live_ctx.order_client.place_gtc_limit_order(
                    signal, client_order_id=attempt.client_order_id,
                )
                return _result, (time.monotonic() - _t_order) * 1000.0

            if parallel_posts is not None:
                # Parallel posting: defer the POST so all reserved orders can
                # be sent at once. Add this stake to the tick's exposure now so
                # the remaining cells' gates count it, conservatively, even if
                # the order is later rejected.
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

            # Serial path: send inline, then process.
            _order_result, _order_ms = _do_post()
            _process_live_post(_order_result, _order_ms)
            return

        # DRY path (or LIVE with no order client): no order, just the shared
        # finalize inside the outer transaction.
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
    """Send an operator notification; failures are swallowed."""
    if live_ctx.notify is None:
        return
    try:
        live_ctx.notify(event_type, message, wallet_id=wallet_id, critical=critical)
    except Exception:  # noqa: BLE001
        logger.warning("pipeline: notify dispatch failed", exc_info=True)


# Exchange-degradation alert. ``venue_unavailable`` (HTTP 425) and
# ``post_only`` (503) come in multi-minute bursts that silently lose entries,
# so a burst pages the operator.
_VENUE_ERROR_KINDS: frozenset[str] = frozenset({"venue_unavailable", "post_only"})
_VENUE_ALERT_WINDOW_MIN: int = 10
_VENUE_ALERT_THRESHOLD: int = 3
_VENUE_ALERT_COOLDOWN_S: float = 600.0
# Per-process cooldown so a long outage sends about one alert per cooldown.
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
    """CRITICAL alert when live orders keep failing with transient exchange
    errors. The threshold counts recent ``live_order_attempts`` rows, so it
    survives restarts; the cooldown dedups the alert. Never raises.
    """
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
