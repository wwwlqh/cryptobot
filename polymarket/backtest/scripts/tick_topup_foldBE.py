"""F2 bankroll replay with a taker scale-in (top-up) sleeve.

This driver reuses the canonical union + PURE_STOP_04 replay
(:mod:`polymarket.backtest.scripts.tick_replay_base`) unchanged for entry
selection, sizing, available-cash locking, DD brake / 40% halt, and the
PURE_STOP_04 exit. On top of a realized entry fill it adds *frozen-at-entry*
taker top-ups on the SAME side while the market has "not dipped", per the
pre-registered scale-in config (see SCALEIN_GRID_DIGEST.md — digest removed in
a6df8ed; recover via ``git show
a6df8ed^:polymarket/backtest/studies/tick_latency/SCALEIN_GRID_DIGEST.md``). It
does NOT modify the canonical driver or the tick primitives -- it imports both.

Top-up mechanics (documented approximations, decided in TOPUP_F2_WFO_TASK.md):

* Schedule the whole top-up ladder at entry-processing time. Integer seconds
  ``t = ceil(entry_fill_ts) + 1, + cadence, ...`` while ``t < settlement_ts``.
* No-dip: tick-exact running min of the side best ask over ticks in
  ``[entry_fill_ts, t]`` (non-crossed) must stay ``>= row.ask - no_dip``. Once it
  fails it never re-arms (break).
* Gate each second on decision-tick best ask ``<= cap`` and spread ``<= spread_max``.
* Stake frozen at entry: ``max(MIN_BET_USD, floor_cents(frac * entry_requested))``.
* Fill: walk the decision-tick ask levels for the stake to get the walked limit,
  then FAK at ``min(walked_limit + FAK_PAD, 0.99)`` against the +latency book.
  NO anchored pad -- the entry anchor would auto-reject deliberate later buys.
* Stop the ladder when max_n filled, exposure_cap reached, or no-dip fails.
* Cash: surviving top-up cash is locked immediately at entry (early lock).
* Stop interaction is two-pass: run exit004 with entry shares only to get the
  trigger time, drop top-ups that fill at/after the stop's decision second, then
  rerun exit004 at TOTAL surviving size so the full-size FOK is honest.

Fees: every taker BUY leg (entry + top-ups) is charged the calibrated
per-SHARE entry fee (``te.entry_fee_usd``); the exit leg is free, mirroring live
settlement. ⚠️ Outputs produced BEFORE this fix (2026-08-01) -- including the
archived ``NOTHROTTLE_1pct_*_20260801`` run set -- book the RETIRED per-dollar
fee (``cash * fee(vwap)``) plus an invented exit-leg fee, so their PnL is not
comparable with post-fix runs; re-run before quoting them.

Run::

    .venv\\Scripts\\python.exe -m polymarket.backtest.scripts.tick_topup_foldBE \\
        --latency 400 --folds B C D E --start-capital 100 \\
        --tier-a-mult 1.5 --tier-a-edge 0.0465 --nl15-mult 0.0 \\
        --stop-chase 0.1 --stop-attempts 3 --brake-scale 0.3 --brake-floor 0.25 \\
        --dd-halt 0.4 --topup-max-n 6 --topup-frac 0.5 --topup-no-dip 0.0 \\
        --topup-cap 0.83 --topup-spread-max 0.02 --topup-exposure-cap 3.0 \\
        --tag TOPUP_T1
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from functools import lru_cache
import json
import math
from pathlib import Path
import time

import numpy as np
import pandas as pd

from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.lib.tick_store import TickStore, open_store
from polymarket.backtest.scripts.tick_replay_base import (
    HEAVY_DIR,
    OPTION3_DIR,
    RUN_DIR,
    ReplayState,
    VariantConfig,
    _floor_cents,
    _held_scores,
    _minimum_edge,
    _state_report,
    load_wfo_inputs,
)

DETAIL_NAME = "foldBE_stop_{latency}{tag}.parquet"
REPORT_NAME = "foldBE_stop_report_{latency}{tag}.json"


@dataclass(frozen=True)
class TopupConfig:
    """Frozen scale-in knobs. ``max_n == 0`` disables top-ups entirely."""

    max_n: int = 0
    frac: float = 0.5
    no_dip: float = 0.0
    cap: float = 0.83
    spread_max: float | None = 0.02
    cadence_s: int = 1
    exposure_cap: float = 3.0

    @property
    def enabled(self) -> bool:
        return self.max_n > 0

    def as_dict(self) -> dict:
        return {
            "max_n": self.max_n,
            "frac": self.frac,
            "no_dip": self.no_dip,
            "cap": self.cap,
            "spread_max": self.spread_max,
            "cadence_s": self.cadence_s,
            "exposure_cap": self.exposure_cap,
        }


@dataclass(frozen=True)
class TopupLeg:
    cash: float
    shares: float
    vwap: float
    fill_ts: float


INTERVAL_SECONDS = {"5m": 300, "15m": 900}


def apply_late_skip(attempts: pd.DataFrame, late_skip: int) -> pd.DataFrame:
    """Drop attempt seconds inside the last ``late_skip`` seconds of a window.

    Mirrors the LIVE gate ``pipeline._late_window_skip`` (env
    ``CRYPTOBOT_LATE_WINDOW_SKIP``): an attempt is dropped when
    ``interval_sec - offset_sec <= late_skip``, i.e. the same inclusive
    boundary live uses. All-coin here — live is pinned to the same cutoff on
    every coin, so no per-coin map is needed; a per-coin split would be the
    only reason to widen this signature.

    ``late_skip <= 0`` is OFF and returns the frame untouched, so historical
    reruns of this driver reproduce bit-for-bit.
    """
    if late_skip <= 0 or attempts.empty:
        return attempts
    interval_sec = (
        attempts["interval"].astype(str).map(INTERVAL_SECONDS).to_numpy(dtype=float)
    )
    to_close = interval_sec - attempts["offset_sec"].to_numpy(dtype=float)
    # Unknown interval (NaN) fails OPEN -- never silently drop an attempt the
    # gate does not understand.
    keep = ~(to_close <= float(late_skip))
    return attempts[keep].copy()


def _walk_limit(
    levels: tuple[tuple[float, float], ...], amount_usd: float
) -> float | None:
    """Highest ask price consumed to fill ``amount_usd`` at the decision book."""
    if amount_usd <= 0 or not math.isfinite(amount_usd):
        return None
    remaining = amount_usd
    limit = 0.0
    for price, size in levels:
        if remaining <= 1e-12:
            break
        take_shares = min(size, remaining / price)
        limit = price
        remaining -= take_shares * price
    return limit if limit > 0 else None


def _simulate_topups(
    tick: te.TickBook,
    *,
    side: str,
    ask0: float,
    entry_fill_ts: float,
    entry_requested: float,
    entry_fill_cash: float,
    settlement_ts: int,
    latency_seconds: float,
    topup: TopupConfig,
) -> tuple[list[TopupLeg], int]:
    """Frozen-at-entry taker top-up ladder. Returns (filled legs, reject count)."""
    if not topup.enabled:
        return [], 0

    topup_stake = max(te.MIN_BET_USD, _floor_cents(topup.frac * entry_requested))
    exposure_budget = topup.exposure_cap * entry_fill_cash

    seconds = tick.seconds
    n = int(seconds.size)
    if n == 0:
        return [], 0
    best_ask = np.full(n, np.nan)
    best_bid = np.full(n, np.nan)
    for i in range(n):
        asks = tick.ask_levels(i, side)
        if asks:
            best_ask[i] = asks[0][0]
        bids = tick.bid_levels(i, side)
        if bids:
            best_bid[i] = bids[0][0]
    valid = np.isfinite(best_ask)
    crossed = valid & np.isfinite(best_bid) & (best_bid >= best_ask - 1e-9)
    good_post = valid & ~crossed & (seconds >= entry_fill_ts - 1e-9)
    post_idx = np.flatnonzero(good_post)
    if post_idx.size == 0:
        return [], 0
    post_secs = seconds[post_idx]
    post_cummin = np.minimum.accumulate(best_ask[post_idx])

    legs: list[TopupLeg] = []
    rejects = 0
    cum_cash = 0.0
    dip_floor = ask0 - topup.no_dip
    t = math.ceil(entry_fill_ts) + 1
    while t < settlement_ts:
        j = int(np.searchsorted(post_secs, t, side="right") - 1)
        if j < 0:
            t += topup.cadence_s
            continue
        if float(post_cummin[j]) < dip_floor - 1e-9:
            break  # no-dip failed; never re-arms

        di = tick.index_at(t)
        if di is None or not valid[di] or crossed[di]:
            t += topup.cadence_s
            continue
        d_ask = float(best_ask[di])
        d_bid = float(best_bid[di])
        if d_ask > topup.cap + 1e-9:
            t += topup.cadence_s
            continue
        if topup.spread_max is not None:
            if not math.isfinite(d_bid) or (d_ask - d_bid) > topup.spread_max + 1e-9:
                t += topup.cadence_s
                continue

        if len(legs) >= topup.max_n:
            break
        if cum_cash + topup_stake > exposure_budget + 1e-9:
            break

        walked_limit = _walk_limit(tick.ask_levels(di, side), topup_stake)
        if walked_limit is None:
            rejects += 1
            t += topup.cadence_s
            continue
        limit_price = min(walked_limit + te.FAK_PAD, 0.99)
        exec_ts = t + latency_seconds
        ei = tick.index_at(exec_ts)
        fill = None
        if ei is not None:
            fill = te.fak_buy(tick.ask_levels(ei, side), topup_stake, limit_price)
        if fill is None:
            rejects += 1
            t += topup.cadence_s
            continue

        legs.append(TopupLeg(fill.cash_usd, fill.shares, fill.vwap, exec_ts))
        cum_cash += fill.cash_usd
        t += topup.cadence_s

    return legs, rejects


def _process_attempt(
    state: ReplayState,
    row,
    *,
    latency_seconds: float,
    get_tick,
    scored: pd.DataFrame,
    score_groups: dict[str, np.ndarray],
    config: VariantConfig,
    topup: TopupConfig,
) -> None:
    slug = str(row.market_slug)
    if slug in state.filled_markets:
        return

    state.counters["qualifying_seen"] += 1
    if config.nl15_mult == 0.0 and str(row.sleeve) == "nl_15m":
        state.counters["sleeve_mult_skip"] += 1
        return
    if state.drawdown >= config.dd_halt:
        state.counters["dd_halt_skip"] += 1
        return
    if state.cash + 1e-9 < te.MIN_BET_USD:
        state.counters["cash_skip"] += 1
        return

    tick = get_tick(slug)
    decision_ts = int(row.entry_ts) + te.DECISION_ANCHOR_SECONDS
    decision_index = tick.index_at(decision_ts)
    if decision_index is None:
        state.counters["missing_decision_book"] += 1
        return
    ask_levels = tick.ask_levels(decision_index, str(row.side))
    if not ask_levels:
        state.counters["missing_decision_book"] += 1
        return

    stake = te.live_stake_usd(
        state.cash,
        str(row.interval),
        state.drawdown,
        int(row.concurrency_k),
        use_k_weight=config.use_k_weight,
        brake_scale=config.brake_scale,
        brake_floor=config.brake_floor,
    )
    tier_a = int(row.attempt_rank) == 1 and float(row.pred_edge) >= config.tier_a_edge
    if tier_a:
        stake *= config.tier_a_mult
    if str(row.sleeve) == "nl_15m":
        stake *= config.nl15_mult
    target = min(max(te.MIN_BET_USD, stake), state.cash)
    quote = te.quote_edge_preserving(
        ask_levels,
        target,
        model_p=float(row.model_p),
        min_edge=_minimum_edge(str(row.band)),
    )
    if quote is None:
        state.counters["walker_skip"] += 1
        return

    requested = _floor_cents(quote.amount_usd)
    if requested + 1e-9 < te.MIN_BET_USD:
        state.counters["walker_skip"] += 1
        return

    anchor_key = (slug, str(row.sleeve), str(row.side))
    anchor = state.fak_anchors.setdefault(anchor_key, quote.limit_price)
    limit_price = te.padded_fak_limit(quote.limit_price, anchor=anchor)
    state.counters["orders"] += 1

    entry_fill_ts = decision_ts + latency_seconds
    execution_index = tick.index_at(entry_fill_ts)
    fill = None
    if execution_index is not None:
        fill = te.fak_buy(
            tick.ask_levels(execution_index, str(row.side)),
            requested,
            limit_price,
        )
    if fill is None:
        state.counters["fak_reject"] += 1
        return

    state.filled_markets.add(slug)
    state.cash -= fill.cash_usd
    state.counters["fills"] += 1
    if fill.cash_usd + 0.005 < requested:
        state.counters["partial_fills"] += 1

    interval_seconds = 300 if str(row.interval) == "5m" else 900
    settlement_ts = int(row.window_start) + interval_seconds
    held_scores = _held_scores(scored, score_groups, slug, str(row.side))

    # --- frozen-at-entry taker top-up ladder ------------------------------
    topup_legs, topup_rejects = _simulate_topups(
        tick,
        side=str(row.side),
        ask0=float(row.ask),
        entry_fill_ts=entry_fill_ts,
        entry_requested=requested,
        entry_fill_cash=fill.cash_usd,
        settlement_ts=settlement_ts,
        latency_seconds=latency_seconds,
        topup=topup,
    )

    def _run_exit(shares: float):
        return te.simulate_exit004(
            tick,
            held_scores,
            side=str(row.side),
            entry_score_ts=int(row.entry_ts),
            entry_fill_ts=entry_fill_ts,
            entry_fill_price=fill.vwap,
            shares=shares,
            settlement_ts=settlement_ts,
            win=int(row.win_true),
            latency_seconds=latency_seconds,
            max_attempts=config.stop_attempts,
            sell_chase=config.stop_chase,
        )

    if state.policy == "exit004":
        pass1 = _run_exit(fill.shares)  # entry-only trigger (shares-independent)
        if topup_legs and pass1.stopped:
            trigger_decision_ts = pass1.close_ts - latency_seconds
            surviving = [
                leg for leg in topup_legs if leg.fill_ts < trigger_decision_ts - 1e-9
            ]
            state.counters["topup_dropped"] += len(topup_legs) - len(surviving)
        else:
            surviving = topup_legs
        topup_shares = math.fsum(leg.shares for leg in surviving)
        if topup_shares > 0:
            outcome = _run_exit(fill.shares + topup_shares)
        else:
            outcome = pass1
    else:  # hold policy: all legs settle at resolution
        surviving = topup_legs
        topup_shares = math.fsum(leg.shares for leg in surviving)
        total_shares = fill.shares + topup_shares
        outcome = te.ExitResult(
            stopped=False,
            stop_triggered=False,
            attempts=0,
            close_ts=float(settlement_ts),
            gross_return_usd=total_shares * float(row.win_true),
            exit_fill=None,
            trigger_bid=None,
            trigger_model_p=None,
        )

    topup_cash = math.fsum(leg.cash for leg in surviving)
    topup_fee = math.fsum(te.entry_fee_usd(leg.vwap, leg.shares) for leg in surviving)
    state.cash -= topup_cash  # early cash lock for surviving top-ups
    state.counters["topup_fills"] += len(surviving)
    state.counters["topup_rejects"] += topup_rejects

    # Live settlement semantics (mirrors 87469fb in the canonical driver): the
    # calibrated per-SHARE entry-leg fee on every taker BUY (entry + top-ups),
    # and NOTHING on the exit leg (Polymarket does not charge the stop SELL --
    # see te.entry_fee_usd / execution.stop_loss). The old
    # `cash_usd * fee(vwap)` + `gross_return * fee(exit_vwap)` pair both
    # under-charged entry (per-dollar, uncalibrated) and invented an exit fee.
    entry_fee = te.entry_fee_usd(fill.vwap, fill.shares)
    exit_fee = 0.0  # kept in the detail schema; always zero on Polymarket
    cash_return = outcome.gross_return_usd - entry_fee - topup_fee - exit_fee
    pnl = cash_return - fill.cash_usd - topup_cash
    state.schedule(outcome.close_ts, cash_return, pnl)

    if outcome.stop_triggered:
        state.counters["stop_triggers"] += 1
    state.counters["stop_attempts"] += outcome.attempts
    if outcome.stopped:
        state.counters["stop_fills"] += 1
        state.counters["stop_rejects"] += max(0, outcome.attempts - 1)
    else:
        state.counters["stop_rejects"] += outcome.attempts

    state.records.append(
        {
            "fold": state.fold,
            "policy": state.policy,
            "market_slug": slug,
            "coin": str(row.coin),
            "interval": str(row.interval),
            "band": str(row.band),
            "side": str(row.side),
            "entry_score_ts": int(row.entry_ts),
            "entry_fill_ts": float(entry_fill_ts),
            "concurrency_k": int(row.concurrency_k),
            "attempt_rank": int(row.attempt_rank),
            "tier_a": bool(tier_a),
            "target_usd": float(target),
            "requested_usd": float(requested),
            "filled_usd": float(fill.cash_usd),
            "shares": float(fill.shares),
            "entry_vwap": float(fill.vwap),
            "fak_limit": float(limit_price),
            "entry_fee": float(entry_fee),
            "win": int(row.win_true),
            "stop_triggered": bool(outcome.stop_triggered),
            "stop_attempts": int(outcome.attempts),
            "stopped": bool(outcome.stopped),
            "exit_vwap": (
                float(outcome.exit_fill.vwap)
                if outcome.exit_fill is not None
                else np.nan
            ),
            "exit_fee": float(exit_fee),
            "close_ts": float(outcome.close_ts),
            "pnl": float(pnl),
            "topup_count": int(len(surviving)),
            "topup_cash": float(topup_cash),
            "topup_shares": float(topup_shares),
            "topup_rejects": int(topup_rejects),
        }
    )


def run_replay(
    scored: pd.DataFrame,
    attempts: pd.DataFrame,
    score_groups: dict[str, np.ndarray],
    *,
    store: TickStore,
    folds: tuple[str, ...],
    latency_ms: int,
    start_capital: float,
    cache_size: int,
    config: VariantConfig,
    topup: TopupConfig,
    policies: tuple[str, ...] = ("exit004",),
) -> tuple[dict, pd.DataFrame]:
    latency_seconds = latency_ms / 1000.0

    @lru_cache(maxsize=cache_size)
    def get_tick(slug: str) -> te.TickBook:
        return te.load_tick_slug(slug, store)

    fold_reports = {}
    records = []
    for fold in folds:
        fold_attempts = attempts[attempts["fold"] == fold]
        states = {
            policy: ReplayState(policy, fold, start_capital) for policy in policies
        }
        started = time.monotonic()
        for group_number, (entry_ts, group) in enumerate(
            fold_attempts.groupby("entry_ts", sort=True), start=1
        ):
            execution_ts = (
                int(entry_ts) + te.DECISION_ANCHOR_SECONDS + latency_seconds
            )
            for state in states.values():
                state.drain(execution_ts, inclusive=False)
            for row in group.itertuples(index=False):
                for state in states.values():
                    _process_attempt(
                        state,
                        row,
                        latency_seconds=latency_seconds,
                        get_tick=get_tick,
                        scored=scored,
                        score_groups=score_groups,
                        config=config,
                        topup=topup,
                    )
            for state in states.values():
                state.drain(execution_ts, inclusive=True)
            if group_number % 2500 == 0:
                elapsed = time.monotonic() - started
                print(
                    f"  fold {fold}: {group_number:,} decision seconds "
                    f"({elapsed:.0f}s)",
                    flush=True,
                )

        for state in states.values():
            state.drain(math.inf, inclusive=True)
            records.extend(state.records)
        fold_reports[fold] = {
            "eligible_markets": int(fold_attempts["market_slug"].nunique()),
            "qualifying_seconds": int(len(fold_attempts)),
        }
        for policy in policies:
            sub = _state_report(states[policy])
            counters = states[policy].counters
            sub["topup_fills"] = counters["topup_fills"]
            sub["topup_rejects"] = counters["topup_rejects"]
            sub["topup_dropped"] = counters["topup_dropped"]
            fold_reports[fold][policy] = sub
        lead = policies[-1]
        print(
            f"  fold {fold} done: {lead} n={fold_reports[fold][lead]['n']:,} "
            f"pnl={fold_reports[fold][lead]['pnl']:+.2f} "
            f"maxDD={fold_reports[fold][lead]['maxdd_pct']:.2f}% "
            f"topups={fold_reports[fold][lead]['topup_fills']:,}",
            flush=True,
        )
        get_tick.cache_clear()

    return fold_reports, pd.DataFrame(records)


def _opt_float(text: str) -> float | None:
    if text.lower() in ("none", "off", "-"):
        return None
    return float(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--latency", type=int, default=400)
    parser.add_argument("--folds", nargs="+", choices=te.FOLDS, default=list(te.FOLDS))
    parser.add_argument("--start-capital", type=float, default=100.0)
    parser.add_argument("--score-path", type=Path, default=te.POOL_PATH)
    parser.add_argument("--tick-db", type=Path, default=te.TICK_DB)
    parser.add_argument("--option3-dir", type=Path, default=OPTION3_DIR)
    parser.add_argument("--run-dir", type=Path, default=RUN_DIR,
                        help="tracked .json report destination")
    parser.add_argument("--heavy-dir", type=Path, default=HEAVY_DIR,
                        help="gitignored detail .parquet destination")
    parser.add_argument("--tick-cache-size", type=int, default=128)
    parser.add_argument("--tag", default="")
    parser.add_argument(
        "--policies", nargs="+", choices=("hold", "exit004"), default=["exit004"]
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help="smoke-test cap: keep only first N markets per fold (0 = all)",
    )
    # F2 variant knobs
    parser.add_argument("--tier-a-mult", type=float, default=1.0)
    parser.add_argument("--tier-a-edge", type=float, default=0.0465)
    parser.add_argument("--nl15-mult", type=float, default=1.0)
    parser.add_argument("--no-k-weight", action="store_true")
    parser.add_argument("--stop-chase", type=float, default=0.0)
    parser.add_argument("--stop-attempts", type=int, default=te.STOP_MAX_ATTEMPTS)
    parser.add_argument("--brake-scale", type=float, default=te.BRAKE_SCALE)
    parser.add_argument("--brake-floor", type=float, default=te.BRAKE_FLOOR)
    parser.add_argument("--dd-halt", type=float, default=te.HARD_DD_HALT)
    # top-up knobs
    parser.add_argument("--topup-max-n", type=int, default=0)
    parser.add_argument("--topup-frac", type=float, default=0.5)
    parser.add_argument("--topup-no-dip", type=float, default=0.0)
    parser.add_argument("--topup-cap", type=float, default=0.83)
    parser.add_argument("--topup-spread-max", type=_opt_float, default=0.02)
    parser.add_argument("--topup-cadence", type=int, default=1)
    parser.add_argument("--topup-exposure-cap", type=float, default=3.0)
    parser.add_argument(
        "--late-skip",
        type=int,
        default=0,
        help="drop attempts with (interval_sec - offset_sec) <= N, mirroring "
             "the live CRYPTOBOT_LATE_WINDOW_SKIP gate (0 = off; the base "
             "driver stays off so historical reruns reproduce)",
    )
    args = parser.parse_args(argv)

    if args.latency <= 0:
        parser.error("--latency must be positive")
    if args.start_capital <= 0:
        parser.error("--start-capital must be positive")

    config = VariantConfig(
        tier_a_mult=args.tier_a_mult,
        tier_a_edge=args.tier_a_edge,
        nl15_mult=args.nl15_mult,
        use_k_weight=not args.no_k_weight,
        stop_chase=args.stop_chase,
        stop_attempts=args.stop_attempts,
        brake_scale=args.brake_scale,
        brake_floor=args.brake_floor,
        dd_halt=args.dd_halt,
    )
    topup = TopupConfig(
        max_n=args.topup_max_n,
        frac=args.topup_frac,
        no_dip=args.topup_no_dip,
        cap=args.topup_cap,
        spread_max=args.topup_spread_max,
        cadence_s=args.topup_cadence,
        exposure_cap=args.topup_exposure_cap,
    )

    started = time.monotonic()
    print("[load] WFO scores, side identity, and tick coverage", flush=True)
    store = open_store(args.tick_db)
    scored, attempts, score_groups, coverage = load_wfo_inputs(
        args.score_path, store, args.option3_dir
    )
    if args.late_skip > 0:
        before = len(attempts)
        attempts = apply_late_skip(attempts, args.late_skip)
        print(f"[late-skip] <= {args.late_skip}s to close dropped "
              f"{before - len(attempts):,} of {before:,} attempts", flush=True)
    if args.max_markets > 0:
        keep = []
        for fold in args.folds:
            fa = attempts[attempts["fold"] == fold]
            slugs = (
                fa.sort_values("entry_ts", kind="stable")["market_slug"]
                .drop_duplicates()
                .head(args.max_markets)
            )
            keep.append(fa[fa["market_slug"].isin(set(slugs))])
        attempts = pd.concat(keep, ignore_index=True)
        print(f"[smoke] capped to {args.max_markets} markets/fold -> "
              f"{len(attempts):,} attempts", flush=True)
    print(
        f"[load] candidate markets={coverage['candidate_markets']:,} "
        f"qualifying seconds={coverage['qualifying_seconds']:,} "
        f"variant={config.as_dict()} topup={topup.as_dict()}",
        flush=True,
    )

    fold_reports, detail = run_replay(
        scored,
        attempts,
        score_groups,
        store=store,
        folds=tuple(args.folds),
        latency_ms=args.latency,
        start_capital=args.start_capital,
        cache_size=args.tick_cache_size,
        config=config,
        topup=topup,
        policies=tuple(args.policies),
    )
    report = {
        "schema_version": 2,
        "strategy": "v2 step1last MID+NL union + PURE_STOP_04 + scale-in top-ups",
        "variant": config.as_dict(),
        "topup": topup.as_dict(),
        "latency_ms": args.latency,
        "late_skip_sec": int(args.late_skip),
        "start_capital_per_fold": args.start_capital,
        "scoring": {
            "features": 11,
            "method": "expanding-window per-fold WFO with embargo",
            "score_path": str(args.score_path),
        },
        "coverage": coverage,
        "folds": fold_reports,
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }

    args.run_dir.mkdir(parents=True, exist_ok=True)
    args.heavy_dir.mkdir(parents=True, exist_ok=True)
    tag = f"_{args.tag}" if args.tag else ""
    detail_path = args.heavy_dir / DETAIL_NAME.format(latency=args.latency, tag=tag)
    report_path = args.run_dir / REPORT_NAME.format(latency=args.latency, tag=tag)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    detail_temp = detail_path.with_name(
        f".{detail_path.stem}.{time.time_ns()}.tmp.parquet"
    )
    try:
        detail.to_parquet(detail_temp, index=False)
        detail_temp.replace(detail_path)
    finally:
        if detail_temp.exists():
            detail_temp.unlink()

    print(
        json.dumps(
            {
                "folds": fold_reports,
                "saved": {"report": str(report_path), "detail": str(detail_path)},
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
