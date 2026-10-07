"""Live-like tick execution primitives for the v2 subminute strategy and PURE_STOP_04.

The scored artifact supplies walk-forward 11-feature probabilities once per
second. Raw PMData supplies sub-second UP-token books. This module owns side
selection, edge-preserving entry quoting, delayed partial FAK buys, dynamic
held-side stop scoring, and delayed full-size FOK sells.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import numpy as np
import pandas as pd

from polymarket.backtest import BT_RUNS_DIR
from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA
from polymarket.backtest.lib.tick_store import DEFAULT_TICK_DB, TickStore, open_store

POOL_PATH = BT_RUNS_DIR / "scored_11set_truth.parquet"
# The last-30s books are ONE SQLite file since 2026-07-31. The old flat
# ``poly_tick_last30/`` dir and its ``TICK_DIR`` constant are GONE on purpose:
# a constant naming a deleted directory silently globs to an empty slug set and
# reports "0 qualifying markets" instead of failing. Read through the store.
TICK_DB = DEFAULT_TICK_DB
FOLDS = ("B", "C", "D", "E")

DECISION_ANCHOR_SECONDS = 1.0
FAK_PAD = 0.01
STOP_BAND = 0.04
STOP_MAX_ATTEMPTS = 3
MIN_BET_USD = 1.0
BASE_FRAC_5M = 0.0125
BASE_FRAC_15M = 0.025
BRAKE_SCALE = 0.50
BRAKE_FLOOR = 0.50
HARD_DD_HALT = 0.40


@dataclass(frozen=True)
class Fill:
    """One fill. ``cash_usd`` is cost for BUY and proceeds for SELL."""

    cash_usd: float
    shares: float
    vwap: float


@dataclass(frozen=True)
class EntryQuote:
    """Decision-book amount and limit before the live FAK pad."""

    amount_usd: float
    shares: float
    vwap: float
    limit_price: float
    realized_edge: float


@dataclass(frozen=True)
class ExitResult:
    """Gross close result; the bankroll replay applies fill fees."""

    stopped: bool
    stop_triggered: bool
    attempts: int
    close_ts: float
    gross_return_usd: float
    exit_fill: Fill | None
    trigger_bid: float | None
    trigger_model_p: float | None


@dataclass
class TickBook:
    """Compact view of one market's UP-token tick books."""

    seconds: np.ndarray
    ask_prices: np.ndarray
    ask_sizes: np.ndarray
    bid_prices: np.ndarray
    bid_sizes: np.ndarray

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> "TickBook":
        required = {
            "timestamp", "ask_prices", "ask_sizes", "bid_prices", "bid_sizes"
        }
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"tick frame missing columns: {sorted(missing)}")
        if frame.empty:
            raise ValueError("tick frame is empty")
        seconds = (
            pd.to_datetime(frame["timestamp"], utc=True)
            .to_numpy(dtype="datetime64[ms]")
            .astype(np.int64)
            / 1000.0
        )
        order = np.argsort(seconds, kind="stable")
        return cls(
            seconds=seconds[order],
            ask_prices=frame["ask_prices"].to_numpy(dtype=object)[order],
            ask_sizes=frame["ask_sizes"].to_numpy(dtype=object)[order],
            bid_prices=frame["bid_prices"].to_numpy(dtype=object)[order],
            bid_sizes=frame["bid_sizes"].to_numpy(dtype=object)[order],
        )

    def index_at(self, timestamp: float) -> int | None:
        """Latest observed book at or before ``timestamp``."""
        index = int(np.searchsorted(self.seconds, timestamp, side="right") - 1)
        return index if index >= 0 else None

    def ask_levels(self, index: int, side: str) -> tuple[tuple[float, float], ...]:
        if side == "UP":
            return _levels(self.ask_prices[index], self.ask_sizes[index], reverse=False)
        return _levels(
            self.bid_prices[index], self.bid_sizes[index], invert=True, reverse=False
        )

    def bid_levels(self, index: int, side: str) -> tuple[tuple[float, float], ...]:
        if side == "UP":
            return _levels(self.bid_prices[index], self.bid_sizes[index], reverse=True)
        return _levels(
            self.ask_prices[index], self.ask_sizes[index], invert=True, reverse=True
        )


def fee(price):
    """Polymarket crypto fee rate at a binary price."""
    return POLY_FEE_THETA * price * (1.0 - price)


def entry_fee_usd(price: float, shares: float) -> float:
    """USD entry-leg taker fee LIVE would book for ``shares`` bought at ``price``.

    SETTLEMENT accounting only — ``fee()`` above stays the DECISION-path rate
    (it feeds ``model_probability`` and the edge-preserving book walk, which
    mirror the live pre-bet edge gate and must not move).

    Delegates to the live source of truth,
    ``cryptobot.execution.grade_fees.entry_fee_usd``, so the replay books the
    exact number the ledger books and the two cannot drift. Two things the old
    inline ``cash_usd * fee(vwap)`` got wrong:

      * the fee is per SHARE, not per dollar of notional — live charges
        ``theta*p*(1-p)`` on every share, i.e. ``cash*theta*(1-p)``, so the
        inline form under-charged by a factor of ``p``;
      * it omitted ``POLY_FEE_CALIBRATION`` (0.864), the wallet-tape measured
        fraction of the published parabola the venue actually extracts.

    Polymarket charges NO fee on the exit (stop SELL) leg — verified on the
    2026-07-23 wallet-tape reconciliation and mirrored by
    ``execution.stop_loss``, which books ``proceeds - cost - entry_fee``.
    There is deliberately no ``exit_fee_usd`` here.
    """
    from cryptobot.execution.grade_fees import entry_fee_usd as _live_entry_fee

    return _live_entry_fee(
        venue="polymarket", entry_price=float(price), shares=float(shares)
    )


def flat_pnl_net(win: int | float, price: float) -> float:
    """Hold-to-resolution PnL for one cash dollar, including entry fee."""
    return float(win) * (1.0 / price - 1.0) - (1.0 - float(win)) - fee(price)


def model_probability(pred_edge, ask):
    """Recover side-specific HGB probability from WFO edge and ask."""
    return pred_edge + ask + fee(ask)


def build_union_attempts(scored: pd.DataFrame) -> pd.DataFrame:
    """Return every live-eligible retry second before fill consumption.

    Live chooses the higher-edge side first, then applies the band gate to that
    side. Live has been subminute-only since 2026-07-21; the retired NL band
    (ask 0.85-0.92, edge >= 0.01) is still scored here purely so the frozen
    tick reports stay bit-for-bit reproducible.
    """
    required = {
        "fold", "market_slug", "entry_ts", "interval", "coin", "ask",
        "offset_sec", "pred_edge", "win_true", "side_up",
    }
    missing = required - set(scored.columns)
    if missing:
        raise ValueError(f"scored panel missing columns: {sorted(missing)}")

    selected = (
        scored.sort_values(
            ["market_slug", "entry_ts", "pred_edge", "side_up"],
            ascending=[True, True, False, False],
            kind="stable",
        )
        .drop_duplicates(["market_slug", "entry_ts"], keep="first")
        .copy()
    )
    selected["coin"] = selected["coin"].astype(str).str.lower()
    selected = selected[
        selected["fold"].isin(FOLDS)
        & selected["interval"].isin(("5m", "15m"))
        & selected["coin"].isin(("btc", "eth", "xrp", "sol"))
    ].copy()
    expected_start = np.where(selected["interval"] == "5m", 270, 870)
    offsets = selected["offset_sec"].to_numpy(dtype=int)
    selected = selected[(offsets >= expected_start) & (offsets <= expected_start + 29)].copy()

    ask = selected["ask"].to_numpy(dtype=float)
    edge = selected["pred_edge"].to_numpy(dtype=float)
    mid = (ask > 0.65) & (ask <= 0.85) & (edge >= 0.03)
    nl = (ask > 0.85) & (ask <= 0.92) & (edge >= 0.01)
    keep = mid | nl
    selected = selected[keep].copy()
    selected["band"] = np.where(mid[keep], "MID", "NL")
    selected["side"] = np.where(selected["side_up"].astype(bool), "UP", "DOWN")
    selected["window_start"] = (
        selected["entry_ts"].astype(np.int64)
        - selected["offset_sec"].astype(np.int64)
    )
    selected["entry_minute"] = selected["offset_sec"].astype(np.int64) // 60
    selected["sleeve"] = np.where(
        selected["band"] == "MID",
        "subminute_" + selected["interval"].astype(str),
        "nl_" + selected["interval"].astype(str),
    )
    selected["model_p"] = model_probability(
        selected["pred_edge"].to_numpy(dtype=float),
        selected["ask"].to_numpy(dtype=float),
    )
    selected["attempt_rank"] = selected.groupby("market_slug").cumcount() + 1
    concurrency_key = [
        "entry_ts", "sleeve", "window_start", "entry_minute", "side"
    ]
    selected["concurrency_k"] = (
        selected.groupby(concurrency_key)["market_slug"].transform("size").astype(int)
    )
    return selected.sort_values(
        ["fold", "entry_ts", "sleeve", "market_slug"], kind="stable"
    ).reset_index(drop=True)


def live_stake_usd(
    available_cash: float,
    interval: str,
    drawdown: float,
    k: int,
    *,
    use_k_weight: bool = True,
    brake_scale: float = BRAKE_SCALE,
    brake_floor: float = BRAKE_FLOOR,
) -> float:
    """Pinned live v2 sizing before the available-cash clamp."""
    base = BASE_FRAC_5M if interval == "5m" else BASE_FRAC_15M
    brake = max(brake_floor, 1.0 - max(0.0, drawdown) / brake_scale)
    weight = 1.0 / math.sqrt(max(1, int(k))) if use_k_weight else 1.0
    return max(MIN_BET_USD, float(available_cash) * base * brake * weight)


def quote_edge_preserving(
    levels: tuple[tuple[float, float], ...],
    target_usd: float,
    *,
    model_p: float,
    min_edge: float,
    min_bet_usd: float = MIN_BET_USD,
) -> EntryQuote | None:
    """Mirror the live entry walker against normalized ask levels."""
    if not all(math.isfinite(x) for x in (target_usd, model_p, min_edge)):
        return None
    if target_usd <= 0:
        return None
    cash = shares = limit = 0.0
    for price, size in levels:
        remaining = target_usd - cash
        if remaining <= 1e-12:
            break
        take_cash = min(price * size, remaining)
        new_cash = cash + take_cash
        new_shares = shares + take_cash / price
        vwap = new_cash / new_shares
        realized_edge = model_p - vwap - fee(vwap)
        if not math.isfinite(realized_edge) or realized_edge < min_edge:
            break
        cash = new_cash
        shares = new_shares
        limit = price
    if shares <= 0 or cash + 1e-12 < min_bet_usd:
        return None
    vwap = cash / shares
    realized_edge = model_p - vwap - fee(vwap)
    if realized_edge < min_edge:
        return None
    return EntryQuote(cash, shares, vwap, limit, realized_edge)


def padded_fak_limit(walked_limit: float, *, anchor: float, pad: float = FAK_PAD) -> float:
    """Live anchored FAK pad, clipped to the binary-price ceiling."""
    return min(walked_limit + pad, anchor + pad, 0.99)


def fak_buy(
    levels: tuple[tuple[float, float], ...], amount_usd: float, limit_price: float
) -> Fill | None:
    """Partial-allowing cash-sized FAK BUY at or below the limit."""
    if amount_usd <= 0 or not math.isfinite(amount_usd):
        return None
    remaining = amount_usd
    cash = shares = 0.0
    for price, size in levels:
        if price > limit_price + 1e-12:
            break
        take_shares = min(size, remaining / price)
        take_cash = take_shares * price
        cash += take_cash
        shares += take_shares
        remaining -= take_cash
        if remaining <= 1e-9:
            break
    if shares <= 0:
        return None
    return Fill(cash, shares, cash / shares)


def fok_sell(
    levels: tuple[tuple[float, float], ...], shares: float, limit_price: float
) -> Fill | None:
    """Full-size FOK SELL at or above the trigger bid."""
    if shares <= 0 or not math.isfinite(shares):
        return None
    remaining = shares
    proceeds = sold = 0.0
    for price, size in levels:
        if price + 1e-12 < limit_price:
            break
        take = min(size, remaining)
        proceeds += take * price
        sold += take
        remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > max(1e-8, shares * 1e-8):
        return None
    return Fill(proceeds, sold, proceeds / sold)


def should_stop(*, bid: float, model_p: float, entry_fill: float, band: float) -> bool:
    """PURE_STOP_04 predicate, equivalent to the live implementation."""
    values = (bid, model_p, entry_fill, band)
    if not all(math.isfinite(float(value)) for value in values):
        return False
    return bid < entry_fill and bid >= model_p + band


def simulate_exit004(
    tick: TickBook,
    held_scores: pd.DataFrame,
    *,
    side: str,
    entry_score_ts: int,
    entry_fill_ts: float,
    entry_fill_price: float,
    shares: float,
    settlement_ts: int,
    win: int,
    latency_seconds: float,
    band: float = STOP_BAND,
    max_attempts: int = STOP_MAX_ATTEMPTS,
    sell_chase: float = 0.0,
) -> ExitResult:
    """Re-score and execute PURE_STOP_04 after a realized entry fill.

    ``sell_chase`` lowers the FOK sell limit to ``bid - sell_chase``. It never
    changes the price of a sell that already fills at the trigger bid (FOK
    proceeds come from the book levels taken, not the limit); it only converts
    would-be failures where full size cannot clear at the trigger bid.
    """
    attempts = 0
    last_bid = last_model_p = None
    post = held_scores[held_scores["entry_ts"] > entry_score_ts].sort_values("entry_ts")
    for row in post.itertuples(index=False):
        score_ts = int(row.entry_ts)
        decision_ts = score_ts + DECISION_ANCHOR_SECONDS
        if decision_ts <= entry_fill_ts or decision_ts >= settlement_ts:
            continue
        decision_index = tick.index_at(decision_ts)
        if decision_index is None:
            continue
        bid_levels = tick.bid_levels(decision_index, side)
        if not bid_levels:
            continue
        bid = bid_levels[0][0]
        model_p = float(model_probability(float(row.pred_edge), float(row.ask)))
        if not should_stop(
            bid=bid, model_p=model_p, entry_fill=entry_fill_price, band=band
        ):
            continue

        attempts += 1
        last_bid, last_model_p = bid, model_p
        execution_ts = decision_ts + latency_seconds
        execution_index = tick.index_at(execution_ts)
        fill = None
        if execution_index is not None:
            sell_limit = max(0.0, bid - sell_chase)
            fill = fok_sell(tick.bid_levels(execution_index, side), shares, sell_limit)
        if fill is not None:
            return ExitResult(
                stopped=True,
                stop_triggered=True,
                attempts=attempts,
                close_ts=execution_ts,
                gross_return_usd=fill.cash_usd,
                exit_fill=fill,
                trigger_bid=bid,
                trigger_model_p=model_p,
            )
        if attempts >= max_attempts:
            break

    return ExitResult(
        stopped=False,
        stop_triggered=attempts > 0,
        attempts=attempts,
        close_ts=float(settlement_ts),
        gross_return_usd=shares * float(win),
        exit_fill=None,
        trigger_bid=last_bid,
        trigger_model_p=last_model_p,
    )


def _levels(
    prices, sizes, *, invert: bool = False, reverse: bool
) -> tuple[tuple[float, float], ...]:
    if prices is None or sizes is None:
        return ()
    out: list[tuple[float, float]] = []
    for raw_price, raw_size in zip(prices, sizes):
        try:
            price = float(raw_price)
            size = float(raw_size)
        except (TypeError, ValueError):
            continue
        if invert:
            price = 1.0 - price
        if not (
            math.isfinite(price)
            and math.isfinite(size)
            and 0.0 < price < 1.0
            and size > 0.0
        ):
            continue
        out.append((price, size))
    out.sort(key=lambda level: level[0], reverse=reverse)
    return tuple(out)


def load_tick(path: str | Path) -> TickBook:
    """Read one slug's books from a standalone parquet file.

    Off every live path since the store landed — kept for ad-hoc reads of a
    one-off parquet export. Replay code must use ``load_tick_slug``.
    """
    return TickBook.from_frame(pd.read_parquet(path))


def load_tick_slug(slug: str, store: TickStore | None = None) -> TickBook:
    """Read one slug's books from the SQLite tick store (the live substrate)."""
    if store is None:
        with open_store() as owned:
            return TickBook.from_frame(owned.read_ticks(slug))
    return TickBook.from_frame(store.read_ticks(slug))
