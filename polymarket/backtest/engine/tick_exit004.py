"""Tick-level execution replay for sub-minute strategies.

The scored artifact gives a model probability every second; raw PMData gives
sub-second YES-token books. This module handles side selection, edge-preserving
entry quotes, delayed partial FAK buys, held-side re-scoring and delayed
full-size FOK sells.
"""
from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd

from polymarket.backtest import BT_RUNS_DIR
from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA
from polymarket.backtest.lib.tick_store import DEFAULT_TICK_DB, TickStore, open_store

POOL_PATH = BT_RUNS_DIR / "scored_11set_truth.parquet"
# The last-30-second books are stored in one SQLite file; read them through the
# tick store.
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
    """USD entry fee live trading would book for ``shares`` bought at
    ``price``.

    For settlement only; ``fee()`` above stays the decision-time rate (it feeds
    ``model_probability`` and the book walk, which mirror the live edge gate).

    Delegates to ``cryptobot.execution.grade_fees.entry_fee_usd`` so the replay
    books exactly what the ledger books. The fee is per share
    (``theta*p*(1-p)`` on every share, i.e. ``cash*theta*(1-p)``), and includes
    ``POLY_FEE_CALIBRATION`` (0.864), the fraction of the published curve the
    venue was measured to charge.

    Polymarket charges no fee on the sell leg, so there is no exit fee here.
    """
    from cryptobot.execution.grade_fees import entry_fee_usd as _live_entry_fee

    return _live_entry_fee(entry_price=float(price), shares=float(shares))


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


def load_tick_slug(slug: str, store: TickStore | None = None) -> TickBook:
    """Read one slug's books from the SQLite tick store (the live substrate)."""
    if store is None:
        with open_store() as owned:
            return TickBook.from_frame(owned.read_ticks(slug))
    return TickBook.from_frame(store.read_ticks(slug))
