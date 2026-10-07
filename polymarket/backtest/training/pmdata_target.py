"""Shared PMData helpers for the sub-minute research pipeline: sleeve and
ask-bucket types, fee math, and pmdata.dev book payloads. Imported by
``training.checkpoint``, ``build_pool`` and ``offset_wfo``.
"""
from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core" / "src"))
from polymarket.backtest import BT_DATA_DIR  # noqa: E402

from cryptobot import config as live_config  # noqa: E402


PMDATA_SNAPSHOT_DIR = BT_DATA_DIR / "pmdata" / "poly_snapshot"


def require_snapshot_dir() -> Path:
    """Fail loudly when the 1 Hz pmdata book tree is missing. Call at entry
    only.

    Every reader below degrades silently: ``_load_raw_snapshot`` and
    ``offset_wfo._load_top_book_series`` return ``None`` per market, so a
    rebuild would drop every row and write an empty pool that looks like "no
    bets found". The three entry points that lead to those readers
    (``training.build_pool.build_grid_pool``,
    ``training.checkpoint._build_pool_for_grid`` and
    ``training.offset_wfo._attach_raw_ladders``) call this once up front.

    Do not call it per row or per market; the readers run inside loops over
    ~128k markets.
    """
    if not PMDATA_SNAPSHOT_DIR.is_dir():
        raise FileNotFoundError(
            f"{PMDATA_SNAPSHOT_DIR}: poly_snapshot deleted 2026-07-31; "
            "re-download via fetch/pmdata_download.py before rebuilding — "
            "see SOURCES.md"
        )
    if next(PMDATA_SNAPSHOT_DIR.iterdir(), None) is None:
        # A bare mkdir (half-finished download, stray env override) re-creates
        # the silent-empty failure the missing-dir branch exists to stop.
        raise FileNotFoundError(
            f"{PMDATA_SNAPSHOT_DIR} is EMPTY: poly_snapshot deleted 2026-07-31; "
            "re-download via fetch/pmdata_download.py before rebuilding — "
            "see SOURCES.md"
        )
    return PMDATA_SNAPSHOT_DIR


COIN_MAP = {"btc": 0, "eth": 1, "sol": 2, "xrp": 3}
INTERVAL_MIN = {"5m": 5, "15m": 15}
ENTRY_MINUTES = {"5m": tuple(range(2, 5)), "15m": tuple(range(2, 15))}


@dataclass(frozen=True)
class ManifestSleeve:
    label: str
    interval: str
    threshold: float
    stake_frac: float
    book_consumption_max: Optional[float]
    ask_bucket_raw: str
    side_filter: str
    feature_columns: list[str]


# --------------------------------------------------------------------------- #
# Sleeve and ask-bucket value types. ``_sleeve_configs`` below is the only
# constructor of SleeveConfig, and ``offset_validation._simulate_wallet`` reads
# three of its fields (stake_frac, max_bet_usd, book_consumption_max).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AskBucket:
    name: str
    ranges: tuple[tuple[float, float], ...]


def _split_csv(raw: str) -> list[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def _parse_buckets(raw: str) -> list[AskBucket]:
    """Parse ``name:lo:hi`` (``|``-joined for multiple ranges), comma-separated.

    Same grammar as ``_ask_bucket_ok`` below, and the one mirrored by
    ``cryptobot.strategy.portfolio_loader._parse_ask_bucket`` /
    ``cryptobot.strategy.hgb_loader._manifest_ask_bucket_ranges``.
    """
    buckets: list[AskBucket] = []
    for token in _split_csv(raw):
        parts = token.split(":", 1)
        if len(parts) != 2:
            raise ValueError(f"bad bucket {token!r}; expected name:min:max or name:min:max|min:max")
        name, ranges_raw = parts
        ranges: list[tuple[float, float]] = []
        for range_raw in ranges_raw.split("|"):
            lo_hi = range_raw.split(":")
            if len(lo_hi) != 2:
                raise ValueError(f"bad bucket range {range_raw!r}")
            ranges.append((float(lo_hi[0]), float(lo_hi[1])))
        buckets.append(AskBucket(name=name, ranges=tuple(ranges)))
    return buckets


@dataclass(frozen=True)
class SleeveConfig:
    name: str
    interval: str
    entry_minutes: tuple[int, ...] | None
    feature_groups: str
    sample_weight_mode: str
    hgb_max_iter: int
    hgb_learning_rate: float
    hgb_max_leaf_nodes: int
    hgb_l2_regularization: float
    side_filter: str
    ask_bucket: AskBucket
    threshold: float
    stake_frac: float
    max_bet_usd: float | None
    book_consumption_max: float
    min_edge: float
    model: str = "hgb"


def _manifest_sleeves(manifest: dict) -> dict[str, ManifestSleeve]:
    out: dict[str, ManifestSleeve] = {}
    for interval, sm in manifest["sleeves"].items():
        iv = str(interval).lower()
        out[iv] = ManifestSleeve(
            label=f"exp044_{iv}",
            interval=iv,
            threshold=float(sm["threshold"]),
            stake_frac=float(sm["stake_frac"]),
            book_consumption_max=(
                None if sm.get("book_consumption_max") is None else float(sm["book_consumption_max"])
            ),
            ask_bucket_raw=str(sm["ask_bucket"]),
            side_filter=str(sm.get("side_filter", "both")).lower(),
            feature_columns=list(sm["feature_columns"]),
        )
    return out


def _ask_bucket_ok(ask: pd.Series, raw: str) -> pd.Series:
    mask = pd.Series(False, index=ask.index)
    for piece in raw.split("|"):
        parts = piece.split(":")
        if len(parts) != 3:
            raise ValueError(f"bad ask_bucket piece: {piece!r}")
        lo, hi = float(parts[1]), float(parts[2])
        mask |= (ask >= lo) & (ask < hi)
    return mask


def _fee(ask: pd.Series | np.ndarray) -> np.ndarray:
    a = np.asarray(ask, dtype=np.float64)
    return live_config.POLY_FEE_THETA * a * (1.0 - a)


def _attach_opr_columns(candidates: pd.DataFrame) -> pd.DataFrame:
    out = candidates.copy()
    if out.empty:
        for col in ("range_strength_bps", "breakout_strength_bps", "signal_delta_ret", "entry_z_signed"):
            out[col] = np.array([], dtype=np.float64)
        return out
    sign = np.where(out["side"].astype(str).eq("Up"), 1.0, -1.0)
    out["range_strength_bps"] = sign * out["range_ret"].astype(float).to_numpy() * 10_000.0
    out["breakout_strength_bps"] = sign * (
        out["entry_ret"].astype(float).to_numpy() - out["range_ret"].astype(float).to_numpy()
    ) * 10_000.0
    out["signal_delta_ret"] = out["breakout_strength_bps"] / 10_000.0
    out["entry_z_signed"] = sign * out["entry_z"].astype(float).to_numpy()
    return out


def _sleeve_configs(sleeves: dict[str, ManifestSleeve]) -> dict[str, SleeveConfig]:
    out: dict[str, SleeveConfig] = {}
    for sleeve in sleeves.values():
        bucket = _parse_buckets(sleeve.ask_bucket_raw)[0]
        out[sleeve.label] = SleeveConfig(
            name=sleeve.label,
            interval=sleeve.interval,
            entry_minutes=ENTRY_MINUTES[sleeve.interval],
            feature_groups=",".join(sleeve.feature_columns),
            sample_weight_mode="fixed_artifact",
            hgb_max_iter=0,
            hgb_learning_rate=0.0,
            hgb_max_leaf_nodes=0,
            hgb_l2_regularization=0.0,
            side_filter=sleeve.side_filter,
            ask_bucket=bucket,
            threshold=sleeve.threshold,
            stake_frac=sleeve.stake_frac,
            max_bet_usd=None,
            book_consumption_max=(
                1.0 if sleeve.book_consumption_max is None else sleeve.book_consumption_max
            ),
            min_edge=-1.0,
            model="hgb_fixed",
        )
    return out


@lru_cache(maxsize=4096)
def _load_raw_snapshot(coin: str, interval: str, slug: str) -> pd.DataFrame | None:
    path = PMDATA_SNAPSHOT_DIR / coin.lower() / interval.lower() / f"{slug}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["timestamp", "ask_prices", "ask_sizes", "bid_prices", "bid_sizes"])
    if df.empty:
        return None
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _raw_book_payload(row) -> tuple[dict, float] | None:
    df = _load_raw_snapshot(str(row.coin), str(row.interval), str(row.market_slug))
    if df is None:
        return None
    ts = pd.to_datetime(df["timestamp"], utc=True)
    entry = pd.Timestamp(int(row.entry_ts), unit="s", tz="UTC")
    pos = int(ts.searchsorted(entry, side="right") - 1)
    if pos < 0:
        return None
    snap = df.iloc[pos]
    side = str(row.side)
    asks: list[dict[str, float]] = []
    if side == "Up":
        prices = snap["ask_prices"]
        sizes = snap["ask_sizes"]
        for price, size in zip(prices, sizes):
            p = float(price)
            z = float(size)
            if math.isfinite(p) and math.isfinite(z) and 0.0 < p < 1.0 and z > 0.0:
                asks.append({"price": p, "size": z})
    else:
        prices = snap["bid_prices"]
        sizes = snap["bid_sizes"]
        for bid, size in zip(prices, sizes):
            p = 1.0 - float(bid)
            z = float(size)
            if math.isfinite(p) and math.isfinite(z) and 0.0 < p < 1.0 and z > 0.0:
                asks.append({"price": p, "size": z})
    if not asks:
        return None
    asks.sort(key=lambda x: x["price"])
    depth_usd = float(sum(a["price"] * a["size"] for a in asks))
    if depth_usd <= 0.0 or not math.isfinite(depth_usd):
        return None
    return {"asks": asks}, depth_usd


# ``build_pool.py`` uses ``_load_raw_snapshot.__wrapped__``, so the @lru_cache
# on it must stay (removing it fails at runtime, not import).
