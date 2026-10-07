"""Shared PMData scoring/fill/settlement helpers for the sub-minute pipeline.

Originally the exp044 target-window evaluator for the frozen
``data/models/hgb_v1.joblib`` + ``rf_v1.joblib`` pair. That CLI was retired with
exp044 on 2026-07-31 and both artifacts are now off the disk; what remains is
the helper library ``training.checkpoint`` / ``build_pool`` / ``offset_wfo``
import (manifest sleeves, fee/ask-bucket math, pmdata.dev book payloads).

(``offset_validation`` does NOT import this module — an earlier revision of
this docstring and the footer comment both claimed it did; offset_validation
imports only ``modeling`` and ``offset_wfo``.)
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
    """Fail loudly when the 1 Hz pmdata book tree is missing. Call at ENTRY only.

    ``poly_snapshot/`` was deleted on 2026-07-31 in the data consolidation, and
    every reader below it degrades **silently**: ``_load_raw_snapshot`` and
    ``offset_wfo._load_top_book_series`` return ``None`` per market, so a rebuild
    walks the whole universe, drops every row on "no snapshot", and writes an
    EMPTY pool that reads exactly like "the strategy found no bets". That is the
    one failure mode a rebuild must never have, so the three chain entries that
    funnel into those readers — ``training.build_pool.build_grid_pool``,
    ``training.checkpoint._build_pool_for_grid`` and
    ``training.offset_wfo._attach_raw_ladders`` — call this ONCE up front.

    Never call it per row or per market: the check is a filesystem stat, and the
    readers it protects run inside 128k-market loops. Guard at the entry, once.
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
# Sleeve/ask-bucket value types. RELOCATED VERBATIM 2026-08-01 (phase 6) out of
# ``training.residual_replay`` (SleeveConfig) and ``training.residual_matrix``
# (AskBucket, _split_csv, _parse_buckets) when both of those research CLIs were
# deleted — their run() entry points died at ``modeling._load_full_table`` on
# the deleted artifacts/decision_table.parquet. Signatures are unchanged.
# ``_sleeve_configs`` below is SleeveConfig's only production constructor, and
# ``offset_validation._simulate_wallet`` reads exactly three of its fields
# (stake_frac, max_bet_usd, book_consumption_max); the rest are carried so the
# dataclass stays signature-compatible with what it replaced.
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


# RETIRED 2026-07-31 (exp044): the main() CLI that used to sit here scored a
# target window with the frozen hgb_v1.joblib + rf_v1.joblib pair. Both joblibs
# and both loaders are gone (phases 1-3), so the entry point went with them.
# RETIRED 2026-08-01 (phase 4): `_evaluate_wallet_raw` — the raw-book wallet
# simulator that CLI drove — and its only helper `_raw_execution_fill` went too;
# with main() gone neither had a caller, and `_raw_execution_fill` was the sole
# user of the `walk_book_edge_preserving` import.
# RETIRED 2026-08-01 (phase 5): the leftovers that CLI had been the only reason
# for — `import argparse`, `import json`, `_json_default` (its json.dumps
# `default=` hook), the `BT_CACHE_DIR`/`BT_RUNS_DIR` imports, `BOOK_DIR` (the
# book_depth_index_pmdata cache, deleted 2026-07-31), `SNAP_FEATURES` (nothing
# has read the narrow snapshot_features.parquet since the pool moved to the
# _full frame) and the `load_book_depth_index` / `_evaluate_wallet` imports.
# Each was confirmed zero-consumer by a repo-wide grep before removal; the
# functions themselves still live in book_depth.py, which training.portfolio
# still imports.
# RETIRED 2026-08-01 (phase 6), all grep-confirmed zero-consumer:
#   * the exp044 scoring chain `_rf_probs` / `_score_side` / `_build_candidates`
#     — their `hgb`/`rf` parameters were the retired hgb_v1/rf_v1 joblib loaders
#     (deleted af3c124). `_build_candidates` held this file's ONLY `ev.` use, so
#     the `modeling as ev` import went with it, cutting the
#     pmdata_target -> modeling -> portfolio -> optuna import edge.
#   * `_attach_labels` + the `forward_oos_regrade.Settler` import (a one-way
#     research->arbiter edge; forward_oos_regrade never imports training/*).
#   * `OPR_SELECTED_FILTERS` + `_apply_selected_opr_filters`. `_attach_opr_columns`
#     STAYS — checkpoint.py calls it.
#   * `RES_CACHE` / `_ts`, and the whole binance-kline subsystem (`KLINE_CACHE`,
#     `SYMBOLS`, `BINANCE_URL`, `_ms`, `_init_kline_cache`, `_fetch_binance`,
#     `_ensure_klines`, `_load_bars`, `_attach_partial_ret_z`) with the
#     `sqlite3` / `time` / `requests` / `Iterable` / `REPO_ROOT` imports it alone
#     needed. Every external consumer of that block lived inside the zero-caller
#     `offset_wfo._attach_offset_features`, deleted in the SAME commit — do both
#     or neither. NOTE the DATA is not missing: both
#     data/server_snapshot/oos_resolutions_cache.sqlite and
#     binance_signal_log_1m_cache.sqlite still exist on disk; only the constants
#     naming them are gone. Nothing was deleted from disk.
# ARRIVED 2026-08-01 (phase 6): `SleeveConfig` (from residual_replay) and
# `AskBucket` / `_split_csv` / `_parse_buckets` (from residual_matrix), verbatim,
# when both of those modules were deleted whole.
# Everything above is the SHARED helper surface the surviving sub-minute
# pipeline imports (training.checkpoint, training.build_pool, and — for the
# per-second feature/book substrate — training.offset_wfo) — do not delete this
# module. `_raw_book_payload` (offset_wfo) and `_load_raw_snapshot` (build_pool,
# offset_wfo) are live, and `require_snapshot_dir` is the fail-fast guard in
# front of both. build_pool.py reaches `_load_raw_snapshot.__wrapped__`, so the
# @lru_cache above it is load-bearing: removing it breaks build_pool at RUNTIME
# with AttributeError, not at import.
