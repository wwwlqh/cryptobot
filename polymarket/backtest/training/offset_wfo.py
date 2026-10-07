"""Second-offset entry-timing substrate: pmdata rows -> per-second features/books.

RETIRED as a CLI on 2026-07-31 (exp044 phase 3). It was a parity-first harness:
offset=0 reproduced the frozen minute-boundary ORB/HGB baseline byte-for-byte and
offset>0 shifted only the entry/fill timestamp inside the same entry minute. Every
one of its modes (``parity`` / ``build`` / ``wfo`` / ``all``) scored through the
frozen ``hgb_v1`` + ``rf_v1`` joblib pair, and both artifacts are gone with no
rebuild path — so ``_score_offset_pool``, ``build_pool``, ``_wfo`` and ``main``
were deleted rather than left as an unrunnable entry point.

What SURVIVES is the substrate underneath, which has nothing to do with the
retired models. Re-verified by literal grep 2026-08-01 — this list is the actual
import surface, per consumer:

* ``training.checkpoint`` — ``SNAP_FULL``, ``OUTCOMES``, ``_load_1s``,
  ``_spot_at_seconds``, ``_chunk``, ``_load_top_book_series`` +
  ``_extract_top_book_from_series``, ``_attach_raw_ladders``
* ``training.build_pool`` — ``OUTCOMES``, ``_side_books_from_snapshot``
* ``training.offset_validation`` — ``FEE``, ``MIN_BET_USD``, ``TRAIN_FOLDS``,
  ``OOS_FOLD``, ``_load_1s``, ``_passes_filter``, ``_flat_metrics``, ``_fill``.
  (It does NOT import ``CHUNKS`` — an earlier revision of this docstring said
  so; offset_validation only names CHUNKS in a prose comment.)
* ``_dedup_one_per_market`` + ``DECISION_KEY`` — no production caller; kept
  deliberately as the only executable statement of the one-bet-per-decision
  invariant, pinned by ``tests/test_offset_wfo.py``.

RETIRED 2026-08-01 (phase 6): the offset>0 half of the dead CLI —
``_attach_offset_features``, ``_attach_offset_books``,
``_extract_book_at_timestamp``, ``_extract_book_from_snapshot_df``,
``_parse_int_csv``, ``_json_safe`` and ``BOOK_WORKERS``. All were zero-caller.
``_attach_offset_books`` was also the last reader of the deleted
``poly_snapshot`` tree carrying no ``require_snapshot_dir()`` guard, so its
deletion removes that hazard outright; anyone resurrecting it from git history
inherits the unguarded read and must guard it. Deleting
``_attach_offset_features`` is what took the ``pmdata_target`` binance-kline
cache to zero consumers, and the two went in the same commit.

Import it as a library; there is no ``__main__``.
"""
from __future__ import annotations

import math
import sys
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core" / "src"))
from polymarket.backtest import BT_CACHE_DIR, BT_DATA_DIR  # noqa: E402

from polymarket.backtest.training import pmdata_target as etgt  # noqa: E402
from cryptobot import config as live_config  # noqa: E402
from cryptobot.execution.walk_book import walk_book_edge_preserving  # noqa: E402


SNAP_FULL = BT_DATA_DIR / "pmdata" / "snapshot_features_full.parquet"
OUTCOMES = BT_DATA_DIR / "pmdata" / "pmdata_outcomes.parquet"
BINANCE_1S_DIR = BT_CACHE_DIR / "binance_1s"
CHUNKS = [
    ("A", "2026-02-01", "2026-03-05"),
    ("B", "2026-03-05", "2026-04-06"),
    ("C", "2026-04-06", "2026-05-08"),
    ("D", "2026-05-08", "2026-06-10"),
]
FEE = float(live_config.POLY_FEE_THETA)
MIN_BET_USD = 1.0


def _chunk(dt: pd.Timestamp) -> str:
    for name, lo, hi in CHUNKS:
        if pd.Timestamp(lo, tz="UTC") <= dt < pd.Timestamp(hi, tz="UTC"):
            return name
    return "?"


# Cached at the DEFINITION site (moved here 2026-08-01, phase 6). This re-reads
# an 11.5M-row parquet per coin; with market sharding x per-second grids that is
# thousands of re-reads, so each coin's spot must load exactly once. It used to
# be an `offset_validation` import-time monkeypatch, which meant the cache
# existed only if somebody imported THAT module first — a load-bearing
# import-order dependency between two modules that otherwise share nothing.
# checkpoint.py satisfied it by accident of import order and build_pool.py only
# transitively, via `checkpoint as cg`. Decorating here makes it unconditional.
# tests/test_checkpoint.py's `monkeypatch.setattr(cg.base, "_load_1s", ...)`
# still works: it replaces the module attribute wholesale.
@lru_cache(maxsize=8)
def _load_1s(coin: str) -> pd.DataFrame:
    path = BINANCE_1S_DIR / f"{coin.lower()}_1s.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing 1s spot cache: {path}")
    df = pd.read_parquet(path, columns=["sec", "open", "close"])
    df = df.dropna(subset=["sec", "open", "close"]).copy()
    df["sec"] = df["sec"].astype(np.int64)
    return df.sort_values("sec").drop_duplicates("sec", keep="last").reset_index(drop=True)


def _spot_at_seconds(sec: np.ndarray, spot: np.ndarray, targets: np.ndarray) -> np.ndarray:
    """Latest spot at or before each target second."""
    pos = np.searchsorted(sec, targets, side="right") - 1
    out = np.full(len(targets), np.nan, dtype=np.float64)
    ok = pos >= 0
    if ok.any():
        out[ok] = spot[pos[ok]]
    return out


def _side_books_from_snapshot(snap: pd.Series) -> tuple[list[dict[str, float]], list[dict[str, float]]]:
    up: list[dict[str, float]] = []
    down: list[dict[str, float]] = []
    for price, size in zip(snap["ask_prices"], snap["ask_sizes"]):
        p = float(price)
        z = float(size)
        if math.isfinite(p) and math.isfinite(z) and 0.0 < p < 1.0 and z > 0:
            up.append({"price": p, "size": z})
    for bid, size in zip(snap["bid_prices"], snap["bid_sizes"]):
        p = 1.0 - float(bid)
        z = float(size)
        if math.isfinite(p) and math.isfinite(z) and 0.0 < p < 1.0 and z > 0:
            down.append({"price": p, "size": z})
    up.sort(key=lambda x: x["price"])
    down.sort(key=lambda x: x["price"])
    return up, down


def _top_side(prices, sizes, *, mirror_bid: bool) -> tuple[float, float]:
    best = math.nan
    depth = 0.0
    if prices is None or sizes is None:
        return float(best), float(depth)
    for raw_price, raw_size in zip(prices, sizes):
        if raw_price is None or raw_size is None:
            continue
        p = 1.0 - float(raw_price) if mirror_bid else float(raw_price)
        z = float(raw_size)
        if math.isfinite(p) and math.isfinite(z) and 0.0 < p < 1.0 and z > 0:
            depth += p * z
            if not math.isfinite(best) or p < best:
                best = p
    return float(best), float(depth)


@lru_cache(maxsize=None)
def _load_top_book_series(coin: str, interval: str, slug: str) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    path = etgt.PMDATA_SNAPSHOT_DIR / coin.lower() / interval.lower() / f"{slug}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["timestamp", "ask_prices", "ask_sizes", "bid_prices", "bid_sizes"])
    if df.empty:
        return None
    ts = (df["timestamp"].astype("datetime64[ns]").astype("int64") // 1_000_000_000).to_numpy(dtype=np.int64)
    order = np.argsort(ts, kind="stable")
    ts = ts[order]
    ask_prices = df["ask_prices"].to_numpy()[order]
    ask_sizes = df["ask_sizes"].to_numpy()[order]
    bid_prices = df["bid_prices"].to_numpy()[order]
    bid_sizes = df["bid_sizes"].to_numpy()[order]

    up_ask = np.full(len(ts), np.nan, dtype=np.float64)
    down_ask = np.full(len(ts), np.nan, dtype=np.float64)
    up_depth = np.full(len(ts), np.nan, dtype=np.float64)
    down_depth = np.full(len(ts), np.nan, dtype=np.float64)
    for i in range(len(ts)):
        up_ask[i], up_depth[i] = _top_side(ask_prices[i], ask_sizes[i], mirror_bid=False)
        down_ask[i], down_depth[i] = _top_side(bid_prices[i], bid_sizes[i], mirror_bid=True)
    return ts, up_ask, down_ask, up_depth, down_depth


def _extract_top_book_from_series(series, entry_ts: int) -> dict | None:
    if series is None:
        return None
    ts, up_ask, down_ask, up_depth, down_depth = series
    pos = int(np.searchsorted(ts, int(entry_ts), side="right") - 1)
    if pos < 0:
        return None
    ask = float(up_ask[pos])
    other_ask = float(down_ask[pos])
    up_d = float(up_depth[pos])
    down_d = float(down_depth[pos])
    if not (
        math.isfinite(ask)
        and math.isfinite(other_ask)
        and math.isfinite(up_d)
        and math.isfinite(down_d)
        and 0.0 < ask < 1.0
        and 0.0 < other_ask < 1.0
        and up_d > 0.0
        and down_d > 0.0
    ):
        return None
    return {
        "ask": ask,
        "other_ask": other_ask,
        "book_spread": ask + other_ask - 1.0,
        "ask_depth_top5_usd": up_d,
        "bid_depth_top5_usd": down_d,
        "up_depth": up_d,
        "down_depth": down_d,
    }


def _attach_raw_ladders(candidates: pd.DataFrame) -> pd.DataFrame:
    # ENTRY GUARD (2026-08-01). Every row here goes through
    # etgt._raw_book_payload -> etgt._load_raw_snapshot -> PMDATA_SNAPSHOT_DIR,
    # which returns None per market when the deleted poly_snapshot tree is
    # absent — so without this the function returns an EMPTY frame and the
    # caller records "no candidates" instead of "no substrate". Once, up front,
    # never inside the itertuples loop.
    etgt.require_snapshot_dir()
    if candidates.empty:
        return candidates.copy()
    recs = []
    for r in candidates.itertuples(index=False):
        payload = etgt._raw_book_payload(r)
        if payload is None:
            continue
        book, depth = payload
        prices = [a["price"] for a in book["asks"]]
        sizes = [a["size"] for a in book["asks"]]
        if not prices or not sizes or not math.isfinite(depth) or depth <= 0:
            continue
        d = r._asdict()
        d["ask_prices"] = prices
        d["ask_sizes"] = sizes
        d["book_depth"] = depth
        recs.append(d)
    return pd.DataFrame(recs)


def _passes_filter(df: pd.DataFrame, cfg: dict) -> pd.Series:
    mask = df["offset_sec"].astype(int).isin(cfg["offsets"])
    mask &= df["pred_edge"].astype(float) >= float(cfg["pred_edge_min"])
    mask &= df["range_strength_bps"].astype(float) >= float(cfg["min_range_bps"])
    mask &= df["breakout_strength_bps"].astype(float) >= float(cfg["breakout_bps"])
    mask &= df["entry_z_signed"].astype(float) >= float(cfg["min_entry_z"])
    return mask


def _flat_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"n": 0, "pnl": 0.0, "wr": None, "ev": None, "ex_top10_ev": None}
    ask = df["ask"].astype(float).to_numpy()
    win = df["y"].astype(int).to_numpy() == 1
    pnl = np.where(win, 1.0 - ask - FEE * ask * (1.0 - ask), -ask)
    keep = np.sort(pnl)[: max(0, len(pnl) - 10)]
    return {
        "n": int(len(df)),
        "pnl": float(pnl.sum()),
        "wr": float(win.mean()),
        "ev": float(pnl.mean()),
        "ex_top10_ev": float(keep.mean()) if len(keep) else 0.0,
    }


DECISION_KEY = ["market_slug", "side", "entry_minute"]


def _dedup_one_per_market(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse the second-offset replication to one bet per *decision*.

    A "decision" is (market, side, entry_minute) — the integer minute slice the
    live HGB/ORB strategy acts on. A config that admits several offsets
    (e.g. {30,35,40,50}) does NOT get to bet the same decision four times: in live
    trading you enter that minute once, at the first second whose gate fires.
    Keying on (market, side, entry_minute) and keeping the earliest passing second
    removes the replicated near-duplicate offset bets WITHOUT touching legitimate
    separate entry-minute decisions on the same market.

    Critically, this is a no-op for the offset-0 baseline (one second per decision),
    so sec0 stays identical to the parity-certified live baseline.
    """
    if df.empty:
        return df
    return (
        df.sort_values(DECISION_KEY + ["entry_ts"])
        .drop_duplicates(DECISION_KEY, keep="first")
        .reset_index(drop=True)
    )


TRAIN_FOLDS = ("A", "B", "C")
OOS_FOLD = "D"


def _fill(prices, sizes, depth, notional, model_prob, book_consumption_max):
    asks = [{"price": float(p), "size": float(s)} for p, s in zip(prices, sizes)]
    walked = walk_book_edge_preserving(
        {"asks": asks},
        float(notional),
        prob_safe_floor=float(model_prob),
        fee_theta=FEE,
        min_edge=-1.0,
        min_bet_usd=MIN_BET_USD,
        max_walk_price=float(live_config.MAX_WALK_PRICE),
    )
    if walked is None:
        return None
    filled_usd, fill = float(walked[0]), float(walked[2])
    if book_consumption_max is not None and filled_usd > float(depth) * float(book_consumption_max) + 1e-9:
        return None
    if not (0.0 < fill < 1.0 and filled_usd >= MIN_BET_USD):
        return None
    return filled_usd, fill
