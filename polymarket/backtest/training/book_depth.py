"""Book-depth lookup for VWAP-based slippage in backtests.

Loads a compact flat in-memory L2 ask index — built from the pmdata.dev
order-book snapshots by the since-deleted ``build_pmdata_book_index`` (dropped
as dead code in f195822: its raw inputs are permanently gone) — and walks it to
compute VWAP-based fill haircuts. Memory footprint: ~800 MB for ~2M
snapshots × ~25 levels avg, vs >8 GB for the naive dict-of-arrays layout
(which OOMs on pickle).

**The default index cache is GONE (deleted 2026-07-31).**
``BT_CACHE_DIR/book_depth_index_pmdata/`` went with the great consolidation,
and its builder had already gone in f195822 — there is no rebuild path. So
``load_book_depth_index()`` with no ``out_dir`` now always raises
``FileNotFoundError``; only an explicit ``out_dir`` naming a surviving prebuilt
index can succeed. The module stays because the walk itself is load-bearing:
``training/portfolio._load_book_idx_for`` still calls it (catching that
FileNotFoundError and degrading to ``_fallback_haircut``), and the linear-impact
fallback below is imported directly at ``portfolio.py:573``.

Layout:
    book_depth_index_pmdata/
        manifest.json         (version, n_snapshots, n_levels_total)
        prices.f32            (raw float32 array, ascending best-first)
        sizes.f32             (raw float32 array, parallel)
        cum_cost.f32          (parallel, cumulative price*size)
        cum_shares.f32        (parallel, cumulative size)
        index.pkl             (dict[(slug, side, ts)] -> (offset, n_levels, is_cf))

At runtime, the four .f32 files are np.memmap'd (OS pages them in on demand),
the index dict loads into RAM (~200 MB for ~2M entries).

Usage (``out_dir`` is now mandatory in practice — see above):
    from polymarket.backtest.training.book_depth import load_book_depth_index
    idx = load_book_depth_index(Path(prebuilt_index_dir))
    if idx.has(slug, side, ts):
        ...  # walk idx.prices / idx.cum_cost / idx.cum_shares yourself

The index's own ``vwap_haircut`` walker was removed 2026-08-01 at zero callers:
``training.portfolio._real_vwap`` superseded it (it returns the executable VWAP
rather than a haircut relative to the quote) and is the only walker left.
"""
from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np

from polymarket.backtest import BT_CACHE_DIR
INDEX_DIR = BT_CACHE_DIR / "book_depth_index_pmdata"


# --------------------------------------------------------------------------- runtime

@dataclass
class BookDepthIndex:
    """In-memory book-depth lookup. Built via build_pmdata_book_index once,
    then memory-mapped from disk by load_book_depth_index() at runtime."""
    index: dict             # (slug, side, ts) -> (offset, n_levels, is_cf)
    prices: np.ndarray      # flat, memmap'd or full array
    sizes: np.ndarray
    cum_cost: np.ndarray
    cum_shares: np.ndarray
    n_snapshots: int

    @classmethod
    def load(cls, out_dir: Path = INDEX_DIR, memmap: bool = True) -> "BookDepthIndex":
        manifest = json.loads((out_dir / "manifest.json").read_text())
        with open(out_dir / "index.pkl", "rb") as fp:
            index = pickle.load(fp)
        if memmap:
            prices = np.memmap(out_dir / "prices.f32", dtype=np.float32, mode="r")
            sizes  = np.memmap(out_dir / "sizes.f32",  dtype=np.float32, mode="r")
            cum_cost   = np.memmap(out_dir / "cum_cost.f32",   dtype=np.float32, mode="r")
            cum_shares = np.memmap(out_dir / "cum_shares.f32", dtype=np.float32, mode="r")
        else:
            prices = np.fromfile(out_dir / "prices.f32", dtype=np.float32)
            sizes  = np.fromfile(out_dir / "sizes.f32",  dtype=np.float32)
            cum_cost   = np.fromfile(out_dir / "cum_cost.f32",   dtype=np.float32)
            cum_shares = np.fromfile(out_dir / "cum_shares.f32", dtype=np.float32)
        return cls(index=index, prices=prices, sizes=sizes,
                   cum_cost=cum_cost, cum_shares=cum_shares,
                   n_snapshots=manifest["n_snapshots"])

    def has(self, slug: str, side: str, ts: int) -> bool:
        return (slug, side, int(ts)) in self.index


# --------------------------------------------------------------------------- fallback

def _fallback_haircut(
    fill_price: float,
    bet_face: float,
    *,
    half_spread: float,
    book_liquidity: float,
    no_impact_threshold: float,
    impact_coefficient: float,
) -> float:
    """Linear-impact fallback (same shape as simulate_portfolio._impact_haircut).
    Used when the L2 book is missing for a (slug, side, ts) tuple."""
    if book_liquidity <= 0:
        return half_spread
    consumed = min(1.0, float(bet_face) / float(book_liquidity))
    if consumed <= no_impact_threshold:
        return half_spread
    headroom = max(0.0, 1.0 - float(fill_price) - half_spread)
    excess = (consumed - no_impact_threshold) / max(1e-9, 1.0 - no_impact_threshold)
    return half_spread + excess * impact_coefficient * headroom


# --------------------------------------------------------------------------- cached loader

@lru_cache(maxsize=1)
def load_book_depth_index(out_dir: Path = INDEX_DIR) -> BookDepthIndex:
    """Cached loader so multiple Optuna trials in the same process share one
    memory-mapped index.

    The no-arg form ALWAYS raises: ``INDEX_DIR`` was deleted 2026-07-31. Fail
    LOUD rather than return an empty index — a silently absent book turns "the
    substrate is gone" into "every fill was free", which is the exact confusion
    this retirement is supposed to end. Callers allowed to degrade must catch
    ``FileNotFoundError`` themselves (``portfolio._load_book_idx_for`` does).
    """
    if not (out_dir / "manifest.json").exists():
        raise FileNotFoundError(
            f"Book-depth index not found at {out_dir}. The default cache "
            f"({INDEX_DIR.name}/) was DELETED 2026-07-31 in the great "
            "consolidation, and its builder (build_pmdata_book_index) had "
            "already gone in f195822 along with its raw pmdata.dev inputs — "
            "there is no rebuild path. Pass an explicit out_dir naming a "
            "surviving prebuilt index directory to use this lookup."
        )
    return BookDepthIndex.load(out_dir)
