"""Shared model/eval helpers over the leak-fixed option-3 pools.

Used by notebooks 01+ so every notebook trains and scores identically.
Reuses the committed trainer (`ev._build_hgb_pipe`/`_fit_pipe`) and the
checkpoint RF-slice fit so results stay comparable with prior research.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

# repo root (legacy-path-ok: pre-import sys.path bootstrap)
_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
for p in (_REPO, os.path.join(_REPO, "core", "src")):
    if p not in sys.path:
        sys.path.insert(0, p)

from polymarket.backtest.training import modeling as ev  # noqa: E402

from polymarket.backtest import BT_RUNS_DIR  # noqa: E402

POOL_DIR = str(BT_RUNS_DIR / "option3_pool")

FOLD_ORDER = ("A", "B", "C", "D", "E")
HOLDOUT_FOLD = "E"  # final holdout: evaluate once, after everything is frozen
EMBARGO_S = 3600

COIN_MAP = {"btc": 0, "eth": 1, "sol": 2, "xrp": 3}

# Top-of-book analysis columns. The fill-ladder lists are left out: they use
# ~10x the memory and only depth-walk simulations need them.
LIGHT_COLUMNS = [
    "market_slug", "coin", "interval", "slug_ts", "entry_ts", "entry_dt",
    "checkpoint_sec", "offset_sec", "range_sec", "entry_minute", "fold", "won_up",
    "partial_ret_z_1h", "range_z", "range_ret", "entry_ret",
    "ask", "other_ask", "book_spread", "ask_depth_top5_usd", "bid_depth_top5_usd",
    "dec_book_age_s", "fill_ask", "fill_other_ask", "fill_book_age_s",
    "fill_book_changed", "fill_up_depth", "fill_down_depth",
    "reachable_up", "reachable_down", "qa_book_crossed",
]


def load_pool(grids=("step5", "step1last"), convention="option3", holdout_visible=False,
              columns: list[str] | None = None, ladders: bool = False,
              offset_mod: tuple[int, int] | None = None) -> pd.DataFrame:
    """Load and concatenate pool part files. Fold E rows are hidden unless
    asked for.

    Defaults to LIGHT_COLUMNS (no book ladders); pass ladders=True only for
    depth-walk simulations. offset_mod=(m, r) keeps rows with offset_sec % m ==
    r, filtered per part file before concatenation to bound memory on the large
    step5 pool.
    """
    if columns is None and not ladders:
        columns = LIGHT_COLUMNS
    import glob as _glob

    frames = []
    for g in grids:
        part_dir = os.path.join(POOL_DIR, f"pool_{g}_{convention}")
        paths = sorted(_glob.glob(os.path.join(part_dir, "part_*.parquet")))
        legacy = os.path.join(POOL_DIR, f"pool_{g}_{convention}.parquet")
        if not paths and os.path.exists(legacy):
            paths = [legacy]
        if not paths:
            raise FileNotFoundError(f"no pool parts under {part_dir}")
        for p in paths:
            df = pd.read_parquet(p, columns=columns)
            if offset_mod is not None:
                m, r = offset_mod
                df = df[df["offset_sec"].astype(int) % m == r]
            df["grid"] = g
            frames.append(df)
    pool = pd.concat(frames, ignore_index=True)
    # step5's 5-second offsets inside the last 30 seconds are the same decision
    # seconds step1last covers at 1s; keep one row per (market, decision
    # second), preferring step1last (listed later in ``grids``) so those rows
    # share one grid label.
    if "offset_sec" in pool.columns:
        pool = pool.drop_duplicates(subset=["market_slug", "offset_sec"], keep="last").copy()
    if "qa_book_crossed" in pool.columns:
        pool = pool[~pool["qa_book_crossed"]].copy()
    # Labels are re-merged from the canonical outcomes parquet on every load,
    # so label corrections made after the pool was built always apply.
    from polymarket.backtest.lib import pmdata_clean as pc

    fresh = pc.load_outcomes(pc.OUTCOMES_PATH, ok_only=True)
    fresh_map = fresh.set_index("market_slug")["won_up"]
    pool["won_up"] = pool["market_slug"].map(fresh_map)
    pool = pool.dropna(subset=["won_up"]).copy()
    pool["won_up"] = pool["won_up"].astype(np.int8)
    if not holdout_visible:
        pool = pool[pool["fold"] != HOLDOUT_FOLD].copy()
    return pool


def side_frame(pool: pd.DataFrame) -> pd.DataFrame:
    """One row per (candidate, side) with side-specific features/labels/fills.

    Model features use the DECISION book; EV uses the FILL book (t+1s).
    """
    rows = []
    for side_up in (1, 0):
        d = pool.copy()
        d["side_up"] = side_up
        d["side_ask"] = d["ask"] if side_up else d["other_ask"]
        d["side_fill_ask"] = d["fill_ask"] if side_up else d["fill_other_ask"]
        d["reachable"] = d["reachable_up"] if side_up else d["reachable_down"]
        d["partial_ret_z_side"] = d["partial_ret_z_1h"] * (1 if side_up else -1)
        d["win"] = (d["won_up"] == side_up).astype(np.int8)
        rows.append(d)
    out = pd.concat(rows, ignore_index=True)
    out["abs_partial_ret_z"] = out["partial_ret_z_1h"].abs()
    out["coin_id"] = out["coin"].map(COIN_MAP).astype(float)
    out["interval_min"] = out["interval"].map({"5m": 5, "15m": 15}).astype(float)
    dt = pd.to_datetime(out["entry_ts"], unit="s", utc=True)
    out["hour_utc"] = dt.dt.hour.astype(float)
    out["dow_utc"] = dt.dt.dayofweek.astype(float)
    # The model sees its own side's decision ask.
    out = out.rename(columns={"ask": "ask_up_raw"})
    out["ask"] = out["side_ask"]
    out["other_ask"] = np.where(out["side_up"] == 1, out["other_ask"], out["ask_up_raw"])
    return out


def fee(ask: np.ndarray) -> np.ndarray:
    return ev.POLY_FEE_THETA * ask * (1.0 - ask)


# ============================================================================
# Canonical bet spec. Filter order: band filters (interval, coins, ask, offset)
# -> reachable -> dedup (first offset per market) -> gate. Changing the order
# roughly halves or doubles bet counts; do not reorder.
# ============================================================================

BET_SPEC_DEFAULT = {
    "intervals": ("15m",),
    "coins": ("btc", "eth", "sol", "xrp"),
    "ask_lo": 0.5, "ask_hi": 0.85,
    "off_lo": 0, "off_hi": 29,          # seconds into the last-30s window
    "reachable_only": True,
    "gate": 0.03,
    "orb_min": 0.0,                      # |range_z| >= orb_min (0 = no ORB gate)
    "z_min": 0.0,                        # |partial_ret_z_side| >= z_min (0 = off)
}


