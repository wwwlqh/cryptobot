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
HOLDOUT_FOLD = "E"  # PROTOCOL.md: touch exactly once, in 08_holdout_E only
EMBARGO_S = 3600

COIN_MAP = {"btc": 0, "eth": 1, "sol": 2, "xrp": 3}

# ``FEATURES_11`` is what LIVE actually scores with, on BOTH intervals. Verified
# against the deployed artifact: data/models/checkpoint_v1/checkpoint_v1_manifest.json
# carries feature_key == "11set" and an IDENTICAL 11-column feature_columns list
# for '5m' and '15m'. Keep this list in that order — it is the manifest's order.
FEATURES_11 = [
    "model_p_side", "rf_edge", "ask", "other_ask", "book_spread",
    "coin_id", "interval_min", "side_up", "entry_minute", "hour_utc", "dow_utc",
]
# DELETED 2026-08-03: ``FEATURES_5`` (exp044's 15m-sleeve convention) and
# ``FEATURES_ASK_ONLY``, with their ``score_wfo`` feature keys "5set" / "ask".
# exp044 was retired 2026-07-31; the subminute sleeves that replaced it score
# through checkpoint_v1 on 11set at BOTH intervals, so "the 15m feature set"
# described no deployed model and a caller selecting it would have silently
# produced a live-parity error. A literal caller census that day found zero
# references to either name anywhere in the repo.


# top-of-book analysis columns; excludes the fill-ladder lists, which multiply
# memory ~10x and are only needed for depth-walk sims (memory rule:
# feedback_wfo_memory_budget)
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
    """Load and concat pool part-files. E rows are hidden unless explicitly asked.

    Defaults to LIGHT_COLUMNS (no ladder lists); pass ladders=True only for
    depth-walk simulations. offset_mod=(m, r) keeps rows with offset_sec % m == r
    — memory-bounded subgrid loading for the 15M-row step5 pool (filtered per
    part-file before concat, per feedback_wfo_memory_budget).
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
    # cross-grid exact duplicates: step5's 5s offsets inside the last 30s are the
    # same decision seconds step1last covers at 1s — keep one row per
    # (market, decision second). step1last (listed later in `grids`) wins the tie
    # so the last-30s rows carry a single consistent grid label.
    if "offset_sec" in pool.columns:
        pool = pool.drop_duplicates(subset=["market_slug", "offset_sec"], keep="last").copy()
    if "qa_book_crossed" in pool.columns:
        pool = pool[~pool["qa_book_crossed"]].copy()
    # labels are re-merged from the canonical outcomes parquet at every load:
    # the 2026-07-07 Chainlink-primary rebuild changed 14 D-fold labels vs the
    # values baked in at pool-build time, and future rebuilds must flow through.
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
    # model sees the decision ask for its side (rename to exp044 conventions)
    out = out.rename(columns={"ask": "ask_up_raw"})
    out["ask"] = out["side_ask"]
    out["other_ask"] = np.where(out["side_up"] == 1, out["other_ask"], out["ask_up_raw"])
    return out


def fee(ask: np.ndarray) -> np.ndarray:
    return ev.POLY_FEE_THETA * ask * (1.0 - ask)


# ============================================================================
# CANONICAL EVALUATOR (frozen 2026-07-08) — every money-relevant number flows
# through these two functions. Filter order is FROZEN: band filters (interval/
# coins/ask/offset) -> reachable -> dedup (first offset per market) -> gate.
# Changing this order changes bet counts ~2x (measured); do not reorder.
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


# ============================================================================
# DELETED 2026-08-03 — the CANONICAL SIZING block (``SIZING_SPEC``,
# ``stake_weights``, ``DEPTH_CAPS_USD``, ``DEPTH_CAP_FIRST_BIND``), recorded
# 2026-07-08. A literal caller census found no code reference anywhere in the
# repo — only prose, including ``polymarket/src/cryptobot/config.py``'s own note that
# its knobs "originally mirrored the research SIZING_SPEC". LIVE sizing is
# owned by ``polymarket/live/config/v2_step1last_live.json`` (``capital_frac``
# 0.01 per sleeve, ``concurrent_exposure_cap_fraction``,
# ``max_drawdown_halt_frac``) and by ``polymarket/src/cryptobot/config.py`` — and the
# two had already diverged, so this block described a sizing policy the bot does
# not run (2.5%/1.25% here vs 1%/1% live, and its per-coin depth caps still
# list SOL/XRP, dropped from the universe 2026-08-02). Keeping it invited
# exactly the live-parity error the census exists to catch.
# Recover with ``git show <this commit>^:polymarket/backtest/lib/pool_study.py``.
# ============================================================================
