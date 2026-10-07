"""Compound-bankroll joint 5m+15m optimizer with chunk ABCD walk-forward CV.

v2 — searches every per-sleeve Optuna param plus 9 portfolio knobs:
  alloc_total, alloc_5m_share, exposure_cap_frac, idempotency_mode (5-way),
  cap_growth_mode (fixed|peak_growth), min_bet_usd, book_consumption_max,
  fallback_liq_{btc,eth,sol,xrp}.

Key realism patches vs v1:
  1. **Compound sizing.** face = running_equity * alloc * capital_frac.
  2. **Joint search.** One trial proposes BOTH a 5m config AND a 15m config.
  3. **Per-minute idempotency.** Tunable mode for collisions across sleeves.
  4. **Real L2 VWAP.** Per-coin via the slug; per-coin fallback liquidity.
  5. **Book-consumption cap.** face hard-capped at frac of available L2 depth.
  6. **Cap growth.** Exposure cap can scale with peak equity (peak_growth mode).
  7. **Honest DD.** Reports both global max_dd AND worst 30-bet window DD.
  8. **Hard gates.** Prune on max_dd > 0.30, worst_30bet > 0.30, n_bets < 30,
     total_pnl <= 0, or any fold final_equity > $5M (sim-artifact guard).
  9. **Walk-forward chunks ABCD.** Train=[A..test_start), test=fold window;
     LUT rebuilt per-fold from training rows only.

Output: backtest/optimize_runs/compound_5m15m_dd030/{run_id}/optimize_proposed.json

CLI:
    python -m polymarket.backtest.training.portfolio --n-trials 2000 --run-id main_v2
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass, replace
from functools import lru_cache
from typing import Literal

import numpy as np
import optuna
import pandas as pd
from optuna.trial import TrialState

from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA, cash_pnl, lut_p_bucket_indices
from polymarket.backtest.training.book_depth import load_book_depth_index


def _real_vwap(book_idx, slug: str, side: str, ts: int,
               face: float, fallback_liq: float) -> tuple[float, float]:
    """Return (VWAP, total_book_cost) walking the real L2 ask book.

    Replaces book_depth.vwap_haircut for cases where we need the actual VWAP
    price (not the delta from best_ask). VWAP is what the bot would *pay*
    per share — the correct number to feed into effective_fill.

    If the book snapshot is missing, returns (NaN, fallback_liq) — caller
    must handle the missing case.
    """
    entry = book_idx.index.get((slug, side, int(ts)))
    if entry is None:
        return float("nan"), fallback_liq
    offset, n_lvls, is_cf = entry
    if is_cf:
        # Carryforward (stale-copied) book — not a real executable snapshot.
        # Refuse to price against it; caller treats NaN as "no fresh book".
        return float("nan"), fallback_liq
    prices_s = book_idx.prices[offset:offset + n_lvls]
    cum_cost_s = book_idx.cum_cost[offset:offset + n_lvls]
    cum_shares_s = book_idx.cum_shares[offset:offset + n_lvls]
    total_book_cost = float(cum_cost_s[-1])

    bet = float(face)
    if bet <= 0:
        return float(prices_s[0]), total_book_cost
    if bet >= total_book_cost:
        total_shares = float(cum_shares_s[-1])
        return total_book_cost / max(total_shares, 1e-9), total_book_cost

    i = int(np.searchsorted(cum_cost_s, bet, side="right"))
    prev_cost = float(cum_cost_s[i - 1]) if i > 0 else 0.0
    prev_shares = float(cum_shares_s[i - 1]) if i > 0 else 0.0
    partial_price = float(prices_s[i])
    partial_shares = (bet - prev_cost) / partial_price
    total_shares = prev_shares + partial_shares
    if total_shares <= 0:
        return float(prices_s[0]), total_book_cost
    return bet / total_shares, total_book_cost

from polymarket.backtest import ARTIFACTS_DIR, BT_RUNS_DIR
# GONE from disk since 2026-07-31 (exp044 phase 3), with no rebuild path: the
# decision_table left the live boot path with the pair sleeves in phase 2 and its
# whole producer chain (build/, features/build.py, prices.db, the binance-kline
# fetchers) was deleted in phase 3. The constant stays so ``_load_full_table``
# can name the file it wants in its failure; it is never written.
DECISION_TABLE = ARTIFACTS_DIR / "decision_table.parquet"
RUNS_DIR = BT_RUNS_DIR / "compound_5m15m_dd030"

CHUNK_A_START = pd.Timestamp("2026-02-09", tz="UTC")
CHUNK_B_START = pd.Timestamp("2026-03-16", tz="UTC")
CHUNK_C_START = pd.Timestamp("2026-04-06", tz="UTC")
CHUNK_D_START = pd.Timestamp("2026-05-01", tz="UTC")
DATA_END = pd.Timestamp("2026-05-19 06:00", tz="UTC")
HOLDOUT_START = pd.Timestamp("2026-05-13", tz="UTC")

WALK_FORWARD_FOLDS = (
    ("fold1_B", CHUNK_A_START, CHUNK_B_START, CHUNK_B_START, CHUNK_C_START),
    ("fold2_C", CHUNK_A_START, CHUNK_C_START, CHUNK_C_START, CHUNK_D_START),
    ("fold3_D", CHUNK_A_START, CHUNK_D_START, CHUNK_D_START, HOLDOUT_START),
)
HOLDOUT_FOLD = ("holdout_E", CHUNK_A_START, HOLDOUT_START, HOLDOUT_START, DATA_END)

COIN_UNIVERSE = ("btc", "eth", "sol", "xrp")
INITIAL_CAPITAL = 100.0


@dataclass
class SleeveCfg:
    interval: Literal["5m", "15m"]
    side: Literal["Up", "Down", "model_signed"]
    entry_minute_set: tuple[int, ...]
    entry_strategy: Literal["single", "first_qualifying", "every_qualifying"]
    min_edge: float
    max_edge: float
    min_fill_price: float
    max_fill_price: float
    dow_set: Literal["all", "weekday", "weekend"]
    min_agreement: int
    capital_frac: float
    max_walk_price: float | None
    slippage_haircut: float
    min_lut_n: int
    signal_source: Literal["rf", "lut", "blend"]
    partial_ret_z_floor: float | None
    partial_ret_z_ceiling: float | None
    fallback_book_liquidity: float
    impact_coefficient: float
    no_impact_threshold: float


@dataclass
class JointCfg:
    """Two ISOLATED wallets — one per sleeve. Each starts at $100, compounds
    independently, hits its own DD halt independently. The two wallets share
    NO bankroll, NO exposure cap, NO dedup. They run in parallel as if they
    were two separate deployments on the same coins.

    Shared (across both wallets):
      - cap_growth_mode (architectural choice)
      - book_consumption_max (market physics)
      - fallback_liq_per_coin (per-coin book characteristic)
      - dd_halt_frac (user constraint)

    Per-wallet (independently sized):
      - sleeve_cfg (all gates)
      - exposure_cap_frac
      - min_bet_usd
    """
    cfg_5m: SleeveCfg
    cfg_15m: SleeveCfg
    exposure_cap_5m: float
    exposure_cap_15m: float
    min_bet_usd_5m: float
    min_bet_usd_15m: float
    dd_halt_frac: float
    cap_growth_mode: str = "fixed"
    fallback_liq_btc: float = 10_000.0
    fallback_liq_eth: float = 10_000.0
    fallback_liq_sol: float = 10_000.0
    fallback_liq_xrp: float = 10_000.0
    book_consumption_max: float = 0.30


@dataclass
class FoldMetrics:
    name: str
    n_bets: int = 0
    n_bets_5m: int = 0
    n_bets_15m: int = 0
    days: int = 0
    final_equity: float = INITIAL_CAPITAL
    total_pnl: float = 0.0
    win_rate: float = float("nan")
    max_dd_pct_peak: float = 0.0
    max_dd_dollars: float = 0.0
    dd_halted: bool = False
    sortino: float = 0.0
    worst_30bet_dd_pct: float = 0.0
    worst_100bet_dd_pct: float = 0.0
    final_exposure_cap: float = 0.0


@lru_cache(maxsize=1)
def _load_full_table() -> pd.DataFrame:
    # Fail LOUD, not empty. Every downstream (`modeling._prepare_base_frame`,
    # `_evaluate_fold`, the residual matrix/replay CLIs) treats this frame as the
    # research universe; handing them a silently empty one would turn "the
    # substrate is gone" into "the strategy found no bets", which is the exact
    # confusion this retirement is supposed to end. Callers that are allowed to
    # degrade must check `DECISION_TABLE.exists()` themselves and skip.
    if not DECISION_TABLE.exists():
        raise FileNotFoundError(
            f"decision_table.parquet is gone: {DECISION_TABLE}. Deleted 2026-07-31 "
            "(exp044 phase 3) together with its producer chain — there is no "
            "rebuild path. Every entry point below this loader is retired with it."
        )
    df = pd.read_parquet(DECISION_TABLE)
    df = df[df["coin"].str.lower().isin(set(COIN_UNIVERSE))].copy()
    df["entry_dt"] = pd.to_datetime(df["entry_ts"], unit="s", utc=True)
    df["slug_dt"] = pd.to_datetime(df["slug_ts"], unit="s", utc=True)
    df["entry_date"] = df["entry_dt"].dt.floor("D")
    df["dow_et"] = df["entry_dt"].dt.tz_convert("US/Eastern").dt.dayofweek
    df = df.dropna(subset=["yes_price", "no_price", "model_p_v1", "won_up_polymarket"])
    df["won_up_polymarket"] = df["won_up_polymarket"].astype(np.int8)
    df = _attach_l2_asks(df)
    return df


@lru_cache(maxsize=4)
def _load_book_idx_for(index_dir: str | None):
    """Load the book-depth index at `index_dir` (str path), or the default DB
    index when None. Cache key IS the dir, so a process can hold more than one
    index without cross-contamination (relevant to A/B book-source comparisons).
    """
    try:
        if index_dir:
            from pathlib import Path
            return load_book_depth_index(Path(index_dir))
        return load_book_depth_index()
    except FileNotFoundError:
        return None


def _load_book_idx():
    """Resolve the book index to use. Honors CRYPTOBOT_BOOK_INDEX_DIR (an
    experiment override pointing at e.g. book_depth_index_pmdata); falls back to
    the default DB-built index. The resolved dir is logged once per dir so a
    mis-set env var surfaces instead of silently scoring the wrong book.

    NOTE: the env var must be set BEFORE the first call (the full-table frame is
    @lru_cache'd with the book asks baked in). Run A/B arms as separate processes.
    """
    d = os.environ.get("CRYPTOBOT_BOOK_INDEX_DIR")
    idx = _load_book_idx_for(d or None)
    _log_book_idx_dir_once(d)
    return idx


_LOGGED_BOOK_DIRS: set = set()


def _log_book_idx_dir_once(d: str | None) -> None:
    key = d or "<default book_depth_index>"
    if key not in _LOGGED_BOOK_DIRS:
        _LOGGED_BOOK_DIRS.add(key)
        print(f"[book-index] using {key}", file=sys.stderr, flush=True)


def _attach_l2_asks(df: pd.DataFrame) -> pd.DataFrame:
    """Attach the REAL L2 best-ask per side from the book index.

    up_ask_l2 = best ask to BUY the Up token   (book key (slug, "Up", entry_ts))
    no_ask_l2 = best ask to BUY the Down token  (book key (slug, "Down", entry_ts))
    up_depth_l2 / no_depth_l2 = total $ cost of the whole ask book (for the
        bet-placement cap — how much the book can physically absorb).

    These are the EXECUTABLE prices. The legacy decision_table yes_price /
    no_price are stale/mid quotes that diverge from the book by ~40c, so the
    strategy must gate + size on the L2 asks instead.

    CARRYFORWARD SNAPSHOTS ARE SKIPPED. ~11% of book entries are "carryforward"
    — stale copies of an earlier snapshot (usually the market-open ~0.50 book)
    re-stamped at later timestamps. Using them is a leakage trap: the optimizer
    learns to "buy" at the stale 0.50 price while the model already knows the
    near-decided outcome, producing a fake 99% win rate. Only fresh (is_cf=0)
    snapshots represent the real executable book at entry_ts. Rows whose book
    is carryforward or missing get NaN and are dropped at pick time.

    Computed once per process (the full table is lru_cached).
    """
    book = _load_book_idx()
    n = len(df)
    up_ask = np.full(n, np.nan, dtype=np.float64)
    no_ask = np.full(n, np.nan, dtype=np.float64)
    up_depth = np.full(n, np.nan, dtype=np.float64)
    no_depth = np.full(n, np.nan, dtype=np.float64)
    n_cf_skipped = 0
    if book is not None:
        slugs = df["market_slug"].to_numpy()
        ts = df["entry_ts"].to_numpy()
        prices = book.prices
        cum_cost = book.cum_cost
        index = book.index
        for i in range(n):
            s = slugs[i]; t = int(ts[i])
            eu = index.get((s, "Up", t))
            if eu is not None and not eu[2]:        # not carryforward
                off, nl, _ = eu
                up_ask[i] = prices[off]
                up_depth[i] = cum_cost[off + nl - 1]
            elif eu is not None:
                n_cf_skipped += 1
            ed = index.get((s, "Down", t))
            if ed is not None and not ed[2]:        # not carryforward
                off, nl, _ = ed
                no_ask[i] = prices[off]
                no_depth[i] = cum_cost[off + nl - 1]
            elif ed is not None:
                n_cf_skipped += 1
    df["up_ask_l2"] = up_ask
    df["no_ask_l2"] = no_ask
    df["up_depth_l2"] = up_depth
    df["no_depth_l2"] = no_depth
    fresh = int(np.isfinite(up_ask).sum() + np.isfinite(no_ask).sum())
    print(f"[_attach_l2_asks] fresh L2 asks={fresh:,}  carryforward-skipped={n_cf_skipped:,}",
          file=sys.stderr)
    return df


_LUT_ROLLING_WINDOW_DAYS = 30  # was build_lut.py's LUT_ROLLING_WINDOW_DAYS


def _rebuild_lut(train_df: pd.DataFrame, label_col: str,
                train_end: pd.Timestamp | None = None) -> pd.DataFrame:
    """Build a per-cell hit-rate LUT from the last 30 days of train_df.

    Restricts to the 30-day window ending at train_end (exclusive) to match
    the rolling window the retired build_lut.py and live LUT lookup used. Using
    the full training window (which can span 80+ days) produces systematically
    higher lut_hit_rate values than the live runtime will see, biasing the
    optimizer toward over-confident signal_source=lut/blend configs.
    """
    if train_end is not None:
        lookback_start = train_end - pd.Timedelta(days=_LUT_ROLLING_WINDOW_DAYS)
        train_df = train_df[train_df["entry_dt"] >= lookback_start]
    train = train_df.dropna(subset=[label_col]).copy()
    train["lut_p_bucket"] = lut_p_bucket_indices(train["model_p_v1"].to_numpy())
    return (
        train.groupby(["coin", "interval", "entry_minute", "lut_p_bucket"], observed=True)
        .agg(lut_n_cum=(label_col, "size"), lut_hits_cum=(label_col, "sum"))
        .reset_index()
        .assign(lut_hit_rate=lambda d: d["lut_hits_cum"].astype(np.float64)
                / d["lut_n_cum"].clip(lower=1))
    )


def _attach_lut(test_df: pd.DataFrame, lut: pd.DataFrame) -> pd.DataFrame:
    test = test_df.copy()
    test["lut_p_bucket"] = lut_p_bucket_indices(test["model_p_v1"].to_numpy())
    test = test.drop(columns=["lut_n_cum", "lut_hit_rate", "lut_hits_cum"], errors="ignore")
    test = test.merge(
        lut[["coin", "interval", "entry_minute", "lut_p_bucket", "lut_n_cum", "lut_hit_rate"]],
        on=["coin", "interval", "entry_minute", "lut_p_bucket"], how="left",
    )
    test["lut_n_cum"] = test["lut_n_cum"].fillna(0).astype(np.int32)
    return test


def _signal_p(df: pd.DataFrame, sleeve: SleeveCfg) -> np.ndarray:
    rf_p = df["model_p_v1"].to_numpy(dtype=np.float64)
    if sleeve.signal_source == "rf":
        return rf_p
    lut_p = df["lut_hit_rate"].to_numpy(dtype=np.float64)
    if sleeve.signal_source == "lut":
        return lut_p
    return 0.5 * rf_p + 0.5 * lut_p


def _pick_bets_for_sleeve(df: pd.DataFrame, sleeve: SleeveCfg, label_col: str) -> pd.DataFrame:
    sub = df[(df["interval"] == sleeve.interval)
             & (df["entry_minute"].isin(sleeve.entry_minute_set))].copy()
    if sleeve.dow_set == "weekday":
        sub = sub[sub["dow_et"] < 5]
    elif sleeve.dow_set == "weekend":
        sub = sub[sub["dow_et"] >= 5]
    if sleeve.min_lut_n > 0:
        sub = sub[sub["lut_n_cum"].fillna(0) >= sleeve.min_lut_n]
    if sleeve.signal_source in ("lut", "blend"):
        sub = sub[sub["lut_hit_rate"].notna()]
    if sleeve.partial_ret_z_floor is not None or sleeve.partial_ret_z_ceiling is not None:
        z = sub["partial_ret_z_1h"]
        sub = sub[z.notna()]
        if sleeve.partial_ret_z_floor is not None:
            sub = sub[sub["partial_ret_z_1h"] >= sleeve.partial_ret_z_floor]
        if sleeve.partial_ret_z_ceiling is not None:
            sub = sub[sub["partial_ret_z_1h"] <= sleeve.partial_ret_z_ceiling]
    if sub.empty:
        return sub.iloc[0:0]

    p = _signal_p(sub, sleeve)
    # EXECUTABLE prices: the real L2 best-ask per side (not the stale
    # decision_table quote). edge is computed against what you'd actually pay
    # at the top of the book. Rows with no L2 snapshot have NaN ask and are
    # dropped below — you cannot place a bet where there is no book.
    up_ask = sub["up_ask_l2"].to_numpy(dtype=np.float64)
    no_ask = sub["no_ask_l2"].to_numpy(dtype=np.float64)
    up_depth = sub["up_depth_l2"].to_numpy(dtype=np.float64)
    no_depth = sub["no_depth_l2"].to_numpy(dtype=np.float64)
    edge_up = p - up_ask - POLY_FEE_THETA * up_ask * (1.0 - up_ask) - sleeve.slippage_haircut
    edge_dn = (1.0 - p) - no_ask - POLY_FEE_THETA * no_ask * (1.0 - no_ask) - sleeve.slippage_haircut

    if sleeve.side == "Up":
        side = np.full(len(sub), "Up", dtype=object); edge = edge_up
        fill = up_ask; depth = up_depth
    elif sleeve.side == "Down":
        side = np.full(len(sub), "Down", dtype=object); edge = edge_dn
        fill = no_ask; depth = no_depth
    else:
        # model_signed: pick the better-edge side, but only among sides that
        # actually have a book (NaN edge loses the comparison).
        eu = np.where(np.isfinite(edge_up), edge_up, -np.inf)
        ed = np.where(np.isfinite(edge_dn), edge_dn, -np.inf)
        side = np.where(eu >= ed, "Up", "Down")
        edge = np.maximum(eu, ed)
        fill = np.where(side == "Up", up_ask, no_ask)
        depth = np.where(side == "Up", up_depth, no_depth)

    keep = ((edge >= sleeve.min_edge) & (edge <= sleeve.max_edge)
            & (fill >= sleeve.min_fill_price) & (fill <= sleeve.max_fill_price)
            & np.isfinite(edge) & np.isfinite(fill))
    if sleeve.max_walk_price is not None:
        anchor = sub["yes_pre1"].to_numpy(dtype=np.float64)
        anchor = np.where(side == "Down", 1.0 - anchor, anchor)
        keep &= np.isfinite(anchor) & ((fill - anchor) <= sleeve.max_walk_price)
    if not keep.any():
        return sub.iloc[0:0]

    out = sub.loc[keep, ["market_slug", "slug_ts", "entry_ts", "entry_dt", "entry_date",
                        "resolve_ts", "coin", label_col]].copy()
    out = out.rename(columns={label_col: "won_up"})
    out["side"] = side[keep]
    out["edge"] = edge[keep]
    out["fill_price"] = fill[keep]          # L2 best-ask (executable top of book)
    out["book_depth"] = depth[keep]          # total L2 ask depth $ (placement cap)
    out["model_p"] = p[keep]
    out["sleeve_interval"] = sleeve.interval

    if sleeve.min_agreement > 0:
        agreed = out.groupby(["slug_ts", "entry_ts", "side"])["coin"].transform("nunique")
        out = out[(agreed - 1) >= sleeve.min_agreement]

    if sleeve.entry_strategy == "single":
        if len(sleeve.entry_minute_set) > 1:
            return sub.iloc[0:0]
    elif sleeve.entry_strategy == "first_qualifying":
        out = out.sort_values("entry_ts").drop_duplicates(
            subset=["slug_ts", "coin", "side"], keep="first")
    return out


def _simulate_isolated_wallet(picks: pd.DataFrame, sleeve_cfg: SleeveCfg,
                              exposure_cap_frac: float, cap_growth_mode: str,
                              min_bet_usd: float, book_consumption_max: float,
                              coin_fb_liq: dict[str, float],
                              dd_halt_frac: float,
                              capital: float = INITIAL_CAPITAL,
                              ) -> tuple[FoldMetrics, np.ndarray]:
    """Run ONE sleeve on its own isolated $100 wallet. No alloc multiplier
    (that's implicit 1.0 — this wallet has no sibling). Bet face =
    equity * sleeve_cfg.capital_frac. Exposure cap and DD halt apply only
    to this wallet's own equity/peak.
    """
    fm = FoldMetrics(name="")
    if picks.empty:
        return fm, np.zeros(0, dtype=np.float64)

    fm.n_bets_5m = int((picks["sleeve_interval"] == "5m").sum())
    fm.n_bets_15m = int((picks["sleeve_interval"] == "15m").sum())

    p = picks.sort_values(["entry_ts", "coin", "side"]).reset_index(drop=True)
    book_idx = _load_book_idx()
    equity = capital; peak = capital; halted = False
    active: list[tuple[int, float, float]] = []
    kept_pls: list[float] = []
    kept_idx: list[int] = []
    kept_face: list[float] = []
    kept_equity_after: list[float] = []

    cap_frac = sleeve_cfg.capital_frac
    haircut_base = sleeve_cfg.slippage_haircut
    imp_coef = sleeve_cfg.impact_coefficient
    no_imp_thr = sleeve_cfg.no_impact_threshold
    base_exposure_cap = capital * exposure_cap_frac
    cap_dollars = base_exposure_cap

    for i, row in enumerate(p.itertuples(index=False)):
        entry_ts = int(row.entry_ts)
        still: list[tuple[int, float, float]] = []
        for resolve_ts, face, pnl in active:
            if resolve_ts <= entry_ts:
                equity += pnl
            else:
                still.append((resolve_ts, face, pnl))
        active = still
        peak = max(peak, equity)
        dd_ratio = (peak - equity) / peak if peak > 0 else 0.0
        if dd_ratio >= dd_halt_frac:
            halted = True; continue
        if equity <= 0:
            halted = True; continue

        face = equity * cap_frac  # no alloc — isolated wallet at full size
        if face < min_bet_usd:
            continue
        slug = str(row.market_slug); side = str(row.side)
        coin = str(row.coin).lower()
        fb_liq = coin_fb_liq.get(coin, 10_000.0)

        # Available depth = the fresh L2 book depth precomputed at pick time
        # (carryforward already excluded — book_depth is NaN for those and the
        # row was dropped). Falls back to per-coin liquidity only if absent.
        avail = float(getattr(row, "book_depth", np.nan))
        if not math.isfinite(avail) or avail <= 0:
            avail = fb_liq
        face_cap_by_book = avail * book_consumption_max
        if face_cap_by_book < min_bet_usd:
            continue
        if face > face_cap_by_book:
            face = face_cap_by_book

        if cap_growth_mode == "peak_growth":
            cap_dollars = max(base_exposure_cap, peak * exposure_cap_frac)
        else:
            cap_dollars = base_exposure_cap
        pending = sum(f for _, f, _ in active)
        if pending + face > cap_dollars:
            continue

        quoted_fill = float(row.fill_price)
        if book_idx is not None and book_idx.has(slug, side, entry_ts):
            real_vwap, _ = _real_vwap(book_idx, slug, side, entry_ts, face, fb_liq)
            if math.isfinite(real_vwap):
                effective_fill = max(real_vwap, quoted_fill)
            else:
                effective_fill = quoted_fill + haircut_base
        else:
            from polymarket.backtest.training.book_depth import _fallback_haircut
            fb_hc = _fallback_haircut(
                quoted_fill, float(face), half_spread=haircut_base,
                book_liquidity=fb_liq, no_impact_threshold=no_imp_thr,
                impact_coefficient=imp_coef,
            )
            effective_fill = quoted_fill + fb_hc
        haircut = max(0.0, effective_fill - quoted_fill)

        pl = cash_pnl(side, quoted_fill, int(row.won_up), face, haircut)
        if not math.isfinite(pl):
            continue
        kept_pls.append(pl); kept_face.append(face)
        kept_idx.append(i)
        active.append((int(row.resolve_ts), face, pl))
        kept_equity_after.append(equity)

    for resolve_ts, face, pnl in active:
        equity += pnl
    peak = max(peak, equity)

    if not kept_pls:
        return fm, np.zeros(0, dtype=np.float64)

    pls_arr = np.array(kept_pls, dtype=np.float64)
    kept = p.iloc[kept_idx].reset_index(drop=True).copy()
    kept["pl"] = pls_arr; kept["face"] = kept_face
    kept["equity_at_entry"] = kept_equity_after

    order = np.argsort(kept["resolve_ts"].to_numpy(), kind="mergesort")
    pls_ordered = pls_arr[order]
    curve = capital + np.cumsum(pls_ordered)
    equity_curve = np.concatenate(([capital], curve))
    peak_curve = np.maximum.accumulate(equity_curve)
    dd_curve = peak_curve - equity_curve
    max_dd_dollars = float(dd_curve.max())
    peak_at_max = float(peak_curve[int(dd_curve.argmax())])
    max_dd_pct = max_dd_dollars / peak_at_max if peak_at_max > 0 else 0.0

    fm.n_bets = len(kept)
    fm.final_equity = float(equity_curve[-1])
    fm.total_pnl = float(pls_arr.sum())
    fm.win_rate = float((pls_arr > 0).mean())
    fm.max_dd_pct_peak = max_dd_pct
    fm.max_dd_dollars = max_dd_dollars
    fm.dd_halted = halted
    fm.days = int(pd.to_datetime(kept["entry_ts"], unit="s", utc=True).dt.floor("D").nunique())

    kept["day"] = pd.to_datetime(kept["entry_ts"], unit="s", utc=True).dt.floor("D")
    day_pnl_sum = kept.groupby("day")["pl"].sum()

    if len(pls_ordered) >= 30:
        roll_peak = pd.Series(equity_curve[1:]).rolling(30, min_periods=1).max()
        roll_drop = roll_peak.values - equity_curve[1:]
        fm.worst_30bet_dd_pct = float((roll_drop / roll_peak.values.clip(min=1.0)).max())
    if len(pls_ordered) >= 100:
        roll_peak = pd.Series(equity_curve[1:]).rolling(100, min_periods=1).max()
        roll_drop = roll_peak.values - equity_curve[1:]
        fm.worst_100bet_dd_pct = float((roll_drop / roll_peak.values.clip(min=1.0)).max())
    fm.final_exposure_cap = float(cap_dollars)

    if len(day_pnl_sum) >= 2:
        downside = day_pnl_sum[day_pnl_sum < 0]
        sd_down = max(float(downside.std()) if len(downside) > 1 else 0.0, 1.0)
        fm.sortino = float(day_pnl_sum.mean() / sd_down)

    return fm, day_pnl_sum.to_numpy() if len(day_pnl_sum) else np.zeros(0)


def _evaluate_fold(name: str, train_start: pd.Timestamp, train_end: pd.Timestamp,
                   test_start: pd.Timestamp, test_end: pd.Timestamp,
                   joint: JointCfg, label_col: str,
                   only_sleeve: str | None = None) -> dict:
    """Evaluate the active wallet(s) independently on this fold.

    Each wallet starts at $100, compounds on its own bankroll, hits its own
    DD halt. ``only_sleeve`` restricts evaluation to "5m" or "15m" (the other
    sleeve returns an empty FoldMetrics and isn't picked/simulated — saves
    compute when optimizing one wallet at a time).
    """
    df = _load_full_table()
    train_df = df[(df["entry_dt"] >= train_start) & (df["entry_dt"] < train_end)]
    test_df = df[(df["entry_dt"] >= test_start) & (df["entry_dt"] < test_end)].copy()
    empty = {"name": name, "fm_5m": FoldMetrics(name=f"{name}_5m"),
             "fm_15m": FoldMetrics(name=f"{name}_15m")}
    if test_df.empty:
        return empty

    do_5m = only_sleeve in (None, "5m")
    do_15m = only_sleeve in (None, "15m")

    needs_lut = ((do_5m and (joint.cfg_5m.signal_source in ("lut", "blend") or joint.cfg_5m.min_lut_n > 0))
                 or (do_15m and (joint.cfg_15m.signal_source in ("lut", "blend") or joint.cfg_15m.min_lut_n > 0)))
    if needs_lut:
        lut = _rebuild_lut(train_df, label_col, train_end=train_end)
        test_df = _attach_lut(test_df, lut)
    else:
        test_df["lut_n_cum"] = 0
        test_df["lut_hit_rate"] = np.nan
        test_df["lut_p_bucket"] = 0

    coin_fb_liq = {"btc": joint.fallback_liq_btc, "eth": joint.fallback_liq_eth,
                   "sol": joint.fallback_liq_sol, "xrp": joint.fallback_liq_xrp}

    fm_5m = FoldMetrics(name=f"{name}_5m")
    fm_15m = FoldMetrics(name=f"{name}_15m")
    if do_5m:
        picks_5m = _pick_bets_for_sleeve(test_df, joint.cfg_5m, label_col)
        fm_5m, _ = _simulate_isolated_wallet(
            picks_5m, joint.cfg_5m,
            exposure_cap_frac=joint.exposure_cap_5m,
            cap_growth_mode=joint.cap_growth_mode,
            min_bet_usd=joint.min_bet_usd_5m,
            book_consumption_max=joint.book_consumption_max,
            coin_fb_liq=coin_fb_liq, dd_halt_frac=joint.dd_halt_frac,
        )
        fm_5m.name = f"{name}_5m"
    if do_15m:
        picks_15m = _pick_bets_for_sleeve(test_df, joint.cfg_15m, label_col)
        fm_15m, _ = _simulate_isolated_wallet(
            picks_15m, joint.cfg_15m,
            exposure_cap_frac=joint.exposure_cap_15m,
            cap_growth_mode=joint.cap_growth_mode,
            min_bet_usd=joint.min_bet_usd_15m,
            book_consumption_max=joint.book_consumption_max,
            coin_fb_liq=coin_fb_liq, dd_halt_frac=joint.dd_halt_frac,
        )
        fm_15m.name = f"{name}_15m"
    return {"name": name, "fm_5m": fm_5m, "fm_15m": fm_15m}


def _decode_subset(max_minute: int, n_active: int, idx: int) -> tuple[int, ...]:
    chosen: list[int] = []
    remaining_k = n_active; remaining_idx = idx; next_v = 1
    while remaining_k > 0:
        left = max_minute - next_v
        sub = math.comb(left, remaining_k - 1) if left >= remaining_k - 1 else 0
        if remaining_idx < sub:
            chosen.append(next_v); remaining_k -= 1
        else:
            remaining_idx -= sub
        next_v += 1
    return tuple(chosen)


def _suggest_sleeve(trial: optuna.Trial, interval: str) -> SleeveCfg:
    prefix = f"{interval}_"
    max_minute = 14 if interval == "15m" else 4
    max_set_len = min(5, max_minute)
    n_active = trial.suggest_int(f"{prefix}emset_n", 1, max_set_len)
    subset_index = trial.suggest_int(f"{prefix}emset_idx_n{n_active}", 0, math.comb(max_minute, n_active) - 1)
    entry_minute_set = _decode_subset(max_minute, n_active, subset_index)

    min_edge = trial.suggest_float(f"{prefix}min_edge", 0.0, 0.30)
    edge_width = trial.suggest_float(f"{prefix}edge_width", 0.05, 0.70)
    max_edge = min(1.0, min_edge + edge_width)
    min_fill = trial.suggest_float(f"{prefix}min_fill_price", 0.0, 0.99)
    fill_width = trial.suggest_float(f"{prefix}fill_width", 0.05, 1.0)
    max_fill = min(1.0, min_fill + fill_width)
    walk_leash = trial.suggest_categorical(f"{prefix}walk_leash", ["bounded", "unbounded"])
    max_walk_price = (trial.suggest_float(f"{prefix}max_walk_price", 0.01, 0.50)
                      if walk_leash == "bounded" else None)
    pz_floor_on = trial.suggest_categorical(f"{prefix}pz_floor_on", ["on", "off"])
    pz_floor = trial.suggest_float(f"{prefix}pz_floor", -3.0, 1.0) if pz_floor_on == "on" else None
    pz_ceil_on = trial.suggest_categorical(f"{prefix}pz_ceil_on", ["on", "off"])
    pz_ceil = trial.suggest_float(f"{prefix}pz_ceil", -1.0, 3.0) if pz_ceil_on == "on" else None

    return SleeveCfg(
        interval=interval,
        side=trial.suggest_categorical(f"{prefix}side", ["Up", "Down", "model_signed"]),
        entry_minute_set=entry_minute_set,
        entry_strategy=trial.suggest_categorical(
            f"{prefix}entry_strategy", ["single", "first_qualifying", "every_qualifying"]),
        min_edge=min_edge, max_edge=max_edge,
        min_fill_price=min_fill, max_fill_price=max_fill,
        dow_set=trial.suggest_categorical(f"{prefix}dow_set", ["all", "weekday", "weekend"]),
        min_agreement=trial.suggest_int(f"{prefix}min_agreement", 0, 3),
        capital_frac=trial.suggest_float(f"{prefix}capital_frac", 0.005, 0.10, log=True),
        max_walk_price=max_walk_price,
        slippage_haircut=trial.suggest_float(f"{prefix}slippage_haircut", 0.005, 0.10),
        min_lut_n=trial.suggest_categorical(f"{prefix}min_lut_n", [0, 10, 20, 30, 50, 100]),
        signal_source=trial.suggest_categorical(f"{prefix}signal_source", ["rf", "lut", "blend"]),
        partial_ret_z_floor=pz_floor, partial_ret_z_ceiling=pz_ceil,
        fallback_book_liquidity=trial.suggest_float(f"{prefix}fb_book_liq", 500.0, 50_000.0, log=True),
        impact_coefficient=trial.suggest_float(f"{prefix}imp_coef", 0.05, 0.50),
        no_impact_threshold=trial.suggest_float(f"{prefix}no_imp_thresh", 0.01, 0.20),
    )


_DEAD_SLEEVE = SleeveCfg(
    interval="15m", side="Up", entry_minute_set=(1,), entry_strategy="single",
    min_edge=0.99, max_edge=1.0, min_fill_price=0.99, max_fill_price=1.0,
    dow_set="all", min_agreement=0, capital_frac=0.01, max_walk_price=None,
    slippage_haircut=0.005, min_lut_n=0, signal_source="rf",
    partial_ret_z_floor=None, partial_ret_z_ceiling=None,
    fallback_book_liquidity=10_000.0, impact_coefficient=0.30, no_impact_threshold=0.05,
)


def _suggest_joint(trial: optuna.Trial, dd_halt_frac: float,
                   only_sleeve: str | None = None) -> JointCfg:
    """Suggest a config. When ``only_sleeve`` is "5m" or "15m", only that
    sleeve's params + that wallet's knobs are searched; the other sleeve is a
    dead config that fires no bets and is never evaluated."""
    if only_sleeve == "5m":
        cfg_5m = _suggest_sleeve(trial, "5m")
        cfg_15m = replace(_DEAD_SLEEVE, interval="15m")
        exposure_cap_5m = trial.suggest_float("exposure_cap_5m", 0.30, 0.90)
        exposure_cap_15m = 0.30
        min_bet_usd_5m = trial.suggest_float("min_bet_usd_5m", 0.50, 5.00)
        min_bet_usd_15m = 0.50
    elif only_sleeve == "15m":
        cfg_5m = replace(_DEAD_SLEEVE, interval="5m", entry_minute_set=(1,))
        cfg_15m = _suggest_sleeve(trial, "15m")
        exposure_cap_5m = 0.30
        exposure_cap_15m = trial.suggest_float("exposure_cap_15m", 0.30, 0.90)
        min_bet_usd_5m = 0.50
        min_bet_usd_15m = trial.suggest_float("min_bet_usd_15m", 0.50, 5.00)
    else:
        cfg_5m = _suggest_sleeve(trial, "5m")
        cfg_15m = _suggest_sleeve(trial, "15m")
        exposure_cap_5m = trial.suggest_float("exposure_cap_5m", 0.30, 0.90)
        exposure_cap_15m = trial.suggest_float("exposure_cap_15m", 0.30, 0.90)
        min_bet_usd_5m = trial.suggest_float("min_bet_usd_5m", 0.50, 5.00)
        min_bet_usd_15m = trial.suggest_float("min_bet_usd_15m", 0.50, 5.00)
    cap_growth_mode = trial.suggest_categorical("cap_growth_mode", ["fixed", "peak_growth"])
    fallback_liq_btc = trial.suggest_float("fallback_liq_btc", 500.0, 50_000.0, log=True)
    fallback_liq_eth = trial.suggest_float("fallback_liq_eth", 500.0, 50_000.0, log=True)
    fallback_liq_sol = trial.suggest_float("fallback_liq_sol", 500.0, 50_000.0, log=True)
    fallback_liq_xrp = trial.suggest_float("fallback_liq_xrp", 500.0, 50_000.0, log=True)
    book_consumption_max = trial.suggest_float("book_consumption_max", 0.05, 0.50)
    return JointCfg(
        cfg_5m=cfg_5m, cfg_15m=cfg_15m,
        exposure_cap_5m=exposure_cap_5m, exposure_cap_15m=exposure_cap_15m,
        min_bet_usd_5m=min_bet_usd_5m, min_bet_usd_15m=min_bet_usd_15m,
        dd_halt_frac=dd_halt_frac, cap_growth_mode=cap_growth_mode,
        fallback_liq_btc=fallback_liq_btc, fallback_liq_eth=fallback_liq_eth,
        fallback_liq_sol=fallback_liq_sol, fallback_liq_xrp=fallback_liq_xrp,
        book_consumption_max=book_consumption_max,
    )


def _objective_factory(dd_halt_frac: float, label_col: str, min_bets_per_fold: int,
                       only_sleeve: str | None = None):
    active_keys = (["fm_5m", "fm_15m"] if only_sleeve is None
                   else [f"fm_{only_sleeve}"])

    def _objective(trial: optuna.Trial) -> float:
        joint = _suggest_joint(trial, dd_halt_frac, only_sleeve=only_sleeve)
        active_cfgs = ([joint.cfg_5m, joint.cfg_15m] if only_sleeve is None
                       else [getattr(joint, f"cfg_{only_sleeve}")])
        for s in active_cfgs:
            if s.entry_strategy == "single" and len(s.entry_minute_set) != 1:
                raise optuna.TrialPruned()
        gc.collect()
        fold_results: list[dict] = []
        for name, ts, te, vs, ve in WALK_FORWARD_FOLDS:
            res = _evaluate_fold(name, ts, te, vs, ve, joint, label_col,
                                 only_sleeve=only_sleeve)
            fold_results.append(res)
            # Per-active-wallet safety gates
            for key in active_keys:
                fm = res[key]
                if fm.max_dd_pct_peak > dd_halt_frac:
                    raise optuna.TrialPruned()
                if fm.dd_halted:
                    raise optuna.TrialPruned()
                if fm.worst_30bet_dd_pct > dd_halt_frac:
                    raise optuna.TrialPruned()
                if fm.final_equity > 5_000_000.0:
                    raise optuna.TrialPruned()
            # Combined (active) bets >= min, combined (active) PnL > 0
            active_bets = sum(res[k].n_bets for k in active_keys)
            if active_bets < min_bets_per_fold:
                raise optuna.TrialPruned()
            active_pnl = sum(res[k].total_pnl for k in active_keys)
            if active_pnl <= 0:
                raise optuna.TrialPruned()
        # Persist per-wallet per-fold metrics
        for res in fold_results:
            for sub in ("fm_5m", "fm_15m"):
                fm = res[sub]
                for k, v in asdict(fm).items():
                    if isinstance(v, (int, float, bool, str)):
                        trial.set_user_attr(f"{fm.name}_{k}", v)
        # Objective: sum across folds of the ACTIVE wallet(s) PnL.
        # WARNING: this is an IN-SAMPLE metric — Optuna directly maximizes
        # performance on the same fixed test windows across all trials (selection-
        # on-test). Compounding amplifies lucky streaks super-linearly. Do NOT
        # treat best_value_pnl_sum as an expected-live-PnL estimate; it is an
        # upper bound. Forward-OOS (polymarket/backtest/lib/forward_oos_regrade.py) is the
        # go/no-go arbiter. See MEMORY: project_pmdata_wfo_abcd.md.
        return float(sum(res[k].total_pnl for res in fold_results for k in active_keys))
    return _objective


def _holdout_check(joint: JointCfg, label_col: str, only_sleeve: str | None = None) -> dict:
    name, ts, te, vs, ve = HOLDOUT_FOLD
    return _evaluate_fold(name, ts, te, vs, ve, joint, label_col, only_sleeve=only_sleeve)


def _sleeve_to_dict(s: SleeveCfg) -> dict:
    d = asdict(s)
    d["entry_minute_set"] = list(s.entry_minute_set)
    return d


def _decode_best(study: optuna.Study, dd_halt_frac: float,
                 only_sleeve: str | None = None) -> JointCfg:
    trial = optuna.trial.FixedTrial(study.best_params)
    return _suggest_joint(trial, dd_halt_frac, only_sleeve=only_sleeve)


class _PlateauStopper:
    """Stops the study when ``study.best_value`` has not improved by ``tol``
    over the last ``no_improve_trials`` gate-passing (COMPLETE) trials.

    Pruned trials don't advance the plateau counter — an unlucky first batch
    of gate-failing configs cannot trigger early stop. Periodically prints
    progress so background runs surface activity.
    """
    def __init__(self, no_improve_trials: int, tol: float, log_every: int = 50) -> None:
        self.no_improve_trials = no_improve_trials
        self.tol = tol
        self.log_every = log_every
        self._last_logged = 0

    def __call__(self, study: optuna.Study, trial: optuna.FrozenTrial) -> None:
        all_trials = study.trials
        completes = [t for t in all_trials if t.state == TrialState.COMPLETE and t.value is not None]
        pruned = [t for t in all_trials if t.state == TrialState.PRUNED]
        # heartbeat log
        if len(all_trials) - self._last_logged >= self.log_every:
            best_so_far = max((t.value for t in completes), default=None)
            best_str = f"${best_so_far:,.0f}" if best_so_far is not None else "none"
            print(f"  [trial {len(all_trials):>4d}] complete={len(completes)} pruned={len(pruned)} "
                  f"best={best_str}", file=sys.stderr, flush=True)
            self._last_logged = len(all_trials)
        if len(completes) < self.no_improve_trials + 1:
            return
        best_now = max(t.value for t in completes)
        prior = completes[:-self.no_improve_trials]
        if not prior:
            return
        best_then = max(t.value for t in prior)
        if best_now - best_then < self.tol:
            print(f"  early stop: best {best_now:,.0f} hasn't improved by {self.tol:,.0f} "
                  f"over last {self.no_improve_trials} gate-passing trials", file=sys.stderr)
            study.stop()


def _run_study(run_id: str, n_trials: int, dd_halt_frac: float,
               label_col: str, min_bets_per_fold: int, seed: int,
               early_stop_patience: int = 100, early_stop_tol: float = 100.0,
               only_sleeve: str | None = None) -> int:
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    storage = f"sqlite:///{(run_dir / 'study.db').as_posix()}"
    sampler = optuna.samplers.TPESampler(seed=seed, n_startup_trials=40)
    study_tag = only_sleeve or "both"
    study = optuna.create_study(
        direction="maximize", storage=storage,
        study_name=f"compound_{study_tag}_dd{int(dd_halt_frac*100):02d}",
        sampler=sampler, load_if_exists=True,
    )
    study.set_user_attr("dd_halt_frac", dd_halt_frac)
    study.set_user_attr("label_col", label_col)
    study.set_user_attr("min_bets_per_fold", min_bets_per_fold)
    study.set_user_attr("initial_capital", INITIAL_CAPITAL)
    study.set_user_attr("coin_universe", list(COIN_UNIVERSE))
    study.set_user_attr("only_sleeve", only_sleeve or "both")

    obj = _objective_factory(dd_halt_frac, label_col, min_bets_per_fold,
                             only_sleeve=only_sleeve)
    stopper = _PlateauStopper(no_improve_trials=early_stop_patience, tol=early_stop_tol)
    t0 = time.time()
    study.optimize(obj, n_trials=n_trials, n_jobs=1, gc_after_trial=True,
                   show_progress_bar=False, callbacks=[stopper])
    elapsed = time.time() - t0

    completes = [t for t in study.trials if t.state == TrialState.COMPLETE]
    pruned = [t for t in study.trials if t.state == TrialState.PRUNED]
    print(f"\n[{study_tag}] complete={len(completes)} pruned={len(pruned)} total={len(study.trials)} "
          f"elapsed={elapsed:.0f}s", file=sys.stderr)
    if not completes:
        return 1

    print(f"[{study_tag}] best test_pnl_sum = ${study.best_value:,.2f}", file=sys.stderr)
    best_joint = _decode_best(study, dd_halt_frac, only_sleeve=only_sleeve)
    holdout_res = _holdout_check(best_joint, label_col, only_sleeve=only_sleeve)
    h5 = holdout_res["fm_5m"]; h15 = holdout_res["fm_15m"]
    print(f"\nholdout 5m  wallet: n={h5.n_bets} pnl=${h5.total_pnl:,.2f} final=${h5.final_equity:,.2f} "
          f"DD={h5.max_dd_pct_peak:.3f}", file=sys.stderr)
    print(f"holdout 15m wallet: n={h15.n_bets} pnl=${h15.total_pnl:,.2f} final=${h15.final_equity:,.2f} "
          f"DD={h15.max_dd_pct_peak:.3f}", file=sys.stderr)
    print(f"holdout combined  : pnl=${h5.total_pnl + h15.total_pnl:,.2f} "
          f"final=${h5.final_equity + h15.final_equity:,.2f} (from $200 total seed)",
          file=sys.stderr)

    best_trial = study.best_trial
    fold_summary = {}
    for name, *_ in WALK_FORWARD_FOLDS:
        per_wallet = {}
        for suffix in ("5m", "15m"):
            prefix = f"{name}_{suffix}_"
            per_wallet[suffix] = {
                k[len(prefix):]: v for k, v in best_trial.user_attrs.items()
                if k.startswith(prefix)
            }
        fold_summary[name] = per_wallet

    proposed = {
        "run_id": run_id, "schema_version": 4,
        "only_sleeve": only_sleeve or "both",
        "purpose": ("v4 ISOLATED-WALLETS + L2-EXECUTABLE PRICES: each sleeve runs on "
                    "its own $100 bankroll, own exposure cap, own min_bet, own DD halt. "
                    "Edge gate AND fill use the REAL L2 best-ask (not the stale "
                    "decision_table quote). Bet placed by walking the actual L2 ask "
                    "book; face capped at book_consumption_max * real L2 depth; fill = "
                    "VWAP of the walk. Rows with no L2 snapshot are dropped (can't "
                    "execute in live). --only-sleeve optimizes one wallet at a time."),
        "objective": ("sum(test_pnl across 3 walk-forward folds for the "
                      + (only_sleeve or "5m+15m") + " wallet(s))"),
        "best_value_pnl_sum": float(study.best_value),
        "initial_capital_per_wallet": INITIAL_CAPITAL,
        "total_initial_capital": INITIAL_CAPITAL * 2,
        "dd_halt_frac": dd_halt_frac,
        "label_col": label_col, "coin_universe": list(COIN_UNIVERSE),
        "wallet_5m": {
            "exposure_cap_frac": best_joint.exposure_cap_5m,
            "min_bet_usd": best_joint.min_bet_usd_5m,
            "cap_growth_mode": best_joint.cap_growth_mode,
            "cfg": _sleeve_to_dict(best_joint.cfg_5m),
        },
        "wallet_15m": {
            "exposure_cap_frac": best_joint.exposure_cap_15m,
            "min_bet_usd": best_joint.min_bet_usd_15m,
            "cap_growth_mode": best_joint.cap_growth_mode,
            "cfg": _sleeve_to_dict(best_joint.cfg_15m),
        },
        "shared": {
            "book_consumption_max": best_joint.book_consumption_max,
            "fallback_liquidity_per_coin": {
                "btc": best_joint.fallback_liq_btc, "eth": best_joint.fallback_liq_eth,
                "sol": best_joint.fallback_liq_sol, "xrp": best_joint.fallback_liq_xrp,
            },
        },
        "fold_diagnostics": fold_summary,
        "holdout_diagnostics": {
            "name": "holdout_E",
            "wallet_5m": {
                "n_bets": h5.n_bets, "total_pnl": h5.total_pnl,
                "final_equity": h5.final_equity, "max_dd_pct_peak": h5.max_dd_pct_peak,
                "worst_30bet_dd_pct": h5.worst_30bet_dd_pct, "win_rate": h5.win_rate,
                "sortino": h5.sortino, "days": h5.days,
            },
            "wallet_15m": {
                "n_bets": h15.n_bets, "total_pnl": h15.total_pnl,
                "final_equity": h15.final_equity, "max_dd_pct_peak": h15.max_dd_pct_peak,
                "worst_30bet_dd_pct": h15.worst_30bet_dd_pct, "win_rate": h15.win_rate,
                "sortino": h15.sortino, "days": h15.days,
            },
            "combined_final_equity": h5.final_equity + h15.final_equity,
            "combined_total_pnl": h5.total_pnl + h15.total_pnl,
        },
        "best_params_raw": study.best_params,
        "n_trials_complete": len(completes), "n_trials_pruned": len(pruned),
        "elapsed_s": round(elapsed, 1),
    }
    out_path = run_dir / "optimize_proposed.json"
    out_path.write_text(json.dumps(proposed, indent=2, default=str))
    study.trials_dataframe().to_csv(run_dir / "trials.csv", index=False)
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default=None)
    p.add_argument("--n-trials", type=int, default=200)
    p.add_argument("--dd-halt-frac", type=float, default=0.30)
    p.add_argument("--label-col", default="won_up_polymarket",
                   choices=("won_up", "won_up_polymarket"))
    p.add_argument("--min-bets-per-fold", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--early-stop-patience", type=int, default=100,
                   help="stop after N gate-passing trials with no improvement (default 100)")
    p.add_argument("--early-stop-tol", type=float, default=100.0,
                   help="minimum $ improvement that counts (default 100.0)")
    p.add_argument("--only-sleeve", choices=("5m", "15m"), default=None,
                   help="optimize ONE wallet independently (5m or 15m). Omit for joint.")
    args = p.parse_args()
    run_id = args.run_id or time.strftime("%Y%m%d_%H%M%S")
    return _run_study(run_id=run_id, n_trials=args.n_trials, dd_halt_frac=args.dd_halt_frac,
                      label_col=args.label_col, min_bets_per_fold=args.min_bets_per_fold,
                      seed=args.seed, early_stop_patience=args.early_stop_patience,
                      early_stop_tol=args.early_stop_tol, only_sleeve=args.only_sleeve)


if __name__ == "__main__":
    raise SystemExit(main())
