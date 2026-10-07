"""Fixed-offset gate/sizing sweep and the wallet simulator used by
``training.checkpoint``.

A library (no ``__main__``), tested by ``tests/test_offset_validation.py``.

``_fixed_set_wfo`` sweeps the opening-range gate and sizing with the entry
offsets fixed, and picks the configuration with the highest training PnL such
that every training fold is positive and the training drawdown stays under the
cap. Fold D is never read by the gate or the sort key.

Fold semantics: fold A is scored in-sample by the model trained on A (it is the
seed training fold, never a held-out test fold in an expanding scheme); B, C
and D are progressively out-of-sample. D is the sole arbiter.
"""
from __future__ import annotations

import itertools
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core" / "src"))

from polymarket.backtest.training import modeling as ev  # noqa: E402
from polymarket.backtest.training import offset_wfo as base  # noqa: E402

COINS = ("btc", "eth", "sol", "xrp")
INTERVALS = ("5m", "15m")
LABEL_COL = "won_up_polymarket"

# Opening-range gate defaults for 4 coins: pred_edge >= 0.08, strengths >= 0.
LIVE_PRED_EDGE_MIN = 0.08

TRAIN_FOLDS = base.TRAIN_FOLDS  # ("A","B","C")
OOS_FOLD = base.OOS_FOLD        # "D"

# Calendar fold boundaries (mirror base.CHUNKS): A=Feb1, B=Mar5, C=Apr6, D=May8, end=Jun10.
_A0 = pd.Timestamp("2026-02-01", tz="UTC")
_B0 = pd.Timestamp("2026-03-05", tz="UTC")
_C0 = pd.Timestamp("2026-04-06", tz="UTC")
_D0 = pd.Timestamp("2026-05-08", tz="UTC")
_END = pd.Timestamp("2026-06-10", tz="UTC")
EMBARGO_SECONDS = ev.FOLD_EMBARGO_SECONDS  # 3600

COIN_MULTS = {
    "baseline_13k": {"btc": 0.95, "eth": 0.85, "xrp": 0.2},
    "btc_eth_half_xrp": {"btc": 1.0, "eth": 1.0, "xrp": 0.5},
    "uniform": {},
}
COIN_SETS = tuple(
    tuple(combo)
    for size in range(1, len(COINS) + 1)
    for combo in itertools.combinations(COINS, size)
)
BASELINE_GATE_GRID = ((LIVE_PRED_EDGE_MIN, 0.0, 0.0),)
EXPANDED_GATE_GRID = tuple(itertools.product(
    (0.08, 0.10, 0.12, 0.14),
    (0.0, 0.5, 1.0),
    (0.0, 0.25, 0.5),
))
BASELINE_STAKE_MULTS = (0.50, 0.75, 0.90, 0.95, 0.98, 0.99, 1.0, 2.0, 3.0, 4.0)
BASELINE_MAX_BET_MULTS = (1.0, 1.5, 2.25)
BASELINE_BOOK_CONSUMPTION_MULTS = (1.0, 1.5, 2.0)
EXPOSURE_CAPS = (0.5, 1.0)
MIN_WALLET_CHUNK_BETS_DEFAULT = 20
CAPITAL_DEFAULT = 100.0
MIN_BET_USD_DEFAULT = base.MIN_BET_USD


# --------------------------------------------------------------------------- #
# Expanding-window fold calendar. ``training.checkpoint`` fits its own models
# against it (its ``_train_windows()`` is the matching helper).
# --------------------------------------------------------------------------- #
# chunk -> train-window key that scores it. D is scored only by the A+B+C
# model, whose training window ends before D starts (minus the embargo), so D
# is never trained on.
CHUNK_MODEL_KEY = {"A": "A", "B": "A", "C": "AB", "D": "ABC"}


# --------------------------------------------------------------------------- #
# Opening-range gate config
# --------------------------------------------------------------------------- #
def _gate_cfg(offsets, pred_edge_min, breakout_bps=0.0, min_entry_z=0.0) -> dict:
    return {
        "offsets": tuple(int(x) for x in offsets),
        "pred_edge_min": float(pred_edge_min),
        "min_range_bps": 0.0,
        "breakout_bps": float(breakout_bps),
        "min_entry_z": float(min_entry_z),
    }


# --------------------------------------------------------------------------- #
# Resolution-level dedupe + baseline-style wallet sizing
# --------------------------------------------------------------------------- #
def _resolution_key_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], index=df.index, dtype=object)
    market = df["market_slug"].astype(str)
    if "condition_id" not in df.columns:
        return market
    condition = df["condition_id"]
    usable = condition.notna() & condition.astype(str).str.len().gt(0)
    return condition.astype(str).where(usable, market)


def _dedup_resolution_with_stats(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Keep one executable candidate per final resolution market.

    This is intentionally stricter than base._dedup_one_per_market, which keeps
    one row per (market, side, entry_minute). Once multiple seconds/sides/sleeves
    are combined, resolution-level exposure is what matters.
    """
    stats = {
        "candidate_resolution_rows": int(len(df)),
        "resolution_unique_keys": 0,
        "resolution_duplicate_groups": 0,
        "resolution_duplicate_drops": 0,
    }
    if df.empty:
        return df.copy(), stats

    work = df.copy()
    work["resolution_key"] = _resolution_key_series(work)
    counts = work["resolution_key"].value_counts(dropna=False)
    stats["resolution_unique_keys"] = int(len(counts))
    stats["resolution_duplicate_groups"] = int((counts > 1).sum())
    stats["resolution_duplicate_drops"] = int(len(work) - len(counts))

    work["_sort_pred_edge"] = work["pred_edge"].astype(float) if "pred_edge" in work.columns else 0.0
    work["_sort_depth"] = work["book_depth"].astype(float) if "book_depth" in work.columns else 0.0
    ask_col = "ask" if "ask" in work.columns else None
    sort_cols = ["resolution_key", "entry_ts", "_sort_pred_edge", "_sort_depth"]
    ascending = [True, True, False, False]
    if ask_col:
        sort_cols.append(ask_col)
        ascending.append(True)
    out = (
        work.sort_values(sort_cols, ascending=ascending, kind="mergesort")
        .drop_duplicates("resolution_key", keep="first")
        .drop(columns=["_sort_pred_edge", "_sort_depth"])
        .reset_index(drop=True)
    )
    return out, stats


def _passes_config(pool: pd.DataFrame, cfg: dict) -> pd.Series:
    mask = base._passes_filter(pool, cfg)
    coins = cfg.get("coins")
    if coins:
        mask &= pool["coin"].astype(str).str.lower().isin({str(c).lower() for c in coins})
    return mask


def _candidate_set(pool: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, dict]:
    return _dedup_resolution_with_stats(pool.loc[_passes_config(pool, cfg)])


def _row_value(row, name: str, default=None):
    if hasattr(row, name):
        return getattr(row, name)
    if isinstance(row, dict):
        return row.get(name, default)
    return default


def _coin_mult_for(cfg: dict, coin: str) -> float:
    return float(cfg.get("coin_mult", {}).get(str(coin).lower(), 1.0))


def _book_consumption_max_for(sleeve, cfg: dict) -> float:
    return float(sleeve.book_consumption_max) * float(cfg.get("book_consumption_mult", 1.0))


def _target_notional(equity: float, sleeve, row, cfg: dict) -> float:
    coin_mult = _coin_mult_for(cfg, str(_row_value(row, "coin", "")))
    if cfg.get("sizing_mode") == "baseline_portfolio":
        target = float(equity) * float(sleeve.stake_frac) * float(cfg["stake_mult"]) * coin_mult
        if sleeve.max_bet_usd is not None:
            target = min(target, float(sleeve.max_bet_usd) * float(cfg["max_bet_mult"]) * coin_mult)
        return float(target)

    stake_key = "stake_5m" if str(_row_value(row, "interval", "")) == "5m" else "stake_15m"
    return float(equity) * float(cfg[stake_key]) * coin_mult


def _sized_notional(equity: float, sleeve, row, cfg: dict) -> float | None:
    min_bet_usd = float(cfg.get("min_bet_usd", MIN_BET_USD_DEFAULT))
    target = _target_notional(equity, sleeve, row, cfg)
    target = min(max(target, min_bet_usd), float(equity))
    if target < min_bet_usd:
        return None
    return float(target)


def _simulate_wallet(df: pd.DataFrame, sleeves: dict, cfg: dict, *, max_dd_frac: float | None = None) -> dict:
    rows = list(df.sort_values(["entry_ts", "interval", "coin", "side"]).itertuples(index=False))
    capital = float(cfg.get("capital", CAPITAL_DEFAULT))
    equity = peak = capital
    active: list[tuple[int, int, str, float, float]] = []
    realized: list[tuple[int, int, str, float]] = []
    kept: list[dict] = []
    max_active = 0
    n_rejected = 0
    n_exposure = 0
    dd_halted = False

    def realize_until(cutoff_ts: int) -> None:
        nonlocal equity, peak, active, dd_halted
        due = [a for a in active if a[0] <= cutoff_ts]
        active = [a for a in active if a[0] > cutoff_ts]
        for resolve_ts, entry_ts, chunk, _notional, pl in sorted(due, key=lambda x: (x[0], x[1])):
            equity += pl
            peak = max(peak, equity)
            realized.append((resolve_ts, entry_ts, chunk, pl))
            if max_dd_frac is not None:
                dd_ratio = (peak - equity) / peak if peak > 0 else 1.0
                if dd_ratio >= max_dd_frac or equity <= 0.0:
                    dd_halted = True

    for row in rows:
        entry_ts = int(row.entry_ts)
        realize_until(entry_ts)
        if dd_halted or equity <= 0.0:
            break

        sleeve = sleeves[str(row.sleeve)]
        target = _sized_notional(equity, sleeve, row, cfg)
        if target is None:
            continue

        filled = base._fill(
            row.ask_prices,
            row.ask_sizes,
            row.book_depth,
            target,
            float(row.model_prob),
            _book_consumption_max_for(sleeve, cfg),
        )
        if filled is None:
            n_rejected += 1
            continue
        filled_usd, fill = filled
        pending = sum(a[3] for a in active)
        if pending + filled_usd > equity * float(cfg["exposure_cap"]) + 1e-9:
            n_exposure += 1
            continue

        pl = float(ev._cash_pnl_vector(np.array([int(row.y) == 1]), np.array([fill]), filled_usd)[0])
        if not math.isfinite(pl):
            continue
        chunk = str(_row_value(row, "chunk", _row_value(row, "fold", "")))
        resolution_key = str(_row_value(row, "resolution_key", _row_value(row, "market_slug", "")))
        active.append((int(row.resolve_ts), entry_ts, chunk, float(filled_usd), pl))
        max_active = max(max_active, len(active))
        kept.append({
            "resolution_key": resolution_key,
            "market_slug": str(row.market_slug),
            "coin": str(row.coin),
            "interval": str(row.interval),
            "side": str(row.side),
            "entry_ts": entry_ts,
            "resolve_ts": int(row.resolve_ts),
            "offset_sec": int(row.offset_sec),
            "chunk": chunk,
            "sleeve": str(row.sleeve),
            "fill": float(fill),
            "notional": float(filled_usd),
            "pl": pl,
            "pred_edge": float(row.pred_edge),
        })

    realize_until(10**18)

    pls = np.array([pl for *_rest, pl in realized], dtype=np.float64)
    kept_df = pd.DataFrame(kept)
    fold_pnl = {c: 0.0 for c in ("A", "B", "C", "D")}
    fold_n = {c: 0 for c in ("A", "B", "C", "D")}
    fold_w = {c: 0 for c in ("A", "B", "C", "D")}
    for _resolve, _entry, chunk, pl in realized:
        if chunk in fold_pnl:
            fold_pnl[chunk] += float(pl)
            fold_n[chunk] += 1
            fold_w[chunk] += int(pl > 0.0)

    mdd = ev._max_dd_frac(pls, capital) if len(pls) else 0.0
    worst_30 = ev._worst_rolling_dd_frac(pls, capital, 30) if len(pls) else 0.0
    return {
        "_bets": kept,
        "n_bets": int(len(pls)),
        "n_rejected_execution": int(n_rejected),
        "n_exposure_skips": int(n_exposure),
        "total_pnl": float(pls.sum()) if len(pls) else 0.0,
        "final_equity": float(capital + pls.sum()) if len(pls) else capital,
        "max_dd_frac": float(mdd),
        "worst_30bet_dd_frac": float(worst_30),
        "dd_halted": bool(dd_halted),
        "fold_pnl": {k: round(v, 2) for k, v in fold_pnl.items()},
        "fold_n": fold_n,
        "fold_wr": {k: (round(fold_w[k] / fold_n[k], 3) if fold_n[k] else None) for k in fold_n},
        "offset_n": kept_df["offset_sec"].value_counts().sort_index().to_dict() if len(kept_df) else {},
        "coin_n": kept_df["coin"].value_counts().sort_index().to_dict() if len(kept_df) else {},
        "max_active_count": int(max_active),
    }


def _train_pass(sim: dict, max_dd_frac: float, min_wallet_chunk_bets: int) -> bool:
    all_pos = all(
        sim["fold_pnl"][c] > 0.0 and sim["fold_n"][c] >= int(min_wallet_chunk_bets)
        for c in TRAIN_FOLDS
    )
    return bool(
        all_pos
        and sim["max_dd_frac"] <= max_dd_frac + 1e-12
        and sim["worst_30bet_dd_frac"] <= max_dd_frac + 1e-12
        and not sim.get("dd_halted", False)
    )


def _full_acceptance_pass(sim: dict, max_dd_frac: float, min_wallet_chunk_bets: int) -> bool:
    all_pos = all(
        sim["fold_pnl"][c] > 0.0 and sim["fold_n"][c] >= int(min_wallet_chunk_bets)
        for c in ("A", "B", "C", "D")
    )
    return bool(
        all_pos
        and sim["max_dd_frac"] <= max_dd_frac + 1e-12
        and sim["worst_30bet_dd_frac"] <= max_dd_frac + 1e-12
        and not sim.get("dd_halted", False)
    )


def _without_bets(sim: dict) -> dict:
    return {k: v for k, v in sim.items() if k != "_bets"}


# --------------------------------------------------------------------------- #
# Stage D: fixed-set config WFO (D holdout, maxDD <= cap)
# --------------------------------------------------------------------------- #
def _sizing_grid():
    return list(itertools.product(
        BASELINE_STAKE_MULTS,
        BASELINE_MAX_BET_MULTS,
        BASELINE_BOOK_CONSUMPTION_MULTS,
        tuple(COIN_MULTS.keys()),
        EXPOSURE_CAPS,
    ))


def _gate_grid(search_profile: str):
    if search_profile == "expanded":
        return EXPANDED_GATE_GRID
    return BASELINE_GATE_GRID


def _fixed_set_wfo(
    pool,
    fixed_offsets,
    sleeves,
    max_dd_frac,
    *,
    min_wallet_chunk_bets: int = MIN_WALLET_CHUNK_BETS_DEFAULT,
    search_profile: str = "baseline",
    capital: float = CAPITAL_DEFAULT,
    min_bet_usd: float = MIN_BET_USD_DEFAULT,
) -> tuple[list, dict | None]:
    """Sweep the opening-range gate and sizing with offsets fixed to
    ``fixed_offsets``. Pick the highest compounded training PnL such that
    every training fold is positive and training drawdown <= cap. D is never
    read by the gate or the sort key.
    """
    fixed = tuple(int(x) for x in fixed_offsets)
    rows = []
    for coin_set in COIN_SETS:
        for pred_edge_min, breakout_bps, min_entry_z in _gate_grid(search_profile):
            gate = {
                **_gate_cfg(fixed, pred_edge_min, breakout_bps, min_entry_z),
                "coins": tuple(coin_set),
            }
            sub, dedup_stats = _candidate_set(pool, gate)
            if sub.empty:
                continue
            per = {c: base._flat_metrics(sub[sub["chunk"] == c]) for c in ("A", "B", "C", "D")}
            if any(per[c]["pnl"] <= 0 or per[c]["n"] == 0 for c in TRAIN_FOLDS):
                continue
            train_sub = sub[sub["chunk"].isin(TRAIN_FOLDS)]
            oos_sub = sub[sub["chunk"] == OOS_FOLD]
            for stake_mult, max_bet_mult, book_mult, coin_mult_name, exposure_cap in _sizing_grid():
                cfg = {
                    **gate,
                    "sizing_mode": "baseline_portfolio",
                    "stake_mult": float(stake_mult),
                    "max_bet_mult": float(max_bet_mult),
                    "book_consumption_mult": float(book_mult),
                    "coin_mult_name": coin_mult_name,
                    "coin_mult": COIN_MULTS[coin_mult_name],
                    "exposure_cap": float(exposure_cap),
                    "capital": float(capital),
                    "min_bet_usd": float(min_bet_usd),
                }
                train_sim = _simulate_wallet(
                    train_sub, sleeves, cfg, max_dd_frac=max_dd_frac)
                selection_pass = _train_pass(
                    train_sim, max_dd_frac, min_wallet_chunk_bets)
                full_sim = None
                oos_sim = None
                acceptance_pass = False
                bets = []
                if selection_pass:
                    full_sim = _simulate_wallet(
                        sub, sleeves, cfg, max_dd_frac=max_dd_frac)
                    acceptance_pass = _full_acceptance_pass(
                        full_sim, max_dd_frac, min_wallet_chunk_bets)
                    if acceptance_pass:
                        oos_sim = _simulate_wallet(
                            oos_sub, sleeves, cfg, max_dd_frac=max_dd_frac)
                        bets = full_sim["_bets"]
                train_min_pnl = min(train_sim["fold_pnl"][c] for c in TRAIN_FOLDS)
                rows.append({
                    "config": {**cfg, "offsets": list(fixed), "coins": list(coin_set)},
                    "pass": acceptance_pass,
                    "selection_pass": selection_pass,
                    "acceptance_pass": acceptance_pass,
                    "train_all_positive": bool(all(
                        train_sim["fold_pnl"][c] > 0
                        and train_sim["fold_n"][c] >= int(min_wallet_chunk_bets)
                        for c in TRAIN_FOLDS
                    )),
                    "train_maxdd_ok": bool(train_sim["max_dd_frac"] <= max_dd_frac + 1e-12),
                    "full_maxdd_ok": bool(
                        full_sim is not None
                        and full_sim["max_dd_frac"] <= max_dd_frac + 1e-12
                    ),
                    "flat_by_chunk": per,
                    "resolution_dedup": dedup_stats,
                    "train": _without_bets(train_sim),
                    "oos_d": (_without_bets(oos_sim) if oos_sim is not None else None),
                    "full": (_without_bets(full_sim) if full_sim is not None else None),
                    "train_min_pnl": float(train_min_pnl),
                    "_bets": bets,
                })
    rows.sort(
        key=lambda r: (
            bool(r["selection_pass"]),
            float(r["train"]["total_pnl"]),
            float(r["train_min_pnl"]),
            -float(r["train"]["max_dd_frac"]),
        ),
        reverse=True,
    )
    best = next((r for r in rows if r["acceptance_pass"]), None)
    return rows, best
