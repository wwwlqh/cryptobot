"""Replay frozen TWAP-symmetric fold F with live Tier-A and T3 execution.

This is a final-policy diagnostic, not a model-selection loop.  The model,
interval gates, ask band, Tier-A rule, and T3 knobs are frozen before fold F is
read.  Entries and add-ons walk the saved per-tick L2 books and use the live
+$0.01 FAK leash.  Every filled share is held to binary settlement.
"""
from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.lib.tick_store import DEFAULT_TICK_DB, open_store
from polymarket.backtest.studies.tick_latency.run_twap_wfo_full import (
    EMBARGO_S,
    LIVE_FAK_LIMIT_PAD,
    _build_live_bets,
    _fit_and_score,
    _load_side_frame,
)

OUT = Path(__file__).with_name("TWAP_T3_TIERA_FOLD_F_20260812.json")
# Equity from the last READY live reconciliation before this replay (2026-08-12
# 02:36 UTC). At 1% this exercises the $1 minimum stake.
START_EQUITY = 80.820665
BASE_FRAC = 0.01
LATENCY_S = 0.4
TIER_A_EDGE = 0.0465
TIER_A_MULT = 1.5
T3_MAX_N = 3
T3_FRAC = 0.5
T3_NO_DIP = 0.0
T3_CAP = 0.83
T3_SPREAD_MAX = 0.02
T3_EXPOSURE_CAP = 1.5
T3_FAK_PAD = 0.01
T3_END_BUFFER_S = 4
T3_DISARM_BAND = 0.04
T3_EDGE_FLOOR = 0.0

SPECS = {
    "5m": {"intervals": ("5m",), "ask_lo": 0.60, "ask_hi": 0.85, "gate": 0.06},
    "15m": {"intervals": ("15m",), "ask_lo": 0.60, "ask_hi": 0.85, "gate": 0.05},
}


def _floor_cents(value: float) -> float:
    return math.floor(max(0.0, value) * 100.0 + 1e-9) / 100.0


def _walk_limit(levels: tuple[tuple[float, float], ...], amount: float) -> float | None:
    remaining = amount
    limit = 0.0
    for price, size in levels:
        if price <= 0 or size <= 0:
            continue
        take = min(size, remaining / price)
        remaining -= take * price
        limit = price
        if remaining <= 1e-9:
            return limit
    return None


def _score_fold_f() -> tuple[pd.DataFrame, pd.DataFrame]:
    sf = _load_side_frame()
    dev = sf[sf["fold"].isin(["C", "D", "E"])].copy()
    holdout = sf[sf["fold"] == "F"].copy()
    f_start = int(holdout["entry_ts"].min())
    train = dev[dev["entry_ts"] < f_start - EMBARGO_S].copy()
    scored = _fit_and_score(train, holdout, "sym_z", "win_twap_s")
    bets = []
    for interval, spec in SPECS.items():
        picked = _build_live_bets(scored, spec).copy()
        picked["selected_interval"] = interval
        bets.append(picked)
    return scored, pd.concat(bets, ignore_index=True)


def _qualifying_ranks(scored: pd.DataFrame) -> pd.DataFrame:
    chunks = []
    for interval, spec in SPECS.items():
        d = scored[scored["interval"] == interval].copy()
        off0 = 270 if interval == "5m" else 870
        d = d[
            (d["offset_sec"] >= off0)
            & (d["offset_sec"] <= off0 + 29)
            & (d["ask"] > spec["ask_lo"])
            & (d["ask"] <= spec["ask_hi"])
        ]
        d = d.sort_values(
            ["market_slug", "entry_ts", "pred_edge"],
            ascending=[True, True, False], kind="mergesort",
        ).groupby(["market_slug", "entry_ts"], as_index=False).first()
        d = d[d["pred_edge"] >= spec["gate"]].copy()
        d["attempt_rank"] = d.groupby("market_slug").cumcount() + 1
        chunks.append(d[["market_slug", "entry_ts", "attempt_rank"]])
    return pd.concat(chunks, ignore_index=True)


def _metrics(rows: pd.DataFrame) -> dict:
    if rows.empty:
        return {"n": 0}
    selected_n = int(len(rows))
    covered_n = int(rows["tick_available"].sum())
    rows = rows[rows["tick_available"]].copy()
    if rows.empty:
        return {
            "n": 0, "selected_n": selected_n, "tick_covered": 0,
            "tick_coverage": 0.0,
        }
    pnl = rows["pnl"].to_numpy(float)
    deployed = rows["deployed"].to_numpy(float)
    filled = rows[rows["entry_filled"]]
    ordered = rows.sort_values("entry_ts", kind="mergesort")
    curve = ordered["pnl"].cumsum().to_numpy(float)
    peak = np.maximum.accumulate(np.r_[0.0, curve])[:-1]
    drawdown = peak - curve
    return {
        "n": int(len(rows)),
        "selected_n": selected_n,
        "tick_covered": covered_n,
        "tick_coverage": float(covered_n / selected_n),
        "wins": int(filled["win"].sum()),
        "win_rate": float(filled["win"].mean()),
        "fills": int(rows["entry_filled"].sum()),
        "fill_rate": float(rows["entry_filled"].mean()),
        "tier_a_n": int(rows["tier_a"].sum()),
        "topup_legs": int(rows["topup_count"].sum()),
        "topup_markets": int((rows["topup_count"] > 0).sum()),
        "deployed_usd": float(deployed.sum()),
        "pnl_usd": float(pnl.sum()),
        "ev_per_selected_signal_usd": float(pnl.mean()),
        "ev_per_filled_trade_usd": float(pnl.sum() / len(filled)),
        "ev_per_dollar": float(pnl.sum() / deployed.sum()),
        "max_fixed_stake_drawdown_usd": float(drawdown.max(initial=0.0)),
    }


def _paired_day_bootstrap(base: pd.DataFrame, variant: pd.DataFrame) -> dict:
    paired = base[["market_slug", "entry_ts", "pnl", "deployed"]].merge(
        variant[["market_slug", "pnl", "deployed"]], on="market_slug",
        suffixes=("_base", "_variant"), validate="one_to_one",
    )
    paired["day"] = pd.to_datetime(
        paired["entry_ts"], unit="s", utc=True
    ).dt.floor("D")
    daily = paired.groupby("day", observed=True).agg(
        variant_pnl=("pnl_variant", "sum"),
        base_pnl=("pnl_base", "sum"),
        variant_deployed=("deployed_variant", "sum"),
        base_deployed=("deployed_base", "sum"),
        n=("market_slug", "size"),
    )
    daily["delta_pnl"] = daily["variant_pnl"] - daily["base_pnl"]
    daily["delta_deployed"] = daily["variant_deployed"] - daily["base_deployed"]
    rng = np.random.default_rng(42)
    values = daily[["delta_pnl", "delta_deployed", "n"]].to_numpy(float)
    draws = rng.integers(0, len(values), size=(10_000, len(values)))
    sampled = values[draws].sum(axis=1)
    delta_per_signal = sampled[:, 0] / sampled[:, 2]
    return {
        "days": int(len(daily)),
        "delta_pnl_usd": float(daily["delta_pnl"].sum()),
        "delta_deployed_usd": float(daily["delta_deployed"].sum()),
        "incremental_roi": float(
            daily["delta_pnl"].sum() / daily["delta_deployed"].sum()
        ),
        "delta_ev_per_signal_usd": float(
            daily["delta_pnl"].sum() / daily["n"].sum()
        ),
        "day_block_bootstrap_p_delta_positive": float(
            np.mean(delta_per_signal > 0)
        ),
        "day_block_bootstrap_delta_ev_95pct": [
            float(x) for x in np.quantile(delta_per_signal, [0.025, 0.975])
        ],
    }


def _replay_variant(
    bets: pd.DataFrame,
    scored: pd.DataFrame,
    *,
    tier_a_mult: float,
    t3_enabled: bool,
) -> pd.DataFrame:
    store = open_store(DEFAULT_TICK_DB, readonly=True)

    @lru_cache(maxsize=256)
    def tick(slug: str) -> te.TickBook | None:
        if not store.has_slug(slug):
            return None
        return te.load_tick_slug(slug, store)

    score_map = {
        (str(r.market_slug), int(r.entry_ts), int(r.side_up)): float(r.model_p)
        for r in scored.dropna(subset=["model_p"]).itertuples()
    }
    records = []
    for row in bets.sort_values("entry_ts", kind="mergesort").itertuples():
        slug = str(row.market_slug)
        side_up = int(row.side_up)
        side = "UP" if side_up == 1 else "DOWN"
        tier_a = int(row.attempt_rank) == 1 and float(row.pred_edge) >= TIER_A_EDGE
        base_requested = max(1.0, _floor_cents(START_EQUITY * BASE_FRAC))
        requested = _floor_cents(
            base_requested * (tier_a_mult if tier_a else 1.0)
        )
        book = tick(slug)
        if book is None:
            records.append({
                "market_slug": slug, "coin": row.coin, "interval": row.interval,
                "entry_ts": int(row.entry_ts), "win": int(row.win_true),
                "tier_a": tier_a, "tick_available": False,
                "entry_filled": False, "topup_count": 0,
                "deployed": 0.0, "pnl": 0.0,
            })
            continue
        decision_ts = int(row.entry_ts) + te.DECISION_ANCHOR_SECONDS
        di = book.index_at(decision_ts)
        ei = book.index_at(decision_ts + LATENCY_S)
        fill = None
        if di is not None and ei is not None:
            decision_levels = book.ask_levels(di, side)
            walked = _walk_limit(decision_levels, requested)
            if walked is not None:
                fill = te.fak_buy(
                    book.ask_levels(ei, side), requested,
                    min(walked + LIVE_FAK_LIMIT_PAD, 0.99),
                )
        if fill is None:
            records.append({
                "market_slug": slug, "coin": row.coin, "interval": row.interval,
                "entry_ts": int(row.entry_ts), "win": int(row.win_true),
                "tier_a": tier_a, "tick_available": True,
                "entry_filled": False, "topup_count": 0,
                "deployed": 0.0, "pnl": 0.0,
            })
            continue

        legs = []
        if t3_enabled:
            entry_fill_ts = decision_ts + LATENCY_S
            settlement_ts = int(row.slug_ts) + (300 if row.interval == "5m" else 900)
            stake = max(1.0, _floor_cents(T3_FRAC * requested))
            running_min = None
            disarmed = False
            t = int(math.ceil(entry_fill_ts)) + 1
            while t < settlement_ts - T3_END_BUFFER_S and not disarmed:
                i = book.index_at(t)
                if i is None:
                    t += 1
                    continue
                asks = book.ask_levels(i, side)
                bids = book.bid_levels(i, side)
                held_ask = asks[0][0] if asks else None
                bid = bids[0][0] if bids else None
                crossed = held_ask is not None and bid is not None and bid >= held_ask - 1e-9
                if held_ask is not None and not crossed:
                    running_min = held_ask if running_min is None else min(running_min, held_ask)
                if running_min is not None and running_min < float(row.ask) - T3_NO_DIP - 1e-9:
                    disarmed = True
                    break
                current_p = score_map.get((slug, t - int(te.DECISION_ANCHOR_SECONDS), side_up))
                if (
                    current_p is not None and bid is not None
                    and bid < fill.vwap and bid >= current_p + T3_DISARM_BAND
                ):
                    disarmed = True
                    break
                if held_ask is None or bid is None or crossed:
                    t += 1
                    continue
                walked = _walk_limit(asks, stake)
                frozen_p = float(row.model_p)
                addon_edge = (
                    float("-inf") if walked is None else
                    frozen_p - walked - ps.fee(walked)
                )
                prior_cash = math.fsum(x.cash_usd for x in legs)
                if (
                    walked is None or walked > T3_CAP + 1e-9
                    or held_ask - bid > T3_SPREAD_MAX + 1e-9
                    or addon_edge < T3_EDGE_FLOOR
                    or len(legs) >= T3_MAX_N
                    or prior_cash + stake > T3_EXPOSURE_CAP * fill.cash_usd + 1e-9
                ):
                    t += 1
                    continue
                xi = book.index_at(t + LATENCY_S)
                extra = None if xi is None else te.fak_buy(
                    book.ask_levels(xi, side), stake,
                    min(walked + T3_FAK_PAD, 0.99),
                )
                if extra is not None:
                    legs.append(extra)
                t += 1

        total_cash = fill.cash_usd + math.fsum(x.cash_usd for x in legs)
        total_shares = fill.shares + math.fsum(x.shares for x in legs)
        fees = te.entry_fee_usd(fill.vwap, fill.shares) + math.fsum(
            te.entry_fee_usd(x.vwap, x.shares) for x in legs
        )
        pnl = total_shares * int(row.win_true) - total_cash - fees
        records.append({
            "market_slug": slug, "coin": row.coin, "interval": row.interval,
            "entry_ts": int(row.entry_ts), "win": int(row.win_true),
            "tier_a": tier_a, "tick_available": True, "entry_filled": True,
            "topup_count": len(legs), "deployed": total_cash, "pnl": pnl,
        })
    store.close()
    return pd.DataFrame(records)


def main() -> None:
    scored, bets = _score_fold_f()
    ranks = _qualifying_ranks(scored)
    bets = bets.merge(ranks, on=["market_slug", "entry_ts"], how="left")
    if bets["attempt_rank"].isna().any():
        raise RuntimeError("selected bet missing qualifying attempt rank")

    variants = {
        "base": (1.0, False),
        "tier_a_1_5x": (TIER_A_MULT, False),
        "t3": (1.0, True),
        "tier_a_1_5x_plus_t3": (TIER_A_MULT, True),
    }
    report = {
        "protocol": {
            "holdout": "native fold F; untouched by model and gate selection",
            "raw_entry_stake_usd": START_EQUITY * BASE_FRAC,
            "effective_base_stake_usd": max(
                1.0, _floor_cents(START_EQUITY * BASE_FRAC)
            ),
            "latency_ms": int(LATENCY_S * 1000),
            "fak_pad": LIVE_FAK_LIMIT_PAD,
            "tier_a_edge": TIER_A_EDGE,
            "tier_a_mult": TIER_A_MULT,
            "t3": {
                "max_n": T3_MAX_N, "frac": T3_FRAC, "no_dip": T3_NO_DIP,
                "cap": T3_CAP, "spread_max": T3_SPREAD_MAX,
                "exposure_cap": T3_EXPOSURE_CAP, "fak_pad": T3_FAK_PAD,
                "end_buffer_s": T3_END_BUFFER_S,
                "disarm_band": T3_DISARM_BAND, "edge_floor": T3_EDGE_FLOOR,
            },
            "tick_db": str(DEFAULT_TICK_DB),
        },
        "selected_markets": int(len(bets)),
        "variants": {},
    }
    replayed = {}
    for name, (mult, t3) in variants.items():
        print(f"[replay] {name}", flush=True)
        rows = _replay_variant(bets, scored, tier_a_mult=mult, t3_enabled=t3)
        replayed[name] = rows
        report["variants"][name] = {
            "all": _metrics(rows),
            "by_interval": {
                str(k): _metrics(v) for k, v in rows.groupby("interval")
            },
            "by_coin": {str(k): _metrics(v) for k, v in rows.groupby("coin")},
        }
    report["paired_vs_base"] = {
        name: _paired_day_bootstrap(replayed["base"], rows)
        for name, rows in replayed.items() if name != "base"
    }
    print(json.dumps(report, indent=2))
    try:
        OUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except PermissionError:
        pass


if __name__ == "__main__":
    main()
