"""Frozen-model exact-TWAP forward replay for Aug 7-11, 2026.

Training ends with native fold F (2026-07-21).  This script evaluates Aug 7-11
using Gamma's exact Chainlink ``priceToBeat``/``finalPrice``, PMData L2 books,
and Binance 1-second spot inputs.  No Aug-7 result is used to fit or select the
model, ask bands, gates, Tier-A rule, or T3 rule.
"""
from __future__ import annotations

import json
import math
import zipfile
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT
from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.training.export_twap_symmetric import (
    FEATURE_COLUMNS, _production_frame,
)
from polymarket.backtest.studies.tick_latency.run_twap_t3_tiera_holdout import (
    LATENCY_S, LIVE_FAK_LIMIT_PAD, SPECS,
    T3_CAP, T3_DISARM_BAND, T3_EDGE_FLOOR, T3_END_BUFFER_S,
    T3_EXPOSURE_CAP, T3_FAK_PAD, T3_FRAC, T3_MAX_N, T3_NO_DIP,
    T3_SPREAD_MAX, TIER_A_EDGE, TIER_A_MULT, _floor_cents, _walk_limit,
)

SNAP = BT_DATA_DIR / "pmdata" / "poly_snapshot"
GAMMA = BT_DATA_DIR / "pmdata" / "gamma_twap_prices_20260807_20260812.parquet"
TEMP = Path.home() / "AppData" / "Local" / "Temp"
DATE = "2026-08-07/2026-08-11"
DAY_START = 1786060800
DAY_END = 1786492800
PAIRS = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}
COIN_ID = ps.COIN_MAP
OUT = Path(__file__).with_name("TWAP_EXACT_AUG7_20260812.json")
V1_MANIFEST = (
    REPO_ROOT
    / "data"
    / "models"
    / "twap_symmetric_v1"
    / "twap_symmetric_v1_manifest.json"
)
START_EQUITY = 80.820665
BASE_FRAC = 0.02


def _load_spot(coin: str) -> tuple[np.ndarray, np.ndarray, pd.Series]:
    frames = []
    pair = PAIRS[coin]
    for day in ("06", "07", "08", "09", "10", "11"):
        archive = TEMP / f"{pair}-1s-2026-08-{day}.zip"
        with zipfile.ZipFile(archive) as zf:
            with zf.open(zf.namelist()[0]) as handle:
                raw = pd.read_csv(handle, header=None, usecols=[0, 1, 4])
        raw.columns = ["open_time", "open", "close"]
        # Binance Vision spot timestamps are microseconds from 2025 onward.
        raw["sec"] = (raw["open_time"].astype(np.int64) // 1_000_000).astype(np.int64)
        frames.append(raw[["sec", "open", "close"]].astype(float))
    spot = pd.concat(frames, ignore_index=True).sort_values("sec")
    sec = spot["sec"].to_numpy(np.int64)
    close = spot["close"].to_numpy(float)
    idx = pd.to_datetime(sec, unit="s", utc=True)
    minute_close = pd.Series(close, index=idx).resample("1min").last()
    # A bar labelled at the boundary closes one minute later. Shift once so
    # boundary RV contains only returns observable by the live scorer.
    rv = np.log(minute_close).diff().rolling(60).std().shift(1)
    return sec, close, rv


def _spot_at(sec: np.ndarray, close: np.ndarray, ts: int) -> float:
    i = int(np.searchsorted(sec, ts, side="right") - 1)
    return float("nan") if i < 0 else float(close[i])


def _trailing_mean_before(
    sec: np.ndarray, close: np.ndarray, boundary_ts: int, window_s: int
) -> float:
    """Arithmetic 1s-close mean over ``[boundary-window, boundary)``."""
    left = int(np.searchsorted(sec, boundary_ts - window_s, side="left"))
    right = int(np.searchsorted(sec, boundary_ts, side="left"))
    if right - left < window_s:
        return float("nan")
    values = close[left:right]
    return float(np.mean(values)) if np.isfinite(values).all() else float("nan")


def _live_twap_z(
    *,
    live_spot: float,
    binance_twap_open: float,
    chainlink_open: float,
    rv_open: float,
    entry_minute: float,
    basis_center_log: float,
    basis_residual_clip_log: float,
) -> float:
    """Mirror ``PartialRetEngine.current_z_from_open`` for offline replay."""
    basis_residual = (
        math.log(binance_twap_open / chainlink_open) - basis_center_log
    )
    basis_residual = max(
        -basis_residual_clip_log,
        min(basis_residual_clip_log, basis_residual),
    )
    return (
        math.log(live_spot / binance_twap_open) + basis_residual
    ) / (rv_open * math.sqrt(entry_minute))


def _fit_frozen_model() -> HistGradientBoostingClassifier:
    train = _production_frame()
    model = HistGradientBoostingClassifier(
        max_iter=100, learning_rate=0.04, max_leaf_nodes=31,
        l2_regularization=0.02, random_state=42,
    )
    model.fit(train[FEATURE_COLUMNS], train["win_twap_s"].astype(np.int8))
    return model


def _feature_rows(
    *,
    gamma_frame: pd.DataFrame | None = None,
    five_minute_window_seconds: int | None = None,
    live_feature_contract: dict | None = None,
) -> tuple[pd.DataFrame, dict[str, Path], dict[str, tuple]]:
    """Build exact-era features with an explicit 5m opening-window contract."""
    gamma = (
        pd.read_parquet(GAMMA)
        if gamma_frame is None
        else gamma_frame.copy()
    )
    if live_feature_contract is None:
        live_feature_contract = json.loads(
            V1_MANIFEST.read_text(encoding="utf-8")
        )["live_feature_contract"]
    contract_windows = live_feature_contract.get("binance_window_seconds") or {}
    contract_5m_window = int(contract_windows.get("5m", 0))
    if five_minute_window_seconds is None:
        five_minute_window_seconds = contract_5m_window
    if five_minute_window_seconds not in (30, 60):
        raise ValueError("five_minute_window_seconds must be 30 or 60")
    if contract_5m_window != five_minute_window_seconds:
        raise ValueError(
            "5m replay window does not match the supplied live feature contract"
        )
    basis_centres = live_feature_contract["basis_center_log"]
    basis_clip = float(live_feature_contract["basis_residual_clip_log"])
    gamma = gamma[
        (gamma["slug_ts"] >= DAY_START) & (gamma["slug_ts"] < DAY_END)
        & gamma["price_to_beat"].notna() & gamma["final_price"].notna()
    ].copy()
    truth = gamma.set_index("market_slug").to_dict("index")
    spots = {coin: _load_spot(coin) for coin in PAIRS}
    paths: dict[str, Path] = {}
    rows = []
    for coin in PAIRS:
        if coin not in basis_centres:
            continue
        sec, close, rv = spots[coin]
        for interval, interval_s in (("5m", 300), ("15m", 900)):
            for path in sorted((SNAP / coin / interval).glob(f"{coin}-updown-{interval}-*.parquet")):
                slug = path.stem
                meta = truth.get(slug)
                if meta is None:
                    continue
                start = int(meta["slug_ts"])
                if not (DAY_START <= start < DAY_END):
                    continue
                frame = pd.read_parquet(path)
                try:
                    book = te.TickBook.from_frame(frame)
                except ValueError:
                    continue
                paths[slug] = path
                grid = pd.Timestamp(start, unit="s", tz="UTC")
                rv_open = float(rv.reindex([grid]).iloc[0])
                if not np.isfinite(rv_open) or rv_open <= 0:
                    continue
                binance_open = _spot_at(sec, close, start)
                binance_twap_open = _trailing_mean_before(
                    sec,
                    close,
                    start,
                    five_minute_window_seconds if interval == "5m" else 60,
                )
                chainlink_open = float(meta["price_to_beat"])
                basis_center = float(basis_centres[coin][interval])
                if (
                    not np.isfinite(binance_open)
                    or binance_open <= 0
                    or not np.isfinite(binance_twap_open)
                    or binance_twap_open <= 0
                ):
                    continue
                off0 = interval_s - 30
                for offset in range(off0, interval_s):
                    score_ts = start + offset
                    i = book.index_at(score_ts + te.DECISION_ANCHOR_SECONDS)
                    if i is None:
                        continue
                    up_asks = book.ask_levels(i, "UP")
                    dn_asks = book.ask_levels(i, "DOWN")
                    if not up_asks or not dn_asks:
                        continue
                    spot = _spot_at(sec, close, score_ts)
                    entry_minute = offset / 60.0
                    z_up = _live_twap_z(
                        live_spot=spot,
                        binance_twap_open=binance_twap_open,
                        chainlink_open=chainlink_open,
                        rv_open=rv_open,
                        entry_minute=entry_minute,
                        basis_center_log=basis_center,
                        basis_residual_clip_log=basis_clip,
                    )
                    hour = float(pd.Timestamp(start, unit="s", tz="UTC").hour)
                    dow = float(pd.Timestamp(start, unit="s", tz="UTC").dayofweek)
                    for side_up, ask, other in (
                        (1, up_asks[0][0], dn_asks[0][0]),
                        (0, dn_asks[0][0], up_asks[0][0]),
                    ):
                        z_side = z_up if side_up else -z_up
                        rows.append({
                            "market_slug": slug, "coin": coin,
                            "interval": interval, "slug_ts": start,
                            "entry_ts": score_ts, "offset_sec": offset,
                            "spot": spot, "binance_open": binance_open,
                            "binance_twap_open": binance_twap_open,
                            "chainlink_open": chainlink_open, "rv_open": rv_open,
                            "side_up": side_up, "ask": ask, "other_ask": other,
                            "book_spread": ask + other - 1.0,
                            "coin_id": float(COIN_ID[coin]),
                            "interval_min": float(interval_s // 60),
                            "entry_minute": entry_minute, "hour_utc": hour,
                            "dow_utc": dow, "partial_ret_z_side": z_side,
                            "abs_partial_ret_z": abs(z_up),
                            "win_true": int(
                                (float(meta["final_price"]) >= chainlink_open)
                                == bool(side_up)
                            ),
                        })
    return pd.DataFrame(rows), paths, spots


def _prepare_scores(
    *,
    gamma_frame: pd.DataFrame | None = None,
    five_minute_window_seconds: int | None = None,
    models_by_interval: dict[str, object] | None = None,
    live_feature_contract: dict | None = None,
) -> tuple[pd.DataFrame, dict[str, Path], dict[str, tuple]]:
    rows, paths, spots = _feature_rows(
        gamma_frame=gamma_frame,
        five_minute_window_seconds=five_minute_window_seconds,
        live_feature_contract=live_feature_contract,
    )
    models = models_by_interval
    if models is None:
        model = _fit_frozen_model()
        models = {"5m": model, "15m": model}
    if set(models) != {"5m", "15m"}:
        raise ValueError("models_by_interval must contain exactly 5m and 15m")
    rows["model_p"] = np.nan
    for interval, model in models.items():
        mask = rows["interval"] == interval
        rows.loc[mask, "model_p"] = model.predict_proba(
            rows.loc[mask, FEATURE_COLUMNS]
        )[:, 1]
    rows["pred_edge"] = rows["model_p"] - rows["ask"] - ps.fee(rows["ask"].to_numpy(float))
    return rows, paths, spots


def _metrics(rows: pd.DataFrame) -> dict:
    filled = rows[rows["entry_filled"]]
    deployed = float(rows["deployed"].sum())
    pnl = float(rows["pnl"].sum())
    ordered_pnl = rows.sort_values(["entry_ts", "market_slug"], kind="mergesort")["pnl"]
    cumulative = ordered_pnl.cumsum()
    running_peak = cumulative.cummax().clip(lower=0.0)
    max_drawdown = float((running_peak - cumulative).max()) if len(cumulative) else 0.0
    return {
        "markets": int(len(rows)), "fills": int(len(filled)),
        "fill_rate": float(len(filled) / len(rows)) if len(rows) else float("nan"),
        "wins": int(filled["win"].sum()),
        "win_rate": float(filled["win"].mean()) if len(filled) else float("nan"),
        "tier_a_n": int(rows["tier_a"].sum()),
        "topup_legs": int(rows["topup_count"].sum()),
        "topup_markets": int((rows["topup_count"] > 0).sum()),
        "orders_including_topups": int(len(filled) + rows["topup_count"].sum()),
        "deployed_usd": deployed, "pnl_usd": pnl,
        "ev_per_filled_trade_usd": float(pnl / len(filled)) if len(filled) else float("nan"),
        "ev_per_dollar": float(pnl / deployed) if deployed else float("nan"),
        "max_drawdown_usd": max_drawdown,
    }


def _replay(
    scored: pd.DataFrame,
    paths: dict[str, Path],
    *,
    tier_mult: float,
    t3: bool,
    coin_filter: set[str] | None = None,
    compound_available_cash: bool = False,
) -> pd.DataFrame:
    @lru_cache(maxsize=128)
    def book(slug: str) -> te.TickBook:
        return te.TickBook.from_frame(pd.read_parquet(paths[slug]))

    score_map = {
        (r.market_slug, int(r.offset_sec), int(r.side_up)): float(r.model_p)
        for r in scored.itertuples()
    }
    out = []
    source = scored if coin_filter is None else scored[scored["coin"].isin(coin_filter)]
    markets = sorted(
        source.groupby("market_slug", sort=False),
        key=lambda item: (int(item[1]["slug_ts"].iloc[0]), item[0]),
    )
    available_cash = START_EQUITY
    pending: list[tuple[int, float]] = []
    for slug, market in markets:
        first_entry_ts = int(market["entry_ts"].min())
        if compound_available_cash:
            settled = [item for item in pending if item[0] <= first_entry_ts]
            available_cash += math.fsum(payout for _, payout in settled)
            pending = [item for item in pending if item[0] > first_entry_ts]
        interval = str(market["interval"].iloc[0])
        spec = SPECS[interval]
        d = market[
            (market["ask"] > spec["ask_lo"]) & (market["ask"] <= spec["ask_hi"])
        ].sort_values(["entry_ts", "pred_edge"], ascending=[True, False])
        d = d.groupby("entry_ts", as_index=False).first()
        d = d[d["pred_edge"] >= spec["gate"]].copy()
        chosen = None
        fill = None
        entry_requested = None
        attempt_rank = 0
        b = book(slug)
        for row in d.itertuples():
            attempt_rank += 1
            tier_a = attempt_rank == 1 and float(row.pred_edge) >= TIER_A_EDGE
            sizing_cash = available_cash if compound_available_cash else START_EQUITY
            base = max(1.0, _floor_cents(sizing_cash * BASE_FRAC))
            requested = _floor_cents(base * (tier_mult if tier_a else 1.0))
            side = "UP" if int(row.side_up) else "DOWN"
            decision_ts = int(row.entry_ts) + te.DECISION_ANCHOR_SECONDS
            di, ei = b.index_at(decision_ts), b.index_at(decision_ts + LATENCY_S)
            if di is None or ei is None:
                continue
            walked = _walk_limit(b.ask_levels(di, side), requested)
            if walked is None:
                continue
            candidate = te.fak_buy(
                b.ask_levels(ei, side), requested,
                min(walked + LIVE_FAK_LIMIT_PAD, 0.99),
            )
            if candidate is not None:
                chosen, fill = row, candidate
                entry_requested = requested
                break
        if chosen is None or fill is None:
            # No filled trade; use the exact market label only for accounting.
            first = market.iloc[0]
            out.append({
                "market_slug": slug, "coin": first.coin, "interval": interval,
                "entry_ts": int(first.entry_ts), "win": 0, "tier_a": False,
                "entry_filled": False, "topup_count": 0,
                "deployed": 0.0, "pnl": 0.0,
            })
            continue
        side_up = int(chosen.side_up)
        side = "UP" if side_up else "DOWN"
        tier_a = attempt_rank == 1 and float(chosen.pred_edge) >= TIER_A_EDGE
        legs = []
        if t3:
            start = int(chosen.slug_ts)
            interval_s = 300 if interval == "5m" else 900
            settlement = start + interval_s
            assert entry_requested is not None
            requested = float(entry_requested)
            stake = max(1.0, _floor_cents(T3_FRAC * requested))
            running_min = None
            disarmed = False
            t = int(math.ceil(int(chosen.entry_ts) + te.DECISION_ANCHOR_SECONDS + LATENCY_S)) + 1
            while t < settlement - T3_END_BUFFER_S and not disarmed:
                i = b.index_at(t)
                if i is None:
                    t += 1
                    continue
                asks, bids = b.ask_levels(i, side), b.bid_levels(i, side)
                ask = asks[0][0] if asks else None
                bid = bids[0][0] if bids else None
                crossed = ask is not None and bid is not None and bid >= ask - 1e-9
                if ask is not None and not crossed:
                    running_min = ask if running_min is None else min(running_min, ask)
                if running_min is not None and running_min < float(chosen.ask) - T3_NO_DIP - 1e-9:
                    disarmed = True
                    break
                current_p = score_map.get((slug, t - start, side_up))
                if current_p is not None and bid is not None and bid < fill.vwap and bid >= current_p + T3_DISARM_BAND:
                    disarmed = True
                    break
                walked = None if not asks else _walk_limit(asks, stake)
                addon_edge = float("-inf") if walked is None else float(chosen.model_p) - walked - ps.fee(walked)
                prior = math.fsum(x.cash_usd for x in legs)
                if (
                    ask is None or bid is None or crossed or walked is None
                    or walked > T3_CAP + 1e-9 or ask - bid > T3_SPREAD_MAX + 1e-9
                    or addon_edge < T3_EDGE_FLOOR or len(legs) >= T3_MAX_N
                    or prior + stake > T3_EXPOSURE_CAP * fill.cash_usd + 1e-9
                ):
                    t += 1
                    continue
                xi = b.index_at(t + LATENCY_S)
                extra = None if xi is None else te.fak_buy(
                    b.ask_levels(xi, side), stake, min(walked + T3_FAK_PAD, 0.99)
                )
                if extra is not None:
                    legs.append(extra)
                t += 1
        cash = fill.cash_usd + math.fsum(x.cash_usd for x in legs)
        shares = fill.shares + math.fsum(x.shares for x in legs)
        fees = te.entry_fee_usd(fill.vwap, fill.shares) + math.fsum(
            te.entry_fee_usd(x.vwap, x.shares) for x in legs
        )
        win = int(chosen.win_true)
        if compound_available_cash:
            available_cash -= cash + fees
            settlement = int(chosen.slug_ts) + (300 if interval == "5m" else 900)
            pending.append((settlement, shares * win))
        out.append({
            "market_slug": slug, "coin": chosen.coin, "interval": interval,
            "entry_ts": int(chosen.entry_ts), "win": win, "tier_a": tier_a,
            "entry_filled": True, "topup_count": len(legs),
            "deployed": cash, "pnl": shares * win - cash - fees,
        })
    return pd.DataFrame(out)


def _paired_hour_bootstrap(base: pd.DataFrame, variant: pd.DataFrame) -> dict:
    x = base[["market_slug", "entry_ts", "pnl", "deployed"]].merge(
        variant[["market_slug", "pnl", "deployed"]], on="market_slug",
        suffixes=("_base", "_variant"), validate="one_to_one",
    )
    x["hour"] = pd.to_datetime(x["entry_ts"], unit="s", utc=True).dt.floor("h")
    x["delta_pnl"] = x["pnl_variant"] - x["pnl_base"]
    x["delta_deployed"] = x["deployed_variant"] - x["deployed_base"]
    blocks = x.groupby("hour").agg(
        delta_pnl=("delta_pnl", "sum"),
        delta_deployed=("delta_deployed", "sum"), n=("market_slug", "size"),
    ).to_numpy(float)
    rng = np.random.default_rng(42)
    draw = blocks[rng.integers(0, len(blocks), size=(10_000, len(blocks)))].sum(axis=1)
    delta = draw[:, 0] / draw[:, 2]
    return {
        "hours": int(len(blocks)), "delta_pnl_usd": float(x["delta_pnl"].sum()),
        "delta_deployed_usd": float(x["delta_deployed"].sum()),
        "p_delta_positive": float(np.mean(delta > 0)),
        "delta_ev_per_market_95pct": [float(v) for v in np.quantile(delta, [0.025, 0.975])],
    }


def _select_both_interval_candidate(replayed: dict[str, pd.DataFrame]) -> dict:
    """Select on Aug 7-10 only; score the chosen contract once on Aug 11."""
    coin_sets = {
        "all_four": {"btc", "eth", "sol", "xrp"},
        "btc_eth": {"btc", "eth"},
        "btc_only": {"btc"},
        "eth_only": {"eth"},
    }
    candidates = []
    for policy, rows in replayed.items():
        dated = rows.copy()
        dated["date"] = pd.to_datetime(
            dated["entry_ts"], unit="s", utc=True
        ).dt.strftime("%Y-%m-%d")
        for coin_name, coins in coin_sets.items():
            dev = dated[
                dated["coin"].isin(coins) & (dated["date"] <= "2026-08-10")
            ]
            per_day = {}
            valid = True
            for day, sub in dev.groupby("date", sort=True):
                m = _metrics(sub)
                per_day[str(day)] = m
                valid &= m["fills"] >= 10 and m["ev_per_dollar"] > 0
            valid &= len(per_day) == 4
            pooled = _metrics(dev)
            candidates.append({
                "policy": policy, "coins": coin_name,
                "intervals": ["5m", "15m"], "valid_all_dev_days": bool(valid),
                "development": pooled, "development_by_day": per_day,
                "min_daily_ev_per_dollar": min(
                    (x["ev_per_dollar"] for x in per_day.values()),
                    default=float("nan"),
                ),
            })
    valid = [x for x in candidates if x["valid_all_dev_days"]]
    if not valid:
        return {"selected": None, "candidates": candidates}
    selected = max(
        valid,
        key=lambda x: (
            x["min_daily_ev_per_dollar"],
            x["development"]["ev_per_dollar"],
            x["development"]["fills"],
        ),
    )
    coins = coin_sets[selected["coins"]]
    holdout_rows = replayed[selected["policy"]]
    holdout_rows = holdout_rows[
        holdout_rows["coin"].isin(coins)
        & (pd.to_datetime(holdout_rows["entry_ts"], unit="s", utc=True)
           .dt.strftime("%Y-%m-%d") == "2026-08-11")
    ]
    selected = dict(selected)
    selected["final_aug11_holdout"] = _metrics(holdout_rows)
    selected["final_aug11_valid"] = bool(
        selected["final_aug11_holdout"]["fills"] >= 10
        and selected["final_aug11_holdout"]["ev_per_dollar"] > 0
    )
    return {"selected": selected, "candidates": candidates}


def main() -> None:
    print("[build] exact Aug-7 features", flush=True)
    scored, paths, _spots = _prepare_scores()
    variants = {
        "base": (1.0, False), "tier_a_1_5x": (TIER_A_MULT, False),
        "t3": (1.0, True), "tier_a_1_5x_plus_t3": (TIER_A_MULT, True),
    }
    replayed = {}
    report = {
        "protocol": {
            "period": DATE, "label": "exact Gamma Chainlink TWAP metadata",
            "model_training_max": "native fold F / 2026-07-21",
            "books": "PMData per-second L2", "spot": "Binance 1-second klines",
            "effective_base_stake_usd": max(1.0, _floor_cents(START_EQUITY * BASE_FRAC)),
            "latency_ms": int(LATENCY_S * 1000), "fak_pad": LIVE_FAK_LIMIT_PAD,
        },
        "feature_rows": int(len(scored)), "markets_with_exact_inputs": int(scored.market_slug.nunique()),
        "variants": {},
    }
    for name, (mult, t3) in variants.items():
        print(f"[replay] {name}", flush=True)
        rows = _replay(scored, paths, tier_mult=mult, t3=t3)
        replayed[name] = rows
        rows.to_parquet(TEMP / f"twap_exact_{name}_20260807_11.parquet", index=False)
        report["variants"][name] = {
            "all": _metrics(rows),
            "by_day": {
                str(k): _metrics(v) for k, v in rows.groupby(
                    pd.to_datetime(rows["entry_ts"], unit="s", utc=True)
                    .dt.strftime("%Y-%m-%d")
                )
            },
            "by_interval": {str(k): _metrics(v) for k, v in rows.groupby("interval")},
            "by_coin": {str(k): _metrics(v) for k, v in rows.groupby("coin")},
        }
    btc_compound = _replay(
        scored,
        paths,
        tier_mult=1.0,
        t3=True,
        coin_filter={"btc"},
        compound_available_cash=True,
    )
    report["btc_t3_2pct_compound_available_cash"] = _metrics(btc_compound)
    report["paired_vs_base"] = {
        name: _paired_hour_bootstrap(replayed["base"], rows)
        for name, rows in replayed.items() if name != "base"
    }
    report["both_interval_selection"] = _select_both_interval_candidate(replayed)
    compact = {
        "protocol": report["protocol"],
        "feature_rows": report["feature_rows"],
        "markets_with_exact_inputs": report["markets_with_exact_inputs"],
        "variants": {
            name: {
                "all": detail["all"], "by_day": detail["by_day"],
                "by_interval": detail["by_interval"], "by_coin": detail["by_coin"],
            }
            for name, detail in report["variants"].items()
        },
        "both_interval_selected": report["both_interval_selection"]["selected"],
        "btc_t3_2pct_compound_available_cash": report["btc_t3_2pct_compound_available_cash"],
    }
    print(json.dumps(compact, indent=2))
    try:
        OUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except PermissionError:
        pass


if __name__ == "__main__":
    main()
