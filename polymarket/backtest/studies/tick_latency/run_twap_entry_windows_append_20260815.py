"""Append Aug 14 PMData to the frozen pooled TWAP entry-window study.

No estimator is fitted here.  The runner loads the deployed two-model bundle,
re-scores the original validation rows, builds non-overlapping current rows,
and replays every shared four-coin entry window at the deployed 1% base sizing.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import joblib
import numpy as np
import pandas as pd

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT
from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.fetch.fetch_gamma_resolutions import (
    GAMMA as GAMMA_ENDPOINT,
    _parse_market,
)
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.studies.tick_latency import (
    run_twap15m60_refit_aug13 as core,
)
from polymarket.backtest.studies.tick_latency.run_twap_t3_tiera_holdout import (
    LATENCY_S,
    LIVE_FAK_LIMIT_PAD,
    _floor_cents,
    _walk_limit,
)
from polymarket.backtest.training.checkpoint import _minute_rv_from_1s


COINS = ("btc", "eth", "sol", "xrp")
PAIRS = {coin: f"{coin.upper()}USDT" for coin in COINS}
COIN_ID = ps.COIN_MAP
FEATURE_COLUMNS = list(core.FEATURE_COLUMNS)
SNAP = BT_DATA_DIR / "pmdata" / "poly_snapshot"
SPOT_ROOT = BT_DATA_DIR / "binance_1s_forward"
SOURCE_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap_pooled_fixed_entry_windows_20260814"
)
FIVE_BASE_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap5m_entry_windows_fresh_20260814"
    / "features_299s"
)
FIVE_TAIL_ROOT = SOURCE_ROOT / "5m_untouched_post1005"
FIFTEEN_BASE_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap15m_entry_windows_20260814"
    / "features_600s"
)
RELEASE_PATH = (
    REPO_ROOT / "polymarket" / "live" / "config" / "twap_symmetric_release.json"
)
MODEL_PATH = (
    REPO_ROOT / "data" / "models" / "twap_symmetric_v1" / "twap_hgb_v1.joblib"
)
OUT_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap_entry_windows_append_20260815"
)
GAMMA_CACHE = OUT_ROOT / "gamma_aug14_exact.parquet"
REPORT_PATH = OUT_ROOT / "report.json"

PERIODS = {
    "5m": {
        "interval_s": 300,
        "old_start": pd.Timestamp("2026-08-14 00:00:00", tz="UTC"),
        "new_start": pd.Timestamp("2026-08-14 14:25:00", tz="UTC"),
        "new_end": pd.Timestamp("2026-08-15 00:00:00", tz="UTC"),
        "windows": (60, 90, 120, 150, 160, 180, 210, 240, 270, 299),
    },
    "15m": {
        "interval_s": 900,
        "old_start": pd.Timestamp("2026-08-07 00:00:00", tz="UTC"),
        "new_start": pd.Timestamp("2026-08-14 00:00:00", tz="UTC"),
        "new_end": pd.Timestamp("2026-08-15 00:00:00", tz="UTC"),
        "windows": (
            60, 90, 120, 150, 180, 210, 240, 270, 300, 360, 420,
            450, 480, 520, 540, 600,
        ),
    },
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _path_ts(path: Path) -> int:
    return int(path.stem.rsplit("-", 1)[1])


def _paths(interval: str, start: pd.Timestamp, end: pd.Timestamp) -> list[Path]:
    lo, hi = int(start.timestamp()), int(end.timestamp())
    out = []
    for coin in COINS:
        for path in (SNAP / coin / interval).glob(
            f"{coin}-updown-{interval}-*.parquet"
        ):
            stamp = _path_ts(path)
            if lo <= stamp < hi:
                out.append(path)
    return sorted(out, key=lambda path: (_path_ts(path), path.stem))


def _fetch_gamma(slugs: list[str], workers: int) -> pd.DataFrame:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    if GAMMA_CACHE.exists():
        cached = pd.read_parquet(GAMMA_CACHE)
    else:
        cached = pd.DataFrame()
    done = set(cached.get("market_slug", pd.Series(dtype=str)).astype(str))
    todo = [slug for slug in slugs if slug not in done]
    if todo:
        def fetch_one(slug: str) -> dict:
            payload = None
            for attempt in range(5):
                try:
                    with httpx.Client(timeout=10.0, http2=True) as client:
                        response = client.get(
                            GAMMA_ENDPOINT,
                            params={"slug": slug, "closed": "true"},
                            headers={
                                "Accept": "application/json",
                                "User-Agent": "cryptobot-entry-window-audit/1.0",
                            },
                        )
                    if response.status_code == 429:
                        time.sleep(2.0 * (attempt + 1))
                        continue
                    response.raise_for_status()
                    payload = response.json()
                    break
                except Exception:  # noqa: BLE001 - bounded research retry
                    time.sleep(1.0 * (attempt + 1))
            return _parse_market(slug, payload)

        chunks = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for start in range(0, len(todo), 200):
                chunk = todo[start : start + 200]
                chunks.extend(executor.map(fetch_one, chunk))
                merged = pd.concat(
                    [cached, pd.DataFrame(chunks)], ignore_index=True
                ).drop_duplicates("market_slug", keep="last")
                merged.to_parquet(GAMMA_CACHE, index=False)
                ok = int(merged["price_to_beat"].notna().sum())
                print(
                    f"[gamma] {min(start + 200, len(todo))}/{len(todo)} "
                    f"new; exact anchors cached={ok}",
                    flush=True,
                )
        cached = pd.read_parquet(GAMMA_CACHE)
    result = cached[cached["market_slug"].isin(slugs)].copy()
    missing = sorted(set(slugs) - set(result.loc[
        result["price_to_beat"].notna(), "market_slug"
    ]))
    if missing:
        raise RuntimeError(f"Gamma lacks exact priceToBeat for {len(missing)} slugs")
    return result


def _load_spot(coin: str) -> pd.DataFrame:
    pair = PAIRS[coin]
    sources = (
        SPOT_ROOT / f"{pair}_2026-08-13_2026-08-14.parquet",
        SPOT_ROOT / f"{pair}_20260814T000000Z_20260815T000000Z.parquet",
    )
    frames = [pd.read_parquet(path, columns=["sec", "open", "close"])
              for path in sources]
    frame = pd.concat(frames, ignore_index=True)
    frame["sec"] = frame["sec"].astype(np.int64)
    frame[["open", "close"]] = frame[["open", "close"]].astype(float)
    return frame.sort_values("sec").drop_duplicates("sec", keep="last")


def _spot_at(sec: np.ndarray, close: np.ndarray, timestamp: int) -> float:
    i = int(np.searchsorted(sec, timestamp, side="right") - 1)
    return float("nan") if i < 0 else float(close[i])


def _trailing_mean(sec: np.ndarray, close: np.ndarray, boundary: int) -> float:
    left = int(np.searchsorted(sec, boundary - 60, side="left"))
    right = int(np.searchsorted(sec, boundary, side="left"))
    values = close[left:right]
    if len(values) < 60 or not np.isfinite(values).all():
        return float("nan")
    return float(values.mean())


def _resolved_up(frame: pd.DataFrame) -> bool | None:
    values = frame["winning_outcome"].dropna().astype(str).str.lower().str.strip()
    if values.empty:
        return None
    if values.iloc[-1] == "up":
        return True
    if values.iloc[-1] == "down":
        return False
    return None


def _build_new_features(
    interval: str,
    paths: list[Path],
    anchors: dict[str, float],
    release: dict,
) -> pd.DataFrame:
    interval_s = PERIODS[interval]["interval_s"]
    widest = max(PERIODS[interval]["windows"])
    off0 = interval_s - widest
    centres = release["live_feature_contract"]["basis_center_log"]
    rows: list[dict] = []
    for coin in COINS:
        spot = _load_spot(coin)
        sec = spot["sec"].to_numpy(np.int64)
        close = spot["close"].to_numpy(float)
        rv = _minute_rv_from_1s(spot)
        coin_paths = [path for path in paths if path.parent.parent.name == coin]
        for number, path in enumerate(coin_paths, start=1):
            start = _path_ts(path)
            slug = path.stem
            grid = pd.Timestamp(start, unit="s", tz="UTC")
            rv_open = float(rv.reindex([grid]).iloc[0])
            binance_open = _trailing_mean(sec, close, start)
            chainlink_open = float(anchors[slug])
            if (
                not np.isfinite(rv_open) or rv_open <= 0
                or not np.isfinite(binance_open) or binance_open <= 0
                or not np.isfinite(chainlink_open) or chainlink_open <= 0
            ):
                continue
            raw = pd.read_parquet(path)
            resolved_up = _resolved_up(raw)
            if resolved_up is None:
                continue
            try:
                book = te.TickBook.from_frame(raw)
            except (TypeError, ValueError):
                continue
            basis_log = math.log(binance_open / chainlink_open)
            stamp = pd.Timestamp(start, unit="s", tz="UTC")
            for offset in range(off0, interval_s):
                entry_ts = start + offset
                i = book.index_at(entry_ts + te.DECISION_ANCHOR_SECONDS)
                if i is None:
                    continue
                spot_now = _spot_at(sec, close, entry_ts)
                if not np.isfinite(spot_now) or spot_now <= 0:
                    continue
                up_asks = book.ask_levels(i, "UP")
                down_asks = book.ask_levels(i, "DOWN")
                if not up_asks or not down_asks:
                    continue
                denom = rv_open * math.sqrt(offset / 60.0)
                if not np.isfinite(denom) or denom <= 0:
                    continue
                log_spot = math.log(spot_now / binance_open)
                for side_up, ask, other in (
                    (1, up_asks[0][0], down_asks[0][0]),
                    (0, down_asks[0][0], up_asks[0][0]),
                ):
                    rows.append({
                        "market_slug": slug,
                        "coin": coin,
                        "interval": interval,
                        "slug_ts": start,
                        "entry_ts": entry_ts,
                        "offset_sec": offset,
                        "side_up": side_up,
                        "ask": float(ask),
                        "other_ask": float(other),
                        "book_spread": float(ask + other - 1.0),
                        "coin_id": float(COIN_ID[coin]),
                        "interval_min": float(interval_s // 60),
                        "entry_minute": offset / 60.0,
                        "hour_utc": float(stamp.hour),
                        "dow_utc": float(stamp.dayofweek),
                        "log_spot_over_binance_open": log_spot,
                        "basis_log": basis_log,
                        "z_denom": denom,
                        "chainlink_open": chainlink_open,
                        "chainlink_close": float("nan"),
                        "win_true": int(resolved_up == bool(side_up)),
                    })
            if number % 50 == 0:
                print(
                    f"[features:{interval}:{coin}] {number}/{len(coin_paths)} markets",
                    flush=True,
                )
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError(f"zero new features for {interval}")
    print(
        f"[features:{interval}] rows={len(frame):,} "
        f"markets={frame['market_slug'].nunique():,}",
        flush=True,
    )
    return frame


def _load_old_features(interval: str) -> pd.DataFrame:
    frames = []
    if interval == "5m":
        for coin in COINS:
            frames.append(pd.read_parquet(
                FIVE_BASE_ROOT / f"forward_features_{coin}.parquet"
            ))
            frames.append(pd.read_parquet(
                FIVE_TAIL_ROOT / f"forward_features_{coin}.parquet"
            ))
    else:
        for coin in COINS:
            frames.append(pd.read_parquet(
                FIFTEEN_BASE_ROOT / f"forward_features_{coin}.parquet"
            ))
    frame = pd.concat(frames, ignore_index=True)
    return frame.drop_duplicates(
        ["market_slug", "offset_sec", "side_up"], keep="last"
    )


def _score(
    raw: pd.DataFrame,
    interval: str,
    model,
    release: dict,
) -> pd.DataFrame:
    out = raw.copy()
    centres = release["live_feature_contract"]["basis_center_log"]
    clip = float(release["live_feature_contract"]["basis_residual_clip_log"])
    centre = np.array([
        float(centres[str(coin)][interval]) for coin in out["coin"]
    ])
    residual = np.clip(out["basis_log"].to_numpy(float) - centre, -clip, clip)
    z_up = (
        out["log_spot_over_binance_open"].to_numpy(float) + residual
    ) / out["z_denom"].to_numpy(float)
    out["partial_ret_z_side"] = np.where(
        out["side_up"].to_numpy(np.int8) == 1, z_up, -z_up
    )
    out["abs_partial_ret_z"] = np.abs(z_up)
    out["model_p"] = model.predict_proba(out[FEATURE_COLUMNS])[:, 1]
    out["pred_edge"] = (
        out["model_p"] - out["ask"] - ps.fee(out["ask"].to_numpy(float))
    )
    return out


def _empty_trade(market: pd.DataFrame, period: str) -> dict:
    first = market.iloc[0]
    return {
        "market_slug": first.market_slug,
        "coin": first.coin,
        "interval": first.interval,
        "entry_ts": int(first.entry_ts),
        "win": 0,
        "tier_a": False,
        "entry_filled": False,
        "topup_count": 0,
        "deployed": 0.0,
        "pnl": 0.0,
        "period": period,
    }


def _replay_all_windows(
    scored: pd.DataFrame,
    paths: dict[str, Path],
    interval: str,
) -> dict[str, dict[int, pd.DataFrame]]:
    spec = PERIODS[interval]
    interval_s = int(spec["interval_s"])
    windows = tuple(spec["windows"])
    execution = {"ask_lo": 0.55, "ask_hi": 0.85, "gate": 0.10}
    requested = max(1.0, _floor_cents(exact.START_EQUITY * 0.01))
    out: dict[str, dict[int, list[dict]]] = {
        variant: {window: [] for window in windows}
        for variant in ("base", "topup")
    }
    markets = sorted(
        scored.groupby("market_slug", sort=False),
        key=lambda item: (int(item[1]["slug_ts"].iloc[0]), item[0]),
    )
    for number, (slug, market) in enumerate(markets, start=1):
        period = str(market["period"].iloc[0])
        raw_book = pd.read_parquet(paths[slug])
        try:
            book = te.TickBook.from_frame(raw_book)
        except (TypeError, ValueError):
            for window in windows:
                for variant in out:
                    out[variant][window].append(_empty_trade(market, period))
            continue
        score_map = {
            (int(row.offset_sec), int(row.side_up)): float(row.model_p)
            for row in market.itertuples()
        }
        candidates = market[
            (market["ask"] > execution["ask_lo"])
            & (market["ask"] <= execution["ask_hi"])
        ].sort_values(
            ["entry_ts", "pred_edge"], ascending=[True, False], kind="mergesort"
        )
        candidates = candidates.groupby("entry_ts", as_index=False).first()
        candidates = candidates[candidates["pred_edge"] >= execution["gate"]]
        for window in windows:
            lo = interval_s - window
            chosen = None
            fill = None
            attempt_rank = 0
            for row in candidates[candidates["offset_sec"] >= lo].itertuples():
                attempt_rank += 1
                side = "UP" if int(row.side_up) else "DOWN"
                decision_ts = int(row.entry_ts) + te.DECISION_ANCHOR_SECONDS
                di = book.index_at(decision_ts)
                ei = book.index_at(decision_ts + LATENCY_S)
                if di is None or ei is None:
                    continue
                walked = _walk_limit(book.ask_levels(di, side), requested)
                if walked is None:
                    continue
                candidate = te.fak_buy(
                    book.ask_levels(ei, side),
                    requested,
                    min(walked + LIVE_FAK_LIMIT_PAD, 0.99),
                )
                if candidate is not None:
                    chosen, fill = row, candidate
                    break
            if chosen is None or fill is None:
                for variant in out:
                    out[variant][window].append(_empty_trade(market, period))
                continue
            side_up = int(chosen.side_up)
            side = "UP" if side_up else "DOWN"
            win = int(chosen.win_true)
            tier_a = bool(
                attempt_rank == 1 and float(chosen.pred_edge) >= exact.TIER_A_EDGE
            )
            variants = {"base": [], "topup": []}
            stake = max(1.0, _floor_cents(exact.T3_FRAC * requested))
            running_min = None
            disarmed = False
            settlement = int(chosen.slug_ts) + interval_s
            timestamp = int(math.ceil(
                int(chosen.entry_ts) + te.DECISION_ANCHOR_SECONDS + LATENCY_S
            )) + 1
            while timestamp < settlement - exact.T3_END_BUFFER_S and not disarmed:
                i = book.index_at(timestamp)
                if i is None:
                    timestamp += 1
                    continue
                asks = book.ask_levels(i, side)
                bids = book.bid_levels(i, side)
                ask = asks[0][0] if asks else None
                bid = bids[0][0] if bids else None
                crossed = (
                    ask is not None and bid is not None and bid >= ask - 1e-9
                )
                if ask is not None and not crossed:
                    running_min = ask if running_min is None else min(running_min, ask)
                if (
                    running_min is not None
                    and running_min < float(chosen.ask) - exact.T3_NO_DIP - 1e-9
                ):
                    disarmed = True
                    break
                current_p = score_map.get((timestamp - int(chosen.slug_ts), side_up))
                if (
                    current_p is not None
                    and bid is not None
                    and bid < fill.vwap
                    and bid >= current_p + exact.T3_DISARM_BAND
                ):
                    disarmed = True
                    break
                walked = None if not asks else _walk_limit(asks, stake)
                addon_edge = (
                    float("-inf")
                    if walked is None
                    else float(chosen.model_p) - walked - ps.fee(walked)
                )
                prior = math.fsum(leg.cash_usd for leg in variants["topup"])
                if (
                    ask is None
                    or bid is None
                    or crossed
                    or walked is None
                    or walked > exact.T3_CAP + 1e-9
                    or ask - bid > exact.T3_SPREAD_MAX + 1e-9
                    or addon_edge < exact.T3_EDGE_FLOOR
                    or len(variants["topup"]) >= exact.T3_MAX_N
                    or prior + stake > exact.T3_EXPOSURE_CAP * fill.cash_usd + 1e-9
                ):
                    timestamp += 1
                    continue
                execution_i = book.index_at(timestamp + LATENCY_S)
                extra = None if execution_i is None else te.fak_buy(
                    book.ask_levels(execution_i, side),
                    stake,
                    min(walked + exact.T3_FAK_PAD, 0.99),
                )
                if extra is not None:
                    variants["topup"].append(extra)
                timestamp += 1
            for variant, legs in variants.items():
                cash = fill.cash_usd + math.fsum(leg.cash_usd for leg in legs)
                shares = fill.shares + math.fsum(leg.shares for leg in legs)
                fees = te.entry_fee_usd(fill.vwap, fill.shares) + math.fsum(
                    te.entry_fee_usd(leg.vwap, leg.shares) for leg in legs
                )
                out[variant][window].append({
                    "market_slug": slug,
                    "coin": chosen.coin,
                    "interval": interval,
                    "entry_ts": int(chosen.entry_ts),
                    "win": win,
                    "tier_a": tier_a,
                    "entry_filled": True,
                    "topup_count": len(legs),
                    "deployed": float(cash),
                    "pnl": float(shares * win - cash - fees),
                    "period": period,
                })
        if number % 250 == 0:
            print(
                f"[replay:{interval}] {number}/{len(markets)} markets",
                flush=True,
            )
    return {
        variant: {
            window: pd.DataFrame(rows) for window, rows in by_window.items()
        }
        for variant, by_window in out.items()
    }


def _metrics(rows: pd.DataFrame) -> dict:
    value = exact._metrics(rows)
    return {
        "markets": value["markets"],
        "trades": value["fills"],
        "wins": value["wins"],
        "win_rate": value["win_rate"],
        "pnl_usd": value["pnl_usd"],
        "ev_per_trade_usd": value["ev_per_filled_trade_usd"],
        "max_drawdown_usd": value["max_drawdown_usd"],
    }


def _validate_old(
    interval: str,
    rows: pd.DataFrame,
    release: dict,
    published_variant: str,
) -> None:
    selected = int(release["decision_window_seconds"][interval])
    published = release["validation"][interval][published_variant]
    full_actual = exact._metrics(rows)
    actual = _metrics(rows)
    for key, published_key in (
        ("markets", "markets"), ("trades", "fills"), ("wins", "wins")
    ):
        if int(actual[key]) != int(published[published_key]):
            raise AssertionError(
                f"{interval} T-{selected} old {key} drift: "
                f"{actual[key]} != {published[published_key]}"
            )
    for key in ("pnl_usd", "max_drawdown_usd"):
        if not math.isclose(actual[key], float(published[key]), abs_tol=1e-10):
            raise AssertionError(
                f"{interval} T-{selected} old {key} drift: "
                f"{actual[key]} != {published[key]}"
            )
    for key in ("topup_legs", "topup_markets"):
        if int(full_actual[key]) != int(published[key]):
            raise AssertionError(
                f"{interval} T-{selected} old {published_variant} {key} drift: "
                f"{full_actual[key]} != {published[key]}"
            )


def _run_interval(
    interval: str,
    model,
    release: dict,
    gamma: pd.DataFrame,
) -> dict:
    spec = PERIODS[interval]
    old = _load_old_features(interval)
    new_paths = _paths(interval, spec["new_start"], spec["new_end"])
    anchors = dict(zip(gamma["market_slug"], gamma["price_to_beat"]))
    new_cache = OUT_ROOT / f"new_features_{interval}.parquet"
    if new_cache.exists():
        new = pd.read_parquet(new_cache)
    else:
        new = _build_new_features(interval, new_paths, anchors, release)
        new.to_parquet(new_cache, index=False)
    old["period"] = "old"
    new["period"] = "today"
    scored = _score(pd.concat([old, new], ignore_index=True), interval, model, release)
    all_paths = {
        path.stem: path for path in _paths(
            interval, spec["old_start"], spec["new_end"]
        )
    }
    replayed = _replay_all_windows(scored, all_paths, interval)
    selected = int(release["decision_window_seconds"][interval])
    _validate_old(
        interval,
        replayed["base"][selected][
            replayed["base"][selected]["period"] == "old"
        ],
        release,
        "base",
    )
    _validate_old(
        interval,
        replayed["topup"][selected][
            replayed["topup"][selected]["period"] == "old"
        ],
        release,
        "t3",
    )
    comparison = []
    window_metrics = {}
    for window in spec["windows"]:
        rows = replayed["base"][window]
        topup_rows = replayed["topup"][window]
        old_rows = rows[rows["period"] == "old"].copy()
        new_rows = rows[rows["period"] == "today"].copy()
        topup_old = topup_rows[topup_rows["period"] == "old"].copy()
        topup_new = topup_rows[topup_rows["period"] == "today"].copy()
        body = {
            "window_seconds": window,
            "entry_start_second": int(spec["interval_s"] - window),
            "old": _metrics(old_rows),
            "today": _metrics(new_rows),
            "combined": _metrics(rows),
            "with_topups": {
                "old": _metrics(topup_old),
                "today": _metrics(topup_new),
                "combined": _metrics(topup_rows),
                "combined_topup_legs": int(topup_rows["topup_count"].sum()),
                "combined_topup_markets": int(
                    (topup_rows["topup_count"] > 0).sum()
                ),
            },
        }
        comparison.append(body)
        window_metrics[f"{window}s"] = body
        rows.to_parquet(OUT_ROOT / f"{interval}_window_{window}s.parquet", index=False)
        topup_rows.to_parquet(
            OUT_ROOT / f"{interval}_window_{window}s_topups.parquet", index=False
        )
        print(
            f"[result:{interval}:T-{window}] "
            f"base={body['combined']['pnl_usd']:+.2f} "
            f"topup={body['with_topups']['combined']['pnl_usd']:+.2f} "
            f"topup_today={body['with_topups']['today']['pnl_usd']:+.2f}",
            flush=True,
        )
    ranked = sorted(
        comparison,
        key=lambda body: body["combined"]["pnl_usd"],
        reverse=True,
    )
    topup_ranked = sorted(
        comparison,
        key=lambda body: body["with_topups"]["combined"]["pnl_usd"],
        reverse=True,
    )
    return {
        "interval": interval,
        "old_period": {
            "start": spec["old_start"].isoformat(),
            "end_exclusive": spec["new_start"].isoformat(),
        },
        "today_period": {
            "start": spec["new_start"].isoformat(),
            "end_exclusive": spec["new_end"].isoformat(),
        },
        "windows": window_metrics,
        "ranking_by_combined_pnl": [body["window_seconds"] for body in ranked],
        "best_combined_window_seconds": ranked[0]["window_seconds"],
        "topup_ranking_by_combined_pnl": [
            body["window_seconds"] for body in topup_ranked
        ],
        "best_topup_combined_window_seconds": topup_ranked[0]["window_seconds"],
        "deployed_window_seconds": selected,
        "deployed_rank_by_combined_pnl": (
            [body["window_seconds"] for body in ranked].index(selected) + 1
        ),
        "deployed_rank_by_topup_combined_pnl": (
            [body["window_seconds"] for body in topup_ranked].index(selected) + 1
        ),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args(argv)
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    release = json.loads(RELEASE_PATH.read_text(encoding="utf-8"))
    model_hash = _sha256(MODEL_PATH)
    if model_hash != release["model_sha256"]:
        raise AssertionError("deployed model hash does not match release manifest")
    models = joblib.load(MODEL_PATH)
    if set(models) != {"5m", "15m"}:
        raise AssertionError("deployed bundle does not contain exactly two interval models")
    requested_paths = []
    for interval, spec in PERIODS.items():
        requested_paths.extend(_paths(interval, spec["new_start"], spec["new_end"]))
    slugs = sorted({path.stem for path in requested_paths})
    gamma = _fetch_gamma(slugs, args.workers)
    started = time.time()
    intervals = {
        interval: _run_interval(interval, models[interval], release, gamma)
        for interval in ("5m", "15m")
    }
    report = {
        "artifact": "twap_entry_windows_append_20260815",
        "status": "research_only_not_deployed",
        "model_refits": 0,
        "model_sha256": model_hash,
        "pooled_across_all_four_coins": True,
        "shared_window_across_all_four_coins": True,
        "twap_seconds": 60,
        "base_equity_fraction": 0.01,
        "effective_base_stake_usd": max(
            1.0, _floor_cents(exact.START_EQUITY * 0.01)
        ),
        "execution": {
            "ask_lo": 0.55,
            "ask_hi": 0.85,
            "edge_gate": 0.10,
            "variants": ["base_only", "t3_topups"],
            "fak_pad": LIVE_FAK_LIMIT_PAD,
        },
        "new_exact_gamma_anchors": int(gamma["price_to_beat"].notna().sum()),
        "intervals": intervals,
        "elapsed_seconds_excluding_gamma_fetch": round(time.time() - started, 2),
    }
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
