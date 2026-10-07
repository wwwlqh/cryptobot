"""Leak-free 15m / 60-second TWAP refit and Aug 7-13 forward replay.

Development begins at the first locally available ordinary Chainlink tape:
BTC on 2026-04-22 and ETH/SOL/XRP on 2026-06-07.  Historical labels are a
trailing arithmetic 60-second proxy reconstructed from that tape.  Models and
entry thresholds are selected with expanding chronological WFO and a one-hour
embargo, then frozen before the exact Gamma/Chainlink 60s Aug 7-13 holdout is
opened.

Only the YES-token PMData book is consumed.  ``TickBook`` constructs the DOWN
side from the symmetric book identity (DOWN ask = 1 - YES bid), so no separate
NO-token book enters either training or execution.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import time
import zipfile
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingClassifier

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT
from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.scripts.build_twap_labels import mean_between
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.studies.tick_latency.run_twap_t3_tiera_holdout import (
    LATENCY_S,
    LIVE_FAK_LIMIT_PAD,
    _floor_cents,
    _walk_limit,
)
from polymarket.backtest.training.checkpoint import _minute_rv_from_1s
from polymarket.backtest.training.offset_wfo import _load_1s


COINS = ("btc", "eth", "sol", "xrp")
SYMBOLS = {"btc": "BTCUSD", "eth": "ETHUSD", "sol": "SOLUSD", "xrp": "XRPUSD"}
PAIRS = {coin: f"{coin.upper()}USDT" for coin in COINS}
COIN_ID = ps.COIN_MAP
TRAIN_START = {
    "btc": pd.Timestamp("2026-04-22 12:30:00", tz="UTC"),
    "eth": pd.Timestamp("2026-06-07", tz="UTC"),
    "sol": pd.Timestamp("2026-06-07", tz="UTC"),
    "xrp": pd.Timestamp("2026-06-07", tz="UTC"),
}
TRAIN_END = pd.Timestamp("2026-07-21 07:00:00", tz="UTC")
FORWARD_START = pd.Timestamp("2026-08-07", tz="UTC")
FORWARD_END = pd.Timestamp("2026-08-14", tz="UTC")
INTERVAL = "15m"
INTERVAL_MIN = 15
INTERVAL_S = INTERVAL_MIN * 60
OFFSETS = range(INTERVAL_S - 60, INTERVAL_S)
TWAP_S = 60
EMBARGO_S = 3600
BASIS_CLIP_LOG = 0.0001
START_EQUITY = exact.START_EQUITY
BASE_STAKE = max(1.0, _floor_cents(START_EQUITY * exact.BASE_FRAC))
MIN_TICKS = 50
WAD = 1e18

PMDATA = BT_DATA_DIR / "pmdata"
SNAP = PMDATA / "poly_snapshot"
CHAINLINK = PMDATA / "chainlink_streams"
DIRECT_TWAP = PMDATA / "streams_twap60s"
GAMMA = PMDATA / "gamma_twap_15m_20260807_20260813.parquet"
FORWARD_SPOT = BT_DATA_DIR / "binance_1s_forward"
TEMP = Path.home() / "AppData" / "Local" / "Temp"
OUT_ROOT = (
    REPO_ROOT
    / "polymarket"
    / "backtest"
    / "optimize_runs"
    / "twap15m60_refit_20260814"
)
REPORT_PATH = OUT_ROOT / "report.json"

FEATURE_COLUMNS = [
    "ask",
    "other_ask",
    "book_spread",
    "coin_id",
    "interval_min",
    "side_up",
    "entry_minute",
    "hour_utc",
    "dow_utc",
    "partial_ret_z_side",
    "abs_partial_ret_z",
]
ASK_BANDS = (
    (0.55, 0.85),
    (0.55, 0.90),
    (0.55, 0.95),
    (0.60, 0.85),
    (0.60, 0.90),
    (0.60, 0.95),
    (0.65, 0.85),
    (0.65, 0.90),
    (0.65, 0.95),
)
EDGE_GATES = (0.03, 0.04, 0.05, 0.06, 0.08, 0.10)


def _epoch(value: pd.Timestamp) -> int:
    return int(value.timestamp())


def _path_slug_ts(path: Path) -> int:
    return int(path.stem.rsplit("-", 1)[1])


def _load_pm_tape(root: Path, coin: str, start: pd.Timestamp, end: pd.Timestamp) -> tuple[np.ndarray, np.ndarray]:
    symbol = SYMBOLS[coin]
    parts = []
    day = (start - pd.Timedelta(days=1)).date()
    last = (end + pd.Timedelta(days=1)).date()
    infix = f"streams_twap{TWAP_S}s" if root == DIRECT_TWAP else "streams"
    while day <= last:
        path = root / symbol / f"{symbol}_{infix}_{day.isoformat()}.parquet"
        if path.exists():
            frame = pd.read_parquet(path, columns=["observationsTimestamp", "price"])
            ts = pd.to_datetime(frame["observationsTimestamp"], utc=True, errors="coerce")
            px = pd.to_numeric(frame["price"], errors="coerce") / WAD
            good = ts.notna() & px.notna() & (px > 0)
            if good.any():
                # Parquet currently loads these timestamps at microsecond
                # resolution.  Converting explicitly to seconds avoids the
                # unit-dependent integer representation in pandas 3.x.
                parts.append(
                    pd.DataFrame(
                        {
                            "sec": ts[good].dt.as_unit("s").astype("int64"),
                            "price": px[good],
                        }
                    )
                )
        day += dt.timedelta(days=1)
    if not parts:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=float)
    tape = pd.concat(parts, ignore_index=True).sort_values("sec").drop_duplicates("sec", keep="last")
    return tape["sec"].to_numpy(np.int64), tape["price"].to_numpy(float)


def _spot_at(sec: np.ndarray, close: np.ndarray, timestamp: int) -> float:
    i = int(np.searchsorted(sec, timestamp, side="right") - 1)
    return float("nan") if i < 0 else float(close[i])


def _trailing_binance_mean(sec: np.ndarray, close: np.ndarray, boundary: int) -> float:
    left = int(np.searchsorted(sec, boundary - TWAP_S, side="left"))
    right = int(np.searchsorted(sec, boundary, side="left"))
    values = close[left:right]
    if len(values) < TWAP_S or not np.isfinite(values).all():
        return float("nan")
    return float(values.mean())


def _chainlink_mean(sec: np.ndarray, price: np.ndarray, boundary: int) -> tuple[float, int]:
    return mean_between(sec, price, boundary - TWAP_S, boundary, False, True)


def _load_forward_spot(coin: str) -> pd.DataFrame:
    pair = PAIRS[coin]
    frames = []
    for day in range(6, 13):
        archive = TEMP / f"{pair}-1s-2026-08-{day:02d}.zip"
        if not archive.exists():
            continue
        with zipfile.ZipFile(archive) as zf:
            with zf.open(zf.namelist()[0]) as handle:
                raw = pd.read_csv(handle, header=None, usecols=[0, 1, 4])
        raw.columns = ["open_time", "open", "close"]
        divisor = 1_000_000 if float(raw["open_time"].iloc[0]) > 1e15 else 1000
        raw["sec"] = (pd.to_numeric(raw["open_time"]) // divisor).astype(np.int64)
        frames.append(raw[["sec", "open", "close"]])
    parquet = FORWARD_SPOT / f"{pair}_2026-08-13_2026-08-14.parquet"
    if parquet.exists():
        frames.append(pd.read_parquet(parquet, columns=["sec", "open", "close"]))
    if not frames:
        raise FileNotFoundError(f"no forward Binance tape for {coin}")
    spot = pd.concat(frames, ignore_index=True)
    spot[["open", "close"]] = spot[["open", "close"]].astype(float)
    spot["sec"] = spot["sec"].astype(np.int64)
    return spot.sort_values("sec").drop_duplicates("sec", keep="last").reset_index(drop=True)


def _book_paths(coin: str, start: pd.Timestamp, end: pd.Timestamp) -> list[Path]:
    lo, hi = _epoch(start), _epoch(end)
    paths = []
    for path in (SNAP / coin / INTERVAL).glob(f"{coin}-updown-{INTERVAL}-*.parquet"):
        stamp = _path_slug_ts(path)
        if lo <= stamp < hi:
            paths.append(path)
    return sorted(paths, key=_path_slug_ts)


def _market_rows(
    *,
    coin: str,
    path: Path,
    spot_sec: np.ndarray,
    spot_close: np.ndarray,
    rv: pd.Series,
    chainlink_open: float,
    chainlink_close: float,
) -> list[dict]:
    start = _path_slug_ts(path)
    slug = path.stem
    grid = pd.Timestamp(start, unit="s", tz="UTC")
    rv_open = float(rv.reindex([grid]).iloc[0])
    binance_open = _trailing_binance_mean(spot_sec, spot_close, start)
    if not np.isfinite(rv_open) or rv_open <= 0 or not np.isfinite(binance_open) or binance_open <= 0:
        return []
    try:
        book = te.TickBook.from_frame(pd.read_parquet(path))
    except (ValueError, TypeError):
        return []
    basis_log = math.log(binance_open / chainlink_open)
    won_up = int(chainlink_close >= chainlink_open)
    stamp = pd.Timestamp(start, unit="s", tz="UTC")
    rows = []
    for offset in OFFSETS:
        score_ts = start + offset
        decision_ts = score_ts + te.DECISION_ANCHOR_SECONDS
        di = book.index_at(decision_ts)
        ei = book.index_at(decision_ts + LATENCY_S)
        if di is None or ei is None:
            continue
        spot = _spot_at(spot_sec, spot_close, score_ts)
        if not np.isfinite(spot) or spot <= 0:
            continue
        up_asks = book.ask_levels(di, "UP")
        down_asks = book.ask_levels(di, "DOWN")
        if not up_asks or not down_asks:
            continue
        denom = rv_open * math.sqrt(offset / 60.0)
        if not np.isfinite(denom) or denom <= 0:
            continue
        log_spot = math.log(spot / binance_open)
        for side_up, asks, other in ((1, up_asks, down_asks), (0, down_asks, up_asks)):
            side = "UP" if side_up else "DOWN"
            walked = _walk_limit(asks, BASE_STAKE)
            fill = None
            if walked is not None:
                fill = te.fak_buy(
                    book.ask_levels(ei, side),
                    BASE_STAKE,
                    min(walked + LIVE_FAK_LIMIT_PAD, 0.99),
                )
            rows.append(
                {
                    "market_slug": slug,
                    "coin": coin,
                    "interval": INTERVAL,
                    "slug_ts": start,
                    "entry_ts": score_ts,
                    "offset_sec": offset,
                    "side_up": side_up,
                    "ask": float(asks[0][0]),
                    "other_ask": float(other[0][0]),
                    "book_spread": float(asks[0][0] + other[0][0] - 1.0),
                    "coin_id": float(COIN_ID[coin]),
                    "interval_min": float(INTERVAL_MIN),
                    "entry_minute": offset / 60.0,
                    "hour_utc": float(stamp.hour),
                    "dow_utc": float(stamp.dayofweek),
                    "log_spot_over_binance_open": log_spot,
                    "basis_log": basis_log,
                    "z_denom": denom,
                    "chainlink_open": chainlink_open,
                    "chainlink_close": chainlink_close,
                    "win_true": int(won_up == side_up),
                    "pre_filled": fill is not None,
                    "fill_cash": 0.0 if fill is None else float(fill.cash_usd),
                    "fill_shares": 0.0 if fill is None else float(fill.shares),
                    "fill_fee": 0.0 if fill is None else float(te.entry_fee_usd(fill.vwap, fill.shares)),
                }
            )
    return rows


def _write_feature_cache(
    coin: str,
    kind: str,
    labels: dict[str, tuple[float, float]],
    spot: pd.DataFrame,
    paths: list[Path],
    target: Path,
) -> None:
    sec = spot["sec"].to_numpy(np.int64)
    close = spot["close"].to_numpy(float)
    rv = _minute_rv_from_1s(spot)
    writer = None
    buffer: list[dict] = []
    markets = 0
    try:
        for path in paths:
            pair = labels.get(path.stem)
            if pair is None:
                continue
            rows = _market_rows(
                coin=coin,
                path=path,
                spot_sec=sec,
                spot_close=close,
                rv=rv,
                chainlink_open=float(pair[0]),
                chainlink_close=float(pair[1]),
            )
            if not rows:
                continue
            buffer.extend(rows)
            markets += 1
            if markets % 250 == 0:
                table = pa.Table.from_pandas(pd.DataFrame(buffer), preserve_index=False)
                if writer is None:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(target, table.schema, compression="zstd")
                writer.write_table(table)
                buffer.clear()
                print(f"  [{kind}:{coin}] {markets:,} markets", flush=True)
        if buffer:
            table = pa.Table.from_pandas(pd.DataFrame(buffer), preserve_index=False)
            if writer is None:
                target.parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(target, table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise RuntimeError(f"zero feature rows for {kind}:{coin}")


def _training_labels(coin: str, paths: list[Path]) -> dict[str, tuple[float, float]]:
    sec, price = _load_pm_tape(CHAINLINK, coin, TRAIN_START[coin], TRAIN_END)
    labels = {}
    for path in paths:
        start = _path_slug_ts(path)
        opening, n_open = _chainlink_mean(sec, price, start)
        closing, n_close = _chainlink_mean(sec, price, start + INTERVAL_S)
        if n_open >= MIN_TICKS and n_close >= MIN_TICKS and opening > 0 and closing > 0:
            labels[path.stem] = (opening, closing)
    return labels


def _forward_labels(coin: str, gamma: pd.DataFrame) -> dict[str, tuple[float, float]]:
    sub = gamma[(gamma["coin"] == coin) & gamma["price_to_beat"].notna() & gamma["final_price"].notna()]
    return {
        str(row.market_slug): (float(row.price_to_beat), float(row.final_price))
        for row in sub.itertuples()
    }


def _with_features(frame: pd.DataFrame, basis_center: float) -> pd.DataFrame:
    out = frame.copy()
    residual = np.clip(out["basis_log"].to_numpy(float) - basis_center, -BASIS_CLIP_LOG, BASIS_CLIP_LOG)
    z_up = (out["log_spot_over_binance_open"].to_numpy(float) + residual) / out["z_denom"].to_numpy(float)
    out["partial_ret_z_side"] = np.where(out["side_up"].to_numpy(np.int8) == 1, z_up, -z_up)
    out["abs_partial_ret_z"] = np.abs(z_up)
    return out


def _fit(train: pd.DataFrame, basis_center: float) -> HistGradientBoostingClassifier:
    featured = _with_features(train, basis_center)
    model = HistGradientBoostingClassifier(
        max_iter=100,
        learning_rate=0.04,
        max_leaf_nodes=31,
        l2_regularization=0.02,
        random_state=42,
    )
    model.fit(featured[FEATURE_COLUMNS], featured["win_true"].astype(np.int8))
    return model


def _score(model: HistGradientBoostingClassifier, frame: pd.DataFrame, basis_center: float) -> pd.DataFrame:
    scored = _with_features(frame, basis_center)
    scored["model_p"] = model.predict_proba(scored[FEATURE_COLUMNS])[:, 1]
    scored["pred_edge"] = scored["model_p"] - scored["ask"] - ps.fee(scored["ask"].to_numpy(float))
    return scored


def _prefilled_trades(scored: pd.DataFrame, ask_lo: float, ask_hi: float, gate: float) -> pd.DataFrame:
    candidate = scored[
        (scored["ask"] > ask_lo)
        & (scored["ask"] <= ask_hi)
        & (scored["pred_edge"] >= gate)
    ].copy()
    if candidate.empty:
        return candidate
    # The scorer selects the best side once per second.  A failed FAK proceeds
    # to the next second, not to the other side at the same timestamp.
    candidate = candidate.sort_values(
        ["market_slug", "offset_sec", "pred_edge"],
        ascending=[True, True, False],
        kind="mergesort",
    ).groupby(["market_slug", "offset_sec"], as_index=False).first()
    candidate = candidate[candidate["pre_filled"]].sort_values(
        ["market_slug", "offset_sec"], kind="mergesort"
    ).groupby("market_slug", as_index=False).first()
    if candidate.empty:
        return candidate
    candidate["pnl"] = (
        candidate["fill_shares"] * candidate["win_true"]
        - candidate["fill_cash"]
        - candidate["fill_fee"]
    )
    return candidate


def _trade_metrics(trades: pd.DataFrame) -> dict:
    n = len(trades)
    deployed = float(trades["fill_cash"].sum()) if n else 0.0
    pnl = float(trades["pnl"].sum()) if n else 0.0
    return {
        "trades": int(n),
        "wins": int(trades["win_true"].sum()) if n else 0,
        "win_rate": float(trades["win_true"].mean()) if n else None,
        "deployed_usd": deployed,
        "pnl_usd": pnl,
        "ev_per_trade_usd": pnl / n if n else None,
        "ev_per_dollar": pnl / deployed if deployed else None,
    }


def _wfo(coin: str, raw: pd.DataFrame) -> tuple[dict, HistGradientBoostingClassifier, float, pd.DataFrame]:
    markets = raw[["market_slug", "slug_ts"]].drop_duplicates().sort_values("slug_ts")
    chunks = np.array_split(markets["market_slug"].to_numpy(), 4)
    oos_parts = []
    fold_meta = []
    for fold_no in range(1, 4):
        test_slugs = set(chunks[fold_no].tolist())
        test = raw[raw["market_slug"].isin(test_slugs)].copy()
        test_start = int(test["slug_ts"].min())
        train = raw[raw["slug_ts"] < test_start - EMBARGO_S].copy()
        center = float(train[["market_slug", "basis_log"]].drop_duplicates()["basis_log"].median())
        print(f"  [wfo:{coin}] fold {fold_no} fit {len(train):,} rows -> {len(test):,}", flush=True)
        model = _fit(train, center)
        scored = _score(model, test, center)
        scored["wfo_fold"] = fold_no
        oos_parts.append(scored)
        fold_meta.append(
            {
                "fold": fold_no,
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "test_markets": int(test["market_slug"].nunique()),
                "test_start": pd.to_datetime(test["slug_ts"].min(), unit="s", utc=True).isoformat(),
                "test_end": pd.to_datetime(test["slug_ts"].max(), unit="s", utc=True).isoformat(),
                "basis_center_log": center,
            }
        )
    oos = pd.concat(oos_parts, ignore_index=True)
    candidates = []
    for ask_lo, ask_hi in ASK_BANDS:
        for gate in EDGE_GATES:
            per_fold = {}
            valid = True
            for fold_no in range(1, 4):
                trades = _prefilled_trades(oos[oos["wfo_fold"] == fold_no], ask_lo, ask_hi, gate)
                metrics = _trade_metrics(trades)
                per_fold[str(fold_no)] = metrics
                valid &= metrics["trades"] >= 20 and metrics["ev_per_dollar"] is not None and metrics["ev_per_dollar"] > 0
            pooled = _trade_metrics(_prefilled_trades(oos, ask_lo, ask_hi, gate))
            min_ev = min(
                (m["ev_per_dollar"] if m["ev_per_dollar"] is not None else -1.0)
                for m in per_fold.values()
            )
            candidates.append(
                {
                    "ask_lo": ask_lo,
                    "ask_hi": ask_hi,
                    "gate": gate,
                    "valid_all_folds": bool(valid),
                    "min_fold_ev_per_dollar": float(min_ev),
                    "pooled": pooled,
                    "per_fold": per_fold,
                }
            )
    valid = [item for item in candidates if item["valid_all_folds"]]
    pool = valid if valid else candidates
    selected = max(
        pool,
        key=lambda item: (
            item["min_fold_ev_per_dollar"],
            item["pooled"]["ev_per_dollar"] if item["pooled"]["ev_per_dollar"] is not None else -1.0,
            item["pooled"]["trades"],
        ),
    )
    final_center = float(raw[["market_slug", "basis_log"]].drop_duplicates()["basis_log"].median())
    print(f"  [final:{coin}] fit {len(raw):,} rows", flush=True)
    final_model = _fit(raw, final_center)
    report = {
        "development_rows": int(len(raw)),
        "development_markets": int(raw["market_slug"].nunique()),
        "development_start": pd.to_datetime(raw["slug_ts"].min(), unit="s", utc=True).isoformat(),
        "development_end": pd.to_datetime(raw["slug_ts"].max(), unit="s", utc=True).isoformat(),
        "folds": fold_meta,
        "selected": selected,
        "wfo_pass": bool(selected["valid_all_folds"]),
        "final_basis_center_log": final_center,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    return report, final_model, final_center, oos


def _exact_metrics(rows: pd.DataFrame) -> dict:
    return exact._metrics(rows)


def _by(rows: pd.DataFrame, column: str) -> dict:
    return {str(key): _exact_metrics(part) for key, part in rows.groupby(column, sort=True)}


def _direct_diagnostic(gamma: pd.DataFrame) -> dict:
    records = []
    for coin in COINS:
        direct_sec, direct_px = _load_pm_tape(DIRECT_TWAP, coin, FORWARD_START, FORWARD_END)
        ordinary_sec, ordinary_px = _load_pm_tape(CHAINLINK, coin, FORWARD_START, FORWARD_END)
        direct_map = dict(zip(direct_sec.tolist(), direct_px.tolist()))
        for row in gamma[gamma["coin"] == coin].itertuples():
            start = int(row.slug_ts)
            direct_open = direct_map.get(start)
            direct_close = direct_map.get(start + INTERVAL_S)
            synth_open, no = _chainlink_mean(ordinary_sec, ordinary_px, start)
            synth_close, nc = _chainlink_mean(ordinary_sec, ordinary_px, start + INTERVAL_S)
            records.append(
                {
                    "coin": coin,
                    "direct_open_bps": None if direct_open is None else 1e4 * (direct_open / float(row.price_to_beat) - 1.0),
                    "direct_close_bps": None if direct_close is None else 1e4 * (direct_close / float(row.final_price) - 1.0),
                    "direct_label_match": None if direct_open is None or direct_close is None else bool((direct_close >= direct_open) == (float(row.final_price) >= float(row.price_to_beat))),
                    "synthetic_open_bps": None if no < MIN_TICKS else 1e4 * (synth_open / float(row.price_to_beat) - 1.0),
                    "synthetic_close_bps": None if nc < MIN_TICKS else 1e4 * (synth_close / float(row.final_price) - 1.0),
                    "synthetic_label_match": None if no < MIN_TICKS or nc < MIN_TICKS else bool((synth_close >= synth_open) == (float(row.final_price) >= float(row.price_to_beat))),
                }
            )
    frame = pd.DataFrame(records)

    def summarize(prefix: str) -> dict:
        op = pd.to_numeric(frame[f"{prefix}_open_bps"], errors="coerce").dropna().abs()
        cl = pd.to_numeric(frame[f"{prefix}_close_bps"], errors="coerce").dropna().abs()
        match = frame[f"{prefix}_label_match"].dropna().astype(bool)
        return {
            "open_n": int(len(op)),
            "close_n": int(len(cl)),
            "median_abs_open_bps": float(op.median()) if len(op) else None,
            "p95_abs_open_bps": float(op.quantile(0.95)) if len(op) else None,
            "median_abs_close_bps": float(cl.median()) if len(cl) else None,
            "p95_abs_close_bps": float(cl.quantile(0.95)) if len(cl) else None,
            "label_n": int(len(match)),
            "label_agreement": float(match.mean()) if len(match) else None,
        }

    return {
        f"direct_twap{TWAP_S}s_vs_gamma": summarize("direct"),
        "ordinary_stream_synthetic_vs_gamma": summarize("synthetic"),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coins", nargs="+", choices=COINS, default=list(COINS))
    parser.add_argument("--rebuild-features", action="store_true")
    args = parser.parse_args(argv)
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    gamma = pd.read_parquet(GAMMA)
    gamma = gamma[
        (gamma["slug_ts"] >= _epoch(FORWARD_START))
        & (gamma["slug_ts"] < _epoch(FORWARD_END))
        & gamma["price_to_beat"].notna()
        & gamma["final_price"].notna()
    ].copy()
    report = {
        "protocol": {
            "interval": INTERVAL,
            "decision_offsets": [INTERVAL_S - 60, INTERVAL_S - 1],
            "decision_window": "T-60s through T-1s inclusive",
            "historical_target": (
                "ordinary Chainlink 1s arithmetic mean over "
                f"(boundary-{TWAP_S},boundary]"
            ),
            "forward_target": (
                "exact Gamma priceToBeat/finalPrice, "
                f"{TWAP_S}s Chainlink TWAP"
            ),
            "book": "PMData YES-token L2 only; DOWN derived as 1 - YES bid",
            "latency_ms": int(LATENCY_S * 1000),
            "stake_usd": BASE_STAKE,
            "development_end_exclusive": TRAIN_END.isoformat(),
            "forward_period": "2026-08-07/2026-08-13",
            "wfo": "four chronological chunks; expanding B/C/D; one-hour embargo; per-coin model and spec",
        },
        "gamma_exact_markets": int(gamma["market_slug"].nunique()),
        "coins": {},
        "forward": {},
    }
    all_base = []
    all_t3 = []
    all_scored = []
    all_paths: dict[str, Path] = {}
    for coin in args.coins:
        train_cache = OUT_ROOT / f"train_features_{coin}.parquet"
        forward_cache = OUT_ROOT / f"forward_features_{coin}.parquet"
        train_paths = _book_paths(coin, TRAIN_START[coin], TRAIN_END)
        forward_paths = _book_paths(coin, FORWARD_START, FORWARD_END)
        if args.rebuild_features or not train_cache.exists():
            print(f"[features] historical {coin}: {len(train_paths):,} book files", flush=True)
            _write_feature_cache(
                coin,
                "train",
                _training_labels(coin, train_paths),
                _load_1s(coin),
                train_paths,
                train_cache,
            )
        if args.rebuild_features or not forward_cache.exists():
            print(f"[features] exact forward {coin}: {len(forward_paths):,} book files", flush=True)
            _write_feature_cache(
                coin,
                "forward",
                _forward_labels(coin, gamma),
                _load_forward_spot(coin),
                forward_paths,
                forward_cache,
            )
        raw = pd.read_parquet(train_cache)
        forward = pd.read_parquet(forward_cache)
        if (
            int(raw["offset_sec"].min()) != INTERVAL_S - 60
            or int(raw["offset_sec"].max()) != INTERVAL_S - 1
        ):
            raise AssertionError(f"{coin} training cache is not the requested full 60-second window")
        coin_report, model, center, oos = _wfo(coin, raw)
        spec = coin_report["selected"]
        joblib.dump(
            {
                "coin": coin,
                "interval": INTERVAL,
                "feature_columns": FEATURE_COLUMNS,
                "basis_center_log": center,
                "basis_clip_log": BASIS_CLIP_LOG,
                "spec": {key: spec[key] for key in ("ask_lo", "ask_hi", "gate")},
                "model": model,
            },
            OUT_ROOT / f"model_{coin}.joblib",
        )
        oos_trades = _prefilled_trades(oos, spec["ask_lo"], spec["ask_hi"], spec["gate"])
        oos_trades.to_parquet(OUT_ROOT / f"wfo_trades_{coin}.parquet", index=False)
        scored = _score(model, forward, center)
        paths = {path.stem: path for path in forward_paths if path.stem in set(scored["market_slug"])}
        old_spec = dict(exact.SPECS[INTERVAL])
        exact.SPECS[INTERVAL] = {
            "intervals": (INTERVAL,),
            "ask_lo": float(spec["ask_lo"]),
            "ask_hi": float(spec["ask_hi"]),
            "gate": float(spec["gate"]),
        }
        try:
            base = exact._replay(scored, paths, tier_mult=1.0, t3=False, coin_filter={coin})
            t3 = exact._replay(scored, paths, tier_mult=1.0, t3=True, coin_filter={coin})
        finally:
            exact.SPECS[INTERVAL] = old_spec
        coin_report["forward_feature_rows"] = int(len(scored))
        coin_report["forward_markets_with_features"] = int(scored["market_slug"].nunique())
        coin_report["forward_base"] = _exact_metrics(base)
        coin_report["forward_t3"] = _exact_metrics(t3)
        report["coins"][coin] = coin_report
        all_base.append(base)
        all_t3.append(t3)
        all_scored.append(scored)
        all_paths.update(paths)
        print(
            f"[forward:{coin}] base={coin_report['forward_base']['fills']} fills, "
            f"WR={coin_report['forward_base']['win_rate']:.3f}, "
            f"PnL=${coin_report['forward_base']['pnl_usd']:.2f}",
            flush=True,
        )
    base = pd.concat(all_base, ignore_index=True)
    t3 = pd.concat(all_t3, ignore_index=True)
    base.to_parquet(OUT_ROOT / "forward_base_trades.parquet", index=False)
    t3.to_parquet(OUT_ROOT / "forward_t3_trades.parquet", index=False)
    report["forward"] = {
        "usable_feature_markets": int(base["market_slug"].nunique()),
        "exact_label_market_coverage": float(
            base["market_slug"].nunique() / report["gamma_exact_markets"]
        ),
        "base_all": _exact_metrics(base),
        "t3_all": _exact_metrics(t3),
        "base_by_coin": _by(base, "coin"),
        "t3_by_coin": _by(t3, "coin"),
        "base_by_day": _by(
            base.assign(day=pd.to_datetime(base["entry_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")),
            "day",
        ),
        "t3_by_day": _by(
            t3.assign(day=pd.to_datetime(t3["entry_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")),
            "day",
        ),
    }
    report["twap_validation"] = _direct_diagnostic(gamma)
    REPORT_PATH.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"forward": report["forward"], "twap_validation": report["twap_validation"]}, indent=2))
    print(f"[done] {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
