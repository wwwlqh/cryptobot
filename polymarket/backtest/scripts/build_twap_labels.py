#!/usr/bin/env python
"""Build historical proxy labels for the Chainlink TWAP settlement rule.

The saved tape is the ordinary Chainlink 1-second stream, not Chainlink's
signed TWAP product. These labels approximate each lookback value with the mean
of the available one-second observations. Chainlink does not publish the feed's
weighting, sampling boundaries, rounding or missing-input rules, so this is an
approximation; exact values after the switch are in Gamma ``eventMetadata``.

Polymarket switched crypto up/down settlement from a single price to a
time-weighted average on **2026-08-07 00:00 UTC**: 5m markets used 30s and 15m
used 60s at first, and from **2026-08-14 00:00 UTC** both use 60s. Historical
rows keep the lookback of their settlement period.

Window semantics: the slug is ``{coin}-updown-{interval}m-{START_ts}`` (see
``cryptobot_polymarket/gamma.py``), so a window is [slug_ts, slug_ts +
interval).

The 30s and 60s windows are lookbacks, so the symmetric opening proxy uses the
window ending at the market boundary, not one after it. Three labels:

  legacy : point open vs point close   (the old rule; should match Gamma
                                        ~99%+, and any gap measures our tape
                                        or plumbing error)
  twap_c : point open vs TWAP close    (close-only TWAP)
  twap_s : TWAP open  vs TWAP close    (symmetric TWAP)

All three resolve Up iff close >= open (a tie is Up), matching Polymarket's
"resolve Up if end price >= start price".

Boundaries: close proxy ``(end - N, end]``, opening proxy ``(start - N,
start]``. A point price is the last tick at or before the boundary; if it is
more than ``--max-stale`` seconds old the market is marked unusable rather than
mislabelled.
"""
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # legacy-path-ok: sys.path bootstrap runs before the package import exists
PMDATA = ROOT / "polymarket" / "backtest" / "data" / "pmdata"
TAPE_ROOT = PMDATA / "chainlink_streams"
OUTCOMES = PMDATA / "pmdata_outcomes.parquet"
DEFAULT_OUT = PMDATA / "twap_labels_trailing_proxy.parquet"

SYM = {"btc": "BTCUSD", "eth": "ETHUSD", "sol": "SOLUSD", "xrp": "XRPUSD"}
FIVE_MINUTE_60S_CUTOVER_TS = 1_786_665_600
INTERVAL_SECONDS = {"5m": 300, "15m": 900}
WAD = 1e18


def twap_seconds(interval: str, boundary_ts: int) -> int:
    """Return the settlement lookback active at one market boundary."""
    if interval == "15m":
        return 60
    if interval == "5m":
        return 60 if int(boundary_ts) >= FIVE_MINUTE_60S_CUTOVER_TS else 30
    raise ValueError(f"unsupported TWAP interval: {interval}")


def load_tape_day(sym: str, day: dt.date) -> tuple[np.ndarray, np.ndarray] | None:
    """Return (epoch_seconds asc, price float) for one symbol-day, or None."""
    p = TAPE_ROOT / sym / f"{sym}_streams_{day.isoformat()}.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p, columns=["observationsTimestamp", "price"])
    ts = pd.to_datetime(df["observationsTimestamp"], utc=True, errors="coerce")
    px = pd.to_numeric(df["price"], errors="coerce") / WAD
    m = ts.notna() & px.notna() & (px > 0)
    ts, px = ts[m], px[m]
    if ts.empty:
        return None
    order = np.argsort(ts.values.astype("datetime64[ns]"), kind="stable")
    secs = ts.values.astype("datetime64[s]").astype("int64")[order]
    return secs, px.to_numpy()[order]


def point_at(secs: np.ndarray, px: np.ndarray, t: int, max_stale: int):
    """Last tick at or before t. Returns (price, staleness_s) or (nan, nan)."""
    i = np.searchsorted(secs, t, side="right") - 1
    if i < 0:
        return np.nan, np.nan
    stale = t - int(secs[i])
    if stale > max_stale:
        return np.nan, float(stale)
    return float(px[i]), float(stale)


def mean_between(secs: np.ndarray, px: np.ndarray, lo: int, hi: int,
                 lo_inclusive: bool, hi_inclusive: bool):
    """Mean price over the (lo, hi] or [lo, hi) tick range. Returns (mean, n)."""
    a = np.searchsorted(secs, lo, side="left" if lo_inclusive else "right")
    b = np.searchsorted(secs, hi, side="right" if hi_inclusive else "left")
    if b <= a:
        return np.nan, 0
    return float(px[a:b].mean()), int(b - a)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-stale", type=int, default=30,
                    help="reject a boundary point price older than this (s)")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args(argv)

    out = pd.read_parquet(OUTCOMES)
    out = out[out["status"] == "ok"].copy()
    out = out[out["coin"].isin(SYM)].copy()
    out["slug_ts"] = pd.to_numeric(out["slug_ts"], errors="coerce").astype("int64")
    out["ivs"] = out["interval"].map(INTERVAL_SECONDS)
    out = out[out["ivs"].notna()].copy()
    out["ivs"] = out["ivs"].astype(int)
    out["twap_n"] = [
        twap_seconds(interval, slug_ts)
        for interval, slug_ts in zip(out["interval"], out["slug_ts"])
    ]
    out["w_start"] = out["slug_ts"]
    out["w_end"] = out["slug_ts"] + out["ivs"]
    out["day"] = pd.to_datetime(out["w_start"], unit="s", utc=True).dt.date

    print(f"==> {len(out):,} resolved markets in outcomes "
          f"({out['day'].min()} .. {out['day'].max()})")

    recs = []
    for coin, g_coin in out.groupby("coin", sort=True):
        sym = SYM[coin]
        days = sorted(g_coin["day"].unique())
        have = 0
        cache: dict[dt.date, tuple] = {}
        for day in days:
            # The opening TWAP is a lookback ending at the market boundary, so
            # day-1 is needed near midnight. A late 15m window may close after
            # midnight, so day+1 is needed as well.
            parts = []
            for d in (
                day - dt.timedelta(days=1),
                day,
                day + dt.timedelta(days=1),
            ):
                if d not in cache:
                    cache[d] = load_tape_day(sym, d)
                    for k in list(cache):
                        if k < day - dt.timedelta(days=1):
                            del cache[k]
                if cache[d] is not None:
                    parts.append(cache[d])
            if not parts:
                continue
            have += 1
            secs = np.concatenate([p[0] for p in parts])
            px = np.concatenate([p[1] for p in parts])

            for r in g_coin[g_coin["day"] == day].itertuples(index=False):
                ws, we, n = int(r.w_start), int(r.w_end), int(r.twap_n)
                o_pt, o_stale = point_at(secs, px, ws, args.max_stale)
                c_pt, c_stale = point_at(secs, px, we, args.max_stale)
                c_tw, c_cnt = mean_between(secs, px, we - n, we, False, True)
                o_tw, o_cnt = mean_between(secs, px, ws - n, ws, False, True)
                recs.append((r.market_slug, coin, r.interval, ws, we,
                             o_pt, c_pt, o_tw, c_tw, o_cnt, c_cnt,
                             o_stale, c_stale, r.won_up))
        print(f"  {coin}: {have}/{len(days)} days had tape, {len(recs):,} rows so far")

    df = pd.DataFrame(recs, columns=[
        "market_slug", "coin", "interval", "w_start", "w_end",
        "open_point", "close_point", "open_twap", "close_twap",
        "n_open_ticks", "n_close_ticks", "open_stale_s", "close_stale_s",
        "gamma_won_up"])

    df["up_legacy"] = np.where(df["open_point"].notna() & df["close_point"].notna(),
                               (df["close_point"] >= df["open_point"]).astype("float"), np.nan)
    df["up_twap_c"] = np.where(df["open_point"].notna() & df["close_twap"].notna(),
                               (df["close_twap"] >= df["open_point"]).astype("float"), np.nan)
    df["up_twap_s"] = np.where(df["open_twap"].notna() & df["close_twap"].notna(),
                               (df["close_twap"] >= df["open_twap"]).astype("float"), np.nan)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"==> wrote {args.out}  ({len(df):,} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
