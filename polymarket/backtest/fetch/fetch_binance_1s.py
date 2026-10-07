#!/usr/bin/env python
"""Fetch a bounded Binance spot 1-second kline range for forward research.

This complements Binance Vision daily ZIPs when the most recently completed
UTC day has not been published yet.  It is public market data and writes only
research parquet files under ``backtest/data/binance_1s_forward``.
"""
from __future__ import annotations

import argparse
import time

import pandas as pd
import requests

from polymarket.backtest import BT_DATA_DIR

# Binance's dedicated public market-data host is also the live bot default and
# remains reachable in environments where the general api.binance.com host is
# filtered or times out.
API = "https://data-api.binance.vision/api/v3/klines"
PAIRS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
OUT_ROOT = BT_DATA_DIR / "binance_1s_forward"


def _utc(value: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def fetch_pair(pair: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    rows: list[list] = []
    cursor = start_ms
    session = requests.Session()
    while cursor < end_ms:
        response = None
        for attempt in range(6):
            try:
                response = session.get(
                    API,
                    params={
                        "symbol": pair,
                        "interval": "1s",
                        "startTime": cursor,
                        "endTime": end_ms - 1,
                        "limit": 1000,
                    },
                    timeout=30,
                )
                if response.status_code in (418, 429):
                    time.sleep(min(30.0, 2.0 ** (attempt + 1)))
                    response = None
                    continue
                response.raise_for_status()
                break
            except requests.RequestException:
                response = None
                if attempt == 5:
                    raise
                time.sleep(min(30.0, 2.0 ** attempt))
        if response is None:
            raise RuntimeError(f"Binance request retries exhausted for {pair} at {cursor}")
        batch = response.json()
        if not batch:
            break
        rows.extend(batch)
        next_cursor = int(batch[-1][0]) + 1000
        if next_cursor <= cursor:
            raise RuntimeError(f"Binance cursor did not advance for {pair}")
        cursor = next_cursor
        time.sleep(0.03)
    if not rows:
        return pd.DataFrame(columns=["sec", "open", "close"])
    frame = pd.DataFrame(rows)
    return pd.DataFrame(
        {
            "sec": (frame[0].astype("int64") // 1000).astype("int64"),
            "open": pd.to_numeric(frame[1], errors="coerce"),
            "close": pd.to_numeric(frame[4], errors="coerce"),
        }
    ).dropna().drop_duplicates("sec", keep="last").sort_values("sec")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="UTC timestamp/date")
    parser.add_argument("--end", required=True, help="exclusive UTC timestamp/date")
    parser.add_argument("--pairs", nargs="+", choices=PAIRS, default=list(PAIRS))
    args = parser.parse_args(argv)
    start_utc = _utc(args.start)
    end_utc = _utc(args.end)
    start_ms = int(start_utc.timestamp() * 1000)
    end_ms = int(end_utc.timestamp() * 1000)
    if end_ms <= start_ms:
        parser.error("--end must be after --start")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    for pair in args.pairs:
        frame = fetch_pair(pair, start_ms, end_ms)
        start_label = start_utc.strftime("%Y%m%dT%H%M%SZ")
        end_label = end_utc.strftime("%Y%m%dT%H%M%SZ")
        target = OUT_ROOT / f"{pair}_{start_label}_{end_label}.parquet"
        temp = target.with_suffix(".parquet.part")
        frame.to_parquet(temp, index=False)
        temp.replace(target)
        print({"pair": pair, "rows": len(frame), "out": str(target)}, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
