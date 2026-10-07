#!/usr/bin/env python
"""Download PMData's direct Chainlink TWAP daily parquet files.

The ordinary Chainlink tape predates this dataset and is used to construct
synthetic historical labels. Direct 60s TWAP files begin on 2026-08-01
and are kept separately so exact forward labels cannot leak into training.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT
from polymarket.backtest.fetch._common import load_dotenv_into_environ

SYMBOLS = ("BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD")
EARLIEST = dt.date(2026, 8, 1)
UA = "Mozilla/5.0 (cryptobot chainlink twap research)"


def _api_key() -> str:
    load_dotenv_into_environ(REPO_ROOT / ".env")
    key = os.environ.get("PMDATA_API_KEY", "").strip()
    if not key:
        sys.exit("ERROR: set PMDATA_API_KEY in cryptobot/.env")
    return key


def _fetch_one(
    api_key: str,
    symbol: str,
    day: dt.date,
    force: bool,
) -> tuple[str, int, str]:
    data_type = "streams_twap60s"
    out_dir = BT_DATA_DIR / "pmdata" / data_type / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}_{data_type}_{day.isoformat()}.parquet"
    target = out_dir / name
    if target.exists() and target.stat().st_size > 0 and not force:
        return "skip", target.stat().st_size, name
    url = f"https://api.pmdata.dev/chainlink/{symbol}/{data_type}/{name}"
    for attempt in range(4):
        try:
            response = requests.get(
                url,
                headers={"api_key": api_key, "User-Agent": UA},
                timeout=180,
            )
            if response.status_code == 200:
                if not response.content:
                    return "missing", 0, name
                temp = target.with_suffix(".parquet.part")
                temp.write_bytes(response.content)
                temp.replace(target)
                return "ok", len(response.content), name
            if response.status_code == 404:
                return "missing", 0, name
            if response.status_code in (401, 403):
                raise RuntimeError(
                    f"PMData authentication failed ({response.status_code})"
                )
        except requests.RequestException:
            if attempt == 3:
                raise
        time.sleep(2.0 ** attempt)
    return "error", 0, name


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=EARLIEST.isoformat())
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbols", default=",".join(SYMBOLS))
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    start = max(dt.date.fromisoformat(args.start), EARLIEST)
    end = dt.date.fromisoformat(args.end)
    if end < start:
        parser.error("--end must be on or after --start")
    symbols = tuple(
        value.strip().upper()
        for value in args.symbols.split(",")
        if value.strip()
    )
    unknown = sorted(set(symbols) - set(SYMBOLS))
    if unknown:
        parser.error(f"unsupported symbols: {unknown}")
    days = [
        start + dt.timedelta(days=offset)
        for offset in range((end - start).days + 1)
    ]
    jobs = [(symbol, day) for symbol in symbols for day in days]
    api_key = _api_key()
    tally: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                _fetch_one,
                api_key,
                symbol,
                day,
                args.force,
            ): (symbol, day)
            for symbol, day in jobs
        }
        for done, future in enumerate(as_completed(futures), start=1):
            status, size, name = future.result()
            tally[status] = tally.get(status, 0) + 1
            print(
                {"progress": f"{done}/{len(jobs)}", "status": status,
                 "bytes": size, "file": name},
                flush=True,
            )
    print({"tally": tally}, flush=True)
    return 1 if tally.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
