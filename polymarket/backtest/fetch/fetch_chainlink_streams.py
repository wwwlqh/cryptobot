#!/usr/bin/env python
"""Pull pmdata's Chainlink Data Streams tape (per-second) for the live coins.

Endpoint:  GET https://api.pmdata.dev/chainlink/{SYM}/streams/{SYM}_streams_{YYYY-MM-DD}.parquet
Auth:      header  api_key: <PMDATA_API_KEY from cryptobot/.env>

⚠️ SYMBOLS ARE ``BTCUSD``/``ETHUSD``/``SOLUSD``/``XRPUSD`` — NOT ``BTC``/``ETH``.
The bare-ticker spelling used by the (deleted) 2026-07 puller 404s on every
date, which reads exactly like "dataset discontinued". It is not. Diagnostic:
a registered-but-empty route answers JSON ``{"error":"Not Found"}``; an unknown
route answers plain ``404 Not Found``.

Coverage probed 2026-08-02: BTCUSD from 2026-04-22, the rest from 2026-06-07.
Retention is NOT the 30-day rolling window the old notes claimed. Today's file
is published a day late, so ``--end`` defaults to yesterday.

Tape quality (measured 2026-08-01, BTCUSD+ETHUSD): 84,462 ticks/day = 0.98/s,
median gap exactly 1.000s, p99 2s, max 4s, zero gaps >30s, 97.8% of the day's
distinct seconds covered. Columns: observationsTimestamp,
receiveMicrosecondTimestamp, price, bid, ask, validFromTimestamp, expiresAt,
version. Prices are 18-decimal fixed-point strings (divide by 1e18).

Resumable: an existing non-empty file for a (symbol, date) is skipped, so a
re-run only fetches the gap. Use --force to refetch.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[3]  # legacy-path-ok: sys.path bootstrap runs before the package import exists
OUT_ROOT = ROOT / "polymarket" / "backtest" / "data" / "pmdata" / "chainlink_streams"
URL = "https://api.pmdata.dev/chainlink/{s}/streams/{s}_streams_{d}.parquet"
UA = "Mozilla/5.0 (cryptobot chainlink puller)"

# Live universe. BNBUSD/DOGEUSD also exist upstream but are not traded.
SYMBOLS = ("BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD")

# Probed 2026-08-02 by binary search; a symbol never serves earlier than this.
EARLIEST = {
    "BTCUSD": dt.date(2026, 4, 22),
    "ETHUSD": dt.date(2026, 6, 7),
    "SOLUSD": dt.date(2026, 6, 7),
    "XRPUSD": dt.date(2026, 6, 7),
    "BNBUSD": dt.date(2026, 6, 7),
    "DOGEUSD": dt.date(2026, 6, 7),
}


def load_api_key() -> str:
    key = os.environ.get("PMDATA_API_KEY", "").strip()
    if key:
        return key
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.strip().startswith("PMDATA_API_KEY="):
                return line.split("=", 1)[1].strip()
    sys.exit("ERROR: set PMDATA_API_KEY in cryptobot/.env")


def daterange(a: dt.date, b: dt.date):
    d = a
    while d <= b:
        yield d
        d += dt.timedelta(days=1)


def fetch_one(session: requests.Session, sym: str, d: dt.date, force: bool) -> tuple[str, int]:
    """Return (status, bytes). status in {skip, ok, missing, error}."""
    out_dir = OUT_ROOT / sym
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"{sym}_streams_{d.isoformat()}.parquet"
    if dest.exists() and dest.stat().st_size > 0 and not force:
        return "skip", dest.stat().st_size

    url = URL.format(s=sym, d=d.isoformat())
    for attempt in range(4):
        try:
            r = session.get(url, timeout=180)
            if r.status_code == 200:
                if not r.content:
                    return "missing", 0
                tmp = dest.with_suffix(".parquet.part")
                tmp.write_bytes(r.content)
                tmp.replace(dest)  # atomic — a killed run never leaves a torn file
                return "ok", len(r.content)
            if r.status_code == 404:
                return "missing", 0
            if r.status_code in (401, 403):
                sys.exit(f"ERROR: {r.status_code} auth failure — check PMDATA_API_KEY")
            time.sleep(1.5 * (attempt + 1))
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return "error", 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", default=",".join(SYMBOLS),
                    help="comma-separated (default: the 4 live coins)")
    ap.add_argument("--start", default=None,
                    help="UTC YYYY-MM-DD (default: each symbol's earliest available)")
    ap.add_argument("--end", default=None,
                    help="UTC YYYY-MM-DD (default: yesterday — today publishes a day late)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--force", action="store_true", help="refetch files already on disk")
    args = ap.parse_args(argv)

    key = load_api_key()
    session = requests.Session()
    session.headers.update({"api_key": key, "User-Agent": UA})

    end = (dt.date.fromisoformat(args.end) if args.end
           else dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    jobs = []
    for sym in syms:
        start = (dt.date.fromisoformat(args.start) if args.start
                 else EARLIEST.get(sym, dt.date(2026, 6, 7)))
        start = max(start, EARLIEST.get(sym, start))
        jobs.extend((sym, d) for d in daterange(start, end))

    print(f"==> {len(jobs)} symbol-days across {len(syms)} symbols, end={end}")
    print(f"==> out: {OUT_ROOT}")

    tally: dict[str, int] = {}
    total_bytes = 0
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch_one, session, s, d, args.force): (s, d) for s, d in jobs}
        for fut in as_completed(futs):
            sym, d = futs[fut]
            try:
                status, n = fut.result()
            except Exception as exc:  # noqa: BLE001 — one bad day must not kill the pull
                status, n = "error", 0
                print(f"  ERROR {sym} {d}: {exc}")
            tally[status] = tally.get(status, 0) + 1
            total_bytes += n
            done += 1
            if done % 25 == 0 or done == len(jobs):
                print(f"  [{done}/{len(jobs)}] {tally}  {total_bytes/1048576:.0f}MB")

    print(f"==> done: {tally}  total {total_bytes/1048576:.1f}MB")
    return 0 if not tally.get("error") else 1


if __name__ == "__main__":
    raise SystemExit(main())
