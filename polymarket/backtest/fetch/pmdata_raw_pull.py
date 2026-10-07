"""Pull RAW tick-level pmdata.dev poly_l2 parquet for every live sub-minute window.

Unlike ``polymarket/backtest/fetch/pmdata_download.py`` (which resamples to 1 row/second and keeps
only 6 columns), this keeps the file BYTE-FOR-BYTE as served: ~2-40k tick rows per
market, ``price_change`` deltas with running ``best_ask``/``best_bid``, periodic
full ``book`` ladders, and the terminal ``market_resolved`` row carrying
``winning_outcome``. That is what a fill-accurate per-tick replay needs.

Window list comes from the pulled server DB (subminute_fills + signals), so we
download exactly the markets the bot actually traded/evaluated.

Resumable: a slug already on disk (> MIN_BYTES) is skipped.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT as ROOT
from polymarket.backtest.fetch._common import load_dotenv_into_environ

OUT = BT_DATA_DIR / "pmdata" / "poly_l2_raw"
DB = ROOT / "data" / "server_snapshot" / "cb_export_latest.db"
URL = "https://api.pmdata.dev/download/poly_l2/{slug}.parquet"
MIN_BYTES = 200
UA = "Mozilla/5.0"

_tl = threading.local()
_lock = threading.Lock()
_backoff = threading.Event()


def _session() -> requests.Session:
    s = getattr(_tl, "s", None)
    if s is None:
        s = _tl.s = requests.Session()
    return s


class Rate:
    def __init__(self, rate: float) -> None:
        self.rate = float(rate)
        self.tok = rate
        self.t = time.monotonic()
        self.lk = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lk:
                now = time.monotonic()
                self.tok = min(self.rate, self.tok + (now - self.t) * self.rate)
                self.t = now
                if self.tok >= 1:
                    self.tok -= 1
                    return
            time.sleep(0.01)


def slugs_from_db(
    db_path: Path, start: str, end: str, *, traded_only: bool = True
) -> list[tuple[str, str, str]]:
    """Unique (slug, coin, interval) for every window the bot touched in [start, end).

    ``traded_only`` (default) keeps windows the bot either PLACED into
    (subminute_fills) or actually SCORED at a real in-band ask (a signals row with
    a non-null ask) — i.e. every window a policy change could possibly move,
    including the upstream-blocked misses that never produced a placement. The
    full signals table also logs markets that were never in band at all; pulling
    those costs ~10x the bytes and buys nothing.
    """
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    if traded_only:
        sql = """
        SELECT DISTINCT coin, sleeve, window_start_ts FROM (
            SELECT coin, sleeve, window_start_ts, created_at FROM subminute_fills
            UNION ALL
            SELECT coin, sleeve, window_start_ts, created_at FROM signals
             WHERE ask IS NOT NULL
        )
        WHERE created_at >= ? AND created_at < ? AND window_start_ts IS NOT NULL
        """
    else:
        sql = """
        SELECT DISTINCT coin, sleeve, window_start_ts FROM (
            SELECT coin, sleeve, window_start_ts, created_at FROM subminute_fills
            UNION ALL
            SELECT coin, sleeve, window_start_ts, created_at FROM signals
        )
        WHERE created_at >= ? AND created_at < ? AND window_start_ts IS NOT NULL
        """
    rows = con.execute(sql, (start, end)).fetchall()
    con.close()
    out: dict[str, tuple[str, str, str]] = {}
    for coin, sleeve, wst in rows:
        if not sleeve or "5m" not in sleeve and "15m" not in sleeve:
            continue
        interval = "15m" if "15m" in sleeve else "5m"
        slug = f"{str(coin).lower()}-updown-{interval}-{int(wst)}"
        out[slug] = (slug, str(coin).lower(), interval)
    return sorted(out.values())


def fetch_one(slug: str, coin: str, iv: str, key: str, rate: Rate, stats: dict) -> None:
    if _backoff.is_set():
        _backoff.wait(timeout=30)
    dest = OUT / coin / iv / f"{slug}.parquet"
    if dest.exists() and dest.stat().st_size > MIN_BYTES:
        with _lock:
            stats["skip"] += 1
        return
    rate.acquire()
    try:
        r = _session().get(
            URL.format(slug=slug), headers={"api_key": key, "User-Agent": UA}, timeout=60
        )
    except Exception as exc:  # network hiccup -> retryable, leave no file
        with _lock:
            stats["error"] += 1
            stats["last_err"] = str(exc)[:120]
        return
    if r.status_code == 200:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".parquet.tmp")
        tmp.write_bytes(r.content)
        tmp.replace(dest)
        with _lock:
            stats["ok"] += 1
            stats["bytes"] += len(r.content)
    elif r.status_code == 404:
        with _lock:
            stats["missing"] += 1
            stats.setdefault("missing_slugs", []).append(slug)
    elif r.status_code == 429:
        _backoff.set()
        time.sleep(15)
        _backoff.clear()
        with _lock:
            stats["error"] += 1
            stats["throttled"] += 1
    elif r.status_code == 401:
        raise SystemExit("401 unauthorized - bad/expired PMDATA_API_KEY")
    else:
        with _lock:
            stats["error"] += 1
            stats["last_err"] = f"HTTP {r.status_code}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-07-17 00:00:00")
    ap.add_argument("--end", default="2026-09-01 00:00:00")
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--rate", type=float, default=20.0)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--all-scanned", action="store_true",
                    help="also pull markets that were only scanned, never in band (~10x bytes)")
    ap.add_argument("--slug-file", default=None,
                    help="newline-separated slug list to pull instead of querying the DB")
    args = ap.parse_args(argv)

    load_dotenv_into_environ(ROOT / ".env")
    key = os.environ.get("PMDATA_API_KEY", "").strip()
    if not key:
        sys.exit("ERROR: PMDATA_API_KEY missing from .env")

    if args.slug_file:
        todo = []
        for line in Path(args.slug_file).read_text().split():
            parts = line.strip().split("-")  # {coin}-updown-{interval}-{ts}
            if len(parts) == 4:
                todo.append((line.strip(), parts[0], parts[2]))
        todo.sort()
    else:
        todo = slugs_from_db(
            Path(args.db), args.start, args.end, traded_only=not args.all_scanned
        )
    print(f"[raw-pull] {len(todo)} unique slugs in [{args.start}, {args.end})", flush=True)
    rate = Rate(args.rate)
    stats = {"ok": 0, "skip": 0, "missing": 0, "error": 0, "throttled": 0, "bytes": 0}
    t0 = time.monotonic()
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(fetch_one, s, c, i, key, rate, stats) for s, c, i in todo]
        for f in futs:
            f.result()
            done += 1
            if done % 100 == 0:
                el = time.monotonic() - t0
                print(
                    f"[raw-pull] {done}/{len(todo)} ok={stats['ok']} skip={stats['skip']} "
                    f"miss={stats['missing']} err={stats['error']} "
                    f"{stats['bytes']/1e6:.0f} MB  {done/el:.1f}/s",
                    flush=True,
                )
    el = time.monotonic() - t0
    print(
        f"[raw-pull] DONE {done} in {el/60:.1f} min  ok={stats['ok']} skip={stats['skip']} "
        f"missing={stats['missing']} err={stats['error']} {stats['bytes']/1e6:.0f} MB",
        flush=True,
    )
    if stats.get("missing_slugs"):
        print("[raw-pull] missing:", stats["missing_slugs"][:20], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
