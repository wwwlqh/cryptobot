"""Resumable bulk downloader for pmdata.dev per-second L2 order books.

Endpoint:  GET https://api.pmdata.dev/download/poly_l2/{slug}.parquet
Auth:      header  api_key: <key>        (PMDATA_API_KEY in .env, falls back to
           the gitignored .pmdata_key file)
Slug:      {coin}-updown-{interval}-{window_start_ts}   ts on the interval grid
Rate:      300 req / 10s per IP -> capped well under (default 24 req/s)

Resume: a slug is skipped if its file already exists (> MIN_BYTES) or the
manifest marks it 'missing' (404 = no such market). 'error' rows are retried.
Filesystem is the source of truth; the SQLite manifest only records status so
re-runs don't re-hit known-missing markets. Safe to kill and re-run.
"""
from __future__ import annotations
import argparse, io, sqlite3, threading, time, os, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests

from polymarket.backtest.fetch._common import load_dotenv_into_environ

# Per-second book schema + derived window outcome (UP-token book settles to ~1 if
# Up won, ~0 if Down). winning_outcome is empty in the raw poly_l2 feed, so we
# derive it from terminal book convergence. Superset of the original 6-col schema.
BOOK_COLS = ["timestamp", "event_type", "ask_prices", "ask_sizes", "bid_prices", "bid_sizes"]
OUT_COLS = BOOK_COLS + ["winning_outcome"]


def _mids(ask_col, bid_col):
    out = []
    for a, b in zip(ask_col, bid_col):
        if len(a) and len(b):
            out.append((min(a) + max(b)) / 2)
    return out


def derive_outcome(ask_col, bid_col) -> str | None:
    """'Up' / 'Down' from the UP-token book settlement: median of the last 30 valid
    mids. Resolved markets settle near 0 or 1, so only a median genuinely near 0.5
    (0.40-0.60) is treated as 'didn't resolve in this file' -> None. Pass the RAW
    book (finer, cleaner terminal) when available; the per-second book also works."""
    m = _mids(ask_col, bid_col)
    if not m:
        return None
    import statistics
    term = statistics.median(m[-30:]) if len(m) >= 8 else m[-1]
    if 0.40 < term < 0.60:
        return None
    return "Up" if term > 0.5 else "Down"


def resample_per_second(content: bytes):
    """Tick-level poly_l2 -> 1 book snapshot/second + derived winning_outcome
    (derived from the RAW book, before resampling, for a clean terminal).
    Returns a slim DataFrame, or None if the file has no book events."""
    d = pd.read_parquet(io.BytesIO(content), columns=BOOK_COLS)
    d = d[d["event_type"] == "book"].sort_values("timestamp")
    if len(d) == 0:
        return None
    outcome = derive_outcome(d["ask_prices"], d["bid_prices"])  # from RAW book
    sec = d["timestamp"].dt.floor("s")
    d = d.assign(_sec=sec).drop_duplicates("_sec", keep="last").drop(columns="_sec")
    d = d[BOOK_COLS].reset_index(drop=True)
    d["winning_outcome"] = outcome
    return d

from polymarket.backtest import BT_DATA_DIR, REPO_ROOT as ROOT

PM = BT_DATA_DIR / "pmdata"
SNAP = PM / "poly_snapshot"
KEY_FILE = PM / ".pmdata_key"
MANIFEST = PM / "pmdata_download.db"
URL = "https://api.pmdata.dev/download/poly_l2/{slug}.parquet"

COINS = ["btc", "eth", "sol", "xrp"]
INTERVALS = {"5m": 300, "15m": 900}
MIN_BYTES = 200
UA = "Mozilla/5.0"


def load_api_key() -> str:
    """PMDATA_API_KEY from .env, falling back to the legacy .pmdata_key file."""
    load_dotenv_into_environ(ROOT / ".env")
    key = os.environ.get("PMDATA_API_KEY", "").strip()
    if key:
        return key
    if KEY_FILE.exists():
        return KEY_FILE.read_text().strip()
    sys.exit("ERROR: set PMDATA_API_KEY in cryptobot/.env "
             f"(or restore {KEY_FILE})")

_tl = threading.local()
_mlock = threading.Lock()
_stats_lock = threading.Lock()
# Shared backoff signal: when a 429 is received, the receiving thread sets this
# event and all workers sleep before the next acquire(), giving the server time
# to reset the rate-limit window across the entire IP.
_backoff_evt = threading.Event()
_BACKOFF_SECONDS = 15.0


def session() -> requests.Session:
    s = getattr(_tl, "s", None)
    if s is None:
        s = _tl.s = requests.Session()
    return s


class Rate:
    def __init__(self, rate): self.rate = float(rate); self.tok = rate; self.t = time.monotonic(); self.lk = threading.Lock()
    def acquire(self):
        while True:
            with self.lk:
                now = time.monotonic(); self.tok = min(self.rate, self.tok + (now - self.t) * self.rate); self.t = now
                if self.tok >= 1:
                    self.tok -= 1; return
            time.sleep(0.01)


def init_manifest():
    # check_same_thread=False: workers write through _mlock (serialized), safe.
    c = sqlite3.connect(MANIFEST, check_same_thread=False)
    c.execute("CREATE TABLE IF NOT EXISTS dl (slug TEXT PRIMARY KEY, coin TEXT, interval TEXT, "
              "status TEXT, http_code INTEGER, n_bytes INTEGER, fetched_at TEXT)")
    c.execute("PRAGMA journal_mode=WAL")
    c.commit()
    return c


def record(conn, slug, coin, iv, status, code, nbytes):
    with _mlock:
        conn.execute("INSERT OR REPLACE INTO dl VALUES (?,?,?,?,?,?,?)",
                     (slug, coin, iv, status, code, nbytes, datetime.now(timezone.utc).isoformat()))
        conn.commit()


def fetch_one(slug, coin, iv, key, rate, conn, stats):
    # If another worker hit a 429, wait for the backoff window to clear.
    if _backoff_evt.is_set():
        _backoff_evt.wait(timeout=_BACKOFF_SECONDS * 2)
    out = SNAP / coin / iv / f"{slug}.parquet"
    if out.exists() and out.stat().st_size > MIN_BYTES:
        with _stats_lock:
            stats["skip"] += 1
        return
    rate.acquire()
    try:
        r = session().get(URL.format(slug=slug), headers={"api_key": key, "User-Agent": UA}, timeout=40)
    except Exception as e:
        record(conn, slug, coin, iv, "error", -1, 0)
        with _stats_lock:
            stats["error"] += 1
            stats["last_err"] = str(e)[:120]
        return
    if r.status_code == 200:
        try:
            slim = resample_per_second(r.content)
        except Exception as e:
            record(conn, slug, coin, iv, "error", 200, 0)
            with _stats_lock:
                stats["error"] += 1
                stats["last_err"] = f"resample: {e}"[:120]
            return
        if slim is None or len(slim) == 0:
            record(conn, slug, coin, iv, "empty", 200, 0)
            with _stats_lock:
                stats["empty"] += 1
            return
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".parquet.tmp")
        slim.to_parquet(tmp, index=False); nbytes = tmp.stat().st_size; tmp.replace(out)
        record(conn, slug, coin, iv, "ok", 200, nbytes)
        with _stats_lock:
            stats["ok"] += 1
    elif r.status_code == 404:
        record(conn, slug, coin, iv, "missing", 404, 0)
        with _stats_lock:
            stats["missing"] += 1
    elif r.status_code == 429:
        # Signal all workers to pause before their next request.
        _backoff_evt.set()
        time.sleep(_BACKOFF_SECONDS)
        _backoff_evt.clear()
        with _stats_lock:
            stats["throttled"] += 1
            stats["error"] += 1
        record(conn, slug, coin, iv, "error", 429, 0)
    elif r.status_code == 401:
        with _stats_lock:
            stats["fatal"] = "401 unauthorized — bad/expired api_key"
        raise SystemExit(stats["fatal"])
    else:
        record(conn, slug, coin, iv, "error", r.status_code, 0)
        with _stats_lock:
            stats["error"] += 1


def known_skip(conn):
    """slugs already terminal (ok file on disk handled separately; here: missing)."""
    rows = conn.execute("SELECT slug FROM dl WHERE status='missing'").fetchall()
    return {r[0] for r in rows}


def enumerate_slugs(start_ts, end_ts, *, coins=None, intervals=None):
    selected_coins = COINS if coins is None else coins
    selected_intervals = tuple(INTERVALS) if intervals is None else intervals
    for coin in selected_coins:
        for iv in selected_intervals:
            step = INTERVALS[iv]
            t0 = (start_ts // step) * step
            for ts in range(t0, end_ts, step):
                yield (f"{coin}-updown-{iv}-{ts}", coin, iv)


def backfill_outcome():
    """Add winning_outcome to existing slim files that lack it (derived from their
    own book path). Idempotent — skips files that already have the column."""
    import glob, pyarrow.parquet as pq
    files = glob.glob(str(SNAP / "*" / "*" / "*.parquet"))
    print(f"[backfill] scanning {len(files)} slim files", flush=True)
    added = skipped = noout = 0
    for i, f in enumerate(files):
        try:
            cols = pq.ParquetFile(f).schema.names
        except Exception:
            continue
        if "winning_outcome" in cols:
            skipped += 1; continue
        d = pd.read_parquet(f)
        oc = derive_outcome(d["ask_prices"], d["bid_prices"])
        if oc is None:
            noout += 1
        d["winning_outcome"] = oc
        tmp = f + ".tmp"; d.to_parquet(tmp, index=False); os.replace(tmp, f); added += 1
        if (i + 1) % 2000 == 0:
            print(f"[backfill] {i+1}/{len(files)} added={added} skipped={skipped} no_outcome={noout}", flush=True)
    print(f"[backfill] DONE added={added} skipped(already had)={skipped} no_outcome={noout}", flush=True)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-02-01", help="UTC date YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="UTC date YYYY-MM-DD (default: now)")
    ap.add_argument("--rate", type=float, default=24.0, help="requests/sec (limit 30)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--coins", nargs="+", choices=COINS, default=COINS)
    ap.add_argument(
        "--intervals", nargs="+", choices=tuple(INTERVALS),
        default=tuple(INTERVALS),
    )
    ap.add_argument("--backfill-outcome", action="store_true",
                    help="add winning_outcome to existing slim files in place (no download)")
    args = ap.parse_args(argv)

    if args.backfill_outcome:
        return backfill_outcome()

    key = load_api_key()
    start_ts = int(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(time.time()) if args.end is None else int(
        datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    conn = init_manifest()
    skip_missing = known_skip(conn)
    candidates = [
        candidate
        for candidate in enumerate_slugs(
            start_ts,
            end_ts,
            coins=args.coins,
            intervals=args.intervals,
        )
        if candidate[0] not in skip_missing
    ]
    total = len(candidates)
    rate = Rate(args.rate)
    stats = {"ok": 0, "skip": 0, "missing": 0, "error": 0, "throttled": 0, "empty": 0}
    t_start = time.monotonic()
    print(f"[pmdata-dl] start={args.start} end_ts={end_ts} candidates={total} "
          f"(skipping {len(skip_missing)} known-missing) rate={args.rate}/s workers={args.workers}", flush=True)

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = []
        for slug, coin, iv in candidates:
            futs.append(ex.submit(fetch_one, slug, coin, iv, key, rate, conn, stats))
        for f in futs:
            f.result()
            done += 1
            if done % 1000 == 0:
                el = time.monotonic() - t_start
                rps = done / el if el else 0
                eta = (total - done) / rps / 60 if rps else 0
                print(f"[pmdata-dl] {done}/{total}  ok={stats['ok']} skip={stats['skip']} "
                      f"miss={stats['missing']} err={stats['error']} thr={stats['throttled']}  "
                      f"{rps:.1f} req/s  ETA {eta:.0f} min", flush=True)
    el = time.monotonic() - t_start
    print(f"[pmdata-dl] DONE {done}/{total} in {el/60:.1f} min  "
          f"ok={stats['ok']} skip={stats['skip']} miss={stats['missing']} err={stats['error']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
