"""Download the last-30-second CLOB window plus a five-second lead buffer."""
from __future__ import annotations

import argparse
import io
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.fetch._common import load_dotenv_into_environ
from polymarket.backtest.lib.tick_store import DEFAULT_TICK_DB, TickStore, open_store

URL = "https://api.pmdata.dev/download/poly_l2/{}.parquet"
KEEP = ["timestamp", "ask_prices", "ask_sizes", "bid_prices", "bid_sizes"]
DEFAULT_OUT = DEFAULT_TICK_DB
_thread_local = threading.local()


def _session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        _thread_local.session = session
    return session


def _window_cutoff(slug: str) -> int:
    parts = slug.split("-")
    if len(parts) < 4 or parts[2] not in {"5m", "15m"}:
        raise ValueError(f"unrecognized market slug: {slug}")
    window_start = int(parts[3])
    return window_start + (865 if parts[2] == "15m" else 265)


def _download_one(
    slug: str,
    *,
    api_key: str,
    store: TickStore,
    retries: int,
) -> str:
    if store.has_slug(slug):
        return "skip"

    for attempt in range(retries + 1):
        try:
            response = _session().get(
                URL.format(slug),
                headers={"api_key": api_key, "User-Agent": "cryptobot-backtest"},
                timeout=40,
            )
        except requests.RequestException:
            if attempt == retries:
                return "error"
            time.sleep(2 ** attempt)
            continue
        if response.status_code == 429 and attempt < retries:
            time.sleep(2 ** (attempt + 1))
            continue
        if response.status_code == 404:
            return "missing"
        if response.status_code != 200:
            return "error"
        try:
            ticks = pd.read_parquet(io.BytesIO(response.content))
            ticks = ticks[ticks["event_type"] == "book"].copy()
            ticks["sec"] = (
                pd.to_datetime(ticks["timestamp"], utc=True)
                .values.astype("datetime64[ms]")
                .astype("int64")
                // 1000
            )
            ticks = ticks[ticks["sec"] >= _window_cutoff(slug)][KEEP].reset_index(drop=True)
            if ticks.empty:
                return "empty"
            store.write_ticks(slug, ticks)
            return "ok"
        except (KeyError, TypeError, ValueError):
            return "error"
    return "error"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slugs", type=Path, required=True, help="JSON array of market slugs")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="SQLite tick store path")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()

    load_dotenv_into_environ(REPO_ROOT / ".env")
    api_key = os.environ.get("PMDATA_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("PMDATA_API_KEY is required")
    slugs = json.loads(args.slugs.read_text(encoding="utf-8"))
    if not isinstance(slugs, list) or not all(isinstance(slug, str) for slug in slugs):
        raise SystemExit("--slugs must contain a JSON array of strings")

    stats = {key: 0 for key in ("ok", "skip", "missing", "empty", "error")}
    started = time.time()
    with open_store(args.out) as store:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    _download_one,
                    slug,
                    api_key=api_key,
                    store=store,
                    retries=args.retries,
                )
                for slug in slugs
            ]
            for index, future in enumerate(as_completed(futures), 1):
                stats[future.result()] += 1
                if index % 100 == 0:
                    print(f"{index}/{len(slugs)} {stats}", flush=True)
    print(f"done {stats} in {(time.time() - started) / 60:.1f} min")
    return 1 if stats["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
