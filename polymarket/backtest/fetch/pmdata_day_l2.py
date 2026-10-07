"""Download PMData daily L2 ZIPs and retain only per-second market snapshots.

The Day API is the quota-safe route for bulk history.  Each archive is handled
serially, extracted into the existing ``poly_snapshot`` layout, and discarded
before the next archive is fetched, bounding temporary disk use.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from polymarket.backtest.fetch._common import load_dotenv_into_environ
from polymarket.backtest.fetch.pmdata_download import MIN_BYTES, SNAP, resample_per_second

COINS = ("btc", "eth", "sol", "xrp")
INTERVALS = ("5m", "15m")
URL = (
    "https://api.pmdata.dev/polymarket/{series}/poly_l2/"
    "{series}_poly_l2_{date}.zip"
)


def _slug_from_member(member: str, coin: str, interval: str) -> str | None:
    name = Path(member).name
    if not name.endswith(".parquet"):
        return None
    slug = name.removesuffix(".parquet")
    prefix = f"{coin}-updown-{interval}-"
    if not slug.startswith(prefix) or not slug.removeprefix(prefix).isdigit():
        return None
    return slug


def download_day(date: str, coin: str, interval: str, *, api_key: str) -> dict:
    series = f"{coin}-{interval}"
    url = URL.format(series=series, date=date)
    out_dir = SNAP / coin / interval
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"pmdata-{series}-{date}-") as td:
        archive = Path(td) / f"{series}.zip"
        for attempt in range(4):
            try:
                with requests.get(
                    url,
                    headers={"api_key": api_key},
                    stream=True,
                    timeout=(15, 180),
                ) as response:
                    response.raise_for_status()
                    with archive.open("wb") as handle:
                        for chunk in response.iter_content(4 * 1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                break
            except requests.RequestException:
                archive.unlink(missing_ok=True)
                if attempt == 3:
                    raise
                time.sleep(2.0 ** attempt)
        written = skipped = ignored = 0
        with zipfile.ZipFile(archive) as bundle:
            for member in bundle.namelist():
                slug = _slug_from_member(member, coin, interval)
                if slug is None:
                    ignored += 1
                    continue
                target = out_dir / f"{slug}.parquet"
                if target.exists() and target.stat().st_size > MIN_BYTES:
                    skipped += 1
                    continue
                frame = resample_per_second(bundle.read(member))
                if frame is None:
                    ignored += 1
                    continue
                temp = target.with_suffix(".tmp.parquet")
                frame.to_parquet(temp, index=False)
                temp.replace(target)
                written += 1
    return {
        "series": series, "date": date, "written": written,
        "skipped": skipped, "ignored": ignored,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--coins", nargs="+", choices=COINS, default=list(COINS))
    parser.add_argument(
        "--intervals", nargs="+", choices=INTERVALS, default=list(INTERVALS)
    )
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    if args.date:
        if args.start or args.end:
            parser.error("--date cannot be combined with --start/--end")
        dates = [args.date]
    else:
        if not args.start or not args.end:
            parser.error("provide --date or both --start and --end")
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
        if end < start:
            parser.error("--end must be on or after --start")
        dates = [
            (start + dt.timedelta(days=offset)).isoformat()
            for offset in range((end - start).days + 1)
        ]
    if args.workers < 1:
        parser.error("--workers must be positive")
    load_dotenv_into_environ(Path(".env"))
    api_key = os.environ.get("PMDATA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("PMDATA_API_KEY is required")
    jobs = [
        (date, coin, interval)
        for date in dates
        for coin in args.coins
        for interval in args.intervals
    ]
    completed = 0
    errors: list[tuple[tuple[str, str, str], str]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                download_day, date, coin, interval, api_key=api_key
            ): (date, coin, interval)
            for date, coin, interval in jobs
        }
        for future in as_completed(futures):
            completed += 1
            job = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # one corrupt archive must not lose the run
                errors.append((job, str(exc)))
                print(
                    {"job": job, "error": str(exc),
                     "progress": f"{completed}/{len(jobs)}"},
                    flush=True,
                )
                continue
            result["progress"] = f"{completed}/{len(jobs)}"
            print(result, flush=True)
    if errors:
        raise RuntimeError(f"{len(errors)} PMData day download(s) failed")


if __name__ == "__main__":
    main()
