"""Download exact post-change crypto TWAP boundary prices from Gamma.

This script is intentionally standalone so it can run on the Ireland trading
host without the research package being deployed there. It writes no live bot
state and never places orders.
"""
from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pandas as pd

COINS = ("btc", "eth", "sol", "xrp")
INTERVAL_SECONDS = {"5m": 300, "15m": 900}
GAMMA_SLUG = "https://gamma-api.polymarket.com/markets/slug/{slug}"


def generated_slugs(
    since: str,
    until: str,
    intervals: tuple[str, ...] | None = None,
) -> list[str]:
    lo = int(pd.Timestamp(since, tz="UTC").timestamp())
    hi = int(pd.Timestamp(until, tz="UTC").timestamp())
    selected = tuple(INTERVAL_SECONDS) if intervals is None else intervals
    return [
        f"{coin}-updown-{interval}-{ts}"
        for coin in COINS
        for interval in selected
        for step in (INTERVAL_SECONDS[interval],)
        for ts in range(lo - (lo % step), hi, step)
    ]


def parse_exact_market(slug: str, market: dict | None) -> dict | None:
    if not market:
        return None
    config = market.get("cryptoMarketConfig") or {}
    if config.get("twapEnabled") is not True:
        return None
    events = market.get("events") or []
    event = events[0] if events and isinstance(events[0], dict) else {}
    metadata = event.get("eventMetadata") or market.get("eventMetadata") or {}
    try:
        price_to_beat = float(metadata["priceToBeat"])
        final_price = float(metadata["finalPrice"])
        coin, _, interval, slug_ts_text = slug.split("-")
        slug_ts = int(slug_ts_text)
    except (KeyError, TypeError, ValueError):
        return None
    prices = market.get("outcomePrices")
    if isinstance(prices, str):
        import json

        prices = json.loads(prices)
    gamma_won_up = None
    if prices and len(prices) == 2 and {float(x) for x in prices} == {0.0, 1.0}:
        gamma_won_up = int(float(prices[0]) == 1.0)
    twap_won_up = int(final_price >= price_to_beat)
    return {
        "market_slug": slug,
        "coin": coin,
        "interval": interval,
        "slug_ts": slug_ts,
        "price_to_beat": price_to_beat,
        "final_price": final_price,
        "twap_lookback_seconds": int(config["twapLookbackSeconds"]),
        "resolution_source": market.get("resolutionSource"),
        "gamma_won_up": gamma_won_up,
        "twap_won_up": twap_won_up,
        "label_matches_gamma": gamma_won_up == twap_won_up,
        "closed": bool(market.get("closed")),
        "fetched_at": time.time(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument(
        "--intervals", nargs="+", choices=tuple(INTERVAL_SECONDS),
        default=tuple(INTERVAL_SECONDS),
    )
    args = parser.parse_args(argv)

    out = Path(args.out)
    existing = pd.DataFrame()
    slugs = generated_slugs(args.since, args.until, tuple(args.intervals))
    if out.exists():
        existing = pd.read_parquet(out)
        done = set(existing["market_slug"].astype(str))
        slugs = [slug for slug in slugs if slug not in done]
        print(f"resume existing={len(existing):,} remaining={len(slugs):,}", flush=True)
    rows: list[dict] = []
    headers = {
        "Accept": "application/json",
        "User-Agent": "cryptobot-research/1.0 (Polymarket Gamma audit)",
    }

    def fetch_one(client: httpx.Client, slug: str) -> dict | None:
        for attempt in range(5):
            try:
                response = client.get(GAMMA_SLUG.format(slug=slug))
                if response.status_code == 429:
                    time.sleep(1.0 + attempt)
                    continue
                response.raise_for_status()
                return parse_exact_market(slug, response.json())
            except Exception:  # noqa: BLE001 - bounded public-read retry
                time.sleep(0.5 * (attempt + 1))
        return None

    with httpx.Client(
        timeout=15.0,
        http2=True,
        headers=headers,
        limits=httpx.Limits(
            max_connections=args.workers,
            max_keepalive_connections=args.workers,
        ),
    ) as client:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for index, row in enumerate(
                executor.map(lambda slug: fetch_one(client, slug), slugs), start=1
            ):
                if row is not None:
                    rows.append(row)
                if index % args.progress_every == 0 or index == len(slugs):
                    print(f"fetched={index:,}/{len(slugs):,} exact={len(rows):,}", flush=True)

    frame = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    if frame.empty:
        raise RuntimeError("Gamma returned zero exact TWAP boundary rows")
    frame = frame.drop_duplicates(subset=["market_slug"], keep="last")
    frame = frame.sort_values(["slug_ts", "coin", "interval"]).reset_index(drop=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out, index=False)
    mismatches = int((~frame["label_matches_gamma"]).sum())
    print(f"wrote {len(frame):,} rows to {out}; label_mismatches={mismatches}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
