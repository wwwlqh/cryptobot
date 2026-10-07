"""Fetch OFFICIAL Polymarket resolutions (Gamma API) and build pmdata_outcomes.parquet.

Polymarket settles crypto up/down markets on Chainlink; the venue's recorded
resolution follows that and exists for FULL history via Gamma ``/markets?slug=``.
This is the SINGLE Gamma resolution fetcher (2026-07-12 consolidation): it pulls
``gamma_won_up`` for every live-coin market and folds the results into
``pmdata_outcomes.parquet`` — the canonical settlement table whose ``won_up``
column is read by the WFO arbiter (polymarket.backtest.training.offset_wfo),
`lib/pmdata_clean.load_outcomes`, `training/build_pool._load_ok_outcomes` and
`training/checkpoint._load_outcomes`.

Universe = live-coin (btc/eth/sol/xrp) markets in pmdata/snapshot_features_full.parquet
(the book-snapshot universe), UNION any slug already in pmdata_outcomes.parquet
(so WFO markets keep coverage; this accumulated parquet also already carries
every slug the retired live-LUT source used to contribute).
2026-07-31 (exp044 phase 3): the retired live-LUT parquet is gone from disk;
the discovery side now reads snapshot_features_full instead.

Two phases (both run by default; --assemble-only / --no-assemble to split):
  fetch    -> gamma_won_up per slug, checkpointed to gamma_resolutions/part_*.parquet
              (resumable: skips slugs already fetched; RPS throttle + backoff)
  assemble -> fold all parts into pmdata_outcomes.parquet
              [market_slug, coin, interval, slug_ts, gamma_won_up, won_up,
               status ('ok'|'ambiguous'), label_source='gamma']

⚠️ DISCOVERY CEILING (why --since/--until exists, added 2026-08-02): both
discovery sources are FROZEN datasets — snapshot_features_full.parquet and the
existing pmdata_outcomes.parquet both end 2026-07-21. So the default universe
can NEVER reach a market newer than the last file refresh, and a plain re-run
silently returns "nothing new" instead of extending coverage. ``--since/--until``
bypasses that by GENERATING the slugs arithmetically: crypto up/down slugs are
fully deterministic (``{coin}-updown-{interval}m-{window_start_ts}`` on exact
300s / 900s boundaries), so every market in a date range can be enumerated
without any prior index. Slugs that never existed simply resolve to NaN and are
recorded 'ambiguous' — harmless.

Usage:
    python -m polymarket.backtest.fetch.fetch_gamma_resolutions            # fetch new + rebuild parquet
    python -m polymarket.backtest.fetch.fetch_gamma_resolutions --assemble-only   # rebuild parquet from parts
    # extend coverage past the frozen-index ceiling:
    python -m polymarket.backtest.fetch.fetch_gamma_resolutions --since 2026-07-21 --until 2026-08-02
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import httpx
import pandas as pd

from polymarket.backtest.engine.sweep_lib import atomic_to_parquet
from polymarket.backtest.fetch._common import COINS, parse_slug

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
from polymarket.backtest import BT_DATA_DIR  # noqa: E402
OUT_DIR = BT_DATA_DIR / "pmdata" / "gamma_resolutions"
OUTCOMES = BT_DATA_DIR / "pmdata" / "pmdata_outcomes.parquet"
TWAP_PRICES = BT_DATA_DIR / "pmdata" / "gamma_twap_prices.parquet"
SNAPSHOT_FEATURES = BT_DATA_DIR / "pmdata" / "snapshot_features_full.parquet"
GAMMA = "https://gamma-api.polymarket.com/markets"


def _parse_market(slug: str, payload) -> dict:
    row = {
        "market_slug": slug,
        "gamma_won_up": float("nan"),
        "outcome_prices": None,
        "closed": None,
        "uma_status": None,
        "resolution_source": None,
        "twap_enabled": None,
        "twap_lookback_seconds": None,
        "price_to_beat": float("nan"),
        "final_price": float("nan"),
        "fetched_at": time.time(),
    }
    if not payload:
        return row
    m = payload[0]
    row["closed"] = bool(m.get("closed")) if m.get("closed") is not None else None
    row["uma_status"] = m.get("umaResolutionStatus")
    row["resolution_source"] = m.get("resolutionSource")
    config = m.get("cryptoMarketConfig") or {}
    row["twap_enabled"] = config.get("twapEnabled")
    row["twap_lookback_seconds"] = config.get("twapLookbackSeconds")
    events = m.get("events") or []
    event = events[0] if events and isinstance(events[0], dict) else {}
    metadata = event.get("eventMetadata") or m.get("eventMetadata") or {}
    try:
        row["price_to_beat"] = float(metadata["priceToBeat"])
        row["final_price"] = float(metadata["finalPrice"])
    except (KeyError, TypeError, ValueError):
        pass
    prices = m.get("outcomePrices")
    outcomes = m.get("outcomes")
    try:
        if isinstance(prices, str):
            prices = json.loads(prices)
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        row["outcome_prices"] = json.dumps(prices)
        if prices and outcomes and len(prices) == len(outcomes):
            by = {str(o).strip().lower(): float(p) for o, p in zip(outcomes, prices)}
            up, down = by.get("up"), by.get("down")
            # resolved markets report 1/0 exactly
            if up is not None and down is not None and {up, down} == {0.0, 1.0}:
                row["gamma_won_up"] = 1.0 if up == 1.0 else 0.0
    except Exception:  # noqa: BLE001 — malformed payloads recorded as NaN
        pass
    return row


INTERVAL_SECONDS = {"5m": 300, "15m": 900}


def _generated_slugs(since: str, until: str) -> list[str]:
    """Enumerate every deterministic up/down slug in [since, until) UTC.

    Crypto up/down markets are minted on exact interval boundaries, so the slug
    set for a date range is computable with no index: 5m windows open at every
    multiple of 300s, 15m at every multiple of 900s. This is what lets coverage
    extend past the frozen snapshot_features_full / pmdata_outcomes ceiling.

    Over-generation is safe and expected: a slug that was never minted returns
    no Gamma market and lands as NaN -> status 'ambiguous', exactly like a
    market that has not settled yet.
    """
    lo = int(pd.Timestamp(since, tz="UTC").timestamp())
    hi = int(pd.Timestamp(until, tz="UTC").timestamp())
    out: list[str] = []
    for coin in COINS:
        for interval, step in INTERVAL_SECONDS.items():
            start = lo - (lo % step)
            out.extend(
                f"{coin}-updown-{interval}-{ts}" for ts in range(start, hi, step)
            )
    return out


def _universe_slugs() -> list[str]:
    """Slugs to resolve: live-coin markets in snapshot_features_full (the pmdata
    book-snapshot universe) UNION any slug already in pmdata_outcomes (so the
    WFO arbiter's markets keep coverage; pmdata_outcomes already carries every
    slug the retired live-LUT source used to contribute, so no history is lost).
    2026-07-31 (exp044 phase 3): the retired live-LUT parquet is gone from disk."""
    slugs: set[str] = set()
    if SNAPSHOT_FEATURES.exists():
        sf = pd.read_parquet(SNAPSHOT_FEATURES, columns=["market_slug", "coin"])
        slugs |= set(sf.loc[sf["coin"].isin(COINS), "market_slug"])
    if OUTCOMES.exists():
        slugs |= set(pd.read_parquet(OUTCOMES, columns=["market_slug"])["market_slug"])
    return sorted(slugs)


def assemble_outcomes(out_path: Path = OUTCOMES) -> None:
    """Fold every gamma_resolutions/ part into pmdata_outcomes.parquet.

    won_up = gamma_won_up (the Gamma Chainlink settlement); status='ok' where
    definitive (0/1), else 'ambiguous'. Keyed by market_slug — last write wins,
    so a re-resolved market supersedes an earlier 'open' fetch; coin/interval/
    slug_ts parsed from the slug. This IS the outcome-table build: the WFO
    arbiter (polymarket.backtest.training.offset_wfo) reads the resulting
    ``won_up`` column (the live LUT reader was retired 2026-07-31, exp044
    phase 3)."""
    parts = sorted(OUT_DIR.glob("part_*.parquet"))
    if not parts:
        sys.exit(f"no gamma resolution parts under {OUT_DIR}; run the fetch first")
    g = pd.concat(
        [pd.read_parquet(p, columns=["market_slug", "gamma_won_up"]) for p in parts],
        ignore_index=True,
    ).drop_duplicates(subset=["market_slug"], keep="last")

    rows = []
    for slug, gwu in zip(g["market_slug"], g["gamma_won_up"]):
        try:
            coin, interval, slug_ts = parse_slug(slug)
        except (ValueError, IndexError):
            continue  # malformed slug — skip
        rows.append((slug, coin, interval, slug_ts, gwu))
    out = pd.DataFrame(rows, columns=["market_slug", "coin", "interval", "slug_ts", "gamma_won_up"])
    out["won_up"] = out["gamma_won_up"]
    out["status"] = out["won_up"].notna().map({True: "ok", False: "ambiguous"})
    out["label_source"] = "gamma"
    out = out.sort_values(["coin", "interval", "slug_ts"]).reset_index(drop=True)

    atomic_to_parquet(out, out_path, index=False)
    n_ok = int(out["won_up"].notna().sum())
    print(f"assembled {len(out):,} markets ({n_ok:,} resolved / "
          f"{len(out) - n_ok:,} ambiguous) -> {out_path}", flush=True)


def assemble_twap_prices(out_path: Path = TWAP_PRICES) -> None:
    """Assemble exact post-change Chainlink TWAP boundary values from Gamma.

    Gamma exposes the values shown by Polymarket as ``eventMetadata.priceToBeat``
    and ``eventMetadata.finalPrice``. Only rows explicitly marked with a
    TWAP-enabled crypto config and finite boundary values are retained.
    """
    columns = [
        "market_slug",
        "gamma_won_up",
        "resolution_source",
        "twap_enabled",
        "twap_lookback_seconds",
        "price_to_beat",
        "final_price",
        "closed",
        "uma_status",
        "fetched_at",
    ]
    frames: list[pd.DataFrame] = []
    for part in sorted(OUT_DIR.glob("part_*.parquet")):
        frame = pd.read_parquet(part)
        if set(columns).issubset(frame.columns):
            frames.append(frame[columns])
    if not frames:
        print("assembled 0 exact TWAP prices (no metadata-bearing parts)", flush=True)
        return

    exact = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["market_slug"], keep="last"
    )
    exact = exact[exact["twap_enabled"].eq(True)].copy()  # noqa: E712
    exact["price_to_beat"] = pd.to_numeric(exact["price_to_beat"], errors="coerce")
    exact["final_price"] = pd.to_numeric(exact["final_price"], errors="coerce")
    exact = exact.dropna(subset=["price_to_beat", "final_price"])

    parsed = exact["market_slug"].map(parse_slug)
    exact["coin"] = [item[0] for item in parsed]
    exact["interval"] = [item[1] for item in parsed]
    exact["slug_ts"] = [item[2] for item in parsed]
    exact["twap_won_up"] = (
        exact["final_price"] >= exact["price_to_beat"]
    ).astype("int8")
    exact["label_matches_gamma"] = (
        exact["twap_won_up"].astype(float) == exact["gamma_won_up"]
    )
    exact = exact.sort_values(["slug_ts", "coin", "interval"]).reset_index(drop=True)
    atomic_to_parquet(exact, out_path, index=False)
    print(
        f"assembled {len(exact):,} exact TWAP prices -> {out_path}", flush=True
    )


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--chunk", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--assemble-only", action="store_true",
                    help="skip the Gamma fetch; only (re)build pmdata_outcomes.parquet from existing parts")
    ap.add_argument("--no-assemble", action="store_true",
                    help="fetch only; do not (re)build pmdata_outcomes.parquet")
    ap.add_argument("--since", default=None,
                    help="UTC YYYY-MM-DD: ALSO generate slugs from this date "
                         "(bypasses the frozen-index discovery ceiling)")
    ap.add_argument("--until", default=None,
                    help="UTC YYYY-MM-DD exclusive; defaults to now when --since is given")
    args = ap.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not args.assemble_only:
        slugs = _universe_slugs()
        if args.since:
            until = args.until or pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
            gen = _generated_slugs(args.since, until)
            before = len(slugs)
            slugs = sorted(set(slugs) | set(gen))
            print(f"generated {len(gen):,} slugs for [{args.since}, {until}) "
                  f"-> universe {before:,} -> {len(slugs):,}", flush=True)
        parts = sorted(OUT_DIR.glob("part_*.parquet"))
        done: set[str] = set()
        for part in parts:
            done.update(pd.read_parquet(part, columns=["market_slug"])["market_slug"])
        todo = [s for s in slugs if s not in done]
        if args.limit:
            todo = todo[: args.limit]
        print(f"slugs total={len(slugs)} done={len(done)} todo={len(todo)}", flush=True)

        from concurrent.futures import ThreadPoolExecutor

        def fetch_one(client, slug):
            payload = None
            for attempt in range(5):
                try:
                    # closed=true is REQUIRED: Gamma's default index drops
                    # settled crypto up/down markets (probe 2026-07-08).
                    r = client.get(GAMMA, params={"slug": slug, "closed": "true"})
                    if r.status_code == 429:
                        time.sleep(2.0 * (attempt + 1))
                        continue
                    r.raise_for_status()
                    payload = r.json()
                    break
                except Exception:  # noqa: BLE001 - retry with backoff
                    time.sleep(1.0 * (attempt + 1))
            return _parse_market(slug, payload)

        part_idx = len(parts)
        done_n = 0
        with httpx.Client(
            timeout=10.0,
            http2=True,
            headers={
                "Accept": "application/json",
                "User-Agent": "cryptobot-research/1.0 (Polymarket Gamma audit)",
            },
            limits=httpx.Limits(max_connections=args.workers, max_keepalive_connections=args.workers),
        ) as client:
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                for start in range(0, len(todo), args.chunk):
                    chunk = todo[start : start + args.chunk]
                    rows = list(ex.map(lambda s: fetch_one(client, s), chunk))
                    part_idx += 1
                    pd.DataFrame(rows).to_parquet(OUT_DIR / f"part_{part_idx:05d}.parquet", index=False)
                    done_n += len(rows)
                    n_ok = sum(1 for x in rows if pd.notna(x["gamma_won_up"]))
                    print(f"[part {part_idx}] {done_n}/{len(todo)} resolved_in_part={n_ok}/{len(rows)}", flush=True)
        print("fetch done", flush=True)

    if not args.no_assemble:
        assemble_outcomes()
        assemble_twap_prices()
    return 0


if __name__ == "__main__":
    sys.exit(main())
