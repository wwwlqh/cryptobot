"""Build the reproducible option3 pool for the last-30-second subminute strategy.

Timing conventions:
- option3 (default): the signal/strike uses the CLOSE of decision second ``t``
  (only known once ``t`` completes), and the fill walks the order-book snapshot
  as-of ``t + fill_delay`` (default 1s). Model features (ask/other_ask/spread/
  depth) come from the decision book as-of ``t`` and are knowable at decision time.
- option2: signal from the close of ``t-1``; fill as-of ``t``.

This is exactly the execution-timing fix for the live-vs-backtest gap
(verified Jul 6 BTC 06:05 offset 292: live saw open 63072.01, the leaky
backtest used the same second's close 63103.52 AND filled on that second's
book).

Output is streamed into the option3 pool as part parquets with decision-book
features, fill economics, settlement labels, folds A-E, reachability, and QA
flags. The default rebuild is step1last only; alternate sub-minute grids remain
available explicitly for validation.
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core" / "src"))
from polymarket.backtest import BT_RUNS_DIR  # noqa: E402

from polymarket.backtest.training import checkpoint as cg  # noqa: E402
from polymarket.backtest.training import pmdata_target as etgt  # noqa: E402
from polymarket.backtest.training import offset_wfo as base  # noqa: E402

OUT_DIR = BT_RUNS_DIR / "option3_pool"

# Frozen fold calendar (E = fresh acceptance holdout).
FOLD_EDGES: tuple[tuple[str, str], ...] = (
    ("A", "2026-02-09"),
    ("B", "2026-03-16"),
    ("C", "2026-04-06"),
    ("D", "2026-05-08"),
    ("E", "2026-06-10"),
    ("_end", "2026-07-06"),
)

REACH_LEASH = 0.05  # live 5-cent price leash; fill anchored to the decision ask

SUBMINUTE_GRIDS = tuple(g for g in cg.GRID_ORDER if g != "baseline60")


def _fold_for_entry(entry_ts: pd.Series) -> pd.Series:
    dt = pd.to_datetime(entry_ts, unit="s", utc=True)
    edges = pd.to_datetime([d for _, d in FOLD_EDGES], utc=True)
    labels = [name for name, _ in FOLD_EDGES[:-1]]
    return pd.cut(dt, bins=edges, labels=labels, right=False).astype(object)


def _load_ok_outcomes() -> pd.DataFrame:
    cols = ["market_slug", "won_up", "status"]
    out = pd.read_parquet(base.OUTCOMES, columns=cols)
    out = out[(out["status"] == "ok") & out["won_up"].notna()].copy()
    out["won_up"] = out["won_up"].astype(int)
    return out[["market_slug", "won_up"]]


def _book_asof(df: pd.DataFrame, ts_sec: np.ndarray, at_ts: int) -> tuple[dict[str, Any], int] | None:
    """Full two-sided book as-of ``at_ts`` plus the snapshot second used."""
    pos = int(np.searchsorted(ts_sec, int(at_ts), side="right") - 1)
    if pos < 0:
        return None
    snap = df.iloc[pos]
    if any(snap[c] is None for c in ("ask_prices", "ask_sizes", "bid_prices", "bid_sizes")):
        return None  # drift variant: rows with null book arrays
    up, down = base._side_books_from_snapshot(snap)
    if not up or not down:
        return None
    up_depth = float(sum(a["price"] * a["size"] for a in up))
    down_depth = float(sum(a["price"] * a["size"] for a in down))
    if up_depth <= 0 or down_depth <= 0:
        return None
    book = {
        "ask": float(up[0]["price"]),
        "other_ask": float(down[0]["price"]),
        "book_spread": float(up[0]["price"] + down[0]["price"] - 1.0),
        "up_prices": [a["price"] for a in up],
        "up_sizes": [a["size"] for a in up],
        "down_prices": [a["price"] for a in down],
        "down_sizes": [a["size"] for a in down],
        "up_depth": up_depth,
        "down_depth": down_depth,
    }
    return book, int(ts_sec[pos])


def _attach_dual_books(rows: pd.DataFrame, fill_delay: int) -> pd.DataFrame:
    """Attach decision book (as-of entry_ts) and fill book (as-of entry_ts+delay)."""
    if rows.empty:
        return rows.copy()
    records: list[dict[str, Any]] = []
    drops = {"no_snapshot": 0, "no_decision_book": 0, "no_fill_book": 0}
    t0 = time.time()
    grouped = rows.groupby(["coin", "interval", "market_slug"], sort=False, observed=True)
    for i, ((coin, interval, slug), group) in enumerate(grouped, start=1):
        if i == 1 or i % 1000 == 0:
            print(
                f"[books] markets={i:,} rows_out={len(records):,} elapsed={time.time() - t0:.1f}s",
                file=sys.stderr,
                flush=True,
            )
        # bypass the lru_cache: each market is visited exactly once here, and the
        # 4096-entry cache pins gigabytes of dead DataFrames (laptop OOM 2026-07-07)
        df = etgt._load_raw_snapshot.__wrapped__(str(coin), str(interval), str(slug))
        if df is None or df.empty:
            drops["no_snapshot"] += len(group)
            continue
        ts_sec = (
            df["timestamp"].astype("datetime64[ns]").astype("int64") // 1_000_000_000
        ).to_numpy(dtype=np.int64)
        order = np.argsort(ts_sec, kind="stable")
        df = df.iloc[order].reset_index(drop=True)
        ts_sec = ts_sec[order]
        for row in group.itertuples(index=False):
            entry_ts = int(row.entry_ts)
            dec = _book_asof(df, ts_sec, entry_ts)
            if dec is None:
                drops["no_decision_book"] += 1
                continue
            fill = _book_asof(df, ts_sec, entry_ts + int(fill_delay))
            if fill is None:
                drops["no_fill_book"] += 1
                continue
            dec_book, dec_ts = dec
            fill_book, fill_ts = fill
            d = row._asdict()
            # model features: decision book (knowable at decision time)
            d["ask"] = float(dec_book["ask"])
            d["other_ask"] = float(dec_book["other_ask"])
            d["book_spread"] = float(dec_book["book_spread"])
            d["ask_depth_top5_usd"] = float(dec_book["up_depth"])
            d["bid_depth_top5_usd"] = float(dec_book["down_depth"])
            d["up_depth"] = float(dec_book["up_depth"])
            d["down_depth"] = float(dec_book["down_depth"])
            d["dec_book_age_s"] = entry_ts - dec_ts
            # execution: fill book (t + fill_delay)
            d["fill_ask"] = float(fill_book["ask"])
            d["fill_other_ask"] = float(fill_book["other_ask"])
            d["fill_up_prices"] = list(fill_book["up_prices"])
            d["fill_up_sizes"] = list(fill_book["up_sizes"])
            d["fill_down_prices"] = list(fill_book["down_prices"])
            d["fill_down_sizes"] = list(fill_book["down_sizes"])
            d["fill_up_depth"] = float(fill_book["up_depth"])
            d["fill_down_depth"] = float(fill_book["down_depth"])
            d["fill_book_age_s"] = entry_ts + int(fill_delay) - fill_ts
            d["fill_book_changed"] = fill_ts != dec_ts
            # reachability vs the decision anchor (per side)
            d["reachable_up"] = bool(d["fill_ask"] <= d["ask"] + REACH_LEASH)
            d["reachable_down"] = bool(d["fill_other_ask"] <= d["other_ask"] + REACH_LEASH)
            # QA flags (mirrors polymarket/backtest/lib/pmdata_clean.py policy)
            d["qa_book_crossed"] = bool(d["book_spread"] < -0.02)
            records.append(d)
    out = pd.DataFrame(records)
    out.attrs["book_drops"] = drops
    return out


def build_grid_pool(
    grid_name: str,
    *,
    convention: str,
    fill_delay: int,
    since: str | None,
    until: str | None,
    limit_markets: int | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    # ENTRY GUARD (2026-08-01). _attach_dual_books below reads every market's
    # book from the poly_snapshot tree deleted 2026-07-31; with it gone
    # _load_raw_snapshot returns None for all of them, drops["no_snapshot"]
    # eats the whole frame, and main() writes an EMPTY part parquet per chunk
    # that is indistinguishable from a genuine no-signal span. Guard here — the
    # entry both main() and any library caller cross — so a rebuild dies before
    # it reads a single parquet, not after it has written 26 empty parts.
    etgt.require_snapshot_dir()
    t0 = time.time()
    markets = cg._read_market_rows(since=since, until=until, limit_markets=limit_markets)
    expanded = cg._expand_checkpoints(markets, grid_name)
    if grid_name == "step5":
        # study spec (2026-07-07): step5 owns ONLY the early/mid window —
        # minute marks (0,60,120,...) belong to baseline60, the last 30s
        # belongs to step1last (1s cadence). Zero cross-grid overlap.
        sec = expanded["checkpoint_sec"].astype(int)
        last30_start = expanded["interval"].map(cg._interval_seconds) - 30
        expanded = expanded[(sec % 60 != 0) & (sec < last30_start)].copy()
    print(f"[prepare] grid={grid_name} expanded={len(expanded):,}", file=sys.stderr, flush=True)

    signal_shift = -1 if convention == "option2" else 0
    featured_in = expanded.copy()
    if signal_shift:
        featured_in["entry_ts"] = featured_in["entry_ts"].astype(np.int64) + signal_shift
        featured_in["entry_minute"] = (
            featured_in["checkpoint_sec"].astype(float) + signal_shift
        ) / 60.0
    featured = cg._attach_checkpoint_features(featured_in)
    if signal_shift:
        featured["entry_ts"] = featured["entry_ts"].astype(np.int64) - signal_shift
        featured["entry_minute"] = featured["checkpoint_sec"].astype(float) / 60.0
    featured = featured.dropna(subset=["range_ret", "entry_ret", "partial_ret_z_1h"]).copy()
    print(f"[prepare] grid={grid_name} featured={len(featured):,}", file=sys.stderr, flush=True)

    fill_at = fill_delay if convention == "option3" else 0
    booked = _attach_dual_books(featured, fill_at)
    print(f"[prepare] grid={grid_name} booked={len(booked):,}", file=sys.stderr, flush=True)

    if booked.empty:
        return booked, {"grid": grid_name, "rows": 0, "elapsed_s": round(time.time() - t0, 2)}

    rows = booked.merge(_load_ok_outcomes(), on="market_slug", how="inner")
    rows["fold"] = _fold_for_entry(rows["entry_ts"])
    rows = rows.dropna(subset=["fold"]).copy()

    diag = {
        "grid": grid_name,
        "convention": convention,
        "fill_delay_s": fill_at,
        "signal": "close(t)" if convention == "option3" else "close(t-1)",
        "market_rows": int(len(markets)),
        "expanded_rows": int(len(expanded)),
        "feature_rows": int(len(featured)),
        "book_rows": int(len(booked)),
        "book_drops": booked.attrs.get("book_drops", {}),
        "labeled_rows": int(len(rows)),
        "fold_rows": rows["fold"].value_counts().sort_index().to_dict(),
        "reachable_up_rate_by_fold": rows.groupby("fold", observed=True)["reachable_up"].mean().round(4).to_dict(),
        "fill_book_changed_rate": round(float(rows["fill_book_changed"].mean()), 4),
        "crossed_book_rate": round(float(rows["qa_book_crossed"].mean()), 5),
        "elapsed_s": round(time.time() - t0, 2),
    }
    return rows.reset_index(drop=True), diag


def _date_chunks(since: str, until: str, days: int) -> list[tuple[str, str]]:
    edges = pd.date_range(pd.Timestamp(since), pd.Timestamp(until), freq=f"{days}D")
    edges = list(edges)
    if edges[-1] < pd.Timestamp(until):
        edges.append(pd.Timestamp(until))
    return [(str(a.date()), str(b.date())) for a, b in zip(edges[:-1], edges[1:])]


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grids", default="step1last")
    parser.add_argument("--convention", choices=("option3", "option2"), default="option3")
    parser.add_argument("--fill-delay", type=int, default=1)
    parser.add_argument("--since", default="2026-02-09")
    parser.add_argument("--until", default="2026-07-06")
    parser.add_argument("--chunk-days", type=int, default=5)
    parser.add_argument("--limit-markets", type=int, default=None)
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    out_dir = Path(args.out_dir)
    grids = cg._parse_csv(args.grids)
    unknown = [g for g in grids if g not in SUBMINUTE_GRIDS]
    if unknown:
        raise SystemExit(
            f"unsupported grids {unknown}; this builder covers sub-minute grids "
            f"{SUBMINUTE_GRIDS} (baseline60 uses the committed minute path)"
        )

    # MEMORY RULE (feedback_wfo_memory_budget): stream part-files per date chunk,
    # never hold the full pool in RAM; existing parts are skipped so a killed run
    # resumes where it stopped.
    chunks = _date_chunks(args.since, args.until, int(args.chunk_days))
    for grid in grids:
        grid_dir = out_dir / f"pool_{grid}_{args.convention}"
        grid_dir.mkdir(parents=True, exist_ok=True)
        part_diags: list[dict[str, Any]] = []
        for lo, hi in chunks:
            part_path = grid_dir / f"part_{lo}.parquet"
            diag_path = grid_dir / f"part_{lo}_diag.json"
            if part_path.exists() and not args.force:
                print(f"[skip] {part_path} exists", file=sys.stderr, flush=True)
                if diag_path.exists():
                    part_diags.append(json.loads(diag_path.read_text()))
                continue
            rows, diag = build_grid_pool(
                grid,
                convention=args.convention,
                fill_delay=int(args.fill_delay),
                since=lo,
                until=hi,
                limit_markets=args.limit_markets,
            )
            diag["chunk"] = [lo, hi]
            rows.to_parquet(part_path, index=False)
            diag_path.write_text(json.dumps(cg._json_safe(diag), indent=2))
            part_diags.append(diag)
            print(f"[part] {part_path} rows={len(rows):,}", flush=True)
            del rows
            gc.collect()
        merged = {
            "grid": grid,
            "convention": args.convention,
            "span": [args.since, args.until],
            "n_parts": len(part_diags),
            "labeled_rows": int(sum(d.get("labeled_rows", 0) for d in part_diags)),
        }
        (out_dir / f"pool_{grid}_{args.convention}_diag.json").write_text(
            json.dumps(cg._json_safe(merged), indent=2)
        )
        print(f"[done] grid={grid} {json.dumps(merged)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
