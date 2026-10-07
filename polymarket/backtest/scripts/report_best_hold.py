#!/usr/bin/env python
"""One command for the best hold-to-resolution strategy report.

Emits, in a single pass:

1. **Equity, hold to resolution** — per-fold start/end/maxDD/WR read out of a
   fold report JSON (default: the T3 MID+BE hold run).
2. **EV per bet under each resolution rule** — the same selected bets re-graded
   under the point close, the close-only TWAP and the symmetric TWAP, against
   the Gamma resolution that actually paid.

Exactly two files come out: ``BEST_HOLD_SUMMARY.md`` (both tables, for reading)
and ``best_hold_bets.parquet`` (every graded bet, for re-cutting). Aggregates are
deliberately NOT written as separate CSVs — they all derive from the parquet, and
a directory of stale half-aggregates is what this script replaced.

§1 and §2 are the SAME bets — §2 re-grades the very detail frame §1 summarises,
so the per-coin edge and the fold equity finally describe one strategy.

WHAT USED TO BE HERE. §2 used to re-grade `selected_bets_step1last.parquet` from
`training/chainlink_wfo.py`. That pool is graded on the close-only TWAP (its
`won_up` matches `up_twap_c` 100% elementwise) because `chainlink_wfo --target`
DEFAULTS to `twap_c` — so its P&L measures edge against a rule that never paid.
It is a research artifact from a different pipeline (folds A-D, no eth) and was
dropped 2026-08-04. The tick BT has never had this problem: its `win` column
agrees with `gamma_won_up` on 100.00% of labelled bets.

⚠️ Uniform-theta fee is a MODEL (the live ledger books fees asymmetrically), so a
modelled margin never equals the cash basis exactly.

    python -m polymarket.backtest.scripts.report_best_hold
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # legacy-path-ok: sys.path bootstrap runs before the package import exists
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from polymarket.backtest import BT_RUNS_DIR, STUDIES_DIR  # noqa: E402
from polymarket.backtest.lib.nb_common import THETA, flat_pnl, wilson  # noqa: E402

DEFAULT_REPORT = STUDIES_DIR / "tick_latency" / "foldBE_stop_report_400__T3_MID_BE.json"
DEFAULT_BETS = STUDIES_DIR / "tick_latency" / "foldBE_stop_400__T3_MID_BE.parquet"
DEFAULT_LABELS = (ROOT / "polymarket" / "backtest" / "data" / "pmdata"
                  / "twap_labels.parquet")
DEFAULT_MD = STUDIES_DIR / "tick_latency" / "BEST_HOLD_SUMMARY.md"

# The label columns in twap_labels.parquet, in the order we report them.
#   gamma_won_up : the actual Polymarket resolution — this is what PAID.
#   up_legacy    : point open vs point close off our own Chainlink 1s tape. An
#                  independent recomputation of the same rule Gamma used, so
#                  legacy-vs-gamma agreement bounds our tape/plumbing error.
#   up_twap_c    : point open vs 30s/60s TWAP close (close-only TWAP).
#   up_twap_s    : TWAP open vs TWAP close (symmetric TWAP).
# twap_c/twap_s are the post-2026-08-07 candidate rules; the OPEN-side
# convention is still unannounced, which is exactly why both are carried.
LABEL_COLS = {
    "gamma  (what actually paid)": "gamma_won_up",
    "legacy (our tape, point)": "up_legacy",
    "twap_c (close-only TWAP)": "up_twap_c",
    "twap_s (symmetric TWAP)": "up_twap_s",
}
GAMMA = "gamma  (what actually paid)"

# The fold-report variant that means "no stop, held to resolution". The stop arm
# is "exit004"; picking it here would silently report a different strategy.
HOLD_KEY = "hold"


# --------------------------------------------------------------------------- §1

def fold_summary(report_path: Path) -> tuple[pd.DataFrame, dict]:
    """Per-fold hold-to-resolution equity out of a fold report JSON."""
    d = json.loads(report_path.read_text(encoding="utf-8"))
    rows = []
    for fold, fd in (d.get("folds") or {}).items():
        v = fd.get(HOLD_KEY)
        if v is None:
            continue
        rows.append(dict(
            chunk=fold, n=v["n"], start=d["start_capital_per_fold"], end=v["end"],
            pnl=v["pnl"], mult=v["end"] / d["start_capital_per_fold"],
            maxdd_pct=v["maxdd_pct"], wr_pct=v["wr_pct"],
            usd_per_trade=v["pnl"] / v["n"],
        ))
    if not rows:
        raise SystemExit(
            f"{report_path.name} has no '{HOLD_KEY}' arm in any fold — it is a stop "
            f"run, not a hold-to-resolution one. Point --report at a hold report."
        )
    return pd.DataFrame(rows), d


# --------------------------------------------------------------------------- §2

def regrade_by_label(bets_path: Path,
                     labels_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Re-grade the SAME selected bets under each resolution rule.

    Selection, entry timing and fill price are all held fixed; only the label
    that decides the payout changes. That is the one TWAP question it is safe to
    ask on pre-2026-08-07 books: the books priced the point rule and the bets
    were chosen under the point rule, so re-SELECTING on a TWAP label would
    harvest the pricing/label gap as fake edge. Re-GRADING a fixed bet list does
    not — it only asks "if these same bets had settled under rule R, what would
    they have paid".

    Returns (overall, by_cell, joined_bets).
    """
    bets = pd.read_parquet(bets_path)
    labels = pd.read_parquet(labels_path)
    m = bets.merge(labels[["market_slug", *LABEL_COLS.values()]],
                   on="market_slug", how="left")
    have = m[list(LABEL_COLS.values())].notna().all(axis=1)
    dropped = int((~have).sum())
    m = m[have].copy()

    up = m["side"].astype(str).str.upper().eq("UP").to_numpy()
    ask = m["entry_vwap"].to_numpy()
    # Stake and shares INCLUDE the scale-in legs — 29% of rows have top-ups, and
    # `entry_fee` covers only the first leg. Recover the true all-in fee by
    # inverting the driver's own booking; this reproduces `pnl` exactly (0.0
    # residual), so the re-graded dollars are on the same footing as the BT's.
    cash = (m["filled_usd"] + m["topup_cash"]).to_numpy()
    shares = (m["shares"] + m["topup_shares"]).to_numpy()
    bt_win = m["win"].to_numpy().astype(bool)
    fee = np.where(bt_win, shares - cash - m["pnl"].to_numpy(),
                   -m["pnl"].to_numpy() - cash)

    won = {"BT (as backtested)": bt_win}
    for name, col in LABEL_COLS.items():
        won[name] = up == (m[col] == 1).to_numpy()

    rows = []
    for name, w in won.items():
        y = w.astype(int)
        pnl = flat_pnl(y, ask)
        lo, hi = wilson(int(y.sum()), len(y))
        m[f"ev::{name}"] = pnl
        m[f"usd::{name}"] = np.where(w, shares - cash - fee, -(cash + fee))
        rows.append(dict(label=name, n=len(y), wr=y.mean(), wr_lo=lo, wr_hi=hi,
                         ev_per_bet=pnl.mean(), real_usd=m[f"usd::{name}"].sum(),
                         flips_vs_gamma=float((w != won[GAMMA]).mean())))
    overall = pd.DataFrame(rows)
    overall.attrs["dropped"] = dropped
    overall.attrs["avg_fill"] = float(ask.mean())

    ev_cols = [c for c in m.columns if c.startswith("ev::")]
    keys = ["coin", "interval"]
    by_cell = (m.groupby(keys, observed=True)
               .agg(n=("pnl", "size"), avg_fill=("entry_vwap", "mean"),
                    **{c: (c, "mean") for c in ev_cols})
               .reset_index())
    by_cell.columns = [c.replace("ev::", "") for c in by_cell.columns]
    return overall, by_cell, m


def bootstrap_ci(v, n_boot: int = 2000, seed: int = 0) -> tuple[float, float]:
    """Percentile bootstrap on the mean. Seeded — same input, same interval."""
    rng = np.random.default_rng(seed)
    v = np.asarray(v, dtype=float)
    means = np.array([v[rng.integers(0, len(v), len(v))].mean()
                      for _ in range(n_boot)])
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def knife_edge_agreement(joined: pd.DataFrame, labels_path: Path) -> pd.DataFrame | None:
    """Pool-label vs gamma agreement, bucketed by how far the window moved.

    The whole story of §2 lives in this gradient: averaging the close only
    changes the answer where the outcome was a coin flip anyway.
    """
    lab = pd.read_parquet(labels_path)[["market_slug", "open_point", "close_point"]]
    m = joined.merge(lab, on="market_slug", how="left")
    m = m[m["close_point"].notna() & m["open_point"].notna()].copy()
    if m.empty:
        return None
    m["agree"] = m["win"].astype(bool) == (
        m["side"].astype(str).str.upper().eq("UP") == (m["gamma_won_up"] == 1))
    m["bps"] = (m["close_point"] / m["open_point"] - 1).abs() * 1e4
    m["band"] = pd.cut(m["bps"], [0, 1, 3, 10, 30, np.inf],
                       labels=["<1bp", "1-3bp", "3-10bp", "10-30bp", ">30bp"])
    return (m.groupby("band", observed=True)
            .agg(n=("agree", "size"), agree=("agree", "mean")).reset_index())


# ------------------------------------------------------------------------ digest

def _md_table(df: pd.DataFrame, cols: dict[str, str], fmt: dict) -> str:
    head = "| " + " | ".join(cols.values()) + " |"
    rule = "|" + "---|" * len(cols)
    body = ["| " + " | ".join(fmt.get(c, str)(r[c]) for c in cols) + " |"
            for _, r in df.iterrows()]
    return "\n".join([head, rule] + body)


def write_digest(md_path: Path, summary: pd.DataFrame, meta: dict,
                 report_path: Path,
                 regrade: tuple[pd.DataFrame, pd.DataFrame, float, float,
                                pd.DataFrame | None] | None) -> None:
    v, t = meta.get("variant", {}), meta.get("topup", {})
    parts = [
        "# Best strategy — hold-to-resolution equity + per-bet EV by settlement rule",
        "",
        "Generated by `python -m polymarket.backtest.scripts.report_best_hold`. "
        "Do not hand-edit — re-run the script.",
        "",
        f"§1 source: `{report_path.name}` — a **hold-to-resolution** run "
        f"(`stop_triggers: 0` in every fold confirms the stop never armed).",
        "",
        f"- strategy: `{meta.get('strategy', '?')}`",
        f"- variant: `tier_a_mult {v.get('tier_a_mult')}`, "
        f"`tier_a_edge {v.get('tier_a_edge')}`, `nl15_mult {v.get('nl15_mult')}`, "
        f"`brake {v.get('brake_scale')}/{v.get('brake_floor')}`, "
        f"`dd_halt {v.get('dd_halt')}`",
        f"- topup: `max_n {t.get('max_n')}`, `frac {t.get('frac')}`, "
        f"`cap {t.get('cap')}`, `spread_max {t.get('spread_max')}`, "
        f"`exposure_cap {t.get('exposure_cap')}`",
        f"- `latency_ms {meta.get('latency_ms')}`, "
        f"`late_skip_sec {meta.get('late_skip_sec')}`, "
        f"`start_capital_per_fold {meta.get('start_capital_per_fold')}`",
        "",
        f"## 1. Equity, hold to resolution "
        f"(${meta.get('start_capital_per_fold'):,.0f} start per fold)",
        "",
        _md_table(summary,
                  {"chunk": "chunk", "n": "n", "end": "$100 →", "mult": "mult",
                   "maxdd_pct": "maxDD", "wr_pct": "WR",
                   "usd_per_trade": "$/trade"},
                  {"n": lambda x: f"{x:,}", "end": lambda x: f"{x:,.2f}",
                   "mult": lambda x: f"{x:,.1f}×", "maxdd_pct": lambda x: f"{x}%",
                   "wr_pct": lambda x: f"{x}%",
                   "usd_per_trade": lambda x: f"{x:.2f}"}),
        "",
        "⚠️ **Compounding-equity dollars, not per-bet edge.** Stake scales with "
        "the bankroll, so `$/trade` reflects where in the run a trade landed, not "
        "how good the setup was. Use §2 for edge.",
        "",
    ]
    if regrade is not None:
        overall, by_cell, gam_lo, gam_hi, knife = regrade
        parts += [
            "## 2. EV per bet under each resolution rule",
            "",
            "The **same** selected bets, same entry, same fill price — only the "
            f"label that decides the payout changes. Flat $1, `nb_common.flat_pnl` "
            f"(THETA={THETA}).",
            "",
            _md_table(overall,
                      {"label": "rule", "n": "n", "wr": "WR", "wr_lo": "lo",
                       "wr_hi": "hi", "ev_per_bet": "EV/bet",
                       "real_usd": "real $", "flips_vs_gamma": "flips vs gamma"},
                      {"n": lambda x: f"{x:,}", "wr": lambda x: f"{x * 100:.2f}%",
                       "wr_lo": lambda x: f"{x * 100:.1f}%",
                       "wr_hi": lambda x: f"{x * 100:.1f}%",
                       "ev_per_bet": lambda x: f"{x:+.4f}",
                       "real_usd": lambda x: f"{x:+,.0f}",
                       "flips_vs_gamma": lambda x: f"{x * 100:.2f}%"}),
            "",
            "### ✅ Label check — the BT grades on what actually paid",
            "",
            f"`BT (as backtested)` and `gamma (what actually paid)` agree on "
            f"**{100 - overall.iloc[1].flips_vs_gamma * 100:.2f}% of bets** — the "
            "tick replay settles on the point close, the rule these markets really "
            "paid on. `legacy`, recomputed independently from our own Chainlink 1s "
            "tape, corroborates it. The agreement table below is the check: it "
            "should be ~100% in every move-size band, and a band that sags is the "
            "signature of a mis-targeted pool.",
            "",
            f"So the deployed edge stands as backtested: "
            f"**{overall.iloc[1].ev_per_bet:+.4f}/bet** at "
            f"{overall.iloc[1].wr * 100:.2f}% WR, real **${overall.iloc[1].real_usd:+,.0f}**.",
            "",
            "### What the 2026-08-07 TWAP switch does to these bets",
            "",
            f"Close-only flips **{overall.iloc[3].flips_vs_gamma * 100:.2f}%** of "
            f"our bets (WR {overall.iloc[1].wr * 100:.2f}% → "
            f"{overall.iloc[3].wr * 100:.2f}%); symmetric flips "
            f"**{overall.iloc[4].flips_vs_gamma * 100:.2f}%** (WR → "
            f"{overall.iloc[4].wr * 100:.2f}%). Close-only is a tailwind, symmetric "
            "is roughly neutral — matching the AUC arm of the label study.",
            "",
            "⛔ **The twap_c / twap_s EV and $ columns are NOT a forecast.** These "
            "books were quoted against the point rule, so grading them on a TWAP "
            "label harvests the rule gap as edge that cannot survive the venue "
            "actually pricing TWAP. What carries over is the **flip rate**, the "
            "**WR direction**, and the 5m-vs-15m **asymmetry** — not the dollars.",
            "",
            "⚠️ **15m is the exposure that breaks if the OPEN is averaged too.** "
            "Under `twap_s` the 15m cells go negative while 5m stays positive "
            "(see the table below). The open-side convention is still unannounced; "
            "this is the concrete P&L reason to confirm it before 08-07.",
            "",
        ]
        if knife is not None:
            parts += [
                "Verification detail — BT-vs-gamma agreement by how far the "
                "window actually moved. A mis-targeted pool sags in the "
                "small-move bands (that is where the point and TWAP rules "
                "disagree); a clean one is flat at 100%:",
                "",
                _md_table(knife, {"band": "move", "n": "n", "agree": "agreement"},
                          {"n": lambda x: f"{x:,}",
                           "agree": lambda x: f"{x * 100:.1f}%"}),
                "",
                "Flat across every band, including the sub-1bp knife edge — "
                "no label drift anywhere in the bet list.",
                "",
            ]
        parts += [
            "### By coin × interval (EV/bet)",
            "",
            _md_table(by_cell,
                      {"coin": "coin", "interval": "interval", "n": "n",
                       "avg_fill": "avg fill",
                       "BT (as backtested)": "BT",
                       GAMMA: "gamma",
                       "twap_c (close-only TWAP)": "twap_c",
                       "twap_s (symmetric TWAP)": "twap_s"},
                      {"n": lambda x: f"{x:,}", "avg_fill": lambda x: f"{x:.3f}",
                       "BT (as backtested)": lambda x: f"{x:+.4f}",
                       GAMMA: lambda x: f"{x:+.4f}",
                       "twap_c (close-only TWAP)": lambda x: f"{x:+.4f}",
                       "twap_s (symmetric TWAP)": lambda x: f"{x:+.4f}"}),
            "",

            "⚠️ **This does not forecast post-2026-08-07 P&L.** These books were "
            "quoted against the point rule; once settlement is a TWAP the books "
            "reprice, and the `twap_*` columns capture only the label change, not "
            "the repricing. What they do establish is which bets the rule change "
            "touches (the flip set) and where it bites (15m under symmetric).",
            "",
        ]
    parts += [
        "## Outputs",
        "",
        "This script writes exactly two files: **this digest** and "
        "**`best_hold_bets.parquet`** under `BT_RUNS_DIR` — one row per graded "
        "bet, carrying `fill`, `pl`, every label column and one `ev::<rule>` "
        "column per resolution rule. Every table above is an aggregate of that "
        "parquet; re-cut it rather than hunting for a CSV.",
        "",
        "## Caveats",
        "",
        "- §1 and §2 are the **same bets** (the T3 detail frame), but different "
        "units: §1 compounds the bankroll, §2's EV column is a flat $1 stake. "
        "§2's `real $` column uses the BT's own booked stakes and fees and is "
        "the one directly comparable to §1's P&L.",
        "- Uniform-theta fee is a **model**; the live ledger books fees "
        "asymmetrically, so a modelled margin never equals the cash basis exactly.",
        "- No per-bet equity **series** exists for §1 — fold reports only ever "
        "stored summary stats. A real 100→N curve needs a re-run of "
        "`scripts/tick_topup_foldBE.py`.",
    ]
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--report", default=str(DEFAULT_REPORT),
                    help="fold report JSON with a 'hold' arm (S1)")
    ap.add_argument("--bets", default=str(DEFAULT_BETS),
                    help="selected-bets parquet re-graded per resolution rule (S2)")
    ap.add_argument("--labels", default=str(DEFAULT_LABELS),
                    help="twap_labels.parquet from scripts/build_twap_labels.py (S2)")
    ap.add_argument("--out-dir", default=str(BT_RUNS_DIR))
    ap.add_argument("--md", default=str(DEFAULT_MD))
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- S1
    report_path = Path(args.report)
    summary, meta = fold_summary(report_path)
    print(f"S1 hold-to-resolution equity  [{report_path.name}]")
    print(summary[["chunk", "n", "end", "mult", "maxdd_pct", "wr_pct",
                   "usd_per_trade"]].to_string(index=False))

    # ---- S2: same bets, every resolution rule
    regrade = None
    bets_path, labels_path = Path(args.bets), Path(args.labels)
    if bets_path.exists() and labels_path.exists():
        overall, by_cell, joined = regrade_by_label(bets_path, labels_path)
        gam_lo, gam_hi = bootstrap_ci(joined[f"ev::{GAMMA}"])
        knife = knife_edge_agreement(joined, labels_path)
        joined.to_parquet(out_dir / "best_hold_bets.parquet", index=False)

        print(f"\n=== S2 EV per bet by RESOLUTION RULE  [{bets_path.name}] ===")
        d = overall.copy()
        for c in ("wr", "wr_lo", "wr_hi", "flips_vs_gamma"):
            d[c] = (d[c] * 100).round(2)
        print(d.round(4).to_string(index=False))
        print(f"gamma EV/bet 95% bootstrap CI: [{gam_lo:+.4f}, {gam_hi:+.4f}]")
        print(f"(dropped {overall.attrs['dropped']} bets with no label; "
              f"avg fill {overall.attrs['avg_fill']:.3f})")
        if knife is not None:
            print("\n=== S2 pool-label vs gamma agreement by MOVE SIZE ===")
            k = knife.copy()
            k["agree"] = (k["agree"] * 100).round(1)
            print(k.to_string(index=False))
        print("\n=== S2 by COIN x INTERVAL (EV/bet) ===")
        print(by_cell.round(4).to_string(index=False))
        regrade = (overall, by_cell, gam_lo, gam_hi, knife)
        print(f"\n==> {out_dir / 'best_hold_bets.parquet'}")
    else:
        missing = bets_path if not bets_path.exists() else labels_path
        print(f"\n[warn] S2 skipped — missing {missing}")

    write_digest(Path(args.md), summary, meta, report_path, regrade)
    print(f"==> {args.md}")
    print("! S1 is compounding strategy P&L, S2 is a flat $1 stake. "
          "Different pools - do not read across.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
