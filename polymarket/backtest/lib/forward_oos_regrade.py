"""Forward-OOS test bed: regrade the frozen live gate on the live signal log
against TRUE pmdata.dev settlement, week by week.

This is the only un-overfittable measurement we have. The live bot logs `model_p
/ ask / edge` for EVERY scored cell into `signals` (cb_export_*.db). Each new
week is data the strategy was frozen *before* it saw. We re-grade the frozen
gate on that week and settle each qualifying bet with the authoritative
Polymarket CLOB outcome (`winning_outcome` from pmdata.dev `poly_l2`, cached in
oos_resolutions_cache.sqlite). Nothing here is selected on the data being graded.

WORKFLOW (repeat each week -- grade NON-OVERLAPPING slices so the weeks stay
independent; the live DB retains only SIGNALS_RETENTION_DAYS=7, so consecutive
full-DB pulls overlap ~6 days):
  1. Pull a fresh signal-log export from EC2 -> data/server_snapshot/cb_export_<date>.db
     (pull_live_db.sh deleted 2026-08-04 — pull manually per RECOVERY.md; keep each dated snapshot so you know the prior --until).
  2. Grade ONLY the new slice since the previous run's --until:
       python -m polymarket.backtest.lib.forward_oos_regrade --signal-db .../cb_export_<date>.db \
           --since '<previous --until>' --until '<this snapshot MAX(created_at)>'
     Omitting --since/--until grades the whole DB (overlaps prior windows).
  3. Read the per-week table. Results accumulate in
     data/server_snapshot/forward_oos_ledger.json, keyed by slice label, so the
     week-over-week series is there to read.

NO AUTOMATED VERDICT (2026-08-03, operator order). The harness prints the
numbers; the operator makes the call. It will NOT warn you that a week is too
thin to act on -- `EV/$1 0.407` off n=1 prints the same as off n=200.

Run as a module from the repo root:

    python -m polymarket.backtest.lib.forward_oos_regrade
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
from pathlib import Path

from polymarket.backtest import REPO_ROOT as ROOT
from polymarket.backtest.engine.sweep_lib import cash_pnl as bt_cash_pnl

DEFAULT_SIGNAL_DB = ROOT / "data" / "server_snapshot" / "cb_export_latest.db"
DEFAULT_RES_CACHE = ROOT / "data" / "server_snapshot" / "oos_resolutions_cache.sqlite"
DEFAULT_LEDGER = ROOT / "data" / "server_snapshot" / "forward_oos_ledger.json"

PMDATA_HOST = "https://api.pmdata.dev"
EX_TOP_N = 10           # fat-tail-robust EV excludes the N best per-$1 bets

# The frozen gate: thr (edge floor), ask window [lo, hi), entry-minute set,
# side filter ('any' | 'up' | 'down'). DO NOT tune it to chase a week's result.
# Re-freeze only when the LIVE config changes, under a NEW dated key so the
# series break is visible.
CONFIGS: dict[str, dict[str, dict]] = {
    # Transcribed 2026-08-03 from polymarket/live/config/v2_step1last_live.json.
    # GRADE FORWARD ONLY — re-grading past weeks is look-ahead, the gate was
    # copied with those outcomes already visible.
    # Caveat: `_passes` tests `lo <= ask < hi`, live is inclusive both ends, so
    # ask==0.85 grades here but not live (biases this arm slightly optimistic).
    "subminute_live_20260803": {
        "subminute_5m": dict(thr=0.03, lo=0.65, hi=0.85, ems=set(range(0, 5)), side="any"),
        "subminute_15m": dict(thr=0.03, lo=0.65, hi=0.85, ems=set(range(0, 15)), side="any"),
    },
}

SLEEVE_INTERVAL = {"subminute_5m": "5m", "subminute_15m": "15m"}

# Live `signals.sleeve` label -> CONFIG key. Identity on current labels; the
# exp044_* rows are back-compat for cb_export snapshots written before the
# 2026-07-31 rename. A label missing from this map ABORTS the run (see main) --
# that silent mismatch cost months of ungraded weeks once.
LIVE_SLEEVE_ALIASES: dict[str, str] = {
    "subminute_5m": "subminute_5m",
    "subminute_15m": "subminute_15m",
    "exp044_5m": "subminute_5m",
    "exp044_15m": "subminute_15m",
}


# --------------------------- settlement resolver ---------------------------
def _load_res_cache(path: Path) -> dict[str, int]:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE IF NOT EXISTS res (slug TEXT PRIMARY KEY, won_up INTEGER)")
    conn.commit()
    out = {r[0]: int(r[1]) for r in conn.execute("SELECT slug, won_up FROM res")}
    conn.close()
    return out


def _cache_put(path: Path, slug: str, won_up: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute("INSERT OR REPLACE INTO res (slug, won_up) VALUES (?, ?)", (slug, int(won_up)))
    conn.commit()
    conn.close()


def _api_key() -> str | None:
    key = os.environ.get("PMDATA_API_KEY", "").strip()
    if key:
        return key
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("PMDATA_API_KEY"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def _fetch_won_up(session, api_key: str, slug: str) -> int | None:
    """Resolve a market's Up outcome from pmdata.dev poly_l2 winning_outcome.

    pmdata.dev DEPRECATED the two-step /get-download-url/... endpoint (now HTTP
    410); the current API serves the parquet directly at
    /download/{data_type}/{slug}.parquet with an `api_key` request header. A
    missing market returns 404. Verified 2026-06-09.
    """
    import pandas as pd

    try:
        blob = session.get(
            f"{PMDATA_HOST}/download/poly_l2/{slug}.parquet",
            headers={"api_key": api_key}, timeout=60,
        )
        if blob.status_code != 200 or not blob.content[:4] == b"PAR1":
            return None
        df = pd.read_parquet(io.BytesIO(blob.content), columns=["winning_outcome"])
        wo = df["winning_outcome"].dropna()
        if wo.empty:
            return None
        val = wo.iloc[-1]
        if isinstance(val, str):
            return 1 if val.strip().lower() in {"up", "yes"} else 0
        return int(round(float(val)))
    except Exception:
        return None


class Settler:
    def __init__(self, res_cache: Path, allow_fetch: bool):
        self.path = res_cache
        self.cache = _load_res_cache(res_cache)
        self.allow_fetch = allow_fetch
        self._session = None
        self._key = None
        self.fetched = 0
        self.fetch_fail = 0
        if allow_fetch:
            self._key = _api_key()
            if self._key:
                import requests

                self._session = requests.Session()

    def won_up(self, slug: str) -> int | None:
        if slug in self.cache:
            return self.cache[slug]
        if self.allow_fetch and self._session and self._key:
            wu = _fetch_won_up(self._session, self._key, slug)
            if wu is not None:
                self.cache[slug] = wu
                _cache_put(self.path, slug, wu)
                self.fetched += 1
                return wu
            self.fetch_fail += 1
        return None


# ------------------------------- regrade -----------------------------------
def _slug(coin: str, interval: str, wst: int) -> str:
    return f"{coin.lower()}-updown-{interval}-{int(wst)}"


def _passes(gate: dict, *, em: int, ask: float, edge: float, side: str) -> bool:
    if em not in gate["ems"]:
        return False
    if not (gate["lo"] <= ask < gate["hi"]):
        return False
    if edge < gate["thr"]:
        return False
    if gate["side"] == "up" and side != "YES":
        return False
    if gate["side"] == "down" and side != "NO":
        return False
    return True


def _slice_clause(since: str | None, until: str | None) -> tuple[str, list]:
    sql, params = "", []
    if since is not None:
        sql += "   AND created_at >= ?\n"
        params.append(since)
    if until is not None:
        sql += "   AND created_at < ?\n"
        params.append(until)
    return sql, params


def sleeves_in_slice(db: Path, since: str | None = None,
                     until: str | None = None) -> dict[str, int]:
    """{raw signals.sleeve -> row count} in the slice, aliases NOT applied.

    Exists so an empty grade can say WHICH labels the log actually carries
    instead of shrugging. A label absent from ``LIVE_SLEEVE_ALIASES`` is the
    rename signature and must abort the run, not pass silently.
    """
    conn = sqlite3.connect(db)
    clause, params = _slice_clause(since, until)
    rows = conn.execute(
        "SELECT sleeve, COUNT(*) FROM signals\n WHERE 1=1\n" + clause
        + " GROUP BY sleeve", params).fetchall()
    conn.close()
    return {str(r[0]): int(r[1]) for r in rows}


def _load_signals(db: Path, since: str | None = None, until: str | None = None) -> list[dict]:
    """Load scored subminute cells, optionally restricted to a created_at slice
    so each weekly run grades a NON-OVERLAPPING window (the live DB retains only
    7 days, so consecutive full-DB pulls would overlap).

    ``sleeve`` comes back canonicalized through ``LIVE_SLEEVE_ALIASES``, with the
    label the box actually wrote preserved as ``sleeve_live``.
    """
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    names = sorted(LIVE_SLEEVE_ALIASES)
    clause, slice_params = _slice_clause(since, until)
    sql = (
        "SELECT coin, sleeve, entry_minute, side, window_start_ts, model_p, ask, edge, created_at\n"
        "  FROM signals\n"
        f" WHERE sleeve IN ({','.join('?' * len(names))})\n"
        "   AND ask IS NOT NULL AND model_p IS NOT NULL AND edge IS NOT NULL\n"
        + clause
    )
    rows = conn.execute(sql, [*names, *slice_params]).fetchall()
    conn.close()
    out = []
    for r in rows:
        rec = dict(r)
        rec["sleeve_live"] = rec["sleeve"]
        rec["sleeve"] = LIVE_SLEEVE_ALIASES[rec["sleeve"]]
        out.append(rec)
    return out


def _first_qualifying_bets(signals: list[dict], config: dict) -> list[dict]:
    """One bet per (coin, sleeve, window): the first entry-minute (ascending)
    whose row clears that sleeve's gate. Mirrors entry_strategy=first_qualifying.
    """
    by_window: dict[tuple, list[dict]] = {}
    for r in signals:
        if r["sleeve"] not in config:
            continue
        by_window.setdefault((r["coin"], r["sleeve"], r["window_start_ts"]), []).append(r)
    bets = []
    for (coin, sleeve, wst), cells in by_window.items():
        gate = config[sleeve]
        for r in sorted(cells, key=lambda x: x["entry_minute"]):
            if _passes(gate, em=int(r["entry_minute"]), ask=float(r["ask"]),
                       edge=float(r["edge"]), side=str(r["side"])):
                bets.append({
                    "coin": coin, "sleeve": sleeve,
                    "interval": SLEEVE_INTERVAL[sleeve], "wst": int(wst),
                    "em": int(r["entry_minute"]), "side": str(r["side"]),
                    "ask": float(r["ask"]), "edge": float(r["edge"]),
                })
                break
    bets.sort(key=lambda b: (b["wst"], b["em"]))
    return bets


def _metrics(graded: list[dict], stake_frac: float, capital: float) -> dict:
    """graded rows carry per-$1 return `r1` and `won`. Compound in time order."""
    n = len(graded)
    if n == 0:
        return dict(n=0, wr=None, ev=None, ev_ex_top10=None, median=None,
                    compound_final=capital, compound_pnl=0.0, compound_dd=0.0)
    r1 = [g["r1"] for g in graded]
    wins = sum(g["won"] for g in graded)
    ev = sum(r1) / n
    srt = sorted(r1)
    median = srt[n // 2] if n % 2 else (srt[n // 2 - 1] + srt[n // 2]) / 2
    keep = sorted(r1)[: max(0, n - EX_TOP_N)]
    ev_ex = (sum(keep) / len(keep)) if keep else 0.0
    # compound, no cap, no halt (matches the audit's headline sim)
    equity = capital
    peak = capital
    max_dd = 0.0
    for g in graded:  # already time-sorted
        equity += g["r1"] * stake_frac * equity
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return dict(n=n, wr=wins / n, ev=ev, ev_ex_top10=ev_ex, median=median,
                compound_final=equity, compound_pnl=equity - capital, compound_dd=max_dd)


def regrade_week(signals: list[dict], settler: Settler, *, stake_frac: float,
                 capital: float) -> dict:
    out: dict[str, dict] = {}
    for cfg_name, config in CONFIGS.items():
        bets = _first_qualifying_bets(signals, config)
        graded_by_iv: dict[str, list[dict]] = {"5m": [], "15m": []}
        unresolved = 0
        for b in bets:
            wu = settler.won_up(_slug(b["coin"], b["interval"], b["wst"]))
            if wu is None:
                unresolved += 1
                continue
            r1 = bt_cash_pnl(b["side"], b["ask"], int(wu), 1.0, slippage_haircut=0.0)
            won = int((b["side"] == "YES") == (wu == 1))
            graded_by_iv[b["interval"]].append({**b, "won_up": wu, "r1": r1, "won": won})
        rec = {"unresolved": unresolved}
        combined = []
        for iv in ("5m", "15m"):
            g = graded_by_iv[iv]
            combined += g
            rec[iv] = _metrics(g, stake_frac, capital)
        combined.sort(key=lambda x: (x["wst"], x["em"]))
        rec["combined"] = _metrics(combined, stake_frac, capital)
        out[cfg_name] = rec
    return out


# --------------------------- rolling ledger + rule -------------------------
def _week_label(db: Path) -> str:
    conn = sqlite3.connect(db)
    a, b = conn.execute(
        "SELECT MIN(substr(created_at,1,10)), MAX(substr(created_at,1,10)) FROM signals"
    ).fetchone()
    conn.close()
    return f"{a}_{b}"


def _load_ledger(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"weeks": {}}


def _history(ledger: dict, cfg_name: str) -> list[tuple[str, dict]]:
    """Week-over-week 5m series for a config, oldest first."""
    return [(w, ledger["weeks"][w][cfg_name]["5m"]) for w in sorted(ledger["weeks"])
            if cfg_name in ledger["weeks"][w] and ledger["weeks"][w][cfg_name]["5m"]["n"] > 0]


def _fmt(x, p=2):
    return "n/a" if x is None else f"{x:.{p}f}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal-db", default=str(DEFAULT_SIGNAL_DB))
    ap.add_argument("--res-cache", default=str(DEFAULT_RES_CACHE))
    ap.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    ap.add_argument("--week-label", default=None, help="default: derived from graded slice span")
    ap.add_argument("--since", default=None, help="created_at lower bound (UTC, inclusive), e.g. '2026-06-07 04:26:42' -- set to the PREVIOUS run's --until to keep weeks non-overlapping")
    ap.add_argument("--until", default=None, help="created_at upper bound (UTC, exclusive)")
    ap.add_argument("--stake-frac", type=float, default=0.02)
    ap.add_argument("--capital", type=float, default=100.0)
    ap.add_argument("--no-fetch", action="store_true", help="cache-only settlement (no pmdata network)")
    ap.add_argument("--force", action="store_true", help="overwrite an existing ledger entry for the same week label (default: refuse and exit)")
    args = ap.parse_args(argv)

    db = Path(args.signal_db)
    signals = _load_signals(db, args.since, args.until)
    if args.week_label:
        label = args.week_label
    elif signals:
        days = [s["created_at"][:10] for s in signals]
        label = f"{min(days)}_{max(days)}"
    elif args.since or args.until:
        since_part = (args.since or "?")[:10]
        until_part = (args.until or "?")[:10]
        label = f"{since_part}_{until_part}"
    else:
        label = _week_label(db)
    print(f"[regrade] signal-db={db.name}  week={label}  since={args.since}  until={args.until}")
    print(f"[regrade] loaded {len(signals):,} scored subminute cells")
    if not signals:
        # A rename must NOT look like a quiet week. Say which labels are there.
        present = sleeves_in_slice(db, args.since, args.until)
        unknown = {s: n for s, n in present.items() if s not in LIVE_SLEEVE_ALIASES}
        if unknown:
            print("[regrade] ERROR: the slice has rows this harness cannot map. "
                  "signals.sleeve was renamed and LIVE_SLEEVE_ALIASES is stale — "
                  "this is exactly the silent no-op fixed 2026-08-01.")
            for sleeve, count in sorted(unknown.items(), key=lambda kv: -kv[1]):
                print(f"[regrade]   unmapped sleeve {sleeve!r}: {count:,} rows")
            print(f"[regrade]   known: {sorted(LIVE_SLEEVE_ALIASES)}")
            return 2
        print("[regrade] WARNING: no signals in slice — nothing to grade; not writing to ledger.")
        return 0

    settler = Settler(Path(args.res_cache), allow_fetch=not args.no_fetch)
    week = regrade_week(signals, settler, stake_frac=args.stake_frac, capital=args.capital)
    if settler.fetched or settler.fetch_fail:
        print(f"[settle] fetched {settler.fetched} new resolutions ({settler.fetch_fail} unresolved)")

    # persist
    ledger = _load_ledger(Path(args.ledger))
    if label in ledger["weeks"] and not args.force:
        print(f"[regrade] ERROR: week label '{label}' already exists in ledger. "
              "Re-running with the same date range would overwrite a prior result "
              "and silently break the independence guarantee. "
              "Pass --force to overwrite intentionally (e.g. for a data-quality re-grade).")
        return 1
    ledger["weeks"][label] = week
    Path(args.ledger).write_text(json.dumps(ledger, indent=2, sort_keys=True), encoding="utf-8")

    # per-week table
    print("=" * 88)
    print(f"FORWARD-OOS WEEK {label}  (stake_frac={args.stake_frac}, $/1 EV = realized return per $1)")
    print("=" * 88)
    # 'combined' is 8 chars, so the iv column must be >=9 or that row shunts
    # every number one column right. ASCII only -- the console is cp1252.
    print(f"{'config':<26}{'iv':>9}{'n':>6}{'WR':>8}{'EV/$1':>9}"
          f"{'exTop10':>9}{'median':>8}{'cmpd$':>10}{'DD%':>7}")
    for cfg_name in CONFIGS:
        rec = week[cfg_name]
        print("-" * 88)
        for iv in ("5m", "15m", "combined"):
            m = rec[iv]
            if iv == "15m" and "subminute_15m" not in CONFIGS[cfg_name]:
                continue
            print(f"{(cfg_name if iv=='5m' else ''):<26}{iv:>9}{m['n']:>6}"
                  f"{_fmt(m['wr'],3):>8}{_fmt(m['ev'],3):>9}{_fmt(m['ev_ex_top10'],3):>9}"
                  f"{_fmt(m['median'],3):>8}{_fmt(m['compound_pnl'],1):>10}{_fmt(100*m['compound_dd'],1):>7}")
        if rec["unresolved"]:
            print(f"{'':<26}(unresolved markets this config: {rec['unresolved']})")
    print("=" * 88)
    print("5m WEEK-OVER-WEEK (no automated verdict - read the trend yourself)")
    for cfg_name in CONFIGS:
        series = _history(ledger, cfg_name)
        if not series:
            print(f"  {cfg_name:<26} no graded 5m weeks yet")
            continue
        print(f"  {cfg_name}")
        for wk, m in series:
            print(f"    {wk:<24}n={m['n']:<5} EV/$1 {_fmt(m['ev'],3):>7}"
                  f"   exTop10 {_fmt(m['ev_ex_top10'],3):>7}   WR {_fmt(m['wr'],3):>6}")
    print("=" * 88)
    print("Next: pull a fresh cb_export_<date>.db from EC2 next week and re-run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
