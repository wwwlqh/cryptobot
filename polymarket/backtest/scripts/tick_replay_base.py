#!/usr/bin/env python
r"""Shared scaffolding for the tick replay drivers.

WHAT THIS IS. The pieces both replay entry points need: the frozen
:class:`VariantConfig` / :class:`ReplayState`, WFO input loading, side-identity
attachment, and the per-fold counter report. It has no CLI and runs nothing on
its own — ``scripts/tick_topup_foldBE.py`` is the single replay driver.

WHAT IT WAS. ``tick_union_exit004_foldBE.py``, a second full replay driver
(``_process_attempt`` / ``run_replay`` / ``_digest`` / ``main``, 468 lines) whose
only difference from the top-up driver was having no scale-in ladder. That is
exactly what ``--topup-max-n 0`` already means, and the two were verified
identical on 2026-08-03: same code, same flags, ``--policies hold exit004``,
all 8 fold x arm cells matched to 1e-9 on both ``n`` and final equity.

⛔ THE NUMBERS BELOW ARE A DRIVER A/B, NOT STRATEGY RESULTS. They run
``tick_union``'s old defaults — UNION universe with the NL sleeve at full weight
(``nl15_mult 1.0``), ``tier_a_mult 1.0``, brake 0.5/0.5, NO top-up ladder and NO
late-window skip. Live is none of that — the T3 twin beats this config by
+55.1% / -7.3% / +120.0% / +79.9% on folds B/C/D/E. For strategy numbers use the T3 MID+BE twin
(``nl15_mult 0.0``, ``tier_a_mult 1.5``, ``--topup-max-n 3``, ``--late-skip 5``)
via ``studies/tick_latency/runners/run_midonly.py`` — fold B hold ends 1050.80
there vs 677.66 here.

  B hold  7582 / 677.6600     B exit004  7617 / 1529.9400
  C hold 10970 / 138851.1300  C exit004 10983 / 146506.3500
  D hold  6496 / 1194.7700    D exit004  6504 / 1514.7100
  E hold  6445 / 1634.3100    E exit004  6463 / 2160.9900

⭐ Side finding from that run: the audit-pinned ``foldBE_stop_report_400.json``
holds the SAME config on OLDER code (pre-restructure, before tick-BT fees
delegated to live ``grade_fees``) and ends higher in every fold: B 796.35 vs
677.66, C 145,660.73 vs 138,851.13, D 1,301.69 vs 1,194.77, E 1,820.01 vs
1,634.31 — so the fee correction cost the hold arm **-14.9 / -4.7 / -8.2 /
-10.2%** of fold equity, consistently negative. The pinned
report has not been regenerated since; ``audit/live_artifacts.py`` only checks
three fields, so it still passes.

To regenerate the audit-pinned ``studies/tick_latency/foldBE_stop_report_400.json``
(checked by ``audit/live_artifacts.py:127``), run the top-up driver with top-ups
off — this is the command that used to be this module's ``main``:

    .venv\Scripts\python.exe -m polymarket.backtest.scripts.tick_topup_foldBE \
        --latency 400 --policies hold exit004 --topup-max-n 0

WARNING: that replays the UNION universe. The live bot is MID-only (the NL band
was purged 2026-07-29) — for the live twin go through
``studies/tick_latency/runners/run_midonly.py``.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import heapq
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from polymarket.backtest import BT_RUNS_DIR, STUDIES_DIR
from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.lib.tick_store import TickStore

# TWO destinations on purpose (the restructure split tracked verdicts from
# heavy regenerable output):
#   RUN_DIR   -> studies/tick_latency/   TRACKED .json reports + the .md digests
#                (as of 2026-08-01 exactly ONE json is tracked there, the
#                audit-pinned foldBE_stop_report_400.json; the rest were dropped
#                2026-07-31 in a6df8ed and live in git history — see the
#                "Canonical run artifacts" section of README.md)
#   HEAVY_DIR -> BT_RUNS_DIR/tick_latency/  the gitignored detail .parquet
# studies/ carries no *.parquet ignore rule, so writing the detail frame there
# drops a heavy binary into a tracked tree — the same README section already
# says the heavy parquets belong under BT_RUNS_DIR.
RUN_DIR = STUDIES_DIR / "tick_latency"
HEAVY_DIR = BT_RUNS_DIR / "tick_latency"
OPTION3_DIR = (
    BT_RUNS_DIR / "option3_pool" / "pool_step1last_option3"
)
DETAIL_NAME = "foldBE_stop_{latency}{tag}.parquet"
REPORT_NAME = "foldBE_stop_report_{latency}{tag}.json"


@dataclass(frozen=True)
class VariantConfig:
    """Findings-driven strategy knobs; defaults reproduce the canonical replay."""

    tier_a_mult: float = 1.0
    tier_a_edge: float = 0.0465
    nl15_mult: float = 1.0
    use_k_weight: bool = True
    stop_chase: float = 0.0
    stop_attempts: int = te.STOP_MAX_ATTEMPTS
    brake_scale: float = te.BRAKE_SCALE
    brake_floor: float = te.BRAKE_FLOOR
    dd_halt: float = te.HARD_DD_HALT

    def as_dict(self) -> dict:
        return {
            "tier_a_mult": self.tier_a_mult,
            "tier_a_edge": self.tier_a_edge,
            "nl15_mult": self.nl15_mult,
            "use_k_weight": self.use_k_weight,
            "stop_chase": self.stop_chase,
            "stop_attempts": self.stop_attempts,
            "brake_scale": self.brake_scale,
            "brake_floor": self.brake_floor,
            "dd_halt": self.dd_halt,
        }

SCORE_COLUMNS = [
    "fold",
    "market_slug",
    "entry_ts",
    "interval",
    "coin",
    "ask",
    "offset_sec",
    "pred_edge",
    "win_true",
]


@dataclass
class ReplayState:
    policy: str
    fold: str
    start_capital: float
    cash: float = field(init=False)
    equity: float = field(init=False)
    peak: float = field(init=False)
    max_drawdown: float = 0.0
    filled_markets: set[str] = field(default_factory=set)
    fak_anchors: dict[tuple[str, str, str], float] = field(default_factory=dict)
    pending: list[tuple[float, int, float, float]] = field(default_factory=list)
    records: list[dict] = field(default_factory=list)
    counters: defaultdict[str, int] = field(default_factory=lambda: defaultdict(int))
    sequence: int = 0

    def __post_init__(self) -> None:
        self.cash = self.start_capital
        self.equity = self.start_capital
        self.peak = self.start_capital

    @property
    def drawdown(self) -> float:
        if self.peak <= 0:
            return 1.0
        return max(0.0, (self.peak - self.equity) / self.peak)

    def schedule(self, close_ts: float, cash_return: float, pnl: float) -> None:
        self.sequence += 1
        heapq.heappush(
            self.pending,
            (float(close_ts), self.sequence, float(cash_return), float(pnl)),
        )

    def drain(self, timestamp: float, *, inclusive: bool) -> None:
        while self.pending:
            close_ts = self.pending[0][0]
            if close_ts > timestamp or (not inclusive and close_ts == timestamp):
                break
            _, _, cash_return, pnl = heapq.heappop(self.pending)
            self.cash += cash_return
            self.equity += pnl
            self.peak = max(self.peak, self.equity)
            self.max_drawdown = max(self.max_drawdown, self.drawdown)


def _load_side_map(
    pool_dir: Path, market_slugs: set[str]
) -> pd.DataFrame:
    frames = []
    parts = sorted(pool_dir.glob("part_*.parquet"))
    if not parts:
        raise FileNotFoundError(f"no option3 pool parts under {pool_dir}")
    for part in parts:
        frame = pd.read_parquet(
            part, columns=["market_slug", "offset_sec", "ask", "other_ask"]
        )
        frame = frame[frame["market_slug"].isin(market_slugs)]
        if not frame.empty:
            frames.append(frame)
    if not frames:
        raise RuntimeError("option3 pool has no rows for the retained tick markets")
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["market_slug", "offset_sec"], keep="last")
        .rename(columns={"ask": "up_ask", "other_ask": "down_ask"})
    )


def attach_side_identity(
    scored: pd.DataFrame,
    *,
    option3_dir: Path,
) -> tuple[pd.DataFrame, dict]:
    """Attach UP/DOWN identity to legacy score caches that omitted side_up."""
    if "side_up" in scored.columns:
        out = scored.copy()
        out["side_up"] = out["side_up"].astype(bool)
        return out, {"source": "scored.side_up", "max_ask_error": 0.0}

    side_map = _load_side_map(option3_dir, set(scored["market_slug"].unique()))
    out = scored.merge(
        side_map,
        on=["market_slug", "offset_sec"],
        how="left",
        validate="m:1",
    )
    if out[["up_ask", "down_ask"]].isna().any(axis=None):
        missing = int(out["up_ask"].isna().sum())
        raise RuntimeError(f"missing option3 side identity for {missing:,} scored rows")

    up_error = (out["ask"] - out["up_ask"]).abs().to_numpy(dtype=float)
    down_error = (out["ask"] - out["down_ask"]).abs().to_numpy(dtype=float)
    tie = np.isclose(up_error, down_error, atol=1e-12)
    pair_order = (
        out.groupby(["market_slug", "entry_ts"], sort=False).cumcount().to_numpy()
    )
    out["side_up"] = (up_error < down_error) | (tie & (pair_order == 0))
    chosen_error = np.where(out["side_up"], up_error, down_error)
    max_error = float(np.nanmax(chosen_error))

    side_counts = out.groupby(["market_slug", "entry_ts"])["side_up"].nunique()
    if not bool((side_counts == 2).all()):
        bad = int((side_counts != 2).sum())
        raise RuntimeError(f"{bad:,} score pairs do not resolve to one UP and one DOWN")
    if max_error > 1e-6:
        raise RuntimeError(f"score/book side identity drift: max ask error={max_error:.6f}")

    return (
        out.drop(columns=["up_ask", "down_ask"]),
        {"source": "option3 ask pair", "max_ask_error": max_error},
    )


def load_wfo_inputs(
    score_path: Path,
    store: TickStore,
    option3_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, np.ndarray], dict]:
    tick_slugs = store.list_slugs()
    schema = set(pq.ParquetFile(score_path).schema_arrow.names)
    columns = SCORE_COLUMNS + (["side_up"] if "side_up" in schema else [])
    scored = pd.read_parquet(score_path, columns=columns)
    scored = scored[scored["market_slug"].isin(tick_slugs)].reset_index(drop=True)
    scored, side_meta = attach_side_identity(scored, option3_dir=option3_dir)

    pair_sizes = scored.groupby(["market_slug", "entry_ts"]).size()
    if not bool((pair_sizes == 2).all()):
        bad = int((pair_sizes != 2).sum())
        raise RuntimeError(f"WFO score artifact has {bad:,} incomplete side pairs")
    pair_sides = scored.groupby(["market_slug", "entry_ts"])["side_up"].nunique()
    if not bool((pair_sides == 2).all()):
        bad = int((pair_sides != 2).sum())
        raise RuntimeError(f"WFO score artifact has {bad:,} invalid side pairs")

    attempts = te.build_union_attempts(scored)
    candidate_slugs = set(attempts["market_slug"].unique())
    scored = (
        scored[scored["market_slug"].isin(candidate_slugs)]
        .sort_values(["market_slug", "entry_ts", "side_up"], kind="stable")
        .reset_index(drop=True)
    )
    groups = scored.groupby("market_slug", sort=False).indices
    coverage = {
        "tick_files": len(tick_slugs),
        "scored_rows_with_ticks": int(len(scored)),
        "candidate_markets": int(len(candidate_slugs)),
        "qualifying_seconds": int(len(attempts)),
        "side_identity": side_meta,
    }
    return scored, attempts, groups, coverage


def _minimum_edge(band: str) -> float:
    return 0.03 if band == "MID" else 0.01


def _held_scores(
    scored: pd.DataFrame,
    score_groups: dict[str, np.ndarray],
    slug: str,
    side: str,
) -> pd.DataFrame:
    group = scored.iloc[score_groups[slug]]
    want_up = side == "UP"
    return group[group["side_up"].astype(bool) == want_up]


def _floor_cents(value: float) -> float:
    return math.floor(max(0.0, value) * 100.0 + 1e-9) / 100.0



def _state_report(state: ReplayState) -> dict:
    counters = state.counters
    n = len(state.records)
    wins = sum(record["win"] for record in state.records)
    return {
        "n": n,
        "pnl": round(state.equity - state.start_capital, 2),
        "end": round(state.equity, 2),
        "maxdd_pct": round(state.max_drawdown * 100.0, 2),
        "wr_pct": round(100.0 * wins / n, 2) if n else None,
        "orders": counters["orders"],
        "fak_rejects": counters["fak_reject"],
        "partial_fills": counters["partial_fills"],
        "stop_triggers": counters["stop_triggers"],
        "stop_trigger_pct": (
            round(100.0 * counters["stop_triggers"] / n, 2) if n else None
        ),
        "stop_fills": counters["stop_fills"],
        "stop_attempts": counters["stop_attempts"],
        "stop_rejects": counters["stop_rejects"],
        "walker_skips": counters["walker_skip"],
        "dd_halt_skips": counters["dd_halt_skip"],
    }
