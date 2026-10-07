"""W6 — THE PARITY GATE (hard gate before any live flip).

Full fold-E fixture parity between the src v2 decision core (``spec_v2``) and the
frozen research evaluator (``polymarket/backtest/lib/pool_study``).  This is the
bet-for-bet proof that the promoted live decision path == the researched spec.

Three assertions on fold E of ``backtest/optimize_runs/scored_11set_truth.parquet``
(spec: ask_lo=0.65, both intervals):

  1. **Frame-level decision parity** — ``build_bets_src`` yields EXACTLY n=2503,
     flat-$1 EV +0.04285, and an identical ``(market_slug, entry_ts)`` set to
     research ``pool_study.build_bets``.  Proves the src pure decision code ==
     research build_bets, bet-for-bet.

  2. **Live state-machine parity (realizability ceiling)** — replaying fold E
     second-by-second through ``spec_v2.WindowDecider`` (the class the pipeline
     consults) with a market-level consume (the ledger's ONE_PER_WINDOW /
     market_dedup) and a reachable-fill constraint yields n=2366, EV +0.04067,
     a strict subset of the 2503 (137-market reachability-peek shortfall).

  3. **Gate-first regression tripwire (informational)** — gate-before-dedup
     ordering yields n=5223 / +0.0358.  Present so a refactor that accidentally
     reverts to gate-first trips a loud, named failure.

The fixture is a 92 MB parquet read directly from its repo path (never copied).
On a machine without it (CI without the research data) the whole module SKIPS
rather than fails, per the W6 spec.
"""

import os

import numpy as np
import pandas as pd
import pytest

from cryptobot.strategy.spec_v2 import build_bets_src, fee, WindowDecider

# ---------------------------------------------------------------------------
# Fixture / research-lib wiring
# ---------------------------------------------------------------------------
# BT_RUNS_DIR is the legacy-first shim (polymarket/backtest/__init__.py), so the
# fixture resolves to whichever optimize_runs/ tree physically exists.
from polymarket.backtest import BT_RUNS_DIR

_FIXTURE = str(BT_RUNS_DIR / "scored_11set_truth.parquet")

pytestmark = pytest.mark.skipif(
    not os.path.exists(_FIXTURE),
    reason=(
        f"fold-E parity fixture missing ({_FIXTURE}); "
        "skip on machines without the research data"
    ),
)

# The v2 live spec overrides on top of BET_SPEC_DEFAULT.
_SPEC = {"ask_lo": 0.65, "intervals": ("5m", "15m")}

# Canonical numbers (verified this session against pool_study on fold E).
# 2026-08-05 theta correction (POLY_FEE_THETA 0.072 -> 0.07, operator: the
# published value is 0.07): every EV pin below rose by ~+0.00038 = the fee
# delta 0.002 * mean[p*(1-p)] over the same bets. The COUNT pins are unchanged
# (2503 / 2366 / 5223 all still hold), i.e. the correction did not reshuffle
# selection on fold E at these thresholds — only the booked fee term moved.
_N_CANONICAL = 2503
_EV_CANONICAL = 0.04322          # research grade() EV, +/- 1e-4
_N_LIVE = 2366
_EV_LIVE = 0.04105              # WindowDecider realizability ceiling, +/- 1e-4
_SHORTFALL = 137               # 2503 - 2366 reachability-peek gap
_N_GATEFIRST = 5223
_EV_GATEFIRST = 0.03620


def _flat_ev(df: pd.DataFrame) -> float:
    """Flat $1-stake EV after fee at the +1s fill price (== pool_study.grade)."""
    p = df["side_fill_ask"].to_numpy(float)
    w = df["win_true"].to_numpy(float)
    pnl = w * (1.0 / p - 1.0) - (1.0 - w) - fee(p)
    return float(pnl.mean())


# ---------------------------------------------------------------------------
# Session fixtures (load the 92 MB parquet ONCE)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def fold_e() -> pd.DataFrame:
    """Fold-E rows with derived ``side`` / ``window_start`` for the replay.

    ``side`` is reconstructed positionally: ``side_frame`` emits every up-side
    row before every down-side row, and ``score_wfo`` preserves that order
    within each fold, so the first half of fold E is the up side and the second
    half the down side.  A cheap {up,down}-pairing sanity check guards the
    assumption (fails loudly if the parquet's row order ever changes).
    """
    df = pd.read_parquet(_FIXTURE)
    e = df[df["fold"] == "E"].reset_index(drop=True).copy()
    assert len(e) % 2 == 0, "fold E must have an even row count (up+down per cell)"
    half = len(e) // 2
    e["side"] = np.where(e.index < half, "up", "down")
    e["side_rank"] = np.where(e["side"] == "up", 0, 1)  # up wins same-second ties
    e["window_start"] = e["entry_ts"] - e["offset_sec"]
    # each (market_slug, offset_sec) must be exactly one up + one down row.
    pair = e.groupby(["market_slug", "offset_sec"])["side_rank"].agg(["size", "sum"])
    assert (pair["size"] == 2).all() and (pair["sum"] == 1).all(), (
        "positional side reconstruction broke: (market_slug, offset_sec) pairs "
        "are not clean {up, down}"
    )
    return e


@pytest.fixture(scope="module")
def research_bets(fold_e: pd.DataFrame) -> pd.DataFrame:
    from polymarket.backtest.lib.pool_study import build_bets as research_build_bets

    return research_build_bets(fold_e, _SPEC)


# ---------------------------------------------------------------------------
# Assertion 1 — frame-level decision parity (the task's stated gate)
# ---------------------------------------------------------------------------
def test_build_bets_src_frame_parity(fold_e, research_bets):
    src = build_bets_src(fold_e, _SPEC)

    # exact bet count
    assert len(src) == _N_CANONICAL, (
        f"build_bets_src fold-E count = {len(src)}, expected {_N_CANONICAL}"
    )

    # flat-$1 EV within 1e-4
    ev = _flat_ev(src)
    assert abs(ev - _EV_CANONICAL) <= 1e-4, (
        f"build_bets_src fold-E EV = {ev:.6f}, expected {_EV_CANONICAL:+.5f}"
    )

    # identical (market_slug, entry_ts) set to research build_bets, bet-for-bet
    src_set = set(zip(src["market_slug"], src["entry_ts"]))
    ref_set = set(zip(research_bets["market_slug"], research_bets["entry_ts"]))
    assert src_set == ref_set, (
        "build_bets_src selected a different (market_slug, entry_ts) set than "
        f"research build_bets (src-only={len(src_set - ref_set)}, "
        f"research-only={len(ref_set - src_set)})"
    )
    assert len(research_bets) == _N_CANONICAL  # research anchor sanity


# ---------------------------------------------------------------------------
# Assertion 2 — live state-machine parity (realizability ceiling)
# ---------------------------------------------------------------------------
def _replay_window_decider(e: pd.DataFrame) -> pd.DataFrame:
    """Drive the real ``WindowDecider`` second-by-second, exactly as the pipeline
    does: per-second commit + the ledger's ONE_PER_WINDOW / market_dedup (one
    bet per resolution, consumed at the first in-band second of EITHER side) +
    a reachable-fill constraint (an unreachable committed second yields no fill).
    """
    q = e.sort_values(["offset_sec", "side_rank", "market_slug"]).reset_index(
        drop=True
    )
    dec = WindowDecider()
    consumed: set = set()          # markets whose window is spent (ledger dedup)
    filled_idx: list = []
    recs = q.to_dict("records")
    for i, c in enumerate(recs):
        mkey = (c["coin"], c["interval"], c["window_start"])
        if mkey in consumed:
            continue               # opposite side blocked by market_dedup
        result = dec.on_second(c)
        if result in ("PLACE", "WINDOW_DONE"):
            consumed.add(mkey)     # window consumed on first in-band second
            if result == "PLACE" and c["reachable"]:
                filled_idx.append(i)   # order fills only if the second is reachable
    return q.loc[filled_idx]


def test_window_decider_live_parity(fold_e, research_bets):
    placed = _replay_window_decider(fold_e)

    assert len(placed) == _N_LIVE, (
        f"WindowDecider replay count = {len(placed)}, expected {_N_LIVE}"
    )

    ev = _flat_ev(placed)
    assert abs(ev - _EV_LIVE) <= 1e-4, (
        f"WindowDecider replay EV = {ev:.6f}, expected {_EV_LIVE:+.5f}"
    )

    # strict subset of the canonical 2503, short by exactly 137 markets.
    live_markets = set(placed["market_slug"])
    canon_markets = set(research_bets["market_slug"])
    assert live_markets.issubset(canon_markets), (
        "WindowDecider bet a market outside the canonical 2503 "
        f"(outside={len(live_markets - canon_markets)}) — reachability leak"
    )
    assert len(canon_markets) - len(live_markets) == _SHORTFALL, (
        f"reachability shortfall = {len(canon_markets) - len(live_markets)}, "
        f"expected {_SHORTFALL}"
    )


# ---------------------------------------------------------------------------
# Assertion 3 — gate-first regression tripwire (informational, non-canonical)
# ---------------------------------------------------------------------------
def _build_bets_gate_first(e: pd.DataFrame) -> pd.DataFrame:
    """Deliberately WRONG ordering: gate BEFORE dedup. Kept only to pin the
    number a gate-first regression would produce (n=5223), so such a regression
    fails here with a named message instead of silently doubling the book.
    """
    d = e[e["interval"].isin(_SPEC["intervals"])
          & e["coin"].isin(("btc", "eth", "sol", "xrp"))]
    d = d[(d["ask"] > 0.65) & (d["ask"] <= 0.85)]
    off = d["offset_sec"] - np.where(d["interval"] == "5m", 270, 870)
    d = d[(off >= 0) & (off <= 29)]
    d = d[d["reachable"]]
    d = d[d["pred_edge"] >= 0.03]                      # GATE FIRST (wrong)
    return d.sort_values(["market_slug", "entry_ts"]).groupby(
        "market_slug", as_index=False
    ).first()


def test_gate_first_ordering_tripwire(fold_e):
    gf = _build_bets_gate_first(fold_e)
    assert len(gf) == _N_GATEFIRST, (
        f"gate-first count = {len(gf)}, expected {_N_GATEFIRST} "
        "(tripwire pin, not the live ordering)"
    )
    ev = _flat_ev(gf)
    assert abs(ev - _EV_GATEFIRST) <= 1e-4, (
        f"gate-first EV = {ev:.6f}, expected {_EV_GATEFIRST:+.5f}"
    )
    # and it must be STRICTLY worse / bigger than the canonical dedup-first book.
    assert len(gf) > _N_CANONICAL, "gate-first must inflate the book vs dedup-first"
