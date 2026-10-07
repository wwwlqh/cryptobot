"""Fast, pure-logic tests for the fixed-set gate/sizing sweep + wallet simulator.

Here we lock the fold-calendar invariants and the selection/sizing math that are
easy to get subtly wrong. The stage B/C/E tests (robust-second selection and the
honest-diagnostics math) were deleted 2026-08-01 with the functions they covered;
the train-window embargo assertions moved to test_checkpoint.py, which owns the
surviving copy of ``_train_windows``.
"""
from __future__ import annotations

import pandas as pd
import pytest
from types import SimpleNamespace

from polymarket.backtest.training import offset_validation as lk


def test_chunk_model_mapping():
    # A is in-sample on the train-A model; B is OOS on the same; C/D progressively OOS.
    assert lk.CHUNK_MODEL_KEY == {"A": "A", "B": "A", "C": "AB", "D": "ABC"}
    # D is scored ONLY by the latest (A+B+C) model -> never by a model that saw D.
    assert lk.CHUNK_MODEL_KEY["D"] == "ABC"


def test_gate_cfg_shape():
    cfg = lk._gate_cfg((0, 10), 0.08, breakout_bps=0.5, min_entry_z=0.25)
    assert cfg["offsets"] == (0, 10)
    assert cfg["pred_edge_min"] == 0.08
    assert cfg["breakout_bps"] == 0.5
    assert cfg["min_entry_z"] == 0.25
    assert cfg["min_range_bps"] == 0.0


# --------------------------------------------------------------------------- #
# Resolution dedupe + baseline-style sizing.
# --------------------------------------------------------------------------- #
def test_resolution_dedupe_keeps_one_candidate_per_market_resolution():
    df = pd.DataFrame({
        "market_slug": ["m1", "m1", "m1", "m2"],
        "entry_ts": [200, 100, 100, 50],
        "pred_edge": [0.99, 0.10, 0.20, 0.01],
        "book_depth": [100, 100, 50, 10],
        "ask": [0.30, 0.40, 0.35, 0.50],
    })

    out, stats = lk._dedup_resolution_with_stats(df)

    assert stats["candidate_resolution_rows"] == 4
    assert stats["resolution_unique_keys"] == 2
    assert stats["resolution_duplicate_groups"] == 1
    assert stats["resolution_duplicate_drops"] == 2
    assert out["market_slug"].tolist() == ["m1", "m2"]
    # Earliest entry_ts wins first; among exact ties, higher pred_edge wins.
    assert out.loc[out["market_slug"] == "m1", "pred_edge"].iloc[0] == pytest.approx(0.20)


def test_resolution_dedupe_prefers_condition_id_over_slug():
    df = pd.DataFrame({
        "market_slug": ["slug-a", "slug-b", "slug-c"],
        "condition_id": ["cond-1", "cond-1", ""],
        "entry_ts": [100, 100, 100],
        "pred_edge": [0.10, 0.30, 0.20],
        "book_depth": [10, 10, 10],
        "ask": [0.50, 0.50, 0.50],
    })

    out, _stats = lk._dedup_resolution_with_stats(df)

    assert len(out) == 2
    assert set(out["resolution_key"]) == {"cond-1", "slug-c"}
    assert out.loc[out["resolution_key"] == "cond-1", "market_slug"].iloc[0] == "slug-b"


def test_baseline_target_notional_applies_coin_mult_and_max_bet_cap():
    sleeve = SimpleNamespace(stake_frac=0.10, max_bet_usd=20.0)
    row = SimpleNamespace(coin="btc", interval="5m")
    cfg = {
        "sizing_mode": "baseline_portfolio",
        "stake_mult": 4.0,
        "max_bet_mult": 2.25,
        "coin_mult": {"btc": 0.95},
    }

    assert lk._target_notional(100.0, sleeve, row, cfg) == pytest.approx(38.0)
    assert lk._target_notional(1000.0, sleeve, row, cfg) == pytest.approx(42.75)


def test_sized_notional_floors_sub_dollar_targets_to_min_bet():
    sleeve = SimpleNamespace(stake_frac=0.01, max_bet_usd=None)
    row = SimpleNamespace(coin="btc", interval="5m")
    cfg = {
        "sizing_mode": "baseline_portfolio",
        "stake_mult": 0.9,
        "max_bet_mult": 1.0,
        "coin_mult": {"btc": 0.95},
        "min_bet_usd": 1.0,
    }

    assert lk._target_notional(100.0, sleeve, row, cfg) == pytest.approx(0.855)
    assert lk._sized_notional(100.0, sleeve, row, cfg) == pytest.approx(1.0)
    assert lk._sized_notional(0.5, sleeve, row, cfg) is None


def test_book_consumption_multiplier_scales_sleeve_cap():
    sleeve = SimpleNamespace(book_consumption_max=0.25)
    assert lk._book_consumption_max_for(sleeve, {"book_consumption_mult": 2.0}) == pytest.approx(0.50)


def test_baseline_search_profile_uses_live_gate_only():
    assert tuple(lk._gate_grid("baseline")) == ((lk.LIVE_PRED_EDGE_MIN, 0.0, 0.0),)
    assert len(tuple(lk._gate_grid("expanded"))) > 1


def test_full_acceptance_requires_D_positive_and_dd_within_cap():
    sim = {
        "fold_pnl": {"A": 1.0, "B": 1.0, "C": 1.0, "D": -0.01},
        "fold_n": {"A": 20, "B": 20, "C": 20, "D": 20},
        "max_dd_frac": 0.20,
        "worst_30bet_dd_frac": 0.20,
        "dd_halted": False,
    }

    assert lk._train_pass(sim, 0.25, 20) is True
    assert lk._full_acceptance_pass(sim, 0.25, 20) is False

    sim["fold_pnl"]["D"] = 1.0
    assert lk._full_acceptance_pass(sim, 0.25, 20) is True

    sim["max_dd_frac"] = 0.251
    assert lk._full_acceptance_pass(sim, 0.25, 20) is False
