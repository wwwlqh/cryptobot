"""Test helper: load the shared multi-wallet portfolio topology fixture.

`config.V6_CONFIG_PATH` points at the LIVE artifact, which today runs a
single shared-equity group of two sleeves. That shape cannot exercise the
multi-wallet machinery the codebase still carries — per-wallet exposure
caps, per-wallet backtest max-DDs, two INDEPENDENT wallet_groups with two
members each, per-wallet default modes — so ~19 test modules load a vendored
4-wallet / 2-group artifact from `tests/fixtures/legacy_recommended.json`
instead.

2026-07-31 (exp044 phase 2): the fixture was REBUILT. It used to be a
snapshot of the deployed-then-dropped `dry_run_recommended.json` (blend+rf
sleeves plus two pair sleeves), which existed to keep the loader's
`lut|rf|blend` + pairs back-compat covered. Those code paths are gone, so
the fixture now carries four `signal_source='hgb'` sleeves while keeping the
same four wallet ids, groups, caps, DD fracs, allocs, capital_fracs, edge/fill
bands and slippage haircuts, so every topology and sizing test keeps its
numbers unchanged. The ids `pairs_5m` / `pairs_15m` are historical labels
only: they are plain single-ledger wallets, NOT pair sleeves.

The sleeves carry no `checkpoint_offset_set`, so they take the tick's MINUTE
entry path — the one the pipeline-mechanic tests were written against. That
path cannot reach a real model since `score_cell_hgb` went checkpoint-only in
phase 1, so those modules install `tests/_checkpoint_stubs.fake_minute_scorer`
(the retired `score_cell`'s arithmetic, to the digit). Real scoring is covered
against the real checkpoint bundle by `test_hgb_scoring` / `test_subminute`.

The drift-check constants are monkeypatched to the fixture's cap/halt so
`load_sleeves()` succeeds regardless of the live config.py defaults.

This fixture is NOT a deployable config.
"""

from __future__ import annotations

import unittest.mock as _mock
from pathlib import Path

from cryptobot import config
from cryptobot.strategy.portfolio_loader import V6Portfolio, load_sleeves

_LEGACY_ARTIFACT = (
    Path(__file__).resolve().parent / "fixtures" / "legacy_recommended.json"
)


def legacy_portfolio() -> V6Portfolio:
    """Load the 4-wallet / 2-group fixture with the boot gates patched."""
    with _mock.patch.object(config, "SHARED_EXPOSURE_CAP", 0.28767857660247903), \
         _mock.patch.object(config, "MAX_DRAWDOWN_HALT_FRAC", 0.5), \
         _mock.patch.object(config, "SUBMINUTE_ENABLED", True):
        return load_sleeves(_LEGACY_ARTIFACT)
