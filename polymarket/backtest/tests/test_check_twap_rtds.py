from __future__ import annotations

import importlib.util
from pathlib import Path

from polymarket.backtest import REPO_ROOT

SCRIPT = REPO_ROOT / "polymarket" / "infra" / "check_twap_rtds.py"
SPEC = importlib.util.spec_from_file_location("check_twap_rtds", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_expected_series_allows_extra_public_streams() -> None:
    expected = {("BTC", 60)}
    accepted = expected | {("ETH", 60)}

    assert MODULE.has_expected_series(accepted, expected)


def test_expected_series_rejects_missing_window() -> None:
    expected = {("BTC", 60)}

    assert not MODULE.has_expected_series(set(), expected)
