from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from polymarket.backtest.lib.tick_store import open_store
from polymarket.backtest.scripts import tick_download_last30 as td
from polymarket.backtest.scripts.tick_download_last30 import _window_cutoff


def test_window_cutoff_includes_five_second_buffer_for_5m() -> None:
    assert _window_cutoff("btc-updown-5m-1000") == 1265


def test_window_cutoff_includes_five_second_buffer_for_15m() -> None:
    assert _window_cutoff("eth-updown-15m-1000") == 1865


@pytest.mark.parametrize("slug", ["bad", "btc-updown-1h-1000", "btc-updown-5m-nope"])
def test_window_cutoff_rejects_unrecognized_slugs(slug: str) -> None:
    with pytest.raises((ValueError, IndexError)):
        _window_cutoff(slug)


def _frame() -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp": pd.to_datetime([1_700_000_270, 1_700_000_271], unit="s", utc=True),
        "ask_prices": [[0.70, 0.71], [0.70]],
        "ask_sizes": [[12.0, 40.0], [5.0]],
        "bid_prices": [[0.68], [0.69, 0.68]],
        "bid_sizes": [[9.0], [3.0, 11.0]],
    })


def test_download_one_skips_slug_already_in_store(tmp_path: Path) -> None:
    with open_store(tmp_path / "ticks.db") as store:
        store.write_ticks("btc-updown-5m-1700000000", _frame())
        result = td._download_one(
            "btc-updown-5m-1700000000", api_key="x", store=store, retries=0
        )
    assert result == "skip"
