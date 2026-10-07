"""The SQLite tick store must round-trip list-valued book columns exactly."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pytest

from polymarket.backtest.engine import tick_exit004 as te
from polymarket.backtest.lib import tick_store
from polymarket.backtest.lib.tick_store import open_store


def _frame(offset: int = 0) -> pd.DataFrame:
    return pd.DataFrame({
        # ms resolution: parquet stores timestamps in ms, so an s-resolution
        # fixture would fail round-trip equality for a reason the store owns none of.
        "timestamp": pd.to_datetime(
            [1_700_000_270 + offset, 1_700_000_271 + offset], unit="s", utc=True
        ).as_unit("ms"),
        "ask_prices": [[0.70, 0.71], [0.70]],
        "ask_sizes": [[12.0, 40.0], [5.0]],
        "bid_prices": [[0.68], [0.69, 0.68]],
        "bid_sizes": [[9.0], [3.0, 11.0]],
    })


def test_round_trip_preserves_list_columns(tmp_path: Path) -> None:
    with open_store(tmp_path / "t.db") as store:
        store.write_ticks("btc-updown-5m-1", _frame())
        out = store.read_ticks("btc-updown-5m-1")
    pd.testing.assert_frame_equal(_frame(), out)


def test_concurrent_writers_are_serialized(tmp_path: Path) -> None:
    with open_store(tmp_path / "t.db") as store:
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: store.write_ticks(f"s{i}", _frame(i)), range(64)))
        assert store.count() == 64


def test_the_module_is_a_store_and_not_a_migrator(tmp_path: Path) -> None:
    """The one-off parquet-dir migration CLI went 2026-08-01 with its source
    tree (pmdata/poly_tick_last30/, deleted 2026-07-31, no rebuild path). It
    could only ever fail on a missing directory, so it is not a CLI anyone can
    run — assert it stays gone rather than rotting back in."""
    for gone in ("main", "_verify", "migrate_parquet_dir", "DEFAULT_PARQUET_DIR"):
        assert not hasattr(tick_store, gone), f"{gone} came back"
    with open_store(tmp_path / "t.db") as store:
        assert not hasattr(store, "write_blob")


def test_load_tick_slug_builds_a_tickbook(tmp_path: Path) -> None:
    with open_store(tmp_path / "t.db") as store:
        store.write_ticks("btc-updown-5m-1", _frame())
        book = te.load_tick_slug("btc-updown-5m-1", store)
    assert book.seconds.shape == (2,)
    assert book.ask_levels(0, "UP")[0] == (0.70, 12.0)
