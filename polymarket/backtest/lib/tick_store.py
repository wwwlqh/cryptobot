"""SQLite store for the last-30-second per-tick books.

One file, ``pmdata/poly_tick_last30.db``, holds each market's frame as parquet
bytes in a BLOB, so the list columns (``ask_prices``, ``ask_sizes``,
``bid_prices``, ``bid_sizes``) round-trip exactly and a reader decodes one
market at a time. A single file avoids the cost of listing tens of thousands of
small files on a synced volume; ``list_slugs()`` is one index scan.

``TickStore`` sends every write through one lock-guarded connection, so the
downloader's thread pool can call ``write_ticks`` from any worker. Reads take
the same lock; decoding happens outside it.
"""
from __future__ import annotations

import io
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from polymarket.backtest import BT_DATA_DIR

DEFAULT_TICK_DB = BT_DATA_DIR / "pmdata" / "poly_tick_last30.db"
SCHEMA_VERSION = 1
_COMPRESSION = "zstd"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    slug   TEXT PRIMARY KEY,
    n_rows INTEGER NOT NULL,
    data   BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def frame_to_blob(frame: pd.DataFrame) -> bytes:
    """Serialize one slug's tick frame to parquet bytes."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression=_COMPRESSION)
    return buffer.getvalue()


def blob_to_frame(blob: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(blob))


class TickStore:
    """Single-file tick substrate. Thread-safe; one connection, one lock."""

    def __init__(self, path: Path | str = DEFAULT_TICK_DB, *, readonly: bool = False):
        self.path = Path(path)
        self._lock = threading.Lock()
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        if not readonly:
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)",
                ("created_utc", datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    # ------------------------------------------------------------- lifecycle
    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "TickStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ----------------------------------------------------------------- reads
    def has_slug(self, slug: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM ticks WHERE slug = ?", (slug,)
            ).fetchone()
        return row is not None


    def count(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0])

    def read_ticks(self, slug: str) -> pd.DataFrame:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM ticks WHERE slug = ?", (slug,)
            ).fetchone()
        if row is None:
            raise KeyError(f"slug not in tick store: {slug}")
        return blob_to_frame(row[0])

    # ---------------------------------------------------------------- writes
    def write_ticks(self, slug: str, frame: pd.DataFrame, *, commit: bool = True) -> None:
        blob = frame_to_blob(frame)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO ticks(slug, n_rows, data) VALUES(?, ?, ?)",
                (slug, int(len(frame)), blob),
            )
            if commit:
                self._conn.commit()

    def commit(self) -> None:
        with self._lock:
            self._conn.commit()


def open_store(path: Path | str = DEFAULT_TICK_DB, *, readonly: bool = False) -> TickStore:
    return TickStore(path, readonly=readonly)
