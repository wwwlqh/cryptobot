"""SQLite-backed store for the last-30s per-tick book substrate.

Replaces the 61k-file ``pmdata/poly_tick_last30/<slug>.parquet`` flat directory
with ONE file, ``pmdata/poly_tick_last30.db``. Each slug's frame is kept as
parquet *bytes* in a BLOB, so the list-valued columns (``ask_prices``,
``ask_sizes``, ``bid_prices``, ``bid_sizes``) round-trip exactly and a reader
still pays one columnar decode per market — no row-shredding, no schema drift.

Why one DB: 61k small files on a OneDrive-synced NTFS volume cost more in
directory metadata than in payload; enumeration (``glob``) was the slow part of
every WFO load. ``list_slugs()`` is a single index scan.

Writers: ``TickStore`` serializes every write through one connection guarded by
a lock, so the downloader's ``ThreadPoolExecutor`` can call ``write_ticks``
from any worker. Readers are also serialized on that lock, which is fine —
replay workloads are single-threaded and the blob decode happens outside it.

RETIRED 2026-08-01: the one-off migration CLI (``main`` / ``_verify`` /
``migrate_parquet_dir`` / ``TickStore.write_blob`` / ``DEFAULT_PARQUET_DIR``,
folded in here from a 114-line script the day before). It walked
``pmdata/poly_tick_last30/<slug>.parquet`` into this DB — and that flat tree was
DELETED 2026-07-31 once ``--verify`` signed the DB off, with no rebuild path, so
every entry point into it could only ever fail on a missing directory. This
module is now the store and nothing else. Recover the migrator with
``git show <this commit>^:polymarket/backtest/lib/tick_store.py`` if a future
consolidation ever needs the same walk.
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

    def list_slugs(self) -> set[str]:
        with self._lock:
            rows = self._conn.execute("SELECT slug FROM ticks").fetchall()
        return {row[0] for row in rows}

    def slug_rows(self) -> dict[str, int]:
        """``{slug: n_rows}`` — the cheap census, one index scan, no blob reads.

        Its migration-skip caller went 2026-08-01; the live one is
        ``lib/nb_common``'s substrate summary.
        """
        with self._lock:
            rows = self._conn.execute("SELECT slug, n_rows FROM ticks").fetchall()
        return {slug: int(n) for slug, n in rows}

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
