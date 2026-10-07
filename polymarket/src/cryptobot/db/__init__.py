"""SQLite persistence layer for the live scanner.

`connection.py` exposes `get_connection()` (raw WAL-mode connection),
`init_db()` (apply schema + idempotent migrations), and
`backfill_wallet_groups()` (portfolio-dependent one-shot UPDATE that runs
after `load_sleeves()` per D-LIVE-15).
"""

from cryptobot.db.connection import (
    backfill_wallet_groups,
    get_connection,
    init_db,
    utc_now_sql,
)

__all__ = [
    "backfill_wallet_groups",
    "get_connection",
    "init_db",
    "utc_now_sql",
]
