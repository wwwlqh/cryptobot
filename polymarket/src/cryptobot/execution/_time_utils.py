"""SQLite-text-format timestamp helpers.

Three modules previously kept identical private copies of `_parse_iso`
and `_seconds_since` (live_action_guard, operator_control,
polymarket_transfer). When the SQLite text format changes, all three
needed synchronised edits — operational risk for the LIVE-money TTL
calculations that depend on them.

This is the single canonical home. `parse_iso` returns a UTC-aware
datetime or None; `seconds_since` returns elapsed seconds from `now()`
or None when the input is unparseable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


_SQLITE_TS_FMT = "%Y-%m-%d %H:%M:%S"


def parse_iso(ts: str) -> Optional[datetime]:
    """Parse the SQLite `datetime('now')` text format into a UTC-aware
    `datetime`. Returns None for empty, non-string, or unparseable input."""
    if not ts:
        return None
    if not isinstance(ts, str):
        return None
    try:
        return datetime.strptime(ts, _SQLITE_TS_FMT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def seconds_since(ts: str) -> Optional[float]:
    """Seconds between `parse_iso(ts)` and now() in UTC. Returns None if
    `ts` cannot be parsed. Clamped to >= 0 so a future-dated timestamp
    (clock skew) reads as 0 elapsed rather than negative."""
    parsed = parse_iso(ts)
    if parsed is None:
        return None
    now = datetime.now(timezone.utc)
    return max(0.0, (now - parsed).total_seconds())


__all__ = ["parse_iso", "seconds_since"]
