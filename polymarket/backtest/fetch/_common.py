"""Shared plumbing for the polymarket/backtest/fetch/*.py downloader scripts.

``RateLimiter`` / ``connect_wal_db`` / ``configure_logging`` were retired
2026-08-01 (phase 6) together with their only two callers, ``fetch_pmdata.py``
and ``build_pmdata_features.py``. ``COINS`` and ``parse_slug`` moved here
VERBATIM out of ``fetch_pmdata.py`` in the same pass, because
``fetch_gamma_resolutions`` is their sole surviving consumer.
"""
from __future__ import annotations

import os
from pathlib import Path

COINS = ("btc", "eth", "sol", "xrp")


def load_dotenv_into_environ(path: Path) -> None:
    """Parse a simple KEY=VALUE .env file into os.environ (setdefault only)."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.split("#", 1)[0].strip().strip('"').strip("'")
        os.environ.setdefault(k.strip(), v)


def parse_slug(slug: str) -> tuple[str, str, int]:
    """Inverse of the market slug: '{coin}-updown-{interval}-{resolve_ts}' -> (coin, interval, resolve_ts).

    Index-based ON PURPOSE, and deliberately lenient: it can raise IndexError as
    well as ValueError, and ``fetch_gamma_resolutions.assemble_outcomes`` catches
    BOTH. Tightening it to a strict regex (as the retired
    ``lib/recorder_store.parse_slug`` was) would silently drop slugs this parser
    accepts and change the label table's row set.
    """
    parts = slug.split("-")
    return parts[0], parts[2], int(parts[3])
