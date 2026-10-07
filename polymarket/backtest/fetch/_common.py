"""Shared helpers for the fetch scripts (``COINS``, ``parse_slug``)."""
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
    """Split a market slug ``'{coin}-updown-{interval}-{ts}'`` into (coin,
    interval, ts). ``ts`` is the window start.

    Deliberately simple and lenient: it can raise IndexError as well as
    ValueError, and ``fetch_gamma_resolutions.assemble_outcomes`` catches both.
    A stricter regex would drop slugs this accepts and change the label table.
    """
    parts = slug.split("-")
    return parts[0], parts[2], int(parts[3])
