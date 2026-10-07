"""Venue registry — the ONLY place core is allowed to reach a venue package.

The map is deliberately strings, not imports: core must not import
``cryptobot_polymarket`` / ``cryptobot_predictfun`` / ``cryptobot_kalshi`` at
module scope, or the "shared engine does not depend on any venue" property
(enforced by ``tools/importcheck.py``) is gone. ``get_adapter`` resolves the
string with ``importlib`` on first use and caches the result.

Two different strictness levels live here, and mixing them up has bitten this
repo before:

**BOOT-LEVEL: STRICT.** ``get_adapter`` raises on an unknown venue name and lets
an ImportError from a known-but-broken venue package propagate. Booting the live
scanner against a venue we cannot actually trade is worse than not booting —
before Phase 2 an unrecognized ``CRYPTOBOT_VENUE`` silently fell back to
Polymarket, i.e. a typo in the unit file would have traded REAL MONEY on the
wrong venue. ``config.py`` now refuses to import instead.

**ROW-LEVEL: PERMISSIVE.** Grade-time fee booking (``execution/grade_fees.py``,
``execution/held_position_pass.py``) dispatches on the LEDGER ROW's ``venue`` column, and
that column is ``NOT NULL DEFAULT 'polymarket'`` over years of history. An
unknown value there must grade as polymarket — matching the column default —
because a raise would strand rows PENDING and wedge the settlement sweep. See
``cryptobot.venues.fees``.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:  # pragma: no cover - typing only
    from cryptobot.venues.base import VenueAdapter

#: venue name -> distribution package that provides ``ADAPTER``.
VENUE_PACKAGES: Dict[str, str] = {
    "polymarket": "cryptobot_polymarket",
    "predictfun": "cryptobot_predictfun",
    "kalshi": "cryptobot_kalshi",
}

#: The ``CRYPTOBOT_VENUE`` allowlist. ``config.py`` validates against this, so
#: the registry map and the env allowlist can never drift apart.
KNOWN_VENUES: tuple[str, ...] = tuple(VENUE_PACKAGES)

_ADAPTER_CACHE: Dict[str, "VenueAdapter"] = {}


class UnknownVenueError(ValueError):
    """``CRYPTOBOT_VENUE`` (or a caller) named a venue with no adapter."""


def get_adapter(venue: str) -> "VenueAdapter":
    """Return the adapter for ``venue``, importing its package on first use.

    Raises ``UnknownVenueError`` for a name that is not in the registry, and
    lets ``ImportError`` / ``AttributeError`` from a registered-but-broken venue
    package propagate untouched — both are boot-stopping conditions on purpose
    (see the module docstring). ``NotImplementedError`` from a stub adapter
    (kalshi) likewise propagates.
    """
    key = (venue or "").strip().lower()
    cached = _ADAPTER_CACHE.get(key)
    if cached is not None:
        return cached
    module_name = VENUE_PACKAGES.get(key)
    if module_name is None:
        raise UnknownVenueError(
            f"unknown venue {venue!r}; known venues: "
            f"{', '.join(sorted(KNOWN_VENUES))}"
        )
    module = importlib.import_module(module_name)
    try:
        adapter = module.ADAPTER
    except AttributeError as exc:  # pragma: no cover - packaging error
        raise AttributeError(
            f"venue package {module_name!r} does not expose ADAPTER"
        ) from exc
    _ADAPTER_CACHE[key] = adapter
    return adapter


def active_adapter() -> "VenueAdapter":
    """The adapter for the venue this PROCESS is pinned to (``config.VENUE``).

    Imported lazily so ``cryptobot.venues`` never drags ``config`` in at module
    scope (``config`` itself imports this module for its allowlist).
    """
    from cryptobot import config

    return get_adapter(config.VENUE)


def _reset_cache_for_tests() -> None:
    """Drop the memoized adapters. Tests that monkeypatch a venue package use
    this; nothing on the live path calls it."""
    _ADAPTER_CACHE.clear()


__all__ = [
    "VENUE_PACKAGES",
    "KNOWN_VENUES",
    "UnknownVenueError",
    "get_adapter",
    "active_adapter",
]
