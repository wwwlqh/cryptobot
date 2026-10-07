"""Venue registry: the only place the engine reaches the exchange package.

Venue packages are named as strings and imported lazily, so importing the
engine never pulls in HTTP clients or the CLOB SDK.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:  # pragma: no cover - typing only
    from cryptobot.venues.base import VenueAdapter

#: venue name -> package that provides ``ADAPTER``.
VENUE_PACKAGES: Dict[str, str] = {
    "polymarket": "cryptobot_polymarket",
}

KNOWN_VENUES: tuple[str, ...] = tuple(VENUE_PACKAGES)

_ADAPTER_CACHE: Dict[str, "VenueAdapter"] = {}


class UnknownVenueError(ValueError):
    """A caller named a venue with no adapter."""


def get_adapter(venue: str = "polymarket") -> "VenueAdapter":
    """Return the adapter for ``venue``, importing its package on first use."""
    key = (venue or "").strip().lower()
    cached = _ADAPTER_CACHE.get(key)
    if cached is not None:
        return cached
    module_name = VENUE_PACKAGES.get(key)
    if module_name is None:
        raise UnknownVenueError(
            f"unknown venue {venue!r}; known venues: {', '.join(KNOWN_VENUES)}"
        )
    adapter = importlib.import_module(module_name).ADAPTER
    _ADAPTER_CACHE[key] = adapter
    return adapter


def active_adapter() -> "VenueAdapter":
    """The adapter for the venue this process trades (``config.VENUE``)."""
    from cryptobot import config

    return get_adapter(config.VENUE)


__all__ = [
    "VENUE_PACKAGES",
    "KNOWN_VENUES",
    "UnknownVenueError",
    "get_adapter",
    "active_adapter",
]
