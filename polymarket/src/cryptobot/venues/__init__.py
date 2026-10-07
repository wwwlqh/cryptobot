"""``cryptobot.venues`` — the seam between the shared engine and a trading venue.

Phase 2 of the venue-tree restructure.
Before it, the engine dispatched with ``if config.VENUE == "predictfun"`` at
eight sites and imported the venue modules (then ``cryptobot.predictfun.*`` /
``cryptobot.data.polymarket_*``) directly from ``main.py`` / ``pipeline.py`` /
``live_readiness.py``. Now:

  * :mod:`cryptobot.venues.base` — the ``VenueAdapter`` protocol, one member per
    former dispatch site.
  * :mod:`cryptobot.venues.registry` — ``get_adapter(venue)``, a LAZY
    ``importlib`` map to the venue distributions. Core never names a venue
    package outside this map.
  * :mod:`cryptobot.venues.types` — the shared DTO contract (``BookSnapshot``,
    ``FeeUnknown``, the ``RESOLUTION_*`` vocabulary...). Both directions import
    these from core, so there is exactly one class object per type and
    ``isinstance`` still works across the seam.
  * :mod:`cryptobot.venues.fees` — the fee dispatcher, with its fail-closed
    decision path and its row-permissive grade-time path.
  * :mod:`cryptobot.venues.shadow_book` — a small venue-agnostic piece both
    venue packages had been sharing through the Polymarket modules. (Its
    sibling ``recorder_util`` went away 2026-08-04 with the book recorders, its
    only two consumers.)

Import-light on purpose: this package is imported by ``config.py`` (for the
``CRYPTOBOT_VENUE`` allowlist), so nothing here may import ``config`` at module
scope. Consumers import ``VenueAdapter`` / ``WsMirrorSetup`` from
:mod:`cryptobot.venues.base` and the DTOs from :mod:`cryptobot.venues.types`
directly.
"""

from __future__ import annotations

from cryptobot.venues.registry import (  # noqa: F401  re-export
    KNOWN_VENUES,
    VENUE_PACKAGES,
    UnknownVenueError,
    active_adapter,
    get_adapter,
)

__all__ = [
    "KNOWN_VENUES",
    "VENUE_PACKAGES",
    "UnknownVenueError",
    "active_adapter",
    "get_adapter",
]
