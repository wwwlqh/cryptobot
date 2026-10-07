"""The boundary between the trading engine and the exchange.

* :mod:`cryptobot.venues.base` - the ``VenueAdapter`` protocol.
* :mod:`cryptobot.venues.registry` - ``get_adapter(venue)``, a lazy import map.
* :mod:`cryptobot.venues.types` - shared data types (``BookSnapshot``,
  ``FeeUnknown``, the ``RESOLUTION_*`` statuses).
* :mod:`cryptobot.venues.fees` - the taker-fee curve.
* :mod:`cryptobot.venues.shadow_book` - WebSocket-vs-REST book comparison.

This package must not import ``config`` at module scope, because ``config``
imports it.
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
