"""``VenueAdapter``: the interface the trading engine uses to reach an exchange.

The engine never imports the exchange package directly; it asks
``cryptobot.venues.get_adapter()`` for an object with these methods.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class WsMirrorSetup:
    """What ``build_ws_mirror`` returns to ``main``.

    ``mirror`` is set when the WebSocket book mirror is authoritative;
    ``shadow`` is set when it only runs alongside REST for comparison.
    Whichever is returned has already been started.
    """

    mirror: Optional[Any] = None
    shadow: Optional[Any] = None


@runtime_checkable
class VenueAdapter(Protocol):
    """One exchange's implementation of the engine-facing interface."""

    #: Registry key.
    name: str

    # -- market discovery and order books -----------------------------------

    def fetch_active_market(self, *args, **kwargs): ...

    def fetch_top_book(self, *args, **kwargs): ...

    def fetch_books_batch(self, *args, **kwargs): ...

    def fetch_resolved_outcome(self, *args, **kwargs): ...

    def market_cache_has(
        self, coin: str, interval_min: int, window_start_ts: int
    ) -> bool: ...

    # -- execution ----------------------------------------------------------

    def build_order_client(self, *, runtime_cfg) -> Optional[object]:
        """The live order client, or ``None`` to run DRY-only."""

    def build_balance_fetcher(self, config) -> Callable[[], Optional[float]]:
        """On-chain collateral balance reader for the live-readiness gate."""

    def derive_wallet_topology(self, config):
        """Signer and deposit wallets derived from the configured key."""

    # -- websocket book mirror ----------------------------------------------

    def build_ws_mirror(self, *, order_client) -> WsMirrorSetup:
        """Construct and start the WebSocket book mirror (or its shadow)."""


__all__ = ["VenueAdapter", "WsMirrorSetup"]
