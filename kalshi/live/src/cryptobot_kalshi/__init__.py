"""``cryptobot_kalshi`` — registered venue, NOT yet implemented.

This package exists so the venue registry map is TOTAL: ``CRYPTOBOT_VENUE`` is
validated against ``cryptobot.venues.registry.KNOWN_VENUES``, and having kalshi
in there with a compiling adapter means whoever implements it has a target that
already type-checks, imports and is wired into ``tools/importcheck.py``'s
contracts.

Every adapter member raises ``NotImplementedError`` naming ``kalshi/README.md``.
That is the deliberate boot-level behavior: pinning ``CRYPTOBOT_VENUE=kalshi``
fails LOUDLY at startup rather than quietly trading somewhere else. Before
Phase 2, an unrecognized venue silently fell back to Polymarket — i.e. a typo in
a unit file would have traded real money on the wrong venue.

Row-level fee grading is a separate, PERMISSIVE path: a historic ledger row
whose ``venue`` column says ``kalshi`` still grades under the Polymarket
parabola (the column's ``NOT NULL DEFAULT``), because grade-time must keep
settling. See ``cryptobot.venues.fees.row_fee_per_share``.

To implement: give this package the real modules, then replace ``ADAPTER``
below. Nothing in core needs to change — that is the point of the seam.
"""

from __future__ import annotations

from typing import Callable, Optional

from cryptobot.venues.base import WsMirrorSetup

__all__ = ["ADAPTER", "KalshiAdapter"]

_POINTER = (
    "the kalshi venue adapter is not implemented — see kalshi/README.md. "
    "Unset CRYPTOBOT_VENUE (or set it to 'polymarket'/'predictfun') to boot."
)


def _unimplemented(what: str) -> "NotImplementedError":
    return NotImplementedError(f"{what}: {_POINTER}")


class KalshiAdapter:
    """Stub ``VenueAdapter``. Every member fails loud."""

    name = "kalshi"

    # -- market discovery + order books -------------------------------------

    def fetch_active_market(self, *args, **kwargs):
        raise _unimplemented("fetch_active_market")

    def fetch_top_book(self, *args, **kwargs):
        raise _unimplemented("fetch_top_book")

    def fetch_books_batch(self, *args, **kwargs):
        raise _unimplemented("fetch_books_batch")

    def fetch_resolved_outcome(self, *args, **kwargs):
        raise _unimplemented("fetch_resolved_outcome")

    def market_cache_has(
        self, coin: str, interval_min: int, window_start_ts: int
    ) -> bool:
        raise _unimplemented("market_cache_has")

    def book_fetch_kwargs(self) -> dict:
        raise _unimplemented("book_fetch_kwargs")

    def prefetch_fetch_kwargs(self) -> dict:
        raise _unimplemented("prefetch_fetch_kwargs")

    # -- execution ----------------------------------------------------------

    def build_order_client(self, *, runtime_cfg) -> Optional[object]:
        raise _unimplemented("build_order_client")

    def build_balance_fetcher(self, config) -> Callable[[], Optional[float]]:
        raise _unimplemented("build_balance_fetcher")

    def derive_wallet_topology(self, config):
        raise _unimplemented("derive_wallet_topology")

    # -- websocket book mirror ----------------------------------------------

    def build_ws_mirror(self, *, order_client) -> WsMirrorSetup:
        raise _unimplemented("build_ws_mirror")

    # -- fees ---------------------------------------------------------------

    def fee_per_share(
        self,
        ask: float,
        *,
        fee_rate_bps: Optional[float] = None,
        theta: Optional[float] = None,
    ) -> float:
        raise _unimplemented("fee_per_share")

    def walk_fee_fn(self, *, fee_rate_bps: Optional[float] = None):
        raise _unimplemented("walk_fee_fn")


ADAPTER = KalshiAdapter()
