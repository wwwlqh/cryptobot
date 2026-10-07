"""``VenueAdapter`` — the protocol the shared engine talks to instead of ``if
config.VENUE == ...``.

The surface here is derived from the dispatch sites that existed before Phase 2,
one method per site — nothing speculative. The sites were:

===========================================  ==========================================
old dispatch site                            adapter member
===========================================  ==========================================
``main._build_deps_factory``                 ``fetch_active_market`` / ``fetch_top_book``
                                             / ``fetch_books_batch``
  (+ the PF-only ``bucket=``/``read_timeout=``
   book-fetch kwargs)                        ``book_fetch_kwargs()``
``main._build_sweep_job`` settlement source  ``fetch_resolved_outcome``
``main`` OrderClient vs PredictFunOrderClient ``build_order_client()``
``main`` WS mirror block                     ``build_ws_mirror()``
``main`` A8 prefetch batch fetcher           ``prefetch_fetch_kwargs()``
``pipeline._venue_market_cache_has``         ``market_cache_has``
``pipeline._venue_walk_fee_fn``              ``walk_fee_fn()``
``pipeline._venue_fee_per_share``            ``fee_per_share()``
``live_readiness.build_venue_balance_fetcher`` ``build_balance_fetcher()``
===========================================  ==========================================

Deliberately NOT on the adapter:

* **the default scan universe (``COINS``)** — and NOT for the reason this
  docstring used to give. There is no import cycle. The adapter packages are
  import-light by design and none of them imports ``config`` at module scope
  (verified: after importing all three adapter packages, ``cryptobot.config``
  is still absent from ``sys.modules``, and ``get_adapter`` resolves fine from
  a cold interpreter that never touched ``config``).

  The real constraint is that ``cryptobot.config`` must stay importable with
  ONLY ``polymarket/src`` on ``sys.path``. Three things depend on exactly that:

  - ``cryptobot/tests/test_venue_dispatch.py:157-165`` probes the
    venue-conditioned ``COINS`` default in a subprocess whose ``PYTHONPATH``
    is the package root and nothing else;
  - ``platform/checks/artifact_resolution.py:32`` probes ``from cryptobot
    import config`` on its own, in a subprocess with a deliberately stripped
    environment — anything ``config`` drags in becomes a hard dependency of the
    platform console's read-only check.

  And ``cryptobot_kalshi`` is deliberately NOT staged onto the box
  (``polymarket/infra/deploy.sh:270-273``). So routing ``COINS`` through
  ``get_adapter`` would convert today's "fails loudly at the first adapter
  USE" into "``config`` cannot be imported AT ALL" — which kills both of
  those read-only tools on a box that is otherwise perfectly healthy.

  ``config.py`` therefore stays monolithic and keeps the whole env contract,
  which is the Phase-2 rule anyway.
* **the wallet-topology gate** (``_derive_topology``/``_validate_wallet_config``)
  — it is not venue-dispatched today: the Polymarket-shaped gate runs on *every*
  venue. Making it dispatch would change live readiness behavior on the
  predict.fun box, which Phase 2 must not do.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class WsMirrorSetup:
    """What ``build_ws_mirror`` hands back to ``main``.

    Exactly one of ``mirror`` / ``shadow`` is set on success, and BOTH are
    ``None`` when the venue could not bring a mirror up (predict.fun swallows
    construction failures and continues REST-only — that fail-soft belongs to
    the venue, so the adapter owns the try/except and ``main`` just consumes the
    result).

    Whichever object is returned has already been ``start()``ed, matching the
    pre-split ordering: predict.fun's start() sat *inside* its guard, so a start
    failure had to degrade to REST-only too.

    ``replaces_rest_prefetch`` is predict.fun's "WS is authoritative, so do not
    also run the A8 REST prefetch daemon" signal (it warms books off-tick over
    the push channel and would otherwise spend the shared 240/min key).
    """

    mirror: Optional[Any] = None
    shadow: Optional[Any] = None
    replaces_rest_prefetch: bool = False


@runtime_checkable
class VenueAdapter(Protocol):
    """One trading venue's implementation of the seam.

    Adapters are plain objects; the registry imports the venue distribution and
    reads its module-level ``ADAPTER``. Nothing here is abstract-base-class
    machinery — the Protocol exists to document and type the surface.
    """

    #: Registry key; matches ``CRYPTOBOT_VENUE``.
    name: str

    # -- market discovery + order books -------------------------------------

    def fetch_active_market(self, *args, **kwargs): ...

    def fetch_top_book(self, *args, **kwargs): ...

    def fetch_books_batch(self, *args, **kwargs): ...

    def fetch_resolved_outcome(self, *args, **kwargs): ...

    def market_cache_has(
        self, coin: str, interval_min: int, window_start_ts: int
    ) -> bool: ...

    def book_fetch_kwargs(self) -> dict:
        """Extra kwargs bound into the DECISION-path book fetchers.

        Polymarket returns ``{}`` (its fetchers do not accept them, so binding
        anything would be a TypeError — this is why the pre-split code guarded
        the kwargs behind the venue branch). predict.fun returns its own
        split-rate ``bucket`` plus an optional ``read_timeout``.
        """

    def prefetch_fetch_kwargs(self) -> dict:
        """Extra kwargs bound into the A8 prefetch daemon's batch fetcher."""

    # -- execution ----------------------------------------------------------

    def build_order_client(self, *, runtime_cfg) -> Optional[object]:
        """Construct the venue's LIVE order client, or ``None`` for DRY-only.

        Fail-soft on BOTH venues: a missing SDK or empty key logs and returns
        ``None``, which the pipeline reads as ``clob_unavailable`` (LIVE cells
        SKIP, DRY cells continue). The adapter owns the log lines so they stay
        byte-identical per venue.
        """

    def build_balance_fetcher(self, config) -> Callable[[], Optional[float]]:
        """On-chain collateral balance reader for the live-readiness gate."""

    # -- websocket book mirror ----------------------------------------------

    def build_ws_mirror(self, *, order_client) -> WsMirrorSetup:
        """Construct + start the venue's WS book mirror (or its shadow)."""

    # -- fees ---------------------------------------------------------------

    def fee_per_share(
        self,
        ask: float,
        *,
        fee_rate_bps: Optional[float] = None,
        theta: Optional[float] = None,
    ) -> float: ...

    def walk_fee_fn(
        self, *, fee_rate_bps: Optional[float] = None
    ) -> Optional[Callable[[float], float]]:
        """Per-share fee fn for ``walk_book_edge_preserving``, or ``None`` to
        keep the walker's built-in parabola (the Polymarket answer)."""


__all__ = ["VenueAdapter", "WsMirrorSetup"]
