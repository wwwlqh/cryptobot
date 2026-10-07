"""``cryptobot_polymarket`` — the Polymarket venue adapter package.

Extracted out of ``cryptobot.*`` in Phase 2 of the venue-tree restructure. The
modules themselves are unchanged code at new addresses:

===============================================  ==========================
old (inside core ``cryptobot``)                  new (here)
===============================================  ==========================
``data/polymarket_clob.py``                      ``clob.py``
``data/polymarket_gamma.py``                     ``gamma.py``
``data/clob_ws_mirror.py``                       ``ws_mirror.py``
``polymarket/primitives.py``                     ``primitives.py``
``execution/walker.py``                          ``walker.py``
``execution/polymarket_relayer.py``              ``relayer.py``
``execution/polymarket_transfer.py``             ``transfer.py``
``cli/derive_poly_creds.py``                     ``derive_creds.py``
``cli/poly_wallet_diagnostics.py``               ``wallet_diagnostics.py``
``reconciliation/backfill_polymarket_settlement.py`` ``backfill_settlement.py``
===============================================  ==========================

This module is deliberately IMPORT-LIGHT: it defines ``ADAPTER`` and nothing
else at module scope. Every adapter method imports its implementation lazily, so
merely resolving the registry (e.g. to price a historic ledger row's fee) never
drags in httpx clients, the CLOB SDK or a websocket thread.
"""

from __future__ import annotations

from typing import Callable, Optional

from cryptobot.venues.base import WsMirrorSetup

__all__ = ["ADAPTER", "PolymarketAdapter"]


class PolymarketAdapter:
    """``VenueAdapter`` for Polymarket. See ``cryptobot.venues.base``."""

    name = "polymarket"

    # -- market discovery + order books -------------------------------------

    def fetch_active_market(self, *args, **kwargs):
        from cryptobot_polymarket.gamma import fetch_active_market

        return fetch_active_market(*args, **kwargs)

    def fetch_top_book(self, *args, **kwargs):
        from cryptobot_polymarket.clob import fetch_top_book

        return fetch_top_book(*args, **kwargs)

    def fetch_books_batch(self, *args, **kwargs):
        from cryptobot_polymarket.clob import fetch_books_batch

        return fetch_books_batch(*args, **kwargs)

    def fetch_resolved_outcome(self, *args, **kwargs):
        from cryptobot_polymarket.gamma import fetch_resolved_outcome

        return fetch_resolved_outcome(*args, **kwargs)

    def market_cache_has(
        self, coin: str, interval_min: int, window_start_ts: int
    ) -> bool:
        from cryptobot_polymarket.gamma import market_cache_has

        return market_cache_has(coin, interval_min, window_start_ts)

    def book_fetch_kwargs(self) -> dict:
        # Polymarket's fetchers take neither `bucket` nor `read_timeout`;
        # binding either would be a TypeError. Empty dict == the pre-Phase-2
        # `else` branch in main._build_deps_factory.
        return {}

    def prefetch_fetch_kwargs(self) -> dict:
        return {}

    # -- execution ----------------------------------------------------------

    def build_order_client(self, *, runtime_cfg) -> Optional[object]:
        """OrderClient, or None for DRY-only. Log lines are byte-identical to
        the pre-split ``main.py`` branch."""
        import logging

        from cryptobot_polymarket.walker import ClobClientUnavailable, OrderClient

        logger = logging.getLogger("cryptobot.main")
        if not runtime_cfg.poly_private_key.get_secret_value():
            logger.info(
                "POLY_PRIVATE_KEY empty — booting DRY-only (LIVE wallets SKIP)"
            )
            return None
        try:
            client = OrderClient(runtime_cfg)
        except ClobClientUnavailable:
            logger.warning(
                "py_clob_client_v2 not installed — running DRY-only. "
                "LIVE wallets will SKIP with clob_unavailable."
            )
            return None
        except Exception:  # noqa: BLE001
            logger.exception("OrderClient init raised — booting DRY-only")
            return None
        logger.info(
            "OrderClient initialized (signature_type=%d funder=%s)",
            runtime_cfg.poly_signature_type,
            runtime_cfg.poly_funder or "(auto)",
        )
        return client

    def build_balance_fetcher(self, config) -> Callable[[], Optional[float]]:
        """Polygon pUSD balance of the configured ``POLY_FUNDER``."""
        from cryptobot_polymarket.balance import build_pusd_balance_fetcher

        return build_pusd_balance_fetcher(config)

    def derive_wallet_topology(self, config):
        """Derive signer/deposit wallets from ``POLY_PRIVATE_KEY``.

        Reached from core's live-readiness gate, which runs this
        Polymarket-shaped check on EVERY venue — that is pre-Phase-2 behavior,
        preserved deliberately (core asks the registry for *this* adapter by
        name rather than for the active venue's). Whether the gate should be
        venue-dispatched is an operator decision, not a mechanical move.
        """
        from cryptobot_polymarket.primitives import derive_wallets

        def _secret_value(value: object) -> str:
            getter = getattr(value, "get_secret_value", None)
            if callable(getter):
                return str(getter() or "")
            return str(value or "")

        private_key = _secret_value(getattr(config, "poly_private_key", ""))
        if not private_key:
            return None
        return derive_wallets(
            private_key,
            relayer_url=getattr(config, "relayer_url", ""),
            rpc_url=getattr(config, "polygon_rpc_url", None) or None,
        )

    # -- websocket book mirror ----------------------------------------------

    def build_ws_mirror(self, *, order_client) -> WsMirrorSetup:
        """Construct + start the Polymarket CLOB book mirror.

        NOTE the asymmetry with predict.fun, preserved from the pre-split
        ``main.py``: Polymarket's construction is NOT exception-guarded, so a
        raise here still propagates and stops the boot exactly as before.
        """
        from cryptobot import config
        from cryptobot.venues.shadow_book import ShadowBookComparator
        from cryptobot_polymarket.ws_mirror import ClobBookWsMirror

        ws_mirror = ClobBookWsMirror(
            max_age_ms=config.BOOK_WS_MAX_AGE_MS,
            url=config.BOOK_WS_URL,
            # 2026-07-25 orphan fix: evict a token's ladder the moment it
            # leaves the subscribed set, so a rolled-off window can't price a
            # live FAK. max_book_age_ms is the (default-off) safety net.
            evict_unsubscribed=config.BOOK_WS_EVICT_UNSUBSCRIBED,
            max_book_age_ms=config.BOOK_WS_MAX_BOOK_AGE_MS,
        )
        if config.BOOK_WS_SHADOW:
            # W3: run the mirror ALONGSIDE REST (never authoritative) so the
            # operator can watch WS/REST divergence for a day before flipping
            # the mirror into the book source chain.
            shadow = ShadowBookComparator(ws_mirror)
            shadow.start()
            return WsMirrorSetup(shadow=shadow)
        ws_mirror.start()
        return WsMirrorSetup(mirror=ws_mirror)

    # -- fees ---------------------------------------------------------------

    def fee_per_share(
        self,
        ask: float,
        *,
        fee_rate_bps: Optional[float] = None,
        theta: Optional[float] = None,
    ) -> float:
        from cryptobot_polymarket.fees import poly_fee_per_share

        if theta is None:
            raise ValueError("polymarket fee_per_share requires theta")
        return poly_fee_per_share(ask, theta)

    def walk_fee_fn(self, *, fee_rate_bps: Optional[float] = None):
        # None == "use walk_book's built-in parabola with the supplied
        # fee_theta", i.e. the walker's arithmetic is untouched on this venue.
        return None


ADAPTER = PolymarketAdapter()
