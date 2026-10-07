"""``cryptobot_polymarket``: the Polymarket adapter.

Modules: ``gamma`` (market discovery and settlement), ``clob`` (order books),
``ws_mirror`` (WebSocket book mirror), ``twap_mirror`` (Chainlink TWAP),
``walker`` (order placement), ``balance``/``transfer``/``relayer``/
``primitives`` (wallet and on-chain helpers).

Only ``ADAPTER`` is defined at import time. Each method imports its
implementation lazily, so resolving the adapter never starts HTTP clients,
the CLOB SDK or a WebSocket thread.
"""

from __future__ import annotations

from typing import Callable, Optional

from cryptobot.venues.base import WsMirrorSetup

__all__ = ["ADAPTER", "PolymarketAdapter"]


class PolymarketAdapter:
    """``VenueAdapter`` for Polymarket. See ``cryptobot.venues.base``."""

    name = "polymarket"

    # -- market discovery and order books -----------------------------------

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

    # -- execution ----------------------------------------------------------

    def build_order_client(self, *, runtime_cfg) -> Optional[object]:
        """OrderClient, or None to run DRY-only (no key or SDK missing)."""
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
        """Derive signer and deposit wallets from ``POLY_PRIVATE_KEY``."""
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
        """Construct and start the CLOB book mirror.

        Construction is not exception-guarded: a failure here stops the boot.
        """
        from cryptobot import config
        from cryptobot.venues.shadow_book import ShadowBookComparator
        from cryptobot_polymarket.ws_mirror import ClobBookWsMirror

        ws_mirror = ClobBookWsMirror(
            max_age_ms=config.BOOK_WS_MAX_AGE_MS,
            url=config.BOOK_WS_URL,
            # Drop a token's ladder as soon as it leaves the subscribed set, so
            # a finished window can never price a live order.
            evict_unsubscribed=config.BOOK_WS_EVICT_UNSUBSCRIBED,
            max_book_age_ms=config.BOOK_WS_MAX_BOOK_AGE_MS,
        )
        if config.BOOK_WS_SHADOW:
            # Run alongside REST for comparison; REST stays authoritative.
            shadow = ShadowBookComparator(ws_mirror)
            shadow.start()
            return WsMirrorSetup(shadow=shadow)
        ws_mirror.start()
        return WsMirrorSetup(mirror=ws_mirror)


ADAPTER = PolymarketAdapter()
