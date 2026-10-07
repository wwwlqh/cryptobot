"""predict.fun venue adapter package (behind ``CRYPTOBOT_VENUE=predictfun``).

predict.fun is a BNB-chain CLOB prediction market whose exchange contract is a
fork of Polymarket's ctf-exchange (EIP-712 order signing). This package mirrors
the Polymarket data adapters so the existing ask-band favorite-taker strategy
can run on predict.fun's BTC/ETH/BNB 5m/15m up-down markets WITHOUT touching the
Polymarket decision/parity chain.

Modules
-------
- :mod:`cryptobot_predictfun.markets` — active-market discovery mirroring
  :func:`cryptobot_polymarket.gamma.fetch_active_market` (same call
  signature + return shape; carries ``fee_rate_bps`` from the market payload).
- :mod:`cryptobot_predictfun.clob` — order-book fetch mirroring
  :mod:`cryptobot_polymarket.clob` (``fetch_top_book`` / ``fetch_books_batch``
  returning the SAME ``BookSnapshot``) plus an optional order-submission client.
- :mod:`cryptobot_predictfun.fees` — the venue's TENT fee-per-share
  ``(feeRateBps/10000) * min(p, 1-p)``. Polymarket's PARABOLA and the dispatcher
  used to live in this same file; Phase 2 split them into
  ``cryptobot_polymarket.fees`` and ``cryptobot.venues.fees``.

⚠️ Nothing here is imported on the Polymarket path. The venue registry
(``cryptobot.venues.get_adapter``, keyed by ``CRYPTOBOT_VENUE``) selects these
implementations only when the operator pins ``CRYPTOBOT_VENUE=predictfun``;
unset/``polymarket`` is byte-identical to today.

Phase 2 addresses (was ``cryptobot.*``): ``predictfun/{_http,auth,clob,markets,
ws_mirror}.py`` kept their names; ``execution/predictfun_wallet.py`` -> ``wallet.py``;
``data/predictfun_book_recorder.py`` -> ``book_recorder.py`` (deleted 2026-08-04);
``cli/derive_predictfun_creds.py`` -> ``derive_creds.py``;
``cli/predictfun_wallet_diagnostics.py`` -> ``wallet_diagnostics.py``;
``cli/predictfun_live_validation.py`` -> ``live_validation.py``;
``reconciliation/backfill_predictfun_settlement.py`` -> ``backfill_settlement.py``.

Like the Polymarket package this module is IMPORT-LIGHT — it defines ``ADAPTER``
and nothing else at module scope, so resolving the registry never pulls in
``predict_sdk``, httpx clients or a websocket thread.

VERIFIED (live MAINNET probe 2026-07-21; OpenAPI spec public at
``https://api.predict.fun/spec``):
  * REST base ``https://api.predict.fun/v1``; AUTH is an ``x-api-key: <key>``
    header (``Authorization: Bearer`` is the /auth JWT for trading endpoints
    and 401s when given the API key); testnet
    ``https://api-testnet.predict.fun/v1`` is public.
  * ``GET /markets`` -> ``{"success": bool, "cursor": ..., "data": [market, ...]}``,
    paginated ``first``/``after`` and filtered ``status=OPEN|RESOLVED`` +
    ``marketVariant=CRYPTO_UP_DOWN`` (NO slug query params exist). A market
    carries ``id``, ``conditionId``, ``oracleQuestionId``, ``categorySlug``
    (``{coin}-updown-{5|15}m-{epoch}`` for the sub-hourly sleeves; there is NO
    ``slug`` field and NO endDate — ``boostStartsAt``/``boostEndsAt`` equal the
    window bounds), ``feeRateBps`` (200 on every crypto up-down market),
    ``status`` lifecycle REGISTERED->PRICE_PROPOSED->(PRICE_DISPUTED)->RESOLVED,
    ``outcomes: [{"name", "indexSet", "onChainId", "status", "bestBid",
    "bestAsk"}]`` and, once settled, a top-level ``resolution`` = the winning
    Outcome object.
  * ``GET /markets/{id}/orderbook`` ->
    ``{"success": bool, "data": {"asks": [[price,size],...],
    "bids": [[price,size],...], "marketId", "updateTimestampMs"}}`` — ONE book
    per market, quoted from the YES/UP (indexSet 1) perspective; the DOWN view
    is the complement at decimalPrecision (see ``clob.py``). A BATCH endpoint
    exists: ``GET /markets/orderbooks?ids=<id>&ids=<id>``.
  * Rate budget: 240 requests/min per API key (``ratelimit-policy 240;w=60``).
  * Order signing SDK: ``predict_sdk`` (pip ``predict_sdk``, imports as
    ``predict_sdk``) exposes ``OrderBuilder.make(ChainId.BNB_MAINNET, signer)``,
    ``build_order``, ``build_typed_data``, ``sign_typed_data_order``. It does NOT
    ship a REST client, so discovery/book/submit are first-party REST here
    (same trust-boundary rule as the Polymarket Gamma adapter).
  * The Polymarket 250ms uncancellable marketable-order delay has NO documented
    predict.fun equivalent as of 2026-07-21 — treated as UNKNOWN (see
    ``clob.py``); do not assume it is absent.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from cryptobot.venues.base import WsMirrorSetup

__all__ = ["ADAPTER", "PredictFunAdapter"]

logger = logging.getLogger(__name__)


class PredictFunAdapter:
    """``VenueAdapter`` for predict.fun. See ``cryptobot.venues.base``."""

    name = "predictfun"

    # -- market discovery + order books -------------------------------------

    def fetch_active_market(self, *args, **kwargs):
        from cryptobot_predictfun.markets import fetch_active_market

        return fetch_active_market(*args, **kwargs)

    def fetch_top_book(self, *args, **kwargs):
        from cryptobot_predictfun.clob import fetch_top_book

        return fetch_top_book(*args, **kwargs)

    def fetch_books_batch(self, *args, **kwargs):
        from cryptobot_predictfun.clob import fetch_books_batch

        return fetch_books_batch(*args, **kwargs)

    def fetch_resolved_outcome(self, *args, **kwargs):
        from cryptobot_predictfun.markets import fetch_resolved_outcome

        return fetch_resolved_outcome(*args, **kwargs)

    def market_cache_has(
        self, coin: str, interval_min: int, window_start_ts: int
    ) -> bool:
        from cryptobot_predictfun.markets import market_cache_has

        return market_cache_has(coin, interval_min, window_start_ts)

    def book_fetch_kwargs(self) -> dict:
        """PF-only decision-path fetch kwargs (bucket split + read timeout).

        Give the inline decision + stop book reads their OWN split-rate bucket
        (1.4/s) so the A8 prefetch daemon (0.6/s) can never starve them; a 429
        on either still pauses both. The optional per-request read timeout is
        unset by default (byte-identical); connect stays 2.0s so the
        dead-socket cap survives — see the config note.
        """
        import httpx

        from cryptobot import config
        from cryptobot_predictfun.clob import decision_bucket

        kwargs: dict = {"bucket": decision_bucket}
        if config.PREDICTFUN_BOOK_READ_TIMEOUT_S is not None:
            kwargs["read_timeout"] = httpx.Timeout(
                config.PREDICTFUN_BOOK_READ_TIMEOUT_S, connect=2.0
            )
        return kwargs

    def prefetch_fetch_kwargs(self) -> dict:
        """Split-rate prefetch bucket (0.6/s): a prefetch burst draws only its
        own allotment, never the decision bucket's tokens; both share the one
        process-wide 429 backoff."""
        from cryptobot_predictfun.clob import prefetch_bucket

        return {"bucket": prefetch_bucket}

    # -- execution ----------------------------------------------------------

    def build_order_client(self, *, runtime_cfg) -> Optional[object]:
        """predict.fun order submission (EIP-712 via ``predict_sdk``).

        DRY mode needs NO key/SDK — returning None makes LIVE cells SKIP. Log
        lines are byte-identical to the pre-split ``main.py`` branch.
        """
        from cryptobot import config
        from cryptobot_predictfun.clob import (
            PredictFunClientUnavailable,
            PredictFunOrderClient,
        )

        main_logger = logging.getLogger("cryptobot.main")
        if not config.PREDICTFUN_PRIVATE_KEY:
            main_logger.info(
                "PREDICTFUN_PRIVATE_KEY empty — booting DRY-only (LIVE cells SKIP)"
            )
            return None
        try:
            return PredictFunOrderClient(
                api_base=config.PREDICTFUN_API_BASE,
                api_key=config.PREDICTFUN_API_KEY,
                private_key=config.PREDICTFUN_PRIVATE_KEY,
                chain_id=config.PREDICTFUN_CHAIN_ID,
                # Kernel smart-account maker (empty = EOA is the maker).
                predict_account=getattr(config, "PREDICTFUN_PREDICT_ACCOUNT", ""),
            )
        except PredictFunClientUnavailable as exc:
            main_logger.warning(
                "PredictFunOrderClient unavailable (%s) — booting DRY-only "
                "(LIVE cells SKIP)", exc,
            )
        except Exception:  # noqa: BLE001
            main_logger.exception("PredictFunOrderClient init raised — DRY-only")
        return None

    def build_balance_fetcher(self, config) -> Callable[[], Optional[float]]:
        """predict.fun collateral (USDT/BSC-USD).

        FAIL-CLOSED: raises when the wallet plumbing is not configured, so
        readiness records chain_unreachable / NOT_READY and the venue stays DRY
        — it never fakes a ready balance.
        """
        from cryptobot_predictfun.wallet import build_collateral_balance_fetcher

        return build_collateral_balance_fetcher()

    def derive_wallet_topology(self, config):
        # Not reached: core's readiness gate asks the POLYMARKET adapter for
        # this by name (the gate is Polymarket-shaped on every venue, which is
        # pre-Phase-2 behavior deliberately preserved). Present so the adapter
        # satisfies the protocol.
        raise NotImplementedError(
            "predict.fun has no Polymarket-style signer/deposit topology"
        )

    # -- websocket book mirror ----------------------------------------------

    def build_ws_mirror(self, *, order_client) -> WsMirrorSetup:
        """Construct + start the predict.fun WS book mirror.

        Own module + thread; never touches the Polymarket mirror. Its own
        default endpoint (wss://ws.predict.fun/ws) is used — ``BOOK_WS_URL`` is
        the Polymarket socket and must NOT be passed.

        Exception-guarded like every optional surface, and the guard covers
        ``start()`` too (pre-split it wrapped the whole block): a raise logs and
        leaves REST authoritative, which also means the A8 prefetcher still
        runs because ``replaces_rest_prefetch`` stays False.
        """
        from cryptobot import config
        from cryptobot.venues.shadow_book import ShadowBookComparator
        from cryptobot_predictfun.ws_mirror import PredictFunBookWsMirror

        try:
            ws_mirror = PredictFunBookWsMirror(max_age_ms=config.BOOK_WS_MAX_AGE_MS)
            if config.PREDICTFUN_WS_WALLET_EVENTS and order_client is not None:
                # predictWalletEvents fill-push (flag default OFF): the mirror
                # carries the authenticated wallet topic on its existing
                # socket/heartbeat/reconnect loop, and the order client awaits
                # the pushed terminal state BEFORE its UNCHANGED
                # GET /orders/{hash} poll. LIVE-only: order_client is None in
                # DRY mode, so nothing subscribes and no waiter exists there;
                # boot never depends on WS (await_fill_state returns
                # immediately while the mirror is inert / not running — a
                # wired-but-dead mirror never taxes the poll path, even when
                # the shadow start below fails after this wiring). The jwt
                # provider is a non-fetching peek — the WS side never spends an
                # /auth round trip of its own — and the rotation hook
                # resubscribes the topic whenever the order path
                # refreshes/invalidates the JWT. Wired BEFORE the shadow split:
                # the shadow comparator start()s this same mirror, so fill-push
                # works in both book modes.
                ws_mirror.enable_wallet_events(order_client.peek_wallet_jwt)
                order_client.set_jwt_rotation_hook(ws_mirror.notify_jwt_rotated)
                order_client.set_fill_event_waiter(
                    ws_mirror,
                    wait_ms=config.PREDICTFUN_WS_WALLET_EVENTS_WAIT_MS,
                )
            if config.BOOK_WS_SHADOW:
                # W3: run the mirror ALONGSIDE REST (never authoritative) so the
                # operator can watch WS/REST divergence — and confirm WS
                # subscribe/heartbeat do not breach the 240/min REST key — for a
                # day before flipping authoritative. REST stays authoritative
                # for every decision.
                shadow = ShadowBookComparator(ws_mirror)
                shadow.start()
                return WsMirrorSetup(shadow=shadow)
            # Authoritative: sit FIRST in the chain so a fresh WS snapshot
            # outranks the REST prefetch cache, and take over the off-tick
            # book-warming role from the A8 REST prefetcher.
            ws_mirror.start()
            return WsMirrorSetup(mirror=ws_mirror, replaces_rest_prefetch=True)
        except Exception:  # noqa: BLE001 — optional surface, never a boot crash
            logging.getLogger("cryptobot.main").warning(
                "predict.fun WS book mirror failed to initialize — "
                "continuing REST-only (book prefetch/batch authoritative)",
                exc_info=True,
            )
            return WsMirrorSetup()

    # -- fees ---------------------------------------------------------------

    def fee_per_share(
        self,
        ask: float,
        *,
        fee_rate_bps: Optional[float] = None,
        theta: Optional[float] = None,
    ) -> float:
        from cryptobot_predictfun.fees import (
            _default_fee_rate_bps,
            predictfun_fee_per_share,
        )

        bps = _default_fee_rate_bps() if fee_rate_bps is None else fee_rate_bps
        return predictfun_fee_per_share(ask, bps)

    def walk_fee_fn(self, *, fee_rate_bps: Optional[float] = None):
        """A TENT closure over the market's ``fee_rate_bps``, which
        ``walk_book_edge_preserving`` applies at each candidate VWAP in place of
        the parabola."""
        from cryptobot_predictfun.fees import (
            _default_fee_rate_bps,
            predictfun_fee_per_share,
        )

        bps = _default_fee_rate_bps() if fee_rate_bps is None else fee_rate_bps

        def _tent(p: float) -> float:
            return predictfun_fee_per_share(p, bps)

        return _tent


ADAPTER = PredictFunAdapter()
