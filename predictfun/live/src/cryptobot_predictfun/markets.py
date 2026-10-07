"""predict.fun active-market discovery.

Mirrors :func:`cryptobot_polymarket.gamma.fetch_active_market` — SAME call
signature and SAME consumed return shape (``coin``, ``interval_min``,
``window_start_ts``, ``window_end_ts``, ``condition_id``, ``yes_token_id``,
``no_token_id``, ``fee_rate_bps``) so the pipeline's ``deps.fetch_market(...)``
seam is venue-agnostic. Two predict.fun-specific fields are ADDED
(``market_id`` + ``slug``) because the order-book endpoint is keyed by the
integer market id, not the outcome token.

Endpoint contract (2026-07-21 MAINNET probe, authoritative — the OpenAPI spec is
public at ``https://api.predict.fun/spec``)::

    GET {PREDICTFUN_API_BASE}/markets
        ?status=OPEN&marketVariant=CRYPTO_UP_DOWN&first=100[&after=<cursor>]
        -> {"success": bool, "cursor": ..., "data": [market, ...]}
    GET {PREDICTFUN_API_BASE}/markets/{id}   (integer id)
        -> {"success": bool, "data": market}

Probe-confirmed facts encoded here (each was a live mismatch in the first draft):

  * AUTH is an ``x-api-key: <key>`` header. ``Authorization: Bearer`` is a
    DIFFERENT credential (a JWT from the ``/auth`` wallet-signature flow, needed
    only for trading/account endpoints) and 401s when given the API key.
  * ``slug`` / ``categorySlug`` QUERY PARAMS DO NOT EXIST on ``GET /markets``
    (unknown params are silently ignored and the default 20-oldest-markets page
    comes back), so discovery lists ``status=OPEN&marketVariant=CRYPTO_UP_DOWN``
    pages and matches ``categorySlug`` client-side. Pagination is
    ``first``/``after`` (``after`` = base64 of the last integer id; the response
    also carries a ``cursor`` which is preferred when present).
  * The Market object has NO ``slug`` field — the up-down window identity lives
    in ``categorySlug`` = ``{coin}-updown-{5|15}m-{windowStartEpochUTC}`` (the
    ``slug_for`` convention, confirmed for 5m/15m). There is also NO
    ``endDate``/``closeTime``-style field; ``boostStartsAt``/``boostEndsAt``
    equal the exact window bounds on up-down markets and are the fallback.
  * HOURLY/DAILY up-downs do NOT follow the convention (categorySlug like
    ``bitcoin-up-or-down-july-21-2026-10am-et``, ET-clock aligned, BINANCE feed)
    — only the 5m/15m sleeves are discoverable here, which is exactly the
    recorder/scanner universe.
  * Resolution: the settled winner is the top-level ``resolution`` field (the
    winning Outcome OBJECT: ``{name, indexSet, onChainId, status: 'WON'}``) and
    each ``outcomes[]`` entry carries ``status`` ``'WON'``/``'LOST'``. The
    status lifecycle is REGISTERED -> PRICE_PROPOSED -> (PRICE_DISPUTED) ->
    RESOLVED; only ``status == 'RESOLVED'`` (or an explicit resolution/outcome
    status) is a safe settled signal — ``tradingStatus == 'CLOSED'`` is NOT
    (trading closes before the price is finalized).
  * Server rate budget: 240 requests/min per API key (``ratelimit-policy
    240;w=60``). The short-TTL listing cache below exists so per-tick discovery
    across 6 (coin, interval) cells costs at most one page sweep per TTL.

Fail-closed on an unmappable outcomes array — refusing an unverifiable
UP/DOWN->token mapping is strictly safer than a positional guess (a reversed map
buys the OPPOSITE side the model scored).
"""

from __future__ import annotations

import base64
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

# FeeUnknown IS polymarket_gamma's sentinel class (one class, both venues):
# the pipeline gates with `isinstance(market, polymarket_gamma.FeeUnknown)`
# regardless of venue, so a distinct predict.fun class would silently defeat
# those gates. Only the FEE_UNKNOWN *instance* is local.
from cryptobot.venues.types import (
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
    FeeUnknown,
    PolymarketResolution,
)
from cryptobot_predictfun._http import api_base as _api_base
from cryptobot_predictfun._http import auth_headers as _auth_headers

logger = logging.getLogger(__name__)

FEE_UNKNOWN = FeeUnknown()

# UP/DOWN outcome name aliases (predict.fun names up-down outcomes "Up"/"Down";
# `lastOrderSettled.outcome` and older payloads use Yes/No — Yes==Up, probe
# 2026-07-21).
_UP_NAMES = ("UP", "YES")
_DOWN_NAMES = ("DOWN", "NO")


@dataclass(frozen=True)
class PredictFunMarket:
    """One resolved predict.fun crypto up-down market.

    The leading fields match ``polymarket_gamma.PolyMarket`` one-for-one so the
    pipeline consumes either interchangeably. ``market_id`` + ``slug`` are the
    predict.fun-only additions the CLOB book endpoint needs.
    """

    coin: str
    interval_min: int
    window_start_ts: int
    window_end_ts: int
    condition_id: str
    yes_token_id: str  # UP outcome onChainId
    no_token_id: str   # DOWN outcome onChainId
    fee_rate_bps: float
    # predict.fun-specific:
    market_id: str     # integer market id (string), keys /markets/{id}/orderbook
    slug: str          # categorySlug ({coin}-updown-{n}m-{epoch})
    # Order-signing metadata (selects the EIP-712 verifying contract; a wrong
    # flag = an invalid signature, fail-safe reject). Standard crypto up-down
    # markets are expected False/False; parsed tolerantly from the payload
    # (missing key -> False) and carried into the token registry because the
    # order client's SELL path resolves by token id with no market object.
    is_neg_risk: bool = False
    is_yield_bearing: bool = False


# ---------------------------------------------------------------------------
# In-process per-window cache — identical semantics to the Gamma adapter's:
# a resolved market's identity is immutable for its window, so cache SUCCESSFUL
# resolutions until window_end + grace; never cache None/FeeUnknown (the market
# may simply not be listed yet — the per-tick retry is desired).
# ---------------------------------------------------------------------------
_MARKET_CACHE: dict[tuple[str, int, int], PredictFunMarket] = {}
_MARKET_CACHE_LOCK = threading.Lock()
_MARKET_CACHE_GRACE_S = 120.0

# ---------------------------------------------------------------------------
# Short-TTL OPEN-listing cache. Discovery has NO per-slug endpoint (see module
# docstring), so every (coin, interval, window) cache-miss would otherwise pay a
# multi-page listing sweep — with 6 cells scanning per tick that blows the
# 240/min key budget. One sweep serves every cell for _LISTING_TTL_S.
# ---------------------------------------------------------------------------
_LISTING_TTL_S = 5.0
_LISTING_MAX_PAGES = 5
_LISTING_PAGE_SIZE = 100
_LISTING_CACHE: dict = {"fetched_at": 0.0, "markets": None}
_LISTING_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# Discovery registries (probe follow-up, 2026-07-21):
#   * token registry: outcome onChainId -> (market_id, 'UP'|'DOWN'). The order
#     book is keyed by MARKET id and quoted from the UP view, so the CLOB
#     adapter resolves the pipeline's outcome-token reads through this map
#     (unknown token -> fail-closed None in clob.fetch_top_book).
#   * market-id registry: (coin, interval_min, window_start_ts) -> market_id.
#     Resolution needs GET /markets/{id} AFTER the window closes (there is no
#     slug query), so discovery persists the id well past the window cache's
#     grace. In-process only: after a restart, un-registered settled windows
#     stay PENDING (fail-closed) until the backfill CLI regrades them.
# ---------------------------------------------------------------------------
_TOKEN_REGISTRY: dict[str, tuple[str, str, float, float, bool, bool]] = {}
_MARKET_ID_REGISTRY: dict[tuple[str, int, int], tuple[str, float]] = {}
_REGISTRY_LOCK = threading.Lock()
_REGISTRY_TTL_S = 26 * 3600.0


def clear_market_cache() -> None:
    """Test/ops hook: drop every cached market resolution + registry entry."""
    with _MARKET_CACHE_LOCK:
        _MARKET_CACHE.clear()
    with _LISTING_LOCK:
        _LISTING_CACHE["markets"] = None
        _LISTING_CACHE["fetched_at"] = 0.0
    with _REGISTRY_LOCK:
        _TOKEN_REGISTRY.clear()
        _MARKET_ID_REGISTRY.clear()


def _market_cache_get(
    coin: str, interval_min: int, window_start_ts: int
) -> Optional[PredictFunMarket]:
    # coin is case-normalized HERE, once (the recorder resolves with lowercase
    # coins, the pipeline with uppercase) so every caller hits one cache entry.
    with _MARKET_CACHE_LOCK:
        return _MARKET_CACHE.get(
            (str(coin).lower(), int(interval_min), int(window_start_ts))
        )


def market_cache_has(coin: str, interval_min: int, window_start_ts: int) -> bool:
    """True when the (coin, interval, window) resolution is already cached."""
    return _market_cache_get(coin, interval_min, window_start_ts) is not None


def _market_cache_put(market: PredictFunMarket, now_ts: float) -> None:
    key = (
        market.coin.lower(),
        int(market.interval_min),
        int(market.window_start_ts),
    )
    with _MARKET_CACHE_LOCK:
        expired = [
            k
            for k, m in _MARKET_CACHE.items()
            if float(m.window_end_ts) + _MARKET_CACHE_GRACE_S < float(now_ts)
        ]
        for k in expired:
            _MARKET_CACHE.pop(k, None)
        _MARKET_CACHE[key] = market


def _register_market(market: PredictFunMarket, now_ts: float) -> None:
    """Record the token->market and window->market-id maps for the CLOB/resolver."""
    now = float(now_ts)
    with _REGISTRY_LOCK:
        for token, entry in list(_TOKEN_REGISTRY.items()):
            if entry[2] + _REGISTRY_TTL_S < now:
                _TOKEN_REGISTRY.pop(token, None)
        for key, entry in list(_MARKET_ID_REGISTRY.items()):
            if entry[1] + _REGISTRY_TTL_S < now:
                _MARKET_ID_REGISTRY.pop(key, None)
        _TOKEN_REGISTRY[str(market.yes_token_id)] = (
            str(market.market_id), "UP", now,
            float(market.fee_rate_bps),
            bool(market.is_neg_risk), bool(market.is_yield_bearing),
        )
        _TOKEN_REGISTRY[str(market.no_token_id)] = (
            str(market.market_id), "DOWN", now,
            float(market.fee_rate_bps),
            bool(market.is_neg_risk), bool(market.is_yield_bearing),
        )
        _MARKET_ID_REGISTRY[
            (market.coin.lower(), int(market.interval_min), int(market.window_start_ts))
        ] = (str(market.market_id), now)


def lookup_token_market(token_id: str) -> Optional[tuple[str, str]]:
    """Resolve an outcome onChainId to ``(market_id, 'UP'|'DOWN')``.

    Returns None for an unknown token — the CLOB adapter treats that as
    fail-closed (no book) rather than guessing a market/side.
    """
    with _REGISTRY_LOCK:
        entry = _TOKEN_REGISTRY.get(str(token_id))
    if entry is None:
        return None
    return entry[0], entry[1]


@dataclass(frozen=True)
class TokenOrderMeta:
    """Order-signing metadata for one outcome token (consumed by the order
    client, whose SELL path receives only a token id — no market object)."""

    market_id: str
    side: str  # 'UP' | 'DOWN'
    fee_rate_bps: float
    is_neg_risk: bool
    is_yield_bearing: bool


def lookup_token_order_meta(token_id: str) -> Optional[TokenOrderMeta]:
    """Signing metadata for an outcome onChainId, or None (fail-closed).

    Same registry as :func:`lookup_token_market`; None means the token was
    never registered by discovery this process — the order client refuses to
    sign rather than guessing fee / EIP-712 domain flags.
    """
    with _REGISTRY_LOCK:
        entry = _TOKEN_REGISTRY.get(str(token_id))
    if entry is None:
        return None
    return TokenOrderMeta(
        market_id=entry[0],
        side=entry[1],
        fee_rate_bps=float(entry[3]),
        is_neg_risk=bool(entry[4]),
        is_yield_bearing=bool(entry[5]),
    )


def lookup_market_id(
    coin: str, interval_min: int, window_start_ts: int
) -> Optional[str]:
    """Market id registered at discovery time for (coin, interval, window)."""
    with _REGISTRY_LOCK:
        entry = _MARKET_ID_REGISTRY.get(
            (coin.lower(), int(interval_min), int(window_start_ts))
        )
    return entry[0] if entry is not None else None


def slug_for(coin: str, interval_min: int, window_start_ts: int) -> str:
    """Canonical predict.fun up-down categorySlug for a window (probe-confirmed
    for the 5m/15m sleeves: ``btc-updown-15m-1784645100``)."""
    return f"{coin.lower()}-updown-{interval_min}m-{int(window_start_ts)}"


def _default_fee_bps() -> float:
    from cryptobot import config

    return float(config.PREDICTFUN_DEFAULT_FEE_RATE_BPS)


def _cat_slug(market: dict) -> str:
    """The market's window-identity slug: ``categorySlug`` (the ONLY slug field
    on the real Market object — probe 2026-07-21)."""
    return str(market.get("categorySlug") or "").lower()


def _coin_matches(coin: str, market: dict) -> bool:
    """True when ``market``'s categorySlug follows the confirmed 5m/15m up-down
    convention for ``coin``: ``{coin}-updown-...`` (probe 2026-07-21; the
    hourly/daily ``bitcoin-up-or-down-...`` slugs must never match)."""
    return _cat_slug(market).startswith(f"{coin.lower()}-updown-")


def _interval_matches(interval_min: int, market: dict) -> bool:
    """Confirmed convention: an exact ``-{n}m-`` segment in the categorySlug."""
    return f"-{interval_min}m-" in _cat_slug(market)


def _window_end_ts_of(market: dict) -> Optional[int]:
    """Window-end epoch from ``boostEndsAt``.

    The real Market object has NO endDate/closeTime field (probe 2026-07-21);
    ``boostEndsAt`` equals the exact window end on up-down markets and is the
    only source. Tolerates numeric epoch (s or ms) or ISO-8601.
    """
    raw = market.get("boostEndsAt")
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        return int(val / 1000.0) if val > 1e12 else int(val)
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _slug_epoch_of(market: dict) -> Optional[int]:
    slug = _cat_slug(market)
    if not slug:
        return None
    tail = slug.rsplit("-", 1)[-1]
    if tail.isdigit():
        return int(tail)
    return None


def _map_outcome_tokens(market: dict) -> Optional[tuple[str, str]]:
    """Return (up_token, down_token) onChainIds, or None if unmappable."""
    outcomes = market.get("outcomes")
    if not isinstance(outcomes, list):
        return None
    up_token = down_token = None
    for oc in outcomes:
        if not isinstance(oc, dict):
            continue
        name = str(oc.get("name", "")).strip().upper()
        tok = oc.get("onChainId")
        if tok in (None, ""):
            continue
        tok = str(tok)
        # Prefix match so "Probably yes ..." style names still map on YES/NO,
        # but the canonical Up/Down names hit exactly.
        if any(name == n or name.startswith(n) for n in _UP_NAMES) and up_token is None:
            up_token = tok
        elif any(name == n or name.startswith(n) for n in _DOWN_NAMES) and down_token is None:
            down_token = tok
    if up_token is None or down_token is None:
        return None
    return up_token, down_token


def _build_market(
    coin: str,
    interval_min: int,
    window_start_ts: int,
    window_end_ts: int,
    market: dict,
) -> Optional[PredictFunMarket] | FeeUnknown:
    cond = str(market.get("conditionId", "") or "")
    if not cond:
        return None
    market_id = market.get("id")
    if market_id in (None, ""):
        return None
    tokens = _map_outcome_tokens(market)
    if tokens is None:
        # Fail closed — never guess UP/DOWN positionally (a reversed map buys
        # the opposite side the model scored: a guaranteed adverse fill).
        logger.warning(
            "predictfun markets: outcomes not mappable for %s — refusing "
            "positional token mapping; skipping this tick",
            slug_for(coin, interval_min, window_start_ts),
        )
        return None
    up_token, down_token = tokens

    fee_raw = market.get("feeRateBps")
    if fee_raw is None:
        fee_rate = _default_fee_bps()
        logger.info(
            "predictfun markets: %s feeRateBps=null — defaulting to %.1f bps",
            slug_for(coin, interval_min, window_start_ts), fee_rate,
        )
    else:
        try:
            fee_rate = float(fee_raw)
        except (TypeError, ValueError):
            return FEE_UNKNOWN
        if not (fee_rate > 0.0) or fee_rate != fee_rate:  # <=0 / NaN
            return FEE_UNKNOWN

    return PredictFunMarket(
        coin=coin,
        interval_min=interval_min,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        condition_id=cond,
        yes_token_id=up_token,
        no_token_id=down_token,
        fee_rate_bps=fee_rate,
        market_id=str(market_id),
        slug=_cat_slug(market) or slug_for(coin, interval_min, window_start_ts),
        is_neg_risk=bool(market.get("isNegRisk", False)),
        is_yield_bearing=bool(market.get("isYieldBearing", False)),
    )


def _select_market(
    coin: str,
    interval_min: int,
    window_start_ts: int,
    window_end_ts: int,
    data: list,
) -> Optional[dict]:
    """Pick the crypto up-down market for (coin, interval, window) from ``data``."""
    for m in data:
        if not isinstance(m, dict):
            continue
        if not _coin_matches(coin, m) or not _interval_matches(interval_min, m):
            continue
        # Window alignment: the categorySlug epoch is authoritative; fall back
        # to boostEndsAt (== window end on up-down markets) within 60s.
        epoch = _slug_epoch_of(m)
        if epoch is not None:
            if epoch == int(window_start_ts):
                return m
            continue
        end_ts = _window_end_ts_of(m)
        if end_ts is not None and abs(end_ts - window_end_ts) <= 60:
            return m
    return None


def _encode_after_cursor(last_id) -> Optional[str]:
    try:
        return base64.b64encode(str(int(last_id)).encode("ascii")).decode("ascii")
    except (TypeError, ValueError):
        return None


def _fetch_open_updown_markets(
    client: httpx.Client, base: str, headers: dict, now_ts: float
) -> Optional[list[dict]]:
    """Fetch (with a short-TTL cache) the OPEN CRYPTO_UP_DOWN market listing.

    Pages ``first=100`` ascending by id, following ``cursor``/``after`` for up
    to ``_LISTING_MAX_PAGES`` pages. The active 5m/15m windows were created
    ~30-70min before they trade, so they sit EARLY in the ascending OPEN listing
    — page 1 in practice; 5 pages is generous headroom. Returns None on any
    HTTP/auth failure (never caches a partial sweep).
    """
    with _LISTING_LOCK:
        cached = _LISTING_CACHE["markets"]
        if cached is not None and float(now_ts) - _LISTING_CACHE["fetched_at"] < _LISTING_TTL_S:
            return cached

    collected: list[dict] = []
    after: Optional[str] = None
    for _page in range(_LISTING_MAX_PAGES):
        params: dict = {
            "status": "OPEN",
            "marketVariant": "CRYPTO_UP_DOWN",
            "first": _LISTING_PAGE_SIZE,
        }
        if after:
            params["after"] = after
        try:
            resp = client.get(f"{base}/markets", params=params, headers=headers or None)
        except httpx.HTTPError as exc:
            logger.info("predictfun markets: listing http error %r", exc)
            return None
        if resp.status_code == 401:
            logger.warning(
                "predictfun markets: 401 unauthorized — set PREDICTFUN_API_KEY "
                "(x-api-key header; mainnet requires it, testnet is public)"
            )
            return None
        if resp.status_code >= 400:
            logger.warning("predictfun markets: listing status=%d", resp.status_code)
            return None
        try:
            body = resp.json()
        except ValueError:
            return None
        data = body.get("data") if isinstance(body, dict) else body
        if not isinstance(data, list):
            break
        collected.extend(m for m in data if isinstance(m, dict))
        if len(data) < _LISTING_PAGE_SIZE:
            break
        cursor = body.get("cursor") if isinstance(body, dict) else None
        if isinstance(cursor, str) and cursor:
            after = cursor
        else:
            last = data[-1] if isinstance(data[-1], dict) else {}
            after = _encode_after_cursor(last.get("id"))
            if after is None:
                break

    with _LISTING_LOCK:
        _LISTING_CACHE["markets"] = collected
        _LISTING_CACHE["fetched_at"] = float(now_ts)
    return collected


def fetch_active_market(
    coin: str,
    *,
    interval_min: int,
    window_start_ts: int,
    conn: Optional[sqlite3.Connection] = None,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    now_ts: float | None = None,
) -> Optional[PredictFunMarket] | FeeUnknown:
    """Resolve the predict.fun up-down market for (coin, interval, window).

    Signature mirrors ``polymarket_gamma.fetch_active_market`` (``conn`` accepted
    for call-site compatibility; predict.fun returns ``feeRateBps`` inline so no
    SQLite fee cache is required — ``conn`` is currently unused here).

    Discovery lists ``status=OPEN&marketVariant=CRYPTO_UP_DOWN`` (there is no
    slug query — probe 2026-07-21) and matches ``categorySlug`` ==
    ``slug_for(...)`` client-side, behind a short-TTL listing cache.

    Returns a ``PredictFunMarket`` on success, ``FEE_UNKNOWN`` when the market
    exists but its fee is unusable, or ``None`` when the market is not listed
    yet / HTTP fails / the window doesn't align (all -> the gate stack's
    SKIP:stale_book, re-resolved next tick).
    """
    if now_ts is None:
        now_ts = time.time()
    window_end_ts = int(window_start_ts) + int(interval_min) * 60

    cached = _market_cache_get(coin, interval_min, window_start_ts)
    if cached is not None:
        return cached

    base = _api_base()
    headers = _auth_headers()

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    try:
        listing = _fetch_open_updown_markets(client, base, headers, float(now_ts))
    finally:
        if owned_client:
            client.close()

    if not listing:
        return None
    selected = _select_market(
        coin, interval_min, window_start_ts, window_end_ts, listing
    )
    if selected is None:
        return None

    built = _build_market(
        coin, interval_min, window_start_ts, window_end_ts, selected
    )
    if isinstance(built, PredictFunMarket):
        _market_cache_put(built, float(now_ts))
        _register_market(built, float(now_ts))
    return built


# ---------------------------------------------------------------------------
# Settlement — API-outcome path (deliverable 4).
#
# predict.fun settles 5m/15m crypto up-down from Chainlink (priceFeedProvider
# CHAINLINK per market; hourly/daily use BINANCE feeds — not our sleeves), and
# the RESOLVED outcome is published on the markets API once the price is
# finalized. We read that published outcome only (the API-outcome path); the
# candle-recompute fallback is documented-but-unbuilt (see the comment block at
# the end of this module — never silently mis-settles). FAIL-CLOSED: any
# ambiguity — market not found, still in the PRICE_PROPOSED/DISPUTED phases, or
# an unmappable winner — returns OPEN so the row stays PENDING and is retried
# next sweep, exactly like Polymarket's resolver.
#
# Payload shape (2026-07-21 mainnet probe): a settled market has
# ``status == 'RESOLVED'``, a top-level ``resolution`` = the winning Outcome
# OBJECT ({name: 'Up'|'Down', indexSet, onChainId, status: 'WON'}), and each
# outcomes[] entry carries status 'WON'|'LOST'. There are NO `won` bools, NO
# payout numerators, NO winningOutcome/winner strings — only the probe-real
# fields are read.
# ---------------------------------------------------------------------------


def _is_market_resolved(market: dict) -> bool:
    """True when the payload unambiguously indicates a SETTLED market.

    Probe 2026-07-21: the lifecycle is REGISTERED -> PRICE_PROPOSED ->
    (PRICE_DISPUTED) -> RESOLVED. ``tradingStatus == 'CLOSED'`` and the
    PRICE_* phases mean trading ended but the price is NOT final — treating
    them as resolved would settle on a disputable price, so only
    ``status == 'RESOLVED'``, a present ``resolution`` object, or an outcome
    with status WON/LOST count.
    """
    status = str(market.get("status") or "").strip().upper()
    if status == "RESOLVED":
        return True
    if isinstance(market.get("resolution"), dict):
        return True
    outcomes = market.get("outcomes")
    if isinstance(outcomes, list):
        for oc in outcomes:
            if not isinstance(oc, dict):
                continue
            if str(oc.get("status") or "").strip().upper() in ("WON", "LOST"):
                return True
    return False


def _outcome_is_up(oc: dict) -> Optional[bool]:
    """Map one Outcome object to UP (True) / DOWN (False) / unknown (None)."""
    name = str(oc.get("name", "")).strip().upper()
    if name:
        if any(name == n or name.startswith(n) for n in _UP_NAMES):
            return True
        if any(name == n or name.startswith(n) for n in _DOWN_NAMES):
            return False
    # indexSet is 1 for Up/Yes, 2 for Down/No on binary up-down markets (probe).
    idx = oc.get("indexSet")
    if idx == 1:
        return True
    if idx == 2:
        return False
    return None


def _resolved_won_up(market: dict) -> Optional[bool]:
    """Return True (UP won) / False (DOWN won) / None (ambiguous -> fail-closed).

    Checks the two probe-real signals only: the top-level ``resolution``
    winning-Outcome OBJECT, then the per-outcome ``status`` WON/LOST pair.
    Never guesses positionally.
    """
    # 1. Probe-real: top-level `resolution` = the winning Outcome object.
    res = market.get("resolution")
    if isinstance(res, dict):
        res_status = str(res.get("status") or "").strip().upper()
        side = _outcome_is_up(res)
        if side is not None and res_status in ("", "WON"):
            return side

    # 2. Probe-real: per-outcome status WON/LOST.
    outcomes = market.get("outcomes")
    if not isinstance(outcomes, list):
        return None
    up_won: Optional[bool] = None
    down_won: Optional[bool] = None
    for oc in outcomes:
        if not isinstance(oc, dict):
            continue
        side = _outcome_is_up(oc)
        if side is None:
            continue
        oc_status = str(oc.get("status") or "").strip().upper()
        if oc_status == "WON":
            decided = True
        elif oc_status == "LOST":
            decided = False
        else:
            continue
        if side:
            up_won = decided
        else:
            down_won = decided
    # Require a consistent, decisive pair to settle (fail-closed otherwise).
    if up_won is True and down_won in (False, None):
        return True
    if down_won is True and up_won in (False, None):
        return False
    if up_won is False and down_won is None:
        return False
    if down_won is False and up_won is None:
        return True
    return None


def _fetch_market_by_id(
    market_id: str, base: str, headers: dict, client: httpx.Client
) -> Optional[dict]:
    """GET /markets/{id} -> the Market object (works for settled markets)."""
    try:
        resp = client.get(f"{base}/markets/{market_id}", headers=headers or None)
    except httpx.HTTPError as exc:
        logger.info("predictfun resolve: http error %r for id=%s", exc, market_id)
        return None
    if resp.status_code == 401:
        logger.warning("predictfun resolve: 401 unauthorized (set PREDICTFUN_API_KEY)")
        return None
    if resp.status_code >= 400:
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    data = body.get("data") if isinstance(body, dict) else body
    return data if isinstance(data, dict) else None


def fetch_resolved_outcome(coin, *, interval_min, window_start_ts, client=None, timeout=3.0):
    """Fetch predict.fun's published UP/DOWN outcome for a settled market.

    Returns a ``PolymarketResolution`` (same dataclass Polymarket uses, so the
    resolution sweep + the settlement backfill consume it venue-agnostically):

      * ``RESOLUTION_RESOLVED`` + ``won_up`` / ``up_price`` / ``down_price`` when
        the API unambiguously reports the winner;
      * ``RESOLUTION_OPEN`` for every other case (market not resolvable / still
        in the PRICE_PROPOSED-DISPUTED phases / winner unmappable / HTTP error)
        — the row stays PENDING and is retried next sweep. NEVER mis-settles on
        an ambiguous payload.

    Market lookup (no slug query exists — probe 2026-07-21): the market id
    registered at discovery time -> ``GET /markets/{id}``; when the registry has
    no entry (e.g. the process restarted after the window closed) a bounded
    OPEN-listing scan is tried as best-effort recovery, else the row stays
    PENDING for the backfill CLI (fail-closed, never guesses).
    """
    window_end_ts = int(window_start_ts) + int(interval_min) * 60
    slug = slug_for(coin, interval_min, window_start_ts)
    base = _api_base()
    headers = _auth_headers()

    def _open(source: str = "predictfun") -> "PolymarketResolution":
        return PolymarketResolution(
            status=RESOLUTION_OPEN,
            coin=coin,
            interval_min=int(interval_min),
            window_start_ts=int(window_start_ts),
            window_end_ts=window_end_ts,
            condition_id=None,
            source=source,
        )

    market_id = lookup_market_id(coin, interval_min, window_start_ts)
    if market_id is None:
        # Coin case is normalized inside the cache helpers, so one lookup
        # serves both the recorder's lowercase and the pipeline's uppercase.
        cached = _market_cache_get(coin, interval_min, window_start_ts)
        if cached is not None:
            market_id = cached.market_id

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    try:
        market: Optional[dict] = None
        if market_id is not None:
            market = _fetch_market_by_id(market_id, base, headers, client)
        if market is None:
            # Registry miss (restart) — best-effort: the market may still be in
            # the OPEN listing during its PRICE_PROPOSED phase.
            listing = _fetch_open_updown_markets(client, base, headers, time.time())
            if listing:
                for m in listing:
                    if _cat_slug(m) == slug:
                        market = m
                        break
    finally:
        if owned_client:
            client.close()

    if market is None:
        return _open()
    if not _is_market_resolved(market):
        return _open()
    won_up = _resolved_won_up(market)
    if won_up is None:
        # Resolved but the winner is not unambiguously mappable — fail closed
        # (row stays PENDING for manual review), NEVER guess.
        logger.warning(
            "predictfun resolve: %s reports resolved but winner unmappable — "
            "leaving PENDING", slug,
        )
        return _open()
    cond = str(market.get("conditionId", "") or "") or None
    return PolymarketResolution(
        status=RESOLUTION_RESOLVED,
        coin=coin,
        interval_min=int(interval_min),
        window_start_ts=int(window_start_ts),
        window_end_ts=window_end_ts,
        condition_id=cond,
        won_up=1 if won_up else 0,
        up_price=1.0 if won_up else 0.0,
        down_price=0.0 if won_up else 1.0,
        source="predictfun",
    )


# ---------------------------------------------------------------------------
# Candle-recompute fallback — DOCUMENTED, DELIBERATELY UNBUILT.
#
# predict.fun's administrator fallback settles from the Binance 1s candle CLOSE
# at window end (window-end price = OPEN of the candle at window end; exact tie
# = 50-50). That candle-based recompute is deliberately NOT implemented: the
# API-outcome path above is authoritative, and computing a settlement price
# from a spot feed risks the Binance-vs-settlement flips that ate a week of
# Polymarket profit (memory: project_deep_research_edge). Until then any market
# whose API outcome never finalizes simply stays PENDING (fail-closed). If the
# API outcome is ever permanently unavailable, implement this against the
# Binance 1s klines and log LOUDLY on every use.
# ---------------------------------------------------------------------------
