"""predict.fun CLOB order-book fetch + order submission.

Book fetch mirrors :mod:`cryptobot_polymarket.clob` and RETURNS THE SAME
``BookSnapshot`` type (imported from there) so ``walk_book`` / the gate stack /
the recorder consume predict.fun books identically to Polymarket books.

Endpoints (2026-07-21 MAINNET probe — the keying open item is RESOLVED)::

    GET {PREDICTFUN_API_BASE}/markets/{marketId}/orderbook
        -> {"success": bool,
            "data": {"asks": [[price,size],...],   # ascending by price
                     "bids": [[price,size],...],   # descending by price
                     "marketId": int,
                     "updateTimestampMs": int,
                     "lastOrderSettled": {...}}}   # ignored here
    GET {PREDICTFUN_API_BASE}/markets/orderbooks?ids=<id>&ids=<id>
        -> {"success": bool, "data": [<orderbook body>, ...]}   # BATCH exists

⚠️ KEYING — CONFIRMED (2026-07-21 mainnet probe + dev.predict.fun/doc-685654).
Polymarket's ``/book`` is keyed by the OUTCOME TOKEN id; predict.fun serves ONE
book per integer MARKET id, quoted from the YES/UP outcome perspective
(indexSet 1). Empirically cross-checked on an asymmetric-spread market
(bnb-updown-15m-1784645100): book best_ask/best_bid == the Up outcome's
bestAsk/bestBid exactly, and Down.bestBid == 1 - best_ask. So:

  * UP reads walk ``data.asks`` directly;
  * DOWN reads are SYNTHESIZED: asks' = complemented bids, bids' =
    complemented asks, complement rounded at the market tick
    (``decimalPrecision`` = 2 on every crypto up-down market probed):
    ``complement = (10^p - round(price * 10^p)) / 10^p``.

The pipeline passes outcome onChainIds (``market.yes_token_id`` /
``no_token_id``); ``fetch_top_book`` / ``fetch_books_batch`` resolve them
through the discovery registry in :mod:`cryptobot_predictfun.markets`
(token -> (market_id, side)). An UNREGISTERED token returns None — fail-closed,
never a guessed market or side. Both tokens of one market share a single
market-book GET (and the batch endpoint serves many markets per request).

AUTH: ``x-api-key: <PREDICTFUN_API_KEY>`` (probe: ``Authorization: Bearer``
401s — Bearer is the /auth JWT used only by trading/account endpoints).

RATE LIMIT: the server budget is 240 requests/min per API key
(``ratelimit-policy 240;w=60``), SHARED by every process using the key (scanner
+ recorder). The local bucket is capped well under that so one process can
never exhaust the key on its own; the server-429 backoff stays as the backstop.

Order submission wraps the official ``predict_sdk`` (``OrderBuilder``) for
EIP-712 signing when it is installed; DRY mode requires NO SDK and NO
credentials (``order_client=None`` -> LIVE cells SKIP, DRY cells continue),
exactly like the Polymarket ``OrderClient`` fail-soft contract. The LIVE
submit path (implemented 2026-07-23 from the dev.predict.fun docs + /spec):
POST /v1/orders with x-api-key + the /auth JWT Bearer
(:mod:`cryptobot_predictfun.auth`) and a signed ContractOrder body; there is
no FAK enum — marketable = strategy MARKET with a ``pricePerShare`` cap
(entry BUY, FAK semantics) or ``isFillOrKill=true`` (stop SELL, FOK).
``strategy`` is ALWAYS MARKET here: LIMIT is structurally refused because no
resting-order management exists in this client. The path must be exercised
end-to-end by ``cryptobot_predictfun.live_validation`` before the scanner
is armed (min size / tick / fill-unit assumptions are pinned there).

250ms taker delay: the Polymarket uncancellable-marketable-order delay has NO
documented predict.fun equivalent as of 2026-07-21 (checked the OpenAPI spec,
dev portal and user docs). Treated as UNKNOWN — do NOT assume orders are
instantly cancellable.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Optional

import httpx

# Reuse the Polymarket adapter's snapshot + limiter types so downstream
# consumers (walk_book, gates, recorder) are venue-agnostic.
# Phase 2: the shared book DTO + rate limiter live in core, not in the
# Polymarket package (importing them from there would be venue -> venue).
from cryptobot.venues.types import BookSnapshot, TokenBucket
from cryptobot.db.connection import utc_now_sql
from cryptobot_predictfun._http import api_base as _api_base
from cryptobot_predictfun._http import auth_headers as _auth_headers

logger = logging.getLogger(__name__)

# Local limiter sized for the venue's 240 req/min per-key budget (probe
# 2026-07-21: ratelimit-policy 240;w=60), which is SHARED across the scanner
# and the recorder processes: 2 req/s steady (=120/min) per process caps the
# worst case at exactly the key budget with both processes saturated. The
# batch orderbooks endpoint keeps steady-state usage far below this (1 batch
# GET per second serves every active market = 60/min).
_default_bucket = TokenBucket(rate_per_s=2.0, burst=6)


class _SharedServer429Backoff:
    """Process-wide 429 cooldown shared by the scanner's decision + prefetch
    book buckets.

    A server 429 is a WHOLE-KEY signal (``ratelimit-policy 240;w=60``, shared
    across the scanner and recorder processes), NOT a per-bucket one: it means
    the key is over budget, so it must pause EVERY scanner book read until the
    rolling window recovers. Pausing only the bucket that happened to draw the
    429 would let the other bucket keep hammering an already-over-budget key
    and risk the IP ban the backoff exists to prevent (review finding #31)."""

    def __init__(self) -> None:
        self._until = 0.0
        self._lock = threading.Lock()

    def blocked(self) -> bool:
        with self._lock:
            return time.monotonic() < self._until

    def trip(self, cooldown_s: float) -> None:
        with self._lock:
            self._until = max(self._until, time.monotonic() + cooldown_s)


class _ScannerBookBucket:
    """A ``TokenBucket`` whose 429 cooldown is a shared, process-wide pause.

    ``_get_json`` calls ``take`` / ``notify_server_429`` on whatever bucket the
    call-site passes; this wrapper splits the scanner's 2 req/s (=120/min, half
    the shared 240/min key) into an independent decision bucket and prefetch
    bucket so a prefetch burst can never starve the inline decision + stop
    reads of tokens — while routing a 429 on EITHER onto ONE shared backoff so
    the whole-key pause still holds. Structurally take/notify_server_429/rate
    compatible with ``TokenBucket`` (extends its usage without editing the
    shared ``polymarket_clob`` class)."""

    def __init__(
        self, *, rate_per_s: float, burst: int, backoff: _SharedServer429Backoff
    ) -> None:
        self._bucket = TokenBucket(rate_per_s=rate_per_s, burst=burst)
        self._backoff = backoff

    @property
    def rate(self) -> float:
        return self._bucket.rate

    @property
    def burst(self) -> int:
        return self._bucket.burst

    def take(self, n: int = 1) -> bool:
        if self._backoff.blocked():
            return False
        return self._bucket.take(n)

    def notify_server_429(self, *, cooldown_s: float = 10.0) -> None:
        self._backoff.trip(cooldown_s)


# Scanner-process book buckets. The scanner's half of the 240/min key (=2 req/s)
# is SPLIT so the A8 prefetch daemon cannot starve inline decision + stop reads:
# a prefetch burst draws only its own 0.6/s (burst 3) allotment, leaving the
# decision bucket's 1.4/s (burst 12) intact for the tick + stop pass. The sum is
# still 2.0/s = 120/min (unchanged scanner budget); the recorder is a SEPARATE
# process that keeps its own _default_bucket (2.0/s) for the other 120/min half.
# Both scanner buckets share ONE 429 backoff so a 429 on either pauses both
# (whole-key semantics). The module-global _default_bucket stays the recorder's
# bucket AND the fail-safe default for any caller that passes no bucket.
_scanner_server_429_backoff = _SharedServer429Backoff()
decision_bucket = _ScannerBookBucket(
    rate_per_s=1.4, burst=12, backoff=_scanner_server_429_backoff
)
prefetch_bucket = _ScannerBookBucket(
    rate_per_s=0.6, burst=3, backoff=_scanner_server_429_backoff
)

# Tick size of every probed crypto up-down market (decimalPrecision=2 => 0.01).
# Used to round the DOWN-side complement exactly as the venue quotes it.
_PRICE_DECIMALS = 2

# Order-signing knobs (LIVE submit path). MARKET orders are expected to be
# terminal ~immediately; the expiration is a venue-side backstop only.
_ORDER_EXPIRY_S = 300
# Terminal-state poll cadence for a just-posted order. MEASURED 2026-07-23: a
# marketable BUY read OPEN at 0.25s and FILLED a second or two later — so poll
# a few times (~2s total) to capture the fill inline before deferring to the
# 60s reconciler. See _await_terminal_state.
_FILL_POLL_DELAY_S = 0.35
_FILL_POLL_MAX = 6
_TERMINAL_ORDER_STATUS = frozenset(
    {"FILLED", "CANCELLED", "CANCELED", "EXPIRED", "INVALIDATED"}
)


class PredictFunClientUnavailable(RuntimeError):
    """Raised when order submission is requested but ``predict_sdk`` is absent."""


def _build_owned_order_client(timeout: float) -> httpx.Client:
    """Owned order-path ``httpx.Client`` with a warm keepalive pool + http2.

    The order POST / stop SELL / JWT auth / order-state GETs all ride this one
    client, kept warm by the ~20s ``ping_order_pool`` job. httpx's default 5s
    ``keepalive_expiry`` silently closed the socket in the ~20s gaps between
    pings, so almost every spaced-out order arrival paid a cold TCP+TLS
    handshake to the Cloudflare edge; ``keepalive_expiry=65.0`` (> the 20s ping
    and the 60s readiness cadence) keeps it hot, and http2 multiplexes the ping
    GET and the order POST over one connection. Fail-soft to HTTP/1.1 when the
    ``h2`` extra is absent — mirrors ``main._mk_client`` — so a box missing the
    optional dep still boots LIVE; an unguarded ImportError here would drop the
    live-money unit to DRY-only (an availability regression)."""
    limits = httpx.Limits(
        max_connections=20, max_keepalive_connections=10, keepalive_expiry=65.0
    )
    try:
        return httpx.Client(timeout=timeout, http2=True, limits=limits)
    except ImportError:
        logger.warning(
            "httpx[http2] extra (h2) not installed — predict.fun order client "
            "falls back to HTTP/1.1"
        )
    return httpx.Client(timeout=timeout, limits=limits)


def _get_json(
    path: str,
    *,
    params=None,
    client: httpx.Client | None,
    timeout: float,
    bucket: TokenBucket,
    label: str,
    read_timeout: httpx.Timeout | None = None,
):
    """Rate-limited authenticated ``GET {api_base}{path}`` -> the ``data`` body.

    One local-bucket draw per call (a batch is ONE request). Returns the
    response envelope's ``data`` field (the body itself when there is no
    envelope), or None on local rate limit / HTTP error / server 429 (with
    backoff) / 401 / status>=400 / JSON parse failure. ``label`` names the
    request in log lines.

    ``read_timeout`` is a PER-REQUEST override for the POOLED-client path (an
    injected ``client``), where ``timeout`` is otherwise dead — it only sizes
    the owned client built when ``client is None``. It MUST be a full
    ``httpx.Timeout`` (connect kept at 2.0s, read/write/pool at the PF book
    knob) rather than a bare float, so the dead-socket connect cap survives.
    Left None the call is byte-identical to today: the pooled client uses its
    own client-level timeout and the owned client uses ``timeout``.
    """
    if not bucket.take(1):
        logger.info("predictfun clob: rate-limited locally; skipping %s", label)
        return None
    base = _api_base()
    headers = _auth_headers()

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    get_kwargs = {} if read_timeout is None else {"timeout": read_timeout}
    try:
        try:
            resp = client.get(
                f"{base}{path}", params=params, headers=headers or None, **get_kwargs
            )
        except httpx.HTTPError as exc:
            logger.info("predictfun clob: http error %r for %s", exc, label)
            return None
        if resp.status_code == 429:
            logger.info("predictfun clob: server 429 for %s — backing off", label)
            bucket.notify_server_429()
            return None
        if resp.status_code == 401:
            logger.warning(
                "predictfun clob: 401 unauthorized — set PREDICTFUN_API_KEY "
                "(x-api-key header)"
            )
            return None
        if resp.status_code >= 400:
            logger.warning(
                "predictfun clob: status=%d for %s", resp.status_code, label
            )
            return None
        try:
            body = resp.json()
        except ValueError:
            return None
    finally:
        if owned_client:
            client.close()

    return body.get("data") if isinstance(body, dict) else body


def _decode_pair_levels(raw) -> tuple[dict[str, float], ...]:
    """Normalize ``[[price, size], ...]`` into a price-ascending tuple of dicts.

    predict.fun sends 2-element ``[price, size]`` arrays (Polymarket sends
    ``{"price","size"}`` objects); tolerate both. Drops non-finite / non-positive
    levels, same rule as ``polymarket_clob._decode_levels``.
    """
    if not isinstance(raw, list):
        return ()
    out: list[dict[str, float]] = []
    for level in raw:
        try:
            if isinstance(level, dict):
                price = float(level.get("price", 0))
                size = float(level.get("size", 0))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = float(level[0])
                size = float(level[1])
            else:
                continue
        except (TypeError, ValueError):
            continue
        if price <= 0 or size <= 0:
            continue
        out.append({"price": price, "size": size})
    out.sort(key=lambda d: d["price"])
    return tuple(out)


def _complement_price(price: float, decimals: int = _PRICE_DECIMALS) -> float:
    """Venue-documented DOWN-side complement, rounded at the market tick.

    ``complement = (10^p - round(price * 10^p)) / 10^p`` with p=decimalPrecision
    (2 on every probed crypto up-down market). Matches the venue's own Down
    quotes exactly (probe cross-check: Down.bestBid == complement(best_ask)).
    """
    scale = 10 ** decimals
    return (scale - round(price * scale)) / scale


def _complement_levels(
    levels: tuple[dict[str, float], ...]
) -> tuple[dict[str, float], ...]:
    """Complement each level's price (sizes unchanged), re-sorted ascending.

    A level whose complement rounds to <= 0 (source price at/over 1.0) is
    dropped — same non-positive-price rule as ``_decode_pair_levels``.
    """
    out: list[dict[str, float]] = []
    for lvl in levels:
        price = _complement_price(lvl["price"])
        if price <= 0:
            continue
        out.append({"price": price, "size": lvl["size"]})
    out.sort(key=lambda d: d["price"])
    return tuple(out)


def _snapshot_from_orderbook(
    data: dict, *, token_id: str, now_ts: float, side: str = "UP"
) -> BookSnapshot | None:
    """Build a ``BookSnapshot`` for one outcome view of a market's book body.

    The venue book IS the UP view (see module keying note); ``side="DOWN"``
    synthesizes the complement view (asks' = complemented bids, bids' =
    complemented asks). ``token_id`` on the snapshot carries whatever id the
    caller keyed the request by (outcome token on the pipeline path, market id
    on the recorder path). ``fetched_at_ts`` is the LOCAL fetch time (same
    caveat as the Polymarket REST path — the payload's ``updateTimestampMs`` is
    a venue book time we do NOT map onto ``fetched_at_ts`` to avoid a false
    staleness signal).
    """
    up_asks = _decode_pair_levels(data.get("asks", []))
    up_bids = _decode_pair_levels(data.get("bids", []))
    if side == "DOWN":
        all_asks = _complement_levels(up_bids)
        all_bids = _complement_levels(up_asks)
    else:
        all_asks = up_asks
        all_bids = up_bids
    if not all_asks:
        return None
    total_ask_depth_usd = float(sum(lvl["price"] * lvl["size"] for lvl in all_asks))
    return BookSnapshot(
        token_id=str(token_id),
        asks=all_asks,
        bids=all_bids,
        fetched_at_ts=now_ts,
        total_ask_depth_usd=total_ask_depth_usd,
    )


def _fetch_orderbook_data(
    market_id: str,
    *,
    client: httpx.Client | None,
    timeout: float,
    bucket: TokenBucket,
    read_timeout: httpx.Timeout | None = None,
) -> Optional[dict]:
    """One market's raw orderbook ``data`` body, or None (rate/HTTP/parse)."""
    data = _get_json(
        f"/markets/{market_id}/orderbook",
        client=client,
        timeout=timeout,
        bucket=bucket,
        label=str(market_id),
        read_timeout=read_timeout,
    )
    return data if isinstance(data, dict) else None


def fetch_book_for_market(
    market_id: str,
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    bucket: TokenBucket | None = None,
    now_ts: float | None = None,
    side: str = "UP",
    read_timeout: httpx.Timeout | None = None,
) -> BookSnapshot | None:
    """Fetch one market's order book, keyed by MARKET id (the recorder path).

    Returns the venue's canonical UP view by default (``side="DOWN"`` returns
    the synthesized complement view). Returns None on rate-limit/HTTP/empty.
    """
    bucket = bucket or _default_bucket
    if now_ts is None:
        now_ts = time.time()
    data = _fetch_orderbook_data(
        market_id, client=client, timeout=timeout, bucket=bucket,
        read_timeout=read_timeout,
    )
    if data is None:
        return None
    return _snapshot_from_orderbook(
        data, token_id=str(market_id), now_ts=now_ts, side=side
    )


def _resolve_token(token_id: str) -> Optional[tuple[str, str]]:
    """Outcome onChainId -> (market_id, 'UP'|'DOWN') via the discovery registry.

    Unknown token -> None (fail-closed: the pipeline registers every market's
    tokens at discovery time in the SAME tick, so a miss means we genuinely do
    not know which market/side this token is — never guess).
    """
    from cryptobot_predictfun import markets as pf_markets

    resolved = pf_markets.lookup_token_market(str(token_id))
    if resolved is None:
        logger.warning(
            "predictfun clob: outcome token %s has no registered market mapping "
            "— returning no book (fail-closed)", token_id,
        )
    return resolved


def fetch_top_book(
    token_id: str,
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    bucket: TokenBucket | None = None,
    now_ts: float | None = None,
    read_timeout: httpx.Timeout | None = None,
) -> BookSnapshot | None:
    """Signature-compatible with ``polymarket_clob.fetch_top_book``.

    ``token_id`` is the OUTCOME onChainId the pipeline carries
    (``market.yes_token_id`` / ``no_token_id``); it is resolved to
    (market_id, side) via the discovery registry, the market's single book is
    fetched, and the side view is returned (UP as-is; DOWN complemented —
    keying confirmed by the 2026-07-21 mainnet probe, see the module note).
    """
    resolved = _resolve_token(token_id)
    if resolved is None:
        return None
    market_id, side = resolved
    bucket = bucket or _default_bucket
    if now_ts is None:
        now_ts = time.time()
    data = _fetch_orderbook_data(
        market_id, client=client, timeout=timeout, bucket=bucket,
        read_timeout=read_timeout,
    )
    if data is None:
        return None
    return _snapshot_from_orderbook(
        data, token_id=str(token_id), now_ts=now_ts, side=side
    )


def _fetch_orderbooks_batch_raw(
    market_ids: list[str],
    *,
    client: httpx.Client | None,
    timeout: float,
    bucket: TokenBucket,
    read_timeout: httpx.Timeout | None = None,
) -> Optional[dict[str, dict]]:
    """Batch GET /markets/orderbooks?ids=..&ids=.. -> {market_id: data body}.

    One rate-limit draw for the whole batch (it is one HTTP request). Returns
    None on rate/HTTP/parse failure so callers can fall back per-market.
    """
    if not market_ids:
        return {}
    data = _get_json(
        "/markets/orderbooks",
        params=[("ids", mid) for mid in market_ids],
        client=client,
        timeout=timeout,
        bucket=bucket,
        label="batch",
        read_timeout=read_timeout,
    )
    if not isinstance(data, list):
        return None
    out: dict[str, dict] = {}
    for ob in data:
        if isinstance(ob, dict) and ob.get("marketId") is not None:
            out[str(ob["marketId"])] = ob
    return out


def fetch_market_books_batch(
    market_ids: list[str] | tuple[str, ...],
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    bucket: TokenBucket | None = None,
    now_ts: float | None = None,
    read_timeout: httpx.Timeout | None = None,
) -> dict[str, BookSnapshot] | None:
    """Batch book fetch keyed by MARKET id (UP view) — the recorder path.

    One batch GET serves every active market (the per-second recorder poll was
    3 coins x 2 sleeves = up to 12 GETs/s before this; the 240/min key budget
    requires the batch). Returns ``{market_id: BookSnapshot}`` for markets with
    a non-empty ask book, or None on batch failure (caller falls back
    per-market).
    """
    ids = [str(m) for m in market_ids if m]
    if not ids:
        return {}
    bucket = bucket or _default_bucket
    if now_ts is None:
        now_ts = time.time()
    raw = _fetch_orderbooks_batch_raw(
        ids, client=client, timeout=timeout, bucket=bucket, read_timeout=read_timeout
    )
    if raw is None:
        return None
    out: dict[str, BookSnapshot] = {}
    for mid in ids:
        data = raw.get(mid)
        if data is None:
            continue
        snap = _snapshot_from_orderbook(
            data, token_id=mid, now_ts=float(now_ts), side="UP"
        )
        if snap is not None:
            out[mid] = snap
    return out


def fetch_books_batch(
    token_ids: list[str] | tuple[str, ...],
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    bucket: TokenBucket | None = None,
    now_ts: float | None = None,
    read_timeout: httpx.Timeout | None = None,
) -> dict[str, BookSnapshot] | None:
    """Signature-compatible with ``polymarket_clob.fetch_books_batch``.

    ``token_ids`` are OUTCOME onChainIds (the pipeline passes both sides of
    every market). Tokens are resolved to (market_id, side) via the discovery
    registry — both sides of a market share ONE market book, and the batch
    endpoint (``GET /markets/orderbooks``) serves every market in ONE request,
    so a full tick's book pass costs a single rate-limit draw.

    Returns ``{token_id: BookSnapshot}`` for tokens whose side view has a
    non-empty ask ladder (absent tokens simply missing — same semantics as
    Polymarket; the pipeline falls back to per-token GETs for missing ids).
    Unregistered tokens are skipped (fail-closed). Returns None only when the
    batch request itself failed.
    """
    ids = [str(t) for t in token_ids if t]
    if not ids:
        return {}
    resolved: dict[str, tuple[str, str]] = {}
    for t in ids:
        r = _resolve_token(t)
        if r is not None:
            resolved[t] = r
    if not resolved:
        return {}
    if now_ts is None:
        now_ts = time.time()
    bucket = bucket or _default_bucket
    market_ids: list[str] = []
    for mid, _side in resolved.values():
        if mid not in market_ids:
            market_ids.append(mid)
    raw = _fetch_orderbooks_batch_raw(
        market_ids, client=client, timeout=timeout, bucket=bucket,
        read_timeout=read_timeout,
    )
    if raw is None:
        return None
    out: dict[str, BookSnapshot] = {}
    for token, (mid, side) in resolved.items():
        data = raw.get(mid)
        if data is None:
            continue
        snap = _snapshot_from_orderbook(
            data, token_id=token, now_ts=float(now_ts), side=side
        )
        if snap is not None:
            out[token] = snap
    return out


# ---------------------------------------------------------------------------
# Order submission — EIP-712 signing via predict_sdk (optional dependency).
# ---------------------------------------------------------------------------
class PredictFunOrderClient:
    """Marketable order client for predict.fun (FAK entry BUYs + FOK stop SELLs).

    Mirrors the ``place_gtc_limit_order(signal, *, client_order_id) ->
    OrderResult`` + ``place_stop_loss_sell(...)`` interface of
    ``execution.walker.OrderClient`` so the pipeline's LIVE routing (entries,
    T3 top-ups, fast-requote, stop-loss pass) is venue-agnostic. Construction
    requires ``predict_sdk`` AND a wallet private key + API key; DRY mode
    never constructs this (``order_client=None``).

    Wire (dev.predict.fun docs + ``/spec``, fetched 2026-07-23)::

        POST /v1/orders   (x-api-key + Authorization: Bearer <JWT from /auth>)
          {"data": {"pricePerShare": <wei str>, "strategy": "MARKET",
                    "slippageBps": "0", "isFillOrKill": <bool>,
                    "isPostOnly": false, "order": {<signed ContractOrder,
                    camelCase>}}}
          -> 201 {"success": true, "data": {"code", "orderId", "orderHash"}}
        GET /v1/orders/{orderHash}
          -> data {"status": OPEN|FILLED|EXPIRED|CANCELLED|INVALIDATED,
                   "amount", "amountFilled", "order": {...}}

    ``strategy`` is ALWAYS "MARKET" (a MARKET order takes book depth up to the
    ``pricePerShare`` cap and never rests). "LIMIT" is structurally refused —
    nothing here manages resting orders, so no order should ever need
    cancelling on the tick path (``cancel_order`` exists only for
    OrderClient-interface parity / pair-unwind, never a hot path). Entry BUY
    posts ``isFillOrKill=false`` (FAK semantics,
    mirrors the Polymarket FAK entry); stop SELL posts ``isFillOrKill=true``
    (atomic full fill or kill, mirrors the walker's FOK stop so a stopped
    position is never split between a realized sell and settlement).

    Predict accounts (Kernel smart wallets — every normal predict.fun UI
    account): pass ``predict_account`` = the smart-account address that
    custodies the collateral. The SDK then builds maker=<predict account> /
    signer=<key's EOA> orders and /auth runs the predict-account signature
    flow. Without it, maker == signer == the key's EOA (external-wallet
    accounts).
    """

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        private_key: str,
        chain_id: Optional[int] = None,
        predict_account: Optional[str] = None,
        client: httpx.Client | None = None,
        timeout: float = 5.0,
    ) -> None:
        if not private_key:
            raise PredictFunClientUnavailable("PREDICTFUN_PRIVATE_KEY empty")
        try:
            import predict_sdk  # noqa: F401
            from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
        except ImportError as exc:  # graceful — LIVE cells SKIP, DRY continues
            raise PredictFunClientUnavailable(
                "predict_sdk not installed — `pip install predict_sdk` to enable "
                "predict.fun LIVE order submission (DRY mode needs no SDK)"
            ) from exc

        self._api_base = api_base.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._owns_client = client is None
        self._client = (
            client if client is not None else _build_owned_order_client(timeout)
        )
        # Collateral-balance fetcher, built lazily ONCE on first check_balance
        # (the closure re-reads config on every call, so memoizing the build
        # changes nothing except the per-refresh rebuild cost).
        self._collateral_fetch = None
        # predictWalletEvents fill-push waiter: injected by main.py via
        # set_fill_event_waiter ONLY when CRYPTOBOT_PREDICTFUN_WS_WALLET_EVENTS
        # is ON and the PF book WS mirror is running. None (the default) keeps
        # _await_terminal_state byte-identical to the poll-only build — no WS
        # object is ever constructed on this path.
        self._fill_event_waiter = None
        self._fill_event_wait_s = 0.0
        chain = chain_id if chain_id is not None else int(getattr(ChainId, "BNB_MAINNET", 56))
        self._predict_account = (predict_account or "").strip() or None
        # OrderBuilder.make(chain_id, signer[, options]) — signer is the raw
        # private key; predict_account routes maker/approvals/redeems through
        # the Kernel smart account per the SDK README.
        if self._predict_account:
            self._builder = OrderBuilder.make(
                chain,
                private_key,
                OrderBuilderOptions(predict_account=self._predict_account),
            )
        else:
            self._builder = OrderBuilder.make(chain, private_key)

        from cryptobot_predictfun.wallet import derive_signer_address

        eoa = derive_signer_address(private_key)
        if not eoa:
            raise PredictFunClientUnavailable(
                "cannot derive the signer EOA from PREDICTFUN_PRIVATE_KEY"
            )
        self._signer_eoa = eoa

        # /auth signer + signature flow (venue docs): predict accounts POST
        # signer=<PREDICT ACCOUNT> with a Kernel-validated signature; plain
        # EOA accounts personal_sign with the key's EOA. The signing closure
        # captures the key so it is never stored on the instance.
        if self._predict_account:
            auth_signer = self._predict_account

            def _sign_auth(message: str) -> str:
                return str(self._builder.sign_predict_account_message(message))

        else:
            auth_signer = eoa

            def _sign_auth(message: str) -> str:
                from eth_account import Account
                from eth_account.messages import encode_defunct

                signed = Account.sign_message(
                    encode_defunct(text=message), private_key=private_key
                )
                sig = signed.signature.hex()
                return sig if sig.startswith("0x") else f"0x{sig}"

        from cryptobot_predictfun.auth import JwtSession

        self._jwt = JwtSession(
            api_base=self._api_base,
            api_key=self._api_key,
            signer_address=auth_signer,
            sign_message=_sign_auth,
            client=self._client,
            timeout=timeout,
        )
        logger.info(
            "PredictFunOrderClient initialized (chain_id=%s, predict_account=%s, "
            "signer_eoa=%s)",
            chain,
            self._predict_account or "<none: EOA is maker>",
            eoa[:10],
        )

    def close(self) -> None:
        if self._owns_client:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass

    def check_balance(self):
        """Collateral (USDT/BSC-USD) balance in USD, or None (fail-soft).

        Mirrors ``execution.walker.OrderClient.check_balance`` so the tick's
        BalanceCache and live-readiness are venue-agnostic. Reads the configured
        wallet's on-chain collateral via ``predictfun_wallet``; any failure
        (unconfigured wallet, unresolved token, unreachable RPC) returns None,
        which the readiness gate treats as clob_unreachable (fail-closed, never
        a fabricated balance).
        """
        try:
            if self._collateral_fetch is None:
                from cryptobot_predictfun.wallet import (
                    build_collateral_balance_fetcher,
                )

                self._collateral_fetch = build_collateral_balance_fetcher()
            return self._collateral_fetch()
        except Exception:  # noqa: BLE001 — degrade to None, never crash the tick
            logger.warning("predictfun check_balance failed", exc_info=True)
            return None

    def ping_order_pool(self) -> None:
        """Keep the HTTPS pool warm between orders (the Polymarket
        OrderClient hook main's clob_ping job calls every ~20s). A minimal
        markets GET doubles as an API-key liveness canary; ~3 req/min at the
        default cadence — well inside the shared 240/min key budget."""
        try:
            self._client.get(
                f"{self._api_base}/markets",
                params={"first": 1},
                headers={"x-api-key": self._api_key} if self._api_key else None,
                timeout=2.5,
            )
        except httpx.HTTPError:
            pass  # warmth only — never raise into the scheduler

    def peek_wallet_jwt(self) -> Optional[str]:
        """The order path's current cached /auth JWT (None when absent or
        expired) WITHOUT triggering a fetch. The WS wallet-events subscriber
        builds its ``predictWalletEvents/{jwt}`` topic from this — it rides
        the credential the order path already holds and must never spend an
        /auth round trip of its own."""
        return self._jwt.peek_token()

    def set_jwt_rotation_hook(self, callback) -> None:
        """Fire ``callback`` after every JWT change (refresh AND invalidate)
        so the WS wallet-events topic resubscribes with the rotated credential
        instead of streaming into a dead subscription."""
        self._jwt.set_on_change(callback)

    def set_fill_event_waiter(self, waiter, *, wait_ms: int) -> None:
        """Inject the WS fill-push waiter (duck-typed
        ``await_fill_state(order_hash, timeout_s) -> Optional[dict]``).

        ``_await_terminal_state`` then asks the pushed wallet event FIRST and
        runs the UNCHANGED GET poll only when WS yields nothing. The waiter
        may only ever shortcut the wait with a poll-shaped ``{status,
        amountFilled}`` dict — status + executed SHARES at most; fill VWAP
        booking stays at the cap price regardless of any WS payload field."""
        self._fill_event_waiter = waiter
        self._fill_event_wait_s = max(0.0, float(wait_ms) / 1000.0)

    def cancel_order(self, order_id: str) -> bool:
        """Best-effort cancel by orderHash (DELETE /v1/orders/{hash}).

        Interface parity with the Polymarket client (the pair-leg unwind
        path), NOT a hot path here: this client posts only MARKET orders,
        which never rest on the book.
        """
        oid = str(order_id or "").strip()
        if not oid:
            return False
        headers, err = self._headers_with_bearer()
        if headers is None:
            logger.warning("predictfun cancel: auth failed (%s)", err)
            return False
        try:
            resp = self._client.delete(
                f"{self._api_base}/orders/{oid}",
                headers=headers,
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            logger.warning("predictfun cancel: %r", exc)
            return False
        return resp.status_code in (200, 201, 204)

    # ------------------------------------------------------------------
    # Signing + wire helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _price_wei(price: float) -> int:
        """Exact wei price at the venue tick (decimalPrecision=2) — no float dust."""
        ticks = int(round(float(price) * 10**_PRICE_DECIMALS))
        return ticks * 10 ** (18 - _PRICE_DECIMALS)

    @staticmethod
    def _expected_fill_price(signal, cap: float) -> float:
        """Price to SIZE a BUY's share quantity at — the expected fill, not the cap.

        ``walked_vwap`` is the walker's multi-level VWAP for the stake (the
        honest expectation); ``fill_price`` (= top_ask at scan) is the fallback
        for signals that never walked a book. Clamped to the signed cap: an
        expectation above the cap can only under-fill (the old behaviour), so
        it degrades to the cap rather than shrinking the order further.
        """
        for attr in ("walked_vwap", "fill_price"):
            raw = getattr(signal, attr, None)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value > 0.0:
                return min(value, cap)
        return cap

    def _build_signed(
        self,
        *,
        side,
        token_id: str,
        price_wei: int,
        quantity_wei: int,
        fee_rate_bps: float,
        is_neg_risk: bool,
        is_yield_bearing: bool,
    ):
        """Amounts -> build -> EIP-712 sign; returns the SDK ``SignedOrder``.

        Amounts come from ``get_limit_order_amounts`` AT THE CAP PRICE (the
        walker's executable limit): "offer maker for >= taker". The wire
        strategy is still MARKET — the venue fills marketable depth up to
        ``pricePerShare`` and never rests the remainder.
        """
        from predict_sdk import BuildOrderInput, LimitHelperInput

        amounts = self._builder.get_limit_order_amounts(
            LimitHelperInput(
                side=side,
                price_per_share_wei=int(price_wei),
                quantity_wei=int(quantity_wei),
            )
        )
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=_ORDER_EXPIRY_S)
        order = self._builder.build_order(
            "MARKET",
            BuildOrderInput(
                side=side,
                token_id=str(token_id),
                maker_amount=str(amounts.maker_amount),
                taker_amount=str(amounts.taker_amount),
                fee_rate_bps=str(int(round(float(fee_rate_bps)))),
                expires_at=expires_at,
            ),
        )
        typed = self._builder.build_typed_data(
            order,
            is_neg_risk=bool(is_neg_risk),
            is_yield_bearing=bool(is_yield_bearing),
        )
        return self._builder.sign_typed_data_order(typed)

    @staticmethod
    def _order_wire(signed) -> dict:
        """SDK ``SignedOrder`` (snake_case) -> REST order object (camelCase).

        Field set + casing per the create-order OpenAPI: everything is an
        integer STRING except ``expiration`` (int), ``side`` /
        ``signatureType`` (ints).
        """
        # predict_sdk 0.0.20 returns the raw hex signature WITHOUT the 0x
        # prefix (observed in the unit tests) — normalize to the canonical
        # 0x-prefixed form the REST examples use.
        sig = str(signed.signature)
        wire = {
            "salt": str(signed.salt),
            "maker": str(signed.maker),
            "signer": str(signed.signer),
            "taker": str(signed.taker),
            "tokenId": str(signed.token_id),
            "makerAmount": str(signed.maker_amount),
            "takerAmount": str(signed.taker_amount),
            "expiration": int(signed.expiration),
            "nonce": str(signed.nonce),
            "feeRateBps": str(signed.fee_rate_bps),
            "side": int(signed.side),
            "signatureType": int(signed.signature_type),
            "signature": sig if sig.startswith("0x") else f"0x{sig}",
        }
        order_hash = getattr(signed, "hash", None)
        if order_hash:
            oh = str(order_hash)
            wire["hash"] = oh if oh.startswith("0x") else f"0x{oh}"
        return wire

    @staticmethod
    def _classify_reject(status_code: Optional[int], body_text: str) -> str:
        """HTTP reject -> the pipeline's coarse ``error_kind`` vocabulary.

        Same bucket names as ``walker._classify_clob_error`` (the pipeline's
        retry policy + venue-degradation alert key off them); classification
        is status-first because predict.fun rejects carry clean HTTP codes.
        """
        text = (body_text or "").lower()
        if status_code in (401, 403):
            return "auth"
        if status_code == 429 or (status_code is not None and status_code >= 500):
            # 429 = the shared 240/min key budget is exhausted — that IS a
            # venue-level degradation for the order path (alert-worthy).
            return "venue_unavailable"
        if "insufficient" in text or "not enough" in text:
            return "insufficient_funds"
        if "closed" in text or "resolved" in text or "expired" in text:
            return "market_closed"
        if (
            "liquidity" in text
            or "no match" in text
            or "unfillable" in text
            or "fill or kill" in text
        ):
            return "no_liquidity"
        if (
            "amount" in text
            or "precision" in text
            or "minimum" in text
            # predict.fun min-order-value reject (measured 2026-07-23: the venue
            # floor is 0.90 USD — "create_order_min_order_value_not_met, order
            # must have a value of at least 0.9 USD"). Terminal like the other
            # size rejects: a re-send of the same sub-min size just re-fails, so
            # classify invalid_amount => the cell SKIPs instead of retrying.
            or "min_order" in text
            or "value_not_met" in text
            or "at least" in text
            or "tick" in text
            or "decimals" in text
        ):
            return "invalid_amount"
        return "unknown"

    def _headers_with_bearer(self):
        """``(headers, None)`` with x-api-key + Bearer, or ``(None, err_str)``."""
        from cryptobot_predictfun.auth import PredictFunAuthError

        try:
            bearer = self._jwt.bearer_header()
        except PredictFunAuthError as exc:
            return None, str(exc)
        headers = dict(bearer)
        if self._api_key:
            headers["x-api-key"] = self._api_key
        return headers, None

    def _post_order(
        self,
        *,
        signed,
        price_wei: int,
        is_fill_or_kill: bool,
        client_order_id: str,
        label: str,
        sign_ms: float,
    ):
        """POST /v1/orders with one 401 -> re-auth retry.

        Returns ``(order_hash, error_result, post_ms)`` — exactly one of
        ``order_hash`` / ``error_result`` is set; error results already carry
        the latency fields.
        """
        from cryptobot.execution.types import OrderResult

        payload = {
            "data": {
                "pricePerShare": str(int(price_wei)),
                "strategy": "MARKET",
                # slippageBps=0 + isPostOnly=false sent EXPLICITLY (the docs'
                # create-order body carries both; omitting an expected field
                # risks a 400 on submit). Zero tolerance is correct FAK-at-cap
                # semantics: the signed makerAmount/takerAmount already encode
                # the executable cap, so the venue fills book depth at/below
                # pricePerShare and never pays past it. isPostOnly=false = we
                # are ALWAYS a taker here (a post-only MARKET is a contradiction).
                "slippageBps": "0",
                "isFillOrKill": bool(is_fill_or_kill),
                "isPostOnly": False,
                "order": self._order_wire(signed),
            }
        }
        t0 = time.monotonic()

        def _err(msg: str, kind: str) -> "OrderResult":
            return OrderResult(
                success=False,
                error=msg,
                error_kind=kind,
                client_order_id=client_order_id,
                sign_ms=sign_ms,
                post_ms=(time.monotonic() - t0) * 1000.0,
            )

        for attempt in (0, 1):
            headers, auth_err = self._headers_with_bearer()
            if headers is None:
                return None, _err(f"predictfun auth failed: {auth_err}", "auth"), 0.0
            try:
                resp = self._client.post(
                    f"{self._api_base}/orders",
                    json=payload,
                    headers=headers,
                    timeout=self._timeout,
                )
            except httpx.TimeoutException as exc:
                return None, _err(f"predictfun {label} POST timeout: {exc!r}", "network"), 0.0
            except httpx.HTTPError as exc:
                return None, _err(f"predictfun {label} POST error: {exc!r}", "network"), 0.0
            if resp.status_code == 401 and attempt == 0:
                logger.info(
                    "predictfun %s: 401 — refreshing JWT and retrying once", label
                )
                self._jwt.invalidate()
                continue
            post_ms = (time.monotonic() - t0) * 1000.0
            if resp.status_code in (200, 201):
                try:
                    body = resp.json()
                except ValueError:
                    body = None
                data = body.get("data") if isinstance(body, dict) else None
                order_hash = data.get("orderHash") if isinstance(data, dict) else None
                if order_hash:
                    code = data.get("code")
                    if code:
                        logger.info(
                            "predictfun %s accepted: code=%s hash=%s",
                            label, code, str(order_hash)[:18],
                        )
                    return str(order_hash), None, post_ms
                return None, _err(
                    f"predictfun {label}: 2xx without orderHash: {str(body)[:200]}",
                    "unknown",
                ), post_ms
            text = resp.text[:300]
            kind = self._classify_reject(resp.status_code, text)
            return None, _err(
                f"predictfun {label} HTTP {resp.status_code}: {text}", kind
            ), post_ms
        return None, _err("predictfun auth: still 401 after JWT refresh", "auth"), 0.0

    def _fetch_order_state(self, order_hash: str) -> Optional[dict]:
        """GET /v1/orders/{hash} -> the ``data`` dict, or None (fail-soft)."""
        headers, err = self._headers_with_bearer()
        if headers is None:
            logger.warning("predictfun order state: auth failed (%s)", err)
            return None
        try:
            resp = self._client.get(
                f"{self._api_base}/orders/{order_hash}",
                headers=headers,
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            logger.warning("predictfun order state: %r", exc)
            return None
        if resp.status_code != 200:
            logger.warning(
                "predictfun order state: HTTP %d for %s",
                resp.status_code,
                order_hash[:18],
            )
            return None
        try:
            body = resp.json()
        except ValueError:
            return None
        data = body.get("data") if isinstance(body, dict) else None
        return data if isinstance(data, dict) else None

    def _await_terminal_state(self, order_hash: str) -> Optional[dict]:
        """WS-event-first wait, then poll GET /orders/{hash} up to ~2s.

        With a fill-event waiter injected (CRYPTOBOT_PREDICTFUN_WS_WALLET_
        EVENTS) the pushed ``predictWalletEvents`` terminal state is awaited
        FIRST — the common fast fill then resolves with ZERO GETs — and ANY
        WS gap/timeout/ambiguity falls through to the poll below, which is
        byte-identical to the pre-WS build (same budget, same cadence). The
        WS state is poll-shaped ``{status, amountFilled}``, so every caller
        branch (cap-price partial booking, kill mapping, unreadable-state
        asymmetry) is consumed unchanged.

        MEASURED 2026-07-23: a real marketable BUY read ``OPEN`` at 0.25s and
        reached ``FILLED`` a second or two later. A single short re-poll
        therefore returned success-WITHOUT-metadata on a fill that DID happen,
        deferring the whole size to the 60s reconciler and leaving the stop
        pass blind to the fresh position until then (and making a real SELL
        fill misreport as a kill). Poll a handful of times so the common fast
        fill is captured inline; the reconciler stays the backstop when the
        venue is genuinely slow."""
        waiter = self._fill_event_waiter
        if waiter is not None:
            try:
                state = waiter.await_fill_state(
                    order_hash, self._fill_event_wait_s
                )
            except Exception:  # noqa: BLE001 — WS is an accelerator, never a gate
                logger.warning(
                    "predictfun fill-event wait raised — falling back to the "
                    "GET poll",
                    exc_info=True,
                )
                state = None
            if state is not None:
                return state
        state = None
        for i in range(_FILL_POLL_MAX):
            state = self._fetch_order_state(order_hash)
            status = str(state.get("status", "")).upper() if state else ""
            if status in _TERMINAL_ORDER_STATUS:
                return state
            # Skip the inter-poll spacing after the FINAL fetch — it is dead
            # wait before returning. The fetch COUNT and cadence are unchanged
            # (all _FILL_POLL_MAX reads still fire), so the returned state is
            # identical to the old loop, just ~0.35s sooner on the all-non-
            # terminal tail (the reconciler-deferred fill-miss path).
            if i < _FILL_POLL_MAX - 1:
                time.sleep(_FILL_POLL_DELAY_S)
        return state

    @staticmethod
    def _filled_shares(state: dict) -> Optional[float]:
        """``amountFilled`` (wei string) -> shares (float), or None unreadable.

        The OpenAPI describes ``amount``/``amountFilled`` only as "the order's
        currency"; shares-wei is the expected unit for our BUY/SELL orders.
        ⚠️ This unit assumption is PINNED by the validation CLI's first real
        order (cross-checked against the on-chain CTF balance delta) before
        the scanner is ever armed.
        """
        raw = state.get("amountFilled")
        try:
            filled_wei = int(str(raw))
        except (TypeError, ValueError):
            return None
        if filled_wei < 0:
            return None
        return filled_wei / 1e18

    def fetch_order_payload(self, order_id: str) -> Optional[dict]:
        """Reconciler hook (persistence.reconciliation D-LIVE-17): map this
        venue's order state onto the payload shape
        ``_read_clob_order_payload`` understands. Closes the rare
        posted-but-state-unreadable gap on a later 60s sweep (fill price
        falls back to the row's intent price there — same conservative cap
        booking as the inline path)."""
        state = self._fetch_order_state(str(order_id))
        if state is None:
            return None
        return {
            "status": str(state.get("status", "UNKNOWN")).upper(),
            "size_matched": self._filled_shares(state),
        }

    @staticmethod
    def _state_price(state: dict) -> Optional[float]:
        """The order's own executable price from a GET /orders/{hash} state.

        ``pricePerShare`` is 18-dec wei; the nested signed ``order`` carries the
        same number as makerAmount/takerAmount (BUY: collateral-wei over
        shares-wei). Either way this is the CAP — a marketable BUY only matches
        at or below it — so booking it errs pessimistic, exactly as the inline
        ``place_gtc_limit_order`` read-back does.
        """
        raw = state.get("pricePerShare")
        if raw is not None:
            try:
                price = int(str(raw)) / 1e18
            except (TypeError, ValueError):
                price = None
            if price is not None and 0.0 < price <= 1.0:
                return price
        order = state.get("order")
        if isinstance(order, dict):
            try:
                maker = int(str(order.get("makerAmount")))
                taker = int(str(order.get("takerAmount")))
            except (TypeError, ValueError):
                return None
            if taker > 0:
                price = maker / taker
                if 0.0 < price <= 1.0:
                    return price
        return None

    def fetch_order_fill(
        self,
        order_id: str,
        *,
        intent_amount_usd: Optional[float] = None,
        intent_price: Optional[float] = None,
    ) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Immediate GET of a just-posted order to read its realized fill.

        Same contract as the Polymarket client's method — the pipeline's
        ``_immediate_fill_for_stop`` calls it AFTER the order posted (off the
        decision→fill critical path) whenever the placement returned no fill,
        so the PURE_STOP_04 pass can see the fresh position within THIS tick
        instead of waiting for the 60s reconciler. Returns
        ``(filled_size_tokens, filled_price_vwap, fill_ts)``; the pipeline
        needs BOTH a positive size and a price, and any failure/no-info
        returns ``(None, None, None)`` — this must never raise into the live
        loop.

        Price: predict.fun's order state reports SHARES only (no filled
        notional, no VWAP), so the price is the order's own cap — the same
        pessimistic booking the inline read-back uses. ``intent_amount_usd``
        is accepted for interface parity but deliberately NOT used to imply a
        price: shares are sized at the expected fill, so the intended stake is
        not the cash actually spent and the implied price would understate
        cost. ``intent_price`` (the ledger row's cap) is the last resort when
        the state carries no price of its own.
        """
        if not order_id:
            return None, None, None
        try:
            state = self._fetch_order_state(str(order_id))
        except Exception:  # noqa: BLE001 — best effort; reconciler is the backstop
            logger.debug(
                "fetch_order_fill: order state GET raised for %s",
                order_id,
                exc_info=True,
            )
            return None, None, None
        if not isinstance(state, dict):
            return None, None, None
        filled = self._filled_shares(state)
        if filled is None or filled <= 0:
            return None, None, None
        price = self._state_price(state)
        if price is None and intent_price is not None:
            try:
                candidate = float(intent_price)
            except (TypeError, ValueError):
                candidate = float("nan")
            if math.isfinite(candidate) and 0.0 < candidate <= 1.0:
                price = candidate
        if price is None:
            return None, None, None
        return filled, price, utc_now_sql()

    # ------------------------------------------------------------------
    # LIVE order paths
    # ------------------------------------------------------------------
    def place_gtc_limit_order(self, signal, *, client_order_id: str):
        """Place a marketable BUY for ``signal`` on predict.fun (FAK semantics).

        NOTE: the method name is retained for caller/schema compatibility
        (same rationale as the Polymarket walker): the order is strategy
        MARKET with ``pricePerShare`` = the walker's executable limit cap and
        ``isFillOrKill=false`` — fill marketable depth at/below the cap, the
        venue kills the remainder, nothing ever rests.

        The realized fill is read back from GET /orders/{orderHash}
        immediately after the POST (no reconciler exists for this venue, so
        this client resolves its own terminal state). ``filled_price_vwap``
        is booked AT THE CAP — a taker fill can only execute at/below it, so
        booking errs pessimistic until per-fill match events are wired.

        The caller MUST reserve a ``live_order_attempts`` row with this
        ``client_order_id`` BEFORE invoking (same contract as the walker).
        """
        from predict_sdk import Side

        from cryptobot.execution.types import OrderResult

        if not client_order_id:
            raise ValueError(
                "place_gtc_limit_order requires a client_order_id; reserve a "
                "live_order_attempts row first"
            )

        limit_price = getattr(signal, "limit_price", None)
        order_price = float(
            limit_price if limit_price is not None else signal.fill_price
        )
        if not (math.isfinite(order_price) and 0.0 < order_price < 1.0):
            return OrderResult(
                success=False,
                error="order price out of (0,1)",
                error_kind="unknown",
                client_order_id=client_order_id,
            )
        try:
            order_amount = float(signal.bet_size)
        except (TypeError, ValueError):
            order_amount = float("nan")
        if not (math.isfinite(order_amount) and order_amount > 0):
            return OrderResult(
                success=False,
                error="order_amount <= 0",
                error_kind="unknown",
                client_order_id=client_order_id,
            )
        # Quantize the cash stake DOWN to cents — never round a bet UP past
        # the walked stake (venue min size / precision re-checked by the
        # validation CLI; predict.fun collateral is 18-dec so cents is a
        # policy floor, not a venue one).
        order_amount = float(
            Decimal(str(order_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        )
        if order_amount <= 0:
            return OrderResult(
                success=False,
                error="order_amount rounds below $0.01",
                error_kind="invalid_amount",
                client_order_id=client_order_id,
            )

        token_id = str(signal.poly_token_id)
        from cryptobot_predictfun.markets import lookup_token_order_meta

        meta = lookup_token_order_meta(token_id)
        if meta is None:
            # Fail-closed: never guess fee / EIP-712 domain flags for a token
            # discovery has not registered in this process.
            return OrderResult(
                success=False,
                error="token not in discovery registry — refusing to sign",
                error_kind="unknown",
                client_order_id=client_order_id,
            )
        sig_fee = getattr(signal, "fee_rate_bps", None)
        fee_bps = float(sig_fee) if sig_fee is not None else float(meta.fee_rate_bps)

        price_wei = self._price_wei(order_price)
        # shares = usd / EXPECTED FILL (floored; Decimal so the signed amount
        # carries no float dust). The signed price stays the padded cap so the
        # order still crosses the whole walked depth, but the SIZE must come
        # off the price we expect to pay: a MARKET order matches at the resting
        # ask, so sizing at the cap deployed stake*ask/cap — ~1.5% short on
        # every bet at a 1c pad. Worst case (book moves, every share fills AT
        # the cap) the order overspends the stake by that same ~1.5%; that is
        # the accepted side of the trade. Sizing at a LOWER price only ever
        # raises the notional, so the venue's $0.90 order minimum stays met.
        size_price = self._expected_fill_price(signal, order_price)
        shares_wei = int(
            (Decimal(str(order_amount)) * 10**18 / Decimal(str(size_price)))
            .to_integral_value(rounding=ROUND_DOWN)
        )
        if shares_wei <= 0:
            return OrderResult(
                success=False,
                error="share quantity rounds to zero",
                error_kind="invalid_amount",
                client_order_id=client_order_id,
            )

        t_sign = time.monotonic()
        try:
            signed = self._build_signed(
                side=Side.BUY,
                token_id=token_id,
                price_wei=price_wei,
                quantity_wei=shares_wei,
                fee_rate_bps=fee_bps,
                is_neg_risk=meta.is_neg_risk,
                is_yield_bearing=meta.is_yield_bearing,
            )
        except Exception as exc:  # noqa: BLE001 — SDK build/sign -> clean reject
            logger.exception("predictfun buy: build/sign failed")
            return OrderResult(
                success=False,
                error=f"build/sign failed: {exc!r}",
                error_kind="unknown",
                client_order_id=client_order_id,
            )
        sign_ms = (time.monotonic() - t_sign) * 1000.0

        order_hash, err, post_ms = self._post_order(
            signed=signed,
            price_wei=price_wei,
            is_fill_or_kill=False,
            client_order_id=client_order_id,
            label="buy",
            sign_ms=sign_ms,
        )
        if err is not None:
            return err
        logger.info(
            "order latency: sign=%.0fms post=%.0fms token=%s",
            sign_ms,
            post_ms,
            token_id[:8],
        )

        state = self._await_terminal_state(order_hash)
        if state is None:
            # POSTED but state unreadable. The order EXISTS on the venue — a
            # failure return here would let the pipeline re-bet the slot
            # (double-spend). Loud success WITHOUT fill metadata instead;
            # cash-basis accounting + the operator alert surface it.
            logger.error(
                "predictfun buy: order %s posted but state unreadable — "
                "returning success WITHOUT fill metadata",
                order_hash[:18],
            )
            return OrderResult(
                success=True,
                order_id=order_hash,
                client_order_id=client_order_id,
                limit_price=order_price,
                fill_ts=utc_now_sql(),
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        status = str(state.get("status", "")).upper()
        filled = self._filled_shares(state)
        if filled is not None and filled > 0:
            return OrderResult(
                success=True,
                order_id=order_hash,
                client_order_id=client_order_id,
                filled_size_tokens=filled,
                filled_price_vwap=order_price,
                filled_value_usd=filled * order_price,
                limit_price=order_price,
                fill_ts=utc_now_sql(),
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        if status in ("CANCELLED", "EXPIRED", "INVALIDATED"):
            return OrderResult(
                success=False,
                error=f"FAK killed unfilled (status={status})",
                error_kind="no_liquidity",
                client_order_id=client_order_id,
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        if status == "FILLED":
            # FILLED but amountFilled unreadable: book the signed size at cap.
            logger.warning(
                "predictfun buy: FILLED with unreadable amountFilled — "
                "booking the signed size"
            )
            shares = shares_wei / 1e18
            return OrderResult(
                success=True,
                order_id=order_hash,
                client_order_id=client_order_id,
                filled_size_tokens=shares,
                filled_price_vwap=order_price,
                filled_value_usd=shares * order_price,
                limit_price=order_price,
                fill_ts=utc_now_sql(),
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        logger.error(
            "predictfun buy: order %s status=%s after MARKET post — returning "
            "success WITHOUT fill metadata",
            order_hash[:18],
            status or "?",
        )
        return OrderResult(
            success=True,
            order_id=order_hash,
            client_order_id=client_order_id,
            limit_price=order_price,
            fill_ts=utc_now_sql(),
            sign_ms=sign_ms,
            post_ms=post_ms,
        )

    def place_stop_loss_sell(
        self, *, token_id: str, size_tokens: float, price: float
    ):
        """FOK taker SELL of the full held size at ``price`` (the stop-loss).

        Mirror of the walker's FOK stop: strategy MARKET +
        ``isFillOrKill=true`` fills the FULL size at/above the cap atomically
        or kills entirely, so a stopped position is never split between a
        realized sell and settlement. ``success=True`` ONLY on status FILLED
        (vwap booked at the cap — the actual fill can only be at/above it);
        any kill leaves the position row PENDING for the caller's retry or
        settlement.

        Asymmetric unreadable-state policy vs the BUY path (deliberate): a
        posted-but-unreadable SELL returns ``success=False`` — the worst case
        of a hidden fill is one rejected retry (the shares are gone, the venue
        refuses a re-sell), while a falsely-booked sell would corrupt the
        ledger. The BUY path must return success in the same spot because a
        false failure there re-bets the slot (double-spend).
        """
        from predict_sdk import Side

        from cryptobot.execution.types import OrderResult

        try:
            price_f = float(price)
            size_f = float(size_tokens)
        except (TypeError, ValueError):
            return OrderResult(
                success=False, error="non-numeric price/size", error_kind="unknown"
            )
        if not (math.isfinite(price_f) and 0.0 < price_f < 1.0):
            return OrderResult(
                success=False,
                error="stop sell price out of (0,1)",
                error_kind="unknown",
            )
        if not (math.isfinite(size_f) and size_f > 0):
            return OrderResult(
                success=False, error="stop sell size <= 0", error_kind="unknown"
            )

        token = str(token_id)
        from cryptobot_predictfun.markets import lookup_token_order_meta

        meta = lookup_token_order_meta(token)
        if meta is None:
            return OrderResult(
                success=False,
                error="token not in discovery registry — refusing to sign",
                error_kind="unknown",
            )

        price_wei = self._price_wei(price_f)
        shares_wei = int(Decimal(str(size_f)) * 10**18)
        if shares_wei <= 0:
            return OrderResult(
                success=False,
                error="stop sell size rounds to zero",
                error_kind="invalid_amount",
            )
        client_order_id = f"pfstop-{token[:12]}-{int(time.time())}"

        t_sign = time.monotonic()
        try:
            signed = self._build_signed(
                side=Side.SELL,
                token_id=token,
                price_wei=price_wei,
                quantity_wei=shares_wei,
                fee_rate_bps=meta.fee_rate_bps,
                is_neg_risk=meta.is_neg_risk,
                is_yield_bearing=meta.is_yield_bearing,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("predictfun stop sell: build/sign failed")
            return OrderResult(
                success=False,
                error=f"build/sign failed: {exc!r}",
                error_kind="unknown",
            )
        sign_ms = (time.monotonic() - t_sign) * 1000.0

        order_hash, err, post_ms = self._post_order(
            signed=signed,
            price_wei=price_wei,
            is_fill_or_kill=True,
            client_order_id=client_order_id,
            label="stop-sell",
            sign_ms=sign_ms,
        )
        if err is not None:
            return err

        state = self._await_terminal_state(order_hash)
        status = str(state.get("status", "")).upper() if state else ""
        if status == "FILLED":
            return OrderResult(
                success=True,
                order_id=order_hash,
                client_order_id=client_order_id,
                filled_size_tokens=size_f,
                filled_price_vwap=price_f,
                filled_value_usd=size_f * price_f,
                limit_price=price_f,
                fill_ts=utc_now_sql(),
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        if state is None:
            logger.error(
                "predictfun stop sell: order %s posted but state unreadable — "
                "treating as KILL (see docstring for why this is safe)",
                order_hash[:18],
            )
            return OrderResult(
                success=False,
                error="posted but state unreadable",
                error_kind="unknown",
                client_order_id=client_order_id,
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        return OrderResult(
            success=False,
            error=f"FOK killed (status={status or '?'})",
            error_kind="no_liquidity",
            client_order_id=client_order_id,
            sign_ms=sign_ms,
            post_ms=post_ms,
        )
