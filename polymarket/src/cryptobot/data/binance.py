"""Live spot prices from Binance's public market-data API.

Two modes. Production (TWAP) mode makes one ``GET /api/v3/klines?interval=1s``
request per coin and uses column 4 (``close``), the same field as the
``binance_1s_forward`` tape the backtest uses. Ticker mode uses one batched
``GET /api/v3/ticker/24hr`` (``type=MINI``) and is kept for other callers and
tests.

Ticker freshness
----------------
The 24h ticker's ``closeTime`` is wall-clock time: it advances on every request
whether or not ``lastPrice`` moved, so using it as ``publish_time`` would make
``spot_age_s`` always ~0 and the ``stale_spot`` gate could never fire on a
frozen feed. Substituting ``now_ts`` when it is missing would be just as bad.

So in ticker mode:

  * ``closeTime`` only marks a complete row. A row without it (absent or
    <= 0) is rejected, which marks the whole batch incomplete.
  * ``BinanceSpot.publish_time`` is a first-seen time: per coin, the
    ``now_ts`` of the first fetch bucket that saw the current ``lastPrice``.
    While the price stays frozen across buckets, ``spot_age_s`` grows and
    ``stale_spot`` fires once it exceeds ``config.SPOT_STALENESS_S``. A real
    price change resets it to ``now_ts``.

Kline mode uses the candle's own time and needs none of this.

Caching: the 1 Hz scheduler reuses one batch for
``config.SPOT_FETCH_INTERVAL_SECONDS``, and a failed bucket is negative-cached
so an outage does not hammer the free endpoint. The cache keeps the last
:data:`_MAX_BUCKETS` buckets, so the current bucket and the prefetcher's next
bucket do not evict each other. :func:`peek_all_spots` reads the cache only and
never fetches.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SYMBOLS: dict[str, str] = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT",
}
_SYMBOL_TO_COIN = {v: k for k, v in SYMBOLS.items()}


@dataclass(frozen=True)
class BinanceSpot:
    """One coin's live spot quote."""

    coin: str
    price: float
    publish_time: int


@dataclass(frozen=True)
class BinanceMinuteBar:
    """One 1-minute kline row from Binance."""

    coin: str
    start_ts: int
    open: float
    close: float
    is_closed: bool


# Number of fetch buckets the cache keeps.
#
# Three readers use overlapping buckets: the 1 Hz tick reads bucket S,
# ``scheduler.spot_prefetch`` warms S+1 ~200ms before the boundary, and the
# fast-trigger scans re-read S several times a second. With a single slot those
# entries evicted each other constantly, and every eviction made the next read
# a network call on the decision path (tick p50 rose from 115ms to 315ms). Four
# slots hold S-1, S and S+1 with one spare.
_MAX_BUCKETS = 4

# ``(bucket, coins) -> batch``. A stored ``None`` is a failed (negative-cached)
# bucket; a missing key is an unknown one. Both are per bucket, so a failure at
# S cannot blank a good batch at S+1. Guarded by ``_cache_lock``; pruned on
# every write (``_store_locked``).
_cache: dict[tuple[int, tuple[str, ...]], Optional[dict[str, BinanceSpot]]] = {}
_cache_lock = threading.Lock()

# Tells "no entry for this bucket" apart from a negative-cached None.
_MISSING = object()

# In-flight fetches, keyed like the cache and guarded by ``_cache_lock``. The
# first caller for an unresolved bucket registers an Event and does the GET;
# other callers for the same bucket (typically the tick racing the prefetcher)
# wait on it, bounded by the timeout, and then read the cache instead of making
# a duplicate GET.
_inflight: dict[tuple[int, tuple[str, ...]], threading.Event] = {}

# Minimum time left (seconds) for a waiter to make its own fallback GET after
# the shared fetch failed or never finished; with less it returns None.
_MIN_FALLBACK_BUDGET_S = 0.2


def _store_locked(
    key: tuple[int, tuple[str, ...]],
    value: Optional[dict[str, BinanceSpot]],
) -> None:
    """Write one bucket entry and prune the cache back to :data:`_MAX_BUCKETS`.

    Caller holds ``_cache_lock``. Bucket ids increase with time, so keeping the
    newest ids keeps the youngest entries, and the entry just written is never
    the one evicted.
    """
    _cache[key] = value
    if len(_cache) > _MAX_BUCKETS:
        for stale in sorted(_cache, key=lambda k: k[0], reverse=True)[_MAX_BUCKETS:]:
            del _cache[stale]


@dataclass
class _FreshSeen:
    """Per-coin first-seen anchor for the frozen-price detector.

    ``price`` is the last distinct ``lastPrice`` seen for the coin and
    ``first_seen_ts`` the ``now_ts`` of the bucket where that value first
    appeared. ``last_bucket`` is the bucket of the latest update; the detector
    only advances on a new bucket, so a cached re-read is not mistaken for a
    new confirmation.
    """

    price: float
    first_seen_ts: int
    last_bucket: int


# Frozen-price detector state, keyed by coin. Updated only on a new fetch
# bucket so repeated cache reads do not reset the staleness clock.
_seen: dict[str, _FreshSeen] = {}
_seen_lock = threading.Lock()


def _freshness_publish_time(coin: str, price: float, *, now_ts: int, bucket: int) -> int:
    """Return the first-seen publish_time for ``(coin, price)``.

    On a coin's first observation, or when ``price`` changed, the anchor resets
    to ``now_ts``. When the price is unchanged in a new bucket the original
    ``first_seen_ts`` is kept, so ``spot_age_s = now_ts - publish_time`` grows
    and ``stale_spot`` fires once the freeze exceeds
    ``config.SPOT_STALENESS_S``.

    A re-read in the same bucket (``bucket == last_bucket``) changes nothing.
    """
    with _seen_lock:
        prev = _seen.get(coin)
        if prev is not None and prev.price == price and bucket != prev.last_bucket:
            # Same price in a new bucket: keep aging it.
            _seen[coin] = _FreshSeen(
                price=price,
                first_seen_ts=prev.first_seen_ts,
                last_bucket=bucket,
            )
            return prev.first_seen_ts
        if prev is not None and prev.price == price and bucket == prev.last_bucket:
            # Same-bucket re-read: leave the clock alone.
            return prev.first_seen_ts
        # First observation or a real price change: anchor at now_ts.
        _seen[coin] = _FreshSeen(price=price, first_seen_ts=now_ts, last_bucket=bucket)
        return now_ts


def _decode_row(row: dict, *, now_ts: int, bucket: int) -> Optional[BinanceSpot]:
    """Convert one ticker row to a BinanceSpot, or None if malformed.

    ``publish_time`` is the first-seen time, not ``closeTime``. ``closeTime``
    is only required as a completeness marker: a row without it is rejected
    rather than given a made-up ``now_ts``, so it fails closed via the
    incomplete-batch path.
    """
    from cryptobot import config

    try:
        symbol = str(row["symbol"]).upper()
        price = float(row["lastPrice"])
    except (KeyError, TypeError, ValueError):
        return None

    # Never substitute now_ts for a missing or zero closeTime; reject the row
    # (the whole bucket then becomes None) rather than treat it as fresh.
    try:
        close_time_ms = int(row.get("closeTime"))
    except (TypeError, ValueError):
        return None
    if close_time_ms <= 0:
        return None

    coin = _SYMBOL_TO_COIN.get(symbol)
    if coin is None or not (price > 0.0):
        return None

    bounds = config.RESOLUTION_PRICE_BOUNDS.get(coin)
    if bounds is not None:
        lo, hi = bounds
        if not (lo <= price <= hi):
            logger.warning(
                "binance: %s price %.6f outside sanity bounds [%.6f, %.6f]; dropping",
                coin,
                price,
                lo,
                hi,
            )
            return None

    # publish_time is when this price value was first seen, not closeTime.
    # While the price is frozen across buckets it ages, so stale_spot
    # eventually fires; a real move resets it.
    publish_time = _freshness_publish_time(coin, price, now_ts=now_ts, bucket=bucket)

    return BinanceSpot(
        coin=coin,
        price=price,
        publish_time=max(0, publish_time),
    )


def _decode_completed_1s_kline(
    coin: str,
    payload: object,
    *,
    target_start_ms: int,
    observed_ts_ms: int,
    now_ts: int,
    bucket: int,
) -> Optional[BinanceSpot]:
    """Decode the latest completed kline at or before the target second.

    The backtest's forward tape comes from this same endpoint and uses column 4
    (the final close) as its per-second spot, so the still-open current candle
    must be rejected. Binance skips empty seconds on quiet pairs; like the
    backtest's ``_spot_at``, use the latest row at or before the target second.
    """
    from cryptobot import config

    if not isinstance(payload, list):
        return None
    candidates: list[tuple[int, float]] = []
    for row in payload:
        if not isinstance(row, list) or len(row) < 7:
            continue
        try:
            start_ms = int(row[0])
            price = float(row[4])
            close_ms = int(row[6])
        except (TypeError, ValueError):
            continue
        if start_ms <= 0 or start_ms > target_start_ms:
            continue
        if close_ms != start_ms + 999 or close_ms > observed_ts_ms:
            continue
        if not math.isfinite(price) or price <= 0.0:
            continue
        candidates.append((start_ms, price))
    if not candidates:
        return None
    start_ms, price = max(candidates, key=lambda item: item[0])
    bounds = config.RESOLUTION_PRICE_BOUNDS.get(coin)
    if bounds is not None:
        lo, hi = bounds
        if not (lo <= price <= hi):
            logger.warning(
                "binance: %s 1s kline close %.6f outside sanity bounds "
                "[%.6f, %.6f]; dropping",
                coin,
                price,
                lo,
                hi,
            )
            return None
    # Klines carry a real venue time, so no first-seen tracking: a run of
    # missing candles ages naturally and trips stale_spot.
    publish_time = (start_ms + 1_000) // 1_000
    return BinanceSpot(
        coin=coin,
        price=price,
        publish_time=max(0, publish_time),
    )


def fetch_all_spots(
    *,
    now_ts: float | None = None,
    tick_seconds: int = 1,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
) -> Optional[dict[str, BinanceSpot]]:
    """Return a {coin: BinanceSpot} batch for the configured coins."""
    from cryptobot import config

    if now_ts is None:
        now_ts = time.time()
    # One deadline covers the whole call: waiting on a shared fetch and any
    # fallback GET both draw from it, so the worst case is ``timeout`` in
    # total, not a wait plus a second full-timeout GET.
    deadline = time.monotonic() + timeout
    # The owner's GET uses the client's configured timeout; a waiter that falls
    # back to its own GET shortens it to the time left.
    request_timeout = httpx.USE_CLIENT_DEFAULT
    bucket = int(now_ts // max(1, tick_seconds))
    coins = tuple(config.COINS)
    unknown = [coin for coin in coins if coin not in SYMBOLS]
    if unknown:
        logger.warning("binance: unknown configured coins %s", unknown)
        return None

    key = (bucket, coins)
    my_event: Optional[threading.Event] = None
    with _cache_lock:
        cached = _cache.get(key, _MISSING)
        if cached is not _MISSING:
            # A resolved bucket: the batch, or None if negative-cached.
            return cached  # type: ignore[return-value]
        inflight = _inflight.get(key)
        if inflight is None:
            # No fetch in flight for this bucket; this caller does it.
            my_event = threading.Event()
            _inflight[key] = my_event

    if my_event is None:
        # A fetch for this bucket is already in flight (usually the
        # prefetcher's). Wait for it, bounded by the shared deadline, and reuse
        # its result.
        inflight.wait(timeout=max(0.0, deadline - time.monotonic()))
        with _cache_lock:
            cached = _cache.get(key)
            if cached is not None:
                return cached
        # The bucket is still unresolved (owner hung or crashed) or
        # negative-cached (owner's GET failed). Make an independent GET with
        # the time left, so wait plus GET never exceed ``timeout``; with too
        # little time left, give up as a failed fetch.
        remaining = deadline - time.monotonic()
        if remaining <= _MIN_FALLBACK_BUDGET_S:
            return None
        timeout = remaining
        request_timeout = remaining

    try:
        owned_client = client is None
        if client is None:
            client = httpx.Client(timeout=timeout)

        def _close_owned_client() -> None:
            if owned_client and client is not None:
                client.close()

        if config.SPOT_USE_1S_KLINES:
            # Binance's kline endpoint has no multi-symbol form, so make one
            # request per coin, in parallel, all pinned to the same completed
            # candle. That avoids ~800ms of sequential latency and a batch that
            # mixes two seconds when requests straddle a boundary.
            by_coin: dict[str, BinanceSpot] = {}
            now_ts_int = int(now_ts)
            target_start_ms = (now_ts_int - 1) * 1_000

            # The prefetcher asks for bucket S before S begins, but candle S-1
            # is not final until S. Wait for the boundary in the prefetch
            # worker rather than cache a partial close.
            wait_s = float(now_ts_int) - time.time()
            if wait_s > 0.0:
                remaining = deadline - time.monotonic()
                if wait_s >= remaining:
                    _mark_bucket_failed(bucket, coins)
                    _close_owned_client()
                    return None
                time.sleep(wait_s)

            remaining = max(0.2, deadline - time.monotonic())
            per_request_timeout = min(timeout, remaining)

            def _request_kline(
                coin: str,
            ) -> tuple[str, Optional[httpx.Response], Optional[httpx.HTTPError]]:
                try:
                    resp = client.get(
                        f"{config.BINANCE_BASE_URL}/api/v3/klines",
                        params={
                            "symbol": SYMBOLS[coin],
                            "interval": "1s",
                            "endTime": target_start_ms + 999,
                            "limit": 10,
                        },
                        timeout=per_request_timeout,
                    )
                except httpx.HTTPError as exc:
                    return coin, None, exc
                return coin, resp, None

            with ThreadPoolExecutor(max_workers=len(coins)) as executor:
                responses = list(executor.map(_request_kline, coins))

            observed_ts_ms = int(time.time() * 1_000.0)
            for coin, resp, request_error in responses:
                if request_error is not None:
                    logger.info(
                        "binance: 1s kline http error coin=%s err=%r",
                        coin,
                        request_error,
                    )
                    _mark_bucket_failed(bucket, coins)
                    _close_owned_client()
                    return None
                assert resp is not None
                if resp.status_code in (418, 429):
                    logger.warning(
                        "binance: 1s kline rate-limited coin=%s status=%d",
                        coin,
                        resp.status_code,
                    )
                    _mark_bucket_failed(bucket, coins)
                    _close_owned_client()
                    return None
                if resp.status_code >= 400:
                    logger.warning(
                        "binance: 1s kline status=%d coin=%s body=%s",
                        resp.status_code,
                        coin,
                        resp.text[:200],
                    )
                    _mark_bucket_failed(bucket, coins)
                    _close_owned_client()
                    return None
                try:
                    payload = resp.json()
                except ValueError:
                    logger.warning(
                        "binance: 1s kline non-JSON coin=%s", coin
                    )
                    _mark_bucket_failed(bucket, coins)
                    _close_owned_client()
                    return None
                spot = _decode_completed_1s_kline(
                    coin,
                    payload,
                    target_start_ms=target_start_ms,
                    observed_ts_ms=observed_ts_ms,
                    now_ts=now_ts_int,
                    bucket=bucket,
                )
                if spot is not None:
                    by_coin[coin] = spot
            if set(coins).issubset(by_coin):
                by_coin = {coin: by_coin[coin] for coin in coins}
                with _cache_lock:
                    _store_locked(key, by_coin)
                _close_owned_client()
                return by_coin
            logger.warning(
                "binance: incomplete 1s kline batch - got %s, want %s",
                sorted(by_coin),
                sorted(coins),
            )
            _mark_bucket_failed(bucket, coins)
            _close_owned_client()
            return None

        symbols = [SYMBOLS[coin] for coin in coins]
        params = {
            "symbols": json.dumps(symbols, separators=(",", ":")),
            "type": "MINI",
        }
        url = f"{config.BINANCE_BASE_URL}/api/v3/ticker/24hr"

        try:
            try:
                resp = client.get(url, params=params, timeout=request_timeout)
            except httpx.HTTPError as exc:
                logger.info("binance: http error %r", exc)
                _mark_bucket_failed(bucket, coins)
                return None
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                logger.info("binance: rate-limited (429) retry_after=%s", retry_after)
                _mark_bucket_failed(bucket, coins)
                return None
            if resp.status_code == 418:
                retry_after = resp.headers.get("Retry-After")
                logger.error("binance: IP banned (418) retry_after=%s", retry_after)
                _mark_bucket_failed(bucket, coins)
                return None
            if resp.status_code >= 400:
                logger.warning("binance: status=%d body=%s", resp.status_code, resp.text[:200])
                _mark_bucket_failed(bucket, coins)
                return None
            try:
                payload = resp.json()
            except ValueError:
                logger.warning("binance: non-JSON response")
                _mark_bucket_failed(bucket, coins)
                return None
        finally:
            if owned_client:
                client.close()

        if isinstance(payload, dict):
            rows = [payload]
        elif isinstance(payload, list):
            rows = payload
        else:
            _mark_bucket_failed(bucket, coins)
            return None

        now_ts_int = int(now_ts)
        by_coin: dict[str, BinanceSpot] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            spot = _decode_row(row, now_ts=now_ts_int, bucket=bucket)
            if spot is not None:
                by_coin[spot.coin] = spot

        if not set(coins).issubset(by_coin):
            logger.warning(
                "binance: incomplete batch - got %s, want %s",
                sorted(by_coin),
                sorted(coins),
            )
            _mark_bucket_failed(bucket, coins)
            return None

        by_coin = {coin: by_coin[coin] for coin in coins}
        with _cache_lock:
            _store_locked(key, by_coin)
        return by_coin
    finally:
        # Resolve the in-flight marker on every exit (success, negative cache
        # or exception) so waiters are never left hanging.
        if my_event is not None:
            with _cache_lock:
                _inflight.pop(key, None)
            my_event.set()


def fetch_recent_minute_bars(
    coin: str,
    *,
    limit: int = 120,
    now_ts: float | None = None,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
) -> list[BinanceMinuteBar]:
    """Fetch recent Binance 1m klines to warm-start PartialRetEngine at boot.
    """
    from cryptobot import config

    symbol = SYMBOLS.get(coin.upper())
    if symbol is None:
        logger.warning("binance: unknown coin for klines %s", coin)
        return []
    if now_ts is None:
        now_ts = time.time()
    now_ms = int(now_ts * 1000)

    url = f"{config.BINANCE_BASE_URL}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": max(1, min(int(limit), 1000)),
    }

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)

    try:
        try:
            resp = client.get(url, params=params)
        except httpx.HTTPError as exc:
            logger.info("binance: kline http error coin=%s err=%r", coin, exc)
            return []
        if resp.status_code >= 400:
            logger.warning(
                "binance: kline status=%d coin=%s body=%s",
                resp.status_code,
                coin,
                resp.text[:200],
            )
            return []
        try:
            payload = resp.json()
        except ValueError:
            logger.warning("binance: kline non-JSON response coin=%s", coin)
            return []
    finally:
        if owned_client:
            client.close()

    if not isinstance(payload, list):
        return []

    bars: list[BinanceMinuteBar] = []
    for row in payload:
        if not isinstance(row, list) or len(row) < 7:
            continue
        try:
            start_ms = int(row[0])
            open_px = float(row[1])
            close_px = float(row[4])
            close_ms = int(row[6])
        except (TypeError, ValueError):
            continue
        if start_ms <= 0 or open_px <= 0.0 or close_px <= 0.0:
            continue
        bars.append(
            BinanceMinuteBar(
                coin=coin.upper(),
                start_ts=start_ms // 1000,
                open=open_px,
                close=close_px,
                is_closed=close_ms <= now_ms,
            )
        )
    return bars


def peek_all_spots(
    *,
    now_ts: float | None = None,
    tick_seconds: int = 1,
) -> Optional[dict[str, BinanceSpot]]:
    """Read this bucket's batch from the cache only; never touches the network.

    For the fast-trigger scans, which run several times a second and must never
    make a venue call (they read books and market metadata from cache too). A
    cold or negative-cached bucket returns None and the scan skips; it never
    fetches, registers an in-flight marker, negative-caches or waits. The 1 Hz
    tick's ``fetch_all_spots`` is the only path that fetches on a miss.
    """
    from cryptobot import config

    if now_ts is None:
        now_ts = time.time()
    bucket = int(now_ts // max(1, tick_seconds))
    with _cache_lock:
        cached = _cache.get((bucket, tuple(config.COINS)))
    return cached


def _mark_bucket_failed(bucket: int, coins: tuple[str, ...]) -> None:
    with _cache_lock:
        _store_locked((bucket, coins), None)


def reset_cache() -> None:
    with _cache_lock:
        _cache.clear()
        # Wake any waiter and drop in-flight markers so a test or restart never
        # inherits a stuck one.
        for event in _inflight.values():
            event.set()
        _inflight.clear()
    # Clear the frozen-price anchors too, so a test or restart starts with a
    # fresh staleness clock.
    with _seen_lock:
        _seen.clear()

