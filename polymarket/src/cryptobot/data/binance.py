"""Binance public-market live spot adapter.

The legacy mode uses ``GET /api/v3/ticker/24hr`` with a batched ``symbols``
parameter and ``type=MINI``.  The production TWAP mode instead uses one
``GET /api/v3/klines?interval=1s`` request per configured coin and column 4
(``close``), matching the ``binance_1s_forward`` tape used by the backtest.
The ticker mode remains available for non-TWAP callers and existing tests.

Freshness model (audit w19oxqf24, findings #2 + #11)
----------------------------------------------------
The 24h-ticker ``closeTime`` is WALL-CLOCK: it advances on every request
regardless of whether ``lastPrice`` actually moved. It is therefore NOT a
genuine freshness signal — using it directly as ``publish_time`` made
downstream ``spot_age_s = now_ts - publish_time`` always ~0, so the gates.py
``stale_spot`` gate could never fire and a frozen/halted feed would silently
keep feeding live bets (finding #2). It also failed OPEN when ``closeTime`` was
missing/0 by substituting ``now_ts`` — fresh in exactly the money-unsafe
direction (finding #11).

This adapter does NOT trust ``closeTime`` as a clock. Instead it derives a
real freshness stamp itself:

  * ``closeTime`` is treated only as a structural "the venue returned a
    complete row" marker. A row missing it (absent or <= 0) is REJECTED
    (``_decode_row`` returns None), which downgrades the whole batch via the
    existing incomplete-batch path — rejecting on a missing publish time
    instead of fabricating a timestamp (#11).
  * ``BinanceSpot.publish_time`` is a FIRST-SEEN stamp, not ``closeTime``: the
    module remembers, per coin, the ``now_ts`` of the first distinct fetch
    bucket at which the current ``lastPrice`` value was observed. While the
    price stays frozen across consecutive distinct buckets, ``publish_time``
    keeps pointing at that first-seen time, so ``spot_age_s`` grows and the
    existing ``stale_spot`` gate fires once the price has been frozen longer
    than ``config.SPOT_STALENESS_S`` — without touching gates.py/pipeline.py
    (finding #2). A genuine price move resets the stamp to ``now_ts``.

Bucket-scoped cache: the 1Hz scheduler reuses one response batch for
``config.SPOT_FETCH_INTERVAL_SECONDS`` seconds, and a
failed bucket is negative-cached so outages do not hammer the free endpoint.
The store keeps the last :data:`_MAX_BUCKETS` buckets rather than one, so the
CURRENT bucket and the prefetcher's NEXT bucket coexist instead of evicting
each other (regression 2026-07-30 — see :data:`_MAX_BUCKETS`).
:func:`peek_all_spots` is the cache-only reader for callers that must never
spend a GET.
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


# Number of distinct fetch buckets the store keeps (regression 2026-07-30).
#
# THREE consumers read spot on OVERLAPPING bucket keys: the 1Hz tick reads
# bucket S, ``scheduler.spot_prefetch`` warms bucket S+1 ~200ms before the
# boundary, and the fast-trigger rungs re-read bucket S at 4Hz+. This store used
# to be a single slot (one ``bucket`` field), so the warm S+1 entry and the live
# S entry evicted each other several times a second — and every eviction turned
# the next read into an inline network GET on the decision path. Measured cost:
# main tick loop p50 115ms -> 315ms with ``stale_spot`` skips x6.9 (p=1e-8).
# Four buckets holds S-1 / S / S+1 with a slot of slack.
_MAX_BUCKETS = 4

# ``(bucket, coins) -> batch``, where a stored ``None`` is a NEGATIVE-cached
# (failed) bucket and an ABSENT key is an unknown one. Both meanings are now
# per-bucket, so a failure at S can no longer blank a good batch at S+1.
# Guarded by ``_cache_lock``; pruned by age on every write (``_store_locked``).
_cache: dict[tuple[int, tuple[str, ...]], Optional[dict[str, BinanceSpot]]] = {}
_cache_lock = threading.Lock()

# Distinguishes "no entry for this bucket" from "negative-cached None".
_MISSING = object()

# In-flight fetch registry (latency: same-bucket coalescing). Keyed like the
# bucket cache on (bucket, coins) and guarded by ``_cache_lock``. The first
# caller for an unresolved bucket registers an Event and performs the GET;
# concurrent same-bucket callers (the tick racing the SpotPrefetcher's
# boundary fetch) wait on it — bounded by the adapter timeout — and re-read
# the cache instead of paying a duplicate GET on the decision path.
_inflight: dict[tuple[int, tuple[str, ...]], threading.Event] = {}

# F2: minimum residual deadline budget (seconds) a coalesced waiter must have
# left to be worth an independent fallback GET after its wait resolves to an
# unresolved or negative-cached bucket; below this it returns None instead.
_MIN_FALLBACK_BUDGET_S = 0.2


def _store_locked(
    key: tuple[int, tuple[str, ...]],
    value: Optional[dict[str, BinanceSpot]],
) -> None:
    """Write one bucket entry and prune the store back to :data:`_MAX_BUCKETS`.

    Caller must hold ``_cache_lock``. Bucket ids are monotonic in time, so
    "newest by bucket id" IS "youngest by age"; pruning by RANK rather than by
    distance from the newest bucket also guarantees the entry just written is
    never the one evicted.
    """
    _cache[key] = value
    if len(_cache) > _MAX_BUCKETS:
        for stale in sorted(_cache, key=lambda k: k[0], reverse=True)[_MAX_BUCKETS:]:
            del _cache[stale]


@dataclass
class _FreshSeen:
    """Per-coin first-seen freshness anchor for the frozen-price detector.

    ``price`` is the last distinct ``lastPrice`` observed for the coin and
    ``first_seen_ts`` is the ``now_ts`` of the bucket at which that price value
    was first seen. ``last_bucket`` is the fetch bucket of the most recent
    update, so we only advance the detector on a genuinely NEW fetch bucket
    (a cached re-read inside the same bucket must not be mistaken for a fresh
    confirmation that the price is still moving) — audit w19oxqf24 #2.
    """

    price: float
    first_seen_ts: int
    last_bucket: int


# Frozen-price detector state (audit w19oxqf24 #2). Keyed by coin; updated
# only on a new fetch bucket so that the bucket-scoped cache's repeated reads
# do not reset the staleness clock.
_seen: dict[str, _FreshSeen] = {}
_seen_lock = threading.Lock()


def _freshness_publish_time(coin: str, price: float, *, now_ts: int, bucket: int) -> int:
    """Return the first-seen publish_time for ``(coin, price)`` (audit #2).

    On the first observation of a coin, or when ``price`` differs from the
    previously seen value, the anchor resets to ``now_ts`` (the price just
    moved → genuinely fresh). When ``price`` is unchanged across a NEW distinct
    fetch bucket, the original ``first_seen_ts`` is retained so downstream
    ``spot_age_s = now_ts - publish_time`` keeps growing and the ``stale_spot``
    gate fires once the freeze exceeds ``config.SPOT_STALENESS_S``.

    Re-reads within the same bucket (``bucket == last_bucket``) do not advance
    the detector and return the existing anchor unchanged.
    """
    with _seen_lock:
        prev = _seen.get(coin)
        if prev is not None and prev.price == price and bucket != prev.last_bucket:
            # Unchanged price across a new distinct bucket → keep aging it.
            _seen[coin] = _FreshSeen(
                price=price,
                first_seen_ts=prev.first_seen_ts,
                last_bucket=bucket,
            )
            return prev.first_seen_ts
        if prev is not None and prev.price == price and bucket == prev.last_bucket:
            # Same bucket re-read; do not touch the clock.
            return prev.first_seen_ts
        # First observation or a genuine price move → fresh anchor at now_ts.
        _seen[coin] = _FreshSeen(price=price, first_seen_ts=now_ts, last_bucket=bucket)
        return now_ts


def _decode_row(row: dict, *, now_ts: int, bucket: int) -> Optional[BinanceSpot]:
    """Convert one ticker row to a BinanceSpot, or None if malformed/stale.

    ``publish_time`` is the first-seen freshness stamp (audit w19oxqf24 #2),
    NOT the wall-clock ``closeTime``. ``closeTime`` is required only as a
    structural completeness marker: a row missing it (absent or <= 0) is
    rejected rather than having a ``now_ts`` fabricated for it, so it fails
    CLOSED via the incomplete-batch path (audit w19oxqf24 #11).
    """
    from cryptobot import config

    try:
        symbol = str(row["symbol"]).upper()
        price = float(row["lastPrice"])
    except (KeyError, TypeError, ValueError):
        return None

    # audit w19oxqf24 #11: do NOT substitute now_ts for a missing/zero
    # closeTime. A row without a real closeTime is rejected (returns None →
    # incomplete batch → whole bucket downgraded to None), instead of being
    # waved through as perfectly fresh in the money-unsafe direction.
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

    # audit w19oxqf24 #2: stamp publish_time with the FIRST-SEEN time of this
    # price value, not closeTime. While the price is frozen across distinct
    # fetch buckets the stamp ages, so the existing gates.py stale_spot gate
    # eventually fires; a genuine move resets it to now.
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

    The forward PMData tape is fetched from this exact Binance kline endpoint
    and uses column 4 (the finalized kline close) as its per-second spot.  Live
    must therefore reject the still-open current candle. Binance omits empty
    seconds for quieter pairs; the backtest's ``_spot_at`` uses the latest row
    at or before the target second, so live applies that same fallback.
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
    # Klines carry a genuine venue event time. Unlike the legacy ticker mode,
    # do not synthesize first-seen freshness: a missing run of candles should
    # age naturally and trip the existing stale-spot gate.
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
    """Return a {coin: BinanceSpot} batch covering configured live coins."""
    from cryptobot import config

    if now_ts is None:
        now_ts = time.time()
    # F2: one deadline budget covers this whole call — a coalesced wait and
    # any fallback GET both draw from it, so the worst case is ``timeout``
    # total, never a full wait PLUS a second full-timeout GET (2x).
    deadline = time.monotonic() + timeout
    # Owner GETs keep the client's configured timeout (unchanged); a waiter
    # falling back to its own GET tightens this to its residual budget.
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
            # A resolved bucket: the batch, or None for a negative-cached one.
            return cached  # type: ignore[return-value]
        inflight = _inflight.get(key)
        if inflight is None:
            # No fetch airborne for this bucket — this caller owns it.
            my_event = threading.Event()
            _inflight[key] = my_event

    if my_event is None:
        # A fetch for this exact bucket is already airborne (typically the
        # SpotPrefetcher's boundary fetch). Wait for it instead of paying a
        # duplicate GET — bounded by the shared deadline, not a fresh full
        # timeout — then reuse a successfully cached batch.
        inflight.wait(timeout=max(0.0, deadline - time.monotonic()))
        with _cache_lock:
            cached = _cache.get(key)
            if cached is not None:
                return cached
        # Bucket unresolved (owner hung/crashed) OR negative-cached (owner's
        # GET failed). Pre-coalescing, this caller's duplicate GET was an
        # independent success path — restore it, bounded by the RESIDUAL
        # deadline budget so wait + GET never exceeds ``timeout``. With no
        # meaningful budget left, give up like a failed fetch instead of
        # stacking a second full-timeout GET on top of the wait (F2).
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
            # One request per configured coin is intentional: Binance's kline
            # REST endpoint has no multi-symbol form. Run the four requests in
            # parallel and pin all of them to the SAME completed candle. This
            # avoids both sequential ~800ms decision lag and a mixed-second
            # batch when requests straddle a wall-clock boundary.
            by_coin: dict[str, BinanceSpot] = {}
            now_ts_int = int(now_ts)
            target_start_ms = (now_ts_int - 1) * 1_000

            # The boundary prefetch intentionally asks for bucket S before S
            # begins. Candle S-1 is not final until S, so wait for that boundary
            # inside the prefetch worker instead of caching a partial close.
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
        # Resolve the in-flight marker on EVERY exit path — success, negative
        # cache, or a raised exception — so waiters are never stranded past
        # their bound.
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
    """Fetch recent Binance 1m klines for partial-ret boot warm-start."""
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
    """Cache-ONLY read of this bucket's batch. NEVER touches the network.

    The fast-trigger rungs' spot access (regression 2026-07-30). They fire at
    4Hz+ and are structurally forbidden from spending a venue round trip — the
    same cache-only contract they already hold for books (``fetch_book=None``)
    and market metadata (``_venue_market_cache_has``). A cold bucket returns
    None and the rung ABSTAINS from the fire; it does not fetch, does not
    register an in-flight marker, does not negative-cache, and does not wait on
    anyone else's GET. The 1Hz tick's ``fetch_all_spots`` remains the ONLY
    fetch-on-miss path, so the network cadence is exactly what it was before the
    fast rungs existed.

    A negative-cached (failed) bucket also reads as None — abstain either way.
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
        # Wake any coalesced waiter and drop the in-flight markers so a fresh
        # test / process restart never inherits a stranded registration.
        for event in _inflight.values():
            event.set()
        _inflight.clear()
    # Also clear the frozen-price detector anchors (audit w19oxqf24 #2) so a
    # fresh test / process restart does not inherit a stale freshness clock.
    with _seen_lock:
        _seen.clear()

