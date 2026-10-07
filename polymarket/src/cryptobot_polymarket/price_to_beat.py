"""Official Polymarket UI ``Price To Beat`` fallback.

The 5m/15m market page server-renders the Chainlink TWAP opening price in its
React Query hydration payload.  This module reads that public first-party page
and accepts a value only when its query key exactly matches the requested coin,
window start/end, duration, TWAP flag, and lookback.  It never substitutes a
spot-exchange price.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

POLYMARKET_BASE = "https://polymarket.com"
_INTERVALS: dict[str, tuple[int, str, int]] = {
    "5m": (300, "fiveminute", 60),
    "15m": (900, "fifteen", 60),
}
_COINS = frozenset({"BTC", "ETH", "SOL", "XRP"})
_NUMBER = r"(?:0|[1-9]\d*)(?:\.\d+)?"


def _iso(epoch_s: int) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def parse_price_to_beat_html(
    html: str,
    *,
    coin: str,
    interval: str,
    boundary_ts: int,
) -> Optional[Decimal]:
    """Extract one exactly-keyed opening price from a market page payload."""
    normalized_coin = str(coin).strip().upper()
    spec = _INTERVALS.get(interval)
    if normalized_coin not in _COINS or spec is None:
        return None
    duration_s, duration_key, lookback_s = spec
    if isinstance(boundary_ts, bool):
        return None
    try:
        boundary = int(boundary_ts)
    except (TypeError, ValueError, OverflowError):
        return None
    if boundary <= 0 or boundary % duration_s != 0:
        return None

    # Next Flight appears in both singly- and multiply-escaped script strings.
    # Normalizing only backslashes immediately before quotes leaves values and
    # page text untouched while giving both encodings one strict parse path.
    normalized = re.sub(r'\\+"', '"', str(html))
    query_key = (
        '["crypto-prices","price",'
        f'"{normalized_coin}","{_iso(boundary)}","{duration_key}",'
        f'"{_iso(boundary + duration_s)}",true,{lookback_s}]'
    )
    # Keep data and queryKey in the same React Query record.  The negative
    # lookahead prevents accidentally pairing a nearby market's openPrice with
    # the requested key when Polymarket embeds past/future windows on one page.
    pattern = re.compile(
        rf'"data":\{{"openPrice":(?P<price>{_NUMBER}),'
        rf'"closePrice":(?:null|{_NUMBER})\}}'
        rf'(?:(?!"queryKey").){{0,2500}}'
        rf'"queryKey":{re.escape(query_key)}',
        re.DOTALL,
    )
    prices: set[Decimal] = set()
    for match in pattern.finditer(normalized):
        try:
            value = Decimal(match.group("price"))
        except InvalidOperation:
            continue
        if value.is_finite() and value > 0:
            prices.add(value)
    # Duplicate server/client hydration copies are normal; disagreement is not.
    return next(iter(prices)) if len(prices) == 1 else None


def fetch_price_to_beat(
    *,
    coin: str,
    interval: str,
    boundary_ts: int,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
) -> Optional[Decimal]:
    """Fetch the official UI anchor, failing closed on any ambiguity/error."""
    normalized_coin = str(coin).strip().upper()
    spec = _INTERVALS.get(interval)
    if normalized_coin not in _COINS or spec is None:
        return None
    duration_s, _duration_key, _lookback_s = spec
    try:
        boundary = int(boundary_ts)
    except (TypeError, ValueError, OverflowError):
        return None
    if boundary <= 0 or boundary % duration_s != 0:
        return None

    slug = f"{normalized_coin.lower()}-updown-{duration_s // 60}m-{boundary}"
    owned = client is None
    if client is None:
        client = httpx.Client(timeout=timeout, follow_redirects=True)
    try:
        try:
            response = client.get(
                f"{POLYMARKET_BASE}/event/{slug}",
                headers={"User-Agent": "cryptobot-price-to-beat/1.0"},
            )
        except httpx.HTTPError as exc:
            logger.info("price_to_beat: http error %r for slug=%s", exc, slug)
            return None
        if response.status_code != 200:
            logger.info(
                "price_to_beat: status=%d for slug=%s",
                response.status_code,
                slug,
            )
            return None
        value = parse_price_to_beat_html(
            response.text,
            coin=normalized_coin,
            interval=interval,
            boundary_ts=boundary,
        )
        if value is None:
            logger.info("price_to_beat: exact payload missing for slug=%s", slug)
        return value
    finally:
        if owned:
            client.close()
