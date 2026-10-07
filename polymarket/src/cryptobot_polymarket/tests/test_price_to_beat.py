"""Official Polymarket UI Price To Beat parser/fetch tests."""

from __future__ import annotations

from decimal import Decimal

import httpx

from cryptobot_polymarket.price_to_beat import (
    fetch_price_to_beat,
    parse_price_to_beat_html,
)


BOUNDARY = 1_800_000_000


def _record(*, price: str, coin: str = "BTC", duration: str = "fiveminute",
            end: int = BOUNDARY + 300, lookback: int = 60) -> str:
    start_iso = "2027-01-15T08:00:00Z"
    end_iso = "2027-01-15T08:05:00Z" if end == BOUNDARY + 300 else "wrong"
    return (
        '{"state":{"data":{"openPrice":' + price + ',"closePrice":null},'
        '"dataUpdateCount":1},"queryKey":["crypto-prices","price",'
        f'"{coin}","{start_iso}","{duration}","{end_iso}",true,{lookback}]}}'
    )


def test_parser_requires_exact_query_key_and_ignores_nearby_market_price():
    html = _record(price="100", coin="ETH") + _record(price="123.456789")
    assert parse_price_to_beat_html(
        html, coin="BTC", interval="5m", boundary_ts=BOUNDARY
    ) == Decimal("123.456789")


def test_parser_accepts_next_flight_escaped_payload():
    escaped = _record(price="123.5").replace('"', '\\"')
    assert parse_price_to_beat_html(
        escaped, coin="BTC", interval="5m", boundary_ts=BOUNDARY
    ) == Decimal("123.5")


def test_parser_accepts_exact_fifteen_minute_sixty_second_key():
    html = (
        '{"state":{"data":{"openPrice":63600.25,"closePrice":null},'
        '"dataUpdateCount":1},"queryKey":["crypto-prices","price",'
        '"BTC","2027-01-15T08:00:00Z","fifteen",'
        '"2027-01-15T08:15:00Z",true,60]}}'
    )
    assert parse_price_to_beat_html(
        html, coin="BTC", interval="15m", boundary_ts=BOUNDARY
    ) == Decimal("63600.25")


def test_parser_fails_closed_on_conflict_wrong_alignment_or_wrong_lookback():
    conflict = _record(price="100") + _record(price="101")
    assert parse_price_to_beat_html(
        conflict, coin="BTC", interval="5m", boundary_ts=BOUNDARY
    ) is None
    assert parse_price_to_beat_html(
        _record(price="100"), coin="BTC", interval="5m", boundary_ts=BOUNDARY + 1
    ) is None
    assert parse_price_to_beat_html(
        _record(price="100", lookback=30),
        coin="BTC", interval="5m", boundary_ts=BOUNDARY,
    ) is None


def test_fetch_uses_exact_market_slug_and_returns_official_open():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, text=_record(price="63665.570921281666"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        value = fetch_price_to_beat(
            coin="BTC", interval="5m", boundary_ts=BOUNDARY, client=client
        )
    assert value == Decimal("63665.570921281666")
    assert seen == [f"https://polymarket.com/event/btc-updown-5m-{BOUNDARY}"]


def test_fetch_fails_closed_on_http_or_payload_error():
    with httpx.Client(
        transport=httpx.MockTransport(lambda _r: httpx.Response(503))
    ) as client:
        assert fetch_price_to_beat(
            coin="BTC", interval="5m", boundary_ts=BOUNDARY, client=client
        ) is None
