"""predict.fun market discovery + resolution against the REAL payload shapes
captured by the 2026-07-21 MAINNET probe:

  * no `slug` field / no slug query params — discovery lists
    status=OPEN&marketVariant=CRYPTO_UP_DOWN pages and matches `categorySlug`;
  * auth = `x-api-key` header (Bearer 401s);
  * settlement = top-level `resolution` winning-Outcome object + per-outcome
    status WON/LOST; lifecycle PRICE_PROPOSED phases are NOT settled;
  * resolution lookup = discovery-registered market id -> GET /markets/{id}.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone

import httpx
import pytest

from cryptobot_predictfun import markets as pf


@pytest.fixture(autouse=True)
def _clear_cache() -> Iterator[None]:
    pf.clear_market_cache()
    yield
    pf.clear_market_cache()


WS = 1784500000  # a window_start_ts


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _market(
    *,
    coin: str = "btc",
    interval: int = 15,
    ws: int = WS,
    fee=200,
    up_tok: str = "111",
    down_tok: str = "222",
    up_name: str = "Up",
    down_name: str = "Down",
    market_id=853960,
    status: str = "REGISTERED",
    trading_status: str = "OPEN",
) -> dict:
    """One predict.fun crypto up-down market, REAL mainnet payload shape
    (2026-07-21 probe): categorySlug carries the window identity, there is NO
    `slug` field and NO endDate — boostStartsAt/boostEndsAt equal the window
    bounds; outcomes carry indexSet + onChainId (+ status once settled)."""
    return {
        "id": market_id,
        "conditionId": "0xf5cbe707cond",
        "oracleQuestionId": "0x6705b62quest",
        "categorySlug": f"{coin}-updown-{interval}m-{ws}",
        "feeRateBps": fee,
        "isNegRisk": False,
        "polymarketConditionIds": [],
        "status": status,
        "tradingStatus": trading_status,
        "boostStartsAt": _iso(ws),
        "boostEndsAt": _iso(ws + interval * 60),
        "variantData": {
            "type": "CRYPTO_UP_DOWN",
            "priceFeedProvider": "CHAINLINK",
            "priceFeedSymbol": f"{coin.upper()}USDT",
            "startPrice": None,
            "endPrice": None,
        },
        "outcomes": [
            {
                "indexSet": 1,
                "name": up_name,
                "onChainId": up_tok,
                "bestBid": {"price": 0.60, "size": 10},
                "bestAsk": {"price": 0.62, "size": 5},
            },
            {
                "indexSet": 2,
                "name": down_name,
                "onChainId": down_tok,
                "bestBid": {"price": 0.38, "size": 8},
                "bestAsk": {"price": 0.40, "size": 4},
            },
        ],
    }


def _listing_client(markets: list[dict], seen: list | None = None) -> httpx.Client:
    """Serves GET /markets (the OPEN listing) and GET /markets/{id}."""

    def handler(req: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(req)
        tail = req.url.path.rstrip("/").split("/")[-1]
        if tail.isdigit():
            for m in markets:
                if str(m.get("id")) == tail:
                    return httpx.Response(200, json={"success": True, "data": m})
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(200, json={"success": True, "data": markets})

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_slug_for():
    assert pf.slug_for("BTC", 15, WS) == f"btc-updown-15m-{WS}"


def test_discovers_and_maps_outcomes():
    seen: list = []
    with _listing_client([_market()], seen) as c:
        m = pf.fetch_active_market(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert isinstance(m, pf.PredictFunMarket)
    assert m.yes_token_id == "111"    # UP outcome
    assert m.no_token_id == "222"     # DOWN outcome
    assert m.condition_id == "0xf5cbe707cond"
    assert m.market_id == "853960"
    assert m.slug == f"btc-updown-15m-{WS}"  # from categorySlug
    assert m.fee_rate_bps == 200.0
    assert m.window_end_ts == WS + 15 * 60
    # Probe-real discovery params: status/marketVariant/first (no slug params).
    params = seen[0].url.params
    assert params.get("status") == "OPEN"
    assert params.get("marketVariant") == "CRYPTO_UP_DOWN"
    assert params.get("first") == "100"
    assert "slug" not in dict(params)


def test_discovery_registers_token_and_market_id_maps():
    with _listing_client([_market()]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(m, pf.PredictFunMarket)
    # Token registry: onChainId -> (market_id, side) — the CLOB keying seam.
    assert pf.lookup_token_market("111") == ("853960", "UP")
    assert pf.lookup_token_market("222") == ("853960", "DOWN")
    assert pf.lookup_token_market("999") is None
    # Market-id registry: (coin, interval, window) -> id — the resolver seam.
    assert pf.lookup_market_id("btc", 15, WS) == "853960"
    assert pf.lookup_market_id("BTC", 15, WS) == "853960"  # case-normalized


def test_auth_header_is_x_api_key(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "sekret", raising=False)
    seen: list = []
    with _listing_client([_market()], seen) as c:
        pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    req = seen[0]
    assert req.headers.get("x-api-key") == "sekret"
    # Bearer is the WRONG credential for the API key (mainnet 401s) — never sent.
    assert "authorization" not in req.headers


def test_yes_no_aliases_map():
    with _listing_client([_market(up_name="Yes", down_name="No")]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(m, pf.PredictFunMarket)
    assert m.yes_token_id == "111"
    assert m.no_token_id == "222"


def test_fee_rate_bps_fallback_when_null(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_DEFAULT_FEE_RATE_BPS", 200.0)
    with _listing_client([_market(fee=None)]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(m, pf.PredictFunMarket)
    assert m.fee_rate_bps == 200.0


def test_fee_unknown_on_bad_fee():
    with _listing_client([_market(fee="abc")]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(m, pf.FeeUnknown)


def test_fee_unknown_on_nonpositive_fee():
    with _listing_client([_market(fee=0)]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(m, pf.FeeUnknown)


def test_window_mismatch_returns_none():
    # Market for a DIFFERENT window than requested => not selected.
    with _listing_client([_market(ws=WS + 900)]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_interval_mismatch_returns_none():
    with _listing_client([_market(interval=5)]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_coin_mismatch_returns_none():
    with _listing_client([_market(coin="eth")]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_hourly_convention_not_matched():
    # Probe: hourly markets use ET-clock categorySlugs with the FULL coin name
    # ("bitcoin-up-or-down-july-21-2026-10am-et") — they must never be selected
    # for a 5m/15m window (and 1h discovery is not supported here).
    mkt = _market()
    mkt["categorySlug"] = "bitcoin-up-or-down-july-21-2026-10am-et"
    with _listing_client([mkt]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_unmappable_outcomes_fail_closed():
    mkt = _market()
    mkt["outcomes"] = [{"indexSet": 3, "name": "Maybe", "onChainId": "999"}]
    with _listing_client([mkt]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_empty_data_returns_none():
    with _listing_client([]) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_401_returns_none():
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(401, json={"error": "unauthorized"})
        )
    )
    with client as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert m is None


def test_cache_short_circuits_second_call():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(200, json={"success": True, "data": [_market()]})

    with httpx.Client(transport=httpx.MockTransport(handler)) as c:
        m1 = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c, now_ts=WS + 10)
        m2 = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c, now_ts=WS + 11)
    assert m1 == m2
    assert calls["n"] == 1  # second call served from the market cache


def test_listing_cache_shared_across_cells():
    """One listing sweep (TTL'd) serves EVERY (coin, interval) cell — the
    240 req/min budget guard: 2 cells resolving at the same instant must not
    pay 2 sweeps."""
    calls = {"n": 0}
    markets = [_market(), _market(coin="eth", up_tok="333", down_tok="444", market_id=853961)]

    def handler(req):
        calls["n"] += 1
        return httpx.Response(200, json={"success": True, "data": markets})

    with httpx.Client(transport=httpx.MockTransport(handler)) as c:
        m1 = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c, now_ts=WS + 1)
        m2 = pf.fetch_active_market("eth", interval_min=15, window_start_ts=WS, client=c, now_ts=WS + 2)
    assert isinstance(m1, pf.PredictFunMarket)
    assert isinstance(m2, pf.PredictFunMarket)
    assert m2.market_id == "853961"
    assert calls["n"] == 1  # second cell served from the listing cache


def test_listing_paginates_with_after_cursor():
    """A full first page (first=100) is followed via the `after` cursor; the
    target market on page 2 is still discovered."""
    pages: list[dict] = []

    def handler(req):
        pages.append(dict(req.url.params))
        if "after" not in req.url.params:
            fillers = [
                {"id": 1000 + i, "categorySlug": f"other-market-{i}"}
                for i in range(100)
            ]
            return httpx.Response(
                200, json={"success": True, "cursor": "MTA5OQ==", "data": fillers}
            )
        return httpx.Response(200, json={"success": True, "data": [_market()]})

    with httpx.Client(transport=httpx.MockTransport(handler)) as c:
        m = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(m, pf.PredictFunMarket)
    assert len(pages) == 2
    assert pages[1].get("after") == "MTA5OQ=="  # the server cursor, passed back


# ---------------------------------------------------------------------------
# Resolution (probe-real: resolution object / outcome statuses; lookup by the
# discovery-registered market id).
# ---------------------------------------------------------------------------


def _discover_then(mkt: dict, extra_markets: list[dict] | None = None):
    """Register the market via discovery, then return a client that serves the
    (possibly mutated) market by id + listing."""
    with _listing_client([_market()]) as c:
        found = pf.fetch_active_market("btc", interval_min=15, window_start_ts=WS, client=c)
    assert isinstance(found, pf.PredictFunMarket)
    return _listing_client([mkt] + (extra_markets or []))


def test_resolved_outcome_open_when_market_still_trading():
    from cryptobot.venues.types import RESOLUTION_OPEN

    # status=REGISTERED / tradingStatus=OPEN => not settled => PENDING.
    with _discover_then(_market()) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_OPEN
    assert res.source == "predictfun"


def test_resolved_outcome_open_when_market_not_found():
    from cryptobot.venues.types import RESOLUTION_OPEN

    # No discovery happened (registry empty) and the listing has nothing.
    with _listing_client([]) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_OPEN


def test_price_proposed_phase_is_not_settled():
    """Probe fix: tradingStatus=CLOSED during PRICE_PROPOSED/PRICE_DISPUTED is
    NOT resolved — settling there would take a disputable price."""
    from cryptobot.venues.types import RESOLUTION_OPEN

    mkt = _market(status="PRICE_PROPOSED", trading_status="CLOSED")
    with _discover_then(mkt) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_OPEN


def test_resolved_up_wins_from_resolution_object():
    """The REAL settled shape: status=RESOLVED + top-level `resolution` = the
    winning Outcome object + per-outcome status WON/LOST."""
    from cryptobot.venues.types import RESOLUTION_RESOLVED

    mkt = _market(status="RESOLVED", trading_status="CLOSED")
    mkt["resolution"] = {
        "indexSet": 1, "name": "Up", "onChainId": "111", "status": "WON",
    }
    mkt["outcomes"][0]["status"] = "WON"
    mkt["outcomes"][1]["status"] = "LOST"
    with _discover_then(mkt) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_RESOLVED
    assert res.won_up == 1
    assert res.up_price == 1.0
    assert res.down_price == 0.0
    assert res.source == "predictfun"


def test_resolved_down_wins_from_outcome_statuses():
    from cryptobot.venues.types import RESOLUTION_RESOLVED

    mkt = _market(status="RESOLVED", trading_status="CLOSED")
    mkt["outcomes"][0]["status"] = "LOST"
    mkt["outcomes"][1]["status"] = "WON"
    with _discover_then(mkt) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_RESOLVED
    assert res.won_up == 0


def test_resolved_outcome_ambiguous_winner_fails_closed():
    from cryptobot.venues.types import RESOLUTION_OPEN

    # status RESOLVED but NO resolution object / outcome statuses / payouts.
    mkt = _market(status="RESOLVED", trading_status="CLOSED")
    with _discover_then(mkt) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_OPEN  # never guesses -> stays PENDING


def test_resolution_registry_miss_falls_back_to_listing_scan():
    """Restart scenario: no discovery registration, but the market is still in
    the OPEN listing (e.g. just resolved) — best-effort recovery resolves it."""
    from cryptobot.venues.types import RESOLUTION_RESOLVED

    mkt = _market(status="RESOLVED", trading_status="CLOSED")
    mkt["resolution"] = {"indexSet": 2, "name": "Down", "onChainId": "222", "status": "WON"}
    with _listing_client([mkt]) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.status == RESOLUTION_RESOLVED
    assert res.won_up == 0


def test_resolution_uses_market_id_get():
    """After discovery the resolver reads GET /markets/{id} (no slug query
    exists on the real API)."""
    seen: list = []
    mkt = _market(status="RESOLVED", trading_status="CLOSED")
    mkt["resolution"] = {"indexSet": 1, "name": "Up", "onChainId": "111", "status": "WON"}
    with _discover_then(mkt) as _c:
        pass
    with _listing_client([mkt], seen) as c:
        res = pf.fetch_resolved_outcome(
            "btc", interval_min=15, window_start_ts=WS, client=c
        )
    assert res.won_up == 1
    assert seen[0].url.path.rstrip("/").endswith("/markets/853960")
