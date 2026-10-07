"""predict.fun CLOB book fetch: market-id keying (CONFIRMED by the 2026-07-21
mainnet probe — one book per market, quoted from the UP/indexSet-1 view),
token->market resolution through the discovery registry, the synthesized
DOWN-side complement view, the batch orderbooks endpoint, x-api-key auth, the
240/min-safe local bucket, and the order-client DRY-without-creds contract."""

from __future__ import annotations

import httpx
import pytest

from cryptobot.venues.types import BookSnapshot, TokenBucket
from cryptobot_predictfun import clob as pf_clob
from cryptobot_predictfun import markets as pf_markets


@pytest.fixture(autouse=True)
def _clear_registry():
    pf_markets.clear_market_cache()
    yield
    pf_markets.clear_market_cache()


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _orderbook_body(asks, bids, market_id=476) -> dict:
    return {
        "success": True,
        "data": {
            "asks": asks,
            "bids": bids,
            "marketId": market_id,
            "updateTimestampMs": 1784513589410,
            "lastOrderSettled": {
                "id": "1211648317", "kind": "LIMIT", "marketId": market_id,
                "outcome": "No", "price": "0.60", "side": "Bid",
            },
        },
    }


def _register(market_id="476", up_tok="111", down_tok="222", coin="btc", ws=1784500000):
    """Register a discovery mapping the way fetch_active_market does."""
    m = pf_markets.PredictFunMarket(
        coin=coin,
        interval_min=15,
        window_start_ts=ws,
        window_end_ts=ws + 900,
        condition_id="0xcond",
        yes_token_id=up_tok,
        no_token_id=down_tok,
        fee_rate_bps=200.0,
        market_id=str(market_id),
        slug=f"{coin}-updown-15m-{ws}",
    )
    pf_markets._register_market(m, float(ws))
    return m


def _fresh_bucket() -> TokenBucket:
    return TokenBucket(rate_per_s=100.0, burst=10)


# ---- market-keyed fetch (recorder path) -------------------------------------


def test_book_parses_pair_arrays_sorted_ascending():
    body = _orderbook_body(
        asks=[[0.65, 3], [0.62, 5], [0.70, 1]],
        bids=[[0.60, 10], [0.55, 2]],
    )
    with _client(lambda r: httpx.Response(200, json=body)) as c:
        snap = pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket(), now_ts=1.0)
    assert isinstance(snap, BookSnapshot)
    assert [a["price"] for a in snap.asks] == [0.62, 0.65, 0.70]
    assert snap.top_ask == pytest.approx(0.62)
    assert snap.token_id == "476"
    # total ask depth = sum(price*size)
    assert snap.total_ask_depth_usd == pytest.approx(0.62 * 5 + 0.65 * 3 + 0.70 * 1)


def test_empty_ask_book_returns_none():
    body = _orderbook_body(asks=[], bids=[[0.5, 2]])
    with _client(lambda r: httpx.Response(200, json=body)) as c:
        assert pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket()) is None


def test_object_style_levels_also_accepted():
    body = _orderbook_body(
        asks=[{"price": "0.62", "size": "5"}], bids=[{"price": "0.60", "size": "10"}]
    )
    with _client(lambda r: httpx.Response(200, json=body)) as c:
        snap = pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket())
    assert snap is not None
    assert snap.top_ask == pytest.approx(0.62)


def test_malformed_levels_dropped():
    body = _orderbook_body(
        asks=[[0.62, 5], ["x", 1], [-0.1, 5], [0.70, 0]], bids=[]
    )
    with _client(lambda r: httpx.Response(200, json=body)) as c:
        snap = pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket())
    assert snap is not None
    assert [a["price"] for a in snap.asks] == [0.62]


def test_server_429_returns_none():
    with _client(lambda r: httpx.Response(429)) as c:
        assert pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket()) is None


def test_401_returns_none():
    with _client(lambda r: httpx.Response(401, json={"error": "unauthorized"})) as c:
        assert pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket()) is None


def test_auth_header_is_x_api_key(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "sekret", raising=False)
    seen = {}

    def handler(req):
        seen["x-api-key"] = req.headers.get("x-api-key")
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json=_orderbook_body(asks=[[0.62, 5]], bids=[]))

    with _client(handler) as c:
        pf_clob.fetch_book_for_market("476", client=c, bucket=_fresh_bucket())
    assert seen["x-api-key"] == "sekret"
    assert seen["auth"] is None  # probe: Bearer<api key> 401s — never sent


# ---- token-keyed fetch (pipeline path, probe-resolved keying) ---------------


def test_fetch_top_book_up_token_returns_book_as_is():
    _register()
    body = _orderbook_body(asks=[[0.62, 5]], bids=[[0.55, 2]])
    seen: list = []

    def handler(req):
        seen.append(req.url.path)
        return httpx.Response(200, json=body)

    with _client(handler) as c:
        snap = pf_clob.fetch_top_book("111", client=c, bucket=_fresh_bucket())
    # Book GET is keyed by MARKET id (the probe-confirmed keying), snapshot by
    # the requested OUTCOME token (what the pipeline's books dict expects).
    assert len(seen) == 1 and seen[0].endswith("/markets/476/orderbook")
    assert snap is not None
    assert snap.token_id == "111"
    assert snap.top_ask == pytest.approx(0.62)


def test_fetch_top_book_down_token_synthesizes_complement_view():
    """Probe cross-check book (bnb-updown-15m-1784645100, asymmetric spread):
    Down.bestAsk == 1 - best_bid and Down.bestBid == 1 - best_ask exactly."""
    _register()
    body = _orderbook_body(
        asks=[[0.53, 8], [0.56, 20], [0.59, 200], [0.95, 100]],
        bids=[[0.33, 8], [0.29, 20], [0.28, 200], [0.03, 133.33]],
    )
    with _client(lambda r: httpx.Response(200, json=body)) as c:
        snap = pf_clob.fetch_top_book("222", client=c, bucket=_fresh_bucket())
    assert snap is not None
    assert snap.token_id == "222"
    # asks' = complemented bids, ascending: best DOWN ask = 1 - best UP bid.
    assert [a["price"] for a in snap.asks] == pytest.approx([0.67, 0.71, 0.72, 0.97])
    assert [a["size"] for a in snap.asks] == pytest.approx([8, 20, 200, 133.33])
    # bids' = complemented asks (stored ascending like every BookSnapshot).
    assert [b["price"] for b in snap.bids] == pytest.approx([0.05, 0.41, 0.44, 0.47])
    assert max(b["price"] for b in snap.bids) == pytest.approx(0.47)  # best DOWN bid


def test_complement_rounds_at_decimal_precision():
    # complement = (10^2 - round(p*10^2)) / 10^2 — exact at the venue tick.
    assert pf_clob._complement_price(0.53) == pytest.approx(0.47)
    assert pf_clob._complement_price(0.005) == pytest.approx(1.0)  # rounds to tick
    assert pf_clob._complement_price(0.995) == 0.0  # dropped by callers (<= 0)


def test_fetch_top_book_unknown_token_fails_closed_without_request():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(200, json=_orderbook_body(asks=[[0.5, 1]], bids=[]))

    with _client(handler) as c:
        snap = pf_clob.fetch_top_book("999", client=c, bucket=_fresh_bucket())
    assert snap is None       # unregistered token -> no market/side guess
    assert calls["n"] == 0    # and no network spend


def test_fetch_books_batch_one_request_serves_both_sides():
    _register()
    seen: list = []

    def handler(req):
        seen.append(req)
        assert req.url.path.endswith("/markets/orderbooks")
        return httpx.Response(
            200,
            json={"success": True, "data": [
                _orderbook_body(asks=[[0.62, 5]], bids=[[0.55, 2]])["data"],
            ]},
        )

    with _client(handler) as c:
        out = pf_clob.fetch_books_batch(["111", "222"], client=c, bucket=_fresh_bucket(), now_ts=1.0)
    assert len(seen) == 1  # both sides of one market share ONE batch GET
    assert seen[0].url.params.get_list("ids") == ["476"]
    assert set(out.keys()) == {"111", "222"}
    assert out["111"].top_ask == pytest.approx(0.62)
    assert out["222"].top_ask == pytest.approx(1.0 - 0.55)


def test_fetch_books_batch_multiple_markets():
    _register(market_id="476", up_tok="111", down_tok="222", ws=1784500000)
    _register(market_id="477", up_tok="333", down_tok="444", coin="eth", ws=1784500000)

    def handler(req):
        assert req.url.params.get_list("ids") == ["476", "477"]
        return httpx.Response(
            200,
            json={"success": True, "data": [
                _orderbook_body(asks=[[0.60, 5]], bids=[], market_id=476)["data"],
                _orderbook_body(asks=[[0.70, 5]], bids=[], market_id=477)["data"],
            ]},
        )

    with _client(handler) as c:
        out = pf_clob.fetch_books_batch(["111", "333"], client=c, bucket=_fresh_bucket(), now_ts=1.0)
    assert out["111"].top_ask == pytest.approx(0.60)
    assert out["333"].top_ask == pytest.approx(0.70)


def test_fetch_books_batch_failure_returns_none():
    _register()
    with _client(lambda r: httpx.Response(500)) as c:
        out = pf_clob.fetch_books_batch(["111"], client=c, bucket=_fresh_bucket())
    assert out is None  # pipeline falls back to per-token GETs


def test_fetch_market_books_batch_keys_by_market_id():
    def handler(req):
        assert req.url.path.endswith("/markets/orderbooks")
        return httpx.Response(
            200,
            json={"success": True, "data": [
                _orderbook_body(asks=[[0.60, 5]], bids=[], market_id=1)["data"],
                _orderbook_body(asks=[[0.70, 5]], bids=[], market_id=2)["data"],
            ]},
        )

    with _client(handler) as c:
        out = pf_clob.fetch_market_books_batch(["1", "2"], client=c, bucket=_fresh_bucket(), now_ts=1.0)
    assert set(out.keys()) == {"1", "2"}
    assert out["1"].top_ask == pytest.approx(0.60)
    assert out["2"].top_ask == pytest.approx(0.70)


def test_scanner_bucket_rate_sum_fits_server_budget():
    # Probe: ratelimit-policy 240;w=60 per API key, SHARED with the recorder
    # process. The scanner process draws on BOTH the decision and prefetch
    # buckets, so the SUM of their steady rates (not either alone) must stay
    # within the scanner's 120/min half. Summing across every scanner bucket —
    # rather than pinning one — means a future duplicated full-rate bucket
    # cannot false-pass this budget invariant.
    scanner_buckets = [pf_clob.decision_bucket, pf_clob.prefetch_bucket]
    scanner_per_min = sum(b.rate for b in scanner_buckets) * 60
    assert scanner_per_min <= 240 / 2
    # The recorder is a SEPARATE process on the same key; its _default_bucket
    # takes the other 120/min half.
    assert pf_clob._default_bucket.rate * 60 <= 240 / 2
    # Combined worst case across both processes stays within the whole key.
    assert scanner_per_min + pf_clob._default_bucket.rate * 60 <= 240


def test_split_buckets_independent_and_share_one_429_backoff():
    # The decision + prefetch buckets are independent on tokens (a prefetch
    # burst cannot starve the inline decision/stop reads) but share ONE
    # process-wide 429 backoff (a 429 on either is a whole-key signal that must
    # pause both — review finding #31).
    backoff = pf_clob._SharedServer429Backoff()
    dec = pf_clob._ScannerBookBucket(rate_per_s=1.4, burst=12, backoff=backoff)
    pre = pf_clob._ScannerBookBucket(rate_per_s=0.6, burst=3, backoff=backoff)

    # Exhaust the prefetch bucket; the decision bucket is untouched.
    drained = 0
    while pre.take(1):
        drained += 1
    assert drained >= 1
    assert pre.take(1) is False
    assert dec.take(1) is True  # NOT starved by the prefetch exhaustion

    # A 429 seen on the prefetch bucket pauses BOTH.
    pre.notify_server_429(cooldown_s=30.0)
    assert dec.take(1) is False
    assert pre.take(1) is False

    # Symmetric: a fresh pair, 429 on the decision bucket pauses the prefetch.
    backoff2 = pf_clob._SharedServer429Backoff()
    dec2 = pf_clob._ScannerBookBucket(rate_per_s=1.4, burst=12, backoff=backoff2)
    pre2 = pf_clob._ScannerBookBucket(rate_per_s=0.6, burst=3, backoff=backoff2)
    dec2.notify_server_429(cooldown_s=30.0)
    assert pre2.take(1) is False


def test_server_429_on_book_read_trips_shared_backoff():
    # A live 429 on a book GET (via the shared decision bucket) must pause the
    # sibling prefetch bucket too — proves _get_json's notify_server_429 routes
    # onto the shared backoff, not an isolated per-bucket one.
    backoff = pf_clob._SharedServer429Backoff()
    dec = pf_clob._ScannerBookBucket(rate_per_s=1.4, burst=12, backoff=backoff)
    pre = pf_clob._ScannerBookBucket(rate_per_s=0.6, burst=3, backoff=backoff)
    _register()
    with _client(lambda r: httpx.Response(429)) as c:
        out = pf_clob.fetch_top_book("111", client=c, bucket=dec)
    assert out is None
    assert pre.take(1) is False  # the whole-key pause reached the prefetch bucket


# ---- per-request book-read timeout knob (pooled-client path) ----------------


def _extract_timeout(req) -> dict | None:
    return req.extensions.get("timeout")


def test_pooled_get_honors_read_timeout_override():
    # A pooled (injected) client normally uses its client-level timeout; the
    # read_timeout override must reach client.get so the PF knob is live even on
    # the pooled decision path. connect stays 2.0s (dead-socket cap survives).
    _register()
    seen: dict = {}

    def handler(req):
        seen["timeout"] = _extract_timeout(req)
        return httpx.Response(200, json=_orderbook_body(asks=[[0.62, 5]], bids=[]))

    knob = httpx.Timeout(0.8, connect=2.0)
    with _client(handler) as c:
        pf_clob.fetch_top_book("111", client=c, bucket=_fresh_bucket(), read_timeout=knob)
    assert seen["timeout"] == {
        "connect": 2.0, "read": 0.8, "write": 0.8, "pool": 0.8
    }


def test_pooled_get_none_read_timeout_preserves_client_timeout():
    # read_timeout=None (the unset default) is BYTE-IDENTICAL to today: the
    # pooled client keeps its own client-level timeout, nothing per-request.
    _register()
    seen: dict = {}

    def handler(req):
        seen["timeout"] = _extract_timeout(req)
        return httpx.Response(200, json=_orderbook_body(asks=[[0.62, 5]], bids=[]))

    c = httpx.Client(
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(3.0, connect=2.0),
    )
    with c:
        pf_clob.fetch_top_book("111", client=c, bucket=_fresh_bucket())
    assert seen["timeout"] == {
        "connect": 2.0, "read": 3.0, "write": 3.0, "pool": 3.0
    }


def test_batch_get_honors_read_timeout_override():
    _register()
    seen: dict = {}

    def handler(req):
        seen["timeout"] = _extract_timeout(req)
        return httpx.Response(
            200,
            json={"success": True, "data": [
                _orderbook_body(asks=[[0.62, 5]], bids=[[0.55, 2]])["data"],
            ]},
        )

    knob = httpx.Timeout(0.9, connect=2.0)
    with _client(handler) as c:
        pf_clob.fetch_books_batch(
            ["111", "222"], client=c, bucket=_fresh_bucket(), read_timeout=knob
        )
    assert seen["timeout"] == {
        "connect": 2.0, "read": 0.9, "write": 0.9, "pool": 0.9
    }


def _bare_order_client():
    """A PredictFunOrderClient shell WITHOUT __init__ (no predict_sdk / creds).

    _await_terminal_state depends only on self._fetch_order_state (set below),
    the fill-event waiter fields (None here = the flag-OFF poll-only path, the
    __init__ default), and the module time.sleep, so the bare shell exercises
    the loop directly.
    """
    obj = pf_clob.PredictFunOrderClient.__new__(pf_clob.PredictFunOrderClient)
    obj._fill_event_waiter = None
    obj._fill_event_wait_s = 0.0
    return obj


def test_await_terminal_never_terminal_keeps_all_fetches_drops_last_sleep(monkeypatch):
    obj = _bare_order_client()
    fetches = {"n": 0}

    def fake_fetch(_h):
        fetches["n"] += 1
        return {"status": "OPEN"}

    obj._fetch_order_state = fake_fetch
    sleeps = {"n": 0}
    monkeypatch.setattr(pf_clob.time, "sleep", lambda *_: sleeps.__setitem__("n", sleeps["n"] + 1))

    state = pf_clob.PredictFunOrderClient._await_terminal_state(obj, "0xabc")
    # ALL fetches still fire (fill-capture window unchanged); only the trailing
    # sleep after the final fetch is skipped.
    assert fetches["n"] == pf_clob._FILL_POLL_MAX
    assert sleeps["n"] == pf_clob._FILL_POLL_MAX - 1
    assert state == {"status": "OPEN"}  # returned state identical to the old loop


def test_await_terminal_on_first_poll_zero_sleeps(monkeypatch):
    obj = _bare_order_client()
    obj._fetch_order_state = lambda _h: {"status": "FILLED", "amountFilled": "0"}
    sleeps = {"n": 0}
    monkeypatch.setattr(pf_clob.time, "sleep", lambda *_: sleeps.__setitem__("n", sleeps["n"] + 1))

    state = pf_clob.PredictFunOrderClient._await_terminal_state(obj, "0xabc")
    assert sleeps["n"] == 0
    assert state["status"] == "FILLED"


def test_await_terminal_unreadable_state_returns_none(monkeypatch):
    # Every poll unreadable -> None (the posted-but-unreadable branch of the
    # callers keys off this; invariant BUY->success / stop->fail live there).
    obj = _bare_order_client()
    obj._fetch_order_state = lambda _h: None
    monkeypatch.setattr(pf_clob.time, "sleep", lambda *_: None)
    assert pf_clob.PredictFunOrderClient._await_terminal_state(obj, "0xabc") is None


# ---- owned order-client transport (warm pool + http2 fail-soft) -------------


def test_owned_order_client_uses_warm_pool_and_http2(monkeypatch):
    captured: list[dict] = []

    class _FakeClient:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    monkeypatch.setattr(pf_clob.httpx, "Client", _FakeClient)
    pf_clob._build_owned_order_client(5.0)

    assert len(captured) == 1
    kw = captured[0]
    assert kw["http2"] is True
    assert kw["timeout"] == 5.0
    assert kw["limits"].keepalive_expiry == 65.0
    assert kw["limits"].max_connections == 20
    assert kw["limits"].max_keepalive_connections == 10


def test_owned_order_client_falls_back_to_h1_when_h2_missing(monkeypatch):
    captured: list[dict] = []

    class _FakeClient:
        def __init__(self, **kwargs):
            captured.append(kwargs)
            if kwargs.get("http2"):
                raise ImportError("h2 extra not installed")

    monkeypatch.setattr(pf_clob.httpx, "Client", _FakeClient)
    # Boot-safe: must NOT raise despite the missing h2 extra.
    pf_clob._build_owned_order_client(5.0)

    assert captured[0]["http2"] is True          # tried h2 first
    assert "http2" not in captured[1]            # fell back to plain HTTP/1.1
    assert captured[1]["limits"].keepalive_expiry == 65.0


# ---- order client -----------------------------------------------------------


def test_order_client_dry_without_creds_raises_unavailable():
    # DRY mode / no private key => construction refuses (LIVE cells then SKIP);
    # this is the fail-soft the boot path catches to run DRY-only.
    with pytest.raises(pf_clob.PredictFunClientUnavailable):
        pf_clob.PredictFunOrderClient(
            api_base="https://api.predict.fun/v1", api_key="", private_key=""
        )
