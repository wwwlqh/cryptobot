"""predict.fun LIVE order client: /auth JWT flow, POST /v1/orders wire shape
(strategy MARKET + pricePerShare cap; FAK entry / FOK stop), fill read-back
via GET /orders/{hash}, 401 -> re-auth-once, reject classification, and the
fail-closed unregistered-token refusal.

Signing uses the REAL predict_sdk (a main dependency) with a throwaway test
key on the BNB testnet chain id — EIP-712 signing is fully offline. All HTTP
goes through httpx.MockTransport.
"""

from __future__ import annotations

import base64
import json
import time

import httpx
import pytest

from cryptobot_predictfun import markets as pf_markets
from cryptobot_predictfun.auth import _jwt_expiry_ts
from cryptobot_predictfun.clob import PredictFunOrderClient

# Throwaway key (never funded anywhere) — valid secp256k1 scalar.
_TEST_KEY = "0x" + "11" * 32
_CHAIN = 97  # BNB testnet addresses ship in predict_sdk

_UP_TOKEN = "111"
_DOWN_TOKEN = "222"


@pytest.fixture(autouse=True)
def _clear_registry():
    pf_markets.clear_market_cache()
    yield
    pf_markets.clear_market_cache()


def _register_market() -> None:
    m = pf_markets.PredictFunMarket(
        coin="btc",
        interval_min=15,
        window_start_ts=1_784_500_000,
        window_end_ts=1_784_500_900,
        condition_id="0xcond",
        yes_token_id=_UP_TOKEN,
        no_token_id=_DOWN_TOKEN,
        fee_rate_bps=200.0,
        market_id="476",
        slug="btc-updown-15m-1784500000",
    )
    pf_markets._register_market(m, time.time())


def _fake_jwt(ttl_s: float = 3600.0) -> str:
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": time.time() + ttl_s}).encode()
    ).decode().rstrip("=")
    return f"eyJhbGciOiJIUzI1NiJ9.{payload}.c2ln"


class _Venue:
    """Scripted predict.fun REST endpoint set with request recording."""

    def __init__(self):
        self.requests: list[tuple[str, str, dict | None]] = []
        self.auth_flows = 0
        self.order_status = "FILLED"
        self.amount_filled_wei: int | None = None
        self.post_responses: list[httpx.Response] = []
        # HTTP status for GET /orders/{hash}; != 200 => state unreadable (None).
        self.order_state_status = 200
        # Extra keys merged into the GET /orders/{hash} `data` object.
        self.order_state_extra: dict = {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = None
        if request.content:
            try:
                body = json.loads(request.content)
            except ValueError:
                body = None
        self.requests.append((request.method, path, body))

        if path.endswith("/auth/message") and request.method == "GET":
            return httpx.Response(
                200, json={"success": True, "data": {"message": "sign me: nonce1"}}
            )
        if path.endswith("/auth") and request.method == "POST":
            self.auth_flows += 1
            return httpx.Response(
                200, json={"success": True, "data": {"token": _fake_jwt()}}
            )
        if path.endswith("/orders") and request.method == "POST":
            if self.post_responses:
                return self.post_responses.pop(0)
            return httpx.Response(
                201,
                json={
                    "success": True,
                    "data": {"code": "OK", "orderId": "9001", "orderHash": "0xhash1"},
                },
            )
        if "/orders/" in path and request.method == "DELETE":
            return httpx.Response(200, json={"success": True})
        if "/orders/" in path and request.method == "GET":
            filled = self.amount_filled_wei
            if self.order_state_status != 200:
                return httpx.Response(
                    self.order_state_status, json={"success": False}
                )
            data = {
                "status": self.order_status,
                "amount": "2000000000000000000",
                "amountFilled": str(filled) if filled is not None else "0",
            }
            data.update(self.order_state_extra)
            return httpx.Response(200, json={"success": True, "data": data})
        return httpx.Response(404, json={"success": False})


def _mk_client(venue: _Venue) -> PredictFunOrderClient:
    return PredictFunOrderClient(
        api_base="https://api-testnet.predict.fun/v1",
        api_key="test-key",
        private_key=_TEST_KEY,
        chain_id=_CHAIN,
        client=httpx.Client(transport=httpx.MockTransport(venue.handler)),
    )


class _Signal:
    poly_token_id = _UP_TOKEN
    bet_size = 1.30
    limit_price = 0.65   # signed cap (walked limit + FAK pad)
    walked_vwap = 0.64   # expected fill — what the SHARE COUNT is sized at
    fill_price = 0.63    # top_ask at scan (sizing fallback)
    fee_rate_bps = 200.0


def test_buy_full_fill_wire_shape_and_result():
    _register_market()
    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18  # 2.0 shares = $1.30 / 0.65
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-1")
    finally:
        client.close()

    assert res.success is True
    assert res.order_id == "0xhash1"
    assert res.client_order_id == "coid-1"
    assert res.filled_size_tokens == pytest.approx(2.0)
    assert res.filled_price_vwap == pytest.approx(0.65)  # booked at the cap
    assert res.filled_value_usd == pytest.approx(1.30)
    assert res.limit_price == pytest.approx(0.65)
    assert res.sign_ms is not None and res.post_ms is not None

    posts = [b for m, p, b in venue.requests if m == "POST" and p.endswith("/orders")]
    assert len(posts) == 1
    data = posts[0]["data"]
    # Wire invariants: MARKET-only, FAK entry (no FOK), cap price in wei,
    # slippageBps + isPostOnly sent explicitly (documented body fields).
    assert data["strategy"] == "MARKET"
    assert data["isFillOrKill"] is False
    assert data["slippageBps"] == "0"
    assert data["isPostOnly"] is False
    assert data["pricePerShare"] == str(65 * 10**16)
    order = data["order"]
    # EOA account: maker == signer == the key's address; BUY side; EOA sigtype.
    assert order["maker"] == order["signer"]
    assert order["side"] == 0 and order["signatureType"] == 0
    assert order["tokenId"] == _UP_TOKEN
    assert order["feeRateBps"] == "200"
    assert isinstance(order["expiration"], int)
    assert order["signature"].startswith("0x")
    # BUY amounts: the share count is sized at the EXPECTED fill (walked_vwap
    # 0.64), NOT the 0.65 cap — $1.30 / 0.64 = 2.03125 shares — so a match at
    # the ask deploys the whole stake. makerAmount escrows that size AT the cap
    # (the accepted ~1.5% worst case if every share fills at the cap).
    # predict_sdk truncates the signed quantity to 5 significant digits
    # (2.03125 -> 2.0312 shares).
    assert int(order["takerAmount"]) == 2031200000000000000
    assert int(order["makerAmount"]) == 1320280000000000000

    # The Bearer + x-api-key headers went out on the POST.
    # (MockTransport keeps headers on the recorded request objects only; the
    # auth flow itself proves the JWT was fetched.)
    assert venue.auth_flows == 1


def test_buy_fak_killed_unfilled_maps_no_liquidity():
    _register_market()
    venue = _Venue()
    venue.order_status = "CANCELLED"
    venue.amount_filled_wei = 0
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-2")
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "no_liquidity"


def test_buy_partial_fill_books_actual_size():
    _register_market()
    venue = _Venue()
    venue.order_status = "CANCELLED"  # FAK: partial then killed remainder
    venue.amount_filled_wei = 1 * 10**18
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-3")
    finally:
        client.close()
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(1.0)
    assert res.filled_value_usd == pytest.approx(0.65)


def test_stop_sell_fok_filled_and_wire():
    _register_market()
    venue = _Venue()
    venue.order_status = "FILLED"
    venue.amount_filled_wei = 3 * 10**18
    client = _mk_client(venue)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(3.0)
    assert res.filled_price_vwap == pytest.approx(0.40)

    posts = [b for m, p, b in venue.requests if m == "POST" and p.endswith("/orders")]
    assert len(posts) == 1
    data = posts[0]["data"]
    assert data["strategy"] == "MARKET"
    assert data["isFillOrKill"] is True  # FOK — atomic full fill or kill
    assert data["isPostOnly"] is False
    assert data["slippageBps"] == "0"
    order = data["order"]
    assert order["side"] == 1
    assert order["tokenId"] == _UP_TOKEN
    # SELL amounts: offer 3.0 shares for >= 3.0 * 0.40 collateral.
    assert int(order["makerAmount"]) == pytest.approx(3 * 10**18, rel=1e-6)
    assert abs(int(order["takerAmount"]) - int(3 * 0.40 * 1e18)) <= 10**16


def test_stop_sell_fok_kill_maps_no_liquidity():
    _register_market()
    venue = _Venue()
    venue.order_status = "CANCELLED"
    venue.amount_filled_wei = 0
    client = _mk_client(venue)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "no_liquidity"


def test_401_reauths_once_then_succeeds():
    _register_market()
    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    venue.post_responses = [httpx.Response(401, json={"success": False})]
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-4")
    finally:
        client.close()
    assert res.success is True
    # Two /auth flows: the initial token + the forced refresh after the 401.
    assert venue.auth_flows == 2
    posts = [1 for m, p, _ in venue.requests if m == "POST" and p.endswith("/orders")]
    assert len(posts) == 2


def test_reject_classification_insufficient_funds():
    _register_market()
    venue = _Venue()
    venue.post_responses = [
        httpx.Response(400, json={"success": False, "error": "insufficient balance"})
    ]
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-5")
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "insufficient_funds"


def test_unregistered_token_refuses_to_sign():
    # NO registry entry: the client must fail closed without any /orders POST.
    venue = _Venue()
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-6")
    finally:
        client.close()
    assert res.success is False
    assert "registry" in (res.error or "")
    assert not [1 for m, p, _ in venue.requests if p.endswith("/orders")]


def test_reconciler_hook_maps_state_to_poly_shape():
    venue = _Venue()
    venue.order_status = "FILLED"
    venue.amount_filled_wei = 2 * 10**18
    client = _mk_client(venue)
    try:
        payload = client.fetch_order_payload("0xhash1")
    finally:
        client.close()
    assert payload == {"status": "FILLED", "size_matched": pytest.approx(2.0)}


# ---- share sizing: expected fill, not the signed cap -----------------------


# predict_sdk truncates the signed quantity to 5 significant digits, so the
# posted share count matches the sizing math to ~1e-5 relative, not exactly.
_SDK_QTY_TOL = 1e-4


def _posted_shares(venue: _Venue) -> float:
    posts = [b for m, p, b in venue.requests if m == "POST" and p.endswith("/orders")]
    return int(posts[-1]["data"]["order"]["takerAmount"]) / 1e18


def _place(venue: _Venue, signal) -> None:
    client = _mk_client(venue)
    try:
        client.place_gtc_limit_order(signal, client_order_id="coid-size")
    finally:
        client.close()


def test_shares_sized_at_expected_fill_deploys_the_whole_stake():
    # The venue matches a MARKET order at the RESTING ASK, not at our padded
    # cap. Sizing off the cap deployed stake * ask/cap (~1.5% short every bet);
    # sizing off walked_vwap deploys the stake (to the wei floor).
    _register_market()
    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    _place(venue, _Signal())
    shares = _posted_shares(venue)
    assert shares == pytest.approx(1.30 / 0.64, rel=_SDK_QTY_TOL)
    # Cash actually deployed when the book fills at the expected ask.
    assert shares * 0.64 == pytest.approx(1.30, abs=0.001)
    # Worst case (every share fills AT the cap) overspends, bounded by the pad.
    assert shares * 0.65 == pytest.approx(1.3203, abs=0.001)


def test_shares_fall_back_to_top_ask_without_walked_vwap():
    _register_market()

    class _NoVwap(_Signal):
        walked_vwap = None

    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    _place(venue, _NoVwap())
    assert _posted_shares(venue) == pytest.approx(1.30 / 0.63, rel=_SDK_QTY_TOL)


def test_expected_fill_above_cap_falls_back_to_cap_sizing():
    # A stale/absurd expectation must never size ABOVE the signed cap — that
    # only under-fills. Degrade to the old cap-sized quantity.
    _register_market()

    class _HighVwap(_Signal):
        walked_vwap = 0.70

    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    _place(venue, _HighVwap())
    assert _posted_shares(venue) == pytest.approx(1.30 / 0.65, rel=_SDK_QTY_TOL)


# ---- fetch_order_fill (pipeline _immediate_fill_for_stop hook) -------------


def test_fetch_order_fill_parses_venue_state():
    venue = _Venue()
    venue.order_status = "FILLED"
    venue.amount_filled_wei = 2 * 10**18
    venue.order_state_extra = {"pricePerShare": str(65 * 10**16)}
    client = _mk_client(venue)
    try:
        size, price, ts = client.fetch_order_fill(
            "0xhash1", intent_amount_usd=1.30, intent_price=0.65
        )
    finally:
        client.close()
    assert size == pytest.approx(2.0)
    assert price == pytest.approx(0.65)
    assert ts is not None


def test_fetch_order_fill_reads_price_from_signed_order_amounts():
    venue = _Venue()
    venue.order_status = "FILLED"
    venue.amount_filled_wei = 2 * 10**18
    venue.order_state_extra = {
        "order": {
            "makerAmount": str(13 * 10**17),   # $1.30 collateral
            "takerAmount": str(2 * 10**18),    # for 2.0 shares => 0.65
        }
    }
    client = _mk_client(venue)
    try:
        size, price, _ = client.fetch_order_fill("0xhash1")
    finally:
        client.close()
    assert size == pytest.approx(2.0)
    assert price == pytest.approx(0.65)


def test_fetch_order_fill_falls_back_to_intent_price():
    venue = _Venue()
    venue.order_status = "FILLED"
    venue.amount_filled_wei = 2 * 10**18
    client = _mk_client(venue)
    try:
        size, price, _ = client.fetch_order_fill("0xhash1", intent_price=0.65)
    finally:
        client.close()
    assert size == pytest.approx(2.0)
    assert price == pytest.approx(0.65)


def test_fetch_order_fill_returns_none_on_api_error_or_no_fill():
    venue = _Venue()
    venue.order_state_status = 500
    client = _mk_client(venue)
    try:
        assert client.fetch_order_fill("0xhash1", intent_price=0.65) == (
            None, None, None
        )
        assert client.fetch_order_fill("") == (None, None, None)
        venue.order_state_status = 200
        venue.amount_filled_wei = 0
        assert client.fetch_order_fill("0xhash1", intent_price=0.65) == (
            None, None, None
        )
        # Fill with NO derivable price: never persist size without price.
        venue.amount_filled_wei = 2 * 10**18
        assert client.fetch_order_fill("0xhash1") == (None, None, None)
    finally:
        client.close()


def test_cancel_order_deletes_by_hash():
    venue = _Venue()
    client = _mk_client(venue)
    try:
        assert client.cancel_order("0xhash1") is True
        assert client.cancel_order("") is False
    finally:
        client.close()
    deletes = [p for m, p, _ in venue.requests if m == "DELETE"]
    assert deletes == ["/v1/orders/0xhash1"]


def test_jwt_expiry_claim_parse():
    tok = _fake_jwt(ttl_s=1234.0)
    exp = _jwt_expiry_ts(tok)
    assert exp is not None
    assert exp - time.time() == pytest.approx(1234.0, abs=5.0)
    assert _jwt_expiry_ts("not-a-jwt") is None


# ---- fill-poll invariants under the trailing-sleep skip ---------------------


def test_buy_posted_but_state_unreadable_stays_success_without_metadata(monkeypatch):
    # Every fill-poll unreadable (GET /orders/{hash} 500): the order EXISTS on
    # the venue, so a failure return here would let the pipeline re-bet the slot
    # (double-spend). Must return success WITHOUT fill metadata.
    _register_market()
    monkeypatch.setattr("cryptobot_predictfun.clob.time.sleep", lambda *_: None)
    venue = _Venue()
    venue.order_state_status = 500
    client = _mk_client(venue)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-u1")
    finally:
        client.close()
    assert res.success is True
    assert res.order_id == "0xhash1"
    assert res.filled_size_tokens is None
    assert res.filled_price_vwap is None
    assert res.filled_value_usd is None


def test_stop_sell_posted_but_state_unreadable_stays_failure(monkeypatch):
    # Asymmetric to the BUY: a posted-but-unreadable stop SELL returns
    # success=False (a falsely-booked sell would corrupt the ledger; the worst
    # case of a hidden fill is one rejected retry).
    _register_market()
    monkeypatch.setattr("cryptobot_predictfun.clob.time.sleep", lambda *_: None)
    venue = _Venue()
    venue.order_state_status = 500
    client = _mk_client(venue)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "unknown"


def test_fok_stop_sell_success_only_on_filled(monkeypatch):
    # Non-terminal readable status at budget exhaustion (OPEN) must NOT book a
    # fill — the FOK SELL succeeds ONLY on status FILLED.
    _register_market()
    monkeypatch.setattr("cryptobot_predictfun.clob.time.sleep", lambda *_: None)
    venue = _Venue()
    venue.order_status = "OPEN"  # never terminal within budget
    venue.amount_filled_wei = 0
    client = _mk_client(venue)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is False


def test_buy_post_transport_error_is_network_failure_not_swallowed_success():
    # A mid-flight connection reset / Cloudflare GOAWAY on the order POST must
    # surface as success=False (error_kind network) — the transport swap
    # (http2 + keepalive pool) provides NO new path to reporting a genuinely
    # accepted BUY as failure NOR to swallowing a transport error as success.
    _register_market()
    venue = _Venue()

    def handler(req):
        if req.url.path.endswith("/orders") and req.method == "POST":
            raise httpx.ConnectError("connection reset / GOAWAY")
        return venue.handler(req)

    client = PredictFunOrderClient(
        api_base="https://api-testnet.predict.fun/v1",
        api_key="test-key",
        private_key=_TEST_KEY,
        chain_id=_CHAIN,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="coid-net")
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "network"


def test_owned_order_client_builds_warm_keepalive_pool():
    # No injected client => the client owns a pool with the 65s keepalive so the
    # 20s ping actually keeps the order socket hot (httpx default 5s expiry
    # closed it between pings). close() still owns/closes it.
    client = PredictFunOrderClient(
        api_base="https://api-testnet.predict.fun/v1",
        api_key="test-key",
        private_key=_TEST_KEY,
        chain_id=_CHAIN,
    )
    try:
        assert client._owns_client is True
        pool = client._client._transport._pool
        assert pool._keepalive_expiry == 65.0
        assert pool._max_connections == 20
        assert pool._max_keepalive_connections == 10
        assert pool._http2 is True  # h2 extra present in this test env
    finally:
        client.close()