"""predict.fun predictWalletEvents fill-push: WS-event-first terminal-state
wait in front of the UNCHANGED GET /orders/{hash} poll
(CRYPTOBOT_PREDICTFUN_WS_WALLET_EVENTS, default OFF).

Covers the round-1 binding conditions: a pushed Success resolves the wait with
ZERO GETs and books shares AT THE CAP price (executedPriceWei/executedValueWei
never read); a partial-settlement-then-kill books the partial; a kill NEVER
zero-fills on its own say-so and a Success with an unreadable size never
resolves any total (poll authoritative, never a guessed/incomplete figure);
a stateless terminal that beats the waiter registration short-circuits the
wait via the buffered marker; a dead/never-started mirror skips the wait
outright; WS timeout falls through to the byte-identical poll; the
JWT-rotation hook resubscribes the wallet topic; flag OFF leaves today's poll
path with no WS object anywhere; and the BUY/SELL unreadable-state asymmetry
survives with WS armed.

All WS tests are network-free (frames fed into ``_handle_message`` /
``_handle_wallet_event``); order HTTP goes through httpx.MockTransport with
the REAL predict_sdk signing offline on the BNB testnet chain id.
"""

from __future__ import annotations

import base64
import json
import threading
import time
from pathlib import Path

import httpx
import pytest

from cryptobot import config
from cryptobot_predictfun import markets as pf_markets
from cryptobot_predictfun.clob import PredictFunOrderClient
from cryptobot_predictfun.ws_mirror import (
    PredictFunBookWsMirror,
    _reduce_wallet_event,
)

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


@pytest.fixture
def no_poll_sleep(monkeypatch):
    # The poll fallback sleeps 0.35s between GETs — the unreadable-state tests
    # exhaust the full budget and would pay ~2s of wall clock each. NOT
    # autouse: the threading tests below need the real time.sleep for their
    # registration-wait loop (clob's ``time`` is the shared module).
    monkeypatch.setattr("cryptobot_predictfun.clob.time.sleep", lambda *_: None)


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
        self.order_state_status = 200

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
            return httpx.Response(
                201,
                json={
                    "success": True,
                    "data": {"code": "OK", "orderId": "9001", "orderHash": "0xhash1"},
                },
            )
        if "/orders/" in path and request.method == "GET":
            if self.order_state_status != 200:
                return httpx.Response(
                    self.order_state_status, json={"success": False}
                )
            filled = self.amount_filled_wei
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "status": self.order_status,
                        "amount": "2000000000000000000",
                        "amountFilled": str(filled) if filled is not None else "0",
                    },
                },
            )
        return httpx.Response(404, json={"success": False})

    def state_gets(self) -> list[str]:
        return [p for m, p, _ in self.requests if m == "GET" and "/orders/" in p]


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
    limit_price = 0.65
    fill_price = 0.63
    fee_rate_bps = 200.0


class _FakeWs:
    """Records everything sent (subscribe/heartbeat); recv never used here."""

    def __init__(self):
        self.sent: list[dict] = []

    def send(self, raw):
        self.sent.append(json.loads(raw))


def _event(etype: str, order_hash: str = "0xhash1", fill: dict | None = None) -> dict:
    """A predictWalletEvents payload (the frame ``data`` body) per the
    subscription-topics doc: base fields + the optional ``fill`` object."""
    ev = {
        "type": etype,
        "orderId": "9001",
        "orderHash": order_hash,
        "walletAddress": "0xwallet",
        "timestamp": 1736696400000,
        "details": {
            "marketId": 476,
            "outcomeIndex": 0,
            "outcome": "YES",
            "quantity": "2.000",
            "quantityFilled": "0.000",
            "price": "0.620",
            "strategyType": "LIMIT",
        },
    }
    if fill is not None:
        ev["fill"] = fill
    return ev


def _wallet_frame(event: dict, jwt: str = "jwt-abc") -> str:
    return json.dumps(
        {"type": "M", "topic": f"predictWalletEvents/{jwt}", "data": event}
    )


# ---- event reducer (poll-ordering parity invariants) ------------------------


def test_reduce_success_resolves_filled_with_settled_size_only():
    ev = _event(
        "orderTransactionSuccess",
        fill={
            # Absurd price/value on purpose: they must never be read (their
            # units are pinned by no validation; booking stays at the cap).
            "executedPriceWei": "999999999999999999999999",
            "executedSizeWei": str(2 * 10**18),
            "executedValueWei": "1",
        },
    )
    settled, state, terminal = _reduce_wallet_event(ev, 0)
    assert settled == 2 * 10**18
    assert terminal is True
    assert state == {"status": "FILLED", "amountFilled": str(2 * 10**18)}
    # Poll-shaped and NOTHING more: status + shares at most (invariant #5).
    assert set(state) == {"status", "amountFilled"}


def test_reduce_success_accumulates_across_settlements():
    ev = _event(
        "orderTransactionSuccess", fill={"executedSizeWei": str(1 * 10**18)}
    )
    settled, state, _ = _reduce_wallet_event(ev, 5 * 10**17)
    assert settled == 15 * 10**17
    assert state == {"status": "FILLED", "amountFilled": str(15 * 10**17)}


def test_reduce_kill_books_accumulated_partial_first():
    # Poll ordering: an executed partial beats the kill status (the poll books
    # amountFilled > 0 before it ever looks at CANCELLED).
    settled, state, terminal = _reduce_wallet_event(
        _event("orderCancelled"), 1 * 10**18
    )
    assert settled == 1 * 10**18
    assert terminal is True
    assert state == {"status": "CANCELLED", "amountFilled": str(1 * 10**18)}


def test_reduce_kill_with_any_fill_field_resolves_nothing():
    # Kills carry no fill object per the docs, so an executed field on one
    # (zero included) is off-contract and never trusted for a zero-book: had
    # a reconnect gap swallowed the earlier Success (accumulator empty), an
    # explicit "0" would zero-book a real partial and the success=False BUY
    # return would re-bet the slot (double-spend). The poll decides.
    for fill in ({"executedSizeWei": "0"}, {"executedSizeWei": str(10**18)}):
        settled, state, terminal = _reduce_wallet_event(
            _event("orderCancelled", fill=fill), 0
        )
        assert (settled, state, terminal) == (0, None, True)


def test_reduce_kill_missing_executed_fields_resolves_nothing():
    # Never a guessed zero: a partial-filled-then-killed FAK whose Success
    # frame was missed must fall to the authoritative poll, not become an
    # untracked naked position. Terminal though — the waiter wakes early so
    # the common bare FAK kill reaches the poll immediately.
    for etype in ("orderCancelled", "orderExpired", "orderNotAccepted"):
        settled, state, terminal = _reduce_wallet_event(_event(etype), 0)
        assert (settled, state, terminal) == (0, None, True)


def test_reduce_submitted_and_failed_never_book():
    # Submitted = settlement still pending on-chain; Failed = it did NOT
    # happen. Neither may ever produce a bookable state.
    sub = _event(
        "orderTransactionSubmitted", fill={"executedSizeWei": str(10**18)}
    )
    assert _reduce_wallet_event(sub, 0) == (0, None, False)
    failed = _event(
        "orderTransactionFailed", fill={"executedSizeWei": str(10**18)}
    )
    assert _reduce_wallet_event(failed, 0) == (0, None, True)


def test_reduce_success_with_unreadable_size_polls():
    # A confirmed fill of UNKNOWN size: resolving FILLED would make the BUY
    # caller book the full signed size (over-booking a partial) — poll instead.
    for fill in (None, {}, {"executedSizeWei": "not-a-number"}):
        ev = _event("orderTransactionSuccess", fill=fill)
        assert _reduce_wallet_event(ev, 0) == (0, None, True)


def test_reduce_success_unreadable_size_polls_despite_prior_settlements():
    # Same rule with the accumulator NON-empty: this Success confirms MORE
    # settled than the accumulator knows, so resolving the prior total would
    # book a knowingly-incomplete fill (cash spent exceeding the booked
    # position — and the inline path books it once, no second look).
    for fill in (None, {}, {"executedSizeWei": "not-a-number"}):
        ev = _event("orderTransactionSuccess", fill=fill)
        assert _reduce_wallet_event(ev, 1 * 10**18) == (1 * 10**18, None, True)


def test_reduce_success_parsed_zero_still_resolves_prior_total():
    # Boundary against the unreadable case: a PARSED "0" adds nothing but
    # leaves the accumulated total COMPLETE, so it may still resolve.
    ev = _event("orderTransactionSuccess", fill={"executedSizeWei": "0"})
    settled, state, terminal = _reduce_wallet_event(ev, 2 * 10**18)
    assert (settled, terminal) == (2 * 10**18, True)
    assert state == {"status": "FILLED", "amountFilled": str(2 * 10**18)}


def test_reduce_accepted_and_unknown_keep_waiting():
    assert _reduce_wallet_event(_event("orderAccepted"), 0) == (0, None, False)
    assert _reduce_wallet_event(_event("somethingNew"), 0) == (0, None, False)


# ---- mirror: routing, buffering, waiter wake, FIFO bounds -------------------


def test_wallet_frame_routes_to_buffer_and_book_path_unaffected():
    _register_market()
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    mirror._handle_message(
        _FakeWs(),
        _wallet_frame(
            _event(
                "orderTransactionSuccess",
                fill={"executedSizeWei": str(2 * 10**18)},
            )
        ),
    )
    # Buffered (no waiter registered yet) under the lowercased hash key.
    assert mirror._wallet_buffer["0xhash1"] == {
        "status": "FILLED",
        "amountFilled": str(2 * 10**18),
    }
    # Book frames still cache exactly as before.
    book = {
        "marketId": 476,
        "updateTimestampMs": 1,
        "asks": [[0.62, 5]],
        "bids": [],
    }
    mirror._handle_message(
        _FakeWs(),
        json.dumps({"type": "M", "topic": "predictOrderbook/476", "data": book}),
    )
    assert mirror.get(_UP_TOKEN, now_ts=time.time()) is not None


def test_await_pre_arrived_event_returns_without_waiting():
    # POST-return-vs-event-arrival race: the event landed before the client
    # registered — the buffer pre-check must return it instantly (case-folded
    # hash match) and consume it exactly once.
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    mirror._handle_wallet_event(
        _event(
            "orderTransactionSuccess",
            order_hash="0xHASH1",
            fill={"executedSizeWei": str(2 * 10**18)},
        )
    )
    t0 = time.monotonic()
    state = mirror.await_fill_state("0xhash1", timeout_s=5.0)
    assert time.monotonic() - t0 < 1.0
    assert state == {"status": "FILLED", "amountFilled": str(2 * 10**18)}
    assert mirror.await_fill_state("0xhash1", timeout_s=0.0) is None


def _started_mirror() -> PredictFunBookWsMirror:
    """A start()ed mirror whose connect factory fails instantly: the loop
    thread RUNS (``await_fill_state`` only ever waits while a push can
    possibly arrive) but no socket is opened and ``websockets`` is never
    imported. Callers must ``stop()`` it."""

    def _no_connect():
        raise ConnectionError("offline test mirror")

    mirror = PredictFunBookWsMirror(
        max_age_ms=2000, connect_factory=_no_connect, reconnect_backoff_s=5.0
    )
    mirror.start()
    return mirror


def _await_on_thread(mirror, order_hash, timeout_s):
    out = {}

    def run():
        out["state"] = mirror.await_fill_state(order_hash, timeout_s)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    # Wait until the waiter is actually registered before feeding the event.
    for _ in range(200):
        with mirror._wallet_lock:
            if order_hash in mirror._wallet_waiters:
                break
        time.sleep(0.005)
    return th, out


def test_registered_waiter_woken_by_success_event():
    mirror = _started_mirror()
    try:
        th, out = _await_on_thread(mirror, "0xhash7", timeout_s=5.0)
        mirror._handle_wallet_event(
            _event(
                "orderTransactionSuccess",
                order_hash="0xhash7",
                fill={"executedSizeWei": str(3 * 10**18)},
            )
        )
        th.join(timeout=2.0)
        assert not th.is_alive()
        assert out["state"] == {
            "status": "FILLED",
            "amountFilled": str(3 * 10**18),
        }
    finally:
        mirror.stop()


def test_bare_kill_wakes_registered_waiter_early_with_none():
    # A kill without executed fields resolves NOTHING but must wake the wait
    # EARLY (None -> the caller polls now) — the plain FAK reject path cannot
    # be slower than today's poll by the full WS budget.
    mirror = _started_mirror()
    try:
        t0 = time.monotonic()
        th, out = _await_on_thread(mirror, "0xhash8", timeout_s=5.0)
        mirror._handle_wallet_event(_event("orderCancelled", order_hash="0xhash8"))
        th.join(timeout=2.0)
        assert not th.is_alive()
        assert out["state"] is None
        assert time.monotonic() - t0 < 4.0
    finally:
        mirror.stop()


def test_bare_kill_before_registration_short_circuits_wait():
    # The reject flavor of the POST-return-vs-event-arrival race: the FAK
    # kill lands while the POST response is still being parsed (no waiter
    # yet). No further event ever arrives for a terminal order, so the
    # buffered terminal-no-state marker must send the late await_fill_state
    # straight to the poll — losing the race must not turn a plain reject
    # into a full-WS-budget stall on the live fast-requote path.
    mirror = _started_mirror()
    try:
        mirror._handle_wallet_event(
            _event("orderCancelled", order_hash="0xhash9")
        )
        t0 = time.monotonic()
        state = mirror.await_fill_state("0xhash9", timeout_s=5.0)
        assert time.monotonic() - t0 < 1.0
        assert state is None
        with mirror._wallet_lock:  # marker consumed exactly once
            assert "0xhash9" not in mirror._wallet_buffer
    finally:
        mirror.stop()


def test_await_skips_wait_when_mirror_not_running():
    # Waiter wired but the mirror never came up (websockets unimportable /
    # shadow start failed after the main.py wiring) or has stopped: no push
    # can ever arrive, so the wait must not tax every entry and stop attempt
    # with the full WS budget in exactly the degraded state the poll
    # fallback exists for.
    mirror = PredictFunBookWsMirror(max_age_ms=2000)  # never start()ed
    t0 = time.monotonic()
    assert mirror.await_fill_state("0xdead", timeout_s=5.0) is None
    assert time.monotonic() - t0 < 1.0
    # Pre-arrived terminal facts still win over liveness: real facts from a
    # previously-live socket stay consumable.
    mirror._handle_wallet_event(
        _event(
            "orderTransactionSuccess",
            order_hash="0xdead",
            fill={"executedSizeWei": str(10**18)},
        )
    )
    assert mirror.await_fill_state("0xdead", timeout_s=5.0) == {
        "status": "FILLED",
        "amountFilled": str(10**18),
    }


def test_wallet_buffer_and_accum_fifo_bounded():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    for i in range(300):
        mirror._handle_wallet_event(
            _event(
                "orderTransactionSuccess",
                order_hash=f"0x{i:04d}",
                fill={"executedSizeWei": "1"},
            )
        )
    assert len(mirror._wallet_buffer) == 256
    assert len(mirror._wallet_accum) == 256
    assert "0x0000" not in mirror._wallet_buffer  # oldest evicted first
    assert "0x0299" in mirror._wallet_buffer


# ---- wallet subscription sync: armed, JWT gating, rotation ------------------


def test_wallet_sync_inert_when_not_armed():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    ws = _FakeWs()
    assert mirror._sync_wallet_subscription(ws, None, -1) == (None, -1)
    assert ws.sent == []


def test_wallet_sync_subscribes_and_rotation_hook_resubscribes():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    ws = _FakeWs()
    token = {"jwt": None}
    mirror.enable_wallet_events(lambda: token["jwt"])

    # No credential yet: nothing sent, version left UNLATCHED so the provider
    # is re-read next iteration (the WS side never fetches a JWT itself).
    topic, ver = mirror._sync_wallet_subscription(ws, None, -1)
    assert topic is None and ws.sent == []
    assert ver == -1

    token["jwt"] = "jwtA"
    topic, ver = mirror._sync_wallet_subscription(ws, topic, ver)
    assert topic == "predictWalletEvents/jwtA"
    assert ws.sent[-1]["method"] == "subscribe"
    assert ws.sent[-1]["params"] == ["predictWalletEvents/jwtA"]

    # Same version -> zero work (no needless resubscribe traffic).
    n = len(ws.sent)
    assert mirror._sync_wallet_subscription(ws, topic, ver) == (topic, ver)
    assert len(ws.sent) == n

    # Stale-JWT rotation (JwtSession hook): unsubscribe dead topic, subscribe
    # the fresh one.
    token["jwt"] = "jwtB"
    mirror.notify_jwt_rotated()
    topic, ver = mirror._sync_wallet_subscription(ws, topic, ver)
    assert topic == "predictWalletEvents/jwtB"
    tail = [(m["method"], m["params"][0]) for m in ws.sent[n:]]
    assert tail == [
        ("unsubscribe", "predictWalletEvents/jwtA"),
        ("subscribe", "predictWalletEvents/jwtB"),
    ]


def test_order_client_rotation_hook_fires_and_peek_never_fetches():
    _register_market()
    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    client = _mk_client(venue)
    calls: list[int] = []
    client.set_jwt_rotation_hook(lambda: calls.append(1))
    try:
        # Nothing fetched yet: peek returns None AND spends no /auth flow.
        assert client.peek_wallet_jwt() is None
        assert venue.auth_flows == 0
        res = client.place_gtc_limit_order(_Signal(), client_order_id="rot-1")
        assert res.success is True
        # The order's JWT refresh fired the hook; peek now returns the cached
        # token without a second /auth flow.
        assert calls
        assert client.peek_wallet_jwt()
        assert venue.auth_flows == 1
    finally:
        client.close()


# ---- order-client integration: WS-first wait + unchanged poll fallback ------


def test_ws_success_resolves_buy_with_no_get_and_books_cap_price():
    _register_market()
    venue = _Venue()
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    # Event arrives (buffered) before the client awaits — full 2.0-share fill.
    mirror._handle_message(
        _FakeWs(),
        _wallet_frame(
            _event(
                "orderTransactionSuccess",
                order_hash="0xHASH1",  # case-folded match against the POST's 0xhash1
                fill={
                    "executedPriceWei": "770000000000000000",  # NEVER booked
                    "executedSizeWei": str(2 * 10**18),
                    "executedValueWei": "1540000000000000000",
                },
            )
        ),
    )
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=2000)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="ws-1")
    finally:
        client.close()
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(2.0)
    # Cap-price booking survives: the pushed 0.77 executedPriceWei is ignored.
    assert res.filled_price_vwap == pytest.approx(0.65)
    assert res.filled_value_usd == pytest.approx(1.30)
    assert venue.state_gets() == []  # resolved purely from the push — zero GETs


def test_ws_partial_settlement_then_cancel_books_partial_no_get():
    _register_market()
    venue = _Venue()
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    fake = _FakeWs()
    # 1.0 share settled, then the FAK remainder killed: the accumulated
    # partial must be booked (executed partial FIRST — poll-ordering parity).
    mirror._handle_message(
        fake,
        _wallet_frame(
            _event(
                "orderTransactionSuccess",
                fill={"executedSizeWei": str(1 * 10**18)},
            )
        ),
    )
    mirror._handle_message(fake, _wallet_frame(_event("orderCancelled")))
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=2000)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="ws-2")
    finally:
        client.close()
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(1.0)
    assert res.filled_price_vwap == pytest.approx(0.65)
    assert res.filled_value_usd == pytest.approx(0.65)
    assert venue.state_gets() == []


def test_ws_kill_missing_executed_fields_falls_back_to_poll():
    _register_market()
    venue = _Venue()
    venue.order_status = "CANCELLED"
    venue.amount_filled_wei = 0
    mirror = _started_mirror()
    # Bare kill, nothing settled: resolves NOTHING (never a guessed zero) —
    # the poll must decide. It arrives pre-await, so the buffered
    # terminal-no-state marker sends the wait straight to the poll instead of
    # burning the full 2s WS budget on an already-decided order.
    mirror._handle_message(_FakeWs(), _wallet_frame(_event("orderCancelled")))
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=2000)
    t0 = time.monotonic()
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="ws-3")
    finally:
        client.close()
        mirror.stop()
    assert time.monotonic() - t0 < 1.5  # never waited the 2s WS budget
    assert res.success is False
    assert res.error_kind == "no_liquidity"
    assert len(venue.state_gets()) >= 1  # the UNCHANGED poll ran


def test_ws_timeout_falls_back_to_poll():
    _register_market()
    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    # RUNNING mirror, but no event ever arrives: the wait genuinely times out
    # (a not-running mirror would skip the wait, a different code path).
    mirror = _started_mirror()
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=20)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="ws-4")
    finally:
        client.close()
        mirror.stop()
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(2.0)
    assert res.filled_price_vwap == pytest.approx(0.65)
    assert len(venue.state_gets()) >= 1


def test_ws_sell_success_books_at_cap_no_get():
    _register_market()
    venue = _Venue()
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    mirror._handle_wallet_event(
        _event(
            "orderTransactionSuccess",
            fill={"executedSizeWei": str(3 * 10**18)},
        )
    )
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=2000)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(3.0)
    assert res.filled_price_vwap == pytest.approx(0.40)
    assert venue.state_gets() == []


def test_ws_sell_explicit_zero_kill_falls_to_poll():
    _register_market()
    venue = _Venue()
    venue.order_status = "CANCELLED"
    venue.amount_filled_wei = 0
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    # Kill WITH an executed-size field: off-contract (kills carry no fill
    # object per the docs), so it is never trusted for a zero-book — the
    # buffered terminal-no-state marker sends the wait straight to the
    # authoritative poll, which reads CANCELLED/0. FOK SELL success only on
    # FILLED -> failure.
    mirror._handle_wallet_event(
        _event("orderCancelled", fill={"executedSizeWei": "0"})
    )
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=2000)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "no_liquidity"
    assert len(venue.state_gets()) >= 1  # the UNCHANGED poll decided


# ---- unreadable-state asymmetry survives with WS armed ----------------------


def test_buy_unreadable_with_ws_on_stays_success_without_metadata(no_poll_sleep):
    # WS yields nothing (not-running mirror skips the wait) AND every poll GET
    # 500s: the order EXISTS on the venue, so this must stay success WITHOUT
    # fill metadata (a failure return would let the pipeline re-bet the slot —
    # double-spend).
    _register_market()
    venue = _Venue()
    venue.order_state_status = 500
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=20)
    try:
        res = client.place_gtc_limit_order(_Signal(), client_order_id="ws-u1")
    finally:
        client.close()
    assert res.success is True
    assert res.order_id == "0xhash1"
    assert res.filled_size_tokens is None
    assert res.filled_price_vwap is None
    assert res.filled_value_usd is None


def test_sell_unreadable_with_ws_on_stays_failure(no_poll_sleep):
    _register_market()
    venue = _Venue()
    venue.order_state_status = 500
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    client = _mk_client(venue)
    client.set_fill_event_waiter(mirror, wait_ms=20)
    try:
        res = client.place_stop_loss_sell(
            token_id=_UP_TOKEN, size_tokens=3.0, price=0.40
        )
    finally:
        client.close()
    assert res.success is False
    assert res.error_kind == "unknown"


# ---- flag OFF = byte-identical poll path ------------------------------------


def test_flag_defaults_off_and_client_has_no_waiter():
    # Default OFF (documented byte-identical contract) …
    assert config.PREDICTFUN_WS_WALLET_EVENTS is False
    # … and a plainly-constructed client carries NO WS object: the waiter is
    # None until main.py's flag-gated wiring injects one.
    _register_market()
    venue = _Venue()
    venue.amount_filled_wei = 2 * 10**18
    client = _mk_client(venue)
    try:
        assert client._fill_event_waiter is None
        res = client.place_gtc_limit_order(_Signal(), client_order_id="off-1")
    finally:
        client.close()
    # Today's poll path: the fill is read back over GET, result unchanged.
    assert res.success is True
    assert res.filled_size_tokens == pytest.approx(2.0)
    assert len(venue.state_gets()) >= 1


def test_wallet_event_wiring_is_predictfun_only_and_live_gated():
    # Source-level guard (test_dry_instance idiom): the wallet wiring lives ONLY
    # in the predict.fun adapter's WS-mirror builder, behind the flag AND an
    # order_client None check (DRY never subscribes); the Polymarket adapter and
    # main.py are untouched by the feature.
    #
    # Phase 2 moved the wiring out of main.py's `if config.VENUE == "predictfun"`
    # branch into cryptobot_predictfun's build_ws_mirror, so the guard now reads
    # the two adapters rather than splitting one main.py block.
    import cryptobot.main as main_mod
    import cryptobot_predictfun

    pf_src = Path(cryptobot_predictfun.__file__).read_text(encoding="utf-8")
    main_src = Path(main_mod.__file__).read_text(encoding="utf-8")
    # The sibling venue package is read by PATH, not imported: a venue package
    # importing another is exactly what tools/importcheck.py forbids, and an
    # absence assertion needs the bytes, not the module.
    # parents[5]: tests -> cryptobot_predictfun -> src -> live -> predictfun -> repo
    poly_src = (
        Path(__file__).resolve().parents[5]  # legacy-path-ok: code-tree anchor for a TRACKED sibling source
        / "polymarket" / "src" / "cryptobot_polymarket" / "__init__.py"
    ).read_text(encoding="utf-8")

    pf_ws = pf_src.split("def build_ws_mirror", 1)[1]
    assert "config.PREDICTFUN_WS_WALLET_EVENTS and order_client is not None" in pf_ws
    assert "enable_wallet_events" in pf_ws
    assert "set_jwt_rotation_hook" in pf_ws
    assert "set_fill_event_waiter" in pf_ws

    for other in (poly_src, main_src):
        assert "PREDICTFUN_WS_WALLET_EVENTS" not in other
        assert "enable_wallet_events" not in other
