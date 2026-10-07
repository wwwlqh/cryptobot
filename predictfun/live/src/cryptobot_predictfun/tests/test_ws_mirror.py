"""predict.fun WebSocket book mirror: frame -> BookSnapshot reuse (UP + DOWN
complement byte-identical to the REST ``_snapshot_from_orderbook``), recv-
liveness staleness, fail-closed unregistered-token resolution, note_tokens
replace + diff-driven (un)subscribe, the mandatory heartbeat echo, and the
missing-``websockets``-dep fail-soft ``start``.

All tests are network-free: frames are fed straight into ``_handle_message`` and
the subscribe diff is driven with a fake ws that records sends. The lazy
``websockets`` import is exercised only via the fail-soft path (sys.modules
stub), never by opening a socket.
"""

from __future__ import annotations

import json
import sys
import time

import pytest

from cryptobot_predictfun import clob as pf_clob
from cryptobot_predictfun import markets as pf_markets
from cryptobot_predictfun.ws_mirror import (
    PredictFunBookWsMirror,
    PredictFunShadowBookComparator,
)


@pytest.fixture(autouse=True)
def _clear_registry():
    pf_markets.clear_market_cache()
    yield
    pf_markets.clear_market_cache()


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


def _ob_data(asks, bids, market_id=476) -> dict:
    """The ``data`` body of a predictOrderbook frame (same shape as the REST
    ``/markets/{id}/orderbook`` data body)."""
    return {
        "version": 7,
        "marketId": market_id,
        "updateTimestampMs": 1784513589410,
        "orderCount": len(asks) + len(bids),
        "asks": asks,
        "bids": bids,
        "lastOrderSettled": None,
        "settlementsPending": [],
    }


def _frame(data, market_id=476) -> str:
    return json.dumps(
        {"type": "M", "topic": f"predictOrderbook/{market_id}", "data": data}
    )


class FakeWs:
    """Records everything sent; recv is never used in the direct-handler tests."""

    def __init__(self):
        self.sent: list[dict] = []

    def send(self, raw):
        self.sent.append(json.loads(raw))


# ---- frame -> snapshot (reuse + byte-identical DOWN complement) --------------


def test_frame_yields_up_and_complemented_down_byte_identical():
    _register(market_id="476", up_tok="111", down_tok="222")
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    data = _ob_data(asks=[[0.65, 3], [0.62, 5]], bids=[[0.60, 10], [0.55, 2]])
    mirror._handle_message(FakeWs(), _frame(data, market_id=476))

    up = mirror.get("111", now_ts=time.time())
    down = mirror.get("222", now_ts=time.time())
    assert up is not None and down is not None

    # Byte-identical to the REST helper for BOTH sides (ignore fetched_at_ts,
    # which is the local recv time on the WS path by design).
    ref_up = pf_clob._snapshot_from_orderbook(data, token_id="111", now_ts=0.0, side="UP")
    ref_down = pf_clob._snapshot_from_orderbook(data, token_id="222", now_ts=0.0, side="DOWN")
    assert up.token_id == ref_up.token_id
    assert up.asks == ref_up.asks
    assert up.bids == ref_up.bids
    assert up.total_ask_depth_usd == pytest.approx(ref_up.total_ask_depth_usd)
    # DOWN: asks' = complement(bids), bids' = complement(asks).
    assert down.asks == ref_down.asks
    assert down.bids == ref_down.bids
    assert down.total_ask_depth_usd == pytest.approx(ref_down.total_ask_depth_usd)
    # DOWN best ask == 1 - UP best bid (0.60) = 0.40 (probe cross-check).
    assert down.top_ask == pytest.approx(0.40)


def test_fetched_at_ts_is_local_recv_not_venue_ts():
    _register()
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    before = time.time()
    mirror._handle_message(FakeWs(), _frame(_ob_data(asks=[[0.62, 5]], bids=[])))
    after = time.time()
    snap = mirror.get("111", now_ts=time.time())
    assert snap is not None
    # Never the venue updateTimestampMs (1784513589410 s would be far-future);
    # must be the local receive wall-clock.
    assert before <= snap.fetched_at_ts <= after


# ---- staleness (recv-liveness gate) -----------------------------------------


def test_stale_frame_returns_none():
    _register()
    mirror = PredictFunBookWsMirror(max_age_ms=500)
    mirror._handle_message(FakeWs(), _frame(_ob_data(asks=[[0.62, 5]], bids=[])))
    assert mirror.get("111", now_ts=time.time()) is not None
    # Age the stored frame past max_age (white-box: rewrite the monotonic recv).
    book = mirror._books["476"]
    book.recv_monotonic = time.monotonic() - 5.0
    assert mirror.get("111", now_ts=time.time()) is None


def test_no_frame_returns_none():
    _register()
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    # Registered token but the mirror never received a frame -> None (REST).
    assert mirror.get("111", now_ts=time.time()) is None


# ---- fail-closed unregistered token -----------------------------------------


def test_unregistered_token_returns_none():
    _register(up_tok="111", down_tok="222")
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    mirror._handle_message(FakeWs(), _frame(_ob_data(asks=[[0.62, 5]], bids=[])))
    # A token discovery never registered -> fail-closed None even though the
    # market's book IS cached (never a guessed market/side).
    assert mirror.get("999", now_ts=time.time()) is None


# ---- note_tokens replace + diff-driven (un)subscribe ------------------------


def test_note_tokens_replaces_wholesale_and_bumps_version():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    mirror.note_tokens(["111", "222", "111"])
    assert mirror.noted_tokens() == ("111", "222")
    v1 = mirror._tokens_version
    mirror.note_tokens(["333"])  # REPLACE, not merge
    assert mirror.noted_tokens() == ("333",)
    assert mirror._tokens_version == v1 + 1
    # Same set again -> no version bump (no needless resubscribe).
    mirror.note_tokens(["333"])
    assert mirror._tokens_version == v1 + 1


def test_resubscribe_diff_subscribes_added_unsubscribes_removed():
    _register(market_id="476", up_tok="111", down_tok="222", coin="btc", ws=1784500000)
    _register(market_id="900", up_tok="777", down_tok="888", coin="eth", ws=1784500900)
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    ws = FakeWs()

    # Initial: subscribe market 476 (both its tokens dedup to one market).
    mirror._resubscribe_diff(
        ws, mirror._desired_market_ids(()), mirror._desired_market_ids(("111", "222"))
    )
    subs = [m for m in ws.sent if m["method"] == "subscribe"]
    assert [s["params"] for s in subs] == [["predictOrderbook/476"]]
    assert all(isinstance(s["requestId"], int) for s in subs)

    ws.sent.clear()
    # Window rolls: 476 -> 900. Expect unsubscribe 476 + subscribe 900.
    mirror._resubscribe_diff(
        ws,
        mirror._desired_market_ids(("111", "222")),
        mirror._desired_market_ids(("777", "888")),
    )
    methods = {m["method"]: m["params"] for m in ws.sent}
    assert methods["unsubscribe"] == ["predictOrderbook/476"]
    assert methods["subscribe"] == ["predictOrderbook/900"]
    # Unsubscribed market's cache is dropped so a stale book can't linger.
    assert "476" not in mirror._books


def test_unregistered_noted_token_yields_no_subscription():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    # Token not in the registry -> no market resolved -> nothing to subscribe.
    assert mirror._desired_market_ids(("nope",)) == frozenset()


# ---- heartbeat echo ---------------------------------------------------------


def test_heartbeat_echo_returns_exact_server_timestamp():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    ws = FakeWs()
    server_ts = 1736696400000
    hb = json.dumps({"type": "M", "topic": "heartbeat", "data": server_ts})
    mirror._handle_message(ws, hb)
    assert ws.sent == [{"method": "heartbeat", "data": server_ts}]
    # Echoed value is byte-exact (int stays int, not stringified/rounded).
    assert ws.sent[0]["data"] == server_ts and isinstance(ws.sent[0]["data"], int)


def test_request_ack_and_unknown_frames_are_ignored_but_keep_liveness():
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    ws = FakeWs()
    mirror._handle_message(ws, json.dumps({"type": "R", "requestId": 1, "success": True}))
    assert ws.sent == []  # acks trigger no send and no book
    assert mirror._books == {}
    assert mirror.last_update_ts() is not None  # liveness still stamped


# ---- missing websockets dep -> start() fails soft ---------------------------


def test_missing_websockets_dep_start_fails_soft(monkeypatch):
    # Force the lazy `from websockets.sync.client import connect` to raise, as on
    # a host without the library: a None sys.modules entry raises ImportError.
    monkeypatch.setitem(sys.modules, "websockets.sync.client", None)
    mirror = PredictFunBookWsMirror(max_age_ms=2000)  # no connect_factory -> lazy import
    _register()
    mirror.start()  # must NOT raise
    assert mirror._inert is True
    assert mirror._thread is None
    # Inert mirror serves nothing (REST stays authoritative) and start is idempotent.
    assert mirror.get("111", now_ts=time.time()) is None
    mirror.start()
    assert mirror._thread is None
    mirror.stop()  # no-op, must not raise


# ---- shadow comparator (off the decision path) ------------------------------


def test_shadow_comparator_counts_divergence_never_authoritative():
    _register(up_tok="111", down_tok="222")
    mirror = PredictFunBookWsMirror(max_age_ms=2000)
    mirror._handle_message(FakeWs(), _frame(_ob_data(asks=[[0.62, 5]], bids=[])))
    cmp = PredictFunShadowBookComparator(mirror, log_min_interval_s=0.0)

    class _RestSnap:
        top_ask = 0.62
        total_ask_depth_usd = 0.62 * 5

    cmp.observe("111", _RestSnap(), now_ts=time.time())
    assert cmp.n_compared == 1
    assert cmp.n_ws_missing == 0
    assert cmp.n_divergent_price == 0

    # WS missing (unregistered token) is counted, never raises.
    cmp.observe("999", _RestSnap(), now_ts=time.time())
    assert cmp.n_ws_missing == 1


def test_shadow_comparator_reuses_shared_class_not_a_fork():
    # The PF shadow comparator is the venue-agnostic Polymarket one, re-exported
    # (no lockstep-drift fork). A future re-fork would flip this identity.
    from cryptobot.venues.shadow_book import ShadowBookComparator

    assert PredictFunShadowBookComparator is ShadowBookComparator


# ---- reconnect backoff recovery after a healthy session ---------------------


class _DropAfterFramesConn:
    """A context-manager fake ws that delivers ``frames`` (one per ``recv``) then
    simulates a disconnect by raising — exercising _stream_once's exception exit,
    the path a real WS drop takes (never the stop-only normal return)."""

    def __init__(self, frames):
        self._frames = list(frames)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def recv(self, timeout=None):
        if self._frames:
            return self._frames.pop(0)
        raise ConnectionError("simulated WS disconnect")

    def send(self, raw):  # subscribe/heartbeat writes are no-ops here
        pass


class _CountingStop:
    """Stand-in for the mirror's threading.Event: records each reconnect
    ``wait`` timeout and ends the loop after ``stop_after`` waits so _run
    terminates deterministically with no real sleeping."""

    def __init__(self, stop_after):
        self._stop_after = stop_after
        self._n = 0
        self.waits: list = []

    def is_set(self):
        return False

    def wait(self, timeout=None):
        self.waits.append(timeout)
        self._n += 1
        return self._n >= self._stop_after

    def set(self):
        pass


def test_backoff_resets_after_each_frame_receiving_session():
    _register()
    frame = _frame(_ob_data(asks=[[0.62, 5]], bids=[]))
    # Every reconnect delivers a frame before dropping -> a healthy endpoint.
    mirror = PredictFunBookWsMirror(
        max_age_ms=2000,
        connect_factory=lambda: _DropAfterFramesConn([frame]),
        reconnect_backoff_s=1.0,
        reconnect_backoff_max_s=30.0,
    )
    # Drive _run directly (no thread): wire the injected factory start() would.
    mirror._resolved_connect = mirror._connect_factory
    mirror._stop = _CountingStop(stop_after=4)
    mirror._run()
    # A session that delivered a frame resets the backoff to base, so it never
    # ratchets 1->2->4->8. (Pre-fix the reset was dead code and these grew.)
    assert mirror._stop.waits == [1.0, 1.0, 1.0, 1.0]


def test_backoff_grows_when_sessions_never_deliver_a_frame():
    _register()
    # Connect then immediate drop with no frame -> unhealthy; backoff must grow.
    mirror = PredictFunBookWsMirror(
        max_age_ms=2000,
        connect_factory=lambda: _DropAfterFramesConn([]),
        reconnect_backoff_s=1.0,
        reconnect_backoff_max_s=30.0,
    )
    mirror._resolved_connect = mirror._connect_factory
    mirror._stop = _CountingStop(stop_after=5)
    mirror._run()
    assert mirror._stop.waits == [1.0, 2.0, 4.0, 8.0, 16.0]
