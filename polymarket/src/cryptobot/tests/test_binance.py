"""Binance spot adapter tests - mocked HTTP boundary, no live network."""

from __future__ import annotations

import json
import threading
import time

import httpx
import pytest

from cryptobot import config
from cryptobot.data import binance


def _payload_all(close_time_ms: int = 1_715_923_440_000) -> list[dict]:
    prices = {
        "BTC": 67_432.12,
        "ETH": 3_120.55,
        "SOL": 175.20,
        "BNB": 590.00,
        "XRP": 0.52,
        "DOGE": 0.13,
    }
    return [
        {
            "symbol": binance.SYMBOLS[coin],
            "lastPrice": str(price),
            "closeTime": close_time_ms,
        }
        for coin, price in prices.items()
    ]


def _client_with_handler(handler) -> httpx.Client:
    transport = httpx.MockTransport(handler)
    return httpx.Client(transport=transport)


def test_happy_path_returns_configured_live_spots():
    binance.reset_cache()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "data-api.binance.vision"
        assert request.url.path == "/api/v3/ticker/24hr"
        assert request.url.params["type"] == "MINI"
        requested = json.loads(request.url.params["symbols"])
        assert requested == [binance.SYMBOLS[coin] for coin in config.COINS]
        return httpx.Response(200, json=_payload_all())

    with _client_with_handler(handler) as c:
        result = binance.fetch_all_spots(client=c, now_ts=1.0, tick_seconds=1)

    assert result is not None
    assert set(result) == set(config.COINS)
    btc = result["BTC"]
    assert btc.coin == "BTC"
    assert btc.price == pytest.approx(67_432.12)
    # audit w19oxqf24 #2: publish_time is the FIRST-SEEN freshness stamp
    # (now_ts on first observation), NOT the wall-clock closeTime. closeTime
    # is intentionally ignored as a clock because it advances every request.
    assert btc.publish_time == 1


def test_1s_kline_mode_matches_forward_spot_contract(monkeypatch):
    binance.reset_cache()
    monkeypatch.setattr(config, "SPOT_USE_1S_KLINES", True)
    prices = {
        "BTC": 67_432.12,
        "ETH": 3_120.55,
        "SOL": 175.20,
        "XRP": 0.52,
    }
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "data-api.binance.vision"
        assert request.url.path == "/api/v3/klines"
        assert request.url.params["interval"] == "1s"
        assert request.url.params["limit"] == "10"
        assert "startTime" not in request.url.params
        assert request.url.params["endTime"] == "1999"
        symbol = request.url.params["symbol"]
        calls.append(symbol)
        coin = next(c for c, s in binance.SYMBOLS.items() if s == symbol)
        return httpx.Response(
            200,
            json=[
                [
                    1_000,
                    str(prices[coin]),
                    str(prices[coin]),
                    str(prices[coin]),
                    str(prices[coin]),
                    "1.0",
                    1_999,
                ],
                [
                    2_000,
                    str(prices[coin]),
                    str(prices[coin]),
                    str(prices[coin]),
                    str(prices[coin] + 1.0),
                    "1.0",
                    2_999,
                ],
            ],
        )

    with _client_with_handler(handler) as c:
        result = binance.fetch_all_spots(client=c, now_ts=2.4, tick_seconds=1)

    assert result is not None
    assert sorted(calls) == sorted(binance.SYMBOLS[coin] for coin in config.COINS)
    assert result["BTC"].price == pytest.approx(prices["BTC"])
    assert result["BTC"].publish_time == 2
    binance.reset_cache()


def test_1s_kline_decoder_rejects_open_candle(monkeypatch):
    monkeypatch.setattr(config, "SPOT_USE_1S_KLINES", True)
    payload = [[1_000, "1", "1", "1", "100", "1", 1_999]]
    assert binance._decode_completed_1s_kline(
        "BTC",
        payload,
        target_start_ms=1_000,
        observed_ts_ms=1_998,
        now_ts=2,
        bucket=2,
    ) is None


def test_1s_kline_decoder_matches_backtest_gap_fallback():
    payload = [[1_000, "1", "1", "1", "67000", "1", 1_999]]
    spot = binance._decode_completed_1s_kline(
        "BTC",
        payload,
        target_start_ms=2_000,
        observed_ts_ms=3_100,
        now_ts=3,
        bucket=3,
    )
    assert spot is not None
    assert spot.price == pytest.approx(67_000.0)
    assert spot.publish_time == 2


def test_cache_reuses_within_same_fetch_bucket():
    binance.reset_cache()
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(200, json=_payload_all())

    with _client_with_handler(handler) as c:
        a = binance.fetch_all_spots(client=c, now_ts=10.5, tick_seconds=10)
        b = binance.fetch_all_spots(client=c, now_ts=19.9, tick_seconds=10)
        c_result = binance.fetch_all_spots(client=c, now_ts=20.0, tick_seconds=10)

    assert a is b
    assert c_result is not None
    assert call_count["n"] == 2


def test_failed_bucket_is_negative_cached():
    binance.reset_cache()
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(503, text="temporary outage")

    with _client_with_handler(handler) as c:
        a = binance.fetch_all_spots(client=c, now_ts=20.1, tick_seconds=10)
        b = binance.fetch_all_spots(client=c, now_ts=29.9, tick_seconds=10)
        c_result = binance.fetch_all_spots(client=c, now_ts=30.0, tick_seconds=10)

    assert a is None
    assert b is None
    assert c_result is None
    assert call_count["n"] == 2


# ---- Multi-bucket store + peek-only reader (regression 2026-07-30) ------


def test_alternating_current_and_next_bucket_never_evict_each_other():
    # THE REGRESSION. The single-slot store made the two live keys — the
    # CURRENT bucket (tick + the 4Hz fast rungs) and the NEXT bucket
    # (SpotPrefetcher, ~200ms before the boundary) — evict each other, and
    # every eviction turned the next read into an inline network GET on the
    # decision path. Alternating the two keys must cost exactly TWO GETs no
    # matter how many times they alternate.
    binance.reset_cache()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=_payload_all())

    with _client_with_handler(handler) as c:
        first_s = binance.fetch_all_spots(client=c, now_ts=500.0, tick_seconds=1)
        first_next = binance.fetch_all_spots(client=c, now_ts=501.0, tick_seconds=1)
        assert calls["n"] == 2
        for _ in range(10):
            # The fast rung re-reads second S at 4Hz...
            assert binance.fetch_all_spots(client=c, now_ts=500.25, tick_seconds=1) is first_s
            # ...while the prefetcher re-warms S+1. Both stay served from cache.
            assert binance.fetch_all_spots(client=c, now_ts=501.0, tick_seconds=1) is first_next

    assert calls["n"] == 2  # zero extra network calls


def test_store_is_pruned_by_age_to_the_newest_buckets():
    binance.reset_cache()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_payload_all())

    with _client_with_handler(handler) as c:
        for bucket in range(600, 600 + binance._MAX_BUCKETS + 3):
            binance.fetch_all_spots(client=c, now_ts=float(bucket), tick_seconds=1)

    with binance._cache_lock:
        buckets = sorted(key[0] for key in binance._cache)
    assert len(buckets) == binance._MAX_BUCKETS
    newest = 600 + binance._MAX_BUCKETS + 2
    assert buckets == list(range(newest - binance._MAX_BUCKETS + 1, newest + 1))


def test_peek_serves_a_warm_bucket_without_a_get():
    binance.reset_cache()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=_payload_all())

    with _client_with_handler(handler) as c:
        warmed = binance.fetch_all_spots(client=c, now_ts=700.0, tick_seconds=1)

    # peek takes no client and performs no I/O — it can only read the store.
    assert binance.peek_all_spots(now_ts=700.4, tick_seconds=1) is warmed
    assert calls["n"] == 1


def test_peek_on_a_cold_bucket_abstains_and_leaves_no_trace():
    # The fast rungs' contract: a cold bucket returns None (the rung abstains
    # from the fire) and peek must NOT fetch, negative-cache, or register an
    # in-flight marker that a later real fetch would coalesce onto.
    binance.reset_cache()
    assert binance.peek_all_spots(now_ts=800.0, tick_seconds=1) is None
    with binance._cache_lock:
        assert binance._cache == {}
        assert binance._inflight == {}


def test_peek_reads_none_for_a_negative_cached_bucket():
    binance.reset_cache()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="temporary outage")

    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=900.0, tick_seconds=1) is None

    assert binance.peek_all_spots(now_ts=900.0, tick_seconds=1) is None


def test_failed_bucket_does_not_blank_a_good_neighbour():
    # Single-slot fallout: `_mark_bucket_failed` overwrote the ONE slot, so a
    # failure at S wiped a perfectly good batch at S+1 (and vice versa).
    binance.reset_cache()
    state = {"fail": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if state["fail"]:
            return httpx.Response(503, text="temporary outage")
        return httpx.Response(200, json=_payload_all())

    with _client_with_handler(handler) as c:
        good = binance.fetch_all_spots(client=c, now_ts=1_100.0, tick_seconds=1)
        state["fail"] = True
        assert binance.fetch_all_spots(client=c, now_ts=1_101.0, tick_seconds=1) is None

    assert good is not None
    assert binance.peek_all_spots(now_ts=1_100.0, tick_seconds=1) is good


# ---- In-flight coalescing (latency: duplicate same-bucket GET) ----------


def test_concurrent_same_bucket_callers_coalesce_into_one_get():
    # A fetch already airborne for the bucket (the SpotPrefetcher's boundary
    # fetch in live) must absorb a concurrent caller: the second caller waits
    # on the in-flight Event and reuses the cached batch instead of paying a
    # full duplicate GET on the decision path.
    binance.reset_cache()
    call_count = {"n": 0}
    airborne = threading.Event()
    release = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        airborne.set()
        assert release.wait(timeout=5.0), "test never released the in-flight GET"
        return httpx.Response(200, json=_payload_all())

    results: dict[str, object] = {}

    def owner():
        with _client_with_handler(handler) as c:
            results["owner"] = binance.fetch_all_spots(client=c, now_ts=50.0, tick_seconds=1)

    def waiter():
        with _client_with_handler(handler) as c:
            results["waiter"] = binance.fetch_all_spots(client=c, now_ts=50.4, tick_seconds=1)

    t_owner = threading.Thread(target=owner)
    t_owner.start()
    assert airborne.wait(timeout=5.0), "owner GET never started"
    t_waiter = threading.Thread(target=waiter)
    t_waiter.start()
    time.sleep(0.05)  # let the waiter reach the in-flight wait
    release.set()
    t_owner.join(timeout=5.0)
    t_waiter.join(timeout=5.0)

    assert call_count["n"] == 1
    assert results["owner"] is not None
    assert results["waiter"] is results["owner"]


def test_negative_cache_wake_triggers_independent_bounded_retry():
    # F2: a waiter that coalesced onto the airborne GET and woke to the
    # owner's FAILURE must not adopt it blindly — pre-coalescing its own
    # duplicate GET was an independent success path. With residual deadline
    # budget it retries inline (bounded by that budget) and can succeed.
    binance.reset_cache()
    call_count = {"n": 0}
    airborne = threading.Event()
    release = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            airborne.set()
            assert release.wait(timeout=5.0), "test never released the in-flight GET"
            return httpx.Response(503, text="temporary outage")
        return httpx.Response(200, json=_payload_all())

    results: dict[str, object] = {}

    def owner():
        with _client_with_handler(handler) as c:
            results["owner"] = binance.fetch_all_spots(client=c, now_ts=50.0, tick_seconds=1)

    def waiter():
        with _client_with_handler(handler) as c:
            results["waiter"] = binance.fetch_all_spots(client=c, now_ts=50.4, tick_seconds=1)

    t_owner = threading.Thread(target=owner)
    t_owner.start()
    assert airborne.wait(timeout=5.0), "owner GET never started"
    t_waiter = threading.Thread(target=waiter)
    t_waiter.start()
    time.sleep(0.1)  # let the waiter reach the in-flight wait
    release.set()
    t_owner.join(timeout=5.0)
    t_waiter.join(timeout=5.0)

    assert call_count["n"] == 2
    assert results["owner"] is None
    assert results["waiter"] is not None
    # The retry's success repaired the bucket: a later same-bucket caller is
    # a cache hit (no third GET), not a negative-cache None.
    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=50.9, tick_seconds=1) is not None
    assert call_count["n"] == 2


def test_hung_owner_exhausted_budget_returns_none_not_second_get():
    # F2: the bounded wait expiring with the bucket STILL unresolved (owner
    # hung, not failed) must NOT stack a second full-timeout GET on top of
    # the spent wait — the deadline budget is gone, so the call returns None.
    # Worst case stays ONE ``timeout``, never 2x.
    binance.reset_cache()
    airborne = threading.Event()
    release = threading.Event()

    def hung_handler(request: httpx.Request) -> httpx.Response:
        airborne.set()
        assert release.wait(timeout=10.0), "test never released the hung GET"
        return httpx.Response(200, json=_payload_all())

    waiter_calls = {"n": 0}

    def live_handler(request: httpx.Request) -> httpx.Response:
        waiter_calls["n"] += 1
        return httpx.Response(200, json=_payload_all())

    results: dict[str, object] = {}

    def owner():
        with _client_with_handler(hung_handler) as c:
            results["owner"] = binance.fetch_all_spots(client=c, now_ts=60.0, tick_seconds=1)

    t_owner = threading.Thread(target=owner)
    t_owner.start()
    try:
        assert airborne.wait(timeout=5.0), "owner GET never started"
        t0 = time.monotonic()
        with _client_with_handler(live_handler) as c:
            # ``timeout`` is the WHOLE deadline budget: wait + any fallback.
            result = binance.fetch_all_spots(
                client=c, now_ts=60.4, tick_seconds=1, timeout=0.3
            )
        elapsed = time.monotonic() - t0
    finally:
        release.set()
        t_owner.join(timeout=5.0)

    assert result is None
    assert waiter_calls["n"] == 0
    assert elapsed < 1.5  # bounded by ~timeout (0.3s), never wait + full GET


def test_unresolved_wake_with_residual_budget_does_bounded_inline_get():
    # F2: an owner that resolved its in-flight Event WITHOUT resolving the
    # bucket (crash path — the finally sets the Event on a raised exception)
    # must not strand the waiter: waking with residual deadline budget, it
    # performs its own inline GET bounded by that RESIDUAL budget, not a
    # fresh full timeout.
    binance.reset_cache()
    calls = {"n": 0}
    seen_timeout: dict[str, float] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        seen_timeout["read"] = request.extensions["timeout"]["read"]
        return httpx.Response(200, json=_payload_all())

    resolved_without_cache = threading.Event()
    resolved_without_cache.set()
    with binance._cache_lock:
        binance._inflight[(80, tuple(config.COINS))] = resolved_without_cache
    try:
        with _client_with_handler(handler) as c:
            result = binance.fetch_all_spots(
                client=c, now_ts=80.0, tick_seconds=1, timeout=1.5
            )
    finally:
        binance.reset_cache()

    assert result is not None
    assert calls["n"] == 1
    # Bounded by the residual budget: never more than what was left of the
    # 1.5s deadline, and only attempted with a meaningful (>0.2s) budget.
    assert 0.2 < seen_timeout["read"] <= 1.5


def test_reset_cache_clears_inflight_markers():
    # Every test isolates via reset_cache(); it must also wake and drop any
    # in-flight marker so no stranded Event leaks across tests / restarts.
    binance.reset_cache()
    stranded = threading.Event()
    with binance._cache_lock:
        binance._inflight[(123, ("BTC",))] = stranded
    binance.reset_cache()
    assert stranded.is_set()
    with binance._cache_lock:
        assert binance._inflight == {}


def test_rate_limit_returns_none():
    binance.reset_cache()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "2"})

    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=1.0, tick_seconds=1) is None


def test_partial_response_downgraded_to_none():
    binance.reset_cache()
    payload = _payload_all()
    payload = [row for row in payload if row["symbol"] != binance.SYMBOLS["XRP"]]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=1.0, tick_seconds=1) is None


def test_out_of_bounds_coin_drops_batch():
    binance.reset_cache()
    payload = _payload_all()
    for row in payload:
        if row["symbol"] == "BTCUSDT":
            row["lastPrice"] = "50"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=1.0, tick_seconds=1) is None


def test_missing_close_time_rejects_row_and_downgrades_batch():
    # audit w19oxqf24 #11: a row without closeTime must fail CLOSED (reject →
    # incomplete batch → None), NOT fail open by fabricating now_ts.
    binance.reset_cache()
    payload = _payload_all()
    for row in payload:
        if row["symbol"] == "BTCUSDT":
            del row["closeTime"]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=1.0, tick_seconds=1) is None


def test_zero_close_time_rejects_row_and_downgrades_batch():
    # audit w19oxqf24 #11: closeTime == 0 is treated the same as missing.
    binance.reset_cache()
    payload = _payload_all()
    for row in payload:
        if row["symbol"] == "BTCUSDT":
            row["closeTime"] = 0

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with _client_with_handler(handler) as c:
        assert binance.fetch_all_spots(client=c, now_ts=1.0, tick_seconds=1) is None


def test_publish_time_ignores_closetime_clock():
    # audit w19oxqf24 #2: closeTime is wall-clock and must NOT be used as the
    # freshness stamp. Even though closeTime advances between buckets, an
    # unchanged price keeps the first-seen publish_time so the spot ages.
    binance.reset_cache()
    call = {"close_time_ms": 1_000_000_000_000}

    def handler(request: httpx.Request) -> httpx.Response:
        # closeTime advances ~1s every request, exactly the misleading
        # behavior that made the stale_spot gate inert.
        call["close_time_ms"] += 1000
        return httpx.Response(200, json=_payload_all(close_time_ms=call["close_time_ms"]))

    with _client_with_handler(handler) as c:
        first = binance.fetch_all_spots(client=c, now_ts=100.0, tick_seconds=1)
        # Distinct later bucket, price unchanged → publish_time must NOT track
        # the ever-advancing closeTime; it stays anchored at the first-seen ts.
        later = binance.fetch_all_spots(client=c, now_ts=160.0, tick_seconds=1)

    assert first is not None and later is not None
    assert first["BTC"].publish_time == 100
    assert later["BTC"].publish_time == 100  # frozen price → aged stamp


def test_frozen_price_ages_so_stale_spot_gate_can_fire():
    # audit w19oxqf24 #2: a price frozen across distinct fetch buckets must
    # produce a growing spot_age_s so gates.py stale_spot eventually fires.
    binance.reset_cache()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_payload_all())  # same prices every call

    with _client_with_handler(handler) as c:
        snap0 = binance.fetch_all_spots(client=c, now_ts=1000.0, tick_seconds=1)
        snap1 = binance.fetch_all_spots(client=c, now_ts=1001.0, tick_seconds=1)
        snap2 = binance.fetch_all_spots(client=c, now_ts=1030.0, tick_seconds=1)

    assert snap0 is not None and snap1 is not None and snap2 is not None
    # publish_time stays anchored at first-seen; age = now_ts - publish_time grows.
    assert snap0["BTC"].publish_time == 1000
    assert snap1["BTC"].publish_time == 1000
    assert snap2["BTC"].publish_time == 1000
    age_at_1030 = 1030.0 - snap2["BTC"].publish_time
    assert age_at_1030 == pytest.approx(30.0)
    # Past the staleness window the gate input would exceed SPOT_STALENESS_S.
    assert age_at_1030 > config.SPOT_STALENESS_S


def test_price_move_resets_freshness_stamp():
    # audit w19oxqf24 #2: a genuine price move re-anchors publish_time to now,
    # so a moving feed never trips the stale gate.
    binance.reset_cache()
    state = {"btc": 67_432.12}

    def handler(request: httpx.Request) -> httpx.Response:
        prices = {
            "BTC": state["btc"], "ETH": 3_120.55, "SOL": 175.20,
            "BNB": 590.00, "XRP": 0.52, "DOGE": 0.13,
        }
        return httpx.Response(200, json=[
            {"symbol": binance.SYMBOLS[c], "lastPrice": str(p), "closeTime": 1_715_923_440_000}
            for c, p in prices.items()
        ])

    with _client_with_handler(handler) as c:
        a = binance.fetch_all_spots(client=c, now_ts=2000.0, tick_seconds=1)
        state["btc"] = 67_500.00  # price moved
        b = binance.fetch_all_spots(client=c, now_ts=2050.0, tick_seconds=1)

    assert a is not None and b is not None
    assert a["BTC"].publish_time == 2000
    assert b["BTC"].publish_time == 2050  # re-anchored on the move


def test_fetch_recent_minute_bars_decodes_closed_and_current_klines():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/klines"
        assert request.url.params["symbol"] == "BTCUSDT"
        assert request.url.params["interval"] == "1m"
        return httpx.Response(
            200,
            json=[
                [
                    1_715_923_800_000,
                    "67000.0",
                    "67010.0",
                    "66990.0",
                    "67005.0",
                    "1.0",
                    1_715_923_859_999,
                ],
                [
                    1_715_923_860_000,
                    "67005.0",
                    "67020.0",
                    "67000.0",
                    "67012.0",
                    "1.0",
                    1_715_923_919_999,
                ],
            ],
        )

    with _client_with_handler(handler) as c:
        bars = binance.fetch_recent_minute_bars("BTC", client=c, now_ts=1_715_923_880)

    assert len(bars) == 2
    assert bars[0].start_ts == 1_715_923_800
    assert bars[0].open == pytest.approx(67_000.0)
    assert bars[0].close == pytest.approx(67_005.0)
    assert bars[0].is_closed is True
    assert bars[1].is_closed is False

