"""U7 Polymarket CLOB book adapter tests."""

from __future__ import annotations

import httpx
import pytest

from cryptobot_polymarket import clob


def _client_with_handler(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_full_asks_sorted_ascending():
    payload = {
        "asks": [
            {"price": "0.58", "size": "40"},
            {"price": "0.55", "size": "50"},
            {"price": "0.57", "size": "30"},
        ],
        "bids": [{"price": "0.50", "size": "20"}],
    }
    fresh_bucket = clob.TokenBucket(rate_per_s=100.0, burst=10)
    with _client_with_handler(lambda r: httpx.Response(200, json=payload)) as c:
        snap = clob.fetch_top_book("token-x", client=c, bucket=fresh_bucket, now_ts=1.0)

    assert snap is not None
    prices = [a["price"] for a in snap.asks]
    assert prices == [0.55, 0.57, 0.58]
    assert snap.top_ask == pytest.approx(0.55)


def test_empty_book_returns_none():
    fresh_bucket = clob.TokenBucket(rate_per_s=100.0, burst=10)
    with _client_with_handler(lambda r: httpx.Response(200, json={"asks": [], "bids": []})) as c:
        assert clob.fetch_top_book("token-x", client=c, bucket=fresh_bucket, now_ts=1.0) is None


def test_rate_limit_local_bucket_skips_fetch():
    # Empty bucket — first call drains the only token, second call gets None.
    fresh_bucket = clob.TokenBucket(rate_per_s=0.0, burst=1)
    call_count = {"n": 0}

    def handler(req):
        call_count["n"] += 1
        return httpx.Response(200, json={"asks": [{"price": "0.5", "size": "10"}], "bids": []})

    with _client_with_handler(handler) as c:
        a = clob.fetch_top_book("t1", client=c, bucket=fresh_bucket, now_ts=1.0)
        b = clob.fetch_top_book("t2", client=c, bucket=fresh_bucket, now_ts=1.0)

    assert a is not None
    assert b is None
    assert call_count["n"] == 1


def test_server_429_returns_none():
    fresh_bucket = clob.TokenBucket(rate_per_s=100.0, burst=10)
    with _client_with_handler(lambda r: httpx.Response(429)) as c:
        assert clob.fetch_top_book("token-x", client=c, bucket=fresh_bucket) is None


def test_malformed_levels_skipped():
    payload = {
        "asks": [
            {"price": "0.55", "size": "50"},
            {"price": "abc", "size": "10"},
            {"price": "-0.10", "size": "5"},  # negative dropped
            {"price": "0.60", "size": "0"},   # zero size dropped
        ],
        "bids": [],
    }
    fresh_bucket = clob.TokenBucket(rate_per_s=100.0, burst=10)
    with _client_with_handler(lambda r: httpx.Response(200, json=payload)) as c:
        snap = clob.fetch_top_book("t", client=c, bucket=fresh_bucket)
    assert snap is not None
    assert [a["price"] for a in snap.asks] == [0.55]


def test_does_not_cap_ask_or_bid_levels():
    payload = {
        "asks": [{"price": str(0.50 + 0.01 * i), "size": "10"} for i in range(10)],
        "bids": [{"price": str(0.40 - 0.01 * i), "size": "10"} for i in range(10)],
    }
    fresh_bucket = clob.TokenBucket(rate_per_s=100.0, burst=10)
    with _client_with_handler(lambda r: httpx.Response(200, json=payload)) as c:
        snap = clob.fetch_top_book("t", client=c, bucket=fresh_bucket)
    assert snap is not None
    assert len(snap.asks) == 10
    assert len(snap.bids) == 10
    assert snap.asks[0]["price"] == pytest.approx(0.50)
    assert snap.asks[-1]["price"] == pytest.approx(0.59)
    assert snap.bids[0]["price"] == pytest.approx(0.31)
    assert snap.bids[-1]["price"] == pytest.approx(0.40)


def test_book_read_keeps_client_timeout_and_has_no_predictfun_knob():
    # Cross-venue parity, asserted from THIS side: predict.fun's per-request
    # read_timeout knob must not couple into Polymarket. fetch_top_book has
    # no such parameter and its pooled book read keeps the client-level
    # timeout unchanged.
    #
    # Lived in the predict.fun suite until Phase 2, where it was the only
    # thing making that package import this one. Stating it here needs no
    # cross-venue import at all.
    import inspect

    assert "read_timeout" not in inspect.signature(clob.fetch_top_book).parameters

    seen: dict = {}

    def handler(req):
        seen["timeout"] = req.extensions.get("timeout")
        return httpx.Response(
            200,
            json={"asks": [{"price": "0.62", "size": "5"}], "bids": []},
        )

    c = httpx.Client(
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(3.0, connect=2.0),
    )
    with c:
        clob.fetch_top_book(
            "tok", client=c,
            bucket=clob.TokenBucket(rate_per_s=100.0, burst=10),
        )
    assert seen["timeout"] == {
        "connect": 2.0, "read": 3.0, "write": 3.0, "pool": 3.0
    }


# ---- fill-poll trailing-sleep skip ------------------------------------------
