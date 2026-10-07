"""W4 parser parity: the WS mirror and the REST ``fetch_books_batch`` MUST
produce byte-identical ``BookSnapshot`` objects for the same book.

``total_ask_depth_usd`` feeds the exp044 book_consumption gate and the walk
limit price, so a silent WS-vs-REST divergence in the parse math would move
live decisions off the parity-certified REST book. This locks that they share
one parser (``_snapshot_from_payload``) all the way to top_ask, depth, and the
full ladder — including the price_change-merged case where the WS ladder is
built from a snapshot + deltas rather than a single payload.
"""
from __future__ import annotations

import time

from cryptobot_polymarket.ws_mirror import ClobBookWsMirror
from cryptobot_polymarket.clob import TokenBucket, fetch_books_batch


class _FakeResp:
    status_code = 200

    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class _FakeClient:
    """Minimal httpx.Client stand-in: POST returns the canned books payload."""

    def __init__(self, data):
        self._data = data

    def post(self, url, json=None):  # noqa: A002 — mirror httpx signature
        return _FakeResp(self._data)


def _rest_snapshot(token, asks, bids, *, now_ts):
    """Parse one book through the REST batch path and return its snapshot."""
    item = {
        "asset_id": token,
        "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
        "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
    }
    batch = fetch_books_batch(
        [token],
        client=_FakeClient([item]),
        bucket=TokenBucket(rate_per_s=100.0, burst=100),
        now_ts=now_ts,
    )
    assert batch is not None
    return batch[token]


def _assert_identical(ws_snap, rest_snap):
    assert ws_snap is not None and rest_snap is not None
    assert ws_snap.top_ask == rest_snap.top_ask
    assert ws_snap.total_ask_depth_usd == rest_snap.total_ask_depth_usd
    # Full ladder byte-identical (both sides, same order, same float values).
    assert ws_snap.asks == rest_snap.asks
    assert ws_snap.bids == rest_snap.bids


def test_ws_and_rest_parse_identically():
    now = 1_700_000_000.0
    token = "tok"
    asks = [(0.55, 50), (0.52, 100), (0.60, 25)]  # deliberately unsorted
    bids = [(0.48, 200), (0.45, 10)]

    rest_snap = _rest_snapshot(token, asks, bids, now_ts=now)

    m = ClobBookWsMirror(max_age_ms=700)
    m._handle_message(
        {
            "event_type": "book",
            "asset_id": token,
            "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
            "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
            "timestamp": str(int(now * 1000)),
        },
        recv_ts=now,
    )
    ws_snap = m.get(token, now_ts=now + 0.1)

    _assert_identical(ws_snap, rest_snap)
    # Venue timestamp stamped from the message (ms→s), matching the REST now_ts.
    assert abs(ws_snap.fetched_at_ts - rest_snap.fetched_at_ts) <= 2e-3


def test_ws_price_change_merge_matches_rest_of_post_merge_book():
    """A retained WS ladder mutated by price_change deltas must parse identically
    to REST fetching the resulting post-merge book fresh."""
    now = 1_700_000_000.0
    token = "tok"

    m = ClobBookWsMirror(max_age_ms=700)
    # Initial full snapshot.
    m._handle_message(
        {
            "event_type": "book",
            "asset_id": token,
            "asks": [{"price": "0.52", "size": "100"}, {"price": "0.55", "size": "50"}],
            "bids": [{"price": "0.48", "size": "200"}],
            "timestamp": str(int(now * 1000)),
        },
        recv_ts=now,
    )
    # Deltas: resize 0.52 ask, remove the 0.55 ask (size 0), add a new 0.53 ask,
    # and bump a bid level.
    now2 = now + 0.4
    m._handle_message(
        {
            "event_type": "price_change",
            "asset_id": token,
            "changes": [
                {"price": "0.52", "size": "70", "side": "SELL"},
                {"price": "0.55", "size": "0", "side": "SELL"},
                {"price": "0.53", "size": "40", "side": "SELL"},
                {"price": "0.48", "size": "250", "side": "BUY"},
            ],
            "timestamp": str(int(now2 * 1000)),
        },
        recv_ts=now2,
    )
    ws_snap = m.get(token, now_ts=now2 + 0.1)

    # The book REST would see after those changes landed.
    rest_snap = _rest_snapshot(
        token,
        asks=[(0.52, 70), (0.53, 40)],
        bids=[(0.48, 250)],
        now_ts=now2,
    )
    _assert_identical(ws_snap, rest_snap)
