"""WS book mirror: message->snapshot parse, RECV-LIVENESS freshness gate,
price_change deltas, and fallback-on-disconnect. No live socket is opened —
messages are fed directly and the connection is a fake context manager."""
from __future__ import annotations

import json
import threading
import time

import pytest

from dataclasses import dataclass

from cryptobot_polymarket.ws_mirror import ClobBookWsMirror, ShadowBookComparator


def _book_msg(token, asks, bids, *, ts_ms):
    return {
        "event_type": "book",
        "asset_id": token,
        "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
        "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
        "timestamp": str(int(ts_ms)),
    }


def test_book_message_parses_into_snapshot():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=1500)
    # Feed a raw JSON string frame, as the socket would deliver it.
    m._handle_message(
        json.dumps(_book_msg("tok", [(0.52, 100), (0.55, 50)], [(0.48, 200)],
                             ts_ms=now * 1000)),
        recv_ts=now,
    )
    snap = m.get("tok", now_ts=now + 0.1)
    assert snap is not None
    assert snap.top_ask == 0.52
    # depth = 0.52*100 + 0.55*50
    assert snap.total_ask_depth_usd == pytest.approx(0.52 * 100 + 0.55 * 50)
    # fetched_at_ts is the VENUE timestamp (ms-truncated), not local recv time.
    assert snap.fetched_at_ts == pytest.approx(now, abs=2e-3)


def test_freshness_gate_uses_recv_liveness_not_venue_ts():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    # A book whose VENUE timestamp is 5s old but a frame was JUST received: a
    # quiet-but-connected updown book (ladder unchanged for seconds). Recv-
    # liveness is fresh, so the snapshot MUST still be served — the whole point
    # of keying freshness on recv-liveness rather than venue last-change ts.
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=(now - 5.0) * 1000),
        recv_ts=now,
    )
    snap = m.get("tok", now_ts=now)
    assert snap is not None
    # venue-ts age stays available for telemetry (fetched_at_ts = venue clock).
    assert snap.fetched_at_ts == pytest.approx(now - 5.0, abs=2e-3)

    # Frozen SOCKET: no frame received within max_age -> recv-liveness stale ->
    # serves nothing (half-open guard), even though the ladder is still cached.
    m._last_recv_monotonic = time.monotonic() - 2.0
    assert m.get("tok", now_ts=now) is None
    assert m.get("unknown", now_ts=now) is None

    # A fresh frame re-arms liveness and the same cached ladder serves again.
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("tok", now_ts=now) is not None


def test_price_change_delta_updates_top_of_book():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=1500)
    m._handle_message(
        _book_msg("tok", [(0.52, 100), (0.55, 50)], [(0.48, 200)], ts_ms=now * 1000),
        recv_ts=now,
    )
    # Remove the 0.52 ask (size 0) and add a new best 0.60 bid.
    m._handle_message(
        {
            "event_type": "price_change",
            "asset_id": "tok",
            "changes": [
                {"price": "0.52", "side": "SELL", "size": "0"},
                {"price": "0.50", "side": "BUY", "size": "10"},
            ],
            "timestamp": str(int((now + 0.5) * 1000)),
        },
        recv_ts=now + 0.5,
    )
    snap = m.get("tok", now_ts=now + 0.6)
    assert snap is not None
    assert snap.top_ask == 0.55  # 0.52 level was removed
    # freshness re-stamped to the delta's venue timestamp (ms-truncated)
    assert snap.fetched_at_ts == pytest.approx(now + 0.5, abs=2e-3)


def _market_price_change_msg(changes, *, ts_ms, market="0xmkt"):
    """The shape the venue ACTUALLY sends (verified live 2026-08-06): no
    top-level ``asset_id``; a ``price_changes`` list whose entries each carry
    their own ``asset_id``, covering BOTH tokens of the market in one frame."""
    return {
        "event_type": "price_change",
        "market": market,
        "price_changes": [dict(ch) for ch in changes],
        "timestamp": str(int(ts_ms)),
    }


def test_market_scoped_price_change_updates_both_tokens():
    """REGRESSION (2026-08-06): the venue's price_change is MARKET-scoped.

    Reading the token from the top level made ``token`` "" and dropped EVERY
    delta, so the mirror only advanced on full ``book`` frames and served a
    5-9s-old, wrong top-of-book — the stall watchdog's eviction storm.
    """
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=1500)
    for tok, ask, bid in (("yes", 0.52, 0.48), ("no", 0.52, 0.48)):
        m._handle_message(
            _book_msg(tok, [(ask, 100), (0.55, 50)], [(bid, 200)], ts_ms=now * 1000),
            recv_ts=now,
        )
    m._handle_message(
        _market_price_change_msg(
            [
                {"asset_id": "yes", "price": "0.52", "side": "SELL", "size": "0"},
                {"asset_id": "no", "price": "0.60", "side": "SELL", "size": "7"},
            ],
            ts_ms=(now + 0.5) * 1000,
        ),
        recv_ts=now + 0.5,
    )
    yes = m.get("yes", now_ts=now + 0.6)
    no = m.get("no", now_ts=now + 0.6)
    assert yes is not None and no is not None
    # BOTH tokens in the one frame were applied.
    assert yes.top_ask == 0.55  # 0.52 level removed
    assert no.top_ask == 0.52  # untouched best ask
    assert {lvl["price"]: lvl["size"] for lvl in no.asks}[0.60] == 7.0
    # freshness re-stamped from the frame's venue timestamp for both.
    for snap in (yes, no):
        assert snap.fetched_at_ts == pytest.approx(now + 0.5, abs=2e-3)


def test_market_scoped_price_change_fires_ask_callback_per_token():
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=1500)
    m.set_ask_update_callback(lambda tok, ts: fired.append(tok))
    m._handle_message(
        _market_price_change_msg(
            [
                {"asset_id": "yes", "price": "0.55", "side": "SELL", "size": "80"},
                {"asset_id": "no", "price": "0.40", "side": "BUY", "size": "80"},
            ],
            ts_ms=now * 1000,
        ),
        recv_ts=now,
    )
    # ask-side change on `yes` only; `no`'s bid-side change must NOT wake it.
    assert fired == ["yes"]


def test_market_scoped_price_change_respects_evict_and_unsubscribe():
    """Per-token gating must survive the regrouping: a resync-armed token and an
    unsubscribed token must both be skipped while the SIBLING in the same frame
    still updates."""
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=1500)
    for tok in ("yes", "no"):
        m._handle_message(
            _book_msg(tok, [(0.52, 100)], [(0.48, 200)], ts_ms=now * 1000),
            recv_ts=now,
        )
    m.note_tokens(["yes", "no"])
    m.evict("yes")  # watchdog eviction arms the resync
    m._handle_message(
        _market_price_change_msg(
            [
                {"asset_id": "yes", "price": "0.30", "side": "SELL", "size": "9"},
                {"asset_id": "no", "price": "0.30", "side": "SELL", "size": "9"},
            ],
            ts_ms=(now + 0.5) * 1000,
        ),
        recv_ts=now + 0.5,
    )
    assert m.get("yes", now_ts=now + 0.6) is None, "resync token rebuilt partially"
    assert "yes" not in m._asks
    assert m.get("no", now_ts=now + 0.6).top_ask == 0.30

    # Window roll: `no` leaves the subscribed set, `keep` joins it.
    m._handle_message(
        _book_msg("keep", [(0.52, 100)], [(0.48, 200)], ts_ms=now * 1000),
        recv_ts=now,
    )
    m.note_tokens(["keep"])
    m._handle_message(
        _market_price_change_msg(
            [
                {"asset_id": "no", "price": "0.10", "side": "SELL", "size": "9"},
                {"asset_id": "keep", "price": "0.10", "side": "SELL", "size": "9"},
            ],
            ts_ms=(now + 1.0) * 1000,
        ),
        recv_ts=now + 1.0,
    )
    assert m.get("no", now_ts=now + 1.1) is None, "unsubscribed token resurrected"
    assert "no" not in m._asks
    assert m.get("keep", now_ts=now + 1.1).top_ask == 0.10


def test_empty_ask_book_is_dropped():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=1500)
    m._handle_message(_book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
                      recv_ts=now)
    assert m.get("tok", now_ts=now) is not None
    # Book empties on the ask side -> snapshot dropped, not served stale.
    m._handle_message(_book_msg("tok", [], [(0.4, 100)], ts_ms=(now + 0.1) * 1000),
                      recv_ts=now + 0.1)
    assert m.get("tok", now_ts=now + 0.2) is None


def test_unknown_and_malformed_messages_are_ignored():
    m = ClobBookWsMirror(max_age_ms=1500)
    # None of these should raise.
    m._handle_message("not json at all", recv_ts=time.time())
    m._handle_message(json.dumps({"event_type": "pong"}), recv_ts=time.time())
    m._handle_message(json.dumps([{"foo": "bar"}, 42]), recv_ts=time.time())
    assert m.get("tok", now_ts=time.time()) is None


class _FakeWs:
    """Context-manager fake socket that subscribes then 'disconnects'."""

    def __init__(self, sent):
        self._sent = sent

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def send(self, payload):
        self._sent.append(payload)

    def recv(self, timeout=None):
        raise ConnectionError("socket dropped")


def test_fallback_on_disconnect_clears_cache_and_serves_nothing():
    now = time.time()
    sent: list = []

    m = ClobBookWsMirror(
        max_age_ms=1500,
        connect_factory=lambda: _FakeWs(sent),
        reconnect_backoff_s=0.05,
        reconnect_backoff_max_s=0.05,
    )
    # Pre-seed the cache as if a prior session had data.
    m._handle_message(_book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
                      recv_ts=now)
    m.note_tokens(["tok"])
    assert m.get("tok", now_ts=now) is not None

    m.start()
    try:
        # The fake socket subscribes then raises on recv; the mirror must clear
        # its cache so the pipeline falls through to REST (serves nothing).
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if m.get("tok", now_ts=time.time()) is None and sent:
                break
            time.sleep(0.02)
    finally:
        m.stop()

    assert sent, "mirror should have sent a subscribe frame before disconnect"
    sub = json.loads(sent[0])
    assert sub["type"] == "market"
    assert sub["assets_ids"] == ["tok"]
    assert m.get("tok", now_ts=time.time()) is None
    assert m._thread is None


# -- W3 shadow-compare --------------------------------------------------------


@dataclass
class _RestSnap:
    top_ask: float
    total_ask_depth_usd: float


def _mirror_with_book(token, asks, *, now):
    m = ClobBookWsMirror(max_age_ms=700)
    m._handle_message(
        _book_msg(token, asks, [(0.4, 100)], ts_ms=now * 1000), recv_ts=now
    )
    return m


def test_shadow_counts_agreement_without_divergence():
    now = time.time()
    m = _mirror_with_book("tok", [(0.52, 100), (0.55, 50)], now=now)
    depth = 0.52 * 100 + 0.55 * 50
    cmp = ShadowBookComparator(m)
    cmp.observe("tok", _RestSnap(top_ask=0.52, total_ask_depth_usd=depth),
                now_ts=now + 0.1)
    assert cmp.n_compared == 1
    assert cmp.n_ws_missing == 0
    assert cmp.n_divergent_price == 0
    assert cmp.n_divergent_depth == 0


def test_shadow_flags_price_and_depth_divergence():
    now = time.time()
    m = _mirror_with_book("tok", [(0.52, 100), (0.55, 50)], now=now)
    ws_depth = 0.52 * 100 + 0.55 * 50
    cmp = ShadowBookComparator(m, price_tick=0.01, depth_rel_tol=0.05,
                               log_min_interval_s=0.0)
    # top_ask off by 3 ticks and depth 40% low → both divergences fire.
    cmp.observe("tok", _RestSnap(top_ask=0.55, total_ask_depth_usd=ws_depth * 0.6),
                now_ts=now + 0.1)
    assert cmp.n_compared == 1
    assert cmp.n_divergent_price == 1
    assert cmp.n_divergent_depth == 1


def test_shadow_counts_ws_missing_when_mirror_has_no_book():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)  # never fed a book
    cmp = ShadowBookComparator(m)
    cmp.observe("tok", _RestSnap(top_ask=0.5, total_ask_depth_usd=100.0),
                now_ts=now)
    assert cmp.n_compared == 1
    assert cmp.n_ws_missing == 1
    assert cmp.n_divergent_price == 0


def test_shadow_never_serves_a_book_to_the_decision_path():
    # The comparator exposes note_tokens/start/stop but NOT get — it can never
    # be mistaken for an authoritative BookSourceChain source.
    m = ClobBookWsMirror(max_age_ms=700)
    cmp = ShadowBookComparator(m)
    assert not hasattr(cmp, "get")
    assert not hasattr(cmp, "get_tagged")


# ── 2026-07-25 orphan fix ────────────────────────────────────────────────
# Live measurement: the venue market channel pushes ~822 msg/s (p50 1ms
# inter-frame) on the open *-updown-* windows, while this mirror served
# decision books with a p50 age of 6,935ms and a MAX of 114,498ms. Cause: a
# token that rolled out of the window kept its cached ladder forever, and
# get()'s only gate was CONNECTION-level recv-liveness, which any frame
# (ping, another token's ack) re-stamps. A two-windows-old ladder priced
# live FAKs.

def test_unsubscribed_token_is_evicted_not_served_stale():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m.note_tokens(["old_tok", "keep_tok"])
    for tok in ("old_tok", "keep_tok"):
        m._handle_message(
            _book_msg(tok, [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
            recv_ts=now,
        )
    assert m.get("old_tok", now_ts=now) is not None

    # Window rolls: old_tok leaves the set. The socket stays live and keeps
    # delivering frames for keep_tok, so recv-liveness alone would happily
    # keep serving old_tok's frozen ladder — this is the 114s bug.
    m.note_tokens(["keep_tok", "new_tok"])
    m._handle_message(
        _book_msg("keep_tok", [(0.6, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("old_tok", now_ts=now) is None, "orphan must not serve"
    assert m.get("keep_tok", now_ts=now) is not None, "subscribed token unaffected"
    # ladders dropped too, so a later price_change can't resurrect a partial book
    assert "old_tok" not in m._asks and "old_tok" not in m._bids


def test_eviction_can_be_disabled_for_rollback():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700, evict_unsubscribed=False)
    m.note_tokens(["old_tok"])
    m._handle_message(
        _book_msg("old_tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    m.note_tokens(["new_tok"])
    # Pre-fix behavior, kept behind CRYPTOBOT_BOOK_WS_EVICT_UNSUBSCRIBED=0.
    assert m.get("old_tok", now_ts=now) is not None


def test_note_tokens_no_op_when_set_unchanged():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m.note_tokens(["a", "b"])
    m._handle_message(
        _book_msg("a", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000), recv_ts=now
    )
    v = m._tokens_version
    m.note_tokens(["a", "b"])
    assert m._tokens_version == v, "identical set must not force a resubscribe"
    assert m.get("a", now_ts=now) is not None


def test_per_token_book_age_gate_when_armed():
    now = time.time()
    # Safety net armed at 400ms; recv-liveness is deliberately fresh throughout.
    m = ClobBookWsMirror(max_age_ms=700, max_book_age_ms=400)
    m.note_tokens(["tok"])
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=(now - 5.0) * 1000),
        recv_ts=now,
    )
    # Venue ladder is 5s old: liveness says "socket fine", the age gate says no.
    assert m.get("tok", now_ts=now) is None
    # A genuinely fresh ladder passes.
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("tok", now_ts=now) is not None


def test_book_age_gate_off_by_default_is_byte_identical():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)  # max_book_age_ms defaults to 0
    m.note_tokens(["tok"])
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=(now - 30.0) * 1000),
        recv_ts=now,
    )
    # Unchanged pre-fix semantics: a quiet-but-connected ladder still serves.
    assert m.get("tok", now_ts=now) is not None


def test_evicted_token_is_not_resurrected_by_a_later_frame():
    """Eviction must be airtight against the venue's subscription semantics.

    _apply_book/_apply_price_change accept any token the socket delivers. If the
    venue MERGES subscriptions (the old _stream_once comment asserted "the venue
    merges" — never verified) it keeps sending the rolled-off token forever, and
    without a membership filter the very next frame would rewrite the cache entry
    note_tokens just evicted, making the whole fix a no-op. Also covers frames
    already in flight during a resubscribe.
    """
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m.note_tokens(["old_tok"])
    m._handle_message(
        _book_msg("old_tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("old_tok", now_ts=now) is not None

    m.note_tokens(["new_tok"])                       # window rolls
    assert m.get("old_tok", now_ts=now) is None      # evicted

    # Venue keeps streaming the rolled-off token (merge semantics / in-flight).
    m._handle_message(
        _book_msg("old_tok", [(0.9, 100)], [(0.1, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("old_tok", now_ts=now) is None, "evicted token was resurrected"
    assert "old_tok" not in m._asks and "old_tok" not in m._bids

    # A price_change delta must not recreate partial ladders either.
    m._handle_message(
        {
            "event_type": "price_change",
            "asset_id": "old_tok",
            "changes": [{"price": "0.77", "size": "50", "side": "SELL"}],
            "timestamp": str(int(now * 1000)),
        },
        recv_ts=now,
    )
    assert m.get("old_tok", now_ts=now) is None
    assert "old_tok" not in m._asks


def test_frames_are_accepted_before_the_first_note_tokens():
    """Empty _tokens = startup. The filter must not swallow everything."""
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("tok", now_ts=now) is not None


def test_membership_filter_off_when_eviction_disabled():
    """The rollback path stays byte-identical to pre-fix behavior."""
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700, evict_unsubscribed=False)
    m.note_tokens(["old_tok"])
    m.note_tokens(["new_tok"])
    m._handle_message(
        _book_msg("old_tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("old_tok", now_ts=now) is not None


# --------------------------------------------------------------------------- #
# Fast-trigger EVENT rung hook (operator 2026-07-30). The mirror notifies core
# once per frame that changed the ASK ladder so an entry can be re-evaluated
# within ~20ms of the push instead of waiting for the next 1Hz tick boundary.
# --------------------------------------------------------------------------- #

def _price_change_msg(token, changes, *, ts_ms):
    return {
        "event_type": "price_change",
        "asset_id": token,
        "changes": changes,
        "timestamp": str(int(ts_ms)),
    }


def test_no_callback_registered_is_a_no_op():
    """Default: the hook is None and the mirror behaves exactly as before."""
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    assert m._on_ask_update is None
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.get("tok", now_ts=now) is not None


def test_book_frame_fires_the_ask_update_callback():
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(lambda tok, ts: fired.append((tok, ts)))
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert len(fired) == 1
    assert fired[0][0] == "tok"
    assert fired[0][1] == pytest.approx(now, abs=2e-3)


def test_ask_side_delta_fires_the_callback():
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(lambda tok, ts: fired.append(tok))
    m._handle_message(
        _price_change_msg(
            "tok", [{"price": "0.55", "size": "80", "side": "SELL"}],
            ts_ms=now * 1000,
        ),
        recv_ts=now,
    )
    assert fired == ["tok"]


def test_bid_only_delta_does_not_fire_the_callback():
    """An entry is priced off the ASK — a bid-only delta must not wake the
    event rung (it would be pure CPU at the measured ~822 msg/s)."""
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(lambda tok, ts: fired.append(tok))
    m._handle_message(
        _price_change_msg(
            "tok", [{"price": "0.40", "size": "80", "side": "BUY"}],
            ts_ms=now * 1000,
        ),
        recv_ts=now,
    )
    assert fired == []


def test_mixed_delta_fires_once():
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(lambda tok, ts: fired.append(tok))
    m._handle_message(
        _price_change_msg(
            "tok",
            [
                {"price": "0.40", "size": "80", "side": "BUY"},
                {"price": "0.55", "size": "80", "side": "ASK"},
            ],
            ts_ms=now * 1000,
        ),
        recv_ts=now,
    )
    assert fired == ["tok"]


def test_unsubscribed_token_never_fires():
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(lambda tok, ts: fired.append(tok))
    m.note_tokens(["new_tok"])
    m._handle_message(
        _book_msg("old_tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert fired == []


def test_raising_callback_cannot_take_the_mirror_down():
    """Load-bearing: an escaping exception would unwind _handle_message ->
    _stream_once -> _run, whose finally CLEARS the cache and backs off. One bad
    consumer would silently degrade the whole bot to REST."""
    now = time.time()
    calls = []

    def _boom(tok, ts):
        calls.append(tok)
        raise RuntimeError("bad consumer")

    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(_boom)
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert calls == ["tok"]
    # The snapshot still landed — the mirror is untouched by the bad callback.
    assert m.get("tok", now_ts=now) is not None


def test_callback_may_read_the_mirror_without_deadlocking():
    """The fire happens with self._lock RELEASED, so a callback that calls
    get() (which takes the same plain Lock) cannot deadlock the recv thread."""
    now = time.time()
    seen = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(
        lambda tok, ts: seen.append(m.get(tok, now_ts=now) is not None)
    )
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert seen == [True]


def test_callback_can_be_cleared():
    now = time.time()
    fired = []
    m = ClobBookWsMirror(max_age_ms=700)
    m.set_ask_update_callback(lambda tok, ts: fired.append(tok))
    m.set_ask_update_callback(None)
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    assert fired == []


# ── watchdog-facing API (operator order 2026-08-06) ──────────────────────────

def test_serving_snapshots_exposes_the_cache_read_only():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    serving = m.serving_snapshots()
    assert set(serving) == {"tok"}
    assert serving["tok"].top_ask == 0.5
    # A copy: mutating it cannot corrupt the mirror.
    serving.pop("tok")
    assert set(m.serving_snapshots()) == {"tok"}


def test_evict_drops_the_ladder_and_blocks_partial_delta_rebuilds():
    """The watchdog evicts a token that is still SUBSCRIBED, so the next frame
    can rebuild it — but a price_change on empty ladders would rebuild a PARTIAL
    book. Deltas must be ignored until a full `book` frame re-seeds it."""
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m._handle_message(
        _book_msg("tok", [(0.50, 100), (0.51, 200)], [(0.40, 100)],
                  ts_ms=now * 1000),
        recv_ts=now,
    )
    assert m.evict("tok") is True
    assert m.get("tok", now_ts=now) is None
    assert m.evict("tok") is False  # already gone

    # A delta must NOT resurrect a one-level book.
    m._handle_message(
        {
            "event_type": "price_change",
            "asset_id": "tok",
            "timestamp": str(int(now * 1000)),
            "changes": [{"price": "0.59", "size": "5", "side": "SELL"}],
        },
        recv_ts=now,
    )
    assert m.get("tok", now_ts=now) is None

    # A full book frame re-seeds it and clears the resync arm.
    m._handle_message(
        _book_msg("tok", [(0.50, 100)], [(0.40, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    snap = m.get("tok", now_ts=now)
    assert snap is not None and snap.top_ask == 0.50
    # Deltas flow again.
    m._handle_message(
        {
            "event_type": "price_change",
            "asset_id": "tok",
            "timestamp": str(int(now * 1000)),
            "changes": [{"price": "0.49", "size": "7", "side": "SELL"}],
        },
        recv_ts=now,
    )
    assert m.get("tok", now_ts=now).top_ask == 0.49


def test_reconnect_clears_a_pending_resync():
    now = time.time()
    m = ClobBookWsMirror(max_age_ms=700)
    m._handle_message(
        _book_msg("tok", [(0.5, 100)], [(0.4, 100)], ts_ms=now * 1000),
        recv_ts=now,
    )
    m.evict("tok")
    m._clear_cache()  # what _run does on any disconnect/error
    assert m._resync == set()
