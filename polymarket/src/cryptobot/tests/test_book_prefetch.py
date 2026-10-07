"""A8 book prefetch-ahead: cache freshness contract + boundary targeting."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from cryptobot.scheduler.book_prefetch import BookPrefetcher, BookSourceChain


@dataclass
class _Snap:
    top_ask: float
    fetched_at_ts: float


def test_get_serves_only_fresh_snapshots():
    p = BookPrefetcher(lambda *a, **k: None, lead_ms=150, max_age_ms=700)
    now = time.time()
    p._cache = {"tok": _Snap(top_ask=0.6, fetched_at_ts=now - 0.2)}
    assert p.get("tok", now_ts=now) is not None
    # stale beyond max_age -> miss
    p._cache = {"tok": _Snap(top_ask=0.6, fetched_at_ts=now - 1.5)}
    assert p.get("tok", now_ts=now) is None
    # unknown token -> miss
    assert p.get("other", now_ts=now) is None


def test_loop_fetches_noted_tokens_at_boundary():
    got = []
    fired = threading.Event()

    def fake_fetch(tokens, *, now_ts):
        got.append((tuple(tokens), now_ts))
        fired.set()
        return {"a": _Snap(top_ask=0.5, fetched_at_ts=time.time())}

    p = BookPrefetcher(fake_fetch, lead_ms=150, max_age_ms=700)
    p.note_tokens(["a", "b", "a"])  # dedup preserved order
    p.start()
    try:
        assert fired.wait(timeout=3.0)
    finally:
        p.stop()
    tokens, now_ts = got[0]
    assert tokens == ("a", "b")
    assert now_ts == float(int(now_ts))  # targets a whole-second boundary
    # successful batch replaced the cache
    assert p.get("a", now_ts=time.time()) is not None


def test_no_tokens_means_no_fetch_and_stop_joins():
    calls = []
    p = BookPrefetcher(lambda *a, **k: calls.append(1), lead_ms=100, max_age_ms=500)
    p.start()
    time.sleep(1.2)
    p.stop()
    assert p._thread is None
    assert calls == []


def test_fetch_failure_keeps_old_cache_until_stale():
    p = BookPrefetcher(lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
                       lead_ms=100, max_age_ms=700)
    now = time.time()
    p._cache = {"tok": _Snap(top_ask=0.6, fetched_at_ts=now)}
    p.note_tokens(["tok"])
    p.start()
    time.sleep(1.2)
    p.stop()
    # failure did not clobber the cache; staleness gate still governs
    assert p._cache.get("tok") is not None


class _TaggedSource:
    def __init__(self, name, snaps):
        self.source_name = name
        self._snaps = snaps

    def note_tokens(self, tokens):
        pass

    def get(self, token, *, now_ts):
        return self._snaps.get(token)


def test_get_tagged_attributes_served_source_for_telemetry():
    now = time.time()
    ws = _TaggedSource("ws", {"a": _Snap(top_ask=0.5, fetched_at_ts=now)})
    pf = _TaggedSource("prefetch", {"b": _Snap(top_ask=0.6, fetched_at_ts=now)})
    chain = BookSourceChain([ws, pf])

    # First fresh hit wins; the tag names which source served it.
    assert chain.get_tagged("a", now_ts=now) == (ws._snaps["a"], "ws")
    assert chain.get_tagged("b", now_ts=now) == (pf._snaps["b"], "prefetch")
    # Miss on all sources → (None, None); plain get() still returns the snap.
    assert chain.get_tagged("z", now_ts=now) == (None, None)
    assert chain.get("a", now_ts=now) is ws._snaps["a"]
