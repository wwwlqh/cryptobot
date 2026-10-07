"""WS mirror stall watchdog (operator order 2026-08-06): decision logic.

The watchdog is the ONLY detector for a stalled-but-connected subscription now
that the mirror's freshness cap is gone, and it sits off the tick — so the
contract under test is: suspect selection, exact-price/tolerant-size compare,
evict-on-mismatch, verify-TTL suppression, and fail-open on every error.
"""
from __future__ import annotations

import time

from cryptobot.scheduler.book_ws_watchdog import (
    BookWsWatchdog,
    build_watchdog_job,
    mirror_supports_watchdog,
    reset_watchdog_counters,
    watchdog_counters,
)
from cryptobot.venues.types import BookSnapshot


def _snap(token: str, *, ask=0.62, ask_sz=100.0, bid=0.60, bid_sz=80.0, ts=0.0):
    return BookSnapshot(
        token_id=token,
        asks=({"price": ask, "size": ask_sz},),
        bids=({"price": bid, "size": bid_sz},),
        fetched_at_ts=ts,
        total_ask_depth_usd=ask * ask_sz,
    )


class _FakeMirror:
    def __init__(self, snapshots: dict):
        self._snapshots = dict(snapshots)
        self.evicted: list[str] = []

    def serving_snapshots(self) -> dict:
        return dict(self._snapshots)

    def evict(self, token: str) -> bool:
        self.evicted.append(token)
        return self._snapshots.pop(token, None) is not None


def _watchdog(mirror, fetch, **kw):
    kw.setdefault("stall_suspect_ms", 5000)
    kw.setdefault("max_tokens_per_pass", 4)
    kw.setdefault("verify_ttl_ms", 30000)
    return BookWsWatchdog(mirror, fetch, **kw)


# ── selection ────────────────────────────────────────────────────────────────

def test_only_stale_tokens_are_checked_oldest_first_and_capped():
    now = 1_000_000.0
    mirror = _FakeMirror({
        "fresh": _snap("fresh", ts=now - 1.0),      # under the suspect bound
        "old1": _snap("old1", ts=now - 9.0),
        "old2": _snap("old2", ts=now - 30.0),       # oldest
        "old3": _snap("old3", ts=now - 6.0),
    })
    asked: list[list[str]] = []

    def fetch(tokens):
        asked.append(list(tokens))
        return {t: mirror.serving_snapshots()[t] for t in tokens}

    wd = _watchdog(mirror, fetch, max_tokens_per_pass=2)
    wd.run_once(now_ts=now)
    # Fresh book never costs a REST call; suspects go oldest first, capped.
    assert asked == [["old2", "old1"]]
    assert mirror.evicted == []


def test_no_suspects_means_no_rest_call():
    now = 1_000.0
    mirror = _FakeMirror({"a": _snap("a", ts=now - 0.5)})
    calls = []
    wd = _watchdog(mirror, lambda tokens: calls.append(tokens))
    assert wd.run_once(now_ts=now) == 0
    assert calls == []


# ── compare / evict ──────────────────────────────────────────────────────────

def test_match_leaves_mirror_alone_and_counts():
    now = 2_000.0
    mirror = _FakeMirror({"a": _snap("a", ts=now - 10.0)})
    # Size rounding inside tolerance must still count as a match.
    rest = {"a": _snap("a", ask_sz=100.5, bid_sz=79.9, ts=now)}
    wd = _watchdog(mirror, lambda tokens: rest)
    assert wd.run_once(now_ts=now) == 0
    assert mirror.evicted == []
    assert (wd.n_checks, wd.n_matches, wd.n_mismatches) == (1, 1, 0)


def test_price_drift_is_a_mismatch_and_evicts():
    now = 3_000.0
    mirror = _FakeMirror({"a": _snap("a", ask=0.62, ts=now - 10.0)})
    rest = {"a": _snap("a", ask=0.63, ts=now)}   # one tick = already an answer
    wd = _watchdog(mirror, lambda tokens: rest)
    assert wd.run_once(now_ts=now) == 1
    assert mirror.evicted == ["a"]
    assert wd.n_mismatches == 1 and wd.n_evictions == 1
    assert wd.last_mismatch_ts == now


def test_size_drift_beyond_tolerance_is_a_mismatch():
    now = 3_500.0
    mirror = _FakeMirror({"a": _snap("a", ask_sz=100.0, ts=now - 10.0)})
    rest = {"a": _snap("a", ask_sz=400.0, ts=now)}
    wd = _watchdog(mirror, lambda tokens: rest, size_rel_tol=0.02)
    assert wd.run_once(now_ts=now) == 1
    assert mirror.evicted == ["a"]


# ── fail-open ────────────────────────────────────────────────────────────────

def test_rest_error_never_evicts():
    now = 4_000.0
    for fetch in (
        lambda tokens: None,                       # transport/HTTP/parse error
        lambda tokens: {},                         # nothing usable came back
        lambda tokens: (_ for _ in ()).throw(RuntimeError("timeout")),
    ):
        mirror = _FakeMirror({"a": _snap("a", ts=now - 10.0)})
        wd = _watchdog(mirror, fetch)
        assert wd.run_once(now_ts=now) == 0
        assert mirror.evicted == []


def test_token_absent_from_batch_response_is_ambiguous_not_a_stall():
    now = 4_500.0
    mirror = _FakeMirror({
        "a": _snap("a", ts=now - 10.0),
        "b": _snap("b", ts=now - 11.0),
    })
    # Venue returned only one of the two books.
    wd = _watchdog(mirror, lambda tokens: {"b": _snap("b", ts=now)})
    assert wd.run_once(now_ts=now) == 0
    assert mirror.evicted == []
    assert wd.n_checks == 1


def test_mirror_without_watchdog_api_is_skipped():
    class _Bare:
        pass

    assert mirror_supports_watchdog(_Bare()) is False
    calls = []
    wd = _watchdog(_Bare(), lambda tokens: calls.append(tokens))
    assert wd.run_once(now_ts=time.time()) == 0
    assert calls == []


def test_job_wrapper_swallows_exceptions():
    class _Boom:
        def serving_snapshots(self):
            raise RuntimeError("boom")

        def evict(self, token):  # pragma: no cover - never reached
            return False

    wd = _watchdog(_Boom(), lambda tokens: None)
    build_watchdog_job(wd)()  # must not raise into the scheduler


# ── verify TTL ───────────────────────────────────────────────────────────────

def test_verified_token_is_not_refetched_until_ttl_or_new_ws_activity():
    now = 5_000.0
    book_ts = now - 10.0
    mirror = _FakeMirror({"a": _snap("a", ts=book_ts)})
    asked: list[list[str]] = []

    def fetch(tokens):
        asked.append(list(tokens))
        return {"a": _snap("a", ts=now)}

    wd = _watchdog(mirror, fetch, verify_ttl_ms=30000)
    wd.run_once(now_ts=now)
    assert asked == [["a"]]

    # Still quiet, still correct, inside the TTL -> no second REST call.
    wd.run_once(now_ts=now + 5.0)
    wd.run_once(now_ts=now + 20.0)
    assert asked == [["a"]]

    # TTL elapsed -> asked again.
    wd.run_once(now_ts=now + 31.0)
    assert len(asked) == 2

    # New ws activity that then goes quiet again is a FRESH question even
    # inside the TTL: the snapshot ts moved past the verified one.
    mirror._snapshots["a"] = _snap("a", ts=now + 32.0)
    wd.run_once(now_ts=now + 40.0)
    assert len(asked) == 3


# ── counters ─────────────────────────────────────────────────────────────────

def test_counters_are_published_for_the_dashboard():
    reset_watchdog_counters()
    assert watchdog_counters()["armed"] is False
    now = 6_000.0
    mirror = _FakeMirror({"a": _snap("a", ask=0.62, ts=now - 10.0)})
    wd = _watchdog(mirror, lambda tokens: {"a": _snap("a", ask=0.61, ts=now)})
    assert watchdog_counters()["armed"] is True
    wd.run_once(now_ts=now)
    counters = watchdog_counters()
    assert counters["checks"] == 1
    assert counters["mismatches"] == 1
    assert counters["evictions"] == 1
    assert counters["last_mismatch_ts"] == now
    reset_watchdog_counters()
