"""U9 verification: Telegram notify + CRITICAL.log fallback.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U9:
  - Rate limit per (event_type, wallet_id) suppresses duplicate sends.
  - critical=True bypasses the rate limit AND writes to CRITICAL.log.
  - Missing token → silent skip; critical events still hit the log.
  - Telegram 5xx falls back to CRITICAL.log for critical events.
"""

from __future__ import annotations

import queue
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from cryptobot.execution import notify
from cryptobot.execution.notify import (
    NotifyDispatcher,
    _format_message,
    reset_rate_limit,
    send_event,
)


@pytest.fixture(autouse=True)
def _reset_rate_limit() -> None:
    reset_rate_limit()
    yield
    reset_rate_limit()


@pytest.fixture
def critical_log_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect CRITICAL.log to a tmp file so tests don't write to logs/."""
    path = tmp_path / "CRITICAL.log"
    monkeypatch.setattr(notify, "_CRITICAL_LOG_PATH", path)
    # Reset the installed-once flag so the new path is honored.
    monkeypatch.setattr(notify, "_critical_handler_installed", False)
    # Detach prior handlers from the dedicated logger.
    for handler in list(notify._critical_logger.handlers):
        notify._critical_logger.removeHandler(handler)
    yield path
    for handler in list(notify._critical_logger.handlers):
        notify._critical_logger.removeHandler(handler)


# ============================================================================
# Rate limit
# ============================================================================


class TestRateLimit:
    def test_within_window_blocks_duplicate(self) -> None:
        with patch.object(notify, "_post_telegram", return_value=200):
            first = send_event(
                "gate_skipped",
                "pairs_5m skipped",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
            second = send_event(
                "gate_skipped",
                "pairs_5m skipped again",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
        assert first.rate_limited is False
        assert second.rate_limited is True
        assert second.skipped_reason == "rate_limited"

    def test_different_event_types_independent_buckets(self) -> None:
        with patch.object(notify, "_post_telegram", return_value=200):
            a = send_event(
                "gate_skipped",
                "x",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
            b = send_event(
                "low_balance",
                "y",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
        assert a.rate_limited is False
        assert b.rate_limited is False

    def test_different_wallet_id_independent_buckets(self) -> None:
        with patch.object(notify, "_post_telegram", return_value=200):
            a = send_event(
                "gate_skipped",
                "x",
                wallet_id="v6_5m",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
            b = send_event(
                "gate_skipped",
                "y",
                wallet_id="pairs_5m",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
        assert a.rate_limited is False
        assert b.rate_limited is False


# ============================================================================
# critical=True bypass
# ============================================================================


class TestCriticalBypass:
    def test_critical_bypasses_rate_limit(self) -> None:
        with patch.object(notify, "_post_telegram", return_value=200) as post:
            send_event(
                "gate_skipped",
                "x",
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
            result = send_event(
                "gate_skipped",
                "y",
                critical=True,
                telegram_token="t",
                telegram_chat_id="c",
                rate_limit_window_s=60,
            )
        assert result.rate_limited is False
        assert post.call_count == 2

    def test_critical_writes_to_log_even_with_telegram(
        self, critical_log_path: Path
    ) -> None:
        with patch.object(notify, "_post_telegram", return_value=200):
            result = send_event(
                "pair_orphan",
                "pair leg failed",
                critical=True,
                telegram_token="t",
                telegram_chat_id="c",
            )
        assert result.critical_logged is True
        contents = critical_log_path.read_text(encoding="utf-8")
        assert "pair_orphan" in contents
        assert "pair leg failed" in contents


# ============================================================================
# Missing creds — silent skip
# ============================================================================


class TestMissingCreds:
    def test_no_token_silently_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NOTIFY_TELEGRAM_TOKEN", raising=False)
        monkeypatch.delenv("NOTIFY_TELEGRAM_CHAT_ID", raising=False)
        result = send_event(
            "gate_skipped",
            "x",
        )
        assert result.sent is False
        assert result.telegram_status is None
        assert result.skipped_reason == "no_telegram_creds"

    def test_critical_logs_even_without_telegram(
        self, critical_log_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("NOTIFY_TELEGRAM_TOKEN", raising=False)
        result = send_event(
            "pair_orphan",
            "no telegram but still critical",
            critical=True,
        )
        assert result.critical_logged is True
        assert "no telegram but still critical" in critical_log_path.read_text(
            encoding="utf-8"
        )


# ============================================================================
# Telegram 5xx fallback
# ============================================================================


class TestFiveXxFallback:
    def test_5xx_critical_falls_back_to_log(
        self, critical_log_path: Path
    ) -> None:
        with patch.object(notify, "_post_telegram", return_value=502):
            result = send_event(
                "pair_orphan",
                "telegram down",
                critical=True,
                telegram_token="t",
                telegram_chat_id="c",
            )
        # Critical always hits the log; sent stays False because the
        # status was 5xx.
        assert result.sent is False
        assert result.telegram_status == 502
        assert result.critical_logged is True
        contents = critical_log_path.read_text(encoding="utf-8")
        assert "telegram down" in contents

    def test_non_critical_5xx_still_logs_fallback(
        self, critical_log_path: Path
    ) -> None:
        with patch.object(notify, "_post_telegram", return_value=503):
            result = send_event(
                "gate_skipped",
                "x",
                telegram_token="t",
                telegram_chat_id="c",
            )
        assert result.telegram_status == 503
        assert result.critical_logged is True
        contents = critical_log_path.read_text(encoding="utf-8")
        assert "[5xx-fallback]" in contents


# ============================================================================
# Format message helper
# ============================================================================


class TestFormatMessage:
    def test_includes_event_type(self) -> None:
        assert (
            _format_message("gate_skipped", "details", wallet_id=None)
            == "[gate_skipped] details"
        )

    def test_includes_wallet_id_when_provided(self) -> None:
        assert (
            _format_message("gate_skipped", "details", wallet_id="v6_5m")
            == "[gate_skipped wallet=v6_5m] details"
        )


# ============================================================================
# NotifyDispatcher — fire-and-forget background dispatch
# ============================================================================
#
# Latency finding: deps.notify ran send_event's blocking Telegram POST on
# the tick thread (notifies inside the serial held-position loop +
# venue-degradation alert on the reject tail). The dispatcher moves the
# NON-CRITICAL POST to a bounded-queue daemon worker; send_event itself is
# UNCHANGED, so the rate-limit / CRITICAL contracts above still hold.
# critical=True BYPASSES the queue and sends inline (blocking) — a queued
# critical could be shed on overflow, lost to a crash before dequeue, or
# abandoned by the bounded shutdown join.


class TestNotifyDispatcher:
    def test_enqueue_returns_before_send_completes(self) -> None:
        """The calling (tick) thread must never wait on the Telegram POST."""
        started = threading.Event()
        release = threading.Event()

        def _slow_send(*_args, **_kwargs):
            started.set()
            release.wait(timeout=5.0)

        dispatcher = NotifyDispatcher()
        with patch.object(notify, "send_event", side_effect=_slow_send):
            dispatcher.start()
            t0 = time.monotonic()
            dispatcher.enqueue("TOPUP_FILL", "T3 top-up FILL BTC/v6_15m")
            elapsed = time.monotonic() - t0
            assert elapsed < 0.5, "enqueue must not block on the send"
            assert started.wait(timeout=2.0), "worker must pick up the event"
            release.set()
            dispatcher.stop()

    def test_worker_delivers_args_to_send_event(self) -> None:
        """The worker replays the exact non-critical call — event_type,
        message, wallet_id — through the unchanged send_event."""
        done = threading.Event()
        calls: list[tuple] = []

        def _record(event_type, message, *, wallet_id=None, critical=False):
            calls.append((event_type, message, wallet_id, critical))
            done.set()

        dispatcher = NotifyDispatcher()
        with patch.object(notify, "send_event", side_effect=_record):
            dispatcher.start()
            dispatcher.enqueue(
                "gate_skipped",
                "pairs_5m skipped",
                wallet_id="v6_5m",
            )
            assert done.wait(timeout=2.0)
            dispatcher.stop()
        assert calls == [
            ("gate_skipped", "pairs_5m skipped", "v6_5m", False)
        ]

    def test_critical_bypasses_queue_and_sends_inline(self) -> None:
        """F1: critical=True never enqueues — send_event runs synchronously
        on the calling thread (CRITICAL.log + POST complete before enqueue
        returns), so overflow shedding / a crash before dequeue / shutdown
        can never erase a critical. Worker deliberately NOT started and the
        queue pre-filled to prove neither is involved."""
        calls: list[tuple] = []

        def _record(event_type, message, *, wallet_id=None, critical=False):
            calls.append((event_type, message, wallet_id, critical))

        dispatcher = NotifyDispatcher(maxsize=1)
        with patch.object(notify, "send_event", side_effect=_record):
            dispatcher.enqueue("chatter", "routine")  # fills the queue
            dispatcher.enqueue(
                "dd_halt",
                "40% drawdown halt",
                wallet_id="v6_5m",
                critical=True,
            )
        # Sent inline despite the full queue + no worker.
        assert calls == [("dd_halt", "40% drawdown halt", "v6_5m", True)]
        # The queued non-critical chatter was NOT shed by the critical.
        assert dispatcher._queue.get_nowait()[0] == "chatter"

    def test_critical_sends_inline_after_stop(self) -> None:
        """A critical fired into a stopped dispatcher (SIGTERM straggler
        job) still delivers — inline, no dead queue involved."""
        calls: list[str] = []
        dispatcher = NotifyDispatcher()
        with patch.object(
            notify, "send_event",
            side_effect=lambda event_type, _message, **_kw: calls.append(event_type),
        ):
            dispatcher.start()
            dispatcher.stop()
            assert not dispatcher._thread.is_alive()
            dispatcher.enqueue("clob_venue_degraded", "x", critical=True)
        assert calls == ["clob_venue_degraded"]

    def test_enqueue_after_stop_noncritical_is_best_effort(self) -> None:
        """Non-critical into a stopped dispatcher must not crash — delivery
        is best-effort by contract (the event is simply never consumed)."""
        dispatcher = NotifyDispatcher()
        dispatcher.start()
        dispatcher.stop()
        assert not dispatcher._thread.is_alive()
        dispatcher.enqueue("gate_skipped", "late chatter")  # must not raise

    def test_overflow_drops_oldest(self) -> None:
        """Bounded queue sheds the OLDEST event on overflow so the newest —
        the one describing the current state — survives. Only non-critical
        chatter can be shed (criticals never enqueue). Worker deliberately
        NOT started so the queue holds still for inspection."""
        dispatcher = NotifyDispatcher(maxsize=2)
        dispatcher.enqueue("first", "1")
        dispatcher.enqueue("second", "2")
        dispatcher.enqueue("third", "3")
        remaining: list[str] = []
        while True:
            try:
                remaining.append(dispatcher._queue.get_nowait()[0])
            except queue.Empty:
                break
        assert remaining == ["second", "third"]

    def test_stop_drains_queued_events_before_exit(self) -> None:
        """Clean shutdown flushes queued events FIFO up to the sentinel
        stop() puts, then the worker thread exits."""
        sent: list[str] = []
        dispatcher = NotifyDispatcher()
        with patch.object(
            notify, "send_event",
            side_effect=lambda event_type, _message, **_kw: sent.append(event_type),
        ):
            # Queued BEFORE the worker starts so the drain is what sends them.
            dispatcher.enqueue("first", "x")
            dispatcher.enqueue("second", "y")
            dispatcher.start()
            dispatcher.stop()
        assert sent == ["first", "second"]
        assert not dispatcher._thread.is_alive()

    def test_worker_survives_send_exception(self) -> None:
        """A raising send_event must never kill the worker — the next
        queued event still dispatches."""
        done = threading.Event()
        calls: list[str] = []

        def _flaky(event_type, _message, **_kwargs):
            calls.append(event_type)
            if event_type == "boom":
                raise RuntimeError("send exploded")
            done.set()

        dispatcher = NotifyDispatcher()
        with patch.object(notify, "send_event", side_effect=_flaky):
            dispatcher.start()
            dispatcher.enqueue("boom", "x")
            dispatcher.enqueue("after", "y")
            assert done.wait(timeout=2.0)
            dispatcher.stop()
        assert calls == ["boom", "after"]
        assert not dispatcher._thread.is_alive()
