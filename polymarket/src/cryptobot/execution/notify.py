"""Telegram and CRITICAL.log notifications.

  * ``send_event(event_type, message, *, wallet_id=None, critical=False)``
    is the single entry point, rate-limited per ``(event_type, wallet_id)``
    so a noisy gate cannot spam the operator.
  * ``critical=True`` bypasses the rate limit and also writes to
    ``logs/CRITICAL.log``, so a Telegram outage cannot swallow it.
  * Without ``NOTIFY_TELEGRAM_TOKEN`` Telegram is skipped silently;
    critical events still reach the log.

The Telegram POST uses ``httpx`` with a 5s timeout so a hung API cannot stall
the caller. On a 5xx the full payload goes to CRITICAL.log instead.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx

from cryptobot import config


logger = logging.getLogger(__name__)


_DEFAULT_RATE_LIMIT_S: float = 60.0
_TELEGRAM_TIMEOUT_S: float = 5.0
# Anchored, not CWD-relative: a systemd unit with a different
# WorkingDirectory used to scatter (or fail to write) CRITICAL.log.
_CRITICAL_LOG_PATH = config.LOGS_DIR / "CRITICAL.log"

# One module-level client so alerts reuse a keep-alive connection instead of
# paying a TCP+TLS handshake each (and holding a thread for the timeout on each
# new connection when Telegram is slow).
_TELEGRAM_CLIENT: Optional[httpx.Client] = None
_TELEGRAM_CLIENT_LOCK = threading.Lock()


def _telegram_client() -> httpx.Client:
    """Lazy-init module-level httpx.Client. Created on first call so
    import-only test setups don't open a real client (and so tests can
    monkeypatch `_TELEGRAM_CLIENT` to inject a stub before the first
    notify fires)."""
    global _TELEGRAM_CLIENT
    if _TELEGRAM_CLIENT is None:
        with _TELEGRAM_CLIENT_LOCK:
            if _TELEGRAM_CLIENT is None:
                _TELEGRAM_CLIENT = httpx.Client(
                    timeout=_TELEGRAM_TIMEOUT_S
                )
    return _TELEGRAM_CLIENT


@dataclass(frozen=True)
class NotifyResult:
    """Outcome of one `send_event` invocation."""

    sent: bool
    rate_limited: bool
    telegram_status: Optional[int]
    critical_logged: bool
    skipped_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Per-process rate limiter
# ---------------------------------------------------------------------------


_RATE_LOCK = threading.Lock()
_LAST_SEND_TS: dict[tuple[str, Optional[str]], float] = {}


def _rate_limit_key(event_type: str, wallet_id: Optional[str]) -> tuple[str, Optional[str]]:
    return (event_type, wallet_id or None)


def _rate_limit_check(
    event_type: str, wallet_id: Optional[str], window_s: float
) -> bool:
    """Return True iff the caller is within the cooldown window. Caller
    that is NOT within the window updates the bookmark to `now`."""
    key = _rate_limit_key(event_type, wallet_id)
    now = time.time()
    with _RATE_LOCK:
        last = _LAST_SEND_TS.get(key)
        if last is not None and (now - last) < window_s:
            return True
        _LAST_SEND_TS[key] = now
        return False


def reset_rate_limit() -> None:
    """Drop every rate-limit bookmark. Used by tests."""
    with _RATE_LOCK:
        _LAST_SEND_TS.clear()


# ---------------------------------------------------------------------------
# CRITICAL.log writer
# ---------------------------------------------------------------------------


_critical_logger = logging.getLogger("cryptobot.critical")
_critical_logger.propagate = False
_critical_handler_installed: bool = False
_critical_handler_lock = threading.Lock()


def _ensure_critical_handler(log_path: Optional[Path] = None) -> None:
    """Attach a rotating file handler to the dedicated CRITICAL logger.

    5 backups of 50 MB. Idempotent: only the first call attaches a handler.

    The path is read from ``_CRITICAL_LOG_PATH`` at call time so tests can
    monkeypatch it.
    """
    global _critical_handler_installed
    if _critical_handler_installed:
        return
    with _critical_handler_lock:
        if _critical_handler_installed:
            return
        path = log_path if log_path is not None else _CRITICAL_LOG_PATH
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                path, maxBytes=50 * 1024 * 1024, backupCount=5
            )
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)s %(name)s: %(message)s"
                )
            )
            _critical_logger.addHandler(handler)
            _critical_logger.setLevel(logging.CRITICAL)
            _critical_handler_installed = True
        except Exception:  # noqa: BLE001
            # Logging is observability; never let it crash the caller.
            logger.warning(
                "Failed to install CRITICAL.log handler", exc_info=True
            )


def _emit_critical(message: str) -> bool:
    """Mirror `message` to logs/CRITICAL.log. Returns True on success."""
    try:
        _ensure_critical_handler()
        _critical_logger.critical(message)
        return True
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# Telegram POST
# ---------------------------------------------------------------------------


def _post_telegram(
    *,
    token: str,
    chat_id: str,
    message: str,
    timeout_s: float = _TELEGRAM_TIMEOUT_S,
) -> Optional[int]:
    """POST to Telegram's sendMessage. Returns the HTTP status or None on
    network error. Never raises — observability path must not crash the
    calling tick."""
    if not token or not chat_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        # No ``parse_mode``: plain text means event text cannot inject markdown
        # links.
        resp = _telegram_client().post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
            },
            timeout=timeout_s,
        )
        return int(resp.status_code)
    except Exception as exc:  # noqa: BLE001
        # The bot token is part of the request URL, and httpx error tracebacks
        # include ``.request.url``, so logging the exception could write the
        # token into the logs. Log only the exception type.
        logger.warning("Telegram POST failed: %s", type(exc).__name__)
        return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def send_event(
    event_type: str,
    message: str,
    *,
    wallet_id: Optional[str] = None,
    critical: bool = False,
    telegram_token: Optional[str] = None,
    telegram_chat_id: Optional[str] = None,
    rate_limit_window_s: float = _DEFAULT_RATE_LIMIT_S,
) -> NotifyResult:
    """Send one notification through Telegram and CRITICAL.log.

      * ``critical=True`` always sends, ignoring the rate limit, and also writes
        CRITICAL.log.
      * Other events are rate-limited per (event_type, wallet_id).
      * No token: skipped silently (``critical=True`` still reaches the log).
      * On a Telegram 5xx, critical events fall back to CRITICAL.log;
        non-critical ones are dropped.

    Returns a ``NotifyResult`` so callers can see what happened without parsing
    logs.
    """
    # Resolve token/chat_id from explicit kwargs first, env vars second.
    token = telegram_token or os.environ.get("NOTIFY_TELEGRAM_TOKEN", "")
    chat_id = telegram_chat_id or os.environ.get("NOTIFY_TELEGRAM_CHAT_ID", "")

    # ``_rate_limit_check`` both tests and records the send, so only call it
    # when a send will actually happen (credentials present). Otherwise a skip
    # without credentials would use up the slot and suppress the first real
    # alert once credentials are set. critical=True bypasses the limiter.
    if not critical and token and chat_id:
        if _rate_limit_check(event_type, wallet_id, window_s=rate_limit_window_s):
            return NotifyResult(
                sent=False,
                rate_limited=True,
                telegram_status=None,
                critical_logged=False,
                skipped_reason="rate_limited",
            )

    formatted = _format_message(event_type, message, wallet_id=wallet_id)

    telegram_status: Optional[int] = None
    sent_via_telegram = False
    if token and chat_id:
        telegram_status = _post_telegram(
            token=token, chat_id=chat_id, message=formatted
        )
        if telegram_status is not None and 200 <= telegram_status < 300:
            sent_via_telegram = True

    critical_logged = False
    if critical:
        # CRITICAL events always hit the log — Telegram is best-effort.
        critical_logged = _emit_critical(formatted)
    elif telegram_status is not None and telegram_status >= 500:
        # 5xx fallback: drop a CRITICAL.log line so the signal survives.
        critical_logged = _emit_critical(
            f"[5xx-fallback] {formatted}"
        )

    if not token or not chat_id:
        return NotifyResult(
            sent=False,
            rate_limited=False,
            telegram_status=None,
            critical_logged=critical_logged,
            skipped_reason="no_telegram_creds",
        )

    return NotifyResult(
        sent=sent_via_telegram,
        rate_limited=False,
        telegram_status=telegram_status,
        critical_logged=critical_logged,
    )


def _format_message(
    event_type: str, message: str, *, wallet_id: Optional[str]
) -> str:
    prefix = f"[{event_type}"
    if wallet_id:
        prefix += f" wallet={wallet_id}"
    prefix += "]"
    return f"{prefix} {message}"


# ---------------------------------------------------------------------------
# Background dispatcher
# ---------------------------------------------------------------------------


class NotifyDispatcher:
    """Bounded-queue daemon worker that runs ``send_event`` off the calling
    thread.

    A Telegram POST can block for its 5s timeout, and ``deps.notify`` is called
    from the tick (e.g. the venue-degradation alert after a reject), where that
    would delay the next order. ``enqueue()`` captures the call arguments and
    returns at once; the worker replays them through the unchanged
    ``send_event``, so rate limiting and CRITICAL.log behave as before, in FIFO
    order.

    Critical events are never queued: ``enqueue(critical=True)`` calls
    ``send_event`` inline, so CRITICAL.log and the Telegram POST finish before
    the caller continues. Queued, a critical could be dropped on overflow, lost
    in a crash, or abandoned at shutdown.

    Non-critical delivery is best-effort: the queue is bounded and drops the
    oldest event on overflow (logged), and ``stop()`` enqueues a ``None``
    sentinel so the worker drains up to it and exits. The join is bounded and
    the worker is a daemon thread, so a hung Telegram never blocks exit. The
    rate limiter records when the worker delivers, not when the caller
    enqueues.
    """

    def __init__(self, maxsize: int = 100) -> None:
        # `None` is the shutdown sentinel put by stop().
        self._queue: queue.Queue[Optional[tuple[str, str, Optional[str], bool]]] = (
            queue.Queue(maxsize=maxsize)
        )
        self._thread = threading.Thread(
            target=self._run,
            name="cryptobot-notify-dispatch",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def enqueue(
        self,
        event_type: str,
        message: str,
        *,
        wallet_id: Optional[str] = None,
        critical: bool = False,
    ) -> None:
        """Queue one non-critical event for background dispatch and return
        immediately. `critical=True` bypasses the queue and sends inline
        (blocking) — see the class docstring. Never raises — overflow
        sheds and logs; send_event failures are logged."""
        if critical:
            # Criticals are rare and allowed to block: run send_event
            # synchronously so CRITICAL.log + the POST complete before the
            # caller's next statement (queue overflow / crash-before-dequeue
            # / shutdown can never erase them).
            try:
                send_event(
                    event_type, message, wallet_id=wallet_id, critical=True
                )
            except Exception:  # noqa: BLE001 — notify must never crash the caller
                logger.warning("critical notify failed", exc_info=True)
            return
        item = (event_type, message, wallet_id, critical)
        try:
            self._queue.put_nowait(item)
            return
        except queue.Full:
            pass
        # Overflow (Telegram hung for minutes): shed the OLDEST queued event
        # so the newest — the one describing the current state — survives.
        # Criticals never reach this path, so only routine chatter is shed.
        try:
            dropped = self._queue.get_nowait()
            if dropped is None:
                # Raced stop(): we popped the shutdown sentinel. Put it back
                # (we just freed a slot) and shed the NEW event — delivery
                # into a stopping dispatcher is best-effort by contract.
                self._queue.put_nowait(None)
                logger.warning(
                    "notify queue full at shutdown; dropped event (%s)",
                    event_type,
                )
                return
            logger.warning(
                "notify queue full; dropped oldest event (%s)", dropped[0]
            )
        except queue.Empty:
            pass
        except queue.Full:
            # Another producer refilled the freed slot before the sentinel
            # went back; worker exit falls to stop()'s bounded join.
            logger.warning(
                "notify queue full at shutdown; dropped event (%s)", event_type
            )
            return
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            # Lost the race to another producer; shed the new one instead.
            logger.warning("notify queue full; dropped event (%s)", event_type)

    def stop(self, timeout_s: float = 10.0) -> None:
        """Sentinel + bounded join. The worker drains queued events FIFO up
        to the sentinel, then exits; the total wait is bounded by
        `timeout_s` — a hung POST is abandoned (daemon thread)."""
        deadline = time.monotonic() + timeout_s
        try:
            self._queue.put(None, timeout=timeout_s)
        except queue.Full:
            # Worker wedged behind a full queue; the bounded join below
            # abandons it (daemon thread).
            pass
        if self._thread.is_alive():
            self._thread.join(timeout=max(0.0, deadline - time.monotonic()))

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                # Shutdown sentinel from stop() — FIFO drain complete.
                return
            event_type, message, wallet_id, critical = item
            try:
                send_event(
                    event_type, message, wallet_id=wallet_id, critical=critical
                )
            except Exception:  # noqa: BLE001 — dispatcher must never die mid-run
                logger.warning("notify dispatch failed", exc_info=True)
