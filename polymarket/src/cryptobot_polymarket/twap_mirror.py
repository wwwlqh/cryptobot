"""Exact public Chainlink TWAP mirror for Polymarket's RTDS socket.

The mirror is deliberately standalone: it has no credentials, no trading-path
imports, and opens no socket until :meth:`ChainlinkTwapRtdsMirror.start` is
called.  Every stored timestamp comes from ``payload.timestamp`` (the Chainlink
observation time), never the outer RTDS delivery timestamp.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://ws-live-data.polymarket.com"

_WINDOW_BY_TOPIC: dict[str, int] = {"crypto_prices_twap_sixty": 60}
_WINDOW_BY_INTERVAL: dict[str, int] = {"5m": 60, "15m": 60}
_SUPPORTED_COINS = frozenset({"BTC", "ETH", "SOL", "XRP"})
_E18 = Decimal(10) ** 18

SUBSCRIBE_MESSAGE: dict = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_twap_sixty",
            "type": "update",
        },
    ],
}


@dataclass(frozen=True)
class TwapSample:
    """One validated Chainlink TWAP observation."""

    coin: str
    window_s: int
    value: Decimal
    observation_ts: float


def _wire_coin(raw: object) -> Optional[str]:
    """Normalize a supported ``BASE/USD`` Chainlink symbol."""
    if not isinstance(raw, str):
        return None
    parts = [part.strip().upper() for part in raw.split("/")]
    if len(parts) != 2 or parts[1] != "USD" or parts[0] not in _SUPPORTED_COINS:
        return None
    return parts[0]


def _lookup_coin(raw: object) -> Optional[str]:
    if not isinstance(raw, str):
        return None
    coin = raw.strip().upper()
    return coin if coin in _SUPPORTED_COINS else None


def _positive_decimal(raw: object) -> Optional[Decimal]:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


def _twap_value(payload: dict) -> Optional[Decimal]:
    """Prefer the exact E18 integer; use ``value`` only when it is absent."""
    if payload.get("full_accuracy_value") is not None:
        scaled = _positive_decimal(payload.get("full_accuracy_value"))
        if scaled is None or scaled != scaled.to_integral_value():
            return None
        return scaled / _E18
    return _positive_decimal(payload.get("value"))


def _observation_ts(raw: object) -> Optional[float]:
    """Normalize a positive payload epoch timestamp to seconds."""
    value = _positive_decimal(raw)
    if value is None:
        return None
    # RTDS payload timestamps are epoch milliseconds.  Accept second-scale
    # epochs too for deterministic replays, without ever consulting the outer
    # message timestamp as a fallback.
    if value >= Decimal("100000000000"):
        value /= Decimal(1000)
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(result) or result <= 0:
        return None
    return result


def _window_seconds(raw: object) -> Optional[int]:
    value = _positive_decimal(raw)
    if value is None or value != value.to_integral_value():
        return None
    try:
        return int(value)
    except (OverflowError, ValueError):
        return None


def parse_twap_message(message: object) -> Optional[TwapSample]:
    """Validate one production RTDS update and return its exact sample."""
    if not isinstance(message, dict) or message.get("type") != "update":
        return None
    topic = message.get("topic")
    expected_window = _WINDOW_BY_TOPIC.get(topic)
    if expected_window is None:
        return None
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None

    coin = _wire_coin(payload.get("symbol"))
    value = _twap_value(payload)
    observed_at = _observation_ts(payload.get("timestamp"))
    window_s = _window_seconds(payload.get("window_s"))
    if (
        coin is None
        or value is None
        or observed_at is None
        or window_s != expected_window
    ):
        return None
    return TwapSample(
        coin=coin,
        window_s=window_s,
        value=value,
        observation_ts=observed_at,
    )


class ChainlinkTwapRtdsMirror:
    """Thread-safe bounded mirror of Polymarket's 60s Chainlink TWAP."""

    def __init__(
        self,
        *,
        url: str = DEFAULT_WS_URL,
        # A 15m sleeve asks for its opening sample up to 899 seconds later.
        # 4096 observations per series covers >4 updates/s for that whole
        # interval while keeping the eight supported series memory-bounded.
        history_size: int = 4096,
        connect_factory=None,
        reconnect_backoff_s: float = 1.0,
        reconnect_backoff_max_s: float = 30.0,
        recv_timeout_s: float = 1.0,
        heartbeat_interval_s: float = 5.0,
        opening_price_fetcher=None,
        fallback_retry_s: float = 2.0,
    ) -> None:
        self._url = str(url)
        self._history_size = max(1, int(history_size))
        self._connect_factory = connect_factory
        self._backoff_base_s = max(0.01, float(reconnect_backoff_s))
        self._backoff_max_s = max(
            self._backoff_base_s, float(reconnect_backoff_max_s)
        )
        self._recv_timeout_s = max(0.01, float(recv_timeout_s))
        self._heartbeat_interval_s = max(0.01, float(heartbeat_interval_s))
        self._opening_price_fetcher = opening_price_fetcher
        self._fallback_retry_s = max(0.0, float(fallback_retry_s))
        self._fallback_retry_after: dict[tuple[str, int, float], float] = {}

        self._lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._history: dict[tuple[str, int], deque[TwapSample]] = {}
        self._boundary_cache: dict[tuple[str, int, float], Optional[TwapSample]] = {}
        self._boundary_cache_order: deque[tuple[str, int, float]] = deque()
        self._boundary_cache_size = 1024
        self._connected = False
        self._connection_count = 0
        self._connection_errors = 0
        self._last_frame_monotonic: Optional[float] = None
        self._last_observation_ts: Optional[float] = None
        self._connection_accepted_sample = False

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- lookup -------------------------------------------------------------

    def get_opening_twap(
        self, coin: str, interval: str, boundary_ts: float
    ) -> Optional[TwapSample]:
        """Return the Chainlink observation at the exact market boundary.

        Both ``5m`` and ``15m`` use the 60-second TWAP.
        RTDS has no replay after a disconnect, so accepting a merely recent
        pre-boundary observation would silently change Polymarket's price to
        beat. Missing, non-exact, or unsupported data therefore returns None.
        """
        normalized_coin = _lookup_coin(coin)
        window_s = (
            _WINDOW_BY_INTERVAL.get(interval) if isinstance(interval, str) else None
        )
        if normalized_coin is None or window_s is None or isinstance(boundary_ts, bool):
            return None
        try:
            boundary = float(boundary_ts)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(boundary) or boundary <= 0:
            return None

        cache_key = (normalized_coin, window_s, boundary)
        with self._lock:
            if cache_key in self._boundary_cache:
                selected = self._boundary_cache[cache_key]
            else:
                selected = None
                for sample in self._history.get((normalized_coin, window_s), ()):
                    if math.isclose(
                        sample.observation_ts,
                        boundary,
                        rel_tol=0.0,
                        abs_tol=1e-6,
                    ):
                        selected = sample
                self._cache_boundary_sample_locked(cache_key, selected)
            if selected is not None or self._opening_price_fetcher is None:
                return selected
            now_mono = time.monotonic()
            if now_mono < self._fallback_retry_after.get(cache_key, 0.0):
                return None
            # Claim this retry before network I/O so concurrent scanner paths
            # cannot stampede the same first-party market page.
            self._fallback_retry_after[cache_key] = now_mono + self._fallback_retry_s

        try:
            value = self._opening_price_fetcher(
                coin=normalized_coin,
                interval=interval,
                boundary_ts=int(boundary),
            )
        except Exception:  # noqa: BLE001 - public fallback must stay fail-closed
            logger.exception(
                "Polymarket Price To Beat fallback raised coin=%s interval=%s boundary=%d",
                normalized_coin,
                interval,
                int(boundary),
            )
            return None
        value = _positive_decimal(value)
        if value is None:
            return None
        fallback_sample = TwapSample(
            coin=normalized_coin,
            window_s=window_s,
            value=value,
            observation_ts=boundary,
        )
        with self._lock:
            # A delayed exact RTDS frame may have arrived during the HTTP GET;
            # it remains preferred. Otherwise cache the official UI value.
            selected = self._boundary_cache.get(cache_key)
            if selected is None:
                self._cache_boundary_sample_locked(cache_key, fallback_sample)
                selected = fallback_sample
                logger.info(
                    "TWAP open recovered from Polymarket Price To Beat "
                    "coin=%s interval=%s boundary=%d value=%s",
                    normalized_coin,
                    interval,
                    int(boundary),
                    value,
                )
        return selected

    def _cache_boundary_sample_locked(
        self,
        cache_key: tuple[str, int, float],
        sample: Optional[TwapSample],
    ) -> None:
        if cache_key not in self._boundary_cache:
            self._boundary_cache_order.append(cache_key)
        self._boundary_cache[cache_key] = sample
        while len(self._boundary_cache_order) > self._boundary_cache_size:
            evicted = self._boundary_cache_order.popleft()
            self._boundary_cache.pop(evicted, None)

    # -- message handling ---------------------------------------------------

    def _handle_message(self, raw: object) -> int:
        """Parse one RTDS frame, returning the number of samples accepted."""
        with self._lock:
            self._last_frame_monotonic = time.monotonic()

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        if isinstance(raw, str):
            raw = raw.strip()
            if not raw or raw.upper() in {"PING", "PONG"}:
                return 0
            try:
                raw = json.loads(raw)
            except ValueError:
                return 0

        messages = raw if isinstance(raw, list) else [raw]
        accepted = 0
        for message in messages:
            sample = self._parse_message(message)
            if sample is None:
                continue
            key = (sample.coin, sample.window_s)
            with self._lock:
                history = self._history.get(key)
                if history is None:
                    history = self._history[key] = deque(maxlen=self._history_size)
                history.append(sample)
                # A delayed exact observation can fill a previously missing
                # boundary cache entry. Never replace it with a nearby sample.
                exact_key = (sample.coin, sample.window_s, sample.observation_ts)
                if exact_key in self._boundary_cache:
                    self._boundary_cache[exact_key] = sample
                # Preserve exact market-boundary observations even if a bursty
                # stream later evicts them from the per-series history.
                rounded_ts = round(sample.observation_ts)
                if math.isclose(
                    sample.observation_ts,
                    float(rounded_ts),
                    rel_tol=0.0,
                    abs_tol=1e-6,
                ):
                    step = 300 if sample.window_s == 30 else 900
                    if rounded_ts % step == 0:
                        self._cache_boundary_sample_locked(
                            (sample.coin, sample.window_s, float(rounded_ts)),
                            sample,
                        )
                if (
                    self._last_observation_ts is None
                    or sample.observation_ts > self._last_observation_ts
                ):
                    self._last_observation_ts = sample.observation_ts
                self._connection_accepted_sample = True
            accepted += 1
        return accepted

    @staticmethod
    def _parse_message(message: object) -> Optional[TwapSample]:
        return parse_twap_message(message)

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Start the daemon receive thread; dependency failures stay soft."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="cryptobot-chainlink-twap-rtds",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lifecycle_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(2.0, self._recv_timeout_s + 1.0))
        with self._lifecycle_lock:
            if self._thread is thread and (thread is None or not thread.is_alive()):
                self._thread = None

    def health(self) -> dict[str, object]:
        """Return compact, lock-consistent diagnostics without sample bodies."""
        with self._lock:
            sample_count = sum(len(history) for history in self._history.values())
            series_count = len(self._history)
            connected = self._connected
            connections = self._connection_count
            errors = self._connection_errors
            last_frame = self._last_frame_monotonic
            last_observation = self._last_observation_ts
        thread = self._thread
        return {
            "running": bool(thread is not None and thread.is_alive()),
            "connected": connected,
            "series_count": series_count,
            "sample_count": sample_count,
            "connections": connections,
            "connection_errors": errors,
            "last_frame_age_s": (
                None if last_frame is None else max(0.0, time.monotonic() - last_frame)
            ),
            "last_observation_ts": last_observation,
        }

    # -- connection loop ----------------------------------------------------

    def _open_connection(self):
        if self._connect_factory is not None:
            return self._connect_factory()
        # Lazy import keeps module import and unit tests independent of the
        # optional websocket dependency and never opens a socket on import.
        from websockets.sync.client import connect

        return connect(
            self._url,
            open_timeout=10.0,
            close_timeout=2.0,
            max_queue=64,
            ping_interval=None,
        )

    def _stream_once(self) -> None:
        conn = self._open_connection()
        with conn as ws:
            ws.send(json.dumps(SUBSCRIBE_MESSAGE))
            with self._lock:
                self._connected = True
                self._connection_count += 1
                self._connection_accepted_sample = False
                connection_count = self._connection_count
            logger.info(
                "Chainlink TWAP RTDS connected (connection=%d)",
                connection_count,
            )
            last_ping = time.monotonic()
            try:
                while not self._stop.is_set():
                    now = time.monotonic()
                    if now - last_ping >= self._heartbeat_interval_s:
                        ws.send("PING")
                        last_ping = now
                    try:
                        raw = ws.recv(timeout=self._recv_timeout_s)
                    except TimeoutError:
                        continue
                    if raw is None:
                        raise ConnectionError("RTDS socket closed")
                    self._handle_message(raw)
            finally:
                with self._lock:
                    self._connected = False

    def _run(self) -> None:
        backoff = self._backoff_base_s
        while not self._stop.is_set():
            try:
                self._stream_once()
                if self._stop.is_set():
                    break
                backoff = self._backoff_base_s
            except Exception as exc:  # noqa: BLE001 - all socket failures reconnect
                with self._lock:
                    self._connected = False
                    self._connection_errors += 1
                    accepted_sample = self._connection_accepted_sample
                if accepted_sample:
                    backoff = self._backoff_base_s
                logger.warning(
                    "Chainlink TWAP RTDS stream error (%s); reconnecting in %.1fs",
                    type(exc).__name__,
                    backoff,
                )
            if self._stop.wait(timeout=backoff):
                break
            backoff = min(self._backoff_max_s, backoff * 2.0)
        with self._lock:
            self._connected = False
