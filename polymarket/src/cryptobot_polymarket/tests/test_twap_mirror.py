"""Focused tests for the public Chainlink TWAP RTDS mirror."""

from __future__ import annotations

import json
import time
from decimal import Decimal

import pytest

from cryptobot_polymarket.twap_mirror import (
    SUBSCRIBE_MESSAGE,
    ChainlinkTwapRtdsMirror,
)


def _event(
    *,
    topic: str = "crypto_prices_twap_sixty",
    symbol: str = "BTC/USD",
    observation_ms: int = 1_800_000_000_000,
    window_s: int = 60,
    full_accuracy_value: object | None = "100000000000000000000",
    value: object | None = 100.0,
    event_type: str = "update",
) -> dict:
    payload = {
        "symbol": symbol,
        "timestamp": observation_ms,
        "window_s": window_s,
    }
    if full_accuracy_value is not None:
        payload["full_accuracy_value"] = full_accuracy_value
    if value is not None:
        payload["value"] = value
    return {
        "topic": topic,
        "type": event_type,
        # Deliberately unrelated: lookup must use payload.timestamp only.
        "timestamp": observation_ms + 9_999_000,
        "payload": payload,
    }


def test_full_accuracy_e18_is_exact_and_uses_payload_timestamp():
    mirror = ChainlinkTwapRtdsMirror()
    raw = _event(full_accuracy_value="123456789012345678901")

    assert mirror._handle_message(json.dumps(raw)) == 1
    sample = mirror.get_opening_twap(
        "BTC", "5m", boundary_ts=1_800_000_000.0
    )

    assert sample is not None
    assert sample.value == Decimal("123.456789012345678901")
    assert sample.observation_ts == pytest.approx(1_800_000_000.0)
    assert sample.window_s == 60


def test_value_is_exact_decimal_fallback_when_full_accuracy_is_absent():
    mirror = ChainlinkTwapRtdsMirror()
    mirror._handle_message(
        _event(
            symbol="eth/usd",
            full_accuracy_value=None,
            value="3456.125",
        )
    )

    sample = mirror.get_opening_twap(
        "eth", "5m", boundary_ts=1_800_000_000.0
    )
    assert sample is not None
    assert sample.coin == "ETH"
    assert sample.value == Decimal("3456.125")


def test_lookup_requires_exact_boundary_and_never_uses_nearby_samples():
    mirror = ChainlinkTwapRtdsMirror()
    mirror._handle_message(
        _event(
            observation_ms=1_800_000_090_000,
            full_accuracy_value="90000000000000000000",
        )
    )
    mirror._handle_message(
        _event(
            observation_ms=1_800_000_101_000,
            full_accuracy_value="101000000000000000000",
        )
    )

    sample = mirror.get_opening_twap("BTC", "5m", boundary_ts=1_800_000_100.0)

    assert sample is None

    mirror._handle_message(
        _event(
            observation_ms=1_800_000_100_000,
            full_accuracy_value="100000000000000000000",
        )
    )
    exact = mirror.get_opening_twap("BTC", "5m", boundary_ts=1_800_000_100.0)
    assert exact is not None
    assert exact.value == Decimal("100")


def test_cached_missing_boundary_is_filled_only_by_delayed_exact_sample():
    mirror = ChainlinkTwapRtdsMirror()
    mirror._handle_message(
        _event(
            observation_ms=1_800_000_090_000,
            full_accuracy_value="90000000000000000000",
        )
    )
    boundary = 1_800_000_100.0
    assert mirror.get_opening_twap("BTC", "5m", boundary) is None

    mirror._handle_message(
        _event(
            observation_ms=1_800_000_101_000,
            full_accuracy_value="101000000000000000000",
        )
    )
    assert mirror.get_opening_twap("BTC", "5m", boundary) is None

    mirror._handle_message(
        _event(
            observation_ms=1_800_000_100_000,
            full_accuracy_value="100000000000000000000",
        )
    )
    improved = mirror.get_opening_twap("BTC", "5m", boundary)
    assert improved is not None
    assert improved.value == Decimal("100")


def test_official_price_to_beat_recovers_missing_exact_rtds_boundary():
    calls = []

    def fetcher(**kwargs):
        calls.append(kwargs)
        return Decimal("63665.570921281666")

    mirror = ChainlinkTwapRtdsMirror(opening_price_fetcher=fetcher)
    sample = mirror.get_opening_twap("BTC", "5m", 1_800_000_000.0)
    assert sample is not None
    assert sample.value == Decimal("63665.570921281666")
    assert sample.observation_ts == 1_800_000_000.0
    assert calls == [{
        "coin": "BTC", "interval": "5m", "boundary_ts": 1_800_000_000,
    }]
    # Cached: no second page request.
    assert mirror.get_opening_twap("BTC", "5m", 1_800_000_000.0) == sample
    assert len(calls) == 1


def test_exact_rtds_sample_remains_preferred_over_ui_fallback():
    mirror = ChainlinkTwapRtdsMirror(
        opening_price_fetcher=lambda **_kwargs: Decimal("999")
    )
    mirror._handle_message(_event(full_accuracy_value=str(100 * 10**18)))
    sample = mirror.get_opening_twap("BTC", "5m", 1_800_000_000.0)
    assert sample is not None
    assert sample.value == Decimal("100")


def test_missing_nonexact_and_unsupported_interval_fail_closed():
    mirror = ChainlinkTwapRtdsMirror()
    mirror._handle_message(_event(observation_ms=1_800_000_080_000))

    assert mirror.get_opening_twap("ETH", "5m", 1_800_000_090.0) is None
    assert mirror.get_opening_twap("BTC", "5m", 1_800_000_100.0) is None
    assert mirror.get_opening_twap("BTC", "1h", 1_800_000_090.0) is None
    assert mirror.get_opening_twap("DOGE", "5m", 1_800_000_090.0) is None


def test_symbol_topic_type_window_and_malformed_values_are_filtered():
    mirror = ChainlinkTwapRtdsMirror()
    rejected = [
        _event(topic="crypto_prices_chainlink"),
        _event(event_type="snapshot"),
        _event(symbol="BTC/USDT"),
        _event(symbol="DOGE/USD"),
        _event(window_s=30),
        _event(full_accuracy_value="not-an-int"),
        _event(full_accuracy_value="1.5"),
        _event(full_accuracy_value="0"),
        _event(full_accuracy_value=None, value=-1),
        _event(observation_ms=0),
        _event(window_s=0),
    ]
    missing_observation = _event()
    missing_observation["payload"].pop("timestamp")
    rejected.append(missing_observation)

    for event in rejected:
        assert mirror._handle_message(event) == 0

    assert mirror._handle_message(_event(symbol="XRP/USD")) == 1
    health = mirror.health()
    assert health["sample_count"] == 1
    assert health["series_count"] == 1


def test_interval_mapping_selects_sixty_second_twap_for_both_intervals():
    mirror = ChainlinkTwapRtdsMirror()
    mirror._handle_message(
        _event(
            topic="crypto_prices_twap_sixty",
            symbol="SOL/USD",
            window_s=60,
            full_accuracy_value="150000000000000000000",
        )
    )

    assert mirror.get_opening_twap("SOL", "5m", 1_800_000_005.0) is None
    five = mirror.get_opening_twap("SOL", "5m", 1_800_000_000.0)
    sample = mirror.get_opening_twap("SOL", "15m", 1_800_000_000.0)
    assert five is not None
    assert five.window_s == 60
    assert sample is not None
    assert sample.window_s == 60


def test_history_is_bounded_per_coin_and_window():
    mirror = ChainlinkTwapRtdsMirror(history_size=2)
    for seconds in (100, 110, 120):
        mirror._handle_message(
            _event(
                observation_ms=1_800_000_000_000 + seconds * 1000,
                full_accuracy_value=str(seconds * 10**18),
            )
        )

    # The 100-second observation was evicted by the per-series maxlen.
    assert mirror.get_opening_twap("BTC", "5m", 1_800_000_100.0) is None
    sample = mirror.get_opening_twap("BTC", "5m", 1_800_000_110.0)
    assert sample is not None
    assert sample.value == Decimal("110")
    assert mirror.health()["sample_count"] == 2


def test_default_history_retains_a_15m_open_until_last_entry_second():
    mirror = ChainlinkTwapRtdsMirror()
    base_ms = 1_800_000_000_000
    for seconds in range(900):
        mirror._handle_message(
            _event(
                topic="crypto_prices_twap_sixty",
                window_s=60,
                observation_ms=base_ms + seconds * 1000,
                full_accuracy_value=str((100 + seconds) * 10**18),
            )
        )

    sample = mirror.get_opening_twap("BTC", "15m", base_ms / 1000)
    assert sample is not None
    assert sample.value == Decimal("100")


def test_exact_boundary_cache_survives_history_eviction():
    mirror = ChainlinkTwapRtdsMirror(history_size=1)
    boundary_ms = 1_800_000_000_000
    mirror._handle_message(_event(observation_ms=boundary_ms))
    mirror._handle_message(_event(observation_ms=boundary_ms + 1_000))

    sample = mirror.get_opening_twap("BTC", "5m", boundary_ms / 1000)

    assert sample is not None
    assert sample.observation_ts == boundary_ms / 1000


class _DroppingConnection:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent
        self.recv_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def send(self, payload: str) -> None:
        self.sent.append(payload)

    def recv(self, timeout=None):
        self.recv_calls += 1
        time.sleep(0.01)
        if self.recv_calls > 8:
            raise ConnectionError("test disconnect")
        raise TimeoutError


def test_lifecycle_subscribes_heartbeats_reconnects_and_fails_soft():
    sent: list[str] = []
    mirror = ChainlinkTwapRtdsMirror(
        connect_factory=lambda: _DroppingConnection(sent),
        heartbeat_interval_s=0.02,
        reconnect_backoff_s=0.01,
        reconnect_backoff_max_s=0.02,
        recv_timeout_s=0.01,
    )

    mirror.start()
    mirror.start()  # idempotent
    try:
        deadline = time.time() + 2.0
        while time.time() < deadline:
            if "PING" in sent and mirror.health()["connection_errors"]:
                break
            time.sleep(0.01)
    finally:
        mirror.stop()

    subscriptions = [json.loads(item) for item in sent if item != "PING"]
    assert subscriptions
    assert subscriptions[0] == SUBSCRIBE_MESSAGE
    assert "gamma_auth" not in json.dumps(subscriptions[0])
    assert "PING" in sent
    assert mirror.get_opening_twap("BTC", "5m", time.time()) is None
    health = mirror.health()
    assert health["running"] is False
    assert health["connected"] is False
    assert health["connection_errors"] >= 1
