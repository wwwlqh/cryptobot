from __future__ import annotations

from polymarket.backtest.scripts.capture_twap_rtds_boundaries import _parse


def test_parse_accepts_only_exact_interval_boundary() -> None:
    message = {
        "topic": "crypto_prices_twap_sixty",
        "type": "update",
        "payload": {
            "symbol": "btc/usd",
            "window_s": 60,
            "timestamp": 1_800_000_000_000,
            "full_accuracy_value": "100000000000000000000",
        },
    }

    assert _parse(message)["observation_ts"] == 1_800_000_000
    message["payload"]["timestamp"] += 1_000
    assert _parse(message) is None
