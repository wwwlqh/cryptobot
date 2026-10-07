from __future__ import annotations

from polymarket.backtest.fetch.fetch_gamma_resolutions import _parse_market


def test_parse_market_extracts_exact_twap_event_metadata() -> None:
    payload = [{
        "closed": True,
        "umaResolutionStatus": "resolved",
        "resolutionSource": "https://data.chain.link/streams/btc-usd-twap-30s-streams",
        "outcomes": '["Up", "Down"]',
        "outcomePrices": '["0", "1"]',
        "cryptoMarketConfig": {
            "twapEnabled": True,
            "twapLookbackSeconds": 30,
        },
        "events": [{
            "eventMetadata": {
                "priceToBeat": 63_912.1302534336,
                "finalPrice": 63_881.52829828407,
            },
        }],
    }]

    row = _parse_market("btc-updown-5m-1786406400", payload)

    assert row["gamma_won_up"] == 0.0
    assert row["twap_enabled"] is True
    assert row["twap_lookback_seconds"] == 30
    assert row["price_to_beat"] == 63_912.1302534336
    assert row["final_price"] == 63_881.52829828407
