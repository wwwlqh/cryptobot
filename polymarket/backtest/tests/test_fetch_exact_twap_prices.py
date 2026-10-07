from __future__ import annotations

from polymarket.backtest.scripts.fetch_exact_twap_prices import (
    generated_slugs,
    parse_exact_market,
)


def test_generated_slugs_include_both_intervals_for_every_coin() -> None:
    slugs = generated_slugs("2026-08-07", "2026-08-08")

    assert len(slugs) == 4 * (288 + 96)
    assert "btc-updown-5m-1786060800" in slugs
    assert "xrp-updown-15m-1786060800" in slugs


def test_parse_exact_market_checks_gamma_label() -> None:
    market = {
        "closed": True,
        "resolutionSource": "https://data.chain.link/streams/btc-usd-twap-30s-streams",
        "outcomePrices": '["0", "1"]',
        "cryptoMarketConfig": {
            "twapEnabled": True,
            "twapLookbackSeconds": 30,
        },
        "events": [{
            "eventMetadata": {"priceToBeat": 100.5, "finalPrice": 100.4},
        }],
    }

    row = parse_exact_market("btc-updown-5m-1786060800", market)

    assert row is not None
    assert row["twap_won_up"] == 0
    assert row["gamma_won_up"] == 0
    assert row["label_matches_gamma"] is True
