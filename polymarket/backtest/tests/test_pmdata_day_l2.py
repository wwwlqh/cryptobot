"""Contract tests for the bounded PMData Day API downloader."""

from polymarket.backtest.fetch.pmdata_day_l2 import _slug_from_member


def test_slug_from_member_accepts_only_requested_market_series():
    assert _slug_from_member(
        "nested/btc-updown-5m-1786060800.parquet", "btc", "5m"
    ) == "btc-updown-5m-1786060800"

    rejected = (
        "nested/eth-updown-5m-1786060800.parquet",
        "nested/btc-updown-15m-1786060800.parquet",
        "nested/btc-updown-5m-not-a-timestamp.parquet",
        "nested/btc-updown-5m-1786060800.csv",
        "nested/readme.txt",
    )
    for member in rejected:
        assert _slug_from_member(member, "btc", "5m") is None
