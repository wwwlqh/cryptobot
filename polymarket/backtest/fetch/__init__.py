"""Raw market data to parquet under the backtest data directory.

* ``fetch_gamma_resolutions``: Polymarket's official resolutions from the Gamma
API, written to ``pmdata_outcomes.parquet`` (its ``won_up`` column is the
settlement label the walk-forward evaluation uses). * ``pmdata_download``: the
1 Hz resampled book tree ``pmdata/poly_snapshot/``.
``training.pmdata_target.require_snapshot_dir()`` points here when that tree is
missing; the research pool cannot be rebuilt without it. * ``pmdata_raw_pull``:
the byte-exact ``poly_l2_raw/`` tick archive. * ``fetch_chainlink_streams`` /
``fetch_chainlink_twap``: Chainlink price and TWAP tapes from pmdata.dev. *
``..scripts.tick_download_last30`` trims poly_l2 to the final 30 seconds into
``poly_tick_last30.db``, which ``engine/tick_exit004`` reads.

The Binance 1-second kline cache the pool uses
(``$BT_CACHE_DIR/binance_1s/<coin>_1s.parquet``, loaded by
``training.offset_wfo._load_1s``) is a bulk download from data.binance.vision,
not fetched by this package.
"""
