"""polymarket.backtest.fetch — raw market data -> parquet in backtest/data/.

Fetches Polymarket's Chainlink-settled resolutions via the Gamma API
(``fetch_gamma_resolutions`` -> ``pmdata_outcomes.parquet``'s ``won_up``, the
settlement label the WFO arbiter reads), plus three pmdata.dev pullers, one per
product:

* ``pmdata_download`` — the 1 Hz resampled book tree ``pmdata/poly_snapshot/``.
  This is the module RECOVERY.md and AGENTS.md name as THE re-download path for
  that tree, and ``training.pmdata_target.require_snapshot_dir()`` names it in
  its FileNotFoundError. poly_snapshot/ was deleted 2026-07-31, so the research
  pool cannot be rebuilt until this has been re-run.
* ``pmdata_raw_pull`` — the byte-exact ``poly_l2_raw/`` tick archive.
* ``..scripts.tick_download_last30`` — trims poly_l2 to the final 30 s into
  ``poly_tick_last30.db``, which is what ``engine/tick_exit004`` reads.

Retired 2026-08-01 (phase 6): ``fetch_pmdata.py``, a second puller into the
SAME ``poly_snapshot`` path as ``pmdata_download`` that wrote the served bytes
verbatim — i.e. the schema-drift variant ``pmdata_download.py
--backfill-outcome`` had to repair — and ``build_pmdata_features.py``, whose
only product was the narrow ``snapshot_features.parquet``. Nothing has read
that narrow frame since the pool moved to ``snapshot_features_full.parquet``,
and the ``poly_snapshot/`` tree the builder walked went on 2026-07-31.
``COINS`` and ``parse_slug`` moved to ``_common`` with them.

The binance-kline fetchers that wrote ``data/prices.db`` (``history.py``,
``gaps.py``) were deleted 2026-07-31 with the exp044 retirement: prices.db fed
only nx_v1 -> rf_v1 -> decision_table, all three of which are gone. They were
the last in-repo writers of a binance substrate — the 1s kline cache the
live-scorer pool actually reads (``$BT_CACHE_DIR/binance_1s/<coin>_1s.parquet``,
loaded by ``training.offset_wfo._load_1s``) is a bulk download from
data.binance.vision, not something this package fetches. See SOURCES.md §4.
"""
