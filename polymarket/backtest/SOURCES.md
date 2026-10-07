# Data sources

Where the research data comes from, which script downloads it, and where it lands. None of it is in git. Paths use the `BT_*` folders described in [README.md](README.md).

| Source | What it provides | Downloader | Lands in |
|---|---|---|---|
| [pmdata.dev](https://pmdata.dev) | Per-second Polymarket order books (L2), per-tick books for the final 30 seconds | `fetch/pmdata_download.py`, `fetch/pmdata_day_l2.py`, `fetch/pmdata_raw_pull.py`, `scripts/tick_download_last30.py` | `$BT_DATA_DIR/pmdata/` (`poly_snapshot/`, `poly_l2_raw/`, `poly_tick_last30.db`) |
| pmdata.dev Chainlink | Chainlink Data Streams prices and direct 60-second TWAP files | `fetch/fetch_chainlink_streams.py`, `fetch/fetch_chainlink_twap.py` | `$BT_DATA_DIR/` |
| Polymarket Gamma API | Market metadata, fee rates and settled outcomes, including the exact `priceToBeat` / `finalPrice` | `fetch/fetch_gamma_resolutions.py`, `scripts/fetch_exact_twap_prices.py` | `$BT_DATA_DIR/pmdata/gamma_resolutions/`, `pmdata_outcomes.parquet` |
| Binance | 1-second spot prices for features | Bulk daily files from `data.binance.vision`; `fetch/fetch_binance_1s.py` for recent days | `$BT_CACHE_DIR/binance_1s/` |
| The live bot | Snapshots of the live SQLite database (ledger and signal log) for comparing live with backtest results | Copied from the server with `sqlite3 .backup` | `$BT_DATA_DIR/server_snapshot/` |

pmdata downloads need `PMDATA_API_KEY` in `.env` and count against an API quota.

## Which pmdata downloader to use

- `pmdata_download.py` builds the wide one-book-per-second history used for training and replay.
- `pmdata_day_l2.py` does the same from daily archives. It's the quota-friendly way to fill long ranges.
- `pmdata_raw_pull.py` keeps raw tick-level books for the markets the bot actually traded, for fill-accurate replay.
- `scripts/tick_download_last30.py` keeps only the final 30 seconds (plus a 5-second lead) of each market, which is what the tick replay engine reads.

## Cleaning and labels

- `lib/pmdata_clean.py` is the single cleaning step. It drops broken books (ask ≤ 0, ask ≥ 1, crossed by more than 2¢) and ambiguous outcomes.
- Only outcomes with `status == 'ok'` are used as labels. Everything else is kept as missing, so it can't count as a loss.
- `scripts/build_twap_labels.py` rebuilds historical labels under the Chainlink 60-second TWAP settlement rule. Direct TWAP files (from 2026-08-01) are kept separate so they can't leak into training.

## Rebuild order

1. Download books, outcomes and Binance prices (table above).
2. Build TWAP labels: `python -m polymarket.backtest.scripts.build_twap_labels`.
3. Train and evaluate: `python -m polymarket.backtest.studies.tick_latency.run_twap_wfo_full`.
4. Export the live models: `python -m polymarket.backtest.training.export_twap_symmetric_60s`.
