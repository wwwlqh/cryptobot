# polymarket/backtest/

Offline research: downloading market data, building features and labels, training the models and replaying bets against real order books.

| Folder | Contents |
|---|---|
| `fetch/` | Data downloaders: pmdata order books, Gamma outcomes, Binance 1-second prices, Chainlink TWAP prices |
| `training/` | Feature and label building, walk-forward training, model export |
| `engine/` | Order-book replay with fees and fill simulation (`tick_exit004.py`) |
| `lib/` | Data cleaning (`pmdata_clean.py`), the research pool loader (`pool_study.py`), the tick-book store (`tick_store.py`) |
| `scripts/` | Command-line tools: tick download, TWAP label building, exact TWAP price fetch |
| `studies/` | Dated research reports and the scripts that produced them ([index](studies/README.md)) |
| `tests/` | Tests for the engine, data loading and scripts |

Data sources and where each one is stored: [SOURCES.md](SOURCES.md).

## Data folders

The research data is about 20 GB, so it isn't in git. Code finds it through three settings in `polymarket/backtest/__init__.py`:

| Setting | Contents | Default |
|---|---|---|
| `BT_DATA_DIR` | pmdata downloads, outcomes, server snapshots | `polymarket/backtest/data/` |
| `BT_CACHE_DIR` | Binance 1-second price cache | `polymarket/backtest/cache/` |
| `BT_RUNS_DIR` | Training pools, scored datasets, study outputs | `polymarket/backtest/optimize_runs/` |

Each can be overridden with an environment variable of the same name. Import them instead of hard-coding paths:

```python
from polymarket.backtest import BT_DATA_DIR, BT_CACHE_DIR, BT_RUNS_DIR
```

## Method

- **Labels.** Each 5m or 15m window settles on the Chainlink 60-second TWAP. Labels use Gamma's exact `priceToBeat` / `finalPrice` values.
- **Features.** 11 features per decision second: price, time remaining, momentum, and a categorical coin ID.
- **Validation.** Walk-forward: train on earlier data, test on the next period, and repeat. A final forward period that the model never saw is graded separately.
- **Execution.** Replays walk the real per-second (or per-tick) order book with Polymarket's fee curve, instead of assuming a fill at the quoted price.

## Reproducing the live model

The research data must be in place first (see [SOURCES.md](SOURCES.md)).

```bash
python -m polymarket.backtest.studies.tick_latency.run_twap_wfo_full                 # walk-forward evaluation
python -m polymarket.backtest.studies.tick_latency.run_twap_pooled_fixed_entry_windows  # entry-window study
python -m polymarket.backtest.training.export_twap_symmetric_60s                     # export the two HGB models
```

The export writes the model bundle to `data/models/twap_symmetric_v1/`. That folder is gitignored, so a fresh clone has no trained models until this runs.

## Limits

The replay can't model queue position, network jitter, other traders taking the same liquidity, or stale balances. Its results are an upper bound on what live trading achieves. The live results in [`analysis/`](../../analysis/) show that gap.
