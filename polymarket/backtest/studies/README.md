# Research studies

Dated research reports. Each one records what was tested and decided at the time, so later reports can supersede earlier ones. The scripts in `tick_latency/` produce them.

| Report | Question | Finding |
|---|---|---|
| [tick_latency/THROTTLE_REMOVAL_SIZE_20260731.md](tick_latency/THROTTLE_REMOVAL_SIZE_20260731.md) | Should the drawdown throttle go, and what bet size? | Throttle off, 1% of equity on both intervals, hold to settlement |
| [tick_latency/BEST_HOLD_SUMMARY.md](tick_latency/BEST_HOLD_SUMMARY.md) | Best hold-to-resolution strategy on the earlier checkpoint model | Generated report (`scripts/report_best_hold.py`) |
| [tick_latency/TWAP_REFIT_PRELIMINARY_20260812.md](tick_latency/TWAP_REFIT_PRELIMINARY_20260812.md) | Can the model be refit on Chainlink TWAP labels? | Research-only first refit |
| [tick_latency/TWAP_WFO_FULL_20260812.md](tick_latency/TWAP_WFO_FULL_20260812.md) | Full-history walk-forward on TWAP labels | Corrected for two label/feature bugs, with regression tests |
| [tick_latency/TWAP_EXACT_FORWARD_20260812.md](tick_latency/TWAP_EXACT_FORWARD_20260812.md) | Forward test against exact Chainlink TWAP labels | Pre-registered design, held to resolution |
| [TWAP_POOLED_FIXED_ENTRY_WINDOWS_20260814.md](TWAP_POOLED_FIXED_ENTRY_WINDOWS_20260814.md) | One model per coin, or pooled? | Two pooled models (5m and 15m) with coin as a categorical feature |
| [TWAP_5M_ENTRY_WINDOWS_FRESH_20260814.md](TWAP_5M_ENTRY_WINDOWS_FRESH_20260814.md) | When should 5m entries start? | Waiting until about T-160s beat entering for the full window |
| [TWAP_15M_ENTRY_WINDOWS_20260814.md](TWAP_15M_ENTRY_WINDOWS_20260814.md) | When should 15m entries start? | Best tested common start was T-520s |
| [TWAP_ENTRY_WINDOWS_APPEND_20260815.md](TWAP_ENTRY_WINDOWS_APPEND_20260815.md) | Do the entry windows hold up on new data? | 5m T-160 stayed best; the live config then used a shared T-150 window |
| [TWAP_LIVE_APPLES_TO_APPLES_20260815.md](TWAP_LIVE_APPLES_TO_APPLES_20260815.md) | Why did live trading lose money? | The loss is real and reproducible: the frozen model became overconfident in a new market regime and piled into correlated "up" bets |

Supporting files: `tick_latency/runners/` holds replay wrappers, and the `foldBE_stop_*` files are the replay output that `audit/live_artifacts.py` and `scripts/report_best_hold.py` read.
