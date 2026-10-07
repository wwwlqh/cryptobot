# Research studies

Dated research reports. Each one records what was tested and decided at the time, so later reports can supersede earlier ones. The scripts in `tick_latency/` produce them.

"T3" in some reports is a top-up variant that added to an already filled position. It was tested and then disabled; the live bot places one entry per market.

| Report | Question | Finding |
|---|---|---|
| [tick_latency/TWAP_REFIT_PRELIMINARY_20260812.md](tick_latency/TWAP_REFIT_PRELIMINARY_20260812.md) | Can the model be refit on Chainlink TWAP labels? | Research-only first refit |
| [tick_latency/TWAP_WFO_FULL_20260812.md](tick_latency/TWAP_WFO_FULL_20260812.md) | Full-history walk-forward on TWAP labels | Corrected for two label/feature bugs, with regression tests |
| [tick_latency/TWAP_EXACT_FORWARD_20260812.md](tick_latency/TWAP_EXACT_FORWARD_20260812.md) | Forward test against exact Chainlink TWAP labels | Pre-registered design, held to resolution |
| [TWAP_POOLED_FIXED_ENTRY_WINDOWS_20260814.md](TWAP_POOLED_FIXED_ENTRY_WINDOWS_20260814.md) | One model per coin, or pooled? | Two pooled models (5m and 15m) with coin as a categorical feature |
| [TWAP_5M_ENTRY_WINDOWS_FRESH_20260814.md](TWAP_5M_ENTRY_WINDOWS_FRESH_20260814.md) | When should 5m entries start? | Waiting until about T-160s beat entering for the full window |
| [TWAP_15M_ENTRY_WINDOWS_20260814.md](TWAP_15M_ENTRY_WINDOWS_20260814.md) | When should 15m entries start? | Best tested common start was T-520s |
| [TWAP_ENTRY_WINDOWS_APPEND_20260815.md](TWAP_ENTRY_WINDOWS_APPEND_20260815.md) | Do the entry windows hold up on new data? | 5m T-160 stayed best; the live config then used a shared T-150 window |
| [TWAP_LIVE_APPLES_TO_APPLES_20260815.md](TWAP_LIVE_APPLES_TO_APPLES_20260815.md) | Why did live trading lose money? | The loss is real and reproducible: the frozen model became overconfident in a new market regime and piled into correlated "up" bets |
