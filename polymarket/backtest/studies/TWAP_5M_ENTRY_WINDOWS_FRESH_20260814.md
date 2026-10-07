# Fresh 5m shared-entry-window comparison — 2026-08-14

## Verdict

The practical full-5m candidate (T-299 through T-1) is profitable, but it is
materially worse than waiting until roughly the final 2 minutes 40 seconds.

The raw one-second winners are:

- Base PnL: T-161, with 175 fills, 74.29% WR, $27.11 PnL, $0.155 EV/trade,
  and $10.02 max drawdown.
- T3 PnL: T-162, with $43.44 PnL, $0.247 EV/initial filled trade, and
  $17.27 max drawdown.

The recommended research setting is **one common T-160 horizon for BTC, ETH,
SOL, and XRP**. T-158 through T-162 is a tight plateau and T-160 is its round,
stable centre. Selecting T-161 or T-162 as if one second were known exactly
would overstate what this short forward sample can establish.

This is not a live admission. The Aug 14 sample was used to select the timing,
so T-160 must now be frozen and re-graded on a later untouched period.

## Fresh data and protocol

- PMData refresh: 484/484 requested 5m markets downloaded successfully for
  BTC, ETH, SOL, and XRP; no missing or failed requests.
- Common coverage: market starts from 2026-08-14 00:00 through 10:00 UTC
  (08:00 through 18:00 MYT), 121 starts per coin.
- Matching Binance tape: 36,300 one-second rows per coin, ending 10:04:59 UTC.
- Settlement: PMData's resolved `winning_outcome`.
- Target anchor: synthetic trailing 60-second Binance TWAP at the opening
  boundary, matching the requested future 60-second design.
- Synthetic 60-second direction agreed with the PMData resolution on 467/475
  usable markets (98.32%).
- Execution: 400 ms latency, edge-preserving FAK fills, fees, base and T3.
- Model policy: one per-coin HGB trained across T-299 through T-1. Its
  development-only expanding WFO ask/edge specification was frozen before any
  horizon comparison. Every cutoff reuses the same models and specs.
- Shared-time policy: every tested horizon is identical across all four coins;
  no per-coin timing selection.
- T-299 represents the practical full 5m window. At exact T-300 elapsed
  variance is zero, so the partial-return z feature is undefined.

Historical 5m markets used a 30-second official TWAP. Gamma's current metadata
endpoint was unavailable during this run, so this test uses the requested
synthetic 60-second opening anchor plus the actual PMData resolution rather
than claiming that today's venue metadata was independently verified as 60s.

## Aggregate results

| Shared start | Fills | WR | Base PnL | Base EV/trade | Base max DD | T3 PnL | T3 EV/trade | T3 max DD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| T-60 | 69 | 75.36% | $5.11 | $0.074 | **$6.55** | $6.29 | $0.091 | **$9.99** |
| T-150 | 163 | 73.62% | $20.54 | $0.126 | $9.54 | $31.07 | $0.191 | $21.23 |
| T-158 | 169 | 74.56% | $25.51 | $0.151 | $9.46 | $40.33 | $0.239 | $20.43 |
| T-159 | 170 | 74.12% | $24.67 | $0.145 | $9.46 | $41.63 | $0.245 | $20.27 |
| **T-160** | **171** | **74.27%** | **$25.44** | **$0.149** | **$9.46** | **$41.10** | **$0.240** | **$19.20** |
| T-161 | 175 | 74.29% | **$27.11** | **$0.155** | $10.02 | $42.57 | $0.243 | $17.32 |
| T-162 | 176 | 73.86% | $25.53 | $0.145 | $9.46 | **$43.44** | **$0.247** | **$17.27** |
| T-299 | 226 | 67.26% | $8.96 | $0.040 | $17.93 | $2.45 | $0.011 | $29.93 |

Trading from the practical start of the 5m market adds fills but sharply lowers
quality: relative to T-160, T-299 loses 7.01 percentage points of WR, $16.47 of
base PnL, and $38.65 of T3 PnL while roughly doubling drawdown.

## Coin attribution at the raw base winner (T-161)

| Coin | Fills | WR | PnL | EV/trade | Max DD |
| --- | ---: | ---: | ---: | ---: | ---: |
| BTC | 39 | 92.31% | $23.30 | $0.598 | $1.65 |
| ETH | 47 | 70.21% | $4.13 | $0.088 | $5.12 |
| SOL | 43 | 60.47% | **-$6.75** | -$0.157 | $10.86 |
| XRP | 46 | 76.09% | $6.42 | $0.140 | $4.03 |

The four-coin portfolio is positive because BTC dominates. SOL is negative on
this fresh sample, but it remains included because the experiment explicitly
requires the same shared horizon and all four coins.

## Recommendation

1. Freeze T-160 as the shared four-coin challenger; do not use separate times
   by coin.
2. Keep T-60 as the conservative control because its drawdown is lowest.
3. Re-grade both unchanged on the next untouched PMData period before any live
   change. Do not choose again from the next period.

Artifacts: `polymarket/backtest/optimize_runs/twap5m_entry_windows_fresh_20260814/`.
Runner: `run_twap5m_entry_window_fresh.py` (a one-off script, not included in this repository).
