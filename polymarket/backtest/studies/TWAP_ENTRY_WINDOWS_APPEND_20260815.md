# Frozen TWAP entry windows with the Aug 14 append

## Verdict

- **5m:** T-160 remains the best combined-PnL window.  It added **+$10.69**
  in the new slice and now stands at **+$35.36**, versus **+$31.37** for the
  deployed T-299 window.  T-299 added only **+$1.46** and its combined maximum
  drawdown expanded to **$23.13**, versus **$12.15** for T-160.
- **15m:** the new day does not confirm a profitable broad entry window.
  T-520 lost **-$6.88**, T-600 lost **-$9.65**, and the deployed T-300 lost
  **-$7.74**.  T-90 was the only positive current candidate at **+$0.75**, but
  that is only 31 trades and was not a preselected window.  T-600 remains first
  on combined PnL because of its old lead; T-520 is second and T-300 is eighth.
- The objective conclusion is **not to retune 15m to today's T-90 winner**.
  The whole 15m timing plateau degraded together, which points to model/regime
  drift rather than a fixable entry-second choice.

## Fixed contract

- Deployed model bundle SHA-256:
  `64cc2148ce3c14faec5ff1de9c752f5bad2b2bcbda8cb31702e5962f341f7248`
- No model refit; one frozen pooled HGB per interval across BTC/ETH/SOL/XRP.
- Every candidate uses one shared entry time across all four coins.
- 60-second TWAP target; ask band `(0.55, 0.85]`; edge gate `0.10`.
- Base only, no T3; 1% sizing with the $1 minimum, so the effective stake is $1.
- Exact Polymarket/Gamma price-to-beat anchors, Binance 1-second features, and
  PMData books/outcomes.
- The old selected-window replay was required to match the release manifest's
  trades, wins, PnL, and max drawdown before the append was accepted.

The 5m old period is `2026-08-14 00:00` through `14:25 UTC`; its appended
period is `14:25` through `24:00 UTC` (22:25 on Aug 14 through 08:00 on Aug 15
MYT).  The 15m old period is Aug 7 through Aug 13 UTC; its appended period is
the full Aug 14 UTC day (08:00 Aug 14 through 08:00 Aug 15 MYT).

## 5m results

`T-x` means entries are allowed from x seconds before settlement through T-1.
Combined drawdown is recomputed from the concatenated chronological trades.

| Window | Old PnL | New n | New WR | New PnL | New EV/trade | New max DD | Combined n | Combined WR | Combined PnL | Combined max DD |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T-60 | $1.91 | 68 | 77.94% | $5.86 | $0.086 | $3.04 | 170 | 74.71% | $7.77 | $5.83 |
| T-90 | -$0.87 | 116 | 76.72% | $8.91 | $0.077 | $5.58 | 273 | 72.53% | $8.04 | $7.48 |
| T-120 | $5.63 | 164 | 75.61% | **$14.58** | $0.089 | $7.22 | 370 | 73.24% | $20.21 | $10.00 |
| T-150 | $20.84 | 190 | 72.63% | $10.95 | $0.058 | $13.09 | 443 | 73.36% | $31.79 | $13.09 |
| **T-160** | **$24.67** | **192** | **72.40%** | **$10.69** | **$0.056** | **$12.15** | **455** | **73.63%** | **$35.36** | **$12.15** |
| T-180 | $15.39 | 203 | 70.94% | $9.33 | $0.046 | $13.09 | 491 | 71.08% | $24.72 | $13.09 |
| T-210 | $16.15 | 214 | 69.63% | $8.08 | $0.038 | $14.08 | 522 | 69.92% | $24.23 | $14.08 |
| T-240 | $13.67 | 220 | 69.09% | $7.53 | $0.034 | $15.00 | 554 | 68.95% | $21.21 | $15.00 |
| T-270 | $13.49 | 223 | 69.06% | $8.20 | $0.037 | $16.04 | 563 | 68.74% | $21.69 | $16.04 |
| **T-299 deployed** | **$29.91** | **257** | **64.98%** | **$1.46** | **$0.006** | **$23.13** | **622** | **67.85%** | **$31.37** | **$23.13** |

## 15m results

| Window | Old PnL | New n | New WR | New PnL | New EV/trade | New max DD | Combined n | Combined WR | Combined PnL | Combined max DD |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T-60 | $8.63 | 16 | 62.50% | -$1.86 | -$0.116 | $3.15 | 151 | 72.19% | $6.78 | $6.94 |
| T-90 | $29.31 | 31 | 70.97% | **$0.75** | $0.024 | $3.23 | 286 | 75.17% | $30.05 | $8.94 |
| T-120 | $34.09 | 41 | 63.41% | -$2.38 | -$0.058 | $7.22 | 349 | 73.64% | $31.71 | $8.52 |
| T-150 | $40.63 | 50 | 66.00% | -$0.62 | -$0.012 | $7.38 | 405 | 73.83% | $40.01 | $7.38 |
| T-180 | $44.33 | 54 | 59.26% | -$6.15 | -$0.114 | $11.03 | 448 | 71.88% | $38.18 | $11.03 |
| T-210 | $49.48 | 58 | 56.90% | -$8.01 | -$0.138 | $12.98 | 464 | 71.98% | $41.47 | $12.98 |
| T-240 | $50.12 | 64 | 56.25% | -$9.16 | -$0.143 | $13.44 | 486 | 71.40% | $40.95 | $13.44 |
| T-270 | $51.39 | 64 | 56.25% | -$9.30 | -$0.145 | $13.29 | 488 | 71.31% | $42.10 | $13.29 |
| **T-300 deployed** | **$51.21** | **67** | **58.21%** | **-$7.74** | **-$0.116** | **$12.60** | **498** | **71.29%** | **$43.47** | **$12.60** |
| T-360 | $52.52 | 71 | 57.75% | -$8.35 | -$0.118 | $13.03 | 519 | 70.71% | $44.17 | $13.03 |
| T-420 | $50.06 | 73 | 58.90% | -$6.56 | -$0.090 | $11.55 | 529 | 70.32% | $43.51 | $11.55 |
| T-450 (7.5m) | $50.82 | 74 | 59.46% | -$5.86 | -$0.079 | $11.55 | 534 | 70.41% | $44.96 | $11.55 |
| T-480 | $53.26 | 77 | 59.74% | -$5.99 | -$0.078 | $10.71 | 543 | 70.53% | $47.28 | $10.71 |
| **T-520 (8m40s)** | **$55.28** | **79** | **59.49%** | **-$6.88** | **-$0.087** | **$11.26** | **558** | **70.61%** | **$48.40** | **$11.26** |
| T-540 | $55.84 | 80 | 58.75% | -$7.88 | -$0.099 | $12.28 | 570 | 70.35% | $47.95 | $12.28 |
| **T-600 (10m)** | **$60.51** | **82** | **57.32%** | **-$9.65** | **-$0.118** | **$14.43** | **609** | **69.95%** | **$50.85** | **$14.43** |

Machine-readable output is under the ignored research directory
`polymarket/backtest/optimize_runs/twap_entry_windows_append_20260815/`.

## T3 top-up variant

Top-ups do not change the number of initial trades or their win rate; they add
exposure to an already-open position.  `Top-up EV/trade` below is therefore PnL
per initial filled market, including every top-up leg.  At the effective $1
base size, the exposure cap allows at most one additional $1 leg per qualifying
market.

The old deployed-window T3 rows were required to reproduce the release
manifest exactly before the appended results were accepted.

### 5m with top-ups

| Window | Base PnL | Top-up PnL | Top-up EV/trade | Top-up max DD | New-period top-up PnL | Top-up legs |
|---:|---:|---:|---:|---:|---:|---:|
| T-60 | $7.77 | $16.14 | $0.095 | $5.77 | $10.13 | 64 |
| T-90 | $8.04 | $13.97 | $0.051 | $11.33 | $17.35 | 122 |
| T-120 | $20.21 | $31.78 | $0.086 | $14.99 | $28.65 | 200 |
| T-150 | $31.79 | $54.72 | $0.124 | $23.46 | $22.20 | 249 |
| **T-160** | **$35.36** | **$57.30** | **$0.126** | **$20.48** | **$20.88** | **258** |
| T-180 | $24.72 | $32.49 | $0.066 | $24.09 | $20.86 | 285 |
| T-210 | $24.23 | $30.05 | $0.058 | $24.31 | $14.75 | 320 |
| T-240 | $21.21 | $29.88 | $0.054 | $24.07 | $17.50 | 342 |
| T-270 | $21.69 | $28.35 | $0.050 | $26.13 | $17.31 | 353 |
| **T-299 deployed** | **$31.37** | **$46.37** | **$0.075** | **$40.01** | **$1.88** | **417** |

T-160 gains $21.94 of full-period PnL from top-ups, but maximum drawdown rises
from $12.15 to $20.48.  Deployed T-299 is much less convincing: the new-period
gain is only $1.88 while combined drawdown reaches $40.01, or 49.5% of the
$80.82 replay starting equity.

### 15m with top-ups

| Window | Base PnL | Top-up PnL | Top-up EV/trade | Top-up max DD | New-period top-up PnL | Top-up legs |
|---:|---:|---:|---:|---:|---:|---:|
| T-60 | $6.78 | $12.30 | $0.081 | $8.53 | -$2.13 | 43 |
| T-90 | $30.05 | $51.89 | $0.181 | $13.91 | $1.91 | 119 |
| T-120 | $31.71 | $59.38 | $0.170 | $10.27 | -$2.75 | 153 |
| T-150 | $40.01 | $74.86 | $0.185 | $9.56 | -$0.86 | 193 |
| T-180 | $38.18 | $68.97 | $0.154 | $19.51 | -$13.00 | 230 |
| T-210 | $41.47 | $70.76 | $0.152 | $26.11 | -$19.82 | 242 |
| T-240 | $40.95 | $73.94 | $0.152 | $24.80 | -$18.48 | 254 |
| T-270 | $42.10 | $71.73 | $0.147 | $23.77 | -$19.47 | 256 |
| **T-300 deployed** | **$43.47** | **$72.97** | **$0.147** | **$23.59** | **-$17.58** | **267** |
| T-360 | $44.17 | $73.86 | $0.142 | $24.46 | -$18.47 | 283 |
| T-420 | $43.51 | $69.07 | $0.131 | $22.97 | -$17.05 | 288 |
| T-450 (7.5m) | $44.96 | $74.26 | $0.139 | $22.97 | -$14.63 | 289 |
| T-480 | $47.28 | $77.70 | $0.143 | $21.56 | -$14.14 | 300 |
| **T-520 (8m40s)** | **$48.40** | **$79.26** | **$0.142** | **$23.95** | **-$18.48** | **315** |
| T-540 | $47.95 | $80.53 | $0.141 | $25.41 | -$19.94 | 329 |
| **T-600 (10m)** | **$50.85** | **$82.08** | **$0.135** | **$28.55** | **-$22.17** | **356** |

The 15m top-up headline is dominated by the old period.  On the appended day,
top-ups deepen the loss for every broad window.  They therefore magnify the
same 15m regime/model failure rather than repair it and should remain disabled.
