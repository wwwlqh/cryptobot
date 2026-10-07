# 15m TWAP entry-window comparison — 2026-08-14

## Verdict

**The best tested common start time is T-520 seconds: begin the entry window
with 8 minutes 40 seconds remaining and use that same horizon for BTC, ETH,
SOL, and XRP.**

No per-coin timing was selected. Per-coin HGB models and development-selected
ask/edge specs remain part of the canonical design, but all four coins share
one entry horizon.

T-520 leads both the base and T3 aggregate forward PnL, is positive on every
coin and every forward day, and keeps drawdown close to the best observed.
T-510 has slightly higher base win rate and EV/trade, but T-520 earns $8.88 more
base PnL and $32.37 more T3 PnL with nearly identical base drawdown and lower
T3 drawdown.

This is a research result, not a live admission. Twenty-three common horizons
were inspected on Aug 7-13, so choosing T-520 makes that period selection data.
It must survive the next untouched exact holdout before deployment.

## Protocol

- Target: 60-second Chainlink TWAP at both 15m boundaries.
- One common entry horizon across all four coins.
- Coarse grid: T-60 through T-600 in 30-second steps.
- Fine grid around the strongest region: T-490, T-500, T-510, T-520, T-530.
- Each horizon receives a separate leak-free per-coin HGB refit and
  development-only ask-band / edge-gate WFO selection.
- WFO uses expanding chronological folds and a one-hour embargo.
- Exact forward period: Aug 7-13 Gamma/PMData replay, 400 ms latency,
  edge-preserving FAK fills, and fees.
- The T-600 cache's T-300 tail matched the validated T-300 cache; its T-60 tail
  also matched the canonical T-60 cache.
- No live artifacts or configuration were changed.

## Base results - one common horizon

| Start | Fills | WR | PnL | EV / filled trade | Max DD |
| --- | ---: | ---: | ---: | ---: | ---: |
| T-60 | 186 | 82.26% | $37.82 | $0.203 | $8.00 |
| T-90 | 340 | 79.41% | $65.94 | $0.194 | $15.34 |
| T-120 | 394 | 80.46% | $68.63 | $0.174 | $6.60 |
| T-150 | 422 | 77.96% | $57.28 | $0.136 | $7.77 |
| T-180 | 453 | 77.92% | $70.49 | $0.156 | $8.67 |
| T-210 | 431 | 78.65% | $65.84 | $0.153 | $12.11 |
| T-240 | 470 | 78.09% | $76.26 | $0.162 | $10.93 |
| T-270 | 469 | 79.74% | $90.38 | $0.193 | $8.48 |
| T-300 | 504 | 76.79% | $92.20 | $0.183 | $13.73 |
| T-330 | 513 | 76.02% | $86.19 | $0.168 | $11.57 |
| T-360 | 520 | 78.65% | $98.86 | $0.190 | $8.06 |
| T-390 | 477 | 77.57% | $81.97 | $0.172 | $12.82 |
| T-420 | 506 | 75.30% | $90.71 | $0.179 | $7.48 |
| T-450 | 446 | 78.70% | $88.14 | $0.198 | **$5.89** |
| T-480 | 495 | 76.36% | $96.12 | $0.194 | $9.29 |
| T-490 | 469 | 76.76% | $89.38 | $0.191 | $7.15 |
| T-500 | 483 | 75.57% | $93.65 | $0.194 | $7.41 |
| T-510 | 446 | **78.25%** | $98.59 | **$0.221** | $5.95 |
| **T-520** | **501** | **75.65%** | **$107.47** | **$0.215** | **$6.08** |
| T-530 | 455 | 76.04% | $86.65 | $0.190 | $8.22 |
| T-540 | 461 | 75.70% | $83.12 | $0.180 | $8.27 |
| T-570 | 487 | 75.98% | $99.89 | $0.205 | $10.17 |
| T-600 | 452 | 76.33% | $94.54 | $0.209 | $10.33 |

T-520 is the highest-PnL common horizon. T-510 is the efficiency alternative;
T-450 is the minimum-drawdown alternative. The sharp deterioration at T-530
is evidence against claiming an exact smooth optimum from this one holdout.

## T3 comparison around the strongest region

| Start | Initial fills | T3 PnL | T3 EV / filled trade | T3 max DD |
| --- | ---: | ---: | ---: | ---: |
| T-450 | 446 | $153.89 | $0.345 | $10.36 |
| T-480 | 495 | $188.64 | $0.381 | $15.25 |
| T-490 | 469 | $181.01 | $0.386 | **$9.77** |
| T-500 | 483 | $199.13 | $0.412 | $10.01 |
| T-510 | 446 | $185.34 | $0.416 | $11.58 |
| **T-520** | **501** | **$217.72** | **$0.435** | **$10.02** |
| T-530 | 455 | $171.93 | $0.378 | $10.72 |
| T-540 | 461 | $169.86 | $0.368 | $10.39 |
| T-570 | 487 | $205.23 | $0.421 | $14.01 |
| T-600 | 452 | $192.02 | $0.425 | $14.05 |

## T-520 forward breakdown

| Coin | Fills | WR | PnL | EV / trade | Max DD |
| --- | ---: | ---: | ---: | ---: | ---: |
| BTC | 134 | 74.63% | $31.99 | $0.239 | $5.68 |
| ETH | 100 | 77.00% | $22.30 | $0.223 | $5.05 |
| SOL | 185 | 73.51% | $38.14 | $0.206 | $8.11 |
| XRP | 82 | 80.49% | $15.03 | $0.183 | $5.62 |

All seven forward days were profitable at T-520. Daily base PnL ranged from
$6.53 to $24.67.

## Recommendation

1. Freeze one shared T-520 horizon for all four coins; do not choose separate
   per-coin timing.
2. Keep T-510 as the conservative challenger because it has the best base
   EV/trade and almost the same minimum drawdown.
3. Re-grade the frozen T-510 and T-520 definitions on the next untouched exact
   period. Promote T-520 only if its aggregate advantage persists.

Artifacts: `polymarket/backtest/optimize_runs/twap15m_entry_windows_20260814/`.
Runner: `polymarket/backtest/studies/tick_latency/run_twap15m_entry_window_compare.py`.
