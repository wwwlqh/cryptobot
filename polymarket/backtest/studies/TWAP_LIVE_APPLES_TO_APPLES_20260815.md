# TWAP-60 live apples-to-apples audit — 2026-08-15

## Verdict

The live loss is real and is reproduced by PMData. It is not explained by
top-ups, a different model artifact, settlement disagreement, or a broad live
fill-price penalty. The frozen model became badly overconfident in the new
forward regime and concentrated 148 of 164 trades on YES across correlated
coins. Keep the strategy halted pending a genuinely forward re-grade and a
cross-coin/resolution concentration redesign.

## Frozen comparison contract

- Period: 2026-08-14 16:26:15 UTC through 22:30:00 UTC.
- Frozen deployed HGB SHA-256:
  `64cc2148ce3c14faec5ff1de9c752f5bad2b2bcbda8cb31702e5962f341f7248`.
- The artifact hash equals the release hash; model refits: zero.
- BTC, ETH, SOL, XRP; pooled model per interval; common timing per coin.
- 5m T-299..T-1; 15m T-300..T-1; edge gate 0.10; ask 0.55..0.85.
- One base fill per market, 1% configured fraction, $1 venue minimum, T3 off.
- PMData download: 840 new books, 680 existing, zero errors/throttles.
- Binance: 86,400 causal one-second rows per coin for Aug 14 UTC.
- Production audit: 228,849 signal rows, 164 `WOULD_BET` rows, 164 ledger
  rows, one config hash, and zero T3 ledger rows.

## Headline comparison

| Sample | Trades | WR | PnL | EV/trade | Max DD |
|---|---:|---:|---:|---:|---:|
| Prior 5m PMData backtest | 365 | 69.86% | +$29.91 | +$0.0819 | $10.20 |
| Prior 15m PMData backtest | 431 | 73.32% | +$51.21 | +$0.1188 | $7.44 |
| New live actual, combined | 164 | 59.15% | -$15.15 | -$0.0924 | $25.11 |
| Same live decisions replayed on PMData | 155 resolved | 58.71% | -$15.51 | -$0.1001 | $25.01 |

PMData filled 158 of the 164 exact live decisions (96.34%). Of those, 155
had a PMData-derived resolved outcome and all 155 agreed with the live ledger.
Median PMData-minus-live signal ask was $0.00 and median
PMData-minus-live fill VWAP was $0.00. The 95th-percentile absolute difference
for each was $0.11, but aggregate PMData PnL remained within $0.36 of live.

## Where the loss came from

| Slice | Trades | WR | PnL |
|---|---:|---:|---:|
| 5m | 150 | 60.67% | -$10.24 |
| 15m | 14 | 42.86% | -$4.91 |
| YES | 148 | 56.76% | -$18.97 |
| NO | 16 | 81.25% | +$3.83 |
| 21:00 UTC hour | 36 | 36.11% | -$16.83 |
| 22:00 UTC partial hour | 13 | 38.46% | -$5.49 |

The direction mix reversed sharply versus the prior exact replay. Among prior
trades whose PMData outcome was available, the backtest had 531 NO trades and
228 YES trades. Live had 16 NO and 148 YES. This is a large covariate/regime
shift, not a small execution discrepancy.

The model's mean probability on live trades was 78.03%, while only 59.15% won:
a -18.88 percentage-point calibration gap (Brier score 0.2651). The bottom two
probability bands were particularly broken:

| Model probability band | Trades | Mean prediction | Live WR |
|---|---:|---:|---:|
| 0.65–0.70 | 32 | 68.77% | 43.75% |
| 0.70–0.75 | 36 | 71.83% | 47.22% |

Under the prior interval WRs, the expected wins for this exact 5m/15m mix were
115.06 versus 97 observed. An independent-trade Poisson-binomial calculation
puts 97 or fewer at 0.173%. That number is optimistic because the 164 bets
represent only 60 resolution clusters, average 2.73 trades per cluster, and as
many as seven trades resolving together. The worst one-resolution cluster lost
$5.57. The correlation is itself part of the risk failure.

## Why the old backtest looked better

1. The new loss window is genuinely after the evaluated data. The 5m release
   was evaluated only through 14:25 UTC on Aug 14; this audit starts at 16:26.
   The 15m headline sample ended at 00:00 UTC on Aug 14.
2. Freezing the HGB did not freeze every strategy choice. Entry-window length
   was selected from a grid on the recent PMData period. The 5m post-selection
   untouched period was only 10:05–14:25 UTC; 15m had no later untouched day
   after the timing comparison. That made the headline optimistic.
3. The pooled trend-following model treated correlated BTC/ETH/SOL/XRP bets as
   separate opportunities. In the new period it stacked almost entirely into
   YES and suffered one cross-coin reversal as many nominal trades.
4. The configured 1% fraction did not remain 1% after equity fell below $100:
   Polymarket's $1 minimum made the ending effective stake 1.71% of $58.58.

## Artifacts and reproducibility

- Runner:
  `polymarket/backtest/studies/tick_latency/run_twap_live_apples_20260815.py`
- Report (gitignored research output):
  `polymarket/backtest/optimize_runs/twap_live_apples_20260815/report.json`
- Per-trade comparison (gitignored research output):
  `polymarket/backtest/optimize_runs/twap_live_apples_20260815/replay_rows.parquet`

No model was refit and no live control was changed during this audit.
