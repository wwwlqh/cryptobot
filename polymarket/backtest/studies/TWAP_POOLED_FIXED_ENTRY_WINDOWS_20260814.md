# Pooled fixed-HGB entry-window study — 2026-08-14

## Decision

The corrected production structure uses exactly two models:

- one 5m HGB pooled across BTC, ETH, SOL, and XRP;
- one 15m HGB pooled across the same four coins.

`coin_id` is categorical. Within each interval, all four coins share the same
model, `0.55 < ask <= 0.85` band, fee-adjusted edge gate `>= 0.10`, and entry
cutoff. The HGB and gate were frozen before comparing timing windows.

Selected entry windows:

- 5m: T-299 through T-1 (`1:299:1` offsets);
- 15m: T-300 through T-1 (`600:899:1` offsets).

## Method

Training used 2026-06-07 through 2026-07-21 07:00 UTC, with a deterministic
five-second stride to reduce duplicated adjacent-second observations and equal
total sample weight per coin. The HGB settings were fixed at 100 iterations,
learning rate 0.04, 31 leaves, L2 0.02, and seed 42. Exact forward execution
replay remained one-second resolution and used PMData L2 books.

The 5m cutoff was selected on 2026-08-14 00:00–10:05 UTC, frozen, then replayed
unchanged on newly downloaded 10:05–14:25 UTC data. The 15m cutoff used the
2026-08-07 through 2026-08-13 exact-forward replay. No timing candidate caused
a model refit or gate reselection.

## Selected results

All PnL, EV/trade, and drawdown figures below are USD for the study's unit stake
simulation. “T3” keeps the current scale-in policy; its EV/trade denominator is
the number of initial filled markets.

| Interval / sample | Mode | Fills | WR | PnL | EV/trade | Max DD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 5m selection, T-299 | Base | 250 | 68.40% | 23.08 | 0.0923 | 16.42 |
| 5m selection, T-299 | T3 | 250 | 68.40% | 40.64 | 0.1626 | 23.08 |
| 5m untouched, T-299 | Base | 115 | 73.04% | 25.00 | 0.2174 | 12.97 |
| 5m untouched, T-299 | T3 | 115 | 73.04% | 33.95 | 0.2952 | 28.83 |
| 5m combined, T-299 | Base | 365 | 69.86% | 48.08 | 0.1317 | 16.42 |
| 5m combined, T-299 | T3 | 365 | 69.86% | 74.59 | 0.2044 | 28.83 |
| 15m Aug-7/13, T-300 | Base | 431 | 73.32% | 82.06 | 0.1904 | 11.97 |
| 15m Aug-7/13, T-300 | T3 | 431 | 73.32% | 159.06 | 0.3691 | 17.68 |

The 5m combined base attribution was positive for every coin:

| Coin | Fills | WR | PnL | EV/trade | Max DD |
| --- | ---: | ---: | ---: | ---: | ---: |
| BTC | 101 | 75.25% | 29.47 | 0.2918 | 4.31 |
| ETH | 85 | 67.06% | 5.11 | 0.0601 | 7.39 |
| SOL | 95 | 69.47% | 13.37 | 0.1407 | 11.03 |
| XRP | 84 | 66.67% | 0.13 | 0.0016 | 13.17 |

The 15m T-300 base attribution was also positive for all four coins: BTC
+22.45, ETH +16.67, SOL +21.09, and XRP +21.85. Each of the seven validation
days was positive, ranging from +1.03 to +23.73.

## Why these cutoffs

T-299 was a predeclared 5m full-window candidate and remained strongest on the
untouched continuation. Earlier local peaks around T-150/T-160 left SOL and XRP
negative on the combined sample, while T-299 made all four attributions
positive. XRP is nevertheless close to flat, so this is evidence for pooled
regularization—not proof of a durable XRP edge.

For 15m, T-300 is a round, conservative point in the stable T-240–T-600 region.
It had the best base EV/trade on the rounded grid and lower drawdown than T-520
or T-600. Choosing the plateau center avoids treating the absolute PnL maximum
as a precisely estimated second.

## Reproduction

- `tick_latency/run_twap_pooled_fixed_entry_windows.py` fits the two pooled
  models once and performs the common-cutoff comparison.
- `tick_latency/run_twap5m_pooled_untouched_validation.py` loads the frozen 5m
  model and performs the untouched replay without fitting.
- `training/export_twap_symmetric_60s.py` exports the approved two-model live
  artifact and manifest.

These are short forward samples, especially 5m. The live 40% shared-equity
drawdown halt, one-base-fill rule, 1% sizing, and 6% depth-consumption limit
remain essential risk controls. Future re-grades should preserve the same
model/gate freeze and evaluate a predeclared cutoff rather than reselecting the
best second every time.

## Ask ceiling and 1% sizing follow-up

The fixed deployed models were replayed again with `ask_hi` 0.85 versus 0.95
and base sizing 2% versus 1%. Raising the ceiling produced exactly zero changes
to fills, WR, PnL, EV, or drawdown in either interval. With the 0.10
fee-adjusted edge gate, no 0.85–0.95 candidate qualified. The live ceiling
therefore remains 0.85 as a useful guardrail.

At the requested 1% base size and unchanged 0.85 ceiling:

| Interval | Mode | Fills | WR | PnL | EV/trade | Max DD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 5m combined | Base | 365 | 69.86% | 29.91 | 0.0819 | 10.20 |
| 5m combined | T3 | 365 | 69.86% | 44.49 | 0.1219 | 15.29 |
| 15m Aug-7/13 | Base | 431 | 73.32% | 51.21 | 0.1188 | 7.44 |
| 15m Aug-7/13 | T3 | 431 | 73.32% | 90.54 | 0.2101 | 10.06 |

PnL and drawdown do not halve exactly because entries are depth-filled and the
live T3 path retains a $1 minimum add-on. At 1% of a $100 bankroll, both the
base entry and each eligible top-up are $1; the exposure cap reduces T3 legs
from 455 to 238 on 5m and from 424 to 222 on 15m.

## Live T3 disable — 2026-08-15

The first 26 settled live base trades after the pooled release were 17W/9L and
lost $2.01. Their 27 T3 legs lost another $4.19, producing total PnL of -$6.20.
By sleeve, 5m was nearly flat at -$0.21 while two 15m trades both lost and
accounted for -$5.99. This is too small a sample to reject the base models, but
T3 caused roughly two-thirds of the observed loss and is disabled pending a
larger, separately graded live sample.

The same audit exposed a deployment mismatch: the tracked strategy was 1%, but
the installer still pinned both environment overrides to 2%. The live host was
corrected immediately to `0.010` for both intervals, and the installer now
asserts those values plus `CRYPTOBOT_SCALEIN_T3_ENABLED=0` before every restart.
