# Full-history TWAP refit — corrected 2026-08-12

> **Corrections:** the first run below graded both Up and Down candidate rows
> with the market-level Up label. A later audit also found that boundary RV
> included the close of the minute beginning at that boundary. The corrected
> evaluator converts labels to proposed-side wins and shifts minute RV once so
> every feature is observable live. Regression tests pin both fixes.

Corrected leak-free results for the selected symmetric sleeves
(`0.60 < ask <= 0.85`) are:

| Sleeve | B EV (n) | C EV (n) | D EV (n) | WFO EV (n) | F WR | F EV (n) |
|---|---:|---:|---:|---:|---:|---:|
| 5m, gate 0.06 | +0.3221 (1,258) | +0.2501 (3,472) | +0.2343 (3,362) | +0.2548 (8,092) | 89.63% | +0.2307 (3,220) |
| 15m, gate 0.05 | +0.2640 (183) | +0.2556 (525) | +0.2643 (517) | +0.2606 (1,225) | 89.11% | +0.2037 (450) |
| 5m + 15m, common gate 0.06 | +0.3145 (1,434) | +0.2508 (3,970) | +0.2393 (3,861) | +0.2559 (9,265) | 89.52% | +0.2272 (3,655) |

The preregistered global rule selects 15m on weakest-fold EV. The 5m sleeve is
also positive in every development fold and untouched F; the operator selected
deployment of both independently valid sleeves. Under the common-gate union,
untouched-F BTC was +0.2889 EV over 929 fills. All 90 registered candidates
passed the per-fold size and positive-EV gate after the causal correction.

The remainder of this file is retained only as the invalid first-run audit
trail and must not be cited as a result.

---

This supersedes `TWAP_REFIT_PRELIMINARY_20260812.md`. It is research-only and
does not change the live model or configuration.

## Data boundary

The saved TWAP-label parquet has labels through 2026-08-01, but the usable
order-book/pool rows end at 2026-07-21 06:59:59 UTC. August labels without
matching book rows cannot produce an executable fill, so they were not counted
as trades. Native pool fold F (2026-07-06 through 2026-07-21) is the untouched
forward holdout.

## Protocol

- Development data: every labeled, tradeable row in native pool folds C/D/E.
- Development WFO: four chronological event-time chunks A/B/C/D; B, C, and D
  are test chunks with expanding training windows.
- Embargo: one hour before each test chunk.
- Validity gate: every B/C/D chunk must have at least 100 selected bets and
  positive fee-adjusted EV.
- Selection: maximize the minimum B/C/D fold EV, then pooled WFO EV.
- Final holdout: native fold F, never used for interval, price band, or edge
  selection.
- Grid: close-only and symmetric targets × 5m, 15m, or both × three ask bands
  × five edge gates = 90 candidates. Sixty passed all B/C/D validity gates.
- EV: flat $1 stake at the stored +1s fill ask, including `theta=0.07`; no
  compounding, top-ups, or stop simulation.

## Best candidate

The winner is the symmetric TWAP target:

- target: `up_twap_s`
- opening feature: opening-TWAP-anchored partial-return z
- intervals: 5m + 15m
- ask band: `0.60 < ask <= 0.85`
- model edge gate: `>= 0.06`

| Chunk | Bets | Win rate | EV/trade |
|---|---:|---:|---:|
| B | 746 | 93.57% | +0.3488 |
| C | 1,934 | 90.54% | +0.2568 |
| D | 2,085 | 92.42% | +0.2908 |
| **WFO total** | **4,765** | — | **+0.2861** |
| **F forward holdout** | **1,990** | **90.35%** | **+0.2946** |

The forward holdout is positive and passes the same minimum-100-bet validity
gate. By coin in F:

| Coin | Bets | Win rate | EV/trade |
|---|---:|---:|---:|
| BTC | 557 | 91.92% | +0.2819 |
| ETH | 499 | 91.58% | +0.2544 |
| SOL | 471 | 89.81% | +0.3170 |
| XRP | 463 | 87.69% | +0.3304 |

## Same-protocol close-only comparison

The best valid `twap_c` candidate was also both-interval, with
`0.60 < ask <= 0.85` and edge `>= 0.04`:

- WFO bets: 4,622
- every B/C/D chunk valid
- pooled WFO EV/trade: **+0.2138**
- weakest WFO chunk EV/trade: **+0.2006**

Therefore the symmetric refit remains ahead under the full-history WFO:
`+0.2861` versus `+0.2138` EV/trade. This is a same-protocol comparison;
the earlier 76.73% symmetric number was a fixed-bet regrade of a model trained
for the point-open target.

## Caveats

The labels are reconstructed from the saved 1-second Chainlink point tape and
are not yet proven byte-for-byte identical to the direct Chainlink TWAP stream.
The order-book side uses stored historical +1s fills, not a fresh live-TWAP
post-open replay. The result is a strong research candidate, not a deployment
approval. Extending the pool with August book rows and repeating F as a later
forward holdout is required before changing production.
