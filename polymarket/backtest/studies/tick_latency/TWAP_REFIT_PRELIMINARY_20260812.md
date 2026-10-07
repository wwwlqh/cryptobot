# Preliminary TWAP refit — 2026-08-12

This is a research-only refit. It does not change the live model.

## Protocol

- Data: saved Chainlink-derived `twap_labels.parquet` joined to the
  `step1last_option3` pool.
- Training: market-entry timestamps before 2026-07-01 00:00 UTC, with a
  one-hour embargo.
- Validation: 2026-07-01 through 2026-07-09 UTC. Used only for interval, ask
  band, and minimum-edge selection.
- Holdout: 2026-07-10 through 2026-07-18 UTC. Never used for selection.
- Model: fresh HistGradientBoosting classifier, trained after the label target
  was frozen. This is not yet the production checkpoint_v1 architecture.
- EV: flat-dollar EV at the stored +1s fill ask, including the current modelled
  fee (`theta=0.07`). No compounding, top-ups, or stop simulation.
- Symmetric feature: opening-TWAP-anchored partial-return z, derived from the
  existing point-open z and the saved opening TWAP.

## Results

| Target | Validation choice | Holdout n | Holdout WR | Holdout EV/trade |
|---|---|---:|---:|---:|
| `twap_c` | 15m, ask (0.65, 0.85], edge >= 0.06 | 373 | 90.88% | +0.1853 |
| `twap_s` | 5m, ask (0.60, 0.85], edge >= 0.06 | 2,004 | 92.47% | +0.2692 |

### Symmetric holdout by coin

| Coin | n | WR | EV/trade |
|---|---:|---:|---:|
| BTC | 515 | 93.01% | +0.2819 |
| ETH | 512 | 93.75% | +0.2924 |
| SOL | 500 | 91.60% | +0.2553 |
| XRP | 477 | 91.40% | +0.2450 |

## Interpretation

The symmetric target is not inherently unusable. The old 76.73% result was a
fixed-bet regrade of a model selected for a different opening reference. After
refitting the target and anchoring the partial-return feature to the opening
TWAP, this preliminary holdout is materially better. The selected symmetric
configuration is 5m-only; the validation did not select 15m.

## Limitations

The historical TWAP labels are reconstructed from the saved 1-second Chainlink
point tape and are not yet proven byte-for-byte identical to the direct
Chainlink TWAP stream. The execution test uses stored fill asks, not a fresh
post-TWAP order-book replay. This result must therefore be repeated with the
direct TWAP feed and post-switch books before deployment.
