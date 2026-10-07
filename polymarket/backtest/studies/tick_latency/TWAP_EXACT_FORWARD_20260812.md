# Exact Chainlink TWAP forward validation — 2026-08-12

## Registered design

- Labels: exact Gamma `eventMetadata.priceToBeat` / `finalPrice`, which are the
  Chainlink TWAP values used by these markets.
- Execution: PMData per-second L2 snapshots, one-cent marketable FAK pad, held
  to binary resolution.
- Features: Binance 1-second spot and boundary-frozen one-hour RV; Chainlink
  TWAP is the opening anchor (30 seconds for 5m, 60 seconds for 15m).
- Model: frozen after the native fold-F training cutoff (2026-07-21). No rows
  from this forward period were used to fit the model.
- Selection period: 2026-08-07 through 2026-08-10.
- Final untouched holdout: 2026-08-11.

## Result

The four-coin contract fails and must not ship. The initial joint candidate was
BTC+ETH, but coin attribution showed that BTC supplied all aggregate profit.
The operator-selected live contract is therefore BTC only, both 5m and 15m,
T3 enabled, Tier A disabled, at 2% shared-equity base sizing.

| Contract | Period | Fills | WR | Dollars deployed | PnL | EV / dollar |
|---|---:|---:|---:|---:|---:|---:|
| All four, base | Aug 7–10 | 537 | 73.0% | $537.00 | -$16.38 | -3.05c |
| All four, base | Aug 11 | 106 | 72.6% | $106.00 | -$3.69 | -3.48c |
| All four, T3 | Aug 7–10 | 537 | 73.0% | $635.00 | -$4.34 | -0.68c |
| All four, T3 | Aug 11 | 106 | 72.6% | $127.00 | -$3.46 | -2.73c |
| BTC+ETH, base | Aug 7–10 | 300 | 78.7% | $300.00 | +$15.31 | +5.10c |
| BTC+ETH, base | Aug 11 | 59 | 78.0% | $59.00 | +$1.86 | +3.15c |
| **BTC+ETH, T3** | **Aug 7–10** | **300** | **78.7%** | **$382.00** | **+$23.82** | **+6.24c** |
| **BTC+ETH, T3** | **Aug 11** | **59** | **78.0%** | **$73.00** | **+$2.69** | **+3.69c** |
| BTC+ETH, Tier A + T3 | Aug 7–10 | 300 | 78.7% | $575.50 | +$35.46 | +6.16c |
| BTC+ETH, Tier A + T3 | Aug 11 | 59 | 78.0% | $111.50 | +$3.79 | +3.40c |
| **BTC, T3, 2% available-cash compound** | **Aug 7–10** | **155** | **81.3%** | **$404.08** | **+$49.52** | **+12.25c** |
| **BTC, T3, 2% available-cash compound** | **Aug 11** | **27** | **81.5%** | **$92.44** | **+$4.82** | **+5.21c** |

T3 improves EV per filled market over the base contract on both periods.
Tier A increases dollars and absolute PnL but reduces EV per dollar on both
periods, so it remains disabled.

## Interval qualification

At BTC-only 2% sizing, 5m produced 167 fills, 80.2% WR, +8.90c per dollar and
+$40.04. 15m produced 15 fills, 93.3% WR, +30.80c per dollar and +$14.29. The
full 182-market replay used 291 orders including T3, earned +$54.33 on $496.52
deployed, and had $14.31 maximum settled-PnL drawdown (17.71% of the $80.82
replay equity). Aug 8 was the only negative day at -$0.79; the final untouched
Aug 11 holdout remained positive at +$4.82 and +5.21c per dollar.

## Live anchor parity

Twenty exact boundary observations captured over public RTDS from the Ireland
host matched Gamma's corresponding opening `priceToBeat` values to Gamma's
published decimal precision. The capture included all four 30-second streams
at four 5m boundaries and all four 60-second streams at a 15m boundary. Frames
arrived about one to two seconds after the boundary; serving must therefore
abstain until the exact observation arrives and retry later seconds. There is
no Binance-open fallback.

## Deployment decision

- Allow: BTC only; 5m gate 0.06 and 15m gate 0.05; trend side; no flip;
  T3 on; Tier A 1.0x.
- Reject: ETH, SOL, XRP; all-four union; Tier A multiplier; stale/nearby TWAP
  substitution; upper-edge cap; fast requote; dip gate.
- Start at 2% shared equity. At the observed $80.82 equity this is a $1.61
  requested base order before book walking and T3 additions. This
  validation does not authorize or establish fillability for a $100 order.
