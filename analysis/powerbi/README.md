# Power BI dashboard

Build steps for the trade-analysis dashboard. It uses `../data/trades.csv` (5,384 live trades) and the measures in `measures.dax`.

## 1. Load the data

1. Power BI Desktop → **Get data → Text/CSV** → `analysis/data/trades.csv` → **Transform Data**.
2. Check the column types:
   - `bet_time_utc`: Date/Time. `bet_date`: Date.
   - `model_prob`, `market_price`, `model_edge`, `fill_price`, `stake_usd`, `pnl_usd`, `cum_pnl_usd`, `drawdown_usd`: Decimal number.
   - `model_correct`, `market_went_up`, `is_top_up`, `bet_hour_utc`, `seconds_into_window`: Whole number.
3. Rename the query to `trades` → **Close & Apply**.
4. Paste each block in `measures.dax` in as a **New measure**. Format `Win Rate %`, `ROI %`, `Model Accuracy %`, `Avg Model Prob %` and `Avg Price Paid %` as percentages, and the `*PnL*`, `Avg Win`, `Avg Loss` and `Max Drawdown` measures as currency.
5. Optional: add a calculated column for the calibration chart:

   ```DAX
   Prob Bucket =
   SWITCH ( TRUE (),
       ISBLANK ( trades[model_prob] ), BLANK (),
       trades[model_prob] < 0.7, "60-70%",
       trades[model_prob] < 0.8, "70-80%",
       trades[model_prob] < 0.9, "80-90%",
       "90-100%" )
   ```

## 2. Pages

**Page 1: Overview**
- Cards: `Resolved Trades`, `Net PnL`, `Win Rate %`, `ROI %`, `Max Drawdown`.
- Line chart: x = `bet_date`, y = `Cumulative PnL`.
- Slicers (one row at the top): `coin`, `strategy`, `window`.

**Page 2: By coin**
- Clustered bar chart: y = `coin`, x = `Net PnL`. Turn on data labels.
- Table: `coin`, `Trades`, `Win Rate %`, `Model Accuracy %`, `Avg Price Paid %`, `Avg Win`, `Avg Loss`, `ROI %`.

**Page 3: Model accuracy**
- Line chart: x = `Prob Bucket`, y = `Avg Model Prob %`, `Avg Price Paid %`, `Model Accuracy %`. This page shows the main finding: the model's hit rate was close to, but below, the price it paid.
- Card: `Accuracy vs Price (pts)`.
- Matrix: rows = `strategy`, `window`; values = `Trades`, `Model Accuracy %`, `Net PnL`.

**Page 4: Timing**
- Column chart: x = `bet_hour_utc`, y = `Net PnL`.
- Column chart: x = `outcome`, y = `Net PnL` (wins vs losses vs stop-loss exits).

## 3. Reference numbers

Use these to check your visuals:

| Measure | Value |
|---|---|
| Resolved trades | 5,382 (2 still pending) |
| Net P&L | −$124.55 |
| Total staked | $8,321 |
| Win rate | 71.1% |
| ROI | −1.5% |
| Avg win / avg loss | $0.56 / −$1.46 |
| Max drawdown | −$170.24 |
| Model accuracy (prediction-model trades) | 73.3% |
| Avg price paid (same trades) | 74.9% |
