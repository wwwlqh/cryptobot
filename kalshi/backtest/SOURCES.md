# Kalshi backtest — data provenance manifest

**Template — nothing is collected yet.** Fill each row in during bring-up; do not
delete rows to make the table look complete.

**The rule that governs all of it:** a venue is backtestable at fill-model
fidelity only from its own recorder onward. API candles are EDA-grade and are
never go-live evidence. That is why the recorder is step 2 of the bring-up and
everything downstream waits on it.

---

## Sources

### 1. Official Kalshi REST market-data API — bootstrap history

| | |
| --- | --- |
| Provides | Series/market metadata, trades, candlesticks — enough for EDA and coarse structure work |
| Puller | _TODO: new `kalshi/backtest/fetch/`_ |
| Lands in | _TODO: `kalshi/backtest/data/api/` (gitignored; never commit a dir under it)_ |
| Cadence | _TODO_ |
| Auth / limits | _TODO: key type, rate limits, and whether cloud IPs are accepted_ |
| Schema | _TODO: record the exact columns and units — Kalshi prices are cents_ |

**Trap.** Candles are aggregates. They cannot tell you whether an order would
have filled, so any P&L derived from them is an upper bound — the same mistake
that made early Polymarket ladder numbers meaningless until real books arrived.

### 2. Own Kalshi recorder (WS orderbook + trades) — fill-model truth

| | |
| --- | --- |
| Provides | Book depth at decision instants — the only basis for a fill model |
| Producer | _TODO: `kalshi/recorder/` — **the first deliverable**_ |
| Lands in | _TODO: `kalshi/backtest/data/recorder_snapshot/{series}/{tf}/`_ |
| Depth so far | _TODO: record first/last capture timestamps; 2-3 weeks minimum before any verdict_ |

**Trap.** Not available retroactively, append-only, and it sits in the box's
`data/` dir that every deploy wipes — its dir must be in the `! -name` whitelist
from day one.

### 3. Settlement source

_TODO: per-series. Confirm the exact index/oracle per market during bring-up._

Do not assume Binance spot (Polymarket) or Chainlink Data Streams
(predict.fun). A label joined from the wrong oracle grades a real strategy
against a fiction.

---

## Venue-specific modelling notes

- **The fee schedule is structurally different** from Polymarket's parabola and
  predict.fun's tent. Implement it in `lib/` and unit-test it against published
  examples before it feeds any EV number.
- Prices are quoted in **cents**; the engine's price primitives assume
  probability units in [0, 1]. Normalize at the loader boundary, once.
- Crypto series are **hourly** — create timeframe folders per series actually
  traded rather than mirroring Polymarket's 5m/15m split.
- Contract mechanics (settlement time, early close, market-maker rebates) need
  writing down here explicitly; they are what the fill model has to respect.
