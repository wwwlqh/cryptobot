# kalshi/backtest/ — empty (steps 3-4 of the bring-up)

Read [`SOURCES.md`](SOURCES.md) before writing any loader, and `../README.md` for
the sequencing.

## Shape to grow into

| Dir | What |
| --- | --- |
| `SOURCES.md` | Provenance manifest — origin, puller, cadence, schema, traps per source |
| `data/` | Gitignored substrate. **Never commit a directory under it, not even a `.gitkeep`** — an empty tracked dir can shadow a populated one |
| `fetch/` | Official Kalshi market-data API pullers (step 3) |
| `lib/` | Kalshi fee model + grading; normalizes every source to ONE canonical frame so notebooks stay source-agnostic |
| `<tf>/notebooks/` | `01_eda` · `02_backtest` · `03_calibration` · `04_live_parity`, created **per series actually traded** — Kalshi crypto is hourly, so do not copy Polymarket's 5m/15m split reflexively |
| `studies/` | Dated verdict `.md` files |

## What unlocks what

- `01_eda` may run on API candles/trades immediately. It is **EDA-grade**: fine
  for market structure, spreads, volumes and settlement mechanics; never
  go-live evidence.
- `02`-`04` stay locked until the recorder (`../recorder/`) has enough depth for a
  real fill model — a 400 ms-style re-read against actual books, the way
  `polymarket/backtest/engine/tick_exit004.py` does it. Omitting that fill model
  once produced a fully wrong "+1.65pt edge" on Polymarket.
- The Polymarket engine is reusable **by import** for walker / pad / FAK
  mechanics. Kalshi's fee schedule is structurally different from both existing
  venues, so the fee function is new work, not a constant to copy.

Thin notebook cells with all logic imported from `lib/` (so the platform console
can run the same functions headless), memory-bounded chunked loading (a WFO build
once froze the machine), and `nbstripout` for clean diffs.
