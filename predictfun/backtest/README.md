# predictfun/backtest/ — empty (no depth yet)

Read [`SOURCES.md`](SOURCES.md) first. predict.fun live is HALTED since
2026-07-23 and there is no third-party book history for this venue, so the only
substrate is the venue's own recorder.

| Dir | What |
| --- | --- |
| `SOURCES.md` | Provenance manifest — PF recorder JSONL, PF markets API, Chainlink Data Streams settlement |
| `data/` | Not created yet. Gitignored substrate when it is. **Never commit a directory under it, not even a `.gitkeep`** |
| `lib/` | **EMPTY today** — reserved for the PF fee model (**tent**, not Polymarket's parabola) + grading, reusing the Polymarket tick engine by import. The live tent-fee implementation currently lives at `predictfun/live/src/cryptobot_predictfun/fees.py`; nothing backtest-side exists |
| `studies/` | **EMPTY today** — reserved for dated verdicts (PF latency, fee calibration, the port plan) |

There are no PF notebooks: `5m/notebooks/` and `15m/notebooks/` exist only as
`.gitkeep` placeholders (no notebook was ever written). The dated PF study
material referenced by earlier drafts
(`PF_LATENCY_20260723.md`, `FEE_CALIBRATION_20260723.md`,
`PREDICTFUN_PORT_PLAN_20260721.md`) is **no longer in the tree** — the
`polymarket/backtest/studies/` dir now holds only `tick_latency/`. Recover
those documents from git history if PF work re-opens.

## The three traps specific to this venue

1. **Wrong fee in the gate.** A real bug once put the *Polymarket* fee inside the
   PF decision gate. Always check which venue's fee function is in play before
   trusting a PF EV number.
2. **Wrong oracle.** PF settles off Chainlink Data Streams, not Binance spot. A
   Polymarket-style label grades PF markets against the wrong truth.
3. **`/orders` is resting-only.** The fill tape is `/matches`. Grading fills off
   `/orders` silently drops every filled order.

And the structural one: the venue minimum is **$0.90**, which is why stops are
dead here — below it a losing position cannot be sold. A PF backtest that assumes
a working stop is not modelling this venue.
