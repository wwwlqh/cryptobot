# predict.fun backtest — data provenance manifest

**Template.** predict.fun live is HALTED (2026-07-23) and this tree has no depth
yet. Fill each row in as the substrate actually lands; do not delete rows to
make the table look complete.

**The rule that governs all of it:** a venue is backtestable at fill-model
fidelity only from its own recorder onward. There is no pmdata.dev equivalent for
predict.fun — no third party sells its book history — so the recorder is not a
nice-to-have here, it is the *only* path to a fill model. Recorder first, always.

---

## Sources

### 1. Own PF book recorder — ⛔ RETIRED, ALL HISTORY DESTROYED

**Do not plan work against this source.** Nothing survives of it.

| | |
| --- | --- |
| Provided | JSONL order-book capture; would have been the sole substrate for any PF fill model |
| Producer | `predictfun/live/src/cryptobot_predictfun/book_recorder.py` (`predictfun/recorder/service/predictfun-book-recorder.service`). ⛔ Retired 2026-08-02; ⛔ **CODE DELETED 2026-08-04** — module, unit, tests and the `CRYPTOBOT_PREDICTFUN_RECORDER_*` env contract are out of the repo, and `install_remote.sh` tears the unit down on both roles |
| Universe | BTC / ETH / **BNB** — note: no SOL, no XRP on this venue |
| Wrote to | `config.DATA_ROOT / "predictfun_book_recorder"` on the box |
| Puller | ⛔ Never existed. `polymarket/recorder/pull/pull_book_recorder.sh` (the Polymarket model) was deleted 2026-08-03 |
| Remote history | ⛔ 3.8 GB deleted 2026-08-02 on operator order |

**The trap this source became.** Append-only and unbackfillable — and every byte
was deleted before a puller was ever written. Recover the recorder from git
history (`git log --diff-filter=D -- '*book_recorder*'`) if it is ever re-armed;
it would start a new series from zero. The `! -name predictfun_book_recorder`
exclusion in `polymarket/infra/deploy.sh` is KEPT as a fail-safe.

### 2. PF markets API — discovery and resolved outcomes

| | |
| --- | --- |
| Provides | Market discovery and resolved outcomes |
| Client | `predictfun/live/src/cryptobot_predictfun/markets.py` (first-party httpx, no SDK needed) |
| Cadence | _TODO_ |
| Trap | `/orders` returns **resting orders only**. The fill tape is `/matches`. Grading fills off `/orders` silently drops every filled order. |

### 3. Settlement — Chainlink Data Streams, NOT Binance spot

predict.fun settles off **Chainlink Data Streams**. Polymarket's label source is
not interchangeable here: reusing the Binance-spot label would grade PF markets
against the wrong oracle. Any PF label column must carry its source explicitly.

_TODO: pin the exact stream/feed id per coin during bring-up._

### 4. PF ledger snapshots — the live side of parity

| | |
| --- | --- |
| Provides | The paper (and, if re-armed, live) ledger for live-vs-backtest grading |
| Source | `data/predictfun_paper/cryptobot_predictfun.db` on the Polymarket box |
| Puller | _TODO_ |

---

## Venue-specific modelling notes

- **Fee shape is a TENT**, not Polymarket's parabola. The PF fee model belongs in
  `predictfun/backtest/lib/`. A known-real bug once had the **Polymarket** fee in
  the PF decision gate — check the fee function's venue before trusting any PF
  EV number.
- **Unit is shares-wei**, and the venue minimum is **$0.90** per order. That
  minimum is the direct cause of the live halt: below it a stop cannot be sold,
  so a PF backtest that assumes a working stop is not modelling this venue.
- The Polymarket tick engine (`polymarket/backtest/engine/tick_exit004.py`) is
  reusable by import for walker / pad / FAK-re-read mechanics — but only with
  PF's own fee function and PF's own books.

## Unlocking 02-04

`5m/notebooks/` and `15m/notebooks/` get the same four notebooks as Polymarket
(01_eda · 02_backtest · 03_calibration · 04_live_parity), but **02 onward stay
locked until the recorder has enough depth to support the 400 ms fill model.**
API-derived backtests are EDA-grade and are never go-live evidence.
