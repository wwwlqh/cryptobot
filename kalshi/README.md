# kalshi/ — GREENFIELD

Nothing is implemented. This tree is a skeleton so the Kalshi bring-up has a home
to grow into, in the order that actually works (recorder first).

**Kalshi has no venue logic in this repo today** — but as of Phase 2
(2026-07-28) it has an ADDRESS: `live/src/cryptobot_kalshi/` is a real,
importable package whose `ADAPTER` implements every `VenueAdapter` member as a
`NotImplementedError` pointing back here. It exists so the venue registry map is
**total**, and so the bring-up has a compiling target to fill in rather than a
blank page. Implementing Kalshi means replacing that stub; **nothing in `polymarket/core/`
has to change** — that is the whole point of the seam.

`kalshi` is now in the real `config.VENUE` allowlist, so pinning
`CRYPTOBOT_VENUE=kalshi` fails LOUDLY at boot instead of silently trading
Polymarket. The old fixtures that used `"kalshi"` as an *unrecognized* venue
sentinel were rewritten accordingly: they still assert the ROW-level fee
fallback (a historic ledger row saying `kalshi` must still settle under the
Polymarket parabola, matching the column's `NOT NULL DEFAULT`), while new tests
in `polymarket/src/cryptobot/tests/test_venue_fees.py` cover BOOT-level strictness
using a genuinely unregistered string.

⚠️ **`hightempbot` is NOT a Kalshi bot.** It is a separate, actively developed
Polymarket **weather** bot with its own repo, deploy and dashboard on Oracle
Cloud. Do not fold it in here or anywhere else.

## Bring-up order (do not reorder — see plan §9)

1. **Eligibility and access probe** (cheapest, first): account/KYC status, API
   keys, rate limits, and — decisively — whether order placement from an AWS IP
   is accepted. **Place ONE REAL order from each candidate region before
   choosing a box.** Passing reads prove nothing; London was geoblocked for
   Polymarket ORDERS while every read succeeded.
2. **Recorder** (`recorder/`): a WS orderbook + trades recorder modelled on the
   Polymarket `book_recorder.py` — RETIRED 2026-08-02 and DELETED from the repo
   2026-08-04, so read it out of git history
   (`git log --diff-filter=D -- '*book_recorder*'`). Deploy it and let it soak
   ≥2-3 weeks. Book depth is **not available retroactively**, which is the entire
   reason this comes before any modelling.
3. **Bootstrap EDA** (`backtest/<tf>/notebooks/01_eda`) on the official API's
   candles/trades: market structure, spreads, volumes, settlement mechanics.
   Pick the target series — crypto hourlies are the obvious analog to the current
   15m sleeve.
4. **Backtest** — only once recorder depth supports a real fill model. Kalshi's
   fee schedule differs structurally from both Polymarket's parabola and
   predict.fun's tent, so its fee function is new work in `backtest/lib/`.
5. **Adapter** (`live/src/cryptobot_kalshi/`) against the core `VenueAdapter`
   protocol; DRY/paper on the box first.
6. **Live gate**: a pre-registered forward rule, same discipline as the
   Polymarket forward-OOS bed. No live money before it passes.

## Layout

| Dir | What belongs here |
| --- | --- |
| `live/src/` | `cryptobot_kalshi` adapter package (step 5) |
| `recorder/` | WS orderbook + trades recorder and its systemd unit — **the first deliverable** |
| `backtest/` | [`SOURCES.md`](backtest/SOURCES.md), then `<tf>/notebooks/` per series actually traded |
| `infra/` | `deploy.sh` / `install_remote.sh` clones and box facts, once a region is chosen |

## Box

**Region TBD.** Candidates `us-east-1` / `us-east-2`. Verify account
eligibility, API rate limits, and cloud-IP order acceptance before committing.
A recorder-only first phase can run on a `t4g.small`; the engine box needs ~4 GB
(the Polymarket unit sets `MemoryMax=3300M`).

Timeframe folders are created **per series actually traded** — Kalshi's crypto
markets are hourly, so do not pre-create `5m/` and `15m/` here just because
Polymarket has them.
