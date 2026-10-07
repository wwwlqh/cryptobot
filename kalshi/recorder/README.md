# kalshi/recorder/ — THE FIRST DELIVERABLE

A WS orderbook + trades recorder for Kalshi. Nothing else in this venue tree can
be validated until this has soaked.

**Why it is first:** book depth cannot be bought or backfilled. Kalshi's official
API gives candles and trades — enough for market-structure EDA, never enough for
a fill model. The hard-won rule from Polymarket is that a venue is backtestable
at fill-model fidelity **only from its own recorder onward**, so every day this
is not running is a day of substrate that can never be recovered.

## Build it as

A copy-shaped sibling of the Polymarket `book_recorder.py` — retired 2026-08-02
and deleted from the repo 2026-08-04, so pull it out of git history
(`git log --diff-filter=D -- '*book_recorder*'`) to copy its shape:

- WS subscribe, then per-second snapshot rows, one parquet part per market
  window, partitioned `{series}/{tf}/` — mirror the Polymarket
  `poly_snapshot/{coin}/{interval}/` convention so the same loaders work.
- Atomic part writes, so an in-flight file is never read half-written.
- Env-gated (`CRYPTOBOT_KALSHI_RECORDER_ENABLED`), writing under
  `config.DATA_ROOT`.
- Its own systemd unit in `service/`, independent of any trading process, so it
  keeps collecting while the strategy work is still nothing but a plan.

## Two things that have bitten before

1. Recorder output is **append-only and irreplaceable**, and it lives inside the
   EC2 `data/` dir that every deploy wipes. Add its dir to the `! -name`
   whitelist in the venue's `deploy.sh` **in the same change** that creates it. A
   stale whitelist has already destroyed collected history twice.
2. Soak for at least 2-3 weeks before drawing conclusions. The Polymarket
   book-parity study needed 3-4 weeks to say anything at all.
