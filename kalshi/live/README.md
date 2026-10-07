# kalshi/live/ — empty (step 5 of the bring-up)

The Kalshi venue adapter goes in `src/cryptobot_kalshi/`: an implementation of
the core `VenueAdapter` protocol (`cryptobot.venues`, introduced in Phase 2 of
the restructure), registering itself against `CRYPTOBOT_VENUE=kalshi`.

Surface to implement — the same one the shared engine already dispatches on for
Polymarket and predict.fun:

`fetch_active_market` · `fetch_top_book` · `fetch_books_batch` ·
`fetch_resolved_outcome` · `market_cache_has` · `build_order_client` ·
`build_ws_mirror` · `build_balance_fetcher` · `fee_per_share`

Nine members, not ten: `spot_fallback` appears in the Phase-2 plan
(`docs/plans/RESTRUCTURE_PLAN_20260728.md:151`) but was **never added** to the
protocol — `polymarket/src/cryptobot/venues/base.py` has no such member, and spot is
venue-agnostic (Binance, via `cryptobot.data.binance`). Do not implement it.

Alongside it: `config/` for the env contract, `service/` for the systemd units,
`runbooks/` for the arming checklist.

**Do not start here.** Live is step 5 of 6 (see `../README.md`); the recorder is
step 2 and gates everything downstream. Building the adapter before there is a
validated fill model repeats the predict.fun sequencing mistake — a venue armed
before its exit mechanics were understood, then halted after 7 bets.
