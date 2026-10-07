# Operator dashboard

Sidebar, multi-page operator console for the cryptobot Polymarket 5m/15m
up/down trading bot (BTC/ETH/SOL/XRP, all at full size since the 2026-07-21
operator override). Dark **"night desk"** terminal theme
(Inter UI + JetBrains Mono tabular numerics + Space Grotesk display numerals;
the token source of truth is `styles.css`), wired to the live SSE payload from
`polymarket/src/cryptobot/observability/snapshot_builder.py`.

Seven tabs (sidebar order):

- **Trading:** Overview (hero band with the gross → fees → rebates → net
  decomposition strip + stats rail, plus the per-coin and
  settled-trades-by-slice tables), Operator (processing state, readiness, live
  funder wallet), Subminute console (last-30s per-second entries,
  equity-fraction sizing, DD-brake, resolution rollup, reject/fill telemetry) —
  Subminute sits directly above the Trade journal.
- **Analytics:** Trade journal (one expandable row per TRADE), Calendar (daily
  realized P&L heatmap), Latency (per-phase timing vs the ~300ms decision→fill
  floor).
- **Config:** Strategy — the full deployed strategy contract as a per-band
  reference document (ask bands · shared decision spec · sizing · execution),
  every value read live from the payload; the band columns are data-driven from
  `spec.bands` (a single band since the 2026-07-21 amendment). The 2026-08-05
  sweep removed stop-loss from the trading SURFACES *and* from the engine, so
  this reference no longer documents a stop contract — the execution section
  states that every filled position holds to binary settlement.

## Venue identity (single-venue deployment)

Current contract: each dashboard instance renders only the venue served by its
process (`:8800` Polymarket or `:8801` predict.fun). The topbar shows a passive
venue badge; there is no venue switcher or cross-venue payload merge. Venue
labels, coin universe, fee description, wallet vocabulary, and latency floor
come from the live payload. The retired static venue dumps and recorder panels
are not part of this dashboard.

Historical venue migration details live in the deployment/backtest records;
this README documents only the current single-venue dashboard contract.

## Counting contract: a TRADE is a CLOB position

The ledger writes **one row per fill** — a base bet plus every top-up it took
in the same market window is several rows. On the venue that is **one
position**: one token balance that settles once. Since 2026-08-05 every count
the dashboard labels a trade / bet / win-rate denominator counts **groups**,
not rows, keyed on the position's identity:

```text
venue · coin · interval_min · window_start_ts · side · wallet
```

`side` is in the key because a YES and a NO position in the same window are two
different CLOB balances; `is_topup` is deliberately NOT (a top-up's whole point
is that it lands in its base bet's group — it only LABELS the fills inside
one). Dollar sums are unaffected: Σ over groups of Σ over fills == Σ over
fills. The rule is emitted as `payload.contracts.trade_group`, implemented
server-side by `_TRADE_GROUP_KEY_SQL` / `_trade_groups` and client-side by
`groupTrades` in `data.jsx`; the Journal renders one row per group and expands
into the individual fills (base / top-up 1 / top-up 2 …).

Win rate is `wins / (wins + losses)` over groups. A HISTORICAL booked stop-loss exit is a
realized loss and counts as a plain **L** — the separate "SL" class was removed
from the dashboard UI on 2026-08-05 (payload counts included). Voided cancels
(`outcome='CANCELLED'` with no `exit_kind`) stay out of both terms.

Lifetime analytics are **server-side**: the payload carries `ledger_rollup`
(headline win-rate / P&L / sample size over the full active-strategy ledger),
`ledger_equity` (downsampled lifetime equity series), `ledger_daily`
(all-time per-day P&L for the calendar), and `ledger_analytics` (the historical
panels — summary · per-coin · per-slice · P&L histogram · streak tape — over the
full ledger, pre-sliced by the Topbar range toggle into `All` / `30d` / `7d`,
because those are counts the client cannot slice itself). Only the browsable
journal/resolution TABLES are client-side, bounded by how many rows the SSE
stream has delivered (bootstrap + session deltas retained by `live-bridge.js`;
each surface labels its row window).

## Run

```bash
.venv/Scripts/python.exe polymarket/dashboard/build_snapshot.py   # write static portfolio + live-state fallback to snapshot.json
```

The dashboard is served by the **scanner process itself** — `main.py` starts
`cryptobot.observability.sse_server` in-process, which serves both the static
files and `/events` on 8800. Open **http://127.0.0.1:8800/** against the running
scanner, or against the live box through the SSH tunnel (see
`polymarket/infra/README.md`). The standalone `dashboard/serve.py` stub was
retired 2026-07-28 — it served static files with no broadcaster, which renders
an empty dashboard that is easy to mistake for a live-but-idle one.

## Redeploy cache busting

`index.html` loads every dashboard module with a `?v=<tag>` cache-bust query
string. `polymarket/infra/deploy.sh` stamps that tag from the staged bundle, so
manual bumps are unnecessary; redeploy after dashboard changes.

The scanner process holds the SSE broadcaster and ticks `/events` at 1 Hz. A
server started without the authenticated router serves static assets, but
snapshot reads, SSE, and operator mutations fail closed; the dashboard stays
empty (state shown in the topbar). Synthetic payloads are never served — stale
mock data is too easy to mistake for the live ledger.

## Live data contract

The dashboard consumes the SSE payload defined in
`polymarket/src/cryptobot/observability/snapshot_builder.py :: build_payload()`:

```js
{
  tick_ts, server_boot_ts, ledger_limit,
                              // boot stamp: the client reloads when it changes
  kpis: {
    initial_bankroll, wallet_bankroll, wallet_start_at,
    realized_capital, peak_capital, drawdown_pct, drawdown_pct_peak,
    max_drawdown_halt_pct, trading_halted, halt_reason,
    halted_wallets, pnl_24h, win_rate_24h, n_topups_24h,
    n_pending,                  // per-FILL count of PENDING ledger rows
                                // (capital.py wallet snapshots) — exposure
                                // bookkeeping's own count, NOT a bet count
    n_pending_trades,           // the same rows folded into TRADE GROUPS: open
                                // CLOB positions. THIS is what the rail "open
                                // bets" tile / sidebar badge render; null on a
                                // legacy schema, where the client falls back to
                                // n_pending and relabels itself "open fills"
    exposure_used, exposure_cap, exposure_warn_ratio,
    account_capital_live, account_pnl_total_live, account_capital_live_source,
    pnl_gross_usd, pnl_fee_usd, pnl_net_usd, account_equity_incl_open_usd,
    pnl_rebate_usd,             // Σ capital_flows kind='rebate' since the
                                // WALLET_START_AT cutover — account cash paid
                                // back by the venue. pnl_net_usd stays the
                                // PAPER net (gross − fee): Σ(ledger_daily.pnl)
                                // == pnl_net_usd is a pinned invariant. The
                                // Overview strip's NET cell adds the rebate
                                // CLIENT-side (gross − fees + rebates = the
                                // CLOB-reconciling figure)
    wins_24h, losses_24h,       // per-FILL wallet-snapshot counts (capital.py),
                                // NOT trade groups; a booked stop exit is
                                // folded into losses_24h (no stops_24h field)
    wallets: [{ id, label, kind, realized_capital, pnl_total, drawdown_pct_peak, exposure_used, exposure_cap, target_bet_size, trading_halted }],
    by_sleeve: { "<label>": { pnl_24h, n_bets_24h, n_topups_24h, open_notional } },
  },
  portfolio: { label, bet_size_multiplier, shared_exposure_cap_frac, max_drawdown_halt_frac, max_walk_price, sleeves },
  markets: [{ coin }],       // scan universe only (Journal coin filter); the
                             // per-sleeve decision subtree + gates/gate_order
                             // retired with the Live markets & gates tab
  ledger: [{ id, bet_ts, coin, wallet_id, sleeve, entry_minute, side, direction,
             outcome, fill_price, walked_vwap, effective_fill_price, bet_size,
             is_topup, pnl_net, exit_ts, venue,
             window_start_ts,  // TRADE-GROUP key component — the market
                               // window's epoch start; the client folds fills
                               // into trades on it (see the counting contract)
             // analytics fields (journal/calendar/overview):
             model_p, p_market, realized_edge, resolution_ts, won_up, interval_min }],
  ledger_rollup,             // lifetime win-rate / P&L / sample size, counted
                             // in TRADE GROUPS (server-side):
                             // { total, decided, wins, losses, voided,
                             //   settled_opened, open, win_rate, pnl_net,
                             //   stake, roi, first/last_ts }
                             // total is the AUDIT count (settled_opened + open
                             // + voided). Render settled_opened as "settled
                             // trades" — never total − open, which keeps the
                             // voided never-filled cancels inside the headline
  ledger_equity,             // lifetime equity series (downsampled, server-side)
                             // ONE POINT PER OPENED SETTLED TRADE GROUP; its n
                             // equals ledger_analytics[range='All'].summary
                             // .settled and rollup.settled_opened by contract
  ledger_daily,              // all-time per-day P&L (calendar, server-side):
                             // { date, pnl, n, wins, losses } — n/wins/losses
                             // are TRADE counts, pnl the per-fill Σ
  ledger_analytics,          // server-side per-range analytics keyed
                             // 'All'/'30d'/'7d' (coins, slices, histogram,
                             // streaks, summary) — the client's agg* helpers
                             // are only a fallback for a payload without it
  cash_equity,               // flow-inclusive CASH equity series — the curve
                             // the client PREFERS over ledger_equity
  ledger_rollup_by_venue,    // the three blocks above, re-keyed by venue slug,
  ledger_daily_by_venue,     // for the topbar venue filter
  ledger_analytics_by_venue,
  contracts,                 // { day_bucket, win_rate, trade_group } — the
                             // producer rules the client mirrors, verbatim
  strategy_flags,            // env-pinned execution-quality knobs (Config tab's
                             // data-driven strategy panel)
  capital_flows,             // account cash events (deposit/withdrawal/rebate)
                             // behind kpis.net_deposits + the rebate KPI
  venue,                     // deploy venue slug + label (topbar scope)
  predictfun_wallet,         // predict.fun deploys ONLY; absent on Polymarket
  live_wallet, operator,
  latency,                   // per-stage timing decomposition (Latency page)
                             // latency.venue_floor:
                             //   taker_delay_ms — the venue's DOCUMENTED
                             //     uncancellable delay (250 Polymarket, null
                             //     predict.fun). A published constant.
                             //   taker_delay_measured_ms — {p50,p95,max,n} of
                             //     the ACCEPTED per-order POST round trip in
                             //     the window (== orders.per_order.post): real
                             //     recorded time = network + enforced delay,
                             //     so it reads at or above the floor. null
                             //     when no accepted order was measured.
  v2,                        // Subminute console: spec, sizing, fill_rate,
                             // bets (ask, exec_price, live_filled_price), rejects
                             // v2.fill_rate = breaker state + TWO windows of the
                             // SAME measurement (Σ filled_usd / Σ requested_usd
                             // over non-'error' subminute_fills):
                             //   fill_rate / n_orders — newest N orders (the
                             //     breaker's own window; drives tripped/reason)
                             //   fill_rate_24h / n_orders_24h — ROLLING 24h by
                             //     order created_at; null when no 24h orders
                             // (full-history fill rate lives in
                             //  v2.reject_stats.fill_rate_usd)
}
```

If you add a field to the dashboard, add it in `snapshot_builder.py` and the
matching dashboard component at the same time. The SSE payload is the only live
data source for KPIs, markets, gates, ledger, and sleeve tiles.

`bet_ts` is stored in UTC by SQLite. The dashboard renders operator-facing
clock and ledger date/time in Malaysia time (`Asia/Kuala_Lumpur`, MYT).

## Snapshot-only

`snapshot.json` is still regenerated by `build_snapshot.py` during deploy so
the static **portfolio + operator/wallet fallback** stays current, but the
operator wallet, market, gate, and ledger surfaces are driven by live SSE. (The
retired Research tab's decision-table stats block was removed 2026-07-11 —
nothing read `window.SNAPSHOT.decision_table`.)

## Files

```text
polymarket/dashboard/
|-- README.md              this file
|-- README-DESIGN.md       original dark design-system spec (legacy reference)
|-- index.html             boot shell; loads React + Babel CDN + the JSX modules
|-- styles.css             dark "night desk" theme + all component styles
|-- time.js                shared UTC and settlement timestamp helpers
|-- live-bridge.js         EventSource('/events') -> window.LIVE (+ ledger retention)
|-- api.js                 authenticated operator POST helper (CSRF header)
|-- data.jsx               constants, formatters, hooks, client-side ledger analytics
|-- charts.jsx             dependency-free SVG charts (equity, drawdown, bars…)
|-- shell.jsx              sidebar, topbar, halt banner, shared UI atoms
|-- pages-overview.jsx     OverviewPage (hero + P&L strip + rail + settled-by-slice; Performance page retired 2026-07-11)
|-- pages-live.jsx         JournalPage (open + settled TRADES, expandable to their fills)  [LivePage / Live markets retired 2026-07-11]
|-- pages-analytics.jsx    CalendarPage (CalibrationPage deleted 2026-07-07)
|-- pages-config.jsx       OperatorPage (console + Polymarket & predict.fun funder wallets); StrategyPage retired
|-- pages-latency.jsx      LatencyPage (per-stage latency decomposition)
|-- pages-subminute.jsx    SubminutePage (DD-brake + resolution rollup + reject telemetry; the stop-loss exits panel was deleted 2026-08-05) + SubminuteSpecPage (Config spec)
|-- app.jsx                root: sidebar nav + page routing
|-- snapshot.json          generated static portfolio + live-state fallback context
|-- build_snapshot.py      writes static portfolio + live-state fallback -> snapshot.json
|-- assets/                glyph + live coin SVGs
`-- SKILL.md               design skill manifest
```

## Layout

- **Sidebar**: brand, grouped nav (Trading / Analytics / Config) with live
  counts, scanner-health dot + MYT clock footer.
- **Topbar**: page title/subtitle, mode badge (LIVE/DRY/MIXED), SSE connection
  pill, and a 7d/30d/All range toggle on the overview + journal pages.
- **Pages**: Overview (incl. settled trades by slice), Operator, Subminute
  (console), Trade journal, Calendar, Latency, and — under Config — the Subminute
  spec reference tab (see the intro for what each shows). The Live markets & gates
  and standalone Performance pages were retired 2026-07-11.
