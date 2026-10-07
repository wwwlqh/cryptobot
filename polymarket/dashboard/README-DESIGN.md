# Cryptobot Operator Night Desk — Design System

A design system for the **Cryptobot** operator dashboard — the surface a single
trader/operator uses to watch the Polymarket 5-minute and 15-minute crypto
up/down betting bot (BTC, ETH, SOL, XRP), audit the ledger, and study how the
HGB model (RF supplies a feature only) and gate stack are performing.

> **2026-07-07 reconstruction.** The light "paper journal" (2026-05-30) was
> replaced with a **dark trading-terminal system** ("night desk") and the two
> flagship pages were structurally rebuilt, not just rethemed: Overview's
> six-box KPI grid + side panels collapsed into a **hero band with a stats
> rail**; Live markets' KPI grid + eligibility-funnel
> panel collapsed into a **status band**, market cards grew real **decision
> tables** (model_p / ask / edge per slot), and the duplicate "how the gate
> stack works" panel was deleted (descriptions folded into the gate chips).
> The live token source of truth is [`styles.css`](styles.css).

---

## What the product is

A live trading **operator console for one user**. The reference frame is a
night trading desk — equity hero, a daily-P&L calendar, a per-bet ledger,
risk ratios, and sub-minute execution telemetry — not a marketing site. Copy voice is engineering-clear: numbers first, lowercase chrome, no
emoji, no exclamation marks, no marketing verbs.

The bot streams a live SSE payload (`window.LIVE`, see
`../src/cryptobot/observability/snapshot_builder.py :: build_payload`). There
lifetime analytics (equity, daily P&L, per-coin/per-slice, streaks) are
rolled up **server-side** over the full ledger and shipped as `ledger_rollup`
/ `ledger_equity` / `cash_equity` / `ledger_daily` / `ledger_analytics`;
[`data.jsx`](data.jsx) prefers those and keeps its client-side aggregations
only as a fallback for a payload that predates them.

---

## Aesthetic

**Layered blue-black, hairline-separated, terminal-dense.** A near-black canvas
(`#0a0c11`), cards one step lighter with a 1px top-light inset, and hairline
dividers instead of heavy borders. One restrained accent (Polymarket blue,
brightened for dark) for active nav, links, and focus; green/red reserved
STRICTLY for **direction and outcome** (P&L, YES/NO, WIN/LOSS) — never for
chrome or status.

## Color (tokens in `styles.css`)

Two tiers per semantic hue — **text tier** (bright, ~4.5:1 on the card surface
for 11–13px text) and **chart-mark tier** (`--c-*`, deeper steps validated with
the dataviz palette validator on surface `#11141b`: dark lightness band, CVD
adjacent separation, ≥3:1 contrast):

- **Surfaces** — `--bg #0a0c11` (canvas), `--bg-2 #0e1117` (wells/table head),
  `--surface #11141b` (cards), `--surface-2 #161a23` (raised).
- **Hairlines** — white-alpha: `--border` 7%, `--border-strong` 13%, `--border-faint` 4%.
- **Ink (4 tiers)** — `--fg #eceef4`, `--fg-2 #a5adbd`, `--fg-3 #6d7688`, `--fg-4 #3f4655`.
- **Accent** — `--accent #7a9dff` (+ `-2`, `-ink #b7c9ff` for active text,
  `-bg`, `-line`). Chart tier `--c-accent #4f7dfd`.
- **Semantic text tier** — `--pos #3ecf90`, `--neg #ff705f`, `--warn #e8a94a`,
  `--info #a08df5` (each with `-bg` tint + `-line`).
- **Chart mark tier (validated)** — `--c-pos #26ab72`, `--c-neg #e5503f`,
  `--c-warn #c4862a`, `--c-accent #4f7dfd`, `--c-info #8b6ff0`, plus
  `--c-grid` / `--c-grid-faint`. **charts.jsx reads these from the computed
  style at runtime** — no hex lives in chart code, so retheming the CSS
  rethemes every chart.

## Type

- **UI / display** — **Inter**; hero and KPI numbers at weight 800, tight
  tracking (−0.02…−0.03em), tabular figures.
- **Numerics / code** — **JetBrains Mono**, `tabular-nums slashed-zero`, for
  every dense/tabular surface: tables, decision grids, calendar, gates, params.
- Body 14px; card titles 12.5px/650; chrome labels 9.5–10px caps, letter-spaced;
  hero value 38px; rail/band values 21–26px.

## Signature components

- **KPI strip** — one segmented card (1px hairline gaps over `--border`),
  not floating boxes. `kpi-grid` → `kpi` cells.
- **Hero band** (Overview) — `.hero`: left = label, 38px equity value + delta
  chip, inline net/24h/win-rate metrics with hairline separators, then the
  equity chart with an integrated drawdown band; right = `.hero-rail` on
  `--bg-2` with open-bets/exposure meter, drawdown-vs-halt meter, profit
  factor, and streak chips.
- **Hero P&L strip** — `.kpi-grid.hero-strip`: the gross / fees / after-fees /
  rebates decomposition, nested inside the equity card under the drawdown
  chart. (The activity tape that used to live below the hero was deleted
  2026-08-05.)
- **Charts** — hand-built SVG (charts.jsx): 2px lines with a soft glow pass,
  gradient area fills, endpoint halo dot ringed with the surface color,
  hairline grid, mono axis labels, width-capped rounded bars with value +
  category labels. The equity chart carries a **crosshair hover tooltip**
  (snaps to the nearest settled bet: $equity, bet i/n, drawdown);
  bars/histogram ship native `<title>` tooltips.
- **Latency waterfall** (Latency page) — `.lat-track`/`.lat-seg`: additive
  stage decomposition of the latest tick + a p95-by-stage row, segments on an
  ordinal blue ramp (pipeline order light→dark, 2px surface gaps), dashed red
  marker at the 900ms soft deadline, legend with per-stage ms.
- **Journal filters** — `.scope-pills` coin + outcome chips above the ledger;
  KPI strip re-aggregates to the filtered scope (win-rate stays lifetime).
- **Journal price columns** — `exec` = the INTENDED order price at decision
  (walked vwap of the decision book, fallback top ask); `fill` = the realized
  venue fill (`live_filled_price`; paper/effective fill on DRY rows).
  Fill is slippage-tinted vs intent (red = paid more) with the exact delta
  in the tooltip.
- **Live motion** — one-shot, mount-keyed: the hero value pulses a soft glow
  when capital changes
  (`hero-pulse` via a `key` remount). No continuous animation.

## Spacing, radii, motion

- 4px base rhythm (`--gap-1 6 … --gap-4 24`); radii `--r-1 5 / --r-2 8 / --r-3 12 / pill`.
- Elevation = top-light inset (`--edge`) + soft black shadows; accent glow
  (`--glow-accent`) only on active market cards and the brand mark.
- Motion is functional: 140ms ease; meters transition width; LIVE dots pulse
  with a soft glow. No bounces.

---

## Information architecture (7 pages)

Sidebar grouped into **Trading / Analytics / Config** (see `NAV` in
[`shell.jsx`](shell.jsx)):

- **Trading** — overview · operator · subminute
- **Analytics** — trade journal · calendar · latency
- **Config** — subminute spec (frozen strategy contract)

(A **Calibration** page — predict.fun recorder status + calibration-gate
checklist — was reintroduced 2026-07-21 for the venue port and removed the same
day for the direct-DRY plan; its recorder KPIs folded into the Operator page's
predict.fun wallet panel. The old model-calibration page was deleted
2026-07-07. The standalone **Performance** page was retired 2026-07-11 — its
"settled bets by slice" table moved to the foot of Overview; the risk-adjusted
/ by-coin panels were dropped with the tab. The **Live markets & gates** page
was retired 2026-07-11 — Subminute is the live strategy and its console
supersedes the per-coin decision/gate view; Subminute was moved directly above
the Trade journal (last Trading item). The Config **Strategy** tab (renamed
from the sub-minute spec tab) is a per-band comparison table — ask bands ·
shared decision spec · sizing · execution — with band columns
data-driven from `spec.bands` (a single band column since the 2026-07-21
universe amendment), read live from the payload, not just the header card it
used to mirror.)

The topbar carries the page title, range control (where applicable), the
**venue switcher** (`ALL / POLYMARKET / PREDICT.FUN` segmented control), the MYT
clock + tick age, the money-mode badge (LIVE/DRY/MIXED), and the SSE state pill.

## Venue dimension

The dashboard is multi-venue (Polymarket + the predict.fun BNB-chain CLOB port).
The topbar **venue switcher** is a first-class filter *and* the current-venue
indicator (it replaced the passive venue badge). Its state persists in
`localStorage` and feeds one shared selector, `window.CB.scopeLive(live, sel)`,
that every ledger-derived page consumes — the venue dimension is a filtering /
scoping pass over the existing layouts, never a per-venue redesign. Each ledger
row may carry a `venue` tag; **absent ⇒ polymarket**, so the ALL/POLYMARKET views
equal the pre-venue output until src emits tags. The active PREDICT.FUN segment
uses the reserved `--info` hue (not the accent) so a non-Polymarket view is
unmistakable on a live-money desk. Account cash/equity is a single balance and is
never split by venue — a scoped view drops the cash basis and reads ledger-paper.
Coin iconography follows the selected venue's universe (BNB glyph only under
predict.fun). Every venue payload field may be ABSENT in older snapshots; the UI
degrades to polymarket / empty-state.

## Component & file map

```
styles.css          all tokens + component styles (the theme source of truth)
data.jsx            constants, fmt.*, hooks, client-side ledger aggregations (window.CB)
charts.jsx          dependency-free inline-SVG charts; reads --c-* tokens at runtime
shell.jsx           Sidebar, Topbar (clock cluster), HaltedBanner, KpiCell, Panel,
                    pills, CoinCell, meters
pages-overview.jsx  OverviewPage (hero + P&L strip + rail + settled-by-slice)
pages-live.jsx      JournalPage (open + settled TRADES, expandable to their fills)
                    — LivePage retired 2026-07-11
pages-analytics.jsx CalendarPage (ledger-paper net headline + daily P&L heatmap)
pages-config.jsx    OperatorPage (console) — StrategyPage retired 2026-07-09
pages-latency.jsx   LatencyPage (per-stage latency decomposition)
pages-subminute.jsx SubminutePage (DD-brake + resolution rollup + reject telemetry) + SubminuteSpecPage (Config spec)
app.jsx             root: sidebar nav + page routing
index.html          boot shell; dark color-scheme meta; loads React + Babel + modules
live-bridge.js      EventSource('/events') -> window.LIVE (+ ledger retention)
```

**Babel-standalone note:** every `text/babel` module compiles into one shared
global scope, so each file is wrapped in an IIFE `(function(){ … })()` and
exports via `window.X = X`. Keep new modules IIFE-wrapped or top-level `const`s
collide across files.

---

## Vocabulary (unchanged from the codebase)

- Tickers ALL CAPS: `BTC`, `ETH`, `SOL`, `XRP`. Coin glyph + ticker, never ticker alone.
- Sides `YES`/`NO`; directions `UP`/`DOWN`, color-tagged green/red.
- Outcomes verbatim: `PENDING`, `WIN`, `LOSS`, `CANCELLED`, `EXPIRED`, `GHOST_FILL`, `PUSH`.
- Slices `15m_e14` / `5m_e4`; sub-minute sleeves render as `sub·5m e4`.
- Skip reasons keep the `SKIP:<reason>` code (truncated with a title tooltip).
- Operator clocks in Malaysia time (`Asia/Kuala_Lumpur`, MYT); persisted timestamps UTC.
- Money 2dp USD with sign on deltas (`+$8.18`, `−$1.00`); probabilities 4dp.
- No emoji, no exclamation marks, no marketing voice.

## Assets

`assets/coins/{btc,eth,sol,xrp}.svg` (coin glyphs, never recolored) and
`assets/glyph.svg` (the 4-bar signal mark — favicon + sidebar brand mark).
