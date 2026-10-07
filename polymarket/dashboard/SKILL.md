---
name: cryptobot-design
description: Use this skill to generate branded interfaces and assets for Cryptobot, a Polymarket crypto trading bot on BTC, ETH, SOL, and XRP 5m/15m up-down markets.
user-invocable: true
---

Read `README-DESIGN.md` within this skill for the full design system, and
explore the runnable dashboard files:

- `styles.css` - the dark "night desk" token system: palette, type scale,
  semantic variables, radii, and motion (the token source of truth).
- `assets/` - 4-bar signal glyph and live coin SVGs for BTC, ETH, SOL, and XRP.
- `index.html` + `data.jsx` + `charts.jsx` + `shell.jsx` + `pages-*.jsx` +
  `app.jsx` - the runnable operator journal. `live-bridge.js` populates
  `window.LIVE` from `/events`; shared helpers live on `window.CB`.

The dashboard's current form is the **2026-07-07 dark "night desk"
reconstruction** (sidebar + 7 pages). The interim light "paper journal"
(2026-05-30) and the older `colors_and_type.css` tokens / `preview/` specimen
cards are all retired — do not reference them.

When creating visual artifacts, copy assets out of `assets/` and write static
files for the user to view. Match the **dark, terminal-dense "night desk"**
aesthetic: blue-black canvas (#0a0c11), hairline-separated dark surfaces, Inter
UI + JetBrains Mono tabular numerics + Space Grotesk display numerals, sharp
2-12px radii, one brightened Polymarket-blue accent for active/links only,
semantic green/red reserved for direction and outcome.

When working on production code, lift CSS variables directly from `styles.css`,
reuse the components in `shell.jsx`/`charts.jsx`, and wrap any new `text/babel`
module in an IIFE (Babel-standalone shares one global scope). New surfaces slot
into the sidebar nav defined by `NAV` in `shell.jsx`.

Conventions to respect:
- Tickers: `BTC`, `ETH`, `SOL`, `XRP`.
- Sides: `YES` / `NO`; directions `UP` / `DOWN`.
- Outcomes: `PENDING`, `WIN`, `LOSS`, `CANCELLED`, `EXPIRED`, `GHOST_FILL`, `PUSH`.
- Skip reasons: keep the exact `SKIP:<reason>` code.
- Sleeve references: `subminute_5m`, `subminute_15m` (the two live checkpoint sub-minute sleeves). The old exp044 minute-slice labels `15m_e14` / `5m_e4` were retired 2026-07-31.
- Times shown to operators in Malaysia time (`Asia/Kuala_Lumpur`, MYT); persisted SQLite timestamps remain UTC. Money in 2dp USD with sign on deltas.
- Probabilities to 4dp.
- No emoji, no exclamation marks, no marketing voice.
