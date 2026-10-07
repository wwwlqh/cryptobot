// ===========================================================================
// data.jsx — constants, formatters, live hooks, and client-side ledger
// analytics for the cryptobot operator journal.
//
// The live scanner streams the SSE payload (window.LIVE) defined in
//   src/cryptobot/observability/snapshot_builder.py :: build_payload()
// and a static research snapshot (window.SNAPSHOT) from snapshot.json. The
// payload carries server-side LIFETIME rollups (ledger_rollup / ledger_daily /
// ledger_equity + their per-venue variants) computed over the FULL ledger —
// prefer-server selectors (lifetimeSummary / selectDaily / lifetimeEquity)
// read those first and fall back to CLIENT-SIDE aggregates over the streamed
// ledger array only for older payloads / static snapshots. Row-based
// aggregates are bounded by how many ledger rows the stream has delivered this
// session (bootstrap + deltas, retained by live-bridge.js); analytics surfaces
// label their row window honestly.
//
// Everything shared across the JSX files is hung on `window.CB`. Component
// files (charts/shell/pages/app) load AFTER this file and read from it.
// ===========================================================================

(function () {
// Venue registry. The bot is being ported from Polymarket (7% parabola fee,
// BTC/ETH/SOL/XRP) to predict.fun (BNB-chain CLOB, 2% tent fee, BTC/ETH/BNB).
// The live payload carries `venue`; an ABSENT venue ⇒ polymarket (older
// snapshots predate the field). Coin universes here are only a fallback for
// the pre-first-tick boot window — the live `markets` array wins when present.
const VENUE_META = {
  polymarket: { key: 'polymarket', label: 'POLYMARKET', short: 'Polymarket', coins: ['BTC', 'ETH', 'SOL', 'XRP'] },
  predictfun: { key: 'predictfun', label: 'PREDICT.FUN', short: 'predict.fun', coins: ['BTC', 'ETH', 'BNB'] },
};
const DASHBOARD_TZ = 'Asia/Kuala_Lumpur';
const INITIAL_BANKROLL = 100.0;
const EMPTY_LIVE = {
  tick_ts: null, kpis: null, portfolio: null, markets: [],
  ledger: [], ledger_rollup: null, ledger_equity: null, ledger_daily: null,
  operator: null, live_wallet: null,
};

// Outcomes that carry a realized result (anything not still open).
const SETTLED = new Set(['WIN', 'LOSS', 'CANCELLED', 'EXPIRED', 'GHOST_FILL', 'PUSH']);

const MYT_PARTS = new Intl.DateTimeFormat('en-US', {
  timeZone: DASHBOARD_TZ, year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
});

// ── time parsing ──────────────────────────────────────────────────────
const parseUtcTimestamp = window.CB_TIME.parseUtcTimestamp;

function mytParts(value) {
  const d = parseUtcTimestamp(value);
  if (!d) return null;
  return Object.fromEntries(
    MYT_PARTS.formatToParts(d).filter((p) => p.type !== 'literal').map((p) => [p.type, p.value])
  );
}

// ── formatters ────────────────────────────────────────────────────────
const fmt = {
  money(x, dp = 2) {
    if (x == null || !isFinite(x)) return '—';
    const s = Math.abs(x).toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp });
    return (x < 0 ? '−$' : '$') + s;
  },
  moneyDelta(x, dp = 2) {
    if (x == null || !isFinite(x)) return '—';
    const sign = x > 0 ? '+' : x < 0 ? '−' : '';
    const s = Math.abs(x).toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp });
    return `${sign}$${s}`;
  },
  pct(x, dp = 1) { return (x == null || !isFinite(x)) ? '—' : (x * 100).toFixed(dp) + '%'; },
  pctPoints(x, dp = 2) { return (x == null || !isFinite(x)) ? '—' : x.toFixed(dp) + '%'; },
  pctDelta(x, dp = 1) {
    if (x == null || !isFinite(x)) return '—';
    return (x >= 0 ? '+' : '−') + Math.abs(x * 100).toFixed(dp) + '%';
  },
  prob(x, dp = 4) { return (x == null || !isFinite(x)) ? '—' : x.toFixed(dp); },
  signed(x, dp = 3) { return (x == null || !isFinite(x)) ? '—' : (x >= 0 ? '+' : '−') + Math.abs(x).toFixed(dp); },
  num(x, dp = 2) { return (x == null || !isFinite(x)) ? '—' : Number(x).toFixed(dp); },
  ratio(x, dp = 2) {
    if (x == null || !isFinite(x)) return x === Infinity ? '∞' : '—';
    return Number(x).toFixed(dp);
  },
  intc(x) {
    if (x == null || !isFinite(x)) return '—';
    const n = Number(x);
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'k';
    return String(Math.round(n));
  },
  dateMyt(v) { const p = mytParts(v); return p ? `${p.year}-${p.month}-${p.day}` : '—'; },
  hmsMyt(v) { const p = mytParts(v); return p ? `${p.hour}:${p.minute}:${p.second}` : '—'; },
  hmMyt(v) { const p = mytParts(v); return p ? `${p.hour}:${p.minute}` : '—'; },
  agoShort(v) {
    if (v == null) return '—';
    const ms = typeof v === 'number' ? v : parseUtcTimestamp(v)?.getTime();
    if (ms == null || !isFinite(ms)) return '—';
    const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
    return `${Math.floor(s / 86400)}d ago`;
  },
  shortAddr(a) {
    if (!a || typeof a !== 'string') return null;
    return a.length <= 12 ? a : `${a.slice(0, 6)}…${a.slice(-4)}`;
  },
};

// ── domain helpers ────────────────────────────────────────────────────
function tone(x) { return x == null || !isFinite(x) ? '' : x > 0 ? 'pos' : x < 0 ? 'neg' : ''; }
function directionFromSide(side) { return side === 'YES' ? 'UP' : side === 'NO' ? 'DOWN' : (side || '—'); }
function isSettled(r) { return !!r && SETTLED.has(r.outcome); }
// WIN/LOSS contract (mirrors the server ledger_rollup / payload.contracts
// .win_rate): win rate = wins/(wins+losses) over TRADE GROUPS.
//   loss   = a decided LOSS, OR a booked stop-loss exit (exit_kind='stop_loss'
//            with a real pnl_net). A stop exit is a realized loss, so it is
//            simply a LOSS — the 2026-08-05 operator order removed the separate
//            "SL" class from every dashboard surface. `isBookedStop` survives
//            ONLY to classify those historical rows: their DB outcome is
//            'CANCELLED', so without it they would read as voided cancels and
//            silently leave the denominator.
//   voided = a CANCELLED row with NO exit_kind (never filled / refunded) —
//            excluded from win rate AND from daily trade counts everywhere.
function isBookedStop(r) { return !!r && r.exit_kind === 'stop_loss' && typeof r.pnl_net === 'number'; }
function isWinRow(r) { return !!r && r.outcome === 'WIN'; }
function isLossRow(r) { return !!r && (r.outcome === 'LOSS' || isBookedStop(r)); }
function isVoided(r) { return !!r && r.outcome === 'CANCELLED' && r.exit_kind == null; }
function rowStake(r) {
  if (r && typeof r.effective_bet_size === 'number') return r.effective_bet_size;
  return r && typeof r.bet_size === 'number' ? r.bet_size : 0;
}
// Canonical settle-timestamp accessor — the ONE place the fallback chain
// lives (raw payload string). H1 day-bucket contract (payload.contracts.
// day_bucket, mirrors the server _MYT_DAY_SQL): a stop-loss row realizes its
// pnl at its IMMUTABLE exit_ts (resolution_ts stays NULL until a display-only
// sweep backfill may stamp the late sweep clock), so stop rows key on exit_ts
// when the payload carries it, then fall through resolution_ts → bet_ts like
// every other row. Every surface that shows or sorts by settle time goes
// through this, so the chain can never drift between the journal sort, the
// settled column, the tape, and the client aggDaily fallback.
function settleTs(r) {
  return window.CB_TIME.settlementTimestamp(r);
}
function settleMs(r) { return window.CB_TIME.settlementMs(r); }
function betMs(r) { const d = parseUtcTimestamp(r && r.bet_ts); return d ? d.getTime() : null; }

function rowInterval(r) {
  if (!r) return null;
  if (r.interval_min === 5 || r.interval_min === 15) return r.interval_min + 'm';
  const s = (r.sleeve || '') + '';
  if (/5m/.test(s)) return '5m';
  if (/15m/.test(s)) return '15m';
  return null;
}
function sliceRef(r) {
  const iv = rowInterval(r) || '?';
  return `${iv}_e${r.entry_minute == null ? '?' : r.entry_minute}`;
}
// Trade Journal price contract: exec is the decision-time intended price
// (walked VWAP, falling back to the top ask), while fill is the venue-reported
// price. LIVE rows without a reconciled fill stay blank; DRY rows use exec as
// their paper fill. Keep this pair shared with Subminute so the two tabs can
// never disagree about what was intended versus what actually traded.
function execPrice(r) {
  if (!r) return null;
  if (r.exec_price != null) return r.exec_price;
  if (r.walked_vwap != null) return r.walked_vwap;
  if (r.fill_price != null) return r.fill_price;
  return r.effective_fill_price != null ? r.effective_fill_price : null;
}
function fillPrice(r) {
  if (!r) return null;
  if (r.live_filled_price != null) return r.live_filled_price;
  return r.mode === 'LIVE' ? null : execPrice(r);
}

// ── venue awareness ───────────────────────────────────────────────────
// Resolve the active venue KEY from the live payload, with a window.SNAPSHOT
// fallback (the static snapshot carries the same field) and a hard default of
// 'polymarket' so older payloads that omit `venue` entirely keep working.
// Any unrecognized value degrades to polymarket rather than blanking the UI.
function venueOf(live) {
  const snap = (typeof window !== 'undefined' && window.SNAPSHOT) ? window.SNAPSHOT : null;
  const raw = (live && live.venue) || (snap && snap.venue) || 'polymarket';
  const v = String(raw).toLowerCase();
  return VENUE_META[v] ? v : 'polymarket';
}
// The venue's coin universe (BTC/ETH/SOL/XRP on polymarket, BTC/ETH/BNB on
// predict.fun) — the boot-window fallback for coin filters/iconography.
function venueUniverse(live) { return VENUE_META[venueOf(live)].coins.slice(); }

// Read the coin universe from the live markets array, falling back to the
// active venue's known universe during the pre-first-tick boot window.
function liveCoins(live) {
  if (live && Array.isArray(live.markets) && live.markets.length) {
    const cs = live.markets.map((m) => (m.coin || '').toUpperCase()).filter(Boolean);
    if (cs.length) return cs;
  }
  return venueUniverse(live);
}

// ── venue scope (SINGLE-VENUE consoles) ───────────────────────────────
// 2026-08-01 (predict.fun Tokyo cutover): the cross-venue view is GONE. This
// dashboard is ONE app PARAMETERIZED by venue — :8800 renders the Polymarket
// box, :8801 renders the predict.fun box, and neither can show the other. What
// was removed and must not come back:
//   * VENUE_SELECTORS + the topbar switcher — a viewer could pin the OTHER
//     venue in localStorage and a console would render an empty (or foreign)
//     ledger on the box it is supposed to be watching;
//   * pfPaperRows / mergedLedger + window.PF_PAPER — the predict.fun paper
//     instance is retired and its predictfun_paper.json producer is deleted;
//   * the window.PF_LIVE payload swap in app.jsx (predictfun_live.json, also
//     deleted). Its money process is on another continent now; a 30s snapshot
//     of a REAL-MONEY box masquerading as a live console is exactly the
//     surface you do not want during an incident.
// The `venueSel` argument that still flows through these helpers is now always
// THIS deployment's venue (app.jsx derives it from live.venue) — kept as an
// argument so the row-level venue tagging stays explicit rather than implied.
function normVenueSel(sel) {
  // Unset resolves to THIS deployment's venue — stashed by App from the live
  // payload (window.__CB_DEPLOY_VENUE) — never a cross-venue passthrough.
  const s = String(sel || '').toLowerCase();
  if (s === 'polymarket' || s === 'predictfun') return s;
  return (typeof window !== 'undefined' && window.__CB_DEPLOY_VENUE) || 'polymarket';
}
// A single row's venue, normalized (ABSENT ⇒ polymarket, unknown ⇒ polymarket).
function rowVenue(r) {
  const v = r && r.venue ? String(r.venue).toLowerCase() : 'polymarket';
  return VENUE_META[v] ? v : 'polymarket';
}
// THE shared row selector every page uses.
function venueFilter(rows, sel) {
  if (!Array.isArray(rows)) return rows;
  const s = normVenueSel(sel);
  return rows.filter((r) => rowVenue(r) === s);
}
// Venue-qualified row identity for React keys / open-row sets. Still
// venue-qualified even on a single-venue console: a box's ledger can carry
// historic rows from a venue it no longer trades.
function rowKey(r) {
  return rowVenue(r) + ':' + String(r && r.id != null ? r.id : '');
}
// A venue-scoped shallow view of the live payload: ledger filtered to THIS
// deployment's venue, with the server LIFETIME rollups resolved per venue.
// Account-level KPIs are not partitioned by venue and pass through untouched
// (kpis is spread as-is). Rollup resolution order:
//   1. the server per-venue rollups (ledger_rollup_by_venue /
//      ledger_daily_by_venue, H5) when the payload emits them — identical
//      W·L·SL definitions computed over the FULL DB, not the bounded stream.
//   2. when the venue filter removed ZERO rows the payload's ledger IS this
//      venue — KEEP the all-venue lifetime rollups instead of nulling them
//      (the failure where every surface collapsed to the bounded 500-row
//      stream window).
//   3. otherwise null ⇒ the prefer-server selectors fall back to a client
//      aggregate over the scoped ledger.
// The static-paper merge (window.PF_PAPER) that used to run BEFORE the filter
// is gone with the cross-venue view — the ledger is now exactly what THIS
// box's own SSE stream carries.
function scopeLive(live, sel) {
  if (!live) return live;
  const s = normVenueSel(sel);
  const rows = Array.isArray(live.ledger) ? live.ledger : [];
  const scoped = venueFilter(rows, s);
  const byRoll = (live.ledger_rollup_by_venue
    && typeof live.ledger_rollup_by_venue === 'object')
    ? live.ledger_rollup_by_venue[s] : null;
  const byDaily = (live.ledger_daily_by_venue
    && typeof live.ledger_daily_by_venue === 'object')
    ? live.ledger_daily_by_venue[s] : null;
  const byAnalytics = (live.ledger_analytics_by_venue
    && typeof live.ledger_analytics_by_venue === 'object')
    ? live.ledger_analytics_by_venue[s] : null;
  // keepAll = "the scoped view IS the server's own population" — only then may
  // the server's all-venue rollups stand in for the venue view.
  // rows.length > 0 guard: with an EMPTY stream (right after a RESET_RECORDS
  // cutover) 0===0 must not leak all-venue blocks into a named-venue view (the
  // predict.fun tab rendered Polymarket lifetime numbers, 2026-07-22).
  const keepAll = rows.length > 0 && scoped.length === rows.length;
  return {
    ...live,
    ledger: scoped,
    ledger_rollup: (byRoll && typeof byRoll.total === 'number') ? byRoll
      : (keepAll ? live.ledger_rollup : null),
    ledger_daily: Array.isArray(byDaily) ? byDaily
      : (keepAll ? live.ledger_daily : null),
    ledger_analytics: (byAnalytics && typeof byAnalytics === 'object') ? byAnalytics
      : (keepAll ? live.ledger_analytics : null),
    ledger_equity: keepAll ? live.ledger_equity : null,
  };
}
// Coin universe for the scoped venue. The DEPLOYED venue reads the live scan
// universe (markets array = config.COINS, e.g. BTC/ETH on predict.fun) so the
// pills never pin a retired coin; a non-deployed venue (only reachable through
// historic rows) falls back to its VENUE_META registry universe. Optional
// `rows` (an already venue-scoped ledger) unions in any historical coins still
// present in the rows — e.g. XRP settled rows stay filterable after the drop.
function scopedCoins(live, sel, rows) {
  const s = normVenueSel(sel);
  const coins = (s === venueOf(live))
    ? liveCoins(live)
    : VENUE_META[s].coins.slice();
  if (Array.isArray(rows)) {
    const seen = new Set(coins);
    rows.forEach((r) => {
      const c = r && r.coin ? String(r.coin).toUpperCase() : null;
      if (c && !seen.has(c)) { seen.add(c); coins.push(c); }
    });
  }
  return coins;
}
// Strip the account cash basis from a money bundle so a venue-scoped headline
// reads pure ledger-paper (cash is one account balance, not split by venue): net
// collapses to the paper figure and the cash bridge/as-of drop out.
function stripCash(m) {
  return { ...m, cashLive: null, cashNet: null, net: m.paper, drift: null, hasCash: false, degraded: false };
}
// Normalize the predict.fun funder wallet block (live.predictfun_wallet, mirrored
// on window.SNAPSHOT). ABSENT ⇒ {present:false} for the wallet panel's clean
// empty state.
function predictfunWallet(source) {
  const snap = (typeof window !== 'undefined' && window.SNAPSHOT) ? window.SNAPSHOT : null;
  const w = (source && source.predictfun_wallet) || (snap && snap.predictfun_wallet) || null;
  if (!w || typeof w !== 'object') return { present: false };
  const col = (w.collateral && typeof w.collateral === 'object') ? w.collateral : null;
  return {
    present: true,
    address: w.address || null,
    chain: w.chain || 'bnb',
    apiKeyPresent: w.api_key_present === true,
    collateral: col ? { symbol: col.symbol || null, balance: typeof col.balance === 'number' ? col.balance : null } : null,
  };
}

function strategyBase(k) {
  if (!k) return INITIAL_BANKROLL;
  return k.initial_bankroll != null ? k.initial_bankroll : INITIAL_BANKROLL;
}
// ONE money bundle — the single source of the dashboard's money story, read by
// the shared MoneyHeadline (Overview hero, Journal KpiCell, Calendar headline).
// Collapses the accreted account_* selector maze into one contract so every
// tab's headline aligns by construction:
//   • cashNet is THE one number called "net P&L" (cash basis); null ⇒ use paper.
//   • net is the ONE headline figure consumers read (m.net): cashNet ?? paper.
//   • paper is the canonical ledger-paper net (k.pnl_net_usd).
//   • drift is the gas/rounding/unsettled bridge; net = paper + drift on screen.
// Prefers the per-tick live CLOB values (account_*_live) the same balance live
// sizing reads; falls back to the ~10-min reconciliation sample (degraded, as-of
// shown), then to ledger paper. See snapshot_builder._live_account_capital.
function money(k) {
  const kk = k || {};
  const cashLive = kk.account_capital_live != null ? kk.account_capital_live
    : (kk.account_capital != null ? kk.account_capital : null);
  // Era-anchored cash net (cash_now − cash_at_reset) preferred so the cash P&L
  // matches the reset-era window the paper rollup already uses; falls back to
  // the audited lifetime-basis account_pnl_total (kept for reconciliation).
  const cashNet = kk.account_pnl_era_cash != null ? kk.account_pnl_era_cash
    : (kk.account_pnl_total_live != null ? kk.account_pnl_total_live
    : (kk.account_pnl_total != null ? kk.account_pnl_total : null));
  const paper = kk.pnl_net_usd != null ? kk.pnl_net_usd : null;
  // LIVE-only strategy ledger paper (account_strategy_pnl_total): cash moved
  // only for LIVE rows, so the cash drift bridge nets against THIS when the
  // producer emits it — DRY paper P&L can never masquerade as
  // "gas·rounding·unsettled" drift. Falls back to the all-rows paper figure.
  const paperLive = kk.account_strategy_pnl_total != null
    ? kk.account_strategy_pnl_total : null;
  const bridgePaper = paperLive != null ? paperLive : paper;
  // Both legs must exist: cashNet − null would coerce to cashNet − 0 and the
  // bridge would show the whole cash net as "gas·rounding·unsettled" drift.
  const drift = cashNet != null && bridgePaper != null ? cashNet - bridgePaper : null;
  const basis = kk.account_initial_capital != null ? kk.account_initial_capital
    : (kk.realized_capital != null ? kk.realized_capital : INITIAL_BANKROLL);
  // C4 producer contract: the basis anchor may be an UNCLEAN sample (no
  // ok+flat-book row existed before the first live bet) — percentage returns
  // off it are approximate, flagged so consumers can render "≈".
  const basisUnclean = kk.account_initial_capital_source === 'unclean_clob_before_first_live_bet'
    || kk.account_capital_comparable === false;
  const asOf = parseUtcTimestamp(kk.account_capital_live_ts);
  const src = kk.account_capital_live_source;
  return {
    cashLive,
    cashNet,
    paper,
    paperLive,
    bridgePaper,
    // Operator 2026-07-24: the headline net is the ERA-anchored CASH result
    // (account_pnl_era_cash = cash_now − cash_at_reset) so it matches the equity
    // tile + the cash equity curve and reads GREEN when the account is up; the
    // ledger-PAPER trading result rides the sub-line (net = paper + drift bridge).
    // Supersedes the 2026-07-23 paper-headline choice (operator confirmed the
    // calendar-blind trade-off: cash is one balance, not a per-day figure, so a
    // date-range pick re-scopes the breakdown below but not this headline). Paper
    // is the fallback only when no cash figure exists.
    net: cashNet != null ? cashNet : bridgePaper,
    // Whether `net` IS the cash figure (drives the MoneyHeadline tag/sub).
    // Equals hasCash here, but withPaperBasis forces it false when it overrides
    // net to a period's paper sum while the account still HAS a cash figure.
    netIsCash: cashNet != null,
    drift,
    basis,
    basisUnclean,
    asOfMs: asOf ? asOf.getTime() : null,
    degraded: kk.account_capital_live_degraded === true
      || (src != null && src !== 'balance_cache' && src !== 'readiness_report'),
    hasCash: cashNet != null,
    openValue: kk.open_positions_value_usd != null ? kk.open_positions_value_usd : null,
    equityInclOpen: kk.account_equity_incl_open_usd != null ? kk.account_equity_incl_open_usd : null,
  };
}

// Re-anchor a money bundle to a caller-supplied `paper` figure, keeping it
// self-consistent. The Calendar headlines Σ(rendered heatmap cells) as its
// "paper", so net and the drift bridge must be recomputed against that
// number. Paper-first headline (operator 2026-07-23): net IS the new paper;
// drift = cash − new paper. The ONE place a consumer overrides "paper".
function withPaperBasis(m, paper) {
  return {
    ...m,
    paper,
    paperLive: null,          // the override IS the bridge basis now
    bridgePaper: paper,
    net: paper != null ? paper : m.cashNet,
    // net is now the period's PAPER sum (not the cash figure) whenever a paper
    // basis was supplied — so the shared MoneyHeadline tags it 'ledger paper',
    // not 'cash · since reset', even though the account still has cash.
    netIsCash: paper == null && m.cashNet != null,
    drift: m.cashNet != null && paper != null ? m.cashNet - paper : null,
  };
}

// Range filter on settlement time (used by overview/journal range toggle).
function inRange(ledger, range) {
  if (!range || range === 'All') return ledger;
  const days = range === '7d' ? 7 : range === '30d' ? 30 : null;
  if (days == null) return ledger;
  const cut = Date.now() - days * 86400000;
  return ledger.filter((r) => { const t = settleMs(r) || betMs(r); return t != null && t >= cut; });
}

// Range slice of a daily series ({date: 'YYYY-MM-DD' MYT, ...}). Daily rows are
// whole MYT buckets, so the exact millisecond boundary is not representable:
// exclude the boundary bucket and retain the following `days` calendar dates.
// The strict comparison prevents a 7d/30d view from silently including an
// eighth/thirty-first full day while the row-based twin uses an exact cutoff.
function dailyInRange(daily, range) {
  if (!Array.isArray(daily) || !range || range === 'All') return daily || [];
  const days = range === '7d' ? 7 : range === '30d' ? 30 : null;
  if (days == null) return daily;
  const cut = fmt.dateMyt(Date.now() - days * 86400000);
  if (cut === '—') return daily;
  return daily.filter((d) => d && d.date > cut);
}

// ===========================================================================
// Trade groups (CLOB positions) — operator 2026-08-05
// ===========================================================================
// The ledger writes ONE ROW PER FILL: a base entry plus every top-up it took in
// the same market window is several rows. On the venue that is ONE position —
// one token balance that settles once — so every count this dashboard labels a
// "trade" (or uses as a win-rate denominator) counts GROUPS, not rows. Dollar
// sums are untouched: Σ over groups of Σ over fills == Σ over fills.
//
// The key is the position's identity, mirroring the producer's
// `_TRADE_GROUP_KEY_SQL` exactly (payload.contracts.trade_group):
//
//     venue · coin · interval_min · window_start_ts · side · wallet
//
// (coin, interval_min, window_start_ts) names the market window, `side` names
// WHICH outcome token was bought (a YES and a NO position in the same window
// are two different CLOB balances and must never merge), and the wallet keeps
// two funded accounts apart. `is_topup` is NOT in the key — a top-up's whole
// point is that it lands in its base bet's group; it only LABELS the fills
// inside one. A row from a payload too old to carry window_start_ts gets a
// row-unique key, so a legacy stream degrades to one-trade-per-fill (the old
// behaviour) instead of merging unrelated positions.
function tradeGroupKey(r, i) {
  if (!r) return 'row:?' + i;
  // No window key ⇒ the row's own identity, so it stays a trade of one. `i` is
  // the caller's row index, the last resort for a row carrying no id either.
  if (r.window_start_ts == null) {
    return 'row:' + (r.id != null ? rowKey(r) : 'i' + i);
  }
  return [
    rowVenue(r), r.coin || '?',
    r.interval_min == null ? '?' : r.interval_min,
    r.window_start_ts, r.side || '?',
    r.wallet_id || r.sleeve || '?',
  ].join('|');
}

// Fold ledger rows into trade groups, newest-fill-last within each group.
// `fills[0]` is the ANCHOR: the base entry (is_topup falsy) with the earliest
// bet time, so the group's slice / model_p / realized edge describe the trade
// the way the base bet defined it. A group whose base row is outside the
// streamed window falls back to its earliest fill rather than blanking.
function groupTrades(ledger) {
  const m = new Map();
  (ledger || []).forEach((r, i) => {
    if (!r) return;
    const k = tradeGroupKey(r, i);
    let g = m.get(k);
    if (!g) { g = { key: k, fills: [] }; m.set(k, g); }
    g.fills.push(r);
  });
  return [...m.values()].map((g) => {
    // base bets before top-ups, then oldest first — the fill order the journal
    // labels as base / top-up 1 / top-up 2 …
    const fills = g.fills.slice().sort((a, b) => (
      (a.is_topup ? 1 : 0) - (b.is_topup ? 1 : 0)
      || (betMs(a) || 0) - (betMs(b) || 0)
      || (a.id || 0) - (b.id || 0)
    ));
    const anchor = fills[0];
    let size = 0, notional = 0, execNotional = 0, pnl = 0, hasPnl = false;
    let open = false, anyWin = false, anyLoss = false, anyReal = false, anySettled = false;
    fills.forEach((r) => {
      const stake = rowStake(r);
      size += stake;
      const fp = fillPrice(r);
      if (fp != null) notional += stake * fp;
      const xp = execPrice(r);
      if (xp != null) execNotional += stake * xp;
      if (typeof r.pnl_net === 'number') { pnl += r.pnl_net; hasPnl = true; }
      if (r.outcome === 'PENDING') open = true;
      if (isWinRow(r)) anyWin = true;
      if (isLossRow(r)) anyLoss = true;
      if (!isVoided(r)) anyReal = true;
      if (isSettled(r)) anySettled = true;
    });
    // Effective fill = size-weighted mean over the fills that reported a price
    // (an unpriced fill contributes its stake to `size` but cannot move the
    // average, so the weighting is over the priced notional's own stake).
    const pricedSize = fills.reduce((a, r) => a + (fillPrice(r) != null ? rowStake(r) : 0), 0);
    const execSize = fills.reduce((a, r) => a + (execPrice(r) != null ? rowStake(r) : 0), 0);
    return {
      ...g,
      fills, anchor, nFills: fills.length,
      coin: anchor.coin, side: anchor.side, direction: anchor.direction,
      wallet_id: anchor.wallet_id, sleeve: anchor.sleeve,
      interval_min: anchor.interval_min, entry_minute: anchor.entry_minute,
      model_p: anchor.model_p, realized_edge: anchor.realized_edge,
      window_start_ts: anchor.window_start_ts,
      size,
      avgFill: pricedSize > 0 ? notional / pricedSize : null,
      avgExec: execSize > 0 ? execNotional / execSize : null,
      pnl: hasPnl ? pnl : null,
      hasPnl,
      settled: anySettled && !open,
      open,
      win: !open && anyWin,
      loss: !open && !anyWin && anyLoss,
      voided: !open && !anyWin && !anyLoss && !anyReal,
      // Display outcome for the collapsed row: the group's single verdict.
      outcome: open ? 'PENDING'
        : anyWin ? 'WIN'
        : anyLoss ? 'LOSS'
        : (anchor.outcome || 'PENDING'),
      settleMs: fills.reduce((mx, r) => {
        const t = settleMs(r);
        return t == null ? mx : (mx == null ? t : Math.min(mx, t));
      }, null),
      settleTs: fills.reduce((best, r) => {
        const t = settleTs(r);
        if (!t) return best;
        return !best || String(t) < String(best) ? t : best;
      }, null),
    };
  });
}
// Group-level predicates — the group twins of isWinRow / isLossRow / isVoided.
function gOutcome(g) { return !!g && (g.win || g.loss); }

// ===========================================================================
// Aggregations (pure; pages wrap in React.useMemo).
// ===========================================================================
// Every COUNT below is one-per-trade-group; every $ figure is the same Σ over
// fills it always was.

// Equity curve: replay settled TRADES in settlement order, compounding the
// realized capital from `base`. Returns points + peak + max drawdown.
//
// ONE POINT PER TRADE GROUP (operator 2026-08-09) — the client twin of the
// server _ledger_equity_series. Fills fold into CLOB positions first (a base
// bet + its top-ups = one point, not three) and voided never-filled cancel
// groups never plot, so this fallback curve's x-axis is the TRADE number, the
// same population the hero "trades" tile counts. `n` is a trade count.
// Σ over groups == Σ over rows, so anchoredEquity's back-out is unaffected.
function aggEquity(ledger, base) {
  const rows = groupTrades((ledger || []).filter((r) => isSettled(r) && typeof r.pnl_net === 'number'))
    .filter((g) => g.settled && !g.voided)
    .map((g) => ({ ts: g.settleMs || 0, pnl: g.pnl || 0 }))
    .sort((a, b) => a.ts - b.ts);
  let eq = base, pk = base, maxDD = 0, maxDDAbs = 0;
  const points = [{ i: 0, eq: base, dd: 0, ddAbs: 0, ts: rows.length ? rows[0].ts : 0 }];
  rows.forEach((r, i) => {
    eq += r.pnl; pk = Math.max(pk, eq);
    const ddAbs = eq - pk, dd = pk > 0 ? ddAbs / pk : 0;
    if (dd < maxDD) maxDD = dd;
    if (ddAbs < maxDDAbs) maxDDAbs = ddAbs;
    points.push({ i: i + 1, eq, dd, ddAbs, ts: r.ts });
  });
  return { points, base, final: eq, peak: pk, maxDD, maxDDAbs, n: rows.length };
}

// Per-day P&L (keyed on MYT settlement date) — the client fallback twin of
// the server ledger_daily rollup, mirroring its exact contract: fills with a
// numeric pnl_net only (parity with SQL `pnl_net IS NOT NULL`), day keyed by
// settleTs (stop rows on exit_ts), and the trade-group counting rule —
// `n`/`wins`/`losses` count TRADE GROUPS filed under the group's EARLIEST
// settle day, with voided groups excluded from `n` (their 0 pnl stays in the
// sum so Σ(day.pnl) still equals the headline net). A booked stop exit is a
// loss. `pnl` is the whole group's Σ, which keeps every settled fill in exactly
// one day exactly once.
function aggDaily(ledger) {
  const m = new Map();
  groupTrades((ledger || []).filter((r) => isSettled(r) && typeof r.pnl_net === 'number'))
    .forEach((g) => {
      const d = fmt.dateMyt(g.settleTs);
      if (d === '—') return;
      const e = m.get(d) || { date: d, pnl: 0, n: 0, wins: 0, losses: 0 };
      e.pnl += g.pnl || 0;
      if (!g.voided) e.n += 1;
      if (g.win) e.wins += 1; else if (g.loss) e.losses += 1;
      m.set(d, e);
    });
  return [...m.values()].sort((a, b) => (a.date < b.date ? -1 : 1));
}

// Per-day P&L series with server-preferred population. When the SSE payload
// carries the server all-time rollup (live.ledger_daily — Σ(day.pnl) ===
// kpis.pnl_net_usd by construction, keyed on the same MYT day as aggDaily), use
// it so the calendar/journal cover the WHOLE deployed history and their headline
// matches the Overview P&L breakdown net. Fall back to the client-derived
// aggDaily over the bounded streamed ledger for older payloads / a static
// snapshot that predates the server block (deploy skew). Same
// {date,pnl,n,wins,losses} shape either way.
function selectDaily(live) {
  const server = live && live.ledger_daily;
  if (Array.isArray(server)) return server;
  return aggDaily((live && live.ledger) || []);
}

// Historical analytics with server-preferred population — the row-based twin
// of selectDaily. live.ledger_analytics is computed over the WHOLE ledger and
// pre-sliced by the SAME range toggle the client offers ('All'|'30d'|'7d'),
// because these are COUNTS, not rows: the client cannot slice them itself the
// way dailyInRange slices the day array. Falls back to the client aggregates
// over the bounded streamed window for older payloads / a static snapshot / a
// venue view whose population the server never saw. `full` flags which path
// produced it so each panel can stamp itself honestly.
function selectAnalytics(live, range) {
  const block = live && live.ledger_analytics;
  const key = (range === '7d' || range === '30d') ? range : 'All';
  const server = (block && typeof block === 'object') ? block[key] : null;
  if (server && server.summary) {
    return {
      full: true,
      summary: srvPf(server.summary),
      coins: (server.coins || []).map(srvPf),
      slices: server.slices || [],
      histogram: server.histogram || [],
      streaks: server.streaks || { current: 0, longestW: 0, longestL: 0, last: [] },
    };
  }
  const rows = inRange((live && live.ledger) || [], range);
  return {
    full: false,
    summary: aggSummary(rows), coins: aggCoins(rows), slices: aggSlices(rows),
    histogram: aggHistogram(rows), streaks: aggStreaks(rows),
  };
}

// Strict JSON has no Infinity, so the producer ships a loss-free book as
// pf=null + pf_unbounded=true. Restore the client's Infinity sentinel so
// fmt.ratio still renders '∞' rather than the '—' it gives a null.
function srvPf(e) {
  if (!e || e.pf != null || !e.pf_unbounded) return e;
  return { ...e, pf: Infinity };
}

// Weekly P&L (Mon-anchored) derived from the daily series. d.date is already
// an MYT calendar date, so it is parsed AND read back in UTC — local getters
// would shift the day (and the Mon anchor) in any browser west of UTC+8.
function aggWeekly(daily) {
  const m = new Map();
  (daily || []).forEach((d) => {
    const dt = new Date(d.date + 'T00:00:00Z');
    if (Number.isNaN(dt.getTime())) return;
    const dow = (dt.getUTCDay() + 6) % 7; // Mon=0
    const mon = new Date(dt); mon.setUTCDate(dt.getUTCDate() - dow);
    const wk = `${mon.getUTCFullYear()}-${String(mon.getUTCMonth() + 1).padStart(2, '0')}-${String(mon.getUTCDate()).padStart(2, '0')}`;
    const e = m.get(wk) || { week: wk, pnl: 0, n: 0, wins: 0, losses: 0 };
    e.pnl += d.pnl; e.n += d.n; e.wins += d.wins; e.losses += d.losses;
    m.set(wk, e);
  });
  return [...m.values()].sort((a, b) => (a.week < b.week ? -1 : 1));
}

// Per-TRADE (group) P&L distribution histogram — a base bet and its top-ups
// contribute ONE bar-count at their combined P&L, not one per fill. Voided
// never-filled cancels are excluded (operator 2026-08-09): a $0 refund is not
// a break-even trade and must not pile up in the centre bin.
function aggHistogram(ledger, nbins = 11) {
  const vals = groupTrades(ledger).filter((g) => g.settled && g.hasPnl && !g.voided).map((g) => g.pnl);
  if (!vals.length) return [];
  const span = Math.max(0.01, Math.max(...vals.map((v) => Math.abs(v))));
  const lo = -span, hi = span, w = (hi - lo) / nbins;
  const bins = Array.from({ length: nbins }, (_, i) => ({ lo: lo + i * w, hi: lo + (i + 1) * w, mid: lo + (i + 0.5) * w, n: 0 }));
  vals.forEach((v) => {
    let idx = Math.floor((v - lo) / w);
    idx = Math.max(0, Math.min(nbins - 1, idx));
    bins[idx].n += 1;
  });
  return bins;
}

// Win rate — wins/(wins+losses) over TRADE GROUPS; null when nothing decided
// yet. The ONE client-side copy of the contract (server rollups carry their own
// win_rate, passed through verbatim by lifetimeSummary). Booked stop exits are
// already inside `losses`; voided cancels are in neither term.
function wr(wins, losses) {
  const decided = wins + losses;
  return decided ? wins / decided : null;
}

// Per-coin performance over TRADE GROUPS. A stop exit is a loss and its
// realized loss flows into the PF gross-loss; ROI denominator is the SETTLED
// stake (open trades have no realized return yet).
function aggCoins(ledger) {
  const m = new Map();
  groupTrades(ledger).forEach((g) => {
    if (!g.coin) return;
    const e = m.get(g.coin) || { coin: g.coin, bets: 0, resolved: 0, wins: 0, losses: 0, pnl: 0, gw: 0, gl: 0, stake: 0, stakeSettled: 0 };
    // `bets`/`resolved` are the "trades" column — voided never-filled cancels
    // are in neither (operator 2026-08-09), matching the server _analytics_entry.
    if (!g.voided) e.bets += 1;
    e.stake += g.size;
    if (g.settled && !g.voided) {
      const p = g.pnl || 0;
      e.resolved += 1; e.pnl += p;
      e.stakeSettled += g.size;
      if (g.win) { e.wins += 1; e.gw += Math.max(0, p); }
      else if (g.loss) { e.losses += 1; e.gl += Math.abs(Math.min(0, p)); }
    }
    m.set(g.coin, e);
  });
  return [...m.values()].map((e) => ({
    ...e,
    winRate: wr(e.wins, e.losses),
    pf: e.gl > 0 ? e.gw / e.gl : (e.gw > 0 ? Infinity : null),
    roi: e.stakeSettled > 0 ? e.pnl / e.stakeSettled : null,
  })).sort((a, b) => b.pnl - a.pnl);
}

// Per-slice (interval × entry_minute) performance over TRADE GROUPS — the
// slice is the BASE bet's, so a top-up placed a minute later never files the
// trade under a second slice. Voided never-filled cancels are excluded
// (operator 2026-08-09) so Σ(slice.n) equals the hero "trades" tile.
function aggSlices(ledger) {
  const m = new Map();
  groupTrades(ledger).filter((g) => g.settled && !g.voided).forEach((g) => {
    const iv = rowInterval(g.anchor) || '?';
    const key = `${iv}_e${g.entry_minute == null ? '?' : g.entry_minute}`;
    const e = m.get(key) || { slice: key, interval: iv, entry: g.entry_minute, n: 0, wins: 0, losses: 0, pnl: 0 };
    e.n += 1; e.pnl += g.pnl || 0;
    if (g.win) e.wins += 1; else if (g.loss) e.losses += 1;
    m.set(key, e);
  });
  return [...m.values()].map((e) => ({
    ...e, winRate: wr(e.wins, e.losses),
  })).sort((a, b) => (a.interval === b.interval ? (a.entry - b.entry) : (a.interval < b.interval ? -1 : 1)));
}

// Win/loss streaks over decided TRADE GROUPS in settlement order — a stop-loss
// exit is a realized loss, so it breaks a win streak like any LOSS.
function aggStreaks(ledger) {
  const dec = groupTrades(ledger).filter(gOutcome)
    .sort((a, b) => (a.settleMs || 0) - (b.settleMs || 0));
  let lw = 0, ll = 0, rw = 0, rl = 0;
  dec.forEach((g) => {
    if (g.win) { rw += 1; rl = 0; } else { rl += 1; rw = 0; }
    lw = Math.max(lw, rw); ll = Math.max(ll, rl);
  });
  let cur = 0, sign = 0;
  for (let i = dec.length - 1; i >= 0; i -= 1) {
    const w = dec[i].win ? 1 : -1;
    if (i === dec.length - 1) { sign = w; cur = 1; }
    else if (w === sign) cur += 1; else break;
  }
  return { current: cur * sign, longestW: lw, longestL: ll, last: dec.slice(-20).map((g) => (g.win ? 'w' : 'l')) };
}

// Risk-adjusted ratios computed from the daily series + equity curve.
// `opts` routes the FULL-ledger sources when the caller has them, so calmar /
// PF / expectancy stop describing the bounded stream window while sharpe and
// sortino (already fed by the server daily series) describe all of it:
//   opts.equity      — a lifetimeEquity curve; its OWN base anchors totalRet
//                      (mixing a lifetime curve's final with a windowed base is
//                      the silent mismatch this replaces)
//   opts.pf/.expectancy — the server analytics summary values
// Omitted ⇒ every value derives from the passed ledger exactly as before.
function aggRatios(ledger, base, dailyIn, opts) {
  const o = opts || {};
  const daily = dailyIn || aggDaily(ledger);
  const dr = daily.map((d) => d.pnl);
  const n = dr.length;
  const mean = n ? dr.reduce((a, b) => a + b, 0) / n : 0;
  const sd = n > 1 ? Math.sqrt(dr.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1)) : 0;
  const negs = dr.filter((x) => x < 0);
  const downside = negs.length ? Math.sqrt(negs.reduce((a, b) => a + b * b, 0) / negs.length) : 0;
  const ANN = Math.sqrt(365);
  const sharpe = sd > 0 ? (mean / sd) * ANN : null;
  const sortino = downside > 0 ? (mean / downside) * ANN : null;
  const eq = o.equity || aggEquity(ledger, base);
  const eqBase = o.equity ? eq.base : base;
  const totalRet = eqBase > 0 ? (eq.final - eqBase) / eqBase : 0;
  const calmar = eq.maxDD < 0 ? totalRet / Math.abs(eq.maxDD) : null;
  // PF over decided TRADE GROUPS: a stop-loss exit's realized loss counts in
  // the gross loss like any other loss.
  const groups = groupTrades(ledger);
  let gw = 0, gl = 0;
  groups.filter(gOutcome).forEach((g) => { const p = g.pnl || 0; if (p >= 0) gw += p; else gl += Math.abs(p); });
  const pf = o.pf !== undefined ? o.pf : (gl > 0 ? gw / gl : (gw > 0 ? Infinity : null));
  // Expectancy = mean realized P&L per booked TRADE (not per fill, and not
  // per voided never-filled cancel — their $0 dragged the mean toward zero
  // until 2026-08-09).
  const settled = groups.filter((g) => g.settled && g.hasPnl && !g.voided);
  const expectancy = o.expectancy !== undefined ? o.expectancy
    : (settled.length ? settled.reduce((a, g) => a + g.pnl, 0) / settled.length : null);
  return {
    sharpe, sortino, calmar, pf, expectancy,
    maxDD: eq.maxDD, maxDDAbs: eq.maxDDAbs, totalRet, tradingDays: n,
    winDays: daily.filter((d) => d.pnl > 0).length,
    lossDays: daily.filter((d) => d.pnl < 0).length,
  };
}

// Headline summary across an arbitrary ledger window, counted in TRADE GROUPS.
// win rate = wins/(wins+losses) with booked stop exits inside `losses` and
// their realized loss inside the PF gross loss; voided groups are excluded from
// the win rate and from the settled stake. avgEdge is the BASE bet's realized
// edge over SETTLED trades only (a pending trade has no realized edge to
// average yet) and ROI's denominator is the settled stake.
function aggSummary(ledger) {
  const groups = groupTrades(ledger);
  const settled = groups.filter((g) => g.settled);
  const outcome = groups.filter(gOutcome);
  const wins = outcome.filter((g) => g.win).length;
  const losses = outcome.filter((g) => g.loss).length;
  const voided = settled.filter((g) => g.voided).length;
  const pnl = settled.reduce((a, g) => a + (g.pnl || 0), 0);
  const stake = groups.reduce((a, g) => a + g.size, 0);
  const stakeSettled = settled.reduce((a, g) => a + (g.voided ? 0 : g.size), 0);
  let gw = 0, gl = 0;
  outcome.forEach((g) => { const p = g.pnl || 0; if (p >= 0) gw += p; else gl += Math.abs(p); });
  const avgEdge = (() => {
    const es = settled.filter((g) => !g.voided && typeof g.realized_edge === 'number').map((g) => g.realized_edge);
    return es.length ? es.reduce((a, b) => a + b, 0) / es.length : null;
  })();
  return {
    // `settled` = settled groups that actually OPENED (operator 2026-08-09):
    // a voided never-filled cancel is not a trade, so it sits in `voided`
    // only. total == settled + open + voided, three disjoint buckets — the
    // same split the server _analytics_entry emits, so this fallback and the
    // server block can never report a different trade count.
    total: groups.length,
    settled: settled.length - voided,
    open: groups.length - settled.length,
    wins, losses, voided,
    winRate: wr(wins, losses),
    pnl, stake, stakeSettled, roi: stakeSettled > 0 ? pnl / stakeSettled : null,
    pf: gl > 0 ? gw / gl : (gw > 0 ? Infinity : null), avgEdge,
  };
}

// Lifetime headline summary for the ACTIVE strategy. Prefers the server-side
// rollup (window.LIVE.ledger_rollup) — computed over the FULL ledger filtered
// to the deployed sleeves — so the headline win-rate / P&L / sample size do
// NOT depend on how many rows the SSE stream has delivered this session. Falls
// back to a windowed client aggregate when the rollup is absent (static
// snapshot / legacy payload). `lifetime` flags which path produced it.
function lifetimeSummary(live, fallbackLedger) {
  const r = live && live.ledger_rollup;
  if (r && typeof r.total === 'number') {
    return {
      lifetime: true,
      total: r.total, decided: r.decided, wins: r.wins,
      // THE settled-trade count: settled groups that actually opened. Read
      // the server scalar; a payload that predates it (deploy skew) falls
      // back to total − open − voided rather than the old total − open, which
      // silently counted the voided never-filled cancels as trades.
      settled: (typeof r.settled_opened === 'number')
        ? r.settled_opened
        : Math.max(0, (r.total || 0) - (r.open || 0) - (r.voided || 0)),
      // Booked stop exits are already inside the server's `losses` (2026-08-05
      // — the standalone `stops` column is gone from the payload). A legacy
      // payload that still carries one folds it in here so the client can never
      // render a stop-blind loss count.
      losses: (r.losses || 0) + (r.stops || 0),
      voided: r.voided || 0,
      open: r.open, winRate: r.win_rate, pnl: r.pnl_net, stake: r.stake, roi: r.roi,
      from: r.first_ts ? fmt.dateMyt(r.first_ts) : null,
      to: r.last_ts ? fmt.dateMyt(r.last_ts) : null,
    };
  }
  const s = aggSummary(fallbackLedger || []);
  return {
    lifetime: false,
    total: s.total, decided: s.wins + s.losses, wins: s.wins, losses: s.losses,
    voided: s.voided, settled: s.settled,
    open: s.open, winRate: s.winRate, pnl: s.pnl, stake: s.stake, roi: s.roi,
    from: null, to: null,
  };
}

// Lifetime equity curve for the ACTIVE strategy. Prefers the server-side series
// (window.LIVE.ledger_equity) — cumulative realized P&L over the FULL ledger,
// anchored so the right edge equals realized_capital — so the chart shows
// whole-strategy history, not the ~50-row session window. Falls back to the
// passed windowed curve (anchoredEquity result) for the static-snapshot/legacy
// path. Mirrors lifetimeSummary's prefer-server pattern.
function lifetimeEquity(live, fallback) {
  // Prefer the PER-TRADE ledger curve (window.LIVE.ledger_equity) — one point
  // per settled trade group (operator 2026-08-09: the x-axis must be the TRADE
  // number, matching the hero "trades" tile, not wall-clock time; voided
  // never-filled cancels are excluded server-side). The hourly cash-balance
  // curve (cash_equity) stays as the fallback, then the windowed
  // anchoredEquity result. The equity TILE stays cash-basis regardless — only
  // the chart's x-axis changed.
  const ledgerCurve = live && live.ledger_equity;
  const e = ledgerCurve || (live && live.cash_equity);
  if (e && Array.isArray(e.points) && e.points.length >= 2) {
    return {
      // basis lets the chart legend label itself honestly — 'paper' (per-trade
      // ledger cumulation) vs 'cash' (hourly balance fallback when the ledger
      // curve is absent) — instead of hard-coding one story.
      lifetime: true, basis: ledgerCurve ? 'paper' : 'cash',
      points: e.points, base: e.base, final: e.final,
      peak: e.peak, maxDD: e.maxDD, maxDDAbs: e.maxDDAbs, n: e.n,
    };
  }
  return { lifetime: false, basis: 'paper', ...(fallback || { points: [], base: 0, final: 0, peak: 0, maxDD: 0, maxDDAbs: 0, n: 0 }) };
}

// Date window covered by the available ledger (for honest "window" labels).
function ledgerWindow(ledger) {
  const ts = ledger.map((r) => settleMs(r) || betMs(r)).filter((x) => x != null).sort((a, b) => a - b);
  if (!ts.length) return null;
  return { from: fmt.dateMyt(ts[0]), to: fmt.dateMyt(ts[ts.length - 1]), n: ledger.length };
}

// ===========================================================================
// v2 "step1last" resolution grouping (cross-attempt; one row per RESOLUTION).
//
// A RESOLUTION is (coin, interval, window_start, side) — NOT the sleeve. A
// single resolution can draw SEVERAL attempts (per-second retries, T3 top-ups,
// and historically a second sleeve on the same window), first-fill-wins. They
// MUST fold into ONE row so a missed resolution (no attempt filled) is counted
// ONCE, not once per attempt. The pre-fix bug keyed on the sleeve, so XRP 15m
// DOWN attempted twice rendered as TWO rows, each showing the full missed
// −$2.63 — double-counting the miss.
//
// interval is taken from the row's own field when present, else derived from
// the sleeve label (subminute_5m/subminute_15m → "5m"/"15m"). Keeping the
// interval in the key (not just window_start_ts) holds a 5m and a 15m window
// that share a boundary epoch (e.g. both on a :00) in separate resolutions.
//
// FILLS fold into POSITIONS, not rows-per-fill (operator 2026-08-05). A base
// entry and the T3 top-ups that landed in the same market window are ONE CLOB
// balance that settles once, so they collapse into one row carrying Σ size,
// Σ P&L and the SIZE-WEIGHTED effective price — exactly what the Trade journal
// shows — with an "N in 1" badge and the individual fills kept in the
// drill-down. The membership test is the SHARED `groupTrades` helper (the
// journal's own, keyed on venue·coin·interval·window_start·side·wallet), never
// a second copy of the rule, so the two tables can never disagree about what
// one trade is. A resolution that genuinely holds two positions (a second
// sleeve/wallet on the same window) still renders two rows.
// The no-fill attempts (rejects + ghost/$0 accepts) collapse into ONE no-fill row
// per resolution (shown only when the resolution produced no fill at all); a
// resolution that did fill attaches its remaining attempts to the FIRST fill's
// drill-down. Missed (counterfactual, PAPER) PnL is the single attempt
// carrying the full intended size (max requested_usd) — deterministic,
// size-honest, and counted ONCE per resolution.
// ===========================================================================
function sleeveInterval(sleeve) {
  const m = /(\d+)m/.exec(sleeve || '');
  return m ? m[1] + 'm' : null;
}
// MID band badge from the sleeve label.
function sleeveBand(sleeve) {
  const s = String(sleeve || '').toLowerCase();
  if (/subminute/.test(s)) return 'MID';
  return null;
}
// Deterministic band badge for a resolution.
function bandBadge(bands) {
  return bands.has('MID') ? 'MID' : null;
}
function resTsMs(t) { const d = parseUtcTimestamp(t); return d ? d.getTime() : null; }
function resIntervalMin(iv) { const n = parseInt(iv, 10); return isFinite(n) && n > 0 ? n : 15; }
function resWindowStartMs(t, mins) {
  const m = resTsMs(t);
  if (m == null) return null;
  const w = mins * 60000;
  return Math.floor(m / w) * w;
}
// interval for a v2 bet ("interval": "15m") OR a reject (from the sleeve label).
function resInterval(row) {
  if (!row) return null;
  if (typeof row.interval === 'string' && /\d+m/.test(row.interval)) return row.interval;
  if (row.interval_min === 5 || row.interval_min === 15) return row.interval_min + 'm';
  return sleeveInterval(row.sleeve);
}
function resWindowMs(row, iv) {
  if (row && row.window_start_ts != null) return row.window_start_ts * 1000;
  return resWindowStartMs(row && row.time, resIntervalMin(iv));
}
function resKey(coin, iv, wsMs, side) {
  return `${coin}|${iv || '?'}|${wsMs == null ? '?' : wsMs}|${side || '?'}`;
}
function offsetFromTs(t, ws) {
  const m = resTsMs(t);
  if (m == null || ws == null) return null;
  return Math.max(0, Math.round((m - ws) / 1000));
}
// A reject's would-be outcome: side YES→UP / NO→DOWN scored vs resolved_outcome.
function rejectOutcome(r) {
  if (r.resolved_outcome == null) return 'PENDING';
  const dir = r.side === 'YES' ? 'UP' : r.side === 'NO' ? 'DOWN' : r.side;
  return String(r.resolved_outcome).toUpperCase() === dir ? 'WIN' : 'LOSS';
}

// ── v2 bet → the ledger row shape `groupTrades` reads ────────────────
// The sub-minute payload names the same facts differently from the ledger
// stream the journal consumes (`time` not `bet_ts`, `interval` "5m" not
// `interval_min` 5, `stake_usd`/`filled_usd` not `bet_size`). This adapter
// renames them — and NOTHING else — so the sub-minute table can call the
// journal's grouping helper instead of growing a second copy of the rule.
//
// `effective_bet_size` is the REALIZED fill value (`filled_usd`), so the size
// groupTrades sums and size-weights the price by is the money that actually
// landed on the position, not the intent (the intent is the row's `target`,
// summed separately). `_v2` carries the untouched payload row back out for
// rendering — no field of the original is lost or overwritten.
function subminuteFillRow(b) {
  return {
    ...b,
    bet_ts: b.bet_ts != null ? b.bet_ts : b.time,
    interval_min: b.interval_min != null
      ? b.interval_min : resIntervalMin(resInterval(b)),
    effective_bet_size: b.filled_usd != null ? b.filled_usd : b.stake_usd,
    _v2: b,
  };
}

// Build ONE renderable row from a resolution + a chosen trade group (the fills
// of one CLOB position, or null for a no-fill row) and the no-fill attempts
// folded onto it.
function buildResolutionRow(res, opts) {
  const group = opts.group || null;   // a groupTrades group, or null
  // The payload rows behind the group, base entry first (groupTrades' order).
  const groupFills = group ? group.fills.map((f) => f._v2 || f) : [];
  const anchor = groupFills.length ? groupFills[0] : null;
  const isFillRow = opts.isFillRow;
  const noFills = opts.noFills || [];  // [{kind:'ghost'|'reject', row}]
  const rejects = noFills.filter((x) => x.kind === 'reject').map((x) => x.row);
  const ghosts = noFills.filter((x) => x.kind === 'ghost').map((x) => x.row);

  const nRejects = rejects.length;
  // ATTEMPTS stay per-ORDER: a top-up is its own order attempt even though it
  // lands in the base bet's position, so folding fills into one row must not
  // shrink this count.
  const nAccepted = groupFills.length + ghosts.length;  // ghosts = accepted-but-$0
  const attempts = nAccepted + nRejects;

  // TARGET is the intent (Σ of the fills' requested stakes — a top-up asked for
  // its own money), FILLED the realized Σ groupTrades already computed.
  // requested_usd (ledger.target_usd, exported 2026-08-09) is the sizer's
  // intent; stake_usd is the REALIZED fill value, kept only as the legacy
  // fallback — comparing filled against Σ stake_usd was filled-vs-itself, so
  // a partially filled FAK could never read PARTIAL.
  const target = anchor
    ? groupFills.reduce((a, f) => a + (f.requested_usd != null ? f.requested_usd
        : (f.stake_usd != null ? f.stake_usd : 0)), 0)
    : (rejects.reduce((m, x) => Math.max(m, x.requested_usd || 0), 0)
       || ghosts.reduce((m, x) => Math.max(m, x.stake_usd || 0), 0) || null);
  const filled = group ? (group.size || 0) : 0;
  const fillRatio = (target && target > 0) ? filled / target : (anchor ? 1 : 0);

  let status;
  if (anchor) {
    // A HISTORICAL stop-closed position (exit_kind='stop_loss') is a
    // booked-once early exit: its pnl_net is a realized loss and it merges into
    // its resolution like any settled bet. The standalone STOP badge was
    // retired with the engine's stop-loss (2026-08-05) — these rows now render
    // as the LOSS they are, matching the journal. Discriminate ONLY on
    // exit_kind so legacy voided CANCELLED rows (exit_kind NULL) keep their
    // current display semantics instead of being mislabelled losses.
    status = (anchor.exit_kind === 'stop_loss') ? 'LOSS'
      : !(filled > 0) ? 'GHOST_FILL'
      // PARTIAL is a RELATIVE shortfall (2% of the stake, floored at half a
      // cent). An absolute half-cent tolerance flagged every venue-rounded
      // fill — a share quantity is integer-floored and matched at the resting
      // ask, so a genuinely complete fill routinely lands a percent or two
      // under the intended stake and read as "partial" on every row.
      : (target != null && filled < target * 0.98 - 0.005) ? 'PARTIAL'
      // The GROUP's single verdict (groupTrades): a position with any fill
      // still open reads PENDING, otherwise its settled WIN/LOSS.
      : (group.outcome || 'PENDING');
  } else {
    status = ghosts.length ? 'GHOST_FILL' : 'NO_FILL';   // preserve the ghost signal
  }

  // Counterfactual would-be PnL — PAPER money — counted ONCE per resolution and
  // only on rows with no real fill. The reject telemetry already scores each
  // reject vs resolved_outcome (missed_pnl); the representative attempt is the
  // one carrying the full intended size (max requested_usd), matching the
  // server-side reject_stats dedup so the row and the aggregate agree.
  const graded = rejects.filter((x) => x.missed_pnl != null && isFinite(x.missed_pnl));
  const missedPnl = (!isFillRow && graded.length)
    ? graded.reduce((rep, x) => ((x.requested_usd || 0) > (rep.requested_usd || 0) ? x : rep),
                    graded[0]).missed_pnl
    : null;

  const ws = res.ws;
  const rows = [];
  // Journal vocabulary for the fills inside one position: the base entry, then
  // the top-ups in the order they landed. `label` is null on a ghost/$0 accept
  // (never part of a position) so only real fills carry the base/top-up story.
  let topupN = 0;
  const fillLabel = (f) => {
    if (!groupFills.includes(f)) return null;
    if (!f.is_topup) return 'base';
    topupN += 1;
    return 'top-up ' + topupN;
  };
  const pushFill = (f) => {
    const stop = f.exit_kind === 'stop_loss';
    const exec = execPrice(f);
    const fill = fillPrice(f);
    rows.push({
      kind: 'fill', side: f.side, band: sleeveBand(f.sleeve),
      fillLabel: fillLabel(f),
      offset: f.offset_sec != null ? f.offset_sec : offsetFromTs(f.time, ws),
      price: exec,
      requested: f.requested_usd != null ? f.requested_usd : f.stake_usd,
      filled: f.filled_usd,
      fillPx: fill, drift: null, pnl: f.pnl_net, missed: null,
      outcome: stop ? 'LOSS'
        : (f.ghost || !(f.filled_usd > 0)) ? 'GHOST_FILL' : (f.outcome || 'PENDING'),
      time: f.time,
      isStop: stop,
      exitPrice: stop ? f.exit_price : null,   // realized stop-sell VWAP
      ifHeldPnl: stop ? f.stop_if_held_pnl : null,  // held-to-resolution counterfactual
      // Unrealized mark on an OPEN (PENDING) fill — marked-to-book, else entry.
      markPx: f.mark_px != null ? f.mark_px : null,
      markSource: f.mark_source || null,
    });
  };
  groupFills.forEach(pushFill);
  ghosts.forEach(pushFill);
  rejects.forEach((r) => rows.push({
    kind: 'reject', side: r.side, band: sleeveBand(r.sleeve),
    offset: r.offset_sec != null ? r.offset_sec : offsetFromTs(r.time, ws),
    price: r.limit_price, requested: r.requested_usd, filled: 0,
    // `drift` = requote_ask − our limit: a PRICE metric (how far the book
    // moved after the reject), never a cause. The refuted probe taxonomy that
    // used to read a cause out of it was deleted 2026-07-30.
    fillPx: null, drift: r.requote_drift, pnl: null, missed: r.missed_pnl,
    // Per-attempt ON-CHAIN verdict (server miss_forensics job) — the ONLY
    // miss-cause axis: the trade tape's answer to faster-taker vs maker-cancel,
    // which a book re-read physically cannot give. All null on older payloads /
    // before the forensics job has reached this row.
    chainVerdict: r.chain_verdict != null ? r.chain_verdict : null,
    chainBeatDt: r.chain_beat_dt_s != null ? r.chain_beat_dt_s : null,
    chainBeatPrice: r.chain_beat_price != null ? r.chain_beat_price : null,
    chainBeatSize: r.chain_beat_size != null ? r.chain_beat_size : null,
    chainRival: r.chain_rival_addr != null ? r.chain_rival_addr : null,
    outcome: rejectOutcome(r), time: r.time,
  }));
  rows.sort((x, y) => (x.offset == null ? 1e9 : x.offset) - (y.offset == null ? 1e9 : y.offset));
  rows.forEach((row, i) => { row.n = i + 1; });

  const lastReject = rejects.length ? rejects[rejects.length - 1] : null;
  const lastGhost = ghosts.length ? ghosts[ghosts.length - 1] : null;
  const displayTime = anchor ? anchor.time
    : (lastReject ? lastReject.time : (lastGhost ? lastGhost.time : null));
  const sortTs = rows.reduce((mx, row) => Math.max(mx, resTsMs(row.time) || 0), 0);

  return {
    key: opts.key,
    coin: res.coin,
    interval: res.interval,
    side: res.side || (anchor && anchor.side) || (lastReject && lastReject.side)
      || (lastGhost && lastGhost.side) || null,
    band: opts.band,
    anchor: anchor || null,
    target, filled, fillRatio,
    attempts, nRejects,
    // How many FILLS this one position took (1 = a plain base bet). Drives the
    // journal's "N in 1" badge and the Σ / size-weighted tooltips.
    nFills: groupFills.length,
    status,
    // Σ over the position's fills (null only when no fill reported one) — the
    // same fold the journal applies, so both tables book the same dollars.
    pnl: group ? group.pnl : null,
    missedPnl,
    // HISTORICAL stop-loss early-exit surface: the row's realized pnl (a loss)
    // is already carried in `pnl`; these expose the exit price and the
    // held-to-resolution counterfactual (NULL until the sweep backfills
    // won_up). Read-only history — the engine no longer books stop exits.
    isStop: !!(anchor && anchor.exit_kind === 'stop_loss'),
    exitPrice: (anchor && anchor.exit_kind === 'stop_loss') ? anchor.exit_price : null,
    ifHeldPnl: (anchor && anchor.exit_kind === 'stop_loss') ? anchor.stop_if_held_pnl : null,
    // SIZE-WEIGHTED across the position's fills (groupTrades) — the effective
    // price actually paid, not the base bet's alone.
    execPx: group ? group.avgExec : null,
    fillPx: group ? group.avgFill : null,
    attemptRows: rows,
    displayTime, sortTs, ws,
  };
}

function groupResolutions(bets, rejects) {
  // Pass 1 — bucket every bet & reject into its RESOLUTION (coin, interval,
  // window_start, side). Fills and no-fills are kept apart within a resolution.
  const res = new Map();
  const getRes = (coin, iv, wsMs, side) => {
    const k = resKey(coin, iv, wsMs, side);
    let r = res.get(k);
    if (!r) {
      r = { key: k, coin, interval: iv || null, side: side || null, ws: wsMs,
            fills: [], noFills: [], bands: new Set() };
      res.set(k, r);
    }
    return r;
  };

  (bets || []).forEach((b) => {
    const iv = resInterval(b);
    const wsMs = resWindowMs(b, iv);
    const r = getRes(b.coin, iv, wsMs, b.side);
    const band = sleeveBand(b.sleeve);
    if (band) r.bands.add(band);
    if (!b.ghost && b.filled_usd > 0) r.fills.push(b);
    else r.noFills.push({ kind: 'ghost', row: b });  // accepted-but-$0 = a no-fill attempt
  });

  (rejects || []).forEach((rj) => {
    const iv = resInterval(rj);
    const wsMs = resWindowMs(rj, iv);   // exact when window_start_ts present; else
                                        // reconstructed from time + interval (legacy
                                        // payloads only — all live rejects carry it).
    const r = getRes(rj.coin, iv, wsMs, rj.side);
    const band = sleeveBand(rj.sleeve);
    if (band) r.bands.add(band);
    r.noFills.push({ kind: 'reject', row: rj });
  });

  // Pass 2 — emit rows: each fill its own row; no-fills fold into one row.
  const out = [];
  res.forEach((r) => {
    const badge = bandBadge(r.bands);
    if (r.fills.length > 0) {
      // The SHARED journal fold: base entry + its top-ups are ONE position, so
      // they become ONE row. Only a genuinely second position in this window
      // (different sleeve/wallet/venue) survives as a second group.
      const positions = groupTrades(r.fills.map(subminuteFillRow));
      positions.forEach((g, idx) => {
        out.push(buildResolutionRow(r, {
          key: `${r.key}#f${idx}`,
          group: g,
          noFills: idx === 0 ? r.noFills : [],  // fold retries/other-band onto the first fill
          band: badge,
          isFillRow: true,
        }));
      });
    } else {
      out.push(buildResolutionRow(r, {
        key: r.key,
        group: null,
        noFills: r.noFills,
        band: badge,
        isFillRow: false,
      }));
    }
  });

  out.sort((x, y) => (y.sortTs || 0) - (x.sortTs || 0));  // newest resolution first
  return out;
}

// ── server-classified missed trades (live.v2.missed_trades) ───────────
// The snapshot builder emits ONE row per missed RESOLUTION (attempts deduped,
// at-limit counterfactual, the ON-CHAIN verdict + its chain_winnable flag, and
// a non-causal miss_kind). The client keys
// them on the same resolution identity groupResolutions uses — but on the
// DIRECTION axis (YES→UP / NO→DOWN) so a YES/UP label drift between tables
// can never split a resolution. Both helpers are null-safe: an older payload
// without missed_trades yields an empty index and every lookup returns null
// (the page falls back to the plain NO_FILL rendering).
function missedTradeKey(coin, interval, wsSec, dir) {
  return `${coin}|${interval || '?'}|${wsSec == null ? '?' : wsSec}|${dir || '?'}`;
}
function missedTradeIndex(list) {
  const idx = new Map();
  (list || []).forEach((t) => {
    if (!t || !t.coin) return;
    idx.set(missedTradeKey(t.coin, t.interval, t.window_start_ts,
      directionFromSide(t.side)), t);
  });
  return idx;
}
// Lookup for a groupResolutions row (g.ws is in ms; g.side is YES/NO).
function missedTradeForGroup(idx, g) {
  if (!idx || !idx.size || !g) return null;
  const wsSec = g.ws != null ? Math.round(g.ws / 1000) : null;
  return idx.get(missedTradeKey(g.coin, g.interval, wsSec,
    directionFromSide(g.side))) || null;
}

// Bounded-DOM pager math (shared by the journal + sub-minute resolution tables).
// Returns the clamped page, the page count, the visible slice, and the 1-based
// "showing start–end of total" bounds so every pager footer renders identically
// (see window.TablePager in shell.jsx).
function paginate(items, page, size) {
  const total = items.length;
  const pages = Math.max(1, Math.ceil(total / size));
  const pg = Math.min(page, pages - 1);
  const off = pg * size;
  return {
    pages, pg, total,
    slice: items.slice(off, off + size),
    start: total ? off + 1 : 0,
    end: Math.min(off + size, total),
  };
}

// ===========================================================================
// React hooks
// ===========================================================================
// Expand/collapse set for keyed table rows (journal + sub-minute resolutions): the
// open-row key set plus the toggle handler that adds/removes a key.
function useOpenSet() {
  const [openSet, setOpenSet] = React.useState(() => new Set());
  const toggle = (k) => setOpenSet((s) => {
    const n = new Set(s); n.has(k) ? n.delete(k) : n.add(k); return n;
  });
  return { openSet, toggle };
}
function useLive() {
  const [, force] = React.useReducer((x) => (x + 1) | 0, 0);
  React.useEffect(() => {
    const on = () => force();
    window.addEventListener('cryptobot:live', on);
    window.addEventListener('cryptobot:state', on);
    return () => { window.removeEventListener('cryptobot:live', on); window.removeEventListener('cryptobot:state', on); };
  }, []);
  return window.LIVE || EMPTY_LIVE;
}
function useSecond() {
  const [, force] = React.useReducer((x) => (x + 1) | 0, 0);
  React.useEffect(() => { const h = setInterval(force, 1000); return () => clearInterval(h); }, []);
}

// ── latency: the venue taker-delay shortfall (Latency page) ──────────
// How much of the venue's DOCUMENTED uncancellable taker delay a measured POST
// figure did NOT already contain. Lives here (not in pages-latency.jsx) so the
// invariant can be unit-tested, the way groupResolutions does for Subminute.
//
// An ACCEPTED marketable order's POST round trip cannot return before the
// enforced delay has elapsed, so wherever accepted orders were measured the
// delay is already inside `post` and the shortfall is 0 — the total-latency
// column adds nothing and cannot double-count. A REJECT-only window measures
// post ~0 (a 400 skips matching and never waits), and there the shortfall is
// the whole documented delay, so the total still never understates the wall.
// Null/absent documented delay (predict.fun documents none) → 0.
function takerDelayShortfall(takerDelayMs, postMs) {
  if (takerDelayMs == null || !isFinite(takerDelayMs)) return 0;
  const measured = typeof postMs === 'number' && isFinite(postMs) ? postMs : 0;
  return Math.max(0, takerDelayMs - measured);
}

// ── latency: which stage rows ADD UP, and what the rest of the tick is ──
// Operator 2026-08-05: "when sum up all ms, why not equal total ms?"
//
// Only these five stages are DISJOINT phases of one tick, so only these are
// summed. `sign` and `post` are measured INSIDE `order` (the whole
// place_gtc_limit_order envelope) and the taker-delay row is that same POST
// round trip a third time — adding them would count the same milliseconds two
// and three times over, which is exactly why the column never tallied.
const LATENCY_ADDITIVE_STAGES = ['spot', 'ctx', 'gamma', 'book', 'order'];

function latencyStageSum(stages, col) {
  let sum = 0;
  LATENCY_ADDITIVE_STAGES.forEach((k) => {
    const s = stages && stages[k];
    const v = s ? s[col] : null;
    if (typeof v === 'number' && isFinite(v)) sum += v;
  });
  return sum;
}

// The residual that makes the column tally: total − Σ(additive stages). It is
// NOT decoration — it is a real, unmeasured quantity, and it absorbs two
// different things at once:
//
//   1. TICK WORK WITH NO STAGE OF ITS OWN. Only network-touching phases are
//      instrumented (pipeline.TickStats), so scoring, gates, sizing, the
//      SQLite writes, the post-POST tail (T3 pass, next-window prefetch, trace
//      write) and the held-position book reads (`ms_book_stop`, split out of
//      `book`) are inside `total` and inside NO row above it.
//   2. PERCENTILE NON-ADDITIVITY. Each cell is an independent order statistic:
//      a p50 of a sum is not the sum of p50s, and the order stages aggregate
//      ORDER-BEARING ticks only while spot/ctx/gamma/book/total aggregate every
//      tick (compare the `n` the payload carries per stage). Those are
//      different populations, so the components can even EXCEED the same
//      column's total.
//
// It is therefore returned SIGNED — never clamped. Clamping at zero would
// silently break the one property the table now promises the operator: the
// visible rows add to the visible total, exactly, in every column.
function latencyResidual(stages, col, totalMs) {
  if (typeof totalMs !== 'number' || !isFinite(totalMs)) return null;
  return totalMs - latencyStageSum(stages, col);
}

// ── publish to the shared namespace ───────────────────────────────────
window.CB = {
  VENUE_META, DASHBOARD_TZ, INITIAL_BANKROLL, EMPTY_LIVE, SETTLED,
  fmt, tone, directionFromSide, isSettled, isVoided,
  isWinRow, isLossRow, settleTs, settleMs, betMs,
  groupTrades, tradeGroupKey, gOutcome,
  rowStake, sliceRef, execPrice, fillPrice, liveCoins, inRange, dailyInRange, parseUtcTimestamp,
  venueOf, venueUniverse,
  normVenueSel, rowVenue, rowKey, venueFilter, scopeLive, scopedCoins,
  stripCash, predictfunWallet,
  aggEquity, aggDaily, selectDaily, selectAnalytics, aggWeekly, aggHistogram, aggCoins,
  aggSlices, aggStreaks, aggRatios, aggSummary, lifetimeSummary, lifetimeEquity,
  strategyBase, money, withPaperBasis, ledgerWindow,
  groupResolutions, paginate, takerDelayShortfall,
  LATENCY_ADDITIVE_STAGES, latencyStageSum, latencyResidual,
  missedTradeIndex, missedTradeForGroup,
  useOpenSet, useLive, useSecond,
};
})();
