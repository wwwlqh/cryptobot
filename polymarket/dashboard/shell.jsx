// ===========================================================================
// shell.jsx — app chrome (sidebar, topbar, halt banner) + shared UI atoms
// (KpiCell, Panel, pills, meters, CoinCell). Loaded after data.jsx + charts.jsx.
// ===========================================================================
(function () {
const { fmt, tone, useSecond, directionFromSide, venueOf } = window.CB;

// ── venue identity ────────────────────────────────────────────────────── The payload carries
// `venue`, and every venue-specific label is derived from it.
const VENUE_LABEL = { polymarket: 'Polymarket' };
const VENUE_LABEL_CAPS = { polymarket: 'POLYMARKET' };
function venueLabel(live) { return VENUE_LABEL[venueOf(live)] || VENUE_LABEL.polymarket; }
function venueLabelCaps(live) { return VENUE_LABEL_CAPS[venueOf(live)] || VENUE_LABEL_CAPS.polymarket; }

// ── icons (lucide-style, 24-grid, currentColor) ──────────────────────
const I = {
  overview: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="7" height="9" /><rect x="14" y="3" width="7" height="5" /><rect x="14" y="12" width="7" height="9" /><rect x="3" y="16" width="7" height="5" /></svg>,
  journal: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><path d="M3 6h18" /><path d="M3 12h18" /><path d="M3 18h12" /></svg>,
  calendar: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="4" width="18" height="17" rx="2" /><path d="M16 2v4M8 2v4M3 10h18" /></svg>,
  operator: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3l8 4v5c0 5-3.5 8-8 9-4.5-1-8-4-8-9V7l8-4z" /><path d="M9 12l2 2 4-5" /></svg>,
  latency: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 3" /></svg>,
  subminute: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><path d="M13 2L3 14h7l-1 8 10-12h-7l1-8z" /></svg>,
  spec: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z" /><path d="M14 3v5h5" /><path d="M9 13h6M9 17h4" /></svg>,
  glyph: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round"><path d="M5 16V12M10 16V8M15 16V10M20 16V6" /></svg>,
  copy: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><rect x="9" y="9" width="11" height="11" rx="2" /><path d="M5 15V5a2 2 0 0 1 2-2h10" /></svg>,
};

// ── page registry (order = sidebar order) ────────────────────────────
const NAV = [
  { section: 'Trading' },
  { key: 'overview', label: 'Overview', title: 'Overview', range: true },
  { key: 'operator', label: 'Operator', title: 'Operator' },
  { key: 'subminute', label: 'Subminute', title: 'Subminute' },
  { section: 'Analytics' },
  { key: 'journal', label: 'Trade journal', title: 'Trade journal', range: true },
  { key: 'calendar', label: 'Calendar', title: 'Calendar' },
  { key: 'latency', label: 'Latency', title: 'Tick & order latency' },
  { section: 'Config' },
  { key: 'spec', label: 'Strategy', title: 'Strategy — Subminute' },
];

// ── derived chrome state ─────────────────────────────────────────────
function globalMode(live) {
  // Trading money-mode. Default to DRY (paper): a wallet is LIVE only when
  // explicitly promoted, and LIVE wallets always ride in kpis.wallets — so a
  // payload with no LIVE wallet (including an older/minimal payload that omits
  // the wallets array entirely) is paper-trading. Never "idle", never a false
  // LIVE — the safe default for a status badge is the conservative DRY.
  const ws = (live.kpis && live.kpis.wallets) || [];
  const modes = ws.map((w) => (w.mode || 'DRY').toUpperCase());
  if (modes.some((m) => m === 'LIVE')) return modes.every((m) => m === 'LIVE') ? 'LIVE' : 'MIXED';
  return 'DRY';
}
const SSE = {
  // "ONLINE" = the SSE stream is connected (distinct from LIVE trading mode,
  // which is the amber/green badge). Avoids reading green "LIVE" as real-money.
  live: { cls: 'live', label: 'ONLINE' },
  connecting: { cls: 'connecting', label: 'CONNECTING' },
  reconnecting: { cls: 'reconnecting', label: 'RECONNECTING' },
  rejected: { cls: 'rejected', label: 'REJECTED' },
  mock: { cls: 'mock', label: 'OFFLINE' },
};

// ── Sidebar ──────────────────────────────────────────────────────────
function Sidebar({ page, setPage, live }) {
  const k = live.kpis;
  const sse = window.__SSE_STATE || 'connecting';
  const counts = {
    // Open trades (CLOB positions), not open fills, so the badge matches the Journal's "open
    // positions" tile. Older payloads without the grouped count fall back to the per-fill count.
    journal: (k && (k.n_pending_trades != null ? k.n_pending_trades : k.n_pending)) || null,
    operator: live.operator && live.operator.state && live.operator.state !== 'LIVE' ? 1 : null,
  };
  const health = sse === 'live' ? (k && k.trading_halted ? 'warn' : 'live') : (sse === 'connecting' ? 'warn' : 'bad');
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">{I.glyph}</div>
        <div>
          <div className="brand-name">cryptobot</div>
          <div className="brand-sub">{venueLabel(live)} 5m/15m journal</div>
        </div>
      </div>
      <div className="nav-section">
        {NAV.map((n, i) => n.section ? (
          <div key={'s' + i} className="nav-heading">{n.section}</div>
        ) : (
          <div key={n.key} className={'nav-item' + (page === n.key ? ' active' : '')} onClick={() => setPage(n.key)}>
            {I[n.key]}
            <span>{n.label}</span>
            {counts[n.key] != null && <span className="nav-count">{counts[n.key]}</span>}
          </div>
        ))}
      </div>
      <div className="sidebar-foot">
        <div className="sidebar-status">
          <span className={'health-dot ' + health} />
          <span>{sse === 'live' ? 'scanner online' : (SSE[sse] || SSE.mock).label.toLowerCase()}</span>
        </div>
      </div>
    </aside>
  );
}

// ── Topbar ───────────────────────────────────────────────────────────
function Topbar({ meta, live, range, setRange }) {
  useSecond();
  const mode = globalMode(live);
  const sse = window.__SSE_STATE || 'connecting';
  const s = SSE[sse] || SSE.mock;
  const modeCls = mode === 'LIVE' ? 'badge-live' : mode === 'DRY' ? 'badge-dry' : 'badge-mixed';
  return (
    <div className="topbar">
      <div>
        <div className="topbar-title">{meta.title}</div>
      </div>
      <div className="topbar-spacer" />
      <div className="topbar-right">
        {/* Passive badge naming the venue this console shows. */}
        <span className="badge badge-venue">
          {venueLabelCaps(live)}
        </span>
        {meta.range && setRange && (
          <div className="seg">
            {['7d', '30d', 'All'].map((r) => (
              <button key={r} className={range === r ? 'active' : ''} onClick={() => setRange(r)}>{r}</button>
            ))}
          </div>
        )}
        {/* Wall clock (client-side, 1 Hz via useSecond) — tick_ts only moves
            when a payload lands, which made the clock stutter at the SSE
            cadence. Payload freshness still shows as "tick Ns ago". */}
        <span className="topbar-clock"><b>{fmt.hmsMyt(Date.now())}</b> MYT · tick {fmt.agoShort(live.tick_ts)}</span>
        <span className={'badge ' + modeCls}>{mode === 'DRY' ? 'DRY · paper' : mode}</span>
        <span className={'conn ' + s.cls}><span className="d" />{s.label}</span>
      </div>
    </div>
  );
}


// ── Halt banner ──────────────────────────────────────────────────────
function HaltedBanner({ live }) {
  const k = live.kpis;
  if (!k || !k.trading_halted) return null;
  // drawdown_pct_peak = (peak − equity)/peak — the metric the live halt gates
  // on. The drawdown_pct fallback is the D14 BACKTEST-PARITY ratio
  // ((peak − equity)/INITIAL_BANKROLL), a different denominator, so the wording
  // below switches with it rather than calling both "from peak".
  const ddFromPeak = k.drawdown_pct_peak != null;
  const dd = ddFromPeak ? k.drawdown_pct_peak : (k.drawdown_pct || 0);
  // Fallback only; the payload always carries max_drawdown_halt_pct
  // (portfolio.max_drawdown_halt_frac).
  const halt = k.max_drawdown_halt_pct || 40;
  const n = (k.halted_wallets || []).length || k.n_wallets_halted || 1;
  return (
    <div className="halt-banner">
      <div className="halt-left">
        <span className="halt-dot" />
        <span className="halt-status">{n} wallet{n > 1 ? 's' : ''} halted</span>
        <span className="halt-msg" title={ddFromPeak
          ? 'drawdown from peak = (peak − equity) / peak — the metric the halt gates on'
          : 'payload predates drawdown_pct_peak: this is the D14 backtest-parity ratio (peak − equity) / initial bankroll, NOT the halt metric'}>
          {ddFromPeak ? 'drawdown from peak' : 'backtest-parity dd ratio'} <strong>{fmt.pctPoints(dd)}</strong> reached the <strong>{fmt.pctPoints(halt, 0)}</strong>-below-peak halt threshold. new bets paused on affected wallets until capital recovers.</span>
      </div>
      <div className="halt-bar">
        <div className="halt-track"><div className="halt-fill" style={{ width: `${Math.min(100, (dd / halt) * 100)}%` }} /></div>
        <div className="halt-labels"><span>0%</span><span>halt {fmt.pctPoints(halt, 0)} below peak</span></div>
      </div>
    </div>
  );
}

// ── shared atoms ─────────────────────────────────────────────────────
function KpiCell({ label, value, sub, bar, valueClass, valueSm, title }) {
  return (
    <div className="kpi" title={title || undefined}>
      <div className="kpi-label">{label}</div>
      <div className={'kpi-value ' + (valueSm ? 'sm ' : '') + (valueClass || '')}>{value}</div>
      {sub != null && (
        <div className="kpi-sub">{sub}</div>
      )}
      {bar && (
        <div className="kpi-bar"><div style={{ width: `${Math.min(100, Math.max(0, bar.pct))}%`, background: bar.color || 'var(--accent)' }} /></div>
      )}
    </div>
  );
}

// `titleHint` is the hover tooltip on a panel heading. Caveats such as the accounting basis, a
// window limit or "these are different populations" go here rather than as caption text under the
// title.
function Panel({ title, titleHint, sub, q, actions, flush, className, children }) {
  return (
    <section className={'card' + (flush ? ' flush' : '') + (className ? ' ' + className : '')}>
      {(title || actions) && (
        <div className={'card-head' + (flush ? ' flush' : '')}>
          {title && <div className="card-title" title={titleHint || undefined}>{title}{q && <span className="q"> · {q}</span>}</div>}
          {sub && <span className="card-sub">{sub}</span>}
          {actions && <div className="card-actions">{actions}</div>}
        </div>
      )}
      {children}
    </section>
  );
}

function EmptyState({ big, msg }) {
  return <div className="empty">{big && <span className="big">{big}</span>}{msg}</div>;
}

function CoinCell({ coin, sub }) {
  if (!coin) return <span className="muted">—</span>;
  return (
    <span className="coin-cell">
      <img src={`assets/coins/${coin.toLowerCase()}.svg`} alt="" onError={(e) => { e.target.style.visibility = 'hidden'; }} />
      {coin}{sub && <span className="slot-ref">{sub}</span>}
    </span>
  );
}

const OUTCOME_PILL = { WIN: 'pill-pos', LOSS: 'pill-neg', STOP: 'pill-neg', PENDING: 'pill-mute', GHOST_FILL: 'pill-warn', CANCELLED: 'pill-mute', EXPIRED: 'pill-mute', PUSH: 'pill-info' };
function OutcomePill({ outcome }) { return <span className={'pill ' + (OUTCOME_PILL[outcome] || 'pill-mute')}>{outcome}</span>; }
function SidePill({ side, direction }) {
  const dir = direction || directionFromSide(side);
  return <span className={side === 'YES' ? 'side-yes' : 'side-no'}>{dir}</span>;
}
function MeterBar({ ratio, color }) {
  const pct = Math.min(100, Math.max(0, (ratio || 0) * 100));
  return <div className="meter"><div style={{ width: pct + '%', background: color || 'var(--accent)' }} /></div>;
}

// Shared "showing a–b of n · prev/next" table pager. Takes a pager bundle from window.CB.paginate
// ({ pg, pages, start, end, total }) and the page setter, so the bounded tables (journal,
// sub-minute resolutions) share one footer. Optional `label` is a window note (e.g. "newest 500
// of 3,214 settled") for when the paged array is a slice of a larger population; it is shown as a
// tooltip.
function TablePager({ pager, setPage, label }) {
  const { pg, pages, start, end, total } = pager;
  return (
    <div className="table-pager">
      <span className="pager-info" title={label || undefined}>showing {start}–{end} of {total}</span>
      <div className="pager-actions">
        <button className="btn" disabled={pg <= 0} onClick={() => setPage(pg - 1)}>prev</button>
        <button className="btn" disabled={pg >= pages - 1} onClick={() => setPage(pg + 1)}>next</button>
      </div>
    </div>
  );
}

// "· as of HH:MM (Ns ago)" suffix — the degraded/stale-cash marker shared by the
// Overview equity hero-label and the MoneyHeadline. Same markup at both sites.
function AsOfTag({ ms }) {
  return <span className="muted mono" style={{ textTransform: 'none', letterSpacing: 0 }}> · as of {fmt.hmMyt(ms)} ({fmt.agoShort(ms)})</span>;
}

// Shared net-P&L headline. Reads only the money bundle (window.CB.money). Cash first: net is the
// cash result since the reset (m.net = account_pnl_era_cash = cash_now - cash_at_reset), so the
// headline matches the equity tile and the cash curve. The value stands alone; the cash-vs-paper
// gap is a measurement artifact, not pending money. Falls back to paper only when there is no cash
// figure. Shows an as-of tag only when the cash sample is degraded. The basis is in the tooltip.
// `variant` only swaps the wrapper so the existing hero-metric / kpi / cal-headline CSS is
// reused.
function MoneyHeadline({ variant, m }) {
  const net = m.net;                                   // cash-first (money(): cashNet ?? paper)
  const netIsCash = m.netIsCash != null ? m.netIsCash : m.hasCash;  // false when withPaperBasis forces a period's paper
  // Only the cash figure is called "net P&L"; the paper basis (calendar or date-range period, via
  // withPaperBasis) is "booked bets" so it never competes with the headline number.
  const label = netIsCash ? 'net P&L' : 'booked bets';
  // Accounting basis, as a tooltip.
  const basis = netIsCash
    ? 'cash basis since reset: account balance now − balance at reset'
    : 'ledger paper: Σ pnl_net over the settled rows in view';
  const asOf = m.degraded && m.asOfMs != null ? <AsOfTag ms={m.asOfMs} /> : null;
  // The headline is just the net figure. The only sub-line is the as-of tag for a degraded cash
  // sample.

  if (variant === 'kpi') {
    return <KpiCell label={<React.Fragment>{label}{asOf}</React.Fragment>} value={fmt.moneyDelta(net)} valueClass={tone(net)} valueSm title={basis} />;
  }
  // 'headline' is the only other variant in use.
  return (
    <div className="cal-headline" title={basis}>
      <div className="cal-headline-row">
        <span className="cal-headline-k">{label}</span>
        {asOf}
      </div>
      <span className={'cal-headline-v ' + tone(net)}>{fmt.moneyDelta(net)}</span>
    </div>
  );
}

window.Sidebar = Sidebar;
window.Topbar = Topbar;
window.HaltedBanner = HaltedBanner;
window.KpiCell = KpiCell;
window.AsOfTag = AsOfTag;
window.MoneyHeadline = MoneyHeadline;
window.Panel = Panel;
window.EmptyState = EmptyState;
window.CoinCell = CoinCell;
window.OutcomePill = OutcomePill;
window.SidePill = SidePill;
window.MeterBar = MeterBar;
window.TablePager = TablePager;
window.NAV = NAV;
window.SHELL_ICONS = I;
})();
