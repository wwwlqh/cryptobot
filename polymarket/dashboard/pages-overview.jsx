// ===========================================================================
// pages-overview.jsx — OverviewPage (hero, rail, per-coin and per-slice tables). The account
// headline and equity widgets use the reconciled account cash when it is comparable; the strategy
// analytics come from the streamed ledger and the server's lifetime rollups, so cash and ledger
// accounting stay visibly separate. CoinTable is shared below.
// ===========================================================================
(function () {
const {
  fmt, tone, isSettled, settleMs, inRange, dailyInRange, selectDaily,
  selectAnalytics, aggEquity, aggWeekly, lifetimeSummary,
  lifetimeEquity, strategyBase, money, ledgerWindow,
  scopeLive, VENUE_META,
} = window.CB;
const { useMemo } = React;

// Anchor fallback equity to strategy realized capital; server lifetime equity
// uses the same strategy ledger anchor when available.
function anchoredEquity(k, ledger) {
  const realized = k && k.realized_capital != null ? k.realized_capital : strategyBase(k);
  const recent = ledger.filter((r) => isSettled(r) && typeof r.pnl_net === 'number').reduce((a, b) => a + b.pnl_net, 0);
  return aggEquity(ledger, realized - recent);
}
function coinSpark(ledger, coin) {
  const rows = ledger.filter((r) => r.coin === coin && isSettled(r) && typeof r.pnl_net === 'number')
    .sort((a, b) => (settleMs(a) || 0) - (settleMs(b) || 0));
  let acc = 0; return rows.map((r) => (acc += r.pnl_net));
}

// `sparkWindowed` marks the cumulative-P&L column as covering only the streamed rows: the count
// and P&L columns come from the server's full-ledger block, but a sparkline needs per-row data that
// block does not carry, so the two columns describe different populations (said in the column
// tooltip).
function CoinTable({ coins, ledger, showSpark, sparkWindowed }) {
  if (!coins.length) return <EmptyState msg="no bets recorded in this window" />;
  return (
    <div className="tbl-wrap">
      <table className="tbl">
        <thead><tr>
          <th>coin</th><th className="num" title="settled / total TRADES — a base bet and its top-ups count once (one CLOB position); voided never-filled cancels are excluded from both, so Σ(settled) matches the hero trades tile">trades</th><th className="num" title="wins / (wins + losses)">win %</th><th className="num" title="gross win / gross loss">PF</th>
          {showSpark && <th title={sparkWindowed ? 'sparkline covers the streamed row window only — the columns beside it cover the full ledger' : undefined}>cum P&L</th>}<th className="num">P&amp;L</th>
        </tr></thead>
        <tbody>
          {coins.map((c) => (
            <tr key={c.coin}>
              <td className="primary"><CoinCell coin={c.coin} /></td>
              <td className="num">{c.resolved}<span className="muted"> / {c.bets}</span></td>
              <td className="num" title={`${c.wins}W · ${c.losses}L`}>{fmt.pct(c.winRate)}</td>
              <td className="num">{fmt.ratio(c.pf)}</td>
              {showSpark && <td><Sparkline values={coinSpark(ledger, c.coin)} w={92} h={22} /></td>}
              <td className={'num ' + tone(c.pnl)}>{fmt.moneyDelta(c.pnl)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Overview ─────────────────────────────────────────────────────────
function OverviewPage({ live, range, venueSel }) {
  // Venue-scoped view: the ledger filtered to THIS deployment's venue.
  // Account-level KPIs (equity hero, cash, exposure) are not partitioned by
  // venue and read the raw payload.
  const scoped = useMemo(() => scopeLive(live, venueSel), [live, venueSel]);
  const sel = window.CB.normVenueSel(venueSel);
  const k = live.kpis;
  const m = useMemo(() => money(k), [k]);
  const allLedger = scoped.ledger || [];
  const ledger = useMemo(() => inRange(allLedger, range), [allLedger, range]);
  const base = m.basis;
  // Historical/aggregate panels read the server FULL-ledger analytics block
  // (range-sliced server-side); only a legacy payload / static snapshot / a
  // venue population the server never saw falls back to the bounded window.
  const an = useMemo(() => selectAnalytics(scoped, range), [scoped, range]);
  const summary = an.summary;
  // Equity curve: the server LIFETIME ledger-paper series rendered UNRESCALED
  // (item 12 — the old affine cash re-anchor could mirror gains into dips when
  // the paper/cash spans had opposite signs; cash stays in the headline only).
  // Falls back to the windowed anchoredEquity for the static-snapshot/legacy
  // path (and for a scoped venue view without a server series).
  const eq = useMemo(() => lifetimeEquity(scoped, anchoredEquity(k, ledger)), [k, ledger, scoped]);
  // Daily series: the server all-time ledger_daily (full deployed history —
  // same population as the Calendar) range-sliced day-granularly; only a
  // legacy payload/static snapshot falls back to the bounded row window
  // (items 3+4). Weekly bars + day-based ratios feed from it.
  const daily = useMemo(() => dailyInRange(selectDaily(scoped), range), [scoped.ledger_daily, allLedger, range]);
  const serverDaily = !!(scoped && Array.isArray(scoped.ledger_daily));
  const weekly = useMemo(() => aggWeekly(daily), [daily]);
  const hist = an.histogram;
  const coins = an.coins;
  const slices = an.slices;
  const streaks = an.streaks;
  const win = ledgerWindow(ledger);
  // Window note for the weekly panel (server daily data), shown in the panel title's tooltip so it
  // is clear which population the bars describe.
  const dailyLabel = serverDaily
    ? (range === 'All' ? 'full history' : `${range} · full history`)
    : (win ? `${win.n}-row window` : 'no rows');
  // Headline win-rate is the LIFETIME rollup over the active strategy (server-
  // computed over the full ledger), not the session-window aggregate — so it
  // is a stable, representative number rather than a noisy recent slice. Under a
  // venue view it becomes the client aggregate over the scoped ledger.
  const life = useMemo(() => lifetimeSummary(scoped, allLedger), [scoped, allLedger]);
  if (!k) return <div className="content"><EmptyState big="waiting for first tick" msg="the dashboard is connected but the scanner has not broadcast yet" /></div>;

  const capitalValue = m.cashLive != null ? m.cashLive : m.basis;
  const capitalPnl = m.net;
  const capitalPnlPct = capitalPnl != null && base > 0 ? capitalPnl / base : null;
  // drawdown_pct_peak = (peak − equity)/peak — the metric the live 40% halt
  // gates on (pipeline _wallet_drawdown_halted). The drawdown_pct fallback is
  // the D14 BACKTEST-PARITY ratio ((peak − equity)/INITIAL_BANKROLL), a
  // different denominator, so the tile renames itself rather than calling both
  // "peak drawdown".
  const ddFromPeak = k.drawdown_pct_peak != null;
  const dd = ddFromPeak ? k.drawdown_pct_peak : k.drawdown_pct;
  const expRatio = k.exposure_cap > 0 ? k.exposure_used / k.exposure_cap : 0;
  const winLabel = (life.wins + life.losses)
    ? `${life.wins}W · ${life.losses}L`
    : 'no decided trades yet';
  const noBetsYet = life.lifetime && life.total === 0;
  // 24h tile: wins/losses when the payload has them. The window is by bet time and open bets are
  // excluded (realized only). These are per-fill wallet counts (capital.py), not trade-group
  // counts; the tooltip says so.
  const wl24 = (k.wins_24h != null || k.losses_24h != null)
    ? `${k.wins_24h || 0}W · ${k.losses_24h || 0}L` : null;
  // "Open bets" on this page means open trade groups (CLOB positions), the same as the Journal's
  // "open positions" tile, from kpis.n_pending_trades. An older payload without it falls back to
  // the per-fill n_pending and says so in the label.
  const openTrades = k.n_pending_trades != null ? k.n_pending_trades : (k.n_pending || 0);
  const openIsFills = k.n_pending_trades == null;
  const pnl24Sub = [
    wl24,
    openTrades ? `${openTrades} open ${openIsFills ? 'fills ' : ''}excluded` : null,
  ].filter(Boolean).join(' · ');

  // Strip NET cell = paper net + rebates (the CLOB-reconciling figure). Null-
  // safe: no paper net at all ⇒ '—' (nothing to add to); a null rebate is the
  // trio's own convention — treat it as "no rebates recorded" (0), exactly as
  // the gross/fee cells treat their own absent halves.
  const netInclRebates = k.pnl_net_usd == null ? null
    : k.pnl_net_usd + (k.pnl_rebate_usd || 0);

  const expPct = Math.min(100, Math.max(0, expRatio * 100));
  const ddPct = k.max_drawdown_halt_pct > 0 ? Math.min(100, (dd / k.max_drawdown_halt_pct) * 100) : 0;

  // When the live cash sample is degraded (reconciliation-sample fallback, not
  // the per-tick balance_cache/readiness mirror), show its as-of time subtly
  // instead of silently reading a stale number as live.
  const capitalAsOf = m.degraded ? m.asOfMs : null;

  // No ledger rows on this console: say so explicitly rather than show empty tables that look like
  // a data outage. The account tiles below are this deployment's own account.
  const emptyVenueBanner = allLedger.length === 0 ? (
    <div className="halt-banner" style={{ background: 'var(--panel)', borderColor: 'var(--line)' }}>
      <span className="tag">{(VENUE_META[sel] || {}).short || sel}</span>
      &nbsp;no trading rows on this box yet — the scanner has not written a
      ledger row since the accounting cutover. The account tiles below are this
      deployment's own Polymarket
      &nbsp;account; the tables stay empty until it trades.
    </div>
  ) : null;

  return (
    <div className="content">
      {emptyVenueBanner}
      {noBetsYet && (
        <div className="empty" style={{ textAlign: 'left', marginBottom: 14 }}>
          <span className="big">no bets recorded yet</span>
          the deployed strategy has not written any ledger rows. headline tiles and
          analytics populate automatically as it trades.
        </div>
      )}

      <section className={'card hero hero-' + (tone(capitalPnl) || 'flat')}>
        <div className="hero-main">
          <div className="hero-top">
            <div>
              {/* The basis is a tooltip; the visible label is the bare word. "· live" only appears
                 for a live cash figure; a degraded sample (reconciliation fallback) shows its
                 as-of time instead.
                 */}
              <div className="hero-label" title={m.cashLive != null ? (m.degraded ? 'equity (CLOB) · cash' : 'equity (CLOB) · cash · live') : 'equity · ledger paper'}>equity{capitalAsOf != null && <AsOfTag ms={capitalAsOf} />}</div>
              {/* key remounts the row when capital changes → hero-pulse fires */}
              <div className="hero-vrow" key={String(capitalValue)}>
                <span className="hero-value">{fmt.money(capitalValue)}</span>
                {/* "≈" when the basis anchor is an unclean (mid-position)
                    sample — the % return off it is approximate (C4). */}
                {capitalPnlPct != null && <span className={'kpi-delta ' + tone(capitalPnl)}
                  title={m.basisUnclean ? 'basis anchored to an unclean (mid-position) balance sample — not strictly comparable' : undefined}>
                  {(m.basisUnclean ? '≈' : '') + fmt.pctDelta(capitalPnlPct)}</span>}
              </div>
            </div>
            <div className="hero-metrics">
              {/* Trade count, not money (the net figure is already in the decomposition strip in
                 this card). Counts settled trade groups in the active range (the same server
                 analytics block the panels below read): all fills of a position are one trade, and
                 voided never-filled cancels are excluded. That is the population the equity curve
                 above plots, so the two agree.
                 */}
              <div className="hero-metric" title="settled CLOB positions in this range that actually opened — a base bet and its top-ups count once; voided never-filled cancels excluded">
                <span className="k">trades</span>
                <span className="v">{summary.settled}</span>
              </div>
              <div className="hero-metric" title="rolling 24h by BET time, realized only — the W·L counts are per-FILL wallet-snapshot counts (capital.py), not the trade groups the tables below count">
                <span className="k">24h</span>
                <span className={'v ' + tone(k.pnl_24h)}>{fmt.moneyDelta(k.pnl_24h)}</span>
                <span className="s">{pnl24Sub}</span>
              </div>
              <div className="hero-metric">
                <span className="k">win rate</span>
                <span className="v">{fmt.pct(life.winRate)}</span>
                <span className="s">{winLabel}</span>
              </div>
            </div>
          </div>
          {/* Two separate panes: equity in dollars and drawdown in percent below the peak are
             different metrics on different scales, and stacked flush they read as one chart. Each
             pane has its own title; the second is set off by a hairline and a gap (.hero-pane).
             The drawdown pane's title is the only place that curve is named.
             */}
          <div className="hero-pane">
            <div className="hero-pane-head">equity</div>
            <div className="hero-chart">
              {/* `unit` labels the crosshair: the ledger series has one point per trade, the
                 cash_equity fallback one point per hourly balance bucket.
                 */}
              <EquityChart points={eq.points} base={eq.base} height={228}
                unit={eq.basis === 'cash' ? 'hour' : 'trade'} />
            </div>
          </div>
          <div className="hero-pane">
            <div className="hero-pane-head" title="drawdown from peak = (peak − equity) / peak, walked along the same curve above — the metric the live halt gates on">max dd %</div>
            <div className="hero-chart">
              <DrawdownChart points={eq.points} height={64} />
            </div>
          </div>
          {/* Gross -> fees -> rebates -> net, inside the equity card. Read left to right, it ends
             on the number to reconcile against: gross - fees + rebates = how much the CLOB balance
             actually moved.

             The payload's `pnl_net_usd` stays the paper net (gross - fees), because the producer
             guarantees sum(ledger_daily.pnl) == pnl_net_usd and a rebate is an account-level cash
             event no ledger row carries. The rebate is added here for display only; nothing reads
             `netInclRebates`. Producer: _ledger_pnl_decomposition.
             */}
          {(k.pnl_gross_usd != null || k.pnl_fee_usd != null) && (
            <div className="kpi-grid c4 hero-strip">
              <KpiCell label="gross P&L"
                value={fmt.moneyDelta(k.pnl_gross_usd)} valueClass={tone(k.pnl_gross_usd)} valueSm
                title="pre-fee: Σ realized P&L over the settled ledger before the venue's taker fee" />
              <KpiCell label="fees paid"
                value={k.pnl_fee_usd != null ? fmt.moneyDelta(-k.pnl_fee_usd) : '—'}
                valueClass={k.pnl_fee_usd ? 'neg' : ''} valueSm
                title="venue skim: Σ taker fee charged on those trades — already subtracted from the paper net" />
              <KpiCell label="rebates"
                value={k.pnl_rebate_usd != null ? fmt.moneyDelta(k.pnl_rebate_usd) : '—'}
                valueClass={tone(k.pnl_rebate_usd)} valueSm
                title="paid back by the venue (taker-fee rebates + rewards) — account cash NOT in the payload's paper net, added to the net cell here on the client" />
              <KpiCell label="net"
                value={netInclRebates != null ? fmt.moneyDelta(netInclRebates) : '—'}
                valueClass={tone(netInclRebates)} valueSm
                title="gross − fees + rebates: the paper net plus account rebates — the figure the venue CLOB balance reconciles to" />
            </div>
          )}
        </div>
        <aside className="hero-rail">
          <div className="rail-item" title={openIsFills
            ? 'legacy payload: per-FILL count of pending ledger rows (a base bet and its top-ups count separately) — not the grouped open-position count the Journal shows'
            : 'open CLOB positions — a base bet and its top-ups count once, the same number the Journal’s "open positions" tile shows'}>
            <span className="rail-k">open {openIsFills ? 'fills' : 'bets'} <span className="mono">{fmt.money(k.exposure_used)} notional</span></span>
            <span className="rail-v">{openTrades}</span>
            <div className="kpi-bar"><div style={{ width: `${expPct}%`, background: 'var(--c-accent)' }} /></div>
            <span className="rail-sub">exposure {fmt.money(k.exposure_used)} / cap {fmt.money(k.exposure_cap)}</span>
          </div>
          <div className="rail-item">
            <span className="rail-k">open value</span>
            <span className="rail-v">{m.openValue != null ? fmt.money(m.openValue) : '—'}</span>
          </div>
          <div className="rail-item">
            <span className="rail-k">equity incl open</span>
            <span className="rail-v">{m.equityInclOpen != null ? fmt.money(m.equityInclOpen) : '—'}</span>
          </div>
          <div className="rail-item" title={ddFromPeak
            ? 'drawdown from peak = (peak − equity) / peak, on the strategy paper ledger — the metric the live halt gates on'
            : 'payload predates drawdown_pct_peak: this is the D14 backtest-parity ratio (peak − equity) / initial bankroll, NOT the halt metric'}>
            <span className="rail-k">{ddFromPeak ? 'dd from peak' : 'dd ratio · backtest parity'}</span>
            <span className={'rail-v' + (dd > 0 ? ' neg' : '')}>{fmt.pctPoints(dd)}</span>
            <div className="kpi-bar"><div style={{ width: `${ddPct}%`, background: 'var(--c-neg)' }} /></div>
            {/* The halt threshold stays on-screen — it is a live control, not
                caption prose; the metric's definition lives in the title. */}
            <span className="rail-sub">{ddFromPeak
              ? `halts at ${fmt.pctPoints(k.max_drawdown_halt_pct, 0)}`
              : `D14 parity · halt ${fmt.pctPoints(k.max_drawdown_halt_pct, 0)}`}</span>
          </div>
          {/* Profit factor is shown in the journal's P&L tile subtitle. */}
          <div className="rail-item">
            <span className="rail-k">streak <span className="mono">{streaks.longestW}W best · {streaks.longestL}L worst</span></span>
            <span className={'rail-v ' + (streaks.current > 0 ? 'pos' : streaks.current < 0 ? 'neg' : '')}>
              {streaks.current > 0 ? `${streaks.current}W` : streaks.current < 0 ? `${-streaks.current}L` : '—'}
            </span>
            <div className="streak-chips" style={{ marginLeft: 0, marginTop: 4 }}>
              {streaks.last.map((c, i) => <span key={i} className={'streak-chip ' + c} />)}
            </div>
          </div>
        </aside>
      </section>
      <div className="section-gap" />

      <div className="row-2b">
        <Panel title="weekly P&L" titleHint={`population: ${dailyLabel}`}><PnlBars items={weekly.map((w) => ({ label: w.week, pnl: w.pnl }))} height={150} /></Panel>
        <Panel title="per-trade P&L distribution" sub={`${summary.settled} settled`}><PnlHistogram bins={hist} height={150} /></Panel>
      </div>
      <div className="section-gap" />

      <Panel title="performance by coin"><CoinTable coins={coins} ledger={ledger} showSpark sparkWindowed={an.full} /></Panel>
      <div className="section-gap" />

      <Panel title="settled trades by slice" flush>
        {slices.length === 0 ? <EmptyState msg="no settled trades yet" /> : (
          <div className="tbl-wrap"><table className="tbl">
            <thead><tr><th>slice</th><th className="num" title="settled TRADES — a base bet and its top-ups count once; voided never-filled cancels excluded, so Σ(n) matches the hero trades tile">trades</th><th className="num">W</th><th className="num">L</th><th className="num" title="wins / (wins + losses)">win %</th><th className="num">P&amp;L</th></tr></thead>
            <tbody>
              {slices.map((s) => (
                <tr key={s.slice}>
                  <td className="primary mono">{s.slice}</td>
                  <td className="num">{s.n}</td><td className="num">{s.wins}</td><td className="num">{s.losses}</td>
                  <td className="num">{fmt.pct(s.winRate)}</td>
                  <td className={'num ' + tone(s.pnl)}>{fmt.moneyDelta(s.pnl)}</td>
                </tr>
              ))}
            </tbody>
          </table></div>
        )}
      </Panel>
    </div>
  );
}

window.OverviewPage = OverviewPage;
})();
