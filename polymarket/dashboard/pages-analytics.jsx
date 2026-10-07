// ===========================================================================
// pages-analytics.jsx — CalendarPage (daily realized-P&L heatmap), derived entirely from the
// streamed ledger.
// ===========================================================================
(function () {
const { fmt, tone, selectDaily, money, withPaperBasis, scopeLive, stripCash } = window.CB;
const { useState, useMemo } = React;

const DOW = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const MONTHS = ['', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

function parseYM(dateStr) { const [y, m] = dateStr.split('-').map(Number); return { y, m0: m - 1 }; }
function addMonth({ y, m0 }, delta) { const d = new Date(y, m0 + delta, 1); return { y: d.getFullYear(), m0: d.getMonth() }; }
function ymd(y, m0, day) { return `${y}-${String(m0 + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`; }

function buildMonthGrid(y, m0, dailyMap) {
  const first = new Date(y, m0, 1);
  const startDow = (first.getDay() + 6) % 7; // Mon=0
  const daysInMonth = new Date(y, m0 + 1, 0).getDate();
  const cells = [];
  // leading dim days
  for (let i = 0; i < startDow; i += 1) cells.push({ dim: true });
  for (let d = 1; d <= daysInMonth; d += 1) {
    const key = ymd(y, m0, d);
    cells.push({ day: d, date: key, data: dailyMap.get(key) || null });
  }
  while (cells.length % 7 !== 0) cells.push({ dim: true });
  const weeks = [];
  for (let i = 0; i < cells.length; i += 7) {
    const wk = cells.slice(i, i + 7);
    // n/wins/losses are TRADE counts (a base bet + its top-ups = one CLOB
    // position = one trade); pnl is the per-fill Σ and is unaffected.
    const total = wk.reduce((acc, c) => {
      if (c.data) {
        acc.pnl += c.data.pnl; acc.n += c.data.n;
        acc.wins += c.data.wins; acc.losses += c.data.losses;
      }
      return acc;
    }, { pnl: 0, n: 0, wins: 0, losses: 0 });
    weeks.push({ cells: wk, total });
  }
  return weeks;
}

function CalendarPage({ live, venueSel }) {
  // Venue-scoped view: for a specific venue the server daily rollup (all venues)
  // is nulled so selectDaily recomputes over the scoped ledger.
  const scoped = useMemo(() => scopeLive(live, venueSel), [live, venueSel]);
  const ledger = scoped.ledger || [];
  const daily = useMemo(() => selectDaily(scoped), [scoped.ledger_daily, ledger]);
  const dailyMap = useMemo(() => new Map(daily.map((d) => [d.date, d])), [daily]);
  const openN = ledger.filter((r) => r && r.outcome === 'PENDING').length;

  // Calendar headline: Σ pnl_net (ledger paper) OVER THE RENDERED CELLS, summed
  // from the same `daily` aggregate that fills the heatmap cells and the
  // win/loss-day KPIs, so the marquee can never contradict what is on screen.
  // `daily` is now the SERVER all-time rollup (live.ledger_daily via selectDaily)
  // whose population is IDENTICAL to kpis.pnl_net_usd — same settled rows, same
  // MYT day-key — so Σ(daily) === the Overview P&L breakdown net by construction,
  // and the two ledger-paper numbers on screen finally agree. (Older payloads /
  // static snapshot fall back to the bounded client aggDaily; the marquee then
  // still matches its own visible cells.) The headline sums the array — it never
  // reads the kpis.pnl_net_usd scalar directly — so cells and headline stay
  // coupled regardless of source.
  const calTotalNet = daily.reduce((a, b) => a + b.pnl, 0);

  const latestYM = daily.length ? parseYM(daily[daily.length - 1].date) : parseYM(fmt.dateMyt(Date.now()));
  const [ym, setYm] = useState(latestYM);
  const weeks = useMemo(() => buildMonthGrid(ym.y, ym.m0, dailyMap), [ym, dailyMap]);
  const monthPnl = weeks.reduce((s, w) => s + w.total.pnl, 0);

  // all-time KPI stats
  const tradingDays = daily.length;
  const winDays = daily.filter((d) => d.pnl > 0).length;
  const lossDays = daily.filter((d) => d.pnl < 0).length;
  const avg = tradingDays ? daily.reduce((a, b) => a + b.pnl, 0) / tradingDays : null;
  const best = daily.reduce((m, d) => (!m || d.pnl > m.pnl ? d : m), null);
  // "worst day" = worst LOSING day; until a day actually closes negative this
  // is null and the cell shows $0.00 (a single positive day is not a "worst").
  const worst = daily.reduce((m, d) => (d.pnl < 0 && (!m || d.pnl < m.pnl) ? d : m), null);

  // The headline is the shared MoneyHeadline with the bridge's `paper` replaced by the sum of the
  // rendered cells (withPaperBasis), so the headline equals its own heatmap. Account cash is not
  // split by venue, so the Calendar always drops the cash basis: net is ledger paper over the
  // rendered cells, as the headline's tooltip says.
  const k = live.kpis || {};
  const m = useMemo(() => money(k), [k]);
  const mCal = stripCash(m);

  return (
    <div className="content">
      <MoneyHeadline variant="headline" m={withPaperBasis(mCal, calTotalNet)} />

      <div className="kpi-grid">
        <KpiCell label="trading days" value={tradingDays} valueSm />
        <KpiCell label="win days" value={winDays} valueClass="pos" valueSm sub={tradingDays ? fmt.pct(winDays / tradingDays) : '—'} />
        <KpiCell label="loss days" value={lossDays} valueClass="neg" valueSm sub={tradingDays ? fmt.pct(lossDays / tradingDays) : '—'} />
        <KpiCell label="avg / day" value={fmt.moneyDelta(avg)} valueClass={tone(avg)} valueSm />
        <KpiCell label="best day" value={best ? fmt.moneyDelta(best.pnl) : '—'} valueClass="pos" valueSm sub={best ? best.date : ''} />
        <KpiCell label="worst day" value={worst ? fmt.moneyDelta(worst.pnl) : fmt.moneyDelta(0)} valueClass={worst ? 'neg' : ''} valueSm sub={worst ? worst.date : 'no losing day'} />
      </div>

      <Panel
        title={`${MONTHS[ym.m0 + 1]} ${ym.y}`}
        actions={(
          <React.Fragment>
            {/* The basis is in the tooltip. */}
            <span className={'mono ' + tone(monthPnl)} style={{ fontSize: 14, fontWeight: 700 }}
              title="ledger paper: Σ pnl_net over this month's settled rows">{fmt.moneyDelta(monthPnl)}</span>
            <button className="cal-nav" onClick={() => setYm(addMonth(ym, -1))}>‹</button>
            <button className="cal-nav" onClick={() => setYm(addMonth(ym, 1))}>›</button>
          </React.Fragment>
        )}>
        {tradingDays === 0 ? <EmptyState msg={openN ? `${openN} open fill awaiting settlement` : 'no settled trades yet'} /> : (
          <div className="cal-grid">
            {DOW.map((d) => <div key={d} className="cal-dow">{d}</div>)}
            <div className="cal-dow cal-dow-week">Week</div>
            {weeks.map((wk, wi) => (
              <React.Fragment key={wi}>
                {wk.cells.map((c, ci) => {
                  if (c.dim) return <div key={ci} className="cal-cell dim" />;
                  const t = c.data ? tone(c.data.pnl) : '';
                  return (
                    <div key={ci} className={'cal-cell ' + t}>
                      <div className="cal-head"><span className="cal-daynum">{c.day}</span></div>
                      {c.data ? (
                        <React.Fragment>
                          <div className={'cal-pnl ' + (c.data.pnl ? t : 'flat')}>{fmt.moneyDelta(c.data.pnl)}</div>
                          {/* TRADE counts (top-ups folded into their base bet);
                              a booked stop exit is simply a loss. */}
                          <div className="cal-meta"><span>{c.data.n} trades</span><span>{c.data.wins}W·{c.data.losses}L</span></div>
                        </React.Fragment>
                      ) : <div className="cal-pnl flat">—</div>}
                    </div>
                  );
                })}
                <div className={'cal-week ' + tone(wk.total.pnl)}>
                  <span className="wk">W{wi + 1}</span>
                  <span className={'wp ' + tone(wk.total.pnl)}>{fmt.moneyDelta(wk.total.pnl)}</span>
                  <span className="ws">{wk.total.n} trades</span>
                </div>
              </React.Fragment>
            ))}
          </div>
        )}
      </Panel>
    </div>
  );
}

window.CalendarPage = CalendarPage;
})();
