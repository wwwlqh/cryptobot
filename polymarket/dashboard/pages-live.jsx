// ===========================================================================
// pages-live.jsx — JournalPage: open positions and the settled ledger, with expandable fill detail.
// ===========================================================================
(function () {
const {
  fmt, tone, isLossRow, inRange, scopeLive, scopedCoins,
  directionFromSide, sliceRef, execPrice, fillPrice, rowStake, selectAnalytics, lifetimeSummary,
  money, paginate, groupTrades,
} = window.CB;
const { useState, useMemo } = React;

// Display outcome for a fill. A stop-loss exit is a realized loss and shows as LOSS; its DB outcome
// is CANCELLED, which would otherwise read as a voided cancel.
function rowOutcome(r) {
  if (!r) return null;
  return isLossRow(r) ? 'LOSS' : r.outcome;
}

// Per-row gross/fee for a settled WIN (item 29) — mirrors the producer's
// _ledger_pnl_decomposition pair rule exactly: the REALIZED live pair
// (live_filled_size × (1−live_filled_price)) when it landed, else the intended
// pair (bet_size × (1/fill_price − 1)); never mixed. fee = gross − pnl_net.
// Losses/cancels forfeit the stake with no winning payout to skim → null.
function rowFeeGross(r) {
  if (!r || r.outcome !== 'WIN' || typeof r.pnl_net !== 'number') return null;
  let gross = null;
  if (typeof r.live_filled_size === 'number' && r.live_filled_size > 0
      && typeof r.live_filled_price === 'number' && r.live_filled_price > 0) {
    gross = r.live_filled_size * (1 - r.live_filled_price);
  } else if (typeof r.fill_price === 'number' && r.fill_price > 0
      && typeof r.bet_size === 'number') {
    gross = r.bet_size * (1 / r.fill_price - 1);
  }
  if (gross == null || !isFinite(gross)) return null;
  return { gross, fee: gross - r.pnl_net };
}

// ===========================================================================
// Trade journal
// ===========================================================================
function DetailGrid({ r }) {
  const isPending = r && r.outcome === 'PENDING';
  const fg = rowFeeGross(r);
  const cells = [
    ['slice', sliceRef(r)],
    ['side', `${r.side} · ${directionFromSide(r.side)}`],
    ['model_p', fmt.prob(r.model_p)],
    ['market p', fmt.prob(r.p_market)],
    ['realized edge', r.realized_edge != null ? fmt.signed(r.realized_edge) : '—'],
    ['top-ask fill', fmt.prob(r.fill_price)],
    ['walked vwap', fmt.prob(r.walked_vwap)],
    ['exec price', fmt.prob(execPrice(r))],
    ['effective stake', fmt.money(rowStake(r))],
    ['intended stake', fmt.money(r.intended_bet_size != null ? r.intended_bet_size : r.bet_size)],
    ['outcome', rowOutcome(r)],
    ['P&L net', isPending ? 'pending settlement' : (r.pnl_net != null ? fmt.moneyDelta(r.pnl_net) : '—')],
    ['wallet', r.wallet_id || r.sleeve],
  ];
  if (fg) {
    cells.push(['gross (pre-fee)', fmt.moneyDelta(fg.gross)]);
    cells.push(['venue fee', fmt.moneyDelta(-fg.fee)]);
  }
  if (r.mode === 'LIVE') {
    // Ghost fills / settled rows without a live pair are DEAD, not pending —
    // only a genuinely open row may say "pending" (item 20).
    const absent = isPending ? 'pending' : '—';
    cells.push(['live fill size', r.live_filled_size != null ? fmt.num(r.live_filled_size, 4) : absent]);
    cells.push(['live fill price', r.live_filled_price != null ? fmt.prob(r.live_filled_price) : (r.live_filled_size != null ? 'awaiting price' : absent)]);
    cells.push(['live fill value', r.live_filled_value_usd != null ? fmt.money(r.live_filled_value_usd) : absent]);
  }
  if (r.is_topup) cells.push(['type', 'top-up']);
  return (
    <div className="detail-wrap">
      {cells.map(([kk, vv], i) => (
        <div key={i} className="detail-cell"><div className="k">{kk}</div><div className="v">{vv}</div></div>
      ))}
    </div>
  );
}

// exec = the price the bot INTENDED to place at decision time (walked vwap of
// the decision book for the intended size, falling back to the top ask) — NOT
// the realized fill. fill = what the venue actually reported
// (live_filled_price), with the paper/effective fill as the DRY fallback.
// Tone on fill = slippage vs intent: paying MORE is red, better is green
// (>0.005 to ignore rounding).
function FillCell({ r }) {
  const fill = fillPrice(r);
  if (fill == null) {
    if (r.outcome === 'PENDING') return <td className="num muted">…</td>;
    return <td className="num muted">—</td>;
  }
  const intent = execPrice(r);
  const slip = intent != null ? fill - intent : null;
  const cls = slip != null && Math.abs(slip) > 0.005 ? (slip > 0 ? 'neg' : 'pos') : '';
  const title = slip != null ? `slippage vs intended ${slip >= 0 ? '+' : ''}${slip.toFixed(4)}` : undefined;
  return <td className={'num ' + cls} title={title}>{fmt.prob(fill)}</td>;
}

// mark = the open position's current value per share: the best executable sell
// on its own book (tag `book`), or the entry fill price when the book is not in
// the active window (tag `entry`, cost basis). Open positions only.
function MarkCell({ r }) {
  const src = r.mark_source;
  return (
    <td className="num" title={src === 'book' ? 'best executable sell (current book)' : 'marked at entry — book not in active window'}>
      {r.mark_px != null ? fmt.prob(r.mark_px, 3) : '—'}
      {src && <span className="slot-ref"> {src}</span>}
    </td>
  );
}

// Label for one fill inside a trade group: the base entry, then the top-ups in
// the order they were taken.
function fillLabel(f, i) { return f.is_topup ? `top-up ${i}` : 'base'; }

// The fills breakdown revealed when a MULTI-fill trade is expanded: one line
// per ledger row with its own price / size / P&L, so the aggregated row above
// is always auditable back to the individual fills it folded.
function FillsTable({ g }) {
  let topupN = 0;
  return (
    <div className="tbl-wrap" style={{ marginBottom: 10 }}>
      <table className="tbl tbl-sub">
        <thead><tr>
          <th>fill</th><th className="num">at</th><th className="num">size</th>
          <th className="num">exec</th><th className="num">fill px</th>
          <th>outcome</th><th className="num">P&amp;L net</th>
        </tr></thead>
        <tbody>
          {g.fills.map((f) => {
            if (f.is_topup) topupN += 1;
            const px = fillPrice(f);
            return (
              <tr key={f.id}>
                <td className="primary mono">{fillLabel(f, topupN)}
                  {f.offset_sec != null && <span className="slot-ref"> +{f.offset_sec}s</span>}</td>
                <td className="num muted">{fmt.hmMyt(f.bet_ts)}</td>
                <td className="num">{fmt.money(rowStake(f))}</td>
                <td className="num">{fmt.prob(execPrice(f))}</td>
                <td className="num">{px != null ? fmt.prob(px) : '—'}</td>
                <td><OutcomePill outcome={rowOutcome(f)} /></td>
                <td className={'num ' + (f.pnl_net == null ? 'muted' : tone(f.pnl_net))}>
                  {f.pnl_net == null ? (f.outcome === 'PENDING' ? '…' : '—') : fmt.moneyDelta(f.pnl_net)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// One table row per TRADE (CLOB position), not per fill: size is Σ of the
// fills' stakes, P&L their Σ, and the fill price the SIZE-WEIGHTED effective
// price actually paid across them. A trade that took top-ups carries an "N in
// 1" badge and expands into the per-fill breakdown above the usual detail grid
// (a single-fill trade expands straight to the detail grid, unchanged).
function LedgerRows({ groups, openSet, toggle, showPnl, showMark }) {
  const cols = 9 + (showMark ? 1 : 0) + (showPnl ? 1 : 0);
  return groups.map((g) => {
    const r = g.anchor;
    const isOpen = openSet.has(g.key);
    const multi = g.nFills > 1;
    return (
      <React.Fragment key={g.key}>
        <tr className={'trade-row' + (isOpen ? ' open' : '')} onClick={() => toggle(g.key)}>
          <td className="primary">
            <span className="row-toggle">{isOpen ? '−' : '+'}</span>
            <CoinCell coin={g.coin} />
          </td>
          <td>
            <span className="wallet-tag">{g.wallet_id || g.sleeve}</span>
            <span className="slot-ref">{sliceRef(r)}{r.offset_sec != null ? ` · +${r.offset_sec}s` : ''}</span>
            {multi && <span className="fill-badge" title={`${g.nFills} fills (base + ${g.nFills - 1} top-up${g.nFills > 2 ? 's' : ''}) — one CLOB position`}>{g.nFills} in 1</span>}
          </td>
          <td><SidePill side={g.side} direction={g.direction} /></td>
          <td className="num" title={multi ? `Σ of ${g.nFills} fills` : undefined}>{fmt.money(g.size)}</td>
          <td className="num" title={multi ? 'size-weighted intended exec across the fills' : 'intended exec at decision · walked vwap (fallback top ask)'}>{g.avgExec != null ? fmt.prob(g.avgExec) : '—'}</td>
          {multi
            ? <td className="num" title="effective fill price — size-weighted across the fills">{g.avgFill != null ? fmt.prob(g.avgFill) : '—'}</td>
            : <FillCell r={r} />}
          {showMark && <MarkCell r={r} />}
          <td className="num">{g.model_p != null ? fmt.prob(g.model_p, 3) : '—'}</td>
          <td><OutcomePill outcome={g.outcome} /></td>
          {/* Open time of the base fill: the settled table shows when the position was opened, not
             when it settled. Settlement still orders the list and decides the Calendar day.
             */}
          <td className="num muted" title={g.settled ? `settled ${fmt.dateMyt(g.settleTs)} ${fmt.hmMyt(g.settleTs)}` : undefined}>{fmt.dateMyt(r.bet_ts)} {fmt.hmMyt(r.bet_ts)}</td>
          {showPnl && <td className={'num ' + (g.pnl == null ? 'muted' : tone(g.pnl))}>{g.pnl == null ? (g.open ? '…' : '—') : fmt.moneyDelta(g.pnl)}</td>}
        </tr>
        {isOpen && (
          <tr className="detail-row"><td colSpan={cols}>
            {multi && (
              <React.Fragment>
                <FillsTable g={g} />
                {/* The grid below describes the BASE bet — label it, or its
                    single-fill P&L reads as a contradiction of the aggregated
                    row above. The explanation is the label's tooltip. */}
                <div className="detail-head" title="the decision that opened this position — one fill, not the group Σ">base bet</div>
              </React.Fragment>
            )}
            <DetailGrid r={r} />
          </td></tr>
        )}
      </React.Fragment>
    );
  });
}

const OUTCOME_SCOPES = ['WIN', 'LOSS', 'PENDING'];

function JournalPage({ live, range, venueSel }) {
  const sel = venueSel || 'all';
  // Venue-scoped ledger; the coin filter offers the venue's coins.
  const scoped = useMemo(() => scopeLive(live, venueSel), [live, venueSel]);
  const allLedger = scoped.ledger || [];
  const ranged = useMemo(() => inRange(allLedger, range), [allLedger, range]);
  const [openSet, setOpenSet] = useState(() => new Set());
  const [page, setPage] = useState(0);
  const [coinScope, setCoinScope] = useState('');
  const [outcomeScope, setOutcomeScope] = useState('');
  const toggle = (id) => setOpenSet((s) => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n; });
  const setScope = (setter) => (v) => { setter(v); setPage(0); };
  const pickCoin = setScope(setCoinScope), pickOutcome = setScope(setOutcomeScope);

  // Pills = the venue's current scan universe ∪ coins still present in the
  // scoped rows (historical coins — a coin dropped by a past universe
  // amendment stays filterable as long as its rows are in view).
  const coins = useMemo(() => scopedCoins(live, venueSel, allLedger), [live, venueSel, allLedger]);
  // Pills filter the tables only; the KPI row uses the whole range (it is labelled with the range,
  // not the pills). The outcome pill matches the trade's verdict, so a stop-closed row appears
  // under LOSS.
  const ledger = useMemo(
    () => ranged.filter((r) => (!coinScope || r.coin === coinScope)),
    [ranged, coinScope],
  );
  const pillActive = !!(coinScope || outcomeScope);
  const pillLabel = pillActive
    ? 'filtered: ' + [coinScope, outcomeScope].filter(Boolean).join(' · ')
    : null;
  // Fills → TRADES (CLOB positions): a base bet and its top-ups become ONE row
  // that expands into its fills. Grouping happens AFTER the coin pill (which is
  // a per-trade property) and BEFORE the outcome pill (which is the group's
  // single verdict).
  const trades = useMemo(
    () => groupTrades(ledger).filter((g) => !outcomeScope
      || (outcomeScope === 'PENDING' ? g.open : g.outcome === outcomeScope)),
    [ledger, outcomeScope],
  );
  const open = useMemo(() => trades.filter((g) => g.open)
    .sort((a, b) => ((b.anchor.id || 0) - (a.anchor.id || 0))), [trades]);
  // Settled trades = groups that settled and actually opened. A voided, never-filled cancel is not
  // a trade, so it is not counted in the badge or pager and has no row here; the journal shows
  // exactly the population the equity curve plots and the hero tile counts. Voided groups appear as
  // the `N voided` sub on the KPI tile below.
  const settled = useMemo(() => trades.filter((g) => g.settled && !g.voided)
    .sort((a, b) => (b.settleMs || 0) - (a.settleMs || 0)), [trades]);
  // KPI-row summary: PRE-PILL population (the whole ranged window), so the
  // headline tiles never silently shrink to a pill selection — and taken from
  // the server FULL-ledger analytics block (range-sliced server-side) so they
  // are not silently the newest streamed rows either. Only a legacy payload /
  // static snapshot falls back to the client aggregate over `ranged`.
  const an = useMemo(() => selectAnalytics(scoped, range), [scoped, range]);
  const summary = an.summary;
  // Open TRADES (CLOB positions) in the ranged window — the notional below is
  // still the Σ of every open fill's effective stake, so grouping moves the
  // COUNT and nothing else.
  const openKpi = useMemo(
    () => groupTrades(ranged.filter((r) => r.outcome === 'PENDING')),
    [ranged],
  );
  // Win-rate + settled tiles read the lifetime rollup (full active-strategy
  // ledger — server-side, or the per-venue server rollup under a venue view),
  // not the browsable window, so they stay representative. Only a legacy
  // payload/static snapshot falls back to the client aggregate.
  const life = useMemo(() => lifetimeSummary(scoped, allLedger), [scoped, allLedger]);
  // Use the explicit settled-and-opened count, never `total - open`: `total` is the audit count
  // and includes voided cancels, which the tile's sub-line reports separately.
  const lifeSettled = life.settled;
  const pager = paginate(settled, page, 15);
  const openNotional = openKpi.reduce((a, g) => a + g.size, 0);
  const pnlSub = openKpi.length
    ? `PF ${fmt.ratio(summary.pf)}; ${openKpi.length} open not in P&L`
    : `PF ${fmt.ratio(summary.pf)}`;
  const wlSub = `${life.wins}W · ${life.losses}L`;

  // Net-P&L tile: THE one cash number via the shared MoneyHeadline. Cash basis can't be
  // range-sliced, so only the All window shows it; a 7d/30d window falls back to
  // the range-sliced client ledger-paper sum (explicitly labeled). The All-range
  // paper fallback uses the canonical server pnl_net_usd (the client Σ of per-row
  // ROUNDED pnl_net drifts by cents — 28.05 vs 27.99 over 450 rows).
  const k = live.kpis || {};
  const m = useMemo(() => money(k), [k]);
  // Account cash is one balance (not venue-split): show it only in the ALL-venue
  // view. A specific-venue view falls back to the scoped ledger-paper sum.
  const showWalletNet = range === 'All' && m.hasCash && sel === 'all';
  const allTimePaper = sel !== 'all'
    ? summary.pnl
    : (k.pnl_net_usd != null ? k.pnl_net_usd : summary.pnl);

  return (
    <div className="content">
      {/* The pill row filters the tables only; the tiles below cover the whole range (this is the
         grid's tooltip).
         */}
      <div className="kpi-grid c5" title={`pills filter the tables below — these tiles stay the unfiltered ${range} window`}>
        <KpiCell label="open positions" value={openKpi.length} valueSm
          sub={`${fmt.money(openNotional)} notional`}
          title="open CLOB positions in this range — a base bet and its top-ups count once; the notional is the Σ effective stake of every open fill (the Overview rail's 'exposure used' is the risk engine's committed notional — a different definition)" />
        <KpiCell label="settled trades"
          value={lifeSettled} valueSm
          sub={`${wlSub}${life.voided ? ` · ${life.voided} voided` : ''}`}
          title={(live.contracts && live.contracts.win_rate ? live.contracts.win_rate + ' — ' : '')
            + 'settled CLOB positions that actually opened; voided never-filled cancels are the separate count in the sub-line, not part of this one. '
            + (life.lifetime ? 'lifetime rollup over the active strategy ledger, since reset' : 'client aggregate over the streamed rows')} />
        <KpiCell label="win rate" value={fmt.pct(life.winRate)} valueSm
          sub={(life.wins + life.losses) ? wlSub : 'no decided trades yet'}
          title={'wins / (wins + losses) over settled trades; voided cancels excluded'
            + (life.lifetime ? ' — lifetime, since reset' : '')} />
        {showWalletNet ? (
          <MoneyHeadline variant="kpi" m={m} />
        ) : (
          <KpiCell label={range === 'All' ? 'realized P&L' : `realized P&L · ${range}`}
            value={fmt.moneyDelta(range === 'All' ? allTimePaper : summary.pnl)}
            valueClass={tone(range === 'All' ? allTimePaper : summary.pnl)} sub={pnlSub}
            title="ledger paper: Σ pnl_net over the settled rows in this window (not the account cash basis)" />
        )}
        <KpiCell label="avg realized edge" value={summary.avgEdge != null ? fmt.signed(summary.avgEdge) : '—'} valueSm
          sub={`${fmt.money(summary.stakeSettled)} stake`}
          title="settled trades only — the stake shown is the Σ effective stake of those settled fills" />
      </div>

      <div className="scope-pills" style={{ marginBottom: 14 }}>
        <button className={'scope-pill' + (coinScope === '' ? ' active' : '')} onClick={() => pickCoin('')}>all coins</button>
        {coins.map((c) => (
          <button key={c} className={'scope-pill' + (coinScope === c ? ' active' : '')} onClick={() => pickCoin(coinScope === c ? '' : c)}>
            <img src={`assets/coins/${c.toLowerCase()}.svg`} alt="" onError={(e) => { e.target.style.visibility = 'hidden'; }} />{c}
          </button>
        ))}
        <span className="scope-gap" />
        <button className={'scope-pill' + (outcomeScope === '' ? ' active' : '')} onClick={() => pickOutcome('')}>all outcomes</button>
        {OUTCOME_SCOPES.map((o) => (
          <button key={o} className={'scope-pill' + (outcomeScope === o ? ' active' : '')} onClick={() => pickOutcome(outcomeScope === o ? '' : o)}>{o}</button>
        ))}
      </div>

      <Panel title="open positions" q={`${open.length}`} sub={pillLabel || undefined} flush>
        {open.length === 0 ? <EmptyState msg="no open positions" /> : (
          <div className="tbl-wrap"><table className="tbl">
            <thead><tr><th>coin</th><th>slot</th><th>dir</th><th className="num">size</th><th className="num">exec</th><th className="num">fill</th><th className="num">mark</th><th className="num">model_p</th><th>status</th><th className="num">opened</th></tr></thead>
            <tbody><LedgerRows groups={open} openSet={openSet} toggle={toggle} showPnl={false} showMark /></tbody>
          </table></div>
        )}
      </Panel>
      <div className="section-gap" />

      {/* The settled table is a bounded window (the stream keeps the newest N fills), while the
         settled and win-rate tiles read the server's lifetime rollup. Rows are trades; expand one
         to see its fills. The panel-title and pager tooltips say so.
         */}
      <Panel title="settled trades" q={`${settled.length}`}
        titleHint={life.lifetime && lifeSettled > settled.length
          ? `${lifeSettled} settled since reset — this table streams the newest rows only`
          : 'settled trades, newest first — a base bet and its top-ups are one row'}
        sub={pillLabel || undefined} flush>
        {settled.length === 0 ? <EmptyState msg="no settled trades" /> : (
          <React.Fragment>
            <div className="tbl-wrap"><table className="tbl">
              <thead><tr><th>coin</th><th>slot</th><th>dir</th><th className="num">size</th><th className="num">exec</th><th className="num">fill</th><th className="num">model_p</th><th>outcome</th><th className="num" title="when the position was opened (base fill) — hover a row's time for its settle time">opened</th><th className="num">P&amp;L net</th></tr></thead>
              <tbody><LedgerRows groups={pager.slice} openSet={openSet} toggle={toggle} showPnl /></tbody>
            </table></div>
            <TablePager pager={pager} setPage={setPage}
              label={life.lifetime && lifeSettled > settled.length
                ? `newest ${settled.length} of ${lifeSettled} settled` : null} />
          </React.Fragment>
        )}
      </Panel>
    </div>
  );
}

window.JournalPage = JournalPage;
})();
