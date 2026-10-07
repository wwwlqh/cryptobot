// ===========================================================================
// pages-subminute.jsx — SubminutePage: the console for the sub-minute strategy (ask bands over a
// per-second entry window, first fill wins per resolution). The bands come from spec.bands, derived
// on the server from the deployed config.
//
// Reads `live.v2` from the SSE payload: the decision spec (spec.bands), the sizing state (base
// fractions, drawdown brake, hard halt), the fill-rate breaker, and recent per-bet rows with
// pred_edge (model_p - ask - fee) and stake. Without the block, sub-minute trading is disabled on
// this deployment and the page shows an EmptyState. Navigation and routing are wired in app.jsx and
// shell.jsx.
//
// This file also holds SubminuteSpecPage (Config → "Strategy"): the deployed strategy as a
// reference table with one column per band, every value read from the payload (live.v2.spec,
// live.v2.sizing, per-coin caps from the portfolio) with '—' fallbacks.
//
// The resolution rollup (every attempt folded into one row per position, missed P&L counted once)
// is in data.jsx (window.CB.groupResolutions) so it can be unit-tested; this file renders the
// grouped rows and the fill-tax totals. A position's fills fold through the Trade journal's
// `groupTrades` helper, so a multi-fill position is one "N in 1" row on both tabs and expands to
// the per-attempt table. Attempt and reject counts stay per order, because each attempt is a
// separate order.
// ===========================================================================
(function () {
const { fmt, tone, groupResolutions, paginate, useOpenSet,
  missedTradeIndex, missedTradeForGroup, directionFromSide } = window.CB;
const { useState, useMemo } = React;

// ── spec band derivation (shared by the Config spec page) ──────────── Bands come from spec.bands;
// an older payload without it falls back to the flat ask_lo/ask_hi/gate fields as a single band.
function specBands(s) {
  return Array.isArray(s.bands) && s.bands.length
    ? s.bands
    : (s.ask_lo != null
        ? [{ label: 'MID', ask_lo: s.ask_lo, ask_hi: s.ask_hi, gate: s.gate }]
        : []);
}

// Throttle-off detection: with floor ×1.00, max(floor, 1 − dd/scale) is 1.00 at every drawdown, so
// the brake does nothing. Every brake display must read that as "off", not "pinned at its floor",
// or it would warn about a throttle that is not throttling.
const throttleOff = (floor) => !(floor < 0.999);

// DD-brake meter normalization: map brake ∈ [floor..1] → [0..1] for the bar fill.
// With the throttle off the range is degenerate (floor == 1), so the bar reads
// FULL rather than collapsing to an empty (max-throttle-looking) track.
const brakeMeter = (brake, floor) => (throttleOff(floor)
  ? 1
  : Math.max(0, Math.min(1, (brake - floor) / Math.max(1e-9, 1 - floor))));

// Fallbacks for older payloads only (live payloads always carry the values); they mirror
// config.V2_BRAKE_FLOOR / V2_BRAKE_SCALE / MAX_DRAWDOWN_HALT_FRAC.
const BRAKE_FLOOR_FALLBACK = 1.0;
const BRAKE_SCALE_FALLBACK = 0.60;
const HARD_HALT_FALLBACK = 0.40;

// ── drawdown brake / hard halt banner ─────────────────────────────────── Brake = max(floor, 1 −
// dd/scale): full size (×1.00) at dd=0, falling to the floor; at the hard halt
// (sizing.hard_halt_frac) all placement stops. With the floor at ×1.00 the brake is always ×1.00
// and the halt is the only drawdown control. Every figure comes from the payload.
//
// Renders only unusual states:
//
//   * trading_halted → the full hard-halt banner;
//   * brake below ×1.00, or resting on a real floor → a compact warning with
//     the live multiplier, the drawdown that caused it and the floor;
//   * brake ×1.00 and not halted → nothing.
//
// The halt threshold is always visible on Overview's drawdown tile ("halts at N%") and in Config →
// Strategy ("hard halt").
function BrakeBanner({ sizing }) {
  const z = sizing || {};
  const halt = z.hard_halt_frac != null ? z.hard_halt_frac : HARD_HALT_FALLBACK;
  const ddFrac = z.dd_frac != null ? z.dd_frac : 0;
  if (z.trading_halted) {
    return (
      <div className="halt-banner" style={{ margin: '0 0 14px' }}>
        <div className="halt-left">
          <span className="halt-dot" />
          <span className="halt-status">hard halt</span>
          <span className="halt-msg" title="drawdown from peak = (peak − equity) / peak — the metric the halt gates on">drawdown from peak <strong>{fmt.pct(ddFrac)}</strong> reached the <strong>{fmt.pct(halt, 0)}</strong>-below-peak halt — all v2 placement paused until capital recovers.</span>
        </div>
        <div className="halt-bar">
          <div className="halt-track"><div className="halt-fill" style={{ width: `${Math.min(100, (ddFrac / halt) * 100)}%` }} /></div>
          <div className="halt-labels"><span>0%</span><span>halt {fmt.pct(halt, 0)} below peak</span></div>
        </div>
      </div>
    );
  }
  const brake = z.brake_multiplier != null ? z.brake_multiplier : 1;
  const floor = z.brake_floor != null ? z.brake_floor : BRAKE_FLOOR_FALLBACK;
  // `brake_at_floor` is server-computed as brake <= floor: with the throttle
  // removed that is trivially true at ×1.00, which is NOT a throttled state.
  const off = throttleOff(floor);
  const atFloor = z.brake_at_floor && !off;
  const throttling = brake < 0.999;
  // INERT brake (throttle removed, ×1.00, not halted) renders nothing at all.
  if (off && !throttling) return null;
  if (!throttling && !atFloor) return null;
  // meter maps [floor..1] → [0..100]
  const meter = brakeMeter(brake, floor);
  const status = atFloor ? 'brake at floor' : 'throttling';
  // DD at which the throttle bottoms out: scale·(1−floor).
  const ddAtFloor = z.throttle_dd_at_floor != null
    ? z.throttle_dd_at_floor
    : (z.brake_scale || BRAKE_SCALE_FALLBACK) * (1 - floor);
  return (
    <div className="halt-banner" style={{ margin: '0 0 14px', animation: 'none',
      background: 'var(--warn-bg)', borderColor: 'var(--warn-line)', color: 'var(--warn)' }}>
      <div className="halt-left">
        <span className="halt-dot" />
        <span className="halt-status">DD-brake ×{fmt.num(brake, 2)}</span>
        <span className="halt-msg"
          title={`stake multiplier = max(floor, 1 − dd/scale) on drawdown FROM PEAK = (peak − equity) / peak; the hard halt stops all placement at ${fmt.pct(halt, 0)} below peak`}>
          {status} · dd from peak {fmt.pct(ddFrac)} · floor ×{fmt.num(floor, 2)} at ≥{fmt.pct(ddAtFloor, 1)}
        </span>
      </div>
      <div className="halt-bar">
        <div className="halt-track"><div className="halt-fill" style={{ width: `${meter * 100}%`, background: 'var(--warn)' }} /></div>
        <div className="halt-labels"><span>floor ×{fmt.num(floor, 2)}</span><span>×1.00</span></div>
      </div>
    </div>
  );
}

// ── reject-drift rendering helpers (the per-attempt drill-down) ───────
// requote_drift is in probability units — render as ¢ and color it: a POSITIVE
// drift means the ask ran away from us after the reject (the tax got worse) →
// red; negative → green. (The would-be outcome + the whole resolution-grouping
// engine live in data.jsx / window.CB.groupResolutions so they can be
// unit-tested; this file only renders the grouped rows.)
function driftCents(x) {
  if (x == null || !isFinite(x)) return '—';
  const c = x * 100;
  return (c >= 0 ? '+' : '−') + Math.abs(c).toFixed(2) + '¢';
}
function driftClass(x) {
  return x == null ? 'muted' : x > 0 ? 'neg' : x < 0 ? 'pos' : '';
}

// Status chip, reusing the existing pill classes (NO_FILL and PARTIAL map onto pill-neg/pill-warn).
// NO_FILL reads "miss": the on-chain verdict chip says the same and also names the cause, so on a
// grouped row that has a verdict the pill is dropped (see ResolutionRows).
const STATUS_PILL = { WIN: 'pill-pos', LOSS: 'pill-neg', PENDING: 'pill-mute',
  GHOST_FILL: 'pill-warn', NO_FILL: 'pill-neg', PARTIAL: 'pill-warn' };
const STATUS_LABEL = { GHOST_FILL: 'ghost', NO_FILL: 'miss', PARTIAL: 'partial' };
function StatusPill({ status }) {
  return <span className={'pill ' + (STATUS_PILL[status] || 'pill-mute')}>
    {STATUS_LABEL[status] || (status || '').toLowerCase()}</span>;
}

// ── miss KIND — what OUR order did, never what the book did ───────────
// NOT a cause. It answers "was the book even contested?", which is also what
// says whether a chain verdict can exist for this row: the forensics job
// adjudicates exactly the no_liquidity book misses. Server-side twin:
// snapshot_builder._miss_kind. `book_miss` is deliberately absent — the
// ordinary case renders its CHAIN VERDICT, not a kind chip.
const MISS_KIND = {
  ghost_fill: { label: 'ghost fill', pill: 'pill-warn',
    title: 'phantom liquidity: the venue ACCEPTED an order for this resolution and filled $0 — nothing came back behind the quote. Not a book contest, so the chain job never classifies it.' },
  venue_error: { label: 'venue error', pill: 'pill-warn',
    title: 'every failed attempt on this resolution carried a non-no_liquidity error_kind (auth · rate · size · outage): we broke, the book was never contested. No chain verdict is possible.' },
};
function MissKindPill({ kind }) {
  const m = MISS_KIND[kind];
  if (!m) return null;
  return <span className={'pill ' + m.pill} title={m.title}>{m.label}</span>;
}

// ── on-chain miss verdict: the miss-cause taxonomy ─────────────────────── Every missed trade is
// classified from chain evidence (faster taker vs maker cancel). Re-reading the book after a reject
// cannot tell an ask that traded away from one that was pulled; the trade tape can.
//
// The vocabulary is the server's (miss_forensics constants via snapshot_builder): every strip and
// table follows the server's key order, and this map only adds labels. `verdictMeta` falls back
// to the raw key, so a new verdict shows as a plain chip instead of disappearing.
const CHAIN_VERDICT = {
  FASTER_TAKER: { label: 'faster taker', pill: 'pill-info',
    reason: 'a print AT OUR LEVEL — the quote was real, another taker got there first',
    title: 'ON-CHAIN PROOF: a print inside our LEVEL BAND — from the ask we saw up to our limit (±0.5c) — matched within seconds of our order, so the quote was REAL and another taker got there first. Beat-us dt is measured in venue-match time (the block timestamp minus the calibrated ~3s settlement lag), so a negative dt means their order matched BEFORE ours arrived.' },
  REPRICED_ONLY: { label: 'repriced', pill: 'pill-mute',
    reason: 'prints exist, none at our level — the book had already moved past us',
    title: 'ON-CHAIN: prints exist in the window, but every one falls OUTSIDE our level band — the level we bid for never traded. The book had already moved past us; nothing was won or lost on speed.' },
  MAKER_CANCEL: { label: 'maker cancel', pill: 'pill-warn',
    reason: 'no print at all — the resting order left the book without trading',
    title: 'ON-CHAIN: NO print at all in the window — the resting order left the book without trading, i.e. the maker withdrew it. Nothing a faster order could have caught.' },
  PENDING: { label: 'chain pending', pill: 'pill-mute',
    reason: 'evidence not fetched yet — the job retries this row',
    title: 'the on-chain evidence has not been fetched yet (too recent for chain settlement, or a transient venue/RPC failure) — the forensics job retries this row on its next pass.' },
  UNRESOLVED: { label: 'chain n/a', pill: 'pill-mute',
    reason: 'the venue no longer exposes both tokens — never classifiable',
    title: 'the venue no longer exposes both tokens for this market, so the complement-side print cannot be checked — this row can never be classified.' },
};
// Decoration for a verdict the SERVER sent. Unknown keys (a verdict added
// upstream before this file learns about it) render readably rather than
// disappearing — the server owns the vocabulary, this file owns the styling.
function verdictMeta(k) {
  return CHAIN_VERDICT[k] || {
    label: String(k || '').toLowerCase().replace(/_/g, ' '),
    pill: 'pill-mute',
    reason: 'verdict emitted by the forensics job, not yet described here',
    title: 'on-chain verdict ' + k + ' — see scheduler/miss_forensics.py.',
  };
}
// Venue-match dt: negative = they matched BEFORE us. Rendered with the sign
// spelled out because the sign IS the finding.
function beatDt(x) {
  if (x == null || !isFinite(x)) return '';
  const s = Math.abs(x) < 0.05 ? '0' : (Math.abs(x) < 10 ? Math.abs(x).toFixed(1).replace(/\.0$/, '') : Math.round(Math.abs(x)));
  return (x > 0 ? '+' : x < 0 ? '−' : '') + s + 's';
}
// The per-miss chip shows only the verdict; the evidence (dt, price, size, rival maker) is in the
// hover title so rows stay readable.
function ChainVerdictChip({ verdict, dt, price, size, rival, detail }) {
  if (!verdict) return null;
  const m = verdictMeta(verdict);
  const parts = [];
  // No verdict NAME is hardcoded here: miss_forensics attaches beat evidence on
  // the faster-taker path ONLY (every other path returns those fields null), so
  // the per-field guards below already decide what carries into the title.
  if (detail) {
    if (dt != null) parts.push(beatDt(dt));
    if (price != null) parts.push('@' + fmt.prob(price, 3));
    if (size != null) parts.push('×' + fmt.num(size, size >= 10 ? 0 : 1));
    if (rival) parts.push('by ' + fmt.shortAddr(rival));
  }
  const evidence = parts.length ? '\n\nbeat evidence: ' + parts.join(' ') : '';
  return (
    <span className={'pill ' + m.pill}
      title={m.title + evidence + (rival ? `\nrival maker (resting order owner): ${rival}` : '')}>
      {m.label}
    </span>
  );
}

// The payload spells the chain fields TWO ways — camelCase on a reject-attempt
// row (client-normalized) and snake_case on a server missed_trade row — so both
// call sites map through here instead of each spelling the props out. Wrong
// spelling = a silently blank chip, which is exactly the failure this prevents.
function chainChipProps(row) {
  if (!row) return null;
  const verdict = row.chainVerdict != null ? row.chainVerdict : row.chain_verdict;
  if (!verdict) return null;
  const pick = (camel, snake) => (row[camel] != null ? row[camel] : row[snake]);
  return {
    verdict,
    dt: pick('chainBeatDt', 'chain_beat_dt_s'),
    price: pick('chainBeatPrice', 'chain_beat_price'),
    size: pick('chainBeatSize', 'chain_beat_size'),
    rival: pick('chainRival', 'chain_rival_addr'),
    detail: true,
  };
}

// count chips are chrome (never green/red) except a live reject count.
function CountPill({ n, neg }) {
  return <span className={'pill ' + (neg && n > 0 ? 'pill-neg' : 'pill-mute')}>{n}</span>;
}

// Band badge: the ask band that attempted this resolution. A neutral pill (never green/red); it
// identifies the sleeve, not an outcome.
function BandPill({ band }) {
  if (!band) return <span className="muted">—</span>;
  return <span className="pill pill-mute" title="ask band(s) that attempted this resolution">{band}</span>;
}

// Counterfactual (would-be) PnL of rejected/unfilled orders — PAPER, never
// real money. Muted + italic + parenthesized (+ a small "missed" tag on the
// grouped row) so a reader can never mistake it for realized PnL, which
// renders toned (pos/neg) and upright. `dim` fades a counterfactual the CHAIN
// proved unwinnable (repriced / maker cancel — the at-limit number is a
// mirage).
function MissedPnl({ value, tagless, dim, toned, title }) {
  // `toned` colors the paper value pos/neg — used when the tape proves the
  // level was live (faster taker), i.e. money we genuinely lost the race for.
  // Still italic+parenthesized so it can never be mistaken for realized money.
  const cls = toned ? (tone(value) || 'muted') : 'muted';
  return (
    <span className={cls}
      style={{ fontStyle: 'italic', ...(dim ? { opacity: 0.55 } : null) }}
      title={title || 'would-be PnL had the rejected order(s) filled — counterfactual, not realized money'}>
      ({fmt.moneyDelta(value)}){!tagless && <span className="slot-ref"> missed</span>}
    </span>
  );
}


// ── per-attempt drill-down (inside the expand) ───────────────────────
function AttemptTable({ group }) {
  const rows = group.attemptRows;
  return (
    <div style={{ borderLeft: '2px solid var(--accent-line)', paddingLeft: 12 }}>
      <table className="tbl compact">
        <thead><tr>
          <th className="num">offset</th><th className="num">#</th><th>dir</th>
          <th>band</th>
          <th className="num">exec / limit</th><th className="num">requested</th>
          <th className="num">filled</th><th className="num">fill px</th>
          <th className="num" title="How far the ask moved AFTER this attempt was already killed (post-reject re-read). Negative = it came back cheaper later — the FAK was dead by then and cannot fill on later prices.">Δask after</th><th>outcome</th><th className="num">P&amp;L</th>
        </tr></thead>
        <tbody>
          {rows.map((a, i) => {
            const zero = !(a.filled > 0);
            return (
              <tr key={i}>
                <td className="num" style={{ whiteSpace: 'nowrap' }}>{a.offset != null ? `+${a.offset}s` : '—'}
                  {/* base / top-up N: the same fill labels as the Trade journal's expanded trade,
                     so a grouped row can always be traced back to its fills.
                     */}
                  {a.fillLabel && <span className="slot-ref"> {a.fillLabel}</span>}</td>
                <td className="num muted">{a.n}</td>
                <td><SidePill side={a.side} /></td>
                <td><BandPill band={a.band} /></td>
                <td className="num">{a.price != null ? fmt.prob(a.price, 3) : '—'}</td>
                <td className="num">{a.requested != null ? fmt.money(a.requested) : '—'}</td>
                <td className={'num ' + (a.kind === 'reject' || zero ? 'neg' : '')}>
                  {a.kind === 'reject' ? fmt.money(0) : (a.filled != null ? fmt.money(a.filled) : '—')}</td>
                <td className="num">{a.fillPx != null ? fmt.prob(a.fillPx, 3) : '—'}</td>
                <td className={'num ' + driftClass(a.drift)}
                    title={a.kind === 'reject'
                      ? 'Ask move AFTER the reject — measured on a later re-read, not a price this attempt could have filled at.'
                      : undefined}>
                  {a.kind === 'reject' ? driftCents(a.drift) : '—'}</td>
                <td>{a.kind === 'reject'
                  ? <React.Fragment>
                      <OutcomePill outcome={a.outcome} />
                      {/* per-ATTEMPT miss cause — the tape, and only the tape */}
                      {chainChipProps(a) && (
                        <div style={{ marginTop: 3 }}>
                          <ChainVerdictChip {...chainChipProps(a)} />
                        </div>
                      )}
                    </React.Fragment>
                  : <StatusPill status={a.outcome} />}</td>
                <td className={'num ' + (a.kind !== 'reject' && a.pnl != null ? tone(a.pnl) : 'muted')}>
                  {a.kind === 'reject'
                    ? (a.missed != null ? <MissedPnl value={a.missed} tagless /> : (a.outcome === 'PENDING' ? '…' : '—'))
                    : (a.pnl != null ? fmt.moneyDelta(a.pnl)
                      : a.outcome === 'PENDING'
                        ? (a.markPx != null
                          ? <span className="mono" title="unrealized mark — best executable sell (current book)">mark {fmt.prob(a.markPx, 3)}</span>
                          : '…')
                        : '—')}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── grouped resolution rows (one row per resolution, cross-sleeve) ────
// `missIdx` is the server missed_trades lookup (missedTradeIndex) — a missed
// resolution is identified by its ON-CHAIN VERDICT chip (the cause, from the
// tape) and shows the status pill only when no verdict/kind chip is available
// to carry that job. Its counterfactual is the server's, priced at OUR limit.
// Older payloads (no missed_trades) fall back to the client-derived missed
// value — guard-rendered.
function ResolutionRows({ groups, openSet, toggle, missIdx }) {
  return groups.map((g) => {
    const isOpen = openSet.has(g.key);
    const single = g.attempts <= 1;
    // A position built from a base bet + top-ups: ONE row, "N in 1", the same
    // badge and the same fold as the Trade journal.
    const multi = g.nFills > 1;
    const zeroFill = !(g.filled > 0);
    const isMissRow = g.status === 'NO_FILL' || g.status === 'GHOST_FILL';
    const miss = isMissRow ? missedTradeForGroup(missIdx, g) : null;
    // Chain-driven presentation of the paper counterfactual: TONED when the
    // tape proves the level was live (we lost a real race), DIMMED when it
    // proves nothing could have won it, plain when there is no verdict yet.
    const winnable = miss ? miss.chain_winnable : null;
    const missVal = miss && miss.missed_pnl != null ? miss.missed_pnl : g.missedPnl;
    const meterColor = zeroFill ? 'var(--neg)'
      : g.fillRatio < 0.995 ? 'var(--warn)' : 'var(--pos)';
    return (
      <React.Fragment key={g.key}>
        <tr className={'trade-row' + (isOpen ? ' open' : '')} onClick={() => toggle(g.key)}>
          <td className="primary">
            <span className="row-toggle" style={single ? { color: 'var(--fg-4)' } : undefined}>
              {isOpen ? '−' : '+'}
            </span>
            <CoinCell coin={g.coin} sub={g.interval} />
            {multi && <span className="fill-badge"
              title={`${g.nFills} fills (base + ${g.nFills - 1} top-up${g.nFills > 2 ? 's' : ''}) — one CLOB position. Expand for the individual fills.`}>{g.nFills} in 1</span>}
          </td>
          <td><SidePill side={g.side} /></td>
          <td><BandPill band={g.band} /></td>
          <td className="num" title={multi ? `Σ of ${g.nFills} requested stakes` : undefined}>{g.target != null ? fmt.money(g.target) : '—'}</td>
          <td className={'num ' + (zeroFill ? 'neg' : '')}
            title={multi ? `Σ of ${g.nFills} fills` : undefined}>
            {fmt.money(g.filled)}
            <MeterBar ratio={g.fillRatio} color={meterColor} />
          </td>
          <td className={'num ' + (g.fillRatio < 0.995 ? 'neg' : 'muted')}>{fmt.pct(g.fillRatio, 0)}</td>
          <td className="num"><CountPill n={g.attempts} /></td>
          <td className="num"><CountPill n={g.nRejects} neg /></td>
          <td className="num" style={{ whiteSpace: 'nowrap' }}
            title={multi ? `size-weighted across the ${g.nFills} fills` : undefined}>
            {g.execPx != null ? fmt.prob(g.execPx, 3) : '—'}
            {' → '}
            {g.fillPx != null ? fmt.prob(g.fillPx, 3) : '—'}
          </td>
          {/* The miss cause: the tape's answer to "faster taker or maker cancel". Absent until the
             forensics job classifies the row; a miss that never reached the book (ghost, venue
             error) never gets one and shows its kind instead. Whichever chip is present is the
             miss label, so the status pill is dropped on those rows; with neither chip (not
             classified yet) the pill alone reads "miss".
             */}
          <td>{chainChipProps(miss)
              ? <ChainVerdictChip {...chainChipProps(miss)} />
              : miss && MISS_KIND[miss.miss_kind]
              ? <MissKindPill kind={miss.miss_kind} />
              : <StatusPill status={g.status} />}</td>
          <td className={'num ' + (g.pnl != null ? tone(g.pnl) : 'muted')}
            style={{ whiteSpace: 'nowrap' }}
            title={multi ? `Σ over the ${g.nFills} fills` : undefined}>
            {isMissRow && missVal != null
              ? <MissedPnl value={missVal}                        /* paper: no real fill */
                  dim={winnable === false}
                  toned={winnable === true}
                  title={winnable === true
                    ? 'would-be PnL at OUR limit — the tape proves a print matched at our level, so this race was genuinely lost'
                    : winnable === false
                    ? 'would-be PnL at OUR limit — the tape proves our level never traded, so this counterfactual is a mirage'
                    : 'would-be PnL at OUR limit — no chain verdict for this miss yet'} />
              : g.pnl != null ? fmt.moneyDelta(g.pnl)             /* realized wins the column */
              : g.status === 'PENDING' ? '…' : '—'}</td>
          <td className="num muted">{fmt.dateMyt(g.displayTime)} {fmt.hmMyt(g.displayTime)}</td>
        </tr>
        {isOpen && (
          <tr className="detail-row"><td colSpan={12}><AttemptTable group={g} /></td></tr>
        )}
      </React.Fragment>
    );
  });
}

// ── on-chain forensics strip (v2.reject_stats.chain_forensics) ─────────── Three questions, in
// order:
//
//   1. What happened to our misses? (verdict counts from the trade tape)
//   2. By how much did we lose the races? (median beat time, in venue-match
//      time: the block timestamp minus the calibrated settlement lag)
//   3. Is speed worth buying? The speed prize compares the FASTER_TAKER
//      misses' would-be win rate with the break-even rate implied by the price
//      we bid. Those are the only misses a faster bot could have won, so if the
//      two are about equal, more speed is worth roughly nothing.
//
// Renders nothing until the job has classified something.
function ChainForensicsStrip({ cf }) {
  if (!cf || !cf.n) return null;
  const counts = cf.counts || {};
  const sp = cf.speed_prize || {};
  // Display order IS the SERVER's key order (_CHAIN_VERDICTS — strongest
  // evidence first), so a verdict added upstream appears here without editing
  // this file. Zero-count keys are omitted server-side.
  const shown = Object.keys(counts).filter((k) => counts[k] > 0);
  const prize = sp.edge_pts;
  return (
    <React.Fragment>
      <div className="section-gap" />
      <div className="card-note" style={{ borderTop: '1px solid var(--border)' }}
        title="Verdicts come from the on-chain trade tape (Polymarket data-api + Polygon receipts), classified off the decision path by the miss_forensics job. A print inside our level band — from the ask we saw up to our limit — within seconds of our order PROVES the quote was real and another taker took it; no print at all means the maker withdrew it. Timing is venue-match time — the block timestamp minus the calibrated ~3s settlement lag — so a negative dt means their order matched before ours arrived.">
        <div className="muted" style={{ marginBottom: 4 }}>
          on-chain miss forensics · last {cf.window_days}d ·{' '}
          <span className="mono">{fmt.intc(cf.n_classified)}</span> of{' '}
          <span className="mono">{fmt.intc(cf.n)}</span> classified
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
          {shown.map((k) => (
            <span key={k} style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
              <ChainVerdictChip verdict={k} />
              <span className="mono">{fmt.intc(counts[k])}</span>
              {k === 'FASTER_TAKER' && cf.share_faster_taker != null && (
                <span className="muted"> ({fmt.pct(cf.share_faster_taker, 0)} of classified)</span>
              )}
            </span>
          ))}
          {cf.median_beat_dt_s != null && (
            <span className="muted">· median beat-us{' '}
              <span className="mono">{beatDt(cf.median_beat_dt_s)}</span></span>
          )}
        </div>
        <div className="muted" style={{ marginTop: 6 }}
          title="Would-win WR of the FASTER_TAKER cohort vs the break-even WR its own limit prices in. Equal = winning every one of those races earns nothing, so latency is NOT the lever. Only this cohort counts: repriced and maker-cancel misses were never winnable at any speed.">
          speed prize (would a faster bot have earned anything?):{' '}
          {sp.would_win_wr != null && sp.breakeven_wr != null
            ? <React.Fragment>
                would-win <span className="mono">{fmt.pct(sp.would_win_wr)}</span>
                {' vs break-even '}<span className="mono">{fmt.pct(sp.breakeven_wr)}</span>
                {' = '}
                {/* pctDelta already carries the % sign — appending "pts" too
                    would render "+1.7% pts". */}
                <span className={'mono ' + (prize != null ? tone(prize) : '')}>
                  {prize != null ? fmt.pctDelta(prize) : '—'}
                </span>
                <span className="muted"> over {fmt.intc(sp.n_resolved)} resolved</span>
              </React.Fragment>
            : <span title="suppressed below 3 resolved faster-taker misses — too few to price the speed prize">—</span>}
        </div>
      </div>
    </React.Fragment>
  );
}

// ── missed-trades panel (the fill-tax totals; rows fold into the resolution table above)
// ────────────────────────────────────────────────────────── Attempts and trades are labelled
// separately: N FAK retries of one window are one missed trade, and a window that later filled is
// none. Every counterfactual is priced at our limit. The cause split is the on-chain verdict. Older
// payloads without the rollup get the simpler four-tile strip.
function RejectStats({ stats, missedTrades }) {
  const rs = stats || {};
  // Per-VERDICT would-be WR, computed client-side from the server
  // missed_trades rows (side vs resolved) — the same rows the counts come
  // from, so the table can never disagree with itself. The same pass lifts the
  // server's own `chain_winnable` verdict → so "is this cohort a mirage?" is
  // read off the payload rather than re-deciding it from a hardcoded list.
  const verdictWr = {};
  const verdictWinnable = {};
  (missedTrades || []).forEach((t) => {
    const v = t.chain_verdict;
    if (!v) return;
    if (!(v in verdictWinnable)) {
      verdictWinnable[v] = t.chain_winnable != null ? t.chain_winnable : null;
    }
    const d = directionFromSide(t.side);
    const ro = String(t.resolved_outcome || '').toUpperCase();
    if ((d !== 'UP' && d !== 'DOWN') || (ro !== 'UP' && ro !== 'DOWN')) return;
    const a = verdictWr[v] || (verdictWr[v] = { resolved: 0, won: 0 });
    a.resolved += 1;
    if (d === ro) a.won += 1;
  });
  const fillRateCell = (
    <KpiCell label="fill rate · all" value={fmt.pct(rs.fill_rate_usd)} valueSm
      valueClass={rs.fill_rate_usd != null && rs.fill_rate_usd < 0.5 ? 'neg' : ''}
      title="HISTORICAL (full) fill rate: Σ filled / Σ requested over the record window (v2 go-live / operator reset). The rolling 24h figure is the top-right tile."
      bar={{ pct: (rs.fill_rate_usd || 0) * 100,
             color: rs.fill_rate_usd != null && rs.fill_rate_usd < 0.5 ? 'var(--neg)' : 'var(--pos)' }} />
  );
  if (rs.n_missed_resolutions == null) {
    // Legacy payload (pre per-resolution rollup) — the old strip.
    return (
      <React.Fragment>
        <div className="section-gap" />
        <div className="kpi-grid c4">
          {fillRateCell}
          <KpiCell label="rejected" value={fmt.intc(rs.n_rejected)} valueSm
            sub={`${rs.n_accepted != null ? fmt.intc(rs.n_accepted) : 0} accepted`} />
          <KpiCell label="missed PnL" value={fmt.moneyDelta(rs.missed_pnl_total)} valueSm
            valueClass={tone(rs.missed_pnl_total)}
            title="would-be PnL, counted once per resolution" />
          <KpiCell label="would-be WR" value={fmt.pct(rs.reject_winrate)} valueSm
            title="share of resolved missed resolutions that would have won — suppressed ('—') below 3 resolved misses" />
        </div>
      </React.Fragment>
    );
  }
  const chainCounts = rs.chain_counts || {};
  const chainPnl = rs.chain_pnl || {};
  // Server key order = _CHAIN_VERDICTS (strongest evidence first).
  const verdictRows = Object.keys(chainCounts).filter((k) => chainCounts[k] > 0);
  const nMissed = rs.n_missed_resolutions != null ? rs.n_missed_resolutions : 0;
  const nNoVerdict = rs.n_no_chain_verdict || 0;
  const noVerdictKinds = rs.no_chain_verdict_kinds || {};
  // Why those rows carry no verdict, in the server's own kind vocabulary. A
  // book_miss without a verdict is simply one the job has not reached.
  const noVerdictWhy = Object.keys(noVerdictKinds)
    .filter((k) => noVerdictKinds[k] > 0)
    .map((k) => `${fmt.intc(noVerdictKinds[k])} ${MISS_KIND[k] ? MISS_KIND[k].label : 'awaiting the job'}`)
    .join(' · ');
  return (
    <React.Fragment>
      <div className="section-gap" />
      <Panel title="missed trades" q={`${nMissed}`}
        titleHint={"One missed trade per RESOLUTION — N FAK retries fold into 1, and a window that later filled counts 0. "
          + "CAUSE comes from the on-chain trade tape: a print at our level proves the quote was real and someone matched it first; no print at all means the maker withdrew it. "
          + "The old post-reject book re-read could not tell those two apart and read them BACKWARDS (2026-07-30 forensic: it implied ~68% maker cancel, the tape says the reverse — live share in the strip below), so it no longer labels anything. "
          + "Every counterfactual below is priced AT OUR LIMIT."}
        sub={rs.n_reject_resolutions_filled ? `${rs.n_reject_resolutions_filled} filled after a reject` : null}
        flush>
        <div className="kpi-grid c5">
          {fillRateCell}
          <KpiCell label="rejected attempts" value={fmt.intc(rs.n_rejected)} valueSm
            title="every FAK retry is its own order attempt — NOT its own missed trade"
            sub={`${rs.n_accepted != null ? fmt.intc(rs.n_accepted) : 0} accepted attempts · ${fmt.intc(nMissed)} missed trades`} />
          <KpiCell label="missed to a faster taker"
            value={fmt.moneyDelta(rs.missed_pnl_winnable)} valueSm
            valueClass={tone(rs.missed_pnl_winnable)}
            title="ON-CHAIN: a print landed inside our level band — the quote was real and another taker matched it first. The ONLY cohort a faster bot could ever have won, so this number is the entire upside of buying latency." />
          <KpiCell label="missed — unwinnable"
            value={<span className={tone(rs.missed_pnl_never_winnable)} style={{ fontStyle: 'italic' }}>{fmt.moneyDelta(rs.missed_pnl_never_winnable)}</span>}
            valueSm
            title="ON-CHAIN: repriced (prints exist, none at our level) or maker cancel (no print at all). Our level never traded, so no order at any speed takes these — the at-limit counterfactual is a mirage." />
          <KpiCell label="would-be WR" value={fmt.pct(rs.reject_winrate)} valueSm
            title="share of resolved missed trades that would have won — suppressed ('—') below 3 resolved misses"
            sub={rs.reject_winrate == null ? null
              : `of ${fmt.intc(rs.n_resolved_misses)} resolved missed trades`} />
        </div>
        <ChainForensicsStrip cf={rs.chain_forensics} />
        {(verdictRows.length > 0 || nNoVerdict > 0) && (
          <React.Fragment>
            <div className="section-gap" />
            <div className="tbl-wrap"><table className="tbl compact">
              <thead><tr>
                <th>chain verdict</th>
                <th className="num" title="missed RESOLUTIONS in the record window — the rows listed above, not the 7d attempt counts in the strip">missed trades</th>
                <th className="num">would-be WR</th>
                <th className="num">would-be PnL</th><th>on-chain evidence</th>
              </tr></thead>
              <tbody>
                {verdictRows.map((k) => {
                  const m = verdictMeta(k);
                  const w = verdictWr[k];
                  // The SERVER decides which cohort is unwinnable — THREE states,
                  // not two: false = the tape proves our level never traded (a
                  // mirage counterfactual), true = it proves the level was live,
                  // null = not adjudicated (PENDING / UNRESOLVED / a verdict this
                  // build has no winnable claim for). Only a PROVEN cohort earns
                  // a tone; toning the unknown one over-claims money the job has
                  // not adjudicated — the same rule ResolutionRows applies.
                  const winnable = verdictWinnable[k];
                  const mirage = winnable === false;
                  return (
                    <tr key={k}>
                      <td><ChainVerdictChip verdict={k} /></td>
                      <td className="num">{fmt.intc(chainCounts[k])}</td>
                      <td className="num muted"
                        title={w ? `${w.won} of ${w.resolved} resolved would have won` : 'no resolved misses yet'}>
                        {w && w.resolved ? fmt.pct(w.won / w.resolved) : '—'}</td>
                      <td className={'num ' + (winnable === true ? tone(chainPnl[k]) : 'muted')}
                        style={mirage ? { fontStyle: 'italic', opacity: 0.55 } : undefined}
                        title={mirage ? 'our level never traded — the at-limit counterfactual is a mirage'
                          : winnable === true ? undefined
                          : 'not adjudicated — the tape has not said whether our level traded'}>
                        {chainPnl[k] != null ? fmt.moneyDelta(chainPnl[k]) : '—'}</td>
                      <td className="muted" style={{ maxWidth: 520 }} title={m.title}>{m.reason}</td>
                    </tr>
                  );
                })}
                {nNoVerdict > 0 && (
                  <tr>
                    <td><span className="pill pill-mute"
                      title="no row in miss_forensics: the job classifies exactly the no_liquidity book misses, so a ghost fill or a venue error can never get a verdict — and a book miss it has not reached yet has none YET.">no chain row</span></td>
                    <td className="num">{fmt.intc(nNoVerdict)}</td>
                    <td className="num muted">—</td>
                    {/* Muted, never toned — nothing is proven about this cohort.
                        It carries dollars all the same, and printing them here is
                        what keeps the table a PARTITION of the missed total. */}
                    <td className="num muted"
                      title="would-be PnL at OUR limit for the misses with no chain row — not adjudicated, so no claim either way">
                      {rs.missed_pnl_no_verdict != null
                        ? fmt.moneyDelta(rs.missed_pnl_no_verdict) : '—'}</td>
                    <td className="muted" style={{ maxWidth: 520 }}>
                      not adjudicable (or not yet adjudicated){noVerdictWhy ? ` — ${noVerdictWhy}` : ''}</td>
                  </tr>
                )}
              </tbody>
            </table></div>
          </React.Fragment>
        )}
      </Panel>
    </React.Fragment>
  );
}


// ── page ─────────────────────────────────────────────────────────────
const RESOLUTION_PAGE_SIZE = 30;   // rows per page (Previous / Next paging)

function SubminutePage({ live }) {
  // The sub-minute stream (live.v2 bets, rejects, sizing) is whatever this process wrote, so it
  // always belongs to this console's venue. Hooks must come before the no-v2 early return (rules of
  // hooks).
  const v2 = live.v2;

  // Fold every attempt (fills + rejects — retries and top-ups) for a
  // resolution into POSITION rows: a base bet and its top-ups are one CLOB
  // position and share a row (the journal's groupTrades fold); a miss is
  // counted once.
  const groups = useMemo(
    () => groupResolutions((v2 && v2.bets) || [], (v2 && v2.rejects) || []),
    [v2]
  );
  // Server-classified missed trades (one per resolution) → the ON-CHAIN verdict
  // chip (or the non-causal miss KIND when no verdict exists) + the server's
  // at-our-limit counterfactual on the miss rows. Empty on old payloads.
  const missIdx = useMemo(
    () => missedTradeIndex((v2 && v2.missed_trades) || []),
    [v2]
  );
  // Exact grouping when the payload carries window_start_ts (bets or rejects);
  // otherwise the boundary is reconstructed and the grouping is approximate.
  const exactGrouping = useMemo(
    () => ((v2 && v2.bets) || []).some((b) => b.window_start_ts != null)
       || ((v2 && v2.rejects) || []).some((r) => r.window_start_ts != null),
    [v2]
  );
  const [page, setPage] = useState(0);
  const { openSet, toggle } = useOpenSet();

  if (!v2) {
    return (
      <div className="content">
        <EmptyState big="sub-minute executor disabled"
          msg="the payload has no live.v2 block — the sub-minute executor is not enabled on this deployment (CRYPTOBOT_SUBMINUTE_ENABLED=0)" />
      </div>
    );
  }

  const sizing = v2.sizing || {};
  const fr = v2.fill_rate || {};
  // Full history is streamed (config.V2_BETS_LIMIT / V2_REJECTS_LIMIT), but the
  // DOM stays bounded: 30 rows per page with Previous / Next.
  const pager = paginate(groups, page, RESOLUTION_PAGE_SIZE);
  const visibleGroups = pager.slice;

  const fillWarn = fr.fill_rate != null
    && (fr.n_orders || 0) >= (fr.min_orders || 0)
    && fr.min_fill_rate != null && fr.fill_rate < fr.min_fill_rate;
  // The breaker is a live control: shown whenever it is tripped or inside its trip condition,
  // hidden otherwise.
  const breakerWarn = fillWarn || !!fr.tripped;
  const brake = sizing.brake_multiplier != null ? sizing.brake_multiplier : 1;
  const brakeFloor = sizing.brake_floor != null ? sizing.brake_floor : BRAKE_FLOOR_FALLBACK;
  // The DD-brake tile is ANOMALOUS-ONLY: rendered only when the brake is
  // actually below full size or resting on a REAL (non-1.00) floor.
  const showBrakeTile = brake < 0.999
    || (!!sizing.brake_at_floor && !throttleOff(brakeFloor));
  const stake15 = sizing.equity != null ? sizing.equity * (sizing.base_frac_15m || 0) * brake : null;
  const stake5 = sizing.equity != null ? sizing.equity * (sizing.base_frac_5m || 0) * brake : null;

  return (
    <div className="content">
      <BrakeBanner sizing={sizing} />

      <div className={'kpi-grid ' + (showBrakeTile ? 'c4' : 'c3')}>
        <KpiCell label={sizing.equity_basis === 'ledger_fallback' ? 'equity (ledger)' : 'equity (CLOB)'}
          value={fmt.money(sizing.equity)} valueSm
          title={sizing.equity_basis === 'ledger_fallback'
            ? 'ledger paper equity — no CLOB balance yet'
            : 'live wallet balance from the CLOB'}
          sub={sizing.peak_capital != null ? `peak ${fmt.money(sizing.peak_capital)}` : null} />
        {/* Drawdown-brake tile: shown only when the brake is active (`showBrakeTile`), with the
           live multiplier, the drawdown that caused it and the floor, alongside the warning banner
           above.
           */}
        {showBrakeTile && (
          <KpiCell label="DD-brake" value={`×${fmt.num(brake, 2)}`} valueSm
            valueClass={sizing.brake_at_floor ? 'neg' : 'warn'}
            title="stake multiplier driven by drawdown FROM PEAK = (peak − equity) / peak — the same metric the hard halt gates on"
            sub={`dd from peak ${fmt.pct(sizing.dd_frac)} · floor ×${fmt.num(brakeFloor, 2)}`}
            bar={{ pct: brakeMeter(brake, brakeFloor) * 100,
                   color: sizing.brake_at_floor ? 'var(--neg)' : 'var(--warn)' }} />
        )}
        <KpiCell label="next stake" value={fmt.money(stake15)} valueSm
          title={`stake = equity × base frac × DD-brake · 15m ${fmt.pct(sizing.base_frac_15m)} · 5m ${fmt.money(stake5)} (${fmt.pct(sizing.base_frac_5m)})`} />
        {/* Top right is the rolling 24h fill rate; the full-history figure is bottom right in the
           missed-trades strip. The breaker's window and threshold appear as a warning sub-line
           only when it is tripped or within its trip condition (`fillWarn`).
           */}
        <KpiCell label="fill rate · 24h"
          value={fr.fill_rate_24h != null ? fmt.pct(fr.fill_rate_24h) : '—'}
          valueClass={breakerWarn ? 'neg' : ''}
          title={`Σ filled / Σ requested over sub-minute orders placed in the last 24h${fr.n_orders_24h != null ? ` (${fr.n_orders_24h} orders)` : ''}. The circuit breaker gates on its OWN window — the newest ${fr.n_orders != null ? fr.n_orders : 0} orders, currently ${fr.fill_rate != null ? fmt.pct(fr.fill_rate) : '—'} — and trips below ${fmt.pct(fr.min_fill_rate, 0)} over ${fr.min_orders != null ? fr.min_orders : '—'}+ orders.`}
          sub={breakerWarn
            ? `breaker ${fr.tripped ? 'TRIPPED' : 'warning'} · ${fmt.pct(fr.fill_rate)} over ${fr.n_orders != null ? fr.n_orders : 0} orders < ${fmt.pct(fr.min_fill_rate, 0)}`
            : null}
          bar={{ pct: (fr.fill_rate_24h || 0) * 100, color: breakerWarn ? 'var(--neg)' : 'var(--pos)' }} />
      </div>

      {/* Titled "resolutions", not "bets": the count includes resolutions that never filled (pure
         misses from the rejects stream) and pending ones, over the newest rows, so it is a
         different population from the settled trades counted elsewhere.
         */}
      <Panel title="resolutions" q={`${groups.length}`}
        titleHint={(exactGrouping ? '' : 'Grouping is APPROXIMATE — the window key is not yet in the payload. ')
          + 'ATTEMPT-level population: one row per resolution/position INCLUDING resolutions that never filled — not the settled-trade count the Overview/Journal report. '
          + 'One row per POSITION: all fills of one position are one CLOB balance, so they fold into a single "N in 1" row (Σ size, Σ P&L, size-weighted price) — the same fold the Trade journal applies. Attempt and reject counts stay per-ORDER; expand a row for the individual fills and attempts. '
          + 'Newest-rows window (server bets/rejects caps); lifetime totals live on Overview/Journal.'} flush>
        {groups.length === 0 ? <EmptyState msg="no v2 resolutions yet" /> : (
          <React.Fragment>
            <div className="tbl-wrap"><table className="tbl subminute-res">
              <thead><tr>
                <th>coin</th><th>dir</th><th>band</th><th className="num">target</th>
                <th className="num">filled</th><th className="num">fill %</th>
                <th className="num">attempts</th><th className="num">rejects</th>
                <th className="num">exec → fill</th><th>status</th>
                <th className="num">P&amp;L</th><th className="num">time</th>
              </tr></thead>
              <tbody><ResolutionRows groups={visibleGroups} openSet={openSet} toggle={toggle} missIdx={missIdx} /></tbody>
            </table></div>
            <TablePager pager={pager} setPage={setPage} />
          </React.Fragment>
        )}
      </Panel>
      <div className="section-gap" />

      <RejectStats stats={v2.reject_stats} missedTrades={v2.missed_trades} />
    </div>
  );
}

window.SubminutePage = SubminutePage;

// ── Sub-minute spec page (Config section) ────────────────────────────
// The deployed strategy contract as one compact reference table. Every value
// comes from the live payload (live.v2.spec, live.v2.sizing, live.portfolio)
// with '—' fallbacks, so nothing is hardcoded. Groups: bands, decision spec,
// sizing, execution, halt.
function SubminuteSpecPage({ live }) {
  const v2 = live.v2;
  if (!v2) {
    return (
      <div className="content">
        <EmptyState big="sub-minute executor disabled"
          msg="the payload has no live.v2 block — the sub-minute executor is not enabled on this deployment (CRYPTOBOT_SUBMINUTE_ENABLED=0)" />
      </div>
    );
  }
  const s = v2.spec || {};
  const z = v2.sizing || {};
  const bands = specBands(s);
  const venueFloor = live.latency && live.latency.venue_floor;
  const takerDelay = venueFloor && venueFloor.taker_delay_ms != null
    ? venueFloor.taker_delay_ms : null;
  const venueDelayText = takerDelay != null
    ? `${takerDelay}ms uncancellable taker delay (live venue floor)`
    : 'not reported by live venue telemetry';
  const p = live.portfolio || {};

  // Sizing: with the brake floor at 1.00 the throttle is off; the row says so
  // and the formula goes in the tooltip.
  const brakeRemoved = z.brake_floor != null && throttleOff(z.brake_floor);
  const brakeRule = brakeRemoved
    ? 'removed — floor ×1.00, full size at every drawdown depth'
    : `full size at 0% dd-from-peak → ×${z.brake_floor != null ? fmt.num(z.brake_floor, 2) : '—'} at ≥${z.throttle_dd_at_floor != null ? fmt.pct(z.throttle_dd_at_floor, 1) : '—'} dd from peak`;
  const brakeHint = `brake = max(×${z.brake_floor != null ? fmt.num(z.brake_floor, 2) : '—'}, 1 − dd/${z.brake_scale != null ? fmt.num(z.brake_scale, 2) : '—'}) · dd = (peak − equity) / peak`
    + (brakeRemoved ? ' — identically ×1.00, so the hard halt is the only drawdown control' : '');
  // Execution: order type, anchored pad, retries, venue floor.
  const padValue = z.fak_pad != null && z.fak_pad > 0
    ? `+${fmt.num(z.fak_pad, 2)} on limit`
    : 'off — bare walked limit';
  // Per-coin hard bet caps from the strategy config (max_bet_usd_by_coin on
  // each sleeve). An empty map renders no row.
  const coinCaps = {};
  (((live.portfolio || {}).sleeves) || []).forEach((sl) => {
    const by = sl && sl.max_bet_usd_by_coin;
    if (by && typeof by === 'object') {
      Object.keys(by).forEach((c) => {
        const v = by[c];
        if (typeof v === 'number' && isFinite(v)) coinCaps[String(c).toUpperCase()] = v;
      });
    }
  });
  const coinCapText = Object.keys(coinCaps).sort()
    .map((c) => `${c} ≤ ${fmt.money(coinCaps[c])}`).join(' · ');

  // One row per spec field, one column per ask band. Fields the bands share
  // render once across all columns, so a single band gives a single column.
  const bandCols = bands.length ? bands : [{ label: '—' }];
  const nCols = bandCols.length;
  const range = (b) => (b.ask_lo != null ? `${fmt.prob(b.ask_lo, 2)} < ask ≤ ${fmt.prob(b.ask_hi, 2)}` : null);
  const GroupRow = ({ label }) => (
    <tr><td colSpan={nCols + 1} className="muted" style={{
      fontSize: 10, letterSpacing: '.12em', textTransform: 'uppercase',
      paddingTop: 14, borderBottom: '1px solid var(--line)' }}>{label}</td></tr>
  );
  // Per-band row: vals[i] aligns with bandCols[i]; shared row: one value
  // spanning every band column. Caveats go in `hint` (a tooltip), never in a
  // second line under the value.
  const Row = ({ k, vals, shared, mono = true, hint }) => (
    <tr title={hint || undefined}>
      <td className="muted" style={{ whiteSpace: 'nowrap', verticalAlign: 'top' }}>{k}</td>
      {shared !== undefined
        ? <td colSpan={nCols} className={mono ? 'mono' : ''} style={{ verticalAlign: 'top' }}>{shared == null ? '—' : shared}</td>
        : bandCols.map((b, i) => (
            <td key={i} className={mono ? 'mono' : ''} style={{ verticalAlign: 'top' }}>{vals && vals[i] != null ? vals[i] : '—'}</td>
          ))}
    </tr>
  );
  const bandShort = (b) => ((b.label || '—').split(' ')[0]);

  return (
    <div className="content">
      <Panel title="strategy" q={bandCols.map(bandShort).join(' vs ')}
        titleHint={`${nCols === 1 ? 'One ask band' : `${nCols} ask bands`} over the entry window; one entry per resolution, first fill wins.`} flush>
        <div className="tbl-wrap"><table className="tbl">
          <thead><tr>
            <th style={{ width: '18%' }}>spec</th>
            {bandCols.map((b, i) => <th key={i}>{b.label || '—'}</th>)}
          </tr></thead>
          <tbody>
            <Row k="ask band" vals={bandCols.map(range)} />
            <Row k="edge gate" vals={bandCols.map((b) => (b.gate != null ? `≥ ${fmt.num(b.gate, 2)}` : null))} />
            <Row k="sleeves" vals={bandCols.map((b) => (b.sleeves || []).join(' · ') || null)} />

            <GroupRow label="decision spec — shared" />
            <Row k="intervals" shared={(s.intervals || []).join(' + ') || null} />
            <Row k="edge" shared="model_p − ask − fee" />
            <Row k="taker fee" mono={false}
              shared={s.fee_spec
                || (s.poly_fee_theta != null ? `POLY_FEE_theta ${fmt.num(s.poly_fee_theta, 3)}` : null)} />
            <Row k="fee calibration" mono={false}
              shared={s.fee_calibration != null
                ? <span title="A flat-to-flat wallet audit (no open positions at either end) found the venue charged ~0.864x of the published fee curve. Applied only where realized pnl_net is booked (execution.grade_fees); decision math (edge gates, book walker) keeps the raw published theta, so bet selection is unchanged.">
                    ×{fmt.num(s.fee_calibration, 2)} on BOOKED fees · decision math keeps raw theta
                  </span>
                : null} />
            <Row k="cadence" shared={s.cadence || null} mono={false} />
            <Row k="entry offsets" shared={s.offset_windows && Object.keys(s.offset_windows).length
              ? Object.keys(s.offset_windows).map((iv) => `${iv}: ${s.offset_windows[iv]}`).join(' · ')
              : null} />
            <Row k="selection" shared={s.selection || 'first-fill-wins per resolution · dedup-before-gate'} mono={false} />
            <Row k="features" shared={s.features || null} mono={false} />
            <Row k="signal" shared={s.signal_source || null} />
            <Row k="coins" shared={(s.coins || []).join(' · ') || null} />

            <GroupRow label="sizing — shared · stake = equity × base frac × DD-brake × concurrency" />
            <Row k="base frac · 15m" shared={z.base_frac_15m != null ? fmt.pct(z.base_frac_15m) : null} />
            <Row k="base frac · 5m" shared={z.base_frac_5m != null ? fmt.pct(z.base_frac_5m) : null} />
            <Row k="capital fracs" shared={(live.portfolio && (live.portfolio.sleeves || []).length)
              ? (live.portfolio.sleeves || []).map((sl) => `${sl.label} ${sl.capital_frac != null ? fmt.pct(sl.capital_frac, 1) : '—'}`).join(' · ')
              : null} />
            <Row k="concurrency" shared={z.conc_downweight ? '1/√k same-minute downweight' : 'off'} mono={false} />
            <Row k="DD-brake" shared={brakeRule} mono={false} hint={brakeHint} />
            <Row k="entries per resolution" shared="one — first fill wins across bands" mono={false} />
            <Row k="min bet" shared={z.min_bet_usd != null ? fmt.money(z.min_bet_usd) : null} />
            {coinCapText && <Row k="per-coin cap" shared={coinCapText}
              hint="Hard per-bet cap from the deployed manifest (max_bet_usd_by_coin) — applied after every sizing multiplier." />}


            <GroupRow label="execution — shared" />
            <Row k="orders" shared="FAK (fill-and-kill) taker" mono={false} />
            <Row k="anchored pad" shared={padValue} mono={false}
              hint="limit = min(walked, first_limit) + pad · CRYPTOBOT_V2_FAK_PAD" />
            <Row k="retries" shared="consume-on-fill · per-second re-eval until a fill or the entry window ends" mono={false} />
            <Row k="venue delay" shared={venueDelayText} mono={false} />
            <Row k="fees" shared="live venue fees — charged and booked on every fill" mono={false}
              hint="The taker-fee contract in the decision-spec row is the SAME schedule the live grader books against; there is no paper-fee mode." />
            <Row k="exits" shared="none — every filled position HOLDs to binary settlement" mono={false} />

            <GroupRow label="halt — shared" />
            <Row k="hard halt" shared={z.hard_halt_frac != null ? `${fmt.pct(z.hard_halt_frac, 0)} drawdown from peak — (peak − equity) / peak` : null} mono={false} />
            <Row k="exposure caps" shared={p.shared_exposure_cap_frac != null
              ? `shared ×${fmt.num(p.shared_exposure_cap_frac, 2)} of equity${(p.sleeves || []).length ? ' · per-wallet ' + (p.sleeves || []).map((sl) => `${sl.label} ×${fmt.num(sl.exposure_cap_frac, 2)}`).join(' · ') : ''}`
              : null} />
          </tbody>
        </table></div>
      </Panel>
    </div>
  );
}
window.SubminuteSpecPage = SubminuteSpecPage;
})();
