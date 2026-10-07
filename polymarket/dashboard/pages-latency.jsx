// ===========================================================================
// pages-latency.jsx — LatencyPage.
//
// STRUCTURE (rewritten 2026-07-25 after the Polymarket latency audit). The page
// answers one question, top to bottom: WHERE DOES THE TIME GO, AND WHICH PART
// CAN I ACTUALLY CHANGE? Three rules it now obeys that the old page did not:
//
//  1. DECISION latency (tick start -> first order POST on the wire) is the
//     headline, not the whole-tick total. The total includes ~100ms of
//     post-POST tail that delays no fill, so presenting it as "decision->fill"
//     overstated the movable part by ~3x.
//  2. ACCEPT and REJECT orders are never blended. A venue-refused FAK is a real
//     completed round trip (~286ms — a 400 skips matching) vs ~415ms for an
//     accept that waits out the 250ms taker delay. Before the walker fix the
//     reject cohort recorded post=0 and got averaged in, so this page displayed
//     order post p50 ~= 0.0ms.
//  3. WALL-CLOCK and STALENESS are separate sections. A 1ms-fast tick that
//     priced a 7s-old ladder is the worse outcome, and the venue WS pushes
//     ~822 msg/s (p50 1ms inter-frame, measured), so a large book age is always
//     a consumer defect — never the feed.
// ===========================================================================
(function () {
const { fmt, tone, takerDelayShortfall,
  LATENCY_ADDITIVE_STAGES, latencyResidual } = window.CB;

// Display order = the additive phases of a tick, then the rows measured INSIDE
// `order` directly under it, then the total. `LATENCY_ADDITIVE_STAGES`
// (window.CB) is the sum set; anything here and not in it is a nested detail
// row that is deliberately NOT added up.
const STAGE_ORDER = ['spot', 'ctx', 'gamma', 'book', 'order', 'sign', 'post', 'total'];
const SOFT_DEADLINE_MS = 900;

function ms(x) {
  if (x == null || !isFinite(x)) return '—';
  return Math.round(x).toLocaleString() + 'ms';
}
function pct(n, d) {
  if (!d) return '—';
  return Math.round((n / d) * 100) + '%';
}
function stat(block, name, key) {
  const s = block && block[name];
  return s && typeof s[key] === 'number' ? s[key] : null;
}

// The "where the tick goes" PANEL and its BudgetBar (a stacked decision /
// venue-round-trip / post-POST-tail bar with the "one order tick · p50",
// "Nms wall clock · Nms to fill" and "250ms taker delay — uncancellable"
// captions, plus the BUDGET descriptor and its `.lat-*` CSS) were DELETED
// 2026-08-05 by operator order: every figure on it is on this page already —
// decision p50 in the KPI strip, the venue round trip in the accept/reject
// cohort panel, the taker delay in its own stage row, and the whole-tick wall
// in the stage table's `total`. Do not re-add.

// ── 1. accept vs reject, never blended ───────────────────────────────
function CohortPanel({ orders, venueFloor }) {
  const po = (orders && orders.per_order) || {};
  const acc = po.post, rej = po.post_rej;
  const nAcc = orders.n_accepted, nRej = orders.n_rejected;
  const nTot = (nAcc || 0) + (nRej || 0);
  // `why` is no longer a COLUMN (2026-08-05 declutter — the operator deleted
  // the "what it is" prose column): it rides the row label's title tooltip.
  const rows = [
    { k: 'accepted', s: acc, n: nAcc,
      why: 'waits out the venue taker delay + matching' },
    { k: 'rejected', s: rej, n: nRej,
      why: 'venue refused (no_liquidity) — a 400 returns before matching' },
  ];
  return (
    <Panel title="POST round trip · accepted vs rejected"
      titleHint={'these are DIFFERENT populations and are never averaged together: a reject skips matching, so blending them describes neither. A reject is still a real completed round trip — it is not free.'
        + (venueFloor ? ` A p50 near ${venueFloor.expected_decision_to_fill_ms_lo}–${venueFloor.expected_decision_to_fill_ms_hi}ms is the FLOOR, not instability`
          + (venueFloor.taker_delay_ms != null
            ? `: ${venueFloor.taker_delay_ms}ms of it is the venue's uncancellable taker delay on crypto up/down markets.`
            : ': this venue documents no uncancellable taker delay, so the band is the measured order POST round-trip.') : '')}
      sub={`${fmt.intc(nTot)} orders · ${pct(nRej || 0, nTot)} rejected`}
      flush>
      <div className="tbl-wrap">
        <table className="tbl">
          <thead><tr>
            <th>cohort</th><th className="num">p50</th><th className="num">p95</th>
            <th className="num">max</th><th className="num">orders</th>
          </tr></thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.k}>
                <td className="mono" title={r.why}>{r.k}</td>
                <td className="num">{ms(r.s && r.s.p50)}</td>
                <td className="num">{ms(r.s && r.s.p95)}</td>
                <td className="num">{ms(r.s && r.s.max)}</td>
                <td className="num">{r.n != null ? fmt.intc(r.n) : '—'}</td>
              </tr>
            ))}
            <tr style={{ background: 'var(--surface-2)' }}>
              <td className="mono primary"
                title="local EIP-712 (secp256k1 is C) — the language-change argument lives or dies here">sign</td>
              <td className="num">{ms(po.sign && po.sign.p50)}</td>
              <td className="num">{ms(po.sign && po.sign.p95)}</td>
              <td className="num">{ms(po.sign && po.sign.max)}</td>
              <td className="num">{po.sign && po.sign.n != null ? fmt.intc(po.sign.n) : '—'}</td>
            </tr>
          </tbody>
        </table>
      </div>
      {/* The "a p50 near X–Yms is the FLOOR, not instability" paragraph that
          used to close this panel is gone (2026-08-05 declutter) — it rides the
          panel-title tooltip above, taker_delay_ms null-branch and all, so the
          predict.fun console still never renders "nullms". */}
    </Panel>
  );
}

// ── 2. staleness — a different axis from wall clock ──────────────────
function FreshnessPanel({ freshness }) {
  const src = freshness.sources || {};
  const total = (src.ws || 0) + (src.prefetch || 0) + (src.rest || 0);
  const age = freshness.ws_age || {};
  const nWs = freshness.n_ws_served_ticks || 0;
  const stale = age.p50 != null && age.p50 > 700;
  // Stall watchdog (operator order 2026-08-06). Age alone gates nothing on
  // these books — quietness reads exactly like a stalled subscription — so the
  // honest reading of the tile beside it is "old, and the out-of-band REST
  // cross-check still agrees". DECLUTTER: the watchdog is invisible while it is
  // only agreeing (tooltip-only); it earns visible text only once it has
  // actually evicted a ladder.
  const wd = freshness.watchdog || null;
  const wdArmed = !!(wd && wd.armed);
  const wdEvictions = wd ? (wd.evictions || 0) : 0;
  const wdText = `watchdog: ${fmt.intc((wd && wd.matches) || 0)} verified · ${fmt.intc(wdEvictions)} evictions`;
  const wdTip = wdArmed
    ? `${wdText} — a REST cross-check of suspect (old-ladder) tokens; a mismatch evicts the token so the next read falls through to REST`
    : 'stall watchdog NOT armed — nothing cross-checks a connected-but-silent subscription';
  return (
    <Panel title="book freshness"
      titleHint="how old was the ladder we priced against — STALENESS, not wall clock. The venue market channel pushes ~822 msg/s (p50 1ms between frames), so a large age here is a consumer defect, never the feed."
      sub={`${fmt.intc(total)} book reads · ws ${pct(src.ws || 0, total)} · prefetch ${pct(src.prefetch || 0, total)} · inline REST ${pct(src.rest || 0, total)}`}>
      <div className="kpi-grid c4">
        <KpiCell label="ws book age p50" value={ms(age.p50)} valueSm
          valueClass={stale ? 'neg' : 'pos'}
          sub={wdEvictions > 0
            ? `p95 ${ms(age.p95)} · max ${ms(age.max)} · ${wdText}`
            : `p95 ${ms(age.p95)} · max ${ms(age.max)}`}
          title={`venue-clock age of the WS ladder at decision time — ${wdTip}`} />
        <KpiCell label="ws ticks over 700ms" value={nWs ? pct(freshness.ws_over_700ms || 0, nWs) : '—'} valueSm
          sub={`${fmt.intc(freshness.ws_over_700ms || 0)} of ${fmt.intc(nWs)} ws-served ticks`}
          title="700ms is the prefetch freshness bound — past it the ladder can price a limit that FAK-rejects" />
        <KpiCell label="over 2s / over 10s" valueSm
          value={nWs ? `${pct(freshness.ws_over_2s || 0, nWs)} / ${pct(freshness.ws_over_10s || 0, nWs)}` : '—'}
          title="a 10s-old sub-minute book is a different market" />
        <KpiCell label="prefetch age" value="unmeasured" valueSm
          sub={`serves ${pct(src.prefetch || 0, total)} of all books`}
          title="the REST prefetch cache carries no age telemetry, so the ws figures beside it do NOT describe the majority of book reads — read them as the ws-served slice only, never as whole-path freshness" />
      </div>
      {/* The ⚠️ "no age instrumentation" paragraph was deleted 2026-08-05: the
          'prefetch age' tile above already says `unmeasured` on its face and
          carries the full caveat in its tooltip. */}
    </Panel>
  );
}

// ── 3. per-stage table (kept, relabelled) ────────────────────────────
// The "meaning" prose COLUMN was deleted 2026-08-05 (operator declutter): each
// stage's note now rides the title tooltip on its own row label, so the table
// is stage name + numbers only.
//
// TAKER DELAY (operator 2026-08-05): its own row, MEASURED-FIRST.
//   * `venue_floor.taker_delay_measured_ms` — p50/p95/max/n of the ACCEPTED
//     per-order POST round trip in this window. REAL RECORDED TIME (network +
//     signing tail + the enforced delay), so it answers "is it really 250, or
//     260?" with an observation instead of a constant. Preferred whenever the
//     window contains an accepted order.
//   * `venue_floor.taker_delay_ms` — the venue's DOCUMENTED constant (250ms on
//     Polymarket, null on predict.fun, which documents none). The fallback
//     when nothing was measured, and always the floor named in the tooltip.
// Both come from the payload; nothing is hardcoded here, and a venue with
// neither renders no row at all rather than a fake 0.
//
// The `total` row never UNDERSTATES that wall: a tick whose orders were all
// REJECTED never waits the delay out (a 400 skips matching), so its measured
// `post` is ~0 and the measured total excludes the venue floor entirely — that
// is the "shows 0" the operator saw. The total therefore adds
// CB.takerDelayShortfall(documented floor, measured post) per percentile
// column: 0 wherever the accepted POST already contains the delay (no double
// count), the full delay on a reject-dominated window.
//
// THE COLUMN NOW TALLIES (operator 2026-08-05: "when sum up all ms, why not
// equal total ms?"). It never did, for three separate reasons, and the table
// now shows each of them instead of leaving the operator to add rows that were
// never addable:
//   1. NESTING. `sign` and `post` are measured INSIDE `order`, and the
//      taker-delay row is that same POST a third time. They are marked '↳ ...'
//      and are NOT part of the sum — only `LATENCY_ADDITIVE_STAGES`
//      (spot · ctx · gamma · book · order) is.
//   2. UNMEASURED TICK WORK. Only network-touching phases are instrumented, so
//      scoring/gates/sizing, SQLite, the post-POST tail and the held-position
//      book reads live in `total` and in no row above it.
//   3. PERCENTILE NON-ADDITIVITY. Every cell is its own order statistic over
//      its own tick population (the order stages see order-bearing ticks only),
//      and a p50 of a sum is not a sum of p50s.
// (2) and (3) land in ONE explicit 'other · unmeasured gaps' row =
// CB.latencyResidual(total column − Σ additive), computed per COLUMN against
// that column's own total, SIGNED (a negative residual is (3) showing itself).
// The invariant the table now promises: the rows that carry a sum marker add,
// with the residual, to the total row — exactly, in every column.
function StageTable({ stages, takerDelayMs }) {
  const rows = STAGE_ORDER.filter((name) => stages[name]);
  if (!rows.length) return <EmptyState msg="no stage timings aggregated yet" />;
  const NOTE = {
    spot: 'batched spot fetch — prefetched, ~free on order ticks',
    ctx: 'live context + balance',
    gamma: 'market metadata — cached; the p95 is a cache miss',
    book: 'ENTRY-path book acquisition only (held-position pass reads split out)',
    sign: 'EIP-712 signing — measured INSIDE `order`, so it is shown but never added',
    post: 'venue round trip · SUM across the tick\'s orders — measured INSIDE `order`, so it is shown but never added',
    order: 'whole placement envelope (contains sign + post) · SUM across the tick\'s orders',
    total: 'whole tick incl. the post-POST tail and the venue taker delay — NOT decision→fill',
  };
  // ROUND ONCE, at the display boundary, and do every sum on the ROUNDED
  // figures. The payload carries one decimal, `ms()` renders whole
  // milliseconds, and a residual computed from the raw decimals would
  // reconcile numbers the operator cannot see — leaving the column he CAN add
  // up off by a millisecond or two. This is the arithmetic he sees.
  const rounded = {};
  Object.keys(stages).forEach((k) => {
    const s = stages[k];
    if (!s) return;
    rounded[k] = {
      p50: Math.round(s.p50 || 0), p95: Math.round(s.p95 || 0),
      max: Math.round(s.max || 0),
    };
  });
  const post = rounded.post || null;
  // How much of the venue's uncancellable delay the measured POST did NOT
  // already capture, per percentile column (pure helper — window.CB).
  const shortfall = (col) => takerDelayShortfall(takerDelayMs, post ? post[col] : null);
  // The standalone ↳ taker row (measured/documented variants) was DELETED
  // 2026-08-06 by operator order together with the "unmeasured gaps" label:
  // the residual row below IS the taker-delay row now. The measured
  // accepted-POST view survives in the accept/reject cohort panel above.
  // The TOTAL as displayed (the measured tick wall + whatever of the venue's
  // documented delay it did not already contain) — and therefore the figure
  // the residual must reconcile the summed rows against, column by column.
  const totalShown = (col) => (rounded.total ? rounded.total[col] + shortfall(col) : null);
  const residual = (col) => latencyResidual(rounded, col, totalShown(col));
  // Operator 2026-08-06: this row is LABELLED "taker delay" because in the
  // windows the operator watches it is dominated by the venue's uncancellable
  // delay — with no accepted order measured, the total carries the documented
  // floor and it all lands here. The tooltip keeps the full truth: the row is
  // arithmetically total − Σ(stage rows), so it also holds the uninstrumented
  // tick work, and once orders flow (delay measured inside `post`) it shrinks
  // to just those gaps.
  const RESIDUAL_TITLE =
    'total − (spot + ctx + gamma + book + order). '
    + (takerDelayMs != null
      ? `Dominated by the venue's uncancellable ${takerDelayMs}ms taker delay whenever no accepted order was measured in the window. When orders fill, the delay is measured inside \`post\` and this row shrinks to the uninstrumented tick work: `
      : 'Everything in the tick with no row of its own: ')
    + 'scoring, gates, sizing, the SQLite writes, the post-POST tail and the held-position book reads. '
    + 'It also absorbs percentile non-additivity (a p50 of a sum is not the sum of p50s), '
    + 'so it can go NEGATIVE. Shown signed either way, so the rows above plus this row '
    + 'equal the total row exactly in every column. The ↳ rows are measured INSIDE `order` and are never added.';
  return (
    <div className="tbl-wrap">
      <table className="tbl">
        <thead><tr>
          <th title="rows without a ↳ are disjoint phases of one tick and ADD UP (with the residual) to the total; ↳ rows are measured inside the row above them and are shown for detail only">stage</th>
          <th className="num">p50</th><th className="num">p95</th>
          <th className="num">max</th>
        </tr></thead>
        <tbody>
          {rows.map((name) => {
            const s = rounded[name];
            const isTotal = name === 'total';
            const add = isTotal ? shortfall : () => 0;
            // NESTED = shown, never summed (it is already inside `order`).
            const nested = !isTotal && LATENCY_ADDITIVE_STAGES.indexOf(name) < 0;
            return (
              <React.Fragment key={name}>
                {/* THE row that makes the column add up. */}
                {isTotal && (
                  <tr>
                    <td className="mono" title={RESIDUAL_TITLE}>taker delay</td>
                    <td className="num">{ms(residual('p50'))}</td>
                    <td className="num">{ms(residual('p95'))}</td>
                    <td className="num">{ms(residual('max'))}</td>
                  </tr>
                )}
                <tr style={isTotal ? { background: 'var(--surface-2)' } : undefined}>
                  <td className={'mono' + (isTotal ? ' primary' : (nested ? ' muted' : ''))}
                    title={NOTE[name] || undefined}>{nested ? '↳ ' : ''}{name}</td>
                  <td className={'num' + (nested ? ' muted' : '')}>{ms(s.p50 + add('p50'))}</td>
                  <td className={'num' + (nested ? ' muted' : '')}>{ms(s.p95 + add('p95'))}</td>
                  <td className={'num' + (nested ? ' muted' : '')}>{ms(s.max + add('max'))}</td>
                </tr>
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// The per-tick "bk age" COLUMN was deleted 2026-08-05 by operator order. Book
// staleness keeps its own dedicated surfaces — the "book age at decision" KPI
// tile and the whole book-freshness panel (p50/p95/max, the over-700ms/2s/10s
// shares and the unmeasured-prefetch caveat) — so nothing measurable was lost;
// the payload still carries `ws_age`/`bk_ws` per tick. Do not re-add.
function RecentTicksTable({ recent }) {
  if (!recent.length) return <EmptyState msg="no network-active ticks in this window" />;
  return (
    <div className="tbl-wrap">
      <table className="tbl compact">
        <thead><tr>
          <th>time</th><th className="num">decision</th><th className="num">book</th>
          <th className="num">post</th><th className="num">rej</th><th className="num">tail</th>
          <th className="num">total</th><th className="num">ord</th>
        </tr></thead>
        <tbody>
          {recent.map((r, i) => {
            const slow = typeof r.total === 'number' && r.total > SOFT_DEADLINE_MS;
            return (
              <tr key={r.ts || i} style={slow ? { background: 'var(--neg-bg)' } : undefined}>
                <td className="mono">{fmt.hmsMyt(r.ts)}</td>
                <td className="num">{ms(r.pre_post)}</td>
                <td className="num">{ms(r.book)}</td>
                <td className="num">{ms(r.post)}</td>
                <td className="num">{ms(r.post_rej)}</td>
                <td className="num">{ms(r.tail)}</td>
                <td className={'num' + (slow ? ' neg' : '')}>{ms(r.total)}</td>
                <td className="num">{r.n_orders != null ? r.n_orders : '—'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function LatencyPage({ live }) {
  const lat = live.latency;
  if (!lat || !lat.stages) {
    return (
      <div className="content">
        <EmptyState big="no latency telemetry yet"
          msg="waiting on network-active ticks" />
      </div>
    );
  }

  const stages = lat.stages || {};
  const recent = Array.isArray(lat.recent) ? lat.recent : [];
  const windowMin = lat.window_min != null ? lat.window_min : 30;
  const orders = lat.orders && lat.orders.per_order ? lat.orders : null;
  const vf = lat.venue_floor || null;
  const venue = window.CB.venueOf(live);
  const venueName = venue === 'predictfun' ? 'predict.fun' : 'Polymarket';
  // Blocks added 2026-07-25 — absent on payloads from an older scanner, in
  // which case the page falls back to the tick-level view rather than lying.
  const decision = lat.decision || null;
  const freshness = lat.freshness || null;

  const decisionP50 = stat(decision, 'pre_post', 'p50');
  const decisionP95 = stat(decision, 'pre_post', 'p95');
  /* `tailP50` (decision.tail p50) went with the "where the tick goes" bar —
     the tail is unlabelled tick time now and lands in the stage table's
     'other · unmeasured gaps' residual, which is where it always belonged. */
  const po = orders ? orders.per_order : null;
  const acceptP50 = po && po.post ? po.post.p50 : null;
  const rejectP50 = po && po.post_rej ? po.post_rej.p50 : null;
  // VENUE-AWARE (2026-08-01). `taker_delay_ms` is null on predict.fun —
  // snapshot_builder._LATENCY_VENUE_FLOOR_BY_VENUE records that the venue has
  // NO documented uncancellable taker delay, only a ~700-900ms observed POST
  // band. The old `vf ? vf.taker_delay_ms : 250` collapsed that null into the
  // string "nullms" on the predict.fun console (and, on a payload without the
  // block at all, asserted Polymarket's 250ms as if it were measured here).
  // Render "—" and let the prose carry the venue's real floor.
  const takerDelay = (vf && vf.taker_delay_ms != null) ? vf.taker_delay_ms : null;
  // The MEASURED accepted-POST view (payload venue_floor.taker_delay_measured_ms)
  // now renders ONLY in the accept/reject cohort panel — the stage table's
  // standalone taker-delay ↳ row was deleted 2026-08-06 (operator) and the
  // residual row carries the "taker delay" label instead.
  const floorLo = vf ? vf.expected_decision_to_fill_ms_lo : null;
  const floorHi = vf ? vf.expected_decision_to_fill_ms_hi : null;
  // The tile's SUB (the "uncancellable taker delay · inside the POST" line and
  // its predict.fun twin) was deleted 2026-08-05 — the whole description rides
  // the tooltip, null-branch and all, so no console ever renders "nullms".
  const floorTitle = takerDelay != null
    ? `${venueName} reports a mandatory ${takerDelay}ms uncancellable taker delay inside the marketable-order POST round trip. No language, region or rewrite beats it.`
    : venue === 'predictfun'
      ? `predict.fun documents NO uncancellable taker delay; the floor here is the measured order POST round-trip (${floorLo != null && floorHi != null ? `${floorLo}-${floorHi}ms observed band` : 'band unavailable'}), not a venue-enforced wait.`
      : `No uncancellable taker delay is reported for this ${venueName} payload; the venue-floor telemetry is incomplete.`;
  const wsAgeP50 = stat(freshness, 'ws_age', 'p50');

  // Decision latency is the only budget the code owns. Judge it against the
  // slice of the venue floor that is NOT the taker delay, not against the tick.
  const decisionBudget = 120;
  const decisionOk = decisionP50 != null ? decisionP50 <= decisionBudget : null;

  return (
    <div className="content">
      <div className="kpi-grid c4">
        <KpiCell label="decision latency p50" value={ms(decisionP50)} valueSm
          valueClass={decisionOk == null ? '' : decisionOk ? 'pos' : 'neg'}
          sub={decisionP50 != null ? `p95 ${ms(decisionP95)}` : null}
          title="tick start → first POST on the wire. THE client-owned number: feeds, scoring, gates, sizing, reserve. Everything after it is venue-side." />
        <KpiCell label="venue floor" value={takerDelay != null ? `${takerDelay}ms` : '—'} valueSm
          title={floorTitle} />
        <KpiCell label="POST accept / reject" valueSm
          value={acceptP50 != null ? `${ms(acceptP50)} / ${ms(rejectP50)}` : '—'}
          sub={orders ? `${fmt.intc(orders.n_accepted || 0)} accepted · ${fmt.intc(orders.n_rejected || 0)} rejected` : ''}
          title="kept apart on purpose — a reject skips matching, so the two are different populations" />
        <KpiCell label="book age at decision" value={ms(wsAgeP50)} valueSm
          valueClass={wsAgeP50 == null ? '' : wsAgeP50 > 700 ? 'neg' : 'pos'}
          sub={null}
          title="staleness, not wall clock — the axis that actually drives no_liquidity rejects. Covers the ws-served slice only; prefetch age is unmeasured." />
      </div>

      {/* the "where the tick goes" panel sat here until 2026-08-05 — see the
          note above CohortPanel for why it was deleted and what carries its
          figures now */}

      {orders && (
        <React.Fragment>
          <CohortPanel orders={orders} venueFloor={vf} />
          <div className="section-gap" />
        </React.Fragment>
      )}

      {freshness && (
        <React.Fragment>
          <FreshnessPanel freshness={freshness} />
          <div className="section-gap" />
        </React.Fragment>
      )}

      {/* the "p50 / p95 / max · Nm window" sub was deleted 2026-08-05
          (operator): the column headers name the percentiles, and the window
          lives in the panel tooltip. */}
      <Panel title="per-stage latency · tick level"
        titleHint={`p50 / p95 / max over a ${windowMin}m window. `
          + "Rows without a ↳ are disjoint phases of one tick and ADD UP — with the 'taker delay' row — to the total row, exactly, in every column. The ↳ rows (sign, post) are measured INSIDE `order` and are shown for detail only, never summed. "
          + "sign / post / order rows are PER-TICK SUMS across every order placed that tick — the per-order cohorts above are the honest view."
          + (takerDelay != null
            ? ` The taker-delay row carries the venue's documented ${takerDelay}ms uncancellable delay whenever no accepted order was measured in the window (a rejected order never waits it out), plus the tick's uninstrumented work; when orders fill, the delay is measured inside post and the row shrinks to the gaps alone.`
            : '')}
        flush>
        <StageTable stages={stages} takerDelayMs={takerDelay} />
      </Panel>
      <div className="section-gap" />

      <Panel title="recent network-active ticks"
        titleHint={`decision = tick start → POST, newest tick first. Rows over the ${SOFT_DEADLINE_MS}ms soft deadline are tinted.`}
        /* the "N ticks · newest first" sub was deleted 2026-08-05 (operator):
           the table is the count, and the ordering rides the tooltip. */
        flush>
        <RecentTicksTable recent={recent} />
      </Panel>
    </div>
  );
}

window.LatencyPage = LatencyPage;
})();
