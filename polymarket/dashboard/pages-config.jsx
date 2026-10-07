// ===========================================================================
// pages-config.jsx - OperatorPage (console). (StrategyPage deleted 2026-07-09
// with the exp044 retirement; the v2 tab owns spec+sizing display. The
// deployed-execution-flags + predict.fun funder-wallet/recorder panels were
// REMOVED by operator request 2026-07-23.) Reads the live SSE payload.
// ===========================================================================
(function () {
const { fmt } = window.CB;
const { useState } = React;
const apiPost = window.CB_API.post;

function tagForStatus(status) {
  const s = String(status || '').toUpperCase();
  if (['LIVE', 'READY', 'OK', 'CONFIRMED'].includes(s)) return 'tag-pos';
  if (['TRANSFER_LOCK', 'SUBMITTING', 'SUBMITTED', 'PREVIEW'].includes(s)) return 'tag-warn';
  if (['FAILED', 'ERROR', 'NOT_READY', 'STOPPED_PROCESSING'].includes(s)) return 'tag-neg';
  return 'tag-mute';
}
function statusDot(status) {
  const cls = tagForStatus(status);
  return cls === 'tag-pos' ? 'ok' : cls === 'tag-neg' ? 'bad' : 'warn';
}
function OperatorConsolePage({ live }) {
  const op = live.operator || { state: 'STOPPED_PROCESSING', readiness: {} };
  // The read-only `is_snapshot` guard that used to live here went away with
  // the cross-venue view (2026-08-01): this console can only ever render THIS
  // process's own live payload, so the operator POSTs always target the bot
  // whose state is on screen. There is no longer any way to view one venue's
  // state while the controls drive another's.
  const walletTitle = 'POLY_FUNDER wallet record';
  const collateral = 'pUSD';
  const lw = live.live_wallet || {};
  const [copied, setCopied] = useState(false);
  const [busy, setBusy] = useState(null);
  const [notice, setNotice] = useState(null);

  const readiness = op.readiness || {};
  const checks = (readiness.checks && readiness.checks.length) ? readiness.checks : [
    { name: 'report_present', passed: !!readiness.status, detail: readiness.status || 'no_report' },
    { name: 'processing_state', passed: op.state === 'LIVE', detail: op.state || 'unknown' },
    { name: 'funder_reconciliation', passed: String(lw.source_status || '').toLowerCase() === 'ok', detail: lw.source_status || 'not_sampled' },
    { name: 'wallet_balance', passed: lw.balance_usd != null, detail: lw.balance_usd != null ? fmt.money(lw.balance_usd) : 'not_sampled' },
  ];
  const src = lw.source_status || null;
  const srcOk = String(src || '').toLowerCase() === 'ok';

  const copy = () => {
    if (!lw.wallet_address) return;
    navigator.clipboard && navigator.clipboard.writeText(lw.wallet_address);
    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
  };

  function runTransition(action) {
    if (busy) return;
    const path = action === 'start' ? '/api/v2/operator/start'
      : action === 'stop' ? '/api/v2/operator/stop'
      : '/api/v2/operator/transfer-lock';
    setBusy(action);
    setNotice(null);
    apiPost(path, { expected_version: op.version, reason: `dashboard ${action}` })
      .then((body) => setNotice({ kind: 'ok', text: `${action} submitted: ${body.state || 'ok'}` }))
      .catch((err) => setNotice({ kind: 'err', text: err.message }))
      .finally(() => setBusy(null));
  }

  return (
    <div className="content">
      <div className="op-grid">
        <Panel title="processing state">
          <div className="op-state-row">
            <div className="op-cell"><div className="k">processing state</div><span className={'tag ' + tagForStatus(op.state)}>{op.state || 'unknown'}</span>{op.updated_at && <div className="meta">updated {op.updated_at}</div>}</div>
            <div className="op-cell"><div className="k">boot mode</div><div className="v">{op.boot_dry_run === false ? 'LIVE' : op.boot_dry_run === true ? 'DRY' : '-'}</div></div>
            <div className="op-cell"><div className="k">reason</div><div className="v sm">{op.reason || '-'}</div></div>
          </div>
          <div className="op-toolbar">
            <button className="btn accent" onClick={() => runTransition('start')} disabled={busy || op.state === 'LIVE'}>Start Processing</button>
            <button className="btn danger" onClick={() => runTransition('stop')} disabled={busy || op.state === 'STOPPED_PROCESSING'}>Stop Processing</button>
            <button className="btn" onClick={() => runTransition('transfer-lock')} disabled={busy || op.state === 'TRANSFER_LOCK'}>Transfer Lock</button>
          </div>
          {notice && <div className={'op-alert ' + (notice.kind === 'err' ? 'bad' : 'ok')}>{notice.text}</div>}
        </Panel>

        <Panel title="readiness">
          <div className="op-checks">
            <div className="op-check">
              <span className={'check-dot ' + statusDot(readiness.status)} />
              <span>status</span>
              <span className={'tag ' + tagForStatus(readiness.status)}>{readiness.status || 'no_report'}</span>
            </div>
            {checks.map((c) => (
              <div key={c.name} className="op-check" title={c.detail || ''}>
                <span className={'check-dot ' + (c.passed ? 'ok' : 'bad')} />
                <span>{c.name}</span>
                <span className={'tag ' + (c.passed ? 'tag-pos' : 'tag-neg')}>{c.passed ? 'OK' : 'FAIL'}</span>
              </div>
            ))}
          </div>
        </Panel>
      </div>
      <div className="section-gap" />

      <Panel title={walletTitle}>
        <div className="op-wallet-head">
          {lw.wallet_address ? (
            <span className="wallet-addr">{fmt.shortAddr(lw.wallet_address)}
              <button className="copy-btn" onClick={copy} title="copy address">{window.SHELL_ICONS.copy}</button>
              {copied && <span className="tag tag-pos">copied</span>}
            </span>
          ) : <span className="card-note">no reconciliation run recorded yet</span>}
          <span className={'tag ' + (srcOk ? 'tag-pos' : src ? 'tag-neg' : 'tag-mute')}>{src || 'not_sampled'}</span>
        </div>
        <div className="metric-grid">
          <div className="metric-cell"><div className="k">{'CLOB ' + collateral}</div><div className="v">{fmt.money(lw.balance_usd)}</div></div>
          <div className="metric-cell"><div className="k">{'on-chain ' + collateral}</div><div className="v">{fmt.money(lw.chain_balance_usd)}</div></div>
          {/* never-sampled ⇒ '—', not a confident 0 (item 26) */}
          <div className="metric-cell"><div className="k">open orders</div><div className="v">{lw.refreshed_at ? fmt.intc(lw.open_orders_count || 0) : '—'}</div></div>
          <div className="metric-cell"><div className="k">open positions</div><div className="v">{lw.refreshed_at ? fmt.intc(lw.open_positions_count || 0) : '—'}</div></div>
          <div className="metric-cell"><div className="k">sampled</div><div className="v sm">{lw.refreshed_at ? fmt.agoShort(lw.refreshed_at) : 'never'}</div></div>
        </div>
      </Panel>
      <div className="section-gap" />

      <CapitalFlowsPanel
        flows={live.capital_flows}
        initial={(live.kpis || {}).initial_bankroll}
        startAt={(live.kpis || {}).wallet_start_at}
      />

    </div>
  );
}

// ── account cash events ────────────────────────────────────────────────
// `flows` are recorded by `cryptobot.cli.operator record-flow`. The opening
// balance is display-only; `net` remains the flow total already in the replay.
function CapitalFlowsPanel({ flows, initial, startAt }) {
  const opening = Number.isFinite(Number(initial)) ? [{
    occurred_at: startAt || 'start',
    kind: 'start',
    amount_usd: Number(initial),
    note: 'starting balance',
    is_start: true,
  }] : [];
  // Rebates live on the Overview P&L strip (operator 2026-08-05) — this panel
  // shows only the money the operator moved. The net row therefore sums the
  // DISPLAYED rows (excl. the opening balance), not kpis.net_deposits, which
  // includes rebates.
  const shown = (flows || []).filter((f) => f.kind !== 'rebate');
  const rows = [...opening, ...shown];
  const shownNet = shown.reduce((s, f) => s + (Number(f.amount_usd) || 0), 0);
  return (
    <Panel title="capital flows" q={`${rows.length}`}
      sub="deposits / withdrawals">
      {rows.length === 0 ? (
        <EmptyState msg="no deposits or withdrawals recorded" />
      ) : (
        <div className="tbl-wrap"><table className="tbl compact">
          <thead><tr>
            <th>date (UTC)</th><th>kind</th><th className="num">amount</th><th>note</th>
          </tr></thead>
          <tbody>
            {rows.map((f, i) => (
              <tr key={i}>
                <td className="mono">{f.occurred_at || '—'}</td>
                <td><span className={'tag ' + (f.amount_usd < 0 ? 'tag-warn' : 'tag-pos')}>{f.kind}</span></td>
                <td className={'num ' + (f.amount_usd < 0 ? 'neg' : 'pos')}>{fmt.moneyDelta(f.amount_usd)}</td>
                <td className="muted">{f.note || '—'}</td>
              </tr>
            ))}
            <tr>
              <td className="muted">net</td><td />
              <td className={'num ' + (shownNet < 0 ? 'neg' : 'pos')}>{fmt.moneyDelta(shownNet)}</td>
              <td />
            </tr>
          </tbody>
        </table></div>
      )}
    </Panel>
  );
}

window.OperatorPage = OperatorConsolePage;
})();
