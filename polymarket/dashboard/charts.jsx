// ===========================================================================
// charts.jsx — dependency-free inline-SVG charts for the operator night desk.
// No chart library: every visual is hand-built SVG so the dashboard stays a
// single static bundle behind the SSE stream.
//
// Colors are READ FROM styles.css AT RUNTIME (the --c-* chart-mark tokens +
// ink tiers), so retheming the CSS rethemes every chart — no hex lives here.
// Loaded after data.jsx; components are hung on `window`.
// ===========================================================================

(function () {
const { fmt } = window.CB;
var _spIdx = 0;

// Runtime token reader: styles.css is the single theme source. Fallbacks
// mirror the dark night-desk tokens so charts render even if a token is
// missing (tests / partial loads). Cached after first read.
var _C = null;
function tok(name, fb) {
  try {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fb;
  } catch (e) { return fb; }
}
function theme() {
  if (_C) return _C;
  _C = {
    pos: tok('--c-pos', '#26ab72'), neg: tok('--c-neg', '#e5503f'),
    grid: tok('--c-grid', 'rgba(255,255,255,0.06)'),
    gridFaint: tok('--c-grid-faint', 'rgba(255,255,255,0.035)'),
    fg: tok('--fg', '#eceef4'), fg3: tok('--fg-3', '#6d7688'), fg4: tok('--fg-4', '#3f4655'),
    surface: tok('--surface', '#11141b'),
  };
  return _C;
}

function _path(pts) {
  return pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
}
function ChartEmpty({ h, msg }) {
  return <div className="chart-empty" style={{ height: h }}>{msg || 'no data yet'}</div>;
}

// ── Sparkline ─────────────────────────────────────────────────────────
function Sparkline({ values, w = 84, h = 24, color, fill = true, strokeWidth = 1.5 }) {
  if (!values || values.length < 2) return <svg className="sparkline" width={w} height={h} />;
  const C = theme();
  const lo = Math.min(...values), hi = Math.max(...values);
  const span = hi - lo || 1;
  const stroke = color || (values[values.length - 1] >= values[0] ? C.pos : C.neg);
  const pad = 2;
  const pts = values.map((v, i) => ({
    x: pad + (i / (values.length - 1)) * (w - 2 * pad),
    y: pad + (1 - (v - lo) / span) * (h - 2 * pad),
  }));
  const d = _path(pts);
  const area = `${d} L${pts[pts.length - 1].x.toFixed(1)},${h} L${pts[0].x.toFixed(1)},${h} Z`;
  const gid = `sp${_spIdx++}`;  // monotonic counter — avoids gradient-id collision across concurrent sparklines
  return (
    <svg className="sparkline" width={w} height={h} viewBox={`0 0 ${w} ${h}`}>
      {fill && (
        <React.Fragment>
          <defs><linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={stroke} stopOpacity="0.22" />
            <stop offset="100%" stopColor={stroke} stopOpacity="0" />
          </linearGradient></defs>
          <path d={area} fill={`url(#${gid})`} />
        </React.Fragment>
      )}
      <path d={d} fill="none" stroke={stroke} strokeWidth={strokeWidth} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

// ── Equity curve ──────────────────────────────────────────────────────
// `unit` names what ONE POINT is, for the crosshair label: 'trade' for the
// per-trade ledger series (and the client aggEquity fallback, which folds to
// trade groups too), 'hour' for the hourly-bucketed cash_equity fallback. It
// defaults to 'trade' — the preferred series — so a legacy caller reads the
// same as before.
function EquityChart({ points, base, height = 210, unit = 'trade' }) {
  // hooks must precede the empty-state return (rules of hooks)
  const [hover, setHover] = React.useState(null);
  if (!points || points.length < 2) return <ChartEmpty h={height} msg="equity curve builds as bets settle" />;
  const C = theme();
  const W = 920, H = height, padL = 52, padR = 14, padT = 12, padB = 22;
  const ys = points.map((p) => p.eq);
  let lo = Math.min(base, ...ys), hi = Math.max(base, ...ys);
  if (hi - lo < 1) { hi += 1; lo -= 1; }
  const padV = (hi - lo) * 0.08; lo -= padV; hi += padV;
  const sx = (i) => padL + (i / (points.length - 1)) * (W - padL - padR);
  const sy = (v) => padT + (1 - (v - lo) / (hi - lo)) * (H - padT - padB);
  const pts = points.map((p, i) => ({ x: sx(i), y: sy(p.eq) }));
  const d = _path(pts);
  const last = points[points.length - 1];
  const up = last.eq >= base;
  const stroke = up ? C.pos : C.neg;
  const area = `${d} L${pts[pts.length - 1].x.toFixed(1)},${sy(lo)} L${pts[0].x.toFixed(1)},${sy(lo)} Z`;
  const ticks = 4;
  const gridY = Array.from({ length: ticks + 1 }, (_, i) => lo + (i / ticks) * (hi - lo));
  // crosshair: map mouse x (rendered px) back into viewBox units, snap to
  // the nearest settled-bet index.
  const onMove = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const vx = ((e.clientX - rect.left) / rect.width) * W;
    const t = (vx - padL) / (W - padL - padR);
    const i = Math.round(t * (points.length - 1));
    setHover(i >= 0 && i < points.length ? i : null);
  };
  const hp = hover != null ? pts[hover] : null;
  const hv = hover != null ? points[hover] : null;
  const tipW = 118, tipX = hp ? Math.min(Math.max(hp.x - tipW / 2, padL), W - padR - tipW) : 0;
  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ height, cursor: 'crosshair' }}
      onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
      <defs>
        <linearGradient id="eqfill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={stroke} stopOpacity="0.20" />
          <stop offset="60%" stopColor={stroke} stopOpacity="0.05" />
          <stop offset="100%" stopColor={stroke} stopOpacity="0" />
        </linearGradient>
        <filter id="eqglow" x="-20%" y="-60%" width="140%" height="220%">
          <feGaussianBlur stdDeviation="4" />
        </filter>
      </defs>
      {gridY.map((v, i) => (
        <g key={i}>
          <line x1={padL} y1={sy(v)} x2={W - padR} y2={sy(v)} stroke={C.gridFaint} strokeWidth="1" />
          <text x={padL - 8} y={sy(v) + 3} textAnchor="end" fontSize="10" fill={C.fg3} fontFamily="JetBrains Mono, monospace">{fmt.money(v, 0)}</text>
        </g>
      ))}
      <line x1={padL} y1={sy(base)} x2={W - padR} y2={sy(base)} stroke={C.fg4} strokeWidth="1" strokeDasharray="3 3" />
      <path d={area} fill="url(#eqfill)" />
      <path d={d} fill="none" stroke={stroke} strokeWidth="3" strokeOpacity="0.35" strokeLinejoin="round" strokeLinecap="round" filter="url(#eqglow)" />
      <path d={d} fill="none" stroke={stroke} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={pts[pts.length - 1].x} cy={pts[pts.length - 1].y} r="8" fill={stroke} fillOpacity="0.15" />
      <circle cx={pts[pts.length - 1].x} cy={pts[pts.length - 1].y} r="3.2" fill={stroke} stroke={C.surface} strokeWidth="1.5" />
      {hp && (
        <g pointerEvents="none">
          <line x1={hp.x} y1={padT} x2={hp.x} y2={H - padB} stroke={C.fg4} strokeWidth="1" strokeDasharray="2 3" />
          <circle cx={hp.x} cy={hp.y} r="4" fill={stroke} stroke={C.surface} strokeWidth="1.5" />
          <rect x={tipX} y={padT + 2} width={tipW} height="34" rx="6" fill={C.surface} stroke={C.grid} />
          <text x={tipX + 9} y={padT + 16} fontSize="11" fontWeight="700" fill={C.fg} fontFamily="JetBrains Mono, monospace">{'$' + hv.eq.toFixed(2)}</text>
          <text x={tipX + 9} y={padT + 29} fontSize="9" fill={C.fg3} fontFamily="JetBrains Mono, monospace">
            {/* points[0] is the base anchor, not a trade. The server lifetime
                series is DOWNSAMPLED, so the array position is NOT the trade
                number — each point carries its TRUE index in p.i (item 21);
                the un-downsampled client fallback uses the array position. */}
            {(() => {
              const bi = hv.i != null ? hv.i : hover;
              const bn = points[points.length - 1].i != null ? points[points.length - 1].i : points.length - 1;
              // The server stamps the base anchor with WALLET_START_AT. Show
              // that date at point zero so the ~$opening-balance point is
              // visibly the accounting start, not the first later settlement.
              if (bi === 0) return 'start · ' + (hv.ts ? fmt.dateMyt(hv.ts) : '—');
              return unit + ' ' + bi + '/' + bn + (hv.dd != null ? ' · dd ' + (hv.dd * 100).toFixed(1) + '%' : '');
            })()}
          </text>
        </g>
      )}
    </svg>
  );
}

// ── Drawdown (underwater) ─────────────────────────────────────────────
function DrawdownChart({ points, height = 92 }) {
  if (!points || points.length < 2) return <ChartEmpty h={height} msg="—" />;
  const C = theme();
  const W = 920, H = height, padL = 52, padR = 14, padT = 8, padB = 16;
  const dds = points.map((p) => p.dd);
  const lo = Math.min(-0.001, ...dds);
  const sx = (i) => padL + (i / (points.length - 1)) * (W - padL - padR);
  const sy = (v) => padT + (1 - (v - lo) / (0 - lo)) * (H - padT - padB);
  const pts = points.map((p, i) => ({ x: sx(i), y: sy(p.dd) }));
  const d = _path(pts);
  const area = `${d} L${pts[pts.length - 1].x.toFixed(1)},${sy(0)} L${pts[0].x.toFixed(1)},${sy(0)} Z`;
  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ height }}>
      <defs><linearGradient id="ddfill" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={C.neg} stopOpacity="0" />
        <stop offset="100%" stopColor={C.neg} stopOpacity="0.2" />
      </linearGradient></defs>
      <line x1={padL} y1={sy(0)} x2={W - padR} y2={sy(0)} stroke={C.grid} strokeWidth="1" />
      <text x={padL - 8} y={sy(lo) + 3} textAnchor="end" fontSize="10" fill={C.fg3} fontFamily="JetBrains Mono, monospace">{(lo * 100).toFixed(1) + '%'}</text>
      <path d={area} fill="url(#ddfill)" />
      <path d={d} fill="none" stroke={C.neg} strokeWidth="1.4" strokeLinejoin="round" />
    </svg>
  );
}

// ── Vertical P&L bars (weekly / arbitrary) ────────────────────────────
function PnlBars({ items, height = 150, labelKey = 'label', valueKey = 'pnl' }) {
  if (!items || !items.length) return <ChartEmpty h={height} msg="—" />;
  const C = theme();
  const W = 920, H = height, padT = 16, padB = 22, padL = 8, padR = 8;
  const vals = items.map((it) => it[valueKey]);
  const hi = Math.max(0.01, ...vals), lo = Math.min(0, ...vals);
  const span = hi - lo || 1;
  const zeroY = padT + (hi / span) * (H - padT - padB);
  const bw = (W - padL - padR) / items.length;
  // Cap bar width so a 1–3 item chart reads as bars, not wall-to-wall slabs.
  const barW = Math.min(bw * 0.82, 96);
  const fmtV = (v) => (v >= 0 ? '+' : '−') + (Math.abs(v) < 10 ? Math.abs(v).toFixed(2) : Math.abs(v).toFixed(0));
  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ height }}>
      <line x1={padL} y1={zeroY} x2={W - padR} y2={zeroY} stroke={C.grid} />
      {items.map((it, i) => {
        const v = it[valueKey];
        const bh = Math.abs(v) / span * (H - padT - padB);
        const yTop = v >= 0 ? zeroY - bh : zeroY;
        const col = v >= 0 ? C.pos : C.neg;
        const cx = padL + i * bw + bw / 2;
        return (
          <g key={i}>
            <rect x={cx - barW / 2} y={yTop} width={barW} height={Math.max(1, bh)} rx="3" fill={col} fillOpacity="0.95">
              <title>{`${it[labelKey] != null ? it[labelKey] + ' · ' : ''}${fmtV(v)}`}</title>
            </rect>
            {items.length <= 16 && (
              <text x={cx} y={v >= 0 ? yTop - 5 : zeroY - 5} textAnchor="middle" fontSize="10" fill={C.fg3} fontFamily="JetBrains Mono, monospace">{fmtV(v)}</text>
            )}
            {items.length <= 16 && it[labelKey] != null && (
              <text x={cx} y={H - 4} textAnchor="middle" fontSize="9" fill={C.fg4} fontFamily="JetBrains Mono, monospace">{String(it[labelKey])}</text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

// ── P&L distribution histogram ────────────────────────────────────────
function PnlHistogram({ bins, height = 150 }) {
  if (!bins || !bins.length) return <ChartEmpty h={height} msg="—" />;
  const C = theme();
  const W = 920, H = height, padT = 12, padB = 18, padL = 8, padR = 8;
  const maxN = Math.max(1, ...bins.map((b) => b.n));
  const bw = (W - padL - padR) / bins.length;
  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ height }}>
      <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke={C.grid} />
      {bins.map((b, i) => {
        const bh = b.n / maxN * (H - padT - padB);
        const col = b.mid >= 0 ? C.pos : C.neg;
        const gap = Math.min(6, bw * 0.16);
        return (
          <g key={i}>
            <rect x={padL + i * bw + gap / 2} y={H - padB - bh} width={bw - gap} height={Math.max(0, bh)} rx="3" fill={col} fillOpacity="0.9">
              {/* "trades", not "bets": b.n counts trade groups (all fills of a position once,
                 voided never-filled cancels excluded), the same word the hero tile and the equity
                 crosshair use.
                 */}
              <title>{`${b.n} trades · mid ${b.mid >= 0 ? '+' : ''}${b.mid.toFixed(2)}`}</title>
            </rect>
            <text x={padL + i * bw + bw / 2} y={H - padB - bh - 3} textAnchor="middle" fontSize="9" fill={C.fg3} fontFamily="JetBrains Mono, monospace">{b.n || ''}</text>
          </g>
        );
      })}
    </svg>
  );
}

window.Sparkline = Sparkline;
window.EquityChart = EquityChart;
window.DrawdownChart = DrawdownChart;
window.PnlBars = PnlBars;
window.PnlHistogram = PnlHistogram;
})();
