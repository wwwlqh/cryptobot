// ===========================================================================
// app.jsx — root: sidebar nav shell and page routing.
//
// A sidebar, multi-page trading journal for the Polymarket 5m/15m bot. Live data arrives over SSE
// (window.LIVE, see live-bridge.js); overview, journal and calendar analytics are derived
// client-side from the streamed ledger in data.jsx.
//
// Load order (see index.html): data → charts → shell → pages-* → app. Shared helpers live on
// window.CB; components on window.<Name>.
// ===========================================================================
(function () {
const { useLive } = window.CB;
const { useState, useEffect } = React;

const PAGE_COMPONENT = {
  overview: window.OverviewPage,
  journal: window.JournalPage,
  calendar: window.CalendarPage,
  operator: window.OperatorPage,
  latency: window.LatencyPage,
  subminute: window.SubminutePage,
  spec: window.SubminuteSpecPage,
};

function AuthGate() {
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);

  function submit(ev) {
    ev.preventDefault();
    if (busy) return;
    setBusy(true);
    setError(null);
    window.CB_AUTH.login(password).catch((err) => {
      setBusy(false);
      setError(err && err.message ? err.message : 'login failed');
    });
  }

  return (
    <div className="auth-screen">
      <form className="auth-box" onSubmit={submit}>
        <div className="auth-mark">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round"><path d="M5 16V12M10 16V8M15 16V10M20 16V6" /></svg>
        </div>
        <div className="auth-title">cryptobot</div>
        <label className="auth-label" htmlFor="dashboard-password">Dashboard password</label>
        <input
          id="dashboard-password"
          className="auth-input"
          type="password"
          value={password}
          autoFocus
          onChange={(ev) => setPassword(ev.target.value)}
        />
        {error && <div className="auth-error">{error}</div>}
        <button className="auth-button" type="submit" disabled={busy}>
          {busy ? 'Checking...' : 'Unlock'}
        </button>
      </form>
    </div>
  );
}

function App() {
  const live = useLive();
  const [page, setPage] = useState('overview');
  const [range, setRange] = useState('All');
  // The venue is whatever this process's payload says it is.
  useEffect(() => {
    if (live && live.venue) window.__CB_DEPLOY_VENUE = window.CB.venueOf(live);
  }, [live]);
  const deployVenue = (live && live.venue)
    ? window.CB.venueOf(live)
    : ((typeof window !== 'undefined' && window.__CB_DEPLOY_VENUE) || 'polymarket');
  const view = live;

  const sse = window.__SSE_STATE || 'connecting';
  const meta = window.NAV.find((n) => n.key === page) || { title: page };
  const Page = PAGE_COMPONENT[page] || (() => <div className="content"><EmptyState msg={`unknown page: ${page}`} /></div>);

  if (sse === 'auth-required' || sse === 'auth-failed') {
    return <AuthGate />;
  }

  return (
    <div className="app">
      <Sidebar page={page} setPage={setPage} live={view} />
      <div className="main">
        <Topbar meta={meta} live={view} range={range} setRange={setRange} />
        <HaltedBanner live={view} />
        <Page live={view} range={range} venueSel={deployVenue} />
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
})();
