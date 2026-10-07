// EventSource bridge and snapshot.json loader.
//
// Connects the bot's `/events` SSE stream to `window.LIVE` and dispatches `cryptobot:live` on
// every state change. The payload contract is defined in
// src/cryptobot/observability/snapshot_builder.py :: build_payload():
//
//    { tick_ts, kpis, portfolio, markets, ledger, ledger_rollup,
//      ledger_equity, ledger_daily, live_wallet, operator, latency, v2 }
//
// When /events is unreachable the dashboard stays empty and the topbar shows the connection state.
// No synthetic trades are ever rendered; mock data is too easy to mistake for the real ledger.

function loadSnapshot() {
  // Static research snapshot for portfolio/backtest metadata.
  // Sync XHR is intentional: the file is small and the dashboard can
  // render portfolio research context before the first SSE tick.
  try {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', 'snapshot.json', false);
    xhr.send(null);
    if (xhr.status === 200 || xhr.status === 0) {
      window.SNAPSHOT = JSON.parse(xhr.responseText);
      console.info('snapshot loaded', window.SNAPSHOT.generated_at);
    } else {
      window.SNAPSHOT = null;
    }
  } catch (e) {
    window.SNAPSHOT = null;
    console.warn('snapshot.json unavailable - research panels will show fallbacks');
  }
}

loadSnapshot();


(function initSse() {
  window.__SSE_STATE = 'connecting';
  window.LIVE = null;
  var es = null;
  var authProbe = null;
  var CLIENT_LEDGER_FALLBACK_LIMIT = 500;
  var reconnectTimer = null;
  var reconnectAttempt = 0;

  function setState(state) {
    var changed = window.__SSE_STATE !== state;
    window.__SSE_STATE = state;
    if (changed) {
      window.dispatchEvent(new CustomEvent('cryptobot:state', { detail: state }));
    }
  }

  function clearLive(state) {
    window.LIVE = null;
    window.SNAPSHOT = null;
    window.dispatchEvent(new CustomEvent('cryptobot:live', { detail: null }));
    setState(state);
  }

  function parseUtcish(value) {
    var parsed = window.CB_TIME && window.CB_TIME.parseUtcTimestamp(value);
    return parsed ? parsed.getTime() : null;
  }

  function activeLedgerLabels(payload) {
    var sleeves = payload && payload.portfolio && payload.portfolio.sleeves;
    if (!Array.isArray(sleeves) || !sleeves.length) return null;
    var labels = new Set();
    sleeves.forEach(function (s) {
      if (!s) return;
      if (Array.isArray(s.ledger_sleeves)) {
        s.ledger_sleeves.forEach(function (label) { if (label) labels.add(label); });
      }
      if (s.label) labels.add(s.label);
    });
    return labels.size ? labels : null;
  }

  function filterActive(rows, labels) {
    if (!labels) return rows;
    return rows.filter(function (r) { return r && labels.has(r.sleeve); });
  }

  function filterWalletStart(rows, walletStart) {
    var startMs = parseUtcish(walletStart);
    if (startMs == null) return rows;
    return rows.filter(function (r) {
      var betMs = parseUtcish(r && r.bet_ts);
      return betMs != null && betMs >= startMs;
    });
  }

  function rowIsPending(row) {
    return row && String(row.outcome || '').toUpperCase() === 'PENDING';
  }

  function rowRecency(row) {
    var id = Number(row && row.id);
    if (Number.isFinite(id)) return id;
    var ms = window.CB_TIME && window.CB_TIME.settlementMs(row);
    return ms == null ? -Infinity : ms;
  }

  function ledgerLimit(payload) {
    var configured = Number(payload && payload.ledger_limit);
    return Number.isFinite(configured) && configured > 0
      ? Math.floor(configured) : CLIENT_LEDGER_FALLBACK_LIMIT;
  }

  function capLedger(rows, incomingCount, limit) {
    if (rows.length <= limit) return rows;
    // Always retain this tick's rows so a settled delta can replace an older
    // PENDING row. Fill the remaining bounded window with the most recent open
    // rows first, then the most recent settled history.
    var incoming = rows.slice(0, incomingCount).slice(0, limit);
    var carry = rows.slice(incoming.length);
    carry.sort(function (a, b) {
      var pendingDiff = Number(rowIsPending(b)) - Number(rowIsPending(a));
      return pendingDiff || (rowRecency(b) - rowRecency(a));
    });
    return incoming.concat(carry.slice(0, limit - incoming.length));
  }

  function mergeLedger(payload) {
    var limit = ledgerLimit(payload);
    var prevLedger = (window.LIVE && window.LIVE.ledger) || [];
    var activeLabels = activeLedgerLabels(payload);
    var walletStart = payload.kpis && payload.kpis.wallet_start_at;
    prevLedger = filterActive(filterWalletStart(prevLedger, walletStart), activeLabels);

    var incoming = Array.isArray(payload.ledger) ? filterActive(payload.ledger, activeLabels) : [];
    if (incoming.length) {
      var seen = new Set(
        incoming.filter(function (r) { return r && r.id != null; }).map(function (r) { return r.id; })
      );
      var keepPrev = prevLedger.filter(function (r) { return r && r.id != null && !seen.has(r.id); });
      var keepNullId = prevLedger.filter(function (r) { return r && r.id == null; });
      payload.ledger = capLedger(incoming.concat(keepPrev).concat(keepNullId), incoming.length, limit);
    } else {
      payload.ledger = capLedger(prevLedger, 0, limit);
    }
  }

  function probeAuth(restartClosedStream) {
    if (authProbe) return authProbe;
    authProbe = fetch('/api/v2/auth/status', {
      credentials: 'same-origin', cache: 'no-store'
    }).then(function (resp) {
      if (resp.status === 401) {
        clearLive('auth-required');
        return false;
      }
      if (!resp.ok) {
        clearLive('rejected');
        return false;
      }
      setState('reconnecting');
      if (restartClosedStream) scheduleEventSourceRestart();
      return true;
    }).catch(function () {
      // A failed auth probe cannot prove the session is still valid. Keep the
      // UI empty until the stream reconnects and a fresh authenticated tick
      // arrives, instead of showing a stale operator session.
      clearLive('rejected');
      return false;
    }).finally(function () {
      authProbe = null;
    });
    return authProbe;
  }

  function scheduleEventSourceRestart() {
    if (reconnectTimer) return;
    var delay = Math.min(30000, 1000 * Math.pow(2, reconnectAttempt));
    reconnectAttempt += 1;
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      startEventSource(false);
    }, delay);
  }

  function startEventSource(resetBackoff) {
    if (resetBackoff) reconnectAttempt = 0;
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (es) es.close();
    setState('connecting');

    try {
      es = new EventSource('/events');

      es.addEventListener('tick', function (ev) {
        try {
          var payload = JSON.parse(ev.data);
          // Deploy detection: server_boot_ts changes when the scanner process
          // restarts (every deploy). Reload once so this tab never keeps
          // rendering pre-deploy JS against post-deploy data. Guarded via
          // sessionStorage so a flapping process can't reload-loop the tab
          // more than once per boot stamp.
          if (payload.server_boot_ts != null) {
            if (window.__SERVER_BOOT_TS == null) {
              window.__SERVER_BOOT_TS = payload.server_boot_ts;
            } else if (payload.server_boot_ts !== window.__SERVER_BOOT_TS) {
              var seenKey = 'cb_reloaded_for_boot_' + payload.server_boot_ts;
              if (!sessionStorage.getItem(seenKey)) {
                sessionStorage.setItem(seenKey, '1');
                location.reload();
                return;
              }
              window.__SERVER_BOOT_TS = payload.server_boot_ts;
            }
          }
          mergeLedger(payload);
          window.LIVE = payload;
          reconnectAttempt = 0;
          setState('live');
          window.dispatchEvent(new CustomEvent('cryptobot:live', { detail: payload }));
        } catch (e) {
          console.warn('SSE tick parse error: ' + e);
        }
      });

      es.onerror = function () {
        // readyState === 2 (CLOSED) -> server gave up reconnecting, often 503 max-clients.
        var nextState = (es.readyState === 2) ? 'rejected' : 'reconnecting';
        setState(nextState);
        probeAuth(nextState === 'rejected');
      };
      window.__SSE = es;
    } catch (e) {
      console.warn('EventSource init failed: ' + e);
      setState('rejected');
    }
  }

  window.CB_AUTH = {
    login: function (password) {
      return fetch('/api/v2/auth/login', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: password || '' }),
      }).then(function (resp) {
        if (!resp.ok) {
          setState('auth-failed');
          return resp.json().catch(function () { return {}; }).then(function (body) {
            throw new Error(body.reason || ('login_failed_' + resp.status));
          });
        }
        loadSnapshot();
        startEventSource(true);
        return true;
      });
    },
  };

  if (!window.fetch) {
    startEventSource(true);
    return;
  }
  fetch('/api/v2/auth/status', { credentials: 'same-origin', cache: 'no-store' })
    .then(function (resp) {
      if (resp.status === 401) {
        clearLive('auth-required');
        return;
      }
      if (!resp.ok) {
        clearLive('rejected');
        return;
      }
      startEventSource();
    })
    .catch(function () {
      clearLive('rejected');
    });
})();
