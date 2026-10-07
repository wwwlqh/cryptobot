# polymarket/dashboard/

The operator dashboard: a dark single-page React app served by the running bot. It updates once a second over server-sent events (SSE).

## Running it

The bot serves the dashboard itself: `main.py` starts `cryptobot.observability.sse_server`, which serves these files and the `/events` stream on `127.0.0.1:8800`.

- Locally: start the bot, then open http://127.0.0.1:8800/.
- On the server: open an SSH tunnel first (see [`../infra/README.md`](../infra/README.md)).

The page stays empty when no live bot is connected; it never shows sample data. `build_snapshot.py` writes `snapshot.json`, a static fallback with portfolio context.

## Pages

| Group | Page | Shows |
|---|---|---|
| Trading | Overview | Equity and drawdown, the gross → fees → rebates → net P&L strip, per-coin and per-slice results |
| Trading | Operator | Bot state, readiness checks, wallet balance |
| Trading | Subminute | Per-second entries in the final window, sizing, drawdown brake, rejects and fills |
| Analytics | Trade journal | One expandable row per trade, with its fills |
| Analytics | Calendar | Daily realised P&L heatmap |
| Analytics | Latency | Time spent in each stage from decision to fill |
| Config | Strategy | The live strategy settings, read from the payload |

## Files

| File | Role |
|---|---|
| `index.html` | Loads React, Babel and the page modules |
| `styles.css` | Theme and component styles |
| `live-bridge.js` | Connects to `/events` and keeps the latest payload |
| `api.js` | Authenticated operator actions |
| `time.js` | Time and settlement helpers |
| `data.jsx`, `charts.jsx`, `shell.jsx`, `app.jsx` | Formatters and analytics, SVG charts, layout, routing |
| `pages-*.jsx` | One module per page group |
| `build_snapshot.py` | Writes `snapshot.json` |
| `assets/` | Icons and coin logos |

The payload comes from `src/cryptobot/observability/snapshot_builder.py`.
