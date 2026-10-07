# polymarket/

Everything for the Polymarket venue: the trading engine, the Polymarket adapter, live config, deployment, research and the operator dashboard.

| Folder | Contents |
|---|---|
| [`src/cryptobot/`](src/cryptobot/) | The trading engine: scheduler, scoring, gates, sizing, order flow, ledger, reconciliation, dashboard server |
| [`src/cryptobot_polymarket/`](src/cryptobot_polymarket/) | Polymarket adapter: market discovery, order books, order placement, fees, wallet |
| [`live/`](live/) | Live strategy config and systemd unit files |
| [`infra/`](infra/) | EC2 deploy and install scripts, environment template |
| [`backtest/`](backtest/) | Data download, model training, order-book replay, research reports |
| [`dashboard/`](dashboard/) | Browser dashboard served by the running bot |

## Engine (`src/cryptobot/`)

| Module | Role |
|---|---|
| `main.py` | Process entry point: boots config, database, scheduler and the dashboard server |
| `scheduler/` | 1-second tick loop plus slower jobs (settlement sweep, wallet snapshots) |
| `strategy/` | Loads the frozen HGB models and computes features and probabilities |
| `execution/` | Entry gates, sizing, order submission, fills, ledger writes, drawdown halt |
| `reconciliation/` | Settles resolved markets and repairs orphaned orders |
| `persistence/`, `db/` | SQLite schema, connection setup, wallet reconciliation records |
| `observability/` | Snapshot builder and server-sent events (SSE) for the dashboard |
| `venues/` | The venue interface; the engine reaches exchanges only through it |
| `cli/` | Operator commands (`cbot-*` scripts in `pyproject.toml`) |

## Adapter (`src/cryptobot_polymarket/`)

| Module | Role |
|---|---|
| `gamma.py` | Market discovery, fee rates, settlement outcomes (Gamma API) |
| `clob.py`, `ws_mirror.py` | Order books over REST and WebSocket |
| `twap_mirror.py` | Chainlink 60-second TWAP at each window boundary |
| `walker.py` | Order placement and fill reconciliation |
| `price_to_beat.py` | Exact window-open price from Polymarket |
| `balance.py`, `transfer.py`, `relayer.py`, `primitives.py` | Wallet balance, transfers and on-chain helpers |

The engine reaches the adapter through `cryptobot.venues.get_adapter()`, and the taker-fee curve lives in `cryptobot/venues/fees.py`.

## Region

Polymarket blocks order placement from the UK, even though read requests work, so the bot ran in AWS `eu-west-1` (Ireland). Only a real order proves a region can trade; read checks don't.
