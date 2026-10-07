# polymarket/live/

The live strategy config and the systemd units used on the EC2 server.

## `config/`

| File | Contents |
|---|---|
| `twap_symmetric_live.json` | The live strategy (below) |
| `twap_symmetric_release.json` | Release pin: model hash, routing, thresholds and execution settings that `deploy.sh` checks the shipped model against |

The live strategy, as frozen on 2026-08-15:

- **Markets:** BTC, ETH, SOL and XRP up/down, 5-minute and 15-minute windows.
- **Models:** two `HistGradientBoostingClassifier`s, one per interval, pooled across the four coins with `coin_id` as a categorical feature. They use 11 features built from price, time and momentum.
- **Labels:** the Chainlink 60-second TWAP that Polymarket settles on.
- **Entry window:** from 150 seconds before the window closes until 1 second before.
- **Gates:** ask price between 0.55 and 0.85, fee-adjusted edge of at least 0.10, and at most 6% of the visible order-book depth taken.
- **Sizing:** 1% of current equity per bet, one fill per market, no top-ups.
- **Risk:** trading halts at a 40% drawdown from peak equity.

The loader refuses to start if the exposure cap or drawdown halt in this file disagree with the server's environment settings.

## `service/`

| Unit | Role |
|---|---|
| `cryptobot.service` | The bot. It also serves the dashboard on `127.0.0.1:8800`. |
| `cryptobot-wal-checkpoint.{service,timer}` | Daily SQLite WAL checkpoint and disk-space check |
| `cryptobot-failure-alert.service` | Sends an alert when the bot unit fails |
| `cryptobot-scanner.logrotate` | Log rotation for `/var/log/cryptobot` |

Paths in these units point at the server layout (`/home/ec2-user/cryptobot`). `infra/install_remote.sh` fills in the user and project directory at install time.
