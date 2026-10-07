# polymarket/infra/

Scripts that deploy the bot to an AWS EC2 server.

| File | Role |
|---|---|
| `deploy.sh` | Runs the tests, builds the runtime bundle, copies it to the server and runs the installer |
| `install_remote.sh` | Runs on the server: installs Python and dependencies, writes the env file and systemd units, starts the service |
| `cryptobot.env.example` | Every environment setting the bot reads, with defaults |
| `requirements.runtime.txt` | Runtime Python dependencies |
| `cron.daily.sh` | Daily SQLite WAL checkpoint and disk-space check |

## Server

- Amazon Linux 2023 on a `t3.medium` with a 50 GB gp3 volume.
- Region `eu-west-1` (Ireland). Polymarket blocks order placement from the UK.
- The security group allows SSH (port 22) from your own IP only. Don't open port 8800; reach the dashboard through an SSH tunnel.

## Deploy

From the repo root, in Git Bash:

```bash
EC2_HOST=<your-ec2-host> PEM_PATH=<path-to-key.pem> bash polymarket/infra/deploy.sh
```

The script:

1. Runs `pytest` and stops if anything fails.
2. Bundles the runtime code, dashboard, models, live config and unit files. Secrets, databases and research data are left out.
3. Copies the bundle to `/home/ec2-user/cryptobot`. The Python environment and the live SQLite database are kept.
4. Runs `install_remote.sh`, which installs dependencies, writes the systemd units and starts the bot.
5. Waits for the `boot OK` log line and fails if it doesn't appear within 120 seconds.

Running it again later updates the server in place.

## Operating

```bash
# Follow the bot's logs
ssh -i <key.pem> ec2-user@<host> 'sudo journalctl -fu cryptobot'

# Dashboard: keep this tunnel open, then browse to http://127.0.0.1:8800/
ssh -L 8800:127.0.0.1:8800 -i <key.pem> ec2-user@<host>

# Stop, start or restart
ssh -i <key.pem> ec2-user@<host> 'sudo systemctl restart cryptobot'

# Query the trade ledger
ssh -i <key.pem> ec2-user@<host> \
  'sqlite3 /home/ec2-user/cryptobot/data/cryptobot.db "SELECT outcome, COUNT(*) FROM ledger GROUP BY outcome;"'
```
