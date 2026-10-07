# cryptobot — AWS deploy

Fully automated push from the Windows dev machine to an EC2 instance.
Idempotent: the same `deploy.sh` invocation handles first-install and every
subsequent update.

## TWO boxes, ONE installer (`CRYPTOBOT_BOX_ROLE`, since 2026-08-01)

| Role | Box | Runs |
| --- | --- | --- |
| `polymarket` (**default**) | `eu-west-1` Ireland | The Polymarket scanner (`cryptobot.service`) and the poly WAL/audit job. Its current contract is the 60s TWAP-symmetric BTC+ETH+SOL+XRP policy. |
| `predictfun` | `ap-northeast-1` Tokyo | The predict.fun scanner ONLY (`cryptobot-predictfun.service`, REAL money). `cryptobot.service` is never written, enabled or restarted here. |

The role is plumbed `deploy.sh` env → the remote `install_remote.sh` invocation.
**Every** unit file ships in **every** tarball; the role picks which are laid
down, enabled and restarted, and which are disabled + removed (idempotent
convergence — a poly deploy now tears down any predict.fun units left on
Ireland). It also selects the daily WAL-checkpoint/audit-prune target DB and
which unit + log pair the boot-OK watcher tails.

Use the per-venue driver rather than exporting the role by hand:

```bash
bash polymarket/infra/deploy.sh     # Ireland (role defaults to polymarket)
bash predictfun/infra/deploy.sh     # Tokyo   (pins the role + host + PEM)
```

⛔ Pointing a driver at the *other* box's host will converge that box to the
wrong role. Both drivers echo `==> box role:` before they stage anything.

## Prerequisites — operator (you)

Before invoking `deploy.sh` once, set up the EC2 side **manually**:

1. **EC2 instance running.** Current deploy target is a `t3.medium` (~4 GB)
   in `eu-west-1` (**Ireland**), Amazon Linux 2023, 50 GB EBS gp3 root volume.
   ⛔ **The region is PINNED to eu-west-1.** `eu-west-2` (London) is geoblocked
   by Polymarket for ORDERS — reads all pass, order placement 403s — so never
   move the box there, and place ONE REAL order from any candidate region
   before committing a box to it (passing reads prove nothing).
   Upsized from `t3.small` on 2026-07-05 — the checkpoint model stack pushes
   resident memory past the old 2 GB box.
2. **Security group** allows inbound TCP/22 from your operator IP (only).
   Do NOT open 8800 — dashboard access goes through SSH tunnel.
3. **PEM key file** on this Windows machine. The driver expects an absolute
   or relative path via `PEM_PATH`.
4. **Public DNS/IP** of the instance (`ec2-x-x-x-x.compute.amazonaws.com`).

If you don't have the instance yet, the simplest path:
```
aws ec2 run-instances \
  --image-id <al2023-arm64-ami-for-your-region> \
  --instance-type t3.medium \
  --key-name cryptobot \
  --security-group-ids <sg-id> \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=50,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=cryptobot}]'
```

## First-time deploy

From the repo root on Windows (Git Bash):

```bash
EC2_HOST=<your-ec2-host> \
PEM_PATH=polymarket/cryptobot.pem \
bash polymarket/infra/deploy.sh
```

The script:

1. Runs `pytest` locally — refuses to ship if any test fails. For an
   emergency hotfix you can skip the gate, but it takes **two** explicit
   flags (audit #10): `SKIP_TESTS=1 CONFIRM_UNSAFE_DEPLOY=1`. `SKIP_TESTS=1`
   alone is refused with a loud banner — skipping the only safeguard between
   a broken commit and the live money bot must be a deliberate, acknowledged
   choice. When honored, a journald-captured banner records the timestamp,
   operator, and target.
2. Stages and tars only the runtime bundle: `src/cryptobot` plus both venue
   adapter packages, dashboard files, the current `twap_symmetric_v1` model,
   retained `checkpoint_v1` compatibility artifacts, the live config artifacts,
   and the unit files + deploy scripts. (`rf_v1`, `hgb_v1` and
   `decision_table.parquet` stopped shipping on 2026-07-31.)
3. Probes SSH connectivity to the instance.
4. `scp`'s the tarball, replaces the runtime tree under
   `/home/ec2-user/cryptobot`, and preserves only `.venv` plus live
   SQLite ledger sidecars.
5. Runs `install_remote.sh`: installs Python + sqlite + venv + runtime
   deps, lays down the systemd unit + env file + daily cron, enables and
   starts the service.
6. Tails `journalctl` and scanner logs for the `boot OK` line, fails loudly
   if it doesn't appear within 120s. Override with
   `BOOT_OK_TIMEOUT_SECONDS=<seconds>` for unusual deploy investigations.

End-to-end takes ~3-5 minutes on a fresh t3.medium (mostly the pip install).
Re-runs after the first usually take ~30-90 seconds.

## Operating

**Watch the live tick stream:**
```bash
ssh -i polymarket/cryptobot.pem ec2-user@<your-ec2-host> \
    'sudo journalctl -fu cryptobot'
```

**Open the dashboard from your laptop:**
```bash
# In one terminal — keep this open:
ssh -L 8800:127.0.0.1:8800 -i polymarket/cryptobot.pem ec2-user@<your-ec2-host>

# In your browser:
# http://127.0.0.1:8800/
```

The SSE stream proxies through the SSH tunnel — the dashboard sees the live
ledger / KPIs / market grid / gate stack updating at 1 Hz.

**Stop / start / restart:**
```bash
ssh -i polymarket/cryptobot.pem ec2-user@<host> 'sudo systemctl stop cryptobot'
ssh -i polymarket/cryptobot.pem ec2-user@<host> 'sudo systemctl start cryptobot'
ssh -i polymarket/cryptobot.pem ec2-user@<host> 'sudo systemctl restart cryptobot'
```

**Inspect the ledger directly:**
```bash
ssh -i polymarket/cryptobot.pem ec2-user@<host> \
    'sqlite3 /home/ec2-user/cryptobot/data/cryptobot.db \
    "SELECT outcome, COUNT(*) FROM ledger GROUP BY outcome;"'
```

## Update flow

After editing code locally and committing on `main`:

```bash
# Same command as first deploy — install_remote.sh handles the upgrade.
EC2_HOST=<your-ec2-host> \
PEM_PATH=polymarket/cryptobot.pem \
bash polymarket/infra/deploy.sh
```

The service restarts automatically. In-flight ticks are drained via
`scheduler.shutdown(wait=True)` before the new code takes over (review fix
#10), so no PENDING row is lost on redeploy.

## Disk + state

| Path | Purpose | Survives redeploy? |
|------|---------|--------------------|
| `/home/ec2-user/cryptobot/` | Runtime source/artifacts | replaced on each deploy |
| `/home/ec2-user/cryptobot/.venv/` | Python venv | preserved (idempotent install upgrades in place) |
| `/home/ec2-user/cryptobot/data/cryptobot.db` | Live ledger + signals | **preserved** — never deleted by deploy |
| `/home/ec2-user/cryptobot/data/models/` | `twap_symmetric_v1` current Polymarket scorer plus retained checkpoint artifacts | replaced on each deploy |
| `/home/ec2-user/cryptobot/runtime/config/` | TWAP source staged as `v2_step1last_live.json` plus the generated predict.fun DRY copy | replaced on each deploy |
| `/etc/cryptobot.env` | Operator env overrides | **preserved** — install only seeds once |
| `/etc/systemd/system/cryptobot.service` | systemd unit | replaced on each deploy |
| `/etc/cron.daily/cryptobot` | WAL checkpoint cron | replaced on each deploy |
| `/var/log/cryptobot/scanner.log` | Flat log file | rotated daily, 30-day retention via `deploy/cryptobot-scanner.logrotate` (installed to `/etc/logrotate.d/cryptobot-scanner`) |
| journald | Structured log stream | rotates per system config |

The daily cron does `PRAGMA wal_checkpoint(TRUNCATE)` and disk-free probe.
The bot's own F3 sweep prunes `signals` rows older than 7 days every 5 min
(review fix #13). `pipeline_health` has no auto-prune — manually trim if it
grows large after long soaks:
```sql
DELETE FROM pipeline_health WHERE created_at < datetime('now','-30 days');
```

## Cost guard-rails

A t3.medium is ~$33/month + ~$4/month for 50 GB gp3, and it keeps running (and
billing) if you walk away. To cap cost when not soaking: stop the instance from
the AWS console, or set a CloudWatch `CPUUtilization < 5% for 4h` auto-stop
alarm. **This is the live-money bot — do NOT stop it while it is trading.**

## Security posture

- **SSE bind = 127.0.0.1** (loopback only) — verified in code via the
  refuse-to-boot check (review fix #12). Dashboard access requires SSH key.
- **This bot places REAL money orders.** It IS live (real money since
  2026-06-12). Spot/book reads (Binance, Polymarket Gamma/CLOB books)
  are unauthenticated public endpoints, but **placing orders needs live
  Polymarket CLOB credentials** (`POLY_API_KEY` / `POLY_SECRET` /
  `POLY_PASSPHRASE` / `POLY_PRIVATE_KEY`) plus the funder/relayer keys.
- **Secrets live in `/etc/cryptobot.env`, NOT in the deploy bundle.** The
  tarball carries no keys: `install_remote.sh` seeds `/etc/cryptobot.env`
  once (mode `0640`, `root:ec2-user`) from the example and then preserves the
  operator-filled `POLY_*` / `RELAYER_*` values across every redeploy. Treat
  that file — and the box — as holding the wallet's signing key.
- **PEM never copied to the instance** — `deploy.sh` excludes `*.pem`
  from the tarball.
- **systemd hardening**: `ProtectSystem=strict`, `ProtectHome=read-only`,
  `ReadWritePaths` whitelisted, `NoNewPrivileges=true`. The scanner can
  only write to `data/`, `runtime/data/`, and `/var/log/cryptobot/`.
- **Memory cap**: `MemoryMax=3300M` / `MemoryHigh=2900M` on the unit — the
  bot OOMs cleanly via systemd instead of swap-thrashing the ~4 GB box
  (raised from 1500M on 2026-07-05 for the 614 MB checkpoint model).

## Troubleshooting

**Boot didn't say "boot OK"** — check `sudo journalctl -u cryptobot -n 100`
for the actual exception. Most common: a missing artifact under
`data/models/twap_symmetric_v1/` (the current runtime bundle must include it).

**Dashboard stuck on "CONNECTING…"** — the SSH tunnel is the chokepoint.
Verify with `curl -v http://127.0.0.1:8800/events` from your laptop while
the tunnel is open; if you get HTTP 200 + `event: tick` frames, the tunnel
is fine and the dashboard JS hook is at fault.

**Tick deadline CRITICAL spam** — the qualifying tick is over budget. Open
issue (Polymarket CLOB calls are sequential; needs parallelization). For
now, switch `CRYPTOBOT_TICK_SECONDS=2` in `/etc/cryptobot.env` and
`sudo systemctl restart cryptobot` — gives the tick more wall time.
