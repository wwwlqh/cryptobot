# polymarket/live/

The deployed Polymarket scanner's adapter package, contract, and unit files.

Phase 2 of the restructure is **done**: the venue adapter package lives at
[`../src/cryptobot_polymarket/`](../src/cryptobot_polymarket/) (see
[`../README.md`](../README.md) for the module-by-module map) — it moved out of
`live/src/` on 2026-08-04 so the venue has ONE src root. The trading *engine* is
the shared `cryptobot` package in `polymarket/src`, alongside it; this tree now
holds only the box-facing contract and units.

## `config/`

`twap_symmetric_live.json` — the **current production contract**. It is the
source-of-truth local artifact for the Chainlink/Binance-TWAP scorer. `deploy.sh`
stages it as `$ROOT/runtime/config/v2_step1last_live.json` to preserve the
remote environment variable and unit contract; the live process also loads
the `twap_symmetric_v1` model and its frozen basis centres from
`$ROOT/data/models/`.

⚠️ Three values move as a **trio** and the loader fails closed at boot if they
disagree: the artifact's `concurrent_exposure_cap_fraction` and
`max_drawdown_halt_frac` must equal `CRYPTOBOT_SHARED_EXPOSURE_CAP` (1.00) and
`CRYPTOBOT_MAX_DD_HALT_FRAC` (0.30), both force-pinned by `install_remote.sh`.
Changing one without the others is a boot failure, not a config tweak.

## `service/`

| Unit | Role |
| --- | --- |
| `cryptobot.service` | The scanner (also serves the dashboard + SSE on loopback 8800) |
| `cryptobot-wal-checkpoint.{service,timer}` | Periodic WAL checkpoint / TRUNCATE + disk probe |
| `cryptobot-failure-alert.service` | `OnFailure=` hook that notifies on unit failure |
| `cryptobot-scanner.logrotate` | Log rotation for scanner and predict.fun logs in `/var/log/cryptobot` (locked to LF; logrotate silently skips a CRLF file) |

`cryptobot-snapshot-refresh.{service,timer}` was RETIRED 2026-08-01 with the
cross-venue dashboard view; `install_remote.sh` disables + removes it on both
box roles.

⚠️ **`cryptobot.service` is POLYMARKET-ONLY.** Since the 2026-08-01 predict.fun
Tokyo cutover the installer is role-parameterized (`CRYPTOBOT_BOX_ROLE`): on a
`predictfun` box this unit is never written, enabled or restarted.

Every `ExecStart=` / `WorkingDirectory=` / `ReadWritePaths=` in these units
points at the **remote** layout (`/home/ec2-user/cryptobot/...`), which the
restructure deliberately left unchanged. `install_remote.sh` rewrites the user
and `$PROJECT_DIR` at install time. Editing a path here means editing the box —
treat it as a deploy change, not a refactor.
