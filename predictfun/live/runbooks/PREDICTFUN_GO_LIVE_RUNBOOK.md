# predict.fun GO-LIVE runbook (operator + Claude, 2026-07-23; Tokyo cutover 2026-08-01)

Operator order 2026-07-23: go live on predict.fun with account
`0x7667fB507E9Fc7Dffa9C776570B7a193Eff3ADF6`, converting the paper unit.
This runbook is the gate between "code merged" and "scanner armed" — the
submit wire (POST /v1/orders + /auth JWT) was implemented from the venue
docs/spec and is only PROVEN by the staged validation below. Do NOT set
`CRYPTOBOT_PREDICTFUN_LIVE=1` before stage 5 passes.

**2026-08-01 — predict.fun moved to its OWN box (ap-northeast-1, Tokyo).**
Ireland keeps the Polymarket bot only. See §6 for the Tokyo-specific
provisioning, the single-key-consumer rule, and the deploy driver.
Sizing is now **1% of equity per bet on BOTH intervals**
(`CRYPTOBOT_V2_BASE_FRAC_{5M,15M}=0.010`, pinned by the installer;
operator order 2026-08-02 — note ~$2 stakes sit below the stop-salvage
floor, accepted). The old
`CRYPTOBOT_BET_SIZE_MULTIPLIER=2` "2x stake" line is GONE — it was inert on
the sub-minute path (v2 sizing overwrites the portfolio target with
sizing_v2's equity fraction) and only made the dashboard misreport stakes.

## 0. Account topology (Kernel/Privy — read first)

predict.fun UI accounts are **Kernel smart wallets** (predict_sdk ships the
Kernel ABI/domain; the SDK calls this the "predict account"):

- The address shown in the predict.fun UI (deposits land there, collateral
  custody) = the **predict account** → `PREDICTFUN_PREDICT_ACCOUNT`.
- The **Privy-exported private key** (predict.fun → settings → export
  wallet) controls a DIFFERENT owner EOA that signs orders and pays gas →
  `PREDICTFUN_PRIVATE_KEY`. Fund the EOA with ~0.01–0.02 BNB.
- External-wallet logins (MetaMask etc.) have NO predict account: leave
  `PREDICTFUN_PREDICT_ACCOUNT` empty; the wallet's own key is both signer
  and maker.

`0x7667…ADF6` was probed 2026-07-23 (BNB-56): **0 USDT, 0 BNB** — which of
the two roles it plays is settled by where the UI deposit flow points.

## 1. Operator prerequisites (out-of-band, never in chat)

1. Deposit USDT via the predict.fun UI deposit flow + send BNB gas to the
   signer EOA. If the UI shows a different address/network than expected —
   STOP and report; the topology assumption is wrong.
2. One tiny (~$1) manual trade in the UI from the account. This is the ONLY
   sanctioned approval path (no approval broadcast is wired in our code;
   the SDK has one but it is deliberately unused for go-live) and it proves
   KYC/geo/ToS lets the account trade at all.
3. On the box, append to `/etc/cryptobot.env` (values from steps above):

       PREDICTFUN_PRIVATE_KEY=0x<privy-exported key>
       PREDICTFUN_PREDICT_ACCOUNT=0x7667fB507E9Fc7Dffa9C776570B7a193Eff3ADF6

   (Omit PREDICTFUN_PREDICT_ACCOUNT only for an external-wallet account.)
   Sizing SCANS the deposit (operator order 2026-07-23): stakes size off the
   live on-chain balance every tick — no bankroll constant to set; deposits
   take effect automatically. (CRYPTOBOT_PREDICTFUN_LIVE_BANKROLL exists
   only as an optional DD-accounting-basis/display seed, default 100.)
   Fund with headroom: readiness requires balance >= sum(group caps) and
   redeem is OFF at first — locked stakes drain the wallet between redeems.

## 2. Staged validation (on the box; env loaded)

    cd /home/ec2-user/cryptobot
    RUN='sudo bash -c "set -a; . /etc/cryptobot.env; set +a; \
        /home/ec2-user/cryptobot/.venv/bin/python -m \
        cryptobot_predictfun.live_validation"'

1. `--stage preflight` — creds/liveness/balances/approvals/market payload.
   Every line must be OK (approvals get satisfied by the UI trade).
2. `--stage auth` — JWT obtained; GET /account matches the configured
   addresses. Note the JWT TTL it prints.
3. `--stage build` — eyeball EVERY printed order field (maker = predict
   account; signer = EOA; tokenId = intended outcome; makerAmount ≈ usd).
4. `--stage reject --i-am-risking-real-money` — deliberately-unfillable
   FAK at 0.01 must kill/reject cleanly with nothing resting.
5. `--stage order --i-am-risking-real-money --usd 1.20 --price <best_ask>`
   — ONE minimal real fill. MUST-PASS: fill parse matches the on-chain CTF
   delta (pins the amountFilled unit); note sign_ms/post_ms (empirical
   answer to the taker-delay unknown) and whether any venue min-size error
   appeared (if min > $1-2, raise floor sizing before arming).
6. Hold to resolution → check `backfill_predictfun_settlement` grades the
   correct side (THE catastrophic silent failure paper cannot prove).
   `--stage sell` can be used on a slice to validate the stop path.
7. First WIN: redeem once (SDK `redeem_positions` via a supervised python
   one-liner, or the UI) — verify USDT returns to the maker address.

## 3. Arm the scanner

    # in /etc/cryptobot.env (BOTH lines — see the naming trap below):
    CRYPTOBOT_PREDICTFUN_LIVE=1
    CRYPTOBOT_PREDICTFUN_DRY_ENABLED=1
    # then from the workstation:
    bash predictfun/infra/deploy.sh

⚠️ **NAMING TRAP.** `CRYPTOBOT_PREDICTFUN_DRY_ENABLED` reads like a paper-mode
flag, but it is the **unit gate in BOTH modes**: `install_remote.sh` arms the
scanner with `arm_flag_gated_unit CRYPTOBOT_PREDICTFUN_DRY_ENABLED
cryptobot-predictfun.service`. With `CRYPTOBOT_PREDICTFUN_LIVE=1` but this
flag unset, the LIVE env file is written and the unit is installed — and never
started. The deploy will report the scanner as intentionally stopped.

install_remote.sh then regenerates /etc/cryptobot-predictfun.env in the
LIVE variant: DRY_RUN=false, LIVE artifact, fresh DB
(data/predictfun_live/), key+telegram inherited, WALLET_START_AT stamped
once, OnFailure paging drop-in installed, `CRYPTOBOT_V2_BASE_FRAC_*=0.010`
(1% both intervals), `CRYPTOBOT_LATE_WINDOW_SKIP=` (PF trades the final
seconds), and the two readiness-unblock lines
(`LIVE_REQUIRE_DEPOSIT_WALLET=false`, `POLY_SIGNATURE_TYPE=0` — without them
readiness fails closed on POLYMARKET wallet topology and every LIVE cell is
skipped forever on a box with no poly key). The paper DB stays untouched
(calibration baseline).

⚠️ **REVERT HAZARD — the era anchor.** Revert = unset `CRYPTOBOT_PREDICTFUN_LIVE`
+ redeploy (drop-in removed, paper variant regenerated). The paper heredoc now
PRESERVES `CRYPTOBOT_WALLET_START_AT` the same way the live one does; before
2026-08-01 it did not, so a revert-to-paper rewrote the file without the key and
the next live arm stamped a brand-new accounting cutover over the venue's era.
If you are reverting on a box that predates that fix, record the current
`CRYPTOBOT_WALLET_START_AT` from `/etc/cryptobot-predictfun.env` FIRST.

### 3b. Start processing (the step that is easy to miss)

A fresh DB seeds `operator_control_state` = **STOPPED_PROCESSING by design**.
The scanner will boot, score, and place NOTHING until the operator starts it.
`wallet_default_modes` seed LIVE from `v2_step1last_live.json` (the LIVE
artifact — the paper unit gets the deploy-generated all-DRY copy instead), so
the wallets are already LIVE; only the operator gate is closed.

As `ec2-user` on the box:

    set -a; . /etc/cryptobot.env; . /etc/cryptobot-predictfun.env; set +a
    /home/ec2-user/cryptobot/.venv/bin/python -m cryptobot.cli.operator start \
        --db-path /home/ec2-user/cryptobot/data/predictfun_live/cryptobot_predictfun_live.db \
        --reason "predict.fun go-live <date>"

Two traps in that command:

- **`cbot-operator` is NOT installed on the boxes.** The deploy stages a flat
  `src/` tree with no console scripts; `python -m cryptobot.cli.operator` is the
  only entry point there.
- **`--db-path` is REQUIRED.** Sourcing `/etc/cryptobot.env` sets
  `CRYPTOBOT_DB_PATH` to the **POLYMARKET** DB (the installer force-pins it on
  every box). Sourcing the predict.fun file second overrides it per-key, but
  pass `--db-path` explicitly anyway — if the order is ever reversed, or only
  the shared file is loaded, the command silently starts processing on the wrong
  ledger.

Verify: the Operator tab on :8801 shows `LIVE`, and the next tick logs an
entry rather than `SKIP:operator_stopped`.

## 4. Post-arm watch (first session)

- `journalctl -u cryptobot-predictfun -f` + dashboard :8801 (tunnel).
- Every entry books a real fill; error_kind histogram: no `auth`, 429s
  rare; stops fire (SELL path live — watch the first trigger).
- Balance drain vs redeem cadence: manual redeem daily until 3 clean
  cycles, then consider PREDICTFUN_AUTO_REDEEM_ENABLED (gas/nonce review
  first; Polymarket's auto-redeem was retired for cause).
- Abort lever: `CRYPTOBOT_SUBMINUTE_KILL=1` in /etc/cryptobot.env +
  restart unit (same as Polymarket).

## 5. Known deferred items (tracked, not blockers)

- ~~**Decision-gate fee charges the Polymarket parabola on this venue.**~~
  **FIXED 2026-08-01.** `strategy/score._fee_adjusted_edge` now dispatches on
  the venue through `cryptobot.venues.fees.fee_per_share`: predict.fun gets the
  200 bps TENT with the real per-market `feeRateBps` threaded from the decision
  context, Polymarket stays byte-identical (same theta, same float-op order —
  the research parity chain is unchanged). ⚠️ **Grading note:** every
  predict.fun row booked BEFORE this change was gated against an over-stated
  fee (at ask 0.75 the parabola is ~2.7x the tent), so the pre-fix cohort mix is
  biased toward high-edge entries. Do not pool pre- and post-fix rows.
  NOTE the deliberate exception: `strategy/spec_v2.py:46,61` and
  `strategy/hgb_features.py:120` still carry the inline theta parabola. That is
  the FROZEN research parity chain (`config.POLY_FEE_THETA ==
  sweep_lib.POLY_FEE_THETA == strategy.parity.POLY_FEE_THETA == 0.07`) and it
  must not be venue-forked — it defines the model's training-time feature, not
  the live gate.
- **filled_price_vwap booked at the cap (pessimistic).** STILL OPEN:
  `predictfun/live/src/cryptobot_predictfun/clob.py:1380,1408` set
  `filled_price_vwap=order_price` (the limit), not a per-fill VWAP; the
  behaviour is pinned by `tests/test_order_client.py:157` ("booked at the
  cap"). Stays until per-fill match events are wired.
- ~~**8800's PREDICT.FUN view reads the PAPER ledger snapshot.**~~
  **RESOLVED 2026-08-01** by deletion, not repair: the cross-venue dashboard
  view is gone. Each console renders only its own deployment (:8800 Polymarket,
  :8801 predict.fun); `predictfun_paper.json` / `predictfun_live.json`, their
  producers and their sse_server allowlist entries were all removed.
- **Dashboard reject-panel fee estimates still use the parabola.** Display
  only (`snapshot_builder._reject_block` / the missed-profit estimator) — the
  gate, the walker and grade-time all dispatch correctly. Cosmetic
  overstatement of "missed profit" on the predict.fun console.
- predict_sdk pinned ==0.0.20; bump deliberately + re-run stage build.

## 6. Tokyo box (ap-northeast-1) — provisioning + cutover

**Deploy driver.** From the repo root:

    bash predictfun/infra/deploy.sh

It is a thin wrapper over `polymarket/infra/deploy.sh` that pins
`CRYPTOBOT_BOX_ROLE=predictfun` and defaults `EC2_HOST` to the Tokyo instance
and `PEM_PATH` to `predictfun/predic.fun.pem` (both env-overridable). The role
is what makes the installer PF-only: it never writes, enables or restarts
`cryptobot.service` (the Polymarket scanner), pins the POLYMARKET book recorder
OFF, skips the poly out-of-band index drop, points the daily WAL-checkpoint /
audit-prune job at the predict.fun LIVE DB, and watches
`cryptobot-predictfun.service` + `predictfun-scanner.err` for boot-OK. Running
the POLYMARKET driver against Ireland is unchanged (role defaults to
`polymarket`), and it now also disables + removes the retired predict.fun units
there.

⛔ **SINGLE-KEY-CONSUMER RULE — do this FIRST.** predict.fun allows ~240
requests/min per API key. Two boxes scanning with the same key is ~480/min
(instant throttling on both) **and** two processes trading the same wallet —
split-brain accounting, double exposure, and stop passes racing each other.
Before the first Tokyo arm:

    # on IRELAND
    sudo systemctl disable --now cryptobot-predictfun.service
    sudo systemctl disable --now predictfun-book-recorder.service
    sudo systemctl disable --now cryptobot-predictfun-snapshot.timer

(A `polymarket/infra/deploy.sh` run against Ireland now does this
idempotently.) Confirm with `systemctl is-active cryptobot-predictfun` →
`inactive` before arming Tokyo.

**Secrets are HAND-PROVISIONED.** The installer never writes secrets: it
regenerates `/etc/cryptobot-predictfun.env` (layout facts only) and preserves
operator edits in `/etc/cryptobot.env`. Append these to
**`/etc/cryptobot.env` on the Tokyo box** before the first arm:

    PREDICTFUN_PRIVATE_KEY=0x<privy-exported signer key>
    PREDICTFUN_API_KEY=<predict.fun API key>
    PREDICTFUN_PREDICT_ACCOUNT=0x7667fB507E9Fc7Dffa9C776570B7a193Eff3ADF6
    NOTIFY_TELEGRAM_TOKEN=<bot token>
    NOTIFY_TELEGRAM_CHAT_ID=<chat id>
    CRYPTOBOT_PREDICTFUN_LIVE=1
    CRYPTOBOT_PREDICTFUN_DRY_ENABLED=1        # the unit gate — see §3
    CRYPTOBOT_PREDICTFUN_LIVE_BANKROLL=<real on-chain deposit, USD>

`CRYPTOBOT_PREDICTFUN_LIVE_BANKROLL` is **not cosmetic**: it seeds
`CRYPTOBOT_INITIAL_BANKROLL`, which is (a) the DENOMINATOR of the drawdown
halt and (b) the sizing fallback when the balance cache is stale
(`execution/pipeline.py:346`). Set it to the REAL deposited amount. Too low ⇒
the 0.30 DD halt trips on noise; too high ⇒ oversized bets whenever the balance
read fails. It is deliberately NOT re-read from chain at boot (a restart with
stakes locked would corrupt the drawdown series), so update it by hand after a
material deposit/withdrawal.

**Re-run the staged validation on Tokyo before arming.** §2's stages are
box-scoped: they prove THIS host's credentials, egress, clock and latency. Run
at minimum `preflight`, `auth`, `build`, `reject`, and one `order`.
The Tokyo POST band should look like the 2026-07-23 measurement (~700-900ms);
if it does not, stop and re-measure before trusting the stop path.

**Console.** :8801 on loopback, reached by SSH tunnel:

    ssh -L 8801:127.0.0.1:8801 -i predictfun/predic.fun.pem ec2-user@<tokyo host>

Dashboard auth is disabled on this box by design
(`CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1`) — the loopback bind plus the tunnel IS
the boundary, and `main.py` independently refuses to boot if the bind is not
loopback. The cross-venue view is gone: :8800 shows Polymarket only, :8801
shows predict.fun only.
