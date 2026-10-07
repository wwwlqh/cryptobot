# predictfun/ — REAL MONEY, own box (Tokyo)

predict.fun venue tree. **Moved to a dedicated `ap-northeast-1` (Tokyo) box on
2026-08-01** and armed as real money. Ireland keeps the Polymarket bot only; the
predict.fun paper instance there is retired.

## Status

| | |
| --- | --- |
| Live | **REAL MONEY on the Tokyo box.** Arming checklist: `live/runbooks/PREDICTFUN_GO_LIVE_RUNBOOK.md` (read section 6 for the Tokyo specifics). |
| Box | `ap-northeast-1` (Tokyo), Amazon Linux 2023, `ec2-user`. The PF origin is Tokyo, so the relocation IS the latency lever. Deploy with `bash predictfun/infra/deploy.sh` — a thin wrapper that pins `CRYPTOBOT_BOX_ROLE=predictfun`. ⛔ The Polymarket scanner unit must never run here (no poly key, no poly ledger); the installer does not even lay it down under this role. |
| Single-key rule | ⛔ predict.fun allows ~240 req/min **per API key**. STOP the Ireland predict.fun units before arming Tokyo — two boxes on one key is ~480/min *and* two processes trading the same wallet (split brain). |
| Sizing | **1% of equity per bet on BOTH intervals** (`CRYPTOBOT_V2_BASE_FRAC_{5M,15M}=0.010`, force-pinned by the installer into `/etc/cryptobot-predictfun.env`; operator order 2026-08-02, superseding the 08-01 2%/2%). ⚠️ At ~$195 bankroll this is ~$2/bet — below the ~$3.5–4 stop-salvage floor (see stop-loss section), so deep decays ride to zero un-stoppable; accepted with the resize. This replaced `CRYPTOBOT_BET_SIZE_MULTIPLIER=2`, which was **inert** on the sub-minute path — v2 sizing overwrites the portfolio target with sizing_v2's equity fraction, so the multiplier never moved a real stake and only made the dashboard misreport it. |
| Entry window | **PF trades through the final 5 seconds.** `CRYPTOBOT_LATE_WINDOW_SKIP=` (empty) is pinned on this venue. The armed `BTC:5,ETH:5,SOL:5,XRP:5` gate is a POLYMARKET policy, justified on Polymarket's live two-era evidence; nothing about it has been measured here. |
| Coins | `BTC,ETH`. |
| Console | `:8801` on loopback, via SSH tunnel. Dashboard auth is disabled by design (the loopback bind + tunnel is the boundary; `main.py` re-checks the bind). The cross-venue view was retired 2026-08-01: `:8800` is Polymarket-only, `:8801` is predict.fun-only. |
| Order signing | `predict_sdk==0.0.20`, exact-pinned (`pip install -e .[predictfun]`). It signs real-money EIP-712 orders; a silent minor bump must never change signing behavior under us. DRY mode and the recorder run fine without it. |

## ⚠️ Stop-loss reality at these stakes

The 2026-07-23 halt cause was structural and is **not fixed** — it is bounded by
sizing. predict.fun rejects `POST /v1/orders` under a **$0.90 minimum order
value**, so a crash-salvage sell only clears the venue floor while

    stake * (bid_at_stop / p_entry) >= 0.90

i.e. roughly `min sellable bid ~= 0.90 * p_entry / stake`. At the ~$1.90 stakes
2% of a small bankroll produces, a position bought at 0.75 can only be stopped
while its bid is above ~0.36 — a shallow drift is exitable, but a **deep crash
is structurally un-stoppable** and rides to −100%. Full stop coverage at crashed
quotes needs roughly a **$3.5–4 stake floor**.

Live behaviour is fail-loud, not silent: `CRYPTOBOT_STOPLOSS_MIN_EXIT_VALUE_USD=0.90`
makes the stop pass record `outcome='below_venue_min'` and fire ONE critical
alert per un-stoppable position, instead of the HTTP-400 spam that hid the
2026-07-23 `id6` incident (three salvage sells 400'd invisibly; the position
rode to −100%). The `CRYPTOBOT_MIN_STOPPABLE_STAKE_USD` lever exists and is
deliberately left **commented out** in the installer — arming it is an operator
policy decision at restart, not a deploy default.

## Fee gate (venue-priced since 2026-08-01)

The entry gate charges the **predict.fun TENT**, not Polymarket's parabola:

    fee = (feeRateBps / 10000) * min(ask, 1 - ask)      # 200 bps observed

`strategy/score._fee_adjusted_edge` dispatches on `config.VENUE` through
`cryptobot.venues.fees`, with the real per-market `feeRateBps` threaded from the
decision context. At the favourite prices these sleeves bet (ask 0.75) the tent
is 0.005/share against the parabola's 0.0135 — the old code over-charged by
~2.7x and rejected entries that cleared the threshold. ⚠️ **Do not pool
pre-2026-08-01 predict.fun rows with post-fix rows when grading**: the earlier
cohort was selected against an over-stated fee.

Deliberately NOT venue-forked: `strategy/spec_v2.py` and
`strategy/hgb_features.py` keep the inline theta parabola. That is the frozen
research parity chain (it defines a model TRAINING feature, not the live gate).

## Where the code is

`live/src/cryptobot_predictfun/` — the adapter package, extracted from the
shared package on 2026-07-28 (Phase 2). Same code, new addresses:

| Module | Was (`core/src/cryptobot/...`) |
| --- | --- |
| `markets.py`, `clob.py`, `auth.py`, `_http.py`, `ws_mirror.py` | `predictfun/` (names unchanged) |
| `wallet.py` | `execution/predictfun_wallet.py` |
| ~~`book_recorder.py`~~ | `data/predictfun_book_recorder.py` — ⛔ RETIRED 2026-08-02, DELETED 2026-08-04 |
| `derive_creds.py`, `wallet_diagnostics.py`, `live_validation.py` | `cli/predictfun_*` (`cbot-*` script names unchanged) |
| `backfill_settlement.py` | `reconciliation/backfill_predictfun_settlement.py` |
| `fees.py` | `predictfun/fees.py` — **reduced to the TENT alone** |

The fee split is done. `predictfun/fees.py` was misnamed: it held **both**
venues' fee math *and* the dispatcher, so every Polymarket fee call imported the
predict.fun package. It is now three files — the tent here, Polymarket's
parabola in `cryptobot_polymarket.fees`, and the dispatcher in
`cryptobot/venues/fees.py`.

The engine reaches all of this through
`cryptobot.venues.get_adapter("predictfun")`; there are no `config.VENUE`
branches left in core. `__init__.py` defines `ADAPTER` and nothing else at
module scope, so resolving the registry never imports `predict_sdk` or starts a
WS thread — which is what lets a Polymarket box carry this package purely to
price historic PF ledger rows.

This package may import core; it must NOT import `cryptobot_polymarket`
(`python tools/importcheck.py`). What it used to borrow from there —
`BookSnapshot`, `TokenBucket`, `FeeUnknown`, `PolymarketResolution`, the WS
shadow comparator, and three recorder helpers — now lives in `cryptobot/venues/`
and is imported from core by both venues.

## Layout

| Dir | What |
| --- | --- |
| `live/src/cryptobot_predictfun/` | The adapter package (above) + its tests |
| `live/service/` | `cryptobot-predictfun.service`. The snapshot timer and `refresh_dashboard_snapshots.sh` were RETIRED 2026-08-01 with the cross-venue dashboard view — each console renders only its own deployment's SSE payload now |
| `live/runbooks/` | `PREDICTFUN_GO_LIVE_RUNBOOK.md` — the arming checklist |
| `live/config/` | PF env contract + the generated all-DRY V6 artifact (generated at deploy time, not committed) |
| ~~`recorder/service/`~~ | Held `predictfun-book-recorder.service` — ⛔ RETIRED 2026-08-02, unit DELETED 2026-08-04 with the recorder code |
| `backtest/` | [`SOURCES.md`](backtest/SOURCES.md) + [`README.md`](backtest/README.md). `lib/` and `studies/` exist but are EMPTY; there are no PF notebooks. The live tent-fee model is `live/src/cryptobot_predictfun/fees.py` |
| `infra/` | `deploy.sh` — the Tokyo deploy driver (pins `CRYPTOBOT_BOX_ROLE=predictfun` plus this box's host/PEM, then execs `polymarket/infra/deploy.sh`) |

## Standing rules

1. Read `live/runbooks/PREDICTFUN_GO_LIVE_RUNBOOK.md` end to end before any
   arm, re-arm or region change.
2. **Place ONE REAL order from a candidate region before committing a box.**
   Reads passing proves nothing — that is the London lesson, and PF's own
   latency work said Tokyo, not "somewhere in Asia".
3. The stop problem is bounded by SIZE, not tuning (see the stop-loss section
   above). Raising stakes changes what is stoppable; treat it as a risk change.
4. `predictfun_paper` / `predictfun_live` must stay in the deploy
   wipe-whitelist (`polymarket/infra/deploy.sh`) — a missing exclusion already
   wiped the real-money PF DB once (2026-07-23), resetting the bot to
   `STOPPED_PROCESSING` mid-session and losing settled-bet accounting.
5. ⛔ ONE consumer per API key. Never run predict.fun units on two boxes.
