# polymarket/ — the live venue

Everything Polymarket-specific. The trading engine itself is shared and lives in
[`src/cryptobot`](src/cryptobot); this tree holds the venue's
**adapter package**, its ops surface, its research substrate, and its box config.

| Dir | What is in it |
| --- | --- |
| [`src/`](src/) | **ONE src root per venue** (2026-08-04): the shared engine `cryptobot/` alongside the adapter `cryptobot_polymarket/`. Was `core/src/` + `live/src/` |
| [`src/cryptobot_polymarket/`](src/cryptobot_polymarket/) | **The adapter package** (Phase 2, 2026-07-28) — everything the engine reaches through `cryptobot.venues.get_adapter("polymarket")` |
| [`live/`](live/) | The current TWAP contract (`config/twap_symmetric_live.json`) + systemd units for the scanner and its timers |
| [`backtest/`](backtest/) | The research tree — engine, lib, training, fetch, studies, and the [`SOURCES.md`](backtest/SOURCES.md) provenance manifest |
| [`infra/`](infra/) | `deploy.sh`, `install_remote.sh`, the env contract (`cryptobot.env.example`), `requirements.runtime.txt`, and the daily WAL-checkpoint/disk-probe cron (the decision-table refresh it used to carry went with the 2026-07-31 exp044 retirement) |

## The adapter package

`src/cryptobot_polymarket/` was carved out of the shared package on
2026-07-28 (it sat at `live/src/` until the 2026-08-04 single-src collapse).
Same code, new addresses:

| Module | Was (`core/src/cryptobot/...`) | What it does |
| --- | --- | --- |
| `clob.py` | `data/polymarket_clob.py` | REST book fetch (`fetch_top_book`, `fetch_books_batch`) |
| `gamma.py` | `data/polymarket_gamma.py` | Market discovery, fee cache, settlement (`fetch_resolved_outcome`) |
| `ws_mirror.py` | `data/clob_ws_mirror.py` | WS book mirror (venue-timestamped snapshots) |
| `twap_mirror.py` | `data/twap_mirror.py` | Chainlink RTDS boundary-TWAP mirror; exact 60s anchors |
| `walker.py` | `execution/walker.py` | `OrderClient` — FAK/GTC placement, fill reconciliation |
| `relayer.py` / `transfer.py` | `execution/polymarket_*.py` | pUSD transfers and relayer submitters; redemption is manual/native |
| `primitives.py` | `polymarket/primitives.py` | Chain constants, wallet derivation, calldata encoders |
| `balance.py` | *(new)* | pUSD balance fetcher, lifted out of core `live_readiness` |
| `fees.py` | *(split)* | The **PARABOLA**. The old `predictfun/fees.py` held BOTH venues' math plus the dispatcher; the dispatcher is now `cryptobot/venues/fees.py` |
| `derive_creds.py` / `wallet_diagnostics.py` | `cli/*poly*` | `cbot-derive-poly-creds`, `cbot-poly-wallet-diagnostics` (script names unchanged) |
| `backfill_settlement.py` | `reconciliation/backfill_polymarket_settlement.py` | Batch settlement regrade |

`__init__.py` defines `ADAPTER` and nothing else at module scope — every method
imports its implementation lazily, so resolving the registry (to price a
historic ledger row, say) never opens an httpx client or starts a WS thread.

The package may import core; it must NOT import another venue package
(`python tools/importcheck.py`). Anything both venues need — `BookSnapshot`,
`TokenBucket`, `FeeUnknown`, and the WS shadow comparator —
lives in `cryptobot/venues/` and is imported from there by both sides, as ONE
class object so `isinstance` still works across the seam.

## Box

**Region eu-west-1 (Ireland) — PINNED.** eu-west-2 (London) is geoblocked for
ORDERS: reads all pass, order placement 403s. That is why the hard rule exists —
**place ONE REAL order from a candidate region before committing any box to
it.** Passing reads prove nothing.

## Deploy shape

`polymarket/infra/deploy.sh` stages a runtime-only tarball and maps the local
tree onto the **unchanged** remote layout:

| Local | Remote |
| --- | --- |
| `polymarket/src/cryptobot` | `$PROJECT_DIR/src/cryptobot` (tests stripped) |  <!-- legacy-path-ok: remote layout frozen until Phase 3 -->
| `polymarket/src/cryptobot_polymarket` | `$PROJECT_DIR/src/` (alongside `cryptobot`, tests stripped) |  <!-- legacy-path-ok: remote layout frozen until Phase 3 -->
| `predictfun/live/src/cryptobot_predictfun` | `$PROJECT_DIR/src/` — **both venues ship to both boxes**: grade-time fee booking dispatches on each ledger ROW's venue, so a Polymarket box with PF rows in its history must be able to price the tent |
| `polymarket/live/config/twap_symmetric_live.json` | `$PROJECT_DIR/runtime/config/v2_step1last_live.json` (remote compatibility name) |
| `data/models/twap_symmetric_v1` | `$PROJECT_DIR/data/models/twap_symmetric_v1/` — current Polymarket scorer |
| `data/models/checkpoint_v1` | `$PROJECT_DIR/data/models/checkpoint_v1/` — retained for rollback/predict.fun compatibility |
| `polymarket/dashboard/` | `$PROJECT_DIR/dashboard/` |
| every unit file + installer, from its per-venue home | `$PROJECT_DIR/deploy/` (flat) |

⚠️ The remote wipe step spares only what its two `! -name` whitelists name
(`deploy.sh`, the `REMOTE_EXTRACT` heredoc). A missing entry has destroyed
real-money DB state and weeks of recorder history. **Any new runtime dir under
`data/` must be added to the whitelist in the same change that creates it.**

## Where things run

- Scanner + dashboard: on the box, loopback `:8800` (SSH-tunnel to view — always
  `127.0.0.1:8800`, never a local render).
- Recorders: none. Both book-recorder units were retired 2026-08-02 and their
  code deleted 2026-08-04; the `data/{book_recorder,predictfun_book_recorder}/`
  deploy exclusions stay as fail-safes only.
- Research + deploy driving: the operator laptop only. Nothing under
  `backtest/`, `docs/`, or `tools/` is ever shipped.
