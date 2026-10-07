# Backtest

This package contains the current TWAP-symmetric production research path plus
the retained checkpoint replay used for rollback and predict.fun compatibility.
The current Polymarket policy is the BTC+ETH+SOL+XRP, 5m + 15m TWAP contract:

- exact 60-second Chainlink boundary TWAP labels for both intervals
- causal Binance opening TWAP over `[boundary-window,boundary)` with a frozen,
  centred and one-basis-point-clipped source-basis correction
- direct 11-feature `twap_symmetric_v1` HGB scorer
- `0.60 < ask <= 0.95`, with fee-adjusted edge thresholds of 0.06 / 0.05
- one entry fill per market, T3 enabled, Tier A disabled

The current WFO and exact-forward drivers live under
`studies/tick_latency/`. The checkpoint material below is historical/reference
coverage, not the current live policy.

## Historical checkpoint artifacts

- Band: `0.65 < ask <= 0.85`, `pred_edge >= 0.03`  ← the only band
- Intervals: 5m and 15m
- Coins: BTC, ETH, SOL, XRP — all four at full equal size (2026-07-21 operator
  override, "follow the backtest face numbers", superseding the judged action
  list's XRP cut and SOL $1 probe — see FOLDF_HOLDOUT_20260721.md)

History: the second (near-lock) band was CUT 2026-07-21 — verdict and
re-admission gate in `FOLDF_HOLDOUT_20260721.md`, retained baseline in
`SUBMINUTE_ONLY_BASELINE_20260721.md`. Those two and every other dated study
file cited below were dropped from `studies/` on 2026-07-31 (a6df8ed); they are
readable in git history only.

**Reporting standard (2026-07-21, binding):** every fold report must show the
face number, a persistence-filtered number, and the OVERLAP-cohort
(live-verified fillable) number; engine-only cohorts only on live-active
hours. Face / engine-only numbers never justify config action — tick-replay
results on this venue are upper bounds unless fillability is modeled.

## Canonical run artifacts (the heavy parquets stay under
`BT_RUNS_DIR/tick_latency/`; the tracked JSON digests were dropped 2026-07-31
in a6df8ed except the audit-pinned one — the rest are in git history)

| Artifact | What it is |
| --- | --- |
| `foldBE_stop_report_400_TOPUP_T3.json` + parquet | Historical canonical (folds B-E, T3 top-ups, includes nl_5m; nl15_mult was already 0). JSON in git history; parquet retained |
| `foldBE_stop_report_400__MIDONLY_T3_20260721.json` + parquet | Retained MID-only twin of the T3 run, folds B-E. JSON in git history; parquet retained |
| `foldBE_stop_report_400__MIDONLY_T3_FOLDF_20260721.json` / `__T3_FOLDF_...` | Fold F (2026-07-06..21) MID-only / with-nl_5m T3 twins. JSONs in git history; parquets retained |
| `foldBE_stop_report_400__FOLDF_20260721.json` | Fold F base variant (no top-ups). JSON in git history; parquet retained |
| `foldBE_stop_report_400.json` | Base variant B-E (audit-pinned) — the only digest still tracked |
| `../scored_foldEF_20260721.parquet` | Fold E+F scores from the A-D-trained scorer (E verified vs truth: corr 0.961) |
- Grid: one decision per second during the final 30 seconds
- Selection: at most one entry per market resolution
- Model: checkpoint RF + HGB with the same 11 features on both intervals
- Execution evidence: real ticks at 400 ms

The retained checkpoint contract is
`polymarket/live/config/v2_step1last_live.json`. It is kept as a rollback and
compatibility artifact; the current source contract is
`polymarket/live/config/twap_symmetric_live.json`.

## Structure

| Path | Responsibility |
| --- | --- |
| `training/` | Complete option3 pool, 11-feature checkpoint training/export, leakage checks, depth helpers, and residual validation needed for a fresh rebuild |
| `lib/` | Canonical pool scoring, subminute selection, grading, and the forward-OOS regraders (was `eval/` before 2026-07-28) |
| `engine/` | Shared fee/PnL primitives and canonical `tick_exit004` real-tick execution engine |
| `scripts/` | Operational tick download, 400 ms replay, and historical parity |
| `audit/` | Read-only validation of the current subminute contract, models, training inputs, caches, and 400 ms report |
| `studies/` | The audit-pinned 400 ms report, the current throttle/sizing note, and the `tick_latency/runners/` drivers. The 47 dated verdict .md / digest .csv / report .json files were dropped 2026-07-31 (a6df8ed) and live in git history |
| `optimize_runs/` | **Gitignored heavies only** — option3 pool, scored truth, study DBs. Resolve it via `BT_RUNS_DIR`, never a literal path |
| `data/` | PMData snapshot features/outcomes and last-30-second tick books. Gitignored; resolve via `BT_DATA_DIR` |
| `cache/` | The Binance 1s kline cache. Gitignored; resolve via `BT_CACHE_DIR` |
| `tests/` | Engine, contract, and audit tests |

## Historical checkpoint rebuild chain

```text
$BT_DATA_DIR/pmdata/snapshot_features_full.parquet
$BT_DATA_DIR/pmdata/pmdata_outcomes.parquet
$BT_DATA_DIR/pmdata/poly_snapshot/          <- DELETED 2026-07-31, re-download first
$BT_CACHE_DIR/binance_1s
        |
        v
polymarket.backtest.training.build_pool
        |
        v
$BT_RUNS_DIR/option3_pool/pool_step1last_option3/
        |
        +--> polymarket.backtest.lib.pool_study
        |         |
        |         +--> scored_11set_truth.parquet
        |
        +--> polymarket.backtest.training.export_models
                  |
                  v
          data/models/checkpoint_v1/
```

The exporter writes all four MID/NL sleeve contracts into the checkpoint
manifest. A rerun does not require hand-editing the manifest.

The raw `poly_snapshot/` tree and the `book_depth_index_pmdata` depth cache
were deleted 2026-07-31 in the data consolidation — deliberate, no backup.
`book_depth_index_pmdata` is off the chain for good (the book-execution pool
variant cannot be rebuilt). `poly_snapshot/` is **not** off the chain: it is the
per-market book substrate `build_pool` walks for every decision/fill pair, so
the chain above is re-runnable only after a coordinated re-download
(`fetch/pmdata_download.py`, quota-metered — SOURCES.md §1). Since 2026-08-01
`training.pmdata_target.require_snapshot_dir()` is called at the entry of
`build_pool.build_grid_pool`, `checkpoint._build_pool_for_grid` and
`offset_wfo._attach_raw_ladders`, so a rebuild attempted without it raises
`FileNotFoundError` immediately instead of walking the universe and writing an
empty pool that reads as "no bets found". `audit/live_artifacts.py` grades the
surviving inputs only.

## Current TWAP research

```powershell
# Research inputs must be present under BT_DATA_DIR / BT_CACHE_DIR first.
.venv\Scripts\python.exe -m polymarket.backtest.studies.tick_latency.run_twap15m60_refit_aug13
.venv\Scripts\python.exe -m polymarket.backtest.studies.tick_latency.run_twap5m60_refit_only
.venv\Scripts\python.exe -m polymarket.backtest.training.export_twap_symmetric
```

Review the 15m exact-forward holdout before exporting a new model bundle. The
5m step is refit-only until an eligible 60-second forward period exists. The
exported artifact is `data/models/twap_symmetric_v1/`.

## Historical checkpoint commands

```powershell
# Rebuild/extend the option3 pool. PRECONDITION: poly_snapshot/ was deleted
# 2026-07-31 — re-download it (SOURCES.md §1) or this raises at entry.
.venv\Scripts\python.exe -m polymarket.backtest.training.build_pool

# Re-export the deployed 11-feature checkpoint models
.venv\Scripts\python.exe -m polymarket.backtest.training.export_models

# Verify config, feature schema, models, inputs, caches, and 400 ms evidence
.venv\Scripts\python.exe -m polymarket.backtest.audit.live_artifacts

# Download a last-30-second tick window for a JSON slug array
.venv\Scripts\python.exe -m polymarket.backtest.scripts.tick_download_last30 --slugs path\to\slugs.json

# Reproduce the retained 400 ms report (the audit-pinned foldBE_stop_report_400.json).
# --topup-max-n 0 disables the scale-in ladder; verified 2026-08-03 to reproduce
# the retired tick_union_exit004_foldBE driver exactly (8/8 fold x arm cells, 1e-9).
.venv\Scripts\python.exe -m polymarket.backtest.scripts.tick_topup_foldBE --latency 400 --policies hold exit004 --topup-max-n 0
```

MID-only / fold-F rerun recipe: the driver hardcodes both bands in
`build_union_attempts`, so a naive CLI call replays the UNION universe (fold B
n=7,386) — a different strategy, not a different setting. Use the checked-in
wrappers in `studies/tick_latency/runners/`: `run_midonly.py` (the MID-only
live twin, fold B n=5,842), `run_size.py` (run_size_mixed retired 2026-08-01,
mixed-stop study closed; the same twin plus two bet-size env knobs — `CB_F5M`
and `CB_F15M`, which override `tick_exit004.BASE_FRAC_5M` / `BASE_FRAC_15M`.
There is no stop-mask knob), and `run_foldf.py` (the UNION reference arm). Each
patches `tick_exit004.build_union_attempts` / `FOLDS` before delegating to
`scripts.tick_topup_foldBE`.

## Retained Inputs

Do not remove these during source cleanup:

- `data/pmdata/snapshot_features_full.parquet`: pmdata book-snapshot feature
  table; also the Gamma resolution fetcher's slug universe
- `data/pmdata/pmdata_outcomes.parquet`: canonical resolution labels
- `data/pmdata/poly_tick_last30.db`: real last-30-second tick books (SQLite tick
  store)
- `cache/binance_1s/`: spot input cache
- `optimize_runs/option3_pool/pool_step1last_option3/`: checkpoint training pool (under `BT_RUNS_DIR`)
- `optimize_runs/scored_11set_truth.parquet`: parity and tick candidate input (under `BT_RUNS_DIR`)
- `data/models/checkpoint_v1/`: deployed checkpoint artifacts and manifest

## Retained Output

Only the live-like 400 ms subminute evidence is retained:

- `studies/tick_latency/foldBE_stop_report_400.json`
- `optimize_runs/tick_latency/foldBE_stop_400.parquet` (local detailed audit trail, gitignored — under `BT_RUNS_DIR`)

The scored 11-feature artifact supplies both sides at every decision second,
so the replay dynamically re-scores the held side for `PURE_STOP_04`. Entry
FAK orders and full-size stop FOK orders execute against the real tick book
400 ms after their respective decisions. Each B-E chunk starts independently
at $100 and includes a separate HOLD control.

This replay cannot reconstruct queue priority, network jitter, competing
takers, balance-cache staleness, or the live multifill branch above $5,000.
It is near-live execution evidence, not a fill guarantee.

Historical experiment outputs remain available in git history. New committed
outputs must be direct inputs to the subminute strategy, current 400 ms
evidence, or a production contract.
