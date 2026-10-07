# Polymarket backtest — data provenance manifest

Every substrate the Polymarket research tree reads: what it provides, who pulls
it, where the bytes physically live today, and the traps that have each produced
a wrong answer at least once.

**The rule that governs all of it:** a venue is backtestable at fill-model
fidelity only from its own recorder onward. Third-party snapshot history is
enough for EDA and coarse grading; it is never go-live evidence on its own.

---

## Where the bytes actually are (read this before writing a path)

The 2026-07-28 venue-tree restructure moved tracked **code** into
`polymarket/backtest/`. It deliberately did **not** move the ~20 GB of
gitignored data: running research sessions depend on literal paths under
`backtest/data/server_snapshot/`, and a 16 GB relocation buys nothing. So every
data dir resolves **legacy-first**:

| Env var | Contents | Resolution order |
| --- | --- | --- |
| `BT_DATA_DIR` | pmdata pulls, `server_snapshot/` (`recorder_snapshot/` deleted 2026-08-01, see §2) | env → `<repo>/backtest/data` if it exists → `<repo>/polymarket/backtest/data` |
| `BT_CACHE_DIR` | `binance_1s/` — the only thing left under it | env → `<repo>/backtest/cache` → `<repo>/polymarket/backtest/cache` |
| `BT_RUNS_DIR` | `option3_pool/`, `scored_11set_truth.parquet`, study DBs — heavy regenerable optimizer output | env → `<repo>/backtest/optimize_runs` → `<repo>/polymarket/backtest/optimize_runs` |

**2026-07-31 (exp044 phase 3).** There is no `NX_FEATURES_DIR` key any more —
`polymarket/backtest/__init__.py` no longer defines or exports it. Deleted from
disk in the same pass, with no backups and no rebuild path: `poly_snapshot/`,
`decision_table.parquet`, `rf_v1.joblib` + its manifest, and
`book_depth_index_pmdata/`. Bytes still sitting in `backtest/features/nx_v1/`,
`data/prices.db` and `data/models/hgb_v1.joblib` are **orphaned** — no producer
and no reader. Do not build anything on them, and do not try to regenerate what
was deleted; the deletions were deliberate.

Import them, never hardcode:

```python
from polymarket.backtest import BT_CACHE_DIR, BT_DATA_DIR, BT_RUNS_DIR, RESOLVED_DIRS

print(RESOLVED_DIRS)   # {'BT_DATA_DIR': (WindowsPath(...), 'legacy'), ...}
```

⚠️ **Never commit a directory under any `data/`, `cache/`, or `optimize_runs/`
path** — not even a `.gitkeep`. An empty NEW dir in git would win the
existence check on a fresh pull and silently shadow a populated legacy tree.

`artifacts/` — the ship-to-live boundary — resolves the same way, on its own
key, but tier 2 checks for the **file** rather than the directory:

| Env var | Contents | Resolution order |
| --- | --- | --- |
| `CRYPTOBOT_PM_BT_ARTIFACTS` | nothing, since 2026-07-31 (exp044 phase 3). `decision_table.parquet` was deleted from disk with its whole producer chain; `config.DECISION_TABLE_PATH` is gone too | env → `$BT_DATA_DIR` **if `decision_table.parquet` is already there** — it never is any more → `<repo>/polymarket/backtest/artifacts` |

Tier 2 keys on that file, so with the file deleted the key now always resolves
to `artifacts/` unless the env var is set. `training/portfolio.py` keeps a
`DECISION_TABLE` constant solely so `_load_full_table` can name the missing file
in a loud `FileNotFoundError` — it fails rather than returning an empty frame,
because "the substrate is gone" must never read as "the strategy found no bets".
The dir itself is tracked (its `README.md` is), and its heavy outputs are
gitignored; the small `*_manifest.json` / provenance files beside them stay
tracked so a checkout records what shipped.

`REPO_ROOT` honors `CRYPTOBOT_ROOT` first, matching `cryptobot.config` — one
env var moves both packages' idea of the root, so a research writer and the
live reader can never end up in different trees.

Tracked, so it follows the code with no fallback: `studies/` (dated verdicts +
retained reports), and the live contract at
`polymarket/live/config/v2_step1last_live.json`.

---

## Sources

### 1. pmdata.dev — third-party Polymarket book history

| | |
| --- | --- |
| Provides | Historical order-book snapshots (`snapshot_features*.parquet`), resolutions (`pmdata_outcomes.parquet`), raw L2, and per-tick last-30s books (`poly_tick_last30.db`, ~1.8 GB) |
| Puller | `polymarket/backtest/fetch/{pmdata_download,pmdata_raw_pull}.py`, `polymarket/backtest/scripts/tick_download_last30.py` — three pullers, three different products; see below |
| Lands in | `$BT_DATA_DIR/pmdata/` — `snapshot_features{,_full}.parquet`, `pmdata_outcomes.parquet`, `gamma_resolutions/`, `poly_l2_raw/`, `poly_tick_last30.db` |
| Cadence | Manual, quota-metered (`PMDATA_API_KEY` in `.env`) |
| Cleaner | `polymarket/backtest/lib/pmdata_clean.py` is the canonical QA/cleaning path — degenerate books (ask ≤ 0, ask ≥ 1, crossed by more than 2¢) and `status == 'ambiguous'` rows are excluded there, once, for everyone |

**Which puller when.** `pmdata_download.py` is the BULK 1 Hz builder: it pulls
the `poly_l2` endpoint, resamples to one book/second, derives `winning_outcome`
from terminal convergence, and fills `poly_snapshot/` behind `pmdata_download.db`
(`--backfill-outcome` re-derives labels in place — `pmdata_clean.py` names it).
`pmdata_raw_pull.py` is the TICK archive: same endpoint, kept byte-for-byte
(`price_change` deltas, periodic full ladders, the `market_resolved` row), window
list read from the pulled server DB so it downloads only markets the bot actually
traded or evaluated, into `poly_l2_raw/`. Neither replaces the other — one is the
wide 1 Hz substrate, the other is the fill-accurate replay substrate — and they
share no output path. `tick_download_last30.py` is the TICKS entry: it trims
`poly_l2` to the final 30 s + 5 s lead into `poly_tick_last30.db`, which is what
`engine/tick_exit004.py` reads; it lives under `scripts/`, not `fetch/`, and its
pinned form is `python -m polymarket.backtest.scripts.tick_download_last30`.
(A fourth puller, `fetch_pmdata.py`, pulled the ready-made `poly_snapshot`
endpoint into the SAME path as `pmdata_download.py` but wrote the served bytes
verbatim — the old-schema drift `--backfill-outcome` had to repair. Retired
2026-08-01, phase 6.)

**`poly_snapshot/` is GONE (2026-07-31).** The 1 Hz tree those bulk pullers
wrote was deleted from disk in the same consolidation that collapsed the tick
parquets into one DB. `snapshot_features_full.parquet` — the wide frame
`training/offset_wfo` and the pool build actually read — is its **frozen
residue**: derived before the delete and unrefreshable, because its source
dataset no longer exists. `platform/checks/substrate_freshness` therefore grades
it on presence only, never on age. The pullers still work against pmdata.dev —
and they are now the **precondition of the rebuild chain below**, not an
optional extra: `poly_snapshot/` is the per-market book tree
`training/build_pool` walks for every decision/fill pair, so nothing downstream
of `build_pool` can be regenerated until the tree is re-downloaded
(`pmdata_download.py`, quota-metered, a coordinated multi-hour pull).

Since 2026-08-01 that is enforced rather than documented.
`training/pmdata_target.require_snapshot_dir()` raises a dated
`FileNotFoundError` and is called ONCE at the entry of the three chain entries
that funnel into the deleted tree — `training/build_pool.build_grid_pool`,
`training/checkpoint._build_pool_for_grid` and
`training/offset_wfo._attach_raw_ladders`. Before that guard existed, every
reader below them (`_load_raw_snapshot`, `_load_top_book_series`) returned
`None` per market and the builders wrote a **zero-row pool** — a silent empty
artifact indistinguishable from "the strategy found no bets". The guard also
rejects a merely EMPTY `poly_snapshot/`, so a half-finished re-download cannot
resurrect the same failure. `checkpoint`'s default
`--grids baseline60 --baseline-mode reference` path never reaches a book and
still runs with no substrate at all.

**The tick substrate is ONE SQLite file since 2026-07-31.** 61,354 per-slug
parquet files became `pmdata/poly_tick_last30.db`, table
`ticks(slug PRIMARY KEY, n_rows, data BLOB)` where `data` is that slug's frame
serialized as zstd parquet bytes — so the list-valued book columns round-trip
byte-exactly and readers still pay one columnar decode per market. All access
goes through `polymarket/backtest/lib/tick_store.py` (`open_store`, `has_slug`,
`list_slugs`, `read_ticks`, `write_ticks`); `engine/tick_exit004.py` exposes
`load_tick_slug(slug, store)`. The old flat directory was **deleted 2026-07-31**
once the migration signed off — the DB is the only copy. The migrator and its
`--verify` re-proof were **deleted 2026-08-01**: they lived briefly inside the
store module, but with the source tree gone every entry point could only fail on
a missing directory. `tick_store.py` is now the store and nothing else — no CLI,
no `migrate_parquet_dir`, no `write_blob`. Recover the walk from that commit's
parent if a future consolidation needs it.

Readers of the tick DB, all DB-native and all still runnable:
`scripts/tick_topup_foldBE.py` — the single replay driver, canonical 400 ms
replay at `--topup-max-n 0` (it absorbed `tick_union_exit004_foldBE.py`, retired
2026-08-03; the shared scaffolding lives on in `scripts/tick_replay_base.py`) —
(+ the `studies/tick_latency/runners/` wrappers
`run_midonly.py` / `run_size.py` / `run_foldf.py`), and
`lib/nb_common.py::tick_slug_index` (the index-scan slug census, zero blob
reads). `scripts/tick_download_last30.py` is the writer side of the same store
(`has_slug` / `write_ticks`). `scripts/tick_attempt_panel.py` — the
every-qualifying-second fill-truth panel builder — was **deleted 2026-08-01**
with its closed study; it was a hand-run CLI with no in-repo importer, and its
own consumer (`tick_scalein_grid.py`) had already been archived in f4894bc.
Recover it from git history if the attempt-rank / multi-entry panel is ever
needed again.

**Traps.** The `last30_book` loader once served the **terminal** book instead of
the decision-instant one (pandas silently kept the datetime *unit*): a
look-ahead bug that moved BT_REALBOOK from +3.06 to −0.24 c/$1. 21 scripts ran
before the fix. Re-derive any ladder number that predates it. Snapshot rows
after resolution and derived (non-Gamma) labels have each also produced a wrong
verdict — join labels from `pmdata_outcomes.parquet`, never reconstruct them.

### 2. Own book recorder — ⛔ RETIRED, ALL HISTORY DESTROYED

**Do not plan work against this source.** Nothing survives of it.

| | |
| --- | --- |
| Provided | Per-second books in byte-identical pmdata layout, `poly_snapshot/{coin}/{5m,15m}/` on the box — the only history of the real NO-side Polymarket books |
| Producer | `polymarket/live/src/cryptobot_polymarket/book_recorder.py` + `polymarket/recorder/service/book-recorder.service`. ⛔ Retired 2026-08-02 (flag pinned 0); ⛔ **CODE DELETED 2026-08-04** — module, unit, tests, the shared `cryptobot.venues.recorder_util` helper and the whole `CRYPTOBOT_BOOK_RECORDER_*` env contract are out of the repo, and `install_remote.sh` tears the unit down on both roles |
| Puller | ⛔ Deleted 2026-08-03 (`pull/pull_book_recorder.sh`, `pull/ingest_recorder_pull.py`, `lib/recorder_store.py`) |
| Local store | ⛔ `data/recorder_snapshot/` deleted 2026-08-01 10:02 UTC. Still in the Windows Recycle Bin (2.2 GB, `recorder_snapshot.db` + `COVERAGE.md`); operator confirmed 2026-08-03 it is not wanted back |
| Remote history | ⛔ Deleted 2026-08-02 on operator order — poly recorder 4.2 GB + PF recorder 3.8 GB + all of `~/backups` |

**The trap this source became.** It was **irreplaceable append-only history**
needing ~3-4 weeks of continuous capture to be usable, and every copy was
deleted inside three days. Re-arming the recorder starts a new series from zero;
no book-parity or NO-side study can be run against the past. Recover the reader
and pull chain with `git show <this commit>^:polymarket/backtest/lib/recorder_store.py`
— but they will find no data.

### 3. Gamma API — discovery and settlement truth

| | |
| --- | --- |
| Provides | Market discovery, `feeRateBps`, resolved outcomes (the **canonical Chainlink-sourced label**) |
| Puller | `polymarket/backtest/fetch/fetch_gamma_resolutions.py` → `$BT_DATA_DIR/pmdata/gamma_resolutions/`, assembled into `pmdata_outcomes.parquet` |
| Consumed by | `training/offset_wfo` (the WFO arbiter's label), `lib/pmdata_clean.load_outcomes`, `training/build_pool._load_ok_outcomes`, `training/checkpoint._load_outcomes` |

**Trap.** Only `status == 'ok'` rows carry a definitive outcome; everything
else must surface as `pd.NA` so a downstream `dropna` filters it out instead of
inflating denominators with zero-hit rows. The owner is now
`lib/pmdata_clean.load_outcomes(..., ok_only=False)`, which keeps every row with
`won_up` as float (NaN where ambiguous). Producer invariant that makes this
safe: `fetch_gamma_resolutions.assemble_outcomes` — the SOLE writer of this
parquet — derives `status` FROM the label (`out["won_up"].notna().map({True:
'ok', False: 'ambiguous'})`), so `status != 'ok'` ⇔ `won_up` is NaN, always.

### 4. Binance — spot truth for features and labels

| | |
| --- | --- |
| Provides | The 1s spot cache behind the sub-minute pool's signal, strike and `y` |
| Puller | **none in repo.** Bulk download from `data.binance.vision` — see the `reference_history_sources` memory for what is reachable from this machine |
| Lands in | `$BT_CACHE_DIR/binance_1s/{btc,eth,sol,xrp}_1s.parquet` |
| Consumed by | `training/offset_wfo` (`BINANCE_1S_DIR`, `_load_1s`) — the ONLY reader. `_load_1s` carries an `@lru_cache(maxsize=8)` at its definition since 2026-08-01; it used to be an import-time monkeypatch in `training/offset_validation`, which made the cache conditional on import order |

**No in-repo fetcher, as of 2026-07-31 (exp044 phase 3).** The kline pullers that
wrote `data/prices.db` (`fetch/history.py`, `fetch/gaps.py`) were deleted with
the nx_v1 → rf_v1 → decision_table chain they fed; `fetch/__init__.py` records
this. There was never a `training/binance.py` — earlier revisions of this file
named one, and it did not exist. The 1s parquets are hand-placed dumps.

### 5. RTDS recorder — RETIRED 2026-08-01 (dataset GONE)

**RTDS recorder retired 2026-08-01, prereg abandoned ungraded by operator
order.** The Binance + Chainlink spot tick tape is no longer a substrate: the
recorder module, its systemd unit, the `rtds_lead_lag.py` grader, the local
`recorder_snapshot/rtds_spot/` parquets (66 hourly files, ~29 MB) and the
`data/rtds_recorder` deploy prune-exclusion were all deleted. On-box collection
is stopped and the next deploy reclaims the directory.

This was a **deliberate override of an open pre-registration**, not a graded
kill: `RTDS_ORACLE_STUDY_PREREG.md`'s kill rule was not evaluable until
~2026-08-06 and no verdict was ever taken. Nothing about oracle lead/lag or
would-be TWAP settlement has been decided — the question is simply unasked.
Recover the study text with
`git show a6df8ed^:polymarket/backtest/studies/RTDS_ORACLE_STUDY_PREREG.md` and
the code from the retirement commit's parent.

### 6. Server ledger snapshots — the live side of parity

| | |
| --- | --- |
| Provides | Live `bets` / `live_order_attempts` / `signals` / `pipeline_health` for live-vs-backtest grading |
| Puller | ⛔ `pull_live_db.sh` DELETED 2026-08-04 (operator order; recover from git history). Manual pull: sqlite3 `.backup` on the box → gzip → scp to `data/server_snapshot/cb_export_latest.db` (repo-root `data/`, gitignored) |
| Consumed by | `polymarket/backtest/lib/forward_oos_regrade.py` — the arbiter, and now the ONLY consumer. `lib/forward_oos_live_selection.py` was deleted 2026-08-01: it selected on the `exp044_live` flat gate and the DRY `WOULD_BET` picks, both retired with exp044. |

**Traps — each of these has produced a fully wrong published number.**

- **Population mismatch.** Only two pairings are legal: live-**attempts** ↔
  `BT_ATASK`, and live-**fills** ↔ `BT_REALBOOK`. Comparing live fills against a
  chunk pool manufactures the adverse-fill-selection gap as if it were alpha.
- **Fill-model omission.** Any grading that skips `engine/tick_exit004`'s
  400 ms FAK re-read is INVALID: 48.3% of gated cells die at that re-read, and
  keeping them invented a "+1.65pt calibrated edge" that does not exist.
- **Scoring-path shortcut.** Live scores z → **checkpoint** RF →
  `build_feature_vector` → **checkpoint** HGB (`strategy/score.score_cell_hgb`,
  the only scorer left). RF-only scoring matches live's logged `model_p` on
  **0.0%** of cells. Always validate against live's own logged `model_p`. Naming
  trap: the standalone `rf_v1`/`hgb_v1` joblibs were retired 2026-07-31 and
  `rf_v1` is deleted — the per-checkpoint forests inside
  `data/models/checkpoint_v1/` are different artifacts and are still live.
- **Config-era smearing.** "PRE" spans six different bots. Match on the config
  sha (plus the sub-minute flag) before any cross-era claim, and read absolute
  margins rather than deltas.
- **Ledger quirks.** `ledger.fill_price` is the QUOTED price, not the executed
  one; `ledger.side` is YES/NO (matching on `'UP'` inverts the labels); fees are
  booked on wins only, so era truth is the flat-moment balance; `signals` logs
  only the higher-edge side (≈43% held-side coverage). Era boundaries are
  recovered from `pipeline_health` boot rows.

---

## Rebuild chain

```text
$BT_DATA_DIR/pmdata/{snapshot_features_full,pmdata_outcomes}.parquet
$BT_DATA_DIR/pmdata/poly_snapshot/{coin}/{5m,15m}/{slug}.parquet   <- DELETED, re-download first
$BT_CACHE_DIR/binance_1s/{coin}_1s.parquet
        |
        v  polymarket.backtest.training.build_pool
$BT_RUNS_DIR/option3_pool/pool_step1last_option3/
        |
        +--> polymarket.backtest.lib.pool_study  --> scored_11set_truth.parquet
        +--> polymarket.backtest.training.export_models --> data/models/checkpoint_v1/
                                                            |
                                          polymarket/infra/deploy.sh --> runtime/
```

Since 2026-07-31 (exp044 phase 3) `checkpoint_v1` is the ONLY leg that ships.
The `nx_v1` → `rf_v1` → `decision_table.parquet` branch that used to hang off
this chain has no producer, no artifact and no live consumer left, and the
`snapshot_features_full.parquet` head is frozen (see §1) — so this is a
description of how the shipped model was built, not a chain you can re-run
end-to-end today. **`build_pool` is hard-blocked** until `poly_snapshot/` is
re-downloaded: `require_snapshot_dir()` fires at its entry (§1). Treat the
re-download as the first step of any rebuild, not a detail.

`polymarket/backtest/audit/live_artifacts.py` validates the whole chain
read-only and is the fastest way to see which input is missing or stale.
