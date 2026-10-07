# kalshi/infra/ — empty (region undecided)

`deploy.sh` / `install_remote.sh` clones plus box facts, once a region is chosen.
Model them on `polymarket/infra/`, which is the proven pattern: a staged tar
allowlist (never rsync of the whole tree), tests stripped, and a remote wipe step
guarded by `! -name` whitelists.

## Region: TBD

Candidates `us-east-1` / `us-east-2`. Before committing a box, verify:

- account / KYC status allows API trading,
- API rate limits fit a per-second decision grid,
- **orders placed from an AWS IP actually succeed.**

⚠️ **HARD RULE: place ONE REAL order from each candidate region before
committing any box to it.** This exists because eu-west-2 (London) turned out to
be geoblocked for Polymarket **ORDERS** while every read endpoint passed — a
migration that looked validated by reads would have silently stopped trading.

## Carry forward, non-negotiably

- The **data-wipe whitelist**, with its incident comments verbatim. The top-level
  wipe must spare `.venv` and `data`; the `data/` wipe must spare the venue DB(s)
  and every recorder dir. Any new runtime dir gets its `! -name` line in the same
  change that creates it.
- A **build stamp** (git sha, tree version, config sha) written at deploy and
  recorded in `pipeline_health` at boot, so era splits become a join instead of
  archaeology.
- A **branch / dirty-tree check** before staging, and a `--dry-run` that prints
  the staged file list.

Cost note: roughly $15-35/mo per box. A recorder-only first phase fits a
`t4g.small`; an engine box wants ~4 GB (the Polymarket unit pins
`MemoryMax=3300M`).
