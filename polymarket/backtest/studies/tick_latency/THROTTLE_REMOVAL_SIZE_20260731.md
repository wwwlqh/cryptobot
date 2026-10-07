# DD-Throttle Removal + Resize to 1%/1% + Both-Hold (2026-07-31)

MID-only live twin, real-tick replay at 400 ms, folds B-F, every fold restarts
at $100. This is the study behind the 2026-07-31 operator order:

    throttle OFF (brake floor 1.0) - 1% of equity on BOTH intervals -
    stop-loss OFF on BOTH intervals - hard halt 0.40 -> 0.30

## Contract

- Universe: MID band only (NL purged 2026-07-29). Fold B n=5,842 attempts.
- Entry: higher-edge side, consume-on-fill retries, anchored +0.01 FAK.
- Top-ups: T3 (max_n 3, frac 0.5, cap 0.83, spread<=0.02, exposure cap 1.5).
- Tier A: mult 1.5 above edge 0.0465.
- Sweep arms run with `--brake-floor 1.0 --dd-halt 1.01` so the sizing and the
  stop policy are the only live variables.

## 1. The throttle is a recovery drag - but it was load-bearing at the old size

At the old sizing (1.25% 5m / 2.5% 15m) the brake buys drawdown, not edge:
edge is flat across dd depth, so throttling after a loss only slows the
recovery. But unbraked at that size the twin is uninsurable:

| Arm | brake floor / scale | B end / maxDD | C end / maxDD | D end / maxDD | E end / maxDD | F end / maxDD |
| --- | --- | --- | --- | --- | --- | --- |
| DEEP (union) | 0.25 / 0.30 | 3736 / 24.8 | 339700 / 23.5 | 5577 / 22.7 | 5278 / 19.7 | 280 / 25.4 |
| LIVE (MID) | 0.50 / 0.60 | 4236 / 28.1 | 297999 / 26.2 | 5111 / 31.5 | 5404 / 23.2 | 306 / 33.3 |
| MILD (MID) | 0.75 / 0.60 | 4420 / 29.5 | 299158 / 28.7 | 5201 / 34.3 | 5490 / 23.1 | 323 / 35.9 |
| OFF, no halt (MID) | 1.00 / -- | 5111 / 45.6 | 310694 / 34.3 | 4819 / **48.5** | 5748 / 27.7 | 335 / 40.6 |

Unbraked worst-fold maxDD is 48.5% (D; B 45.6%) - straight through a 40% halt.
`BRAKE_OFF` (floor 1.0 but halt still 0.40) confirms it: folds B and D halt
mid-replay (n collapses 5,892 -> 4,460 and 4,893 -> 2,126). So the throttle
could not simply be deleted; the size had to come down with it.

## 2. Size sweep (throttle off, halt disabled, both-stop)

End equity from $100 / maxDD %, per fold:

| 5m / 15m frac | B | C | D | E | F |
| --- | --- | --- | --- | --- | --- |
| 0.0047 / 0.0094 | 1318 / 17.7 | 43863 / 15.5 | 1587 / 13.6 | 1607 / 12.9 | 202 / 15.6 |
| 0.0047 / 0.0125 | 1684 / 20.9 | 57947 / 16.3 | 2132 / 17.1 | 2196 / 13.5 | 218 / 18.5 |
| 0.0063 / 0.0094 | 1596 / 17.7 | 70119 / 16.7 | 2109 / 12.6 | 2106 / 13.8 | 207 / 16.4 |
| 0.0063 / 0.0125 | 1861 / 20.9 | 92624 / 18.1 | 2640 / 15.7 | 2635 / 14.3 | 228 / 18.3 |
| 0.0078 / 0.0094 | 1869 / 18.5 | 107505 / 17.1 | 2771 / 13.9 | 2739 / 16.1 | 232 / 18.0 |
| 0.0094 / 0.0094 | 2147 / 20.0 | 144182 / 20.2 | 3410 / 15.6 | 3319 / 17.7 | 232 / 20.2 |
| 0.0094 / 0.0109 | 2230 / 21.1 | 156989 / 20.5 | 3591 / 17.9 | 3686 / 17.9 | 240 / 21.4 |
| **0.0100 / 0.0100** | **2300 / 21.1** | **165597 / 21.8** | **3558 / 18.3** | **3776 / 18.6** | **244 / 21.5** |
| 0.0094 / 0.0188 | 3378 / 28.1 | 196115 / 24.0 | 4471 / **30.1** | 5288 / 19.9 | 277 / **32.1** |
| 0.0125 / 0.0250 (old live) | 5111 / **45.6** | 310694 / 34.3 | 4819 / **48.5** | 5748 / 27.7 | 335 / **40.6** |

maxDD scales close to linearly in size while end equity scales super-linearly,
so the choice is purely a risk budget. 1%/1% is the largest flat size whose
worst fold stays around 21-22% with the stop on, and it removes the 5m/15m
size asymmetry that the 07-30 era-loss study blamed for amplifying the 15m
loss (flat-$1 lost $6.49 where live lost $66.25).

## 3. Stop-policy triangle at 0.010 / 0.010

Same universe, same size, throttle off, halt disabled - only the exit differs:

| Fold | both-stop end / maxDD | mixed (5m stop, 15m hold) | both-hold |
| --- | --- | --- | --- |
| B | 2300 / 21.1 | 1994 / 19.4 | 1216 / 24.6 |
| C | 165597 / 21.8 | 163696 / 23.7 | 150834 / 26.6 |
| D | 3558 / 18.3 | 3333 / 20.0 | 2484 / 19.8 |
| E | 3776 / 18.6 | 3256 / 19.5 | 1976 / 21.9 |
| F | 244 / 21.5 | 247 / 21.3 | 225 / 24.9 |

In backtest the stop is worth roughly +0.5 to +1.4 c/$ and improves maxDD -
consistent with the 07-29 no-stop twin. Both-hold costs ~2-5 points of maxDD
and gives up end equity in every fold except F.

## 4. Deploy decision (operator, 2026-07-31)

**Both-hold, 1% / 1%, brake floor 1.0 (throttle removed), hard halt 0.30.**

The backtest prefers the stop; live execution does not corroborate it. The
07-28 15m stop study measured stopped-15m positions that *would have won*
53.8% of the time versus 24.4% in backtest, and exec-vs-fill showed the ask
collapsing in flight - i.e. the simulated stop fills at a price the live book
does not honour, so the BT stop credit is not bankable. The operator accepts
the both-hold arm's cost as the price of not trading a fill model we distrust.

Halt 0.30 is chosen against the both-hold worst-fold maxDD of 26.6% (fold C):
above every measured drawdown, so the halt never fires in this backtest, but
tight enough to be a real circuit breaker at the new size.

Note the fail-closed trio: `MAX_DRAWDOWN_HALT_FRAC` in
`polymarket/src/cryptobot/config.py`, `max_drawdown_halt_frac` in
`polymarket/live/config/v2_step1last_live.json`, and
`CRYPTOBOT_MAX_DD_HALT_FRAC` pinned by `polymarket/infra/install_remote.sh`
must all read 0.30 or the portfolio loader refuses to boot.

## 5. Saved output

> **HISTORICAL (annotated 2026-08-01).** The run set below was pruned: **none**
> of these `SIZE_*` jsons are tracked any more. The only json still in git under
> `studies/tick_latency/` is the audit-pinned `foldBE_stop_report_400.json`.
> Read the list as a record of what the study produced, not as files you can
> open — recover them from git history if needed. `runners/` still holds
> `run_midonly.py`, `run_size.py` and `run_foldf.py`; `run_size_mixed.py` is
> gone (retired 2026-08-01 with the mixed-stop study).

Committed here at the time of writing (machine-readable, 1%/1% arms of the
triangle):

- `foldBE_stop_report_400_SIZE_5M0100_15M0100.json` (+ `_F`) - both-stop
- `foldBE_stop_report_400_SIZE_HOLD_5M0100_15M0100.json` (+ `_F`) - both-hold,
  the deployed config's twin
- `foldBE_stop_report_400_SIZE_MIXED_5M0100_15M0100.json` (+ `_F`) - the
  5m-stop / 15m-hold twin of the 07-28 live config, for the record
- `runners/` - the wrappers that produced them

Per-fill `.parquet` detail is deliberately not committed.

## 6. Repro traps

- **The band wrapper is mandatory.** Calling
  `polymarket.backtest.scripts.tick_topup_foldBE` directly replays the UNION
  universe (fold B n=7,386), not the MID-only live twin (n=5,842). Always go
  through `runners/run_size*.py` / `run_midonly.py`.
- **Fold F needs its own score path:**
  `polymarket/backtest/optimize_runs/scored_foldEF_20260721.parquet`. B-E use
  `scored_11set_truth.parquet`.
- **The tick corpus drifts.** This study saw 46,187 tick files; the frozen
  2026-07-21 digests were built on 40,517. Numbers from those older reports
  are *not* reproducible against today's corpus - compare arms only within a
  single sweep.
- Sweep arms ran `--brake-scale 0.6 --brake-floor 1.0 --dd-halt 1.01`; the
  halt is deliberately disabled in the sweep so drawdown is measured, not
  truncated. The deployed halt is 0.30.

## 7. Rerun

> **NOT RUNNABLE — preserved for provenance only (annotated 2026-08-01).** The
> entry point below, `runners/run_size_mixed.py`, was **deleted 2026-08-01**
> when the mixed-stop study closed; this block records the exact invocation the
> reported numbers came from and will fail as written. The surviving wrappers
> are `run_size.py` (both-stop), `run_midonly.py` and `run_foldf.py`, and they
> expose no `CB_MASK` knob — only `CB_F5M` / `CB_F15M`. Reconstruct the mixed
> arm from git history if it ever has to be reproduced.

    export PYTHONPATH=<repo>
    .venv/Scripts/python.exe polymarket/backtest/studies/tick_latency/runners/run_size_mixed.py \
      --latency 400 --start-capital 100 \
      --tier-a-mult 1.5 --tier-a-edge 0.0465 --nl15-mult 0.0 \
      --stop-chase 0.1 --stop-attempts 3 \
      --brake-scale 0.6 --brake-floor 1.0 --dd-halt 1.01 \
      --topup-max-n 3 --topup-frac 0.5 --topup-no-dip 0.0 --topup-cap 0.83 \
      --topup-spread-max 0.02 --topup-cadence 1 --topup-exposure-cap 1.5 \
      --folds B C D E --score-path <repo>/polymarket/backtest/optimize_runs/scored_11set_truth.parquet \
      --tag SIZE_HOLD_5M0100_15M0100

with `CB_F5M=0.010 CB_F15M=0.010 CB_MASK=all` for the both-hold arm
(`CB_MASK` unset for mixed; `run_size.py` for both-stop).
