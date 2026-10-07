"""MID-only + fold-F twin runner with patchable BASE_FRAC sizing knobs.

run_midonly.py plus two env overrides for the bet-size sweep, including its
--late-skip 5 default. Same wrapper
caveat: a naive CLI call to the tick_topup_foldBE driver replays the UNION
universe (fold B n=7,386), not the MID-only live twin (fold B n=5,842).

Env:
  CB_F5M   -> tick_exit004.BASE_FRAC_5M
  CB_F15M  -> tick_exit004.BASE_FRAC_15M

Writes foldBE_stop_report_400_SIZE_<tag>.json (both-stop arm) into
studies/tick_latency/. None of those SIZE_* reports are tracked any more — they
were dropped 2026-07-31 (a6df8ed) and only the audit-pinned
foldBE_stop_report_400.json remains in git; rerun to regenerate, or read the
retired numbers out of git history / THROTTLE_REMOVAL_SIZE_20260731.md.
"""
import os
import sys

import polymarket.backtest.engine.tick_exit004 as te

_orig = te.build_union_attempts


def _midonly(scored):
    out = _orig(scored)
    return out[out["band"] == "MID"].copy()


te.build_union_attempts = _midonly
te.FOLDS = ("B", "C", "D", "E", "F")

if os.environ.get("CB_F5M"):
    te.BASE_FRAC_5M = float(os.environ["CB_F5M"])
if os.environ.get("CB_F15M"):
    te.BASE_FRAC_15M = float(os.environ["CB_F15M"])

import polymarket.backtest.scripts.tick_topup_foldBE as drv  # noqa: E402
drv.te.build_union_attempts = _midonly
drv.te.FOLDS = te.FOLDS

# matches live CRYPTOBOT_LATE_WINDOW_SKIP=*:5 armed 2026-08-01, and keeps this
# runner an actual superset of run_midonly.py -- it previously inherited the base
# driver's 0 and so was NOT the live twin its docstring claimed.
DEFAULT_LATE_SKIP = "5"


def _with_default_late_skip(argv: list[str]) -> list[str]:
    """Prepend the twin's late-skip default unless the caller set one.

    Pass `--late-skip 0` to reproduce the pre-2026-08-03 SIZE_* runs behind
    THROTTLE_REMOVAL_SIZE_20260731.md, which were made before this default.
    """
    if any(a == "--late-skip" or a.startswith("--late-skip=") for a in argv):
        return argv
    return ["--late-skip", DEFAULT_LATE_SKIP, *argv]


if __name__ == "__main__":
    print(
        f"[size] BASE_FRAC_5M={te.BASE_FRAC_5M} BASE_FRAC_15M={te.BASE_FRAC_15M}",
        flush=True,
    )
    raise SystemExit(drv.main(_with_default_late_skip(sys.argv[1:])))
