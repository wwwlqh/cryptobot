"""MID-only / fold-F twin runner (recipe: polymarket/backtest/README.md ~L105).

Wraps tick_exit004.build_union_attempts to keep only band == "MID" and extends
FOLDS with "F", then delegates to the tick_topup_foldBE driver's main().

WHY THE WRAPPER IS MANDATORY
    The live bot is MID-only (the NL band was purged 2026-07-29). Invoking
    ``polymarket.backtest.scripts.tick_topup_foldBE`` from the CLI *without*
    this wrapper replays the UNION universe -- fold B n=7,386 -- which is NOT
    the live twin. The MID-only twin is fold B n=5,842. Any number quoted from
    a naive CLI run is a different strategy, not a different setting.

Produced (with --brake-floor 1.0 --dd-halt 1.01): the BRAKE_* sweep behind
THROTTLE_REMOVAL_SIZE_20260731.md.

LATE-WINDOW SKIP
    This twin defaults to ``--late-skip 5`` because it is the *live* twin and
    live now drops those seconds. The base driver's default stays 0, so any
    historical rerun through ``tick_topup_foldBE`` still reproduces. Pass an
    explicit ``--late-skip N`` (including 0) to override.
"""
import sys

import polymarket.backtest.engine.tick_exit004 as te

_orig = te.build_union_attempts


def _midonly(scored):
    out = _orig(scored)
    return out[out["band"] == "MID"].copy()


te.build_union_attempts = _midonly
te.FOLDS = ("B", "C", "D", "E", "F")

import polymarket.backtest.scripts.tick_topup_foldBE as drv  # noqa: E402
drv.te.build_union_attempts = _midonly
drv.te.FOLDS = te.FOLDS

# matches live CRYPTOBOT_LATE_WINDOW_SKIP=*:5 armed 2026-08-01
DEFAULT_LATE_SKIP = "5"


def _with_default_late_skip(argv: list[str]) -> list[str]:
    """Prepend the twin's late-skip default unless the caller set one."""
    if any(a == "--late-skip" or a.startswith("--late-skip=") for a in argv):
        return argv
    return ["--late-skip", DEFAULT_LATE_SKIP, *argv]


if __name__ == "__main__":
    raise SystemExit(drv.main(_with_default_late_skip(sys.argv[1:])))
