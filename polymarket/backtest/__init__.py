"""Backtest package root: repo paths and data-directory resolution.

Every backtest module gets its data, model and config locations from here
rather than from ``Path(__file__).parents[N]`` arithmetic, which breaks when a
module moves::

    from polymarket.backtest import REPO_ROOT, BACKTEST_DIR, BT_DATA_DIR

Subpackages: ``engine/`` (P&L and fee primitives, tick replay), ``lib/`` (the
canonical evaluator, pmdata cleaning, the tick store), ``training/`` (pool
building, walk-forward validation, model export), ``fetch/`` (raw data to
parquet), ``scripts/`` (research and ops CLIs), ``studies/`` (study reports and
their drivers) and ``tests/``. They import each other by full name::

    from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA

The repo root must be on ``sys.path``: pytest adds it via ``pythonpath =
["polymarket/src", "."]`` in pyproject, and command-line use is ``python -m
polymarket.backtest.<subpackage>.<module>`` from the repo root.

Data directories
---------------
Research data (~20 GB) is gitignored and may live in an older
``<repo>/backtest/{data,cache,optimize_runs}`` tree. Each directory resolves
as:

    1. explicit env override   (BT_DATA_DIR / BT_CACHE_DIR / BT_RUNS_DIR)
    2. the old path            (``<repo>/backtest/...``) if it exists
    3. the new path            (``<repo>/polymarket/backtest/...``)

so an existing checkout keeps reading its data in place and a fresh clone uses
the new layout. For the same reason no ``data/`` directory is committed (not
even a ``.gitkeep``): an empty new directory would take precedence over a
populated old one. Writers create these directories themselves.

``ARTIFACTS_DIR`` resolves the same way with its own env key,
``CRYPTOBOT_PM_BT_ARTIFACTS`` (see ``_resolve_artifacts_dir``).

``REPO_ROOT`` honours ``CRYPTOBOT_ROOT`` first, like ``cryptobot.config``, so
research code and the live bot always agree on the root.
"""

from __future__ import annotations

import os
from pathlib import Path

# This file is polymarket/backtest/__init__.py, so these are fixed:
#  BACKTEST_DIR             -> polymarket/backtest/
#  BACKTEST_DIR.parents[0]  -> polymarket/  (legacy-path-ok)
#  BACKTEST_DIR.parents[1]  -> repo root  (legacy-path-ok)
BACKTEST_DIR = Path(__file__).resolve().parent


def _resolve_repo_root() -> Path:
    """``CRYPTOBOT_ROOT`` if set, else the parents[] anchor.

    Matches the first step of ``cryptobot.config._resolve_repo_root`` on
    purpose: research code writes artifacts the live bot reads (e.g.
    ``data/models/twap_symmetric_v1/``), so both must resolve the same tree
    from the same env var. The marker-directory search is not needed here
    because this file's position relative to the root never changes.
    """
    raw = os.environ.get("CRYPTOBOT_ROOT")
    if raw:
        return Path(raw).resolve()
    return BACKTEST_DIR.parents[1]  # legacy-path-ok: THE shim anchor; this file never moves


REPO_ROOT = _resolve_repo_root()

# Pre-restructure physical home of the gitignored research data (still the real
# location in every existing checkout).
LEGACY_BACKTEST_DIR = REPO_ROOT / "backtest"  # legacy-path-ok: names the pre-restructure tree on purpose

# Records what each shim key resolved to, and why ("env" / "legacy" / "new").
# Purely observational — import it from a notebook or a health check to see
# which tree a process is actually reading.
RESOLVED_DIRS: dict[str, tuple[Path, str]] = {}


def legacy_first(root: Path | str, relative: str) -> Path:
    """``<root>/backtest/<relative>`` if it exists, else the new-layout path.

    Takes ``root`` explicitly so tests can point the rule at a synthetic tree.
    Module code should use the ``BT_*`` constants below, which are built on it.
    """
    legacy = Path(root) / "backtest" / relative
    if legacy.exists():
        return legacy
    return Path(root) / "polymarket" / "backtest" / relative


def _resolve_dir(env_key: str, relative: str) -> Path:
    """Resolve one data dir: env override -> legacy-if-exists -> new default."""
    raw = os.environ.get(env_key)
    if raw:
        chosen, why = Path(raw), "env"
    else:
        chosen = legacy_first(REPO_ROOT, relative)
        why = "legacy" if chosen.is_relative_to(LEGACY_BACKTEST_DIR) else "new"
    RESOLVED_DIRS[env_key] = (chosen, why)
    return chosen


# Bulk substrate: the pmdata pull (snapshot features + outcomes + the last-30s
# tick DB), the recorder store, server snapshots. Gitignored.
BT_DATA_DIR = _resolve_dir("BT_DATA_DIR", "data")
# Derived caches (gitignored), e.g. the Binance 1-second kline cache
# (``binance_1s/``, read by ``training.offset_wfo``).
BT_CACHE_DIR = _resolve_dir("BT_CACHE_DIR", "cache")
# Heavy WFO/optimizer artifacts: scored_11set_truth.parquet, option3_pool/,
# study DBs. Gitignored. NOTE: the *tracked* verdict .md / report .json files
# that used to sit in optimize_runs/ moved to ``studies/`` (see STUDIES_DIR) —
# this key is for the heavy regenerable outputs only.
BT_RUNS_DIR = _resolve_dir("BT_RUNS_DIR", "optimize_runs")


# Tracked, so this follows the code and needs no legacy fallback.
STUDIES_DIR = BACKTEST_DIR / "studies"


def _resolve_artifacts_dir() -> Path:
    """Where research artifacts are written for the live bot.

      1. ``CRYPTOBOT_PM_BT_ARTIFACTS`` if set;
      2. ``BT_DATA_DIR`` if an old checkout still has
         ``decision_table.parquet`` there;
      3. otherwise ``BACKTEST_DIR/"artifacts"``.

    Step 2 checks for the file rather than the directory because BT_DATA_DIR
    always resolves to something; only the file shows an old artifact tree is
    in use.
    """
    raw = os.environ.get("CRYPTOBOT_PM_BT_ARTIFACTS")
    if raw:
        chosen, why = Path(raw), "env"
    elif (BT_DATA_DIR / "decision_table.parquet").exists():
        chosen, why = BT_DATA_DIR, "legacy"
    else:
        chosen, why = BACKTEST_DIR / "artifacts", "new"
    RESOLVED_DIRS["CRYPTOBOT_PM_BT_ARTIFACTS"] = (chosen, why)
    return chosen


ARTIFACTS_DIR = _resolve_artifacts_dir()
# The deployed strategy config lives with the live bot, not in the research
# tree.
LIVE_CONFIG_DIR = REPO_ROOT / "polymarket" / "live" / "config"
LIVE_V6_CONFIG_PATH = LIVE_CONFIG_DIR / "twap_symmetric_live.json"

__all__ = [
    "legacy_first",
    "REPO_ROOT",
    "BACKTEST_DIR",
    "LEGACY_BACKTEST_DIR",
    "BT_DATA_DIR",
    "BT_CACHE_DIR",
    "BT_RUNS_DIR",
    "STUDIES_DIR",
    "ARTIFACTS_DIR",
    "LIVE_CONFIG_DIR",
    "LIVE_V6_CONFIG_PATH",
    "RESOLVED_DIRS",
]
