"""Polymarket backtest package root.

Defines the canonical repo-root / backtest-dir paths AND the **data-directory
resolution shim** so every backtest submodule resolves data / model / config
locations from ONE place instead of per-file ``Path(__file__).parents[N]`` math
(which silently mis-resolves when a module moves between subpackages). Import
them as::

    from polymarket.backtest import REPO_ROOT, BACKTEST_DIR, BT_DATA_DIR

Submodules live in subpackages — ``engine/`` (scoring/search/sim + the
residual-edge signal core), ``lib/`` (the frozen research evaluator + pmdata
cleaning + the forward-OOS regraders; was ``eval/`` before the 2026-07-28
venue-tree restructure), ``training/`` (pool build + WFO + model export),
``fetch/`` (raw -> parquet), ``audit/`` (parity), ``scripts/`` (research/ops
CLIs), ``studies/`` (dated verdicts + archived one-off drivers), ``tests/`` —
and import each other by fully-qualified name::

    from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA

The repo root must be on ``sys.path`` for that to resolve: pytest gets it via
``pythonpath = ["polymarket/src", "."]`` (pyproject), and CLI usage via
``python -m polymarket.backtest.<subpackage>.<module>`` run from the repo root.

--- The LEGACY-FIRST data shim (read this before changing a path) -------------

The ~20 GB of research data under the OLD ``backtest/{data,cache,optimize_runs}``
tree is **gitignored** — the venue-tree restructure moved tracked *code*, and
deliberately did NOT move those bytes. Running research sessions depend on
literal paths inside ``backtest/data/server_snapshot/`` and a 16 GB relocation
is hours of I/O for zero behavioral gain, so each data dir resolves in this
order:

    1. explicit env override   (BT_DATA_DIR / BT_CACHE_DIR / BT_RUNS_DIR)
    2. LEGACY physical path    (``<repo>/backtest/...``) **if it exists**
    3. NEW path                (``<repo>/polymarket/backtest/...``)

Order 2-before-3 is what makes merge day a physical no-op: an existing checkout
keeps reading the bytes exactly where they already are, while a fresh clone (no
legacy dir) lands on the new layout. Corollary, and the reason there is no
``.gitkeep`` anywhere under a ``data/`` dir: an empty NEW directory committed to
git would win over a populated legacy one on a fresh pull and silently blank the
substrate. Never create these dirs in git; let the writer ``mkdir -p`` them.

``ARTIFACTS_DIR`` (the ship-to-live boundary) follows the same shape with its
own env key ``CRYPTOBOT_PM_BT_ARTIFACTS`` — see ``_resolve_artifacts_dir``.

``REPO_ROOT`` honors ``CRYPTOBOT_ROOT`` first, exactly like
``cryptobot.config``. The two packages must agree on the root or a producer
here writes into one tree while the live consumer reads another.
"""

from __future__ import annotations

import os
from pathlib import Path

# This file lives at polymarket/backtest/__init__.py and NEVER moves, so the
# math here is stable no matter where submodules live:
#   BACKTEST_DIR             -> polymarket/backtest/
#   BACKTEST_DIR.parents[0]  -> polymarket/  (legacy-path-ok)
#   BACKTEST_DIR.parents[1]  -> repo root  (legacy-path-ok)
BACKTEST_DIR = Path(__file__).resolve().parent


def _resolve_repo_root() -> Path:
    """``CRYPTOBOT_ROOT`` if set, else the parents[] anchor.

    Mirrors ``cryptobot.config._resolve_repo_root``'s first tier ON PURPOSE.
    Both roots must name the same tree: research code writes artifacts the live
    engine reads back (``data/models/checkpoint_v1/*.joblib``),
    and an operator who repoints one root but not the other gets a silent split
    — the producer writing into tree A while the consumer reads tree B. One env
    var, both packages.

    The marker-dir walk is deliberately NOT mirrored: this file's location is
    fixed relative to the repo root, so there is nothing for a walk to discover.
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

    Takes an explicit ``root`` so audits/tests can point the same resolution
    rule at a synthetic tree. Module-level callers want the ``BT_*`` constants
    below instead — this is the primitive they are built on.
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
# Derived caches. Gitignored. As of 2026-07-31 the only live tenant is the
# binance 1s kline cache (``binance_1s/``, read by ``training.offset_wfo``): the
# book-depth index was deleted in the data consolidation and the per-interval RF
# prediction parquets went with the rf_v1 retirement.
BT_CACHE_DIR = _resolve_dir("BT_CACHE_DIR", "cache")
# Heavy WFO/optimizer artifacts: scored_11set_truth.parquet, option3_pool/,
# study DBs. Gitignored. NOTE: the *tracked* verdict .md / report .json files
# that used to sit in optimize_runs/ moved to ``studies/`` (see STUDIES_DIR) —
# this key is for the heavy regenerable outputs only.
BT_RUNS_DIR = _resolve_dir("BT_RUNS_DIR", "optimize_runs")

# NOTE (2026-07-31, exp044 phase 3): there is no ``NX_FEATURES_DIR`` key any
# more. The nx_v1 feature parquets existed only to feed rf_v1 and the
# decision_table, both retired in phase 2, and their builder
# (``features/build.py``, which read ``data/prices.db``) was deleted in phase 3
# along with the whole ``features/`` package. Any bytes still sitting in
# ``backtest/features/nx_v1/`` are orphaned data with no producer and no reader.

# Tracked, so this follows the code and needs no legacy fallback.
STUDIES_DIR = BACKTEST_DIR / "studies"


def _resolve_artifacts_dir() -> Path:
    """The ship-to-live artifact boundary (plan §3) — legacy-first, like BT_*.

    ``decision_table.parquet`` used to be the one file both trees touched, and
    it is still the token tier 2 keys on. It left the live boot path with the
    pair sleeves in the 2026-07-31 exp044 phase-2 retirement and was DELETED
    from disk in phase 3, builder and all, so on a current checkout tier 2 can
    no longer fire and this resolves to tier 3. The tier is kept rather than
    ripped out because an older checkout (or a restored backup) still has the
    file next to its other legacy bulk data, and silently relocating that
    checkout's artifact dir would be worse than a dead branch. Resolution order:

      1. ``CRYPTOBOT_PM_BT_ARTIFACTS`` override,
      2. ``BT_DATA_DIR`` **if a decision_table.parquet is still sitting there** —
         a pre-phase-3 checkout,
      3. ``BACKTEST_DIR/"artifacts"`` — the fresh-clone home, and the answer
         everywhere today.

    Tier 2 keys on the FILE, not the directory, because BT_DATA_DIR always
    resolves to something; only the file's presence proves a legacy artifact
    tree is in play.
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
# The deployed v6 portfolio contract. Tracked, and it moved OUT of the research
# tree into the live venue config on 2026-07-28 (it is a live artifact, not a
# research output).
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
