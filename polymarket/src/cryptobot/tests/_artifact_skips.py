"""Shared pytest skip markers for tests that need heavy, gitignored data
artifacts (today: the checkpoint_v1 bundle).

Those artifacts are bulk local data — not checked into git and not always
present on a fresh checkout (e.g. after a deliberate dataset swap or a fresh
clone before the fetch/build steps run). Tests that genuinely need them should
SKIP — not ERROR — when they are absent, so the suite is green-with-skips on a
data-less checkout and runs the full set again the moment the artifacts are
rebuilt. Tests that exercise pure logic, the missing-artifact error paths, or
synthetic in-tmp fixtures must NOT carry these markers.

Usage:

    from cryptobot.tests._artifact_skips import needs_checkpoint

    @needs_checkpoint
    class TestScoresAgainstTheRealBundle: ...
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cryptobot import config


def _exists(path) -> bool:
    try:
        return Path(path).exists()
    except (TypeError, ValueError):
        return False


needs_checkpoint = pytest.mark.skipif(
    not (
        _exists(config.CHECKPOINT_RF_MODEL_PATH)
        and _exists(config.CHECKPOINT_HGB_MODEL_PATH)
        and _exists(config.CHECKPOINT_MANIFEST_PATH)
    ),
    reason=(
        "needs data/models/checkpoint_v1/* (gitignored; rebuild via "
        "python -m polymarket.backtest.training.export_models)"
    ),
)
