"""Skip markers for tests that need the gitignored trained model files."""

from __future__ import annotations

from pathlib import Path

import pytest

from cryptobot import config


def _exists(path) -> bool:
    try:
        return Path(path).exists()
    except (TypeError, ValueError):
        return False


needs_twap_model = pytest.mark.skipif(
    not (
        _exists(config.TWAP_SYMMETRIC_MODEL_PATH)
        and _exists(config.TWAP_SYMMETRIC_MANIFEST_PATH)
    ),
    reason=(
        "needs data/models/twap_symmetric_v1/* (gitignored; build it with "
        "python -m polymarket.backtest.training.export_twap_symmetric_60s)"
    ),
)
