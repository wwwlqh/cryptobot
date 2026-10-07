"""rf_loader predict-memo regressions (2026-07-03 tick-latency fix).

Each tick re-predicted the same (slice, z) key 2-4x (pre-book + rescore, and
before 2026-07-31 the baseline + checkpoint sleeves both asked), so
``predict_proba`` memoizes per artifact.

The companion half of that fix — the loader forcing ``n_jobs=1`` on every
persisted estimator, since a training-parallel forest spawns and tears down a
joblib pool per 1-row predict — now lives with the LOADERS that survive:
``strategy/checkpoint_loader.py:68`` (live). The
``load_rf`` that used to be tested here moved to
``polymarket/backtest/training/legacy_rf.py`` with the rest of the rf_v1 boot
chain on 2026-07-31 (phase 2), and was deleted outright later the same day
(phase 3) when the rf_v1 joblib left the disk — there is no rf_v1 loader to
test any more, on either side of the tree.
"""

from __future__ import annotations

import numpy as np
import pytest

from cryptobot.strategy.rf_loader import RfArtifact, predict_proba


class _CountingEst:
    """Minimal estimator double counting predict_proba invocations."""

    classes_ = np.array([0, 1])
    n_jobs = 1

    def __init__(self) -> None:
        self.calls = 0

    def predict_proba(self, frame):
        self.calls += 1
        return np.array([[0.4, 0.6]])


def test_predict_proba_memoizes_identical_inputs() -> None:
    est = _CountingEst()
    art = RfArtifact(
        estimators={("btc", "5m", 4): est}, manifest={}, model_sha1="stub"
    )
    p1 = predict_proba(
        art, coin="BTC", interval="5M", entry_minute=4, partial_ret_z_1h=0.5
    )
    p2 = predict_proba(
        art, coin="btc", interval="5m", entry_minute=4, partial_ret_z_1h=0.5
    )
    assert p1 == p2 == pytest.approx(0.6)
    assert est.calls == 1, "identical (slice, z) must be served from cache"

    # A different z is a different key — must re-predict.
    predict_proba(
        art, coin="btc", interval="5m", entry_minute=4, partial_ret_z_1h=0.6
    )
    assert est.calls == 2


def test_predict_proba_missing_slice_returns_none_uncached() -> None:
    art = RfArtifact(estimators={}, manifest={}, model_sha1="stub")
    assert (
        predict_proba(
            art, coin="btc", interval="5m", entry_minute=4, partial_ret_z_1h=0.5
        )
        is None
    )
    assert not art._proba_cache, "missing-slice None must not be cached"


def test_predict_proba_tolerates_ducktyped_artifact_without_cache() -> None:
    """Test stubs duck-type RfArtifact without _proba_cache — predict_proba
    must degrade to uncached predicts, not AttributeError."""

    class _StubArt:
        def __init__(self) -> None:
            self.estimators = {("btc", "5m", 4): _CountingEst()}

    stub = _StubArt()
    p = predict_proba(
        stub, coin="btc", interval="5m", entry_minute=4, partial_ret_z_1h=0.5
    )
    assert p == pytest.approx(0.6)
