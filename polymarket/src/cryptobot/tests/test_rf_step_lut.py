"""RF step-function LUT (2026-07-25 latency audit).

rf_v1 is univariate (``feature_columns == ['partial_ret_z_1h']``), so each forest
is exactly a step function of z and can be served by a ``searchsorted`` instead of
200 tree traversals — measured 0.003ms vs ~6.8ms, and ``score.py`` calls
``predict_proba`` unconditionally per cell (~36ms/order tick).

These tests are about EXACTNESS, not speed: the LUT must reproduce sklearn bit for
bit, including at the float32 boundaries where a naive build is wrong.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from cryptobot.strategy.rf_loader import (
    EXPECTED_FEATURE_COLUMNS,
    RfArtifact,
    _build_step_lut,
    predict_proba,
    warm_step_luts,
)

sklearn_ensemble = pytest.importorskip("sklearn.ensemble")


def _univariate_forest(n_estimators: int = 40, seed: int = 0):
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(1500, 1))
    y = (x[:, 0] + rng.normal(scale=0.5, size=1500) > 0).astype(int)
    est = sklearn_ensemble.RandomForestClassifier(
        n_estimators=n_estimators, min_samples_leaf=15, random_state=seed
    )
    est.fit(pd.DataFrame(x, columns=EXPECTED_FEATURE_COLUMNS), y)
    est.n_jobs = 1
    return est


def _adversarial_grid(est) -> np.ndarray:
    """Every point where a naive LUT could disagree with sklearn."""
    thr = np.unique(
        np.concatenate([
            t.tree_.threshold[t.tree_.feature >= 0] for t in est.estimators_
        ])
    )
    f32 = thr.astype(np.float32).astype(np.float64)
    return np.unique(
        np.concatenate([
            np.linspace(thr.min() - 2.0, thr.max() + 2.0, 5000),
            thr,
            np.nextafter(thr, np.inf),
            np.nextafter(thr, -np.inf),
            f32,
            np.nextafter(f32, np.inf),
            np.nextafter(f32, -np.inf),
        ])
    )


def test_step_lut_is_bit_exact_vs_sklearn():
    est = _univariate_forest()
    lut = _build_step_lut(est)
    assert lut is not None, "a univariate forest must be LUT-able"
    bps, vals = lut

    grid = _adversarial_grid(est)
    truth = est.predict_proba(grid.reshape(-1, 1))[:, 1]
    got = vals[np.searchsorted(bps, grid.astype(np.float32), side="left")]

    mismatches = int(np.count_nonzero(got != truth))
    assert mismatches == 0, (
        f"{mismatches}/{grid.size} probes differ; "
        f"max|diff|={np.abs(got - truth).max():.3e}"
    )


def test_threshold_snap_is_downward():
    """np.float32(t) rounds to NEAREST and can land ABOVE t.

    Every breakpoint must be <= the threshold it came from, or the boundary
    shifts by one ulp and threshold-adjacent z scores against the wrong leaf.
    """
    est = _univariate_forest(seed=3)
    bps, _ = _build_step_lut(est)
    thr = np.unique(
        np.concatenate([
            t.tree_.threshold[t.tree_.feature >= 0] for t in est.estimators_
        ])
    )
    for b in bps:
        # the breakpoint must be a float32 that is <= some threshold, and the
        # next float32 up must exceed it (i.e. it is the LARGEST such float32)
        nearest = thr[np.argmin(np.abs(thr - float(b)))]
        assert float(b) <= nearest or np.isclose(float(b), nearest)


def test_non_univariate_forest_is_rejected():
    rng = np.random.default_rng(1)
    x = rng.normal(size=(600, 2))
    y = (x[:, 0] + x[:, 1] > 0).astype(int)
    est = sklearn_ensemble.RandomForestClassifier(n_estimators=10, random_state=1)
    est.fit(x, y)
    assert _build_step_lut(est) is None, "multivariate forest must keep sklearn"


def test_predict_proba_identical_with_and_without_lut():
    est = _univariate_forest(seed=7)
    key = ("btc", "5m", 4)
    rf = RfArtifact(estimators={key: est}, manifest={}, model_sha1="t")

    zs = _adversarial_grid(est)[::37]
    before = [
        predict_proba(rf, coin="BTC", interval="5m", entry_minute=4,
                      partial_ret_z_1h=float(z))
        for z in zs
    ]
    rf._proba_cache.clear()

    warm_step_luts(rf, background=False)
    assert key in rf._step_lut

    after = [
        predict_proba(rf, coin="BTC", interval="5m", entry_minute=4,
                      partial_ret_z_1h=float(z))
        for z in zs
    ]
    assert after == before


def test_nan_z_falls_back_to_sklearn_path():
    """NaN routing is the estimator's business — the LUT must not guess."""
    est = _univariate_forest(seed=11)
    rf = RfArtifact(estimators={("btc", "5m", 4): est}, manifest={}, model_sha1="t")
    warm_step_luts(rf, background=False)
    # Must not raise and must not index the LUT with NaN (which sorts right).
    try:
        got = predict_proba(rf, coin="BTC", interval="5m", entry_minute=4,
                            partial_ret_z_1h=float("nan"))
    except ValueError:
        pytest.skip("this sklearn build rejects NaN input outright")
    truth = est.predict_proba(
        pd.DataFrame([[float("nan")]], columns=EXPECTED_FEATURE_COLUMNS)
    )[:, 1]
    assert got == pytest.approx(float(truth[0]))


def test_missing_slice_still_returns_none():
    rf = RfArtifact(estimators={}, manifest={}, model_sha1="t")
    warm_step_luts(rf, background=False)
    assert predict_proba(
        rf, coin="BTC", interval="5m", entry_minute=4, partial_ret_z_1h=0.1
    ) is None
