"""HGB predict memo tests (2026-07-15 tick-latency fix).

``hgb_loader.predict_model_prob`` memoizes ``predict_proba`` per-artifact on
(interval, feature-row values), mirroring rf_loader's ``_proba_cache``. These
pin the memo's PARITY-critical properties:

  - a hit returns the byte-identical float from the earlier predict (never a
    recompute that could drift a parity-pinned score),
  - interval is IN the key so 5m/15m never cross-serve,
  - a repeated (interval, feature_row) does NOT re-run the pipeline (the win),
  - the cache clears at the eviction cap (bounded footprint),
  - a duck-typed stand-in without the cache field still predicts (uncached).

The interval-separation / counting / eviction cases use a fake pipeline so
they are exact and artifact-free; the byte-identity case uses the real
``data/models/checkpoint_v1/checkpoint_hgb_v1.joblib`` (the LIVE serving
artifact) and is skipped when that gitignored bundle is absent.
"""

from __future__ import annotations

import pandas as pd
import pytest

from cryptobot.strategy.hgb_loader import (
    HgbArtifact,
    _PROBA_CACHE_MAX,
    predict_model_prob,
)
from cryptobot.tests._artifact_skips import needs_checkpoint


class _FakePipe:
    """Minimal predict_proba stand-in returning a fixed P(class=1)=``p`` and
    counting how many times it actually runs (to prove cache hits skip it)."""

    def __init__(self, p: float) -> None:
        self.p = float(p)
        self.calls = 0

    def predict_proba(self, frame):
        self.calls += 1
        return pd.DataFrame([[1.0 - self.p, self.p]]).to_numpy()


def _fake_artifact(pipes: dict, cols: dict) -> HgbArtifact:
    return HgbArtifact(
        pipelines=pipes,
        feature_columns=cols,
        manifest={},
        rf_v1_model_sha1="fake",
    )


def test_repeat_predict_is_a_cache_hit_and_skips_the_pipeline() -> None:
    pipe = _FakePipe(0.42)
    hgb = _fake_artifact({"5m": pipe}, {"5m": ["a", "b"]})

    r1 = predict_model_prob(hgb, interval="5m", feature_row_values=[1.0, 2.0])
    r2 = predict_model_prob(hgb, interval="5m", feature_row_values=[1.0, 2.0])

    assert r1 == 0.42
    assert r2 == r1  # byte-identical hit
    assert pipe.calls == 1  # second call served from the memo, pipeline not re-run
    assert hgb._proba_cache == {("5m", (1.0, 2.0)): 0.42}


def test_interval_is_in_the_key_no_cross_serve() -> None:
    # Same feature row, different interval → different probability. If interval
    # were absent from the key, the 15m call would wrongly hit the 5m entry.
    p5, p15 = _FakePipe(0.30), _FakePipe(0.70)
    hgb = _fake_artifact(
        {"5m": p5, "15m": p15}, {"5m": ["a", "b"], "15m": ["a", "b"]}
    )

    assert predict_model_prob(hgb, interval="5m", feature_row_values=[1.0, 2.0]) == 0.30
    assert predict_model_prob(hgb, interval="15m", feature_row_values=[1.0, 2.0]) == 0.70
    assert set(hgb._proba_cache) == {("5m", (1.0, 2.0)), ("15m", (1.0, 2.0))}


def test_different_feature_row_is_a_miss() -> None:
    pipe = _FakePipe(0.5)
    hgb = _fake_artifact({"5m": pipe}, {"5m": ["a"]})

    predict_model_prob(hgb, interval="5m", feature_row_values=[1.0])
    predict_model_prob(hgb, interval="5m", feature_row_values=[2.0])
    assert pipe.calls == 2  # distinct rows both computed
    assert len(hgb._proba_cache) == 2


def test_cache_clears_at_eviction_cap() -> None:
    pipe = _FakePipe(0.1)
    hgb = _fake_artifact({"5m": pipe}, {"5m": ["a"]})
    # Pre-fill to the cap with dummy keys, then one fresh predict on a key
    # OUTSIDE that range (a genuine miss) must trip the hard clear (len >= cap)
    # and re-seed the cache with just its own entry.
    hgb._proba_cache.update({("5m", (float(i),)): 0.0 for i in range(_PROBA_CACHE_MAX)})
    assert len(hgb._proba_cache) == _PROBA_CACHE_MAX

    predict_model_prob(hgb, interval="5m", feature_row_values=[99999.0])
    assert hgb._proba_cache == {("5m", (99999.0,)): 0.1}


def test_duck_typed_stand_in_without_cache_still_predicts() -> None:
    # A stub carrying only pipelines/feature_columns (no _proba_cache) must
    # degrade to uncached predicts, not raise.
    from types import SimpleNamespace

    pipe = _FakePipe(0.9)
    stub = SimpleNamespace(pipelines={"5m": pipe}, feature_columns={"5m": ["a"]})
    assert predict_model_prob(stub, interval="5m", feature_row_values=[1.0]) == 0.9
    assert pipe.calls == 1


@needs_checkpoint
def test_cached_value_is_byte_identical_to_fresh_recompute() -> None:
    from cryptobot.strategy.checkpoint_loader import load_checkpoint_models

    hgb = load_checkpoint_models()[1]
    cols = hgb.columns_for("5m")
    row = [0.1 * (i + 1) for i in range(len(cols))]

    # Ground truth: recompute directly off the pipeline, no cache involved.
    pipe = hgb.pipelines["5m"]
    truth = float(pipe.predict_proba(pd.DataFrame([row], columns=cols))[:, 1][0])

    first = predict_model_prob(hgb, interval="5m", feature_row_values=row)
    hit = predict_model_prob(hgb, interval="5m", feature_row_values=row)

    assert first == truth  # memoized path matches the raw predict bit-for-bit
    assert hit == first  # and the hit is the exact same float object value
    assert hgb._proba_cache[("5m", tuple(row))] == truth
