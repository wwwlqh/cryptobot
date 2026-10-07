"""HGB model bundle and the boot-time sleeve/manifest check.

``HgbArtifact`` is the in-memory bundle every HGB model is served through, and
``predict_model_prob`` is the single prediction entry point. ``twap_loader``
builds the artifact from disk; this module defines the bundle, the predict path
and the fail-closed check that the configured sleeves match the model manifest.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Memo-cache eviction bound (mirrors rf_loader._PROBA_CACHE_MAX). Each tick
# adds at most a few keys per interval — a feature row's partial_ret_z_1h moves
# every tick, so cross-tick hits are rare — and the cache exists to collapse the
# WITHIN-tick duplicate predicts (pre-book score + post-book rescore over YES+NO,
# and the per-second held-position re-scoring a position whose features have not
# moved). A hard clear at the cap keeps the steady-state footprint bounded
# without LRU bookkeeping on the hot path.
_PROBA_CACHE_MAX = 8192


@dataclass(frozen=True)
class HgbArtifact:
    """In-memory HGB model bundle.

    ``pipelines`` keys are interval strings ("5m"/"15m"); each value is a fitted
    sklearn Pipeline (SimpleImputer(median) -> HistGradientBoostingClassifier).
    ``feature_columns`` mirrors the manifest's per-interval ordered column list —
    the EXACT order the pipeline was fit on, used to build the serve-time frame.
    """

    pipelines: dict[str, Any]
    feature_columns: dict[str, list[str]]
    manifest: dict
    rf_v1_model_sha1: str
    # Per-artifact cache of predictions keyed on (interval, feature-row
    # values). One cell can ask for the same row more than once in a tick
    # (scoring before and after the book read, over YES and NO), and a repeat
    # is then a dict hit instead of a ~2ms predict_proba. The interval is part
    # of the key so 5m and 15m never mix. compare=False keeps it out of the
    # dataclass's eq/hash.
    _proba_cache: dict[tuple[str, tuple[float, ...]], float] = field(
        default_factory=dict, repr=False, compare=False
    )

    def columns_for(self, interval: str) -> list[str]:
        return self.feature_columns[interval.strip().lower()]


# Cache-miss sentinel — mirrors rf_loader; distinct from any storable float so
# a lookup can tell "absent" from a stored 0.0 without a second membership test.
_MISS = object()


def predict_model_prob(
    hgb: HgbArtifact,
    *,
    interval: str,
    feature_row_values: list[float],
) -> float:
    """Return P(class=1) for one feature row from the interval's model.

    ``feature_row_values`` must already be in manifest column order (the output
    of ``hgb_features.build_feature_vector(... feature_columns=
    hgb.columns_for(interval))``). Builds a one-row DataFrame with the manifest
    column names, as sklearn's column check requires, and returns
    ``predict_proba(...)[:, 1]``.

    Cached per artifact on (interval, feature values); see
    ``HgbArtifact._proba_cache``. A hit returns the identical float. A row with
    a NaN never hits (NaN != NaN), so it is just recomputed.
    """
    import pandas as pd

    iv = interval.strip().lower()
    pipe = hgb.pipelines.get(iv)
    if pipe is None:
        raise ValueError(f"HGB: no pipeline for interval {interval!r}")
    cols = hgb.feature_columns[iv]
    if len(feature_row_values) != len(cols):
        raise ValueError(
            f"HGB {iv}: got {len(feature_row_values)} feature values, expected "
            f"{len(cols)} ({cols})"
        )
    # interval is IN the key so 5m/15m never cross-serve. getattr: duck-typed
    # stand-ins for HgbArtifact (test stubs) predate the cache field — degrade
    # to uncached predicts rather than requiring it.
    key = (iv, tuple(feature_row_values))
    cache = getattr(hgb, "_proba_cache", None)
    if cache is not None:
        cached = cache.get(key, _MISS)
        if cached is not _MISS:
            return cached
    frame = pd.DataFrame([list(feature_row_values)], columns=cols)
    proba = pipe.predict_proba(frame)
    result = float(proba[:, 1][0])
    if cache is not None:
        if len(cache) >= _PROBA_CACHE_MAX:
            cache.clear()
        cache[key] = result
    return result
