"""HGB model bundle + the serve-time sleeve/manifest contract check.

``HgbArtifact`` is the in-memory shape every HGB pipeline is served through, and
``predict_model_prob`` is the single serve entry point. Since the 2026-07-31
exp044 retirement the ONLY producer of an ``HgbArtifact`` is
``checkpoint_loader.load_checkpoint_models`` (checkpoint_v1, the live sub-minute
scorer) — the frozen minute-sliced ``hgb_v1`` artifact, its loader and its
``rf_v1`` sha drift-check are gone. This module therefore no longer reads any
artifact from disk; it only defines the bundle shape, the predict path, and the
fail-closed runtime-sleeve-vs-manifest check used at boot.
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
    # 2026-07-15 tick-latency fix (mirrors rf_loader): per-artifact predict
    # memo keyed on (interval, feature-row values). One live cell asks the HGB
    # for the same (interval, feature_row) up to 2x (pre-book score + post-book
    # rescore over YES/NO), and the per-second held-position pass re-scores each open
    # position every second — while its feature row is unchanged that becomes a
    # pure dict hit instead of a ~2ms single-row predict_proba. interval is part
    # of the key so the 5m/15m pipelines never cross-serve. compare=False keeps
    # the dict out of the frozen dataclass's eq/hash.
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
    """Return P(class=1) for one feature row from the matching interval pipeline.

    ``feature_row_values`` MUST already be in manifest column order (i.e. the
    output of ``hgb_features.build_feature_vector(... feature_columns=
    hgb.columns_for(interval))``). Builds a 1-row pandas frame keyed by the
    manifest column names so sklearn's column validation is satisfied, then
    returns ``predict_proba(...)[:, 1]``.

    Memoized per-artifact on (interval, feature-row values) — see
    ``HgbArtifact._proba_cache``. A cache hit returns the byte-identical float
    from the earlier ``predict_proba`` (tuple keys compare by value, so a hit
    can only serve the exact same interval + feature row; a row carrying a NaN
    never hits, since NaN != NaN — a safe recompute, never a wrong value).
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
