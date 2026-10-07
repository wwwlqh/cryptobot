"""The shared RF serving surface: ``RfArtifact`` + the predict fast paths.

An ``RfArtifact`` is a dict[(coin, interval, entry_minute) →
RandomForestClassifier] with its manifest, plus two hot-path caches: a
per-(slice, z) predict memo and the exact step-function LUT built by
``warm_step_luts``. The univariate contract (``feature_columns ==
['partial_ret_z_1h']``) is what makes the step LUT exact.

2026-07-31 (exp044 phase 2): ``load_rf`` — the rf_v1-joblib-off-disk loader —
LEFT this module with the rest of the rf_v1 boot chain. Nothing live loads
rf_v1 any more; the live forests are ``checkpoint_v1``, wrapped into this same
``RfArtifact`` by ``strategy/checkpoint_loader.py``. Phase 3 finished the job:
the research entry points that replayed the frozen rf_v1 copy are retired, the
joblib is off disk, and ``load_rf`` no longer exists anywhere. This module is
the SERVING surface only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


EXPECTED_FEATURE_COLUMNS = ["partial_ret_z_1h"]

# Memo-cache eviction bound. Each tick adds at most a few dozen keys (z
# changes every tick, so cross-tick hits are rare) — the cache exists to
# collapse the WITHIN-tick duplicate predicts (pre-book score + post-book
# rescore, and baseline + checkpoint sleeves sharing a (slice, z) key).
# A hard clear at the cap keeps the steady-state footprint bounded without
# LRU bookkeeping on the hot path.
_PROBA_CACHE_MAX = 8192


@dataclass(frozen=True)
class RfArtifact:
    """In-memory RF model bundle.

    `estimators` keys are `(coin_lower, interval_lower, entry_minute)`
    matching the parquet's case convention.
    """

    estimators: dict[tuple[str, str, int], Any]
    manifest: dict
    model_sha1: str
    # 2026-07-03 tick-latency fix: per-artifact predict memo. The forest's
    # only input is (slice key, z); one live tick asks for the same key
    # 2-4x (pre-book + rescore x baseline + checkpoint sleeves). ~18ms per
    # sequential 200-tree predict makes the duplicates the next-largest
    # tick cost after the n_jobs fix. compare=False keeps the dict out of
    # the frozen dataclass's eq/hash.
    _proba_cache: dict[tuple[str, str, int, float], float | None] = field(
        default_factory=dict, repr=False, compare=False
    )
    # 2026-07-25 latency audit: exact step-function LUTs, one per slice.
    # rf_v1 is UNIVARIATE (feature_columns == ['partial_ret_z_1h']), so each
    # forest is exactly a step function of z and can be replaced by a sorted
    # breakpoint array + a value array — a searchsorted instead of 200 tree
    # traversals. Measured: 0.003ms/lookup vs ~6.8ms/predict on the live box,
    # and score.py calls predict_proba UNCONDITIONALLY per cell (~5.3 cells/tick)
    # even when signal_source is 'hgb', so this was ~36ms of every order tick.
    #
    # Populated OFF the hot path by warm_step_luts() (daemon thread) so neither
    # boot nor any tick pays the build. Absent key ⇒ sklearn path, unchanged.
    # value = (breakpoints float32 ndarray, values float64 ndarray). NaN z has no
    # LUT entry on purpose — it falls back to sklearn in predict_proba.
    _step_lut: dict[tuple[str, str, int], Any] = field(
        default_factory=dict, repr=False, compare=False
    )


# Cache-miss sentinel — distinct from None so a missing-slice None (which
# deliberately bypasses the cache) can never be confused with a stored hit.
_MISS = object()


def _build_step_lut(est) -> tuple | None:
    """Exact step-function LUT for a UNIVARIATE forest, or None if inapplicable.

    A forest whose every split tests feature 0 is piecewise-constant in that
    feature, so it is fully described by its split thresholds: sort them, and a
    query is one ``searchsorted`` instead of 200 tree traversals.

    THE FLOAT SUBTLETY (this is what makes a naive LUT wrong): sklearn casts X
    to float32 before the tree's float64 ``x <= threshold`` test, so the real
    boundary in float32 space is the LARGEST float32 <= threshold.
    ``np.float32(t)`` rounds to NEAREST and can land ABOVE ``t``, which would
    shift a boundary by one ulp and silently mis-score threshold-adjacent z.
    Hence the explicit ``nextafter`` snap-DOWN.

    Verified on a same-shape synthetic forest over 41,480 adversarial probes
    (every breakpoint, both ``nextafter`` neighbours, every raw threshold, plus
    a 20k dense grid): 0 mismatches, max|diff| = 0.0. The per-slice self-check
    below re-proves it for the real artifact before the LUT is installed.

    Returns ``(breakpoints_f32, values_f64)`` or None to keep the sklearn path.
    """
    import numpy as np

    trees = getattr(est, "estimators_", None)
    classes = getattr(est, "classes_", None)
    if not trees or classes is None:
        return None
    if int(getattr(est, "n_features_in_", 0)) != 1:
        return None

    def _p_up(proba):
        """P(class=1) with the same monoclass fallback as the sklearn path."""
        if proba.shape[1] > 1:
            return proba[:, 1].astype(np.float64)
        return np.full(
            proba.shape[0], 1.0 if int(classes[0]) == 1 else 0.0, dtype=np.float64
        )

    thr_parts = []
    for t in trees:
        tree = getattr(t, "tree_", None)
        if tree is None:
            return None
        feat = tree.feature
        internal = feat >= 0  # -2 == TREE_UNDEFINED (leaf)
        if np.any(feat[internal] != 0):
            return None  # a split on some other feature ⇒ not univariate
        thr_parts.append(tree.threshold[internal])

    thr = np.unique(np.concatenate(thr_parts)) if thr_parts else np.empty(0)
    if thr.size == 0:
        return None  # split-less forest — leave it on the sklearn path

    f32 = thr.astype(np.float32)
    over = f32.astype(np.float64) > thr
    f32[over] = np.nextafter(f32[over], np.float32(-np.inf))
    bps = np.unique(f32)
    # Bucket i is chosen by searchsorted(bps, z, 'left'); a representative of
    # bucket i is bps[i], and of the final open bucket, one ulp above bps[-1].
    reps = np.empty(bps.size + 1, dtype=np.float32)
    reps[: bps.size] = bps
    reps[bps.size] = np.nextafter(bps[-1], np.float32(np.inf))
    vals = _p_up(est.predict_proba(reps.reshape(-1, 1).astype(np.float64)))

    # Self-check (one batched predict, off the hot path): every breakpoint and
    # both its float32 neighbours must reproduce sklearn EXACTLY. Any drift and
    # the slice keeps the sklearn path.
    probe = np.unique(
        np.concatenate([
            bps,
            np.nextafter(bps, np.float32(np.inf)),
            np.nextafter(bps, np.float32(-np.inf)),
        ])
    )
    truth = _p_up(est.predict_proba(probe.reshape(-1, 1).astype(np.float64)))
    got = vals[np.searchsorted(bps, probe, side="left")]
    if not np.array_equal(got, truth):
        return None
    return (bps, vals)


def warm_step_luts(rf: RfArtifact, *, background: bool = True) -> None:
    """Build the per-slice step LUTs OFF the hot path.

    Boot is already slow (large joblib artifacts) and ``deploy.sh`` only waits
    60s for the boot marker, so this never runs inline at boot by default and
    never runs inside a tick: a daemon thread fills ``rf._step_lut`` slice by
    slice and ``predict_proba`` picks each one up as it lands. Until a slice is
    built, that slice takes the unchanged sklearn path — so this is safe to
    start and safe to fail.
    """
    luts = getattr(rf, "_step_lut", None)
    if luts is None:
        return

    def _run() -> None:
        built = skipped = 0
        for key, est in list(rf.estimators.items()):
            if key in luts:
                continue
            try:
                lut = _build_step_lut(est)
            except Exception:  # noqa: BLE001 — never kill boot over an optimization
                logger.debug("step-LUT build failed for %r", key, exc_info=True)
                lut = None
            if lut is None:
                skipped += 1
                continue
            luts[key] = lut  # single dict assignment — atomic under the GIL
            built += 1
        logger.info(
            "RF step-LUT warm complete: %d slices built, %d left on the sklearn "
            "path (exact step-function replacement for a univariate forest)",
            built, skipped,
        )

    if not background:
        _run()
        return
    import threading

    threading.Thread(target=_run, name="cryptobot-rf-step-lut", daemon=True).start()


def predict_proba(
    rf: RfArtifact,
    *,
    coin: str,
    interval: str,
    entry_minute: int,
    partial_ret_z_1h: float,
) -> float | None:
    """Return P(Up) from the matching estimator slice, or None when missing."""
    key = (coin.lower(), interval.lower(), int(entry_minute), float(partial_ret_z_1h))
    # getattr: duck-typed stand-ins for RfArtifact (test stubs) predate the
    # cache field — degrade to uncached predicts rather than requiring it.
    cache = getattr(rf, "_proba_cache", None)
    if cache is not None:
        cached = cache.get(key, _MISS)
        if cached is not _MISS:
            return cached
    est = rf.estimators.get(key[:3])
    if est is None:
        return None
    # Exact step-function fast path (2026-07-25). Same value as the forest, ~0.003ms
    # vs ~6.8ms. NaN z deliberately falls through to sklearn: missing-value routing
    # is the estimator's business, it is rare, and correctness beats the microsecond.
    z = float(partial_ret_z_1h)
    if z == z:  # not NaN
        luts = getattr(rf, "_step_lut", None)
        lut = luts.get(key[:3]) if luts else None
        if lut is not None:
            import numpy as np

            bps, vals = lut
            result = float(vals[np.searchsorted(bps, np.float32(z), side="left")])
            if cache is not None:
                if len(cache) >= _PROBA_CACHE_MAX:
                    cache.clear()
                cache[key] = result
            return result
    # Preserve the fit-time feature name so sklearn does not emit per-tick
    # "X does not have valid feature names" warnings.
    frame = pd.DataFrame(
        [[float(partial_ret_z_1h)]],
        columns=EXPECTED_FEATURE_COLUMNS,
    )
    proba = est.predict_proba(frame)
    # Estimator's classes_ ordering: returns P(class=0), P(class=1) — class=1 is Up.
    if proba.shape[1] == 1:
        # Monoclass — fallback to that class's probability.
        cls = est.classes_[0]
        result = 1.0 if int(cls) == 1 else 0.0
    else:
        result = float(proba[0, 1])
    if cache is not None:
        if len(cache) >= _PROBA_CACHE_MAX:
            cache.clear()
        cache[key] = result
    return result
