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


def _manifest_ask_bucket_ranges(raw: str) -> tuple[tuple[float, float], ...]:
    """Parse a manifest ``ask_bucket`` string ("name:lo:hi", optionally
    ``|``-joined) into (lo, hi) range tuples.

    Mirrors ``portfolio_loader._parse_ask_bucket`` / ``training.pmdata_target.
    _parse_buckets`` (relocated there 2026-08-01 from the deleted
    ``training.residual_matrix``; ``pmdata_target._ask_bucket_ok`` parses the
    identical grammar and is the live one, via ``training.checkpoint``)
    so the manifest's recorded bucket and the runtime sleeve's
    parsed ranges are compared on the same normalized shape. Reproduced here
    (not imported) to avoid a hgb_loader -> portfolio_loader dependency.
    """
    ranges: list[tuple[float, float]] = []
    for piece in str(raw).split("|"):
        parts = piece.split(":")
        if len(parts) != 3:
            raise ValueError(f"manifest ask_bucket piece {piece!r} not name:lo:hi")
        ranges.append((float(parts[1]), float(parts[2])))
    return tuple(ranges)


def validate_sleeves_against_manifest(sleeves, checkpoint_manifest: dict) -> None:
    """Fail-closed: every runtime hgb sleeve's serve-time bet-selection knobs
    must equal the values recorded in the checkpoint manifest (alignment
    finding #4).

    ``checkpoint_loader.load_checkpoint_models`` already pins
    ``feature_columns`` (the model's input contract), but ``threshold`` /
    ``ask_bucket`` / ``book_consumption_max`` / ``side_filter`` are NOT baked
    into the joblib pipeline — they live in the runtime config and are applied
    at serve time. A typo there (e.g. ``mid:0.4:0.7`` vs the trained
    ``mid:0.4:0.6``) would pass every other boot check and silently make the
    live bot bet a band the backtest never validated. This closes that gap by
    asserting the runtime sleeve matches the manifest the model was selected on.

    2026-07-31 exp044 retirement: every hgb sleeve is now a CHECKPOINT sleeve
    served by checkpoint_v1, so the checkpoint manifest is the one and only
    contract. A sleeve with ``signal_source == "hgb"`` and an EMPTY
    ``checkpoint_offset_set`` is a retired baseline sleeve and is rejected here
    rather than silently scoring against models that no longer load.

    Args:
        sleeves: iterable of ``portfolio_loader.Sleeve``; only
            ``signal_source == "hgb"`` sleeves are checked.
        checkpoint_manifest: parsed checkpoint_v1 manifest; its
            ``sleeves[label|interval]`` block carries the recorded knobs.

    Raises:
        ValueError: on the first runtime-vs-manifest divergence.
    """
    ckpt_sleeves = (checkpoint_manifest or {}).get("sleeves") or {}
    for sleeve in sleeves:
        if getattr(sleeve, "signal_source", None) != "hgb":
            continue
        iv = str(sleeve.interval).strip().lower()
        if not getattr(sleeve, "checkpoint_offset_set", ()):
            raise ValueError(
                f"hgb_loader: sleeve {sleeve.label!r} is a BASELINE (minute-entry) "
                "hgb sleeve — that path was retired with exp044 on 2026-07-31. "
                "Give it a checkpoint_offset_set or drop it from the config."
            )
        # Look up BY SLEEVE LABEL first, falling back to the interval key.
        # Two checkpoint sleeves can share one interval but serve DIFFERENT
        # contracts (a different band and/or pred_edge gate). An interval-only
        # lookup would resolve both to manifest['sleeves']['5m'] and reject the
        # second one at boot. The label key expresses the per-sleeve contract;
        # the interval fallback keeps an interval-keyed manifest valid for the
        # live subminute sleeves.
        sm = (
            ckpt_sleeves.get(sleeve.label)
            or ckpt_sleeves.get(iv)
            or ckpt_sleeves.get(str(sleeve.interval))
        )
        if sm is None:
            raise ValueError(
                f"hgb_loader: checkpoint sleeve {sleeve.label!r} interval {iv} "
                f"has no 'sleeves' entry in the checkpoint manifest — "
                f"re-export models with the sleeves block (polymarket.backtest.training.export_models)"
            )

        # threshold (pred_edge gate) — the checkpoint manifest DECLARES the
        # serve threshold, so compare hard, same fail-closed contract as every
        # other field below.
        if sleeve.threshold is None:
            raise ValueError(
                f"hgb_loader: checkpoint sleeve {sleeve.label!r} requires "
                "an explicit threshold"
            )
        m_thr = float(sm["threshold"])
        if abs(float(sleeve.threshold) - m_thr) > 1e-12:
            raise ValueError(
                f"hgb_loader: checkpoint sleeve {sleeve.label!r} threshold "
                f"{sleeve.threshold} != checkpoint manifest {m_thr} — runtime "
                f"config drifted from the exported model contract"
            )

        # book_consumption_max (optional per-tick depth cap). Runtime None is
        # the explicit edge-preserving top-up mode: no fixed depth fraction;
        # the execution walker stops when realized VWAP edge falls below the
        # entry threshold. Numeric values still must match the manifest.
        if sleeve.book_consumption_max is not None:
            if sm.get("book_consumption_max") is None:
                raise ValueError(
                    f"hgb_loader: sleeve {sleeve.label!r} book_consumption_max "
                    f"{sleeve.book_consumption_max} != manifest None"
                )
            m_bcm = float(sm["book_consumption_max"])
            if abs(float(sleeve.book_consumption_max) - m_bcm) > 1e-12:
                raise ValueError(
                    f"hgb_loader: sleeve {sleeve.label!r} book_consumption_max "
                    f"{sleeve.book_consumption_max} != manifest {m_bcm}"
                )

        # side_filter (both|up|down)
        m_sf = str(sm.get("side_filter", "both")).lower()
        if str(sleeve.side_filter).lower() != m_sf:
            raise ValueError(
                f"hgb_loader: sleeve {sleeve.label!r} side_filter "
                f"{sleeve.side_filter!r} != manifest {m_sf!r}"
            )

        # ask_bucket (compared on parsed (lo, hi) ranges, formatting-agnostic)
        m_ranges = _manifest_ask_bucket_ranges(str(sm["ask_bucket"]))
        if tuple(sleeve.ask_bucket_ranges) != m_ranges:
            raise ValueError(
                f"hgb_loader: sleeve {sleeve.label!r} ask_bucket ranges "
                f"{tuple(sleeve.ask_bucket_ranges)} != manifest {m_ranges} "
                f"(manifest raw={sm['ask_bucket']!r}) — live would serve a band "
                f"the backtest never validated"
            )

        # feature_groups (the model's input contract is already pinned by
        # checkpoint_loader via feature_columns; assert the human-facing group
        # string too so a config that *looks* different is caught at the same
        # boot site).
        m_fg = str(sm["feature_groups"])
        if sleeve.feature_groups is not None and str(sleeve.feature_groups) != m_fg:
            raise ValueError(
                f"hgb_loader: sleeve {sleeve.label!r} feature_groups "
                f"{sleeve.feature_groups!r} != manifest {m_fg!r}"
            )


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
