"""Loader for the frozen per-checkpoint-second sub-minute models (checkpoint_v1).

Built by ``polymarket/backtest/training/export_models.py`` to preserve live/backtest
DECISION parity for the sub-minute checkpoint sleeves (the 2026-07-05 parity audit
found live was scoring them at the ``offset//60`` integer minute against the frozen
minute-sliced ``rf_v1``/``hgb_v1`` instead of the backtest's per-checkpoint-second
models). Loads:

  - ``checkpoint_rf_v1.joblib``  -> ``dict[(coin, interval, sec) -> RandomForestClassifier]``
  - ``checkpoint_hgb_v1.joblib`` -> ``dict[interval -> fitted sklearn Pipeline]``
  - ``checkpoint_v1_manifest.json``

and wraps them in the EXISTING ``RfArtifact`` / ``HgbArtifact`` so the tested
``rf_loader.predict_proba`` / ``hgb_loader.predict_model_prob`` serve them unchanged
— including the n_jobs=1 predict-latency fix and the per-artifact RF memo. The only
difference from the baseline loaders is the RF slice key is ``checkpoint_sec`` (an
int in [5, 899]) rather than ``entry_minute`` (1..14); ``predict_proba``'s
``entry_minute`` int parameter carries the second transparently.

⚠️ HONEST LIMITS (see the export script header + 2026-07-05 audit): this is a single
frozen model, NOT the backtest's per-WFO-window OOS ensemble, so it restores
*decision* parity, not the archived $170k OOS number; and it does nothing for the
250ms Polymarket taker-delay *fill* gap.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from cryptobot import config
from cryptobot.strategy.hgb_loader import HgbArtifact
from cryptobot.strategy.rf_loader import RfArtifact

logger = logging.getLogger(__name__)

EXPECTED_INTERVALS = {"5m", "15m"}


def load_checkpoint_models(
    *,
    rf_path: Path | str = config.CHECKPOINT_RF_MODEL_PATH,
    hgb_path: Path | str = config.CHECKPOINT_HGB_MODEL_PATH,
    manifest_path: Path | str = config.CHECKPOINT_MANIFEST_PATH,
) -> tuple[RfArtifact, HgbArtifact]:
    """Load the checkpoint RF + HGB, fail-closed on a truncated/mismatched build."""
    import joblib

    rfp, hgp, mfp = Path(rf_path), Path(hgb_path), Path(manifest_path)
    for label, p in (("RF", rfp), ("HGB", hgp), ("manifest", mfp)):
        if not p.exists():
            raise FileNotFoundError(f"checkpoint {label} artifact missing: {p}")

    manifest = json.loads(mfp.read_text(encoding="utf-8"))

    # --- RF: dict[(coin, interval, sec) -> forest] --------------------------
    raw_rf = joblib.load(rfp)
    if not isinstance(raw_rf, dict) or not raw_rf:
        raise ValueError("checkpoint RF: expected non-empty dict of estimators")
    estimators: dict[tuple[str, str, int], object] = {}
    for key, est in raw_rf.items():
        if not (isinstance(key, tuple) and len(key) == 3):
            raise ValueError(f"checkpoint RF key {key!r} not a 3-tuple")
        coin, interval, sec = key
        # Same predict-latency fix the retired rf_v1 loader carried: forests persisted with a
        # training n_jobs spawn a worker pool per 1-row predict otherwise.
        if hasattr(est, "n_jobs"):
            est.n_jobs = 1
        estimators[(str(coin).lower(), str(interval).lower(), int(sec))] = est

    n_slices = manifest.get("n_rf_slices")
    if n_slices is None:
        raise ValueError("checkpoint manifest missing 'n_rf_slices' — refusing to boot")
    if int(n_slices) != len(estimators):
        raise ValueError(
            f"checkpoint RF slice-count mismatch: manifest n_rf_slices={int(n_slices)} "
            f"but loaded {len(estimators)} — artifact looks truncated"
        )

    rf = RfArtifact(
        estimators=estimators,
        manifest=manifest,
        model_sha1=str(manifest.get("model_sha1", "checkpoint_v1")),
    )

    # --- HGB: dict[interval -> pipeline] ------------------------------------
    raw_hgb = joblib.load(hgp)
    if not isinstance(raw_hgb, dict) or not raw_hgb:
        raise ValueError("checkpoint HGB: expected non-empty dict of pipelines")
    pipelines = {str(k).lower(): v for k, v in raw_hgb.items()}
    if set(pipelines) != EXPECTED_INTERVALS:
        raise ValueError(
            f"checkpoint HGB interval keys {sorted(pipelines)} != {sorted(EXPECTED_INTERVALS)}"
        )

    mf_cols = manifest.get("feature_columns")
    if not isinstance(mf_cols, dict):
        raise ValueError("checkpoint manifest 'feature_columns' must be a per-interval dict")
    feature_columns: dict[str, list[str]] = {}
    for iv, pipe in pipelines.items():
        cols = list(mf_cols.get(iv) or [])
        if not cols:
            raise ValueError(f"checkpoint manifest {iv}: empty feature_columns")
        est = pipe.steps[-1][1] if hasattr(pipe, "steps") else pipe
        n_in = getattr(est, "n_features_in_", None)
        if n_in is not None and int(n_in) != len(cols):
            raise ValueError(
                f"checkpoint HGB {iv}: model n_features_in_={n_in} != "
                f"manifest feature count {len(cols)}"
            )
        feature_columns[iv] = cols

    hgb = HgbArtifact(
        pipelines=pipelines,
        feature_columns=feature_columns,
        manifest=manifest,
        rf_v1_model_sha1=str(manifest.get("rf_v1_model_sha1", "checkpoint_v1")),
    )

    logger.info(
        "checkpoint_v1 loaded: %d RF slices, HGB intervals=%s (staged_rows=%s, "
        "hgb_subsample=%s)",
        len(estimators), sorted(pipelines),
        manifest.get("staged_rows"), manifest.get("hgb_subsample_rows"),
    )
    return rf, hgb
