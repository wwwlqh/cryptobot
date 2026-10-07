"""Load and validate the direct TWAP-symmetric HGB serving artifact."""

from __future__ import annotations

import json
import hashlib
import logging
import math
from pathlib import Path

from cryptobot import config
from cryptobot.strategy.hgb_loader import HgbArtifact

logger = logging.getLogger(__name__)

EXPECTED_INTERVALS = {"5m", "15m"}
LIVE_FEATURE_CONTRACT = "binance_trailing_twap_centered_chainlink_basis_v2"
EXPECTED_CHAINLINK_WINDOWS = {"5m": 60, "15m": 60}
EXPECTED_BINANCE_WINDOWS = {"5m": 60, "15m": 60}
EXPECTED_DECISION_WINDOWS = {"5m": 150, "15m": 150}
EXPECTED_CHECKPOINT_OFFSETS = {
    "5m": tuple(range(150, 300)),
    "15m": tuple(range(750, 900)),
}
EXPECTED_BASIS_RESIDUAL_CLIP_LOG = 0.0001
FEATURE_COLUMNS = [
    "ask",
    "other_ask",
    "book_spread",
    "coin_id",
    "interval_min",
    "side_up",
    "entry_minute",
    "hour_utc",
    "dow_utc",
    "partial_ret_z_side",
    "abs_partial_ret_z",
]


def validate_twap_coin_universe(
    scanner_coins, configured_coins, manifest: dict
) -> None:
    """Fail boot unless the scanner can visit exactly the approved coins."""
    scanner = {str(coin).upper() for coin in scanner_coins}
    approved = {str(coin).upper() for coin in configured_coins}
    frozen = {str(coin).upper() for coin in manifest.get("live_coins", [])}
    if not approved or scanner != approved or approved != frozen:
        raise ValueError(
            "TWAP scanner coin universe "
            f"{sorted(scanner)} != live contract {sorted(approved)} "
            f"!= manifest {sorted(frozen)}"
        )


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def twap_feature_params(
    manifest: dict, *, coin: str, interval: str
) -> tuple[float, float]:
    """Read one frozen live feature centre and clip from the model manifest."""
    contract = manifest.get("live_feature_contract") or {}
    if contract.get("name") != LIVE_FEATURE_CONTRACT:
        raise ValueError("TWAP symmetric live feature contract is missing or wrong")
    if contract.get("binance_window_seconds") != EXPECTED_BINANCE_WINDOWS:
        raise ValueError("TWAP symmetric Binance windows must be 60s for both intervals")
    try:
        clip = float(contract["basis_residual_clip_log"])
        centre = float(
            contract["basis_center_log"][str(coin).lower()][str(interval).lower()]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"TWAP symmetric feature centre missing for {coin}:{interval}"
        ) from exc
    if (
        not math.isfinite(clip)
        or abs(clip - EXPECTED_BASIS_RESIDUAL_CLIP_LOG) > 1e-12
        or not math.isfinite(centre)
    ):
        raise ValueError("TWAP symmetric basis centre/clip is invalid")
    return centre, clip


def validate_twap_feature_contract(manifest: dict) -> None:
    """Fail boot unless every approved live coin has both frozen centres."""
    approved = {str(coin).lower() for coin in manifest.get("live_coins", [])}
    if not approved:
        raise ValueError("TWAP symmetric live feature contract has no live coins")
    contract = manifest.get("live_feature_contract") or {}
    centres = contract.get("basis_center_log") or {}
    if set(centres) != approved:
        raise ValueError(
            "TWAP symmetric basis-centre coins "
            f"{sorted(centres)} != live coins {sorted(approved)}"
        )
    for coin in sorted(approved):
        if set(centres.get(coin, {})) != EXPECTED_INTERVALS:
            raise ValueError(f"TWAP symmetric basis centres incomplete for {coin}")
        for interval in sorted(EXPECTED_INTERVALS):
            twap_feature_params(manifest, coin=coin, interval=interval)


def twap_execution_params(
    manifest: dict, *, interval: str
) -> tuple[tuple[tuple[float, float], ...], float]:
    """Return the frozen all-four-coin ask band and edge gate."""
    interval_key = str(interval).lower()
    try:
        spec = manifest["execution_by_interval"][interval_key]
        ask_lo = float(spec["ask_lo"])
        ask_hi = float(spec["ask_hi"])
        threshold = float(spec["gate"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"TWAP execution contract missing for {interval_key}") from exc
    if not (
        math.isfinite(ask_lo)
        and math.isfinite(ask_hi)
        and 0.0 <= ask_lo < ask_hi < 1.0
        and math.isfinite(threshold)
        and threshold >= 0.0
    ):
        raise ValueError(
            f"TWAP execution contract invalid for {interval_key}"
        )
    return ((ask_lo, ask_hi),), threshold


def validate_twap_sleeves(sleeves, manifest: dict) -> None:
    """Fail boot if serving knobs drift from the WFO-selected contract."""
    contracts = manifest.get("sleeves") or {}
    hgb_sleeves = [
        sleeve for sleeve in sleeves
        if getattr(sleeve, "signal_source", None) == "hgb"
    ]
    intervals = {str(sleeve.interval).lower() for sleeve in hgb_sleeves}
    if intervals != EXPECTED_INTERVALS or set(contracts) != EXPECTED_INTERVALS:
        raise ValueError(
            f"TWAP symmetric intervals {sorted(intervals)} / "
            f"{sorted(contracts)} != {sorted(EXPECTED_INTERVALS)}"
        )
    for sleeve in hgb_sleeves:
        interval = str(sleeve.interval).lower()
        contract = contracts.get(interval)
        if not isinstance(contract, dict):
            raise ValueError(f"TWAP symmetric manifest has no {interval} sleeve")
        threshold = float(contract["threshold"])
        if sleeve.threshold is None or abs(float(sleeve.threshold) - threshold) > 1e-12:
            raise ValueError(
                f"TWAP symmetric {interval} threshold {sleeve.threshold} != {threshold}"
            )
        pieces = str(contract["ask_bucket"]).split(":")
        if len(pieces) != 3:
            raise ValueError(f"TWAP symmetric {interval} ask bucket is malformed")
        expected_ranges = ((float(pieces[1]), float(pieces[2])),)
        if tuple(sleeve.ask_bucket_ranges) != expected_ranges:
            raise ValueError(
                f"TWAP symmetric {interval} ask ranges "
                f"{sleeve.ask_bucket_ranges} != {expected_ranges}"
            )
        side_filter = str(contract.get("side_filter", "both")).lower()
        if str(sleeve.side_filter).lower() != side_filter:
            raise ValueError(
                f"TWAP symmetric {interval} side_filter "
                f"{sleeve.side_filter!r} != {side_filter!r}"
            )
        book_consumption_max = float(contract["book_consumption_max"])
        if (
            sleeve.book_consumption_max is None
            or abs(
                float(sleeve.book_consumption_max) - book_consumption_max
            )
            > 1e-12
        ):
            raise ValueError(
                f"TWAP symmetric {interval} book_consumption_max "
                f"{sleeve.book_consumption_max} != {book_consumption_max}"
            )
        expected_offsets = EXPECTED_CHECKPOINT_OFFSETS[interval]
        if tuple(sleeve.checkpoint_offset_set) != expected_offsets:
            raise ValueError(
                f"TWAP symmetric {interval} checkpoint offsets "
                f"{tuple(sleeve.checkpoint_offset_set)} != {expected_offsets}"
            )
        interval_specs = manifest.get("execution_by_interval") or {}
        if set(interval_specs) != EXPECTED_INTERVALS:
            raise ValueError(
                "TWAP symmetric shared execution contract is incomplete"
            )
        sleeve_lo, sleeve_hi = expected_ranges[0]
        shared_ranges, shared_threshold = twap_execution_params(
            manifest, interval=interval
        )
        shared_lo, shared_hi = shared_ranges[0]
        if (shared_lo, shared_hi) != (sleeve_lo, sleeve_hi):
            raise ValueError(
                f"TWAP symmetric {interval} shared ask range is outside sleeve"
            )
        if abs(shared_threshold - threshold) > 1e-12:
            raise ValueError(
                f"TWAP symmetric {interval} shared threshold "
                f"{shared_threshold} != {threshold}"
            )


def load_twap_symmetric_model(
    *,
    model_path: Path | str = config.TWAP_SYMMETRIC_MODEL_PATH,
    manifest_path: Path | str = config.TWAP_SYMMETRIC_MANIFEST_PATH,
) -> HgbArtifact:
    """Load the final all-history model, rejecting any contract mismatch."""
    import joblib

    model_p = Path(model_path)
    manifest_p = Path(manifest_path)
    try:
        manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"TWAP symmetric manifest missing: {manifest_p}"
        ) from exc
    if manifest.get("artifact") != "twap_symmetric_v1":
        raise ValueError("TWAP symmetric manifest has the wrong artifact id")
    if manifest.get("contract_version") != 3:
        raise ValueError("TWAP symmetric manifest must use contract version 3")
    if manifest.get("target") != "win_twap_s":
        raise ValueError("TWAP symmetric manifest target must be win_twap_s")
    expected_hash = str(manifest.get("model_sha256") or "")
    try:
        actual_hash = _sha256(model_p)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"TWAP symmetric model missing: {model_p}") from exc
    if not expected_hash or actual_hash != expected_hash:
        raise ValueError("TWAP symmetric model SHA-256 does not match manifest")

    try:
        raw = joblib.load(model_p)
    except FileNotFoundError as exc:  # file removed after hash validation
        raise FileNotFoundError(f"TWAP symmetric model missing: {model_p}") from exc
    if not isinstance(raw, dict) or not raw:
        raise ValueError("TWAP symmetric model must be a non-empty interval dict")
    pipelines = {str(key).lower(): value for key, value in raw.items()}
    if set(pipelines) != EXPECTED_INTERVALS:
        raise ValueError(
            f"TWAP symmetric intervals {sorted(pipelines)} != "
            f"{sorted(EXPECTED_INTERVALS)}"
        )
    if manifest.get("model_count") != 2:
        raise ValueError("TWAP symmetric manifest must contain exactly two models")
    if manifest.get("model_routing") != {
        "5m": "pooled_all4",
        "15m": "pooled_all4",
    }:
        raise ValueError("TWAP symmetric models must pool all four coins")
    if manifest.get("categorical_features") != {
        "5m": ["coin_id"],
        "15m": ["coin_id"],
    }:
        raise ValueError("TWAP symmetric coin_id must be categorical")

    raw_columns = manifest.get("feature_columns")
    if not isinstance(raw_columns, dict):
        raise ValueError("TWAP symmetric manifest feature_columns must be a dict")
    feature_columns: dict[str, list[str]] = {}
    for interval, pipeline in pipelines.items():
        columns = list(raw_columns.get(interval) or [])
        if columns != FEATURE_COLUMNS:
            raise ValueError(
                f"TWAP symmetric {interval} feature contract {columns} != "
                f"{FEATURE_COLUMNS}"
            )
        estimator = pipeline.steps[-1][1] if hasattr(pipeline, "steps") else pipeline
        n_features = getattr(estimator, "n_features_in_", None)
        if n_features is not None and int(n_features) != len(columns):
            raise ValueError(
                f"TWAP symmetric {interval} model has {n_features} features, "
                f"manifest has {len(columns)}"
            )
        categorical = getattr(estimator, "is_categorical_", None)
        if (
            categorical is None
            or not bool(categorical[FEATURE_COLUMNS.index("coin_id")])
        ):
            raise ValueError(
                f"TWAP symmetric {interval} model does not treat coin_id as categorical"
            )
        feature_columns[interval] = columns

    windows = manifest.get("chainlink_window_seconds") or {}
    if windows != EXPECTED_CHAINLINK_WINDOWS:
        raise ValueError(
            "TWAP symmetric manifest must use 60s Chainlink windows for both intervals"
        )
    decision_windows = manifest.get("decision_window_seconds") or {}
    if decision_windows != EXPECTED_DECISION_WINDOWS:
        raise ValueError(
            "TWAP symmetric manifest must use T-150 windows for both intervals"
        )
    validate_twap_feature_contract(manifest)

    artifact = HgbArtifact(
        pipelines=pipelines,
        feature_columns=feature_columns,
        manifest=manifest,
        rf_v1_model_sha1="twap_symmetric_v1_no_rf",
    )
    logger.info(
        "twap_symmetric_v1 loaded: intervals=%s train_rows=%s train_end=%s",
        sorted(pipelines),
        manifest.get("train_rows"),
        manifest.get("max_entry_dt_utc"),
    )
    return artifact
