"""Boot-time parity guard: runtime hgb sleeve knobs == checkpoint manifest (#4).

``hgb_loader.validate_sleeves_against_manifest`` fail-closes when a runtime
sleeve's serve-time bet-selection knobs (threshold / ask_bucket /
book_consumption_max / side_filter / feature_groups) drift from the values
recorded in the manifest the model was selected on.
``checkpoint_loader.load_checkpoint_models`` already pins ``feature_columns``
(the model input contract); these knobs are NOT baked into the joblib pipeline,
so a config typo would otherwise pass every boot check and silently serve a band
the backtest never validated.

2026-07-31 exp044 retirement: the frozen minute-sliced ``hgb_v1`` artifact and
its ``load_hgb`` loader are gone, so the CHECKPOINT manifest is the one and only
contract and the validator takes it directly (no ``HgbArtifact`` argument). A
baseline (no ``checkpoint_offset_set``) hgb sleeve is rejected outright.

These tests build a SYNTHETIC manifest dict and REAL Sleeves from the deployed
v2 config, so they run without any gitignored joblib.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from cryptobot import config
from cryptobot.strategy.hgb_loader import validate_sleeves_against_manifest
from cryptobot.strategy.portfolio_loader import load_sleeves

_V2_CONFIG = str(config.V6_CONFIG_PATH)  # resolved: runtime/ -> legacy -> polymarket/live/config/


@pytest.fixture(scope="module")
def hgb_sleeves():
    # v2 is checkpoint-only; the loader fails closed unless SUBMINUTE_ENABLED.
    prev = config.SUBMINUTE_ENABLED
    config.SUBMINUTE_ENABLED = True
    try:
        port = load_sleeves(_V2_CONFIG)
    finally:
        config.SUBMINUTE_ENABLED = prev
    sleeves = [s for s in port.sleeves if s.signal_source == "hgb"]
    assert sleeves, "v2 config must define hgb sleeves"
    return sleeves


def _manifest_for(sleeves) -> dict:
    """Build a synthetic checkpoint manifest mirroring the runtime sleeves
    exactly (the in-parity baseline these tests then perturb)."""
    sleeve_manifest = {}
    for s in sleeves:
        # Reconstruct the manifest ``ask_bucket`` string from the parsed ranges
        # (single range: "name:lo:hi"). Name is cosmetic; the validator compares
        # parsed ranges, so any name works. Keyed BY LABEL (not interval) so two
        # checkpoint sleeves sharing an interval but serving different bands each
        # get their own contract — matching the label-first lookup.
        lo, hi = s.ask_bucket_ranges[0]
        sleeve_manifest[s.label] = {
            "threshold": float(s.threshold),
            "book_consumption_max": (
                None if s.book_consumption_max is None else float(s.book_consumption_max)
            ),
            "side_filter": str(s.side_filter),
            "ask_bucket": f"b:{lo}:{hi}",
            "feature_groups": str(s.feature_groups),
        }
    return {"sleeves": sleeve_manifest}


def test_in_parity_sleeves_pass(hgb_sleeves):
    """The deployed sleeves match a manifest built from themselves — no raise."""
    validate_sleeves_against_manifest(hgb_sleeves, _manifest_for(hgb_sleeves))


def _synthetic_second_band_sleeve(hgb_sleeves):
    """A second sleeve on the SAME interval serving a DIFFERENT band/gate (band
    (0.85, 0.92], gate 0.01), built off a real MID sleeve. The deployed artifact
    is MID-only, but the label-first manifest lookup must stay
    regression-covered — admitting a second same-interval sleeve would hit the
    exact July-8 crash-loop class otherwise."""
    mid = next(s for s in hgb_sleeves if s.label == "subminute_5m")
    return replace(
        mid,
        label="alt_5m",
        ask_bucket_ranges=((0.85, 0.92),),
        threshold=0.01,
    )


def test_two_same_interval_sleeves_validate_by_label(hgb_sleeves):
    """Two sleeves can share an interval while serving DIFFERENT bands/gates.
    Each must validate against its OWN label-keyed manifest entry — an
    interval-only lookup would collide two contracts on '5m'/'15m' and reject
    the second at boot (the exact July-8 crash-loop class). The second sleeve is
    synthetic here (the deployed artifact is MID-only)."""
    labels = {s.label for s in hgb_sleeves}
    assert {"subminute_5m", "subminute_15m"} <= labels, (
        f"v2 config must define the MID checkpoint sleeves; got {sorted(labels)}"
    )
    combined = list(hgb_sleeves) + [_synthetic_second_band_sleeve(hgb_sleeves)]
    # Label-keyed manifest resolves each sleeve's own contract -> no raise.
    validate_sleeves_against_manifest(combined, _manifest_for(combined))


def test_second_sleeve_without_label_key_fails_closed(hgb_sleeves):
    """If the checkpoint manifest omits the second sleeve's label key, that
    sleeve falls back to the interval key (the MID contract: gate 0.03 / band
    0.65:0.85) and must FAIL closed rather than silently serve its own band
    under the MID contract. Proves the label-first lookup is actually engaged
    (synthetic sleeve — see _synthetic_second_band_sleeve)."""
    alt = [_synthetic_second_band_sleeve(hgb_sleeves)]
    interval_only = {
        "sleeves": {
            "5m": {
                "threshold": 0.03, "ask_bucket": "b:0.65:0.85",
                "book_consumption_max": 0.3, "side_filter": "both",
                "feature_groups": "rf,price,identity,time",
            },
            "15m": {
                "threshold": 0.03, "ask_bucket": "b:0.65:0.85",
                "book_consumption_max": 0.06, "side_filter": "both",
                "feature_groups": "rf,price,identity,time",
            },
        }
    }
    with pytest.raises(ValueError):
        validate_sleeves_against_manifest(alt, interval_only)


def test_threshold_drift_rejected(hgb_sleeves):
    """The serve threshold is declared by the checkpoint manifest and compared
    hard — drift is rejected at boot, not warned about."""
    manifest = _manifest_for(hgb_sleeves)
    drifted = [replace(s, threshold=float(s.threshold) + 0.01) for s in hgb_sleeves]
    with pytest.raises(ValueError, match="threshold"):
        validate_sleeves_against_manifest(drifted, manifest)


def test_ask_bucket_drift_rejected(hgb_sleeves):
    """A widened ask_bucket (e.g. mid:0.4:0.7 vs trained 0.4:0.6) is the
    headline finding-#4 scenario: passes every other boot check, serves an
    unvalidated band. Must be rejected."""
    manifest = _manifest_for(hgb_sleeves)
    drifted = []
    for s in hgb_sleeves:
        lo, hi = s.ask_bucket_ranges[0]
        drifted.append(replace(s, ask_bucket_ranges=((lo, hi + 0.1),)))
    with pytest.raises(ValueError, match="ask_bucket"):
        validate_sleeves_against_manifest(drifted, manifest)


def test_book_consumption_drift_rejected(hgb_sleeves):
    manifest = _manifest_for(hgb_sleeves)
    drifted = [replace(s, book_consumption_max=0.05) for s in hgb_sleeves]
    with pytest.raises(ValueError, match="book_consumption_max"):
        validate_sleeves_against_manifest(drifted, manifest)


def test_side_filter_drift_rejected(hgb_sleeves):
    manifest = _manifest_for(hgb_sleeves)
    drifted = [replace(s, side_filter="up") for s in hgb_sleeves]
    with pytest.raises(ValueError, match="side_filter"):
        validate_sleeves_against_manifest(drifted, manifest)


def test_missing_manifest_entry_rejected(hgb_sleeves):
    """A sleeve whose label AND interval have no manifest entry fails closed."""
    with pytest.raises(ValueError, match="no 'sleeves' entry"):
        validate_sleeves_against_manifest(hgb_sleeves, {"sleeves": {}})


def test_baseline_hgb_sleeve_rejected(hgb_sleeves):
    """exp044 retirement: an hgb sleeve with no checkpoint_offset_set has no
    model to serve it any more and must be rejected at boot, not scored."""
    baseline = [replace(s, checkpoint_offset_set=()) for s in hgb_sleeves]
    with pytest.raises(ValueError, match="BASELINE"):
        validate_sleeves_against_manifest(baseline, _manifest_for(hgb_sleeves))


def test_non_hgb_sleeves_skipped(hgb_sleeves):
    """Legacy lut/rf/blend sleeves are not HGB-served, so they're ignored
    even when the manifest has no entry for them."""
    lut_like = [replace(s, signal_source="lut") for s in hgb_sleeves]
    validate_sleeves_against_manifest(lut_like, {"sleeves": {}})  # must not raise
