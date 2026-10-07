"""Checkpoint sub-minute DECISION parity (2026-07-05 parity port).

The 2026-07-05 audit found live scored the sub-minute checkpoint sleeves at the
``offset//60`` integer minute against the frozen minute-sliced ``rf_v1``/``hgb_v1``
— a materially different strategy than the backtest's per-checkpoint-second
models. These tests are the deploy GATE for the fix: they assert that

  1. ``score_cell_hgb`` on a checkpoint cell (``offset_sec`` set) reproduces a
     DIRECT replay against the frozen ``checkpoint_v1`` models with a FRACTIONAL
     ``entry_minute`` (offset/60) and the per-SECOND RF slice, and
  2. the fractional ``entry_minute`` actually reaches the HGB (dropping the old
     ``float(int(...))`` cast changes the model output), and
  3. the per-second RF resolves DISTINCTLY within a minute (289 != 290), which
     the old integer-minute path collapsed.

Skipped when the (gitignored, 614 MB) ``checkpoint_v1`` artifact is not built.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from cryptobot import config
from cryptobot.strategy import hgb_loader, rf_loader
from cryptobot.strategy.hgb_features import build_feature_vector
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.strategy.score import _fee_adjusted_edge, score_cell_hgb
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._fixtures import seed_for_tests

_CKPT = Path("data/models/checkpoint_v1/checkpoint_rf_v1.joblib")
pytestmark = pytest.mark.skipif(
    not _CKPT.exists(), reason="checkpoint_v1 artifact not built (data/models/checkpoint_v1/)"
)

_TS = 1_715_923_200  # divisible by 300 (5m window boundary)
_SPOT = 67_000.0
_ARTIFACT = str(config.V6_CONFIG_PATH)  # resolved: runtime/ -> legacy -> polymarket/live/config/


@pytest.fixture(scope="module")
def ckpt():
    from cryptobot.strategy.checkpoint_loader import load_checkpoint_models

    return load_checkpoint_models()  # (RfArtifact, HgbArtifact)


@pytest.fixture(scope="module")
def s5():
    from cryptobot import config

    # v2 is checkpoint-only; the loader fails closed unless SUBMINUTE_ENABLED.
    prev = config.SUBMINUTE_ENABLED
    config.SUBMINUTE_ENABLED = True
    try:
        portfolio = load_sleeves(_ARTIFACT)
    finally:
        config.SUBMINUTE_ENABLED = prev
    sleeve = next(s for s in portfolio.sleeves if s.interval == "5m")
    # Widen the ask-bucket so the test asks always produce a candidate — the
    # bucket is applied identically to both the live and replay paths, so it is
    # orthogonal to the orchestration parity being tested.
    return replace(sleeve, ask_bucket_ranges=((0.0, 1.0),))


def _warm() -> PartialRetEngine:
    eng = PartialRetEngine()
    seed_for_tests(eng, "btc", now_ts=_TS, spot=_SPOT)
    return eng


def _replay_best(ckpt_rf, ckpt_hgb, *, sec: int, z: float, yes_ask: float, no_ask: float):
    """Direct frozen-model replay: fractional entry_minute + per-second RF."""
    rf_p = rf_loader.predict_proba(
        ckpt_rf, coin="btc", interval="5m", entry_minute=sec, partial_ret_z_1h=z
    )
    cols = ckpt_hgb.columns_for("5m")
    best = None  # (edge, model_prob)
    for feat_side, ask, other in (("Up", yes_ask, no_ask), ("Down", no_ask, yes_ask)):
        feat = build_feature_vector(
            side=feat_side, coin="btc", interval="5m", interval_min=5,
            entry_minute=sec / 60.0, window_start_ts=_TS, model_p_up=rf_p,
            ask=ask, other_ask=other, partial_ret_z_1h=z, feature_columns=cols,
        )
        mp = hgb_loader.predict_model_prob(ckpt_hgb, interval="5m", feature_row_values=feat)
        edge = _fee_adjusted_edge(mp, ask)
        if best is None or (edge, mp) > (best[0], best[1]):
            best = (edge, mp)
    return best  # (edge, model_prob)


def test_checkpoint_score_matches_frozen_model_replay(ckpt, s5) -> None:
    ckpt_rf, ckpt_hgb = ckpt
    sec, live_spot, yes_ask, no_ask = 290, 67_500.0, 0.55, 0.46

    res = score_cell_hgb(
        coin="btc", sleeve=s5, entry_minute=sec // 60, window_start_ts=_TS,
        live_spot=live_spot, yes_ask=yes_ask, no_ask=no_ask,
        partial_ret_engine=_warm(),
        offset_sec=sec, checkpoint_rf=ckpt_rf, checkpoint_hgb=ckpt_hgb,
    )
    assert res.skip_reason is None, f"unexpected skip: {res.skip_reason}"
    assert res.partial_ret_z_1h is not None

    edge, model_prob = _replay_best(
        ckpt_rf, ckpt_hgb, sec=sec, z=res.partial_ret_z_1h, yes_ask=yes_ask, no_ask=no_ask
    )
    # If score_cell_hgb passed int(290/60)=4 anywhere (RF slice OR HGB feature),
    # these would diverge — this is the exact bug the port fixes.
    assert res.model_p == pytest.approx(model_prob, rel=1e-9)
    assert res.edge == pytest.approx(edge, rel=1e-9)


def test_per_second_rf_resolves_distinctly(ckpt) -> None:
    """The old offset//60 path collapsed 240-299s onto one minute-4 forest."""
    ckpt_rf, _ = ckpt
    p289 = rf_loader.predict_proba(ckpt_rf, coin="btc", interval="5m", entry_minute=289, partial_ret_z_1h=0.5)
    p290 = rf_loader.predict_proba(ckpt_rf, coin="btc", interval="5m", entry_minute=290, partial_ret_z_1h=0.5)
    assert p289 is not None and p290 is not None
    assert p289 != p290  # distinct per-second forests, not one collapsed minute


def test_fractional_entry_minute_reaches_hgb(ckpt) -> None:
    """The HGB output must vary with entry_minute — proof the fractional minute
    is not silently re-quantized (a single split point could coincidentally
    match, so check variation across the domain, not one pair)."""
    ckpt_rf, ckpt_hgb = ckpt
    cols = ckpt_hgb.columns_for("5m")
    rf_p = rf_loader.predict_proba(ckpt_rf, coin="btc", interval="5m", entry_minute=290, partial_ret_z_1h=0.5)
    kw = dict(
        side="Up", coin="btc", interval="5m", interval_min=5, window_start_ts=_TS,
        model_p_up=rf_p, ask=0.55, other_ask=0.46, partial_ret_z_1h=0.5, feature_columns=cols,
    )
    outs = {
        round(hgb_loader.predict_model_prob(
            ckpt_hgb, interval="5m", feature_row_values=build_feature_vector(entry_minute=em, **kw)
        ), 10)
        for em in (0.5, 2.5, 4.0, 4.5, 4.8333)
    }
    assert len(outs) > 1  # entry_minute is a live feature; fractional values matter
