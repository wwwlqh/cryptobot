from __future__ import annotations

import numpy as np
import pytest
from decimal import Decimal
from types import SimpleNamespace

from cryptobot.strategy.hgb_loader import HgbArtifact
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.strategy.portfolio_loader import Sleeve
from cryptobot.strategy.score import score_cell_twap
from cryptobot.strategy.twap_loader import FEATURE_COLUMNS
import cryptobot.execution.pipeline as pipeline
from cryptobot.execution.pipeline import _score_cell_for, _window_open_price


class _SideAwarePipeline:
    def __init__(self):
        self.frames = []

    def predict_proba(self, frame):
        self.frames.append(frame.copy())
        p = np.where(frame["side_up"].to_numpy() == 1.0, 0.92, 0.75)
        return np.column_stack([1.0 - p, p])


def _sleeve() -> Sleeve:
    return Sleeve(
        label="subminute_5m",
        alloc=0.5,
        interval="5m",
        interval_min=5,
        side_strategy="model_signed",
        entry_minute_set=(),
        entry_strategy="first_qualifying",
        min_edge=0.0,
        max_edge=1.0,
        min_fill_price=0.60,
        max_fill_price=0.85,
        min_agreement=0,
        capital_frac=0.01,
        max_walk_price=0.99,
        slippage_haircut=0.0,
        min_lut_n=0,
        signal_source="hgb",
        hour_bucket_set="all",
        dow_set="all",
        threshold=0.06,
        feature_groups="price,identity,time,momentum",
        ask_bucket="band:0.60:0.85",
        ask_bucket_ranges=((0.60, 0.85),),
        side_filter="both",
        checkpoint_offset_set=tuple(range(270, 300)),
    )


def _artifact(pipe) -> HgbArtifact:
    return HgbArtifact(
        pipelines={"5m": pipe, "15m": pipe},
        feature_columns={"5m": FEATURE_COLUMNS, "15m": FEATURE_COLUMNS},
        manifest={
            "chainlink_window_seconds": {"5m": 60, "15m": 60},
            "live_feature_contract": {
                "name": "binance_trailing_twap_centered_chainlink_basis_v2",
                "binance_window_seconds": {"5m": 60, "15m": 60},
                "basis_residual_clip_log": 0.0001,
                "basis_center_log": {
                    "btc": {"5m": 0.0, "15m": 0.0},
                },
            },
            "execution_by_interval": {
                "5m": {"ask_lo": 0.55, "ask_hi": 0.85, "gate": 0.10},
                "15m": {"ask_lo": 0.55, "ask_hi": 0.85, "gate": 0.10},
            },
        },
        rf_v1_model_sha1="no-rf",
    )


def _engine() -> PartialRetEngine:
    engine = PartialRetEngine()
    state = engine._coin("btc")
    state.rv_1h_at_open_5m = 0.002
    state.window_start_twap_5m = 100.0
    return engine


def test_twap_scorer_uses_symmetric_z_and_best_trend_side(monkeypatch):
    pipe = _SideAwarePipeline()

    result = score_cell_twap(
        coin="btc",
        sleeve=_sleeve(),
        window_start_ts=1_776_860_800,
        live_spot=101.0,
        open_twap=100.0,
        yes_ask=0.70,
        no_ask=0.65,
        partial_ret_engine=_engine(),
        offset_sec=270,
        twap_hgb=_artifact(pipe),
    )

    assert result.skip_reason is None
    assert result.side == "YES"
    assert result.model_p == pytest.approx(0.92)
    assert len(pipe.frames) == 2
    assert pipe.frames[0]["partial_ret_z_side"].iloc[0] == pytest.approx(
        -pipe.frames[1]["partial_ret_z_side"].iloc[0]
    )
    assert pipe.frames[0]["abs_partial_ret_z"].iloc[0] == pytest.approx(
        pipe.frames[1]["abs_partial_ret_z"].iloc[0]
    )


def test_twap_scorer_fails_closed_without_exact_open_anchor():
    result = score_cell_twap(
        coin="btc",
        sleeve=_sleeve(),
        window_start_ts=1_776_860_800,
        live_spot=101.0,
        open_twap=None,
        yes_ask=0.70,
        no_ask=0.65,
        partial_ret_engine=_engine(),
        offset_sec=270,
        twap_hgb=_artifact(_SideAwarePipeline()),
    )

    assert result.skip_reason == "twap_open_missing"
    assert np.isnan(result.model_p)


def test_twap_scorer_enforces_shared_interval_ask_floor():
    artifact = _artifact(_SideAwarePipeline())
    artifact.manifest["execution_by_interval"]["5m"]["ask_lo"] = 0.70
    result = score_cell_twap(
        coin="btc",
        sleeve=_sleeve(),
        window_start_ts=1_776_860_800,
        live_spot=101.0,
        open_twap=100.0,
        yes_ask=0.70,
        no_ask=0.65,
        partial_ret_engine=_engine(),
        offset_sec=160,
        twap_hgb=artifact,
    )

    assert result.skip_reason == "ask_bucket"
    assert np.isnan(result.model_p)


def test_twap_scorer_disarms_any_non_sixty_second_contract():
    artifact = _artifact(_SideAwarePipeline())
    artifact.manifest["chainlink_window_seconds"]["5m"] = 30
    result = score_cell_twap(
        coin="btc",
        sleeve=_sleeve(),
        window_start_ts=1_786_665_600,
        live_spot=101.0,
        open_twap=100.0,
        yes_ask=0.70,
        no_ask=0.65,
        partial_ret_engine=_engine(),
        offset_sec=240,
        twap_hgb=artifact,
    )

    assert result.skip_reason == "twap_window_contract_mismatch"
    assert np.isnan(result.model_p)


def test_pipeline_persists_the_same_exact_twap_anchor(monkeypatch):
    class Mirror:
        calls = 0

        def get_opening_twap(self, coin, interval, boundary_ts):
            assert (coin, interval, boundary_ts) == ("btc", "5m", 1_776_860_800)
            self.calls += 1
            value = "100.125" if self.calls == 1 else "999.0"
            return SimpleNamespace(value=Decimal(value))

    mirror = Mirror()
    deps = SimpleNamespace(
        twap_mirror=mirror,
        partial_ret_engine=_engine(),
        twap_hgb_artifact=_artifact(_SideAwarePipeline()),
    )

    captured = _window_open_price(
        deps,
        coin="btc",
        sleeve=_sleeve(),
        window_start_ts=1_776_860_800,
    )
    result = _score_cell_for(
        coin="btc",
        sleeve=_sleeve(),
        entry_minute=4,
        window_start_ts=1_776_860_800,
        live_spot=101.0,
        yes_ask=0.70,
        no_ask=0.65,
        offset_sec=270,
        deps=deps,
        twap_open=captured,
    )

    assert captured == pytest.approx(100.125)
    assert result.skip_reason is None
    assert mirror.calls == 1


def test_resolver_carries_captured_twap_to_rescore_and_ledger(monkeypatch):
    class Mirror:
        def get_opening_twap(self, *args, **kwargs):
            raise AssertionError("resolver must not re-read a captured TWAP")

    monkeypatch.setattr(pipeline, "_venue_market_cache_has", lambda *a: False)
    captured_kwargs = []
    frozen_score = SimpleNamespace(skip_reason=None, side="YES")

    def rescore(**kwargs):
        captured_kwargs.append(kwargs)
        return frozen_score

    monkeypatch.setattr(pipeline, "_score_cell_for", rescore)
    market = SimpleNamespace(
        yes_token_id="yes",
        no_token_id="no",
        condition_id="condition",
        fee_rate_bps=0.0,
    )
    deps = SimpleNamespace(
        conn_factory=None,
        fetch_market=lambda *a, **k: market,
        order_client=None,
        book_prefetch=None,
        fetch_books=lambda *a, **k: {
            "yes": SimpleNamespace(top_ask=0.70),
            "no": SimpleNamespace(top_ask=0.65),
        },
        fetch_book=None,
        book_shadow=None,
        twap_mirror=Mirror(),
    )
    cell = pipeline._NetCell(
        sleeve=_sleeve(),
        entry_minute=4,
        window_start=1_776_860_800,
        window_end=1_776_861_100,
        coin="btc",
        spot=SimpleNamespace(price=101.0),
        spot_age_s=0.1,
        score=frozen_score,
        twap_open=100.125,
        offset_sec=270,
    )

    decisions = pipeline._resolve_net_cells(
        [cell],
        conn=None,
        deps=deps,
        portfolio=SimpleNamespace(),
        now_ts=1_776_861_070,
        tick=pipeline.TickResult(tick_ts=1_776_861_070),
    )

    assert captured_kwargs[0]["twap_open"] == pytest.approx(100.125)
    assert decisions[0].window_open_spot == pytest.approx(100.125)
