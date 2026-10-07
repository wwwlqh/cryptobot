"""A cheap stand-in scorer for the pipeline-mechanic tests.

`test_pipeline_tick`, `test_pipeline_live_routing`, `test_e2e_topup_fixture`
and `test_phase_b_parallel_post` test the TICK (gate order, dedup, exposure
caps, sizing, LIVE routing, top-up walks), not the model. They install one
scorer stub with fixed probabilities so no model file has to load:

    model_p_up      = 0.84 on 5m, 0.80 on 15m
    yes_edge        = p_up       - yes_ask - theta*yes_ask*(1-yes_ask)
    no_edge         = (1 - p_up) - no_ask  - theta*no_ask *(1-no_ask)
    side            = the higher-edge side (ties -> YES)
    realized_edge   = that edge - sleeve.slippage_haircut

The window's opening price comes from the partial-return engine instead of
the Chainlink TWAP mirror, so the tests need no WebSocket feed. Real TWAP
scoring is covered by `test_twap_scoring.py`.
"""

from __future__ import annotations

from typing import Optional

from cryptobot.strategy.score import ScoreResult, _fee_adjusted_edge

#: model_p_up the stub serves per interval (see the module docstring).
STUB_P_UP: dict[str, float] = {"5m": 0.84, "15m": 0.80}

#: LUT columns are dead sentinels on every real ScoreResult now; the stub
#: reproduces the values the retired scorer wrote so signals-row assertions
#: that read them keep their expectations.
_STUB_BUCKET = 8
_STUB_HIT_RATE = 0.88
_STUB_N_CUM = 1000


def fake_minute_scorer(
    *,
    coin: str,
    sleeve,
    entry_minute,
    window_start_ts: int,
    live_spot: float,
    yes_ask: float,
    no_ask: float,
    deps,
    offset_sec: Optional[int] = None,
    twap_open: Optional[float] = None,
    fee_rate_bps: Optional[float] = None,
) -> ScoreResult:
    """Drop-in for ``pipeline._score_cell_for``."""
    z = deps.partial_ret_engine.current_z(
        coin,
        interval_min=sleeve.interval_min,
        live_spot=live_spot,
        entry_minute=float(entry_minute),
    )
    if z is None:
        return ScoreResult(
            model_p=float("nan"), side="YES", edge=float("nan"),
            realized_edge=float("nan"),
            lut_p_bucket=-1, lut_hit_rate=None, lut_n_cum=0,
            partial_ret_z_1h=None, skip_reason="no_feature",
        )

    p_up = STUB_P_UP.get(sleeve.interval, 0.80)
    yes_edge = _fee_adjusted_edge(p_up, yes_ask)
    no_edge = _fee_adjusted_edge(1.0 - p_up, no_ask)
    if yes_edge >= no_edge:
        side, model_p, raw_edge = "YES", p_up, yes_edge
    else:
        side, model_p, raw_edge = "NO", 1.0 - p_up, no_edge

    return ScoreResult(
        model_p=model_p,
        side=side,
        edge=raw_edge,
        realized_edge=raw_edge - sleeve.slippage_haircut,
        lut_p_bucket=_STUB_BUCKET,
        lut_hit_rate=_STUB_HIT_RATE,
        lut_n_cum=_STUB_N_CUM,
        partial_ret_z_1h=z,
        skip_reason=None,
    )


def engine_window_open(deps, *, coin: str, sleeve, window_start_ts: int):
    """Window opening price from the partial-return engine (no TWAP mirror)."""
    return deps.partial_ret_engine.window_open_spot(coin, sleeve.interval_min)


def install_fake_scorer(monkeypatch) -> None:
    """Patch the stub scorer and engine-based window open over the pipeline."""
    from cryptobot.execution import pipeline

    monkeypatch.setattr(pipeline, "_score_cell_for", fake_minute_scorer)
    monkeypatch.setattr(pipeline, "_window_open_price", engine_window_open)
