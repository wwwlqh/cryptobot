"""A cheap stand-in scorer for the pipeline-mechanic tests.

`test_pipeline_tick`, `test_pipeline_live_routing` and `test_e2e_topup_fixture`
test the TICK — gate order, dedup, exposure caps, sizing, LIVE routing, top-up
walks — not the model. They always drove it with a stub RF forest plus a
synthetic LUT table so no real artifact had to load, and asserted exact edges
and bet sizes off that stub's fixed probabilities.

The lut|rf|blend scorer those stubs fed was retired with the pair sleeves on
2026-07-31, and its replacement (`score_cell_hgb`) is CHECKPOINT-only: it fails
closed without an `offset_sec`, and a checkpoint sleeve routes the tick down the
sub-minute spec_v2 path with a different gate stack. So instead of two artifact
stubs these modules install ONE scorer stub, which reproduces the retired
`score.score_cell` arithmetic to the digit:

    model_p_up      = 0.84 on 5m   (the old 0.5*rf(0.80) + 0.5*lut(0.88) blend)
                    = 0.80 on 15m  (the old rf-sourced sleeve)
    yes_edge        = p_up       - yes_ask - theta*yes_ask*(1-yes_ask)
    no_edge         = (1 - p_up) - no_ask  - theta*no_ask *(1-no_ask)
    side            = the higher-edge side (ties -> YES), model_signed
    realized_edge   = that edge - sleeve.slippage_haircut

Every edge / side / bet-size assertion in those modules therefore keeps the
value it had before the retirement.

Real scoring is covered against the real `checkpoint_v1` bundle by
`test_hgb_scoring.py`, and the sub-minute tick path by `test_subminute.py`.
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
    partial_ret_engine,
    offset_sec: Optional[int] = None,
    checkpoint_rf=None,
    checkpoint_hgb=None,
    fee_rate_bps: Optional[float] = None,
) -> ScoreResult:
    """Drop-in for ``pipeline.score_cell_hgb`` — see the module docstring.

    Signature-compatible with the real scorer (the pipeline passes everything
    by keyword) so it can be monkeypatched straight onto ``pipeline``.

    ``fee_rate_bps`` is the resolved market's predict.fun ``feeRateBps``,
    threaded through since the 2026-08-01 venue fee dispatch. It is forwarded
    to ``_fee_adjusted_edge`` exactly like the real scorer does; on the
    Polymarket venue these tests run under it is ignored (the parabola takes
    ``theta``), so every edge / side / bet-size assertion keeps its value.
    """
    z = partial_ret_engine.current_z(
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
    yes_edge = _fee_adjusted_edge(p_up, yes_ask, fee_rate_bps=fee_rate_bps)
    no_edge = _fee_adjusted_edge(1.0 - p_up, no_ask, fee_rate_bps=fee_rate_bps)
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


def install_fake_scorer(monkeypatch) -> None:
    """Patch ``fake_minute_scorer`` over the pipeline's scorer for one test."""
    from cryptobot.execution import pipeline

    monkeypatch.setattr(pipeline, "score_cell_hgb", fake_minute_scorer)
