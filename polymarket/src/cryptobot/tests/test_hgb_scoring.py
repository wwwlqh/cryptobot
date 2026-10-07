"""v2 HGB live-scoring tests (PHASE 4 of the HGB live port).

Covers:
  - ``score.score_cell_hgb`` skip/sentinel/best-side paths.
  - ``gates._gate_threshold`` / ``gates._gate_book_consumption`` mechanics.
  - a pipeline-level DRY tick on a single hgb sleeve producing a PENDING fill
    (WOULD-BET equivalent) or the correct SKIP reason.

These exercise the LIVE decision path.

2026-07-31 (exp044): the artifacts named here when the file was written are
gone. ``score_cell_hgb`` is checkpoint-only (phase 1), so the baseline
``hgb_v1.joblib`` + ``rf_v1`` pair these tests once scored against is no longer
loaded by anything, and the lut|rf|blend sleeves the gates were checked inert
against left with the pair sleeves (phase 2).
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import replace

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot_polymarket.gamma import PolyMarket
from cryptobot.data.binance import BinanceSpot
from cryptobot.execution.gates import (
    GATE_NAMES,
    GateInput,
    evaluate_gates,
    _gate_book_consumption,
    _gate_threshold,
)
from cryptobot.execution.pipeline import TickDeps, run_scanner_tick
from cryptobot.strategy.hgb_loader import HgbArtifact
from cryptobot.strategy.partial_ret import PartialRetEngine
from cryptobot.strategy.rf_loader import RfArtifact
from cryptobot.strategy.score import _fee_adjusted_edge, score_cell_hgb
from cryptobot.strategy.portfolio_loader import V6Portfolio, load_sleeves
from cryptobot.tests._artifact_skips import needs_checkpoint
from cryptobot.tests._fixtures import seed_for_tests


_V2_CONFIG = str(config.V6_CONFIG_PATH)  # resolved: runtime/ -> legacy -> polymarket/live/config/
_SEED_TS = 1_715_923_200
_SEED_SPOT = 67_000.0


@pytest.fixture(autouse=True)
def _enable_subminute(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployed v2 config is checkpoint-only (ORB/exp044 retired
    2026-07-09); the loader fails closed unless the sub-minute flag is on."""
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)


# ----- module-scoped real artifacts (slow to load) ---------------------


#: Checkpoint seconds inside the live sleeves' offset sets (5m 270:299:1,
#: 15m 870:899:1). score_cell_hgb is checkpoint-only since the 2026-07-31
#: exp044 retirement, so every scoring case drives one of these.
_OFF5 = 290
_OFF15 = 890


@pytest.fixture(scope="module")
def _ckpt_models():
    from cryptobot.strategy.checkpoint_loader import load_checkpoint_models

    return load_checkpoint_models()  # (RfArtifact, HgbArtifact)


@pytest.fixture(scope="module")
def hgb(_ckpt_models) -> HgbArtifact:
    return _ckpt_models[1]


@pytest.fixture(scope="module")
def rf(_ckpt_models) -> RfArtifact:
    return _ckpt_models[0]


@pytest.fixture(scope="module")
def hgb_portfolio() -> V6Portfolio:
    # Module-scoped: the autouse function fixture may not have run yet, so pin
    # the flag directly here for the checkpoint-only v2 load.
    prev = config.SUBMINUTE_ENABLED
    config.SUBMINUTE_ENABLED = True
    try:
        return load_sleeves(_V2_CONFIG)
    finally:
        config.SUBMINUTE_ENABLED = prev


@pytest.fixture(scope="module")
def s5(hgb_portfolio: V6Portfolio):
    return next(s for s in hgb_portfolio.sleeves if s.interval == "5m")


@pytest.fixture(scope="module")
def s15(hgb_portfolio: V6Portfolio):
    return next(s for s in hgb_portfolio.sleeves if s.interval == "15m")


def _warm_engine(coin: str = "btc") -> PartialRetEngine:
    eng = PartialRetEngine()
    seed_for_tests(eng, coin, now_ts=_SEED_TS, spot=_SEED_SPOT)
    return eng


# ----- score_cell_hgb --------------------------------------------------
#
# 2026-07-31 exp044 retirement: ``score_cell_hgb`` is CHECKPOINT-ONLY. The
# baseline (minute-entry) arm that scored against the frozen rf_v1/hgb_v1
# artifacts is gone, so every case here drives a real checkpoint second off the
# live sleeve's offset set and the checkpoint_v1 bundle.


class TestScoreCellHgb:
    def test_checkpoint_models_missing(self, s5) -> None:
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.55, no_ask=0.45,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=None, checkpoint_hgb=None,
        )
        assert r.skip_reason == "checkpoint_models_missing"
        assert math.isnan(r.model_p)
        assert r.side == "YES"
        assert math.isnan(r.edge)
        assert math.isnan(r.realized_edge)
        assert r.lut_p_bucket == -1
        assert r.lut_hit_rate is None
        assert r.lut_n_cum == 0
        assert r.partial_ret_z_1h is None

    @needs_checkpoint
    def test_no_offset_sec_is_the_retired_baseline_arm(self, rf, hgb, s5) -> None:
        """A cell with no checkpoint second has no model to score it since the
        exp044 retirement — it must fail closed, never reach for rf_v1."""
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.75, no_ask=0.70,
            partial_ret_engine=_warm_engine(),
            offset_sec=None, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason == "checkpoint_models_missing"

    @needs_checkpoint
    def test_no_feature_when_engine_unstamped(self, rf, hgb, s5) -> None:
        eng = PartialRetEngine()  # never warmed -> current_z None
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.55, no_ask=0.45,
            partial_ret_engine=eng,
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason == "no_feature"
        assert r.partial_ret_z_1h is None

    @needs_checkpoint
    def test_rf_slice_missing(self, rf, hgb, s5) -> None:
        # offset_sec=99_999 has no checkpoint RF slice -> rf_slice_missing.
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.55, no_ask=0.45,
            partial_ret_engine=_warm_engine(),
            offset_sec=99_999, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason == "rf_slice_missing"
        # z was computed before the RF lookup - it should be carried.
        assert r.partial_ret_z_1h is not None

    @needs_checkpoint
    def test_prebook_sentinel_when_both_asks_nan(self, rf, hgb, s5) -> None:
        # PRE-BOOK pass: neither ask finite -> skip_reason None sentinel that
        # tells the pipeline to fetch the book. Carries rf_proba + z.
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=float("nan"), no_ask=float("nan"),
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason is None
        assert r.side == "YES"
        assert math.isfinite(r.model_p)  # == rf_proba
        assert 0.0 <= r.model_p <= 1.0
        assert math.isnan(r.edge)
        assert math.isnan(r.realized_edge)
        assert r.partial_ret_z_1h is not None

    @needs_checkpoint
    def test_15m_both_asks_out_of_bucket_skips_ask_bucket(self, rf, hgb, s15) -> None:
        # v2 15m bucket is band:0.65:0.85. Both asks outside (0.65,0.85] -> ask_bucket.
        assert s15.ask_bucket_ranges == ((0.65, 0.85),)
        r = score_cell_hgb(
            coin="btc", sleeve=s15, entry_minute=14, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=0.20, no_ask=0.95,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF15, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason == "ask_bucket"
        assert r.partial_ret_z_1h is not None

    @needs_checkpoint
    def test_valid_5m_picks_best_side_and_edge_equals_pred_edge(
        self, rf, hgb, s5
    ) -> None:
        eng = _warm_engine()
        # v2 5m bucket is band:0.65:0.85 - both asks must be in-band to produce
        # two candidates (they are NOT complementary; a real book spread sums >1).
        yes_ask, no_ask = 0.75, 0.70
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=yes_ask, no_ask=no_ask,
            partial_ret_engine=eng,
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason is None
        # slippage_haircut == 0.0 for hgb -> realized_edge == edge.
        assert s5.slippage_haircut == 0.0
        assert r.realized_edge == pytest.approx(r.edge, abs=1e-12)
        assert r.side in ("YES", "NO")
        assert r.lut_p_bucket == -1 and r.lut_hit_rate is None and r.lut_n_cum == 0

        # Independently recompute both sides and confirm the higher-pred_edge
        # side was selected and edge == that pred_edge.
        edges = _checkpoint_side_edges(rf, hgb, eng, yes_ask=yes_ask, no_ask=no_ask)
        best_side = max(edges, key=lambda k: edges[k])
        assert r.side == best_side
        assert r.edge == pytest.approx(edges[best_side], abs=1e-12)


def _checkpoint_side_edges(rf, hgb, eng, *, yes_ask, no_ask, offset_sec=None):
    """Independently recompute (yes_edge, no_edge) the way score_cell_hgb does,
    on the checkpoint models at ``offset_sec``."""
    from cryptobot.strategy.hgb_features import build_feature_vector
    from cryptobot.strategy.hgb_loader import predict_model_prob
    from cryptobot.strategy.rf_loader import predict_proba

    off = _OFF5 if offset_sec is None else int(offset_sec)
    em = off / 60.0
    z = eng.current_z("btc", interval_min=5, live_spot=67_500.0, entry_minute=em)
    rfp = predict_proba(
        rf, coin="btc", interval="5m", entry_minute=off, partial_ret_z_1h=z
    )
    cols = hgb.columns_for("5m")
    edges = {}
    for res_side, ask, other, feat_side in (
        ("YES", yes_ask, no_ask, "Up"),
        ("NO", no_ask, yes_ask, "Down"),
    ):
        feat = build_feature_vector(
            side=feat_side, coin="btc", interval="5m", interval_min=5,
            entry_minute=em, window_start_ts=_SEED_TS, model_p_up=rfp,
            ask=ask, other_ask=other, partial_ret_z_1h=z, feature_columns=cols,
        )
        mp = predict_model_prob(hgb, interval="5m", feature_row_values=feat)
        edges[res_side] = _fee_adjusted_edge(mp, ask)
    return edges


@needs_checkpoint
class TestSideFilterParity:
    """audit w19oxqf24 (P2): live ``score_cell_hgb`` must enforce a sleeve's
    ``side_filter`` exactly like the backtest's pre-serve side drop
    (``training.checkpoint._score_rows_with_model``, which appends only the
    permitted side's frame — re-pointed 2026-08-01 from the deleted
    ``training.residual_matrix``).

    The backtest drops every row whose side != side_filter BEFORE the model
    serves, so the forbidden side can never win the best-side argmax. Before
    this fix the live path always built both Up/Down candidates and would bet
    the opposite higher-edge side the backtest forbade - a parity break.

    side_filter "up"   -> keep only the Up/YES candidate
    side_filter "down" -> keep only the Down/NO candidate
    side_filter "both" -> unchanged (argmax over both)
    """

    def _find_asks_where(self, rf, hgb, s5, *, winner: str):
        """Scan ask pairs for one where ``winner`` is the STRICTLY higher-edge
        side, so a side_filter test that forces the other side is a genuine
        regression (selects the non-argmax side). The wide (complementary) ask
        sweep is needed to flip the argmax; the side_filter tests widen the
        sleeve's ask bucket so score_cell_hgb accepts the found pair (the bucket
        is orthogonal to the side-drop being tested)."""
        eng = _warm_engine()
        other = "NO" if winner == "YES" else "YES"
        for i in range(5, 96):
            yes_ask = round(i / 100.0, 4)
            no_ask = round(1.0 - yes_ask, 4)
            edges = _checkpoint_side_edges(rf, hgb, eng, yes_ask=yes_ask, no_ask=no_ask)
            if edges[winner] > edges[other]:
                return yes_ask, no_ask, edges
        return None

    @staticmethod
    def _wide(sleeve, **overrides):
        """Widen a sleeve's ask bucket + fill band so any scanned ask pair is
        accepted - decouples the side_filter drop from v2's narrow band."""
        return replace(
            sleeve,
            ask_bucket_ranges=((0.0, 1.0),),
            min_fill_price=0.0,
            max_fill_price=1.0,
            **overrides,
        )

    def test_side_filter_up_keeps_only_yes_even_when_no_has_higher_edge(
        self, rf, hgb, s5
    ) -> None:
        # Find an ask pair where NO (the FORBIDDEN side under side_filter=up)
        # is the natural argmax winner - the exact parity-break scenario.
        found = self._find_asks_where(rf, hgb, s5, winner="NO")
        assert found is not None, "could not find an ask pair where NO wins"
        yes_ask, no_ask, edges = found
        assert edges["NO"] > edges["YES"]  # NO would win under "both"

        up_sleeve = self._wide(s5, side_filter="up")
        assert up_sleeve.side_filter == "up"
        r = score_cell_hgb(
            coin="btc", sleeve=up_sleeve, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=yes_ask, no_ask=no_ask,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason is None
        # The forbidden NO side must NOT be picked even though it has the
        # higher edge - only the Up/YES candidate survives the pre-serve drop.
        assert r.side == "YES"
        assert r.edge == pytest.approx(edges["YES"], abs=1e-12)

    def test_side_filter_down_keeps_only_no_even_when_yes_has_higher_edge(
        self, rf, hgb, s5
    ) -> None:
        found = self._find_asks_where(rf, hgb, s5, winner="YES")
        assert found is not None, "could not find an ask pair where YES wins"
        yes_ask, no_ask, edges = found
        assert edges["YES"] > edges["NO"]  # YES would win under "both"

        down_sleeve = self._wide(s5, side_filter="down")
        assert down_sleeve.side_filter == "down"
        r = score_cell_hgb(
            coin="btc", sleeve=down_sleeve, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=yes_ask, no_ask=no_ask,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason is None
        assert r.side == "NO"
        assert r.edge == pytest.approx(edges["NO"], abs=1e-12)

    def test_side_filter_both_is_unchanged_argmax(self, rf, hgb, s5) -> None:
        """Regression guard: side_filter="both" (the live config value) must
        leave the existing argmax-over-both behavior byte-for-byte unchanged."""
        assert s5.side_filter == "both"
        eng = _warm_engine()
        yes_ask, no_ask = 0.75, 0.70
        edges = _checkpoint_side_edges(rf, hgb, eng, yes_ask=yes_ask, no_ask=no_ask)
        best_side = max(edges, key=lambda k: edges[k])
        r = score_cell_hgb(
            coin="btc", sleeve=s5, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=yes_ask, no_ask=no_ask,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason is None
        assert r.side == best_side
        assert r.edge == pytest.approx(edges[best_side], abs=1e-12)

    def test_side_filter_up_with_only_forbidden_side_executable_skips(
        self, rf, hgb, s5
    ) -> None:
        """When side_filter=up but only the NO (forbidden) ask is finite, no
        candidate qualifies and the cell skips with ask_bucket - mirroring the
        backtest dropping that row entirely (no opposite-side fallback)."""
        up_sleeve = replace(s5, side_filter="up")
        r = score_cell_hgb(
            coin="btc", sleeve=up_sleeve, entry_minute=4, window_start_ts=_SEED_TS,
            live_spot=67_500.0, yes_ask=float("nan"), no_ask=0.45,
            partial_ret_engine=_warm_engine(),
            offset_sec=_OFF5, checkpoint_rf=rf, checkpoint_hgb=hgb,
        )
        assert r.skip_reason == "ask_bucket"
        assert r.partial_ret_z_1h is not None


# ----- gate mechanics --------------------------------------------------


def _hgb_sleeve(s5):
    return s5


def _gate_input(sleeve, **overrides) -> GateInput:
    defaults: dict = dict(
        coin="BTC",
        sleeve=sleeve,
        entry_minute=4,
        side="YES",
        realized_edge=0.10,
        fill_price=0.55,
        fee_rate_bps=100.0,
        spot_age_s=1.0,
        proposed_bet_size=2.0,
        pending_exposure_total=0.0,
        realized_capital=100.0,
        slot_filled_usd=0.0,
        slot_target_usd=5.0,
        disk_free_mb=10_000.0,
        book_top_ask_age_s=0.5,
        book_total_ask_depth_usd=100.0,
        exposure_cap_fraction=0.3,
    )
    defaults.update(overrides)
    return GateInput(**defaults)


class TestThresholdGate:
    def test_below_threshold_fails(self, s5) -> None:
        # v2 pred_edge gate: s5.threshold == 0.03.
        assert s5.threshold == pytest.approx(0.03)
        r = _gate_threshold(_gate_input(s5, realized_edge=0.029))
        assert r == "edge_below_threshold"

    def test_at_threshold_passes(self, s5) -> None:
        assert _gate_threshold(_gate_input(s5, realized_edge=0.03)) is None

    def test_above_threshold_passes(self, s5) -> None:
        assert _gate_threshold(_gate_input(s5, realized_edge=0.10)) is None


class TestBookConsumptionGate:
    @pytest.fixture(scope="class")
    def capped_s5(self, s5):
        from dataclasses import replace

        return replace(s5, book_consumption_max=0.15)

    def test_deployed_hgb_book_consumption_gate_active(self, s5) -> None:
        # 4x-parity: the deployed 5m sleeve now carries book_consumption_max
        # 0.30 (book x2 vs the +$13,721 backtest), so the gate is ACTIVE (no
        # longer inert). A missing depth fails closed rather than passing through.
        assert s5.book_consumption_max == pytest.approx(0.30)
        r = _gate_book_consumption(
            _gate_input(s5, proposed_bet_size=500.0, book_total_ask_depth_usd=None)
        )
        assert r == "insufficient_depth"

    def test_at_cap_passes(self, capped_s5) -> None:
        # frac=0.15, depth=100 → cap=15.0. proposed exactly at cap passes.
        assert capped_s5.book_consumption_max == pytest.approx(0.15)
        r = _gate_book_consumption(
            _gate_input(capped_s5, proposed_bet_size=15.0, book_total_ask_depth_usd=100.0)
        )
        assert r is None

    def test_just_below_cap_passes(self, capped_s5) -> None:
        r = _gate_book_consumption(
            _gate_input(capped_s5, proposed_bet_size=14.99, book_total_ask_depth_usd=100.0)
        )
        assert r is None

    def test_above_cap_fails(self, capped_s5) -> None:
        r = _gate_book_consumption(
            _gate_input(capped_s5, proposed_bet_size=15.0 + 1e-3, book_total_ask_depth_usd=100.0)
        )
        assert r == "book_consumption_cap"

    def test_depth_none_insufficient(self, capped_s5) -> None:
        r = _gate_book_consumption(
            _gate_input(capped_s5, proposed_bet_size=1.0, book_total_ask_depth_usd=None)
        )
        assert r == "insufficient_depth"

    def test_depth_zero_insufficient(self, capped_s5) -> None:
        r = _gate_book_consumption(
            _gate_input(capped_s5, proposed_bet_size=1.0, book_total_ask_depth_usd=0.0)
        )
        assert r == "insufficient_depth"

    def test_depth_negative_insufficient(self, capped_s5) -> None:
        r = _gate_book_consumption(
            _gate_input(capped_s5, proposed_bet_size=1.0, book_total_ask_depth_usd=-5.0)
        )
        assert r == "insufficient_depth"


class TestGatesInertWhenHgbKnobsAreNone:
    """Both gates must be inert when threshold / book_consumption_max are None.

    2026-07-31: this used to load the legacy lut sleeve from the shared fixture,
    but the loader now rejects a non-hgb sleeve outright. The gates' inertness
    is a property of the two None knobs, not of the signal_source, so pin it on
    a sleeve that carries them as None.
    """

    @pytest.fixture(scope="class")
    def inert_sleeve(self):
        from cryptobot.tests._legacy_artifact import legacy_portfolio
        from dataclasses import replace as _dc_replace

        port = legacy_portfolio()
        s = next(s for s in port.sleeves if s.label == "v6_5m")
        s = _dc_replace(s, threshold=None, book_consumption_max=None)
        assert s.threshold is None
        assert s.book_consumption_max is None
        return s

    def test_threshold_gate_inert(self, inert_sleeve) -> None:
        # Even a deeply-negative edge does not trip the threshold gate.
        assert _gate_threshold(_gate_input(inert_sleeve, realized_edge=-5.0)) is None

    def test_book_consumption_gate_inert(self, inert_sleeve) -> None:
        # Even depth None / huge bet does not trip the book gate.
        assert (
            _gate_book_consumption(
                _gate_input(
                    inert_sleeve, proposed_bet_size=1e9, book_total_ask_depth_usd=None
                )
            )
            is None
        )

    def test_full_stack_passes_happy_path(self, inert_sleeve) -> None:
        # The two gates do not affect the happy path. Pin realized_edge inside
        # the sleeve's own min/max band.
        inp = _gate_input(
            inert_sleeve,
            realized_edge=(inert_sleeve.min_edge + inert_sleeve.max_edge) / 2.0,
            fill_price=(
                inert_sleeve.min_fill_price + inert_sleeve.max_fill_price
            ) / 2.0,
        )
        assert evaluate_gates(inp).passed is True


class TestGateStackContainsNewGates:
    def test_threshold_and_book_consumption_present(self) -> None:
        assert "threshold" in GATE_NAMES
        assert "book_consumption" in GATE_NAMES
        # threshold sits between fee and edge; book_consumption between
        # capital and exposure.
        assert GATE_NAMES.index("fee") < GATE_NAMES.index("threshold")
        assert GATE_NAMES.index("threshold") < GATE_NAMES.index("edge")
        assert GATE_NAMES.index("capital") < GATE_NAMES.index("book_consumption")
        assert GATE_NAMES.index("book_consumption") < GATE_NAMES.index("exposure")


# ----- pipeline-level DRY tick -----------------------------------------


def _spots(now_ts: int) -> dict[str, BinanceSpot]:
    return {
        coin: BinanceSpot(coin=coin, price=p, publish_time=now_ts - 1)
        for coin, p in (("BTC", 67_000.0), ("ETH", 3_100.0), ("SOL", 175.0), ("XRP", 0.52))
    }


def _book(
    token_id: str,
    top: float,
    depth: float,
    now_ts: float,
    total_ask_depth_usd: float | None = None,
) -> BookSnapshot:
    asks = ({"price": top, "size": depth},)
    if total_ask_depth_usd is None:
        # Default: the single synthetic level IS the whole book.
        total_ask_depth_usd = float(sum(l["price"] * l["size"] for l in asks))
    return BookSnapshot(
        token_id=token_id,
        asks=asks,
        bids=(),
        fetched_at_ts=now_ts,
        total_ask_depth_usd=total_ask_depth_usd,
    )


def _markets(window_start: int, interval_min: int) -> dict[tuple[str, int, int], PolyMarket]:
    out = {}
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        out[(coin, interval_min, window_start)] = PolyMarket(
            coin=coin,
            interval_min=interval_min,
            window_start_ts=window_start,
            window_end_ts=window_start + interval_min * 60,
            condition_id=f"COND-{coin}",
            yes_token_id=f"YES-{coin}",
            no_token_id=f"NO-{coin}",
            fee_rate_bps=100.0,
        )
    return out


class _StubBookDeps:
    def __init__(self, markets, books, spots) -> None:
        self._markets = markets
        self._books = books
        self._spots = spots

    def fetch_spots(self, *, now_ts, tick_seconds):
        return self._spots

    def fetch_market(self, coin, *, interval_min, window_start_ts, conn, now_ts):
        return self._markets.get((coin, interval_min, window_start_ts))

    def fetch_book(self, token_id, *, now_ts):
        return self._books.get(token_id)

    def disk_free(self):
        return 10_000.0


# The baseline (minute-boundary) hgb DRY-tick case that lived here scored the
# 5m sleeve with its checkpoint offsets cleared, i.e. against the frozen
# hgb_v1/rf_v1 pair. That path was retired with exp044 on 2026-07-31 — the
# equivalent live coverage is the sub-minute checkpoint tick tests below.


def test_fetch_top_book_total_depth_is_full_ask_book():
    """book_consumption PARITY: fetch_top_book stores the full ask book and
    `total_ask_depth_usd` over that same book, matching the backtest's
    full-book up_depth_l2."""
    import httpx
    from cryptobot_polymarket.clob import TokenBucket, fetch_top_book

    levels = [{"price": round(0.30 + i * 0.002, 4), "size": 100.0} for i in range(10)]

    def _handler(_req):
        return httpx.Response(200, json={"asks": levels, "bids": []})

    client = httpx.Client(transport=httpx.MockTransport(_handler))
    snap = fetch_top_book("TOK", client=client, bucket=TokenBucket(rate_per_s=100, burst=100))
    client.close()

    assert snap is not None
    assert len(snap.asks) == len(levels)
    full = float(sum(l["price"] * l["size"] for l in levels))
    from_snapshot = float(sum(l["price"] * l["size"] for l in snap.asks))
    assert snap.total_ask_depth_usd == pytest.approx(full)
    assert snap.total_ask_depth_usd == pytest.approx(from_snapshot)
    assert snap.asks[-1]["price"] == pytest.approx(levels[-1]["price"])


# ----- B1 sub-minute checkpoint pipeline ticks --------------------------


@pytest.fixture(scope="module")
def ckpt(_ckpt_models):
    return _ckpt_models  # (RfArtifact, HgbArtifact)


@needs_checkpoint
def test_pipeline_subminute_consecutive_offsets_stack(
    db: sqlite3.Connection, hgb, rf, ckpt, hgb_portfolio, monkeypatch
) -> None:
    """B1 regression (consecutive-offset stacking): step1last offsets share
    an entry MINUTE (270-299 are all minute 4), so after the first tail bet
    the minute-keyed `_initial_entry_bet_seen` guard must NOT latch the next
    offset — per-offset dedup is `subminute_slot_seen`. Runs two DRY ticks
    at offsets 275 then 276 on the real subminute_5m sleeve and asserts the
    276 tick never emits entry_scan_latched for a coin that just bet at 275
    (either it stacks a second bet or skips for a decision-path reason).
    """
    from dataclasses import replace as _dc_replace

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", False)
    monkeypatch.setattr(config, "SUBMINUTE_ONE_PER_WINDOW", False)

    import cryptobot.strategy.portfolio_loader as _vl
    monkeypatch.setattr(_vl.config, "SUBMINUTE_ENABLED", True)
    full = load_sleeves(_V2_CONFIG)
    sub5 = next(s for s in full.sleeves if s.label == "subminute_5m")
    portfolio = _dc_replace(full, sleeves=(sub5,))

    window5 = ((_SEED_TS + 7 * 60) // 300) * 300

    # Spots/books must be fresh AT EACH TICK TIME: sub-minute ticks land
    # mid-window (+275s), so fixture data stamped at window open would fail
    # the stale_spot / stale_book freshness gates and make every downstream
    # assertion vacuous (caught by a probe run — do not "simplify" back).
    class _FreshStub(_StubBookDeps):
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(int(now_ts))

        def fetch_book(self, token_id, *, now_ts):
            return _book(token_id, top=0.30, depth=500.0, now_ts=now_ts)

    stub = _FreshStub(_markets(window5, 5), {}, _spots(window5))
    eng = PartialRetEngine()
    for coin, spot in stub._spots.items():
        seed_for_tests(eng, coin, now_ts=window5, spot=spot.price)

    ckpt_rf, ckpt_hgb = ckpt

    def _deps() -> TickDeps:
        return TickDeps(
            partial_ret_engine=eng,
            config_sha256="stub-sha256",
            fetch_spots=stub.fetch_spots,
            fetch_market=stub.fetch_market,
            fetch_book=stub.fetch_book,
            get_disk_free_mb=stub.disk_free,
            checkpoint_rf_artifact=ckpt_rf,
            checkpoint_hgb_artifact=ckpt_hgb,
        )

    r275 = run_scanner_tick(db, portfolio, now_ts=window5 + 275, deps=_deps())
    assert r275.cells_evaluated == len(config.COINS)  # offset qualified

    bet_coins = {
        row["coin"]
        for row in db.execute(
            "SELECT coin FROM ledger WHERE offset_sec = 275 AND outcome != 'CANCELLED'"
        )
    }
    # Whatever landed must be stamped with the checkpoint offset and satisfy
    # the venue minimum. The exact target follows the live portfolio sizing
    # config and may be above MIN_BET_USD.
    for row in db.execute("SELECT offset_sec, sleeve, bet_size FROM ledger"):
        assert row["offset_sec"] == 275
        assert row["sleeve"] == "subminute_5m"
        assert row["bet_size"] >= config.MIN_BET_USD

    r276 = run_scanner_tick(db, portfolio, now_ts=window5 + 276, deps=_deps())
    assert r276.cells_evaluated == len(config.COINS)

    if bet_coins:
        # The regression: pre-fix, these cells died as entry_scan_latched.
        latched = {
            d.coin
            for d in r276.cell_decisions
            if d.final_skip_reason == "entry_scan_latched"
        }
        assert not (bet_coins & latched), (
            f"minute-keyed guard latched consecutive offsets: {bet_coins & latched}"
        )
        # And the offset-276 checkpoint actually STACKED bets on the same
        # coins (with the current model artifact this branch is exercised —
        # a probe run confirmed 3 fills at 275 and 3 more at 276).
        stacked = {
            row["coin"]
            for row in db.execute(
                "SELECT coin FROM ledger WHERE offset_sec = 276 "
                "AND outcome != 'CANCELLED'"
            )
        }
        assert bet_coins <= stacked, (
            f"offset 276 failed to stack on {bet_coins - stacked}"
        )

    # Off-checkpoint second: silent no-op (no cells, no signal spam).
    r58 = run_scanner_tick(db, portfolio, now_ts=window5 + 58, deps=_deps())
    assert r58.cells_evaluated == 0
    assert r58.signals_written == 0


@needs_checkpoint
def test_pipeline_subminute_signals_rows_carry_offset_sec(
    db: sqlite3.Connection, hgb, rf, ckpt, hgb_portfolio, monkeypatch
) -> None:
    """2026-07-03 regression: `signals.offset_sec` is the baseline-vs-
    sub-minute split key (schema note in ledger.py), but every gate-stack
    skip site omitted it — only the pre-gate skip helpers passed it — so
    live sub-minute rows landed with NULL offset_sec and the dashboard /
    forensic splits under-counted. Every signals row a checkpoint tick
    writes must carry the tick's offset.
    """
    from dataclasses import replace as _dc_replace

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", False)

    import cryptobot.strategy.portfolio_loader as _vl
    monkeypatch.setattr(_vl.config, "SUBMINUTE_ENABLED", True)
    full = load_sleeves(_V2_CONFIG)
    sub5 = next(s for s in full.sleeves if s.label == "subminute_5m")
    portfolio = _dc_replace(full, sleeves=(sub5,))

    window5 = ((_SEED_TS + 7 * 60) // 300) * 300

    class _FreshStub(_StubBookDeps):
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(int(now_ts))

        def fetch_book(self, token_id, *, now_ts):
            return _book(token_id, top=0.30, depth=500.0, now_ts=now_ts)

    stub = _FreshStub(_markets(window5, 5), {}, _spots(window5))
    eng = PartialRetEngine()
    for coin, spot in stub._spots.items():
        seed_for_tests(eng, coin, now_ts=window5, spot=spot.price)

    ckpt_rf, ckpt_hgb = ckpt

    deps = TickDeps(
        partial_ret_engine=eng,
        config_sha256="stub-sha256",
        fetch_spots=stub.fetch_spots,
        fetch_market=stub.fetch_market,
        fetch_book=stub.fetch_book,
        get_disk_free_mb=stub.disk_free,
        checkpoint_rf_artifact=ckpt_rf,
        checkpoint_hgb_artifact=ckpt_hgb,
    )

    result = run_scanner_tick(db, portfolio, now_ts=window5 + 275, deps=deps)
    assert result.cells_evaluated == len(config.COINS)

    rows = db.execute(
        "SELECT skip_reason, offset_sec FROM signals WHERE sleeve = 'subminute_5m'"
    ).fetchall()
    assert rows, "checkpoint tick wrote no signals rows"
    for r in rows:
        assert r["offset_sec"] == 275, (
            f"signals row (skip_reason={r['skip_reason']}) lost offset_sec"
        )


@needs_checkpoint
def test_pipeline_subminute_flag_off_is_silent(
    db: sqlite3.Connection, hgb, rf, monkeypatch
) -> None:
    """Kill-switch/flag-off: a subminute sleeve present in the portfolio must
    produce ZERO cells, signals, and API calls when disabled at tick time."""
    from dataclasses import replace as _dc_replace

    import cryptobot.strategy.portfolio_loader as _vl
    monkeypatch.setattr(_vl.config, "SUBMINUTE_ENABLED", True)
    full = load_sleeves(_V2_CONFIG)
    sub5 = next(s for s in full.sleeves if s.label == "subminute_5m")
    portfolio = _dc_replace(full, sleeves=(sub5,))

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", True)  # kill wins

    window5 = ((_SEED_TS + 7 * 60) // 300) * 300

    class _BlowUp:
        def fetch_spots(self, *, now_ts, tick_seconds):
            return _spots(int(now_ts))
        def fetch_market(self, *a, **kw):
            raise AssertionError("no API calls when killed")
        def fetch_book(self, *a, **kw):
            raise AssertionError("no API calls when killed")
        def disk_free(self):
            return 10_000.0

    blowup = _BlowUp()
    eng = PartialRetEngine()
    for coin, spot in _spots(window5).items():
        seed_for_tests(eng, coin, now_ts=window5, spot=spot.price)
    deps = TickDeps(
        partial_ret_engine=eng,
        config_sha256="stub-sha256",
        fetch_spots=blowup.fetch_spots,
        fetch_market=blowup.fetch_market,
        fetch_book=blowup.fetch_book,
        get_disk_free_mb=blowup.disk_free,
    )

    result = run_scanner_tick(db, portfolio, now_ts=window5 + 275, deps=deps)
    assert result.cells_evaluated == 0
    assert result.pending_rows_written == 0
    assert result.signals_written == 0


class _PricedStub(_StubBookDeps):
    """Stub that serves fresh books at each tick time with a fixed per-token
    price (YES-* vs NO-*), so the model's best-side pick is deterministic:
    the cheap side wins on edge (subminute_5m ask_bucket is the full (0,1)
    range, so both sides are always admissible candidates)."""

    def __init__(self, markets, spots, *, yes_top: float, no_top: float) -> None:
        super().__init__(markets, {}, spots)
        self._yes_top = yes_top
        self._no_top = no_top

    def fetch_spots(self, *, now_ts, tick_seconds):
        return _spots(int(now_ts))

    def fetch_book(self, token_id, *, now_ts):
        top = self._yes_top if token_id.startswith("YES-") else self._no_top
        return _book(token_id, top=top, depth=500.0, now_ts=now_ts)


@needs_checkpoint
def test_pipeline_subminute_one_per_window_blocks_opposite_side(
    db: sqlite3.Connection, hgb, rf, ckpt, hgb_portfolio, monkeypatch
) -> None:
    """Parity regression (SUBMINUTE_ONE_PER_WINDOW cross-side dedup).

    The retained combo backtest dedupes at final RESOLUTION-market level, not
    side level (``_dedup_resolution_with_stats`` keys on condition_id/
    market_slug with NO side component — it keeps exactly ONE bet per market).
    Live must match: once a window is owned by one side, the OPPOSITE side is
    rejected as ``market_dedup`` (``subminute_window_existing_side`` guard in
    ``_commit_cell``), while the SAME side may still top up. The pre-guard
    behaviour let a window hold both a YES and a NO position.

    Deterministic setup: seed a YES owner for every coin in the window, then
    price the YES book unbettably high and the NO book cheap so the model's
    best side is NO on every coin — every NO cell must be blocked, and no NO
    row may open. A second tick flips the prices so the model wants YES (the
    owned side): that same-side attempt must NOT be dedup'd.
    """
    from dataclasses import replace as _dc_replace

    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests.test_subminute import make_subminute_signal

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "SUBMINUTE_KILL", False)
    monkeypatch.setattr(config, "SUBMINUTE_ONE_PER_WINDOW", True)

    import cryptobot.strategy.portfolio_loader as _vl
    monkeypatch.setattr(_vl.config, "SUBMINUTE_ENABLED", True)
    full = load_sleeves(_V2_CONFIG)
    sub5 = next(s for s in full.sleeves if s.label == "subminute_5m")
    # Widen the ask-bucket + fill band so the fixture asks always produce a
    # bettable candidate that reaches the one-per-window guard — the band is
    # applied identically to every side, so it is orthogonal to the cross-side
    # dedup orchestration tested here (same pattern as test_checkpoint_parity).
    sub5 = _dc_replace(
        sub5, ask_bucket_ranges=((0.0, 1.0),), min_fill_price=0.0, max_fill_price=1.0
    )
    portfolio = _dc_replace(full, sleeves=(sub5,))

    window5 = ((_SEED_TS + 7 * 60) // 300) * 300

    # Seed a YES owner for every fixture coin via an EARLIER checkpoint (offset
    # 200). It owns the window (earliest bet_ts), so any later opposite-side
    # (NO) checkpoint in the same window must be rejected.
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        record_bet(
            db,
            make_subminute_signal(
                coin=coin, side="YES", offset_sec=200,
                window_start_ts=window5, bet_size=2.0,
            ),
            mode="DRY",
        )
    db.commit()

    eng = PartialRetEngine()
    for coin, spot in _spots(window5).items():
        seed_for_tests(eng, coin, now_ts=window5, spot=spot.price)

    # Sub-minute checkpoint cells score against the per-SECOND checkpoint_v1
    # models (offset 275 has an RF slice; the baseline minute RF does not), so
    # the tick must be given the real checkpoint artifacts.
    ckpt_rf, ckpt_hgb = ckpt

    def _deps(stub) -> TickDeps:
        return TickDeps(
            partial_ret_engine=eng,
            config_sha256="stub-sha256",
            fetch_spots=stub.fetch_spots,
            fetch_market=stub.fetch_market,
            fetch_book=stub.fetch_book,
            get_disk_free_mb=stub.disk_free,
            checkpoint_rf_artifact=ckpt_rf,
            checkpoint_hgb_artifact=ckpt_hgb,
        )

    def _side_counts() -> tuple[int, int]:
        yes = db.execute(
            "SELECT COUNT(*) c FROM ledger WHERE side='YES' "
            "AND window_start_ts=? AND outcome != 'CANCELLED'",
            (window5,),
        ).fetchone()["c"]
        no = db.execute(
            "SELECT COUNT(*) c FROM ledger WHERE side='NO' "
            "AND window_start_ts=? AND outcome != 'CANCELLED'",
            (window5,),
        ).fetchone()["c"]
        return int(yes), int(no)

    assert _side_counts() == (4, 0)  # only the seeded YES owners

    # --- Tick A: NO cheap, YES unbettable → model picks NO → guard blocks. ----
    block_stub = _PricedStub(_markets(window5, 5), _spots(window5), yes_top=0.985, no_top=0.30)
    r_block = run_scanner_tick(db, portfolio, now_ts=window5 + 275, deps=_deps(block_stub))
    assert r_block.cells_evaluated == len(config.COINS)
    # The opposite (NO) side was rejected on ≥1 coin. Under the exp044-style
    # config the reason is ``market_dedup`` (cross-side guard in _commit_cell);
    # under v2 the WindowDecider consumes the owned window earlier and reports
    # ``v2_window_consumed`` — both preserve the one-bet-per-market invariant.
    blocked = (r_block.skips_by_reason.get("market_dedup", 0)
               + r_block.skips_by_reason.get("v2_window_consumed", 0))
    assert blocked >= 1, (
        f"cross-side guard never fired: {r_block.skips_by_reason}"
    )
    # ...and the parity invariant holds: NO second-side position opened.
    assert _side_counts() == (4, 0), "opposite side opened a position in an owned window"

    # --- Tick B: YES cheap, NO unbettable → model wants the OWNED side. -------
    # The guard is side-SPECIFIC: a checkpoint whose best side is the OWNED side
    # (YES) must pass the cross-side guard and fall through to the top-up path,
    # never be rejected as market_dedup. (A coin whose model still flips to NO
    # under this pricing is CORRECTLY blocked — that is the guard working, not a
    # regression — so we assert per-decision on the owned side only.)
    same_stub = _PricedStub(_markets(window5, 5), _spots(window5), yes_top=0.30, no_top=0.985)
    r_same = run_scanner_tick(db, portfolio, now_ts=window5 + 276, deps=_deps(same_stub))
    assert r_same.cells_evaluated == len(config.COINS)
    owned = [d for d in r_same.cell_decisions if d.score.side == "YES"]
    assert owned, f"no owned-side checkpoint to exercise the guard: {r_same.skips_by_reason}"
    assert all(d.final_skip_reason != "market_dedup" for d in owned), (
        "same-side checkpoint wrongly blocked as market_dedup: "
        f"{[(d.coin, d.final_skip_reason) for d in owned]}"
    )
    # Invariant still holds: no opposite-side position ever opened.
    yes_after, no_after = _side_counts()
    assert no_after == 0
    assert yes_after >= 4
