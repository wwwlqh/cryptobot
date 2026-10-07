"""PartialRetEngine tests — training-parity formulation.

These cover the four behavioural pillars of the rewritten engine:

* z is None until the 1m-log-return Welford has at least
  ``_MIN_RV_1H_SAMPLES`` observations AND the window-start spot is
  stamped (no more "fall back to raw return when buffer is thin").
* z denominator is ``rv_1h_at_open * sqrt(entry_minute)`` — the
  numerator is a log return, not a simple return.
* ``rv_1h_at_open`` is frozen at the moment the interval window opens,
  not recomputed on the fly inside the window.
* ``window_start_spot`` prefers the last tick before the boundary over
  the first tick after it (fix #4).
* A parity regression replays a synthetic minute series against the
  canonical training formula and demands agreement to 1e-9.
"""

from __future__ import annotations

import math
import random

import pytest

from cryptobot.strategy.partial_ret import (
    _MIN_RV_1H_SAMPLES,
    _floor_to,
    MinuteBar,
    PartialRetEngine,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _warm_up_minutes(eng: PartialRetEngine, coin: str, *, start_ts: int,
                     start_spot: float, n_minutes: int, step_log_ret: float = 0.0,
                     jitter: float = 0.0, seed: int = 0) -> float:
    """Feed ``n_minutes`` of 1-per-minute ticks. Returns the final spot."""
    rng = random.Random(seed)
    spot = start_spot
    for i in range(n_minutes):
        ts = start_ts + i * 60
        if i > 0:
            ret = step_log_ret + (rng.uniform(-jitter, jitter) if jitter else 0.0)
            spot = spot * math.exp(ret)
        eng.update(coin, live_spot=spot, now_ts=ts)
    return spot


# ---------------------------------------------------------------------------
# Core behavioural contracts
# ---------------------------------------------------------------------------


def test_returns_none_when_engine_unstamped():
    eng = PartialRetEngine()
    assert eng.current_z(
        "BTC", interval_min=5, live_spot=67_000.0, entry_minute=4
    ) is None


def test_unknown_interval_returns_none():
    eng = PartialRetEngine()
    eng.update("BTC", live_spot=67_000.0, now_ts=1_715_923_800)
    assert eng.current_z(
        "BTC", interval_min=99, live_spot=67_000.0, entry_minute=4
    ) is None


def test_non_positive_entry_minute_returns_none():
    """Review-pass #20 closeout. entry_minute is now required; passing
    a non-positive value should refuse rather than silently scale by
    sqrt(0) (NaN/inf denominator).
    """
    eng = PartialRetEngine()
    eng.update("BTC", live_spot=67_000.0, now_ts=1_715_923_800)
    # Even with seeded state, entry_minute<=0 is rejected.
    from cryptobot.tests._fixtures import seed_for_tests
    seed_for_tests(eng, "BTC", now_ts=1_715_923_800, spot=67_000.0)
    assert eng.current_z(
        "BTC", interval_min=15, live_spot=67_500.0, entry_minute=0
    ) is None
    assert eng.current_z(
        "BTC", interval_min=15, live_spot=67_500.0, entry_minute=-1
    ) is None


def test_returns_none_until_rv_window_populated():
    """Until _MIN_RV_1H_SAMPLES minute-returns accumulate, z is None.

    The training formula's denominator is undefined without 60 minutes of
    1m-return history; we accept >= 30 with a wider error bar.  Below
    that floor, we refuse to score rather than fall back to an unscaled
    raw return (the historical bug we are fixing).
    """
    eng = PartialRetEngine()
    # Stamp the window with a single tick — buffer is empty.
    eng.update("BTC", live_spot=67_000.0, now_ts=1_715_923_800)
    assert eng.current_z(
        "BTC", interval_min=5, live_spot=67_500.0, entry_minute=4
    ) is None


def test_bootstrap_from_minute_bars_scores_current_window_without_warmup():
    eng = PartialRetEngine()
    start_ts = 1_715_923_800
    now_ts = start_ts + 92 * 60 + 125
    rng = random.Random(123)
    spot = 67_000.0
    bars: list[MinuteBar] = []

    for i in range(95):
        minute_ts = start_ts + i * 60
        open_px = spot
        spot = spot * math.exp(rng.gauss(0.0, 0.001))
        bars.append(
            MinuteBar(
                start_ts=minute_ts,
                open=open_px,
                close=spot,
                is_closed=minute_ts < _floor_to(now_ts, 60),
            )
        )

    loaded = eng.bootstrap_from_minute_bars("BTC", bars, now_ts=now_ts)

    assert loaded >= _MIN_RV_1H_SAMPLES
    assert eng.current_z(
        "BTC",
        interval_min=5,
        live_spot=spot * 1.001,
        entry_minute=2,
    ) is not None


def test_z_value_after_warmup_matches_training_formula():
    """End-to-end: feed 90 minutes of synthetic ticks, then verify the
    engine's z output exactly matches the canonical training-time
    expression ``log(spot/open) / (rv_1h_at_open * sqrt(entry_minute))``.
    """
    eng = PartialRetEngine()
    start_ts = 1_715_923_800  # arbitrary, aligned to both 5m and 15m boundaries
    assert start_ts % 900 == 0 and start_ts % 300 == 0, "test pre-condition: aligned ts"
    # 90 minutes of ticks WITH jitter so the Welford std is non-zero.
    closes = []
    spot = 67_000.0
    rng = random.Random(42)
    for i in range(90):
        ts = start_ts - 90 * 60 + i * 60
        if i > 0:
            spot = spot * math.exp(rng.gauss(0.0, 0.001))
        eng.update("BTC", live_spot=spot, now_ts=ts)
        closes.append(spot)

    # The next tick triggers a 5m boundary crossing → fresh window anchor
    # and a frozen rv_1h_at_open snapshot.
    window_open_ts = start_ts
    eng.update("BTC", live_spot=spot, now_ts=window_open_ts)
    state = eng.state["BTC"]
    rv_5m = state.rv_1h_at_open_5m
    assert rv_5m is not None and rv_5m > 0

    # Compute the expected z at entry_minute=4, with live_spot 0.4% above
    # the anchor.
    em = 4
    live_spot = state.window_start_spot_5m * math.exp(0.004)
    expected_z = math.log(live_spot / state.window_start_spot_5m) / (
        rv_5m * math.sqrt(em)
    )
    z = eng.current_z(
        "BTC", interval_min=5, live_spot=live_spot, entry_minute=em
    )
    assert z is not None
    assert z == pytest.approx(expected_z, rel=1e-12, abs=1e-12)


def test_rv_snapshot_is_frozen_at_window_open():
    """rv_1h_at_open MUST NOT change mid-window. Training computes it at
    the window open exactly, so subsequent in-window ticks should not
    perturb the denominator used for that window's z.
    """
    eng = PartialRetEngine()
    start_ts = 1_715_923_800  # 15m-aligned
    # Warm up 60 minutes of low-volatility ticks before the window opens.
    rng = random.Random(7)
    spot = 67_000.0
    for i in range(60):
        ts = start_ts - 60 * 60 + i * 60
        if i > 0:
            spot = spot * math.exp(rng.gauss(0.0, 0.0005))
        eng.update("BTC", live_spot=spot, now_ts=ts)

    # Trigger window open.
    eng.update("BTC", live_spot=spot, now_ts=start_ts)
    snapshot = eng.state["BTC"].rv_1h_at_open_5m
    assert snapshot is not None and snapshot > 0

    # Inject a single high-vol minute INSIDE the window. The frozen
    # snapshot must not change.
    eng.update("BTC", live_spot=spot * 1.05, now_ts=start_ts + 60)
    assert eng.state["BTC"].rv_1h_at_open_5m == snapshot


def test_window_start_spot_prefers_previous_tick_over_first_after_boundary():
    """Fix #4: training's log_open is the bar-open at exactly the
    interval boundary. The previous tick (one second before the
    boundary) is closer to that instant than the first tick after the
    boundary. Verify the engine picks the previous tick when it is
    close to the boundary.
    """
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800  # 5m and 15m aligned
    # Warm up enough minutes of history so rv_1h_at_open snapshots OK.
    _warm_up_minutes(
        eng, "BTC",
        start_ts=aligned_ts - 60 * 60,
        start_spot=67_000.0,
        n_minutes=60,
        jitter=0.0005,
        seed=1,
    )
    state = eng.state["BTC"]
    # Final warm-up tick lands at aligned_ts - 60 (close to but before
    # the next 5m boundary at aligned_ts + 300). Inject a "just before
    # boundary" tick at aligned_ts + 299, then a "just after" tick at
    # aligned_ts + 300 with a noticeably different spot.
    pre_boundary_ts = aligned_ts + 299
    pre_boundary_spot = 67_500.0
    eng.update("BTC", live_spot=pre_boundary_spot, now_ts=pre_boundary_ts)

    post_boundary_ts = aligned_ts + 300
    post_boundary_spot = 68_000.0
    eng.update("BTC", live_spot=post_boundary_spot, now_ts=post_boundary_ts)

    # The new 5m window opened at post_boundary_ts. window_start_spot
    # should be the pre_boundary_spot, not the post_boundary_spot.
    assert state.window_start_ts_5m == post_boundary_ts
    assert state.window_start_spot_5m == pytest.approx(pre_boundary_spot)


def test_window_start_spot_falls_back_to_current_tick_on_cold_start():
    """If there is no last_tick before the boundary (cold boot), we
    accept the first-after-boundary tick — but the engine still needs
    enough rv history before it can score, so it returns None for the
    very-first-tick case.
    """
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800
    eng.update("BTC", live_spot=67_000.0, now_ts=aligned_ts)
    state = eng.state["BTC"]
    assert state.window_start_spot_5m == pytest.approx(67_000.0)
    # No rv history yet → z is None even though window-start is stamped.
    assert eng.current_z(
        "BTC", interval_min=5, live_spot=67_000.0, entry_minute=1
    ) is None


class TestWindowOpenSpotAccessor:
    """Divergence #13 fix (2026-05-21): the engine exposes its captured
    window-open spot per (coin, interval) so the live ledger can grade
    bets against the same anchor the model trains against and Polymarket
    Chainlink settles on.
    """

    def test_returns_none_for_unknown_coin(self):
        eng = PartialRetEngine()
        # No update() called — coin state never created.
        assert eng.window_open_spot("BTC", interval_min=5) is None
        assert eng.window_open_spot("ETH", interval_min=15) is None

    def test_returns_none_before_first_boundary_observed(self):
        """When the engine has only seen mid-window ticks (no boundary
        crossing yet), the anchor field is None and the accessor must
        relay that. The pipeline's no_window_open_spot skip path
        depends on this signal."""
        eng = PartialRetEngine()
        # Pick a now_ts that's far from any 5m/15m boundary AND set
        # last_minute_ts/last_tick_ts to a non-default value so the
        # cold-start branch in update() does NOT cross a boundary and
        # stamp the anchor. The simplest trick is one tick mid-window:
        # boundary-stamp only fires when (getattr attr_ts) != w, and on
        # cold start last_tick_ts is None so the boundary check IS hit.
        # Therefore even one tick stamps both 5m and 15m anchors.
        # To prove the "None before first boundary" contract we have
        # to construct the state directly.
        cs = eng._coin("BTC")
        cs.window_start_spot_5m = None
        cs.window_start_spot_15m = None
        assert eng.window_open_spot("BTC", interval_min=5) is None
        assert eng.window_open_spot("BTC", interval_min=15) is None

    def test_returns_spot_after_boundary_observed(self):
        """A single tick at a 5m-aligned boundary stamps the 5m
        window_start_spot. The accessor must return that value."""
        eng = PartialRetEngine()
        aligned_5m = 1_715_923_500  # 5m-aligned (divisible by 300)
        eng.update("BTC", live_spot=67_200.0, now_ts=aligned_5m)
        spot = eng.window_open_spot("BTC", interval_min=5)
        assert spot == pytest.approx(67_200.0)

    def test_5m_and_15m_anchors_are_independent(self):
        """The 5m accessor and 15m accessor read different fields of
        _CoinState. A 5m-only boundary crossing must update only the
        5m anchor; the 15m accessor still returns the previous value."""
        eng = PartialRetEngine()
        aligned_15m = 1_715_923_800  # 15m-aligned (divisible by 900)
        eng.update("BTC", live_spot=67_000.0, now_ts=aligned_15m)
        open_5m_initial = eng.window_open_spot("BTC", interval_min=5)
        open_15m_initial = eng.window_open_spot("BTC", interval_min=15)
        assert open_5m_initial == pytest.approx(67_000.0)
        assert open_15m_initial == pytest.approx(67_000.0)

        # Cross only the 5m boundary — at +300s the 5m window opens
        # but the 15m window stays open (next 15m boundary is +900s).
        eng.update("BTC", live_spot=68_500.0, now_ts=aligned_15m + 300)
        open_5m_after = eng.window_open_spot("BTC", interval_min=5)
        open_15m_after = eng.window_open_spot("BTC", interval_min=15)
        # 5m anchor moved to the new window's open.
        assert open_5m_after is not None
        assert open_5m_after != open_5m_initial
        # 15m anchor still points at the original 15m window.
        assert open_15m_after == pytest.approx(open_15m_initial)

    def test_rejects_unsupported_interval(self):
        """Only 5m and 15m are wired — any other interval_min returns
        None so callers get a hard signal rather than a wrong-bucket lookup."""
        eng = PartialRetEngine()
        eng.update("BTC", live_spot=67_000.0, now_ts=1_715_923_500)
        assert eng.window_open_spot("BTC", interval_min=1) is None
        assert eng.window_open_spot("BTC", interval_min=10) is None
        assert eng.window_open_spot("BTC", interval_min=60) is None
        # Negative + zero edge cases
        assert eng.window_open_spot("BTC", interval_min=0) is None
        assert eng.window_open_spot("BTC", interval_min=-5) is None

    def test_per_coin_isolation_of_window_open_spot(self):
        """Each coin's window-open spot is independent — observing BTC
        does not populate ETH's anchor."""
        eng = PartialRetEngine()
        aligned_5m = 1_715_923_500
        eng.update("BTC", live_spot=67_200.0, now_ts=aligned_5m)
        # ETH never observed
        assert eng.window_open_spot("ETH", interval_min=5) is None
        eng.update("ETH", live_spot=3_500.0, now_ts=aligned_5m)
        # Both populated, independent values
        assert eng.window_open_spot("BTC", interval_min=5) == pytest.approx(67_200.0)
        assert eng.window_open_spot("ETH", interval_min=5) == pytest.approx(3_500.0)


def test_separate_intervals_have_independent_anchors():
    """Per-interval state must not bleed: a 5m anchor refresh must not
    overwrite the 15m anchor.
    """
    eng = PartialRetEngine()
    # Warm up enough history for both intervals.
    aligned_ts = 1_715_923_800  # 15m-aligned
    _warm_up_minutes(
        eng, "BTC", start_ts=aligned_ts - 60 * 60,
        start_spot=67_000.0, n_minutes=60, jitter=0.0005, seed=2,
    )
    eng.update("BTC", live_spot=67_500.0, now_ts=aligned_ts)
    state = eng.state["BTC"]
    open_5m = state.window_start_spot_5m
    open_15m = state.window_start_spot_15m
    assert open_5m is not None and open_15m is not None
    # Cross only the 5m boundary, NOT the 15m boundary.
    eng.update("BTC", live_spot=68_000.0, now_ts=aligned_ts + 300)
    state2 = eng.state["BTC"]
    assert state2.window_start_ts_5m == aligned_ts + 300
    assert state2.window_start_ts_15m == aligned_ts
    # The 15m anchor must be unchanged.
    assert state2.window_start_spot_15m == open_15m


def test_per_coin_state_isolation():
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800
    _warm_up_minutes(
        eng, "BTC", start_ts=aligned_ts - 60 * 60,
        start_spot=67_000.0, n_minutes=60, jitter=0.0005, seed=3,
    )
    _warm_up_minutes(
        eng, "ETH", start_ts=aligned_ts - 60 * 60,
        start_spot=3_500.0, n_minutes=60, jitter=0.001, seed=4,
    )
    eng.update("BTC", live_spot=67_000.0, now_ts=aligned_ts)
    eng.update("ETH", live_spot=3_500.0, now_ts=aligned_ts)
    rv_btc = eng.state["BTC"].rv_1h_at_open_5m
    rv_eth = eng.state["ETH"].rv_1h_at_open_5m
    assert rv_btc is not None and rv_eth is not None
    # Different vol regimes → different rv snapshots.
    assert rv_btc != rv_eth


# ---------------------------------------------------------------------------
# Parity regression vs the canonical training formula
# ---------------------------------------------------------------------------


def _canonical_z(closes_1m: list[float], entry_minute: int) -> float:
    """Recreate ``slice_features_at_E`` from research/notebooks/analysis.ipynb,
    adjusted for the live engine's 1-sample boundary-instant lag.

    Inputs are 1m closes laid out as ``[c_(open-60), ..., c_(open-1),
    c_open, c_open+1, ..., c_open+entry_minute]``. We expect exactly
    60 + 1 + entry_minute items.

    Returns ``partial_ret / (rv_1h_at_open * sqrt(entry_minute))``.

    1-sample lag note (2026-05-19): training's ``rolling(60).std()`` at
    the boundary uses returns ending AT the boundary minute (i.e.,
    includes ``log_close[60] - log_close[59]``). The live engine at the
    boundary INSTANT does not yet know the close of the boundary
    minute — only its open is observed — so its rv estimate spans
    returns 1..59 (59 samples). This canonical mirror uses the same
    59-sample slice so the parity comparison is exact.
    """
    assert len(closes_1m) == 60 + 1 + entry_minute
    log_closes = [math.log(c) for c in closes_1m]
    # 1m log returns spanning minutes 1..59 (the welford content at the
    # boundary instant — minute 60's return is unobservable live until
    # the next boundary).
    rets_pre = [log_closes[i] - log_closes[i - 1] for i in range(1, 60)]
    mean = sum(rets_pre) / len(rets_pre)
    var = sum((r - mean) ** 2 for r in rets_pre) / (len(rets_pre) - 1)
    rv_1h = math.sqrt(var)
    open_ref = log_closes[60]
    final_ref = log_closes[60 + entry_minute]
    partial_ret = final_ref - open_ref
    return partial_ret / (rv_1h * math.sqrt(entry_minute))


def test_parity_against_canonical_training_formula():
    """Drive the engine with the same 1m close series used by the
    canonical formula; demand identical z within float tolerance.

    Parity reflects the live engine's 1-sample boundary-instant lag
    (see ``_canonical_z`` docstring). The engine's welford at the
    window-open boundary holds 59 1m-returns (those spanning
    closes[i-1]→closes[i] for i=1..59); ``_canonical_z`` uses the same
    59-sample slice so the comparison is exact.
    """
    rng = random.Random(99)
    aligned_ts = 1_715_923_800  # 15m-aligned
    n_pre = 60
    entry_min = 7
    n_total = n_pre + 1 + entry_min

    # Random walk in log space, 60 minutes of vol + window open + 7 in-window.
    closes = [67_000.0]
    for _ in range(1, n_total):
        closes.append(closes[-1] * math.exp(rng.gauss(0.0, 0.0008)))

    # Feed minute-by-minute up to the window open instant.
    eng = PartialRetEngine()
    # First closes[0] sits at minute (aligned_ts - n_pre*60), and
    # closes[n_pre] sits at exactly aligned_ts (the window open).
    for i in range(n_pre + 1):
        ts = aligned_ts - n_pre * 60 + i * 60
        eng.update("BTC", live_spot=closes[i], now_ts=ts)

    # Inject the in-window ticks until we reach minute 7 of the window.
    for k in range(1, entry_min + 1):
        ts = aligned_ts + k * 60
        eng.update("BTC", live_spot=closes[n_pre + k], now_ts=ts)

    # Query the z at minute 7 against live_spot = closes[n_pre + 7].
    z_live = eng.current_z(
        "BTC", interval_min=15, live_spot=closes[n_pre + entry_min],
        entry_minute=entry_min,
    )
    z_canon = _canonical_z(closes, entry_min)
    assert z_live is not None
    # Sample variance + log returns → identical up to float noise once
    # the 1-sample lag is mirrored in _canonical_z.
    assert z_live == pytest.approx(z_canon, rel=1e-9, abs=1e-9)


def test_parity_at_1hz_cadence_exercises_previous_tick_anchor():
    """Production drives the engine at 1Hz, not 1/min. The previous-tick
    anchor path (window_start_spot = tick BEFORE the boundary instant)
    only activates when ``cs.last_tick_ts`` is within 5 seconds of the
    boundary — at 1Hz cadence this is the common case; at 1/min cadence
    the engine falls back to the current-tick anchor. The pre-existing
    parity test (``test_parity_against_canonical_training_formula``)
    drives ticks at 1/min so it never exercises the previous-tick path.

    Here we feed 1Hz ticks into the second leading up to the 5m
    boundary so the previous-tick anchor IS the one picked, and verify
    the engine's z agrees with the canonical training formula (with
    open_ref adjusted to the previous-tick spot, not the
    boundary-minute close — that is exactly the divergence the
    previous-tick anchor was designed to embrace).
    """
    rng = random.Random(2026)
    # 15m-aligned boundary (also 5m-aligned). Using the 15m window so
    # entry_min=7 can sit comfortably inside the window without
    # crashing into the next 5m sub-boundary (which would re-stamp
    # window_start and break the parity check).
    boundary_ts = 1_715_923_800 + 3600
    assert boundary_ts % 900 == 0
    n_pre = 60
    entry_min = 7

    # 1m closes: closes[0..n_pre-1] are the minute closes for minutes
    # ending at (boundary_ts - 60*(n_pre - i)). closes[n_pre - 1] is
    # the close at the minute ending one minute before the boundary.
    # closes[n_pre] is the boundary-minute close (== boundary_ts tick).
    closes = [67_000.0]
    for _ in range(1, n_pre + 1 + entry_min):
        closes.append(closes[-1] * math.exp(rng.gauss(0.0, 0.0007)))

    eng = PartialRetEngine()

    # Warm-up: one tick per minute ending one minute BEFORE the
    # boundary. Each tick sits at the close of its 1m bar.
    for i in range(n_pre):
        ts = boundary_ts - (n_pre - i) * 60
        eng.update("BTC", live_spot=closes[i], now_ts=ts)

    # Densify the 59 seconds before the boundary at the LAST warmup
    # close spot. This keeps the 60th 1m bar's "close" equal to
    # closes[n_pre - 1] (so the canonical 1m-return for the boundary
    # bar is preserved), and stamps last_tick_ts at boundary_ts - 1
    # within the 5-second freshness window.
    for sec in range(1, 60):
        eng.update("BTC", live_spot=closes[n_pre - 1], now_ts=boundary_ts - 60 + sec)

    # Boundary cross. previous-tick anchor activates: last_tick_ts =
    # boundary_ts - 1, gap=1 ≤ 5, so window_start_spot_5m =
    # closes[n_pre - 1].
    eng.update("BTC", live_spot=closes[n_pre], now_ts=boundary_ts)
    state = eng.state["BTC"]
    assert state.rv_1h_at_open_15m is not None and state.rv_1h_at_open_15m > 0
    assert state.window_start_spot_15m == pytest.approx(closes[n_pre - 1])

    # Drive entry_min more minute ticks into the window. entry_min=7
    # is comfortably inside the 15m window (no boundary re-cross).
    for k in range(1, entry_min + 1):
        eng.update(
            "BTC", live_spot=closes[n_pre + k], now_ts=boundary_ts + k * 60
        )

    z_live = eng.current_z(
        "BTC", interval_min=15, live_spot=closes[n_pre + entry_min],
        entry_minute=entry_min,
    )
    # Canonical formula adjusted to the engine's anchor choice
    # (open_ref = closes[n_pre - 1], previous-tick anchor) AND to the
    # live engine's 1-sample boundary-instant lag: the welford holds
    # 1m returns spanning closes[i-1]→closes[i] for i=1..n_pre-1 (one
    # short of training's full n_pre samples — see _canonical_z).
    log_closes = [math.log(c) for c in closes]
    rets_pre = [log_closes[i] - log_closes[i - 1] for i in range(1, n_pre)]
    mean = sum(rets_pre) / len(rets_pre)
    var = sum((r - mean) ** 2 for r in rets_pre) / (len(rets_pre) - 1)
    rv_1h = math.sqrt(var)
    expected = (log_closes[n_pre + entry_min] - log_closes[n_pre - 1]) / (
        rv_1h * math.sqrt(entry_min)
    )
    assert z_live is not None
    assert z_live == pytest.approx(expected, rel=1e-9, abs=1e-9)

    # The above synthetic-minute pass covers the 1Hz cadence + previous-
    # tick anchor end-to-end. A second outside-of-process check against
    # ``backtest/features/nx_v1/15m_e7.parquet`` was considered (the
    # review finding's suggestion) but the parquet stores already-
    # bucketed feature rows, not raw 1m closes, so we can't replay it
    # through the engine without a separate minute-close reconstruction
    # path. That replay belongs in a research-side notebook test, not
    # this unit suite. The synthetic-minute parity above is the
    # behavioural pin.


def test_welford_samples_full_minute_returns_at_1hz_cadence():
    """Regression for the 2026-05-19 z-inflation bug.

    Production drives the engine at 1Hz with prices that DRIFT within
    each minute. The welford that feeds ``rv_1h_at_open`` must
    accumulate full minute-to-minute log-returns (close M_t to close
    M_{t-1}), not the gap between the last tick of M_{t-1} and the
    first tick of M_t (which at 1Hz is a ~1-second log-return).

    Historical bug: the boundary-cross welford add was
    ``log(live_spot / last_minute_close)`` where ``live_spot`` is the
    first tick after the boundary. At 1Hz cadence that collapses each
    sample from a 60-second return to a 1-second return, shrinking the
    rv estimate by ~sqrt(60)× and inflating live z-scores ~5× vs
    training — exactly the divergence observed in the 5/17-5/19
    dry-run (mean live |z|=1.89 vs training |z|=0.36 on the same 73
    markets).

    Test design: feed 1Hz ticks where the spot within each minute is
    CONSTANT and changes only at minute boundaries. The welford std
    must equal the std of the minute-anchor log-return series within
    float tolerance — NOT collapse to ~0 like the historical bug did.
    """
    eng = PartialRetEngine()
    rng = random.Random(2026)
    n_minutes = 65
    closes = [67_000.0]
    for _ in range(n_minutes):
        closes.append(closes[-1] * math.exp(rng.gauss(0.0, 0.001)))

    # 1Hz feed: 60 ticks per minute, spot constant within minute, jumps
    # at each minute boundary. The first tick (m=0, sec=0) is the cold
    # start; the first boundary cross (m=1, sec=0) seeds
    # ``prev_minute_close`` without adding to welford; from m=2 onward
    # each boundary cross adds one full-minute log-return.
    start_ts = 1_715_923_800
    for m in range(n_minutes):
        for sec in range(60):
            ts = start_ts + m * 60 + sec
            eng.update("BTC", live_spot=closes[m], now_ts=ts)

    state = eng.state["BTC"]
    welford_std = state.minute_ret_welford.std

    # Trace of welford additions:
    #   m=0           cold start  (no add)
    #   m=1 boundary  prev=None   skip add  (seeds prev=closes[0])
    #   m=2 boundary  add#1 = log(closes[1]/closes[0])
    #   m=3 boundary  add#2 = log(closes[2]/closes[1])
    #   ...
    #   m=64 boundary add#63 = log(closes[63]/closes[62])
    # Welford(maxlen=60) drops adds #1..#3, retains adds #4..#63 =
    # log(closes[i]/closes[i-1]) for i in 4..63.
    log_rets = [
        math.log(closes[i] / closes[i - 1]) for i in range(4, 64)
    ]
    mean = sum(log_rets) / len(log_rets)
    var = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
    expected_std = math.sqrt(var)

    assert welford_std == pytest.approx(expected_std, rel=1e-9, abs=1e-12), (
        f"welford captured {welford_std:.6e}, expected {expected_std:.6e} "
        f"— would indicate the boundary-cross add reverted to using "
        f"`live_spot` (sub-minute return) instead of last_minute_close "
        f"(close-to-close return)."
    )
    # Sanity: vol should be on the order of the ~0.001 minute drift,
    # not the ~1e-6 the historical bug would produce for this test
    # (constant within-minute → log(open_next / close_prev) ≈ 0).
    assert welford_std > 5e-4, (
        f"welford_std={welford_std:.6e} is too small — historical "
        f"sub-minute-return bug regression."
    )


def test_engine_refuses_non_finite_inputs():
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800
    eng.update("BTC", live_spot=float("nan"), now_ts=aligned_ts)
    eng.update("BTC", live_spot=0.0, now_ts=aligned_ts)
    eng.update("BTC", live_spot=-1.0, now_ts=aligned_ts)
    # All ignored; engine remains unstamped for BTC's 5m window.
    assert eng.current_z(
        "BTC", interval_min=5, live_spot=67_000.0, entry_minute=1
    ) is None


def test_min_rv_samples_constant_threshold():
    """Sanity: the parity formula requires enough rv samples to be
    stable. Below the documented threshold, z is None.
    """
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800
    # Only _MIN_RV_1H_SAMPLES - 1 prior minutes — must still return None.
    n_warm = _MIN_RV_1H_SAMPLES - 1
    _warm_up_minutes(
        eng, "BTC", start_ts=aligned_ts - n_warm * 60,
        start_spot=67_000.0, n_minutes=n_warm, jitter=0.0005, seed=5,
    )
    eng.update("BTC", live_spot=67_000.0, now_ts=aligned_ts)
    z = eng.current_z(
        "BTC", interval_min=5, live_spot=67_500.0, entry_minute=4
    )
    assert z is None


def test_z_returns_value_exactly_at_min_rv_samples():
    """Boundary case: with EXACTLY ``_MIN_RV_1H_SAMPLES`` prior minutes of
    1m-return history, the engine should return a non-None z. This
    pairs with ``test_min_rv_samples_constant_threshold`` (which tests
    MIN-1) to pin the inclusive threshold.

    The engine emits one log-return per completed minute boundary, but
    the FIRST boundary cross after a cold start has no
    ``prev_minute_close`` yet and skips the welford add. So we need
    ``_MIN_RV_1H_SAMPLES + 2`` ticks total to produce
    ``_MIN_RV_1H_SAMPLES`` samples (1 cold-start tick + 1 prev-seeding
    tick + N adding ticks).
    """
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800
    # 2026-05-19 fix: bump warmup by 1 to absorb the cold-start sample
    # loss. The first boundary cross has prev_minute_close=None and
    # skips the welford add; subsequent boundary crosses each add one
    # sample.
    n_warm = _MIN_RV_1H_SAMPLES + 1
    _warm_up_minutes(
        eng, "BTC", start_ts=aligned_ts - n_warm * 60,
        start_spot=67_000.0, n_minutes=n_warm, jitter=0.0005, seed=11,
    )
    eng.update("BTC", live_spot=67_050.0, now_ts=aligned_ts)
    state = eng.state["BTC"]
    assert state.minute_ret_welford.n >= _MIN_RV_1H_SAMPLES
    assert state.rv_1h_at_open_5m is not None
    z = eng.current_z(
        "BTC", interval_min=5, live_spot=67_500.0, entry_minute=4
    )
    assert z is not None and math.isfinite(z)


def test_min_rv_samples_30_to_59_warmup_divergence():
    """audit w19oxqf24 (P3): document the accepted 30..59-sample warmup
    divergence vs a strict ``rolling(60).std()`` backtest.

    The floor is 30, not training's 60. In the 30..59-sample warmup
    window the live engine DOES emit an rv_1h (and a z, hence a bet) on a
    cell that a strict ``rolling(60).std()`` (min_periods == 60) backtest
    would drop as NaN. This is the bounded, transient warmup tradeoff —
    not a bug — and raising the floor to 60 was rejected because the live
    engine's 1-sample boundary lag means a 60-sample gate would refuse to
    score the very scenario the canonical training formula scores (see
    ``_canonical_z`` and the ``_MIN_RV_1H_SAMPLES`` constant note).

    Here we land the welford at exactly 45 samples (strictly inside the
    30..59 band) at the window-open boundary and assert the engine emits
    a finite z. A strict-60 gate would return None for this same cell.
    """
    assert _MIN_RV_1H_SAMPLES == 30, (
        "this test pins the accepted 30..59 warmup-divergence band; if the "
        "floor changes, revisit the audit w19oxqf24 P3 decision"
    )
    eng = PartialRetEngine()
    aligned_ts = 1_715_923_800
    # Target 45 welford samples at the boundary. The cold-start tick plus
    # the first (prev-seeding) boundary cross cost 2 ticks before adds
    # begin, so 45 + 2 == 47 warmup minutes lands ~45 adds; the boundary
    # update then makes the count exactly 45.
    target_samples = 45
    assert 30 <= target_samples < 60, "must sit strictly inside the divergence band"
    n_warm = target_samples + 1
    _warm_up_minutes(
        eng, "BTC", start_ts=aligned_ts - n_warm * 60,
        start_spot=67_000.0, n_minutes=n_warm, jitter=0.0005, seed=23,
    )
    eng.update("BTC", live_spot=67_050.0, now_ts=aligned_ts)
    state = eng.state["BTC"]
    n = state.minute_ret_welford.n
    # Strictly inside [30, 60): training's strict rolling(60).std() would be
    # NaN here, but the live engine emits because n >= _MIN_RV_1H_SAMPLES.
    assert 30 <= n < 60, f"expected a 30..59-sample warmup count, got {n}"
    assert state.rv_1h_at_open_5m is not None
    z = eng.current_z(
        "BTC", interval_min=5, live_spot=67_500.0, entry_minute=4
    )
    assert z is not None and math.isfinite(z), (
        "engine must emit a z in the 30..59 warmup band — a strict-60 "
        "backtest would have dropped this cell as NaN (the documented "
        "audit w19oxqf24 P3 divergence)."
    )


# ---------------------------------------------------------------------------
# Gap reset paths (partial_ret.py:241-260)
# ---------------------------------------------------------------------------
#
# When the engine sees a minute-boundary cross after a gap larger than
# the expected 60s cadence, the close-lineage chain (`prev_minute_close`
# -> `last_minute_close`) must not silently span the gap. Two regimes:
#
#   * 60 < gap_seconds <= 300 (1m < gap <= 5m): skip the welford add and
#     drop `prev_minute_close` to None so the NEXT boundary's add does
#     not span the gap. Welford content is retained — a 1-5 minute stall
#     does not invalidate the rv estimate.
#   * gap_seconds > 300 (>5m): full reset. Drop the welford content AND
#     clear `prev_minute_close`. The prior rv samples are stale after a
#     stall this long.


def _seed_with_continuous_minutes(eng, coin, *, start_ts, start_spot,
                                  n_minutes, step_log_ret):
    """Drive the engine through `n_minutes` boundary crosses at 60s
    cadence with a constant per-minute log-return."""
    spot = start_spot
    for i in range(n_minutes):
        ts = start_ts + i * 60
        if i > 0:
            spot = spot * math.exp(step_log_ret)
        eng.update(coin, live_spot=spot, now_ts=ts)
    return spot


def test_gap_between_one_and_five_minutes_drops_prev_close_keeps_welford():
    """1m < gap <= 5m: skip the add and reset prev_minute_close.

    Regression for partial_ret.py:256-260. After a 3-minute stall, the
    next boundary cross must NOT add to the welford (we have no close
    for the skipped minutes), and `prev_minute_close` must be None so
    the boundary after that also skips (until a fresh continuous run
    re-seeds the lineage). The welford's existing samples are kept —
    they're not stale yet.
    """
    eng = PartialRetEngine()
    start_ts = 1_715_923_800  # aligned to a 5m boundary

    # Build up 10 continuous welford samples (m=2..11 boundary crosses,
    # since m=0 is cold-start and m=1 just seeds prev_minute_close).
    _seed_with_continuous_minutes(
        eng, "BTC", start_ts=start_ts, start_spot=67_000.0,
        n_minutes=12, step_log_ret=0.001,
    )
    state = eng.state["BTC"]
    n_before = state.minute_ret_welford.n
    assert n_before == 10
    assert state.prev_minute_close is not None

    # Jump 3 minutes forward (gap = 180s, hits the middle branch).
    gap_ts = start_ts + 12 * 60 + 3 * 60
    eng.update("BTC", live_spot=67_800.0, now_ts=gap_ts)
    state = eng.state["BTC"]

    # Welford content is retained — only the lineage is dropped.
    assert state.minute_ret_welford.n == n_before, (
        f"welford.n changed from {n_before} to {state.minute_ret_welford.n} — "
        f"the 60-300s branch must NOT touch the welford content."
    )
    assert state.prev_minute_close is None, (
        "prev_minute_close should be cleared after a 1-5 minute gap so "
        "the next boundary's add does not span the gap."
    )
    assert state.last_minute_close == 67_800.0
    assert state.last_minute_ts == gap_ts

    # The next boundary cross at the right cadence (60s later) sees
    # prev_minute_close=None and skips the add (no spurious sample).
    eng.update("BTC", live_spot=67_810.0, now_ts=gap_ts + 60)
    state = eng.state["BTC"]
    assert state.minute_ret_welford.n == n_before, (
        "next boundary after the gap reset must skip the welford add — "
        "prev_minute_close was just cleared."
    )
    # prev_minute_close is reseeded at the post-gap boundary so the
    # boundary AFTER that one can resume adding.
    assert state.prev_minute_close == 67_800.0


def test_gap_greater_than_five_minutes_resets_welford_and_prev_close():
    """gap > 5m: full reset (welford AND prev_minute_close).

    Regression for partial_ret.py:252-255. After a stall longer than
    5 minutes the rv estimate is stale, so the welford buffer is
    discarded and the engine returns to cold-start lineage tracking.
    """
    eng = PartialRetEngine()
    start_ts = 1_715_923_800

    # Build up >=30 welford samples — enough that rv_1h_at_open is
    # populated after the next interval-boundary cross.
    _seed_with_continuous_minutes(
        eng, "BTC", start_ts=start_ts, start_spot=67_000.0,
        n_minutes=32, step_log_ret=0.001,
    )
    state = eng.state["BTC"]
    assert state.minute_ret_welford.n >= _MIN_RV_1H_SAMPLES
    assert state.prev_minute_close is not None

    # Jump 10 minutes forward (gap = 600s, hits the >300s branch).
    gap_ts = start_ts + 32 * 60 + 10 * 60
    eng.update("BTC", live_spot=68_500.0, now_ts=gap_ts)
    state = eng.state["BTC"]

    # Welford fully reset.
    assert state.minute_ret_welford.n == 0, (
        f"welford.n={state.minute_ret_welford.n} after >5m gap — should be 0; "
        "the prior samples span a stale window and must be dropped."
    )
    assert state.prev_minute_close is None, (
        "prev_minute_close should be cleared after a >5m gap reset."
    )
    assert state.last_minute_close == 68_500.0
    assert state.last_minute_ts == gap_ts

    # current_z must now return None (welford is below _MIN_RV_1H_SAMPLES
    # and we should be back in cold-start state).
    z = eng.current_z(
        "BTC", interval_min=5, live_spot=68_510.0, entry_minute=4
    )
    assert z is None, (
        f"current_z={z} after a >5m gap reset — should be None until "
        "the welford rewarms to _MIN_RV_1H_SAMPLES."
    )


def test_gap_exactly_300_seconds_falls_in_middle_branch():
    """Boundary case: gap == 300s should hit the 60 < gap <= 300 branch
    (per the strict > 300 condition in partial_ret.py:252).

    `gap_seconds` is computed between floored 1-minute boundaries so it
    is always a multiple of 60. 300s == 5 minute boundaries skipped.
    """
    eng = PartialRetEngine()
    start_ts = 1_715_923_800

    # Seed 12 continuous minutes. last_minute_ts after seeding =
    # start_ts + 11*60.
    _seed_with_continuous_minutes(
        eng, "BTC", start_ts=start_ts, start_spot=67_000.0,
        n_minutes=12, step_log_ret=0.001,
    )
    state = eng.state["BTC"]
    n_before = state.minute_ret_welford.n

    # gap_ts at start_ts + 16*60 → minute_ts = start_ts + 16*60,
    # gap_seconds = 16*60 - 11*60 = 5*60 = 300s.
    gap_ts = start_ts + 16 * 60
    eng.update("BTC", live_spot=67_900.0, now_ts=gap_ts)
    state = eng.state["BTC"]

    assert state.minute_ret_welford.n == n_before, (
        "gap == 300s must NOT reset the welford — only gap > 300 does."
    )
    assert state.prev_minute_close is None, (
        "gap == 300s falls in the middle branch which clears prev_minute_close."
    )


def test_gap_360_seconds_triggers_full_reset():
    """Boundary case: gap == 360s (smallest gap > 300 since
    `gap_seconds` is a multiple of 60) should hit the > 300s full-reset
    branch.
    """
    eng = PartialRetEngine()
    start_ts = 1_715_923_800

    _seed_with_continuous_minutes(
        eng, "BTC", start_ts=start_ts, start_spot=67_000.0,
        n_minutes=12, step_log_ret=0.001,
    )

    # gap_ts at start_ts + 17*60 → gap_seconds = 6*60 = 360s, > 300.
    gap_ts = start_ts + 17 * 60
    eng.update("BTC", live_spot=67_900.0, now_ts=gap_ts)
    state = eng.state["BTC"]

    assert state.minute_ret_welford.n == 0, (
        "gap == 360s must trigger the full welford reset (>300 branch)."
    )
    assert state.prev_minute_close is None


def test_current_z_from_open_uses_explicit_twap_and_boundary_rv():
    eng = PartialRetEngine()
    cs = eng._coin("BTC")
    cs.rv_1h_at_open_5m = 0.002
    cs.window_start_spot_5m = 99.0  # point-open must not influence TWAP scoring
    cs.window_start_twap_5m = 100.05

    z = eng.current_z_from_open(
        "BTC",
        interval_min=5,
        live_spot=101.0,
        open_price=100.0,
        entry_minute=4.5,
        basis_center_log=0.0,
        basis_residual_clip_log=0.0001,
    )

    assert z == pytest.approx(
        (math.log(101.0 / 100.05) + 0.0001) / (0.002 * math.sqrt(4.5))
    )


@pytest.mark.parametrize("open_price", [0.0, -1.0, float("nan")])
def test_current_z_from_open_fails_closed_on_invalid_twap(open_price):
    eng = PartialRetEngine()
    eng._coin("BTC").rv_1h_at_open_15m = 0.002

    assert eng.current_z_from_open(
        "BTC",
        interval_min=15,
        live_spot=101.0,
        open_price=open_price,
        entry_minute=14.5,
        basis_center_log=0.0,
        basis_residual_clip_log=0.0001,
    ) is None


def test_boundary_twap_is_causal_and_uses_matching_windows():
    eng = PartialRetEngine()

    for ts in range(840, 900):
        eng.update("BTC", live_spot=float(ts), now_ts=ts)
    eng.update("BTC", live_spot=9_999.0, now_ts=900)

    assert eng.window_open_twap("BTC", 5) == pytest.approx(
        sum(float(ts) for ts in range(840, 900)) / 60.0
    )
    assert eng.window_open_twap("BTC", 15) == pytest.approx(
        sum(float(ts) for ts in range(840, 900)) / 60.0
    )
    assert eng.window_open_twap("BTC", 5) != 9_999.0


def test_boundary_twap_fails_closed_after_large_tick_gap():
    eng = PartialRetEngine()
    for ts in range(870, 894):
        eng.update("BTC", live_spot=100.0, now_ts=ts)
    eng.update("BTC", live_spot=101.0, now_ts=900)

    assert eng.window_open_twap("BTC", 5) is None
