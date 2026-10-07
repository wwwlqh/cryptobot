"""Live computation of ``partial_ret_z_1h`` — training-parity formulation.

Canonical training formula (research/notebooks/analysis.ipynb section 4,
``slice_features_at_E``)::

    rets        = 1m log returns from window_open to entry_minute   (vector of length E)
    partial_ret = rets.sum()                                        # cumulative log-return
    rv_1h       = std of last 60 1m-log-returns measured AT window open
    z_denom     = rv_1h * sqrt(entry_minute)
    z_1h        = partial_ret / z_denom

Prior live implementation kept a 3600-element Welford buffer of
tick-level ``(spot - window_start)/window_start`` simple returns and
returned a rolling z-score over that mixture. That reference
distribution has nothing to do with the training-time reference
distribution — it mixes within-window partial-returns at every time
offset T, so a live bucket-9 lookup never sees what a training-time
bucket-9 sample looked like.

This rewrite restores parity:

* Track the last in-minute tick at every completed minute boundary as
  that minute's CLOSE proxy, and the close of the prior minute as
  ``prev_minute_close``. At each minute-boundary crossing emit
  ``log(close_t / close_{t-1})`` into a Welford accumulator of size 60
  — matching training's ``ret_1m = log_close.diff()``.
* At every interval boundary crossing, snapshot the current Welford std
  as ``rv_1h_at_open`` (per interval) — this is the volatility
  measurement the training pipeline aligns to the window open via
  ``ret_1m.rolling(60).std().shift(1).reindex(grid)``. The shift makes the
  historical feature causal: at the boundary instant the close of the
  just-completed minute is known but the close of the current minute is not.
* At score time, compute ``log(live_spot / window_start_spot) /
  (rv_1h_at_open * sqrt(entry_minute))``.
* Window-start spot tracking now prefers the **last tick observed before
  the boundary** over the first tick after it (training uses the
  log_open at window-open exactly; the previous tick is closer to that
  instant than the first-after-boundary tick).
* Returns ``None`` while ``rv_1h_at_open`` is missing or zero (cold
  boot, first hour of operation, or a flat-spot regime). Callers treat
  ``None`` as ``no_feature`` SKIP — the old "fall back to raw return
  when the buffer is thin" behaviour is exactly what bent live's bucket
  distribution and is no longer offered.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterable, Optional

# Rolling window for the 1-minute log-return std that becomes ``rv_1h_at_open``.
# Matches ``ret_1m.rolling(60).std()`` in research/notebooks/analysis.ipynb section 4.
RV_1H_WINDOW_MINUTES = 60

# Minimum 1m-return samples required before we trust the rv_1h estimate and
# emit a z (hence a bet).
#
# audit w19oxqf24 (P3) — honest accounting of a known, BOUNDED warmup
# divergence vs training:
#
#   Training (polymarket/backtest/features/build.py) computes rv via
#   ``ret_1m.rolling(60).std()``, whose default ``min_periods == window``
#   means the value is NaN until 60 contiguous 1m-returns exist — so a
#   strict-parity backtest drops every cell with < 60 samples.
#
#   This floor is 30, NOT 60. Raising it to 60 was evaluated and REJECTED:
#   the live engine carries an intrinsic 1-sample boundary-instant lag
#   (at a window-open boundary the close of the boundary minute is not yet
#   observed, only its open), so a window-open snapshot taken after 60
#   contiguous minutes of closes holds only 59 returns. The canonical
#   parity mirror (``test_partial_ret._canonical_z``) replays training on
#   exactly that 59-sample slice and matches the live z to 1e-9. A literal
#   ``n >= 60`` gate would force ``rv_1h_at_open = None`` on that very
#   parity-correct scenario — i.e. it would make live REFUSE to score a
#   cell the canonical training formula DOES score. So 60 is more divergent
#   here, not less.
#
#   The accepted tradeoff: for 30..59 contiguous 1m-returns the live engine
#   emits an rv_1h (and a z, hence a bet) on cells a strict rolling(60).std()
#   backtest would drop as NaN. This is a transient warmup-only regime
#   (first ~30..60 minutes after a cold boot or a >5min gap reset; the
#   ``bootstrap_from_minute_bars`` warm-start usually skips it entirely),
#   the rv estimate carries a wider error bar there, and the alternative —
#   sitting idle for a full hour after every restart — is worse for a
#   live bot. The boundary is pinned by
#   ``test_partial_ret.test_min_rv_samples_30_to_59_warmup_divergence``.
_MIN_RV_1H_SAMPLES = 30

# The live TWAP feature uses one Binance observation per wall second over the
# same trailing 60-second window as Chainlink for both live intervals. Retain
# enough history for the longest window plus ordinary scheduler jitter.
_SPOT_TWAP_HISTORY_SECONDS = 120
_SPOT_TWAP_MAX_CARRY_SECONDS = 5


@dataclass
class _Welford:
    """Numerically stable running mean + variance.

    See Knuth TAOCP vol 2 §4.2.2. Each ``add()`` is O(1) and avoids the
    catastrophic cancellation of a two-pass sum-of-(x-mean)**2 over many
    samples. ``buf`` retains the underlying deque so older observations
    can be removed when the rolling window shifts.
    """

    maxlen: int
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0
    buf: Deque[float] = field(default_factory=deque)

    def add(self, x: float) -> None:
        # Evict the oldest sample when the buffer is full BEFORE the
        # incremental update so the running mean/variance always reflect
        # the current window.
        if self.maxlen > 0 and len(self.buf) >= self.maxlen:
            old = self.buf.popleft()
            self.n -= 1
            if self.n > 0:
                old_delta = old - self.mean
                self.mean -= old_delta / self.n
                self.m2 -= old_delta * (old - self.mean)
            else:
                self.mean = 0.0
                self.m2 = 0.0
        self.buf.append(x)
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        # Sample variance (divisor n - 1) to match pandas ``rolling().std()``
        # default ``ddof=1`` used by the training feature builder.
        return self.m2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        v = self.variance
        return math.sqrt(v) if v > 0 else 0.0


@dataclass
class _CoinState:
    # Rolling 1m-log-return std measured at "now" — snapshotted into the
    # per-interval ``rv_1h_at_open_*`` fields at every window boundary.
    minute_ret_welford: _Welford = field(
        default_factory=lambda: _Welford(maxlen=RV_1H_WINDOW_MINUTES)
    )
    last_minute_ts: int = -1
    # Latest tick observed within the most recent minute. At the next
    # minute-boundary crossing this is the proxy for that minute's CLOSE
    # (training-time ``log_close[t]`` is the bar close at second 59.999
    # of minute t; the last in-minute tick is the closest live analogue).
    last_minute_close: float = 0.0
    # Close of the minute BEFORE ``last_minute_ts`` — required so the
    # welford accumulates true 1m close-to-close log-returns
    # ``log(close_t / close_{t-1})`` (training's ``log_close.diff()``).
    # Without this, at sub-minute tick cadence the boundary-cross sample
    # collapses to ``log(first_tick_of_M+1 / last_tick_of_M) ≈ 1-second
    # return``, which at 1Hz cadence is ~sqrt(60)× smaller in std than
    # the training-time 1m return — the 2026-05-19 z-inflation bug.
    # ``None`` when two consecutive minute closes are not yet observed
    # (cold start, or after a >5min gap reset).
    prev_minute_close: Optional[float] = None

    # Per-interval window state. ``window_start_spot`` is the spot at (or
    # nearest before) the interval-open instant; ``rv_1h_at_open`` is the
    # frozen-at-open volatility used as the z denominator scaler.
    window_start_spot_5m: Optional[float] = None
    window_start_twap_5m: Optional[float] = None
    window_start_ts_5m: Optional[int] = None
    rv_1h_at_open_5m: Optional[float] = None

    window_start_spot_15m: Optional[float] = None
    window_start_twap_15m: Optional[float] = None
    window_start_ts_15m: Optional[int] = None
    rv_1h_at_open_15m: Optional[float] = None

    # ORB (opening-range-breakout) — close of the FIRST minute of the current
    # window, per interval. Backtest range_ret = log_close@grid - log_open@grid
    # (the 1-minute opening-range return); ``range_spot`` is the live close-of-
    # minute-1 proxy (the last in-minute tick of [window_start, window_start+60))
    # and ``window_start_spot`` is the open proxy, so range_partial_ret =
    # log(range_spot / window_start_spot) reproduces the backtest range_ret in
    # the same parity sense as ``rv_1h_at_open`` (both use last_minute_close as
    # the 1m close). Reset to None when a new window opens; set when that
    # window's first minute completes. ``None`` until then (cold boot, or the
    # entry-minute==1 decision tick where the opening range is not yet closed)
    # — callers treat None as a hard ORB skip (orb_no_feature).
    range_spot_5m: Optional[float] = None
    range_spot_15m: Optional[float] = None

    # 5s ORB opening-range anchor for the sub-minute checkpoint sleeves — the
    # backtest step5/step1last grids use range_sec=5, NOT the 60s first minute.
    # Spot at ~window_open+5s, captured live once per window; None until then
    # (and on a mid-window boot the +5s instant already passed, so it stays None
    # and the checkpoint ORB gate fails closed = orb_no_feature).
    range5s_spot_5m: Optional[float] = None
    range5s_spot_15m: Optional[float] = None

    # Last tick observed — used to seed ``window_start_spot`` with the tick
    # immediately before the boundary instead of the tick immediately after
    # (the training-time open is the bar-open at exactly the boundary; the
    # previous tick is closer to that instant than the next tick).
    last_tick_spot: Optional[float] = None
    last_tick_ts: Optional[int] = None

    # One observation per scanner wall-second. At a market boundary the live
    # feature averages [boundary-window, boundary), excluding the current
    # boundary tick so the opening TWAP is strictly causal.
    recent_spot_ticks: Deque[tuple[int, float]] = field(default_factory=deque)


@dataclass(frozen=True)
class MinuteBar:
    """One Binance-style 1-minute OHLC bar used for boot warm-start."""

    start_ts: int
    open: float
    close: float
    is_closed: bool = True


def _floor_to(ts: int, step: int) -> int:
    return (ts // step) * step


def _trailing_spot_twap(
    ticks: Deque[tuple[int, float]],
    *,
    boundary_ts: int,
    window_s: int,
) -> Optional[float]:
    """Return a causal per-second TWAP over ``[boundary-window, boundary)``.

    Small scheduler gaps carry the latest observation forward for at most five
    seconds, matching the live spot-staleness contract. Larger gaps fail closed
    rather than manufacturing an opening anchor.
    """
    if window_s <= 0:
        return None
    observations = [
        (ts, spot)
        for ts, spot in ticks
        if ts < boundary_ts and ts >= boundary_ts - window_s - _SPOT_TWAP_MAX_CARRY_SECONDS
    ]
    if not observations:
        return None
    values: list[float] = []
    cursor = 0
    latest_ts: Optional[int] = None
    latest_spot: Optional[float] = None
    for second in range(boundary_ts - window_s, boundary_ts):
        while cursor < len(observations) and observations[cursor][0] <= second:
            latest_ts, latest_spot = observations[cursor]
            cursor += 1
        if (
            latest_ts is None
            or latest_spot is None
            or second - latest_ts > _SPOT_TWAP_MAX_CARRY_SECONDS
            or latest_spot <= 0.0
            or not math.isfinite(latest_spot)
        ):
            return None
        values.append(latest_spot)
    return math.fsum(values) / len(values) if values else None


@dataclass
class PartialRetEngine:
    """Per-coin volatility + per-interval window anchors."""

    state: dict[str, _CoinState] = field(default_factory=dict)

    def _coin(self, coin: str) -> _CoinState:
        cs = self.state.get(coin)
        if cs is None:
            cs = _CoinState()
            self.state[coin] = cs
        return cs

    def window_open_spot(self, coin: str, interval_min: int) -> Optional[float]:
        """Live spot captured AT (or one tick before) the most recent
        window-open boundary for this (coin, interval).

        Divergence #13 fix: the bot's self-resolution must compare against
        the window-open price (the same anchor the model trains against
        and Polymarket settles on), NOT against the decision-time spot.
        Decision-time spot is 4-10 minutes into the window for a v6_5m
        or v6_15m sleeve — by then the directional move that the model
        was meant to predict has already partially materialized into the
        market, and grading the bet against that later anchor systematically
        marks model-correct bets as losses whenever most of the move
        happened before the bot acted.

        Returns ``None`` when:
          * the coin is unknown (engine has never observed a tick for it)
          * the interval has not yet seen its first boundary crossing
            (cold-start case — bot booted mid-window, no anchor captured)
          * ``interval_min`` is not one of the supported values

        Callers MUST treat ``None`` as a hard skip rather than a
        fall-through to decision-time spot — the whole point of the fix
        is to refuse to grade bets we can't honestly grade.
        """
        cs = self.state.get(coin)
        if cs is None:
            return None
        if interval_min == 5:
            return cs.window_start_spot_5m
        if interval_min == 15:
            return cs.window_start_spot_15m
        return None

    def window_open_twap(self, coin: str, interval_min: int) -> Optional[float]:
        """Binance trailing TWAP frozen at the latest interval boundary."""
        cs = self.state.get(coin)
        if cs is None:
            return None
        if interval_min == 5:
            return cs.window_start_twap_5m
        if interval_min == 15:
            return cs.window_start_twap_15m
        return None

    def update(
        self,
        coin: str,
        *,
        live_spot: float,
        now_ts: int,
    ) -> None:
        """Ingest one spot tick.

        Two responsibilities:

        1. Maintain the rolling 1m-log-return Welford. On every completed
           minute boundary, push ``log(close_t / close_{t-1})`` into the
           buffer. The very first observation only seeds the
           ``last_minute_close``; no return is generated until the second
           minute closes.
        2. Maintain the per-interval window anchors (5m and 15m). On a
           boundary crossing, snapshot the previous-tick spot (closer to
           the open boundary than the current tick) as the new
           ``window_start_spot``, and snapshot the current Welford std
           as the new ``rv_1h_at_open``.
        """
        if live_spot <= 0 or not math.isfinite(live_spot):
            return

        cs = self._coin(coin)

        # ------------------------------------------------------------------
        # 1) rolling 1m-log-return std (60-minute window)
        # ------------------------------------------------------------------
        minute_ts = _floor_to(now_ts, 60)
        if cs.last_minute_ts < 0:
            cs.last_minute_ts = minute_ts
            cs.last_minute_close = live_spot
        elif minute_ts == cs.last_minute_ts:
            # Same minute — keep refreshing the running close so we capture
            # the most recent in-minute spot. Training uses bar closes; at
            # boundary time the most-recent tick within the minute is the
            # closest live proxy for the would-be bar close.
            cs.last_minute_close = live_spot
        else:
            # Crossed at least one minute boundary. The just-ended minute
            # is now complete: ``last_minute_close`` is its CLOSE (the
            # latest tick observed before the boundary). To emit the 1m
            # log-return for THAT minute, we need the close of the
            # minute before it — tracked separately as
            # ``prev_minute_close`` (set at the previous boundary).
            #
            # Bug fix (2026-05-19): previously this added
            # ``log(live_spot / cs.last_minute_close)`` where
            # ``live_spot`` is the FIRST tick AFTER the boundary — that
            # is a ~1-second log-return at 1Hz cadence, not a 1-minute
            # one. The resulting rv collapsed by ~sqrt(60)×, inflating
            # live z-scores by ~5× relative to training, saturating
            # ``model_p`` near 0.95 on most candidates and pushing ~80%
            # of dry-run bets through the V6 edge gates spuriously
            # (live WR 41% vs backtest 80%).
            #
            # Policy:
            #   * gap == 1m  → close-to-close add via prev/last; rotate
            #     prev = last
            #   * 1m < gap <= 5m → skip the add AND reset
            #     ``prev_minute_close`` (we have no close for the
            #     skipped minutes, so the next boundary's prev/last pair
            #     would span a multi-minute discontinuity)
            #   * gap > 5m → reset welford AND prev — the prior samples
            #     span a stall and the rv estimate is stale regardless
            gap_seconds = minute_ts - cs.last_minute_ts
            if gap_seconds == 60:
                if (
                    cs.prev_minute_close is not None
                    and cs.prev_minute_close > 0
                    and cs.last_minute_close > 0
                ):
                    cs.minute_ret_welford.add(
                        math.log(cs.last_minute_close / cs.prev_minute_close)
                    )
                cs.prev_minute_close = cs.last_minute_close
            elif gap_seconds > 300:
                # Buffer is stale — drop it and start a clean warmup.
                cs.minute_ret_welford = _Welford(maxlen=RV_1H_WINDOW_MINUTES)
                cs.prev_minute_close = None
            else:
                # 1m < gap_seconds <= 5m → skip the welford add and drop
                # the close lineage so the next boundary's prev/last
                # pair doesn't span the gap.
                cs.prev_minute_close = None
            # ORB range capture: the minute that just ended is
            # ``cs.last_minute_ts`` (still the OLD value here) and its CLOSE
            # is ``cs.last_minute_close``. If that minute was the FIRST minute
            # of the current window for an interval (its start == that
            # interval's window_start_ts, which the window-anchor section
            # below has NOT yet advanced for this tick), its close is the
            # window's opening-range close. Capture once per window.
            ended_minute_ts = cs.last_minute_ts
            ended_close = cs.last_minute_close
            if ended_close > 0 and math.isfinite(ended_close):
                if (
                    cs.window_start_ts_5m is not None
                    and ended_minute_ts == cs.window_start_ts_5m
                    and cs.range_spot_5m is None
                ):
                    cs.range_spot_5m = ended_close
                if (
                    cs.window_start_ts_15m is not None
                    and ended_minute_ts == cs.window_start_ts_15m
                    and cs.range_spot_15m is None
                ):
                    cs.range_spot_15m = ended_close
            cs.last_minute_ts = minute_ts
            cs.last_minute_close = live_spot

        # ------------------------------------------------------------------
        # 2) per-interval window anchors
        # ------------------------------------------------------------------
        for (
            interval_s,
            attr_spot,
            attr_twap,
            twap_window_s,
            attr_ts,
            attr_rv,
            attr_range,
            attr_range5s,
        ) in (
            (300, "window_start_spot_5m", "window_start_twap_5m", 60, "window_start_ts_5m", "rv_1h_at_open_5m", "range_spot_5m", "range5s_spot_5m"),
            (900, "window_start_spot_15m", "window_start_twap_15m", 60, "window_start_ts_15m", "rv_1h_at_open_15m", "range_spot_15m", "range5s_spot_15m"),
        ):
            w = _floor_to(now_ts, interval_s)
            if getattr(cs, attr_ts) != w:
                # Prefer the spot AT or just BEFORE the boundary. Training's
                # log_open is the 1m-open at exactly the boundary; the
                # last_tick_spot (one tick before now) is closer to that
                # instant than the current tick. Fall back to the current
                # tick on cold-start when last_tick is absent or stale
                # (older than the boundary).
                anchor_spot = live_spot
                if (
                    cs.last_tick_spot is not None
                    and cs.last_tick_ts is not None
                    and cs.last_tick_ts < w
                    and (now_ts - cs.last_tick_ts) <= 5
                ):
                    anchor_spot = cs.last_tick_spot
                setattr(cs, attr_spot, anchor_spot)
                setattr(
                    cs,
                    attr_twap,
                    _trailing_spot_twap(
                        cs.recent_spot_ticks,
                        boundary_ts=w,
                        window_s=twap_window_s,
                    ),
                )
                setattr(cs, attr_ts, w)
                rv = cs.minute_ret_welford.std
                if cs.minute_ret_welford.n >= _MIN_RV_1H_SAMPLES and rv > 0:
                    setattr(cs, attr_rv, rv)
                else:
                    setattr(cs, attr_rv, None)
                # New window → the opening range is not closed yet. Clear the
                # stale prior-window range_spot(s) so range_partial_ret returns
                # None until this window's first minute (60s) / +5s completes.
                setattr(cs, attr_range, None)
                setattr(cs, attr_range5s, None)
            # 5s ORB anchor (sub-minute checkpoint sleeves, backtest range_sec=5):
            # capture the spot at the first tick crossing window_open+5s while we
            # were already observing this window (prev tick in [open, +5s)). A
            # mid-window boot / >5s tick gap across +5s leaves it None so the
            # checkpoint ORB gate fails closed (orb_no_feature). Sibling of the
            # boundary branch above — runs every tick until captured, then the
            # None-check below short-circuits it for the rest of the window.
            if getattr(cs, attr_range5s) is None:
                ws5 = getattr(cs, attr_ts)
                if (
                    ws5 is not None
                    and now_ts >= ws5 + 5
                    and cs.last_tick_ts is not None
                    and ws5 <= cs.last_tick_ts < ws5 + 5
                ):
                    setattr(cs, attr_range5s, live_spot)

        # last_tick is recorded AFTER the boundary check so a boundary
        # crossing sees the tick that came before the boundary, not after.
        cs.last_tick_spot = live_spot
        cs.last_tick_ts = now_ts
        if cs.recent_spot_ticks and cs.recent_spot_ticks[-1][0] == now_ts:
            cs.recent_spot_ticks[-1] = (now_ts, live_spot)
        elif not cs.recent_spot_ticks or cs.recent_spot_ticks[-1][0] < now_ts:
            cs.recent_spot_ticks.append((now_ts, live_spot))
        cutoff = now_ts - _SPOT_TWAP_HISTORY_SECONDS
        while cs.recent_spot_ticks and cs.recent_spot_ticks[0][0] < cutoff:
            cs.recent_spot_ticks.popleft()

    def bootstrap_from_minute_bars(
        self,
        coin: str,
        bars: Iterable[MinuteBar],
        *,
        now_ts: int,
    ) -> int:
        """Warm-start feature state from recent 1-minute OHLC bars.

        This avoids a 30-minute ``SKIP:no_feature`` period after every
        deploy/restart while keeping the same training-parity feature shape:
        current-window open comes from the minute bar open, and
        ``rv_1h_at_open`` comes from close-to-close 1m returns before the
        interval boundary.

        Returns the number of close-to-close returns loaded into the live
        Welford state for the coin. A return of 0 means the caller should
        treat the bootstrap as ineffective and rely on organic warmup.
        """
        normalized = sorted(
            (
                b for b in bars
                if (
                    b.start_ts > 0
                    and b.open > 0.0
                    and b.close > 0.0
                    and math.isfinite(b.open)
                    and math.isfinite(b.close)
                )
            ),
            key=lambda b: b.start_ts,
        )
        if len(normalized) < _MIN_RV_1H_SAMPLES + 2:
            return 0

        closed = [b for b in normalized if b.is_closed]
        if len(closed) < _MIN_RV_1H_SAMPLES + 2:
            return 0

        returns: list[tuple[int, float]] = []
        contiguous_closed: list[MinuteBar] = [closed[0]]
        for prev, cur in zip(closed, closed[1:]):
            if cur.start_ts - prev.start_ts != 60:
                returns = []
                contiguous_closed = [cur]
                continue
            ret = math.log(cur.close / prev.close)
            returns.append((cur.start_ts, ret))
            contiguous_closed.append(cur)

        # Leave the latest closed bar out of the Welford state. The first
        # live tick in the next minute will add that return exactly once,
        # mirroring the normal boundary-crossing path in ``update``.
        if len(contiguous_closed) < _MIN_RV_1H_SAMPLES + 2 or len(returns) < _MIN_RV_1H_SAMPLES:
            return 0
        state_returns = returns[:-1]
        if len(state_returns) < _MIN_RV_1H_SAMPLES:
            return 0

        cs = self._coin(coin)
        welford = _Welford(maxlen=RV_1H_WINDOW_MINUTES)
        for _ts, ret in state_returns[-RV_1H_WINDOW_MINUTES:]:
            welford.add(ret)
        cs.minute_ret_welford = welford

        prev_closed = contiguous_closed[-2]
        latest_closed = contiguous_closed[-1]
        cs.prev_minute_close = prev_closed.close
        cs.last_minute_ts = latest_closed.start_ts
        cs.last_minute_close = latest_closed.close
        cs.last_tick_spot = latest_closed.close
        cs.last_tick_ts = latest_closed.start_ts + 59

        def _rv_before(boundary_ts: int) -> Optional[float]:
            eligible = [ret for ts, ret in returns if ts < boundary_ts]
            if len(eligible) < _MIN_RV_1H_SAMPLES:
                return None
            wf = _Welford(maxlen=RV_1H_WINDOW_MINUTES)
            for ret in eligible[-RV_1H_WINDOW_MINUTES:]:
                wf.add(ret)
            rv = wf.std
            return rv if rv > 0.0 else None

        bars_by_start = {b.start_ts: b for b in normalized}
        for interval_s, attr_spot, attr_ts, attr_rv, attr_range in (
            (300, "window_start_spot_5m", "window_start_ts_5m", "rv_1h_at_open_5m", "range_spot_5m"),
            (900, "window_start_spot_15m", "window_start_ts_15m", "rv_1h_at_open_15m", "range_spot_15m"),
        ):
            window_start = _floor_to(now_ts, interval_s)
            open_bar = bars_by_start.get(window_start)
            if open_bar is None:
                continue
            setattr(cs, attr_spot, open_bar.open)
            setattr(cs, attr_ts, window_start)
            setattr(cs, attr_rv, _rv_before(window_start))
            # ORB range_spot: the window's first-minute bar IS open_bar (its
            # start == window_start); its CLOSE is the opening-range close.
            # Only valid once that minute has actually completed (closed bar
            # and now_ts past its end) — else leave None so range_partial_ret
            # refuses until the live boundary path captures it.
            if open_bar.is_closed and open_bar.close > 0 and now_ts >= window_start + 60:
                setattr(cs, attr_range, float(open_bar.close))
            else:
                setattr(cs, attr_range, None)

        return welford.n

    def current_z(
        self,
        coin: str,
        *,
        interval_min: int,
        live_spot: float,
        entry_minute: float,
    ) -> Optional[float]:
        """Return the current ``partial_ret_z_1h`` for the coin × interval.

        ``entry_minute`` is required (review-pass #20 closeout — the
        previous ``Optional[int] = None`` fallback to ``interval_min``
        silently produced a z whose denominator was scaled by
        ``sqrt(interval_min)`` instead of ``sqrt(entry_minute)``, which
        for a 15m sleeve at e=4 is off by ``sqrt(15/4) ≈ 1.94``). All
        production callers (``score_cell_hgb``) pass ``entry_minute``
        explicitly.

        Returns ``None`` when the engine cannot produce a parity z:

        * window-start spot not yet stamped (very first tick after boot
          for the relevant interval)
        * ``rv_1h_at_open`` snapshot still ``None`` (< ``_MIN_RV_1H_SAMPLES``
          == 30 1m-returns accumulated; see the constant's audit w19oxqf24
          note for why this floor is 30 and not training's 60)
        * a zero/non-positive denominator (degenerate vol)
        * ``entry_minute <= 0`` (degenerate `sqrt(0)` denominator)
        """
        cs = self.state.get(coin)
        if cs is None:
            return None
        if interval_min == 5:
            ws = cs.window_start_spot_5m
            rv = cs.rv_1h_at_open_5m
        elif interval_min == 15:
            ws = cs.window_start_spot_15m
            rv = cs.rv_1h_at_open_15m
        else:
            return None
        if ws is None or ws <= 0 or not math.isfinite(ws):
            return None
        if live_spot <= 0 or not math.isfinite(live_spot):
            return None
        if rv is None or rv <= 0:
            return None
        # Numerator: log-return, training-parity. log(a/b) ≈ (a-b)/b for
        # small returns but the gap matters at high vol; training uses
        # log explicitly.
        partial_ret = math.log(live_spot / ws)
        em = entry_minute
        if em <= 0:
            return None
        z_denom = rv * math.sqrt(em)
        if z_denom <= 0:
            return None
        return partial_ret / z_denom

    def current_z_from_open(
        self,
        coin: str,
        *,
        interval_min: int,
        live_spot: float,
        open_price: float,
        entry_minute: float,
        basis_center_log: float,
        basis_residual_clip_log: float,
    ) -> Optional[float]:
        """Return the source-corrected TWAP z for the frozen live contract.

        Binance movement is measured relative to its own causal trailing TWAP.
        The Binance-vs-Chainlink opening basis is centred by an earlier-period
        median and clipped before being added as the point-to-TWAP adjustment.
        This removes the persistent USDT/USD source offset without discarding
        the small opening-shape feature used by training.
        """
        cs = self.state.get(coin)
        if cs is None:
            return None
        if interval_min == 5:
            rv = cs.rv_1h_at_open_5m
            binance_twap = cs.window_start_twap_5m
        elif interval_min == 15:
            rv = cs.rv_1h_at_open_15m
            binance_twap = cs.window_start_twap_15m
        else:
            return None
        if open_price <= 0 or not math.isfinite(open_price):
            return None
        if live_spot <= 0 or not math.isfinite(live_spot):
            return None
        if binance_twap is None or binance_twap <= 0 or not math.isfinite(binance_twap):
            return None
        if rv is None or rv <= 0 or not math.isfinite(rv):
            return None
        if not math.isfinite(basis_center_log):
            return None
        if basis_residual_clip_log < 0 or not math.isfinite(basis_residual_clip_log):
            return None
        if entry_minute <= 0 or not math.isfinite(entry_minute):
            return None
        denom = rv * math.sqrt(entry_minute)
        if denom <= 0 or not math.isfinite(denom):
            return None
        basis_residual = math.log(binance_twap / open_price) - basis_center_log
        basis_residual = max(
            -basis_residual_clip_log,
            min(basis_residual_clip_log, basis_residual),
        )
        numerator = math.log(live_spot / binance_twap) + basis_residual
        return numerator / denom

    def entry_partial_ret(
        self,
        coin: str,
        *,
        interval_min: int,
        live_spot: float,
    ) -> Optional[float]:
        """ORB ``entry_ret``: cumulative log-return window-open → ``live_spot``.

        This is exactly the NUMERATOR of ``current_z`` (``log(live_spot /
        window_start_spot)``), so it inherits the proven z-parity: the
        backtest's ``entry_ret`` at entry-minute E equals
        ``partial_ret_z_1h * rv_1h * sqrt(E)``, and dividing this accessor by
        the same ``rv_1h * sqrt(E)`` reproduces the live z to 1e-9. Returns
        ``None`` on the same window-not-anchored / degenerate-spot conditions
        as ``current_z`` (minus the rv requirement, which ORB's entry_ret does
        not need).
        """
        cs = self.state.get(coin)
        if cs is None:
            return None
        if interval_min == 5:
            ws = cs.window_start_spot_5m
        elif interval_min == 15:
            ws = cs.window_start_spot_15m
        else:
            return None
        if ws is None or ws <= 0 or not math.isfinite(ws):
            return None
        if live_spot <= 0 or not math.isfinite(live_spot):
            return None
        return math.log(live_spot / ws)

    def range_partial_ret(
        self,
        coin: str,
        *,
        interval_min: int,
        range_sec: int = 60,
    ) -> Optional[float]:
        """ORB ``range_ret``: the opening-range log-return.

        ``range_sec`` selects the opening-range length: 60 (default) uses the
        window's first-minute close (baseline sleeves); 5 uses the
        ``window_open+5s`` anchor the sub-minute checkpoint sleeves need
        (backtest step5/step1last ``range_sec=5``). Any other value falls back
        to the 60s anchor.

        ``log(range_spot / window_start_spot)`` where ``range_spot`` is the
        close of the window's first minute and ``window_start_spot`` is the
        window-open anchor — the live mirror of the backtest's
        ``log_close@grid - log_open@grid`` (range_minute == 1). Returns
        ``None`` until the window's first minute has completed (cold boot, or
        the entry-minute==1 tick where the opening range is not yet closed) so
        ORB fails closed rather than gating on a half-formed range.
        """
        cs = self.state.get(coin)
        if cs is None:
            return None
        if interval_min == 5:
            ws = cs.window_start_spot_5m
            rs = cs.range5s_spot_5m if range_sec == 5 else cs.range_spot_5m
        elif interval_min == 15:
            ws = cs.window_start_spot_15m
            rs = cs.range5s_spot_15m if range_sec == 5 else cs.range_spot_15m
        else:
            return None
        if ws is None or ws <= 0 or not math.isfinite(ws):
            return None
        if rs is None or rs <= 0 or not math.isfinite(rs):
            return None
        return math.log(rs / ws)


# ``seed_for_tests`` was historically defined here but lives in the test
# tree now (``cryptobot.tests._fixtures``). The production module no
# longer carries test-only surface (review #7, 2026-05-18).
