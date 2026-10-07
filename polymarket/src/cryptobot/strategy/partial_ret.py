"""Live spot features: 1-hour volatility, window-open anchors and the z-score.

The training feature (``slice_features_at_E``) is::

    rets        = 1m log returns from window_open to entry_minute   (length E)
    partial_ret = rets.sum()                                        # cumulative log return
    rv_1h       = std of the last 60 1m log returns, measured at window open
    z_denom     = rv_1h * sqrt(entry_minute)
    z_1h        = partial_ret / z_denom

The engine reproduces it from 1 Hz spot ticks:

* The last tick inside each minute stands in for that minute's close. At every
minute boundary ``log(close_t / close_{t-1})`` goes into a 60-sample rolling
accumulator, matching training's ``ret_1m = log_close.diff()``. * At every
5m/15m window boundary the accumulator's std is frozen as that window's
``rv_1h_at_open``, matching ``ret_1m.rolling(60).std().shift(1)``: at the
boundary the just-finished minute's close is known, the current minute's is
not. * The window-open spot is the last tick before the boundary when there is
one, which is closer to the true open than the first tick after it. * At score
time z = ``log(live_spot / window_start_spot) / (rv_1h_at_open *
sqrt(entry_minute))``. * Anything that cannot be computed honestly (cold boot,
flat spot, missing volatility) returns ``None``, and callers skip the cell.

The TWAP model uses ``current_z_from_open``, which measures the move from a
causal 60-second Binance TWAP at window open instead of a single tick.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterable, Optional

# Rolling window for the 1-minute log-return std that becomes
# ``rv_1h_at_open``, matching ``ret_1m.rolling(60).std()`` in training.
RV_1H_WINDOW_MINUTES = 60

# Minimum 1m-return samples before the rv_1h estimate is used (and a z, hence a
# bet, can be produced).
#
# Training's ``rolling(60).std()`` is NaN until 60 returns exist. This floor is
# 30, not 60, for two reasons. First, the engine is always one sample behind at
# a window boundary (the boundary minute's close is not known yet), so 60
# minutes of closes yield 59 returns; ``test_partial_ret._canonical_z`` replays
# training on exactly that slice and matches live z to 1e-9, and a floor of 60
# would refuse that cell. Second, waiting a full hour after every restart is
# worse than a slightly noisier estimate.
#
# The cost: with 30-59 returns the engine scores cells a strict backtest would
# drop as NaN. That only happens in the first 30-60 minutes after a cold boot
# or a gap of more than 5 minutes, and ``bootstrap_from_minute_bars`` usually
# skips it. Pinned by
# ``test_partial_ret.test_min_rv_samples_30_to_59_warmup_divergence``.
_MIN_RV_1H_SAMPLES = 30

# The opening TWAP uses one Binance observation per second over the same
# trailing 60 seconds as Chainlink, for both intervals. Keep enough history for
# that window plus scheduler jitter.
_SPOT_TWAP_HISTORY_SECONDS = 120
_SPOT_TWAP_MAX_CARRY_SECONDS = 5


@dataclass
class _Welford:
    """Numerically stable running mean and variance (Welford's algorithm).

    Each ``add()`` is O(1) and avoids the cancellation error of a two-pass sum
    of squares. ``buf`` keeps the samples so the oldest can be removed as the
    window rolls.
    """

    maxlen: int
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0
    buf: Deque[float] = field(default_factory=deque)

    def add(self, x: float) -> None:
        # Evict the oldest sample before adding, so the mean and variance
        # always cover the current window.
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
        # Sample variance (divisor n - 1), matching pandas ``rolling().std()``.
        return self.m2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        v = self.variance
        return math.sqrt(v) if v > 0 else 0.0


@dataclass
class _CoinState:
    # Rolling std of 1m log returns, frozen into ``rv_1h_at_open_*`` at each
    # window boundary.
    minute_ret_welford: _Welford = field(
        default_factory=lambda: _Welford(maxlen=RV_1H_WINDOW_MINUTES)
    )
    last_minute_ts: int = -1
    # Latest tick in the current minute. At the next minute boundary it is that
    # minute's close (training's ``log_close[t]`` is the bar close at second
    # 59.999; the last tick in the minute is the closest live value).
    last_minute_close: float = 0.0
    # Close of the minute before ``last_minute_ts``, so the accumulator gets
    # true close-to-close 1m returns. Without it, the sample at a boundary
    # would be ``log(first_tick_of_next_minute / last_tick_of_this_minute)``, a
    # ~1-second return roughly sqrt(60) times too small, which inflates z about
    # fivefold. ``None`` until two consecutive minute closes are seen (cold
    # start, or after a gap reset).
    prev_minute_close: Optional[float] = None

    # Per-interval window state. ``window_start_spot`` is the spot at (or just
    # before) the window open; ``rv_1h_at_open`` is the volatility frozen at
    # open and used in the z denominator.
    window_start_spot_5m: Optional[float] = None
    window_start_twap_5m: Optional[float] = None
    window_start_ts_5m: Optional[int] = None
    rv_1h_at_open_5m: Optional[float] = None

    window_start_spot_15m: Optional[float] = None
    window_start_twap_15m: Optional[float] = None
    window_start_ts_15m: Optional[int] = None
    rv_1h_at_open_15m: Optional[float] = None

    # Last tick seen, used to anchor ``window_start_spot`` at the tick just
    # before the boundary instead of just after.
    last_tick_spot: Optional[float] = None
    last_tick_ts: Optional[int] = None

    # One observation per scanner second. At a market boundary the opening TWAP
    # averages [boundary - window, boundary), excluding the boundary tick so it
    # is strictly causal.
    recent_spot_ticks: Deque[tuple[int, float]] = field(default_factory=deque)


@dataclass(frozen=True)
class MinuteBar:
    """One 1-minute OHLC bar, used to warm-start after a restart."""

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
    """Return a causal per-second TWAP over ``[boundary - window, boundary)``.

    A small scheduler gap carries the last observation forward for up to five
    seconds, the same staleness limit as live spot. A longer gap returns None
    rather than inventing an opening anchor.
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
    """Per-coin volatility and per-interval window anchors."""

    state: dict[str, _CoinState] = field(default_factory=dict)

    def _coin(self, coin: str) -> _CoinState:
        cs = self.state.get(coin)
        if cs is None:
            cs = _CoinState()
            self.state[coin] = cs
        return cs

    def window_open_spot(self, coin: str, interval_min: int) -> Optional[float]:
        """Spot captured at (or one tick before) the latest window open for
        this coin and interval.

        Bets are graded against the window-open price, the same anchor the
        model trains on and Polymarket settles against, never the spot at
        decision time (by then part of the predicted move has already happened,
        and grading against it would mark correct predictions as losses).

        Returns ``None`` when the coin has never been seen, the interval has
        not crossed a boundary yet (booted mid-window), or ``interval_min`` is
        not 5 or 15. Callers must skip on ``None``, not fall back to the
        current spot.
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
        """Binance trailing TWAP frozen at the latest window boundary."""
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

        1. Maintain the rolling 1m log-return accumulator: at each completed
        minute push ``log(close_t / close_{t-1})``. The first observation only
        seeds ``last_minute_close``; no return is produced until the second
        minute closes. 2. Maintain the 5m and 15m window anchors: on a
        boundary, freeze the spot (preferring the tick before the boundary),
        the trailing TWAP, and the current rolling std as ``rv_1h_at_open``.
        """
        if live_spot <= 0 or not math.isfinite(live_spot):
            return

        cs = self._coin(coin)

        # ------------------------------------------------------------------
        # 1) rolling 1m log-return std (60-minute window)
        # ------------------------------------------------------------------
        minute_ts = _floor_to(now_ts, 60)
        if cs.last_minute_ts < 0:
            cs.last_minute_ts = minute_ts
            cs.last_minute_close = live_spot
        elif minute_ts == cs.last_minute_ts:
            # Same minute: keep updating the running close; the latest tick in
            # the minute is the closest live value to the bar close.
            cs.last_minute_close = live_spot
        else:
            # Crossed at least one minute boundary. ``last_minute_close`` is
            # now the close of the minute that just ended;
            # ``prev_minute_close`` (set at the previous boundary) is the close
            # before it. Note ``live_spot`` here is the first tick after the
            # boundary and must not be used as a close.
            #
            #  * gap == 1m: add log(last / prev) and rotate prev = last.
            #  * 1m < gap <= 5m: skip the add and clear prev, because there is no close
            #    for the skipped minutes and the next pair would span the gap.
            #  * gap > 5m: reset the accumulator and prev; the old samples are stale.
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
                # Stale buffer: drop it and warm up from scratch.
                cs.minute_ret_welford = _Welford(maxlen=RV_1H_WINDOW_MINUTES)
                cs.prev_minute_close = None
            else:
                # Short gap: skip the add and clear prev so the next pair does
                # not span the gap.
                cs.prev_minute_close = None
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
        ) in (
            (300, "window_start_spot_5m", "window_start_twap_5m", 60, "window_start_ts_5m", "rv_1h_at_open_5m"),
            (900, "window_start_spot_15m", "window_start_twap_15m", 60, "window_start_ts_15m", "rv_1h_at_open_15m"),
        ):
            w = _floor_to(now_ts, interval_s)
            if getattr(cs, attr_ts) != w:
                # Use the spot at or just before the boundary. Training's open
                # is the 1m open at exactly the boundary, and the previous tick
                # is closer to it than the current one. Fall back to the
                # current tick when there is no previous tick or it is older
                # than 5 seconds.
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

        # Record last_tick after the boundary check, so a boundary sees the
        # tick from before it.
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

        Avoids ~30 minutes of ``SKIP:no_feature`` after every restart while
        keeping the training feature shape: the current window's open comes
        from the bar open, and ``rv_1h_at_open`` from close-to-close 1m returns
        before the window boundary.

        Returns the number of returns loaded into the accumulator for the coin;
        0 means the bootstrap did not take and the engine will warm up from
        live ticks.
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

        # Leave the latest closed bar out of the accumulator. The first live
        # tick of the next minute adds that return, exactly as in ``update``.
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
        for interval_s, attr_spot, attr_ts, attr_rv in (
            (300, "window_start_spot_5m", "window_start_ts_5m", "rv_1h_at_open_5m"),
            (900, "window_start_spot_15m", "window_start_ts_15m", "rv_1h_at_open_15m"),
        ):
            window_start = _floor_to(now_ts, interval_s)
            open_bar = bars_by_start.get(window_start)
            if open_bar is None:
                continue
            setattr(cs, attr_spot, open_bar.open)
            setattr(cs, attr_ts, window_start)
            setattr(cs, attr_rv, _rv_before(window_start))

        return welford.n

    def current_z(
        self,
        coin: str,
        *,
        interval_min: int,
        live_spot: float,
        entry_minute: float,
    ) -> Optional[float]:
        """Return ``partial_ret_z_1h`` for the coin and interval.

        ``entry_minute`` is required; defaulting it to ``interval_min`` would
        scale the denominator by the wrong square root.

        Returns ``None`` when:

        * the window-open spot is not set yet (first tick after boot for the
        interval); * ``rv_1h_at_open`` is ``None`` (fewer than
        ``_MIN_RV_1H_SAMPLES`` returns; see that constant); * the denominator
        is zero or negative (degenerate volatility); * ``entry_minute <= 0``.
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
        # Log return, as in training; the difference from (a - b) / b matters
        # at high volatility.
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
        """Return the source-corrected TWAP z used by the live TWAP model.

        Binance movement is measured from its own causal trailing TWAP at
        window open. The Binance-vs-Chainlink opening basis is centred by a
        median from an earlier period and clipped, then added as the
        point-to-TWAP adjustment. That removes the steady USDT/USD offset
        between the two sources but keeps the small opening-shape signal the
        model was trained on.
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
