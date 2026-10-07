"""Per-second entry decision for sub-minute sleeves.

``WindowDecider`` decides, second by second, whether a scored cell places a
bet, enforcing at most one bet per market. It matches the backtest evaluator
``polymarket/backtest/lib/pool_study.py`` bet for bet.

``V2_SPEC`` holds the defaults the decider falls back to (and the parity tests
pin):

  * intervals: 5m and 15m;
  * ask band ``0.65 < ask <= 0.85`` (open low, closed high);
  * offsets: the last 30 seconds (5m: 270-299, 15m: 870-899);
  * gate ``pred_edge >= 0.03`` on ``model_p - ask - fee``;
  * fee ``POLY_FEE_THETA * ask * (1 - ask)``.

Live TWAP sleeves pass their own band, gate and entry offsets from the strategy
config.

Selection order is band -> reachable -> dedup (first second per market) ->
gate, and the order matters: deduplicating before gating yields about half as
many bets as gating first. Do not reorder.
"""

from __future__ import annotations

from typing import Any, Mapping


from cryptobot import config

# ---------------------------------------------------------------------------
# Default spec
# ---------------------------------------------------------------------------
# Mirrors pool_study.BET_SPEC_DEFAULT with both intervals and ask_lo=0.65.
V2_SPEC: dict[str, Any] = {
    "intervals": ("5m", "15m"),
    "coins": ("btc", "eth", "sol", "xrp"),
    "ask_lo": 0.65,
    "ask_hi": 0.85,
    "off_lo": 0,          # seconds into the last-30s window
    "off_hi": 29,
    "reachable_only": True,
    "gate": 0.03,         # pred_edge >= gate
    "orb_min": 0.0,       # |range_z| >= orb_min (0 = no ORB gate)
    "z_min": 0.0,         # |partial_ret_z_side| >= z_min (0 = off)
    "fee_theta": config.POLY_FEE_THETA,  # 0.07
}

# Window start seconds per interval (offset_sec origin for the last-30s window).
_WINDOW_START = {"5m": 270, "15m": 870}


# ---------------------------------------------------------------------------
# Primitive predicates (fee accepts arrays, like pool_study.fee)
# ---------------------------------------------------------------------------
def fee(ask):
    """Polymarket fee: ``theta * ask * (1 - ask)``.

    Accepts a scalar or numpy array, like ``pool_study.fee``.
    """
    return config.POLY_FEE_THETA * ask * (1.0 - ask)


def in_offset_window(interval: str, offset_sec: int) -> bool:
    """True when ``offset_sec`` lands in the interval's last-30s per-second window.

    5m -> seconds 270-299, 15m -> seconds 870-899.
    """
    start = _WINDOW_START.get(interval)
    if start is None:
        return False
    off = offset_sec - start
    return (off >= V2_SPEC["off_lo"]) and (off <= V2_SPEC["off_hi"])


# ---------------------------------------------------------------------------
# Live per-second state machine
# ---------------------------------------------------------------------------
class WindowDecider:
    """Per-second, one-bet-per-market decision state for a 1 Hz loop.

    Two policies, chosen by ``consume_on_fill`` (live:
    ``config.V2_REJECT_RETRY``):

    * **first-commit** (``consume_on_fill=False``, the default, used by the
    parity tests). A live loop cannot look ahead across the window, so the rule
    is: at the first in-band, in-window second, gate that second and consume
    the window, pass or fail (one attempt per market).

    * **consume-on-fill** (``consume_on_fill=True``). A rejected FAK does not
    consume the window; only a fill (via :meth:`note_fill`) or the window
    ending does. Every qualifying second is a new attempt evaluated on its own
    book, score, band and gate. Once either side fills, the market is done for
    both sides. ``next_attempt`` numbers the attempts (1, 2, 3, ...) for
    ``subminute_fills.attempt``.

    Side key = ``(coin, interval, window_start, side)``; market key = ``(coin,
    interval, window_start)``. ``on_second`` returns:

      * ``"PLACE"``: a qualifying second whose gate passed; place a bet.
      * ``"WINDOW_DONE"``: first-commit only; the first qualifying second
        failed the gate, so the window is consumed with no bet.
      * ``"SKIP"``: the window is already consumed, this second is outside the
        band or window, this second already fired, or (consume-on-fill) the gate
        failed this second and the next second will be evaluated.
    """

    def __init__(
        self,
        gate: float | None = None,
        *,
        consume_on_fill: bool = False,
        ask_lo: float | None = None,
        ask_hi: float | None = None,
        checkpoint_offsets: tuple[int, ...] | None = None,
    ) -> None:
        self.gate = V2_SPEC["gate"] if gate is None else gate
        # Per-decider ask band (open low, closed high), defaulting to
        # V2_SPEC's. The band belongs to the decider, not the cell, so each
        # sleeve's decider applies its own band.
        self.ask_lo = V2_SPEC["ask_lo"] if ask_lo is None else float(ask_lo)
        self.ask_hi = V2_SPEC["ask_hi"] if ask_hi is None else float(ask_hi)
        # Sleeves pass their own entry offsets; ``None`` means V2_SPEC's
        # last-30s window.
        self.checkpoint_offsets = (
            None
            if checkpoint_offsets is None
            else frozenset(int(offset) for offset in checkpoint_offsets)
        )
        self.consume_on_fill = bool(consume_on_fill)
        # first-commit state: side keys whose window is spent.
        self._committed: set[tuple] = set()
        # consume-on-fill state.
        self._filled: set[tuple] = set()              # market keys spent by a fill
        self._attempts: dict[tuple, int] = {}         # side key -> placement count
        self._attempted_offsets: dict[tuple, set] = {}  # side key -> offsets fired
        # FAK pad anchor (config.V2_FAK_PAD): side key -> walked_limit of the
        # first placed attempt in that window. See :meth:`anchor_limit`.
        self._first_limit: dict[tuple, float] = {}

    def _in_band(self, ask: float) -> bool:
        """This decider's band test: ``ask_lo < ask <= ask_hi`` (open low,
        closed high).
        """
        return (ask > self.ask_lo) and (ask <= self.ask_hi)

    def _in_offset_window(self, cell: Mapping[str, Any]) -> bool:
        if self.checkpoint_offsets is None:
            return in_offset_window(cell["interval"], cell["offset_sec"])
        return int(cell["offset_sec"]) in self.checkpoint_offsets

    @staticmethod
    def _key(cell: Mapping[str, Any]) -> tuple:
        return (cell["coin"], cell["interval"], cell["window_start"], cell["side"])

    @staticmethod
    def _market_key(cell: Mapping[str, Any]) -> tuple:
        return (cell["coin"], cell["interval"], cell["window_start"])

    def on_second(self, cell: Mapping[str, Any]) -> str:
        """Advance the state machine one second for one scored cell.

        ``cell`` must have ``coin, interval, window_start, side, ask,
        offset_sec, pred_edge``.
        """
        if self.consume_on_fill:
            return self._on_second_consume_on_fill(cell)
        return self._on_second_first_commit(cell)

    def _on_second_first_commit(self, cell: Mapping[str, Any]) -> str:
        key = self._key(cell)
        if key in self._committed:
            return "SKIP"
        if not (self._in_band(cell["ask"]) and self._in_offset_window(cell)):
            # Not a qualifying second yet; window stays open (not consumed).
            return "SKIP"
        # First in-band, in-window second: commit and gate exactly this second.
        self._committed.add(key)
        if cell["pred_edge"] >= self.gate:
            return "PLACE"
        return "WINDOW_DONE"

    def _on_second_consume_on_fill(self, cell: Mapping[str, Any]) -> str:
        # A fill on EITHER side spends the whole market window.
        if self._market_key(cell) in self._filled:
            return "SKIP"
        if not (self._in_band(cell["ask"]) and self._in_offset_window(cell)):
            # Not a qualifying second yet; window stays open (not consumed).
            return "SKIP"
        skey = self._key(cell)
        seen = self._attempted_offsets.setdefault(skey, set())
        offset = cell["offset_sec"]
        if offset in seen:
            # The same second never fires twice (defensive; one tick is one
            # second).
            return "SKIP"
        seen.add(offset)
        if cell["pred_edge"] < self.gate:
            # A failed gate does not consume the window; the next second (new
            # book, score and ask) is evaluated on its own.
            return "SKIP"
        return "PLACE"

    def next_attempt(self, cell: Mapping[str, Any]) -> int:
        """Increment and return the 1-based attempt number for this (market,
        side) window.

        Called once per actual order (where ``subminute_fills`` is written), so
        skips from later gates never inflate it. In first-commit mode a window
        places at most once, so this returns 1.
        """
        skey = self._key(cell)
        n = self._attempts.get(skey, 0) + 1
        self._attempts[skey] = n
        return n

    def anchor_limit(self, cell: Mapping[str, Any], walked_limit: float) -> float:
        """Get or set the first placed walked limit for this side window.

        The first call for a ``(coin, interval, window_start, side)`` key
        stores ``walked_limit`` and returns it; later calls in the same window
        return the stored value. The FAK pad (``config.V2_FAK_PAD``) uses it so
        a retry's cap can never exceed ``first_limit + PAD``.

        Memory only: after a restart mid-window the next placement sets a new
        anchor, which is fine for windows under a minute (no database read on
        the hot path). Pruned by ``window_start`` with the rest of the decider
        state (pipeline ``_v2_prune_window_decider``).
        """
        skey = self._key(cell)
        anchor = self._first_limit.get(skey)
        if anchor is None:
            anchor = float(walked_limit)
            self._first_limit[skey] = anchor
        return anchor

    def note_fill(self, cell: Mapping[str, Any]) -> None:
        """Record an accepted fill, which consumes the market for both sides.

        No-op in first-commit mode (the window was consumed at commit); in
        consume-on-fill mode this, or the window ending, is the only thing that
        consumes it.
        """
        if self.consume_on_fill:
            self._filled.add(self._market_key(cell))

    def reset(self) -> None:
        """Clear all window state (e.g. on a fresh session/boot)."""
        self._committed.clear()
        self._filled.clear()
        self._attempts.clear()
        self._attempted_offsets.clear()
        self._first_limit.clear()
