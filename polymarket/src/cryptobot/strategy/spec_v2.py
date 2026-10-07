"""Shared v2 "step1last" decision core — the single source of truth for the
promoted sub-minute spec, kept bet-for-bet with the frozen research evaluator
``polymarket/backtest/lib/pool_study.py`` (``build_bets`` / ``BET_SPEC_DEFAULT``).

Two consumers share this module:
  * the pipeline's live per-second decision path (via :class:`WindowDecider`),
  * the parity tests (via :func:`build_bets_src`, a pure frame-level replica of
    research ``build_bets``).

Frozen spec (do NOT reorder or retune without re-running the W6 parity gate):
  * intervals = both 5m and 15m,
  * ask band = ``0.65 < ask <= 0.85`` (open low, closed high — matches
    ``pool_study.build_bets`` line 266, and the flipped ``score._ask_in_bucket``),
  * offset window = last 30s per-second (5m: 270-299, 15m: 870-899),
  * gate = ``pred_edge >= 0.03`` on ``model_p - ask - fee``,
  * fee = ``POLY_FEE_THETA * ask * (1 - ask)`` with theta = 0.07,
  * selection order = band -> reachable -> dedup (first entry_ts per market)
    -> gate.  This order is load-bearing: dedup-before-gate yields ~half the
    bet count of gate-first (measured n=2503 vs 5223 on fold E). Do not reorder.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from cryptobot import config

# ---------------------------------------------------------------------------
# Frozen spec
# ---------------------------------------------------------------------------
# Mirrors pool_study.BET_SPEC_DEFAULT with the two live overrides:
#   intervals=("5m","15m")  and  ask_lo=0.65 (WFO-confirmed floor amendment).
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
# Primitive predicates (scalar-safe; fee is array-safe like pool_study.fee)
# ---------------------------------------------------------------------------
def fee(ask):
    """Polymarket fee haircut: ``theta * ask * (1 - ask)`` (theta=0.07).

    Accepts a scalar or numpy array, matching ``pool_study.fee``.
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
# Frame-level bet construction — pure replica of pool_study.build_bets
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Live per-second state machine
# ---------------------------------------------------------------------------
class WindowDecider:
    """Live-legal replica of the dedup-before-gate rule for a 1 Hz decision loop.

    Two consume policies, selected by ``consume_on_fill`` (the live master switch
    is ``config.V2_REJECT_RETRY``):

    * **first-commit** (``consume_on_fill=False``, the DEFAULT — pins the frozen
      fold-E parity gate at n=2366).  A real-time loop cannot peek "reachable"
      across the whole window before placing, so the faithful no-retry rule is:
      **commit the window at the first in-band, in-window second, gate THAT
      second, and consume the window** (one attempt per market/resolution).  A
      gate-FAILED first second still consumes the window.

    * **consume-on-fill** (``consume_on_fill=True``, operator order 2026-07-09).
      A FAK rejection must NOT consume the window; only a FILL (recorded via
      :meth:`note_fill`) or window end does.  Every qualifying second is a fresh,
      independent placement attempt evaluated by the normal per-second path
      (fresh book / fresh score / band / gate at THAT second's ask).  Once ANY
      side fills, the window is consumed for BOTH sides (one-bet-per-market,
      matching the ledger market-level dedup).  ``next_attempt`` numbers the
      attempts (1, 2, 3…) for the ``subminute_fills.attempt`` telemetry column.

    Side key = ``(coin, interval, window_start, side)``; market key =
    ``(coin, interval, window_start)``.  ``on_second`` returns:
      * ``"PLACE"``       — a qualifying second, gate passed -> place a bet.
      * ``"WINDOW_DONE"`` — first-commit only: first qualifying second, gate
                            FAILED -> window consumed, no bet, no further attempts.
      * ``"SKIP"``        — window already consumed, this second is not in
                            band/window yet, this exact second already fired, or
                            (consume-on-fill) the gate failed this second (window
                            stays open — the next second is re-evaluated).
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
        # Per-sleeve ask band (open low, closed high). Defaults reproduce the
        # frozen MID band (0.65, 0.85] byte-identically, so ``WindowDecider()``
        # is unchanged; a caller that owns a different band passes its own
        # ask_lo/ask_hi. The band is per-DECIDER (not per-cell) so the state
        # machine stays a faithful replica of ``in_ask_band`` for whichever
        # sleeve owns it.
        self.ask_lo = V2_SPEC["ask_lo"] if ask_lo is None else float(ask_lo)
        self.ask_hi = V2_SPEC["ask_hi"] if ask_hi is None else float(ask_hi)
        # New sleeves own their exact decision grid.  ``None`` preserves the
        # retired checkpoint strategy's frozen last-30s behavior for historical
        # callers/tests, while TWAP sleeves pass their manifest-backed offsets.
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
        # Anchored FAK pad (config.V2_FAK_PAD): side key -> walked_limit of the
        # FIRST placed attempt in this (coin, interval, window_start, side)
        # window. See :meth:`anchor_limit`.
        self._first_limit: dict[tuple, float] = {}

    def _in_band(self, ask: float) -> bool:
        """This decider's band test: ``ask_lo < ask <= ask_hi`` (open low,
        closed high). With the default bounds this is byte-identical to the
        module-level :func:`in_ask_band`."""
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

        ``cell`` must expose: ``coin, interval, window_start, side, ask,
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
            # The SAME second never double-fires (defensive; 1 tick == 1 second).
            return "SKIP"
        seen.add(offset)
        if cell["pred_edge"] < self.gate:
            # Gate FAIL does NOT consume — this second is dead, the next second
            # (fresh book / fresh score / fresh ask) is independently eligible.
            return "SKIP"
        return "PLACE"

    def next_attempt(self, cell: Mapping[str, Any]) -> int:
        """Increment and return the 1-based placement-attempt number for this
        (market, side) window.

        Called once per ACTUAL order placement (at the ``subminute_fills``
        record site) so the ``attempt`` column counts real placements (1, 2, 3…)
        and skips induced by downstream gates never inflate it.  In first-commit
        mode a window places at most once, so this returns 1.
        """
        skey = self._key(cell)
        n = self._attempts.get(skey, 0) + 1
        self._attempts[skey] = n
        return n

    def anchor_limit(self, cell: Mapping[str, Any], walked_limit: float) -> float:
        """Get-or-set the FIRST-placed walked limit for this side window.

        Records ``walked_limit`` as the anchor on the FIRST call for this
        ``(coin, interval, window_start, side)`` key and returns it; every later
        call in the same window returns the STORED first limit (ignoring the
        passed value). Feeds the anchored FAK pad (``config.V2_FAK_PAD``, applied
        at the pipeline's sub-minute BetSignal build) so a retry's padded cap can
        never exceed ``first_limit + PAD``.

        In-memory only — a process restart mid-window re-seeds the anchor from the
        next placement, which is acceptable (windows are <=60s; do NOT add a DB
        read on the hot path). Multifill nuance (operator 2026-07-10): a multifill
        extra entry shares the side-window anchor of the window's first attempt —
        acceptable (multifill is inert below the $5k equity floor today). Keyed
        exactly like ``_attempts`` and pruned by ``window_start`` with the rest of
        the decider state (pipeline ``_v2_prune_window_decider``).
        """
        skey = self._key(cell)
        anchor = self._first_limit.get(skey)
        if anchor is None:
            anchor = float(walked_limit)
            self._first_limit[skey] = anchor
        return anchor

    def note_fill(self, cell: Mapping[str, Any]) -> None:
        """Record an accepted fill — consumes the window for BOTH sides.

        No-op in first-commit mode (the window is already consumed at first
        commit); in consume-on-fill mode this is the ONLY thing (besides window
        end) that spends the window."""
        if self.consume_on_fill:
            self._filled.add(self._market_key(cell))

    def reset(self) -> None:
        """Clear all window state (e.g. on a fresh session/boot)."""
        self._committed.clear()
        self._filled.clear()
        self._attempts.clear()
        self._attempted_offsets.clear()
        self._first_limit.clear()
