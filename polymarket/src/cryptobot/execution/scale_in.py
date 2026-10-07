"""T3 scale-in taker top-ups (research WFO pick 2026-07-15).

After a sub-minute entry fills, scale into the SAME side with small
frozen-at-entry taker (FAK) top-ups while the market "has not dipped". This
module owns the per-second T3 state machine; it is driven from
``execution/held_position_pass.py::_evaluate_position`` (T3 rides inside the
per-second held-position loop, which already fetches the held-side book, so T3
adds no new network fetch).

Reference semantics = ``polymarket/backtest/scripts/tick_topup_foldBE.py``
``::_simulate_topups`` (the pre-registered config **T3**, SCALEIN_GRID_DIGEST.md
— digest removed in a6df8ed; recover via ``git show
a6df8ed^:polymarket/backtest/studies/tick_latency/SCALEIN_GRID_DIGEST.md``):

  Each second, while ALL hold, place ONE FAK top-up on side S of market M:
    (i)   no-dip 0.00 — side-S best ask has NEVER been observed below the entry
          ask ``ask0`` (= the entry ledger row's ``p_market``) since entry, on
          any non-crossed book. The FIRST violation permanently disarms M
          (matches the backtest ``break``).
    (ii)  the WALKED ask for the top-up stake <= ``cap`` (0.83; the binding rule).
    (iii) spread (side best ask - side best bid) <= ``spread_max`` (0.02).
    (iv)  fewer than ``max_n`` (3) top-up FILLS so far for M.
    (v)   cumulative top-up FILL cash + this stake <= ``exposure_cap`` (1.5x) of
          the entry's realized filled cash.
    (vi)  all existing entry risk checks (40% DD halt, available cash, $1 MIN_BET
          floor) — the top-up cash flows through the same accounting because each
          top-up is written as a normal PENDING LIVE ledger row.

  Order: FAK buy, stake = ``frac`` (0.5x) the entry's REQUESTED stake (cents-
  floored, $1 floor), limit = ``walked_limit + fak_pad`` (0.01) capped at 0.99.
  Each top-up pads from its OWN walk — NOT the entry's anchored FAK pad (which
  would auto-reject a deliberate higher-ask top-up).

Every top-up row is tagged ``is_topup=1`` + ``scalein_topup=1`` so the F3
resolution sweep + capital.py settle/count it for free and live grading can
separate T3 fills from entries.

Same-second "blind" fresh gate (``config.TOPUP_FRESH_GATE_MODE``, DEFAULT off)
-----------------------------------------------------------------------------
A top-up placed in the SAME SECOND as its parent entry is blind: this scan runs
inside the same 1s held-position-pass tick as the entry, against a held-side book that can
PREDATE the parent's POST, so a book crashing onto the parent in-flight (the
250ms uncancellable taker delay) is not yet observable and the no-dip check
cannot see it — the top-up buys the same crater. Live ledger since 2026-07-14:
same-second top-ups n=50 -> -$5.81; top-ups >=1s after the parent n=117 -> +$4.28.

When armed, the gate requires ``now_ts`` to be STRICTLY after the parent's fill
second (rejecting when ``now_ts <= parent_fill_epoch``), parsed from
``live_fill_ts`` then ``bet_ts`` then ``created_at``. A book RECV timestamp would
be better evidence but is not available: ``BookSnapshot.fetched_at_ts`` is the
LOCAL fetch time on the REST path, not venue liveness (polymarket_clob.py audit
#12). The time rule also restores live<->backtest parity — the reference
``tick_topup_foldBE._simulate_topups`` starts at ``t = ceil(entry_fill_ts)
+ 1``, so it never simulates the same-second top-up live currently allows (the
first attempt has ``last_attempt_ts`` None, so the cadence throttle cannot catch
it). A live skip is a DELAY, not a kill: it consumes no cadence throttle and
never disarms, so the next second's scan proceeds normally. ⚠️ FAIL-OPEN — when
no timestamp parses at all the top-up is ALLOWED (T3 is +EV overall and must
never be silently killed by a data gap; this gate targets one narrow cohort and
is not a safety system).

DEFAULT OFF (``config.SCALEIN_T3_ENABLED``): with the flag off ``scan_topup``
returns immediately and the whole T3 diff is a runtime no-op.

Best-effort throughout: every path is fail-closed (skip the top-up, never crash
the held-position pass) — this is a real-money system.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution import order_attempts as _oa
from cryptobot.execution.ledger import BetSignal, record_bet

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Frozen config snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TopupConfig:
    """Frozen T3 knobs read from ``config`` at scan time (so tests that
    monkeypatch ``config`` are honoured). Mirrors the backtest ``TopupConfig``."""

    enabled: bool
    max_n: int
    frac: float
    no_dip: float
    cap: float
    spread_max: float
    exposure_cap: float
    cadence_s: int
    fak_pad: float
    end_buffer_s: int

    @classmethod
    def from_config(cls) -> "TopupConfig":
        return cls(
            enabled=bool(config.SCALEIN_T3_ENABLED),
            max_n=int(config.SCALEIN_T3_MAX_N),
            frac=float(config.SCALEIN_T3_FRAC),
            no_dip=float(config.SCALEIN_T3_NO_DIP),
            cap=float(config.SCALEIN_T3_CAP),
            spread_max=float(config.SCALEIN_T3_SPREAD_MAX),
            exposure_cap=float(config.SCALEIN_T3_EXPOSURE_CAP),
            cadence_s=int(config.SCALEIN_T3_CADENCE_S),
            fak_pad=float(config.SCALEIN_T3_FAK_PAD),
            end_buffer_s=int(config.SCALEIN_T3_END_BUFFER_S),
        )


# ---------------------------------------------------------------------------
# Cents floor + walk
# ---------------------------------------------------------------------------


def _floor_cents(value: float) -> float:
    """Floor a USD amount DOWN to whole cents. Byte-identical to the backtest
    ``tick_replay_base._floor_cents`` the reference driver uses."""
    return math.floor(max(0.0, value) * 100.0 + 1e-9) / 100.0


def _walk_limit(book: Optional[BookSnapshot], amount_usd: float) -> Optional[float]:
    """Highest ask price consumed to fill ``amount_usd`` at ``book``.

    Mirrors the backtest ``tick_topup_foldBE._walk_limit`` — a PLAIN cap
    walk over the (price-ascending) ask levels, NOT the edge-preserving walk,
    because T3 gate (ii) is a raw ``<= cap`` cap, not an edge floor. Returns the
    price of the deepest level touched (the executable limit), or None when the
    book is empty / the amount is non-finite. Tolerates a book thinner than the
    stake (returns the top price walked to), exactly like the reference.
    """
    if book is None or amount_usd <= 0 or not math.isfinite(amount_usd):
        return None
    remaining = amount_usd
    limit = 0.0
    for level in book.asks:
        try:
            price = float(level["price"])
            size = float(level["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if price <= 0 or size <= 0:
            continue
        if remaining <= 1e-12:
            break
        take_shares = min(size, remaining / price)
        limit = price
        remaining -= take_shares * price
    return limit if limit > 0 else None


# ---------------------------------------------------------------------------
# Durable no-dip state
# ---------------------------------------------------------------------------


def _load_state(conn: sqlite3.Connection, ledger_id: int) -> Optional[sqlite3.Row]:
    try:
        return conn.execute(
            "SELECT ledger_id, ask0, running_min_ask, disarmed, last_attempt_ts "
            "FROM scalein_state WHERE ledger_id = ?",
            (int(ledger_id),),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _write_state(
    conn: sqlite3.Connection,
    ledger_id: int,
    *,
    ask0: Optional[float],
    running_min: Optional[float],
    disarmed: bool,
    last_attempt_ts: Optional[int],
) -> None:
    """Upsert the durable no-dip state (autocommit connection). ``ask0`` is set
    once on first insert and never rewritten."""
    now = utc_now_sql()
    conn.execute(
        "INSERT OR IGNORE INTO scalein_state "
        "(ledger_id, ask0, running_min_ask, disarmed, last_attempt_ts, updated_at) "
        "VALUES (?, ?, ?, 0, ?, ?)",
        (int(ledger_id), ask0, running_min, last_attempt_ts, now),
    )
    conn.execute(
        "UPDATE scalein_state SET running_min_ask = ?, disarmed = ?, "
        "last_attempt_ts = ?, updated_at = ? WHERE ledger_id = ?",
        (running_min, int(bool(disarmed)), last_attempt_ts, now, int(ledger_id)),
    )


def disarm(conn: sqlite3.Connection, ledger_id: int) -> None:
    """Permanently disarm T3 for the market whose ENTRY row is ``ledger_id``.

    Idempotent; used on a no-dip violation and on any deterioration trigger.
    Best-effort (never raises into the pass)."""
    try:
        now = utc_now_sql()
        conn.execute(
            "INSERT OR IGNORE INTO scalein_state "
            "(ledger_id, disarmed, updated_at) VALUES (?, 1, ?)",
            (int(ledger_id), now),
        )
        conn.execute(
            "UPDATE scalein_state SET disarmed = 1, updated_at = ? WHERE ledger_id = ?",
            (now, int(ledger_id)),
        )
    except Exception:  # noqa: BLE001 — disarm must never fail the pass
        logger.debug("scale-in disarm failed for ledger_id=%s", ledger_id, exc_info=True)


# ---------------------------------------------------------------------------
# Per-second scan
# ---------------------------------------------------------------------------


def _reject(ledger_id: int, reason: str) -> float:
    logger.debug("scale-in top-up skipped ledger_id=%s reason=%s", ledger_id, reason)
    return 0.0


# ---------------------------------------------------------------------------
# Same-second "blind" top-up fresh gate (flag-gated, DEFAULT off)
# ---------------------------------------------------------------------------
#
# A top-up placed in the SAME SECOND as its parent ENTRY is blind: the held-position
# pass drives scan_topup inside the same 1s loop tick as the entry, against a book
# that can PREDATE the parent's POST, so a book crashing onto the parent
# in-flight (250ms uncancellable taker delay) is invisible to the no-dip check
# and the top-up buys the same crater. Counter bumped in SHADOW only — it is the
# would-skip count the operator grades the blind cohort's realized ledger
# outcomes against before flipping to live.
_FRESH_GATE_SHADOW_COUNTS = {"would_skip": 0}


def _parse_ts_epoch(value: Optional[str]) -> Optional[float]:
    """Parse a ledger timestamp column to a UTC epoch second, else None.

    Accepts BOTH shapes the ledger actually stores: the SQL text format written
    by ``db.connection.utc_now_sql`` (``YYYY-MM-DD HH:MM:SS``, used for bet_ts /
    created_at) and an ISO-8601 CLOB fill payload (``T`` separator, trailing
    ``Z``, optional fractional seconds / offset — live_fill_ts). ``fromisoformat``
    covers every one of those on the supported interpreter floor (pyproject
    requires-python >=3.11, where it gained full ISO-8601 incl. the ``Z``
    suffix), so no secondary format pass is needed. A NAIVE timestamp is
    interpreted as UTC, which is what every writer on this path produces.
    Returns None for NULL / blank / unparseable input so the caller can FAIL
    OPEN (see ``_parent_fill_epoch``).
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _parent_fill_epoch(entry: sqlite3.Row) -> Optional[float]:
    """Best available epoch for the parent ENTRY's POST/fill moment, else None.

    Precedence: ``live_fill_ts`` (the CLOB fill payload — the true fill moment)
    then ``bet_ts`` (placement, written by record_bet in the same second) then
    ``created_at`` (row insert). Returns None ONLY when none of the three parse,
    which the fresh gate treats as FAIL-OPEN (allow the top-up): T3 overall is
    +EV and must never be silently killed by a data gap, and this gate targets
    one narrow cohort rather than acting as a safety system.
    """
    for column in ("live_fill_ts", "bet_ts", "created_at"):
        try:
            raw = entry[column]
        except (IndexError, KeyError):  # column absent from this row shape
            continue
        epoch = _parse_ts_epoch(raw)
        if epoch is not None:
            return epoch
    return None


def _fold_and_dip(
    running_min: Optional[float],
    *,
    bid: Optional[float],
    held_ask: Optional[float],
    ask0: float,
    no_dip: float,
) -> tuple[Optional[float], bool, bool]:
    """Fold the held-side best ask into the no-dip running min (skipping crossed
    books) and report whether it has EVER dipped below ``ask0 - no_dip``.

    Pure — shared by ``observe_no_dip`` (the tick-exact early observation) and
    ``_scan_topup_inner`` (placement) so both fold identically. Returns
    ``(running_min, dipped, crossed)``.
    """
    crossed = (
        bid is not None and held_ask is not None
        and float(bid) >= float(held_ask) - 1e-9
    )
    if held_ask is not None and math.isfinite(float(held_ask)) and not crossed:
        ha = float(held_ask)
        running_min = ha if running_min is None else min(running_min, ha)
    dipped = running_min is not None and running_min < ask0 - no_dip - 1e-9
    return running_min, dipped, crossed


def observe_no_dip(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    bid: Optional[float],
    held_ask: Optional[float],
) -> None:
    """Tick-exact no-dip observation for one open ENTRY position.

    Fold the held-side best ask into the durable running min and PERMANENTLY
    disarm T3 on the first dip below ``ask0 - no_dip``. Called every held-position pass
    right after the held book is read — BEFORE the full re-score's early returns
    (other-side book / model_p) — so a transient dip on a tick whose re-score
    bails is still recorded. The backtest reference folds its cummin over EVERY
    post-entry non-crossed tick; ``scan_topup`` alone runs only after the
    re-score succeeds and would miss dips on ticks that bail before it, keeping
    T3 armed where the reference permanently disarms. Placement still needs the
    full risk context and stays in ``scan_topup`` (which re-folds the same ask
    idempotently).

    Flag-gated + fail-closed: never raises into the held-position pass, and never touches
    the cadence throttle (``last_attempt_ts`` is preserved — this is an
    observation, not an attempt).
    """
    cfg = TopupConfig.from_config()
    if not cfg.enabled or held_ask is None:
        return
    ledger_id = int(row["id"])
    try:
        state = _load_state(conn, ledger_id)
        if state is not None and int(state["disarmed"] or 0) == 1:
            return  # already permanently disarmed — nothing to observe
        arow = conn.execute(
            "SELECT p_market FROM ledger WHERE id = ?", (ledger_id,)
        ).fetchone()
        if arow is None or arow["p_market"] is None:
            return
        ask0 = float(arow["p_market"])
        if not math.isfinite(ask0):
            return
        running_min = (
            None if state is None or state["running_min_ask"] is None
            else float(state["running_min_ask"])
        )
        last_attempt_ts = (
            None if state is None or state["last_attempt_ts"] is None
            else int(state["last_attempt_ts"])
        )
        running_min, dipped, _crossed = _fold_and_dip(
            running_min, bid=bid, held_ask=held_ask, ask0=ask0, no_dip=cfg.no_dip,
        )
        _write_state(
            conn, ledger_id, ask0=ask0, running_min=running_min,
            disarmed=dipped, last_attempt_ts=last_attempt_ts,
        )
    except Exception:  # noqa: BLE001 — observation must never crash the pass
        logger.debug(
            "scale-in observe_no_dip failed for ledger_id=%s", ledger_id,
            exc_info=True,
        )


def scan_topup(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    held_book: Optional[BookSnapshot],
    *,
    bid: Optional[float],
    held_ask: Optional[float],
    now_ts: int,
    deps,
    wallet_snaps: Optional[dict],
    budget: list,
    triggered: bool,
) -> float:
    """Run the T3 state machine for one open ENTRY position this second.

    ``row`` is a held-position candidate row (carries id/coin/sleeve/side/window_*). The
    full entry row is re-read here for the fields the top-up BetSignal needs.
    ``held_ask``/``bid`` are the side-S best ask/bid the pass already read;
    ``held_book`` is walked for gate (ii). ``wallet_snaps`` is the per-pass
    wallet snapshot dict (or None if unavailable — then the DD gate fails
    closed). ``budget`` is a one-element mutable ``[remaining_live_balance]``
    (None disables the cash gate, mirroring the entry path never blocking on a
    balance fetch); it is decremented by a filled top-up's cash. ``triggered``
    is True when the model-deterioration disarm predicate fires THIS second
    (disarm + no top-up — matches the backtest dropping legs at/after the
    trigger second).

    Returns the realized top-up FILL cash (0.0 when nothing filled).
    """
    cfg = TopupConfig.from_config()
    if not cfg.enabled:
        return 0.0

    ledger_id = int(row["id"])
    try:
        return _scan_topup_inner(
            conn, ledger_id, held_book,
            bid=bid, held_ask=held_ask, now_ts=int(now_ts), deps=deps,
            wallet_snaps=wallet_snaps, budget=budget, triggered=bool(triggered),
            cfg=cfg,
        )
    except Exception:  # noqa: BLE001 — a T3 fault must never crash the pass
        logger.warning(
            "scale-in scan_topup failed for ledger_id=%s", ledger_id, exc_info=True
        )
        return 0.0


def _scan_topup_inner(
    conn: sqlite3.Connection,
    ledger_id: int,
    held_book: Optional[BookSnapshot],
    *,
    bid: Optional[float],
    held_ask: Optional[float],
    now_ts: int,
    deps,
    wallet_snaps: Optional[dict],
    budget: list,
    triggered: bool,
    cfg: TopupConfig,
) -> float:
    state = _load_state(conn, ledger_id)
    if state is not None and int(state["disarmed"] or 0) == 1:
        return 0.0

    entry = conn.execute(
        "SELECT id, coin, sleeve, entry_minute, side, window_start_ts, "
        "window_end_ts, interval_min, poly_market_id, poly_token_id, "
        "fee_rate_bps, model_p, p_market, bet_size, realized_edge, "
        "entry_top_price, yes_pre1_anchor, strike_price, window_open_spot, "
        "wallet_id, wallet_group, config_sha256, live_filled_size, "
        "live_filled_price, live_fill_ts, bet_ts, created_at "
        "FROM ledger WHERE id = ?",
        (ledger_id,),
    ).fetchone()
    if entry is None:
        return 0.0

    # Interval entry-disable. This pass is driven off open LEDGER rows, not the
    # tick loop's sleeve iteration, so it is the one placement path the loop's
    # gate cannot cover — and a top-up is NEW money into the position, which is
    # exactly what a disabled interval must not take. Inert while
    # SCALEIN_T3_ENABLED is off, but that flag is armed env-only, so the gate
    # has to be here rather than assumed.
    from cryptobot.execution.pipeline import _interval_entry_disabled

    if _interval_entry_disabled(entry["interval_min"]):
        return _reject(ledger_id, "interval_disabled")

    ask0 = entry["p_market"]
    if ask0 is None or not math.isfinite(float(ask0)):
        return 0.0
    ask0 = float(ask0)

    running_min = (
        None if state is None or state["running_min_ask"] is None
        else float(state["running_min_ask"])
    )
    last_attempt_ts = (
        None if state is None or state["last_attempt_ts"] is None
        else int(state["last_attempt_ts"])
    )

    # --- (i) no-dip observation: always runs, skips crossed books ----------
    # Shares _fold_and_dip with observe_no_dip (called earlier this pass), which
    # already folded this ask idempotently; re-folding here keeps scan_topup
    # self-contained for the direct-call unit tests.
    running_min, dip, crossed = _fold_and_dip(
        running_min, bid=bid, held_ask=held_ask, ask0=ask0, no_dip=cfg.no_dip,
    )
    new_disarmed = bool(dip or triggered)
    _write_state(
        conn, ledger_id, ask0=ask0, running_min=running_min,
        disarmed=new_disarmed, last_attempt_ts=last_attempt_ts,
    )
    if dip:
        return _reject(ledger_id, "no_dip")
    if triggered:
        # Model deterioration this second — disarm, place NO top-up (the
        # backtest drops legs at/after the trigger second).
        return 0.0

    # --- same-second "blind" fresh gate (flag-gated; DEFAULT off) ----------
    # The book this top-up would buy must be demonstrably FRESHER than the
    # parent's POST/fill moment. BookSnapshot.fetched_at_ts cannot supply that
    # evidence — on the REST path it is stamped with the caller's own now_ts
    # (local fetch time, NOT venue liveness; data/polymarket_clob.py audit #12) —
    # so the rule is time-based: require now_ts STRICTLY after the parent's fill
    # second. This also matches the T3 reference, whose first top-up lands at
    # t = ceil(entry_fill_ts) + 1 (tick_topup_foldBE._simulate_topups).
    #
    # Mode check FIRST so a flag-off pass does ZERO extra work. A live skip is
    # NOT an attempt and NOT a disarm: it returns before the reservation, so
    # last_attempt_ts stays unwritten (no cadence consumed) and the state row
    # keeps disarmed=0 from the no-dip fold above — the NEXT second's scan
    # proceeds normally. This delays a top-up; it never kills one.
    if config.TOPUP_FRESH_GATE_MODE != "off":
        parent_fill_epoch = _parent_fill_epoch(entry)
        if parent_fill_epoch is None:
            # FAIL-OPEN: no parseable parent timestamp -> allow (T3 is +EV
            # overall; a data gap must never silently disable it).
            logger.debug(
                "scale-in fresh-gate: no parseable parent fill ts for "
                "ledger_id=%s — allowing (fail-open)", ledger_id,
            )
        elif now_ts <= parent_fill_epoch:
            if config.TOPUP_FRESH_GATE_MODE == "live":
                return _reject(ledger_id, "fresh_gate")
            # SHADOW: count + log one grep-able line, then PROCEED unchanged.
            _FRESH_GATE_SHADOW_COUNTS["would_skip"] += 1
            logger.info(
                "topup fresh-gate shadow: would_skip ledger_id=%s coin=%s "
                "sleeve=%s now_ts=%s parent_fill_ts=%s parent_fill_epoch=%.0f "
                "(cum would_skip=%d) — top-up PROCEEDS (shadow)",
                ledger_id, entry["coin"], entry["sleeve"], now_ts,
                entry["live_fill_ts"] or entry["bet_ts"], parent_fill_epoch,
                _FRESH_GATE_SHADOW_COUNTS["would_skip"],
            )

    # --- (iii cadence throttle) + end-of-window buffer ---------------------
    if last_attempt_ts is not None and (now_ts - last_attempt_ts) < cfg.cadence_s:
        return _reject(ledger_id, "cadence")
    window_end_ts = int(entry["window_end_ts"])
    if now_ts >= window_end_ts - cfg.end_buffer_s:
        return _reject(ledger_id, "end_buffer")

    if held_ask is None or bid is None or crossed:
        return _reject(ledger_id, "no_book")

    # --- stake (frozen at 0.5x entry requested, cents-floored, $1 floor) ----
    entry_bet_size = entry["bet_size"]
    if entry_bet_size is None:
        return _reject(ledger_id, "no_entry_stake")
    stake = max(
        float(config.MIN_BET_USD), _floor_cents(cfg.frac * float(entry_bet_size))
    )

    # --- (ii) walked ask <= cap; (iii) spread <= spread_max ----------------
    walked_limit = _walk_limit(held_book, stake)
    if walked_limit is None:
        return _reject(ledger_id, "no_walk")
    if walked_limit > cfg.cap + 1e-9:
        return _reject(ledger_id, "cap")
    if (float(held_ask) - float(bid)) > cfg.spread_max + 1e-9:
        return _reject(ledger_id, "spread")

    # --- (ii-b) re-priced ADD-ON edge: ceiling + floor ----------------------
    # The ONE edge quantity this path prices and records (forensics fix
    # 2026-07-23): frozen entry model_p - walked_limit - parabola fee (the
    # entry gate's fee basis). Computed once, gated here, then stamped
    # verbatim as the top-up row's realized_edge below.
    _mp = float(entry["model_p"]) if entry["model_p"] is not None else float(ask0)
    addon_edge = (
        _mp
        - float(walked_limit)
        - config.POLY_FEE_THETA * float(walked_limit) * (1.0 - float(walked_limit))
    )
    # V2_EDGE_CAP ceiling (operator order 2026-07-23: EVERY entry and top-up,
    # all coins, BOTH venues, obeys the armed cap). Under the no-dip
    # invariant (walked >= ask0) the add-on edge is <= the entry's own gated
    # edge, so with capped entries this is a structural belt-and-braces — it
    # binds only for pre-cap parents, config drift, or a future no-dip
    # relaxation.
    if config.V2_EDGE_CAP > 0.0 and addon_edge > config.V2_EDGE_CAP:
        return _reject(ledger_id, "edge_above_ceiling")
    # TOPUP_EDGE_FLOOR (live id7 2026-07-23: this path chased 5c above its
    # 0.75 anchor and filled at 0.81 with realized_edge -0.0119 — the ceiling
    # alone never stops a top-up whose recomputed edge has gone NEGATIVE).
    # Strict <, so an edge exactly at the floor passes; the 0.0 default kills
    # exactly the negative-edge cohort and nothing else. Venue-independent.
    if addon_edge < config.TOPUP_EDGE_FLOOR:
        return _reject(ledger_id, "edge_below_floor")

    # --- (iv) fill count; (v) exposure cap ---------------------------------
    coin = str(entry["coin"])
    sleeve = str(entry["sleeve"])
    side = str(entry["side"])
    entry_minute = int(entry["entry_minute"])
    window_start_ts = int(entry["window_start_ts"])
    sibs = conn.execute(
        "SELECT live_filled_size, live_filled_price FROM ledger "
        "WHERE coin = ? AND sleeve = ? AND entry_minute = ? AND side = ? "
        "AND window_start_ts = ? AND scalein_topup = 1 AND outcome = 'PENDING' "
        "AND live_filled_size IS NOT NULL AND live_filled_size > 0",
        (coin, sleeve, entry_minute, side, window_start_ts),
    ).fetchall()
    if len(sibs) >= cfg.max_n:
        return _reject(ledger_id, "max_n")
    prior_topup_cash = math.fsum(
        float(r["live_filled_size"]) * float(r["live_filled_price"]) for r in sibs
    )

    entry_fill_size = entry["live_filled_size"]
    entry_fill_price = entry["live_filled_price"]
    if not entry_fill_size or not entry_fill_price:
        return _reject(ledger_id, "no_entry_fill")
    entry_fill_cash = float(entry_fill_size) * float(entry_fill_price)
    if prior_topup_cash + stake > cfg.exposure_cap * entry_fill_cash + 1e-9:
        return _reject(ledger_id, "exposure")

    # --- (vi) DD halt (fresh per-pass wallet snapshot) ---------------------
    wallet_id = entry["wallet_id"]
    if wallet_snaps is None:
        return _reject(ledger_id, "dd_snapshot_unavailable")
    snap = wallet_snaps.get(wallet_id) if wallet_id is not None else None
    if snap is None:
        return _reject(ledger_id, "dd_snapshot_missing")
    halt_limit = max(0.0, float(config.MAX_DRAWDOWN_HALT_FRAC))
    if halt_limit > 0.0 and float(snap.drawdown_pct_peak_frac) >= halt_limit:
        return _reject(ledger_id, "dd_halt")

    # --- (vi) available cash (never block on an UNAVAILABLE balance) --------
    remaining = budget[0] if budget else None
    if remaining is not None and float(remaining) + 1e-9 < stake:
        return _reject(ledger_id, "cash")

    # --- order: FAK buy, own walk + pad, tagged scalein_topup=1 ------------
    limit_price = min(walked_limit + cfg.fak_pad, 0.99)
    offset_sec = now_ts - window_start_ts
    signal = BetSignal(
        coin=coin,
        sleeve=sleeve,
        entry_minute=entry_minute,
        side=side,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        interval_min=int(entry["interval_min"]),
        poly_market_id=str(entry["poly_market_id"]),
        poly_token_id=str(entry["poly_token_id"]),
        fee_rate_bps=float(entry["fee_rate_bps"]),
        model_p=_mp,
        p_market=float(held_ask),
        fill_price=float(held_ask),
        walked_vwap=float(walked_limit),
        # Re-priced ADD-ON edge (forensics fix 2026-07-23): frozen entry
        # model_p minus the walked limit THIS add-on pays, minus the same
        # parabola fee basis the entry's realized_edge used — previously the
        # entry's edge was echoed verbatim, which made the ledger claim a
        # stale edge for a fill at a different price (and zeroed the
        # add_on_realized_edge_minus_initial_edge diagnostic). The identical
        # ``addon_edge`` value already passed the V2_EDGE_CAP ceiling and the
        # TOPUP_EDGE_FLOOR above — the row records exactly what was gated.
        realized_edge=addon_edge,
        partial_ret_z_1h=None,
        lut_p_bucket=None,
        lut_hit_rate=None,
        lut_n_cum=None,
        target_usd=float(stake),
        bet_size=float(stake),
        slot_filled_pre=float(entry_fill_cash + prior_topup_cash),
        entry_top_price=(
            float(entry["entry_top_price"])
            if entry["entry_top_price"] is not None else float(ask0)
        ),
        yes_pre1_anchor=entry["yes_pre1_anchor"],
        strike_price=entry["strike_price"],
        window_open_spot=entry["window_open_spot"],
        limit_price=float(limit_price),
        wallet_id=(str(wallet_id) if wallet_id is not None else None),
        offset_sec=int(offset_sec),
        config_sha256=str(entry["config_sha256"] or ""),
    )
    wallet_group = entry["wallet_group"]

    order_client = getattr(deps, "order_client", None)
    if order_client is None:
        return _reject(ledger_id, "no_order_client")

    # Durable reservation BEFORE the CLOB call (crash-recovery artifact). The
    # top-up shares the entry's slot key, so at most ONE may be in flight — the
    # uniq_loa_active_slot partial index enforces it; a race surfaces as an
    # IntegrityError we skip cleanly. reserve_live_order_attempt commits AND
    # bumps the placement epoch — this pass runs on the held-position job thread,
    # concurrently with the fast rungs, and the row it writes is group pending
    # exposure their cached commit-stage views must drop (see order_attempts).
    try:
        attempt = _oa.reserve_live_order_attempt(
            conn, signal, wallet_group=wallet_group,
        )
    except sqlite3.IntegrityError:
        return _reject(ledger_id, "slot_inflight")

    # An attempt IS being made — throttle the next one regardless of outcome.
    _write_state(
        conn, ledger_id, ask0=ask0, running_min=running_min,
        disarmed=False, last_attempt_ts=now_ts,
    )

    result = order_client.place_gtc_limit_order(
        signal, client_order_id=attempt.client_order_id,
    )
    if not result.success:
        try:
            _oa.mark_attempt_failed(
                conn, attempt.id, result.error_kind or "unknown", result.error,
            )
        except Exception:  # noqa: BLE001 — reservation cleanup is best-effort
            logger.debug("scale-in mark_attempt_failed raised", exc_info=True)
        return _reject(ledger_id, f"order_{result.error_kind or 'fail'}")

    live_order_id = result.order_id
    live_filled_size = result.filled_size_tokens
    live_filled_price = result.filled_price_vwap
    live_fill_ts = result.fill_ts

    # Populate the realized fill THIS pass (the FAK response usually carries
    # none) so the NEXT pass's sibling scan sees this top-up: gates (iv) max_n
    # and (v) exposure both count only rows with a non-NULL live_filled_size,
    # and the 60s reconciler lands after a sub-minute window has already closed.
    if live_filled_size is None or float(live_filled_size) <= 0:
        imm = _immediate_fill(
            order_client, live_order_id, offset_sec=offset_sec,
            current_size=live_filled_size, intent_amount_usd=float(stake),
            intent_price=float(walked_limit),
        )
        if imm is not None:
            live_filled_size, live_filled_price, imm_ts = imm
            if imm_ts is not None:
                live_fill_ts = imm_ts

    # Write the LIVE ledger row + attempt link ATOMICALLY (mirrors the entry
    # path's _finalize_bet_row LIVE branch). record_bet enforces
    # scalein_topup ⇒ is_topup via the slot_filled_pre>0 anchor.
    conn.execute("BEGIN IMMEDIATE")
    try:
        _oa.mark_attempt_submitted(conn, attempt.id, live_order_id or "", commit=False)
        row_id = record_bet(
            conn, signal,
            is_topup=True, scalein_topup=True, mode="LIVE",
            # R-NEW-9 contract (was NULL on every T3 row): with the signal's
            # realized_edge now the re-priced add-on edge, this makes
            # add_on_realized_edge_minus_initial_edge = addon - entry edge.
            initial_realized_edge=(
                float(entry["realized_edge"])
                if entry["realized_edge"] is not None else None
            ),
            wallet_group=wallet_group,
            live_order_id=live_order_id,
            live_client_order_id=attempt.client_order_id,
            live_filled_size=live_filled_size,
            live_filled_price=live_filled_price,
            live_fill_ts=live_fill_ts,
            top_ask_at_decision=float(held_ask),
        )
        _oa.link_attempt_to_ledger(conn, attempt.id, row_id)
    except Exception:
        conn.rollback()
        raise

    filled_cash = 0.0
    if live_filled_size and live_filled_price and float(live_filled_size) > 0:
        filled_cash = float(live_filled_size) * float(live_filled_price)
        if budget and budget[0] is not None:
            budget[0] = float(budget[0]) - filled_cash
        logger.info(
            "scale-in top-up FILLED ledger_id=%s row=%s %s/%s side=%s off=%s "
            "stake=%.2f limit=%.4f size=%.4f price=%.4f cash=%.2f "
            "(fills=%d/%d exposure=%.2f/%.2f)",
            ledger_id, row_id, coin, sleeve, side, offset_sec, stake, limit_price,
            float(live_filled_size), float(live_filled_price), filled_cash,
            len(sibs) + 1, cfg.max_n, prior_topup_cash + filled_cash,
            cfg.exposure_cap * entry_fill_cash,
        )
    else:
        logger.info(
            "scale-in top-up ACCEPTED no-fill ledger_id=%s row=%s %s/%s off=%s "
            "stake=%.2f limit=%.4f (reconciler backfills)",
            ledger_id, row_id, coin, sleeve, offset_sec, stake, limit_price,
        )
    return filled_cash


def _immediate_fill(
    order_client,
    order_id: Optional[str],
    *,
    offset_sec: int,
    current_size: Optional[float],
    intent_amount_usd: float,
    intent_price: Optional[float],
):
    """Reuse the entry path's immediate-fill GET so a just-posted top-up's
    realized fill is visible THIS pass. Lazy import avoids a module-load cycle
    (pipeline imports held_position_pass imports this module). Best-effort: any failure
    leaves the fill NULL for the 60s reconciler to backfill."""
    try:
        from cryptobot.execution.pipeline import _immediate_fill_for_held_pass
    except Exception:  # noqa: BLE001 — never break the order path on import
        return None
    try:
        return _immediate_fill_for_held_pass(
            order_client, order_id, offset_sec=offset_sec,
            current_size=current_size, intent_amount_usd=intent_amount_usd,
            intent_price=intent_price,
        )
    except Exception:  # noqa: BLE001 — reconciler is the backstop
        logger.debug("scale-in immediate-fill GET raised", exc_info=True)
        return None
