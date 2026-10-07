"""Per-second held-position pass — the T3 scale-in driver.

After a sub-minute position FILLS, the live entry path HOLDs to binary
settlement. This module re-reads each open sub-minute LIVE position once per
pass and drives T3 scale-in (``execution/scale_in.py``) against a fresh book +
model re-score:

  * fold the held side's best ask into T3's durable no-dip running min
    (``scale_in.observe_no_dip``), and
  * place at most ONE FAK top-up per position per pass
    (``scale_in.scan_topup``), subject to every T3 gate.

**History.** This pass began life as ``execution/stop_loss.py`` (PURE_STOP_04,
operator order 2026-07-09): the same per-second loop additionally re-scored the
held side and SOLD it early when the book overpaid the model AND the position
was underwater. The stop-loss EXIT was retired by operator order 2026-08-05 and
its execution path — trigger→FOK SELL→``book_stop_exit`` PnL booking, the
attempt telemetry writes, the venue-min floor, the slip-exemption and the
``STOP_LOSS`` notifies — is DELETED. Ledger rows that a past stop DID close
(``outcome='CANCELLED'`` + ``exit_kind='stop_loss'`` + ``exit_ts``) remain valid
history: ``execution/capital.py``, ``scheduler/resolution_sweep.py`` and the
dashboard still read them, and neither the ``exit_kind`` column nor the
``stop_loss_attempts`` table is dropped. Nothing writes them any more.

**The deterioration predicate SURVIVES — as a T3 disarm signal, not an exit.**
``_deterioration_disarm`` is the old PURE_STOP_04 trigger. It never sold on the
deployed configuration (both traded intervals were exempt via the retired
``STOPLOSS_EXCLUDE_INTERVALS='5,15'`` pin), but its result was still passed to
``scan_topup`` as ``triggered=``, where a True permanently DISARMS further
top-ups for that market. Dropping it would let T3 keep adding to a position the
model says is deteriorating — a live behavior change. It is kept verbatim, with
its band knob renamed ``SCALEIN_T3_DISARM_BAND`` (same frozen 0.04 default).

Every entry point is best-effort — a failure here must never slow or crash the
live tick. The whole pass is a no-op when ``config.SCALEIN_T3_ENABLED`` is off.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from typing import Optional

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot.execution import scale_in
from cryptobot.execution.capital import get_wallet_snapshots
from cryptobot.strategy.score import score_side_hgb, score_side_twap

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Observability (silence-is-undiagnosable fix, 2026-07-10)
# ---------------------------------------------------------------------------
#
# Before this, the pass logged NOTHING until a position actually acted — so a
# structurally-empty candidate set (the 60s-reconciler-vs-30s-window bug that
# stranded the old stop for 13h / 20 losses / 0 attempts) was invisible. These
# two lines make "the pass sees candidates" observable without spamming the log
# at 1Hz.
_CANDIDATE_LOG_INTERVAL_S = 600.0  # rate-limit the count line to ~10min
_last_candidate_log_ts: float = 0.0
_first_candidate_eval_logged: bool = False


def _observe_candidates(n: int, now_ts: int) -> None:
    """Log a once-per-boot INFO the first time the pass ever sees an open
    candidate, then a rate-limited (>=10min) INFO of the live candidate count.

    Best-effort telemetry only — never raises into the tick."""
    global _last_candidate_log_ts, _first_candidate_eval_logged
    try:
        now = float(now_ts)
        if not _first_candidate_eval_logged:
            _first_candidate_eval_logged = True
            _last_candidate_log_ts = now
            logger.info(
                "held-position pass: first open-position candidate(s) now "
                "visible (n=%d)", n,
            )
            return
        if now - _last_candidate_log_ts >= _CANDIDATE_LOG_INTERVAL_S:
            _last_candidate_log_ts = now
            logger.info(
                "held-position pass: %d open-position candidate(s) this pass", n
            )
    except Exception:  # noqa: BLE001 — telemetry must never fail the tick
        logger.debug("held-position candidate observability failed", exc_info=True)


# ---------------------------------------------------------------------------
# T3 deterioration disarm predicate (ex-PURE_STOP_04 trigger)
# ---------------------------------------------------------------------------


def _deterioration_disarm(
    *, bid: Optional[float], model_p: Optional[float],
    entry_fill: Optional[float], band: float,
) -> bool:
    """True when the held position is deteriorating on the model's own terms:

      ``bid >= model_p + band``   (the book pays a premium over fair value), AND
      ``bid <  entry_fill``       (underwater).

    Any missing / non-finite input returns False. ``bid >= entry_fill`` (a
    winner or flat) always returns False.

    This is the ORIGINAL PURE_STOP_04 trigger, retained verbatim. It no longer
    sells anything — its only consumer is ``scan_topup(triggered=...)``, where
    True permanently disarms further T3 top-ups for the market (matching the
    backtest's drop-legs-at/after-the-trigger-second reference). Semantics are
    unchanged so the disarm cadence T3 was graded under is preserved exactly.
    """
    if bid is None or model_p is None or entry_fill is None:
        return False
    try:
        b = float(bid)
        m = float(model_p)
        e = float(entry_fill)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(b) and math.isfinite(m) and math.isfinite(e)):
        return False
    if b >= e:
        return False
    return b >= m + float(band)


# ---------------------------------------------------------------------------
# Book helpers
# ---------------------------------------------------------------------------


def _top_bid(book: Optional[BookSnapshot]) -> Optional[float]:
    """Highest (best) executable sell price on the token's own book, or None."""
    if book is None:
        return None
    bids = getattr(book, "bids", None)
    if not bids:
        return None
    try:
        price = float(bids[-1]["price"])  # bids are sorted ascending by price
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0 else None


def _top_ask(book: Optional[BookSnapshot]) -> Optional[float]:
    if book is None:
        return None
    ask = book.top_ask
    if ask is None:
        return None
    try:
        a = float(ask)
    except (TypeError, ValueError):
        return None
    return a if math.isfinite(a) else None


def _chain_book(
    chain,
    token: str,
    *,
    now_ts: float,
    require_bid: bool,
) -> Optional[BookSnapshot]:
    """Chain (WS-mirror/prefetch) tier of the pass's book ladder, or None.

    Serve the WS-mirror/prefetch snapshot iff it is present AND — for the HELD
    side, whose bid is load-bearing (``require_bid``) — carries an executable
    bid; on ANY chain miss / stale / bid-missing / exception (and when no chain
    is wired at all — flag ``CRYPTOBOT_HELD_PASS_BOOK_CHAIN`` off) return None
    so the caller falls back to the authoritative REST tier for this token.

    The bid guard is deliberate and load-bearing: a chain source can produce an
    ask-only snapshot (a WS ``book`` frame whose bids side is empty, or deltas
    that cleared every bid level) that ``_snapshot_from_payload`` keeps because
    its ASK side is non-empty. Serving that for the held token would make
    ``_top_bid`` return None, and the pass would then bail before the re-score —
    silently skipping T3's no-dip fold and top-up for that tick (the 73c6153
    silent-blindness class). REST stays authoritative for any snapshot that
    cannot price the held side.
    """
    if chain is None:
        return None
    try:
        get_tagged = getattr(chain, "get_tagged", None)
        if get_tagged is not None:
            snap, _src = get_tagged(token, now_ts=now_ts)
        else:
            snap = chain.get(token, now_ts=now_ts)
        if snap is not None and (not require_bid or _top_bid(snap) is not None):
            return snap
    except Exception:  # noqa: BLE001 — a chain fault must fall back, not crash
        logger.debug(
            "held-position book chain read failed for %s — REST fallback",
            str(token)[:8], exc_info=True,
        )
    return None


def _books_for_pass(
    deps,
    chain,
    held_token: str,
    other_token: str,
    *,
    now_ts: float,
) -> tuple[Optional[BookSnapshot], Optional[BookSnapshot]]:
    """(held_book, other_book) for the re-score, minimizing round trips.

    Chain tier first — per token, with the exact ``require_bid`` semantics the
    per-token ladder always had (held side needs an executable bid) — then ONE
    batched ``POST /books`` for every chain-missed token instead of two SERIAL
    per-token REST GETs. The fallback ladder is token-for-token the entry
    Pass-2 ladder (pipeline.py Pass 2): ``deps.fetch_books`` unwired/None or
    batch error ⇒ per-token ``deps.fetch_book`` GETs for all missed tokens; a
    successful-but-partial batch ⇒ per-token GETs for exactly the omitted ids.
    Parse semantics are identical by construction (the batch POST and the
    per-token GET share ``_snapshot_from_payload``), so no token is ever served
    by fewer data sources than the old serial path — only the round-trip count
    (2 serial GETs → 1 batch POST) and the TokenBucket draw (2 → 1) change.
    ``fetch_books`` is getattr-guarded: deps stubs without the attribute (and
    TickDeps' Optional-None default) take the per-token path unchanged.
    ``config.HELD_PASS_BATCH_BOOKS`` (default ON) is the env kill switch: off ⇒
    the batch attempt is skipped entirely and every missed token takes the
    pre-batch per-token GET path byte-identically.
    """
    held_book = _chain_book(chain, held_token, now_ts=now_ts, require_bid=True)
    other_book = _chain_book(
        chain, other_token, now_ts=now_ts, require_bid=False
    )
    missed = [
        t for t, b in ((held_token, held_book), (other_token, other_book))
        if b is None
    ]
    if not missed:
        return held_book, other_book

    batch = None
    fetch_books = (
        getattr(deps, "fetch_books", None)
        if config.HELD_PASS_BATCH_BOOKS
        else None
    )
    if fetch_books is not None:
        try:
            batch = fetch_books(missed, now_ts=now_ts)
        except Exception:  # noqa: BLE001 — fall back to per-token GETs
            logger.warning(
                "held-position fetch_books batch raised — falling back to "
                "per-token GETs", exc_info=True,
            )
            batch = None

    def _rest(token: str) -> Optional[BookSnapshot]:
        # Batch-served when present; per-token GET on batch error/omission. An
        # omitted token is a venue-partial response or a genuinely ask-empty
        # book — the fallback GET then returns the same None the batch implied
        # (debug, not warning: unlike the entry boundary tick, this pass re-reads
        # the same token at >=1Hz for the whole window, so a persistent
        # ask-empty book would otherwise spam WARNING for up to 30s).
        if batch is not None and token in batch:
            return batch[token]
        if batch is not None:
            logger.debug(
                "held-position POST /books omitted %s — per-token fallback GET",
                str(token)[:8],
            )
        # None-guarded like deps.fetch_books above: TickDeps.fetch_book is
        # Optional, and an unwired fetcher must read as "no book".
        if deps.fetch_book is None:
            return None
        return deps.fetch_book(token, now_ts=now_ts)

    if held_book is None:
        held_book = _rest(held_token)
    if other_book is None:
        other_book = _rest(other_token)
    return held_book, other_book


# ---------------------------------------------------------------------------
# T3 arm gate
# ---------------------------------------------------------------------------


def topup_gate_open(conn: sqlite3.Connection, ledger_id: int) -> bool:
    """False when this position can no longer produce a T3 top-up this pass.

    Replaces the old ``attempts_used``/``STOPLOSS_MAX_ATTEMPTS`` pre-filter,
    which counted rows in ``stop_loss_attempts`` — a table nothing writes any
    more. The FAIL-SAFE SHAPE is preserved exactly: a query error parks the
    position for this pass rather than proceeding blindly (the old gate returned
    MAX_ATTEMPTS on error for the same reason).

    Returning False on an already-disarmed position is behavior-preserving, not
    a new gate: both downstream consumers (``scale_in.observe_no_dip`` and
    ``scale_in._scan_topup_inner``) already return immediately on
    ``disarmed=1``, so the only thing skipped is the wasted market/book fetch
    and HGB re-score. A MISSING state row (never topped up yet) keeps the gate
    OPEN — it is not a disarm.
    """
    try:
        row = conn.execute(
            "SELECT disarmed FROM scalein_state WHERE ledger_id = ?",
            (int(ledger_id),),
        ).fetchone()
    except Exception:  # noqa: BLE001 — fail SAFE (park this position)
        logger.warning(
            "held-position T3 arm-gate query failed for ledger_id=%s",
            ledger_id, exc_info=True,
        )
        return False
    if row is None:
        return True  # no state yet — never observed, still armed
    return int(row["disarmed"] or 0) == 0


# ---------------------------------------------------------------------------
# Candidate query
# ---------------------------------------------------------------------------


def _open_held_positions(conn: sqlite3.Connection, now_ts: int) -> list:
    """Open LIVE sub-minute ENTRY positions still inside their window with a
    real fill.

    Only ``offset_sec IS NOT NULL`` (sub-minute), ``mode='LIVE'``,
    ``outcome='PENDING'`` rows whose window has not yet closed and which carry a
    realized fill size. ``scalein_topup = 0`` excludes T3 scale-in top-up rows —
    the ENTRY row is the position of record; a top-up is NEVER an independent
    candidate (its own no-dip state lives on the parent).
    """
    try:
        return conn.execute(
            """
            SELECT id, coin, sleeve, entry_minute, side, window_start_ts,
                   window_end_ts, interval_min, poly_token_id, live_filled_size,
                   live_filled_price, fill_price, top_ask_at_decision,
                   window_open_spot
              FROM ledger
             WHERE outcome = 'PENDING'
               AND mode = 'LIVE'
               AND offset_sec IS NOT NULL
               AND scalein_topup = 0
               AND window_end_ts > ?
               AND live_filled_size IS NOT NULL
               AND live_filled_size > 0
            """,
            (int(now_ts),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []


# ---------------------------------------------------------------------------
# Per-tick pass
# ---------------------------------------------------------------------------


def run_held_position_pass(
    conn: sqlite3.Connection,
    portfolio,
    *,
    now_ts: int,
    deps,
    spots: dict,
) -> None:
    """Drive T3 scale-in for every open sub-minute LIVE position.

    No-op unless ``config.SCALEIN_T3_ENABLED`` is set AND an OrderClient is
    wired. Best-effort throughout: any per-position error is logged and skipped
    so the tick continues. Runs AFTER Phase B so it never sits between an entry
    decision and its order POST.
    """
    if not config.SCALEIN_T3_ENABLED:
        return
    order_client = getattr(deps, "order_client", None)
    if order_client is None:
        return

    from cryptobot.venues.types import FeeUnknown

    rows = _open_held_positions(conn, now_ts)
    if not rows:
        return

    _observe_candidates(len(rows), now_ts)

    sleeves_by_label = {s.label: s for s in portfolio.sleeves}
    band = float(config.SCALEIN_T3_DISARM_BAND)

    # T3 per-pass risk context (§vi). ``wallet_snaps`` feeds the 35% DD-halt
    # gate (None ⇒ the gate fails closed); ``budget`` is a mutable
    # [remaining_live_balance] the top-up path decrements per fill (None ⇒ never
    # block on a balance fetch, mirroring the entry path). Both are
    # best-effort — a fault here just parks T3 for the pass.
    wallet_snaps: Optional[dict] = None
    try:
        wallet_snaps = get_wallet_snapshots(conn, portfolio)
    except Exception:  # noqa: BLE001 — T3 gate fails closed without snapshots
        logger.warning("scale-in wallet snapshot load failed", exc_info=True)
        wallet_snaps = None
    budget: list = [_fetch_live_balance(deps, order_client)]

    for row in rows:
        try:
            _evaluate_position(
                conn,
                row,
                now_ts=now_ts,
                deps=deps,
                spots=spots,
                sleeves_by_label=sleeves_by_label,
                band=band,
                fee_unknown_cls=FeeUnknown,
                wallet_snaps=wallet_snaps,
                budget=budget,
            )
        except Exception:  # noqa: BLE001 — per-position isolation
            logger.warning(
                "held-position evaluation failed for ledger_id=%s",
                row["id"], exc_info=True,
            )


def _fetch_live_balance(deps, order_client) -> Optional[float]:
    """Latest CLOB collateral balance for the T3 available-cash gate, mirroring
    the entry path's precedence (``deps.live_balance_fetcher`` then
    ``order_client.check_balance``). Best-effort: any failure returns None so
    the top-up cash gate is skipped (placement never blocks on a balance fetch)."""
    fetcher = getattr(deps, "live_balance_fetcher", None)
    if fetcher is not None:
        try:
            return fetcher()
        except Exception:  # noqa: BLE001 — never block T3 on a balance fetch
            logger.debug("scale-in live_balance_fetcher raised", exc_info=True)
            return None
    check = getattr(order_client, "check_balance", None)
    if check is not None:
        try:
            return check()
        except Exception:  # noqa: BLE001 — never block T3 on a balance fetch
            logger.debug("scale-in check_balance raised", exc_info=True)
            return None
    return None


def _evaluate_position(
    conn,
    row,
    *,
    now_ts: int,
    deps,
    spots: dict,
    sleeves_by_label: dict,
    band: float,
    fee_unknown_cls,
    wallet_snaps: Optional[dict] = None,
    budget: Optional[list] = None,
) -> None:
    ledger_id = int(row["id"])
    coin = str(row["coin"])
    sleeve_label = str(row["sleeve"])
    side = str(row["side"])
    window_start_ts = int(row["window_start_ts"])
    interval_min = int(row["interval_min"])
    held_token = row["poly_token_id"]

    # Already permanently disarmed (or the state table is unreadable) — nothing
    # this pass can do for the position. See topup_gate_open.
    if not topup_gate_open(conn, ledger_id):
        return

    sleeve = sleeves_by_label.get(sleeve_label)
    if sleeve is None:
        return  # sub-minute sleeve not loaded (flag off) — cannot re-score
    spot = spots.get(coin)
    if spot is None or not held_token:
        return

    # Current re-score offset = seconds since window open (inside the sleeve's
    # configured terminal checkpoint band, and the window has not closed).
    offset_sec = int(now_ts) - window_start_ts

    market = deps.fetch_market(
        coin, interval_min=interval_min, window_start_ts=window_start_ts,
        conn=conn, now_ts=float(now_ts),
    )
    if market is None or isinstance(market, fee_unknown_cls):
        return
    yes_token = getattr(market, "yes_token_id", None)
    no_token = getattr(market, "no_token_id", None)
    if not yes_token or not no_token:
        return
    other_token = no_token if str(held_token) == str(yes_token) else yes_token

    # Post-order tick hygiene (CRYPTOBOT_HELD_PASS_BOOK_CHAIN): route the two
    # book reads through the WS-mirror/prefetch chain the entry scan already
    # warms, instead of hitting REST. Flag OFF (or no chain wired) ⇒ chain is
    # None ⇒ both tokens go straight to the REST tier of _books_for_pass (one
    # batched POST /books when deps.fetch_books is wired AND the
    # CRYPTOBOT_HELD_PASS_BATCH_BOOKS kill switch is on; per-token GETs
    # otherwise).
    #
    # We deliberately do NOT re-prime the chain here (no note_tokens). The REST
    # BookPrefetcher and the WS mirror both REPLACE their token set wholesale on
    # note_tokens, and the prefetch daemon then replaces its cache wholesale from
    # exactly that set. This chain is SHARED with the entry scan, which primes
    # note_tokens(full window set) each tick; a per-position note_tokens([held,
    # other]) run once per open position inside this loop would leave the shared
    # set as only the LAST position's 2 tokens, silently evicting the entry
    # scan's window tokens from the next boundary's prefetch — a recurring,
    # un-logged latency hit on the entry order path for as long as any position
    # is held. Priming buys almost nothing anyway: _books_for_pass already falls
    # back to the authoritative REST tier on any chain miss.
    chain = (
        getattr(deps, "book_prefetch", None)
        if config.HELD_PASS_BOOK_CHAIN
        else None
    )

    # Held side: its BID is load-bearing (the no-dip crossed-book check and the
    # disarm predicate), so a chain snapshot must carry an executable bid or the
    # token falls to REST. Other side: used ask-only. Chain-missed tokens arrive
    # in ONE batched POST /books instead of two serial per-token GETs.
    held_book, other_book = _books_for_pass(
        deps, chain, str(held_token), str(other_token), now_ts=float(now_ts),
    )
    held_ask = _top_ask(held_book)
    other_ask = _top_ask(other_book)
    bid = _top_bid(held_book)

    # T3 no-dip observation is tick-exact: fold the held-side best ask into the
    # durable running min and permanently disarm on a dip BEFORE the re-score
    # early returns below, so a dip on a tick whose OTHER-side book or model
    # re-score transiently fails still disarms T3 (matches the backtest cummin
    # over every post-entry non-crossed tick). Placement needs the full risk
    # context and still runs later via scan_topup. Fail-closed.
    scale_in.observe_no_dip(conn, row, bid=bid, held_ask=held_ask)

    if held_ask is None or other_ask is None or bid is None:
        return  # cannot re-score / no executable bid this tick

    if config.TWAP_SYMMETRIC_ENABLED:
        persisted_open = row["window_open_spot"]
        open_twap = (
            float(persisted_open)
            if persisted_open is not None and float(persisted_open) > 0.0
            else None
        )
        sample = None
        if open_twap is None and deps.twap_mirror is not None:
            sample = deps.twap_mirror.get_opening_twap(
                coin, sleeve.interval, window_start_ts
            )
            if sample is not None:
                open_twap = float(sample.value)
        model_p = score_side_twap(
            coin=coin,
            sleeve=sleeve,
            offset_sec=offset_sec,
            side=side,
            live_spot=spot.price,
            open_twap=float("nan") if open_twap is None else open_twap,
            held_ask=held_ask,
            other_ask=other_ask,
            window_start_ts=window_start_ts,
            partial_ret_engine=deps.partial_ret_engine,
            twap_hgb=deps.twap_hgb_artifact,
        )
    else:
        model_p = score_side_hgb(
            coin=coin,
            sleeve=sleeve,
            offset_sec=offset_sec,
            side=side,
            live_spot=spot.price,
            held_ask=held_ask,
            other_ask=other_ask,
            window_start_ts=window_start_ts,
            partial_ret_engine=deps.partial_ret_engine,
            checkpoint_rf=deps.checkpoint_rf_artifact,
            checkpoint_hgb=deps.checkpoint_hgb_artifact,
        )
    if model_p is None:
        return

    entry_fill = row["live_filled_price"]
    if entry_fill is None:
        entry_fill = row["fill_price"]
    entry_fill = None if entry_fill is None else float(entry_fill)

    # Model-deterioration disarm (ex-PURE_STOP_04 trigger). Computed BEFORE the
    # top-up so an add-on is never placed on the same second the model turns
    # against the position; scan_topup then only disarms.
    deteriorating = _deterioration_disarm(
        bid=bid, model_p=model_p, entry_fill=entry_fill, band=band
    )

    scale_in.scan_topup(
        conn, row, held_book,
        bid=bid, held_ask=held_ask, now_ts=now_ts, deps=deps,
        wallet_snaps=wallet_snaps,
        budget=budget if budget is not None else [None],
        triggered=deteriorating,
    )

    if deteriorating:
        # scan_topup above already persisted the permanent disarm for this
        # market (it folds `triggered` into the state row's `disarmed`), so
        # there is deliberately NO extra scale_in.disarm() write here — the
        # retired stop path never reached one either on the deployed
        # (both-intervals-exempt) configuration. Log only.
        logger.debug(
            "T3 disarmed on model deterioration: ledger_id=%s %s/%s side=%s "
            "off=%s bid=%.4f model_p=%.4f interval=%dm",
            ledger_id, coin, sleeve_label, side, offset_sec, bid, model_p,
            interval_min,
        )
