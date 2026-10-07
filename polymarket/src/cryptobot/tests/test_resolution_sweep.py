"""U7a resolution sweep tests.

Covers AE6 from the brainstorm: Chainlink success / upstream-down /
out-of-bounds CANCELLED / idempotency.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from cryptobot_polymarket.gamma import (
    RESOLUTION_NOT_FOUND,
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
    PolymarketResolution,
)
from cryptobot.execution.ledger import record_bet
from cryptobot.scheduler.resolution_sweep import run_resolution_sweep
from cryptobot.tests.test_slot_state import make_signal


def _expire(db: sqlite3.Connection, window_end_ts: int = 1_715_923_500) -> None:
    db.execute("UPDATE ledger SET window_end_ts = ?", (window_end_ts,))


def test_chainlink_success_settles_row(db: sqlite3.Connection):
    record_bet(
        db,
        make_signal(
            bet_size=1.0, fill_price=0.55,
        ),
    )
    # yes_pre1_anchor = 0.55 (from make_signal). Resolution > strike → won_up=True.
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 70_000.0,   # > strike anchor 0.55 ish; trivially > 0.55
    )
    assert counters["settled"] == 1
    row = db.execute(
        "SELECT outcome, won_up, resolution_source, pnl_net FROM ledger"
    ).fetchone()
    assert row["outcome"] in ("WIN", "LOSS")
    assert row["resolution_source"] == "chainlink"
    assert row["won_up"] in (0, 1)
    assert row["pnl_net"] is not None


def test_chainlink_down_leaves_row_pending(db: sqlite3.Connection):
    """No oracle price → the row is NOT guessed at; it stays PENDING for the
    next sweep cycle (there is no spot fallback tier any more)."""
    record_bet(db, make_signal(bet_size=1.0, fill_price=0.55))
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: None,
    )
    assert counters["settled"] == 0
    row = db.execute(
        "SELECT outcome, resolution_source FROM ledger"
    ).fetchone()
    assert row["outcome"] == "PENDING"
    assert row["resolution_source"] is None


def test_live_row_settles_from_reconciled_fill_not_intended_stake(
    db: sqlite3.Connection,
):
    """Audit w19oxqf24 finding #7 (P1): a LIVE row must settle from the
    reconciled actual fill (live_filled_size * live_filled_price), NOT the
    intended bet_size / estimated walked_vwap. A YES win at fill 0.50 of 20
    shares = $10 realized stake; settling against an inflated bet_size would
    overstate PnL."""
    record_bet(db, make_signal(bet_size=999.0, fill_price=0.99, walked_vwap=0.99,
                               side="YES"))
    # Reconciler landed the real fill: 20 shares @ 0.50 = $10 actually spent.
    db.execute(
        "UPDATE ledger SET mode='LIVE', live_filled_size=20.0, "
        "live_filled_price=0.50"
    )
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )
    assert counters["settled"] == 1
    row = db.execute("SELECT outcome, pnl_net FROM ledger").fetchone()
    assert row["outcome"] == "WIN"
    # YES win on $10 stake at fill 0.50 → payoff ~ (1/0.50 - 1) * 10 = ~$10,
    # net of fees. It must be on the order of the $10 stake, NOT the $999
    # intended bet_size. A bet_size-based settle would be ~100x larger.
    assert 0 < row["pnl_net"] < 30, (
        f"LIVE pnl_net {row['pnl_net']} not sized on the $10 realized fill — "
        "settlement is still using the intended stake"
    )


def test_live_row_without_reconciled_fill_stays_pending(db: sqlite3.Connection):
    """Audit w19oxqf24 finding #7 (P1): a LIVE row the reconciler has NOT yet
    touched (live_filled_size NULL) must NOT settle against the optimistic
    placeholder — it stays PENDING for the order-reconciliation job."""
    record_bet(db, make_signal(bet_size=10.0, fill_price=0.50, walked_vwap=0.50,
                               side="YES"))
    db.execute("UPDATE ledger SET mode='LIVE', live_filled_size=NULL, "
               "live_filled_price=NULL")
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )
    assert counters["settled"] == 0
    assert counters["skipped"] == 1
    row = db.execute("SELECT outcome FROM ledger").fetchone()
    assert row["outcome"] == "PENDING", "un-reconciled LIVE row must stay PENDING"


def test_live_row_size_no_price_settles_from_recorded_taker_price(
    db: sqlite3.Connection,
) -> None:
    """FIX 2: a LIVE row with a reconciled size but NULL live_filled_price
    settles from the taker price actually PAID, already on the row — preferring
    walked_vwap — and backfills live_filled_price with it. The legacy
    bet_size/live_filled_size inference is not used when a recorded price
    exists (it is wrong for partial fills).
    """
    bet_size = 4.142162676416512
    live_size = 9.857141
    record_bet(
        db,
        make_signal(
            bet_size=bet_size,
            fill_price=0.41,
            walked_vwap=0.43,
            side="YES",
        ),
    )
    db.execute(
        "UPDATE ledger SET mode='LIVE', live_filled_size=?, "
        "live_filled_price=NULL",
        (live_size,),
    )
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )

    assert counters["settled"] == 1
    row = db.execute(
        "SELECT outcome, pnl_net, live_filled_price FROM ledger"
    ).fetchone()
    assert row["outcome"] == "WIN"
    assert row["pnl_net"] > 0
    # walked_vwap (0.43) is the recorded taker price and takes precedence over
    # both the top-ask fill_price (0.41) and the broken implied inference.
    assert row["live_filled_price"] == pytest.approx(0.43)


def test_live_row_571_shape_settles_from_fill_price(db: sqlite3.Connection):
    """Regression for LIVE row 571: a partial fill persisted size 0.33 with a
    NULL price and no walked_vwap. The sweep must settle from the top-ask
    fill_price (0.76) actually recorded — realized stake = 0.33 * 0.76 — not
    strand the row via the bet_size/size inference (1.097/0.33 = 3.32 > 1.0)."""
    record_bet(
        db,
        make_signal(
            bet_size=1.097, fill_price=0.76, walked_vwap=None,
            strike_price=65_000.0, window_open_spot=65_000.0, side="YES",
        ),
        mode="LIVE",
    )
    db.execute(
        "UPDATE ledger SET live_filled_size=0.33, live_filled_price=NULL"
    )
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )

    assert counters["settled"] == 1
    row = db.execute(
        "SELECT outcome, pnl_net, live_filled_price FROM ledger"
    ).fetchone()
    assert row["outcome"] == "WIN"
    assert row["live_filled_price"] == pytest.approx(0.76)
    # Realized stake = 0.33 shares * 0.76 = $0.2508. A YES win pays ~ (1/0.76)
    # per share, so pnl is on the order of the $0.25 stake — NOT the $1.097
    # intended bet_size. Graded under the 2026-07-23 calibrated fee convention
    # (entry-leg fee; formula pinned in test_grade_fees).
    from cryptobot.execution.grade_fees import graded_cash_pnl

    assert row["pnl_net"] == pytest.approx(
        graded_cash_pnl("YES", 0.76, 1, 0.33 * 0.76),
        rel=1e-9,
    )


def test_dry_row_still_settles_from_bet_size(db: sqlite3.Connection):
    """The LIVE-aware path must not change DRY settlement: a DRY row settles
    from bet_size / walked_vwap exactly as before (no live fill columns)."""
    record_bet(db, make_signal(bet_size=10.0, fill_price=0.50, walked_vwap=0.50,
                               side="YES"))
    # mode stays DRY (schema default); no live_filled_* set.
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )
    assert counters["settled"] == 1
    row = db.execute("SELECT outcome, pnl_net FROM ledger").fetchone()
    assert row["outcome"] == "WIN"
    assert row["pnl_net"] > 0


def test_polymarket_gamma_outcome_takes_precedence_over_oracle(
    db: sqlite3.Connection,
):
    """Live ledger must settle against Polymarket's final outcome when
    Gamma exposes it, even if the oracle comparator would disagree."""
    record_bet(
        db,
        make_signal(
            bet_size=10.0,
            fill_price=0.20,
            walked_vwap=0.20,
            strike_price=65_000.0,
            window_open_spot=65_000.0,
            side="YES",
        ),
    )
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_RESOLVED,
            coin=c,
            interval_min=interval,
            window_start_ts=start,
            window_end_ts=start + interval * 60,
            condition_id="COND",
            won_up=1,
            up_price=1.0,
            down_price=0.0,
        ),
        chainlink_fetch=lambda c, t: None,
    )

    assert counters["settled"] == 1
    row = db.execute(
        "SELECT outcome, won_up, resolution_price, resolution_source, "
        "polymarket_resolution_price, polymarket_resolution_source, pnl_net "
        "FROM ledger"
    ).fetchone()
    assert row["outcome"] == "WIN"
    assert row["won_up"] == 1
    assert row["resolution_source"] == "polymarket_gamma"
    assert row["resolution_price"] == pytest.approx(1.0)
    assert row["polymarket_resolution_price"] == pytest.approx(1.0)
    assert row["polymarket_resolution_source"] == "polymarket_gamma"
    assert row["pnl_net"] > 0


def test_unresolved_polymarket_market_stays_pending_without_oracle_guess(
    db: sqlite3.Connection,
):
    """If Gamma says the market is still open, leave the row pending.
    Settling from an oracle spot at that point can create the exact mismatch
    seen live."""
    record_bet(
        db,
        make_signal(
            bet_size=10.0,
            fill_price=0.20,
            walked_vwap=0.20,
            strike_price=65_000.0,
            window_open_spot=65_000.0,
            side="YES",
        ),
    )
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        polymarket_resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_OPEN,
            coin=c,
            interval_min=interval,
            window_start_ts=start,
            window_end_ts=start + interval * 60,
        ),
        chainlink_fetch=lambda c, t: None,
    )

    assert counters["skipped"] == 1
    assert counters["settled"] == 0
    row = db.execute("SELECT outcome, resolution_source FROM ledger").fetchone()
    assert row["outcome"] == "PENDING"
    assert row["resolution_source"] is None


def _crit_resolve_count(db: sqlite3.Connection) -> int:
    return db.execute(
        "SELECT COUNT(*) FROM pipeline_health WHERE stage='resolve' "
        "AND status='CRITICAL'"
    ).fetchone()[0]


def test_stale_live_pending_unresolvable_escalates_critical(
    db: sqlite3.Connection,
):
    """Audit P1 #12: a LIVE PENDING row Polymarket can no longer resolve,
    older than MAX_PENDING_SETTLE_AGE_S, escalates to a CRITICAL
    pipeline_health row (stranded capital surfaced) but is NOT auto-cancelled
    (the market may have resolved and only the fetch is failing)."""
    record_bet(
        db,
        make_signal(
            bet_size=10.0, fill_price=0.20, walked_vwap=0.20,
            strike_price=65_000.0, window_open_spot=65_000.0, side="YES",
        ),
        mode="LIVE",
    )
    db.execute("UPDATE ledger SET live_filled_size=18.0, live_filled_price=0.20")
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_000 + 7 * 3600,  # 7h > 6h default threshold
        polymarket_resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_NOT_FOUND, coin=c, interval_min=interval,
            window_start_ts=start, window_end_ts=start + interval * 60,
        ),
        chainlink_fetch=lambda c, t: None,
    )
    assert counters["skipped"] == 1
    assert db.execute("SELECT outcome FROM ledger").fetchone()["outcome"] == "PENDING"
    assert _crit_resolve_count(db) >= 1


def test_fresh_live_pending_unresolvable_stays_warn(db: sqlite3.Connection):
    """A recently-ended LIVE PENDING row briefly unresolvable is WARN, not
    CRITICAL — a transient Gamma outage should self-heal first."""
    record_bet(
        db,
        make_signal(
            bet_size=10.0, fill_price=0.20, walked_vwap=0.20,
            strike_price=65_000.0, window_open_spot=65_000.0, side="YES",
        ),
        mode="LIVE",
    )
    db.execute("UPDATE ledger SET live_filled_size=18.0, live_filled_price=0.20")
    _expire(db, window_end_ts=2_000_000_000)

    run_resolution_sweep(
        db,
        now_ts=2_000_000_100,  # only 100s after window_end
        polymarket_resolution_fetch=lambda c, interval, start: PolymarketResolution(
            status=RESOLUTION_NOT_FOUND, coin=c, interval_min=interval,
            window_start_ts=start, window_end_ts=start + interval * 60,
        ),
        chainlink_fetch=lambda c, t: None,
    )
    assert _crit_resolve_count(db) == 0


def test_stale_live_unreconciled_fill_escalates_critical(db: sqlite3.Connection):
    """FIX 3: a LIVE PENDING row whose market RESOLVED but that the reconciler
    never landed a fill on (live_filled_size NULL) holds its bet_size in the
    group cap. Past MAX_PENDING_SETTLE_AGE_S it must escalate to CRITICAL —
    like the unresolvable-market path — but NOT auto-cancel (a real fill may
    just be un-priced)."""
    record_bet(
        db,
        make_signal(
            bet_size=10.0, fill_price=0.20, walked_vwap=0.20,
            strike_price=65_000.0, window_open_spot=65_000.0, side="YES",
        ),
        mode="LIVE",
    )
    db.execute("UPDATE ledger SET live_filled_size=NULL, live_filled_price=NULL")
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_000 + 7 * 3600,  # 7h > 6h default threshold
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )
    assert counters["skipped"] == 1
    assert counters["settled"] == 0
    assert counters["cancelled"] == 0  # never auto-cancel
    assert db.execute("SELECT outcome FROM ledger").fetchone()["outcome"] == "PENDING"
    assert _crit_resolve_count(db) >= 1


def test_fresh_live_unreconciled_fill_stays_warn(db: sqlite3.Connection):
    """A freshly-ended LIVE row still awaiting reconciliation is WARN, not
    CRITICAL — the 60s reconciler tick should land the fill first."""
    record_bet(
        db,
        make_signal(
            bet_size=10.0, fill_price=0.20, walked_vwap=0.20,
            strike_price=65_000.0, window_open_spot=65_000.0, side="YES",
        ),
        mode="LIVE",
    )
    db.execute("UPDATE ledger SET live_filled_size=NULL, live_filled_price=NULL")
    _expire(db, window_end_ts=2_000_000_000)

    run_resolution_sweep(
        db,
        now_ts=2_000_000_100,  # only 100s after window_end
        polymarket_resolution_fetch=lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=1, up_price=1.0, down_price=0.0,
        ),
    )
    assert _crit_resolve_count(db) == 0
    assert db.execute("SELECT outcome FROM ledger").fetchone()["outcome"] == "PENDING"


def test_strike_price_drives_won_up_decision(db: sqlite3.Connection):
    """Review #2 — won_up MUST compare resolution_price to the underlying
    spot (strike_price column), NOT to yes_pre1_anchor (a Polymarket
    probability in [0, 1])."""
    # YES bet with strike $65000. Resolution $70000 > $65000 → won_up=True
    # → outcome=WIN. If the sweep mistakenly compared to yes_pre1_anchor
    # (0.55), every resolution_price would be > 0.55 → silent WINs.
    record_bet(db, make_signal(bet_size=1.0, fill_price=0.55, strike_price=65_000.0))
    _expire(db, window_end_ts=2_000_000_000)
    run_resolution_sweep(
        db, now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    row = db.execute(
        "SELECT outcome, won_up, resolution_price FROM ledger"
    ).fetchone()
    assert row["resolution_price"] == 70_000.0
    assert row["won_up"] == 1   # 70000 > 65000
    assert row["outcome"] == "WIN"

    # Drop the YES win → reset for the bear case below.
    db.execute("DELETE FROM ledger")
    record_bet(db, make_signal(bet_size=1.0, fill_price=0.55, strike_price=65_000.0))
    _expire(db, window_end_ts=2_000_000_000)
    run_resolution_sweep(
        db, now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 60_000.0,   # < strike
    )
    row = db.execute(
        "SELECT outcome, won_up FROM ledger"
    ).fetchone()
    assert row["won_up"] == 0
    assert row["outcome"] == "LOSS"


def test_row_with_no_usable_anchor_is_skipped(db: sqlite3.Connection):
    """Sweep must refuse to grade rows that have neither window_open_spot
    nor strike_price (and yes_pre1_anchor is in probability range, so
    unsafe). Carries the original review #2 invariant — silent fall-back
    to yes_pre1_anchor would compare a probability against a price.

    Renamed from test_legacy_row_without_strike_price_is_skipped after
    divergence #13 (2026-05-21): the anchor hierarchy is now
    window_open_spot → strike_price → yes_pre1_anchor (spot-shaped only).
    The skip path now requires BOTH window_open_spot AND strike_price
    to be NULL, not just strike_price.
    """
    record_bet(
        db,
        make_signal(
            bet_size=1.0, fill_price=0.55,
            strike_price=None, window_open_spot=None,
        ),
    )
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db, now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    assert counters["skipped"] == 1
    assert counters["settled"] == 0
    row = db.execute("SELECT outcome FROM ledger").fetchone()
    assert row["outcome"] == "PENDING"   # left for manual repair


class TestWindowOpenSpotAnchor:
    """Divergence #13 fix (2026-05-21): the sweep grades against
    window_open_spot (the same anchor the model trains against and
    Polymarket Chainlink settles on), not against strike_price
    (decision-time spot — the buggy anchor).
    """

    def test_record_bet_persists_window_open_spot(self, db: sqlite3.Connection):
        """BetSignal.window_open_spot round-trips through the INSERT to
        the new column. Without this, the sweep can't read what
        pipeline wrote."""
        record_bet(
            db,
            make_signal(
                bet_size=1.0, fill_price=0.55,
                strike_price=65_000.0,
                window_open_spot=64_800.0,
            ),
        )
        row = db.execute(
            "SELECT strike_price, window_open_spot FROM ledger"
        ).fetchone()
        assert row["strike_price"] == 65_000.0
        assert row["window_open_spot"] == 64_800.0

    def test_window_open_spot_drives_outcome_when_present(
        self, db: sqlite3.Connection
    ):
        """When window_open_spot is set, it must be what
        _decide_won_up compares resolution_price against — NOT
        strike_price. Resolution 70k > window_open 64k → WIN."""
        record_bet(
            db,
            make_signal(
                bet_size=1.0, fill_price=0.55,
                strike_price=65_000.0,
                window_open_spot=64_000.0,
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 70_000.0,
        )
        row = db.execute(
            "SELECT outcome, won_up FROM ledger"
        ).fetchone()
        assert row["won_up"] == 1
        assert row["outcome"] == "WIN"

    def test_window_open_spot_preference_can_flip_outcome(
        self, db: sqlite3.Connection
    ):
        """The smoking gun: same resolution_price, same YES bet, but
        window_open_spot < resolution_price < strike_price → with the
        old anchor (strike_price) the bet would be a LOSS; with the
        new anchor (window_open_spot) it's a WIN. This is exactly the
        4-day-33%-WR pattern divergence #13 produced live."""
        # Simulate: 4-minute trend ran the price up before bot acted.
        #   window_open price:    77_800  (where Polymarket actually anchors)
        #   bot decision-time:    77_950  (4 min later — strike_price)
        #   window_close resol:   77_900  (small pullback)
        #
        # Under the OLD anchor (strike_price=77_950), 77_900 < 77_950 →
        # YES would LOSE. Under the NEW anchor (window_open_spot=77_800),
        # 77_900 > 77_800 → YES WINS. That flip is the entire fix.
        record_bet(
            db,
            make_signal(
                bet_size=1.0, fill_price=0.55,
                strike_price=77_950.0,
                window_open_spot=77_800.0,
                side="YES",
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 77_900.0,
        )
        row = db.execute(
            "SELECT outcome, won_up FROM ledger"
        ).fetchone()
        # Under the fix: window_open_spot is the anchor; resolution
        # > window_open → YES wins.
        assert row["won_up"] == 1
        assert row["outcome"] == "WIN"

    def test_falls_back_to_strike_price_when_window_open_spot_null(
        self, db: sqlite3.Connection
    ):
        """Legacy rows written before the divergence-#13 fix (or any
        future hole where partial_ret somehow misses a boundary) have
        window_open_spot=NULL. The sweep must still settle them by
        falling back to strike_price — never raise, never silently
        drop the row."""
        record_bet(
            db,
            make_signal(
                bet_size=1.0, fill_price=0.55,
                strike_price=65_000.0,
                window_open_spot=None,
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        counters = run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 70_000.0,
        )
        assert counters["settled"] == 1
        row = db.execute(
            "SELECT outcome, won_up FROM ledger"
        ).fetchone()
        # Resolution 70k > strike_price 65k → YES wins.
        assert row["won_up"] == 1
        assert row["outcome"] == "WIN"


def test_pnl_formula_is_calibrated_backtest_cash_pnl(db: sqlite3.Connection):
    """Settlement P&L treats bet_size as cash spent, and since the 2026-07-23
    calibrated fee grading it books the backtest payout base with the fee term
    scaled by POLY_FEE_CALIBRATION: WIN = S*(1-p) - k*theta*p*(1-p)*S, i.e.
    backtest cash_pnl + (1-k)*theta*p*(1-p)*S. Nothing but the fee term may
    diverge from the frozen backtest formula."""
    from polymarket.backtest.engine.sweep_lib import cash_pnl as backtest_cash_pnl
    from cryptobot import config

    record_bet(db, make_signal(bet_size=3.0, fill_price=0.55, strike_price=65_000.0))
    _expire(db, window_end_ts=2_000_000_000)
    run_resolution_sweep(
        db, now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    row = db.execute(
        "SELECT outcome, pnl_net, fill_price, bet_size FROM ledger"
    ).fetchone()
    p = row["fill_price"]
    shares = row["bet_size"] / p
    base = backtest_cash_pnl(
        "YES", p, won_up=1, stake_usd=row["bet_size"], slippage_haircut=0.0,
    )
    k = config.POLY_FEE_CALIBRATION
    expected = base + (1.0 - k) * config.POLY_FEE_THETA * p * (1.0 - p) * shares
    assert row["pnl_net"] == pytest.approx(expected, abs=1e-9)


def test_both_upstreams_down_leaves_pending(db: sqlite3.Connection):
    record_bet(db, make_signal(bet_size=1.0))
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: None,
    )
    assert counters["skipped"] == 1
    row = db.execute("SELECT outcome FROM ledger").fetchone()
    assert row["outcome"] == "PENDING"


def test_out_of_bounds_cancels_row(db: sqlite3.Connection):
    record_bet(db, make_signal(bet_size=1.0))
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 0.0,   # BTC=$0 → out-of-bounds
    )
    assert counters["cancelled"] == 1
    row = db.execute("SELECT outcome, pnl_net FROM ledger").fetchone()
    assert row["outcome"] == "CANCELLED"
    assert row["pnl_net"] == 0
    health = db.execute(
        "SELECT status, message FROM pipeline_health WHERE stage='resolve'"
    ).fetchone()
    assert health is not None
    assert health["status"] == "CRITICAL"


def test_excessive_deviation_cancels_row(db: sqlite3.Connection):
    record_bet(db, make_signal(bet_size=1.0))
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db,
        now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 30_000.0,
        reference_spot_fetch=lambda c, t: 67_000.0,
    )
    assert counters["cancelled"] == 1
    row = db.execute("SELECT outcome FROM ledger").fetchone()
    assert row["outcome"] == "CANCELLED"


def test_sweep_idempotent(db: sqlite3.Connection):
    record_bet(db, make_signal(bet_size=1.0))
    _expire(db, window_end_ts=2_000_000_000)
    run_resolution_sweep(
        db, now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    # Second sweep should not re-settle (outcome != PENDING now).
    counters2 = run_resolution_sweep(
        db, now_ts=2_000_000_200,
        chainlink_fetch=lambda c, t: 80_000.0,
    )
    assert counters2["settled"] == 0
    assert counters2["cancelled"] == 0
    assert counters2["skipped"] == 0
    n_rows = db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
    assert n_rows == 1


def test_unexpired_rows_not_swept(db: sqlite3.Connection):
    record_bet(db, make_signal(bet_size=1.0))
    _expire(db, window_end_ts=3_000_000_000)   # well into the future
    counters = run_resolution_sweep(
        db, now_ts=2_000_000_000,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    assert counters["settled"] == 0
    assert counters["cancelled"] == 0
    assert counters["skipped"] == 0


class TestWalkedVwapPnl:
    """Divergence #6: pnl_net MUST use walked_vwap when present.

    Real walked_vwap is on average ~52 bps higher than fill_price in the
    live ledger; using fill_price (top_ask) under-reports losses. These
    tests assert resolution_sweep picks walked_vwap whenever it's set,
    while falling back to fill_price for legacy/None rows.
    """

    def test_pnl_uses_walked_vwap_when_present(self, db: sqlite3.Connection):
        # YES bet on BTC. fill_price=0.55 (top_ask at decision), but the
        # book was thin so we walked the levels — walked_vwap=0.60.
        # Strike 65_000; resolution 70_000 → won_up → YES wins.
        # Calibrated grade at fill=0.60: pnl_per_share = 0.40 - 0.864*0.07*0.60*0.40
        #   ≈ 0.38548 — using walked_vwap.
        # At fill=0.55: pnl_per_share = 0.45 - 0.864*0.07*0.55*0.45 ≈ 0.43503
        #   — using top_ask.
        # The two are materially different (~5pp) so we can detect which path fired.
        record_bet(
            db,
            make_signal(
                bet_size=10.0, fill_price=0.55, walked_vwap=0.60,
                strike_price=65_000.0,
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 70_000.0,
            slippage_haircut_by_sleeve={"v6_5m": 0.0},
        )
        row = db.execute(
            "SELECT outcome, won_up, fill_price, walked_vwap, bet_size, pnl_net "
            "FROM ledger"
        ).fetchone()
        assert row["outcome"] == "WIN"
        assert row["fill_price"] == pytest.approx(0.55)
        assert row["walked_vwap"] == pytest.approx(0.60)
        from cryptobot.execution.grade_fees import graded_cash_pnl

        assert row["pnl_net"] == pytest.approx(
            graded_cash_pnl("YES", 0.60, 1, 10.0), rel=1e-9
        )

    def test_pnl_falls_back_to_fill_price_when_walked_vwap_null(
        self, db: sqlite3.Connection
    ):
        # Legacy row: walked_vwap is NULL. Must use fill_price.
        record_bet(
            db,
            make_signal(
                bet_size=10.0, fill_price=0.55, walked_vwap=None,
                strike_price=65_000.0,
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 70_000.0,
            slippage_haircut_by_sleeve={"v6_5m": 0.0},
        )
        row = db.execute(
            "SELECT pnl_net FROM ledger"
        ).fetchone()
        from cryptobot.execution.grade_fees import graded_cash_pnl

        assert row["pnl_net"] == pytest.approx(
            graded_cash_pnl("YES", 0.55, 1, 10.0), rel=1e-9
        )

    def test_loss_path_uses_walked_vwap(self, db: sqlite3.Connection):
        # YES bet, strike=65_000, resolution=64_000 → YES loses.
        # 2026-07-23 calibrated fee grading: the venue charged the taker fee
        # on the ENTRY fill, so the loss books -stake MINUS the calibrated
        # entry-leg fee — walked_vwap controls both the share count and the
        # fee's p*(1-p).
        record_bet(
            db,
            make_signal(
                bet_size=10.0, fill_price=0.55, walked_vwap=0.60,
                strike_price=65_000.0,
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 64_000.0,
            slippage_haircut_by_sleeve={"v6_5m": 0.0},
        )
        row = db.execute(
            "SELECT outcome, pnl_net FROM ledger"
        ).fetchone()
        assert row["outcome"] == "LOSS"
        # -10 stake - 0.864*0.07*0.60*0.40*(10/0.60 shares) = -10 - 0.24192.
        assert row["pnl_net"] == pytest.approx(
            -10.0 - 0.864 * 0.07 * 0.60 * 0.40 * (10.0 / 0.60), rel=1e-9
        )

    def test_realized_pnl_does_not_double_apply_strategy_haircut(
        self, db: sqlite3.Connection
    ):
        # Strategy slippage_haircut is a backtest/scoring conservatism. Once
        # the live ledger has walked_vwap, that is the actual paid fill; a
        # losing cash-sized row must lose exactly the cash spent plus the
        # calibrated entry-leg fee — the sleeve haircut (0.25 here) must NOT
        # shift the fill the fee/shares are computed from.
        record_bet(
            db,
            make_signal(
                bet_size=5.0, fill_price=0.55, walked_vwap=0.60,
                strike_price=65_000.0,
            ),
        )
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            chainlink_fetch=lambda c, t: 64_000.0,
            slippage_haircut_by_sleeve={"v6_5m": 0.25},
        )
        row = db.execute("SELECT outcome, pnl_net FROM ledger").fetchone()
        assert row["outcome"] == "LOSS"
        assert row["pnl_net"] == pytest.approx(
            -5.0 - 0.864 * 0.07 * 0.60 * 0.40 * (5.0 / 0.60), rel=1e-9
        )


# ---------------------------------------------------------------------------
# Calibrated fee grading is forward-only
# ---------------------------------------------------------------------------
class TestCalibratedFeeGrading:
    """The sweep never rewrites an already-graded row. Formula pins live in
    test_grade_fees."""

    def _resolved(self, won_up: int):
        return lambda c, i, s: PolymarketResolution(
            status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
            window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
            won_up=won_up, up_price=1.0 if won_up else 0.0,
            down_price=0.0 if won_up else 1.0,
        )

    def test_sweep_never_rewrites_a_graded_row(self, db, monkeypatch):
        """Forward-only proof: the sweep predicates on outcome='PENDING', so a
        second sweep — even with a FLIPPED resolution and a retuned
        calibration knob — leaves a graded row's pnl_net byte-identical. The
        convention change applies to NEW grades only; no migration path
        exists in the sweep."""
        from cryptobot import config

        record_bet(db, make_signal(bet_size=10.0, fill_price=0.55, side="YES"))
        _expire(db, window_end_ts=2_000_000_000)
        run_resolution_sweep(
            db, now_ts=2_000_000_100,
            polymarket_resolution_fetch=self._resolved(1),
        )
        first = db.execute("SELECT outcome, pnl_net FROM ledger").fetchone()
        assert first["outcome"] == "WIN"

        monkeypatch.setattr(config, "POLY_FEE_CALIBRATION", 0.10)
        counters2 = run_resolution_sweep(
            db, now_ts=2_000_000_200,
            polymarket_resolution_fetch=self._resolved(0),
        )
        assert counters2["settled"] == 0
        second = db.execute("SELECT outcome, pnl_net FROM ledger").fetchone()
        assert second["outcome"] == "WIN"
        assert second["pnl_net"] == first["pnl_net"]


# ---------------------------------------------------------------------------
# Stop-loss resolution backfill (display-only; booked-once invariants sacred)
# ---------------------------------------------------------------------------
def _resolved(won_up: int):
    """A polymarket_resolution_fetch that always returns RESOLVED for won_up."""
    return lambda c, i, s: PolymarketResolution(
        status=RESOLUTION_RESOLVED, coin=c, interval_min=i,
        window_start_ts=s, window_end_ts=s + i * 60, condition_id="C",
        won_up=won_up, up_price=1.0 if won_up else 0.0,
        down_price=0.0 if won_up else 1.0,
    )


def _make_stop_row(
    db: sqlite3.Connection,
    *,
    side: str = "YES",
    pnl_net: float = -1.0,
    exit_price: float = 0.50,
    live_filled_size: float = 5.0,
    live_filled_price: float = 0.70,
    window_start_ts: int = 1_715_923_200,
) -> int:
    """Insert a booked-once stop-closed row: outcome='CANCELLED',
    exit_kind='stop_loss', a realized round-trip pnl_net, won_up NULL
    (pre-backfill)."""
    rid = record_bet(
        db,
        make_signal(side=side, window_start_ts=window_start_ts),
        mode="LIVE",
        live_filled_size=live_filled_size,
        live_filled_price=live_filled_price,
    )
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=?, exit_price=?, "
        "exit_ts='2026-07-11T00:00:00Z', exit_kind='stop_loss', won_up=NULL "
        "WHERE id=?",
        (pnl_net, exit_price, rid),
    )
    return rid


def test_stop_backfill_sets_won_up_without_touching_booked_pnl(
    db: sqlite3.Connection,
):
    """The display-only backfill writes won_up + resolution fields onto a
    stop-closed row while leaving pnl_net / outcome / exit_* — the booked-once
    invariants — untouched. The settlement loop never touches it (it predicates
    on outcome='PENDING')."""
    rid = _make_stop_row(db, side="YES", pnl_net=-1.0, exit_price=0.50)
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db, now_ts=2_000_000_100, polymarket_resolution_fetch=_resolved(1),
    )
    assert counters["stop_resolutions_backfilled"] == 1
    assert counters["settled"] == 0  # a CANCELLED row is never re-settled

    row = db.execute(
        "SELECT outcome, pnl_net, exit_price, exit_kind, exit_ts, won_up, "
        "resolution_price, resolution_source, resolution_ts, "
        "polymarket_resolution_price, polymarket_resolution_source "
        "FROM ledger WHERE id=?",
        (rid,),
    ).fetchone()
    # Backfilled display fields.
    assert row["won_up"] == 1
    assert row["resolution_price"] == pytest.approx(1.0)
    assert row["resolution_source"] == "polymarket_gamma"
    assert row["polymarket_resolution_price"] == pytest.approx(1.0)
    assert row["polymarket_resolution_source"] == "polymarket_gamma"
    # resolution_ts is left NULL here (defense in depth). capital.py's DD peak
    # replay keys stop rows on the immutable exit_ts (not resolution_ts), so the
    # gate is invariant regardless; we still avoid stamping the sweep clock so
    # the with/without-backfill states stay visibly identical.
    assert row["resolution_ts"] is None
    # SACRED: the booked-once loss + exit tags are untouched.
    assert row["outcome"] == "CANCELLED"
    assert row["pnl_net"] == pytest.approx(-1.0)
    assert row["exit_price"] == pytest.approx(0.50)
    assert row["exit_kind"] == "stop_loss"
    assert row["exit_ts"] == "2026-07-11T00:00:00Z"


def test_stop_backfill_is_idempotent(db: sqlite3.Connection):
    """Once won_up is set the row drops out of the backfill SELECT, and the
    guarded UPDATE (won_up IS NULL) can never overwrite it on a later sweep."""
    rid = _make_stop_row(db)
    _expire(db, window_end_ts=2_000_000_000)
    run_resolution_sweep(
        db, now_ts=2_000_000_100, polymarket_resolution_fetch=_resolved(1),
    )
    # Second sweep would resolve DOWN — the already-backfilled row is untouched.
    counters2 = run_resolution_sweep(
        db, now_ts=2_000_000_200, polymarket_resolution_fetch=_resolved(0),
    )
    assert counters2["stop_resolutions_backfilled"] == 0
    row = db.execute("SELECT won_up FROM ledger WHERE id=?", (rid,)).fetchone()
    assert row["won_up"] == 1  # first backfill stands; never overwritten


def test_stop_backfill_skips_legacy_voided_cancelled(db: sqlite3.Connection):
    """A legacy voided CANCELLED row (exit_kind IS NULL — cutover reset / orphan
    close) must NEVER be backfilled: the pass discriminates on
    exit_kind='stop_loss' only."""
    legacy = record_bet(db, make_signal(), mode="LIVE")
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=0, exit_kind=NULL, "
        "won_up=NULL WHERE id=?",
        (legacy,),
    )
    _expire(db, window_end_ts=2_000_000_000)

    counters = run_resolution_sweep(
        db, now_ts=2_000_000_100, polymarket_resolution_fetch=_resolved(1),
    )
    assert counters["stop_resolutions_backfilled"] == 0
    row = db.execute(
        "SELECT won_up, outcome, pnl_net FROM ledger WHERE id=?", (legacy,)
    ).fetchone()
    assert row["won_up"] is None
    assert row["outcome"] == "CANCELLED"
    assert row["pnl_net"] == 0


def test_stop_backfill_bounded_to_expired_windows(db: sqlite3.Connection):
    """A stop row whose window has NOT expired is not backfilled — bounded to
    window_end_ts <= now, mirroring the settlement expiry predicate."""
    rid = _make_stop_row(db)
    db.execute("UPDATE ledger SET window_end_ts=2_000_001_000 WHERE id=?", (rid,))
    counters = run_resolution_sweep(
        db, now_ts=2_000_000_100, polymarket_resolution_fetch=_resolved(1),
    )
    assert counters["stop_resolutions_backfilled"] == 0
    row = db.execute("SELECT won_up FROM ledger WHERE id=?", (rid,)).fetchone()
    assert row["won_up"] is None


def test_stop_backfill_noop_without_polymarket_fetcher(db: sqlite3.Connection):
    """The legacy oracle-spot path (no polymarket fetcher) never runs the
    stop-loss backfill — the counter is absent and the stop row is left alone."""
    rid = _make_stop_row(db)
    _expire(db, window_end_ts=2_000_000_000)
    counters = run_resolution_sweep(
        db, now_ts=2_000_000_100,
        chainlink_fetch=lambda c, t: 70_000.0,
    )
    assert counters["stop_resolutions_backfilled"] == 0
    row = db.execute("SELECT won_up FROM ledger WHERE id=?", (rid,)).fetchone()
    assert row["won_up"] is None


def test_stop_backfill_does_not_perturb_capital_drawdown_replay(
    db: sqlite3.Connection,
):
    """Regression (the skeptic's exact scenario): the display-only stop backfill
    must NOT change capital.py's order-sensitive peak/drawdown replay that gates
    the LIVE dd_halt + brake.

    capital.py builds each wallet's peak equity by replaying its settled rows in
    equity-change order (``_equity_change_sort_key``: exit_ts for stop rows, else
    resolution_ts). Realized capital is order-independent but the peak is not.
    Scenario: an early WIN at T1 (+30, resolution_ts=T1) then a later booked-once
    stop-loss CANCELLED loss at T2>T1 (−8, exit_ts=T2, resolution_ts NULL).

    The CORRECT chronological replay keys the stop row on its immutable exit_ts
    (T2, after the win):
        100 -> 130 (win +30 at T1, PEAK) -> 122 (stop −8 at T2);
        peak = 130, drawdown_pct = 8/130 ≈ 6.15%, drawdown_ratio = 8/100 = 0.08.
    This is invariant to the backfill because exit_ts never changes and
    resolution_ts is never read for a stop row — so the peak/drawdown are
    byte-identical before and after the sweep. (The old resolution_ts key made
    the stop sort FIRST on its NULL→'' key, hiding the real drawdown as 0%; the
    exit_ts key both fixes that AND removes the backfill's power to move it.)
    """
    from cryptobot.execution.capital import get_wallet_snapshots
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests._legacy_artifact import legacy_portfolio

    # Early WIN on v6_5m at T1: +30, resolution_ts set.
    win_id = record_bet(db, make_signal(side="YES"), mode="LIVE")
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=30.0, won_up=1, "
        "resolution_ts='2026-01-01 00:00:00' WHERE id=?",
        (win_id,),
    )
    # Later booked-once stop loss on the SAME wallet at T2>T1: −8, exit_ts set
    # by _make_stop_row to 2026-07-11 (> the win's 2026-01-01), resolution_ts
    # NULL. Distinct window_start_ts so it is a separate market row.
    stop_id = _make_stop_row(
        db, side="YES", pnl_net=-8.0, window_start_ts=1_715_923_500
    )
    _expire(db, window_end_ts=2_000_000_000)

    before = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]
    # Correct chronological replay: win (T1) then stop (T2) → peak 130, dd 8/130.
    assert before.realized_capital == pytest.approx(122.0)
    assert before.peak_realized_capital == pytest.approx(130.0)
    assert before.drawdown_pct_peak_frac == pytest.approx(8.0 / 130.0)
    assert before.drawdown_ratio == pytest.approx(0.08)

    counters = run_resolution_sweep(
        db, now_ts=2_000_000_100, polymarket_resolution_fetch=_resolved(1),
    )
    assert counters["stop_resolutions_backfilled"] == 1

    # The stop row's outcome IS recorded for display; resolution_ts stays NULL
    # (defense in depth — the fix does not rely on this, see the companion test).
    stop_row = db.execute(
        "SELECT won_up, resolution_ts FROM ledger WHERE id=?", (stop_id,)
    ).fetchone()
    assert stop_row["won_up"] == 1
    assert stop_row["resolution_ts"] is None

    after = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]
    # The DD replay is untouched — identical peak/drawdown before and after.
    assert after.realized_capital == pytest.approx(before.realized_capital)
    assert after.peak_realized_capital == pytest.approx(130.0)
    assert after.drawdown_pct_peak_frac == pytest.approx(8.0 / 130.0)
    assert after.drawdown_ratio == pytest.approx(0.08)


def test_stop_row_dd_invariant_to_backfilled_resolution_ts(
    db: sqlite3.Connection,
):
    """The exit_ts replay key yields ONE deterministic ordering for stop rows
    WITH and WITHOUT a backfilled resolution_ts.

    The live box carries a handful of stop rows that were stamped with a
    resolution_ts by an older backfill path, alongside fresh rows left NULL.
    Merely "stop writing resolution_ts" cannot neutralize the already-stamped
    rows; keying the replay on the immutable exit_ts does — a stop row's peak
    contribution is identical whether resolution_ts is NULL or a late sweep
    clock. This is the reason the fix lives in capital.py, not (only) the sweep.
    """
    from cryptobot.execution.capital import (
        get_capital_snapshot,
        get_wallet_snapshots,
    )
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests._legacy_artifact import legacy_portfolio

    win_id = record_bet(db, make_signal(side="YES"), mode="LIVE")
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=30.0, won_up=1, "
        "resolution_ts='2026-01-01 00:00:00' WHERE id=?",
        (win_id,),
    )
    stop_id = _make_stop_row(
        db, side="YES", pnl_net=-8.0, window_start_ts=1_715_923_500
    )
    _expire(db, window_end_ts=2_000_000_000)

    # NULL-resolution_ts state (fresh stop row).
    wallet_null = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]
    cap_null = get_capital_snapshot(db)

    # Simulate an already-backfilled stop row: stamp a LATE sweep clock onto
    # resolution_ts (a value that, under the OLD resolution_ts key, would have
    # re-sorted the stop LAST and changed the peak). The exit_ts key ignores it.
    db.execute(
        "UPDATE ledger SET resolution_ts='2033-01-01 00:00:00' WHERE id=?",
        (stop_id,),
    )
    wallet_backfilled = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]
    cap_backfilled = get_capital_snapshot(db)

    # Per-wallet replay: identical peak/drawdown regardless of resolution_ts.
    assert wallet_backfilled.peak_realized_capital == pytest.approx(
        wallet_null.peak_realized_capital
    )
    assert wallet_null.peak_realized_capital == pytest.approx(130.0)
    assert wallet_backfilled.drawdown_pct_peak_frac == pytest.approx(
        wallet_null.drawdown_pct_peak_frac
    )
    # Global capital snapshot (SQL replay key) agrees with the Python key.
    assert cap_backfilled.peak_realized_capital == pytest.approx(
        cap_null.peak_realized_capital
    )
    assert cap_null.peak_realized_capital == pytest.approx(130.0)
    assert cap_backfilled.drawdown_pct_peak_frac == pytest.approx(
        cap_null.drawdown_pct_peak_frac
    )
