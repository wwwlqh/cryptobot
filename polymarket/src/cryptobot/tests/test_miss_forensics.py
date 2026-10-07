"""On-chain miss forensics (scheduler/miss_forensics.py + its dashboard read).

Operator order 2026-07-30: every missed trade must be classified from CHAIN
evidence — a faster TAKER took the ask, or the MAKER pulled it — because the
~1s post-reject book re-read cannot tell those apart ("the ask is gone" reads
identically either way), which is how the "68% maker-cancel" inference got made
and then refuted.

These tests pin the parts that can be wrong SILENTLY — a wrong verdict looks
exactly like a right one on the dashboard:

  * MINT-MATCH — an ask on token T is consumed by a taker BUY of T at p OR a
    taker SELL of the complement at 1-p. Testing one shape halves the evidence
    and manufactures MAKER_CANCELs.
  * SETTLEMENT LAG — block timestamps run ~+3s after the venue match. Skipping
    the correction reads every rival print as LATE, which INVERTS the verdict
    (test_lag_correction_flips_the_verdict pins exactly that).
  * VERDICT RULES + the at-our-level tolerance.
  * RIVAL MAKER — one match emits several OrderFilled logs including the
    taker's OWN order leg; picking the wrong one reports ourselves/the taker as
    the rival.
  * POPULATION — no_liquidity only, age-bounded, top-ups excluded, PENDING
    retried but terminal verdicts not re-walked.
  * GATING — flag off / non-Polymarket venue must register no job at all.
  * The dashboard's SPEED PRIZE arithmetic (would-win WR vs break-even WR),
    the number the whole panel exists to keep re-checking.

Everything runs with a stubbed transport; no test touches the network.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from cryptobot.scheduler import miss_forensics as mf


# =========================================================================
# MINT-MATCH: both shapes of consuming the same resting ask
# =========================================================================

TOK, COMP = "111", "222"


def _norm(asset, side, price, complement=COMP):
    return mf.normalize_print(
        asset, side, price, token_id=TOK, complement_id=complement
    )


class TestMintMatch:
    def test_taker_buy_of_our_token_counts_at_face_price(self):
        assert _norm(TOK, "BUY", 0.62) == (0.62, "buyT")

    def test_taker_sell_of_complement_counts_at_one_minus_price(self):
        """The load-bearing half: selling the complement at 0.38 IS buying our
        token at 0.62 — same resting ask, other instrument."""
        got = _norm(COMP, "SELL", 0.38)
        assert got is not None
        price, kind = got
        assert kind == "sellC"
        assert price == pytest.approx(0.62)

    def test_buy_of_complement_is_not_our_ask(self):
        assert _norm(COMP, "BUY", 0.38) is None

    def test_sell_of_our_token_is_not_our_ask(self):
        assert _norm(TOK, "SELL", 0.62) is None

    def test_unrelated_token_ignored(self):
        assert _norm("999", "BUY", 0.62) is None

    def test_lowercase_side_still_matches(self):
        assert _norm(TOK, "buy", 0.5) == (0.5, "buyT")

    def test_without_a_known_complement_only_our_own_token_matches(self):
        assert _norm(TOK, "BUY", 0.5, complement=None) == (0.5, "buyT")
        assert _norm(COMP, "SELL", 0.5, complement=None) is None

    def test_unparseable_price_is_dropped_not_raised(self):
        assert _norm(TOK, "BUY", "n/a") is None
        assert _norm(TOK, "BUY", None) is None

    def test_integer_and_string_token_ids_compare_equal(self):
        """live_order_attempts stores the token as TEXT, data-api may hand back
        either — a type mismatch here would silently match nothing."""
        assert mf.normalize_print(
            111, "BUY", 0.5, token_id="111", complement_id=None
        ) == (0.5, "buyT")


# =========================================================================
# SETTLEMENT-LAG CORRECTION
# =========================================================================


class TestLagCorrection:
    def test_block_stamp_maps_back_onto_our_clock(self):
        # A print whose block landed 3s after our reject actually matched at
        # the same moment we arrived.
        assert mf.venue_match_dt(1_003, 1_000, 3) == pytest.approx(0.0)

    def test_a_print_that_beat_us_reads_negative(self):
        assert mf.venue_match_dt(1_001, 1_000, 3) == pytest.approx(-2.0)

    def test_zero_lag_leaves_the_raw_delta(self):
        assert mf.venue_match_dt(1_004, 1_000, 0) == pytest.approx(4.0)

    def test_window_bounds_are_inclusive(self):
        assert mf.in_window(mf.WINDOW_LO_S) is True
        assert mf.in_window(mf.WINDOW_HI_S) is True
        assert mf.in_window(mf.WINDOW_LO_S - 0.01) is False
        assert mf.in_window(mf.WINDOW_HI_S + 0.01) is False

    def test_lag_correction_flips_the_verdict(self):
        """THE regression this constant exists for.

        A rival print on the block 2s after our reject matched ~1s BEFORE we
        arrived. With the lag applied it beats us at our level (FASTER_TAKER);
        with lag=0 the same tape reads as a print 2s LATE and 0.02 above our
        limit, i.e. REPRICED_ONLY — the exact inversion that produced the
        original wrong reading of these misses.
        """
        trades = [
            # our level, matched just before us once the lag is removed
            {"asset": TOK, "side": "BUY", "price": 0.60, "size": 5,
             "timestamp": 1_002},
            # a genuinely-above-our-limit print, in window under either lag
            {"asset": TOK, "side": "BUY", "price": 0.65, "size": 5,
             "timestamp": 1_004},
        ]
        corrected = mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP,
            reject_ts=1_000, lag_s=3,
        )
        verdict, beat = mf.classify_prints(corrected, 0.60)
        assert verdict == mf.VERDICT_FASTER_TAKER
        assert beat is not None and beat.dt_s < 0

        naive = mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP,
            reject_ts=1_000, lag_s=0,
        )
        # Same tape, no correction: our level now looks like it never traded
        # in time — the print reads as arriving after us.
        assert all(p.dt_s > 0 for p in naive)


class TestCalibrateLag:
    def test_too_few_samples_keeps_the_established_constant(self):
        assert mf.calibrate_lag_s([3, 3, 3]) is None

    def test_median_of_enough_samples(self):
        assert mf.calibrate_lag_s([2, 3, 3, 3, 4, 4]) == 3

    def test_negative_lag_is_impossible_and_clamped(self):
        """A block cannot predate the match it records."""
        assert mf.calibrate_lag_s([-9, -8, -7, -6, -5]) == 0

    def test_absurd_lag_is_clamped_not_trusted(self):
        assert mf.calibrate_lag_s([40, 41, 42, 43, 44]) == mf._LAG_MAX_S

    def test_garbage_samples_are_skipped(self):
        assert mf.calibrate_lag_s(["x", None, 3, 3, 3, 3, 3]) == 3

    def test_all_garbage_is_too_small_a_sample(self):
        assert mf.calibrate_lag_s(["x", None, "y"]) is None


# =========================================================================
# VERDICT RULES
# =========================================================================


def _p(dt, price, size=1.0, kind="buyT", tx=None, taker=None):
    return mf.ChainPrint(dt_s=dt, price=price, size=size, kind=kind,
                         tx_hash=tx, taker=taker)


class TestVerdicts:
    def test_no_print_at_all_is_a_maker_cancel(self):
        assert mf.classify_prints([], 0.60)[0] == mf.VERDICT_MAKER_CANCEL

    def test_only_prints_above_our_limit_is_repriced_only(self):
        verdict, beat = mf.classify_prints(
            [_p(-1.0, 0.63), _p(2.0, 0.70)], 0.60
        )
        assert verdict == mf.VERDICT_REPRICED_ONLY
        assert beat is None

    def test_a_print_at_our_limit_is_a_faster_taker(self):
        verdict, beat = mf.classify_prints([_p(-1.0, 0.60)], 0.60)
        assert verdict == mf.VERDICT_FASTER_TAKER
        assert beat is not None and beat.price == 0.60

    def test_a_print_far_below_our_ask_is_a_different_level_not_our_loss(self):
        """REGRESSION (found by the live smoke run): the first port tested only
        `price <= limit + tol`, so a print deep on the other side of the book —
        0.056 against a 0.810 limit — was filed as a faster taker. That single
        missing lower bound moved the live distribution to 91.7% FASTER_TAKER /
        0 MAKER_CANCEL from the hand-verified 84% / 2%."""
        verdict, beat = mf.classify_prints([_p(-1.0, 0.056)], 0.81, 0.80)
        assert verdict == mf.VERDICT_REPRICED_ONLY
        assert beat is None

    def test_a_print_just_inside_the_ask_side_of_the_band_still_convicts(self):
        assert mf.classify_prints([_p(-1.0, 0.796)], 0.81, 0.80)[0] == (
            mf.VERDICT_FASTER_TAKER
        )

    def test_the_band_runs_from_the_ask_we_saw_to_our_limit(self):
        lo, hi = mf.level_band(0.81, 0.80)
        assert lo == pytest.approx(0.795)
        assert hi == pytest.approx(0.815)

    def test_a_missing_quote_ask_falls_back_to_a_tick_under_the_limit(self):
        lo, hi = mf.level_band(0.60, None)
        assert lo == pytest.approx(0.585)
        assert hi == pytest.approx(0.605)

    def test_an_ask_above_its_own_limit_cannot_invert_the_band(self):
        lo, hi = mf.level_band(0.60, 0.70)
        assert lo < hi

    def test_a_whole_dollar_of_prints_below_the_band_is_still_repriced(self):
        prints = [_p(float(i) - 4.0, 0.05 * i) for i in range(1, 12)]
        assert mf.classify_prints(prints, 0.81, 0.80)[0] == (
            mf.VERDICT_REPRICED_ONLY
        )

    def test_tolerance_admits_a_half_cent_above(self):
        assert mf.classify_prints([_p(0.0, 0.604)], 0.60)[0] == (
            mf.VERDICT_FASTER_TAKER
        )

    def test_beyond_tolerance_is_repriced(self):
        assert mf.classify_prints([_p(0.0, 0.606)], 0.60)[0] == (
            mf.VERDICT_REPRICED_ONLY
        )

    def test_earliest_at_level_print_is_reported_not_the_first_listed(self):
        verdict, beat = mf.classify_prints(
            [_p(4.0, 0.58), _p(-2.0, 0.59), _p(1.0, 0.60)], 0.60
        )
        assert verdict == mf.VERDICT_FASTER_TAKER
        assert beat is not None and beat.dt_s == -2.0

    def test_missing_limit_price_is_unresolved_not_a_guess(self):
        verdict, beat = mf.classify_prints([_p(0.0, 0.60)], None)
        assert verdict == mf.VERDICT_UNRESOLVED
        assert beat is None

    def test_unresolved_is_terminal_and_pending_is_not(self):
        """A terminal verdict must not be re-walked forever; PENDING must."""
        assert mf.VERDICT_PENDING in mf.RETRYABLE_VERDICTS
        assert mf.VERDICT_UNRESOLVED not in mf.RETRYABLE_VERDICTS
        assert mf.VERDICT_MAKER_CANCEL not in mf.RETRYABLE_VERDICTS


class TestCollectPrints:
    def test_our_own_fill_is_never_counted_as_a_rival(self):
        """Under multifill a sibling attempt on the same window can genuinely
        fill; counting it would invent a FASTER_TAKER out of our own success."""
        trades = [{"asset": TOK, "side": "BUY", "price": 0.60, "size": 3,
                   "timestamp": 1_003, "proxyWallet": "0xUS"}]
        assert mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP, reject_ts=1_000,
            lag_s=3, funder="0xus",
        ) == []

    def test_prints_outside_the_window_are_dropped(self):
        trades = [
            {"asset": TOK, "side": "BUY", "price": 0.6, "size": 1,
             "timestamp": 1_000 + 3 - 60},      # dt = -60
            {"asset": TOK, "side": "BUY", "price": 0.6, "size": 1,
             "timestamp": 1_000 + 3 + 60},      # dt = +60
        ]
        assert mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP, reject_ts=1_000, lag_s=3,
        ) == []

    def test_both_shapes_are_collected_and_sorted_by_dt(self):
        trades = [
            {"asset": TOK, "side": "BUY", "price": 0.60, "size": 2,
             "timestamp": 1_006},
            {"asset": COMP, "side": "SELL", "price": 0.40, "size": 7,
             "timestamp": 1_002},
        ]
        prints = mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP, reject_ts=1_000, lag_s=3,
        )
        assert [p.kind for p in prints] == ["sellC", "buyT"]
        assert prints[0].dt_s < prints[1].dt_s
        assert prints[0].price == pytest.approx(0.60)

    def test_malformed_rows_are_skipped_not_fatal(self):
        trades = [
            None, "junk", {},
            {"asset": TOK, "side": "BUY", "price": 0.6},          # no timestamp
            {"asset": TOK, "side": "BUY", "price": 0.6, "size": 1,
             "timestamp": 1_003},
        ]
        prints = mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP, reject_ts=1_000, lag_s=3,
        )
        assert len(prints) == 1

    def test_unparseable_size_defaults_to_zero_and_keeps_the_verdict(self):
        trades = [{"asset": TOK, "side": "BUY", "price": 0.6, "size": "x",
                   "timestamp": 1_003}]
        prints = mf.collect_prints(
            trades, token_id=TOK, complement_id=COMP, reject_ts=1_000, lag_s=3,
        )
        assert len(prints) == 1 and prints[0].size == 0.0


# =========================================================================
# RIVAL MAKER off the OrderFilled logs
# =========================================================================

EXCH = "0xe111180000d2663c0091e4f400237545b87b996b"
TOPIC0 = "0xd543adfd945773f1" + "0" * 48
TAKER = "0x" + "aa" * 20
RIVAL = "0x" + "bb" * 20


def _topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:]


def _log(address=EXCH, topic0=TOPIC0, maker=RIVAL, taker=TAKER, words=5):
    return {
        "address": address,
        "topics": [topic0, "0x" + "11" * 32, _topic(maker), _topic(taker)],
        "data": "0x" + "0" * (64 * words),
    }


class TestRivalMaker:
    def test_address_extracted_from_the_indexed_topic(self):
        assert mf.addr_from_topic(_topic(RIVAL)) == RIVAL

    def test_short_or_missing_topic_yields_none(self):
        assert mf.addr_from_topic("0x1234") is None
        assert mf.addr_from_topic(None) is None
        assert mf.addr_from_topic("not-a-topic") is None

    def test_the_takers_own_order_leg_is_skipped(self):
        """Every order has an owner from the exchange's view, so one match
        emits an OrderFilled whose `maker` IS the taker. Reporting that as the
        rival would name the taker as the resting-order owner."""
        logs = [_log(maker=TAKER), _log(maker=RIVAL)]
        assert mf.rival_maker_from_logs(
            logs, exchange=EXCH, topic0=TOPIC0, taker_addr=TAKER
        ) == RIVAL

    def test_logs_from_other_contracts_are_ignored(self):
        logs = [_log(address="0x" + "cc" * 20, maker=RIVAL)]
        assert mf.rival_maker_from_logs(
            logs, exchange=EXCH, topic0=TOPIC0, taker_addr=TAKER
        ) is None

    def test_other_events_on_the_exchange_are_ignored(self):
        logs = [_log(topic0="0x" + "ee" * 32, maker=RIVAL)]
        assert mf.rival_maker_from_logs(
            logs, exchange=EXCH, topic0=TOPIC0, taker_addr=TAKER
        ) is None

    def test_no_matching_log_stores_null_rather_than_guessing(self):
        assert mf.rival_maker_from_logs(
            [], exchange=EXCH, topic0=TOPIC0, taker_addr=TAKER
        ) is None

    def test_case_insensitive_on_address_and_topic(self):
        logs = [_log(address=EXCH.upper(), topic0=TOPIC0.upper())]
        assert mf.rival_maker_from_logs(
            logs, exchange=EXCH, topic0=TOPIC0, taker_addr=TAKER
        ) == RIVAL

    def test_missing_signature_refuses_to_scan(self):
        assert mf.rival_maker_from_logs(
            [_log()], exchange="", topic0=TOPIC0, taker_addr=TAKER
        ) is None


class TestChainSignature:
    def test_most_frequent_orderfilled_shape_wins(self):
        receipts = [
            {"logs": [_log(), _log(maker=TAKER)]},
            {"logs": [_log()]},
            {"logs": [{"address": "0x" + "dd" * 20,
                       "topics": [TOPIC0, "0x1", "0x2", "0x3"],
                       "data": "0x" + "0" * 320}]},
        ]
        assert mf.derive_chain_signature(receipts) == (EXCH, TOPIC0)

    def test_transfer_shaped_logs_are_not_mistaken_for_orderfilled(self):
        """ERC1155/ERC20 transfers ride in the same receipt; OrderFilled is
        pinned by 4 topics AND 5 data words."""
        transfer = {"address": "0x" + "ff" * 20,
                    "topics": ["0xddf252ad", "0x1", "0x2"],
                    "data": "0x" + "0" * 64}
        assert mf.derive_chain_signature([{"logs": [transfer]}]) is None

    def test_short_data_payload_rejected(self):
        assert mf.derive_chain_signature([{"logs": [_log(words=2)]}]) is None

    def test_no_receipts_yields_none_so_the_caller_skips_rival_lookup(self):
        assert mf.derive_chain_signature([]) is None
        assert mf.derive_chain_signature([None, {}, {"logs": None}]) is None


# =========================================================================
# TOKEN PAIR resolution (CLOB, because Gamma has purged these markets)
# =========================================================================


class TestTokenPair:
    def test_pair_read_from_the_clob_payload(self):
        market = {"tokens": [{"outcome": "Up", "token_id": TOK},
                             {"outcome": "Down", "token_id": COMP}]}
        assert mf.token_pair_from_market(market) == (TOK, COMP)

    def test_a_one_legged_market_is_not_a_pair(self):
        market = {"tokens": [{"outcome": "Up", "token_id": TOK}]}
        assert mf.token_pair_from_market(market) is None

    def test_garbage_payload_yields_none(self):
        assert mf.token_pair_from_market(None) is None
        assert mf.token_pair_from_market({"tokens": ["x", None]}) is None
        assert mf.token_pair_from_market({}) is None

    def test_complement_is_the_other_leg(self):
        assert mf.complement_of(TOK, (TOK, COMP)) == COMP
        assert mf.complement_of(COMP, (TOK, COMP)) == TOK

    def test_token_outside_the_pair_has_no_complement(self):
        assert mf.complement_of("999", (TOK, COMP)) is None


# =========================================================================
# GATING
# =========================================================================


class TestGating:
    def test_config_default_is_on(self):
        from cryptobot import config
        assert config.MISS_FORENSICS_ENABLED is True
        assert config.MISS_FORENSICS_SECONDS == 300


class TestSchedulerRegistration:
    """The job reaches the scheduler only through start_scheduler's params
    (the clob_ping precedent), so main.py's gate is the ONLY switch."""

    def test_registered_when_main_supplies_the_job(self, start_scheduler_jobs):
        from apscheduler.triggers.interval import IntervalTrigger

        jobs = start_scheduler_jobs(
            tick_seconds=3600,
            miss_forensics_job=lambda: None, miss_forensics_seconds=300,
        )
        job = jobs.get("miss_forensics")
        assert job is not None
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval.total_seconds() == 300

    def test_absent_when_main_withholds_the_job(self, start_scheduler_jobs):
        assert "miss_forensics" not in start_scheduler_jobs(tick_seconds=3600)

    def test_zero_cadence_disables_just_this_job(self, start_scheduler_jobs):
        assert "miss_forensics" not in start_scheduler_jobs(
            tick_seconds=3600,
            miss_forensics_job=lambda: None, miss_forensics_seconds=0,
        )


# =========================================================================
# SCHEMA / MIGRATION
# =========================================================================

_MF_COLS = {
    "id", "fill_id", "verdict", "beat_dt_s", "beat_price", "beat_size",
    "rival_addr", "lag_s_used", "checked_at", "detail_json",
}


class TestSchema:
    def test_fresh_db_has_the_table(self, db: sqlite3.Connection):
        cols = {r[1] for r in db.execute("PRAGMA table_info(miss_forensics)")}
        assert cols == _MF_COLS

    def test_fill_id_is_unique(self, db: sqlite3.Connection):
        db.execute(
            "INSERT INTO miss_forensics (fill_id, verdict) VALUES (1, 'PENDING')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO miss_forensics (fill_id, verdict) "
                "VALUES (1, 'MAKER_CANCEL')"
            )

    def test_existing_db_without_the_table_gains_it(self, tmp_path):
        """The live box's DB predates this table — init_db must add it in place
        without touching the rows already there."""
        from cryptobot.db.connection import init_db

        path = tmp_path / "legacy.db"
        first = init_db(path)
        first.execute("DROP TABLE miss_forensics")
        first.execute(
            "INSERT INTO subminute_fills (coin, sleeve, offset_sec, "
            "window_start_ts, side, requested_usd, outcome) "
            "VALUES ('BTC', 'subminute_5m', 270, 100, 'UP', 5.0, 'rejected')"
        )
        first.commit()
        first.close()

        second = init_db(path)
        try:
            cols = {r[1] for r in
                    second.execute("PRAGMA table_info(miss_forensics)")}
            assert cols == _MF_COLS
            assert second.execute(
                "SELECT COUNT(*) FROM subminute_fills"
            ).fetchone()[0] == 1
        finally:
            second.close()

    def test_schema_sql_and_connection_ddl_stay_in_lockstep(self):
        """Two places create this table (fresh install reads schema.sql, an
        existing DB is migrated by connection.py). A column added to one only
        is a live-vs-test divergence."""
        from cryptobot.db.connection import _SCHEMA_PATH
        import cryptobot.db.connection as dbconn
        import inspect

        schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
        conn_src = inspect.getsource(dbconn._ensure_per_strategy_live_tables)
        for text in (schema_sql, conn_src):
            assert "CREATE TABLE IF NOT EXISTS miss_forensics" in text
            for col in _MF_COLS - {"id"}:
                assert col in text


# =========================================================================
# POPULATION selection
# =========================================================================

NOW = 1_800_000_000.0


def _ts(offset_s: float) -> str:
    import time
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(NOW + offset_s))


def _seed_reject(
    conn: sqlite3.Connection, *, coid="c1", age_s=600.0, error_kind="no_liquidity",
    token="111", market="0xcond", outcome="rejected", coin="BTC",
    sleeve="subminute_5m", ws=100, side="UP", limit_price=0.60, offset_sec=270,
) -> int:
    conn.execute(
        "INSERT INTO subminute_fills (created_at, coin, sleeve, offset_sec, "
        "window_start_ts, side, requested_usd, quote_ask, limit_price, "
        "client_order_id, outcome) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (_ts(-age_s), coin, sleeve, offset_sec, ws, side, 5.0, 0.59,
         limit_price, coid, outcome),
    )
    fill_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        "INSERT INTO live_order_attempts (wallet_id, coin, sleeve, "
        "entry_minute, side, window_start_ts, client_order_id, poly_market_id, "
        "poly_token_id, amount_usd, status, error_kind) "
        "VALUES ('w1',?,?,?,?,?,?,?,?,5.0,'FAILED',?)",
        (coin, sleeve, 4, side, ws, coid, market, token, error_kind),
    )
    conn.commit()
    return fill_id


def _ids(conn):
    return [int(r["id"]) for r in
            mf.select_pending_rejects(conn, now_ts=NOW)]


class TestPopulation:
    def test_a_plain_no_liquidity_reject_is_selected(self, db):
        fid = _seed_reject(db)
        assert _ids(db) == [fid]

    def test_the_selected_row_carries_the_token_we_actually_sent(self, db):
        _seed_reject(db, token="tok-9", market="cond-9")
        row = mf.select_pending_rejects(db, now_ts=NOW)[0]
        assert row["poly_token_id"] == "tok-9"
        assert row["poly_market_id"] == "cond-9"
        assert row["reject_ts"] == pytest.approx(NOW - 600, abs=1.5)

    def test_a_reject_younger_than_the_settlement_lag_waits(self, db):
        """Classifying a 5s-old reject would read an empty tape as a maker
        cancel — the chain has not settled yet."""
        _seed_reject(db, age_s=5.0)
        assert _ids(db) == []

    def test_a_reject_older_than_the_backfill_horizon_is_dropped(self, db):
        _seed_reject(db, age_s=mf.MAX_AGE_S + 3600)
        assert _ids(db) == []

    def test_infra_rejects_carry_no_book_event_to_explain(self, db):
        _seed_reject(db, error_kind="insufficient_balance")
        assert _ids(db) == []

    def test_accepted_fills_are_not_misses(self, db):
        _seed_reject(db, outcome="accepted")
        assert _ids(db) == []

    def test_a_reject_without_a_token_cannot_be_classified(self, db):
        _seed_reject(db, token="")
        assert _ids(db) == []

    def test_a_topup_reject_is_not_a_missed_trade(self, db):
        """A rejected extra on a slot that ALREADY filled is not a miss."""
        db.execute(
            "INSERT INTO subminute_fills (created_at, coin, sleeve, offset_sec, "
            "window_start_ts, side, requested_usd, outcome) "
            "VALUES (?,'BTC','subminute_5m',240,100,'UP',5.0,'accepted')",
            (_ts(-900),),
        )
        db.commit()
        _seed_reject(db, age_s=600.0)
        assert _ids(db) == []

    def test_a_later_fill_on_the_same_slot_does_not_excuse_the_earlier_miss(
        self, db
    ):
        fid = _seed_reject(db, age_s=600.0)
        db.execute(
            "INSERT INTO subminute_fills (created_at, coin, sleeve, offset_sec, "
            "window_start_ts, side, requested_usd, outcome) "
            "VALUES (?,'BTC','subminute_5m',280,100,'UP',5.0,'accepted')",
            (_ts(-300),),
        )
        db.commit()
        assert _ids(db) == [fid]

    def test_a_settled_verdict_is_not_re_walked(self, db):
        fid = _seed_reject(db)
        db.execute(
            "INSERT INTO miss_forensics (fill_id, verdict) VALUES (?, ?)",
            (fid, mf.VERDICT_FASTER_TAKER),
        )
        db.commit()
        assert _ids(db) == []

    def test_a_pending_verdict_comes_back(self, db):
        fid = _seed_reject(db)
        db.execute(
            "INSERT INTO miss_forensics (fill_id, verdict) VALUES (?, ?)",
            (fid, mf.VERDICT_PENDING),
        )
        db.commit()
        assert _ids(db) == [fid]

    def test_a_terminal_unresolved_is_never_retried(self, db):
        fid = _seed_reject(db)
        db.execute(
            "INSERT INTO miss_forensics (fill_id, verdict) VALUES (?, ?)",
            (fid, mf.VERDICT_UNRESOLVED),
        )
        db.commit()
        assert _ids(db) == []

    def test_oldest_first_and_bounded(self, db):
        first = _seed_reject(db, coid="a", age_s=3000.0)
        second = _seed_reject(db, coid="b", age_s=2000.0)
        assert _ids(db) == [first, second]
        assert [int(r["id"]) for r in mf.select_pending_rejects(
            db, now_ts=NOW, max_rows=1)] == [first]

    def test_a_legacy_db_without_the_tables_degrades_to_empty(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "bare.db")
        conn.row_factory = sqlite3.Row
        try:
            assert mf.select_pending_rejects(conn, now_ts=NOW) == []
        finally:
            conn.close()


# =========================================================================
# THE JOB, end to end with a stubbed transport
# =========================================================================

COND = "0xcond"
UP_TOK, DOWN_TOK = "111", "222"


class _Transport:
    """Stub for the injected fetch_json — records calls, no network."""

    def __init__(self, *, trades=None, own_trades=(), market="pair",
                 receipt=None, fail=None):
        self.trades = trades if trades is not None else []
        self.own_trades = list(own_trades)
        self.market = market
        self.receipt = receipt
        self.fail = fail
        self.calls: list[str] = []

    def __call__(self, url, payload=None):
        self.calls.append(url if payload is None else f"RPC {payload['method']}")
        if self.fail:
            raise RuntimeError(self.fail)
        if payload is not None:
            return {"jsonrpc": "2.0", "id": payload["id"], "result": self.receipt}
        if "/markets/" in url:
            if self.market == "missing":
                return {"tokens": [{"outcome": "Up", "token_id": UP_TOK}]}
            return {"tokens": [{"outcome": "Up", "token_id": UP_TOK},
                               {"outcome": "Down", "token_id": DOWN_TOK}]}
        if "user=" in url:
            return self.own_trades
        return self.trades


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """The lag/signature/pair caches live for the process; a leaked value would
    make one test's calibration decide another's verdict."""
    mf._LAG_STATE.clear()
    mf._CHAIN_SIG.clear()
    mf._PAIR_CACHE.clear()
    yield
    mf._LAG_STATE.clear()
    mf._CHAIN_SIG.clear()
    mf._PAIR_CACHE.clear()


def _verdict_row(conn, fill_id):
    return conn.execute(
        "SELECT * FROM miss_forensics WHERE fill_id = ?", (fill_id,)
    ).fetchone()


def _run(conn, transport, **kw):
    kw.setdefault("funder", "0xus")
    kw.setdefault("now_ts", NOW)
    kw.setdefault("sleep", lambda _s: None)
    return mf.run_miss_forensics(conn, fetch_json=transport, **kw)


class TestJob:
    def test_a_rival_print_at_our_level_is_stored_as_faster_taker(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        reject_ts = NOW - 600
        transport = _Transport(trades=[
            {"asset": UP_TOK, "side": "BUY", "price": 0.60, "size": 21,
             "timestamp": reject_ts + 2, "proxyWallet": "0xrival",
             "transactionHash": "0xdead"},
        ])
        counters = _run(db, transport)
        assert counters["classified"] == 1
        row = _verdict_row(db, fid)
        assert row["verdict"] == mf.VERDICT_FASTER_TAKER
        assert row["beat_dt_s"] == pytest.approx(-1.0)
        assert row["beat_price"] == pytest.approx(0.60)
        assert row["beat_size"] == pytest.approx(21.0)
        assert row["lag_s_used"] == pytest.approx(mf.DEFAULT_LAG_S)
        assert json.loads(row["detail_json"])["token_id"] == UP_TOK

    def test_a_complement_sell_also_convicts(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        transport = _Transport(trades=[
            {"asset": DOWN_TOK, "side": "SELL", "price": 0.40, "size": 8,
             "timestamp": NOW - 600 + 2, "proxyWallet": "0xrival"},
        ])
        _run(db, transport)
        row = _verdict_row(db, fid)
        assert row["verdict"] == mf.VERDICT_FASTER_TAKER
        assert json.loads(row["detail_json"])["beat_kind"] == "sellC"

    def test_an_empty_window_is_stored_as_maker_cancel(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        _run(db, _Transport(trades=[]))
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_MAKER_CANCEL

    def test_prints_above_our_limit_are_stored_as_repriced_only(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        transport = _Transport(trades=[
            {"asset": UP_TOK, "side": "BUY", "price": 0.72, "size": 4,
             "timestamp": NOW - 600 + 2, "proxyWallet": "0xrival"},
        ])
        _run(db, transport)
        row = _verdict_row(db, fid)
        assert row["verdict"] == mf.VERDICT_REPRICED_ONLY
        assert row["beat_dt_s"] is None

    def test_a_market_the_clob_no_longer_pairs_is_terminal(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        counters = _run(db, _Transport(market="missing"))
        assert counters["unresolved"] == 1
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_UNRESOLVED

    def test_a_transport_failure_leaves_the_row_pending(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        counters = _run(db, _Transport(fail="boom"))
        assert counters["pending"] == 1
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_PENDING

    def test_no_funder_refuses_to_classify(self, db):
        """Without our own address we cannot exclude our OWN prints, and a
        sibling fill would be booked as a rival beating us."""
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        counters = _run(db, _Transport(trades=[]), funder="")
        assert counters["classified"] == 0
        assert _verdict_row(db, fid) is None

    def test_exhausted_budget_stops_and_leaves_rows_pending(self, db):
        _seed_reject(db, coid="a", token=UP_TOK, market=COND, age_s=900)
        _seed_reject(db, coid="b", token=UP_TOK, market="0xcond2", age_s=800,
                     ws=200)
        counters = _run(db, _Transport(trades=[]), http_budget=1)
        assert counters["pending"] >= 1
        assert counters["classified"] == 0
        assert db.execute(
            "SELECT COUNT(*) FROM miss_forensics WHERE verdict = ?",
            (mf.VERDICT_PENDING,),
        ).fetchone()[0] >= 1

    def test_reclassification_overwrites_in_place(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        _run(db, _Transport(fail="down"))
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_PENDING
        mf._PAIR_CACHE.clear()
        _run(db, _Transport(trades=[
            {"asset": UP_TOK, "side": "BUY", "price": 0.60, "size": 3,
             "timestamp": NOW - 600 + 2, "proxyWallet": "0xrival"},
        ]))
        assert db.execute(
            "SELECT COUNT(*) FROM miss_forensics WHERE fill_id = ?", (fid,)
        ).fetchone()[0] == 1
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_FASTER_TAKER

    def test_our_own_print_does_not_convict_a_rival(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        transport = _Transport(trades=[
            {"asset": UP_TOK, "side": "BUY", "price": 0.60, "size": 3,
             "timestamp": NOW - 600 + 2, "proxyWallet": "0xUS"},
        ])
        _run(db, transport)
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_MAKER_CANCEL

    def test_rival_maker_resolved_when_an_rpc_url_is_configured(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        transport = _Transport(
            trades=[{"asset": UP_TOK, "side": "BUY", "price": 0.60, "size": 9,
                     "timestamp": NOW - 600 + 2, "proxyWallet": TAKER,
                     "transactionHash": "0xbeef"}],
            own_trades=[{"asset": UP_TOK, "timestamp": NOW - 5000,
                         "transactionHash": "0xown"}],
            receipt={"logs": [_log(maker=TAKER), _log(maker=RIVAL)]},
        )
        _run(db, transport, rpc_url="https://rpc.example")
        assert _verdict_row(db, fid)["rival_addr"] == RIVAL

    def test_verdict_survives_a_dead_rpc(self, db):
        """A missing counterparty address must never downgrade the verdict."""
        fid = _seed_reject(db, token=UP_TOK, market=COND)

        class _RpcDown(_Transport):
            def __call__(self, url, payload=None):
                if payload is not None:
                    raise RuntimeError("rpc down")
                return super().__call__(url, payload)

        transport = _RpcDown(trades=[
            {"asset": UP_TOK, "side": "BUY", "price": 0.60, "size": 9,
             "timestamp": NOW - 600 + 2, "proxyWallet": TAKER,
             "transactionHash": "0xbeef"},
        ])
        _run(db, transport, rpc_url="https://rpc.example")
        row = _verdict_row(db, fid)
        assert row["verdict"] == mf.VERDICT_FASTER_TAKER
        assert row["rival_addr"] is None

    def test_one_tape_fetch_serves_every_reject_on_the_same_market(self, db):
        _seed_reject(db, coid="a", token=UP_TOK, market=COND, age_s=900)
        _seed_reject(db, coid="b", token=UP_TOK, market=COND, age_s=800,
                     offset_sec=280)
        transport = _Transport(trades=[])
        _run(db, transport)
        assert sum(1 for c in transport.calls if "market=" in c) == 1
        assert sum(1 for c in transport.calls if "/markets/" in c) == 1

    def test_detail_json_stays_parseable_when_the_tape_is_huge(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        reject_ts = NOW - 600
        transport = _Transport(trades=[
            {"asset": UP_TOK, "side": "BUY", "price": 0.60 - i / 10_000,
             "size": 1, "timestamp": reject_ts + 2,
             "proxyWallet": f"0x{i:040x}", "transactionHash": f"0x{i:064x}"}
            for i in range(400)
        ])
        _run(db, transport)
        blob = _verdict_row(db, fid)["detail_json"]
        parsed = json.loads(blob)          # never a truncated, unparseable slice
        assert parsed["token_id"] == UP_TOK
        # The stored evidence is CAPPED, not sliced: 12 sample prints, while the
        # count still reports the whole window (a truncated blob would be
        # unparseable and a full one unbounded).
        assert len(parsed["prints"]) == 12
        assert parsed["n_prints_in_window"] == 400

    def test_an_oversized_detail_sheds_the_prints_not_the_json(self, db):
        """The last-resort guard for a detail that still blows the column: shed
        the optional print list wholesale rather than slicing the encoded blob
        (a sliced blob is unparseable, i.e. the evidence is simply lost)."""
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        detail = {
            "token_id": UP_TOK,
            "prints": [{"dt": -1.0, "px": 0.6, "sz": 1.0, "kind": "buyT"}] * 200,
        }
        mf._upsert(
            db, fill_id=fid, verdict=mf.VERDICT_FASTER_TAKER, beat=None,
            rival_addr=None, lag_s_used=3.0, detail=detail,
        )
        parsed = json.loads(_verdict_row(db, fid)["detail_json"])
        assert parsed["token_id"] == UP_TOK
        assert "prints" not in parsed
        assert parsed["prints_omitted"] == 200

    def test_nothing_to_do_costs_no_calls(self, db):
        transport = _Transport(trades=[])
        counters = _run(db, transport)
        assert counters["scanned"] == 0
        assert transport.calls == []

    def test_a_write_lock_is_honoured_when_supplied(self, db):
        import threading

        fid = _seed_reject(db, token=UP_TOK, market=COND)
        lock = threading.Lock()
        _run(db, _Transport(trades=[]), write_lock=lock)
        assert _verdict_row(db, fid)["verdict"] == mf.VERDICT_MAKER_CANCEL
        assert not lock.locked()           # released, not leaked

    def test_lag_is_recalibrated_from_our_own_fills(self, db):
        """The stored lag must come from OUR fills, not the constant, whenever
        the sample is big enough — and the value used is persisted per row."""
        for i in range(8):
            coid = f"own{i}"
            db.execute(
                "INSERT INTO subminute_fills (created_at, coin, sleeve, "
                "offset_sec, window_start_ts, side, requested_usd, "
                "filled_size, client_order_id, outcome) "
                "VALUES (?,'BTC','subminute_5m',270,?,'UP',5.0,3.0,?, "
                "'accepted')",
                (_ts(-3600 - i), 500 + i, coid),
            )
            db.execute(
                "INSERT INTO live_order_attempts (wallet_id, coin, sleeve, "
                "entry_minute, side, window_start_ts, client_order_id, "
                "poly_market_id, poly_token_id, amount_usd, status) "
                "VALUES ('w1','BTC','subminute_5m',4,'UP',?,?,?,?,5.0,"
                "'LINKED')",
                (500 + i, coid, COND, f"own-tok-{i}"),
            )
        db.commit()
        fid = _seed_reject(db, token=UP_TOK, market=COND)
        own = [
            {"asset": f"own-tok-{i}", "timestamp": int(NOW - 3600 - i) + 5,
             "transactionHash": f"0x{i:064x}"}
            for i in range(8)
        ]
        _run(db, _Transport(trades=[], own_trades=own))
        assert _verdict_row(db, fid)["lag_s_used"] == pytest.approx(5.0)

    def test_one_bad_row_does_not_stop_the_batch(self, db):
        good = _seed_reject(db, coid="a", token=UP_TOK, market=COND, age_s=900)
        bad = _seed_reject(db, coid="b", token=UP_TOK, market=COND, age_s=800,
                           offset_sec=280, limit_price=None)
        _run(db, _Transport(trades=[]))
        assert _verdict_row(db, good)["verdict"] == mf.VERDICT_MAKER_CANCEL
        assert _verdict_row(db, bad) is not None


# =========================================================================
# DASHBOARD read path (snapshot_builder)
# =========================================================================

from cryptobot.observability import snapshot_builder as sb  # noqa: E402


def _cf_row(verdict, *, created="2026-07-30 09:00:00", dt=-1.0, price=0.60,
            size=20.0, rival=None, coin="BTC", sleeve="subminute_5m", ws=100,
            side="UP", offset=270, limit=0.60, resolved="UP", fill_id=1):
    return {
        "fill_id": fill_id, "verdict": verdict, "beat_dt_s": dt,
        "beat_price": price, "beat_size": size, "rival_addr": rival,
        "created_at": created, "coin": coin, "sleeve": sleeve,
        "window_start_ts": ws, "side": side, "offset_sec": offset,
        "limit_price": limit, "resolved_outcome": resolved,
    }


def _epoch(stamp: str) -> float:
    import calendar
    import time
    return float(calendar.timegm(time.strptime(stamp, "%Y-%m-%d %H:%M:%S")))


def _resolved_map(rows: list) -> dict:
    """``_miss_res_key`` -> settled outcome, the way ``_reject_block`` builds it.

    The strip no longer reads ``resolved_outcome`` off each verdict row (a JOIN
    back from miss_forensics multiplies a verdict whenever a resolution logged
    several requotes); the caller hands the per-resolution map in.
    """
    out: dict = {}
    for r in rows:
        key = sb._miss_res_key(
            r["coin"], r["sleeve"], r["window_start_ts"], r["side"]
        )
        if key is not None and r.get("resolved_outcome") is not None:
            out.setdefault(key, r["resolved_outcome"])
    return out


def _stats(rows: list, **kw):
    """``_chain_forensics_stats`` over ``rows`` with their own resolved map."""
    kw.setdefault("now_ts", NOW_STATS)
    kw.setdefault("resolved_by_res", _resolved_map(rows))
    return sb._chain_forensics_stats(rows, **kw)


#: "now" for the strip tests, a few hours after the rows above — the 7d window
#: is relative, so the two must be pinned together or every row falls out of it.
NOW_STATS = _epoch("2026-07-30 12:00:00")


def _seed_requote(
    conn: sqlite3.Connection, *, created: str, coin="BTC",
    sleeve="subminute_5m", ws=100, side="UP", offset=270, limit=0.60,
    requote_ask=0.62, resolved="UP",
) -> None:
    """The dashboard's reject list is built from reject_requotes; the chain
    verdict is joined onto it by the (coin, sleeve, window, side, offset)
    5-tuple that both tables carry for the same order."""
    conn.execute(
        "INSERT INTO reject_requotes (created_at, coin, sleeve, offset_sec, "
        "window_start_ts, side, limit_price, requested_usd, requote_ask, "
        "requote_age_ms, resolved_outcome) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (created, coin, sleeve, offset, ws, side, limit, 5.0, requote_ask,
         1200, resolved),
    )
    conn.commit()


class TestDashboardPayload:
    def test_chip_fields_are_rounded_for_the_wire(self):
        payload = sb._chain_verdict_payload(
            _cf_row("FASTER_TAKER", price=0.6049999, size=20.4449,
                    rival="0xABC")
        )
        assert payload["chain_verdict"] == "FASTER_TAKER"
        assert payload["chain_rival_addr"] == "0xABC"
        assert payload["chain_beat_dt_s"] == -1.0

    def test_blank_rival_becomes_null_not_an_empty_string(self):
        payload = sb._chain_verdict_payload(_cf_row("FASTER_TAKER", rival=""))
        assert payload["chain_rival_addr"] is None

    def test_attempt_index_keys_include_the_offset(self):
        """subminute_fills and reject_requotes share this 5-tuple, so keying on
        it is what stops a verdict landing on the wrong attempt row."""
        idx = sb._chain_attempt_index([
            _cf_row("FASTER_TAKER", offset=270),
            _cf_row("MAKER_CANCEL", offset=280, fill_id=2),
        ])
        assert idx[("BTC", "subminute_5m", 100, "UP", 270)][
            "chain_verdict"] == "FASTER_TAKER"
        assert idx[("BTC", "subminute_5m", 100, "UP", 280)][
            "chain_verdict"] == "MAKER_CANCEL"

    def test_resolution_verdict_takes_the_strongest_evidence(self):
        """One at-our-level print PROVES the level was live, so it outranks the
        weaker "nothing printed" readings from the other attempts."""
        idx = sb._chain_resolution_index([
            _cf_row("MAKER_CANCEL", offset=270),
            _cf_row("FASTER_TAKER", offset=280, fill_id=2),
            _cf_row("REPRICED_ONLY", offset=290, fill_id=3),
        ])
        assert len(idx) == 1
        assert next(iter(idx.values()))["chain_verdict"] == "FASTER_TAKER"

    def test_pending_never_outranks_a_real_verdict(self):
        idx = sb._chain_resolution_index([
            _cf_row("PENDING", offset=270),
            _cf_row("MAKER_CANCEL", offset=280, fill_id=2),
        ])
        assert next(iter(idx.values()))["chain_verdict"] == "MAKER_CANCEL"

    def test_stats_count_verdicts_and_report_the_median_beat(self):
        rows = [_cf_row("FASTER_TAKER", dt=-1.0, fill_id=1),
                _cf_row("FASTER_TAKER", dt=-3.0, fill_id=2),
                _cf_row("REPRICED_ONLY", dt=None, fill_id=3),
                _cf_row("MAKER_CANCEL", dt=None, fill_id=4)]
        for i, r in enumerate(rows):
            r["window_start_ts"] = 100 + i
        stats = _stats(rows)
        assert stats["n"] == 4
        assert stats["counts"]["FASTER_TAKER"] == 2
        assert stats["median_beat_dt_s"] == pytest.approx(-2.0)
        assert stats["share_faster_taker"] == pytest.approx(0.5)

    def test_pending_rows_are_excluded_from_the_classified_share(self):
        rows = [_cf_row("FASTER_TAKER", fill_id=1),
                _cf_row("PENDING", fill_id=2),
                _cf_row("UNRESOLVED", fill_id=3)]
        stats = _stats(rows)
        assert stats["n"] == 3
        assert stats["n_classified"] == 1
        assert stats["share_faster_taker"] == pytest.approx(1.0)

    def test_speed_prize_compares_would_win_wr_to_the_price_we_bid(self):
        """The number the panel exists for: if would-win WR only matches the
        break-even WR the limit already implies, winning those races earns
        nothing and latency is NOT the lever."""
        rows = [
            _cf_row("FASTER_TAKER", side="UP", resolved="UP", limit=0.50,
                    fill_id=1, ws=101),
            _cf_row("FASTER_TAKER", side="UP", resolved="UP", limit=0.50,
                    fill_id=2, ws=102),
            _cf_row("FASTER_TAKER", side="UP", resolved="DOWN", limit=0.50,
                    fill_id=3, ws=103),
            _cf_row("FASTER_TAKER", side="UP", resolved="DOWN", limit=0.50,
                    fill_id=4, ws=104),
        ]
        sp = _stats(rows)["speed_prize"]
        assert sp["n_resolved"] == 4
        assert sp["would_win_wr"] == pytest.approx(0.5)
        assert sp["breakeven_wr"] == pytest.approx(0.5)
        assert sp["edge_pts"] == pytest.approx(0.0)

    def test_a_no_side_reject_scores_as_a_win_when_it_settled_down(self):
        rows = [_cf_row("FASTER_TAKER", side="NO", resolved="DOWN",
                        limit=0.40, fill_id=i, ws=100 + i) for i in range(3)]
        sp = _stats(rows)["speed_prize"]
        assert sp["would_win_wr"] == pytest.approx(1.0)
        assert sp["breakeven_wr"] == pytest.approx(0.40)

    def test_under_three_resolutions_the_wr_is_withheld(self):
        rows = [_cf_row("FASTER_TAKER", resolved="UP", fill_id=1, ws=101),
                _cf_row("FASTER_TAKER", resolved="UP", fill_id=2, ws=102)]
        sp = _stats(rows)["speed_prize"]
        assert sp["n_resolved"] == 2
        assert sp["would_win_wr"] is None
        assert sp["edge_pts"] is None

    def test_only_the_faster_taker_cohort_enters_the_prize(self):
        """Repriced and maker-cancel misses were never winnable at any speed."""
        rows = [_cf_row("MAKER_CANCEL", resolved="UP", fill_id=i, ws=100 + i)
                for i in range(5)]
        sp = _stats(rows)["speed_prize"]
        assert sp["n_resolved"] == 0
        assert sp["would_win_wr"] is None

    def test_unresolved_markets_do_not_enter_the_prize(self):
        rows = [_cf_row("FASTER_TAKER", resolved=None, fill_id=i, ws=100 + i)
                for i in range(5)]
        sp = _stats(rows)["speed_prize"]
        assert sp["n_resolved"] == 0

    def test_rows_older_than_the_strip_window_are_excluded(self):
        stats = _stats([_cf_row("FASTER_TAKER", created="2020-01-01 00:00:00")])
        assert stats is None

    def test_no_rows_renders_nothing(self):
        assert _stats([]) is None

    def test_a_legacy_db_without_the_table_reads_empty(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "bare.db")
        conn.row_factory = sqlite3.Row
        try:
            assert sb._chain_forensics_rows(conn, "2026-01-01 00:00:00") == []
        finally:
            conn.close()

    def test_verdicts_reach_the_reject_block_payload(self, db):
        """End to end: a stored verdict must surface on both the per-attempt
        reject row and the grouped missed-trade row."""
        fid = _seed_reject(db, token=UP_TOK, market=COND, age_s=600)
        created = db.execute(
            "SELECT created_at FROM subminute_fills WHERE id = ?", (fid,)
        ).fetchone()[0]
        _seed_requote(db, created=created)
        db.execute(
            "INSERT INTO miss_forensics (fill_id, verdict, beat_dt_s, "
            "beat_price, beat_size, rival_addr, lag_s_used) "
            "VALUES (?, 'FASTER_TAKER', -1.0, 0.6, 21.0, ?, 3.0)",
            (fid, RIVAL),
        )
        db.commit()
        block = sb._reject_block(db, 0.02, "2000-01-01 00:00:00")
        assert block["rejects"], "reject row missing from the payload"
        assert block["rejects"][0]["chain_verdict"] == "FASTER_TAKER"
        assert block["rejects"][0]["chain_rival_addr"] == RIVAL
        assert block["rejects"][0]["chain_beat_size"] == pytest.approx(21.0)
        assert block["missed_trades"][0]["chain_verdict"] == "FASTER_TAKER"
        assert block["reject_stats"]["chain_forensics"]["counts"][
            "FASTER_TAKER"] == 1

    def test_reject_block_is_unchanged_when_nothing_is_classified(self, db):
        fid = _seed_reject(db, token=UP_TOK, market=COND, age_s=600)
        created = db.execute(
            "SELECT created_at FROM subminute_fills WHERE id = ?", (fid,)
        ).fetchone()[0]
        _seed_requote(db, created=created)
        block = sb._reject_block(db, 0.02, "2000-01-01 00:00:00")
        assert block["rejects"][0].get("chain_verdict") is None
        assert block["missed_trades"][0].get("chain_verdict") is None
        assert block["reject_stats"]["chain_forensics"] is None
