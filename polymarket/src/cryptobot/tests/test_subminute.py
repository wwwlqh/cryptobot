"""B1-B5 sub-minute checkpoint add-on tests.

Covers the flag-gated pieces the checkpoint-grid combo port added:

  * ``_parse_checkpoint_offsets`` range-spec grammar + fail-closed bounds
    (portfolio_loader B1/B2);
  * ``load_sleeves`` flag-gating on the real ``v2_step1last_live.json``
    artifact — the config is checkpoint-only, so flag off drops every sleeve
    and the loader fails closed (ORB/exp044 retired 2026-07-09);
  * ``_qualifying_offset_sec`` / ``_window_start_ts`` tick qualification;
  * per-offset scan latch (``claim_subminute_scan``) + per-offset ledger
    dedup (``subminute_slot_seen``);
  * fill-rate breaker (B4): fill-rate arm, realized-EV arm, and the
    fail-closed default on any internal error;
  * staged sizing ramp (B5): rung 1 min-bet + hard cumulative cap, rung 2/3
    advancement on settled sub-minute ledger bets.

Every test is hermetic and fast: tmp_path-backed SQLite via ``init_db``
(conftest ``tmp_db``) and ``monkeypatch.setattr(config, ...)`` for every
``SUBMINUTE_*`` knob a module reads at call time.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cryptobot import config
from cryptobot.execution.fill_rate_breaker import (
    evaluate_breaker,
    ramp_state,
    record_subminute_fill,
)
from cryptobot.execution.ledger import (
    BetSignal,
    claim_subminute_scan,
    record_bet,
    subminute_slot_seen,
    subminute_window_existing_side,
    subminute_window_state,
)
from cryptobot.execution.pipeline import _qualifying_offset_sec, _window_start_ts
from cryptobot.strategy.portfolio_loader import _parse_checkpoint_offsets, load_sleeves


ARTIFACT_PATH = Path(config.V6_CONFIG_PATH)
# 2026-07-05 backtest-exposure mode (group_live.cap_tracks_equity=true): the
# concurrent-exposure ceiling tracks live equity and the fixed funder floor is
# retired, so 35% DD is the sole stop. cap_usd is omitted from the artifact
# entirely (nothing hardcoded; optional under tracking, defaults to 0.0), so
# cap_usd and all_group_caps_sum_usd() are both 0.0.
CURRENT_LIVE_GROUP_CAP_USD = 0.0

# Divisible by 300 (5m window boundary) — same anchor as test_slot_state.
WINDOW_START = 1_715_923_200
assert WINDOW_START % 300 == 0

# The live 5m grid: step5 (5,10,...,295) ∪ step1last (270..299).
FIVE_M_SPEC = "5:295:5,270:299:1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_subminute_signal(
    *,
    coin: str = "BTC",
    sleeve: str = "subminute_5m",
    entry_minute: int = 4,
    side: str = "YES",
    window_start_ts: int = WINDOW_START,
    offset_sec: int | None = 275,
    bet_size: float = 2.0,
    target_usd: float = 5.0,
    slot_filled_pre: float = 0.0,
    fill_price: float = 0.55,
) -> BetSignal:
    """Minimal valid BetSignal carrying the sub-minute ``offset_sec`` stamp.

    Field values mirror ``test_slot_state.make_signal`` — only the sleeve
    label and ``offset_sec`` differ.
    """
    return BetSignal(
        coin=coin,
        sleeve=sleeve,
        entry_minute=entry_minute,
        side=side,
        window_start_ts=window_start_ts,
        window_end_ts=window_start_ts + 300,
        interval_min=5,
        poly_market_id="MARKET",
        poly_token_id="TOKEN",
        fee_rate_bps=100.0,
        model_p=0.78,
        p_market=fill_price,
        fill_price=fill_price,
        walked_vwap=fill_price,
        realized_edge=0.10,
        partial_ret_z_1h=2.5,
        lut_p_bucket=7,
        lut_hit_rate=0.85,
        lut_n_cum=200,
        target_usd=target_usd,
        bet_size=bet_size,
        slot_filled_pre=slot_filled_pre,
        entry_top_price=fill_price,
        yes_pre1_anchor=fill_price,
        strike_price=65_000.0,
        window_open_spot=65_000.0,
        offset_sec=offset_sec,
        config_sha256="sha256-stub",
    )


def add_fill(
    conn: sqlite3.Connection,
    *,
    offset_sec: int = 275,
    window_start_ts: int = WINDOW_START,
    requested_usd: float = 1.0,
    filled_usd: float | None = 0.0,
    outcome: str = "accepted",
) -> None:
    """Insert one subminute_fills row with breaker-relevant fields set."""
    record_subminute_fill(
        conn,
        coin="BTC",
        sleeve="subminute_5m",
        offset_sec=offset_sec,
        window_start_ts=window_start_ts,
        side="YES",
        requested_usd=requested_usd,
        quote_ask=0.55,
        limit_price=0.56,
        filled_size=None,
        filled_price=None,
        filled_usd=filled_usd,
        order_id=None,
        client_order_id=None,
        outcome=outcome,
    )


def add_settled_bet(
    conn: sqlite3.Connection,
    *,
    offset_sec: int | None,
    window_start_ts: int,
    bet_size: float = 2.0,
    outcome: str = "WIN",
    pnl_net: float = 0.1,
) -> int:
    """record_bet + settle via the same UPDATE the resolution sweep issues.

    mode='LIVE' because the ramp/breaker deliberately count ONLY LIVE
    sub-minute bets — DRY paper fills must never advance a rung or feed the
    realized-EV arm (they fill at the observed book by construction).
    """
    rowid = record_bet(
        conn,
        make_subminute_signal(
            offset_sec=offset_sec,
            window_start_ts=window_start_ts,
            bet_size=bet_size,
        ),
        mode="LIVE",
    )
    conn.execute(
        "UPDATE ledger SET outcome=?, pnl_net=? WHERE id=?",
        (outcome, pnl_net, rowid),
    )
    return rowid


@pytest.fixture(autouse=True)
def _hermetic_subminute_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin every SUBMINUTE_* knob the modules read so an operator env on the
    box (e.g. CRYPTOBOT_SUBMINUTE_BREAKER_RESET) can never leak into a test."""
    monkeypatch.setattr(config, "SUBMINUTE_BREAKER_RESET", "")
    monkeypatch.setattr(config, "SUBMINUTE_BREAKER_WINDOW", 20)
    monkeypatch.setattr(config, "SUBMINUTE_BREAKER_MIN_ORDERS", 5)
    monkeypatch.setattr(config, "SUBMINUTE_BREAKER_MIN_FILL_RATE", 0.5)
    monkeypatch.setattr(config, "SUBMINUTE_BREAKER_EV_MIN_SETTLED", 3)
    monkeypatch.setattr(config, "SUBMINUTE_BREAKER_EV_MAX_LOSS_FRAC", 0.15)
    monkeypatch.setattr(config, "SUBMINUTE_RAMP_GREEN_BETS", 2)
    monkeypatch.setattr(config, "SUBMINUTE_RAMP_CAP_USD", 5.0)


# ---------------------------------------------------------------------------
# 1. _parse_checkpoint_offsets
# ---------------------------------------------------------------------------


class TestParseCheckpointOffsets:
    def test_live_5m_range_spec(self) -> None:
        offsets = _parse_checkpoint_offsets(FIVE_M_SPEC, "s", 5)
        # 59 step5 offsets + 30 step1last offsets − 6 overlaps = 83.
        assert len(offsets) == 83
        assert offsets == tuple(sorted(set(offsets)))
        assert offsets[0] == 5
        assert offsets[-1] == 299
        assert 275 in offsets and 276 in offsets  # step5 member + step1last member
        assert 3 not in offsets and 58 not in offsets

    def test_list_of_ints_accepted(self) -> None:
        assert _parse_checkpoint_offsets([20, 5, 10, 10], "s", 5) == (5, 10, 20)

    def test_none_is_baseline_sleeve(self) -> None:
        assert _parse_checkpoint_offsets(None, "s", 5) == ()

    def test_offset_zero_rejected(self) -> None:
        # Offset 0 is the window boundary — the baseline sleeves' territory.
        with pytest.raises(ValueError, match="outside"):
            _parse_checkpoint_offsets([0], "s", 5)

    def test_offset_at_window_end_rejected_for_5m(self) -> None:
        with pytest.raises(ValueError, match="outside"):
            _parse_checkpoint_offsets([300], "s", 5)
        # 299 is the last legal 5m offset.
        assert _parse_checkpoint_offsets([299], "s", 5) == (299,)

    @pytest.mark.parametrize(
        "spec",
        [
            "",             # empty
            "   ",          # whitespace-only
            "5:295",        # not start:end:step
            "a:b:c",        # non-integer
            "10:5:1",       # end < start
            "5:10:0",       # step must be > 0
            "5:10:-1",      # negative step
        ],
    )
    def test_bad_spec_strings_raise(self, spec: str) -> None:
        with pytest.raises(ValueError):
            _parse_checkpoint_offsets(spec, "s", 5)

    def test_non_int_list_items_raise(self) -> None:
        with pytest.raises(ValueError):
            _parse_checkpoint_offsets([5, True], "s", 5)
        with pytest.raises(ValueError):
            _parse_checkpoint_offsets({"5": 1}, "s", 5)


# ---------------------------------------------------------------------------
# 2. load_sleeves flag-gating on the real artifact
# ---------------------------------------------------------------------------


class TestLoadSleevesFlagGating:
    def test_flag_off_fails_closed_on_checkpoint_only_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # v2 step1last is a checkpoint-ONLY config (ORB/exp044 retired
        # 2026-07-09). With the flag off every sleeve is dropped, so the loader
        # fails CLOSED rather than booting an empty portfolio.
        monkeypatch.setattr(config, "SUBMINUTE_ENABLED", False)
        with pytest.raises(ValueError, match="nothing to run"):
            load_sleeves(ARTIFACT_PATH)

    def test_flag_on_loads_subminute_sleeves_and_group(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
        portfolio = load_sleeves(ARTIFACT_PATH)
        # The artifact carries exactly the two sub-minute checkpoint sleeves.
        assert [s.label for s in portfolio.sleeves] == [
            "subminute_5m",
            "subminute_15m",
        ]
        groups = {g.group_id: g for g in portfolio.wallet_groups}
        assert set(groups) == {"group_live"}
        assert groups["group_live"].cap_usd == pytest.approx(
            CURRENT_LIVE_GROUP_CAP_USD
        )
        assert portfolio.all_group_caps_sum_usd() == pytest.approx(
            CURRENT_LIVE_GROUP_CAP_USD
        )
        assert groups["group_live"].members == (
            "subminute_5m",
            "subminute_15m",
        )

        by_label = {s.label: s for s in portfolio.sleeves}
        assert len(by_label["subminute_5m"].checkpoint_offset_set) == 30
        assert by_label["subminute_5m"].checkpoint_offset_set[0] == 270
        assert by_label["subminute_5m"].checkpoint_offset_set[-1] == 299
        # 15m grid ends inside the window (899 < 900).
        assert by_label["subminute_15m"].checkpoint_offset_set[-1] == 899
        # MID bands/gates: (0.65, 0.85] and pred_edge >= 0.03.
        assert by_label["subminute_5m"].ask_bucket_ranges == ((0.65, 0.85),)
        assert by_label["subminute_5m"].threshold == pytest.approx(0.03)
        assert by_label["subminute_15m"].ask_bucket_ranges == ((0.65, 0.85),)
        assert by_label["subminute_15m"].threshold == pytest.approx(0.03)

    def test_sol_in_scan_universe_only_when_subminute_enabled(self) -> None:
        """config.COINS gains SOL only under the sub-minute flag — flag-off
        deploys keep the 3-coin scan set (no per-second SOL noise)."""
        import subprocess
        import sys

        for env_val, expected in (("1", "SOL"), ("0", "no-sol")):
            out = subprocess.run(
                [sys.executable, "-c",
                 "from cryptobot import config; "
                 "print('SOL' if 'SOL' in config.COINS else 'no-sol')"],
                capture_output=True, text=True,
                env={**__import__('os').environ,
                     "CRYPTOBOT_SUBMINUTE_ENABLED": env_val},
                cwd=str(Path(__file__).resolve().parents[2]),  # legacy-path-ok: package root for the subprocess PYTHONPATH
            )
            assert out.stdout.strip() == expected, out.stderr

# ---------------------------------------------------------------------------
# 3. _qualifying_offset_sec / _window_start_ts
# ---------------------------------------------------------------------------


@pytest.fixture
def flag_on_portfolio(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    return load_sleeves(ARTIFACT_PATH)


class TestQualifyingOffsetSec:
    def test_window_start_ts_alignment(self) -> None:
        assert _window_start_ts(WINDOW_START, 5) == WINDOW_START
        assert _window_start_ts(WINDOW_START + 275, 5) == WINDOW_START
        assert _window_start_ts(WINDOW_START + 300, 5) == WINDOW_START + 300

    def test_member_offset_qualifies(self, flag_on_portfolio) -> None:
        sleeve = {s.label: s for s in flag_on_portfolio.sleeves}["subminute_5m"]
        assert _qualifying_offset_sec(sleeve, WINDOW_START + 275) == 275
        assert _qualifying_offset_sec(sleeve, WINDOW_START + 276) == 276  # step1last
        assert _qualifying_offset_sec(sleeve, WINDOW_START + 270) == 270

    def test_non_member_offset_returns_none(self, flag_on_portfolio) -> None:
        sleeve = {s.label: s for s in flag_on_portfolio.sleeves}["subminute_5m"]
        assert _qualifying_offset_sec(sleeve, WINDOW_START + 3) is None
        assert _qualifying_offset_sec(sleeve, WINDOW_START + 58) is None
        # Offset 0 (the boundary tick) is never a checkpoint.
        assert _qualifying_offset_sec(sleeve, WINDOW_START) is None

    def test_baseline_sleeve_never_qualifies(self, flag_on_portfolio) -> None:
        # A minute-boundary (non-checkpoint) sleeve has no offsets and never
        # qualifies at a sub-minute offset. v2 is checkpoint-only, so synthesize
        # a baseline sleeve by stripping the checkpoint offsets.
        from dataclasses import replace as _replace

        checkpoint = {s.label: s for s in flag_on_portfolio.sleeves}["subminute_5m"]
        baseline = _replace(checkpoint, checkpoint_offset_set=())
        assert baseline.checkpoint_offset_set == ()
        assert _qualifying_offset_sec(baseline, WINDOW_START + 275) is None
        for off in (0, 3, 5, 275, 299):
            assert _qualifying_offset_sec(baseline, WINDOW_START + off) is None


# ---------------------------------------------------------------------------
# 4. claim_subminute_scan
# ---------------------------------------------------------------------------


class TestClaimSubminuteScan:
    def test_first_claim_wins_second_loses(self, tmp_db: sqlite3.Connection) -> None:
        key = dict(
            coin="BTC",
            sleeve="subminute_5m",
            offset_sec=275,
            window_start_ts=WINDOW_START,
        )
        assert claim_subminute_scan(tmp_db, **key) is True
        assert claim_subminute_scan(tmp_db, **key) is False

    def test_different_offset_is_its_own_latch(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        base = dict(coin="BTC", sleeve="subminute_5m", window_start_ts=WINDOW_START)
        assert claim_subminute_scan(tmp_db, offset_sec=275, **base) is True
        assert claim_subminute_scan(tmp_db, offset_sec=280, **base) is True
        assert claim_subminute_scan(tmp_db, offset_sec=280, **base) is False


# ---------------------------------------------------------------------------
# 5. subminute_slot_seen
# ---------------------------------------------------------------------------


class TestSubminuteSlotSeen:
    def test_false_before_true_after_record_bet(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        key = dict(
            coin="BTC",
            sleeve="subminute_5m",
            offset_sec=275,
            window_start_ts=WINDOW_START,
        )
        assert subminute_slot_seen(tmp_db, **key) is False
        record_bet(tmp_db, make_subminute_signal(offset_sec=275))
        assert subminute_slot_seen(tmp_db, **key) is True
        # Per-offset semantics: a different checkpoint in the same window is
        # still an open slot.
        assert (
            subminute_slot_seen(
                tmp_db,
                coin="BTC",
                sleeve="subminute_5m",
                offset_sec=280,
                window_start_ts=WINDOW_START,
            )
            is False
        )

    def test_cancelled_row_reopens_slot(self, tmp_db: sqlite3.Connection) -> None:
        rowid = record_bet(tmp_db, make_subminute_signal(offset_sec=275))
        tmp_db.execute("UPDATE ledger SET outcome='CANCELLED' WHERE id=?", (rowid,))
        assert (
            subminute_slot_seen(
                tmp_db,
                coin="BTC",
                sleeve="subminute_5m",
                offset_sec=275,
                window_start_ts=WINDOW_START,
            )
            is False
        )


# ---------------------------------------------------------------------------
# 5b. subminute_window_state (SUBMINUTE_ONE_PER_WINDOW one-per-resolution)
# ---------------------------------------------------------------------------


class TestSubminuteWindowState:
    def test_empty_window_is_zero(self, tmp_db: sqlite3.Connection) -> None:
        filled, anchor = subminute_window_state(
            tmp_db, coin="BTC", sleeve="subminute_5m", side="YES",
            window_start_ts=WINDOW_START,
        )
        assert filled == 0.0
        assert anchor is None

    def test_aggregates_across_offsets_and_minutes(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        # Unlike subminute_slot_seen (per-offset), the window state SUMS fills
        # across every offset/minute of the same (coin, sleeve, side, window),
        # so a later checkpoint sees the first entry and TOPS UP instead of
        # stacking a fresh bet. Offsets 100 (minute 1) and 296 (minute 4) share
        # the window — both must aggregate.
        record_bet(
            tmp_db,
            make_subminute_signal(
                offset_sec=100, entry_minute=1, bet_size=2.0, fill_price=0.55
            ),
        )
        record_bet(
            tmp_db,
            make_subminute_signal(
                offset_sec=296, entry_minute=4, bet_size=1.5, fill_price=0.61
            ),
        )
        filled, anchor = subminute_window_state(
            tmp_db, coin="BTC", sleeve="subminute_5m", side="YES",
            window_start_ts=WINDOW_START,
        )
        assert filled == pytest.approx(3.5)
        # Anchor is the FIRST entry's top price (sticky top-up cap), not the
        # later checkpoint's.
        assert anchor == pytest.approx(0.55)

    def test_side_isolated(self, tmp_db: sqlite3.Connection) -> None:
        record_bet(
            tmp_db, make_subminute_signal(offset_sec=296, side="YES", bet_size=2.0)
        )
        filled_no, anchor_no = subminute_window_state(
            tmp_db, coin="BTC", sleeve="subminute_5m", side="NO",
            window_start_ts=WINDOW_START,
        )
        assert filled_no == 0.0
        assert anchor_no is None

    def test_cancelled_row_excluded(self, tmp_db: sqlite3.Connection) -> None:
        rowid = record_bet(
            tmp_db, make_subminute_signal(offset_sec=296, bet_size=2.0)
        )
        tmp_db.execute(
            "UPDATE ledger SET outcome='CANCELLED' WHERE id=?", (rowid,)
        )
        filled, anchor = subminute_window_state(
            tmp_db, coin="BTC", sleeve="subminute_5m", side="YES",
            window_start_ts=WINDOW_START,
        )
        assert filled == 0.0
        assert anchor is None


class TestSubminuteWindowExistingSide:
    def test_empty_window_has_no_side(self, tmp_db: sqlite3.Connection) -> None:
        assert (
            subminute_window_existing_side(
                tmp_db, coin="BTC", sleeve="subminute_5m",
                window_start_ts=WINDOW_START,
            )
            is None
        )

    def test_first_non_cancelled_side_owns_market_window(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        record_bet(
            tmp_db,
            make_subminute_signal(
                offset_sec=293, side="YES", bet_size=1.5, fill_price=0.57
            ),
        )
        record_bet(
            tmp_db,
            make_subminute_signal(
                offset_sec=297, side="NO", bet_size=1.5, fill_price=0.72
            ),
        )

        assert subminute_window_existing_side(
            tmp_db, coin="BTC", sleeve="subminute_5m",
            window_start_ts=WINDOW_START,
        ) == "YES"

    def test_cancelled_first_side_does_not_own_window(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        rowid = record_bet(
            tmp_db,
            make_subminute_signal(offset_sec=293, side="YES", bet_size=1.5),
        )
        tmp_db.execute(
            "UPDATE ledger SET outcome='CANCELLED' WHERE id=?", (rowid,)
        )
        record_bet(
            tmp_db,
            make_subminute_signal(offset_sec=297, side="NO", bet_size=1.5),
        )

        assert subminute_window_existing_side(
            tmp_db, coin="BTC", sleeve="subminute_5m",
            window_start_ts=WINDOW_START,
        ) == "NO"


# ---------------------------------------------------------------------------
# 6/7/9. evaluate_breaker
# ---------------------------------------------------------------------------


class TestEvaluateBreaker:
    def test_green_on_empty_db(self, tmp_db: sqlite3.Connection) -> None:
        state = evaluate_breaker(tmp_db)
        assert state.tripped is False
        assert state.reason is None
        assert state.n_orders == 0

    def test_fill_rate_trip_on_ghost_fills(self, tmp_db: sqlite3.Connection) -> None:
        # MIN_ORDERS (pinned 5) orders requested $1 each, ZERO filled — the
        # exact stale-quote/ghost signature the breaker exists for.
        for i in range(int(config.SUBMINUTE_BREAKER_MIN_ORDERS)):
            add_fill(tmp_db, offset_sec=5 + 5 * i, requested_usd=1.0, filled_usd=0.0)
        state = evaluate_breaker(tmp_db)
        assert state.tripped is True
        assert state.reason == "fill_rate"
        assert state.n_orders == int(config.SUBMINUTE_BREAKER_MIN_ORDERS)
        assert state.fill_rate == pytest.approx(0.0)

    def test_no_trip_when_orders_fill(self, tmp_db: sqlite3.Connection) -> None:
        for i in range(int(config.SUBMINUTE_BREAKER_MIN_ORDERS)):
            add_fill(tmp_db, offset_sec=5 + 5 * i, requested_usd=1.0, filled_usd=1.0)
        state = evaluate_breaker(tmp_db)
        assert state.tripped is False
        assert state.reason is None
        assert state.fill_rate == pytest.approx(1.0)

    def test_below_min_orders_never_trips_fill_rate(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        for i in range(int(config.SUBMINUTE_BREAKER_MIN_ORDERS) - 1):
            add_fill(tmp_db, offset_sec=5 + 5 * i, requested_usd=1.0, filled_usd=0.0)
        assert evaluate_breaker(tmp_db).tripped is False

    def test_realized_ev_trip(self, tmp_db: sqlite3.Connection) -> None:
        # EV_MIN_SETTLED (pinned 3) settled sub-minute bets, stake $2 each,
        # pnl −$1 each: Σpnl −3.0 < −(0.15 × 6.0) = −0.9 → realized_ev trip.
        for i in range(int(config.SUBMINUTE_BREAKER_EV_MIN_SETTLED)):
            add_settled_bet(
                tmp_db,
                offset_sec=275,
                window_start_ts=WINDOW_START + 300 * i,
                bet_size=2.0,
                outcome="LOSS",
                pnl_net=-1.0,
            )
        state = evaluate_breaker(tmp_db)
        assert state.tripped is True
        assert state.reason == "realized_ev"
        assert state.n_settled == int(config.SUBMINUTE_BREAKER_EV_MIN_SETTLED)
        assert state.settled_pnl == pytest.approx(-3.0)
        assert state.settled_stake == pytest.approx(6.0)

    def test_realized_ev_ignores_baseline_and_winning_bets(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        # Winning sub-minute bets: no trip.
        for i in range(int(config.SUBMINUTE_BREAKER_EV_MIN_SETTLED)):
            add_settled_bet(
                tmp_db,
                offset_sec=275,
                window_start_ts=WINDOW_START + 300 * i,
                outcome="WIN",
                pnl_net=0.5,
            )
        # Baseline (offset_sec NULL) losses never feed the sub-minute breaker.
        add_settled_bet(
            tmp_db,
            offset_sec=None,
            window_start_ts=WINDOW_START + 3000,
            bet_size=100.0,
            outcome="LOSS",
            pnl_net=-100.0,
        )
        state = evaluate_breaker(tmp_db)
        assert state.tripped is False
        assert state.n_settled == int(config.SUBMINUTE_BREAKER_EV_MIN_SETTLED)

    def test_fail_closed_on_missing_table(self, tmp_db: sqlite3.Connection) -> None:
        tmp_db.execute("DROP TABLE subminute_fills")
        state = evaluate_breaker(tmp_db)
        assert state.tripped is True
        assert state.reason == "error"


# ---------------------------------------------------------------------------
# 8. ramp_state
# ---------------------------------------------------------------------------


class TestRampState:
    @pytest.fixture(autouse=True)
    def _enable_legacy_ramp(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # 2026-07-05: the staged ramp runs only when SUBMINUTE_RAMP_ENABLED=1
        # (default off = full-target sizing, 35% DD is the sole control). Every
        # test in THIS class exercises the legacy rung/cap logic, so enable it;
        # the disabled default is covered by test_ramp_disabled_is_full_size.
        monkeypatch.setattr(config, "SUBMINUTE_RAMP_ENABLED", True)

    def test_ramp_disabled_is_full_size(
        self, tmp_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Default (ramp off): no probation — sub-minute sizes at its full backtest
        # target from the first bet; the 35% DD halt is the sole risk control.
        monkeypatch.setattr(config, "SUBMINUTE_RAMP_ENABLED", False)
        state = ramp_state(tmp_db)
        assert state.rung == 3
        assert state.stake_multiplier == pytest.approx(1.0)
        assert state.min_bet_override is False
        assert state.cap_blocked is False

    def test_fresh_db_is_rung_1_min_bet(self, tmp_db: sqlite3.Connection) -> None:
        state = ramp_state(tmp_db)
        assert state.rung == 1
        assert state.min_bet_override is True
        assert state.cap_blocked is False
        assert state.stake_multiplier == pytest.approx(1.0)
        assert state.cumulative_filled_usd == pytest.approx(0.0)
        assert state.n_settled == 0

    def test_rung_1_cumulative_cap_blocks(self, tmp_db: sqlite3.Connection) -> None:
        # SUBMINUTE_RAMP_CAP_USD pinned to 5.0; 3.0 + 2.5 = 5.5 >= cap.
        add_fill(tmp_db, offset_sec=275, filled_usd=3.0)
        add_fill(tmp_db, offset_sec=280, filled_usd=2.5)
        state = ramp_state(tmp_db)
        assert state.rung == 1
        assert state.cumulative_filled_usd == pytest.approx(5.5)
        assert state.cap_blocked is True

    def test_unfilled_ghosts_do_not_consume_the_cap(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        # The cap counts REALIZED fill value — unfilled FAKs cost nothing.
        for i in range(10):
            add_fill(tmp_db, offset_sec=5 + 5 * i, requested_usd=1.0, filled_usd=0.0)
        state = ramp_state(tmp_db)
        assert state.cumulative_filled_usd == pytest.approx(0.0)
        assert state.cap_blocked is False

    def test_rung_2_after_green_bets(self, tmp_db: sqlite3.Connection) -> None:
        # SUBMINUTE_RAMP_GREEN_BETS pinned to 2.
        for i in range(int(config.SUBMINUTE_RAMP_GREEN_BETS)):
            add_settled_bet(
                tmp_db, offset_sec=275, window_start_ts=WINDOW_START + 300 * i
            )
        state = ramp_state(tmp_db)
        assert state.rung == 2
        assert state.stake_multiplier == pytest.approx(0.5)
        assert state.min_bet_override is False
        assert state.cap_blocked is False
        assert state.n_settled == int(config.SUBMINUTE_RAMP_GREEN_BETS)

    def test_rung_3_after_double_green(self, tmp_db: sqlite3.Connection) -> None:
        for i in range(2 * int(config.SUBMINUTE_RAMP_GREEN_BETS)):
            add_settled_bet(
                tmp_db, offset_sec=275, window_start_ts=WINDOW_START + 300 * i
            )
        state = ramp_state(tmp_db)
        assert state.rung == 3
        assert state.stake_multiplier == pytest.approx(1.0)
        assert state.min_bet_override is False

    def test_baseline_and_pending_rows_do_not_advance_the_ramp(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        # Settled BASELINE rows (offset_sec NULL) don't count...
        for i in range(2 * int(config.SUBMINUTE_RAMP_GREEN_BETS)):
            add_settled_bet(
                tmp_db, offset_sec=None, window_start_ts=WINDOW_START + 300 * i
            )
        # ...and unsettled (PENDING) sub-minute rows don't either.
        record_bet(
            tmp_db,
            make_subminute_signal(offset_sec=275, window_start_ts=WINDOW_START + 9000),
        )
        state = ramp_state(tmp_db)
        assert state.rung == 1
        assert state.n_settled == 0

    def test_fail_closed_on_missing_table(self, tmp_db: sqlite3.Connection) -> None:
        tmp_db.execute("DROP TABLE subminute_fills")
        state = ramp_state(tmp_db)
        assert state.rung == 1
        assert state.min_bet_override is True
        assert state.cap_blocked is True  # fail-closed: refuse placement

    def test_dry_paper_bets_never_advance_the_ramp(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Regression: DRY paper sub-minute bets fill at the observed book by
        construction — 50 of them must NOT jump the ramp to rung 3 before the
        first REAL (LIVE) order proves fills are possible."""
        from cryptobot import config as _cfg

        for i in range(2 * _cfg.SUBMINUTE_RAMP_GREEN_BETS):
            rowid = record_bet(
                tmp_db,
                make_subminute_signal(
                    offset_sec=275,
                    window_start_ts=WINDOW_START + 300 * i,
                ),
                mode="DRY",
            )
            tmp_db.execute(
                "UPDATE ledger SET outcome='WIN', pnl_net=0.1 WHERE id=?",
                (rowid,),
            )
        state = ramp_state(tmp_db)
        assert state.rung == 1
        assert state.n_settled == 0


# ---------------------------------------------------------------------------
# Adversarial-review regression fixes (2026-07-03)
# ---------------------------------------------------------------------------


class TestBreakerResetFormat:
    def test_t_separated_iso_reset_includes_same_day_rows(
        self, tmp_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Review major: the documented 'YYYY-MM-DDTHH:MM:SS' reset sorted
        AFTER every same-day space-format row, silently excluding the reset
        day from the breaker window AND the rung-1 cumulative cap. The
        clause must normalize both spellings."""
        for _ in range(6):
            add_fill(tmp_db, filled_usd=0.0, requested_usd=1.0)
        # T-format reset dated well in the past — every row is AFTER it.
        monkeypatch.setattr(
            config, "SUBMINUTE_BREAKER_RESET", "2020-01-01T00:00:00"
        )
        state = evaluate_breaker(tmp_db)
        assert state.n_orders == 6  # same-day rows are IN the window
        assert state.tripped and state.reason == "fill_rate"

    def test_unparseable_reset_fails_safe_to_full_history(
        self, tmp_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for _ in range(6):
            add_fill(tmp_db, filled_usd=0.0, requested_usd=1.0)
        monkeypatch.setattr(config, "SUBMINUTE_BREAKER_RESET", "not-a-date")
        state = evaluate_breaker(tmp_db)
        # Full history (safe side): the ghost fills still trip the breaker.
        assert state.n_orders == 6
        assert state.tripped


class TestRampArithmetic:
    def test_green_bets_reachable_under_rung1_cap(self) -> None:
        """Review major (deadlock): rung 2 requires GREEN_BETS settled bets,
        but rung-1 fills are ~MIN_BET_USD each and stop at RAMP_CAP_USD —
        the shipped defaults must leave the target reachable with headroom
        (the original 25 x $1 deadlocked against the $20 cap)."""
        need = config.SUBMINUTE_RAMP_GREEN_BETS * config.MIN_BET_USD
        assert need <= config.SUBMINUTE_RAMP_CAP_USD * 0.8, (
            f"rung-2 unreachable: {config.SUBMINUTE_RAMP_GREEN_BETS} bets x "
            f"${config.MIN_BET_USD} = ${need} vs rung-1 cap "
            f"${config.SUBMINUTE_RAMP_CAP_USD}"
        )


class TestPruneScoping:
    def test_boot_and_critical_health_rows_survive_prune(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Review finding: the 14d pipeline_health prune must NOT erase the
        boot config-sha generation log or CRITICAL forensics."""
        from cryptobot.scheduler.resolution_sweep import prune_pipeline_health

        old = "datetime('now', '-30 days')"
        for stage, status in (
            ("boot", "WARN"),
            ("risk", "CRITICAL"),
            ("latency", "OK"),
            ("tick", "WARN"),
        ):
            tmp_db.execute(
                f"INSERT INTO pipeline_health (created_at, stage, status, message) "
                f"VALUES ({old}, ?, ?, 'x')",
                (stage, status),
            )
        pruned = prune_pipeline_health(tmp_db, retention_days=14)
        assert pruned == 2  # latency OK + tick WARN
        kept = {
            (r["stage"], r["status"])
            for r in tmp_db.execute("SELECT stage, status FROM pipeline_health")
        }
        assert ("boot", "WARN") in kept
        assert ("risk", "CRITICAL") in kept

    def test_subminute_scan_latches_pruned_by_window(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        from cryptobot.scheduler.resolution_sweep import (
            prune_subminute_scan_latches,
        )

        now = 1_800_000_000
        claim_subminute_scan(
            tmp_db, coin="BTC", sleeve="subminute_5m",
            offset_sec=275, window_start_ts=now - 10 * 86400,
        )
        claim_subminute_scan(
            tmp_db, coin="BTC", sleeve="subminute_5m",
            offset_sec=275, window_start_ts=now - 60,
        )
        pruned = prune_subminute_scan_latches(
            tmp_db, retention_days=3, now_ts=float(now)
        )
        assert pruned == 1
        left = tmp_db.execute(
            "SELECT COUNT(*) AS n FROM subminute_scan_latches"
        ).fetchone()
        assert left["n"] == 1
