"""U6 group exposure cap tests.

Covers `_gate_group_exposure` (LIVE-only, D-LIVE-2 / D-LIVE-4 / D-LIVE-14)
both at the unit level (GateInput â†’ GateResult) and the integration level
(`get_group_state` aggregating LIVE PENDING + unresolved attempts).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.capital import (
    get_group_state,
    get_wallet_snapshots,
)
from cryptobot.execution.gates import GateInput, evaluate_gates
from cryptobot.execution.operator_control import LIVE, OperatorState
from cryptobot.execution.pipeline import _live_wallet_skip_reason
from cryptobot.tests._legacy_artifact import legacy_portfolio


# ----- Unit tests over `_gate_group_exposure` (via evaluate_gates) -----


def _base_input(**over) -> GateInput:
    """Build a happy-path GateInput; override per-test fields."""
    from cryptobot.strategy.portfolio_loader import Sleeve

    sleeve = Sleeve(
        label="v6_5m",
        alloc=1.0,
        interval="5m",
        interval_min=5,
        side_strategy="model_signed",
        entry_minute_set=(4,),
        entry_strategy="first_qualifying",
        min_edge=0.0,
        max_edge=1.0,
        min_fill_price=0.0,
        max_fill_price=1.0,
        min_agreement=0,
        capital_frac=0.05,
        max_walk_price=0.7,
        slippage_haircut=0.0,
        min_lut_n=10,
        signal_source="hgb",
        hour_bucket_set="all",
        dow_set="all",
    )
    defaults: dict = dict(
        coin="BTC",
        sleeve=sleeve,
        entry_minute=4,
        side="YES",
        realized_edge=0.10,
        fill_price=0.5,
        fee_rate_bps=100.0,
        spot_age_s=1.0,
        proposed_bet_size=5.0,
        pending_exposure_total=0.0,
        realized_capital=100.0,
        slot_filled_usd=0.0,
        slot_target_usd=5.0,
        disk_free_mb=10_000.0,
        book_top_ask_age_s=1.0,
        is_topup=False,
        exposure_cap_fraction=0.5,
        mode="LIVE",
        group_cap_usd=100.0,
        group_pending_usd=0.0,
        live_wallet_balance_usd=None,
        all_group_caps_sum_usd=200.0,
    )
    defaults.update(over)
    return GateInput(**defaults)


class TestGroupExposureGate:
    def test_dry_mode_bypasses_group_exposure(self) -> None:
        """DRY rows skip the group_exposure gate even when pending > cap."""
        inp = _base_input(
            mode="DRY", group_pending_usd=200.0, group_cap_usd=100.0
        )
        result = evaluate_gates(inp)
        assert result.skip_reason != "group_exposure_cap"

    def test_live_within_cap_passes(self) -> None:
        result = evaluate_gates(_base_input(
            mode="LIVE", group_pending_usd=50.0, proposed_bet_size=5.0,
            group_cap_usd=100.0,
        ))
        assert result.passed is True

    def test_live_exceeds_cap_skips(self) -> None:
        result = evaluate_gates(_base_input(
            mode="LIVE", group_pending_usd=99.0, proposed_bet_size=5.0,
            group_cap_usd=100.0,
        ))
        assert result.skip_reason == "group_exposure_cap"

    def test_ungrouped_wallet_passes(self) -> None:
        """No group_cap_usd â†’ gate is a no-op (legacy artifact, no caps)."""
        result = evaluate_gates(_base_input(
            mode="LIVE", group_cap_usd=None, group_pending_usd=999.0,
        ))
        assert result.passed is True

    def test_live_balance_shrinks_effective_cap(self) -> None:
        """D-LIVE-4: live_balance < SUM(group_caps) shrinks the cap pro-rata."""
        # share = 100 / 200 = 0.5; balance=150 â†’ effective cap = min(100, 75)=75
        result = evaluate_gates(_base_input(
            mode="LIVE",
            group_cap_usd=100.0,
            all_group_caps_sum_usd=200.0,
            live_wallet_balance_usd=150.0,
            group_pending_usd=70.0,
            proposed_bet_size=10.0,  # would push to $80 > $75
        ))
        assert result.skip_reason == "group_exposure_cap"

    def test_live_balance_zero_collapses_cap_to_zero(self) -> None:
        """Funder drained â†’ effective cap is 0 â†’ every LIVE bet skips."""
        result = evaluate_gates(_base_input(
            mode="LIVE",
            group_cap_usd=100.0,
            all_group_caps_sum_usd=200.0,
            live_wallet_balance_usd=0.0,
            group_pending_usd=0.0,
            proposed_bet_size=1.0,
        ))
        assert result.skip_reason == "group_exposure_cap"

    def test_live_cap_basis_counts_already_funded_pending(self) -> None:
        """Open LIVE positions should not make static caps shrink by themselves."""
        result = evaluate_gates(_base_input(
            mode="LIVE",
            group_cap_usd=100.0,
            all_group_caps_sum_usd=120.0,
            live_wallet_balance_usd=95.0,
            live_cap_basis_usd=120.0,
            group_pending_usd=95.0,
            proposed_bet_size=4.0,
        ))
        assert result.passed is True

    def test_live_cap_basis_still_shrinks_after_real_depletion(self) -> None:
        result = evaluate_gates(_base_input(
            mode="LIVE",
            group_cap_usd=100.0,
            all_group_caps_sum_usd=120.0,
            live_wallet_balance_usd=70.0,
            live_cap_basis_usd=80.0,
            group_pending_usd=65.0,
            proposed_bet_size=5.0,
        ))
        assert result.skip_reason == "group_exposure_cap"

    def test_cap_tracks_equity_uses_live_equity_not_static_cap(self) -> None:
        """Backtest-exposure mode: the ceiling IS live equity, not the static
        cap_usd. A bet far above the static $100 cap passes as long as it fits
        within equity — the exposure ceiling grows with the account. (The
        per-wallet exposure gate is set non-binding — exposure_cap_fraction=1.0
        on full equity, as live — so the GROUP gate is what's under test.)"""
        result = evaluate_gates(_base_input(
            mode="LIVE",
            group_cap_tracks_equity=True,
            group_cap_usd=100.0,           # static cap is ignored under tracking
            live_cap_basis_usd=1000.0,     # full-equity ceiling
            all_group_caps_sum_usd=0.0,    # retired-floor sentinel (loader -> 0)
            realized_capital=2000.0,       # per-wallet exposure gate non-binding
            exposure_cap_fraction=1.0,
            pending_exposure_total=500.0,
            group_pending_usd=500.0,
            proposed_bet_size=400.0,       # 900 < 1000 equity -> passes
        ))
        assert result.passed is True

    def test_cap_tracks_equity_still_blocks_above_equity(self) -> None:
        """Backtest-exposure mode is not unbounded: exposure above live equity
        still skips (you can't deploy more than you hold)."""
        result = evaluate_gates(_base_input(
            mode="LIVE",
            group_cap_tracks_equity=True,
            group_cap_usd=100.0,
            live_cap_basis_usd=1000.0,
            all_group_caps_sum_usd=0.0,
            realized_capital=2000.0,       # per-wallet exposure gate non-binding
            exposure_cap_fraction=1.0,
            pending_exposure_total=950.0,
            group_pending_usd=950.0,
            proposed_bet_size=100.0,       # 1050 > 1000 -> skip
        ))
        assert result.skip_reason == "group_exposure_cap"


def _operator_live_state() -> OperatorState:
    return OperatorState(
        state=LIVE,
        boot_dry_run=False,
        reason="test",
        updated_by="pytest",
        updated_at=utc_now_sql(),
    )


class TestLiveWalletFunderFloor:
    def test_counts_live_pending_exposure_as_already_funded(self) -> None:
        reason = _live_wallet_skip_reason(
            op_state=_operator_live_state(),
            readiness_fresh=True,
            live_balance=115.0,
            live_pending_exposure_usd=5.0,
            all_group_caps_sum_usd=120.0,
            has_order_client=True,
        )
        assert reason is None

    def test_still_blocks_when_cash_plus_pending_below_floor(self) -> None:
        reason = _live_wallet_skip_reason(
            op_state=_operator_live_state(),
            readiness_fresh=True,
            live_balance=114.0,
            live_pending_exposure_usd=5.0,
            all_group_caps_sum_usd=120.0,
            has_order_client=True,
        )
        assert reason == "funder_balance_low"

    def test_funder_floor_retired_under_cap_tracks_equity(self) -> None:
        """Backtest-exposure mode: funder_balance_floor_usd() -> 0, so the
        caps sum reaching the gate is 0 and funder_balance_low NEVER fires,
        even at a tiny balance. The 35% DD halt is the sole stop; a failed
        balance fetch is still caught by funder_balance_unknown."""
        reason = _live_wallet_skip_reason(
            op_state=_operator_live_state(),
            readiness_fresh=True,
            live_balance=3.0,             # far below any old fixed cap
            live_pending_exposure_usd=0.0,
            all_group_caps_sum_usd=0.0,   # retired-floor sentinel
            has_order_client=True,
        )
        assert reason is None


# ----- Integration tests over `get_group_state` -----


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    conn = init_db(tmp_path / "group.db")
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    yield conn
    conn.close()


def _insert_live_pending(
    conn: sqlite3.Connection,
    *,
    sleeve: str,
    wallet_id: str,
    bet_size: float,
    wallet_group: str,
) -> None:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
           window_start_ts, window_end_ts, interval_min, poly_market_id,
           poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
           realized_edge, target_usd, bet_size, entry_top_price,
           config_sha256, mode, wallet_group)
        VALUES (?, 'BTC', ?, ?, 4, 'YES', 1715923200, 1715923500, 5, 'M',
                'T', 100.0, 0.78, 0.55, 0.55, 0.10, ?, ?, 0.55,
                'sha-stub', 'LIVE', ?)
        """,
        (utc_now_sql(), wallet_id, sleeve, bet_size, bet_size, wallet_group),
    )


def _insert_dry_pending(
    conn: sqlite3.Connection,
    *,
    sleeve: str,
    wallet_id: str,
    bet_size: float,
    wallet_group: str,
) -> None:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
           window_start_ts, window_end_ts, interval_min, poly_market_id,
           poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
           realized_edge, target_usd, bet_size, entry_top_price,
           config_sha256, mode, wallet_group)
        VALUES (?, 'BTC', ?, ?, 4, 'YES', 1715923200, 1715923500, 5, 'M',
                'T', 100.0, 0.78, 0.55, 0.55, 0.10, ?, ?, 0.55,
                'sha-stub', 'DRY', ?)
        """,
        (utc_now_sql(), wallet_id, sleeve, bet_size, bet_size, wallet_group),
    )


class TestGetGroupState:
    def test_sums_live_pending_across_member_wallets(
        self, db: sqlite3.Connection
    ) -> None:
        portfolio = legacy_portfolio()
        _insert_live_pending(
            db, sleeve="v6_5m", wallet_id="v6_5m", bet_size=20.0,
            wallet_group="group_5m",
        )
        _insert_live_pending(
            db, sleeve="pairs_5m_long", wallet_id="pairs_5m", bet_size=10.0,
            wallet_group="group_5m",
        )
        db.commit()

        state = get_group_state(db, portfolio)
        assert state["group_5m"].pending_usd == pytest.approx(30.0)
        assert state["group_15m"].pending_usd == pytest.approx(0.0)

    def test_excludes_dry_rows_from_pending(
        self, db: sqlite3.Connection
    ) -> None:
        """D-LIVE-14: parity shadow does NOT consume real-money headroom."""
        portfolio = legacy_portfolio()
        _insert_dry_pending(
            db, sleeve="v6_5m", wallet_id="v6_5m", bet_size=99.0,
            wallet_group="group_5m",
        )
        db.commit()

        state = get_group_state(db, portfolio)
        assert state["group_5m"].pending_usd == pytest.approx(0.0)

    def test_counts_unlinked_live_order_attempts(
        self, db: sqlite3.Connection
    ) -> None:
        """D-LIVE-28: in-flight SUBMITTING attempts count toward group cap."""
        portfolio = legacy_portfolio()
        now = utc_now_sql()
        db.execute(
            """
            INSERT INTO live_order_attempts
              (created_at, updated_at, wallet_id, wallet_group, mode, coin,
               sleeve, entry_minute, side, window_start_ts, client_order_id,
               poly_market_id, poly_token_id, price, amount_usd, status,
               signal_json)
            VALUES (?, ?, 'v6_5m', 'group_5m', 'LIVE', 'BTC', 'v6_5m', 4,
                    'YES', 1715923200, 'coid-1', 'M', 'T', 0.55, 25.0,
                    'SUBMITTING', '{}')
            """,
            (now, now),
        )
        db.commit()

        state = get_group_state(db, portfolio)
        assert state["group_5m"].pending_usd == pytest.approx(25.0)

    def test_linked_attempts_excluded(self, db: sqlite3.Connection) -> None:
        """LINKED attempts have a matching ledger row â€” counting them would
        double-count the same exposure."""
        portfolio = legacy_portfolio()
        now = utc_now_sql()
        db.execute(
            """
            INSERT INTO live_order_attempts
              (created_at, updated_at, wallet_id, wallet_group, mode, coin,
               sleeve, entry_minute, side, window_start_ts, client_order_id,
               poly_market_id, poly_token_id, price, amount_usd, status,
               ledger_id, signal_json)
            VALUES (?, ?, 'v6_5m', 'group_5m', 'LIVE', 'BTC', 'v6_5m', 4,
                    'YES', 1715923200, 'coid-1', 'M', 'T', 0.55, 25.0,
                    'LINKED', 42, '{}')
            """,
            (now, now),
        )
        db.commit()

        state = get_group_state(db, portfolio)
        assert state["group_5m"].pending_usd == pytest.approx(0.0)


class TestWalletSnapshotGroupFields:
    def test_snapshot_carries_mode_group_and_balance(
        self, db: sqlite3.Connection
    ) -> None:
        portfolio = legacy_portfolio()
        # Seed the schema's wallet_mode_state so the snapshot loader has
        # something to read.
        from cryptobot.execution.operator_control import boot_operator_state
        boot_operator_state(db, portfolio, boot_dry_run=True)

        group_state = get_group_state(db, portfolio)
        snaps = get_wallet_snapshots(
            db, portfolio,
            mode_state={"v6_5m": "LIVE", "v6_15m": "DRY",
                        "pairs_5m": "DRY", "pairs_15m": "DRY"},
            group_state=group_state,
            live_wallet_balance=150.0,
        )
        assert snaps["v6_5m"].mode == "LIVE"
        assert snaps["v6_5m"].wallet_group == "group_5m"
        assert snaps["v6_5m"].group_cap_usd == pytest.approx(100.0)
        assert snaps["v6_5m"].live_wallet_balance == pytest.approx(150.0)
        # DRY wallets still get the group fields populated for forensics.
        assert snaps["pairs_5m"].mode == "DRY"
        assert snaps["pairs_5m"].wallet_group == "group_5m"

    def test_legacy_call_without_mode_state_defaults_to_dry(
        self, db: sqlite3.Connection
    ) -> None:
        """Existing test fixtures and the dashboard snapshot builder call
        `get_wallet_snapshots(conn, portfolio)` with no LIVE-routing kwargs;
        every wallet must read back as DRY without errors."""
        portfolio = legacy_portfolio()
        snaps = get_wallet_snapshots(db, portfolio)
        for snap in snaps.values():
            assert snap.mode == "DRY"
            assert snap.live_wallet_balance is None
