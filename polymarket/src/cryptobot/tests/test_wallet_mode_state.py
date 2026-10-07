"""U4 verification: per-wallet DRY/LIVE mode state machine.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U4 (D-LIVE-1,
D-LIVE-3, D-LIVE-19, D-LIVE-25):

  - seed_wallet_modes populates one row per portfolio wallet, defaulting
    to DRY (or whatever the portfolio's `wallet_default_modes` says).
  - set_wallet_mode enforces optimistic concurrency, asymmetric rate
    limits (DRY → LIVE: 60s cooldown unless force=True; LIVE → DRY:
    always allowed), and the live-money guard composition.
  - Audit events live in `operator_control_events` with scope='wallet:<id>'.

The live-money guard is stubbed out via the `guard_live_action_context`
injection so the tests don't need to seed live_readiness_reports +
wallet_reconciliation_runs (those land in Phase 2 — U7 + U9).
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field

import pytest

from cryptobot.db import init_db
from cryptobot.execution.operator_control import (
    BlockedError,
    ConcurrentMutationError,
    DRY,
    LIVE,
    OperatorControlError,
    RateLimitError,
    get_wallet_mode,
    load_wallet_mode_state,
    seed_operator_control_state,
    seed_wallet_modes,
    set_wallet_mode,
    start_processing,
)


# -- Stub portfolio that matches the load_sleeves shape we use ---------------


@dataclass
class _Sleeve:
    label: str


@dataclass
class _StubPortfolio:
    sleeve_labels: list[str]
    default_modes: dict[str, str] = field(default_factory=dict)

    @property
    def market_sleeves(self) -> list[_Sleeve]:
        return [_Sleeve(label=label) for label in self.sleeve_labels]

    def default_mode_for(self, wallet_id: str) -> str:
        return self.default_modes.get(wallet_id, DRY)


def _portfolio_four_wallets() -> _StubPortfolio:
    return _StubPortfolio(
        sleeve_labels=["v6_5m", "v6_15m", "pairs_5m", "pairs_15m"]
    )


def _portfolio_one_wallet() -> _StubPortfolio:
    return _StubPortfolio(sleeve_labels=["v6_5m"])


def _portfolio_exp044_wallets() -> _StubPortfolio:
    return _StubPortfolio(sleeve_labels=["exp044_5m", "exp044_15m"])


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    seed_operator_control_state(db, boot_dry_run=True)
    yield db
    db.close()


def _accept(_conn: sqlite3.Connection, _wallet_id: str) -> None:
    """Stub guard that always accepts — passes Phase 2 readiness/recon."""
    return None


def _reject(_conn: sqlite3.Connection, _wallet_id: str) -> None:
    """Stub guard that always blocks — simulates Phase 1 (no readiness writer)."""
    raise BlockedError("no_readiness_report", "phase 1 has no writer yet")


# ============================================================================
# Seeding
# ============================================================================


class TestSeedWalletModes:
    def test_seed_populates_one_row_per_wallet(
        self, conn: sqlite3.Connection
    ) -> None:
        portfolio = _portfolio_four_wallets()
        out = seed_wallet_modes(conn, portfolio)
        assert set(out.keys()) == {"v6_5m", "v6_15m", "pairs_5m", "pairs_15m"}
        rows = conn.execute(
            "SELECT wallet_id, mode FROM wallet_mode_state ORDER BY wallet_id"
        ).fetchall()
        assert {r["wallet_id"] for r in rows} == {
            "v6_5m",
            "v6_15m",
            "pairs_5m",
            "pairs_15m",
        }
        assert all(r["mode"] == "DRY" for r in rows)

    def test_seed_honors_portfolio_defaults(
        self, conn: sqlite3.Connection
    ) -> None:
        portfolio = _StubPortfolio(
            sleeve_labels=["v6_5m", "v6_15m"],
            default_modes={"v6_5m": "LIVE", "v6_15m": "DRY"},
        )
        out = seed_wallet_modes(conn, portfolio)
        assert out == {"v6_5m": "LIVE", "v6_15m": "DRY"}

    def test_seed_invalid_default_normalizes_to_dry(
        self, conn: sqlite3.Connection
    ) -> None:
        portfolio = _StubPortfolio(
            sleeve_labels=["v6_5m"], default_modes={"v6_5m": "WAFFLE"}
        )
        out = seed_wallet_modes(conn, portfolio)
        assert out == {"v6_5m": "DRY"}

    def test_seed_is_idempotent_does_not_overwrite_operator_choice(
        self, conn: sqlite3.Connection
    ) -> None:
        # First boot seeds v6_5m=DRY. Operator flips to LIVE. Second boot
        # must NOT overwrite that choice — `seed_wallet_modes` uses
        # INSERT OR IGNORE.
        portfolio = _portfolio_one_wallet()
        seed_wallet_modes(conn, portfolio)
        start_processing(conn)
        set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            actor="test",
            guard_live_action_context=_accept,
        )

        # Re-seed (simulates restart). Portfolio default is DRY again.
        seed_wallet_modes(conn, portfolio)
        assert get_wallet_mode(conn, "v6_5m").mode == "LIVE"

    def test_seed_prunes_wallets_missing_from_loaded_portfolio(
        self, conn: sqlite3.Connection
    ) -> None:
        seed_wallet_modes(conn, _portfolio_four_wallets())

        out = seed_wallet_modes(conn, _portfolio_exp044_wallets())

        assert out == {"exp044_5m": "DRY", "exp044_15m": "DRY"}
        assert load_wallet_mode_state(conn) == {
            "exp044_5m": "DRY",
            "exp044_15m": "DRY",
        }
        for wallet_id in ("v6_5m", "v6_15m", "pairs_5m", "pairs_15m"):
            with pytest.raises(OperatorControlError, match="row missing"):
                get_wallet_mode(conn, wallet_id)
        rows = conn.execute(
            """
            SELECT scope, action, from_state, to_state, actor, reason
            FROM operator_control_events
            WHERE action='prune_stale_wallet'
            ORDER BY scope
            """
        ).fetchall()
        assert {row["scope"] for row in rows} == {
            "wallet:pairs_15m",
            "wallet:pairs_5m",
            "wallet:v6_15m",
            "wallet:v6_5m",
        }
        assert all(row["from_state"] == "DRY" for row in rows)
        assert all(row["to_state"] is None for row in rows)
        assert all(row["actor"] == "boot" for row in rows)
        assert all(row["reason"] == "wallet not in loaded portfolio" for row in rows)


class TestLoadWalletModeState:
    def test_bulk_load_returns_dict(self, conn: sqlite3.Connection) -> None:
        seed_wallet_modes(conn, _portfolio_four_wallets())
        out = load_wallet_mode_state(conn)
        assert out == {
            "v6_5m": "DRY",
            "v6_15m": "DRY",
            "pairs_5m": "DRY",
            "pairs_15m": "DRY",
        }

    def test_empty_db_returns_empty_dict(self, conn: sqlite3.Connection) -> None:
        assert load_wallet_mode_state(conn) == {}


# ============================================================================
# Setting wallet mode
# ============================================================================


class TestSetWalletMode:
    def test_unknown_wallet_raises(self, conn: sqlite3.Connection) -> None:
        with pytest.raises(OperatorControlError, match="row missing"):
            set_wallet_mode(conn, "not_a_wallet", "LIVE", guard_live_action_context=_accept)

    def test_invalid_target_raises(self, conn: sqlite3.Connection) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        with pytest.raises(OperatorControlError, match="DRY or LIVE"):
            set_wallet_mode(conn, "v6_5m", "WAFFLE")

    def test_same_mode_is_noop(self, conn: sqlite3.Connection) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        before = get_wallet_mode(conn, "v6_5m")
        out = set_wallet_mode(conn, "v6_5m", "DRY", guard_live_action_context=_accept)
        # No version bump on a no-op flip.
        assert out.mode == "DRY"
        assert out.version == before.version

    def test_dry_to_live_flips_when_guard_accepts(
        self, conn: sqlite3.Connection
    ) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        out = set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            actor="dashboard",
            reason="phase 3 canary",
            guard_live_action_context=_accept,
        )
        assert out.mode == "LIVE"
        assert out.version == 1
        assert out.updated_by == "dashboard"

    def test_dry_to_live_blocked_when_guard_rejects(
        self, conn: sqlite3.Connection
    ) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        with pytest.raises(BlockedError) as exc:
            set_wallet_mode(
                conn, "v6_5m", "LIVE", guard_live_action_context=_reject
            )
        assert exc.value.reason == "no_readiness_report"
        # Mode is unchanged because the guard rejected.
        assert get_wallet_mode(conn, "v6_5m").mode == "DRY"

    def test_live_to_dry_always_allowed_even_when_guard_would_reject(
        self, conn: sqlite3.Connection
    ) -> None:
        """D-LIVE-5: emergency halt path. LIVE → DRY bypasses the guard
        entirely because halting is always safer than continuing."""
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        set_wallet_mode(conn, "v6_5m", "LIVE", guard_live_action_context=_accept)
        # Even though the guard would reject, LIVE → DRY succeeds.
        out = set_wallet_mode(
            conn,
            "v6_5m",
            "DRY",
            actor="dashboard",
            guard_live_action_context=_reject,
        )
        assert out.mode == "DRY"

    def test_force_bypasses_dry_to_live_rate_limit_but_not_guard(
        self, conn: sqlite3.Connection
    ) -> None:
        """`force` only bypasses the cooldown — the readiness guard still
        fires (defense-in-depth)."""
        seed_wallet_modes(conn, _portfolio_one_wallet())
        with pytest.raises(BlockedError):
            set_wallet_mode(
                conn,
                "v6_5m",
                "LIVE",
                force=True,
                guard_live_action_context=_reject,
            )


class TestOptimisticConcurrency:
    def test_stale_version_raises(self, conn: sqlite3.Connection) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        # Reader A reads version=0. Writer B flips to LIVE.
        set_wallet_mode(
            conn, "v6_5m", "LIVE", guard_live_action_context=_accept
        )
        # Reader A submits with stale version.
        with pytest.raises(ConcurrentMutationError, match="version mismatch"):
            set_wallet_mode(
                conn,
                "v6_5m",
                "DRY",
                expected_version=0,
                guard_live_action_context=_accept,
            )


class TestRateLimit:
    def test_rapid_dry_to_live_blocked_within_cooldown(
        self, conn: sqlite3.Connection
    ) -> None:
        """D-LIVE-19: 60s cooldown on DRY → LIVE flips. Two flips of the
        same wallet within the window raise RateLimitError unless --force."""
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        # First flip: DRY → LIVE.
        set_wallet_mode(
            conn, "v6_5m", "LIVE", guard_live_action_context=_accept
        )
        # Revert.
        set_wallet_mode(
            conn, "v6_5m", "DRY", guard_live_action_context=_accept
        )
        # Second DRY → LIVE within the cooldown.
        with pytest.raises(RateLimitError, match="cooldown"):
            set_wallet_mode(
                conn,
                "v6_5m",
                "LIVE",
                guard_live_action_context=_accept,
            )

    def test_force_bypasses_cooldown(self, conn: sqlite3.Connection) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        set_wallet_mode(
            conn, "v6_5m", "LIVE", guard_live_action_context=_accept
        )
        set_wallet_mode(
            conn, "v6_5m", "DRY", guard_live_action_context=_accept
        )
        out = set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            force=True,
            guard_live_action_context=_accept,
        )
        assert out.mode == "LIVE"

    def test_no_cooldown_on_live_to_dry(
        self, conn: sqlite3.Connection
    ) -> None:
        """LIVE → DRY is always allowed instantaneously (emergency halt)."""
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        set_wallet_mode(
            conn, "v6_5m", "LIVE", guard_live_action_context=_accept
        )
        set_wallet_mode(
            conn, "v6_5m", "DRY", guard_live_action_context=_accept
        )
        # Three more LIVE → DRY-after-promote cycles back-to-back are fine.
        # (We use force=True so the cooldown doesn't interfere with the
        # LIVE half — the assertion is on the DRY side.)
        for _ in range(3):
            set_wallet_mode(
                conn,
                "v6_5m",
                "LIVE",
                force=True,
                guard_live_action_context=_accept,
            )
            set_wallet_mode(
                conn, "v6_5m", "DRY", guard_live_action_context=_accept
            )
        # The final state is DRY and no exception was raised.
        assert get_wallet_mode(conn, "v6_5m").mode == "DRY"

    def test_short_explicit_rate_limit_zero_disables_cooldown(
        self, conn: sqlite3.Connection
    ) -> None:
        """Test fixture knob: `rate_limit_s=0` disables the cooldown for
        tests that need rapid flips without setting `force=True` every time."""
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            guard_live_action_context=_accept,
            rate_limit_s=0,
        )
        set_wallet_mode(
            conn,
            "v6_5m",
            "DRY",
            guard_live_action_context=_accept,
            rate_limit_s=0,
        )
        set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            guard_live_action_context=_accept,
            rate_limit_s=0,
        )


class TestAuditEvents:
    def test_flip_writes_wallet_scoped_event(
        self, conn: sqlite3.Connection
    ) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            actor="cli",
            reason="canary",
            guard_live_action_context=_accept,
        )
        event = conn.execute(
            "SELECT scope, action, from_state, to_state, actor, reason "
            "FROM operator_control_events "
            "WHERE scope='wallet:v6_5m' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert event is not None
        assert event["scope"] == "wallet:v6_5m"
        assert event["action"] == "mode_flip"
        assert event["from_state"] == "DRY"
        assert event["to_state"] == "LIVE"
        assert event["actor"] == "cli"
        assert event["reason"] == "canary"

    def test_force_flag_recorded_in_detail(self, conn: sqlite3.Connection) -> None:
        import json

        seed_wallet_modes(conn, _portfolio_one_wallet())
        start_processing(conn)
        # Drive into LIVE first then trigger the cooldown bypass.
        set_wallet_mode(
            conn, "v6_5m", "LIVE", guard_live_action_context=_accept
        )
        set_wallet_mode(
            conn, "v6_5m", "DRY", guard_live_action_context=_accept
        )
        set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            force=True,
            guard_live_action_context=_accept,
        )
        event = conn.execute(
            "SELECT detail FROM operator_control_events "
            "WHERE scope='wallet:v6_5m' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        detail = json.loads(event["detail"])
        assert detail == {"force": True}


class TestGlobalStateInterplay:
    """D-LIVE-5 + D-LIVE-19: the per-wallet guard composes with global state.

    These tests use the REAL guard (`require_wallet_can_go_live`) so we
    cover the integration path the dashboard hits.
    """

    def _seed_readiness_and_recon(self, conn: sqlite3.Connection) -> None:
        """Insert fresh READY rows so the guard's table reads pass."""
        from cryptobot.db.connection import utc_now_sql

        now = utc_now_sql()
        conn.execute(
            "INSERT INTO live_readiness_reports (created_at, status, report_json) "
            "VALUES (?, 'READY', '{}')",
            (now,),
        )
        conn.execute(
            "INSERT INTO wallet_reconciliation_runs "
            "(sampled_at, source_status) VALUES (?, 'ok')",
            (now,),
        )
        conn.commit()

    def test_dry_to_live_blocked_when_operator_stopped(
        self, conn: sqlite3.Connection
    ) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        self._seed_readiness_and_recon(conn)
        # operator_state still STOPPED_PROCESSING by default.
        with pytest.raises(BlockedError) as exc:
            set_wallet_mode(conn, "v6_5m", "LIVE")
        assert exc.value.reason == "operator_stopped"

    def test_dry_to_live_blocked_when_transfer_lock(
        self, conn: sqlite3.Connection
    ) -> None:
        from cryptobot.execution.operator_control import enter_transfer_lock

        seed_wallet_modes(conn, _portfolio_one_wallet())
        self._seed_readiness_and_recon(conn)
        start_processing(conn)
        enter_transfer_lock(conn)
        with pytest.raises(BlockedError) as exc:
            set_wallet_mode(conn, "v6_5m", "LIVE")
        assert exc.value.reason == "transfer_lock"

    def test_dry_to_live_passes_with_fresh_readiness_and_live_state(
        self, conn: sqlite3.Connection
    ) -> None:
        seed_wallet_modes(conn, _portfolio_one_wallet())
        self._seed_readiness_and_recon(conn)
        start_processing(conn)
        out = set_wallet_mode(conn, "v6_5m", "LIVE")
        assert out.mode == "LIVE"
