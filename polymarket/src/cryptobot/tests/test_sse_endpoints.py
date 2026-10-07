"""U12 SSE POST endpoints + PostRouter tests.

Exercises the dashboard mutation surface end-to-end at the router
level (no HTTP needed):

  * mode toggle (200 / 401 / 403 / 409)
  * operator start/stop/transfer-lock transitions
  * transfer preview + submit binding (D-LIVE-21)
  * cookie auth (SameSite=Strict; HttpOnly; Path=/)
"""

from __future__ import annotations

import sqlite3
from http import HTTPStatus
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    boot_operator_state,
    set_wallet_mode,
    start_processing,
    stop_processing,
)
from cryptobot.observability.sse_server import (
    PostRouter,
    derive_session_token,
)
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    conn = init_db(tmp_path / "router.db")
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    yield conn
    conn.close()


@pytest.fixture
def portfolio():
    return legacy_portfolio()


def _router(
    db: sqlite3.Connection,
    portfolio,
    *,
    password: str = "secret",
    relayer=None,
    auth_disabled: bool = False,
    to_wallet_allowlist: str = "0xreturn",
) -> PostRouter:
    return PostRouter(
        conn=db,
        portfolio=portfolio,
        session_token=derive_session_token(password) if password else "",
        relayer_submitter=relayer,
        auth_disabled=auth_disabled,
        transfer_to_wallet_allowlist=to_wallet_allowlist,
    )


def _cookie_for(password: str) -> str:
    return f"session_token={derive_session_token(password)}"


def _seed_live_readiness(conn: sqlite3.Connection) -> None:
    now = utc_now_sql()
    conn.execute(
        "INSERT INTO live_readiness_reports (created_at, status, report_json) "
        "VALUES (?, 'READY', '{}')",
        (now,),
    )
    conn.execute(
        "INSERT INTO wallet_reconciliation_runs (sampled_at, source_status) "
        "VALUES (?, 'ok')",
        (now,),
    )
    conn.commit()


# ============================================================================
# Auth
# ============================================================================


class TestAuth:
    def test_missing_cookie_returns_401(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        router = _router(db, portfolio, password="secret")
        status, body = router.dispatch(
            "/api/v2/operator/start", {}, cookie_header=None,
        )
        assert status == HTTPStatus.UNAUTHORIZED
        assert body["reason"] == "unauthorized"

    def test_wrong_cookie_returns_401(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        router = _router(db, portfolio, password="secret")
        status, body = router.dispatch(
            "/api/v2/operator/start",
            {},
            cookie_header="session_token=wrong",
        )
        assert status == HTTPStatus.UNAUTHORIZED

    def test_valid_cookie_passes_auth(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        router = _router(db, portfolio, password="secret")
        status, body = router.dispatch(
            "/api/v2/operator/start",
            {"reason": "test"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["state"] == "LIVE"

    def test_empty_password_refuses_auth(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """Security finding #1: a boot with `CRYPTOBOT_DASHBOARD_PASS` unset
        no longer accepts unauthenticated POSTs by default. Loopback is not
        an access boundary because every process on the host shares it.
        Operator must either set a password or opt into auth_disabled
        explicitly via `CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1`, which main.py
        also requires a loopback bind for."""
        router = _router(db, portfolio, password="")
        status, body = router.dispatch(
            "/api/v2/operator/start", {}, cookie_header=None,
        )
        assert status == HTTPStatus.UNAUTHORIZED
        assert body["reason"] == "unauthorized"

    def test_auth_disabled_opt_in_skips_auth(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """`auth_disabled=True` (wired from
        `CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1`) is the explicit local-dev
        escape hatch. main.py refuses to set it when the bind is
        non-loopback, so the local-dev workflow stays viable without
        opening the production surface."""
        router = _router(db, portfolio, password="", auth_disabled=True)
        status, body = router.dispatch(
            "/api/v2/operator/start", {}, cookie_header=None,
        )
        assert status == HTTPStatus.OK


# ============================================================================
# Mode toggle
# ============================================================================


class TestModeToggle:
    def test_dry_to_live_succeeds_with_readiness(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        start_processing(db)
        _seed_live_readiness(db)
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/wallet/pairs_15m/mode",
            {"target": "LIVE", "reason": "canary"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["mode"] == "LIVE"
        assert body["version"] == 1

    def test_invalid_target_returns_400(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "MAYBE"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert body["reason"] == "invalid_target"

    def test_blocked_returns_403_with_reason(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """DRY â†’ LIVE with no readiness row â†’ BlockedError â†’ 403."""
        start_processing(db)  # state=LIVE, but no readiness rows yet
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "LIVE", "reason": "premature"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.FORBIDDEN
        assert body["reason"] == "no_readiness_report"

    def test_version_conflict_returns_409_with_current(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """Stale version â†’ 409 with the current state in the body."""
        start_processing(db)
        _seed_live_readiness(db)
        router = _router(db, portfolio)
        # A successful flip bumps version to 1.
        router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "LIVE"},
            cookie_header=_cookie_for("secret"),
        )
        # Retry with stale expected_version=0.
        status, body = router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "DRY", "expected_version": 0},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.CONFLICT
        assert body["reason"] == "version_conflict"
        assert body["current"]["version"] == 1

    def test_force_field_silently_dropped(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """D-LIVE-19: `force` is CLI-only; the dashboard POST body must
        NOT accept it. The router silently drops the field."""
        start_processing(db)
        _seed_live_readiness(db)
        router = _router(db, portfolio)
        # First flip lands; version becomes 1.
        router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "LIVE"},
            cookie_header=_cookie_for("secret"),
        )
        router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "DRY"},
            cookie_header=_cookie_for("secret"),
        )
        # Try to DRY â†’ LIVE within cooldown WITH `force: true` â€” the
        # router must ignore the field and the request should hit the
        # rate limiter.
        status, body = router.dispatch(
            "/api/v2/wallet/v6_5m/mode",
            {"target": "LIVE", "force": True},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.FORBIDDEN
        assert body["reason"] == "rate_limited"


# ============================================================================
# Operator controls
# ============================================================================


class TestOperatorControls:
    def test_start_transitions_to_live(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/operator/start",
            {"reason": "ready"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["state"] == "LIVE"

    def test_stop_transitions_to_stopped_processing(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        start_processing(db)
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/operator/stop",
            {"reason": "emergency"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["state"] == "STOPPED_PROCESSING"

    def test_transfer_lock_transitions_state(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        start_processing(db)
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/operator/transfer-lock",
            {"reason": "manual lock"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["state"] == "TRANSFER_LOCK"

    def test_stop_with_stale_expected_version_returns_409(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """Security finding #10: operator state mutations honor
        `expected_version`. Two concurrent POSTs cannot silently flip
        state in opposite directions -- the second loses with 409."""
        # version=1 after start_processing.
        start_processing(db, reason="boot")
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/operator/stop",
            {"reason": "stale", "expected_version": 0},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.CONFLICT
        assert body["reason"] == "version_conflict"
        assert body["current"]["version"] == 1

    def test_start_uses_router_actor_ignoring_body(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        """Security finding #15: HTTP callers cannot override `actor`.
        Body-supplied actor is silently ignored; the audit row reflects
        the router's immutable HTTP actor."""
        router = _router(db, portfolio)
        router.dispatch(
            "/api/v2/operator/start",
            {"reason": "claimed boot", "actor": "boot"},
            cookie_header=_cookie_for("secret"),
        )
        row = db.execute(
            "SELECT actor FROM operator_control_events "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["actor"] == "dashboard"


# ============================================================================
# Transfer preview / submit (D-LIVE-21)
# ============================================================================


def _seed_live_pnl(conn: sqlite3.Connection, wallet_id: str, amount: float) -> None:
    """Insert a settled LIVE WIN row so the wallet has extractable PnL."""
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
           window_start_ts, window_end_ts, interval_min, poly_market_id,
           poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
           realized_edge, target_usd, bet_size, entry_top_price,
           config_sha256, mode, outcome, pnl_net)
        VALUES (?, 'BTC', ?, ?, 4, 'YES', 1715923200, 1715923500, 5, 'M',
                'T', 100.0, 0.78, 0.55, 0.55, 0.10, 5.0, 5.0, 0.55,
                'sha-stub', 'LIVE', 'WIN', ?)
        """,
        (utc_now_sql(), wallet_id, wallet_id, amount),
    )
    conn.commit()


class TestTransferEndpoints:
    def test_preview_succeeds_with_extractable_pnl(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        _seed_live_pnl(db, "v6_5m", 10.0)
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/operator/transfer/preview",
            {
                "from_wallet": "0xfunder",
                "to_wallet": "0xreturn",
                "amount_usd": 5.0,
                "source_wallet_id": "v6_5m",
            },
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["amountUsd"] == 5.0
        assert body["sourceWalletId"] == "v6_5m"
        assert body["idempotencyKey"]

    def test_preview_rejects_above_extractable(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        _seed_live_pnl(db, "v6_5m", 3.0)
        router = _router(db, portfolio)
        status, body = router.dispatch(
            "/api/v2/operator/transfer/preview",
            {
                "from_wallet": "0xfunder",
                "to_wallet": "0xreturn",
                "amount_usd": 10.0,
                "source_wallet_id": "v6_5m",
            },
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert body["reason"] == "insufficient_extractable_pnl"

    def test_submit_without_relayer_returns_503(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        router = _router(db, portfolio, relayer=None)
        status, body = router.dispatch(
            "/api/v2/operator/transfer/submit",
            {"idempotency_key": "stub"},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.SERVICE_UNAVAILABLE
        assert body["reason"] == "relayer_not_configured"

    def test_submit_binds_idempotency_key_to_preview(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        _seed_live_pnl(db, "v6_5m", 10.0)
        start_processing(db)
        relayer_calls: list[dict] = []

        def fake_relayer(payload):
            relayer_calls.append(payload)
            return {"success": True, "tx_hash": "0xdeadbeef"}

        router = _router(db, portfolio, relayer=fake_relayer)
        # 1. preview
        _, preview_body = router.dispatch(
            "/api/v2/operator/transfer/preview",
            {
                "from_wallet": "0xfunder",
                "to_wallet": "0xreturn",
                "amount_usd": 5.0,
                "source_wallet_id": "v6_5m",
            },
            cookie_header=_cookie_for("secret"),
        )
        key = preview_body["idempotencyKey"]
        # 2. submit
        status, body = router.dispatch(
            "/api/v2/operator/transfer/submit",
            {"idempotency_key": key},
            cookie_header=_cookie_for("secret"),
        )
        assert status == HTTPStatus.OK
        assert body["status"] == "CONFIRMED"
        assert body["tx_hash"] == "0xdeadbeef"
        assert relayer_calls and relayer_calls[0]["idempotency_key"] == key


# ============================================================================
# Unknown route
# ============================================================================


def test_unknown_route_returns_404(db: sqlite3.Connection, portfolio) -> None:
    router = _router(db, portfolio)
    status, body = router.dispatch(
        "/api/v2/does/not/exist",
        {},
        cookie_header=_cookie_for("secret"),
    )
    assert status == HTTPStatus.NOT_FOUND
    assert body["reason"] == "not_found"
