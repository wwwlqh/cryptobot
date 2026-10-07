"""U5 verification: durable live_order_attempts lifecycle.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U5 / D-LIVE-28:

  - reserve_live_order_attempt INSERTs a SUBMITTING row before any CLOB call.
  - mark_attempt_submitted transitions SUBMITTING → SUBMITTED with the
    CLOB-side order_id.
  - link_attempt_to_ledger transitions → LINKED and releases the active-
    slot reservation (LINKED is outside the partial unique index).
  - mark_attempt_failed transitions → FAILED with the coarse error_kind.
  - stale_active_attempts returns reservations the reconciler must
    recover or release.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from cryptobot.db import init_db
from cryptobot.execution.ledger import BetSignal
from cryptobot.execution.order_attempts import (
    LiveOrderAttempt,
    SUBMITTING,
    SUBMITTED,
    LINKED,
    FAILED,
    get_attempt,
    link_attempt_to_ledger,
    make_client_order_id,
    mark_attempt_failed,
    mark_attempt_submitted,
    reserve_live_order_attempt,
    stale_active_attempts,
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    yield db
    db.close()


def _make_signal(
    *,
    wallet_id: str = "v6_5m",
    coin: str = "BTC",
    sleeve: str = "v6_5m",
    entry_minute: int = 4,
    side: str = "YES",
    window_start_ts: int = 1715923200,
    bet_size: float = 5.0,
    fill_price: float = 0.55,
    limit_price: float | None = None,
    poly_token_id: str = "token-yes",
) -> BetSignal:
    return BetSignal(
        coin=coin,
        sleeve=sleeve,
        entry_minute=entry_minute,
        side=side,
        window_start_ts=window_start_ts,
        window_end_ts=window_start_ts + 300,
        interval_min=5,
        poly_market_id="market-1",
        poly_token_id=poly_token_id,
        fee_rate_bps=100.0,
        model_p=0.78,
        p_market=fill_price,
        fill_price=fill_price,
        walked_vwap=fill_price,
        realized_edge=0.10,
        partial_ret_z_1h=None,
        lut_p_bucket=None,
        lut_hit_rate=None,
        lut_n_cum=None,
        target_usd=bet_size,
        bet_size=bet_size,
        slot_filled_pre=0.0,
        entry_top_price=fill_price,
        yes_pre1_anchor=fill_price,
        config_sha256="sha-stub",
        wallet_id=wallet_id,
        limit_price=limit_price,
    )


# ============================================================================
# make_client_order_id
# ============================================================================


class TestMakeClientOrderId:
    def test_returns_uuid4_hex(self) -> None:
        coid = make_client_order_id()
        assert isinstance(coid, str)
        assert len(coid) == 32  # uuid4 hex
        int(coid, 16)  # parses as hex

    def test_each_call_returns_unique_id(self) -> None:
        ids = {make_client_order_id() for _ in range(50)}
        assert len(ids) == 50


# ============================================================================
# reserve_live_order_attempt
# ============================================================================


class TestReserveLiveOrderAttempt:
    def test_inserts_submitting_row(self, conn: sqlite3.Connection) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        assert attempt.status == SUBMITTING
        assert attempt.wallet_id == "v6_5m"
        assert attempt.wallet_group == "group_5m"
        assert attempt.amount_usd == 5.0
        assert attempt.poly_market_id == "market-1"
        assert attempt.poly_token_id == "token-yes"
        assert attempt.price == 0.55
        assert attempt.ledger_id is None
        assert attempt.live_order_id is None

    def test_attempt_price_uses_executable_limit_price_when_present(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn,
            _make_signal(fill_price=0.50, limit_price=0.53),
            wallet_group="group_5m",
        )
        assert attempt.price == 0.53
        row = conn.execute(
            "SELECT price FROM live_order_attempts WHERE id=?",
            (attempt.id,),
        ).fetchone()
        assert row["price"] == 0.53

    def test_generates_unique_client_order_id_when_not_provided(
        self, conn: sqlite3.Connection
    ) -> None:
        a = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        b = reserve_live_order_attempt(
            conn,
            _make_signal(entry_minute=5),
            wallet_group="group_5m",
        )
        assert a.client_order_id != b.client_order_id

    def test_accepts_explicit_client_order_id(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn,
            _make_signal(),
            wallet_group="group_5m",
            client_order_id="custom-coid-1",
        )
        assert attempt.client_order_id == "custom-coid-1"

    def test_requires_wallet_id(self, conn: sqlite3.Connection) -> None:
        signal = _make_signal(wallet_id="")
        with pytest.raises(ValueError, match="wallet_id"):
            reserve_live_order_attempt(conn, signal, wallet_group="group_5m")

    def test_active_slot_unique_blocks_duplicate_submitting(
        self, conn: sqlite3.Connection
    ) -> None:
        """D-LIVE-28: only one SUBMITTING attempt per slot key — prevents
        a crash/retry from submitting two LIVE orders on the same slot."""
        reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        with pytest.raises(sqlite3.IntegrityError):
            reserve_live_order_attempt(
                conn, _make_signal(), wallet_group="group_5m"
            )

    def test_different_slots_can_coexist(
        self, conn: sqlite3.Connection
    ) -> None:
        a = reserve_live_order_attempt(
            conn,
            _make_signal(entry_minute=4),
            wallet_group="group_5m",
        )
        b = reserve_live_order_attempt(
            conn,
            _make_signal(entry_minute=5),
            wallet_group="group_5m",
        )
        c = reserve_live_order_attempt(
            conn,
            _make_signal(side="NO"),
            wallet_group="group_5m",
        )
        assert a.id != b.id != c.id

    def test_duplicate_client_order_id_raises(
        self, conn: sqlite3.Connection
    ) -> None:
        reserve_live_order_attempt(
            conn,
            _make_signal(),
            wallet_group="group_5m",
            client_order_id="dup-coid",
        )
        with pytest.raises(sqlite3.IntegrityError):
            reserve_live_order_attempt(
                conn,
                _make_signal(entry_minute=99),
                wallet_group="group_5m",
                client_order_id="dup-coid",
            )

    def test_signal_payload_serialized(self, conn: sqlite3.Connection) -> None:
        """The signal_json column persists the BetSignal so the reconciler
        can reconstruct intent after a crash."""
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        row = conn.execute(
            "SELECT signal_json FROM live_order_attempts WHERE id=?",
            (attempt.id,),
        ).fetchone()
        import json

        payload = json.loads(row["signal_json"])
        assert payload["coin"] == "BTC"
        assert payload["sleeve"] == "v6_5m"


# ============================================================================
# mark_attempt_submitted
# ============================================================================


class TestMarkAttemptSubmitted:
    def test_transitions_submitting_to_submitted_with_order_id(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_submitted(conn, attempt.id, "clob-order-A")
        row = get_attempt(conn, attempt.id)
        assert row is not None
        assert row.status == SUBMITTED
        assert row.live_order_id == "clob-order-A"

    def test_requires_non_empty_live_order_id(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        with pytest.raises(ValueError, match="live_order_id"):
            mark_attempt_submitted(conn, attempt.id, "")

    def test_raises_when_attempt_not_in_submitting(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_failed(conn, attempt.id, "auth")
        with pytest.raises(RuntimeError, match="not in SUBMITTING"):
            mark_attempt_submitted(conn, attempt.id, "clob-order-late")


# ============================================================================
# link_attempt_to_ledger
# ============================================================================


class TestLinkAttemptToLedger:
    def test_transitions_to_linked(self, conn: sqlite3.Connection) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_submitted(conn, attempt.id, "clob-order-Z")
        link_attempt_to_ledger(conn, attempt.id, ledger_id=42)
        row = get_attempt(conn, attempt.id)
        assert row is not None
        assert row.status == LINKED
        assert row.ledger_id == 42

    def test_linked_releases_active_slot_reservation(
        self, conn: sqlite3.Connection
    ) -> None:
        """After LINKED, a new SUBMITTING attempt for the same slot is allowed
        (the partial unique index drops LINKED rows)."""
        a = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_submitted(conn, a.id, "order-1")
        link_attempt_to_ledger(conn, a.id, ledger_id=1)
        # Subsequent attempt for the same slot key — allowed.
        b = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        assert b.status == SUBMITTING
        assert b.id != a.id


# ============================================================================
# mark_attempt_failed
# ============================================================================


class TestMarkAttemptFailed:
    def test_transitions_to_failed_with_error_kind(
        self, conn: sqlite3.Connection
    ) -> None:
        attempt = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_failed(
            conn, attempt.id, error_kind="market_closed", error="market closed"
        )
        row = get_attempt(conn, attempt.id)
        assert row is not None
        assert row.status == FAILED
        assert row.error_kind == "market_closed"
        assert row.error == "market closed"

    def test_failed_releases_active_slot_reservation(
        self, conn: sqlite3.Connection
    ) -> None:
        a = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_failed(conn, a.id, error_kind="auth")
        # Subsequent attempt for the same slot key — allowed.
        b = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        assert b.status == SUBMITTING
        assert b.id != a.id


# ============================================================================
# stale_active_attempts (reconciler input)
# ============================================================================


class TestStaleActiveAttempts:
    def test_returns_no_rows_when_all_fresh(
        self, conn: sqlite3.Connection
    ) -> None:
        reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        # Just-inserted rows aren't stale.
        assert stale_active_attempts(conn, older_than_seconds=10) == []

    def test_returns_rows_older_than_threshold(
        self, conn: sqlite3.Connection
    ) -> None:
        a = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        # Backdate the updated_at so the row is "stale".
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (a.id,),
        )
        conn.commit()
        stale = stale_active_attempts(conn, older_than_seconds=10)
        assert len(stale) == 1
        assert stale[0].id == a.id

    def test_excludes_linked_and_failed_rows(
        self, conn: sqlite3.Connection
    ) -> None:
        # Backdated LINKED row should not show up.
        a = reserve_live_order_attempt(
            conn, _make_signal(), wallet_group="group_5m"
        )
        mark_attempt_submitted(conn, a.id, "order-A")
        link_attempt_to_ledger(conn, a.id, ledger_id=1)
        conn.execute(
            "UPDATE live_order_attempts SET updated_at='2026-01-01 00:00:00' "
            "WHERE id=?",
            (a.id,),
        )
        conn.commit()
        assert stale_active_attempts(conn, older_than_seconds=10) == []

