"""U10 verification: per-strategy transfer preview + submit + watchdog.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U10 / D-LIVE-21:
  - preview_transfer refuses amount > extractable_pnl for the source wallet.
  - submit_transfer binds the idempotency_key from a prior preview
    (mismatch raises preview_mismatch).
  - submit_transfer transitions operator state into TRANSFER_LOCK and back
    to LIVE on completion.
  - Concurrent submit on the same from_wallet raises transfer_in_progress.

The stale-TRANSFER_LOCK watchdog's own cases live in core's
test_operator_control.py, alongside the function (panel verdict D4-1).
"""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot.db import init_db
from cryptobot.execution.operator_control import (
    LIVE,
    TRANSFER_LOCK,
    get_operator_state,
    seed_operator_control_state,
    start_processing,
)
from cryptobot_polymarket.transfer import (
    TransferError,
    TransferPreview,
    preview_transfer,
    submit_transfer,
)


_TO_WALLET = "0xreturn"  # default allowlist used by every test in this module


def _submit(conn, **kwargs):
    """submit_transfer wrapper that defaults to the test allowlist so the
    14 existing call sites don't each need to re-pass the kwarg."""
    kwargs.setdefault("to_wallet_allowlist", _TO_WALLET)
    return submit_transfer(conn, **kwargs)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    seed_operator_control_state(db, boot_dry_run=False)
    start_processing(db)
    yield db
    db.close()


def _insert_settled_live_row(
    conn: sqlite3.Connection,
    *,
    wallet_id: str,
    pnl_net: float,
    outcome: str = "WIN",
) -> None:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side, window_start_ts,
           window_end_ts, interval_min, poly_market_id, poly_token_id,
           fee_rate_bps, model_p, p_market, fill_price, realized_edge,
           target_usd, bet_size, entry_top_price, config_sha256, mode,
           outcome, pnl_net)
        VALUES ('2026-05-24', 'BTC', ?, ?, 4, 'YES', 1715923200,
                1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55, 0.55, 0.10,
                5.0, 5.0, 0.55, 'sha', 'LIVE', ?, ?)
        """,
        (wallet_id, wallet_id, outcome, pnl_net),
    )
    conn.commit()


# ============================================================================
# preview_transfer
# ============================================================================


class TestPreviewTransfer:
    def test_inserts_preview_row_without_lock(self, conn) -> None:
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=5.0)
        preview = preview_transfer(
            conn,
            from_wallet="0xfunder",
            to_wallet="0xreturn",
            to_wallet_allowlist=_TO_WALLET,
            amount_usd=3.0,
            source_wallet_id="v6_5m",
            current_balance_usd=200.0,
        )
        assert preview.amount_usd == 3.0
        assert preview.source_wallet_id == "v6_5m"
        assert preview.extractable_pnl_usd == 5.0
        # No state change.
        state = get_operator_state(conn)
        assert state.state == LIVE

    def test_refuses_amount_above_extractable_pnl(self, conn) -> None:
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=2.0)
        with pytest.raises(TransferError) as exc:
            preview_transfer(
                conn,
                from_wallet="0xfunder",
                to_wallet="0xreturn",
                to_wallet_allowlist=_TO_WALLET,
                amount_usd=5.0,
                source_wallet_id="v6_5m",
            )
        assert exc.value.reason == "insufficient_extractable_pnl"

    def test_extractable_subtracts_prior_confirmed_transfers(
        self, conn
    ) -> None:
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=10.0)
        # Prior confirmed transfer for $4 reduces extractable to $6.
        conn.execute(
            "INSERT INTO transfer_requests (actor, from_wallet, to_wallet, "
            "source_wallet_id, amount_usd, status, tx_hash) VALUES "
            "('dashboard', '0xfunder', '0xreturn', 'v6_5m', 4.0, 'CONFIRMED', '0xprior')"
        )
        conn.commit()
        # Asking for $7 fails (extractable=$6).
        with pytest.raises(TransferError) as exc_info:
            preview_transfer(
                conn,
                from_wallet="0xfunder",
                to_wallet="0xreturn",
                to_wallet_allowlist=_TO_WALLET,
                amount_usd=7.0,
                source_wallet_id="v6_5m",
            )
        assert exc_info.value.reason == "insufficient_extractable_pnl"
        # Asking for $6 passes.
        preview = preview_transfer(
            conn,
            from_wallet="0xfunder",
            to_wallet="0xreturn",
            to_wallet_allowlist=_TO_WALLET,
            amount_usd=6.0,
            source_wallet_id="v6_5m",
        )
        assert preview.extractable_pnl_usd == 6.0

    def test_whole_funder_null_source_is_rejected(self, conn) -> None:
        """Audit P0 #1: the legacy whole-funder path (`source_wallet_id=None`)
        is disabled. A CONFIRMED NULL-source transfer was never subtracted
        from extractable PnL (capital.extractable_pnl_by_wallet filters
        `source_wallet_id IS NOT NULL`), so the same realized profit could be
        withdrawn repeatedly. The preview must now refuse a NULL source."""
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=10.0)
        _insert_settled_live_row(conn, wallet_id="v6_15m", pnl_net=-3.0)
        with pytest.raises(TransferError) as exc:
            preview_transfer(
                conn,
                from_wallet="0xfunder",
                to_wallet="0xreturn",
                to_wallet_allowlist=_TO_WALLET,
                amount_usd=8.0,
                source_wallet_id=None,
            )
        assert exc.value.reason == "source_wallet_id_required"
        # No PREVIEW row was staged.
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM transfer_requests"
        ).fetchone()["n"]
        assert n == 0

    def test_submit_rejects_prefix_null_source_preview_row(self, conn) -> None:
        """A PREVIEW row staged before the fix (NULL source_wallet_id) must
        be failed closed at submit, not drained."""
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=50.0)
        key = "legacy-null-source-key"
        conn.execute(
            "INSERT INTO transfer_requests (actor, from_wallet, to_wallet, "
            "source_wallet_id, amount_usd, status, idempotency_key, "
            "preview_json) VALUES "
            "('dashboard','0xfunder',?,NULL,5.0,'PREVIEW',?,'{}')",
            (_TO_WALLET, key),
        )
        conn.commit()
        with pytest.raises(TransferError) as exc:
            submit_transfer(
                conn,
                idempotency_key=key,
                relayer_submitter=lambda payload: {"tx_hash": "0xdead"},
                to_wallet_allowlist=_TO_WALLET,
            )
        assert exc.value.reason == "source_wallet_id_required"
        row = conn.execute(
            "SELECT status, error FROM transfer_requests WHERE idempotency_key=?",
            (key,),
        ).fetchone()
        assert row["status"] == "FAILED"
        assert row["error"] == "source_wallet_id_required"

    def test_rejects_invalid_amount(self, conn) -> None:
        with pytest.raises(TransferError) as exc_info:
            preview_transfer(
                conn,
                from_wallet="0xfunder",
                to_wallet="0xreturn",
                to_wallet_allowlist=_TO_WALLET,
                amount_usd=0.0,
            )
        assert exc_info.value.reason == "invalid_amount"

    def test_rejects_when_allowlist_unconfigured(self, conn) -> None:
        """Security finding #2: when POLY_RETURN_WALLET is unset
        (to_wallet_allowlist empty/None), every preview is refused. The
        operator must opt into transfers by configuring the destination."""
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=5.0)
        with pytest.raises(TransferError) as exc:
            preview_transfer(
                conn,
                from_wallet="0xfunder",
                to_wallet="0xreturn",
                to_wallet_allowlist="",
                amount_usd=3.0,
                source_wallet_id="v6_5m",
            )
        assert exc.value.reason == "to_wallet_not_configured"
        # No row was inserted (defense in depth verified by DB state).
        n = conn.execute("SELECT COUNT(*) AS n FROM transfer_requests").fetchone()["n"]
        assert n == 0

    def test_rejects_when_to_wallet_does_not_match_allowlist(self, conn) -> None:
        """Security finding #2: arbitrary to_wallet from the POST body is
        rejected with `to_wallet_not_allowed` BEFORE any DB write."""
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=5.0)
        with pytest.raises(TransferError) as exc:
            preview_transfer(
                conn,
                from_wallet="0xfunder",
                to_wallet="0xATTACKER",
                to_wallet_allowlist=_TO_WALLET,
                amount_usd=3.0,
                source_wallet_id="v6_5m",
            )
        assert exc.value.reason == "to_wallet_not_allowed"
        n = conn.execute("SELECT COUNT(*) AS n FROM transfer_requests").fetchone()["n"]
        assert n == 0


# ============================================================================
# submit_transfer
# ============================================================================


class TestSubmitTransfer:
    def _seed_preview(
        self, conn, *, wallet_id="v6_5m", pnl=5.0, amount=3.0
    ) -> TransferPreview:
        _insert_settled_live_row(conn, wallet_id=wallet_id, pnl_net=pnl)
        return preview_transfer(
            conn,
            from_wallet="0xfunder",
            to_wallet="0xreturn",
            to_wallet_allowlist=_TO_WALLET,
            amount_usd=amount,
            source_wallet_id=wallet_id,
            current_balance_usd=200.0,
        )

    def test_happy_path_confirms_and_restores_live(self, conn) -> None:
        preview = self._seed_preview(conn)
        relayer = lambda payload: {
            "tx_hash": "0xtxA",
            "relayer_tx_id": "rly-1",
            "success": True,
        }
        result = _submit(
            conn,
            idempotency_key=preview.idempotency_key,
            relayer_submitter=relayer,
        )
        assert result.status == "CONFIRMED"
        assert result.tx_hash == "0xtxA"
        # Operator state restored to LIVE.
        assert get_operator_state(conn).state == LIVE

    def test_relayer_tx_id_without_hash_marks_submitted(self, conn) -> None:
        preview = self._seed_preview(conn)
        result = _submit(
            conn,
            idempotency_key=preview.idempotency_key,
            relayer_submitter=lambda _payload: {
                "transactionID": "relayer-123",
                "success": True,
            },
        )

        assert result.status == "SUBMITTED"
        assert result.tx_hash is None
        assert result.relayer_tx_id == "relayer-123"
        assert get_operator_state(conn).state == LIVE
        row = conn.execute(
            "SELECT status, relayer_tx_id, tx_hash FROM transfer_requests WHERE id=?",
            (preview.preview_id,),
        ).fetchone()
        assert row["status"] == "SUBMITTED"
        assert row["relayer_tx_id"] == "relayer-123"
        assert row["tx_hash"] is None

    def test_sequential_double_submit_blocked_by_extractable_recheck(
        self, conn
    ) -> None:
        """Audit w19oxqf24 finding #13 (P2): two PREVIEWs each staged at the
        full extractable amount, submitted SEQUENTIALLY, must not over-extract.
        After the first CONFIRMs (clearing SUBMITTING so the unique index no
        longer blocks), the second submit must be refused by the
        extractable-PnL re-check, since the first transfer consumed the PnL."""
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=5.0)
        # Stage TWO previews, each for the full $5 extractable.
        preview_a = preview_transfer(
            conn, from_wallet="0xfunder", to_wallet="0xreturn",
            to_wallet_allowlist=_TO_WALLET, amount_usd=5.0,
            source_wallet_id="v6_5m", current_balance_usd=200.0,
        )
        preview_b = preview_transfer(
            conn, from_wallet="0xfunder", to_wallet="0xreturn",
            to_wallet_allowlist=_TO_WALLET, amount_usd=5.0,
            source_wallet_id="v6_5m", current_balance_usd=200.0,
        )
        relayer = lambda payload: {"tx_hash": "0xtxA", "success": True}

        # First submit confirms and consumes the $5 extractable.
        result_a = _submit(
            conn, idempotency_key=preview_a.idempotency_key,
            relayer_submitter=relayer,
        )
        assert result_a.status == "CONFIRMED"
        assert get_operator_state(conn).state == LIVE  # SUBMITTING cleared

        # Second submit must now be refused — the prior CONFIRMED transfer
        # drove extractable to $0.
        with pytest.raises(TransferError) as exc:
            _submit(
                conn, idempotency_key=preview_b.idempotency_key,
                relayer_submitter=relayer,
            )
        assert exc.value.reason == "insufficient_extractable_pnl"
        # Operator state is not left stuck in TRANSFER_LOCK.
        assert get_operator_state(conn).state == LIVE

    def test_submit_acquires_and_releases_transfer_lock(self, conn) -> None:
        preview = self._seed_preview(conn)
        observed_state: list[str] = []

        def _watching_relayer(payload):
            observed_state.append(get_operator_state(conn).state)
            return {"tx_hash": "0xtxA", "success": True}

        _submit(
            conn,
            idempotency_key=preview.idempotency_key,
            relayer_submitter=_watching_relayer,
        )
        # During the relayer call, state was TRANSFER_LOCK.
        assert observed_state == [TRANSFER_LOCK]
        # After, restored to LIVE.
        assert get_operator_state(conn).state == LIVE

    def test_relayer_failure_releases_lock(self, conn) -> None:
        preview = self._seed_preview(conn)
        relayer = lambda payload: {"success": False, "error": "relayer down"}
        result = _submit(
            conn,
            idempotency_key=preview.idempotency_key,
            relayer_submitter=relayer,
        )
        assert result.status == "FAILED"
        # Lock released even on failure.
        assert get_operator_state(conn).state == LIVE
        # Row marked FAILED.
        row = conn.execute(
            "SELECT status, error FROM transfer_requests WHERE id=?",
            (result.request_id,),
        ).fetchone()
        assert row["status"] == "FAILED"
        assert "relayer down" in (row["error"] or "")

    def test_relayer_exception_releases_lock_and_raises(self, conn) -> None:
        preview = self._seed_preview(conn)

        def _boom(payload):
            raise RuntimeError("network")

        with pytest.raises(TransferError) as exc:
            _submit(
                conn,
                idempotency_key=preview.idempotency_key,
                relayer_submitter=_boom,
            )
        assert exc.value.reason == "relayer_error"
        # Lock still released.
        assert get_operator_state(conn).state == LIVE

    def test_missing_idempotency_key_raises(self, conn) -> None:
        with pytest.raises(TransferError, match="missing_idempotency_key"):
            _submit(
                conn,
                idempotency_key="",
                relayer_submitter=lambda _p: {"tx_hash": "x"},
            )

    def test_unknown_idempotency_key_raises(self, conn) -> None:
        with pytest.raises(TransferError) as exc:
            _submit(
                conn,
                idempotency_key="ghost",
                relayer_submitter=lambda _p: {"tx_hash": "x"},
            )
        assert exc.value.reason == "preview_not_found"

    def test_preview_consumed_blocks_resubmit(self, conn) -> None:
        preview = self._seed_preview(conn)
        relayer = lambda _p: {"tx_hash": "0xA", "success": True}
        _submit(
            conn,
            idempotency_key=preview.idempotency_key,
            relayer_submitter=relayer,
        )
        # Second submit with same key is rejected.
        with pytest.raises(TransferError) as exc:
            _submit(
                conn,
                idempotency_key=preview.idempotency_key,
                relayer_submitter=relayer,
            )
        assert exc.value.reason == "preview_consumed"

    def test_submit_rejects_when_preview_to_wallet_drifts_from_allowlist(
        self, conn
    ) -> None:
        """Security finding #2 defense-in-depth: even if a PREVIEW row was
        created when the allowlist was looser (or absent), submit
        re-verifies the stored `to_wallet` against the current allowlist
        and refuses on mismatch."""
        preview = self._seed_preview(conn)
        # Mutate the PREVIEW row to simulate a pre-allowlist insert.
        conn.execute(
            "UPDATE transfer_requests SET to_wallet='0xATTACKER' WHERE id=?",
            (preview.preview_id,),
        )
        conn.commit()
        with pytest.raises(TransferError) as exc:
            _submit(
                conn,
                idempotency_key=preview.idempotency_key,
                relayer_submitter=lambda _p: {"tx_hash": "x"},
            )
        assert exc.value.reason == "to_wallet_not_allowed"

    def test_submit_rejects_when_allowlist_unconfigured(self, conn) -> None:
        """If POLY_RETURN_WALLET is unset at submit time, refuse even if a
        PREVIEW row exists (consistent with preview's behavior)."""
        preview = self._seed_preview(conn)
        with pytest.raises(TransferError) as exc:
            submit_transfer(
                conn,
                idempotency_key=preview.idempotency_key,
                relayer_submitter=lambda _p: {"tx_hash": "x"},
                to_wallet_allowlist="",
            )
        assert exc.value.reason == "to_wallet_not_configured"

    def test_submit_relayer_hang_holds_transfer_lock(
        self, conn, monkeypatch
    ) -> None:
        """Audit P1 #7: a relayer timeout leaves the transfer in-flight with
        unknown on-chain state. The row stays SUBMITTING AND the operator is
        HELD in TRANSFER_LOCK (NOT restored to LIVE) — the bot must not resume
        trading against a funder balance that may be about to drop by the
        transfer amount. A CRITICAL health row makes the held state visible.
        (Reliability finding #5 still holds: PostRouter._lock is released by
        the 30s ThreadPoolExecutor envelope; this is the OPERATOR lock.)"""
        from cryptobot_polymarket import transfer as mod
        from cryptobot.execution.operator_control import TRANSFER_LOCK

        # Shrink the timeout for the test so we don't actually wait 30s.
        monkeypatch.setattr(mod, "RELAYER_TIMEOUT_S", 0.2)

        preview = self._seed_preview(conn)
        import time as _time

        def _hang(_payload):
            _time.sleep(2.0)
            return {"tx_hash": "x"}

        with pytest.raises(TransferError) as exc:
            _submit(
                conn,
                idempotency_key=preview.idempotency_key,
                relayer_submitter=_hang,
            )
        assert exc.value.reason == "relayer_timeout"
        # Operator HELD in TRANSFER_LOCK, not restored to LIVE.
        assert get_operator_state(conn).state == TRANSFER_LOCK
        row = conn.execute(
            "SELECT status, error FROM transfer_requests WHERE id=?",
            (preview.preview_id,),
        ).fetchone()
        assert row["status"] == "SUBMITTING"
        assert row["error"] == "relayer_timeout"
        # The held state is escalated loudly.
        health = conn.execute(
            "SELECT COUNT(*) FROM pipeline_health WHERE stage='transfer' "
            "AND status='CRITICAL'"
        ).fetchone()[0]
        assert health >= 1

        # A second submit cannot proceed while the operator is locked, so a
        # timeout retry cannot double-send funds.
        retry = self._seed_preview(conn, pnl=10.0, amount=1.0)
        with pytest.raises(TransferError) as retry_exc:
            _submit(
                conn,
                idempotency_key=retry.idempotency_key,
                relayer_submitter=lambda _p: {"tx_hash": "x"},
            )
        assert retry_exc.value.reason == "operator_not_live"

    def test_operator_not_live_blocks_submit(self, conn) -> None:
        preview = self._seed_preview(conn)
        # Switch to STOPPED.
        from cryptobot.execution.operator_control import stop_processing

        stop_processing(conn)
        with pytest.raises(TransferError) as exc_info:
            _submit(
                conn,
                idempotency_key=preview.idempotency_key,
                relayer_submitter=lambda _p: {"tx_hash": "x"},
            )
        assert exc_info.value.reason == "operator_not_live"


# ============================================================================
# Concurrent submit
# ============================================================================


class TestConcurrentSubmit:
    def test_second_submitting_for_same_from_wallet_blocked(
        self, conn
    ) -> None:
        """The partial unique index on transfer_requests(from_wallet)
        WHERE status='SUBMITTING' prevents two transfers from the same
        funder racing."""
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=10.0)
        preview_a = preview_transfer(
            conn,
            from_wallet="0xfunder",
            to_wallet="0xreturn",
            to_wallet_allowlist=_TO_WALLET,
            amount_usd=2.0,
            source_wallet_id="v6_5m",
        )
        # Seed another SUBMITTING row directly to simulate a parallel
        # in-flight transfer.
        conn.execute(
            "INSERT INTO transfer_requests (actor, from_wallet, to_wallet, "
            "amount_usd, status) VALUES "
            "('manual', '0xfunder', '0xreturn', 1.0, 'SUBMITTING')"
        )
        conn.commit()
        with pytest.raises(TransferError) as exc:
            _submit(
                conn,
                idempotency_key=preview_a.idempotency_key,
                relayer_submitter=lambda _p: {"tx_hash": "x"},
            )
        assert exc.value.reason == "transfer_in_progress"


# ============================================================================
# Watchdog
#
# TestReleaseStaleTransferLock MOVED with its subject to
# core/src/cryptobot/tests/test_operator_control.py (panel verdict D4-1) —
# release_stale_transfer_lock is core's now.
# ============================================================================
