"""U9 verification: live readiness evaluator + secret-redacting report_json.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U9 / D-LIVE-26b:
  - status=READY only when every check passes.
  - status=NOT_READY when CLOB balance below SUM(group_caps), chain drift
    exceeds tolerance, recon stale, or operator in TRANSFER_LOCK.
  - report_json is built from an EXPLICIT field whitelist — POLY_PRIVATE_KEY
    et al. never leak into the SQLite DB.
"""

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from typing import Optional

import pytest

from cryptobot.db import init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.live_readiness import (
    _REPORT_FIELD_WHITELIST,
    _derive_topology,
    evaluate_readiness,
    latest_readiness_report,
    read_erc20_balance,
)
from cryptobot.execution.operator_control import (
    enter_transfer_lock,
    seed_operator_control_state,
    start_processing,
)


_SIGNER = "0x" + "a" * 40
_DEPOSIT = "0x" + "b" * 40
_OTHER_DEPOSIT = "0x" + "c" * 40
_PRIVATE_KEY = "0x" + "1" * 64


def _topology_provider(_config):
    return SimpleNamespace(
        signer=_SIGNER,
        proxy="0x" + "d" * 40,
        safe="0x" + "e" * 40,
        deposit=_DEPOSIT,
    )


def _make_config(
    *,
    signature_type: int = 3,
    funder: str = _DEPOSIT,
    relayer_owner: str = _SIGNER,
    onchain_verify: bool = True,
    require_deposit_wallet: bool = True,
    private_key: str = _PRIVATE_KEY,
):
    from cryptobot.runtime_secrets import Config

    return Config(
        poly_private_key=private_key,
        poly_api_key="ak",
        poly_secret="as",
        poly_passphrase="ap",
        poly_signature_type=signature_type,
        poly_funder=funder,
        relayer_api_key_address=relayer_owner,
        polygon_rpc_url="https://polygon.example/rpc",
        live_onchain_verify_enabled=onchain_verify,
        live_require_deposit_wallet=require_deposit_wallet,
    )


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = init_db(":memory:")
    seed_operator_control_state(db, boot_dry_run=False)
    start_processing(db)
    yield db
    db.close()


def _seed_fresh_recon(conn: sqlite3.Connection) -> None:
    """Insert a fresh wallet_reconciliation_runs row so the freshness gate passes."""
    conn.execute(
        "INSERT INTO wallet_reconciliation_runs (sampled_at, source_status) "
        "VALUES (?, 'ok')",
        (utc_now_sql(),),
    )
    conn.commit()


def test_derive_topology_uses_configured_polygon_rpc(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_derive_wallets(private_key: str, *, relayer_url: str, rpc_url: str | None = None):
        seen["private_key"] = private_key
        seen["relayer_url"] = relayer_url
        seen["rpc_url"] = rpc_url
        return _topology_provider(None)

    from cryptobot_polymarket import primitives

    monkeypatch.setattr(primitives, "derive_wallets", fake_derive_wallets)

    topology = _derive_topology(_make_config())

    assert topology is not None
    assert seen == {
        "private_key": _PRIVATE_KEY,
        "relayer_url": "https://relayer-v2.polymarket.com",
        "rpc_url": "https://polygon.example/rpc",
    }


class _StubOrderClient:
    def __init__(self, balance: Optional[float]) -> None:
        self._balance = balance

    def check_balance(self) -> Optional[float]:
        return self._balance


class _StubPortfolio:
    def __init__(self, total_caps: float = 200.0) -> None:
        self._total = total_caps

    def all_group_caps_sum_usd(self) -> float:
        return self._total


class _RaisingPortfolio:
    """Portfolio whose floor/cap computation always raises — exercises the
    fail-closed branch of `_required_balance`."""

    def funder_balance_floor_usd(self) -> float:
        raise RuntimeError("cannot compute funder floor")

    def all_group_caps_sum_usd(self) -> float:
        raise RuntimeError("cannot compute group caps")


# ============================================================================
# Happy path
# ============================================================================


class TestReadinessHappyPath:
    def test_ready_when_all_checks_pass(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        assert result.status == "READY"
        # Row persisted.
        row = conn.execute(
            "SELECT status, clob_balance_usd FROM live_readiness_reports "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["status"] == "READY"
        assert row["clob_balance_usd"] == 250.0

    def test_report_row_inserted_every_call(self, conn) -> None:
        _seed_fresh_recon(conn)
        for _ in range(3):
            evaluate_readiness(
                conn,
                config=_make_config(),
                order_client=_StubOrderClient(balance=250.0),
                portfolio=_StubPortfolio(),
                chain_balance_fetcher=lambda: 250.0,
                topology_provider=_topology_provider,
            )
        n = conn.execute(
            "SELECT COUNT(*) FROM live_readiness_reports"
        ).fetchone()[0]
        assert n == 3


# ============================================================================
# Failure paths
# ============================================================================


class TestReadinessFailures:
    def test_fail_when_chain_required_and_fetcher_raises(self, conn) -> None:
        _seed_fresh_recon(conn)

        def _raise():
            raise RuntimeError(
                "RPC https://polygon.example/rpc/superSecretToken1234567890"
                "?api_key=LEAKME failed"
            )

        result = evaluate_readiness(
            conn,
            config=_make_config(onchain_verify=True),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=_raise,
            topology_provider=_topology_provider,
        )

        assert result.status == "NOT_READY"
        chain_check = next(c for c in result.checks if c.name == "chain_balance")
        assert chain_check.passed is False
        assert "LEAKME" not in chain_check.detail
        assert "superSecretToken" not in chain_check.detail
        assert "/rpc/" not in chain_check.detail

    def test_chain_check_passes_when_onchain_verify_disabled(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(onchain_verify=False),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            topology_provider=_topology_provider,
        )
        chain_check = next(c for c in result.checks if c.name == "chain_balance")
        assert chain_check.passed is True
        assert chain_check.detail == "on_chain_verification_disabled"
        assert result.status == "READY"

    def test_fail_when_funder_does_not_match_derived_deposit(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(funder=_OTHER_DEPOSIT),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        check = next(c for c in result.checks if c.name == "funder_matches_deposit")
        assert check.passed is False
        assert result.status == "NOT_READY"

    def test_fail_when_relayer_owner_does_not_match_signer(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(relayer_owner="0x" + "f" * 40),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        check = next(c for c in result.checks if c.name == "relayer_owner")
        assert check.passed is False
        assert result.status == "NOT_READY"

    def test_fail_on_low_balance(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=50.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 50.0,
            topology_provider=_topology_provider,
        )
        assert result.status == "NOT_READY"
        balance_check = next(c for c in result.checks if c.name == "clob_balance")
        assert balance_check.passed is False
        assert "balance=50" in balance_check.detail

    def test_fail_on_chain_drift(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=200.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 100.0,
            topology_provider=_topology_provider,
            balance_tolerance_usd=0.5,
        )
        drift_check = next(
            c for c in result.checks if c.name == "chain_clob_drift"
        )
        assert drift_check.passed is False
        assert result.status == "NOT_READY"

    def test_fail_on_no_recon_row(self, conn) -> None:
        # No wallet_reconciliation_runs row.
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        recon_check = next(
            c for c in result.checks if c.name == "wallet_reconciliation"
        )
        assert recon_check.passed is False
        assert recon_check.detail == "no_recon_row"

    def test_fail_on_stale_recon(self, conn) -> None:
        conn.execute(
            "INSERT INTO wallet_reconciliation_runs (sampled_at, source_status) "
            "VALUES ('2026-01-01 00:00:00', 'ok')"
        )
        conn.commit()
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
            snapshot_freshness_ttl_s=60,
        )
        recon_check = next(
            c for c in result.checks if c.name == "wallet_reconciliation"
        )
        assert recon_check.passed is False
        assert result.status == "NOT_READY"

    def test_fail_on_transfer_lock(self, conn) -> None:
        _seed_fresh_recon(conn)
        enter_transfer_lock(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        op_check = next(c for c in result.checks if c.name == "operator_state")
        assert op_check.passed is False
        assert "TRANSFER_LOCK" in op_check.detail

    def test_fail_on_recent_critical_boot(self, conn) -> None:
        _seed_fresh_recon(conn)
        conn.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('boot', 'CRITICAL', 'wallet topology invalid')"
        )
        conn.commit()
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        boot_check = next(
            c for c in result.checks if c.name == "no_recent_boot_critical"
        )
        assert boot_check.passed is False

    def test_fail_on_invalid_wallet_topology(self, conn) -> None:
        _seed_fresh_recon(conn)
        # SIG=3 but funder is empty → topology gate fails.
        cfg = _make_config(signature_type=3, funder="")
        result = evaluate_readiness(
            conn,
            config=cfg,
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        topo_check = next(c for c in result.checks if c.name == "wallet_topology")
        assert topo_check.passed is False


# ============================================================================
# Fail-closed gates (audit P1 #9 / #10)
# ============================================================================


class TestFailClosedGates:
    def test_required_balance_fails_closed_to_inf_on_exception(self) -> None:
        from cryptobot.execution.live_readiness import _required_balance

        # A portfolio that cannot compute its floor must yield an unreachable
        # required balance, NOT 0.0 (which would pass for any balance).
        assert _required_balance(_RaisingPortfolio()) == float("inf")

    def test_required_balance_none_portfolio_returns_zero(self) -> None:
        from cryptobot.execution.live_readiness import _required_balance

        assert _required_balance(None) == 0.0

    def test_not_ready_when_required_balance_uncomputable(self, conn) -> None:
        _seed_fresh_recon(conn)
        # Even a huge CLOB balance must NOT pass when the floor is unknown.
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=1_000_000_000.0),
            portfolio=_RaisingPortfolio(),
            chain_balance_fetcher=lambda: 1_000_000_000.0,
            topology_provider=_topology_provider,
        )
        balance_check = next(c for c in result.checks if c.name == "clob_balance")
        assert balance_check.passed is False
        assert result.status == "NOT_READY"

    def test_not_ready_when_deposit_required_but_no_private_key(self, conn) -> None:
        _seed_fresh_recon(conn)
        # require_deposit defaults True; with an empty private key the
        # deposit-identity checks must NOT be silently skipped.
        result = evaluate_readiness(
            conn,
            config=_make_config(private_key=""),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        derived = next(
            c for c in result.checks if c.name == "wallet_topology_derived"
        )
        assert derived.passed is False
        assert derived.detail == "no_private_key_for_required_deposit_wallet"
        assert result.status == "NOT_READY"


# ============================================================================
# D-LIVE-26b — secret redaction
# ============================================================================


class TestReportJsonWhitelist:
    def test_report_json_only_contains_whitelisted_fields(self, conn) -> None:
        _seed_fresh_recon(conn)
        result = evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        # Pull the persisted JSON and verify field set.
        row = conn.execute(
            "SELECT report_json FROM live_readiness_reports ORDER BY id DESC LIMIT 1"
        ).fetchone()
        payload = json.loads(row["report_json"])
        assert set(payload.keys()) == set(_REPORT_FIELD_WHITELIST)

    def test_report_json_never_contains_private_key(self, conn) -> None:
        _seed_fresh_recon(conn)
        # Use a memorable private key so a leak is easy to catch.
        from cryptobot.runtime_secrets import Config

        cfg = Config(
            poly_private_key="LEAK-SENTINEL-PRIVATE-KEY-DO-NOT-INCLUDE",
            poly_api_key="ak",
            poly_secret="as",
            poly_passphrase="ap",
            poly_signature_type=3,
            poly_funder="0xfunder",
        )
        evaluate_readiness(
            conn,
            config=cfg,
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        row = conn.execute(
            "SELECT report_json FROM live_readiness_reports ORDER BY id DESC LIMIT 1"
        ).fetchone()
        payload = row["report_json"]
        assert "LEAK-SENTINEL-PRIVATE-KEY" not in payload

    def test_report_json_records_check_results(self, conn) -> None:
        _seed_fresh_recon(conn)
        evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        row = conn.execute(
            "SELECT report_json FROM live_readiness_reports ORDER BY id DESC LIMIT 1"
        ).fetchone()
        payload = json.loads(row["report_json"])
        check_names = {c["name"] for c in payload["readiness_check_results"]}
        assert "wallet_topology" in check_names
        assert "clob_balance" in check_names
        assert "operator_state" in check_names


# ============================================================================
# ERC-20 balanceOf
# ============================================================================


class TestReadErc20Balance:
    def test_decodes_raw_json_rpc_balance(self, monkeypatch) -> None:
        calls: list[dict] = []

        class _Resp:
            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {"result": hex(123_456_789)}

        def _post(url, *, json, timeout):
            calls.append({"url": url, "json": json, "timeout": timeout})
            return _Resp()

        import requests

        monkeypatch.setattr(requests, "post", _post)
        balance = read_erc20_balance(
            rpc_url="https://polygon.example",
            token_address="0x" + "1" * 40,
            wallet_address="0x" + "2" * 40,
            decimals=6,
            timeout_s=7.5,
        )

        assert balance == 123.456789
        assert calls[0]["url"] == "https://polygon.example"
        assert calls[0]["timeout"] == 7.5
        assert calls[0]["json"]["method"] == "eth_call"
        data = calls[0]["json"]["params"][0]["data"]
        assert data.startswith("0x70a08231")
        assert data.endswith(("2" * 40).rjust(64, "0"))

    def test_rejects_bad_addresses(self) -> None:
        with pytest.raises(ValueError, match="token_address"):
            read_erc20_balance(
                rpc_url="https://polygon.example",
                token_address="0x123",
                wallet_address="0x" + "2" * 40,
            )

    def test_surfaces_rpc_error(self, monkeypatch) -> None:
        class _Resp:
            def raise_for_status(self) -> None:
                return None

            def json(self):
                return {"error": {"message": "bad call"}}

        import requests

        monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp())
        with pytest.raises(RuntimeError, match="RPC error"):
            read_erc20_balance(
                rpc_url="https://polygon.example",
                token_address="0x" + "1" * 40,
                wallet_address="0x" + "2" * 40,
            )


# ============================================================================
# latest_readiness_report
# ============================================================================


class TestLatestReadinessReport:
    def test_returns_none_when_empty(self, conn) -> None:
        assert latest_readiness_report(conn) is None

    def test_returns_latest_row(self, conn) -> None:
        _seed_fresh_recon(conn)
        evaluate_readiness(
            conn,
            config=_make_config(),
            order_client=_StubOrderClient(balance=250.0),
            portfolio=_StubPortfolio(total_caps=200.0),
            chain_balance_fetcher=lambda: 250.0,
            topology_provider=_topology_provider,
        )
        latest = latest_readiness_report(conn)
        assert latest is not None
        assert latest["status"] == "READY"
        assert "funder_address" in latest["report_json"]
