"""U11a verification: poly_wallet_diagnostics smoke test exit codes."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

import cryptobot_polymarket.wallet_diagnostics as diag
from cryptobot_polymarket.primitives import CONFIRMED_STATE, WalletTopology


cli_main = diag.main


_KEY = "0x" + "1" * 64
_PROXY = "0x" + "a" * 40
_SAFE = "0x" + "b" * 40
_DEPOSIT = "0x" + "d" * 40
_SIGNER = "0x" + "e" * 40


@pytest.fixture(autouse=True)
def _reset_runtime_secrets() -> None:
    from cryptobot.runtime_secrets import set_config

    set_config(None)
    yield
    set_config(None)


def _install_clob_stub(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock_client = MagicMock()
    mock_client.get_balance_allowance.return_value = {"balance": "200000000"}

    class _ApiCreds:
        def __init__(self, **_k):
            pass

    class _ClobClient:
        def __init__(self, **_k):
            self.mock = mock_client

        def __getattr__(self, name):
            return getattr(mock_client, name)

    class _AssetType:
        COLLATERAL = "collateral"
        CONDITIONAL = "conditional"

    class _BalanceAllowanceParams:
        def __init__(self, asset_type, token_id=None):
            self.asset_type = asset_type

    class _OrderArgs:
        pass

    class _OrderType:
        GTC = "GTC"
        FOK = "FOK"

    pkg = ModuleType("py_clob_client_v2")
    client_mod = ModuleType("py_clob_client_v2.client")
    client_mod.ClobClient = _ClobClient
    clob_types_mod = ModuleType("py_clob_client_v2.clob_types")
    clob_types_mod.ApiCreds = _ApiCreds
    clob_types_mod.AssetType = _AssetType
    clob_types_mod.BalanceAllowanceParams = _BalanceAllowanceParams
    clob_types_mod.OrderArgs = _OrderArgs
    clob_types_mod.OrderType = _OrderType
    constants_mod = ModuleType("py_clob_client_v2.order_builder.constants")
    constants_mod.BUY = "BUY"
    constants_mod.SELL = "SELL"
    builder_mod = ModuleType("py_clob_client_v2.order_builder")
    builder_mod.constants = constants_mod
    http_helpers_mod = ModuleType("py_clob_client_v2.http_helpers")
    helpers_submod = ModuleType("py_clob_client_v2.http_helpers.helpers")
    helpers_submod._http_client = None
    http_helpers_mod.helpers = helpers_submod
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.client", client_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.clob_types", clob_types_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.order_builder", builder_mod)
    monkeypatch.setitem(
        sys.modules, "py_clob_client_v2.order_builder.constants", constants_mod
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.http_helpers", http_helpers_mod)
    monkeypatch.setitem(
        sys.modules, "py_clob_client_v2.http_helpers.helpers", helpers_submod
    )
    return mock_client


def _set_funding_env(monkeypatch: pytest.MonkeyPatch, *, funder: str = _DEPOSIT) -> None:
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "3")
    monkeypatch.setenv("POLY_PRIVATE_KEY", _KEY)
    monkeypatch.setenv("POLY_API_KEY", "ak")
    monkeypatch.setenv("POLY_SECRET", "as")
    monkeypatch.setenv("POLY_PASSPHRASE", "ap")
    monkeypatch.setenv("POLY_FUNDER", funder)
    monkeypatch.setenv("RELAYER_URL", "https://relayer-v2.polymarket.com")
    monkeypatch.setenv("RELAYER_API_KEY", "rk")
    monkeypatch.setenv("RELAYER_API_KEY_ADDRESS", "0x" + "c" * 40)


def test_probe_clob_happy_path(monkeypatch, capsys) -> None:
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0xabcd")
    monkeypatch.setenv("POLY_API_KEY", "ak")
    monkeypatch.setenv("POLY_SECRET", "as")
    monkeypatch.setenv("POLY_PASSPHRASE", "ap")
    _install_clob_stub(monkeypatch)
    rc = cli_main(["--probe-clob"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "clob_balance_usd=200.0000" in out


def test_probe_clob_fails_on_unreachable(monkeypatch) -> None:
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("POLY_PRIVATE_KEY", "0xabcd")
    monkeypatch.setenv("POLY_API_KEY", "ak")
    monkeypatch.setenv("POLY_SECRET", "as")
    monkeypatch.setenv("POLY_PASSPHRASE", "ap")
    stub = _install_clob_stub(monkeypatch)
    stub.get_balance_allowance.side_effect = RuntimeError("clob down")
    rc = cli_main(["--probe-clob"])
    assert rc == 1


def test_probe_chain_skipped_without_funder(monkeypatch, capsys) -> None:
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("POLY_FUNDER", "")
    monkeypatch.setenv("POLYGON_RPC_URL", "")
    rc = cli_main(["--probe-chain"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "chain_balance=skipped" in out


def test_probe_chain_reads_polygon_balance(monkeypatch, capsys) -> None:
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("POLY_FUNDER", "0x" + "1" * 40)
    monkeypatch.setenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
    from cryptobot_polymarket import balance as _balance

    monkeypatch.setattr(
        _balance,
        "build_pusd_balance_fetcher",
        lambda _cfg: lambda: 12.3456,
    )
    rc = cli_main(["--probe-chain"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "chain_balance_usd=12.3456" in out


def test_invalid_topology_exits_2(monkeypatch) -> None:
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "3")
    monkeypatch.setenv("POLY_FUNDER", "")
    rc = cli_main(["--probe-clob"])
    assert rc == 2


def test_transfer_all_proxy_pusd_to_matching_funder(monkeypatch, capsys) -> None:
    _set_funding_env(monkeypatch)
    topology = WalletTopology(
        signer=_SIGNER,
        proxy=_PROXY,
        safe=_SAFE,
        deposit=_DEPOSIT,
    )
    monkeypatch.setattr(diag, "derive_wallets", lambda *_a, **_k: topology)

    def _probe_balance(key, trio, *, signature_type, funder, sync_balance):
        assert key == _KEY
        assert trio == ("ak", "as", "ap")
        assert signature_type == 1
        assert funder == _PROXY
        assert sync_balance is True
        return 103.55

    observed: dict[str, object] = {}

    def _submit_proxy_wallet_pusd_transfer(**kwargs):
        observed.update(kwargs)
        return {"success": True, "transactionID": "relayer-1"}

    monkeypatch.setattr(diag, "_probe_balance", _probe_balance)
    monkeypatch.setattr(
        diag,
        "submit_proxy_wallet_pusd_transfer",
        _submit_proxy_wallet_pusd_transfer,
    )
    monkeypatch.setattr(
        diag,
        "poll_relayer_transaction",
        lambda **_k: {"state": CONFIRMED_STATE, "transactionHash": "0xtx"},
    )

    rc = cli_main([
        "--transfer-pusd-from-proxy",
        "all",
        "--expected-min-usd",
        "100",
        "--expected-max-usd",
        "106.50",
        "--sync-balance",
        "--wait",
        "--polls",
        "1",
        "--poll-seconds",
        "0",
    ])

    assert rc == 0
    assert observed["to_address"] == _DEPOSIT
    assert observed["amount_base_units"] == 103_550_000
    out = capsys.readouterr().out
    assert "proxy_pusd_transfer_submitted" in out
    assert f"from_proxy={_PROXY}" in out
    assert f"to_funder={_DEPOSIT}" in out
    assert "relayer_tx_id=relayer-1" in out
    assert "final_tx_hash=0xtx" in out


def test_transfer_all_refuses_funder_mismatch(monkeypatch) -> None:
    _set_funding_env(monkeypatch, funder="0x" + "f" * 40)
    monkeypatch.setattr(
        diag,
        "derive_wallets",
        lambda *_a, **_k: WalletTopology(
            signer=_SIGNER,
            proxy=_PROXY,
            safe=_SAFE,
            deposit=_DEPOSIT,
        ),
    )
    submit = MagicMock()
    monkeypatch.setattr(diag, "submit_proxy_wallet_pusd_transfer", submit)

    rc = cli_main([
        "--transfer-pusd-from-proxy",
        "all",
        "--expected-min-usd",
        "100",
        "--expected-max-usd",
        "106.50",
    ])

    assert rc == 2
    submit.assert_not_called()


def test_transfer_all_refuses_balance_outside_expected_window(monkeypatch) -> None:
    _set_funding_env(monkeypatch)
    monkeypatch.setattr(
        diag,
        "derive_wallets",
        lambda *_a, **_k: WalletTopology(
            signer=_SIGNER,
            proxy=_PROXY,
            safe=_SAFE,
            deposit=_DEPOSIT,
        ),
    )
    monkeypatch.setattr(diag, "_probe_balance", lambda *_a, **_k: 99.99)
    submit = MagicMock()
    monkeypatch.setattr(diag, "submit_proxy_wallet_pusd_transfer", submit)

    rc = cli_main([
        "--transfer-pusd-from-proxy",
        "all",
        "--expected-min-usd",
        "100",
        "--expected-max-usd",
        "106.50",
    ])

    assert rc == 2
    submit.assert_not_called()


def test_transfer_wait_requires_terminal_confirmation(monkeypatch) -> None:
    _set_funding_env(monkeypatch)
    monkeypatch.setattr(
        diag,
        "derive_wallets",
        lambda *_a, **_k: WalletTopology(
            signer=_SIGNER,
            proxy=_PROXY,
            safe=_SAFE,
            deposit=_DEPOSIT,
        ),
    )
    monkeypatch.setattr(diag, "_probe_balance", lambda *_a, **_k: 103.55)
    monkeypatch.setattr(
        diag,
        "submit_proxy_wallet_pusd_transfer",
        lambda **_k: {"success": True, "transactionID": "relayer-1"},
    )
    monkeypatch.setattr(diag, "poll_relayer_transaction", lambda **_k: None)

    rc = cli_main([
        "--transfer-pusd-from-proxy",
        "100",
        "--wait",
        "--polls",
        "1",
        "--poll-seconds",
        "0",
    ])

    assert rc == 1


def test_transfer_submit_rejects_unsuccessful_relayer_response(monkeypatch) -> None:
    _set_funding_env(monkeypatch)
    monkeypatch.setattr(
        diag,
        "derive_wallets",
        lambda *_a, **_k: WalletTopology(
            signer=_SIGNER,
            proxy=_PROXY,
            safe=_SAFE,
            deposit=_DEPOSIT,
        ),
    )
    monkeypatch.setattr(diag, "_probe_balance", lambda *_a, **_k: 103.55)
    monkeypatch.setattr(
        diag,
        "submit_proxy_wallet_pusd_transfer",
        lambda **_k: {"success": False, "error": "relayer rejected"},
    )

    rc = cli_main(["--transfer-pusd-from-proxy", "100"])

    assert rc == 1
