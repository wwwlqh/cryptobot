"""CLI coverage for cryptobot_polymarket.derive_creds."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from cryptobot_polymarket.derive_creds import main as cli_main


PRIVATE_KEY = "0x" + ("a" * 64)


@pytest.fixture(autouse=True)
def _reset_runtime_secrets() -> None:
    from cryptobot.runtime_secrets import set_config

    set_config(None)
    yield
    set_config(None)


def _install_clob_stub(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock_client = MagicMock()
    mock_client.get_balance_allowance.return_value = {"balance": "123000000"}

    class _Creds:
        api_key = "derived-key"
        api_secret = "derived-secret"
        api_passphrase = "derived-passphrase"

    class _ClobClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def create_or_derive_api_key(self):
            return _Creds()

        def update_balance_allowance(self, params):
            return None

        def get_balance_allowance(self, params):
            return mock_client.get_balance_allowance(params)

    class _ApiCreds:
        def __init__(self, **_kwargs):
            pass

    class _AssetType:
        COLLATERAL = "collateral"

    class _BalanceAllowanceParams:
        def __init__(self, asset_type):
            self.asset_type = asset_type

    pkg = ModuleType("py_clob_client_v2")
    client_mod = ModuleType("py_clob_client_v2.client")
    client_mod.ClobClient = _ClobClient
    clob_types_mod = ModuleType("py_clob_client_v2.clob_types")
    clob_types_mod.ApiCreds = _ApiCreds
    clob_types_mod.AssetType = _AssetType
    clob_types_mod.BalanceAllowanceParams = _BalanceAllowanceParams
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.client", client_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.clob_types", clob_types_mod)
    return mock_client


def test_derives_and_does_not_print_private_key(monkeypatch, capsys) -> None:
    _install_clob_stub(monkeypatch)

    rc = cli_main(["--key", PRIVATE_KEY])

    assert rc == 0
    out = capsys.readouterr().out
    assert "POLY_API_KEY=derived-key" in out
    assert "POLY_SECRET=derived-secret" in out
    assert "POLY_PASSPHRASE=derived-passphrase" in out
    assert PRIVATE_KEY not in out
    assert "POLY_PRIVATE_KEY=" not in out


def test_check_reports_mismatch_without_private_key(monkeypatch, capsys) -> None:
    _install_clob_stub(monkeypatch)
    monkeypatch.setenv("POLY_PRIVATE_KEY", PRIVATE_KEY)
    monkeypatch.setenv("POLY_API_KEY", "wrong-key")
    monkeypatch.setenv("POLY_SECRET", "wrong-secret")
    monkeypatch.setenv("POLY_PASSPHRASE", "wrong-passphrase")
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")

    rc = cli_main(["--check"])

    assert rc == 2
    out = capsys.readouterr().out
    assert "MISMATCH" in out
    assert PRIVATE_KEY not in out
    assert "POLY_PRIVATE_KEY=" not in out


def test_probe_balance_prints_selected_topology(monkeypatch, capsys) -> None:
    _install_clob_stub(monkeypatch)

    rc = cli_main(["--key", PRIVATE_KEY, "--probe-balance"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "pUSD balance for signature_type=0 (EOA)" in out
    assert "$123.00" in out


def test_signature_type_3_requires_funder(monkeypatch) -> None:
    _install_clob_stub(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main(["--key", PRIVATE_KEY, "--signature-type", "3"])

    assert "POLY_SIGNATURE_TYPE=3" in str(exc.value)
