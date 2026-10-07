"""Exit-code coverage for cryptobot_predictfun.wallet_diagnostics (offline)."""

from __future__ import annotations

import httpx
import pytest

import cryptobot_predictfun.wallet_diagnostics as diag
from cryptobot_predictfun import wallet as pfw


KEY = "0x" + "1" * 64
ADDR = "0x" + "a" * 40
TOKEN = "0x" + "c" * 40
EXCHANGE = "0x" + "e" * 40
CTF = "0x" + "d" * 40


@pytest.fixture(autouse=True)
def _pin_config(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", KEY, raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "bearer", raising=False)
    monkeypatch.setattr(
        config, "PREDICTFUN_API_BASE", "https://api.predict.fun/v1", raising=False
    )
    monkeypatch.setattr(config, "PREDICTFUN_CHAIN_ID", 56, raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_RPC_URL", "https://rpc", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_COLLATERAL_TOKEN", "", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_EXCHANGE_CONTRACT", "", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_CTF_CONTRACT", "", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_COLLATERAL_DECIMALS", 18, raising=False)
    monkeypatch.setattr(pfw, "derive_signer_address", lambda key: ADDR if key else None)


def _cfg():
    from cryptobot import config

    return config


def test_probe_api_ok():
    client = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": []}))
    )
    with client:
        assert diag._probe_api(_cfg(), client=client) == 0


def test_probe_api_sends_x_api_key_header():
    """2026-07-21 mainnet probe: the API key goes in `x-api-key`; a Bearer
    probe 401s and would falsely report a valid key as bad."""
    seen = {}

    def handler(req):
        seen["x-api-key"] = req.headers.get("x-api-key")
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"data": []})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert diag._probe_api(_cfg(), client=client) == 0
    assert seen["x-api-key"] == "bearer"  # the configured key value
    assert seen["auth"] is None


def test_probe_api_401_mainnet():
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(401, json={"error": "unauthorized"})
        )
    )
    with client:
        assert diag._probe_api(_cfg(), client=client) == 2


def test_probe_api_unreachable():
    def _raise(_req):
        raise httpx.ConnectError("down")

    client = httpx.Client(transport=httpx.MockTransport(_raise))
    with client:
        assert diag._probe_api(_cfg(), client=client) == 1


def test_probe_chain_not_configured(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "", raising=False)
    assert diag.main(["--probe-chain"]) == 2


def test_probe_chain_ok(monkeypatch, capsys):
    monkeypatch.setattr(pfw, "resolve_collateral_token", lambda *_a, **_k: TOKEN)
    monkeypatch.setattr(pfw, "resolve_exchange_contract", lambda *_a, **_k: EXCHANGE)
    monkeypatch.setattr(
        pfw,
        "read_wallet_balances",
        lambda **_k: pfw.WalletBalances(
            address=ADDR,
            collateral_symbol="USDT",
            collateral_balance=42.0,
            collateral_token=TOKEN,
            gas_balance_bnb=0.5,
        ),
    )
    monkeypatch.setattr(pfw, "read_erc20_allowance", lambda **_k: 1000.0)
    rc = diag.main(["--probe-chain"])
    assert rc == 0
    out = capsys.readouterr().out
    assert f"wallet={ADDR}" in out
    assert "USDT_balance=42.0" in out
    assert "collateral_allowance=1000.0" in out
    assert "bnb_gas_balance=0.5" in out


def test_probe_chain_unreachable(monkeypatch):
    monkeypatch.setattr(pfw, "resolve_collateral_token", lambda *_a, **_k: TOKEN)
    monkeypatch.setattr(
        pfw,
        "read_wallet_balances",
        lambda **_k: pfw.WalletBalances(
            address=ADDR,
            collateral_symbol="USDT",
            collateral_balance=None,
            collateral_token=TOKEN,
            gas_balance_bnb=None,
        ),
    )
    assert diag.main(["--probe-chain"]) == 1


def test_topology_shows_not_configured(monkeypatch, capsys):
    monkeypatch.setattr(pfw, "resolve_collateral_token", lambda *_a, **_k: None)
    monkeypatch.setattr(pfw, "resolve_exchange_contract", lambda *_a, **_k: None)
    monkeypatch.setattr(pfw, "resolve_ctf_contract", lambda *_a, **_k: None)
    rc = diag.main(["--topology"])
    assert rc == 0
    out = capsys.readouterr().out
    assert f"signer_address={ADDR}" in out
    assert "collateral_token=NOT-CONFIGURED" in out
    assert "chain=" not in out  # topology prints chain_id, not chain
    assert "chain_id=56" in out


def test_topology_resolved(monkeypatch, capsys):
    monkeypatch.setattr(pfw, "resolve_collateral_token", lambda *_a, **_k: TOKEN)
    monkeypatch.setattr(pfw, "resolve_exchange_contract", lambda *_a, **_k: EXCHANGE)
    monkeypatch.setattr(pfw, "resolve_ctf_contract", lambda *_a, **_k: CTF)
    rc = diag.main(["--topology"])
    assert rc == 0
    out = capsys.readouterr().out
    assert f"collateral_token={TOKEN}" in out
    assert f"exchange_contract={EXCHANGE}" in out
    assert f"ctf_contract={CTF}" in out
