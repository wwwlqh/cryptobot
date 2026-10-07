"""CLI coverage for cryptobot_predictfun.derive_creds (offline)."""

from __future__ import annotations

import pytest

import cryptobot_predictfun.derive_creds as cli
from cryptobot.cli._exitcodes import NOGO, OK, USAGE
from cryptobot_predictfun import wallet as pfw


KEY = "0x" + "1" * 64
ADDR = "0x" + "a" * 40


@pytest.fixture(autouse=True)
def _pin_config(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", KEY, raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "bearer-key", raising=False)
    monkeypatch.setattr(
        config, "PREDICTFUN_API_BASE", "https://api.predict.fun/v1", raising=False
    )
    monkeypatch.setattr(config, "PREDICTFUN_CHAIN_ID", 56, raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_RPC_URL", "https://rpc", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_COLLATERAL_TOKEN", "", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_COLLATERAL_DECIMALS", 18, raising=False)
    # Derivation is stubbed so the CLI test never needs eth_account.
    monkeypatch.setattr(pfw, "derive_signer_address", lambda key: ADDR)


def test_check_api_liveness_sends_x_api_key_header():
    """2026-07-21 mainnet probe: the API key goes in `x-api-key`; a Bearer
    probe 401s and would falsely report a valid key as bad."""
    import httpx

    seen = {}

    def handler(req):
        seen["x-api-key"] = req.headers.get("x-api-key")
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"success": True, "data": []})

    with httpx.Client(transport=httpx.MockTransport(handler)) as c:
        code, ok, _detail = cli._check_api_liveness(
            "https://api.predict.fun/v1", "sekret", client=c
        )
    assert (code, ok) == (200, True)
    assert seen["x-api-key"] == "sekret"
    assert seen["auth"] is None


def test_display_prints_address_never_key(capsys):
    rc = cli.main([])
    assert rc == OK
    out = capsys.readouterr().out
    assert ADDR in out
    assert KEY not in out
    assert "PREDICTFUN_PRIVATE_KEY=<" in out  # the placeholder, never the value


def test_display_usage_when_no_key(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "", raising=False)
    rc = cli.main([])
    assert rc == USAGE


def test_check_ok(monkeypatch, capsys):
    monkeypatch.setattr(
        cli, "_check_api_liveness", lambda base, api_key, **_k: (200, True, "200 OK")
    )
    rc = cli.main(["--check"])
    assert rc == OK
    out = capsys.readouterr().out
    assert "complete and API is reachable" in out
    assert KEY not in out


def test_check_fails_when_key_missing(monkeypatch, capsys):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "", raising=False)
    monkeypatch.setattr(
        cli, "_check_api_liveness", lambda base, api_key, **_k: (200, True, "200 OK")
    )
    rc = cli.main(["--check"])
    assert rc == NOGO
    out = capsys.readouterr().out
    assert "PREDICTFUN_PRIVATE_KEY: MISSING" in out


def test_check_fails_on_401(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_check_api_liveness",
        lambda base, api_key, **_k: (401, False, "401 unauthorized"),
    )
    rc = cli.main(["--check"])
    assert rc == NOGO


def test_check_testnet_needs_no_key(monkeypatch, capsys):
    from cryptobot import config

    monkeypatch.setattr(
        config, "PREDICTFUN_API_BASE", "https://api-testnet.predict.fun/v1", raising=False
    )
    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "", raising=False)
    monkeypatch.setattr(
        cli, "_check_api_liveness", lambda base, api_key, **_k: (200, True, "200 OK")
    )
    rc = cli.main(["--check"])
    assert rc == OK
    out = capsys.readouterr().out
    assert "testnet base is public" in out


def test_probe_balance(monkeypatch, capsys):
    monkeypatch.setattr(pfw, "resolve_collateral_token", lambda *_a, **_k: "0x" + "c" * 40)
    monkeypatch.setattr(
        pfw,
        "read_wallet_balances",
        lambda **_k: pfw.WalletBalances(
            address=ADDR,
            collateral_symbol="USDT",
            collateral_balance=12.5,
            collateral_token="0x" + "c" * 40,
            gas_balance_bnb=0.25,
        ),
    )
    rc = cli.main(["--probe-balance"])
    assert rc == OK
    out = capsys.readouterr().out
    assert "USDT collateral balance: 12.5000" in out
    assert "BNB gas balance: 0.250000" in out
