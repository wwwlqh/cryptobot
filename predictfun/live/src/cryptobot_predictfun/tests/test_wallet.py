"""On-chain wallet reads + redeem calldata for the predict.fun venue.

Offline: the JSON-RPC HTTP layer is stubbed by injecting a fake ``requests``
module into ``sys.modules`` (the module does ``import requests`` lazily inside
each read). No network.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from cryptobot_predictfun import wallet as pfw


_TOKEN = "0x" + "a" * 40
_WALLET = "0x" + "b" * 40
_EXCHANGE = "0x" + "c" * 40
_CTF = "0x" + "d" * 40
_COND = "0x" + "e" * 64


def _install_fake_requests(monkeypatch, *, result_hex: str) -> dict:
    """Stub ``requests.post`` to return an eth_call/eth_getBalance result."""
    calls: dict = {"n": 0, "last_json": None}

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"jsonrpc": "2.0", "id": 1, "result": result_hex}

    def _post(url, json=None, timeout=None):
        calls["n"] += 1
        calls["last_json"] = json
        return _Resp()

    fake = ModuleType("requests")
    fake.post = _post
    monkeypatch.setitem(sys.modules, "requests", fake)
    return calls


# ---- address validation -----------------------------------------------------

def test_is_bnb_address():
    assert pfw.is_bnb_address(_WALLET)
    assert not pfw.is_bnb_address("0x123")
    assert not pfw.is_bnb_address(None)


def test_is_bytes32():
    assert pfw.is_bytes32(_COND)
    assert not pfw.is_bytes32(_WALLET)


# ---- address resolution (env override -> SDK -> None fail-closed) -----------

def test_resolve_collateral_token_env_override():
    assert pfw.resolve_collateral_token(56, override=_TOKEN) == _TOKEN


def test_resolve_collateral_token_none_without_sdk(monkeypatch):
    # No override + SDK absent => None (fail-closed).
    monkeypatch.setattr(pfw, "_sdk_addresses", lambda chain_id: None)
    assert pfw.resolve_collateral_token(56) is None
    assert pfw.resolve_exchange_contract(56) is None
    assert pfw.resolve_ctf_contract(56) is None


def test_resolve_from_sdk_bundle(monkeypatch):
    bundle = SimpleNamespace(USDT=_TOKEN, CTF_EXCHANGE=_EXCHANGE, CONDITIONAL_TOKENS=_CTF)
    monkeypatch.setattr(pfw, "_sdk_addresses", lambda chain_id: bundle)
    assert pfw.resolve_collateral_token(56) == _TOKEN
    assert pfw.resolve_exchange_contract(56) == _EXCHANGE
    assert pfw.resolve_ctf_contract(56) == _CTF


def test_resolve_bad_override_ignored(monkeypatch):
    monkeypatch.setattr(pfw, "_sdk_addresses", lambda chain_id: None)
    # A malformed override is not an address -> falls through to SDK (None here).
    assert pfw.resolve_collateral_token(56, override="not-an-address") is None


# ---- signer derivation ------------------------------------------------------

def test_derive_signer_address_rejects_malformed():
    assert pfw.derive_signer_address("nope") is None
    assert pfw.derive_signer_address("") is None


def test_derive_signer_address_known_key():
    eth_account = pytest.importorskip("eth_account")
    # Well-known test vector: privkey 0x…0001 -> this address.
    key = "0x" + "0" * 63 + "1"
    addr = pfw.derive_signer_address(key)
    assert addr == "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"


# ---- on-chain reads (stubbed RPC) -------------------------------------------

def test_read_erc20_balance(monkeypatch):
    # 1e18 raw at 18 decimals = 1.0.
    _install_fake_requests(monkeypatch, result_hex=hex(10 ** 18))
    bal = pfw.read_erc20_balance(
        rpc_url="https://rpc", token_address=_TOKEN, wallet_address=_WALLET
    )
    assert bal == pytest.approx(1.0)


def test_read_native_balance(monkeypatch):
    _install_fake_requests(monkeypatch, result_hex=hex(2 * 10 ** 18))
    bal = pfw.read_native_balance(rpc_url="https://rpc", wallet_address=_WALLET)
    assert bal == pytest.approx(2.0)


def test_read_erc20_allowance(monkeypatch):
    _install_fake_requests(monkeypatch, result_hex=hex(5 * 10 ** 18))
    allowance = pfw.read_erc20_allowance(
        rpc_url="https://rpc",
        token_address=_TOKEN,
        owner_address=_WALLET,
        spender_address=_EXCHANGE,
    )
    assert allowance == pytest.approx(5.0)


def test_read_wallet_balances_partial(monkeypatch):
    _install_fake_requests(monkeypatch, result_hex=hex(3 * 10 ** 18))
    balances = pfw.read_wallet_balances(
        rpc_url="https://rpc", wallet_address=_WALLET, collateral_token=_TOKEN
    )
    assert balances.collateral_balance == pytest.approx(3.0)
    assert balances.gas_balance_bnb == pytest.approx(3.0)


def test_read_wallet_balances_none_collateral_when_token_unresolved(monkeypatch):
    _install_fake_requests(monkeypatch, result_hex=hex(10 ** 18))
    balances = pfw.read_wallet_balances(
        rpc_url="https://rpc", wallet_address=_WALLET, collateral_token=None
    )
    assert balances.collateral_balance is None  # fail-closed: no token => no balance
    assert balances.gas_balance_bnb == pytest.approx(1.0)


def test_build_collateral_balance_fetcher_fails_closed_without_key(monkeypatch):
    from cryptobot import config

    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "", raising=False)
    fetcher = pfw.build_collateral_balance_fetcher()
    with pytest.raises(RuntimeError):
        fetcher()


# ---- redeem calldata + fail-closed submit -----------------------------------

def test_build_redeem_positions_calldata_deterministic():
    cd1 = pfw.build_redeem_positions_calldata(
        collateral_token=_TOKEN, condition_id=_COND, index_sets=[1]
    )
    cd2 = pfw.build_redeem_positions_calldata(
        collateral_token=_TOKEN, condition_id=_COND, index_sets=[2]
    )
    assert cd1.startswith("0x") and cd2.startswith("0x")
    assert cd1 != cd2  # different index set -> different calldata
    # Same selector prefix (first 4 bytes = 8 hex + 0x).
    assert cd1[:10] == cd2[:10]


def test_build_redeem_positions_calldata_rejects_bad_input():
    with pytest.raises(ValueError):
        pfw.build_redeem_positions_calldata(
            collateral_token="bad", condition_id=_COND, index_sets=[1]
        )
    with pytest.raises(ValueError):
        pfw.build_redeem_positions_calldata(
            collateral_token=_TOKEN, condition_id="bad", index_sets=[1]
        )
    with pytest.raises(ValueError):
        pfw.build_redeem_positions_calldata(
            collateral_token=_TOKEN, condition_id=_COND, index_sets=[]
        )


def test_redeem_not_configured_when_addresses_unresolved(monkeypatch):
    monkeypatch.setattr(pfw, "_sdk_addresses", lambda chain_id: None)
    res = pfw.redeem_won_position(condition_id=_COND, index_sets=[1], chain_id=56)
    assert res.success is False
    assert res.status == "not_configured"


def test_redeem_built_but_not_posted_by_default():
    res = pfw.redeem_won_position(
        condition_id=_COND,
        index_sets=[1],
        chain_id=56,
        collateral_token_override=_TOKEN,
        ctf_contract_override=_CTF,
        auto_redeem_enabled=False,
    )
    assert res.success is False
    assert res.status == "built"
    assert res.calldata and res.calldata.startswith("0x")
    assert res.to_contract == _CTF


def test_redeem_fail_closed_when_enabled_but_unvalidated():
    res = pfw.redeem_won_position(
        condition_id=_COND,
        index_sets=[1],
        chain_id=56,
        collateral_token_override=_TOKEN,
        ctf_contract_override=_CTF,
        auto_redeem_enabled=True,
    )
    assert res.success is False
    assert res.status == "venue_unavailable"  # never broadcasts unvalidated
    assert res.calldata  # calldata still available for manual redeem
