from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from enum import Enum
from types import SimpleNamespace

import pytest

from cryptobot_polymarket import relayer
from cryptobot_polymarket import primitives


OWNER = "0x1111111111111111111111111111111111111111"
API_KEY_ADDRESS = "0x2222222222222222222222222222222222222222"
DEPOSIT_WALLET = "0x3333333333333333333333333333333333333333"
TARGET = "0x4444444444444444444444444444444444444444"
TO_ADDRESS = "0x5555555555555555555555555555555555555555"
PRIVATE_KEY = "0x" + "b" * 64


@dataclass
class _FakeCall:
    target: str
    value: str
    data: str


@dataclass
class _FakeArgs:
    from_address: str
    chain_id: int
    wallet_address: str
    nonce: str
    deadline: str
    calls: list[_FakeCall]


class _FakeTransactionType(Enum):
    WALLET = "WALLET"
    PROXY = "PROXY"


def _install_fake_builder(monkeypatch):
    captured: dict[str, object] = {}

    class FakeSigner:
        def address(self):
            return OWNER

    class FakeRelayClient:
        def __init__(self, relayer_url, chain_id, private_key):
            captured["client_init"] = (relayer_url, chain_id, private_key)
            self.signer = FakeSigner()
            self.contract_config = {"config": "ok"}

    class FakeBuiltRequest:
        def __init__(self, body):
            self._body = body

        def to_dict(self):
            return self._body

    def fake_build_deposit_wallet_batch_request(*, signer, args, config):
        captured["build"] = {"signer": signer, "args": args, "config": config}
        return FakeBuiltRequest(
            {
                "from": args.from_address,
                "wallet": args.wallet_address,
                "nonce": args.nonce,
                "deadline": args.deadline,
                "calls": [
                    {
                        "target": call.target,
                        "value": call.value,
                        "data": call.data,
                    }
                    for call in args.calls
                ],
            }
        )

    client_module = types.ModuleType("py_builder_relayer_client.client")
    client_module.RelayClient = FakeRelayClient
    models_module = types.ModuleType("py_builder_relayer_client.models")
    models_module.DepositWalletCall = _FakeCall
    models_module.DepositWalletTransactionArgs = _FakeArgs
    models_module.TransactionType = _FakeTransactionType
    package_module = types.ModuleType("py_builder_relayer_client")
    builder_module = types.ModuleType("py_builder_relayer_client.builder")
    deposit_wallet_module = types.ModuleType(
        "py_builder_relayer_client.builder.deposit_wallet"
    )
    deposit_wallet_module.build_deposit_wallet_batch_request = (
        fake_build_deposit_wallet_batch_request
    )
    package_module.client = client_module
    package_module.models = models_module
    package_module.builder = builder_module
    builder_module.deposit_wallet = deposit_wallet_module

    monkeypatch.setitem(sys.modules, "py_builder_relayer_client", package_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.builder", builder_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.client", client_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.models", models_module)
    monkeypatch.setitem(
        sys.modules,
        "py_builder_relayer_client.builder.deposit_wallet",
        deposit_wallet_module,
    )
    return captured


def _install_fake_proxy_builder(monkeypatch):
    captured: dict[str, object] = {}

    class FakeSigner:
        def address(self):
            return OWNER

    class FakeRelayClient:
        def __init__(self, relayer_url, chain_id, private_key):
            captured["client_init"] = (relayer_url, chain_id, private_key)
            self.signer = FakeSigner()
            self.contract_config = SimpleNamespace(proxy_factory="proxy-factory")

        def _estimate_proxy_gas(self, owner, proxy_factory, data):
            captured["gas"] = {
                "owner": owner,
                "proxy_factory": proxy_factory,
                "data": data,
            }
            return 12345

    class FakeBuiltRequest:
        def __init__(self, body):
            self._body = body

        def to_dict(self):
            return self._body

    class FakeCallType:
        Call = "call"

    @dataclass
    class FakeProxyTransaction:
        to: str
        type_code: str
        data: str
        value: str

    @dataclass
    class FakeProxyTransactionArgs:
        from_address: str
        nonce: str
        gas_price: str
        data: str
        relay: str
        gas_limit: int

    def fake_encode_proxy_transaction_data(transactions):
        captured["transactions"] = transactions
        return "0xencoded"

    def fake_build_proxy_transaction_request(signer, args, config, metadata):
        captured["build"] = {
            "signer": signer,
            "args": args,
            "config": config,
            "metadata": metadata,
        }
        return FakeBuiltRequest({
            "from": args.from_address,
            "nonce": args.nonce,
            "gasPrice": args.gas_price,
            "data": args.data,
            "relay": args.relay,
            "gasLimit": args.gas_limit,
            "metadata": metadata,
        })

    client_module = types.ModuleType("py_builder_relayer_client.client")
    client_module.RelayClient = FakeRelayClient
    models_module = types.ModuleType("py_builder_relayer_client.models")
    models_module.CallType = FakeCallType
    models_module.ProxyTransaction = FakeProxyTransaction
    models_module.ProxyTransactionArgs = FakeProxyTransactionArgs
    models_module.TransactionType = _FakeTransactionType
    package_module = types.ModuleType("py_builder_relayer_client")
    builder_module = types.ModuleType("py_builder_relayer_client.builder")
    proxy_builder_module = types.ModuleType("py_builder_relayer_client.builder.proxy")
    proxy_builder_module.build_proxy_transaction_request = (
        fake_build_proxy_transaction_request
    )
    encode_module = types.ModuleType("py_builder_relayer_client.encode")
    proxy_encode_module = types.ModuleType("py_builder_relayer_client.encode.proxy")
    proxy_encode_module.encode_proxy_transaction_data = (
        fake_encode_proxy_transaction_data
    )
    package_module.client = client_module
    package_module.models = models_module
    package_module.builder = builder_module
    builder_module.proxy = proxy_builder_module
    encode_module.proxy = proxy_encode_module

    monkeypatch.setitem(sys.modules, "py_builder_relayer_client", package_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.builder", builder_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.client", client_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.models", models_module)
    monkeypatch.setitem(
        sys.modules,
        "py_builder_relayer_client.builder.proxy",
        proxy_builder_module,
    )
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.encode", encode_module)
    monkeypatch.setitem(
        sys.modules,
        "py_builder_relayer_client.encode.proxy",
        proxy_encode_module,
    )
    return captured


def test_submit_deposit_wallet_contract_call_builds_and_submits(
    monkeypatch,
) -> None:
    captured = _install_fake_builder(monkeypatch)
    request_calls: list[dict[str, object]] = []

    def fake_request_json(method, url, **kwargs):
        request_calls.append({"method": method, "url": url, **kwargs})
        if method == "GET":
            return {"nonce": 17}
        return {"tx_hash": "0xtx", "success": True}

    monkeypatch.setattr(relayer, "request_json", fake_request_json)
    monkeypatch.setattr(relayer.time, "time", lambda: 1000.9)

    result = relayer.submit_deposit_wallet_contract_call(
        relayer_url="https://relayer.example/",
        api_key="key",
        api_key_address=API_KEY_ADDRESS,
        private_key=PRIVATE_KEY,
        deposit_wallet=DEPOSIT_WALLET,
        target=TARGET,
        data="0xdeadbeef",
        value="9",
        deadline_seconds=60,
    )

    assert result == {"tx_hash": "0xtx", "success": True}
    assert captured["client_init"] == (
        "https://relayer.example",
        primitives.CHAIN_ID,
        PRIVATE_KEY,
    )
    assert request_calls[0] == {
        "method": "GET",
        "url": "https://relayer.example/nonce",
        "params": {"address": OWNER, "type": "WALLET"},
    }
    assert request_calls[1]["method"] == "POST"
    assert request_calls[1]["url"] == "https://relayer.example/submit"
    assert request_calls[1]["headers"] == {
        "RELAYER_API_KEY": "key",
        "RELAYER_API_KEY_ADDRESS": API_KEY_ADDRESS,
    }
    assert request_calls[1]["body"] == {
        "from": OWNER,
        "wallet": DEPOSIT_WALLET,
        "nonce": "17",
        "deadline": "1060",
        "calls": [{"target": TARGET, "value": "9", "data": "0xdeadbeef"}],
    }


def test_submit_deposit_wallet_contract_call_rejects_bad_inputs(
    monkeypatch,
) -> None:
    _install_fake_builder(monkeypatch)
    with pytest.raises(relayer.RelayerSubmitError, match="calldata"):
        relayer.submit_deposit_wallet_contract_call(
            relayer_url="https://relayer.example",
            api_key="key",
            api_key_address=API_KEY_ADDRESS,
            private_key=PRIVATE_KEY,
            deposit_wallet=DEPOSIT_WALLET,
            target=TARGET,
            data="deadbeef",
        )

    with pytest.raises(relayer.RelayerSubmitError, match="deadline_seconds"):
        relayer.submit_deposit_wallet_contract_call(
            relayer_url="https://relayer.example",
            api_key="key",
            api_key_address=API_KEY_ADDRESS,
            private_key=PRIVATE_KEY,
            deposit_wallet=DEPOSIT_WALLET,
            target=TARGET,
            data="0xdeadbeef",
            deadline_seconds=0,
        )


def test_submit_deposit_wallet_contract_call_rejects_missing_nonce(
    monkeypatch,
) -> None:
    _install_fake_builder(monkeypatch)
    monkeypatch.setattr(relayer, "request_json", lambda *args, **kwargs: {})

    with pytest.raises(relayer.RelayerSubmitError, match="nonce payload"):
        relayer.submit_deposit_wallet_contract_call(
            relayer_url="https://relayer.example",
            api_key="key",
            api_key_address=API_KEY_ADDRESS,
            private_key=PRIVATE_KEY,
            deposit_wallet=DEPOSIT_WALLET,
            target=TARGET,
            data="0xdeadbeef",
        )


def test_submit_deposit_wallet_pusd_transfer_builds_transfer_call(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_submit(**kwargs):
        captured.update(kwargs)
        return {"tx_hash": "0xtransfer"}

    monkeypatch.setattr(relayer, "submit_deposit_wallet_contract_call", fake_submit)

    result = relayer.submit_deposit_wallet_pusd_transfer(
        relayer_url="https://relayer.example",
        api_key="key",
        api_key_address=API_KEY_ADDRESS,
        private_key=PRIVATE_KEY,
        deposit_wallet=DEPOSIT_WALLET,
        to_address=TO_ADDRESS,
        amount_base_units=123,
        deadline_seconds=42,
    )

    assert result == {"tx_hash": "0xtransfer"}
    assert captured["target"] == primitives.PUSD_ADDRESS
    assert captured["value"] == "0"
    assert captured["data"] == primitives.erc20_transfer_calldata(TO_ADDRESS, 123)
    assert captured["deadline_seconds"] == 42


def test_submit_deposit_wallet_pusd_transfer_requires_positive_amount() -> None:
    with pytest.raises(relayer.RelayerSubmitError, match="positive"):
        relayer.submit_deposit_wallet_pusd_transfer(
            relayer_url="https://relayer.example",
            api_key="key",
            api_key_address=API_KEY_ADDRESS,
            private_key=PRIVATE_KEY,
            deposit_wallet=DEPOSIT_WALLET,
            to_address=TO_ADDRESS,
            amount_base_units=0,
        )


def test_submit_proxy_wallet_pusd_transfer_builds_proxy_request(monkeypatch) -> None:
    captured = _install_fake_proxy_builder(monkeypatch)
    request_calls: list[dict[str, object]] = []

    def fake_request_json(method, url, **kwargs):
        request_calls.append({"method": method, "url": url, **kwargs})
        if method == "GET":
            return {"address": "0x9999999999999999999999999999999999999999", "nonce": "8"}
        return {"transactionID": "relayer-1", "success": True}

    monkeypatch.setattr(relayer, "request_json", fake_request_json)

    result = relayer.submit_proxy_wallet_pusd_transfer(
        relayer_url="https://relayer.example/",
        api_key="key",
        api_key_address=API_KEY_ADDRESS,
        private_key=PRIVATE_KEY,
        to_address=DEPOSIT_WALLET,
        amount_base_units=103_550_000,
    )

    assert result == {"transactionID": "relayer-1", "success": True}
    assert captured["client_init"] == (
        "https://relayer.example",
        primitives.CHAIN_ID,
        PRIVATE_KEY,
    )
    assert request_calls[0] == {
        "method": "GET",
        "url": "https://relayer.example/relay-payload",
        "params": {"address": OWNER, "type": "PROXY"},
    }
    assert request_calls[1]["method"] == "POST"
    assert request_calls[1]["url"] == "https://relayer.example/submit"
    assert request_calls[1]["headers"] == {
        "RELAYER_API_KEY": "key",
        "RELAYER_API_KEY_ADDRESS": API_KEY_ADDRESS,
    }
    transaction = captured["transactions"][0]
    assert transaction.to == primitives.PUSD_ADDRESS
    assert transaction.type_code == "call"
    assert transaction.data == primitives.erc20_transfer_calldata(
        DEPOSIT_WALLET,
        103_550_000,
    )
    assert transaction.value == "0"
    assert captured["gas"] == {
        "owner": OWNER,
        "proxy_factory": "proxy-factory",
        "data": "0xencoded",
    }
    assert request_calls[1]["body"] == {
        "from": OWNER,
        "nonce": "8",
        "gasPrice": "0",
        "data": "0xencoded",
        "relay": "0x9999999999999999999999999999999999999999",
        "gasLimit": 12345,
        "metadata": "cryptobot proxy pUSD transfer to deposit wallet",
    }


def test_submit_proxy_wallet_pusd_transfer_requires_positive_amount() -> None:
    with pytest.raises(relayer.RelayerSubmitError, match="positive"):
        relayer.submit_proxy_wallet_pusd_transfer(
            relayer_url="https://relayer.example",
            api_key="key",
            api_key_address=API_KEY_ADDRESS,
            private_key=PRIVATE_KEY,
            to_address=DEPOSIT_WALLET,
            amount_base_units=0,
        )


def test_poll_relayer_transaction_returns_terminal_state(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_request_json(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        if len(calls) == 1:
            return {"state": "STATE_PENDING"}
        return {"state": primitives.CONFIRMED_STATE, "transactionHash": "0xtx"}

    monkeypatch.setattr(relayer, "request_json", fake_request_json)
    monkeypatch.setattr(relayer.time, "sleep", lambda _s: None)

    result = relayer.poll_relayer_transaction(
        relayer_url="https://relayer.example/",
        transaction_id="relayer-1",
        max_polls=2,
        sleep_seconds=0,
    )

    assert result == {"state": primitives.CONFIRMED_STATE, "transactionHash": "0xtx"}
    assert calls == [
        {
            "method": "GET",
            "url": "https://relayer.example/transaction",
            "params": {"id": "relayer-1"},
        },
        {
            "method": "GET",
            "url": "https://relayer.example/transaction",
            "params": {"id": "relayer-1"},
        },
    ]


def test_build_transfer_submitter_converts_decimal_amount(monkeypatch) -> None:
    captured: dict[str, object] = {}
    cfg = SimpleNamespace(
        relayer_url="https://relayer.example",
        relayer_api_key="key",
        relayer_api_key_address=API_KEY_ADDRESS,
        poly_private_key=PRIVATE_KEY,
        poly_funder=DEPOSIT_WALLET,
    )

    def fake_submit(**kwargs):
        captured.update(kwargs)
        return {"transactionID": "relayer-1"}

    monkeypatch.setattr(relayer, "submit_deposit_wallet_pusd_transfer", fake_submit)
    submitter = relayer.build_transfer_submitter(cfg)

    result = submitter(
        {
            "from_wallet": DEPOSIT_WALLET,
            "to_wallet": TO_ADDRESS,
            "amount_usd": "12.3456789",
        }
    )

    assert result["relayer_tx_id"] == "relayer-1"
    assert captured["amount_base_units"] == 12_345_678
    assert captured["deposit_wallet"] == DEPOSIT_WALLET
    assert captured["to_address"] == TO_ADDRESS

