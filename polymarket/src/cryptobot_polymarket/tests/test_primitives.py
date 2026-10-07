from __future__ import annotations

import sys
import types

import pytest

from cryptobot_polymarket import primitives


VALID_ADDRESS = "0x1111111111111111111111111111111111111111"
VALID_PRIVATE_KEY = "0x" + "a" * 64


class TestValidationAndHeaders:
    def test_validate_address_strips_and_preserves_case(self) -> None:
        address = "  0x111111111111111111111111111111111111aAaa  "
        assert primitives.validate_address(address, name="wallet") == address.strip()

    @pytest.mark.parametrize(
        "address",
        ["", "1111111111111111111111111111111111111111", "0x123", "0x" + "g" * 40],
    )
    def test_validate_address_rejects_invalid_values(self, address: str) -> None:
        with pytest.raises(ValueError, match="wallet"):
            primitives.validate_address(address, name="wallet")

    @pytest.mark.parametrize(
        "key",
        ["", "0x" + "a" * 63, "0x" + "z" * 64, "a" * 64, " " + VALID_PRIVATE_KEY],
    )
    def test_validate_private_key_rejects_invalid_values(self, key: str) -> None:
        with pytest.raises(ValueError, match="POLY_PRIVATE_KEY"):
            primitives.validate_private_key(key)

    def test_relayer_helpers(self) -> None:
        assert primitives.normalize_relayer_url("https://relayer.example/") == (
            "https://relayer.example"
        )
        assert primitives.normalize_relayer_url("") == primitives.DEFAULT_RELAYER_URL
        assert primitives.relayer_headers("key", VALID_ADDRESS) == {
            "RELAYER_API_KEY": "key",
            "RELAYER_API_KEY_ADDRESS": VALID_ADDRESS,
        }


class TestCalldataHelpers:
    def test_erc20_transfer_calldata(self) -> None:
        data = primitives.erc20_transfer_calldata(VALID_ADDRESS, 123)
        assert data == (
            "0xa9059cbb"
            "0000000000000000000000001111111111111111111111111111111111111111"
            "000000000000000000000000000000000000000000000000000000000000007b"
        )

    def test_redeem_positions_calldata_passes_normalized_args(self, monkeypatch) -> None:
        captured: dict[str, object] = {}

        def fake_encode(signature, arg_types, args):
            captured["signature"] = signature
            captured["arg_types"] = arg_types
            captured["args"] = args
            return "0xredeem"

        monkeypatch.setattr(primitives, "encode_contract_call", fake_encode)

        condition_id = "0x" + "22" * 32
        parent_collection_id = "0x" + "33" * 32
        data = primitives.redeem_positions_calldata(
            condition_id=condition_id,
            parent_collection_id=parent_collection_id,
            collateral_token=VALID_ADDRESS,
            index_sets=[1, "2"],
        )

        assert data == "0xredeem"
        assert captured["signature"] == (
            "redeemPositions(address,bytes32,bytes32,uint256[])"
        )
        assert captured["arg_types"] == ["address", "bytes32", "bytes32", "uint256[]"]
        assert captured["args"] == [
            VALID_ADDRESS,
            bytes.fromhex("33" * 32),
            bytes.fromhex("22" * 32),
            [1, 2],
        ]

    @pytest.mark.parametrize("index_sets", [[], [0], [-1], ["bad"]])
    def test_redeem_positions_rejects_invalid_index_sets(
        self, index_sets: list[int]
    ) -> None:
        with pytest.raises(ValueError, match="index_sets"):
            primitives.redeem_positions_calldata(
                condition_id="0x" + "22" * 32,
                index_sets=index_sets,
            )

    def test_redeem_positions_rejects_invalid_condition_id(self) -> None:
        with pytest.raises(ValueError, match="condition_id"):
            primitives.redeem_positions_calldata(
                condition_id="0x123",
                index_sets=[1],
            )


def test_request_json_raises_with_response_payload(monkeypatch) -> None:
    class FakeResponse:
        status_code = 500
        text = "plain failure"

        def json(self):
            return {"error": "boom"}

    calls: list[dict[str, object]] = []

    def fake_request(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        return FakeResponse()

    import requests

    monkeypatch.setattr(requests, "request", fake_request)
    with pytest.raises(RuntimeError, match="HTTP 500"):
        primitives.request_json(
            "POST",
            "https://relayer.example/submit",
            headers={"h": "1"},
            params={"p": "2"},
            body={"b": 3},
            timeout=4.0,
        )

    assert calls == [
        {
            "method": "POST",
            "url": "https://relayer.example/submit",
            "headers": {"h": "1"},
            "params": {"p": "2"},
            "json": {"b": 3},
            "timeout": 4.0,
        }
    ]


def test_derive_wallets_uses_builder_relayer_client(monkeypatch) -> None:
    seen: dict[str, object] = {}

    class FakeSigner:
        def address(self):
            return "0x2222222222222222222222222222222222222222"

    class FakeRelayClient:
        def __init__(self, relayer_url, chain_id, private_key, *, rpc_url=None):
            seen["init"] = (relayer_url, chain_id, private_key, rpc_url)
            self.signer = FakeSigner()

        def get_expected_proxy_wallet(self):
            return "0x3333333333333333333333333333333333333333"

        def get_expected_safe(self):
            return "0x4444444444444444444444444444444444444444"

        def get_expected_deposit_wallet(self):
            return "0x5555555555555555555555555555555555555555"

    client_module = types.ModuleType("py_builder_relayer_client.client")
    client_module.RelayClient = FakeRelayClient
    package_module = types.ModuleType("py_builder_relayer_client")
    package_module.client = client_module
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client", package_module)
    monkeypatch.setitem(sys.modules, "py_builder_relayer_client.client", client_module)

    topology = primitives.derive_wallets(
        VALID_PRIVATE_KEY,
        relayer_url="https://relayer.example/",
        rpc_url="https://polygon.example",
    )

    assert seen["init"] == (
        "https://relayer.example",
        primitives.CHAIN_ID,
        VALID_PRIVATE_KEY,
        "https://polygon.example",
    )
    assert topology.signer == "0x2222222222222222222222222222222222222222"
    assert topology.proxy == "0x3333333333333333333333333333333333333333"
    assert topology.safe == "0x4444444444444444444444444444444444444444"
    assert topology.deposit == "0x5555555555555555555555555555555555555555"
