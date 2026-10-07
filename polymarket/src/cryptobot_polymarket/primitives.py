"""Polymarket constants, wallet topology, and calldata helpers.

Shared by operator tooling and low-level relayer adapters. Validation failures
raise ValueError. Missing optional dependencies and HTTP errors raise
RuntimeError so callers can decide whether to surface or wrap them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


CHAIN_ID = 137
DEFAULT_RELAYER_URL = "https://relayer-v2.polymarket.com"
PUSD_DECIMALS = 6

PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF_COLLATERAL_ADAPTER_ADDRESS = "0xAdA100Db00Ca00073811820692005400218FcE1f"
NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"

NONCE_PATH = "/nonce"
RELAY_PAYLOAD_PATH = "/relay-payload"
SUBMIT_PATH = "/submit"
TRANSACTION_PATH = "/transaction"

CONFIRMED_STATE = "STATE_CONFIRMED"
FAILED_STATES = frozenset({"STATE_FAILED", "STATE_INVALID"})


@dataclass(frozen=True)
class WalletTopology:
    signer: str
    proxy: str
    safe: str
    deposit: str


def validate_private_key(key: str) -> None:
    text = key or ""
    if not (
        len(text) == 66
        and text.startswith("0x")
        and all(c in "0123456789abcdefABCDEF" for c in text[2:])
    ):
        raise ValueError(
            "POLY_PRIVATE_KEY must be 0x-prefixed and 64 hex chars "
            "(66 chars total)."
        )


def validate_address(address: str, *, name: str) -> str:
    text = (address or "").strip()
    if not (
        len(text) == 42
        and text.startswith("0x")
        and all(c in "0123456789abcdefABCDEF" for c in text[2:])
    ):
        raise ValueError(f"{name} must be a 0x-prefixed 20-byte address.")
    return text


def normalize_relayer_url(url: str) -> str:
    return (url or DEFAULT_RELAYER_URL).rstrip("/")


def relayer_headers(api_key: str, api_key_address: str) -> dict[str, str]:
    if not api_key:
        raise ValueError("RELAYER_API_KEY is empty.")
    validate_address(api_key_address, name="RELAYER_API_KEY_ADDRESS")
    return {
        "RELAYER_API_KEY": api_key,
        "RELAYER_API_KEY_ADDRESS": api_key_address,
    }


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> Any:
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError("requests is not installed.") from exc

    response = requests.request(
        method,
        url,
        headers=headers,
        params=params,
        json=body,
        timeout=timeout,
    )
    try:
        payload: Any = response.json()
    except ValueError:
        payload = response.text
    if response.status_code >= 400:
        details = payload if isinstance(payload, str) else json.dumps(payload)
        raise RuntimeError(
            f"{method} {url} failed with HTTP {response.status_code}: {details}"
        )
    return payload


def encode_contract_call(signature: str, arg_types: list[str], args: list[Any]) -> str:
    if len(arg_types) != len(args):
        raise ValueError("arg_types and args must have the same length.")
    try:
        from eth_abi import encode
        from eth_utils import keccak, to_checksum_address
    except ModuleNotFoundError as exc:
        raise RuntimeError("eth_abi / eth_utils are required for relayer calldata.") from exc

    normalized_args: list[Any] = []
    for arg_type, arg in zip(arg_types, args):
        if arg_type == "address":
            normalized_args.append(
                to_checksum_address(validate_address(arg, name="address"))
            )
        else:
            normalized_args.append(arg)
    selector = keccak(text=signature)[:4]
    return "0x" + (selector + encode(arg_types, normalized_args)).hex()


def erc20_transfer_calldata(to_address: str, amount_base_units: int) -> str:
    return encode_contract_call(
        "transfer(address,uint256)",
        ["address", "uint256"],
        [to_address, amount_base_units],
    )


def _validate_bytes32(value: str, *, name: str) -> bytes:
    text = (value or "").strip()
    if text.startswith("0x"):
        text = text[2:]
    if len(text) != 64 or any(c not in "0123456789abcdefABCDEF" for c in text):
        raise ValueError(f"{name} must be a 32-byte hex value.")
    return bytes.fromhex(text)


def derive_wallets(
    private_key: str,
    *,
    relayer_url: str,
    rpc_url: str | None = None,
) -> WalletTopology:
    validate_private_key(private_key)
    try:
        from py_builder_relayer_client.client import RelayClient
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "py_builder_relayer_client is not installed; install the "
            "Polymarket relayer dependency before deriving wallets."
        ) from exc

    client = RelayClient(
        normalize_relayer_url(relayer_url),
        CHAIN_ID,
        private_key,
        rpc_url=rpc_url,
    )
    return WalletTopology(
        signer=client.signer.address(),
        proxy=client.get_expected_proxy_wallet(),
        safe=client.get_expected_safe(),
        deposit=client.get_expected_deposit_wallet(),
    )
