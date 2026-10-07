"""Small Polymarket relayer helpers for deposit-wallet calls."""

from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

from cryptobot_polymarket.primitives import (
    CONFIRMED_STATE,
    CHAIN_ID,
    FAILED_STATES,
    NONCE_PATH,
    PUSD_ADDRESS,
    PUSD_DECIMALS,
    RELAY_PAYLOAD_PATH,
    SUBMIT_PATH,
    TRANSACTION_PATH,
    erc20_transfer_calldata,
    normalize_relayer_url,
    relayer_headers,
    request_json,
    validate_address,
    validate_private_key,
)


class RelayerSubmitError(RuntimeError):
    """Raised when a relayer transaction cannot be built or submitted."""


def _secret_value(value: object) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        return str(getter() or "")
    return str(value or "")


def _amount_to_base_units(amount_usd: object) -> int:
    try:
        amount = Decimal(str(amount_usd))
    except (InvalidOperation, ValueError) as exc:
        raise RelayerSubmitError("amount_usd must be a decimal pUSD value") from exc
    if not amount.is_finite() or amount <= 0:
        raise RelayerSubmitError("amount_usd must be positive")
    units = int(
        (amount * (Decimal(10) ** PUSD_DECIMALS)).to_integral_value(
            rounding=ROUND_DOWN
        )
    )
    if units <= 0:
        raise RelayerSubmitError("amount_usd is below one pUSD base unit")
    return units


def normalize_relayer_response(response: dict[str, Any]) -> dict[str, Any]:
    """Normalize Polymarket relayer keys into cryptobot's local schema."""
    if not isinstance(response, dict):
        return {
            "success": False,
            "error": f"unexpected relayer response: {response!r}",
        }
    tx_hash = (
        response.get("tx_hash")
        or response.get("txHash")
        or response.get("transactionHash")
        or response.get("hash")
    )
    relayer_tx_id = (
        response.get("relayer_tx_id")
        or response.get("relayerTxId")
        or response.get("transactionID")
        or response.get("transactionId")
    )
    normalized = dict(response)
    normalized["success"] = bool(response.get("success", True))
    if tx_hash:
        normalized["tx_hash"] = tx_hash
    if relayer_tx_id:
        normalized["relayer_tx_id"] = relayer_tx_id
    return normalized


def relayer_configured(config: object) -> bool:
    return all(
        [
            _secret_value(getattr(config, "poly_private_key", "")),
            _secret_value(getattr(config, "relayer_api_key", "")),
            getattr(config, "relayer_api_key_address", ""),
            getattr(config, "poly_funder", ""),
            getattr(config, "relayer_url", ""),
        ]
    )


def submit_deposit_wallet_contract_call(
    *,
    relayer_url: str,
    api_key: str,
    api_key_address: str,
    private_key: str,
    deposit_wallet: str,
    target: str,
    data: str,
    value: str = "0",
    deadline_seconds: int = 600,
) -> dict[str, Any]:
    """Submit one arbitrary deposit-wallet contract call via the relayer."""
    try:
        from py_builder_relayer_client.builder.deposit_wallet import (
            build_deposit_wallet_batch_request,
        )
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import (
            DepositWalletCall,
            DepositWalletTransactionArgs,
            TransactionType,
        )
    except ModuleNotFoundError as exc:
        raise RelayerSubmitError(
            "py_builder_relayer_client is not installed; cannot submit relayer call"
        ) from exc

    validate_private_key(private_key)
    if not data or not str(data).startswith("0x"):
        raise RelayerSubmitError("data must be 0x-prefixed calldata")
    if int(deadline_seconds) <= 0:
        raise RelayerSubmitError("deadline_seconds must be positive")

    relayer = normalize_relayer_url(relayer_url)
    headers = relayer_headers(api_key, api_key_address)
    client = RelayClient(relayer, CHAIN_ID, private_key)
    owner = client.signer.address()
    nonce_payload = request_json(
        "GET",
        f"{relayer}{NONCE_PATH}",
        params={"address": owner, "type": TransactionType.WALLET.value},
    )
    if not isinstance(nonce_payload, dict):
        raise RelayerSubmitError(f"Unexpected relayer nonce payload: {nonce_payload!r}")
    nonce = str(nonce_payload.get("nonce", ""))
    if not nonce:
        raise RelayerSubmitError(f"Unexpected relayer nonce payload: {nonce_payload!r}")

    call = DepositWalletCall(
        target=validate_address(target, name="target contract"),
        value=str(value or "0"),
        data=str(data),
    )
    args = DepositWalletTransactionArgs(
        from_address=owner,
        chain_id=CHAIN_ID,
        wallet_address=validate_address(deposit_wallet, name="deposit wallet"),
        nonce=nonce,
        deadline=str(int(time.time()) + int(deadline_seconds)),
        calls=[call],
    )
    body = build_deposit_wallet_batch_request(
        signer=client.signer,
        args=args,
        config=client.contract_config,
    ).to_dict()
    return request_json(
        "POST",
        f"{relayer}{SUBMIT_PATH}",
        headers=headers,
        body=body,
    )


def submit_deposit_wallet_pusd_transfer(
    *,
    relayer_url: str,
    api_key: str,
    api_key_address: str,
    private_key: str,
    deposit_wallet: str,
    to_address: str,
    amount_base_units: int,
    deadline_seconds: int = 600,
) -> dict[str, Any]:
    """Submit a deposit-wallet pUSD transfer through the Polymarket relayer."""
    if amount_base_units <= 0:
        raise RelayerSubmitError("amount_base_units must be positive")

    return submit_deposit_wallet_contract_call(
        relayer_url=relayer_url,
        api_key=api_key,
        api_key_address=api_key_address,
        private_key=private_key,
        deposit_wallet=deposit_wallet,
        target=PUSD_ADDRESS,
        value="0",
        data=erc20_transfer_calldata(
            validate_address(to_address, name="return wallet"),
            amount_base_units,
        ),
        deadline_seconds=deadline_seconds,
    )


def submit_proxy_wallet_pusd_transfer(
    *,
    relayer_url: str,
    api_key: str,
    api_key_address: str,
    private_key: str,
    to_address: str,
    amount_base_units: int,
    metadata: str = "cryptobot proxy pUSD transfer to deposit wallet",
) -> dict[str, Any]:
    """Submit a legacy proxy-wallet pUSD transfer through the relayer.

    This funds the deterministic Polymarket deposit wallet from the legacy
    proxy wallet derived from the same signer. It is intentionally separate
    from the dashboard return-transfer path, which moves pUSD out of the
    deposit wallet.
    """
    if amount_base_units <= 0:
        raise RelayerSubmitError("amount_base_units must be positive")

    try:
        from py_builder_relayer_client.builder.proxy import (
            build_proxy_transaction_request,
        )
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.encode.proxy import encode_proxy_transaction_data
        from py_builder_relayer_client.models import (
            CallType,
            ProxyTransaction,
            ProxyTransactionArgs,
            TransactionType,
        )
    except ModuleNotFoundError as exc:
        raise RelayerSubmitError(
            "py_builder_relayer_client is not installed; cannot submit proxy transfer"
        ) from exc

    validate_private_key(private_key)
    relayer = normalize_relayer_url(relayer_url)
    headers = relayer_headers(api_key, api_key_address)
    client = RelayClient(relayer, CHAIN_ID, private_key)
    owner = client.signer.address()
    relay_payload = request_json(
        "GET",
        f"{relayer}{RELAY_PAYLOAD_PATH}",
        params={"address": owner, "type": TransactionType.PROXY.value},
    )
    if not isinstance(relay_payload, dict):
        raise RelayerSubmitError(
            f"Unexpected proxy relay payload: {relay_payload!r}"
        )
    relay = str(relay_payload.get("address", ""))
    nonce = str(relay_payload.get("nonce", ""))
    if not relay or not nonce:
        raise RelayerSubmitError(
            f"Unexpected proxy relay payload: {relay_payload!r}"
        )

    transaction = ProxyTransaction(
        to=PUSD_ADDRESS,
        type_code=CallType.Call,
        data=erc20_transfer_calldata(
            validate_address(to_address, name="deposit wallet"),
            amount_base_units,
        ),
        value="0",
    )
    encoded_data = encode_proxy_transaction_data([transaction])
    try:
        gas_limit = client._estimate_proxy_gas(
            owner,
            client.contract_config.proxy_factory,
            encoded_data,
        )
    except Exception as exc:  # noqa: BLE001
        raise RelayerSubmitError(f"proxy gas estimate failed: {exc}") from exc
    args = ProxyTransactionArgs(
        from_address=owner,
        nonce=nonce,
        gas_price="0",
        data=encoded_data,
        relay=relay,
        gas_limit=gas_limit,
    )
    body = build_proxy_transaction_request(
        client.signer,
        args,
        client.contract_config,
        metadata=metadata,
    ).to_dict()
    return request_json(
        "POST",
        f"{relayer}{SUBMIT_PATH}",
        headers=headers,
        body=body,
    )


def poll_relayer_transaction(
    *,
    relayer_url: str,
    transaction_id: str,
    max_polls: int = 30,
    sleep_seconds: float = 2.0,
) -> dict[str, Any] | None:
    """Poll a relayer transaction until it reaches a terminal state."""
    if not transaction_id:
        raise RelayerSubmitError("transaction_id is required")
    if int(max_polls) <= 0:
        raise RelayerSubmitError("max_polls must be positive")
    if float(sleep_seconds) < 0:
        raise RelayerSubmitError("sleep_seconds must be non-negative")

    relayer = normalize_relayer_url(relayer_url)
    for _ in range(int(max_polls)):
        payload = request_json(
            "GET",
            f"{relayer}{TRANSACTION_PATH}",
            params={"id": transaction_id},
        )
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if not isinstance(row, dict):
                continue
            state = row.get("state")
            if state == CONFIRMED_STATE or state in FAILED_STATES:
                return row
        time.sleep(float(sleep_seconds))
    return None


def build_pusd_transfer_submitter(config: object):
    """Build the payload->response callable consumed by transfer flows."""
    if not relayer_configured(config):
        raise RelayerSubmitError("relayer_not_configured: missing relayer env")

    relayer_url = str(getattr(config, "relayer_url", ""))
    api_key = _secret_value(getattr(config, "relayer_api_key", ""))
    api_key_address = str(getattr(config, "relayer_api_key_address", ""))
    private_key = _secret_value(getattr(config, "poly_private_key", ""))
    configured_deposit = str(getattr(config, "poly_funder", ""))

    def _submit(payload: dict[str, Any]) -> dict[str, Any]:
        from_wallet = str(payload.get("from_wallet") or configured_deposit)
        to_wallet = str(payload.get("to_wallet") or "")
        response = submit_deposit_wallet_pusd_transfer(
            relayer_url=relayer_url,
            api_key=api_key,
            api_key_address=api_key_address,
            private_key=private_key,
            deposit_wallet=from_wallet,
            to_address=to_wallet,
            amount_base_units=_amount_to_base_units(payload.get("amount_usd")),
        )
        return normalize_relayer_response(response)

    return _submit


def build_transfer_submitter(config: object):
    """Alias used by the operator CLI."""
    return build_pusd_transfer_submitter(config)
