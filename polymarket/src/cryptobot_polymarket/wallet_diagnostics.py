"""Polymarket wallet smoke checks.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U11a:

  python -m cryptobot_polymarket.wallet_diagnostics --probe-clob
  python -m cryptobot_polymarket.wallet_diagnostics --probe-chain

`--probe-clob` instantiates OrderClient against the live POLY_* creds
and prints the funder's pUSD balance. `--probe-chain` reads the same
balance via Polygon RPC (when `POLYGON_RPC_URL` is set; otherwise
prints `chain_balance=skipped`).

Exit codes:
  0 — probe succeeded
  1 — CLOB or chain unreachable
  2 — credential / config error (caught by the boot gate)
"""

from __future__ import annotations

import argparse
import logging
import sys
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

from cryptobot_polymarket.derive_creds import _probe_balance
from cryptobot_polymarket.relayer import (
    RelayerSubmitError,
    normalize_relayer_response,
    poll_relayer_transaction,
    submit_proxy_wallet_pusd_transfer,
)
from cryptobot_polymarket.walker import (
    ClobClientUnavailable,
    OrderClient,
    _validate_wallet_config,
)
from cryptobot_polymarket.primitives import (
    CONFIRMED_STATE,
    FAILED_STATES,
    PUSD_DECIMALS,
    derive_wallets,
    normalize_relayer_url,
    validate_address,
    validate_private_key,
)
from cryptobot.runtime_secrets import get_config


logger = logging.getLogger(__name__)


def _secret_value(value: object) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        return str(getter() or "")
    return str(value or "")


def _amount_to_base_units(amount: str | float | Decimal) -> int:
    if isinstance(amount, str):
        cleaned = amount.strip().replace(",", "")
        if cleaned.startswith("$"):
            cleaned = cleaned[1:].strip()
        lower = cleaned.lower()
        for suffix in ("pusd", "usd"):
            if lower.endswith(suffix):
                cleaned = cleaned[: -len(suffix)].strip()
                break
        amount = cleaned
    try:
        decimal_amount = Decimal(str(amount))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("amount must be a decimal pUSD value") from exc
    if not decimal_amount.is_finite() or decimal_amount <= 0:
        raise ValueError("amount must be greater than 0")
    base_units = (
        decimal_amount * (Decimal(10) ** PUSD_DECIMALS)
    ).to_integral_value(rounding=ROUND_DOWN)
    if base_units <= 0:
        raise ValueError("amount is below one pUSD base unit")
    return int(base_units)


def _base_units_to_usd(amount_base_units: int) -> Decimal:
    return (Decimal(amount_base_units) / (Decimal(10) ** PUSD_DECIMALS)).quantize(
        Decimal("0.000001")
    )


def _transaction_id(response: dict[str, Any]) -> str:
    return str(
        response.get("relayer_tx_id")
        or response.get("transactionID")
        or response.get("transactionId")
        or ""
    )


def _tx_hash(response: dict[str, Any]) -> str:
    return str(
        response.get("tx_hash")
        or response.get("txHash")
        or response.get("transactionHash")
        or response.get("hash")
        or ""
    )


def _probe_clob() -> int:
    """Try to construct an OrderClient and read the funder pUSD balance."""
    cfg = get_config()
    try:
        _validate_wallet_config(cfg.poly_signature_type, cfg.poly_funder)
    except ValueError as exc:
        logger.error("invalid wallet topology: %s", exc)
        return 2

    try:
        client = OrderClient(cfg)
    except ClobClientUnavailable as exc:
        logger.error("clob_client_unavailable: %s", exc)
        return 1
    except ValueError as exc:
        logger.error("OrderClient init failed: %s", exc)
        return 2

    balance = client.check_balance()
    if balance is None:
        logger.error("check_balance returned None (CLOB unreachable)")
        return 1
    print(f"clob_balance_usd={balance:.4f} funder={cfg.poly_funder}")
    return 0


def _probe_chain() -> int:
    """Read the funder's on-chain pUSD ERC-20 balance via Polygon RPC.

    A configured RPC URL is probed with the same raw ERC-20 balanceOf path
    used by live readiness; an empty POLYGON_RPC_URL is skipped.
    """
    cfg = get_config()
    if not cfg.polygon_rpc_url:
        print("chain_balance=skipped (POLYGON_RPC_URL empty)")
        return 0
    if not cfg.poly_funder:
        logger.error("POLY_FUNDER is empty; cannot query chain balance")
        return 2
    try:
        from cryptobot_polymarket.balance import build_pusd_balance_fetcher

        balance = build_pusd_balance_fetcher(cfg)()
    except ValueError as exc:
        logger.error("invalid chain probe config: %s", exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.error("chain_balance_unreachable: %s", exc)
        return 1
    print(f"chain_balance_usd={balance:.4f} funder={cfg.poly_funder}")
    return 0


def _transfer_pusd_from_proxy(args: argparse.Namespace) -> int:
    """Move legacy proxy pUSD into the configured POLY_FUNDER deposit wallet."""
    cfg = get_config()
    private_key = _secret_value(cfg.poly_private_key)
    api_trio = (
        _secret_value(cfg.poly_api_key),
        _secret_value(cfg.poly_secret),
        _secret_value(cfg.poly_passphrase),
    )
    relayer_api_key = _secret_value(cfg.relayer_api_key)
    relayer_api_key_address = str(cfg.relayer_api_key_address or "").strip()
    relayer_url = normalize_relayer_url(str(cfg.relayer_url or ""))
    configured_funder = str(cfg.poly_funder or "").strip()

    try:
        validate_private_key(private_key)
        validate_address(configured_funder, name="POLY_FUNDER")
        wallets = derive_wallets(
            private_key,
            relayer_url=relayer_url,
            rpc_url=str(cfg.polygon_rpc_url or "") or None,
        )
    except (RuntimeError, ValueError) as exc:
        logger.error("wallet topology unavailable: %s", exc)
        return 2

    if configured_funder.lower() != wallets.deposit.lower():
        logger.error(
            "POLY_FUNDER mismatch: configured=%s derived_deposit=%s",
            configured_funder,
            wallets.deposit,
        )
        return 2
    if not all(api_trio):
        logger.error("POLY_API_KEY/POLY_SECRET/POLY_PASSPHRASE are required")
        return 2
    if not relayer_api_key or not relayer_api_key_address:
        logger.error("RELAYER_API_KEY and RELAYER_API_KEY_ADDRESS are required")
        return 2

    try:
        proxy_balance = _probe_balance(
            private_key,
            api_trio,
            signature_type=1,
            funder=wallets.proxy,
            sync_balance=bool(args.sync_balance),
        )
    except SystemExit as exc:
        logger.error("proxy balance probe failed: %s", exc)
        return 1

    amount_arg = str(args.transfer_pusd_from_proxy or "").strip()
    try:
        if amount_arg.lower() == "all":
            if args.expected_min_usd is None or args.expected_max_usd is None:
                logger.error(
                    "--expected-min-usd and --expected-max-usd are required with all"
                )
                return 2
            min_usd = Decimal(str(args.expected_min_usd))
            max_usd = Decimal(str(args.expected_max_usd))
            detected = Decimal(str(proxy_balance))
            if min_usd > max_usd:
                logger.error("expected balance window is invalid")
                return 2
            if detected < min_usd or detected > max_usd:
                logger.error(
                    "detected proxy balance %.6f outside expected window %.6f-%.6f",
                    proxy_balance,
                    float(min_usd),
                    float(max_usd),
                )
                return 2
            amount_base_units = _amount_to_base_units(detected)
        else:
            amount_base_units = _amount_to_base_units(amount_arg)
    except (InvalidOperation, ValueError) as exc:
        logger.error("invalid transfer amount: %s", exc)
        return 2

    amount_usd = _base_units_to_usd(amount_base_units)
    if amount_usd > Decimal(str(proxy_balance)) + Decimal("0.000001"):
        logger.error(
            "transfer amount %s exceeds detected proxy balance %.6f",
            amount_usd,
            proxy_balance,
        )
        return 2

    try:
        response = normalize_relayer_response(
            submit_proxy_wallet_pusd_transfer(
                relayer_url=relayer_url,
                api_key=relayer_api_key,
                api_key_address=relayer_api_key_address,
                private_key=private_key,
                to_address=wallets.deposit,
                amount_base_units=amount_base_units,
            )
        )
    except (RelayerSubmitError, RuntimeError, ValueError) as exc:
        logger.error("proxy transfer submit failed: %s", exc)
        return 1

    if response.get("success") is False:
        logger.error("proxy transfer submit rejected: %s", response)
        return 1

    relayer_tx_id = _transaction_id(response)
    tx_hash = _tx_hash(response)
    print("proxy_pusd_transfer_submitted")
    print(f"from_proxy={wallets.proxy}")
    print(f"to_funder={wallets.deposit}")
    print(f"amount_usd={amount_usd}")
    if relayer_tx_id:
        print(f"relayer_tx_id={relayer_tx_id}")
    if tx_hash:
        print(f"tx_hash={tx_hash}")

    if not args.wait:
        return 0
    if not relayer_tx_id:
        logger.error("relayer response did not include a transaction id")
        return 1

    try:
        final = poll_relayer_transaction(
            relayer_url=relayer_url,
            transaction_id=relayer_tx_id,
            max_polls=int(args.polls),
            sleep_seconds=float(args.poll_seconds),
        )
    except (RelayerSubmitError, RuntimeError, ValueError) as exc:
        logger.error("relayer transaction poll failed: %s", exc)
        return 1
    if final is None:
        logger.error("relayer transaction did not reach a terminal state")
        return 1

    final_hash = _tx_hash(final)
    state = str(final.get("state") or "")
    print(f"relayer_final_state={state}")
    if final_hash:
        print(f"final_tx_hash={final_hash}")
    if state == CONFIRMED_STATE:
        return 0
    if state in FAILED_STATES:
        logger.error("relayer transaction failed: %s", final)
        return 1
    logger.error("unexpected relayer terminal payload: %s", final)
    return 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="cryptobot_polymarket.wallet_diagnostics",
        description="Probe Polymarket CLOB and Polygon RPC for the funder",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--probe-clob",
        action="store_true",
        help="construct OrderClient and read pUSD balance",
    )
    group.add_argument(
        "--probe-chain",
        action="store_true",
        help="read on-chain pUSD balance via Polygon RPC",
    )
    group.add_argument(
        "--transfer-pusd-from-proxy",
        metavar="AMOUNT",
        help=(
            "submit a legacy proxy-wallet pUSD transfer to POLY_FUNDER; "
            "use 'all' with --expected-min-usd/--expected-max-usd"
        ),
    )
    parser.add_argument(
        "--expected-min-usd",
        type=float,
        default=None,
        help="minimum detected proxy pUSD allowed when AMOUNT is all",
    )
    parser.add_argument(
        "--expected-max-usd",
        type=float,
        default=None,
        help="maximum detected proxy pUSD allowed when AMOUNT is all",
    )
    parser.add_argument(
        "--sync-balance",
        action="store_true",
        help="call CLOB update_balance_allowance before probing proxy balance",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="poll relayer transaction until confirmed or failed",
    )
    parser.add_argument(
        "--polls",
        type=int,
        default=30,
        help="maximum relayer transaction polls when --wait is set",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=2.0,
        help="seconds between relayer transaction polls when --wait is set",
    )
    args = parser.parse_args(argv)

    if args.probe_clob:
        return _probe_clob()
    if args.probe_chain:
        return _probe_chain()
    if args.transfer_pusd_from_proxy:
        return _transfer_pusd_from_proxy(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
