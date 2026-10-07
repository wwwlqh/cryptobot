"""Derive Polymarket CLOB API credentials for cryptobot.

The private key is read from runtime config unless --key is provided. The CLI
never prints the private key value.
"""

from __future__ import annotations

import argparse
import re
import sys

from cryptobot.cli._exitcodes import NOGO, OK
from cryptobot.cli.env_parity import env_fingerprint
from cryptobot_polymarket.walker import (
    _SIGNATURE_TYPE_LABELS,
    _validate_wallet_config as _validate_wallet_config_shared,
)


_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _validate_key(key: str) -> None:
    if not _PRIVATE_KEY_RE.match(key or ""):
        raise SystemExit(
            "Private key must be 0x-prefixed with exactly 64 hex characters. "
            "Refusing to call CLOB with an invalid key."
        )


def _validate_wallet_config(
    signature_type: int | None,
    funder: str | None,
) -> tuple[int, str]:
    try:
        return _validate_wallet_config_shared(signature_type, funder)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def _client_kwargs(signature_type: int, funder: str) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    if signature_type != 0:
        kwargs["signature_type"] = signature_type
    if funder:
        kwargs["funder"] = funder
    return kwargs


def _derive(
    key: str,
    *,
    signature_type: int = 0,
    funder: str = "",
) -> tuple[str, str, str]:
    _validate_key(key)
    signature_type, funder = _validate_wallet_config(signature_type, funder)

    try:
        from py_clob_client_v2.client import ClobClient
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "py_clob_client_v2 is not installed; install the bot's runtime "
            "dependencies before running this CLI."
        ) from exc

    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=key,
        **_client_kwargs(signature_type, funder),
    )
    creds = client.create_or_derive_api_key()
    return creds.api_key, creds.api_secret, creds.api_passphrase


def _probe_balance(
    key: str,
    trio: tuple[str, str, str],
    *,
    signature_type: int,
    funder: str,
    sync_balance: bool = False,
) -> float:
    _validate_key(key)
    signature_type, funder = _validate_wallet_config(signature_type, funder)

    try:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import (
            ApiCreds,
            AssetType,
            BalanceAllowanceParams,
        )
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "py_clob_client_v2 is not installed; install the bot's runtime "
            "dependencies before running this CLI."
        ) from exc

    api_key, api_secret, api_passphrase = trio
    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=key,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        ),
        **_client_kwargs(signature_type, funder),
    )
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    if sync_balance:
        client.update_balance_allowance(params)
    result = client.get_balance_allowance(params)
    try:
        return int(result.get("balance", "0")) / 1e6
    except (AttributeError, TypeError, ValueError) as exc:
        raise SystemExit(f"Unexpected CLOB balance response: {result!r}") from exc


def _print_probe_balance(
    key: str,
    trio: tuple[str, str, str],
    *,
    signature_type: int,
    funder: str,
    sync_balance: bool = False,
) -> None:
    balance = _probe_balance(
        key,
        trio,
        signature_type=signature_type,
        funder=funder,
        sync_balance=sync_balance,
    )
    label = _SIGNATURE_TYPE_LABELS[signature_type]
    funder_text = funder or "(auto-derived)"
    print()
    if sync_balance:
        print("CLOB balance allowance sync requested before probe.")
    print(
        f"pUSD balance for signature_type={signature_type} ({label}), "
        f"funder={funder_text}: ${balance:.2f}"
    )


def _warn_suspicious_topology(signature_type: int, funder: str) -> None:
    if signature_type == 1 and funder:
        print(
            "WARNING: POLY_SIGNATURE_TYPE=1 with POLY_FUNDER set. If this is "
            "a Polymarket deposit wallet, use POLY_SIGNATURE_TYPE=3 with the "
            "deposit wallet as POLY_FUNDER.",
            file=sys.stderr,
        )


def _load_runtime_values(args: argparse.Namespace) -> tuple[str, tuple[str, str, str] | None, int, str]:
    if args.key:
        signature_type = args.signature_type if args.signature_type is not None else 0
        return args.key, None, signature_type, args.funder or ""

    try:
        from cryptobot.runtime_secrets import get_config
    except ImportError as exc:
        raise SystemExit(
            "Could not import cryptobot.runtime_secrets; run from the project "
            "root with the bot's environment active."
        ) from exc

    cfg = get_config()
    key = cfg.poly_private_key.get_secret_value()
    if not key:
        raise SystemExit(
            "POLY_PRIVATE_KEY is empty in runtime config. Set it or pass "
            "--key explicitly."
        )
    current_trio = (
        cfg.poly_api_key.get_secret_value(),
        cfg.poly_secret.get_secret_value(),
        cfg.poly_passphrase.get_secret_value(),
    )
    signature_type = (
        args.signature_type
        if args.signature_type is not None
        else cfg.poly_signature_type
    )
    funder = args.funder if args.funder is not None else cfg.poly_funder
    return key, current_trio, signature_type, funder


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--key",
        help="L2 signer private key. When omitted, read runtime config.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Compare derived credentials against configured POLY_API_* values.",
    )
    parser.add_argument(
        "--signature-type",
        type=int,
        choices=sorted(_SIGNATURE_TYPE_LABELS),
        default=None,
        help=(
            "Polymarket wallet signature type: 0=EOA, 1=POLY_PROXY, "
            "2=POLY_GNOSIS_SAFE, 3=POLY_1271 deposit wallet."
        ),
    )
    parser.add_argument(
        "--funder",
        default=None,
        help="Wallet address holding funds. Required for signature type 3.",
    )
    parser.add_argument(
        "--probe-balance",
        action="store_true",
        help="After deriving credentials, print CLOB pUSD balance.",
    )
    parser.add_argument(
        "--sync-balance",
        action="store_true",
        help="Call CLOB update_balance_allowance before probing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.sync_balance:
        args.probe_balance = True

    key, current_trio, signature_type, funder = _load_runtime_values(args)
    signature_type, funder = _validate_wallet_config(signature_type, funder)
    _warn_suspicious_topology(signature_type, funder)

    api_key, api_secret, api_passphrase = _derive(
        key,
        signature_type=signature_type,
        funder=funder,
    )

    if args.check:
        if current_trio is None:
            raise SystemExit(
                "--check requires reading existing credentials from runtime "
                "config. Drop --key to compare."
            )
        cur_k, cur_s, cur_p = current_trio
        results = [
            ("POLY_API_KEY", cur_k, api_key),
            ("POLY_SECRET", cur_s, api_secret),
            ("POLY_PASSPHRASE", cur_p, api_passphrase),
        ]
        any_mismatch = False
        for name, current, derived in results:
            if not current:
                print(f"  {name}: MISSING in config (derived fingerprint={env_fingerprint(derived)})")
                any_mismatch = True
            elif current == derived:
                print(f"  {name}: OK")
            else:
                print(
                    f"  {name}: MISMATCH - config fingerprint={env_fingerprint(current)} "
                    f"but derived fingerprint={env_fingerprint(derived)}"
                )
                any_mismatch = True
        if any_mismatch:
            print()
            print("One or more values differ. Update runtime config with the derived trio.")
            return NOGO
        print()
        print("All three configured CLOB credentials match the selected signer.")
        if args.probe_balance:
            _print_probe_balance(
                key,
                (api_key, api_secret, api_passphrase),
                signature_type=signature_type,
                funder=funder,
                sync_balance=args.sync_balance,
            )
        return OK

    print("# Derived CLOB API credentials. Paste these lines into the bot env.")
    print(
        f"# Wallet topology: POLY_SIGNATURE_TYPE={signature_type} "
        f"({_SIGNATURE_TYPE_LABELS[signature_type]}), "
        f"POLY_FUNDER={funder or '(blank/auto-derived)'}"
    )
    print(f"POLY_API_KEY={api_key}")
    print(f"POLY_SECRET={api_secret}")
    print(f"POLY_PASSPHRASE={api_passphrase}")
    print(f"POLY_SIGNATURE_TYPE={signature_type}")
    print(f"POLY_FUNDER={funder}")
    if args.probe_balance:
        _print_probe_balance(
            key,
            (api_key, api_secret, api_passphrase),
            signature_type=signature_type,
            funder=funder,
            sync_balance=args.sync_balance,
        )
    return OK


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
