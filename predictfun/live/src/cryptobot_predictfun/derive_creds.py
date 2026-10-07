"""Derive / verify predict.fun wallet + API credentials for cryptobot.

The predict.fun counterpart of :mod:`cryptobot_polymarket.derive_creds`. Unlike
Polymarket (where the CLOB API trio is DERIVED from the signer key), predict.fun
issues the API key out-of-band, so this CLI's job is:

  * default            — derive and DISPLAY the EOA wallet address from
    ``PREDICTFUN_PRIVATE_KEY`` (never prints the private key), plus the env
    template the operator should set.
  * ``--check``        — validate env COMPLETENESS (key format + which of the
    LIVE-required vars are present) AND API-key LIVENESS: a ``GET
    {API_BASE}/markets`` with the x-api-key header must return 200; 401 means the key
    is missing/invalid on a mainnet base. The PUBLIC testnet base needs no key
    (a 200 without one is healthy).
  * ``--probe-balance`` — read the wallet's collateral (USDT/BSC-USD) + native
    BNB gas balance from the BNB RPC. Needs NO API key (chain reads are
    unauthenticated); needs the wallet ADDRESS (derived from the key, or
    ``--address``).

Exit codes follow :mod:`cryptobot.cli._exitcodes` (OK / NOGO / USAGE).
Fail-closed: an unverifiable API state or a missing required env returns NOGO.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from cryptobot.cli._exitcodes import NOGO, OK, USAGE
from cryptobot_predictfun import wallet as pfw

# The shared probe (GET /markets, 200/401 classification, x-api-key header —
# probe 2026-07-21: a Bearer probe 401s and would falsely report a valid key as
# bad) is single-sourced in predictfun._http; the diagnostics CLI uses it too.
from cryptobot_predictfun._http import probe_markets as _check_api_liveness

# Single source of the key-shape regex (predictfun_wallet uses it to derive).
_PRIVATE_KEY_RE = pfw._PRIVATE_KEY_RE


def _cfg():
    from cryptobot import config

    return config


def _is_testnet_base(base: str) -> bool:
    return "testnet" in (base or "").lower()


def _derive_address(key: str) -> str:
    if not _PRIVATE_KEY_RE.match(key or ""):
        raise SystemExit(
            "PREDICTFUN_PRIVATE_KEY must be 0x-prefixed with exactly 64 hex "
            "characters. Refusing to derive from an invalid key."
        )
    address = pfw.derive_signer_address(key)
    if not address:
        raise SystemExit(
            "Could not derive the predict.fun signer address — install eth_account "
            "(`pip install eth-account`) or pass a valid --key."
        )
    return address


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cryptobot_predictfun.derive_creds",
        description=__doc__.split("\n\n")[0],
    )
    parser.add_argument(
        "--key",
        help="EOA signer private key. When omitted, read PREDICTFUN_PRIVATE_KEY.",
    )
    parser.add_argument(
        "--address",
        help="Wallet address to probe when no private key is available.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate env completeness + API-key liveness (200 vs 401).",
    )
    parser.add_argument(
        "--probe-balance",
        action="store_true",
        help="Read collateral + BNB gas balance from the BNB RPC (no API key).",
    )
    return parser


def _resolve_address(args: argparse.Namespace, key: str) -> Optional[str]:
    if args.address:
        if not pfw.is_bnb_address(args.address):
            raise SystemExit("--address must be a 0x-prefixed 20-byte address")
        return args.address.strip()
    if key:
        return _derive_address(key)
    return None


def _print_probe_balance(address: str) -> None:
    cfg = _cfg()
    collateral = pfw.resolve_collateral_token(
        int(cfg.PREDICTFUN_CHAIN_ID),
        override=cfg.PREDICTFUN_COLLATERAL_TOKEN,
    )
    balances = pfw.read_wallet_balances(
        rpc_url=cfg.PREDICTFUN_RPC_URL,
        wallet_address=address,
        collateral_token=collateral,
        collateral_decimals=int(cfg.PREDICTFUN_COLLATERAL_DECIMALS),
    )
    print()
    if collateral is None:
        print(
            "collateral token address unresolved (set PREDICTFUN_COLLATERAL_TOKEN "
            "or install predict_sdk) — collateral balance skipped"
        )
    else:
        bal = balances.collateral_balance
        bal_txt = f"{bal:.4f}" if bal is not None else "unreadable"
        print(f"{balances.collateral_symbol} collateral balance: {bal_txt}")
    gas = balances.gas_balance_bnb
    gas_txt = f"{gas:.6f}" if gas is not None else "unreadable"
    print(f"BNB gas balance: {gas_txt}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = _cfg()

    key = (args.key or cfg.PREDICTFUN_PRIVATE_KEY or "").strip()
    base = str(cfg.PREDICTFUN_API_BASE)
    api_key = str(cfg.PREDICTFUN_API_KEY or "").strip()
    testnet = _is_testnet_base(base)

    if args.check:
        any_fail = False
        # 1. Private key present + well-formed (LIVE-required).
        if not key:
            print("  PREDICTFUN_PRIVATE_KEY: MISSING (required for LIVE order signing)")
            any_fail = True
        elif not _PRIVATE_KEY_RE.match(key):
            print("  PREDICTFUN_PRIVATE_KEY: INVALID (must be 0x + 64 hex)")
            any_fail = True
        else:
            addr = pfw.derive_signer_address(key)
            if addr:
                print(f"  PREDICTFUN_PRIVATE_KEY: OK (signer {addr})")
            else:
                print("  PREDICTFUN_PRIVATE_KEY: OK (format valid; eth_account absent, address not derived)")
        # 2. API key presence (mainnet required; testnet optional).
        if api_key:
            print("  PREDICTFUN_API_KEY: present")
        elif testnet:
            print("  PREDICTFUN_API_KEY: absent (OK — testnet base is public)")
        else:
            print("  PREDICTFUN_API_KEY: MISSING (mainnet base requires it)")
            any_fail = True
        # 3. API liveness (GET /markets -> 200 vs 401).
        code, ok, detail = _check_api_liveness(base, api_key)
        print(f"  API liveness ({base}): {detail}")
        if not ok:
            any_fail = True
        print()
        if any_fail:
            print("predict.fun credential check FAILED — resolve the items above.")
        else:
            print("predict.fun credentials complete and API is reachable.")
        if args.probe_balance:
            addr = _resolve_address(args, key)
            if addr:
                _print_probe_balance(addr)
        return NOGO if any_fail else OK

    # Default (display) mode.
    address = _resolve_address(args, key)
    if address is None:
        print(
            "No PREDICTFUN_PRIVATE_KEY / --key / --address given — nothing to "
            "derive.",
            file=sys.stderr,
        )
        return USAGE
    print("# predict.fun wallet. Paste the relevant lines into the bot env.")
    print(f"# Derived EOA signer address: {address}")
    print(f"PREDICTFUN_API_BASE={base}")
    print("# PREDICTFUN_API_KEY=<mainnet API key (sent as x-api-key); omit for the public testnet base>")
    print("# PREDICTFUN_PRIVATE_KEY=<the signer key; NEVER printed by this CLI>")
    print(f"PREDICTFUN_CHAIN_ID={cfg.PREDICTFUN_CHAIN_ID}")
    if args.probe_balance:
        _print_probe_balance(address)
    return OK


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
