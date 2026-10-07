"""predict.fun wallet smoke checks (the predict.fun counterpart of
:mod:`cryptobot_polymarket.wallet_diagnostics`).

  python -m cryptobot_predictfun.wallet_diagnostics --probe-api
  python -m cryptobot_predictfun.wallet_diagnostics --probe-chain
  python -m cryptobot_predictfun.wallet_diagnostics --topology

`--probe-api` checks REST connectivity + x-api-key auth (``GET /markets`` → 200 vs
401). `--probe-chain` reads the wallet's collateral (USDT/BSC-USD) balance +
allowance-vs-exchange + native BNB gas balance over the BNB JSON-RPC. `--topology`
derives the signer address and shows the resolved contract addresses, with clear
NOT-CONFIGURED states when the envs are absent (env-first → predict_sdk → unset).

Exit codes:
  0 — probe succeeded / topology printed
  1 — REST or chain unreachable
  2 — credential / config error (NOT-CONFIGURED)
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

import httpx

from cryptobot_predictfun import wallet as pfw
from cryptobot_predictfun._http import probe_markets

logger = logging.getLogger(__name__)


def _cfg():
    from cryptobot import config

    return config


def _signer_address(cfg) -> Optional[str]:
    key = str(cfg.PREDICTFUN_PRIVATE_KEY or "").strip()
    if not key:
        return None
    return pfw.derive_signer_address(key)


def _probe_api(cfg, *, client: httpx.Client | None = None) -> int:
    base = str(cfg.PREDICTFUN_API_BASE).rstrip("/")
    api_key = str(cfg.PREDICTFUN_API_KEY or "").strip()
    testnet = "testnet" in base.lower()
    # Shared probe (predictfun._http): x-api-key header, 200/401 classification.
    code, ok, detail = probe_markets(base, api_key, client=client, timeout=3.0)
    if ok:
        print(f"predictfun_api_ok base={base} status=200 authorized={bool(api_key) or testnet}")
        return 0
    if code is None:
        logger.error("predictfun_api_unreachable: %s", detail)
        return 1
    if code == 401:
        if testnet:
            logger.error("401 on a testnet base is unexpected (public); check base URL")
            return 1
        logger.error("predictfun_api_unauthorized: set PREDICTFUN_API_KEY (mainnet requires it)")
        return 2
    logger.error("predictfun_api_error: status=%d base=%s", code, base)
    return 1


def _probe_chain(cfg) -> int:
    address = _signer_address(cfg)
    if address is None:
        logger.error(
            "PREDICTFUN_PRIVATE_KEY empty / underivable — cannot resolve the wallet "
            "address to probe (NOT-CONFIGURED)"
        )
        return 2
    collateral = pfw.resolve_collateral_token(
        int(cfg.PREDICTFUN_CHAIN_ID), override=cfg.PREDICTFUN_COLLATERAL_TOKEN
    )
    decimals = int(cfg.PREDICTFUN_COLLATERAL_DECIMALS)
    rpc = str(cfg.PREDICTFUN_RPC_URL)
    try:
        balances = pfw.read_wallet_balances(
            rpc_url=rpc,
            wallet_address=address,
            collateral_token=collateral,
            collateral_decimals=decimals,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("predictfun_chain_unreachable: %s", exc)
        return 1
    if balances.gas_balance_bnb is None and balances.collateral_balance is None:
        logger.error("predictfun_chain_unreachable: no balance readable at %s", rpc)
        return 1
    print(f"wallet={address}")
    if collateral is None:
        print("collateral=NOT-CONFIGURED (set PREDICTFUN_COLLATERAL_TOKEN or install predict_sdk)")
    else:
        bal = balances.collateral_balance
        print(f"collateral_token={collateral} {balances.collateral_symbol}_balance={bal if bal is not None else 'unreadable'}")
        # Allowance vs the exchange contract (0 allowance blocks fills).
        exchange = pfw.resolve_exchange_contract(
            int(cfg.PREDICTFUN_CHAIN_ID), override=cfg.PREDICTFUN_EXCHANGE_CONTRACT
        )
        if exchange is None:
            print("exchange_contract=NOT-CONFIGURED (allowance check skipped)")
        else:
            try:
                allowance = pfw.read_erc20_allowance(
                    rpc_url=rpc,
                    token_address=collateral,
                    owner_address=address,
                    spender_address=exchange,
                    decimals=decimals,
                )
                print(f"exchange_contract={exchange} collateral_allowance={allowance}")
            except Exception as exc:  # noqa: BLE001
                logger.warning("allowance read failed: %s", exc)
                print(f"exchange_contract={exchange} collateral_allowance=unreadable")
    gas = balances.gas_balance_bnb
    print(f"bnb_gas_balance={gas if gas is not None else 'unreadable'}")
    return 0


def _topology(cfg) -> int:
    chain_id = int(cfg.PREDICTFUN_CHAIN_ID)
    address = _signer_address(cfg)
    print(f"chain_id={chain_id}")
    print(f"api_base={cfg.PREDICTFUN_API_BASE}")
    print(f"api_key_present={bool(str(cfg.PREDICTFUN_API_KEY or '').strip())}")
    print(f"signer_address={address or 'NOT-CONFIGURED (no PREDICTFUN_PRIVATE_KEY / eth_account)'}")
    for label, resolver, override in (
        ("collateral_token", pfw.resolve_collateral_token, cfg.PREDICTFUN_COLLATERAL_TOKEN),
        ("exchange_contract", pfw.resolve_exchange_contract, cfg.PREDICTFUN_EXCHANGE_CONTRACT),
        ("ctf_contract", pfw.resolve_ctf_contract, cfg.PREDICTFUN_CTF_CONTRACT),
    ):
        resolved = resolver(chain_id, override=override)
        print(f"{label}={resolved or 'NOT-CONFIGURED (set env or install predict_sdk)'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="cryptobot_predictfun.wallet_diagnostics",
        description="Probe predict.fun REST + BNB chain for the configured wallet",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--probe-api", action="store_true", help="check REST connectivity + x-api-key auth")
    group.add_argument("--probe-chain", action="store_true", help="read collateral + allowance + BNB gas balance")
    group.add_argument("--topology", action="store_true", help="derive signer + show resolved contract addresses")
    args = parser.parse_args(argv)
    cfg = _cfg()

    if args.probe_api:
        return _probe_api(cfg)
    if args.probe_chain:
        return _probe_chain(cfg)
    if args.topology:
        return _topology(cfg)
    return 2


if __name__ == "__main__":
    sys.exit(main())
