"""predict.fun on-chain wallet reads + redeem-positions calldata (BNB chain).

Mirrors :mod:`cryptobot_polymarket.redeemer` for the predict.fun
ctf-exchange fork (a fork of Polymarket's ctf-exchange) on BNB chain. It is
only ever reached on the ``CRYPTOBOT_VENUE=predictfun`` path (the diagnostics
CLIs, the live-readiness balance fetch and the order client's
``check_balance``); the Polymarket path never imports it. NO automated redeem
sweep is wired for this venue — ``redeem_won_position`` only BUILDS calldata
for manual/forensic use and fails closed on submit.

Provides:
  * collateral (USDT / BSC-USD, 18 decimals) + native BNB gas balance reads via
    raw JSON-RPC (NO web3 dependency — the ERC-20 balance read delegates to
    :func:`cryptobot.execution.live_readiness.read_erc20_balance`, which also
    applies its plausibility bound on the returned value);
  * ``redeemPositions`` calldata for the Gnosis CTF form
    ``redeemPositions(address,bytes32,bytes32,uint256[])`` (standard markets);
  * a FAIL-CLOSED redeem SUBMIT path gated behind
    ``config.PREDICTFUN_AUTO_REDEEM_ENABLED`` (default OFF), mirroring how
    ``AUTO_REDEEM_ENABLED`` is handled for Polymarket. There is no path that
    broadcasts a transaction.

⚠️ CONTRACT ADDRESSES. predict.fun's docs publish NO on-chain addresses; the
only public source is the ``predict_sdk`` ``ADDRESSES_BY_CHAIN_ID`` export. So
every address here is resolved ENV-first -> ``predict_sdk`` at runtime ->
FAIL-CLOSED with NO hardcoded hex. A hardcoded (possibly mis-transcribed)
contract address on a real-money redeem path is exactly the class of error this
avoids: an unverifiable address is left ``None`` and the caller refuses to act.

VERIFIED (2026-07-21, predict_sdk source + BscScan):
  * BNB mainnet (56) / testnet (97) chain ids.
  * Collateral = USDT / BSC-USD, 18 decimals (mainnet token 0x55d3...7955 on
    BscScan). The ADDRESS itself is still resolved via env/SDK, never pinned.
  * ``redeemPositions(address,bytes32,bytes32,uint256[])`` matches Gnosis CTF.
  * predict_sdk exposes ``ADDRESSES_BY_CHAIN_ID`` / ``RPC_URLS_BY_CHAIN_ID``.
"""

from __future__ import annotations

import functools
import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# EOA signer private key shape (0x + 64 hex). Single source for the predict.fun
# stack — the derive_predictfun_creds CLI reuses this instead of keeping a copy.
_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")

_BYTES32_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")

# All-zero parent collection id — the top-level collection for a binary market,
# same value the predict_sdk uses for a standard (non-neg-risk) redeem.
ZERO_HASH = "0x" + "0" * 64

# ERC-20 ``allowance(address,address)`` eth_call selector.
_ALLOWANCE_SELECTOR = "0xdd62ed3e"


def is_bnb_address(value: str | None) -> bool:
    """True when ``value`` is a 0x-prefixed 20-byte hex address.

    Same shape on BNB chain as on any EVM chain — delegates to
    :func:`cryptobot.execution.live_readiness.is_address` (one regex, not two).
    """
    from cryptobot.execution.live_readiness import is_address

    return is_address(value)


def is_bytes32(value: str | None) -> bool:
    return bool(value and _BYTES32_RE.match(value.strip()))


# ---------------------------------------------------------------------------
# Contract-address resolution (ENV -> predict_sdk -> None, fail-closed)
# ---------------------------------------------------------------------------
def _sdk_addresses(chain_id: int) -> Optional[object]:
    """Return the predict_sdk ``Addresses`` bundle for ``chain_id`` or None.

    Fail-soft on every failure mode (SDK absent, chain not in the map, any
    attribute error) — a missing SDK must never crash the diagnostics/redeem
    path, it just means the addresses have to come from the environment.
    """
    try:
        import predict_sdk  # noqa: F401
        from predict_sdk import ADDRESSES_BY_CHAIN_ID  # type: ignore
    except Exception:  # noqa: BLE001 — optional dep / API drift
        return None
    try:
        return ADDRESSES_BY_CHAIN_ID.get(chain_id) or ADDRESSES_BY_CHAIN_ID.get(
            int(chain_id)
        )
    except Exception:  # noqa: BLE001
        return None


def _sdk_attr(addresses: object, *names: str) -> Optional[str]:
    """First non-empty attribute of ``addresses`` across candidate ``names``.

    The exact field spelling in the SDK (CTF_EXCHANGE vs ctf_exchange) is not
    byte-verified, so every read tries several spellings and validates the
    result is an address before returning it.
    """
    for name in names:
        val = getattr(addresses, name, None)
        if isinstance(val, str) and is_bnb_address(val):
            return val
    return None


def resolve_collateral_token(
    chain_id: int, *, override: str = ""
) -> Optional[str]:
    """Collateral (USDT/BSC-USD) token address: env override -> SDK -> None."""
    if override and is_bnb_address(override):
        return override.strip()
    addrs = _sdk_addresses(chain_id)
    if addrs is not None:
        return _sdk_attr(addrs, "USDT", "usdt", "COLLATERAL", "collateral")
    return None


def resolve_exchange_contract(
    chain_id: int, *, override: str = ""
) -> Optional[str]:
    """ctf-exchange contract address: env override -> SDK -> None."""
    if override and is_bnb_address(override):
        return override.strip()
    addrs = _sdk_addresses(chain_id)
    if addrs is not None:
        return _sdk_attr(addrs, "CTF_EXCHANGE", "ctf_exchange", "EXCHANGE", "exchange")
    return None


def resolve_ctf_contract(chain_id: int, *, override: str = "") -> Optional[str]:
    """ConditionalTokens (CTF) contract address: env override -> SDK -> None."""
    if override and is_bnb_address(override):
        return override.strip()
    addrs = _sdk_addresses(chain_id)
    if addrs is not None:
        return _sdk_attr(
            addrs,
            "CONDITIONAL_TOKENS",
            "conditional_tokens",
            "CTF",
            "ctf",
        )
    return None


# ---------------------------------------------------------------------------
# Signer derivation
# ---------------------------------------------------------------------------
@functools.lru_cache(maxsize=16)
def _cached_signer_address(key: str) -> Optional[str]:
    """Point derivation for one well-formed key (memoized for the process —
    the key set is static config, and per-refresh balance reads re-derive)."""
    try:
        from eth_account import Account  # type: ignore
    except Exception:  # noqa: BLE001 — optional dep
        logger.info("eth_account not installed — cannot derive predict.fun signer")
        return None
    try:
        return str(Account.from_key(key).address)
    except Exception:  # noqa: BLE001
        logger.warning("predict.fun signer derivation failed", exc_info=True)
        return None


def derive_signer_address(private_key: str) -> Optional[str]:
    """Derive the EOA address from ``private_key`` (predict.fun signs as EOA).

    Returns None when ``eth_account`` is unavailable (optional dep) or the key
    is malformed — never raises, so the diagnostics CLI degrades to a clear
    "cannot derive" state instead of crashing. Successful (and failed)
    derivations are memoized per key string.
    """
    key = (private_key or "").strip()
    if not _PRIVATE_KEY_RE.match(key):
        return None
    return _cached_signer_address(key)


# ---------------------------------------------------------------------------
# On-chain reads (raw JSON-RPC — no web3)
# ---------------------------------------------------------------------------
def _addr_word(address: str) -> str:
    """ABI-encode an address as a 32-byte call-data word (hex, no 0x)."""
    return address[2:].lower().rjust(64, "0")


def _eth_rpc(rpc_url: str, method: str, params: list, *, timeout_s: float = 15.0) -> int:
    """One JSON-RPC call returning a hex quantity, decoded to int.

    Raises on any transport error / RPC error object / malformed result so the
    caller surfaces it as ``chain_unreachable`` rather than a number to trust.
    """
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError("requests is not installed") from exc

    resp = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=timeout_s,
    )
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(f"RPC error: {payload['error']!r}")
    raw = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise RuntimeError(f"Unexpected RPC {method} response: {payload!r}")
    return int(raw, 16)


def read_erc20_balance(
    *,
    rpc_url: str,
    token_address: str,
    wallet_address: str,
    decimals: int = 18,
    timeout_s: float = 15.0,
) -> float:
    """ERC-20 ``balanceOf`` via raw JSON-RPC (BNB chain, 18-dec collateral).

    Delegates to :func:`cryptobot.execution.live_readiness.read_erc20_balance`
    with a BNB-appropriate default of 18 decimals. That shared read ALSO
    applies live-readiness' plausibility bound (a corrupt/hostile RPC answer
    over $1M raises instead of feeding the readiness gate) — a deliberate
    strengthening over the first-draft clone, which had dropped the bound.
    Raises on any RPC error / malformed response so the caller surfaces it as
    ``chain_unreachable`` rather than a number to trust.
    """
    from cryptobot.execution.live_readiness import read_erc20_balance as _read

    return _read(
        rpc_url=rpc_url,
        token_address=token_address,
        wallet_address=wallet_address,
        decimals=decimals,
        timeout_s=timeout_s,
    )


def read_erc20_allowance(
    *,
    rpc_url: str,
    token_address: str,
    owner_address: str,
    spender_address: str,
    decimals: int = 18,
    timeout_s: float = 15.0,
) -> float:
    """ERC-20 ``allowance(owner, spender)`` via raw JSON-RPC.

    Used by the diagnostics to show the collateral allowance the wallet has
    granted the exchange contract (a 0 allowance blocks order fills). Selector
    ``0xdd62ed3e`` = ``allowance(address,address)``.
    """
    for name, val in (("token", token_address), ("owner", owner_address), ("spender", spender_address)):
        if not is_bnb_address(val):
            raise ValueError(f"{name}_address must be a 0x-prefixed 20-byte address")

    data = _ALLOWANCE_SELECTOR + _addr_word(owner_address) + _addr_word(spender_address)
    raw = _eth_rpc(
        rpc_url,
        "eth_call",
        [{"to": token_address, "data": data}, "latest"],
        timeout_s=timeout_s,
    )
    return raw / (10 ** decimals)


def read_native_balance(
    *,
    rpc_url: str,
    wallet_address: str,
    timeout_s: float = 15.0,
) -> float:
    """Native BNB gas balance (``eth_getBalance``), in BNB (18 decimals)."""
    if not is_bnb_address(wallet_address):
        raise ValueError("wallet_address must be a 0x-prefixed 20-byte address")
    raw = _eth_rpc(
        rpc_url, "eth_getBalance", [wallet_address, "latest"], timeout_s=timeout_s
    )
    return raw / (10 ** 18)


def build_collateral_balance_fetcher():
    """Return a ``() -> Optional[float]`` reading the configured predict.fun
    wallet's collateral (USDT/BSC-USD) balance from the BNB RPC.

    FAIL-CLOSED for live-readiness (deliverable 7): the returned callable RAISES
    when the wallet plumbing is not configured (no private key, underivable
    address, unresolved collateral token) so live readiness records
    ``chain_unreachable`` / NOT_READY and the venue is kept DRY — it never
    fakes a ready balance. A configured-but-unreachable RPC also raises (surfaced
    as chain_unreachable), same as the Polymarket fetcher.
    """
    def _fetch() -> Optional[float]:
        from cryptobot import config as cfg

        # Funds custody: predict.fun UI accounts hold collateral on the Kernel
        # smart wallet (PREDICTFUN_PREDICT_ACCOUNT), NOT on the signer EOA —
        # read THAT address when configured. External-wallet accounts leave it
        # empty and the key's EOA is both signer and funds holder.
        predict_account = str(
            getattr(cfg, "PREDICTFUN_PREDICT_ACCOUNT", "") or ""
        ).strip()
        if predict_account:
            if not is_bnb_address(predict_account):
                raise RuntimeError(
                    "PREDICTFUN_PREDICT_ACCOUNT is not a valid 0x address"
                )
            address = predict_account
        else:
            key = str(getattr(cfg, "PREDICTFUN_PRIVATE_KEY", "") or "").strip()
            if not key:
                raise RuntimeError(
                    "PREDICTFUN_PRIVATE_KEY is required to read the predict.fun balance"
                )
            address = derive_signer_address(key)
            if not address:
                raise RuntimeError("could not derive the predict.fun signer address")
        token = resolve_collateral_token(
            int(getattr(cfg, "PREDICTFUN_CHAIN_ID", 56)),
            override=str(getattr(cfg, "PREDICTFUN_COLLATERAL_TOKEN", "") or ""),
        )
        if token is None:
            raise RuntimeError(
                "predict.fun collateral token address unresolved "
                "(set PREDICTFUN_COLLATERAL_TOKEN or install predict_sdk)"
            )
        return read_erc20_balance(
            rpc_url=str(getattr(cfg, "PREDICTFUN_RPC_URL", "")),
            token_address=token,
            wallet_address=address,
            decimals=int(getattr(cfg, "PREDICTFUN_COLLATERAL_DECIMALS", 18)),
        )

    return _fetch


@dataclass(frozen=True)
class WalletBalances:
    """Snapshot of one predict.fun wallet's on-chain balances."""

    address: str
    collateral_symbol: str
    collateral_balance: Optional[float]
    collateral_token: Optional[str]
    gas_balance_bnb: Optional[float]


def read_wallet_balances(
    *,
    rpc_url: str,
    wallet_address: str,
    collateral_token: Optional[str],
    collateral_symbol: str = "USDT",
    collateral_decimals: int = 18,
    timeout_s: float = 15.0,
) -> WalletBalances:
    """Read collateral + native BNB balance for ``wallet_address``.

    Each read fails soft into ``None`` (logged) so a partial read still returns
    a usable snapshot (e.g. gas readable but the collateral token address is not
    configured). ``collateral_balance`` is None when the token address is
    unresolved (fail-closed: never fabricate a balance against an unknown token).
    """
    collateral: Optional[float] = None
    if collateral_token and is_bnb_address(collateral_token):
        try:
            collateral = read_erc20_balance(
                rpc_url=rpc_url,
                token_address=collateral_token,
                wallet_address=wallet_address,
                decimals=collateral_decimals,
                timeout_s=timeout_s,
            )
        except Exception:  # noqa: BLE001 — degrade to None, never crash
            logger.warning("predict.fun collateral balance read failed", exc_info=True)
    gas: Optional[float] = None
    try:
        gas = read_native_balance(
            rpc_url=rpc_url, wallet_address=wallet_address, timeout_s=timeout_s
        )
    except Exception:  # noqa: BLE001
        logger.warning("predict.fun gas balance read failed", exc_info=True)
    return WalletBalances(
        address=wallet_address,
        collateral_symbol=collateral_symbol,
        collateral_balance=collateral,
        collateral_token=collateral_token,
        gas_balance_bnb=gas,
    )


# ---------------------------------------------------------------------------
# redeemPositions calldata (Gnosis CTF form, standard markets)
#
# ⚠️ NO RUNTIME CALLER — this is a MANUAL/OPERATOR surface, deliberately kept.
# Nothing in the scanner, the settlement backfill or any scheduler job calls
# `build_redeem_positions_calldata` / `redeem_won_position`; the only in-repo
# users are the tests and manual operator redemption.
# PF live is HALTED (2026-07-23), not dead, and NO superseding redeem path
# exists for PF winnings — this is the only exit. Do not delete as dead code.
# ---------------------------------------------------------------------------
def build_redeem_positions_calldata(
    *,
    collateral_token: str,
    condition_id: str,
    index_sets: list[int],
    parent_collection_id: str = ZERO_HASH,
) -> str:
    """ABI-encode ``redeemPositions(address,bytes32,bytes32,uint256[])`` calldata.

    Standard (non-neg-risk) markets only. ``index_sets`` are the CTF index sets
    to redeem (``[1]`` for the YES/UP outcome, ``[2]`` for NO/DOWN, ``[1, 2]``
    for both). Returns the 0x-prefixed calldata hex. Raises ValueError on a
    malformed address / condition id / empty index set — this is real-money
    calldata, so it fails closed on bad input rather than encoding garbage.

    ``eth_utils`` (selector keccak) + ``eth_abi`` (arg encoding) are runtime
    deps (both in pyproject); a missing one raises RuntimeError.
    """
    if not is_bnb_address(collateral_token):
        raise ValueError("collateral_token must be a 0x-prefixed 20-byte address")
    if not is_bytes32(condition_id):
        raise ValueError("condition_id must be a 0x-prefixed 32-byte hash")
    if not is_bytes32(parent_collection_id):
        raise ValueError("parent_collection_id must be a 0x-prefixed 32-byte hash")
    sets = [int(s) for s in index_sets]
    if not sets or any(s <= 0 for s in sets):
        raise ValueError("index_sets must be a non-empty list of positive ints")
    try:
        from eth_abi import encode as _abi_encode  # type: ignore
        from eth_utils import keccak as _keccak  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "eth_abi / eth_utils required to build redeem calldata"
        ) from exc

    selector = _keccak(
        text="redeemPositions(address,bytes32,bytes32,uint256[])"
    )[:4]
    encoded = _abi_encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [
            collateral_token,
            bytes.fromhex(parent_collection_id[2:]),
            bytes.fromhex(condition_id[2:]),
            sets,
        ],
    )
    return "0x" + (selector + encoded).hex()


@dataclass(frozen=True)
class RedeemResult:
    """Outcome of a redeem attempt. ``calldata`` is always populated on a
    successful build so the operator can inspect / submit it manually even when
    the live-post path is (deliberately) fail-closed."""

    success: bool
    status: str  # 'built' / 'disabled' / 'not_configured' / 'venue_unavailable'
    calldata: Optional[str] = None
    to_contract: Optional[str] = None
    tx_hash: Optional[str] = None
    error: Optional[str] = None


def redeem_won_position(
    *,
    condition_id: str,
    index_sets: list[int],
    chain_id: int,
    collateral_token_override: str = "",
    ctf_contract_override: str = "",
    auto_redeem_enabled: bool = False,
) -> RedeemResult:
    """Build redeem calldata and (fail-closed) attempt to submit it.

    The redeem SUBMIT (on-chain send) is FAIL-CLOSED and mirrors AUTO_REDEEM for
    Polymarket + the ``PredictFunOrderClient`` submit contract:

      * ``auto_redeem_enabled`` False (default) -> status 'built' (calldata
        returned for manual/forensic use), NO tx is ever sent;
      * addresses unresolved -> status 'not_configured' (no calldata), fail-closed;
      * ``auto_redeem_enabled`` True but the wire format is UNVALIDATED on
        mainnet -> status 'venue_unavailable' (calldata returned, NO tx sent).

    There is no path here that broadcasts a transaction — a real broadcast is
    only wired once a mainnet dry-run confirms the signing + submit contract,
    exactly like the order-submit client.
    """
    collateral = resolve_collateral_token(
        chain_id, override=collateral_token_override
    )
    ctf = resolve_ctf_contract(chain_id, override=ctf_contract_override)
    if collateral is None or ctf is None:
        missing = []
        if collateral is None:
            missing.append("collateral_token")
        if ctf is None:
            missing.append("ctf_contract")
        return RedeemResult(
            success=False,
            status="not_configured",
            error=(
                "predict.fun redeem addresses unresolved "
                f"({', '.join(missing)}); set PREDICTFUN_COLLATERAL_TOKEN / "
                "PREDICTFUN_CTF_CONTRACT or install predict_sdk"
            ),
        )
    try:
        calldata = build_redeem_positions_calldata(
            collateral_token=collateral,
            condition_id=condition_id,
            index_sets=index_sets,
        )
    except (ValueError, RuntimeError) as exc:
        return RedeemResult(success=False, status="not_configured", error=str(exc))

    if not auto_redeem_enabled:
        # Default: never broadcast. Calldata is returned for manual redeem.
        return RedeemResult(
            success=False,
            status="built",
            calldata=calldata,
            to_contract=ctf,
            error="PREDICTFUN_AUTO_REDEEM_ENABLED off — calldata built, not posted",
        )

    # Enabled, but the predict.fun on-chain submit wire format is NOT validated
    # on mainnet — refuse to broadcast (fail-closed), same rule as
    # PredictFunOrderClient.place_gtc_limit_order.
    logger.error(
        "predict.fun redeem submit requested but the send path is unvalidated on "
        "mainnet — refusing to broadcast (condition_id=%s). Calldata built for "
        "manual redeem.",
        condition_id,
    )
    return RedeemResult(
        success=False,
        status="venue_unavailable",
        calldata=calldata,
        to_contract=ctf,
        error="predictfun redeem submit not validated on mainnet",
    )
