"""Polygon pUSD balance fetcher for the live-readiness on-chain check.

Reached through ``VenueAdapter.build_balance_fetcher``. The generic JSON-RPC
reader (``read_erc20_balance``) and the address validator live in core, since
they are not Polymarket-specific.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from cryptobot.execution.live_readiness import is_address, read_erc20_balance
from cryptobot_polymarket.primitives import PUSD_ADDRESS, PUSD_DECIMALS

if TYPE_CHECKING:  # pragma: no cover - typing only
    from cryptobot.runtime_secrets import Config


def build_pusd_balance_fetcher(config: "Config") -> Callable[[], Optional[float]]:
    """Return a callable that reads configured POLY_FUNDER pUSD on Polygon."""

    def _fetch() -> Optional[float]:
        if not getattr(config, "polygon_rpc_url", ""):
            raise RuntimeError("POLYGON_RPC_URL is required")
        wallet_address = (getattr(config, "poly_funder", "") or "").strip()
        if not is_address(wallet_address):
            raise ValueError("POLY_FUNDER must be a 0x-prefixed 40-byte address")
        return read_erc20_balance(
            rpc_url=getattr(config, "polygon_rpc_url", ""),
            token_address=PUSD_ADDRESS,
            wallet_address=wallet_address,
            decimals=PUSD_DECIMALS,
        )

    return _fetch


__all__ = ["build_pusd_balance_fetcher"]
