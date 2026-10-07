"""Process-wide config singleton for live-trading credentials.

Holds Polymarket credentials and on-chain endpoints. ``get_config()`` caches
the first successful load, so a tick never re-reads ``.env`` (and never sees a
half-written file while the operator edits it).

  * ``dashboard_user`` / ``dashboard_pass`` back the dashboard's cookie
    auth. A short or empty password with any LIVE wallet blocks boot.
  * ``_validate_relayer_url`` checks RELAYER_URL against known prefixes. A
    mismatch logs CRITICAL but boot continues, since Polymarket may publish a
    new endpoint.

Secrets are ``SecretStr``, so ``repr(cfg)`` never shows them; call
``.get_secret_value()`` to read one.

Tests call ``set_config(SimpleConfig(...))`` to inject a stub before anything
calls ``get_config()``; ``set_config(None)`` forces a fresh load.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional
from urllib.parse import urlsplit

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)


# Known-good RELAYER_URL prefixes. A mismatch sends a CRITICAL log and Telegram
# alert but does not block boot: Polymarket may publish a regional or
# replacement endpoint, and failing closed would stop the bot over a rename.
_KNOWN_RELAYER_PREFIXES: tuple[str, ...] = (
    "https://relayer-v2.polymarket.com",
    "https://relayer.polymarket.com",
)
_KNOWN_RELAYER_HOSTS: frozenset[str] = frozenset(
    urlsplit(prefix).hostname or "" for prefix in _KNOWN_RELAYER_PREFIXES
)


class Config(BaseSettings):
    """Settings loaded from ``.env`` and the process environment (POLY_*,
    NOTIFY_*, LIVE_* names).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        # dashboard_user / dashboard_pass bind to CRYPTOBOT_DASHBOARD_* (with
        # DASHBOARD_* as a fallback), the same variables the auth code reads.
        # populate_by_name lets tests set them by field name too.
        populate_by_name=True,
    )

    # ---- Polymarket CLOB credentials (SecretStr prevents accidental log) -
    poly_private_key: SecretStr = SecretStr("")
    poly_api_key: SecretStr = SecretStr("")
    poly_secret: SecretStr = SecretStr("")
    poly_passphrase: SecretStr = SecretStr("")

    # ---- Polymarket V2 wallet topology -----------------------------------
    # 0=EOA, 1=POLY_PROXY (legacy), 2=POLY_GNOSIS_SAFE, 3=POLY_1271
    # (standard "deposit wallet" for most operators including MetaMask
    # users — funds sit in a Polymarket-managed 1271 smart contract).
    poly_signature_type: int = Field(default=0)
    poly_funder: str = Field(default="")
    poly_return_wallet: str = Field(default="")

    # ---- Polymarket relayer (transfer + redemption submissions) ----------
    relayer_url: str = Field(default="https://relayer-v2.polymarket.com")
    relayer_api_key: SecretStr = SecretStr("")
    relayer_api_key_address: str = Field(default="")

    # ---- Polygon RPC (on-chain wallet/CTF reconciliation) ----------------
    polygon_rpc_url: str = Field(default="https://polygon-rpc.com")

    # ---- Live readiness / wallet reconciliation TTLs --------------------
    live_require_deposit_wallet: bool = Field(default=True)
    live_onchain_verify_enabled: bool = Field(default=True)
    live_balance_tolerance_usd: float = Field(default=0.25)
    wallet_snapshot_freshness_ttl_s: int = Field(default=900)
    wallet_snapshot_interval_minutes: int = Field(default=10)

    # ---- Mode + bankroll -------------------------------------------------
    # DRY_RUN sets the DEFAULT mode for fresh wallets on first boot only.
    # Once wallet_mode_state has rows, per-wallet mode is authoritative
    # (toggled via dashboard or `cryptobot.cli.set_wallet_mode`).
    dry_run: bool = Field(default=True)
    initial_bankroll: float = Field(default=100.0)

    # ---- Dashboard cookie auth -------------------------------------------
    # Used by the dashboard's POST endpoints (wallet mode, operator controls,
    # transfers). An empty password with any LIVE wallet stops boot; the boot
    # check enforces that, keeping Config construction side-effect free. These
    # bind to the same CRYPTOBOT_DASHBOARD_* variables that main.py and the
    # login handler read, so setting the documented variable always takes
    # effect; the unprefixed names remain as fallbacks.
    dashboard_user: str = Field(
        default="admin",
        validation_alias=AliasChoices("CRYPTOBOT_DASHBOARD_USER", "DASHBOARD_USER"),
    )
    dashboard_pass: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("CRYPTOBOT_DASHBOARD_PASS", "DASHBOARD_PASS"),
    )
    dashboard_bind_host: str = Field(default="127.0.0.1")
    dashboard_tls_terminated: bool = Field(default=False)
    allow_remote_dashboard: bool = Field(default=False)

    # ---- Telegram alerts (optional — silently skipped if not set) -------
    notify_telegram_token: SecretStr = SecretStr("")
    notify_telegram_chat_id: str = Field(default="")

    def relayer_url_is_known(self) -> bool:
        """True if ``relayer_url`` starts with a known-good prefix.

        The boot path logs CRITICAL when this is False but still boots, since a
        legitimate new endpoint may appear before this list is updated.
        """
        try:
            parsed = urlsplit((self.relayer_url or "").strip())
        except ValueError:
            return False
        if parsed.scheme.lower() != "https":
            return False
        if parsed.username or parsed.password:
            return False
        try:
            port = parsed.port
        except ValueError:
            return False
        if port not in (None, 443):
            return False
        return (parsed.hostname or "").lower() in _KNOWN_RELAYER_HOSTS


# --- Process-wide Config singleton -------------------------------------

_cfg_singleton: Config | None = None
_cfg_lock = threading.Lock()


def get_config() -> Config:
    """Return the process-wide Config singleton.

    Thread-safe via double-checked locking. Tests that need an override
    must call `set_config(custom)` before any caller hits `get_config`.
    Production code should NOT call `set_config(...)` mid-process — boot
    is the only authorized caller besides tests.
    """
    global _cfg_singleton
    if _cfg_singleton is not None:
        return _cfg_singleton
    with _cfg_lock:
        if _cfg_singleton is None:
            _cfg_singleton = Config()
        return _cfg_singleton


def set_config(cfg: Optional[Config]) -> None:
    """Override / reset the singleton.

    `None` forces a fresh re-read on the next `get_config()` call. Used by
    tests to inject stubs, and by the boot path's `--env-file` override.
    """
    global _cfg_singleton
    with _cfg_lock:
        _cfg_singleton = cfg


