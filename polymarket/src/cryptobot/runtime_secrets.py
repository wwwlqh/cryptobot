"""Process-wide config singleton for live-execution credentials.

Cryptobot's per-strategy LIVE deploy plan §U2 (2026-05-24-001) introduces real
Polymarket credentials and on-chain endpoints. Loading them every tick (the
default `BaseSettings()` shape) wastes I/O and exposes the bot to torn-write
bugs during operator `.env` edits, so `get_config()` caches the first
successful construction in a process-wide singleton.

Mirrors `hightempbot/runtime_config.py` to keep the operator's mental model
consistent across the two bots, with cryptobot-specific tweaks:

  * `dashboard_user`/`dashboard_pass` are sized for cryptobot's hand-rolled
    cookie auth (D-LIVE-13). A short or empty pass with any LIVE wallet in
    `wallet_mode_state` blocks boot.
  * `_validate_relayer_url` enforces the D-LIVE-27 prefix check at first
    construction — mismatched RELAYER_URL emits a CRITICAL log but still
    allows boot, since Polymarket may publish a replacement endpoint.

Secrets are wrapped in `SecretStr` so `repr(cfg)` never leaks. Code that
needs the raw value must call `.get_secret_value()` explicitly.

Test mode: tests call `set_config(SimpleConfig(...))` to inject a stub before
any caller hits `get_config()`. `set_config(None)` forces a fresh read.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional
from urllib.parse import urlsplit

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)


# D-LIVE-27: known-good RELAYER_URL prefixes. Mismatch → CRITICAL log + Telegram
# alert; still allowed to boot. Polymarket may publish a regional or
# replacement endpoint we don't know about, and a fail-closed gate would brick
# the operator's deploy on a Polymarket-side rename.
_KNOWN_RELAYER_PREFIXES: tuple[str, ...] = (
    "https://relayer-v2.polymarket.com",
    "https://relayer.polymarket.com",
)
_KNOWN_RELAYER_HOSTS: frozenset[str] = frozenset(
    urlsplit(prefix).hostname or "" for prefix in _KNOWN_RELAYER_PREFIXES
)


class Config(BaseSettings):
    """Pydantic settings model loaded from `.env` + process env vars.

    Field naming follows the hightempbot port verbatim so the operator's
    env vars carry over unchanged (POLY_*, NOTIFY_*, LIVE_*).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        # Audit w19oxqf24 (P2): dashboard_user/dashboard_pass carry a
        # validation_alias (CRYPTOBOT_DASHBOARD_* with DASHBOARD_* fallback) so
        # they bind to the same env var the auth path reads. populate_by_name
        # keeps them settable by their Python field name too (kwargs / tests).
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

    # ---- Dashboard cookie auth (D-LIVE-13 / D-LIVE-26) -------------------
    # Used by the SSE server's do_POST router for mode-flip / operator-
    # control / transfer endpoints. Empty pass + any LIVE wallet → boot
    # refuses (asserted by the boot gate, not here, to keep the Config
    # construction side-effect free for tests).
    # Audit w19oxqf24 (P2): the AUTH path (main.py boot gate + sse_server login
    # handler) reads CRYPTOBOT_DASHBOARD_PASS directly via os.environ. These
    # pydantic fields previously bound to the un-prefixed DASHBOARD_PASS /
    # DASHBOARD_USER (no env_prefix, case-insensitive), so an operator who set
    # the password as the root .env.example documented (DASHBOARD_PASS=) set a
    # value with ZERO effect on auth. Bind to the SAME prefixed env vars the
    # auth path uses so the documented config and the live check agree. The
    # un-prefixed names are kept as secondary aliases for backward compat.
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
        """D-LIVE-27: True iff `relayer_url` points at a known-good host.

        Callers (boot path) log a CRITICAL warning when this returns False;
        boot is NOT refused because a replacement endpoint may legitimately
        appear before this list is updated.
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


# --- Process-wide Config singleton (mirrors hightempbot/runtime_config.py) -

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


