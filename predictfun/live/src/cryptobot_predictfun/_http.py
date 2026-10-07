"""Shared predict.fun REST plumbing — API base, auth headers, liveness probe.

Single source for the probe-confirmed auth contract (2026-07-21 MAINNET probe):
the API key goes in an ``x-api-key: <key>`` header. ``Authorization: Bearer``
is a DIFFERENT credential (the JWT from the ``/auth`` wallet-signature flow,
needed only for trading/account endpoints) and 401s when given the API key, so
a Bearer probe would falsely report a valid key as bad. The public testnet base
needs no key, so an empty header set is valid — never inject a placeholder key.

Consumed by :mod:`cryptobot_predictfun.markets`, :mod:`cryptobot_predictfun.clob`
and the two predict.fun CLIs (``derive_predictfun_creds`` /
``predictfun_wallet_diagnostics``) so the header convention can never drift
between them.
"""

from __future__ import annotations

from typing import Optional

import httpx


def api_base() -> str:
    """``config.PREDICTFUN_API_BASE`` without a trailing slash."""
    from cryptobot import config

    return str(config.PREDICTFUN_API_BASE).rstrip("/")


def auth_headers() -> dict[str, str]:
    """``x-api-key`` header when an API key is configured; empty otherwise."""
    from cryptobot import config

    key = str(getattr(config, "PREDICTFUN_API_KEY", "") or "").strip()
    return {"x-api-key": key} if key else {}


def probe_markets(
    base: str,
    api_key: str,
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
) -> tuple[Optional[int], bool, str]:
    """GET ``{base}/markets`` and classify the auth outcome.

    Returns ``(status_code, ok, detail)``. ``ok`` is True on 200 (key accepted,
    or a public testnet base). 401 => not authorized (mainnet needs a key). A
    network error returns ``(None, False, ...)``.
    """
    headers = {"x-api-key": api_key} if api_key else {}
    owned = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    try:
        resp = client.get(f"{base.rstrip('/')}/markets", headers=headers or None)
    except httpx.HTTPError as exc:
        return None, False, f"unreachable: {exc!r}"
    finally:
        if owned:
            client.close()
    code = resp.status_code
    if code == 200:
        return code, True, "200 OK (markets endpoint reachable + authorized)"
    if code == 401:
        return code, False, "401 unauthorized (set PREDICTFUN_API_KEY; mainnet requires it)"
    return code, False, f"HTTP {code}"
