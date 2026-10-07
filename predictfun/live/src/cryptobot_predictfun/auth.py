"""predict.fun ``/auth`` JWT session — the Bearer credential for trading endpoints.

Wire contract (dev.predict.fun "How to authenticate your API requests" +
OpenAPI at ``https://api.predict.fun/spec``, fetched 2026-07-23)::

    GET  {base}/auth/message   (x-api-key)       -> data.message: str
    POST {base}/auth           (x-api-key, JSON) -> data.token: str  (the JWT)
         body: {"signer": <address>, "message": <msg>, "signature": <sig>}

Two signing flows, selected by the ORDER CLIENT (this module is transport-only
and takes an injected ``sign_message`` callable):

* **EOA** (external wallet): ``personal_sign`` of the message text by the
  ``PREDICTFUN_PRIVATE_KEY`` EOA; ``signer`` = that EOA address.
* **Predict account** (Kernel smart wallet — ``PREDICTFUN_PREDICT_ACCOUNT``
  set): the SDK's ``OrderBuilder.sign_predict_account_message`` produces the
  Kernel/ECDSA-validator signature; ``signer`` = the PREDICT ACCOUNT address
  (NOT the EOA), per the venue docs' predict-account example.

JWT lifetime is undocumented. Expiry is read from the token's own ``exp``
claim (an UNVERIFIED payload decode — we are the party that just received the
token over TLS, not a validator of third-party input) minus a 60 s refresh
skew; a token without ``exp`` gets a conservative 10-minute TTL.
``invalidate()`` supports the order path's 401 -> re-auth-once retry.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import threading
import time
from typing import Callable, Optional

import httpx

logger = logging.getLogger(__name__)

# Refresh this many seconds BEFORE the token's exp claim.
_EXP_SKEW_S = 60.0
# TTL when the token carries no readable exp claim.
_FALLBACK_TTL_S = 600.0


class PredictFunAuthError(RuntimeError):
    """The /auth flow failed (network, HTTP status, or malformed body)."""


def _jwt_expiry_ts(token: str) -> Optional[float]:
    """Best-effort ``exp`` claim (unix seconds) from a JWT, else None.

    Tolerates missing padding (base64url). Never raises — an undecodable
    token simply gets the fallback TTL.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = payload.get("exp")
        return float(exp) if exp is not None else None
    except (ValueError, TypeError, binascii.Error):
        return None


class JwtSession:
    """Cached predict.fun JWT with lazy fetch + expiry refresh.

    Thread-safe: the order path can run from the tick thread and the
    fast-requote drain concurrently; refreshes are serialized under a lock.
    """

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        signer_address: str,
        sign_message: Callable[[str], str],
        client: httpx.Client,
        timeout: float = 5.0,
    ) -> None:
        self._api_base = api_base.rstrip("/")
        self._api_key = api_key
        self._signer_address = signer_address
        self._sign_message = sign_message
        self._client = client
        self._timeout = timeout
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        # Rotation hook (set_on_change): fired AFTER every token change —
        # refresh and invalidate — outside the lock, so the WS wallet-events
        # subscriber can resubscribe its predictWalletEvents/{jwt} topic with
        # the fresh credential instead of streaming into a dead subscription.
        self._on_change: Optional[Callable[[], None]] = None

    # -- public -------------------------------------------------------------
    def bearer_header(self) -> dict[str, str]:
        """``{"Authorization": "Bearer <jwt>"}`` — fetching/refreshing as needed.

        Raises :class:`PredictFunAuthError` when a token cannot be obtained;
        the order client maps that to an ``auth`` OrderResult (never posts
        unauthenticated).
        """
        rotated = False
        with self._lock:
            now = time.time()
            if self._token is None or now >= self._expires_at:
                self._refresh_locked()
                rotated = True
            header = {"Authorization": f"Bearer {self._token}"}
        if rotated:
            self._notify_change()
        return header

    def invalidate(self) -> None:
        """Drop the cached token (server said 401 — force a fresh /auth)."""
        with self._lock:
            self._token = None
            self._expires_at = 0.0
        self._notify_change()

    def peek_token(self) -> Optional[str]:
        """The cached, unexpired JWT — or None. NEVER fetches.

        The WS wallet-events subscriber builds its ``predictWalletEvents/{jwt}``
        topic from this so it rides the credential the order path already
        holds; a fetch here would spend /auth round trips solely for WS (the
        rotation hook re-arms the subscription once the order path refreshes).
        """
        with self._lock:
            if self._token is not None and time.time() < self._expires_at:
                return self._token
            return None

    def set_on_change(self, callback: Optional[Callable[[], None]]) -> None:
        """Register the rotation hook fired after every refresh/invalidate.

        The callback runs on the auth caller's thread with NO JwtSession lock
        held, must be cheap, and must not call back into this session (it is
        meant to bump a version/flag — e.g. the WS mirror's
        ``notify_jwt_rotated`` — never to do I/O)."""
        self._on_change = callback

    def _notify_change(self) -> None:
        cb = self._on_change
        if cb is None:
            return
        try:
            cb()
        except Exception:  # noqa: BLE001 — the auth path must never fail on a hook
            logger.debug("JwtSession on_change hook raised", exc_info=True)

    # -- internals ----------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self._api_key} if self._api_key else {}

    def _refresh_locked(self) -> None:
        message = self._fetch_auth_message()
        try:
            signature = self._sign_message(message)
        except Exception as exc:  # noqa: BLE001 — surface as one error type
            raise PredictFunAuthError(f"auth message signing failed: {exc!r}") from exc
        token = self._post_signature(message, signature)
        expiry = _jwt_expiry_ts(token)
        now = time.time()
        if expiry is not None and expiry > now:
            self._expires_at = expiry - _EXP_SKEW_S
        else:
            self._expires_at = now + _FALLBACK_TTL_S
        self._token = token
        logger.info(
            "predictfun auth: JWT obtained for %s (refresh in %.0fs)",
            self._signer_address[:10],
            self._expires_at - now,
        )

    def _fetch_auth_message(self) -> str:
        try:
            resp = self._client.get(
                f"{self._api_base}/auth/message",
                headers=self._headers() or None,
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            raise PredictFunAuthError(f"GET /auth/message failed: {exc!r}") from exc
        if resp.status_code != 200:
            raise PredictFunAuthError(
                f"GET /auth/message -> HTTP {resp.status_code}"
            )
        try:
            body = resp.json()
        except ValueError as exc:
            raise PredictFunAuthError("GET /auth/message: non-JSON body") from exc
        data = body.get("data") if isinstance(body, dict) else None
        # Tolerate both {"data": {"message": "..."}} and {"data": "..."}.
        message = data.get("message") if isinstance(data, dict) else data
        if not isinstance(message, str) or not message:
            raise PredictFunAuthError("GET /auth/message: no message in body")
        return message

    def _post_signature(self, message: str, signature: str) -> str:
        payload = {
            "signer": self._signer_address,
            "message": message,
            "signature": signature,
        }
        try:
            resp = self._client.post(
                f"{self._api_base}/auth",
                json=payload,
                headers=self._headers() or None,
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            raise PredictFunAuthError(f"POST /auth failed: {exc!r}") from exc
        if resp.status_code not in (200, 201):
            # Body text is short and diagnostic (e.g. bad signer/signature);
            # never contains our key material.
            raise PredictFunAuthError(
                f"POST /auth -> HTTP {resp.status_code}: {resp.text[:200]}"
            )
        try:
            body = resp.json()
        except ValueError as exc:
            raise PredictFunAuthError("POST /auth: non-JSON body") from exc
        data = body.get("data") if isinstance(body, dict) else None
        token = data.get("token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token:
            raise PredictFunAuthError("POST /auth: no token in body")
        return token
