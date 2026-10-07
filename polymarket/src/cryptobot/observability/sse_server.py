"""Dashboard HTTP server: static files, the SSE stream and operator POST
endpoints.

An ``http.server.HTTPServer`` with a ``SimpleHTTPRequestHandler`` subclass
serves ``dashboard/``. ``/events`` holds a long-lived connection and streams
JSON payloads as server-sent events; the ``/api/v2/`` POST routes change
operator state, wallet mode and transfers.

Every ``/api/v2/*`` POST needs the session cookie, whatever interface the
server is bound to (loopback is shared by every process on the host, so it is
not an access boundary). With ``CRYPTOBOT_DASHBOARD_PASS`` empty the router
rejects every POST with ``dashboard_pass_unset``, and main.py refuses to boot
unless ``CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1`` is set for local development
(itself refused on a non-loopback bind).

POST bodies are capped at ``MAX_POST_BODY_BYTES`` (8 KiB), and
``/api/v2/auth/login`` is rate-limited per client IP with an in-memory sliding
window.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import posixpath
import queue
import secrets
import sqlite3
import threading
import time
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Any, Callable, Optional
from urllib.parse import unquote, urlsplit

from cryptobot import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cookie auth helpers
# ---------------------------------------------------------------------------


_BOOT_SALT = secrets.token_hex(16)

# API bodies are small JSON objects; anything bigger is misuse.
MAX_POST_BODY_BYTES = 8 * 1024

# Login throttling: sliding window per client IP.
LOGIN_RATE_LIMIT_ATTEMPTS = 5
LOGIN_RATE_LIMIT_WINDOW_S = 60.0

_ALLOWED_DASHBOARD_STATIC = frozenset({
    "/index.html",
    "/styles.css",
    "/time.js",
    "/api.js",
    "/live-bridge.js",
    "/data.jsx",
    "/charts.jsx",
    "/shell.jsx",
    "/pages-overview.jsx",
    "/pages-live.jsx",
    "/pages-analytics.jsx",
    "/pages-config.jsx",
    "/pages-latency.jsx",
    "/pages-subminute.jsx",
    "/app.jsx",
    "/snapshot.json",
    # Only these files are served. Keep the list minimal: every JSON file added
    # here is readable without a session unless it is also in the auth-gated
    # list in ``do_GET``.
})
_ALLOWED_ASSET_EXTENSIONS = frozenset({
    ".svg", ".png", ".jpg", ".jpeg", ".webp", ".ico",
})


def derive_session_token(dashboard_pass: str) -> str:
    """Session token derived from ``CRYPTOBOT_DASHBOARD_PASS`` and a
    per-process ``_BOOT_SALT``, so a restart invalidates existing sessions.
    """
    return hashlib.sha256(
        f"{dashboard_pass}:{_BOOT_SALT}".encode("utf-8")
    ).hexdigest()


def derive_csrf_token(session_token: str) -> str:
    """Derive the browser-readable CSRF token for a session cookie.

    Deterministic, so no extra session store is needed, but bound to the
    per-process session token, so a CSRF cookie cannot outlive a restart.
    """
    return hmac.new(
        session_token.encode("utf-8"),
        b"cryptobot-dashboard-csrf",
        hashlib.sha256,
    ).hexdigest()


def _parse_expected_version(body: dict[str, Any]) -> Optional[int]:
    """Parse the optimistic-concurrency version without leaking a traceback."""
    raw = body.get("expected_version")
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError("expected_version must be an integer")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        return int(raw.strip())
    raise ValueError("expected_version must be an integer")


def _extract_session_token(cookie_header: Optional[str]) -> Optional[str]:
    if not cookie_header:
        return None
    parsed = SimpleCookie()
    try:
        parsed.load(cookie_header)
    except Exception:  # noqa: BLE001
        return None
    morsel = parsed.get("session_token")
    return morsel.value if morsel is not None else None


def _cookie_secure_attr() -> str:
    if os.environ.get("CRYPTOBOT_DASHBOARD_TLS_TERMINATED", "0") == "1":
        return "Secure; "
    return ""


def _dashboard_static_path(raw_path: str) -> Optional[str]:
    path = unquote((raw_path or "/").split("?", 1)[0])
    if path in ("", "/"):
        return "/index.html"
    if "\\" in path:
        return None
    normalized = posixpath.normpath(path)
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if normalized in ("/..",) or normalized.startswith("/../"):
        return None
    return normalized


def _is_allowed_dashboard_static(raw_path: str) -> bool:
    path = _dashboard_static_path(raw_path)
    if path is None:
        return False
    if path in _ALLOWED_DASHBOARD_STATIC:
        return True
    if path.startswith("/assets/"):
        ext = posixpath.splitext(path)[1].lower()
        return ext in _ALLOWED_ASSET_EXTENSIONS
    return False


class Broadcaster:
    """Fans payloads out to SSE clients: any thread can call
    ``publish(payload)``, and each connected client has its own bounded
    queue.

    Payloads are ``(event_id, json)`` tuples so the handler can send an ``id:``
    field. The id is informational only: ``Last-Event-ID`` is not read on
    reconnect, and every reconnect gets the full ledger bootstrap from
    ``register()``. That is safe because the dashboard's ``mergeLedger`` (in
    ``live-bridge.js``) dedupes rows by id.

    Late joiners: the most recent full-ledger payload (built with
    ``last_seen_ledger_id=0``) is cached, and ``register()`` queues it first so
    a new client sees existing rows immediately.
    """

    def __init__(self, max_clients: int = config.MAX_SSE_CLIENTS) -> None:
        self.max_clients = max_clients
        self.clients: list[queue.Queue] = []
        self._lock = threading.Lock()
        self.last_broadcast_ts: float = 0.0
        self._event_id: int = 0
        # Ids of client queues that dropped a delta on queue.Full. The next
        # set_bootstrap re-sends the full ledger to just those clients, so a
        # slow client recovers without every client paying for a full snapshot
        # each tick. Guarded by self._lock.
        self._needs_bootstrap: set[int] = set()
        # Cached (event_id, json) bootstrap for late joiners, refreshed every
        # tick by main.py via ``set_bootstrap``.
        self._bootstrap: Optional[tuple[int, str]] = None

    def set_bootstrap(self, payload: dict) -> None:
        """Replace the cached bootstrap. main.py calls this every tick with a
        payload built with ``last_seen_ledger_id=0``, so it always holds the
        current ledger.
        """
        data = json.dumps(payload, separators=(",", ":"))
        with self._lock:
            self._event_id += 1
            self._bootstrap = (self._event_id, data)
            # Re-send the full ledger to clients that dropped a delta.
            # mergeLedger dedupes by row id, so this is harmless for a client
            # that is already current.
            if self._needs_bootstrap:
                for q in list(self.clients):
                    if id(q) in self._needs_bootstrap:
                        try:
                            q.put_nowait(self._bootstrap)
                            self._needs_bootstrap.discard(id(q))
                        except queue.Full:
                            pass

    def register(self) -> Optional[queue.Queue]:
        with self._lock:
            if len(self.clients) >= self.max_clients:
                return None
            q: queue.Queue = queue.Queue(maxsize=64)
            self.clients.append(q)
            if self._bootstrap is not None:
                # If the queue is somehow full already, the client still gets
                # the next tick.
                try:
                    q.put_nowait(self._bootstrap)
                except queue.Full:
                    pass
            return q

    def unregister(self, q: queue.Queue) -> None:
        with self._lock:
            try:
                self.clients.remove(q)
            except ValueError:
                pass
            self._needs_bootstrap.discard(id(q))

    def publish(self, payload: dict) -> None:
        data = json.dumps(payload, separators=(",", ":"))
        with self._lock:
            # All Broadcaster state is written under self._lock.
            self.last_broadcast_ts = time.time()
            self._event_id += 1
            event = (self._event_id, data)
            for q in list(self.clients):
                try:
                    q.put_nowait(event)
                except queue.Full:
                    # Slow client: drop rather than block the broadcast thread,
                    # and flag it so the next set_bootstrap re-sends the full
                    # ledger.
                    self._needs_bootstrap.add(id(q))


class _ThreadingServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class PostRouter:
    """Holds what the POST endpoints need.

    Separate from the HTTP handler so tests can drive the router with a fake
    payload and no server. Each handler returns ``(status_code, body_dict)``,
    which the server sends as JSON.
    """

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        portfolio: Any,
        session_token: str,
        relayer_submitter: Optional[Callable[[dict], dict]] = None,
        actor: str = "dashboard",
        auth_disabled: bool = False,
        transfer_to_wallet_allowlist: str = "",
    ) -> None:
        self._conn = conn
        self._portfolio = portfolio
        self._session_token = session_token
        self._relayer_submitter = relayer_submitter
        # Transfers may only go to ``POLY_RETURN_WALLET``, which main.py passes
        # in. Any other ``to_wallet`` is rejected here, before anything is
        # written, and again inside submit_transfer.
        self._transfer_to_wallet_allowlist = transfer_to_wallet_allowlist or ""
        # The actor recorded on every HTTP-originated change. The body's
        # ``actor`` field is ignored so a caller cannot write a false identity
        # into the audit trail; the CLI uses the operator_control functions
        # with its own actor.
        self._actor = actor
        # Explicit opt-out for local development without a password. main.py
        # sets it from CRYPTOBOT_DASHBOARD_AUTH_DISABLED and refuses it on a
        # non-loopback bind. Defaults to False.
        self._auth_disabled = bool(auth_disabled)
        self._lock = threading.Lock()

    # ---- auth -------------------------------------------------------------

    def check_auth(self, cookie_header: Optional[str]) -> bool:
        """Return True when the request carries a valid session token.

        * ``auth_disabled=True`` (``CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1``, loopback
          only) allows everything; for local development.
        * An empty ``_session_token`` without that opt-out rejects every request
          (boot also refuses this state).
        * Otherwise the cookie must match, compared with ``hmac.compare_digest``.
        """
        if self._auth_disabled:
            return True
        token = _extract_session_token(cookie_header)
        if not self._session_token or token is None:
            return False
        return hmac.compare_digest(token, self._session_token)

    def check_csrf(
        self, cookie_header: Optional[str], csrf_header: Optional[str]
    ) -> bool:
        """Validate the double-submit CSRF token for an authenticated session.
        """
        if self._auth_disabled:
            return True
        if not self.check_auth(cookie_header):
            return False
        session_token = _extract_session_token(cookie_header)
        expected = derive_csrf_token(session_token or "")
        supplied = (csrf_header or "").strip()
        return bool(supplied) and hmac.compare_digest(supplied, expected)

    # ---- route dispatch ---------------------------------------------------

    def dispatch(
        self, path: str, body: dict[str, Any], *, cookie_header: Optional[str]
    ) -> tuple[int, dict[str, Any]]:
        if not self.check_auth(cookie_header):
            return HTTPStatus.UNAUTHORIZED, {"reason": "unauthorized"}

        # Mode toggle: /api/v2/wallet/<id>/mode
        if path.startswith("/api/v2/wallet/") and path.endswith("/mode"):
            wallet_id = path[len("/api/v2/wallet/"): -len("/mode")]
            return self._handle_wallet_mode(wallet_id, body)

        # Operator controls
        if path == "/api/v2/operator/start":
            return self._handle_operator_start(body)
        if path == "/api/v2/operator/stop":
            return self._handle_operator_stop(body)
        if path == "/api/v2/operator/transfer-lock":
            return self._handle_operator_transfer_lock(body)
        if path == "/api/v2/operator/transfer/preview":
            return self._handle_transfer_preview(body)
        if path == "/api/v2/operator/transfer/submit":
            return self._handle_transfer_submit(body)

        return HTTPStatus.NOT_FOUND, {"reason": "not_found"}

    # ---- handlers ---------------------------------------------------------

    def _handle_wallet_mode(
        self, wallet_id: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from cryptobot.execution.operator_control import (
            BlockedError,
            ConcurrentMutationError,
            OperatorControlError,
            get_wallet_mode,
            set_wallet_mode,
        )
        target = (body.get("target") or "").strip().upper()
        reason = body.get("reason") or ""
        try:
            expected_version = _parse_expected_version(body)
        except (TypeError, ValueError):
            return HTTPStatus.BAD_REQUEST, {
                "reason": "invalid_expected_version",
            }
        # The actor is always the HTTP-origin actor, never taken from the body.
        actor = self._actor
        if target not in ("DRY", "LIVE"):
            return HTTPStatus.BAD_REQUEST, {
                "reason": "invalid_target",
                "detail": "target must be DRY or LIVE",
            }
        # ``force`` is CLI-only; any body field is ignored.
        with self._lock:
            try:
                state = set_wallet_mode(
                    self._conn,
                    wallet_id,
                    target,
                    actor=actor,
                    reason=reason,
                    expected_version=expected_version,
                )
            except ConcurrentMutationError:
                current = get_wallet_mode(self._conn, wallet_id)
                return HTTPStatus.CONFLICT, {
                    "reason": "version_conflict",
                    "current": {
                        "mode": current.mode,
                        "version": current.version,
                        "updated_at": current.updated_at,
                    },
                }
            except BlockedError as exc:
                return HTTPStatus.FORBIDDEN, {
                    "reason": exc.reason,
                    "detail": str(exc),
                }
            except OperatorControlError as exc:
                return HTTPStatus.BAD_REQUEST, {
                    "reason": "operator_control_error",
                    "detail": str(exc),
                }
        return HTTPStatus.OK, {
            "mode": state.mode,
            "version": state.version,
            "updated_at": state.updated_at,
            "wallet_id": wallet_id,
        }

    def _handle_operator_start(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        return self._operator_state_transition(
            body, action="start", default_reason="dashboard start"
        )

    def _handle_operator_stop(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        return self._operator_state_transition(
            body, action="stop", default_reason="dashboard stop"
        )

    def _handle_operator_transfer_lock(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        return self._operator_state_transition(
            body, action="transfer_lock", default_reason="dashboard transfer lock"
        )

    def _operator_state_transition(
        self, body: dict[str, Any], *, action: str, default_reason: str
    ) -> tuple[int, dict[str, Any]]:
        """Shared transition path for start / stop / transfer_lock.

        Passes ``expected_version`` to ``set_operator_state`` so two concurrent
        POSTs cannot flip state in opposite directions. The actor is always
        ``self._actor``.
        """
        from cryptobot.execution.operator_control import (
            ConcurrentMutationError,
            OperatorControlError,
            enter_transfer_lock,
            get_operator_state,
            start_processing,
            stop_processing,
        )
        dispatch = {
            "start": start_processing,
            "stop": stop_processing,
            "transfer_lock": enter_transfer_lock,
        }[action]
        reason = body.get("reason") or default_reason
        try:
            expected_version = _parse_expected_version(body)
        except (TypeError, ValueError):
            return HTTPStatus.BAD_REQUEST, {
                "reason": "invalid_expected_version",
            }
        with self._lock:
            try:
                state = dispatch(
                    self._conn,
                    actor=self._actor,
                    reason=reason,
                    expected_version=expected_version,
                )
            except ConcurrentMutationError:
                current = get_operator_state(self._conn)
                return HTTPStatus.CONFLICT, {
                    "reason": "version_conflict",
                    "current": current.to_public_dict(),
                }
            except OperatorControlError as exc:
                return HTTPStatus.BAD_REQUEST, {
                    "reason": "operator_control_error",
                    "detail": str(exc),
                }
        return HTTPStatus.OK, state.to_public_dict()

    def _handle_transfer_preview(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from cryptobot_polymarket.transfer import (
            TransferError,
            preview_transfer,
        )
        try:
            amount_usd = float(body.get("amount_usd"))
        except (TypeError, ValueError):
            return HTTPStatus.BAD_REQUEST, {"reason": "invalid_amount"}
        from_wallet = body.get("from_wallet") or ""
        to_wallet = body.get("to_wallet") or ""
        source_wallet_id = body.get("source_wallet_id")
        # The actor is always the HTTP-origin actor, never taken from the body.
        actor = self._actor
        if not from_wallet or not to_wallet:
            return HTTPStatus.BAD_REQUEST, {"reason": "invalid_addresses"}
        with self._lock:
            try:
                preview = preview_transfer(
                    self._conn,
                    from_wallet=from_wallet,
                    to_wallet=to_wallet,
                    amount_usd=amount_usd,
                    source_wallet_id=source_wallet_id,
                    actor=actor,
                    to_wallet_allowlist=self._transfer_to_wallet_allowlist,
                )
            except TransferError as exc:
                return HTTPStatus.BAD_REQUEST, {
                    "reason": exc.reason,
                    "detail": str(exc),
                }
        return HTTPStatus.OK, preview.to_public_dict()

    def _handle_transfer_submit(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from cryptobot_polymarket.transfer import (
            TransferError,
            submit_transfer,
        )
        idempotency_key = body.get("idempotency_key") or ""
        # The actor is always the HTTP-origin actor, never taken from the body.
        actor = self._actor
        if not idempotency_key:
            return HTTPStatus.BAD_REQUEST, {"reason": "missing_idempotency_key"}
        if self._relayer_submitter is None:
            return HTTPStatus.SERVICE_UNAVAILABLE, {
                "reason": "relayer_not_configured",
            }
        with self._lock:
            try:
                result = submit_transfer(
                    self._conn,
                    idempotency_key=idempotency_key,
                    relayer_submitter=self._relayer_submitter,
                    actor=actor,
                    to_wallet_allowlist=self._transfer_to_wallet_allowlist,
                )
            except TransferError as exc:
                code = HTTPStatus.GONE if exc.reason == "preview_expired" else HTTPStatus.BAD_REQUEST
                return code, {"reason": exc.reason, "detail": str(exc)}
        return HTTPStatus.OK, {
            "request_id": result.request_id,
            "status": result.status,
            "tx_hash": result.tx_hash,
            "relayer_tx_id": result.relayer_tx_id,
        }


class _LoginRateLimiter:
    """Sliding-window rate limiter for ``/api/v2/auth/login``.

    Tracks attempt times per client IP; once the count in the trailing
    ``LOGIN_RATE_LIMIT_WINDOW_S`` exceeds ``LOGIN_RATE_LIMIT_ATTEMPTS`` further
    attempts are rejected. State is in memory and resets on restart, which also
    rotates sessions.
    """

    def __init__(self) -> None:
        self._by_ip: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def allow(self, ip: str) -> bool:
        now = time.time()
        cutoff = now - LOGIN_RATE_LIMIT_WINDOW_S
        with self._lock:
            history = self._by_ip.get(ip, [])
            history = [t for t in history if t > cutoff]
            if len(history) >= LOGIN_RATE_LIMIT_ATTEMPTS:
                self._by_ip[ip] = history
                return False
            history.append(now)
            self._by_ip[ip] = history
            return True


def _make_handler(
    broadcaster: Broadcaster,
    dashboard_dir,
    *,
    post_router: Optional[PostRouter] = None,
):
    login_limiter = _LoginRateLimiter()

    class Handler(SimpleHTTPRequestHandler):
        # Read deadline for the request line and headers. Without it a
        # slowloris client could hold a worker thread forever: the write
        # timeout in _handle_sse is only set after auth and header parsing, and
        # MAX_SSE_CLIENTS is enforced later still. socketserver applies
        # Handler.timeout to the connection socket; _handle_sse sets its own
        # longer timeout once streaming.
        timeout = config.SSE_HEADER_READ_TIMEOUT_S

        # Serve files from the dashboard directory.
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(dashboard_dir), **kwargs)

        def log_message(self, fmt, *args):
            # Log at debug; the default writes every request to stderr.
            logger.debug(fmt, *args)

        # No caching for dashboard assets.
        def end_headers(self):
            self.send_header("Cache-Control", "no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            # Deny framing (clickjacking) and content-type sniffing. No CSP:
            # the dashboard's in-browser Babel transpile needs 'unsafe-eval',
            # which would weaken it.
            self.send_header("X-Frame-Options", "SAMEORIGIN")
            self.send_header("X-Content-Type-Options", "nosniff")
            super().end_headers()

        def do_GET(self):  # noqa: N802 — http.server signature
            canonical_path = _dashboard_static_path(self.path)
            path = canonical_path or self.path.split("?", 1)[0]
            if path == "/events":
                self._handle_sse()
                return
            if path == "/api/v2/auth/login":
                self._handle_login()
                return
            if path == "/api/v2/auth/status":
                self._handle_auth_status()
                return
            # snapshot.json holds operator and mode state, per-wallet DRY/LIVE,
            # group caps, the live CLOB balance and strategy thresholds, so it
            # needs the same auth as /events (JS and CSS stay open so the login
            # page can load). Without a router, _handle_auth_status returns
            # 503. Keep this tuple and _ALLOWED_DASHBOARD_STATIC in sync: an
            # allowlisted JSON file missing from here is served without auth.
            if path in ("/snapshot.json",):
                if post_router is None:
                    self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                        "reason": "auth_not_configured",
                    })
                    return
                if not post_router.check_auth(self.headers.get("Cookie")):
                    self._send_json(HTTPStatus.UNAUTHORIZED, {"reason": "unauthorized"})
                    return
            if not _is_allowed_dashboard_static(self.path):
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            super().do_GET()

        def do_HEAD(self):  # noqa: N802
            canonical_path = _dashboard_static_path(self.path)
            path = canonical_path or self.path.split("?", 1)[0]
            if path in ("/snapshot.json",):
                if post_router is None:
                    self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
                    return
                if not post_router.check_auth(self.headers.get("Cookie")):
                    self.send_error(HTTPStatus.UNAUTHORIZED)
                    return
            if not _is_allowed_dashboard_static(self.path):
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            super().do_HEAD()

        def do_POST(self):  # noqa: N802 — http.server signature
            path = self.path.split("?", 1)[0]
            if path == "/api/v2/auth/login":
                self._handle_login()
                return
            if post_router is None:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                    "reason": "post_router_not_configured",
                })
                return
            cookie_header = self.headers.get("Cookie")
            if post_router.check_auth(cookie_header) and not self._csrf_valid(
                cookie_header
            ):
                self._send_json(HTTPStatus.FORBIDDEN, {"reason": "csrf_failed"})
                return
            raw = self._read_capped_body()
            if raw is None:
                return  # response already emitted
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send_json(HTTPStatus.BAD_REQUEST, {"reason": "invalid_json"})
                return
            if not isinstance(body, dict):
                self._send_json(HTTPStatus.BAD_REQUEST, {"reason": "invalid_body"})
                return
            status, response = post_router.dispatch(
                path, body, cookie_header=cookie_header
            )
            self._send_json(status, response)

        def _csrf_valid(self, cookie_header: Optional[str]) -> bool:
            """Require both a session-bound token and same-origin metadata."""
            if post_router is None:
                return False
            if post_router._auth_disabled:  # type: ignore[attr-defined]
                return True
            if not post_router.check_csrf(
                cookie_header, self.headers.get("X-CSRF-Token")
            ):
                return False
            expected_scheme = (
                "https"
                if os.environ.get("CRYPTOBOT_DASHBOARD_TLS_TERMINATED", "0") == "1"
                else "http"
            )
            host = (self.headers.get("Host") or "").strip()
            expected_origin = f"{expected_scheme}://{host}"
            origin = (self.headers.get("Origin") or "").strip().rstrip("/")
            if origin:
                return hmac.compare_digest(origin, expected_origin)
            referer = (self.headers.get("Referer") or "").strip()
            if not referer:
                return False
            parsed = urlsplit(referer)
            referer_origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
            return hmac.compare_digest(referer_origin, expected_origin)

        def _handle_login(self) -> None:
            """Set a ``session_token`` cookie when the request carries the
            dashboard password. POST ``{ "password": "..." }``.

            Rate-limited per IP to ``LOGIN_RATE_LIMIT_ATTEMPTS`` attempts per
            ``LOGIN_RATE_LIMIT_WINDOW_S``. The 429 slows guessing without
            locking the operator out for good.
            """
            if post_router is None:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                    "reason": "post_router_not_configured",
                })
                return
            # A credential endpoint must not answer a safe, cacheable method.
            # GET routes here too (do_GET), so reject anything but POST before
            # reading the body or counting an attempt.
            if self.command != "POST":
                self._send_json(HTTPStatus.METHOD_NOT_ALLOWED, {
                    "reason": "method_not_allowed",
                })
                return
            # With no password set there is nothing to guess; return before
            # counting an attempt.
            if not post_router._session_token:  # type: ignore[attr-defined]
                self._send_json(HTTPStatus.UNAUTHORIZED, {
                    "reason": "dashboard_pass_unset",
                })
                return
            # Behind a TLS-terminating proxy every request comes from the
            # proxy's IP, which would put all operators in one rate-limit
            # bucket. Use the first X-Forwarded-For hop instead, but only when
            # the trusted-proxy flag is set (otherwise the header can be
            # spoofed). The limiter is per process and can be dodged by
            # rotating IPs; it slows guessing, it is not access control.
            if os.environ.get("CRYPTOBOT_DASHBOARD_TLS_TERMINATED", "0") == "1":
                fwd = (self.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
                client_ip = fwd or (
                    self.client_address[0] if self.client_address else "unknown"
                )
            else:
                client_ip = self.client_address[0] if self.client_address else "unknown"
            if not login_limiter.allow(client_ip):
                self._send_json(HTTPStatus.TOO_MANY_REQUESTS, {
                    "reason": "rate_limited",
                    "retry_after_s": int(LOGIN_RATE_LIMIT_WINDOW_S),
                })
                return
            raw = self._read_capped_body()
            if raw is None:
                return  # response already emitted
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send_json(HTTPStatus.BAD_REQUEST, {"reason": "invalid_json"})
                return
            if not isinstance(body, dict):
                self._send_json(HTTPStatus.BAD_REQUEST, {"reason": "invalid_body"})
                return
            supplied = body.get("password")
            if supplied is None:
                supplied = ""
            elif not isinstance(supplied, str):
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "reason": "invalid_password",
                })
                return
            expected_pass = os.environ.get("CRYPTOBOT_DASHBOARD_PASS", "")
            if not expected_pass or not hmac.compare_digest(supplied, expected_pass):
                self._send_json(HTTPStatus.UNAUTHORIZED, {"reason": "bad_password"})
                return
            cookie_value = post_router._session_token  # type: ignore[attr-defined]
            csrf_value = derive_csrf_token(cookie_value)
            secure_attr = _cookie_secure_attr()
            data = b'{"ok":true}'
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header(
                "Set-Cookie",
                f"session_token={cookie_value}; HttpOnly; "
                f"SameSite=Strict; {secure_attr}Path=/",
            )
            self.send_header(
                "Set-Cookie",
                f"csrf_token={csrf_value}; SameSite=Strict; "
                f"{secure_attr}Path=/",
            )
            self.end_headers()
            self.wfile.write(data)

        def _handle_auth_status(self) -> None:
            # Standalone (post_router=None): return 503. A 200 would make the
            # client think it is logged in and open the SSE stream.
            if post_router is None:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                    "reason": "auth_not_configured",
                })
                return
            if post_router.check_auth(self.headers.get("Cookie")):
                self._send_json(HTTPStatus.OK, {"ok": True})
                return
            self._send_json(HTTPStatus.UNAUTHORIZED, {"reason": "unauthorized"})

        def _read_capped_body(self) -> Optional[bytes]:
            """Read the POST body, capped at ``MAX_POST_BODY_BYTES``.

            Prevents a large ``Content-Length`` from allocating arbitrary
            memory. Returns the body, or ``None`` after sending a 413.
            """
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except (TypeError, ValueError):
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "reason": "invalid_content_length",
                })
                return None
            if length < 0:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "reason": "invalid_content_length",
                })
                return None
            if length > MAX_POST_BODY_BYTES:
                self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {
                    "reason": "body_too_large",
                    "limit_bytes": MAX_POST_BODY_BYTES,
                })
                return None
            return self.rfile.read(length) if length > 0 else b"{}"

        def _send_json(self, status: int, body: dict) -> None:
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except OSError:
                pass

        def _handle_sse(self) -> None:
            if post_router is None:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                    "reason": "auth_not_configured",
                })
                return
            if not post_router.check_auth(self.headers.get("Cookie")):
                self._send_json(HTTPStatus.UNAUTHORIZED, {"reason": "unauthorized"})
                return
            q = broadcaster.register()
            if q is None:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"too many SSE clients")
                return
            # Set a socket timeout so one stuck client cannot block the handler
            # longer than the per-client write timeout; otherwise
            # ``wfile.write`` could stall on TCP backpressure and stop the
            # dashboard behind one slow tab.
            try:
                self.connection.settimeout(config.SSE_CLIENT_WRITE_TIMEOUT_S)
            except (OSError, AttributeError):
                pass
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()
                # Initial comment so the browser marks the stream connected.
                try:
                    self.wfile.write(b": connected\n\n")
                    self.wfile.flush()
                except OSError:
                    return
                while True:
                    try:
                        event = q.get(timeout=15.0)
                    except queue.Empty:
                        # Heartbeat comment to keep the connection open.
                        try:
                            self.wfile.write(b": heartbeat\n\n")
                            self.wfile.flush()
                        except OSError:
                            return
                        continue
                    try:
                        event_id, data = event
                        self.wfile.write(b"id: ")
                        self.wfile.write(str(event_id).encode("ascii"))
                        self.wfile.write(b"\n")
                        self.wfile.write(b"event: tick\n")
                        self.wfile.write(b"data: ")
                        self.wfile.write(data.encode("utf-8"))
                        self.wfile.write(b"\n\n")
                        self.wfile.flush()
                    except (OSError, BrokenPipeError):
                        return
            finally:
                broadcaster.unregister(q)
    return Handler


def start_server(
    broadcaster: Broadcaster,
    *,
    host: str | None = None,
    port: int | None = None,
    dashboard_dir=None,
    post_router: Optional[PostRouter] = None,
) -> tuple[_ThreadingServer, threading.Thread]:
    """Bind and start in a daemon thread; return (httpd, thread).

    With ``post_router`` the handler serves the ``/api/v2/*`` endpoints with
    cookie auth. Without it, operator POSTs, snapshot reads and the SSE stream
    all return 503.
    """
    host = host if host is not None else config.DASHBOARD_BIND_HOST
    port = port if port is not None else config.DASHBOARD_HTTP_PORT
    dashboard_dir = dashboard_dir if dashboard_dir is not None else config.DASHBOARD_DIR

    handler = _make_handler(broadcaster, dashboard_dir, post_router=post_router)
    httpd = _ThreadingServer((host, port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    logger.info("sse_server listening on http://%s:%d/events", host, port)
    return httpd, thread
