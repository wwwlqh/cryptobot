"""HTTP-level auth tests for the SSE server's /api/v2 surface.

The router-level tests in `test_sse_endpoints.py` cover dispatch + body
shape. This file fires real HTTP requests against a temporary server so
the cookie attributes (`HttpOnly`, `SameSite=Strict`, `Path=/`)
are observable in the actual `Set-Cookie` header.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from http.cookies import SimpleCookie
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.execution.operator_control import boot_operator_state
from cryptobot.observability.sse_server import (
    Broadcaster,
    PostRouter,
    derive_session_token,
    start_server,
)
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio


@pytest.fixture
def dashboard_dir(tmp_path: Path) -> Path:
    """Empty directory just so the static handler has a root to mount."""
    return tmp_path


@pytest.fixture
def db_and_portfolio(tmp_path: Path):
    conn = init_db(tmp_path / "auth.db")
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    yield conn, portfolio
    conn.close()


@pytest.fixture
def server(db_and_portfolio, dashboard_dir, tmp_path: Path):
    conn, portfolio = db_and_portfolio
    os.environ["CRYPTOBOT_DASHBOARD_PASS"] = "secret"
    router = PostRouter(
        conn=conn,
        portfolio=portfolio,
        session_token=derive_session_token("secret"),
    )
    broadcaster = Broadcaster()
    httpd, thread = start_server(
        broadcaster,
        host="127.0.0.1",
        port=0,  # OS picks a free port
        dashboard_dir=dashboard_dir,
        post_router=router,
    )
    host, port = httpd.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        httpd.shutdown()
        # serve_forever exits on shutdown; the daemon thread will tear down.
        thread.join(timeout=2.0)
        os.environ.pop("CRYPTOBOT_DASHBOARD_PASS", None)


@pytest.fixture
def auth_disabled_server(db_and_portfolio, dashboard_dir):
    conn, portfolio = db_and_portfolio
    router = PostRouter(
        conn=conn,
        portfolio=portfolio,
        session_token="",
        auth_disabled=True,
    )
    broadcaster = Broadcaster()
    httpd, thread = start_server(
        broadcaster,
        host="127.0.0.1",
        port=0,
        dashboard_dir=dashboard_dir,
        post_router=router,
    )
    host, port = httpd.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        httpd.shutdown()
        thread.join(timeout=2.0)


def _post(
    url: str,
    body: object,
    *,
    cookie: str | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict, str | None]:
    request_headers = {"Content-Type": "application/json"}
    request_headers.update(headers or {})
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers=request_headers,
        method="POST",
    )
    if cookie:
        req.add_header("Cookie", cookie)
    try:
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            return (
                resp.status,
                json.loads(resp.read().decode("utf-8") or "{}"),
                "\n".join(resp.headers.get_all("Set-Cookie", [])),
            )
    except urllib.error.HTTPError as exc:
        body_raw = exc.read().decode("utf-8") or "{}"
        return exc.code, json.loads(body_raw), "\n".join(
            exc.headers.get_all("Set-Cookie", [])
        )


def _cookie_header(set_cookie: str | None) -> str:
    cookies = SimpleCookie()
    for line in (set_cookie or "").splitlines():
        cookies.load(line)
    return "; ".join(f"{key}={morsel.value}" for key, morsel in cookies.items())


def _cookie_value(set_cookie: str | None, name: str) -> str:
    cookies = SimpleCookie()
    for line in (set_cookie or "").splitlines():
        cookies.load(line)
    morsel = cookies.get(name)
    assert morsel is not None, f"missing {name} cookie in {set_cookie!r}"
    return morsel.value


def _get(url: str, *, cookie: str | None = None, raw: bool = False) -> tuple[int, object]:
    """``(status, parsed_body)``.

    ``raw=True`` returns the body as TEXT and never parses it — needed for the
    non-allowlisted paths, whose 404 comes from http.server's own HTML error
    page rather than a JSON envelope.
    """
    req = urllib.request.Request(url, method="GET")
    if cookie:
        req.add_header("Cookie", cookie)
    try:
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            text = resp.read().decode("utf-8", "replace")
            return resp.status, (text if raw else json.loads(text or "{}"))
    except urllib.error.HTTPError as exc:
        body_raw = exc.read().decode("utf-8", "replace")
        return exc.code, (body_raw if raw else json.loads(body_raw or "{}"))


def _head(url: str, *, cookie: str | None = None) -> int:
    req = urllib.request.Request(url, method="HEAD")
    if cookie:
        req.add_header("Cookie", cookie)
    try:
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def test_login_with_correct_password_issues_session_cookie(server: str) -> None:
    status, body, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    assert status == 200
    assert body == {"ok": True}
    assert set_cookie is not None
    # Cookie carries the security attributes.
    assert "HttpOnly" in set_cookie
    assert "SameSite=Strict" in set_cookie
    assert "Path=/" in set_cookie
    assert "Path=/api/v2/" not in set_cookie
    assert "csrf_token=" in set_cookie


def test_login_with_wrong_password_returns_401(server: str) -> None:
    status, body, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "wrong"})
    assert status == 401
    assert body["reason"] == "bad_password"
    assert set_cookie is None or "session_token" not in set_cookie


def test_unauthenticated_post_to_operator_start_returns_401(server: str) -> None:
    status, body, _ = _post(f"{server}/api/v2/operator/start", {"reason": "test"})
    assert status == 401
    assert body["reason"] == "unauthorized"


def test_authenticated_post_to_operator_start_returns_200(server: str) -> None:
    # Login first.
    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    cookie_kv = _cookie_header(set_cookie)
    status, body, _ = _post(
        f"{server}/api/v2/operator/start",
        {"reason": "via http"},
        cookie=cookie_kv,
        headers={
            "Origin": server,
            "X-CSRF-Token": _cookie_value(set_cookie, "csrf_token"),
        },
    )
    assert status == 200
    assert body["state"] == "LIVE"


def test_state_changing_post_requires_csrf_and_same_origin(server: str) -> None:
    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    cookie_kv = _cookie_header(set_cookie)
    csrf = _cookie_value(set_cookie, "csrf_token")

    status, body, _ = _post(
        f"{server}/api/v2/operator/start",
        {"reason": "missing csrf"},
        cookie=cookie_kv,
        headers={"Origin": server},
    )
    assert status == 403
    assert body["reason"] == "csrf_failed"

    status, body, _ = _post(
        f"{server}/api/v2/operator/start",
        {"reason": "wrong csrf"},
        cookie=cookie_kv,
        headers={"Origin": server, "X-CSRF-Token": "wrong-token"},
    )
    assert status == 403
    assert body["reason"] == "csrf_failed"

    status, body, _ = _post(
        f"{server}/api/v2/operator/start",
        {"reason": "foreign origin"},
        cookie=cookie_kv,
        headers={"Origin": "http://evil.example", "X-CSRF-Token": csrf},
    )
    assert status == 403
    assert body["reason"] == "csrf_failed"


def test_login_rejects_non_object_json_body(server: str) -> None:
    status, body, _ = _post(f"{server}/api/v2/auth/login", [])
    assert status == 400
    assert body["reason"] == "invalid_body"

    status, body, _ = _post(
        f"{server}/api/v2/auth/login", {"password": ["not-a-string"]}
    )
    assert status == 400
    assert body["reason"] == "invalid_password"


def test_non_object_json_body_returns_400(server: str) -> None:
    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    status, body, _ = _post(
        f"{server}/api/v2/operator/start",
        [],
        cookie=_cookie_header(set_cookie),
        headers={
            "Origin": server,
            "X-CSRF-Token": _cookie_value(set_cookie, "csrf_token"),
        },
    )
    assert status == 400
    assert body["reason"] == "invalid_body"


def test_invalid_expected_version_returns_400(server: str) -> None:
    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    status, body, _ = _post(
        f"{server}/api/v2/operator/start",
        {"expected_version": {"not": "an integer"}},
        cookie=_cookie_header(set_cookie),
        headers={
            "Origin": server,
            "X-CSRF-Token": _cookie_value(set_cookie, "csrf_token"),
        },
    )
    assert status == 400
    assert body["reason"] == "invalid_expected_version"


def test_auth_disabled_local_post_does_not_require_browser_origin(
    auth_disabled_server: str,
) -> None:
    status, body, _ = _post(
        f"{auth_disabled_server}/api/v2/operator/start", {"reason": "local dev"}
    )
    assert status == 200
    assert body["state"] == "LIVE"


def test_auth_status_and_events_require_cookie(server: str) -> None:
    status, body = _get(f"{server}/api/v2/auth/status")
    assert status == 401
    assert body["reason"] == "unauthorized"

    status, body = _get(f"{server}/events")
    assert status == 401
    assert body["reason"] == "unauthorized"

    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    cookie_kv = _cookie_header(set_cookie)
    status, body = _get(f"{server}/api/v2/auth/status", cookie=cookie_kv)
    assert status == 200
    assert body == {"ok": True}


def test_encoded_snapshot_path_requires_auth(server: str, dashboard_dir: Path) -> None:
    (dashboard_dir / "snapshot.json").write_text('{"ok": true}', encoding="utf-8")
    status, body = _get(f"{server}/%73napshot.json")
    assert status == 401
    assert body["reason"] == "unauthorized"

    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    status, body = _get(
        f"{server}/%73napshot.json", cookie=_cookie_header(set_cookie)
    )
    assert status == 200
    assert body == {"ok": True}
    assert _head(f"{server}/%73napshot.json") == 401
    assert _head(f"{server}/%73napshot.json", cookie=_cookie_header(set_cookie)) == 200


def test_retired_predictfun_dumps_are_not_served_even_authenticated(
    server: str, dashboard_dir: Path
) -> None:
    """The predict.fun dashboard dumps were RETIRED 2026-08-01 with the
    cross-venue view.

    They used to be allowlisted AND auth-gated (they carry trading rows, wallet
    addresses and operator state). Now they are off the allowlist entirely, so
    they 404 — before auth is even consulted — even for a session that HAS the
    cookie. This is the stronger property and the one that matters on a box
    still holding a stale dump from before the cutover.

    The allowlist and the auth tuple must change TOGETHER: an allowlisted JSON
    missing from the auth tuple would serve unauthenticated."""
    for retired in ("predictfun_paper.json", "predictfun_live.json"):
        (dashboard_dir / retired).write_text(
            '[{"id": 1, "venue": "predictfun"}]', encoding="utf-8"
        )

    _, _, set_cookie = _post(f"{server}/api/v2/auth/login", {"password": "secret"})
    cookie_kv = _cookie_header(set_cookie)
    for retired in ("predictfun_paper.json", "predictfun_live.json"):
        status, _ = _get(f"{server}/{retired}", raw=True)
        assert status == 404, f"/{retired} must not be served unauthenticated"
        status, _ = _get(f"{server}/{retired}", cookie=cookie_kv, raw=True)
        assert status == 404, f"/{retired} must not be served even with a session"

    # snapshot.json is still the one auth-gated JSON: 401 without, 200 with.
    (dashboard_dir / "snapshot.json").write_text('{"ok": true}', encoding="utf-8")
    status, body = _get(f"{server}/snapshot.json")
    assert status == 401
    assert body["reason"] == "unauthorized"
    status, body = _get(f"{server}/snapshot.json", cookie=cookie_kv)
    assert status == 200
    assert body == {"ok": True}


def test_post_router_not_configured_returns_503(
    dashboard_dir: Path, tmp_path: Path
) -> None:
    """A Phase 1 SSE server boots without a PostRouter â€” every POST must
    fail closed with 503 rather than landing on an undefined handler."""
    broadcaster = Broadcaster()
    httpd, thread = start_server(
        broadcaster,
        host="127.0.0.1",
        port=0,
        dashboard_dir=dashboard_dir,
        post_router=None,
    )
    host, port = httpd.server_address
    try:
        status, body, _ = _post(
            f"http://{host}:{port}/api/v2/operator/start", {"reason": "test"}
        )
        (dashboard_dir / "snapshot.json").write_text(
            '{"secret": true}', encoding="utf-8"
        )
        snapshot_status, snapshot_body = _get(
            f"http://{host}:{port}/snapshot.json"
        )
        events_status, events_body = _get(f"http://{host}:{port}/events")
        snapshot_head_status = _head(f"http://{host}:{port}/snapshot.json")
    finally:
        httpd.shutdown()
        thread.join(timeout=2.0)
    assert status == 503
    assert body["reason"] == "post_router_not_configured"
    assert snapshot_status == 503
    assert snapshot_body["reason"] == "auth_not_configured"
    assert events_status == 503
    assert events_body["reason"] == "auth_not_configured"
    assert snapshot_head_status == 503


def test_oversized_post_body_returns_413(server: str) -> None:
    """Security finding #34: POST bodies are capped to bound memory.

    The 8 KiB cap (MAX_POST_BODY_BYTES) is conservative for our JSON
    envelopes; anything larger is misuse or attack.

    The server rejects an oversized body WITHOUT draining it — deliberate
    DoS resistance: it must not read/buffer attacker-controlled megabytes.
    On Linux it writes the 413 before the client finishes streaming the
    body; on Windows the un-read body makes the OS reset the client socket
    mid-send (WinError 10053) before the 413 is read — which flaked this
    test under full-suite load. Both outcomes prove the body was rejected
    un-buffered, so accept either a clean 413 or a connection reset. The
    reject path itself is exercised by test_sse_endpoints at the router
    level too.
    """
    huge = "x" * (9 * 1024)
    try:
        status, body, _ = _post(
            f"{server}/api/v2/operator/start", {"padding": huge}
        )
    except (ConnectionError, urllib.error.URLError):
        # Connection reset by the server's no-drain reject path is itself a
        # valid rejection of the oversized body (platform-dependent timing).
        return
    assert status == 413
    assert body["reason"] == "body_too_large"


def test_login_rate_limit_blocks_after_threshold(server: str) -> None:
    """Security finding #35: per-IP sliding-window rate limit throttles
    brute-force attempts against `CRYPTOBOT_DASHBOARD_PASS`. After
    LOGIN_RATE_LIMIT_ATTEMPTS failures within the window, subsequent
    requests return 429 without consulting the password."""
    # Burn 5 attempts; 6th should rate-limit before the password check.
    for _ in range(5):
        status, body, _ = _post(
            f"{server}/api/v2/auth/login", {"password": "wrong"}
        )
        assert status == 401
    status, body, _ = _post(
        f"{server}/api/v2/auth/login", {"password": "wrong"}
    )
    assert status == 429
    assert body["reason"] == "rate_limited"
