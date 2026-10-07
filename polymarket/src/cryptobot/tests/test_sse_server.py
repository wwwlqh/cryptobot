"""U11 SSE server tests — broadcaster + static file serving."""

from __future__ import annotations

import json
import queue
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from cryptobot.observability.sse_server import Broadcaster, PostRouter, start_server


def test_broadcaster_caps_clients():
    bc = Broadcaster(max_clients=2)
    a = bc.register()
    b = bc.register()
    c = bc.register()
    assert a is not None and b is not None
    assert c is None   # over cap


def test_broadcaster_publish_fans_out():
    bc = Broadcaster(max_clients=5)
    a = bc.register()
    b = bc.register()
    bc.publish({"hello": "world"})
    # Queue items are (event_id, json_text) tuples so the SSE handler can
    # emit `id:` and support Last-Event-ID resumption (review #29).
    eid_a, data_a = a.get_nowait()
    eid_b, data_b = b.get_nowait()
    assert eid_a == eid_b == 1   # both subscribers see the same event_id
    assert json.loads(data_a) == {"hello": "world"}
    assert json.loads(data_b) == {"hello": "world"}


def test_broadcaster_event_id_increments():
    bc = Broadcaster(max_clients=5)
    q = bc.register()
    bc.publish({"n": 1})
    bc.publish({"n": 2})
    eid1, _ = q.get_nowait()
    eid2, _ = q.get_nowait()
    assert eid1 == 1
    assert eid2 == 2


def test_broadcaster_drop_on_slow_client():
    bc = Broadcaster(max_clients=2)
    a = bc.register()
    # Fill the queue past its bound (64).
    for i in range(64):
        a.put_nowait(str(i))
    # The 65th publish must NOT block.
    t0 = time.monotonic()
    bc.publish({"new": True})
    elapsed = time.monotonic() - t0
    assert elapsed < 0.1


def test_broadcaster_unregister_drops_client():
    bc = Broadcaster(max_clients=5)
    a = bc.register()
    bc.unregister(a)
    assert bc.clients == []


def test_server_serves_static_files_and_events(tmp_path: Path):
    # Build a minimal dashboard dir with one file.
    dash = tmp_path / "dashboard"
    dash.mkdir()
    (dash / "index.html").write_text("hi from dashboard", encoding="utf-8")
    (dash / "ledger_full.json").write_text('{"secret":true}', encoding="utf-8")
    # RETIRED 2026-08-01 with the cross-venue dashboard view: the predict.fun
    # dumps are no longer allowlisted, so a file sitting in the dashboard dir
    # (e.g. left over on a box from before the cutover) must 404 like every
    # other non-allowlisted JSON. Written here precisely so the guard is
    # exercised against a file that EXISTS.
    (dash / "predictfun_paper.json").write_text(
        '[{"id": 7, "venue": "predictfun"}]', encoding="utf-8"
    )
    (dash / "predictfun_live.json").write_text('{"venue":"predictfun"}', encoding="utf-8")

    bc = Broadcaster(max_clients=5)
    # Use port 0 to let the OS pick a free port.
    router = PostRouter(
        conn=None,
        portfolio=None,
        session_token="",
        auth_disabled=True,
    )
    httpd, _thread = start_server(
        bc, host="127.0.0.1", port=0, dashboard_dir=dash, post_router=router
    )
    port = httpd.server_address[1]
    try:
        # Static file fetch
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/index.html", timeout=2) as r:
            body = r.read()
            assert body == b"hi from dashboard"
            assert r.headers.get("Cache-Control", "").startswith("no-store")
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}/ledger_full.json", timeout=2
            )
        assert exc.value.code == 404
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/ledger_full.json", method="HEAD"
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(req, timeout=2)
        assert exc.value.code == 404
        # The retired predict.fun dumps are OFF the allowlist: a stale file on
        # disk must never be served, GET or HEAD.
        for retired in ("predictfun_paper.json", "predictfun_live.json"):
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/{retired}", timeout=2
                )
            assert exc.value.code == 404
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(
                    urllib.request.Request(
                        f"http://127.0.0.1:{port}/{retired}", method="HEAD"
                    ),
                    timeout=2,
                )
            assert exc.value.code == 404

        # SSE stream — open with a short read timeout, then publish a payload.
        req = urllib.request.Request(f"http://127.0.0.1:{port}/events")
        with urllib.request.urlopen(req, timeout=2) as resp:
            # Read until we have a tick event or timeout.
            received = bytearray()
            done = threading.Event()

            def push():
                time.sleep(0.1)
                bc.publish({"ok": 1})

            threading.Thread(target=push, daemon=True).start()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                chunk = resp.read1(1024)
                if not chunk:
                    break
                received += chunk
                if b"event: tick" in received and b'"ok":1' in received:
                    done.set()
                    break
            assert done.is_set(), f"timed out: {received[:200]!r}"
    finally:
        httpd.shutdown()


def test_server_rejects_503_over_client_cap(tmp_path: Path):
    dash = tmp_path / "dashboard"
    dash.mkdir()
    bc = Broadcaster(max_clients=1)
    # Pre-fill cap so the SSE request bounces.
    bc.register()
    router = PostRouter(
        conn=None,
        portfolio=None,
        session_token="",
        auth_disabled=True,
    )
    httpd, _ = start_server(
        bc, host="127.0.0.1", port=0, dashboard_dir=dash, post_router=router
    )
    port = httpd.server_address[1]
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/events", timeout=2)
            assert False, "expected 503"
        except urllib.error.HTTPError as e:
            assert e.code == 503
    finally:
        httpd.shutdown()
