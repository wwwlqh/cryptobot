"""U14 main.py boot smoke tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest



@pytest.fixture(autouse=True)
def _enable_subminute(monkeypatch):
    """The deployed v2 "step1last" config is checkpoint-ONLY, so load_sleeves
    fails closed unless CRYPTOBOT_SUBMINUTE_ENABLED=1 (which the live EC2 env
    sets). These boot smoke tests must reflect that live topology; pin the flag
    on for the whole module so boot reaches the gate each test actually
    exercises. Tests that want the flag off set it explicitly."""
    from cryptobot import config

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)


def test_main_importable():
    from cryptobot.main import main  # noqa: F401


def test_main_once_smoke(tmp_path: Path, monkeypatch):
    """Boot + run one tick + exit. Spot fetch may fail offline; boot must
    survive that (TickRunner catches per-tick errors).

    Patches the module-level DB_PATH attribute directly — `monkeypatch.setattr`
    restores it after the test, no reload required.
    """
    from cryptobot import config

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "smoke.db")
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    from cryptobot import main as main_mod
    monkeypatch.setattr(main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False)
    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 0


def test_main_once_with_sse_publisher_exits_cleanly(tmp_path: Path, monkeypatch):
    """Latency item #1: the SSE publish runs on a dedicated daemon publisher
    thread (latest-wins depth-1 queue) instead of inline in on_tick. This
    smoke boots WITH the SSE surface and pins the shutdown contract: the
    publisher must start alongside the broadcaster and must never block
    process exit (daemon=True + stop event + bounded join on the --once
    finally path)."""
    import threading

    from cryptobot import config

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "ssepub.db")
    # OS-assigned port so parallel test runs can't collide on 8800.
    monkeypatch.setattr(config, "DASHBOARD_HTTP_PORT", 0)
    # Loopback bind + explicit auth opt-out satisfies the boot auth gate.
    monkeypatch.setenv("CRYPTOBOT_DASHBOARD_AUTH_DISABLED", "1")
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    from cryptobot import main as main_mod
    monkeypatch.setattr(
        main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False
    )

    rc = main_mod.main(["--once"])
    assert rc == 0

    # _stop_publisher on the --once finally must have stopped the thread; even
    # if a straggler were mid-join it MUST be a daemon (cannot block exit).
    for t in threading.enumerate():
        if t.name == "cryptobot-sse-publisher":
            assert t.daemon, "publisher must be a daemon thread"
            t.join(timeout=2.0)
            assert not t.is_alive(), "publisher must stop on shutdown"


def test_main_once_notify_dispatcher_exits_cleanly(tmp_path: Path, monkeypatch):
    """Latency finding (stop-fill notify / venue-degradation alert): _notify
    enqueues to a bounded daemon dispatcher instead of running send_event's
    blocking Telegram POST on the tick thread. Pin the shutdown contract on
    the --once finally path: daemon=True + drain/stop + bounded join, same
    as the SSE publisher above."""
    import threading

    from cryptobot import config

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "notifydisp.db")
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    from cryptobot import main as main_mod
    monkeypatch.setattr(
        main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False
    )

    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 0

    # notify_dispatcher.stop() on the --once finally must have stopped the
    # worker; even a straggler MUST be a daemon (cannot block exit).
    for t in threading.enumerate():
        if t.name == "cryptobot-notify-dispatch":
            assert t.daemon, "notify dispatcher must be a daemon thread"
            t.join(timeout=2.0)
            assert not t.is_alive(), "notify dispatcher must stop on shutdown"


def test_missing_v6_config_raises(tmp_path: Path, monkeypatch):
    """Boot fails fast when dry_run_recommended.json is missing."""
    from cryptobot import config

    monkeypatch.setattr(config, "V6_CONFIG_PATH", tmp_path / "no-such.json")
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "missing.db")
    from cryptobot import main as main_mod
    with pytest.raises(FileNotFoundError):
        main_mod.main(["--once", "--no-sse"])


def test_boot_refuses_tick_seconds_above_5(tmp_path: Path, monkeypatch):
    """Review-pass #6 closeout: TICK_SECONDS > 5 silently degrades the
    prev-tick anchor (divergence #4 fix) to first-after-boundary.
    Boot must refuse rather than degrade silently.
    """
    from cryptobot import config

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "ticktest.db")
    monkeypatch.setattr(config, "TICK_SECONDS", 6)
    from cryptobot import main as main_mod
    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 1


def test_boot_refuses_remote_bind_without_tls(tmp_path: Path, monkeypatch):
    """Audit w19oxqf24 finding #4 (P1) / D-LIVE-13: a non-loopback bind with
    ALLOW_REMOTE_DASHBOARD=1 but no TLS termination must refuse boot, because
    the operator-control session cookie would travel in cleartext and without
    the Secure flag. (This is the regression test the D-LIVE-13 spec planned
    but was never written.)
    """
    from cryptobot import config

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "tlsgate.db")
    monkeypatch.setattr(config, "DASHBOARD_BIND_HOST", "0.0.0.0")
    monkeypatch.setattr(config, "ALLOW_REMOTE_DASHBOARD", True)
    monkeypatch.setattr(config, "DASHBOARD_TLS_TERMINATED", False)
    from cryptobot import main as main_mod
    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 1, "remote bind without TLS must refuse boot"


def test_boot_refuses_remote_bind_without_remote_opt_in(tmp_path: Path, monkeypatch):
    """A non-loopback bind without ALLOW_REMOTE_DASHBOARD=1 still refuses boot
    (pre-existing gate; pinned alongside the new TLS condition)."""
    from cryptobot import config

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "remotegate.db")
    monkeypatch.setattr(config, "DASHBOARD_BIND_HOST", "0.0.0.0")
    monkeypatch.setattr(config, "ALLOW_REMOTE_DASHBOARD", False)
    from cryptobot import main as main_mod
    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 1


def test_boot_writes_partial_ret_warmup_warn_row(tmp_path: Path, monkeypatch):
    """Review-pass #25 closeout: a single pipeline_health WARN at boot
    distinguishes the ~30-min warmup window (100% SKIP:no_feature
    expected) from a broken feature pipeline.
    """
    import sqlite3
    from cryptobot import config

    db_path = tmp_path / "warmup.db"
    monkeypatch.setattr(config, "DB_PATH", db_path)
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    from cryptobot import main as main_mod
    monkeypatch.setattr(main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False)
    main_mod.main(["--once", "--no-sse"])

    # Open a fresh connection so any pending writes are visible.
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT message FROM pipeline_health "
            "WHERE stage='boot' AND status='WARN' "
            "  AND message LIKE '%partial_ret warmup%'"
        ).fetchall()
    assert rows, "expected boot WARN row about partial_ret warmup window"


# ============================================================================
# Per-strategy LIVE plan §U2 — Polymarket wallet topology + RELAYER_URL gate
# ============================================================================


def test_boot_refuses_invalid_poly_signature_topology(
    tmp_path: Path, monkeypatch
):
    """D-LIVE-7: SIG=3 (POLY_1271 deposit wallet) with empty POLY_FUNDER
    is impossible-to-fulfill — every LIVE order would sign against the
    wrong on-chain identity. Boot must fail closed (return 1) and write
    a CRITICAL pipeline_health row so the audit trail captures the cause.
    """
    import sqlite3

    from cryptobot import config
    from cryptobot import main as main_mod
    from cryptobot.runtime_secrets import set_config

    # Reset the Pydantic singleton so the next get_config() reads fresh
    # env vars set by monkeypatch.
    set_config(None)
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "3")
    monkeypatch.setenv("POLY_FUNDER", "")

    db_path = tmp_path / "sig_topology_fail.db"
    monkeypatch.setattr(config, "DB_PATH", db_path)
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    monkeypatch.setattr(
        main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False
    )

    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 1, (
        "POLY_SIGNATURE_TYPE=3 + empty POLY_FUNDER must refuse boot"
    )

    # Verify the audit trail row exists.
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT message FROM pipeline_health "
            "WHERE stage='boot' AND status='CRITICAL' "
            "  AND message LIKE '%poly_wallet_topology%'"
        ).fetchall()
    assert rows, "expected CRITICAL boot row when wallet topology invalid"

    # Reset singleton for subsequent tests.
    set_config(None)


def test_boot_allows_sig_zero_without_funder(tmp_path: Path, monkeypatch):
    """SIG=0 (EOA) with empty POLY_FUNDER is the dev/on-chain trader path
    and must boot cleanly — the funder is auto-derived from the private
    key inside py-clob-client."""
    from cryptobot import config
    from cryptobot import main as main_mod
    from cryptobot.runtime_secrets import set_config

    set_config(None)
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("POLY_FUNDER", "")
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "sig0.db")
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    monkeypatch.setattr(
        main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False
    )
    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 0
    set_config(None)


def test_boot_writes_critical_row_on_unknown_relayer_url(
    tmp_path: Path, monkeypatch
):
    """D-LIVE-27: RELAYER_URL not in the known-prefix list emits a
    CRITICAL audit row and blocks boot until the operator explicitly
    acknowledges the override."""
    import sqlite3

    from cryptobot import config
    from cryptobot import main as main_mod
    from cryptobot.runtime_secrets import set_config

    set_config(None)
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("RELAYER_URL", "https://unknown-relayer.example.com")

    db_path = tmp_path / "relayer_unknown.db"
    monkeypatch.setattr(config, "DB_PATH", db_path)
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    monkeypatch.setattr(
        main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False
    )

    try:
        rc = main_mod.main(["--once", "--no-sse"])
    finally:
        set_config(None)
    assert rc == 1

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT message FROM pipeline_health "
            "WHERE stage='boot' AND status='CRITICAL' "
            "  AND message LIKE '%relayer_url%'"
        ).fetchall()
    assert rows, "expected CRITICAL boot row when relayer_url is not known"


def test_unknown_relayer_url_override_allows_boot(
    tmp_path: Path, monkeypatch
):
    """The explicit CRYPTOBOT_RELAYER_URL_OVERRIDE=1 acknowledgement keeps
    emergency endpoint rotations possible after operator verification."""
    from cryptobot import config
    from cryptobot import main as main_mod
    from cryptobot.runtime_secrets import set_config

    set_config(None)
    monkeypatch.setenv("POLY_SIGNATURE_TYPE", "0")
    monkeypatch.setenv("RELAYER_URL", "https://unknown-relayer.example.com")
    monkeypatch.setenv("CRYPTOBOT_RELAYER_URL_OVERRIDE", "1")
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "relayer_override.db")
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)
    monkeypatch.setattr(
        main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False
    )

    try:
        rc = main_mod.main(["--once", "--no-sse"])
    finally:
        set_config(None)
    assert rc == 0


def test_boot_survives_orphan_reconcile_db_exception(
    tmp_path: Path, monkeypatch, caplog
):
    """Code-review finding #22 (ce-code-review run 20260520-162951).

    Step 5 of boot calls reconcile_orphan_pending(conn). On a stale or
    locked DB, that call can raise a sqlite3.DatabaseError /
    OperationalError BEFORE RF and LUT load. Without a guard, the entire
    boot sequence aborts. With the guard, the exception is logged and
    boot continues so the next F3 sweep can retry the orphan rows.
    """
    import logging
    import sqlite3
    from cryptobot import config
    from cryptobot import main as main_mod

    monkeypatch.setattr(config, "DB_PATH", tmp_path / "orphan_fail.db")
    monkeypatch.setattr(main_mod, "_warm_start_partial_ret_from_binance", lambda *a, **k: False)
    from cryptobot.scheduler import tick as tick_mod
    monkeypatch.setattr(tick_mod.TickRunner, "run_one_tick", lambda self: None)

    def _boom(_conn):
        raise sqlite3.OperationalError("database is locked")

    # main() does a lazy `from cryptobot.reconciliation.orphan_pending
    # import reconcile_orphan_pending` inside the boot sequence, so we
    # patch the source module — the lazy import resolves to the mock.
    from cryptobot.reconciliation import orphan_pending as orphan_mod
    monkeypatch.setattr(orphan_mod, "reconcile_orphan_pending", _boom)

    caplog.set_level(logging.WARNING, logger="cryptobot.main")
    # Boot must still complete; --once + --no-sse path returns when the
    # tick runner exits.
    rc = main_mod.main(["--once", "--no-sse"])
    assert rc == 0, (
        "boot must not abort just because reconcile_orphan_pending raised; "
        "the orphan rows can wait for the next F3 sweep"
    )

    assert any(
        "orphan_reconcile_failed" in record.message
        for record in caplog.records
    ), "expected a WARNING log row when orphan reconcile fails"
