"""U11a verification: `python -m cryptobot.cli.live_go_no_go` exit codes.

Phase 2 deploy needs the CLI to exit 0 when readiness=READY and 1 otherwise
so install_remote.sh's pre-deploy probe can fail closed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cryptobot.cli.live_go_no_go import main as cli_main
from cryptobot.db import init_db
from cryptobot.db.connection import utc_now_sql


@pytest.fixture(autouse=True)
def _reset_runtime_secrets() -> None:
    from cryptobot.runtime_secrets import set_config
    set_config(None)
    yield
    set_config(None)


def _seed_db(
    db_path: Path,
    *,
    status: str = "READY",
    funder: str = "0xfunder",
    clob_balance_usd: float = 250.0,
) -> None:
    conn = init_db(db_path)
    try:
        conn.execute(
            """
            INSERT INTO live_readiness_reports
              (created_at, status, mode, funder, signer, clob_balance_usd,
               chain_balance_usd, report_json)
            VALUES (?, ?, 'LIVE', ?, NULL, ?, NULL, ?)
            """,
            (utc_now_sql(), status, funder, clob_balance_usd, '{"funder_address": "0xfunder", "readiness_status": "READY"}'),
        )
        conn.commit()
    finally:
        conn.close()


def test_offline_exits_zero_when_latest_is_ready(
    tmp_path: Path, monkeypatch
) -> None:
    db = tmp_path / "ready.db"
    _seed_db(db, status="READY")
    monkeypatch.setattr("cryptobot.cli.live_go_no_go.config.DB_PATH", db)
    rc = cli_main(["--offline"])
    assert rc == 0


def test_offline_exits_one_when_latest_is_not_ready(
    tmp_path: Path, monkeypatch
) -> None:
    db = tmp_path / "notready.db"
    _seed_db(db, status="NOT_READY")
    monkeypatch.setattr("cryptobot.cli.live_go_no_go.config.DB_PATH", db)
    rc = cli_main(["--offline"])
    assert rc == 1


def test_offline_exits_one_when_no_report(
    tmp_path: Path, monkeypatch
) -> None:
    db = tmp_path / "empty.db"
    # Just init the schema; don't insert any rows.
    conn = init_db(db)
    conn.close()
    monkeypatch.setattr("cryptobot.cli.live_go_no_go.config.DB_PATH", db)
    rc = cli_main(["--offline"])
    assert rc == 1


def test_offline_prints_report(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    db = tmp_path / "print.db"
    _seed_db(db, status="READY", funder="0xfunder", clob_balance_usd=250.0)
    monkeypatch.setattr("cryptobot.cli.live_go_no_go.config.DB_PATH", db)
    cli_main(["--offline"])
    out = capsys.readouterr().out
    assert "status=READY" in out
    assert "funder=0xfunder" in out
