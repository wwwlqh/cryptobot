"""U4 verification: CLI `python -m cryptobot.cli.set_wallet_mode`.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U4 / §U11a:

  - happy path: LIVE → DRY (always-allowed halt) returns exit 0.
  - blocked path: DRY → LIVE on a Phase 1 deploy (no readiness writer)
    returns exit 1 because the live-action guard rejects.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cryptobot.cli.set_wallet_mode import main as cli_main
from cryptobot.db import init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    seed_operator_control_state,
    seed_wallet_modes,
    set_wallet_mode,
    start_processing,
)


class _Sleeve:
    def __init__(self, label: str) -> None:
        self.label = label


class _StubPortfolio:
    def __init__(self, labels: list[str]) -> None:
        self._labels = labels

    @property
    def market_sleeves(self) -> list[_Sleeve]:
        return [_Sleeve(label=lbl) for lbl in self._labels]

    def default_mode_for(self, _wallet_id: str) -> str:
        return "DRY"


def _seed_db(db_path: Path) -> None:
    conn = init_db(db_path)
    try:
        seed_operator_control_state(conn, boot_dry_run=True)
        seed_wallet_modes(conn, _StubPortfolio(["v6_5m"]))
    finally:
        conn.close()


def test_cli_live_to_dry_succeeds(tmp_path: Path) -> None:
    db_path = tmp_path / "cli_halt.db"
    _seed_db(db_path)
    # Flip v6_5m to LIVE first by writing directly with a stub-accepting guard.
    conn = init_db(db_path)
    try:
        start_processing(conn)
        set_wallet_mode(
            conn,
            "v6_5m",
            "LIVE",
            guard_live_action_context=lambda *_: None,
        )
    finally:
        conn.close()

    rc = cli_main(
        ["--wallet", "v6_5m", "--target", "DRY", "--db-path", str(db_path)]
    )
    assert rc == 0


def test_cli_dry_to_live_blocked_without_readiness(tmp_path: Path) -> None:
    """Phase 1 deploy lacks readiness writer → CLI returns exit 1."""
    db_path = tmp_path / "cli_block.db"
    _seed_db(db_path)
    conn = init_db(db_path)
    try:
        start_processing(conn)
    finally:
        conn.close()
    rc = cli_main(
        [
            "--wallet",
            "v6_5m",
            "--target",
            "LIVE",
            "--db-path",
            str(db_path),
        ]
    )
    assert rc == 1


def test_cli_dry_to_live_succeeds_with_readiness_seeded(tmp_path: Path) -> None:
    db_path = tmp_path / "cli_promote.db"
    _seed_db(db_path)
    conn = init_db(db_path)
    try:
        start_processing(conn)
        now = utc_now_sql()
        conn.execute(
            "INSERT INTO live_readiness_reports (created_at, status, report_json) "
            "VALUES (?, 'READY', '{}')",
            (now,),
        )
        conn.execute(
            "INSERT INTO wallet_reconciliation_runs (sampled_at, source_status) "
            "VALUES (?, 'ok')",
            (now,),
        )
        conn.commit()
    finally:
        conn.close()
    rc = cli_main(
        [
            "--wallet",
            "v6_5m",
            "--target",
            "LIVE",
            "--db-path",
            str(db_path),
            "--reason",
            "smoke",
            "--actor",
            "test",
        ]
    )
    assert rc == 0


def test_cli_unknown_wallet_exits_3(tmp_path: Path) -> None:
    db_path = tmp_path / "cli_unknown.db"
    _seed_db(db_path)
    rc = cli_main(
        ["--wallet", "not_a_wallet", "--target", "DRY", "--db-path", str(db_path)]
    )
    assert rc == 3


def test_cli_normalizes_lowercase_target(tmp_path: Path) -> None:
    db_path = tmp_path / "cli_lower.db"
    _seed_db(db_path)
    rc = cli_main(
        ["--wallet", "v6_5m", "--target", "dry", "--db-path", str(db_path)]
    )
    # 'dry' lowercase target → normalize to 'DRY'; wallet is already DRY so
    # this is a no-op → exit 0.
    assert rc == 0


def test_cli_requires_wallet_and_target() -> None:
    with pytest.raises(SystemExit):
        cli_main(["--wallet", "v6_5m"])
