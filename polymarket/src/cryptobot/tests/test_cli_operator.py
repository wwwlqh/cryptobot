"""CLI coverage for cryptobot.cli.operator."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from types import ModuleType

import pytest

from cryptobot.cli.operator import main as cli_main
from cryptobot.db import init_db
from cryptobot.execution.operator_control import (
    get_operator_state,
    seed_operator_control_state,
    start_processing,
)
from cryptobot_polymarket.transfer import preview_transfer


@pytest.fixture(autouse=True)
def _reset_runtime_secrets() -> None:
    from cryptobot.runtime_secrets import set_config

    set_config(None)
    yield
    set_config(None)


def _seed_db(db_path: Path, *, live: bool = False) -> None:
    conn = init_db(db_path)
    try:
        seed_operator_control_state(conn, boot_dry_run=False)
        if live:
            start_processing(conn)
    finally:
        conn.close()


def _insert_settled_live_row(
    conn: sqlite3.Connection,
    *,
    wallet_id: str,
    pnl_net: float,
) -> None:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side, window_start_ts,
           window_end_ts, interval_min, poly_market_id, poly_token_id,
           fee_rate_bps, model_p, p_market, fill_price, realized_edge,
           target_usd, bet_size, entry_top_price, config_sha256, mode,
           outcome, pnl_net)
        VALUES ('2026-05-24', 'BTC', ?, ?, 4, 'YES', 1715923200,
                1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55, 0.55, 0.10,
                5.0, 5.0, 0.55, 'sha', 'LIVE', 'WIN', ?)
        """,
        (wallet_id, wallet_id, pnl_net),
    )
    conn.commit()


def _seed_preview(db_path: Path) -> str:
    conn = init_db(db_path)
    try:
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=5.0)
        preview = preview_transfer(
            conn,
            from_wallet="0xfunder",
            to_wallet="0xreturn",
            to_wallet_allowlist="0xreturn",
            amount_usd=3.0,
            source_wallet_id="v6_5m",
        )
        return preview.idempotency_key
    finally:
        conn.close()


def test_status_prints_operator_payload(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "status.db"
    _seed_db(db_path)

    rc = cli_main(["status", "--db-path", str(db_path)])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["operator"]["state"] == "STOPPED_PROCESSING"
    assert payload["walletModes"] == {}


def test_start_and_stop_mutate_global_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _seed_db(db_path)

    assert cli_main(["start", "--db-path", str(db_path), "--reason", "smoke"]) == 0
    conn = init_db(db_path)
    try:
        assert get_operator_state(conn).state == "LIVE"
        row = conn.execute(
            "SELECT actor, reason FROM operator_control_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["actor"] == "cli:start"
        assert row["reason"] == "smoke"
    finally:
        conn.close()

    assert cli_main(["stop", "--db-path", str(db_path), "--reason", "halt"]) == 0
    conn = init_db(db_path)
    try:
        assert get_operator_state(conn).state == "STOPPED_PROCESSING"
    finally:
        conn.close()


def test_transfer_preview_uses_per_wallet_transfer_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    monkeypatch.setenv("POLY_FUNDER", "0xfunder")
    monkeypatch.setenv("POLY_RETURN_WALLET", "0xreturn")
    db_path = tmp_path / "preview.db"
    _seed_db(db_path)
    conn = init_db(db_path)
    try:
        _insert_settled_live_row(conn, wallet_id="v6_5m", pnl_net=5.0)
    finally:
        conn.close()

    rc = cli_main(
        [
            "transfer-preview",
            "--db-path",
            str(db_path),
            "--amount",
            "3.0",
            "--source-wallet-id",
            "v6_5m",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["amountUsd"] == 3.0
    assert payload["sourceWalletId"] == "v6_5m"
    assert payload["idempotencyKey"]


def test_transfer_submit_fails_closed_without_relayer_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    monkeypatch.setenv("POLY_RETURN_WALLET", "0xreturn")
    db_path = tmp_path / "submit_no_relayer.db"
    _seed_db(db_path, live=True)
    key = _seed_preview(db_path)

    def _missing_module(name: str):
        if name == "cryptobot_polymarket.relayer":
            raise ModuleNotFoundError(name)
        return __import__(name)

    monkeypatch.setattr("cryptobot.cli.operator.importlib.import_module", _missing_module)
    rc = cli_main(
        [
            "transfer-submit",
            "--db-path",
            str(db_path),
            "--idempotency-key",
            key,
            "--i-have-read-the-preview",
        ]
    )

    assert rc == 2
    assert "relayer_not_configured" in capsys.readouterr().err


def test_transfer_submit_uses_future_relayer_builder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    monkeypatch.setenv("POLY_FUNDER", "0xfunder")
    monkeypatch.setenv("POLY_RETURN_WALLET", "0xreturn")
    db_path = tmp_path / "submit.db"
    _seed_db(db_path, live=True)
    key = _seed_preview(db_path)
    observed: dict[str, object] = {}

    relayer_mod = ModuleType("cryptobot_polymarket.relayer")

    def build_transfer_submitter(config):
        observed["funder"] = config.poly_funder

        def _submitter(payload):
            observed["payload"] = payload
            return {"success": True, "tx_hash": "0xtx", "relayer_tx_id": "rly-1"}

        return _submitter

    relayer_mod.build_transfer_submitter = build_transfer_submitter
    monkeypatch.setitem(sys.modules, "cryptobot_polymarket.relayer", relayer_mod)

    rc = cli_main(
        [
            "transfer-submit",
            "--db-path",
            str(db_path),
            "--idempotency-key",
            key,
            "--i-have-read-the-preview",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "CONFIRMED"
    assert payload["txHash"] == "0xtx"
    assert observed["funder"] == "0xfunder"
    assert observed["payload"]["idempotency_key"] == key


class TestCapitalFlowCommands:
    """record-flow / list-flows / propose-flows — the operator surface for
    cash-flow-aware equity accounting."""

    def test_record_flow_writes_a_validated_row(self, tmp_path: Path, capsys) -> None:
        db_path = tmp_path / "flow.db"
        _seed_db(db_path)

        rc = cli_main([
            "record-flow", "--db-path", str(db_path),
            "--kind", "deposit", "--amount", "91.65",
            "--at", "2026-07-26 06:10:00", "--note", "wire",
        ])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)["capitalFlow"]
        assert payload["kind"] == "deposit"
        assert payload["amount_usd"] == 91.65
        assert payload["occurred_at"] == "2026-07-26 06:10:00"

    @pytest.mark.parametrize(
        "kind,amount,reason",
        [
            ("deposit", "-91.65", "bad_sign"),
            ("withdrawal", "20", "bad_sign"),
            ("rebate", "-1", "bad_sign"),
        ],
    )
    def test_record_flow_refuses_wrong_sign(
        self, tmp_path: Path, capsys, kind: str, amount: str, reason: str
    ) -> None:
        """A wrong-signed amount is refused, never silently flipped — guessing
        would move the halt metric by twice the amount."""
        db_path = tmp_path / f"sign_{kind}.db"
        _seed_db(db_path)

        rc = cli_main([
            "record-flow", "--db-path", str(db_path),
            "--kind", kind, "--amount", amount, "--at", "2026-07-26 06:10:00",
        ])

        assert rc == 2
        assert reason in capsys.readouterr().err

    def test_record_flow_refuses_bad_timestamp(self, tmp_path: Path, capsys) -> None:
        db_path = tmp_path / "ts.db"
        _seed_db(db_path)

        rc = cli_main([
            "record-flow", "--db-path", str(db_path),
            "--kind", "deposit", "--amount", "5", "--at", "26/07/2026",
        ])

        assert rc == 2
        assert "bad_timestamp" in capsys.readouterr().err

    def test_record_flow_refuses_duplicate_without_force(
        self, tmp_path: Path, capsys
    ) -> None:
        db_path = tmp_path / "dupe.db"
        _seed_db(db_path)
        args = [
            "record-flow", "--db-path", str(db_path),
            "--kind", "deposit", "--amount", "91.65",
            "--at", "2026-07-26 06:10:00",
        ]
        assert cli_main(args) == 0
        capsys.readouterr()

        assert cli_main(args) == 2
        assert "duplicate" in capsys.readouterr().err

        assert cli_main(args + ["--force"]) == 0

    def test_record_flow_rejects_unknown_kind_at_parse_time(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(SystemExit):
            cli_main([
                "record-flow", "--db-path", str(tmp_path / "x.db"),
                "--kind", "airdrop", "--amount", "5", "--at", "2026-07-26 06:10:00",
            ])

    def test_list_flows_prints_rows_and_net(self, tmp_path: Path, capsys) -> None:
        db_path = tmp_path / "list.db"
        _seed_db(db_path)
        assert cli_main([
            "record-flow", "--db-path", str(db_path), "--kind", "deposit",
            "--amount", "91.65", "--at", "2026-07-26 06:10:00",
        ]) == 0
        assert cli_main([
            "record-flow", "--db-path", str(db_path), "--kind", "withdrawal",
            "--amount", "-20", "--at", "2026-07-27 08:50:00",
        ]) == 0
        capsys.readouterr()

        assert cli_main(["list-flows", "--db-path", str(db_path)]) == 0

        out = capsys.readouterr().out
        assert "+91.65" in out
        assert "-20.00" in out
        assert "+71.65" in out  # net line

    def test_list_flows_empty_state(self, tmp_path: Path, capsys) -> None:
        db_path = tmp_path / "empty.db"
        _seed_db(db_path)

        assert cli_main(["list-flows", "--db-path", str(db_path)]) == 0
        assert "no capital flows recorded" in capsys.readouterr().out

    def test_propose_flows_emits_commands_and_writes_nothing(
        self, tmp_path: Path, capsys
    ) -> None:
        db_path = tmp_path / "propose.db"
        _seed_db(db_path)
        conn = init_db(db_path)
        try:
            for at, bal in (
                ("2026-07-26 06:05:00", 100.0), ("2026-07-26 06:15:00", 191.65),
            ):
                conn.execute(
                    "INSERT INTO bankroll_peak (sampled_at, wallet_balance, "
                    "realized_pnl, pending_exposure) VALUES (?, ?, 0.0, 0.0)",
                    (at, bal),
                )
            conn.commit()
        finally:
            conn.close()

        rc = cli_main(["propose-flows", "--db-path", str(db_path)])

        assert rc == 0
        out = capsys.readouterr().out
        assert "READ-ONLY" in out
        assert '--kind deposit --amount 91.65 --at "2026-07-26 06:10:00"' in out
        conn = init_db(db_path)
        try:
            assert conn.execute("SELECT COUNT(*) c FROM capital_flows").fetchone()[0] == 0
        finally:
            conn.close()

    def test_propose_flows_read_only_does_not_bootstrap_the_db(
        self, tmp_path: Path, capsys
    ) -> None:
        """--read-only must open a forensic snapshot without migrating it."""
        db_path = tmp_path / "snapshot.db"
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "CREATE TABLE bankroll_peak (id INTEGER PRIMARY KEY, "
                "sampled_at TEXT, wallet_balance REAL, realized_pnl REAL, "
                "pending_exposure REAL)"
            )
            conn.execute(
                "CREATE TABLE ledger (bet_ts TEXT, mode TEXT, outcome TEXT, "
                "exit_kind TEXT, exit_ts TEXT, resolution_ts TEXT)"
            )
            conn.executemany(
                "INSERT INTO bankroll_peak (sampled_at, wallet_balance, "
                "realized_pnl, pending_exposure) VALUES (?, ?, 0.0, 0.0)",
                [("2026-07-27 08:45:00", 100.0), ("2026-07-27 08:55:00", 183.98)],
            )
            conn.commit()
        finally:
            conn.close()

        rc = cli_main([
            "propose-flows", "--db-path", str(db_path), "--read-only",
        ])

        assert rc == 0
        assert "--amount 83.98" in capsys.readouterr().out
        conn = sqlite3.connect(db_path)
        try:
            tables = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            assert "capital_flows" not in tables  # never bootstrapped
        finally:
            conn.close()


def test_transfer_submit_requires_acknowledgement(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "submit_ack.db"
    _seed_db(db_path, live=True)

    rc = cli_main(
        [
            "transfer-submit",
            "--db-path",
            str(db_path),
            "--idempotency-key",
            "abc",
        ]
    )

    assert rc == 1
    assert "--i-have-read-the-preview" in capsys.readouterr().err
