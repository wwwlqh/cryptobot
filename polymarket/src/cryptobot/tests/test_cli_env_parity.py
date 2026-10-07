"""CLI coverage for cryptobot.cli.env_parity."""

from __future__ import annotations

import json
from pathlib import Path

from cryptobot.cli.env_parity import main as cli_main


def test_env_parity_ok_with_explicit_key_filter(tmp_path: Path, capsys) -> None:
    local = tmp_path / "local.env"
    server = tmp_path / "server.env"
    local.write_text("POLY_FUNDER=0xfunder\n", encoding="utf-8")
    server.write_text("POLY_FUNDER=0xfunder\n", encoding="utf-8")

    rc = cli_main(
        [
            "--local-env",
            str(local),
            "--server-env-file",
            str(server),
            "--key",
            "POLY_FUNDER",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert "Env parity: OK" in out


def test_env_parity_mismatch_prints_fingerprints_not_values(
    tmp_path: Path,
    capsys,
) -> None:
    local = tmp_path / "local.env"
    server = tmp_path / "server.env"
    local_secret = "local-super-secret-value"
    server_secret = "server-super-secret-value"
    local.write_text(f"POLY_SECRET={local_secret}\n", encoding="utf-8")
    server.write_text(f"POLY_SECRET={server_secret}\n", encoding="utf-8")

    rc = cli_main(
        [
            "--local-env",
            str(local),
            "--server-env-file",
            str(server),
            "--key",
            "POLY_SECRET",
            "--json",
        ]
    )

    assert rc == 2
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["status"] == "ERROR"
    assert payload["mismatches"][0]["key"] == "POLY_SECRET"
    assert local_secret not in out
    assert server_secret not in out


def test_env_parity_parser_handles_export_quotes_and_inline_comments(
    tmp_path: Path,
    capsys,
) -> None:
    local = tmp_path / "local.env"
    server = tmp_path / "server.env"
    local.write_text("export CRYPTOBOT_SPOT_SOURCE='binance' # active\n", encoding="utf-8")
    server.write_text('CRYPTOBOT_SPOT_SOURCE="binance"\n', encoding="utf-8")

    rc = cli_main(
        [
            "--local-env",
            str(local),
            "--server-env-file",
            str(server),
            "--key",
            "CRYPTOBOT_SPOT_SOURCE",
        ]
    )

    assert rc == 0
    assert "Env parity: OK" in capsys.readouterr().out
