"""Compare local and server env live-critical values safely.

Values are never printed directly. Differences are shown as short SHA-256
fingerprints so operators can identify drift without leaking secrets.

Examples:
    python -m cryptobot.cli.env_parity --server-env-file server.env
    python -m cryptobot.cli.env_parity --ssh-target ec2-user@host --ssh-key polymarket/cryptobot.pem
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping

from cryptobot.cli._exitcodes import NOGO, OK
from cryptobot.db.connection import utc_now_sql


CRITICAL_ENV_KEYS: tuple[str, ...] = (
    "DRY_RUN",
    "POLY_PRIVATE_KEY",
    "POLY_API_KEY",
    "POLY_SECRET",
    "POLY_PASSPHRASE",
    "POLY_SIGNATURE_TYPE",
    "POLY_FUNDER",
    "POLY_RETURN_WALLET",
    "RELAYER_URL",
    "RELAYER_API_KEY",
    "RELAYER_API_KEY_ADDRESS",
    "POLYGON_RPC_URL",
    "LIVE_REQUIRE_DEPOSIT_WALLET",
    "LIVE_ONCHAIN_VERIFY_ENABLED",
    "LIVE_BALANCE_TOLERANCE_USD",
    "WALLET_SNAPSHOT_FRESHNESS_TTL_S",
    "WALLET_SNAPSHOT_INTERVAL_MINUTES",
    "CRYPTOBOT_SHARED_EXPOSURE_CAP",
    "CRYPTOBOT_MAX_DD_HALT_FRAC",
    "CRYPTOBOT_MIN_BET_USD",
    "CRYPTOBOT_ENABLE_TOPUPS",
    "CRYPTOBOT_SPOT_SOURCE",
    "CRYPTOBOT_SWEEP_MINUTES",
    "CRYPTOBOT_SIGNALS_RETENTION_DAYS",
    "CRYPTOBOT_DASHBOARD_HOST",
    "CRYPTOBOT_DASHBOARD_PORT",
    "CRYPTOBOT_ALLOW_REMOTE_DASHBOARD",
    "CRYPTOBOT_DASHBOARD_TLS_TERMINATED",
    "CRYPTOBOT_DASHBOARD_PASS",
)

SECRET_ENV_KEYS: frozenset[str] = frozenset(
    {
        "POLY_PRIVATE_KEY",
        "POLY_API_KEY",
        "POLY_SECRET",
        "POLY_PASSPHRASE",
        "RELAYER_API_KEY",
        "CRYPTOBOT_DASHBOARD_PASS",
        "DASHBOARD_PASS",
        "NOTIFY_TELEGRAM_TOKEN",
    }
)


@dataclass(frozen=True)
class EnvParityReport:
    status: str
    generated_at: str
    compared_keys: list[str]
    mismatches: list[dict[str, str]]
    missing_local: list[str]
    missing_server: list[str]

    @property
    def ok(self) -> bool:
        return self.status == "OK"

    def to_public_dict(self) -> dict[str, object]:
        return asdict(self)


def env_fingerprint(value: str | None) -> str:
    if value is None:
        return "missing"
    normalized = str(value).strip()
    if normalized == "":
        return "blank"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def _strip_inline_comment(value: str) -> str:
    in_single = False
    in_double = False
    for idx, char in enumerate(value):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            if idx == 0 or value[idx - 1].isspace():
                return value[:idx].rstrip()
    return value.strip()


def _parse_env_lines(lines: Iterable[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, value = line.split("=", 1)
        key = key.strip()
        # Secrets (POLY_SECRET/POLY_PASSPHRASE/RELAYER_API_KEY/...) may contain
        # ' #' or be wrapped in characters that the runtime loader keeps. Do NOT
        # comment-strip or quote-strip them: fingerprint the raw assigned bytes
        # (minus surrounding whitespace) so parity reflects the true value and
        # never silently truncates at a '#' or eats outer quote chars.
        if key.upper() in SECRET_ENV_KEYS:
            value = value.strip()
        else:
            value = _strip_inline_comment(value.strip())
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
        if key:
            values[key] = value
    return values


def parse_env_text(text: str) -> dict[str, str]:
    return _parse_env_lines((text or "").splitlines())


def load_env_file(path: str | Path) -> dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}
    return parse_env_text(env_path.read_text(encoding="utf-8"))


def _env_value_for_compare(key: str, value: str | None) -> str | None:
    if value is None:
        return None
    if key.upper() in SECRET_ENV_KEYS:
        return str(value).strip()
    return " ".join(str(value).strip().split())


def compare_env_maps(
    local_env: Mapping[str, str],
    server_env: Mapping[str, str],
    *,
    keys: Iterable[str] = CRITICAL_ENV_KEYS,
    ignore: Iterable[str] = (),
) -> EnvParityReport:
    ignore_set = {key.upper() for key in ignore}
    compared = [key for key in keys if key.upper() not in ignore_set]
    mismatches: list[dict[str, str]] = []
    missing_local: list[str] = []
    missing_server: list[str] = []

    for key in compared:
        local_present = key in local_env
        server_present = key in server_env
        if not local_present:
            missing_local.append(key)
        if not server_present:
            missing_server.append(key)
        if not local_present or not server_present:
            continue

        local_value = _env_value_for_compare(key, local_env.get(key))
        server_value = _env_value_for_compare(key, server_env.get(key))
        if local_value != server_value:
            mismatches.append(
                {
                    "key": key,
                    "localFingerprint": env_fingerprint(local_value),
                    "serverFingerprint": env_fingerprint(server_value),
                }
            )

    status = "OK" if not mismatches and not missing_local and not missing_server else "ERROR"
    return EnvParityReport(
        status=status,
        generated_at=utc_now_sql(),
        compared_keys=compared,
        mismatches=mismatches,
        missing_local=missing_local,
        missing_server=missing_server,
    )


def read_server_env_via_ssh(
    *,
    ssh_target: str,
    ssh_key: str | Path,
    server_env_path: str = "/etc/cryptobot.env",
    timeout_s: int = 30,
    known_hosts: str | Path | None = None,
    insecure: bool = False,
) -> str:
    def _quote_remote_path(path: str) -> str:
        if path == "~":
            return "$HOME"
        if path.startswith("~/"):
            return "$HOME/" + shlex.quote(path[2:])
        return shlex.quote(path)

    # This pulls /etc/cryptobot.env, which holds every live secret
    # (POLY_PRIVATE_KEY, POLY_SECRET, POLY_PASSPHRASE, RELAYER_API_KEY, ...).
    # Never auto-accept an unknown host key for that transfer: a first-connect
    # MITM would harvest the cleartext env. Default to a pinned known_hosts
    # when given, otherwise accept-new (TOFU) which still refuses a CHANGED key.
    # `insecure=True` restores the old StrictHostKeyChecking=no behaviour.
    if insecure:
        host_key_opts = ["-o", "StrictHostKeyChecking=no"]
    elif known_hosts is not None:
        host_key_opts = [
            "-o",
            f"UserKnownHostsFile={Path(known_hosts)}",
            "-o",
            "StrictHostKeyChecking=yes",
        ]
    else:
        host_key_opts = ["-o", "StrictHostKeyChecking=accept-new"]

    cmd = [
        "ssh",
        "-i",
        str(Path(ssh_key)),
        *host_key_opts,
        ssh_target,
        f"cat -- {_quote_remote_path(server_env_path)}",
    ]
    completed = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    return completed.stdout


def build_env_parity_report(
    *,
    local_env_path: str | Path = ".env",
    server_env_text: str | None = None,
    keys: Iterable[str] = CRITICAL_ENV_KEYS,
    ignore: Iterable[str] = (),
) -> EnvParityReport:
    local_env = load_env_file(local_env_path)
    if server_env_text is not None:
        server_env = parse_env_text(server_env_text)
    else:
        server_env = {}
    return compare_env_maps(local_env, server_env, keys=keys, ignore=ignore)


def _print_human(report: EnvParityReport) -> None:
    print(f"Env parity: {report.status}")
    print(f"Compared keys: {', '.join(report.compared_keys)}")
    if report.missing_local:
        print(f"Missing locally: {', '.join(report.missing_local)}")
    if report.missing_server:
        print(f"Missing on server: {', '.join(report.missing_server)}")
    if report.mismatches:
        print("Mismatches:")
        for item in report.mismatches:
            print(
                "  "
                f"{item['key']}: local={item['localFingerprint']} "
                f"server={item['serverFingerprint']}"
            )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--local-env", default=".env", help="Local env path.")
    parser.add_argument(
        "--server-env-file",
        help="Already-downloaded server env file to compare against.",
    )
    parser.add_argument("--ssh-target", help="SSH target, for example ec2-user@host.")
    parser.add_argument("--ssh-key", help="SSH private key for --ssh-target.")
    parser.add_argument(
        "--server-env-path",
        default="/etc/cryptobot.env",
        help="Remote env path when using --ssh-target.",
    )
    parser.add_argument(
        "--ssh-timeout",
        type=int,
        default=30,
        help="SSH command timeout in seconds.",
    )
    parser.add_argument(
        "--ssh-known-hosts",
        default=None,
        help=(
            "Pinned known_hosts file for the SSH env fetch. When set, the "
            "host key must match (StrictHostKeyChecking=yes)."
        ),
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help=(
            "Disable SSH host-key verification (StrictHostKeyChecking=no). "
            "Unsafe: this pulls the secret-bearing prod env; only use on a "
            "trusted private network."
        ),
    )
    parser.add_argument(
        "--key",
        action="append",
        dest="keys",
        help="Restrict comparison to this key. Repeatable.",
    )
    parser.add_argument(
        "--ignore",
        action="append",
        default=[],
        help="Ignore this key. Repeatable.",
    )
    parser.add_argument(
        "--all-keys",
        action="store_true",
        help="Compare the full union of local and server env keys.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.server_env_file:
        server_env_text = Path(args.server_env_file).read_text(encoding="utf-8")
    elif args.ssh_target:
        if not args.ssh_key:
            raise SystemExit("--ssh-key is required with --ssh-target")
        server_env_text = read_server_env_via_ssh(
            ssh_target=args.ssh_target,
            ssh_key=args.ssh_key,
            server_env_path=args.server_env_path,
            timeout_s=args.ssh_timeout,
            known_hosts=args.ssh_known_hosts,
            insecure=args.insecure,
        )
    else:
        raise SystemExit("Provide --server-env-file or --ssh-target/--ssh-key.")

    if args.keys:
        compare_keys = args.keys
    elif args.all_keys:
        local_env_map = load_env_file(args.local_env)
        server_env_map = parse_env_text(server_env_text)
        compare_keys = sorted(set(local_env_map.keys()) | set(server_env_map.keys()))
    else:
        compare_keys = CRITICAL_ENV_KEYS

    report = build_env_parity_report(
        local_env_path=args.local_env,
        server_env_text=server_env_text,
        keys=compare_keys,
        ignore=args.ignore,
    )

    if args.json:
        print(json.dumps(report.to_public_dict(), indent=2, sort_keys=True))
    else:
        _print_human(report)
    return OK if report.ok else NOGO


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
