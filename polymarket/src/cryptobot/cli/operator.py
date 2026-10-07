"""Operator CLI for live controls.

The shell counterpart to the dashboard's operator controls. It uses the same
functions for global state and transfers, so audit rows and transfer checks are
identical either way.

Examples:
    python -m cryptobot.cli.operator status
    python -m cryptobot.cli.operator stop --reason "manual halt"
    python -m cryptobot.cli.operator start --reason "resume"
    python -m cryptobot.cli.operator transfer-preview --amount 5 --source-wallet-id subminute_5m
    python -m cryptobot.cli.operator transfer-submit --idempotency-key <key> --i-have-read-the-preview
    python -m cryptobot.cli.operator record-flow --kind deposit --amount 91.65 --at "2026-07-26 06:10:00"
    python -m cryptobot.cli.operator list-flows
    python -m cryptobot.cli.operator propose-flows --read-only --db-path snap.db
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import inspect
import json
import sqlite3
import sys
from collections.abc import Callable
from typing import Any

from cryptobot import config as path_config
from cryptobot.cli._exitcodes import NOGO, OK, UNEXPECTED, USAGE
from cryptobot.db import init_db
from cryptobot.execution.capital import CAPITAL_FLOW_KINDS
from cryptobot.execution.capital_flow_backfill import FLOW_RESIDUAL_MIN_USD
from cryptobot.runtime_secrets import get_config


_RELAYER_BUILDER_NAMES: tuple[str, ...] = (
    "build_transfer_submitter",
    "build_pusd_transfer_submitter",
)


class _RelayerNotConfigured(RuntimeError):
    """Raised when no production transfer submitter is available."""


def _print_json(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True, default=str))
    sys.stdout.write("\n")


def _connect(args: argparse.Namespace):
    return init_db(args.db_path or path_config.DB_PATH)


def _actor(args: argparse.Namespace, command: str) -> str:
    return args.actor or f"cli:{command}"


def _call_relayer_builder(builder: Callable[..., Any], cfg: Any) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Call a future polymarket_relayer builder without pinning its signature."""
    try:
        signature = inspect.signature(builder)
    except (TypeError, ValueError):
        submitter = builder(cfg)
    else:
        params = signature.parameters
        if "config" in params:
            submitter = builder(config=cfg)
        elif any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
            submitter = builder(config=cfg)
        elif params:
            submitter = builder(cfg)
        else:
            submitter = builder()
    if not callable(submitter):
        raise _RelayerNotConfigured(
            "relayer builder did not return a callable submitter"
        )
    return submitter


def _build_relayer_submitter(cfg: Any) -> Callable[[dict[str, Any]], dict[str, Any]]:
    try:
        relayer_mod = importlib.import_module(
            "cryptobot_polymarket.relayer"
        )
    except ModuleNotFoundError as exc:
        raise _RelayerNotConfigured(
            "relayer_not_configured: cryptobot_polymarket.relayer "
            "is not available yet"
        ) from exc

    for name in _RELAYER_BUILDER_NAMES:
        builder = getattr(relayer_mod, name, None)
        if callable(builder):
            return _call_relayer_builder(builder, cfg)

    expected = ", ".join(_RELAYER_BUILDER_NAMES)
    raise _RelayerNotConfigured(
        "relayer_not_configured: cryptobot_polymarket.relayer "
        f"does not expose one of: {expected}"
    )


def _cmd_status(args: argparse.Namespace) -> int:
    from cryptobot.execution.operator_control import (
        OperatorControlError,
        get_operator_state,
        load_wallet_mode_state,
    )

    conn = _connect(args)
    try:
        try:
            state = get_operator_state(conn)
            wallet_modes = load_wallet_mode_state(conn)
        except OperatorControlError as exc:
            sys.stderr.write(f"status refused: {exc}\n")
            return NOGO
        _print_json(
            {
                "operator": state.to_public_dict(),
                "walletModes": wallet_modes,
            }
        )
        return OK
    finally:
        conn.close()


def _cmd_set_wallet_mode(args: argparse.Namespace) -> int:
    """Switch one wallet between DRY and LIVE through the audited
    ``operator_control.set_wallet_mode`` (DRY is always allowed; LIVE runs
    the readiness guard and the flip rate limit).
    """
    from cryptobot.execution.operator_control import (
        OperatorControlError,
        RateLimitError,
        set_wallet_mode,
    )

    conn = _connect(args)
    try:
        try:
            state = set_wallet_mode(
                conn,
                args.wallet_id,
                args.target.upper(),
                actor=_actor(args, "set-wallet-mode"),
                reason=args.reason or "operator wallet mode flip (cli)",
                force=bool(args.force),
            )
        except (OperatorControlError, RateLimitError) as exc:
            sys.stderr.write(f"set-wallet-mode refused: {exc}\n")
            return NOGO
        _print_json({"walletMode": dataclasses.asdict(state)})
        return OK
    finally:
        conn.close()


def _cmd_stop(args: argparse.Namespace) -> int:
    from cryptobot.execution.operator_control import (
        ConcurrentMutationError,
        OperatorControlError,
        stop_processing,
    )

    conn = _connect(args)
    try:
        try:
            state = stop_processing(
                conn,
                actor=_actor(args, "stop"),
                reason=args.reason or "operator stop processing (cli)",
                expected_version=args.expected_version,
            )
        except ConcurrentMutationError as exc:
            sys.stderr.write(f"stop refused: {exc}\n")
            return NOGO
        except OperatorControlError as exc:
            sys.stderr.write(f"stop refused: {exc}\n")
            return NOGO
        _print_json(state.to_public_dict())
        return OK
    finally:
        conn.close()


def _cmd_start(args: argparse.Namespace) -> int:
    from cryptobot.execution.operator_control import (
        ConcurrentMutationError,
        OperatorControlError,
        start_processing,
    )

    conn = _connect(args)
    try:
        try:
            state = start_processing(
                conn,
                actor=_actor(args, "start"),
                reason=args.reason or "operator start processing (cli)",
                expected_version=args.expected_version,
            )
        except ConcurrentMutationError as exc:
            sys.stderr.write(f"start refused: {exc}\n")
            return NOGO
        except OperatorControlError as exc:
            sys.stderr.write(f"start refused: {exc}\n")
            return NOGO
        _print_json(state.to_public_dict())
        return OK
    finally:
        conn.close()


def _cmd_transfer_preview(args: argparse.Namespace) -> int:
    from cryptobot_polymarket.transfer import (
        TransferError,
        preview_transfer,
    )

    cfg = get_config()
    from_wallet = args.from_wallet or (cfg.poly_funder or "").strip()
    to_wallet = args.to_wallet or (cfg.poly_return_wallet or "").strip()
    conn = _connect(args)
    try:
        try:
            preview = preview_transfer(
                conn,
                from_wallet=from_wallet,
                to_wallet=to_wallet,
                amount_usd=args.amount_usd,
                source_wallet_id=args.source_wallet_id,
                actor=_actor(args, "transfer-preview"),
                current_balance_usd=args.current_balance_usd,
                to_wallet_allowlist=(cfg.poly_return_wallet or "").strip(),
            )
        except TransferError as exc:
            sys.stderr.write(f"transfer-preview refused ({exc.reason}): {exc}\n")
            return NOGO
        _print_json(preview.to_public_dict())
        return OK
    finally:
        conn.close()


def _cmd_transfer_submit(args: argparse.Namespace) -> int:
    if not args.i_have_read_the_preview:
        sys.stderr.write(
            "transfer-submit requires --i-have-read-the-preview. "
            "Run transfer-preview first and verify the idempotency key, "
            "amount, source wallet, and destination before submitting.\n"
        )
        return USAGE

    from cryptobot_polymarket.transfer import (
        TransferError,
        submit_transfer,
    )

    cfg = get_config()
    try:
        relayer_submitter = _build_relayer_submitter(cfg)
    except _RelayerNotConfigured as exc:
        sys.stderr.write(f"{exc}\n")
        return NOGO

    conn = _connect(args)
    try:
        try:
            result = submit_transfer(
                conn,
                idempotency_key=args.idempotency_key,
                relayer_submitter=relayer_submitter,
                actor=_actor(args, "transfer-submit"),
                to_wallet_allowlist=(cfg.poly_return_wallet or "").strip(),
            )
        except TransferError as exc:
            sys.stderr.write(f"transfer-submit refused ({exc.reason}): {exc}\n")
            return NOGO
        except Exception as exc:  # noqa: BLE001 - CLI should surface the fault
            sys.stderr.write(
                f"transfer-submit failed: {type(exc).__name__}: {exc}\n"
            )
            return UNEXPECTED
        _print_json(
            {
                "requestId": result.request_id,
                "status": result.status,
                "txHash": result.tx_hash,
                "relayerTxId": result.relayer_tx_id,
                "error": result.error,
            }
        )
        return OK
    finally:
        conn.close()


def _cmd_record_flow(args: argparse.Namespace) -> int:
    """Record one account-level cash event so the equity replay (and with it
    the DD halt / brake / stake sizing) moves at the flow's own timestamp."""
    from cryptobot.execution.capital import CapitalFlowError, record_capital_flow

    conn = _connect(args)
    try:
        try:
            flow = record_capital_flow(
                conn,
                kind=args.kind,
                amount_usd=args.amount_usd,
                occurred_at=args.occurred_at,
                note=args.note or None,
                source=args.source,
                force=bool(args.force),
            )
        except CapitalFlowError as exc:
            sys.stderr.write(f"record-flow refused ({exc.reason}): {exc}\n")
            return NOGO
        _print_json({"capitalFlow": flow})
        return OK
    finally:
        conn.close()


def _cmd_list_flows(args: argparse.Namespace) -> int:
    from cryptobot.execution.capital import list_capital_flows

    conn = _connect(args)
    try:
        flows = list_capital_flows(conn)
        if not flows:
            sys.stdout.write("no capital flows recorded\n")
            return OK
        sys.stdout.write(
            f"{'id':>4}  {'occurred_at (UTC)':<19}  {'kind':<10} "
            f"{'amount':>10}  source    note\n"
        )
        for f in flows:
            sys.stdout.write(
                f"{f['id']:>4}  {f['occurred_at']:<19}  {f['kind']:<10} "
                f"{f['amount_usd']:>+10.2f}  {str(f['source'] or ''):<8}  "
                f"{f['note'] or ''}\n"
            )
        net = sum(f["amount_usd"] for f in flows)
        sys.stdout.write(f"{'':>4}  {'net':<19}  {'':<10} {net:>+10.2f}\n")
        return OK
    finally:
        conn.close()


def _cmd_propose_flows(args: argparse.Namespace) -> int:
    """Read-only backfill assist: print `record-flow` lines for unexplained
    balance moves. NEVER inserts — the timestamps are ~10-minute midpoints."""
    from cryptobot.execution.capital_flow_backfill import (
        format_proposals,
        propose_capital_flows,
    )

    db_path = str(args.db_path or path_config.DB_PATH)
    if args.read_only:
        # Attach without the schema bootstrap so a snapshot/forensic copy is
        # never migrated (or WAL-touched) just to be read.
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    else:
        conn = init_db(db_path)
    try:
        try:
            proposals, stats = propose_capital_flows(
                conn,
                min_residual_usd=args.min_residual_usd,
                since=args.since or None,
            )
        except sqlite3.OperationalError as exc:
            sys.stderr.write(f"propose-flows refused: {exc}\n")
            return NOGO
        sys.stdout.write(
            format_proposals(
                proposals,
                stats,
                min_residual_usd=args.min_residual_usd,
                db_path=db_path,
            )
        )
        sys.stdout.write("\n")
        return OK
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cryptobot.cli.operator",
        description="Shell-side operator surface for cryptobot live state.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--db-path",
        default=None,
        help="override CRYPTOBOT_DB_PATH for this command",
    )
    common.add_argument(
        "--actor",
        default=None,
        help="actor recorded in audit rows (default: cli:<subcommand>)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser(
        "status", parents=[common], help="Print operator and wallet mode state."
    )
    p_status.set_defaults(func=_cmd_status)

    p_start = sub.add_parser(
        "start", parents=[common], help="Set global state to LIVE."
    )
    p_start.add_argument("--reason", default="")
    p_start.add_argument("--expected-version", type=int, default=None)
    p_start.set_defaults(func=_cmd_start)

    p_stop = sub.add_parser(
        "stop", parents=[common], help="Set global state to STOPPED_PROCESSING."
    )
    p_stop.add_argument("--reason", default="")
    p_stop.add_argument("--expected-version", type=int, default=None)
    p_stop.set_defaults(func=_cmd_stop)

    p_wmode = sub.add_parser(
        "set-wallet-mode",
        parents=[common],
        help="Set one wallet/sleeve to DRY or LIVE (audited; DRY always allowed).",
    )
    p_wmode.add_argument("wallet_id", help="sleeve wallet id, e.g. subminute_15m")
    p_wmode.add_argument("target", choices=("DRY", "LIVE", "dry", "live"))
    p_wmode.add_argument("--reason", default="")
    p_wmode.add_argument(
        "--force", action="store_true",
        help="bypass the wallet-mode flip rate limit",
    )
    p_wmode.set_defaults(func=_cmd_set_wallet_mode)

    p_preview = sub.add_parser(
        "transfer-preview",
        parents=[common],
        help="Stage a read-only per-wallet realized-PnL transfer preview.",
    )
    p_preview.add_argument(
        "--amount",
        "--amount-usd",
        dest="amount_usd",
        type=float,
        required=True,
        help="transfer amount in pUSD",
    )
    p_preview.add_argument(
        "--from-wallet",
        default="",
        help="source wallet address (default: POLY_FUNDER)",
    )
    p_preview.add_argument(
        "--to-wallet",
        default="",
        help="destination address (default: POLY_RETURN_WALLET)",
    )
    p_preview.add_argument(
        "--source-wallet-id",
        default=None,
        help="strategy wallet whose extractable PnL funds this transfer",
    )
    p_preview.add_argument(
        "--current-balance-usd",
        type=float,
        default=None,
        help="optional current pUSD balance for post-balance display",
    )
    p_preview.set_defaults(func=_cmd_transfer_preview)

    p_submit = sub.add_parser(
        "transfer-submit",
        parents=[common],
        help="Submit a staged transfer preview through the relayer.",
    )
    p_submit.add_argument(
        "--idempotency-key",
        required=True,
        help="idempotency key returned by transfer-preview",
    )
    p_submit.add_argument(
        "--i-have-read-the-preview",
        "--i-have-read-the-confirmation",
        dest="i_have_read_the_preview",
        action="store_true",
        help="required safety acknowledgement before funds move",
    )
    p_submit.set_defaults(func=_cmd_transfer_submit)

    p_flow = sub.add_parser(
        "record-flow",
        parents=[common],
        help="Record a deposit/withdrawal/rebate/adjustment on the account.",
    )
    p_flow.add_argument(
        "--kind", required=True, choices=CAPITAL_FLOW_KINDS,
        help="deposit/rebate are positive, withdrawal negative",
    )
    p_flow.add_argument(
        "--amount", "--amount-usd", dest="amount_usd", type=float, required=True,
        help="SIGNED amount in USD (wrong signs are refused, never flipped)",
    )
    p_flow.add_argument(
        "--at", dest="occurred_at", required=True,
        help='when the cash moved, UTC "YYYY-MM-DD HH:MM:SS"',
    )
    p_flow.add_argument("--note", default="")
    p_flow.add_argument(
        "--source", default="manual",
        help="provenance tag (default: manual)",
    )
    p_flow.add_argument(
        "--force", action="store_true",
        help="record even when an identical kind+amount+timestamp exists",
    )
    p_flow.set_defaults(func=_cmd_record_flow)

    p_flows = sub.add_parser(
        "list-flows", parents=[common],
        help="Print recorded capital flows oldest-first with a net total.",
    )
    p_flows.set_defaults(func=_cmd_list_flows)

    p_propose = sub.add_parser(
        "propose-flows",
        parents=[common],
        help="Read-only: propose record-flow lines from bankroll_peak residuals.",
    )
    p_propose.add_argument(
        "--min-residual", dest="min_residual_usd", type=float,
        default=FLOW_RESIDUAL_MIN_USD,
        help=f"ignore residuals under this many USD (default: {FLOW_RESIDUAL_MIN_USD})",
    )
    p_propose.add_argument(
        "--since", default="",
        help='only scan samples at/after this UTC "YYYY-MM-DD HH:MM:SS"',
    )
    p_propose.add_argument(
        "--read-only", action="store_true",
        help="open the DB read-only (no schema bootstrap) — for snapshot copies",
    )
    p_propose.set_defaults(func=_cmd_propose_flows)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - final CLI fault boundary
        sys.stderr.write(f"unexpected error: {type(exc).__name__}: {exc}\n")
        return UNEXPECTED


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
