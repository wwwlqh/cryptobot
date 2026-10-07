"""Switch a wallet between DRY and LIVE from the shell, with an audit event.

  python -m cryptobot.cli.set_wallet_mode \\
      --wallet subminute_5m --target LIVE --reason "smoke test"

Same effect as the dashboard's POST /api/v2/wallet/<id>/mode, but usable over
SSH. ``--force`` skips the DRY -> LIVE cooldown; only this CLI accepts it, the
dashboard endpoint does not.

Exit codes:
  0  switched (or no-op when already in the target mode)
  1  blocked by a guard (readiness stale, operator stopped, ...)
  2  concurrent change (someone else switched it meanwhile)
  3  argument, database or validation error
"""

from __future__ import annotations

import argparse
import logging
import sys

from cryptobot import config
from cryptobot.db import init_db
from cryptobot.execution.operator_control import (
    BlockedError,
    ConcurrentMutationError,
    OperatorControlError,
    RateLimitError,
    get_wallet_mode,
    set_wallet_mode,
)


logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cryptobot.cli.set_wallet_mode",
        description=(
            "Flip a wallet between DRY and LIVE with an audit event. "
            "LIVE → DRY is always allowed; DRY → LIVE requires fresh "
            "readiness + reconciliation rows AND operator state==LIVE. "
            "Pass --force to bypass the DRY → LIVE cooldown only."
        ),
    )
    parser.add_argument(
        "--wallet",
        required=True,
        help="wallet_id to flip (e.g. exp044_5m, exp044_15m)",
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=["DRY", "LIVE", "dry", "live"],
        help="target mode",
    )
    parser.add_argument(
        "--reason",
        default="",
        help="free-text reason for the audit log",
    )
    parser.add_argument(
        "--actor",
        default="cli",
        help="actor recorded on the operator_control_events row "
        "(default: 'cli')",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="bypass the DRY → LIVE rate-limit cooldown",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help=(
            "override CRYPTOBOT_DB_PATH (default: cryptobot's configured "
            "DB path)"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    target = args.target.upper()
    db_path = args.db_path or config.DB_PATH

    try:
        conn = init_db(db_path)
    except Exception as exc:
        logger.error("init_db failed: %s", exc)
        return 3

    try:
        try:
            current = get_wallet_mode(conn, args.wallet)
        except OperatorControlError as exc:
            logger.error("%s", exc)
            return 3

        try:
            new_state = set_wallet_mode(
                conn,
                args.wallet,
                target,
                actor=args.actor,
                reason=args.reason,
                force=args.force,
            )
        except RateLimitError as exc:
            logger.error("rate-limited: %s", exc)
            return 1
        except BlockedError as exc:
            logger.error("blocked (reason=%s): %s", exc.reason, exc)
            return 1
        except ConcurrentMutationError as exc:
            logger.error("concurrent mutation: %s", exc)
            return 2
        except OperatorControlError as exc:
            logger.error("operator control error: %s", exc)
            return 3

        if current.mode == new_state.mode:
            logger.info(
                "wallet=%s already %s; no flip applied", args.wallet, target
            )
        else:
            logger.info(
                "wallet=%s flipped %s → %s (version=%s)",
                args.wallet,
                current.mode,
                new_state.mode,
                new_state.version,
            )
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
