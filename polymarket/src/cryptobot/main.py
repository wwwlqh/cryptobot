"""Live scanner process entry point.

Boot sequence:

   1. Load config and parse the command line
   2. Open the database and apply the schema (idempotent)
   3. Validate the wallet setup and relayer URL
   4. Load the strategy sleeves and hash the config
   5. Refuse an unsafe dashboard bind (non-loopback needs explicit opt-in,
      TLS and a password)
   6. Settle orphan PENDING rows left by a previous run
   7. Load the TWAP model (boot fails if it is missing or mismatched) and
      start the Chainlink TWAP mirror
   8. Build the order client, book sources and background jobs
   9. Start the scheduler (1 Hz tick plus periodic jobs) and the dashboard
  10. Log the boot, then serve until SIGINT/SIGTERM
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from functools import partial
from pathlib import Path
from typing import Callable, Optional

import httpx

# Type-only imports for the string annotations on ``_build_deps_factory``.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cryptobot.strategy.hgb_loader import HgbArtifact
    from cryptobot.strategy.partial_ret import PartialRetEngine

logger = logging.getLogger(__name__)


def _disk_free_mb() -> float:
    """Free disk space in MB, for the disk gate."""
    import shutil
    try:
        usage = shutil.disk_usage(".")
        return usage.free / (1024 * 1024)
    except OSError:
        return 0.0


def _build_deps_factory(
    partial_ret_engine: "PartialRetEngine",
    config_sha256: str,
    *,
    spot_client: httpx.Client,
    gamma_client: httpx.Client,
    clob_client: httpx.Client,
    order_client: Optional[object] = None,
    notify: Optional[Callable[..., None]] = None,
    twap_hgb: Optional["HgbArtifact"] = None,
    twap_mirror: Optional[object] = None,
    live_balance_fetcher: Optional[Callable[[], Optional[float]]] = None,
    book_prefetch: Optional[object] = None,
    book_shadow: Optional[object] = None,
    conn_factory: Optional[Callable[[], sqlite3.Connection]] = None,
) -> Callable[[], "TickDeps"]:
    """Return a closure that builds fresh TickDeps for each tick.

    The persistent ``httpx.Client`` objects are passed in so every call reuses
    one TCP+TLS connection per host; without reuse a tick could pay ~20 TLS
    handshakes and miss its 900ms soft deadline.
    """
    from cryptobot import config
    from cryptobot.execution.pipeline import TickDeps

    # Market discovery and order books come from the venue adapter.
    from cryptobot.venues import get_adapter

    _adapter = get_adapter(config.VENUE)
    _fetch_market_impl = _adapter.fetch_active_market
    _fetch_book_impl = _adapter.fetch_top_book
    _fetch_books_impl = _adapter.fetch_books_batch

    if config.SPOT_SOURCE == "binance":
        from cryptobot.data.binance import fetch_all_spots as _fetch_spots_impl
        from cryptobot.data.binance import peek_all_spots as _peek_spots_impl
    else:
        raise ValueError(f"unsupported CRYPTOBOT_SPOT_SOURCE={config.SPOT_SOURCE!r}")

    fetch_spots = partial(_fetch_spots_impl, client=spot_client)
    fetch_market = partial(_fetch_market_impl, client=gamma_client)
    fetch_book = partial(_fetch_book_impl, client=clob_client)
    # Batched book fetch: one round trip per tick for every cell's tokens.
    fetch_books = partial(_fetch_books_impl, client=clob_client)

    def factory() -> TickDeps:
        return TickDeps(
            partial_ret_engine=partial_ret_engine,
            config_sha256=config_sha256,
            fetch_spots=fetch_spots,
            # Cache-only spot read for the fast-entry scans (no client bound,
            # so it can never fetch).
            peek_spots=_peek_spots_impl,
            fetch_market=fetch_market,
            fetch_book=fetch_book,
            fetch_books=fetch_books,
            get_disk_free_mb=_disk_free_mb,
            order_client=order_client,
            notify=notify,
            twap_hgb_artifact=twap_hgb,
            twap_mirror=twap_mirror,
            # Cached balance: no network call on the tick path.
            live_balance_fetcher=live_balance_fetcher,
            # Book read-ahead chain; None = inline fetch only.
            book_prefetch=book_prefetch,
            # WebSocket-vs-REST comparator; None = off.
            book_shadow=book_shadow,
            # Per-thread DB handle for the parallel market lookups.
            conn_factory=conn_factory,
        )
    return factory


def _build_sweep_job(
    conn_factory: Callable[[], sqlite3.Connection],
    portfolio,
    *,
    gamma_client: httpx.Client,
) -> Callable[[], None]:
    """Closure that runs the resolution sweep on every scheduler fire.

    Settlement uses Gamma's final ``outcomePrices``; rows stay PENDING until
    the market is decisively resolved. The sweep uses its own connection from
    ``conn_factory`` (one per scheduler thread), so its writes can never end
    the tick's open transaction; SQLite's WAL lock and busy_timeout serialize
    the two writers.
    """
    from cryptobot import config
    from cryptobot.scheduler.resolution_sweep import run_resolution_sweep

    # Settlement reads Gamma's final outcomePrices; rows stay PENDING until
    # the market is decisively resolved.
    from cryptobot.venues import get_adapter

    _adapter = get_adapter(config.VENUE)
    fetch_resolved_outcome = _adapter.fetch_resolved_outcome

    def _polymarket_resolution(coin: str, interval_min: int, window_start_ts: int):
        return fetch_resolved_outcome(
            coin,
            interval_min=int(interval_min),
            window_start_ts=int(window_start_ts),
            client=gamma_client,
        )

    def _sweep() -> None:
        try:
            counters = run_resolution_sweep(
                conn_factory(),
                polymarket_resolution_fetch=_polymarket_resolution,
                chainlink_fetch=None,
                reference_spot_fetch=None,
            )
        except Exception:
            logger.exception("resolution_sweep raised")
            return
        if any(counters.values()):
            logger.info("resolution_sweep %s", counters)

    return _sweep


def _log_boot(conn: sqlite3.Connection, *, sha: str, bankroll: float, host: str, port: int) -> None:
    from cryptobot import config

    conn.execute(
        "INSERT INTO pipeline_health (stage, status, message) VALUES "
        "(?, ?, ?)",
        (
            "boot",
            "OK",
            f"config_sha256={sha[:12]} live_bankroll={config.INITIAL_BANKROLL:.2f} "
            f"artifact_bankroll={bankroll:.2f} "
            f"spot_source={config.SPOT_SOURCE} bind={host}:{port}",
            ),
        )


def _warm_start_partial_ret_from_binance(
    conn: sqlite3.Connection,
    partial_ret_engine: "PartialRetEngine",
    *,
    spot_client: httpx.Client,
) -> bool:
    """Seed the PartialRetEngine from Binance 1m klines so a restart can score
    immediately.
    """
    from cryptobot import config
    from cryptobot.data.binance import fetch_recent_minute_bars
    from cryptobot.strategy.partial_ret import MinuteBar, _MIN_RV_1H_SAMPLES

    if config.SPOT_SOURCE != "binance":
        return False

    now_ts = int(time.time())
    loaded: dict[str, int] = {}
    for coin in config.COINS:
        try:
            bars = fetch_recent_minute_bars(
                coin,
                limit=120,
                now_ts=now_ts,
                client=spot_client,
            )
            minute_bars = [
                MinuteBar(
                    start_ts=b.start_ts,
                    open=b.open,
                    close=b.close,
                    is_closed=b.is_closed,
                )
                for b in bars
            ]
            loaded[coin] = partial_ret_engine.bootstrap_from_minute_bars(
                coin,
                minute_bars,
                now_ts=now_ts,
            )
        except Exception as exc:  # noqa: BLE001 - degrade to organic warmup
            logger.warning("partial_ret bootstrap failed coin=%s err=%r", coin, exc)
            loaded[coin] = 0

    ready = [coin for coin, n in loaded.items() if n >= _MIN_RV_1H_SAMPLES]
    status = "OK" if len(ready) == len(config.COINS) else "WARN"
    conn.execute(
        "INSERT INTO pipeline_health (stage, status, message) VALUES "
        "(?, ?, ?)",
        (
            "boot",
            status,
            "partial_ret bootstrap from Binance 1m klines: "
            + ", ".join(f"{coin}={loaded.get(coin, 0)}" for coin in config.COINS),
            ),
        )
    if status == "OK":
        logger.info("partial_ret bootstrap OK %s", loaded)
    else:
        logger.warning("partial_ret bootstrap incomplete %s", loaded)
    return status == "OK"


def main(argv: list[str] | None = None) -> int:
    """Boot and serve; returns the process exit code."""
    parser = argparse.ArgumentParser(
        description=(
            "cryptobot live scanner. Reads CRYPTOBOT_DB_PATH "
            "(default data/cryptobot.db), CRYPTOBOT_DASHBOARD_HOST "
            "(default 127.0.0.1), CRYPTOBOT_DASHBOARD_PORT (default 8800), "
            "CRYPTOBOT_V6_CONFIG (default backtest/optimize_runs/"
            "v2_step1last_live.json). Set CRYPTOBOT_ALLOW_REMOTE_DASHBOARD=1 "
            "only if you have authenticated the SSE stream externally."
        )
    )
    parser.add_argument("--tick-seconds", type=int, default=None, help="override TICK_SECONDS")
    parser.add_argument("--no-sse", action="store_true", help="skip starting the SSE server")
    parser.add_argument("--once", action="store_true", help="run one tick + exit (smoke)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from cryptobot import config
    from cryptobot.db import backfill_wallet_groups, init_db
    from cryptobot.db.connection import (
        ThreadLocalConnections,
        get_connection,
        passive_wal_checkpoint,
    )
    from cryptobot.execution.balance_cache import BalanceCache
    from cryptobot.execution.notify import NotifyDispatcher, send_event
    from cryptobot.execution.operator_control import boot_operator_state
    from cryptobot.execution.wallet_topology import _validate_wallet_config
    from cryptobot.observability.snapshot_builder import build_payload
    from cryptobot.observability.sse_server import (
        Broadcaster,
        PostRouter,
        derive_session_token,
        start_server,
    )
    from cryptobot.reconciliation.orphan_pending import reconcile_orphan_pending
    from cryptobot.runtime_secrets import get_config
    from cryptobot.scheduler.tick import TickRunner, start_scheduler
    from cryptobot.strategy.partial_ret import PartialRetEngine
    from cryptobot.strategy.portfolio_loader import load_sleeves

    # Step 2: database. The schema is applied once on this thread. Every other
    # thread (tick, scheduler jobs, dashboard) gets its own connection through
    # ``conns``; an in-memory DB (tests) shares the one connection, since a
    # second ``:memory:`` connection would be empty.
    boot_conn = init_db(config.DB_PATH)
    if str(config.DB_PATH) == ":memory:":
        conns = ThreadLocalConnections(config.DB_PATH, shared=boot_conn)
    else:
        conns = ThreadLocalConnections(config.DB_PATH)
        conns.register_current_thread(boot_conn)
    conn = conns.get()

    # Wallet setup check: signature type 3 (deposit wallet) without POLY_FUNDER
    # would sign the first live order against the wrong address, so boot fails.
    runtime_cfg = get_config()
    try:
        _validate_wallet_config(
            runtime_cfg.poly_signature_type, runtime_cfg.poly_funder
        )
    except ValueError as exc:
        msg = (
            f"poly_wallet_topology invalid: {exc}. "
            "Set POLY_SIGNATURE_TYPE + POLY_FUNDER in /etc/cryptobot.env "
            "and restart."
        )
        try:
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('boot', 'CRITICAL', ?)",
                (msg,),
            )
            conn.commit()
        except sqlite3.DatabaseError:
            # Boot still fails even if the audit row cannot be written.
            pass
        logger.error(msg)
        conns.close_all()
        return 1

    # An unknown RELAYER_URL refuses boot unless
    # ``CRYPTOBOT_RELAYER_URL_OVERRIDE=1`` acknowledges it, so a typo or
    # look-alike URL cannot slip through.
    if not runtime_cfg.relayer_url_is_known():
        warn_msg = (
            f"relayer_url={runtime_cfg.relayer_url!r} does not start with a "
            "known Polymarket relayer prefix; LIVE transfer/redeem may "
            "target an unknown endpoint. Verify with the Polymarket docs."
        )
        try:
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('boot', 'CRITICAL', ?)",
                (warn_msg,),
            )
            conn.commit()
        except sqlite3.DatabaseError:
            pass
        logger.critical(warn_msg)
        if os.environ.get("CRYPTOBOT_RELAYER_URL_OVERRIDE", "0") != "1":
            logger.error(
                "relayer_url is not a known Polymarket prefix and "
                "CRYPTOBOT_RELAYER_URL_OVERRIDE=1 was not set. Refusing "
                "to boot. Set the override env var only after verifying "
                "the URL against the Polymarket docs."
            )
            conns.close_all()
            return 1

    # Step 3: portfolio and config hash.
    portfolio = load_sleeves()
    sha = portfolio.config_sha256

    # Portfolio-dependent backfill and seed (wallet groups, default wallet
    # modes). Idempotent.
    backfill_counts = backfill_wallet_groups(conn, portfolio)
    op_state, wallet_modes = boot_operator_state(
        conn, portfolio, boot_dry_run=runtime_cfg.dry_run
    )
    if backfill_counts["ledger"] or backfill_counts["signals"]:
        logger.info(
            "backfill_wallet_groups stamped ledger=%d signals=%d rows",
            backfill_counts["ledger"],
            backfill_counts["signals"],
        )
    logger.info(
        "operator_control_state state=%s wallet_modes=%s",
        op_state.state,
        wallet_modes,
    )

    # Step 4: live-mode safety. The dashboard's read stream is protected by its
    # loopback bind, so a non-loopback bind needs
    # ``CRYPTOBOT_ALLOW_REMOTE_DASHBOARD=1``.
    loopback_hosts = {"127.0.0.1", "::1", "localhost"}
    bind_is_loopback = config.DASHBOARD_BIND_HOST in loopback_hosts
    if not bind_is_loopback:
        if not config.ALLOW_REMOTE_DASHBOARD:
            logger.error(
                "DASHBOARD_BIND_HOST=%s is non-loopback but "
                "CRYPTOBOT_ALLOW_REMOTE_DASHBOARD is not set. Refusing to "
                "expose unauthenticated SSE stream. Set the env var to '1' "
                "to override after placing the dashboard behind auth.",
                config.DASHBOARD_BIND_HOST,
            )
            conns.close_all()
            return 1
        # A remote bind also needs TLS
        # (``CRYPTOBOT_DASHBOARD_TLS_TERMINATED=1``): the operator session
        # cookie authorizes wallet-mode changes, stop/start and transfers, and
        # without TLS it would travel in cleartext.
        if not config.DASHBOARD_TLS_TERMINATED:
            logger.error(
                "DASHBOARD_BIND_HOST=%s is non-loopback and "
                "CRYPTOBOT_ALLOW_REMOTE_DASHBOARD=1, but "
                "CRYPTOBOT_DASHBOARD_TLS_TERMINATED is not set. Refusing to "
                "boot: the operator-control session cookie would travel in "
                "cleartext and without the Secure flag, where any network "
                "observer could capture and replay it. Terminate TLS in front "
                "of the dashboard and set CRYPTOBOT_DASHBOARD_TLS_TERMINATED=1.",
                config.DASHBOARD_BIND_HOST,
            )
            conns.close_all()
            return 1
        logger.warning(
            "DASHBOARD_BIND_HOST=%s (non-loopback) — operator-acknowledged "
            "via CRYPTOBOT_ALLOW_REMOTE_DASHBOARD=1 with TLS termination",
            config.DASHBOARD_BIND_HOST,
        )

    # Refuse to boot with an empty dashboard password unless auth is explicitly
    # disabled, and never allow disabled auth on a non-loopback bind.
    dashboard_pass = os.environ.get("CRYPTOBOT_DASHBOARD_PASS", "")
    auth_disabled = (
        os.environ.get("CRYPTOBOT_DASHBOARD_AUTH_DISABLED", "0") == "1"
    )
    if not args.no_sse:
        if auth_disabled and not bind_is_loopback:
            logger.error(
                "CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1 is only valid on a "
                "loopback bind (current bind: %s). Refusing to expose "
                "unauthenticated /api/v2 mutation endpoints to the network. "
                "Either set CRYPTOBOT_DASHBOARD_PASS or bind to loopback.",
                config.DASHBOARD_BIND_HOST,
            )
            conns.close_all()
            return 1
        if not dashboard_pass and not auth_disabled:
            logger.error(
                "CRYPTOBOT_DASHBOARD_PASS is empty. /api/v2 mutation endpoints "
                "(wallet mode flip, operator start/stop, transfers) would "
                "accept every request unauthenticated. Refusing to boot. "
                "Set CRYPTOBOT_DASHBOARD_PASS to a long random string, or set "
                "CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1 for local-loopback "
                "development."
            )
            conns.close_all()
            return 1
        if auth_disabled:
            logger.warning(
                "CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1 — /api/v2 endpoints "
                "accept every request without a session cookie. Loopback "
                "bind only."
            )

    # The window-open anchor needs the previous tick to be within 5 seconds of
    # the boundary, so a tick interval above 5s would silently reintroduce a
    # timing bias. Validate the effective interval (``--tick-seconds``
    # overrides the config) and refuse anything outside 1-5 seconds.
    effective_tick = (
        args.tick_seconds if args.tick_seconds is not None else config.TICK_SECONDS
    )
    if effective_tick > 5 or effective_tick < 1:
        logger.error(
            "effective tick cadence=%d (the lower of --tick-seconds and "
            "CRYPTOBOT_TICK_SECONDS) is outside [1,5]; the previous-tick "
            "window-start anchor in PartialRetEngine (divergence #4 fix) "
            "requires sub-5s tick cadence. Refusing to boot. Set the cadence "
            "to 1-5s (--tick-seconds and CRYPTOBOT_TICK_SECONDS).",
            effective_tick,
        )
        conns.close_all()
        return 1

    # Step 5: orphan reconcile, without network resolvers (the HTTP clients do
    # not exist yet); unresolved rows stay PENDING for the scheduled sweep. A
    # DB error here is logged and boot continues.
    try:
        reconcile_orphan_pending(conn)
    except (sqlite3.DatabaseError, sqlite3.OperationalError) as exc:
        logger.warning(
            "orphan_reconcile_failed: %s — boot continuing; orphan PENDING "
            "rows will retry on the next F3 sweep",
            exc,
        )

    # Step 6: model (boot-blocking).
    #
    # The sleeves' selection settings (threshold, ask band, book consumption,
    # side filter) live in the config, not the model file, so check them
    # against the model's manifest and refuse to boot on a mismatch.
    from cryptobot.strategy.twap_loader import (
        load_twap_symmetric_model,
        validate_twap_coin_universe,
        validate_twap_sleeves,
    )

    twap_manifest = json.loads(
        Path(config.TWAP_SYMMETRIC_MANIFEST_PATH).read_text(encoding="utf-8")
    )
    validate_twap_sleeves(portfolio.sleeves, twap_manifest)
    live_contract = json.loads(Path(config.V6_CONFIG_PATH).read_text(encoding="utf-8"))
    configured_coins = {
        str(coin).upper() for coin in live_contract.get("coin_universe", [])
    }
    validate_twap_coin_universe(config.COINS, configured_coins, twap_manifest)
    twap_hgb = load_twap_symmetric_model()
    logger.info("TWAP model loaded for both intervals")

    partial_ret_engine = PartialRetEngine()
    # Chainlink TWAP mirror: the opening price each window settles against.
    from cryptobot_polymarket.price_to_beat import fetch_price_to_beat
    from cryptobot_polymarket.twap_mirror import ChainlinkTwapRtdsMirror

    twap_mirror = ChainlinkTwapRtdsMirror(
        url=config.TWAP_RTDS_URL,
        opening_price_fetcher=fetch_price_to_beat,
    )
    twap_mirror.start()
    logger.info("Chainlink TWAP mirror started (60s boundary TWAP, Price To Beat fallback)")

    # Persistent HTTP clients, one per host, reused for the whole run and
    # closed at shutdown. The Polymarket read clients use HTTP/2 when the
    # ``h2`` extra is installed (the batched book POST and fallback GETs share
    # one connection). Read timeout is ``CRYPTOBOT_DECISION_READ_TIMEOUT_S``;
    # connect is 2s.
    http_timeout = httpx.Timeout(config.DECISION_READ_TIMEOUT_S, connect=2.0)
    # Keep idle sockets for 65s (httpx defaults to 5s), longer than the 60s job
    # intervals, so spaced-out reads do not each pay a new handshake.
    http_limits = httpx.Limits(
        max_connections=20, max_keepalive_connections=10, keepalive_expiry=65.0
    )

    def _mk_client(*, http2: bool = False) -> httpx.Client:
        if http2:
            try:
                return httpx.Client(
                    timeout=http_timeout, http2=True, limits=http_limits
                )
            except ImportError:
                logger.warning(
                    "httpx[http2] extra (h2) not installed — read client "
                    "falls back to HTTP/1.1"
                )
        return httpx.Client(timeout=http_timeout, limits=http_limits)

    spot_client = _mk_client()
    gamma_client = _mk_client(http2=True)
    clob_client = _mk_client(http2=True)
    partial_ret_bootstrapped = _warm_start_partial_ret_from_binance(
        conn,
        partial_ret_engine,
        spot_client=spot_client,
    )

    # ------------------------------------------------------------------
    # Live execution: order client and scheduler jobs.
    # ------------------------------------------------------------------
    # Without the CLOB SDK or credentials the bot boots DRY-only:
    # ``order_client=None`` makes LIVE wallets skip with ``clob_unavailable``.
    from cryptobot.venues import get_adapter

    order_client: Optional[object] = get_adapter(config.VENUE).build_order_client(
        runtime_cfg=runtime_cfg
    )

    from cryptobot.execution.live_readiness import build_venue_balance_fetcher

    # Cached balance: the tick reads this instead of calling the exchange; the
    # 60s readiness job keeps it fresh.
    balance_cache: Optional[BalanceCache] = None
    if order_client is not None:
        balance_cache = BalanceCache(order_client.check_balance)

    chain_balance_fetcher: Optional[Callable[[], Optional[float]]] = None
    if runtime_cfg.live_onchain_verify_enabled:
        # On-chain Polygon pUSD balance of the funder wallet.
        chain_balance_fetcher = build_venue_balance_fetcher(runtime_cfg)

    transfer_relayer_submitter: Optional[
        Callable[[dict[str, object]], dict[str, object]]
    ] = None
    # Import the relayer names in their own guarded block, so a failed import
    # degrades to DRY instead of a NameError in the except clause below.
    try:
        from cryptobot_polymarket.relayer import (
            RelayerSubmitError,
            build_pusd_transfer_submitter,
            relayer_configured,
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "Polymarket relayer module import failed; dashboard transfer "
            "submit disabled (booting DRY-only relayer surface)"
        )
    else:
        try:
            if relayer_configured(runtime_cfg):
                transfer_relayer_submitter = build_pusd_transfer_submitter(runtime_cfg)
                logger.info(
                    "Polymarket relayer submitters configured (url=%s)",
                    runtime_cfg.relayer_url,
                )
            else:
                logger.info(
                    "Polymarket relayer env incomplete; dashboard transfer submit "
                    "disabled"
                )
        except RelayerSubmitError as exc:
            logger.warning("Polymarket relayer submitter not configured: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("Polymarket relayer submitter setup raised")

    # A LIVE wallet without a relayer can place orders but cannot withdraw
    # pUSD. Record a CRITICAL row so that half-configured state is visible.
    any_wallet_live = any(
        str(m).upper() == "LIVE" for m in wallet_modes.values()
    )
    if any_wallet_live and transfer_relayer_submitter is None:
        relayer_gap_msg = (
            "a wallet is LIVE but the Polymarket relayer is not configured "
            "(RELAYER_API_KEY / RELAYER_API_KEY_ADDRESS / POLY_FUNDER / "
            "POLY_PRIVATE_KEY / RELAYER_URL): pUSD withdrawal "
            "is DISABLED while real-money orders can be placed. Wire the "
            "relayer env or revert the wallet to DRY before going live."
        )
        logger.critical(relayer_gap_msg)
        try:
            conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('boot', 'CRITICAL', ?)",
                (relayer_gap_msg,),
            )
            conn.commit()
        except sqlite3.DatabaseError:
            pass

    # Non-critical notifications go through a background dispatcher so a
    # Telegram request never blocks the tick thread. Critical ones are sent
    # inline. The dispatcher is drained on shutdown.
    notify_dispatcher = NotifyDispatcher()
    notify_dispatcher.start()

    def _notify(event_type: str, message: str, *, wallet_id=None, critical=False) -> None:
        """Send a notification. Non-critical events are queued; critical ones
        are sent inline. Failures are logged, never raised.
        """
        try:
            notify_dispatcher.enqueue(
                event_type, message, wallet_id=wallet_id, critical=critical
            )
        except Exception:  # noqa: BLE001
            logger.warning("notify dispatch failed", exc_info=True)

    # Steps 7-8: scheduler and dashboard server.
    broadcaster: Optional[Broadcaster] = None
    httpd = None
    post_router: Optional[PostRouter] = None
    if not args.no_sse:
        broadcaster = Broadcaster()
        # The dashboard's POST handlers get their own connection, separate from
        # the tick's, so an operator action can never commit or roll back the
        # tick's transaction. PostRouter serializes its handlers internally.
        dashboard_conn = conns.get() if str(config.DB_PATH) == ":memory:" else get_connection(config.DB_PATH)
        if dashboard_conn is not conn:
            conns.register_extra(dashboard_conn)
        post_router = PostRouter(
            conn=dashboard_conn,
            portfolio=portfolio,
            session_token=(
                derive_session_token(dashboard_pass) if dashboard_pass else ""
            ),
            relayer_submitter=transfer_relayer_submitter,
            auth_disabled=auth_disabled,
            transfer_to_wallet_allowlist=runtime_cfg.poly_return_wallet,
        )
        httpd, _ = start_server(broadcaster, post_router=post_router)

    # Snapshot publisher. After each tick, on_tick only sets a latest-wins
    # flag; a daemon thread rebuilds the payload from SQLite on its own
    # connection and broadcasts it. Publishing therefore never delays the next
    # tick, and an older tick can never publish after a newer one.
    state = {
        "max_ledger_id": 0,
        "last_bootstrap_ts": 0.0,
        "last_bootstrap_max": -1,
    }
    publish_lock = threading.Lock()      # guards the pending flag only
    publish_pending = False              # True when a tick awaits publish
    publish_wake = threading.Event()
    publish_stop = threading.Event()

    # Called with the TickResult, which the publisher does not need: the flag
    # only signals that a tick completed.
    def on_tick(_tick_result):
        if broadcaster is None:
            return
        nonlocal publish_pending
        # A burst of ticks collapses to one pending flag.
        with publish_lock:
            publish_pending = True
        publish_wake.set()

    def _publish_latest(tick_conn: sqlite3.Connection) -> None:
        # Cash headline from the cached balance; open positions marked from the
        # cached book source.
        live_balance_get = balance_cache.get if balance_cache is not None else None
        book_mark_get = book_prefetch.get if book_prefetch is not None else None
        last_seen = state["max_ledger_id"]
        last_bootstrap_ts = state["last_bootstrap_ts"]
        last_bootstrap_max = state["last_bootstrap_max"]
        payload, new_max = build_payload(
            tick_conn, portfolio,
            last_seen_ledger_id=last_seen,
            live_balance_get=live_balance_get,
            book_mark_get=book_mark_get,
        )
        state["max_ledger_id"] = new_max
        broadcaster.publish(payload)
        # Rebuild the late-joiner bootstrap payload only when new ledger rows
        # arrived, or at least every 30 seconds.
        now = time.time()
        if new_max == last_bootstrap_max and (now - last_bootstrap_ts) < 30.0:
            return
        if last_seen <= 0:
            # The first publish after boot is already a full snapshot; reuse
            # it.
            bootstrap_payload = payload
        else:
            bootstrap_payload, _ = build_payload(
                tick_conn, portfolio,
                last_seen_ledger_id=0,
                live_balance_get=live_balance_get,
                book_mark_get=book_mark_get,
            )
        broadcaster.set_bootstrap(bootstrap_payload)
        state["last_bootstrap_ts"] = now
        state["last_bootstrap_max"] = new_max

    def _publisher_loop() -> None:
        # This thread's own connection.
        nonlocal publish_pending
        tick_conn = conns.get()
        while not publish_stop.is_set():
            if not publish_wake.wait(timeout=0.5):
                continue
            publish_wake.clear()
            with publish_lock:
                pending = publish_pending
                publish_pending = False
            if not pending or publish_stop.is_set():
                continue
            try:
                _publish_latest(tick_conn)
            except Exception:  # noqa: BLE001 — publisher must never die mid-run
                logger.exception("sse publisher raised")

    publisher_thread: Optional[threading.Thread] = None
    if broadcaster is not None:
        # Daemon thread so a hung build can never block process exit.
        publisher_thread = threading.Thread(
            target=_publisher_loop,
            name="cryptobot-sse-publisher",
            daemon=True,
        )
        publisher_thread.start()

    def _stop_publisher() -> None:
        """Signal and briefly join the publisher before connections are closed.
        """
        if publisher_thread is None:
            return
        publish_stop.set()
        publish_wake.set()
        publisher_thread.join(timeout=5.0)

    # Book read-ahead sources, in priority order (the first fresh hit wins):
    #
    #  1. WebSocket mirror (``CRYPTOBOT_BOOK_WS_ENABLED``): exchange-timestamped
    #     snapshots from the market channel.
    #  2. REST prefetch (``CRYPTOBOT_BOOK_PREFETCH_ENABLED``): re-fetches the
    #     previous tick's tokens just before each second.
    #
    # Anything neither serves fresh falls through to the pipeline's inline
    # fetch.
    book_sources: list = []
    book_shadow = None
    # The authoritative mirror, kept for the stall watchdog below (None in
    # shadow mode or with WS off).
    ws_mirror_authoritative = None
    if config.BOOK_WS_ENABLED:
        _ws_setup = get_adapter(config.VENUE).build_ws_mirror(
            order_client=order_client
        )
        if _ws_setup.shadow is not None:
            # Shadow mode: the mirror runs alongside REST but never decides.
            book_shadow = _ws_setup.shadow
        elif _ws_setup.mirror is not None:
            # Authoritative: first in the chain, ahead of the REST prefetch.
            book_sources.append(_ws_setup.mirror)
            ws_mirror_authoritative = _ws_setup.mirror
    if config.BOOK_PREFETCH_ENABLED:
        # Re-fetch the previous tick's token set just before each second.
        from cryptobot.scheduler.book_prefetch import BookPrefetcher

        book_prefetcher = BookPrefetcher(
            partial(get_adapter(config.VENUE).fetch_books_batch, client=clob_client),
            lead_ms=config.BOOK_PREFETCH_LEAD_MS,
            max_age_ms=config.BOOK_PREFETCH_MAX_AGE_MS,
        )
        book_prefetcher.start()
        book_sources.append(book_prefetcher)
    book_prefetch = None
    if book_sources:
        from cryptobot.scheduler.book_prefetch import BookSourceChain

        book_prefetch = BookSourceChain(book_sources)

    deps_factory = _build_deps_factory(
        partial_ret_engine, sha,
        spot_client=spot_client,
        gamma_client=gamma_client,
        clob_client=clob_client,
        order_client=order_client,
        notify=_notify,
        twap_hgb=twap_hgb,
        twap_mirror=twap_mirror,
        live_balance_fetcher=(
            balance_cache.get if balance_cache is not None else None
        ),
        book_prefetch=book_prefetch,
        book_shadow=book_shadow,
        # Each parallel market lookup gets its own connection.
        conn_factory=conns.get,
    )
    runner = TickRunner(
        conn_factory=conns.get,
        portfolio=portfolio,
        deps_factory=deps_factory,
        on_tick=on_tick,
    )

    # Step 9: boot log.
    _log_boot(conn, sha=sha, bankroll=portfolio.bankroll,
              host=config.DASHBOARD_BIND_HOST, port=config.DASHBOARD_HTTP_PORT)

    # Without the Binance warm start the engine needs ~30 minutes of 1m returns
    # before it can score, and cells skip with no_feature meanwhile. Write one
    # WARN row so that reads as warm-up, not breakage.
    if not partial_ret_bootstrapped:
        from cryptobot.strategy.partial_ret import _MIN_RV_1H_SAMPLES
        conn.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('boot', 'WARN', ?)",
            (
                f"partial_ret warmup: SKIP:no_feature expected on every "
                f"cell for ~{_MIN_RV_1H_SAMPLES} minutes after boot until "
                f"the 1m-return Welford has enough samples",
        ),
    )

    logger.info(
        "boot OK · tick=%ds · components=%d · bind=%s:%d · sha=%s",
        args.tick_seconds or config.TICK_SECONDS,
        len(portfolio.market_sleeves),
        config.DASHBOARD_BIND_HOST,
        config.DASHBOARD_HTTP_PORT,
        sha[:12],
    )
    logger.info("live spot source=%s", config.SPOT_SOURCE)

    if args.once:
        try:
            runner.run_one_tick()
        finally:
            _stop_publisher()
            notify_dispatcher.stop()
            if twap_mirror is not None:
                twap_mirror.stop()
            if httpd is not None:
                httpd.shutdown()
            spot_client.close()
            gamma_client.close()
            clob_client.close()
            conns.close_all()
        return 0

    # Each writer thread has its own connection, and SQLite serializes them.
    # ``db_write_lock`` only serializes the few scheduler jobs that share it.
    db_write_lock = threading.Lock()

    sweep_job = _build_sweep_job(
        conns.get,
        portfolio,
        gamma_client=gamma_client,
    )

    def _on_job_missed(job_id: str) -> None:
        """Record a scheduler fire that APScheduler dropped (a previous run
        still going), so degraded cadence leaves a trail in pipeline_health.
        """
        try:
            conn = conns.get()  # this listener thread's own connection
            with db_write_lock:
                conn.execute(
                    "INSERT INTO pipeline_health (stage, status, message) "
                    "VALUES (?, 'WARN', ?)",
                    (job_id, f"scheduler dropped a fire of job '{job_id}'"),
                )
                conn.commit()
        except Exception:   # noqa: BLE001 — observability path must never crash
            logger.exception("failed to record missed-fire for job_id=%s", job_id)

    # ------------------------------------------------------------------
    # Live-execution jobs, each wired only when what it needs exists, so a
    # DRY-only deploy still boots.
    # ------------------------------------------------------------------
    wallet_recon_job: Optional[Callable[[], None]] = None
    readiness_job: Optional[Callable[[], None]] = None
    order_recon_job: Optional[Callable[[], None]] = None
    transfer_watchdog_job: Optional[Callable[[], None]] = None
    clob_ping_job: Optional[Callable[[], None]] = None

    if order_client is not None:
        from cryptobot.persistence.wallet_reconciliation import WalletReconciler

        wallet_reconciler = WalletReconciler(
            order_client=order_client,
            funder_address=runtime_cfg.poly_funder,
            chain_balance_fetcher=chain_balance_fetcher,
            balance_tolerance_usd=runtime_cfg.live_balance_tolerance_usd,
        )

        def _wallet_recon_job() -> None:
            try:
                # Exchange calls run outside db_write_lock; only the DB writes
                # are serialized.
                conn = conns.get()  # this scheduler-pool thread's own connection
                fetched = wallet_reconciler.fetch_external_state()
                with db_write_lock:
                    wallet_reconciler.persist_run(conn, fetched)
            except Exception:  # noqa: BLE001
                logger.exception("wallet_reconciler.sample raised")
        wallet_recon_job = _wallet_recon_job

        from cryptobot.execution.live_readiness import (
            evaluate_readiness,
            prefetch_readiness_state,
        )

        def _readiness_job() -> None:
            try:
                # The balance check runs outside db_write_lock.
                conn = conns.get()  # this scheduler-pool thread's own connection
                prefetched = prefetch_readiness_state(
                    order_client=order_client,
                    chain_balance_fetcher=chain_balance_fetcher,
                )
                # Feed the tick's balance cache from this fetch.
                if balance_cache is not None:
                    balance_cache.store(prefetched.clob_balance)
                with db_write_lock:
                    evaluate_readiness(
                        conn,
                        config=runtime_cfg,
                        order_client=order_client,
                        portfolio=portfolio,
                        chain_balance_fetcher=chain_balance_fetcher,
                        balance_tolerance_usd=runtime_cfg.live_balance_tolerance_usd,
                        snapshot_freshness_ttl_s=runtime_cfg.wallet_snapshot_freshness_ttl_s,
                        prefetched=prefetched,
                    )
            except Exception:  # noqa: BLE001
                logger.exception("evaluate_readiness raised")
        readiness_job = _readiness_job

        # Reconcile live fills and release stale SUBMITTING reservations every
        # 60s. Without it, LIVE rows never get their realized fill size and
        # price, and a rejected attempt would count against the group cap
        # forever.
        from cryptobot.persistence.reconciliation import (
            reconcile_open_orders,
            reconcile_stale_attempts,
        )

        def _order_recon_job() -> None:
            try:
                conn = conns.get()  # this scheduler-pool thread's own connection
                with db_write_lock:
                    open_counts = reconcile_open_orders(conn, order_client)
                    stale_counts = reconcile_stale_attempts(conn, order_client)
                if any(open_counts.values()) or any(stale_counts.values()):
                    logger.info(
                        "order_reconciliation open=%s stale=%s",
                        open_counts,
                        stale_counts,
                    )
            except Exception:  # noqa: BLE001
                logger.exception("order_reconciliation raised")
        order_recon_job = _order_recon_job

        # Keep the order connection warm. Between orders the signing pool's
        # only traffic is the 60s balance read, close to its 65s idle expiry,
        # so the next order could pay a full handshake. A lightweight GET every
        # ``CLOB_PING_SECONDS`` (20s) prevents that; 0 disables it.
        def _clob_ping_job() -> None:
            try:
                order_client.ping_order_pool()
            except Exception:  # noqa: BLE001 — warmth job must never crash the scheduler
                logger.warning("clob_ping job raised", exc_info=True)
        clob_ping_job = _clob_ping_job

    # The transfer watchdog always runs: it releases TRANSFER_LOCK states older
    # than 15 minutes and needs no order client.
    from cryptobot.execution.operator_control import release_stale_transfer_lock

    def _transfer_watchdog_job() -> None:
        try:
            conn = conns.get()  # this scheduler-pool thread's own connection
            with db_write_lock:
                released = release_stale_transfer_lock(conn)
                if released is not None:
                    logger.warning(
                        "transfer_watchdog released stale lock: %s", released
                    )
                    _notify(
                        "transfer_lock_released",
                        f"transfer_watchdog released stale lock: {released}",
                        critical=True,
                    )
        except Exception:  # noqa: BLE001
            logger.exception("transfer_watchdog raised")
    transfer_watchdog_job = _transfer_watchdog_job

    # Reject telemetry: re-read the book about a second after each rejected
    # order. Observation only; needs only the read client.
    _rr_fetch_book = get_adapter(config.VENUE).fetch_top_book
    from cryptobot.execution.reject_requote import drain_requote_queue

    def _requote_drain_job() -> None:
        try:
            conn = conns.get()  # this scheduler-pool thread's own connection
            # The drain takes db_write_lock only around its batched UPDATE, so
            # a slow book read cannot stall other jobs.
            counters = drain_requote_queue(
                conn,
                fetch_book=partial(
                    _rr_fetch_book, client=clob_client
                ),
                write_lock=db_write_lock,
            )
            if counters.get("updated"):
                logger.info("reject_requote_drain %s", counters)
        except Exception:  # noqa: BLE001
            logger.exception("reject_requote_drain raised")
    requote_drain_job = _requote_drain_job

    # Periodic PASSIVE WAL checkpoint from a scheduler thread. db/connection.py
    # raises SQLite's inline checkpoint threshold so the tick's commits never
    # pay for it; this job drains the WAL instead.
    # ``CRYPTOBOT_WAL_CHECKPOINT_SECONDS`` (default 60, 0 disables).
    def _wal_checkpoint_job() -> None:
        try:
            passive_wal_checkpoint(conns.get())
        except Exception:  # noqa: BLE001 — hygiene job must never crash the scheduler
            logger.warning("wal_checkpoint job raised", exc_info=True)


    # Forward every new CRITICAL pipeline_health row to Telegram, so no
    # recorded issue is silent. send_event rate-limits each event type to one
    # per 30 min.
    _critical_alert_state: dict[str, int] = {}
    try:
        _seed_row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM pipeline_health "
            "WHERE created_at < datetime('now', '-300 seconds')"
        ).fetchone()
        _critical_alert_state["last_id"] = int(_seed_row[0] or 0)
    except sqlite3.DatabaseError:
        _critical_alert_state["last_id"] = 0

    def _critical_alert_job() -> None:
        try:
            job_conn = conns.get()
            last_id = _critical_alert_state.get("last_id", 0)
            rows = job_conn.execute(
                "SELECT id, stage, message FROM pipeline_health "
                "WHERE id > ? AND status = 'CRITICAL' "
                "ORDER BY id LIMIT 20",
                (last_id,),
            ).fetchall()
            max_row = job_conn.execute(
                "SELECT COALESCE(MAX(id), ?) FROM pipeline_health", (last_id,)
            ).fetchone()
            _critical_alert_state["last_id"] = int(max_row[0] or last_id)
            for _rid, stage, message in rows:
                send_event(
                    f"pipeline_critical_{stage}",
                    str(message or "")[:500],
                    critical=False,
                    rate_limit_window_s=1800.0,
                )
        except Exception:  # noqa: BLE001
            logger.warning("critical_alert job raised", exc_info=True)

    # Spot prefetch: a daemon thread warms the spot cache just before each
    # second so the tick's spot read is a cache hit.
    # ``CRYPTOBOT_SPOT_PREFETCH_ENABLED``.
    spot_prefetcher = None
    if config.SPOT_PREFETCH_ENABLED:
        from cryptobot.data.binance import fetch_all_spots as _prefetch_spots_impl
        from cryptobot.scheduler.spot_prefetch import SpotPrefetcher

        spot_prefetcher = SpotPrefetcher(
            partial(_prefetch_spots_impl, client=spot_client),
            tick_seconds=config.SPOT_FETCH_INTERVAL_SECONDS,
            lead_ms=config.SPOT_PREFETCH_LEAD_MS,
        )
        spot_prefetcher.start()

    # WebSocket mirror stall watchdog. A subscription can stall while the
    # socket stays connected; this job cross-checks old-looking books against
    # REST on the scheduler pool (never the order path) and evicts any that
    # drifted, so the next read falls back to REST. Only wired when the mirror
    # is authoritative.
    _book_ws_watchdog_job: Optional[Callable[[], None]] = None
    if config.BOOK_WS_WATCHDOG_ENABLED and ws_mirror_authoritative is not None:
        from cryptobot.scheduler.book_ws_watchdog import (
            BookWsWatchdog,
            build_watchdog_job,
            mirror_supports_watchdog,
        )

        if mirror_supports_watchdog(ws_mirror_authoritative):
            _wd_adapter = get_adapter(config.VENUE)
            _watchdog = BookWsWatchdog(
                ws_mirror_authoritative,
                # One batched REST book request per pass.
                partial(_wd_adapter.fetch_books_batch, client=clob_client),
                stall_suspect_ms=config.BOOK_WS_WATCHDOG_STALL_SUSPECT_MS,
                max_tokens_per_pass=config.BOOK_WS_WATCHDOG_MAX_TOKENS_PER_PASS,
                verify_ttl_ms=config.BOOK_WS_WATCHDOG_VERIFY_TTL_MS,
                size_rel_tol=config.BOOK_WS_WATCHDOG_SIZE_REL_TOL,
            )
            _book_ws_watchdog_job = build_watchdog_job(_watchdog)
            logger.info(
                "book WS stall watchdog armed (every %.1fs, suspect>%dms, "
                "<=%d tokens/pass, verify TTL %dms)",
                config.BOOK_WS_WATCHDOG_INTERVAL_S,
                config.BOOK_WS_WATCHDOG_STALL_SUSPECT_MS,
                config.BOOK_WS_WATCHDOG_MAX_TOKENS_PER_PASS,
                config.BOOK_WS_WATCHDOG_VERIFY_TTL_MS,
            )
        else:
            logger.info(
                "book WS stall watchdog NOT armed: this venue's mirror has no "
                "serving_snapshots/evict API"
            )

    # On-chain miss forensics: classify each rejected order (faster taker,
    # repriced, maker cancel) from the trade tape for the dashboard.
    _miss_forensics_job: Optional[Callable[[], None]] = None
    if config.MISS_FORENSICS_ENABLED:
        from cryptobot.scheduler.miss_forensics import run_miss_forensics

        def _run_miss_forensics_job() -> None:
            try:
                # Own connection; exchange reads run outside db_write_lock.
                counters = run_miss_forensics(
                    conns.get(),
                    funder=runtime_cfg.poly_funder,
                    rpc_url=runtime_cfg.polygon_rpc_url,
                    # Reuse the shared read client's keep-alive pool.
                    client=clob_client,
                    write_lock=db_write_lock,
                )
                if counters.get("classified") or counters.get("pending"):
                    logger.info("miss_forensics %s", counters)
            except Exception:  # noqa: BLE001 — forensics must never crash the scheduler
                logger.warning("miss_forensics job raised", exc_info=True)

        _miss_forensics_job = _run_miss_forensics_job

    # Fast-entry triggers: a 250ms sweep plus WebSocket ask-update events, both
    # running the tick's own evaluation (``pipeline.run_fast_entry_scan``), so
    # every gate applies unchanged. Master switch off: no job, no callback, and
    # the tick takes no lock.
    from cryptobot.scheduler.fast_entry import FastEntryTrigger, fast_trigger_seconds

    _fast_trigger: Optional[FastEntryTrigger] = None
    _fast_trigger_job: Optional[Callable[[], None]] = None
    _fast_trigger_seconds = 0.0
    if config.FAST_TRIGGER_ENABLED:
        _fast_trigger = FastEntryTrigger(
            # Own connection per calling thread.
            conn_factory=conns.get,
            portfolio=portfolio,
            deps_factory=deps_factory,
            book_source=book_prefetch,
        )
        _fast_trigger_job = _fast_trigger.run_sweep
        _fast_trigger_seconds = fast_trigger_seconds()
        # Event trigger: register the ask-update hook on any source that
        # supports it (the WebSocket mirror); without one, only the sweep runs.
        _event_rung = "disabled by CRYPTOBOT_FAST_TRIGGER_EVENT_ENABLED=0"
        if config.FAST_TRIGGER_EVENT_ENABLED:
            if book_prefetch is not None and book_prefetch.set_ask_update_callback(
                _fast_trigger.on_ask_update
            ):
                _fast_trigger.start_event_worker()
                _event_rung = "ARMED"
            else:
                _event_rung = "INERT (no book source exposes an ask-update hook)"
        logger.info(
            "fast_trigger: sweep %.0fms armed; WS event rung %s",
            _fast_trigger_seconds * 1000.0,
            _event_rung,
        )

    scheduler = start_scheduler(
        runner,
        tick_seconds=args.tick_seconds,
        sweep_job=sweep_job,
        wallet_recon_job=wallet_recon_job,
        wallet_recon_minutes=max(1, int(runtime_cfg.wallet_snapshot_interval_minutes or 10)),
        readiness_job=readiness_job,
        order_recon_job=order_recon_job,
        transfer_watchdog_job=transfer_watchdog_job,
        critical_alert_job=_critical_alert_job,
        requote_drain_job=requote_drain_job,
        wal_checkpoint_job=_wal_checkpoint_job,
        wal_checkpoint_seconds=config.WAL_CHECKPOINT_SECONDS,
        clob_ping_job=clob_ping_job,
        clob_ping_seconds=config.CLOB_PING_SECONDS,
        miss_forensics_job=_miss_forensics_job,
        miss_forensics_seconds=config.MISS_FORENSICS_SECONDS,
        book_ws_watchdog_job=_book_ws_watchdog_job,
        book_ws_watchdog_seconds=config.BOOK_WS_WATCHDOG_INTERVAL_S,
        fast_trigger_job=_fast_trigger_job,
        fast_trigger_seconds=_fast_trigger_seconds,
        on_job_missed=_on_job_missed,
    )

    # Step 10: signal handling.
    stop = threading.Event()

    def _shutdown(_signum, _frame):
        logger.info("shutdown requested")
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    try:
        while not stop.is_set():
            stop.wait(1.0)
    finally:
        # Wait for an in-flight tick before closing the DB, or a mid-tick write
        # would be rolled back. APScheduler's shutdown(wait=True) has no
        # timeout, so run it on a helper thread joined for at most 35s.
        shutdown_thread = threading.Thread(
            target=lambda: scheduler.shutdown(wait=True),
            name="cryptobot-scheduler-shutdown",
            daemon=True,
        )
        shutdown_thread.start()
        shutdown_thread.join(timeout=35.0)
        if shutdown_thread.is_alive():
            logger.warning(
                "scheduler.shutdown did not return within 35s; "
                "proceeding with conn.close() — orphan jobs may roll back"
            )
        # Stop the fast-entry worker after the scheduler drains and before
        # closing connections. Bounded join.
        if _fast_trigger is not None:
            try:
                _fast_trigger.stop()
            except Exception:  # noqa: BLE001 — shutdown must not raise
                logger.warning("fast_trigger stop raised", exc_info=True)
        # Stop the publisher after the scheduler drains and before closing
        # connections. Bounded join.
        _stop_publisher()
        # Flush queued notifications before exit. Bounded join.
        notify_dispatcher.stop()
        if httpd is not None:
            httpd.shutdown()
        if spot_prefetcher is not None:
            spot_prefetcher.stop()
        if book_prefetch is not None:
            book_prefetch.stop()
        if book_shadow is not None:
            book_shadow.stop()
        if twap_mirror is not None:
            twap_mirror.stop()
        spot_client.close()
        gamma_client.close()
        clob_client.close()
        # Close every per-thread connection and the dashboard's connection.
        conns.close_all()
        logger.info("shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
