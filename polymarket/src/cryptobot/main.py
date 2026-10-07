"""Live scanner process entrypoint.

Boot sequence (per plan U14):
  1. Load config + parse CLI
  2. Init DB + apply schema (idempotent)
  3. Load v6 sleeves + compute config_sha256
  4. Live-mode safety: REFUSE non-loopback DASHBOARD_BIND_HOST unless the
     operator has explicitly set CRYPTOBOT_ALLOW_REMOTE_DASHBOARD=1. The
     SSE stream is unauthenticated; binding beyond loopback exposes the
     full ledger / PnL / position state to anyone on the network.
  5. Reconcile orphan PENDING rows from prior runs
  6. Load RF + LUT artifacts (boot-blocking on missing / mismatched)
  7. Start BackgroundScheduler (1Hz tick + 5min F3 sweep)
  8. Start SSE server on (DASHBOARD_BIND_HOST, DASHBOARD_HTTP_PORT)
  9. Log boot summary into pipeline_health
 10. Install SIGTERM handler → graceful shutdown
 11. serve_forever()
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

# TYPE_CHECKING imports keep the deferred string annotations on
# ``_build_deps_factory`` resolvable to ``mypy``/IDEs without paying the
# import cost at boot — the concrete loads still happen inside ``main()``
# behind a feature flag check.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cryptobot.strategy.hgb_loader import HgbArtifact
    from cryptobot.strategy.partial_ret import PartialRetEngine
    from cryptobot.strategy.rf_loader import RfArtifact

logger = logging.getLogger(__name__)


def _disk_free_mb() -> float:
    """Cross-platform disk-free probe in MB for the disk gate (R39)."""
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
    checkpoint_rf: Optional["RfArtifact"] = None,
    checkpoint_hgb: Optional["HgbArtifact"] = None,
    twap_hgb: Optional["HgbArtifact"] = None,
    twap_mirror: Optional[object] = None,
    live_balance_fetcher: Optional[Callable[[], Optional[float]]] = None,
    book_prefetch: Optional[object] = None,
    book_shadow: Optional[object] = None,
    conn_factory: Optional[Callable[[], sqlite3.Connection]] = None,
) -> Callable[[], "TickDeps"]:
    """Closure returning fresh TickDeps each tick.

    Persistent ``httpx.Client`` instances are passed in via the caller so
    every adapter call reuses one TCP+TLS connection per host (review
    finding #11). Without reuse, each qualifying tick pays ~19-27 TLS
    handshakes (50-150ms each) and routinely breaches the 900ms soft
    deadline.
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
    # A2 latency: batched POST /books — one round trip for every qualifying
    # cell's tokens per tick (pipeline falls back to fetch_book on failure).
    fetch_books = partial(_fetch_books_impl, client=clob_client)

    def factory() -> TickDeps:
        return TickDeps(
            partial_ret_engine=partial_ret_engine,
            config_sha256=config_sha256,
            fetch_spots=fetch_spots,
            # Cache-only spot read for the fast-trigger rungs — SAME adapter,
            # SAME bucket math, but it can never spend a GET (no ``client``
            # bound at all). See TickDeps.peek_spots.
            peek_spots=_peek_spots_impl,
            fetch_market=fetch_market,
            fetch_book=fetch_book,
            fetch_books=fetch_books,
            get_disk_free_mb=_disk_free_mb,
            order_client=order_client,
            notify=notify,
            checkpoint_rf_artifact=checkpoint_rf,
            checkpoint_hgb_artifact=checkpoint_hgb,
            twap_hgb_artifact=twap_hgb,
            twap_mirror=twap_mirror,
            # A3 latency: cached balance (BalanceCache.get) — zero network
            # on the tick path; None keeps the legacy inline check_balance.
            live_balance_fetcher=live_balance_fetcher,
            # A8 latency: boundary book prefetcher; None = inline batch only.
            book_prefetch=book_prefetch,
            # W3 shadow-compare: WS-vs-REST divergence observer; None = off.
            book_shadow=book_shadow,
            # Per-thread DB handle for the Pass-1 Gamma fan-out: executor
            # workers resolve their OWN connection here instead of sharing the
            # tick conn (SQLITE_MISUSE silent-miss fix).
            conn_factory=conn_factory,
        )
    return factory


def _build_sweep_job(
    conn_factory: Callable[[], sqlite3.Connection],
    portfolio,
    *,
    gamma_client: httpx.Client,
) -> Callable[[], None]:
    """Closure that invokes the F3 resolution sweep on every scheduler fire.

    Gamma/Polymarket final outcome is the live settlement source: the scheduler
    leaves rows PENDING until Gamma publishes final ``outcomePrices`` so the
    ledger matches Polymarket's actual result. No oracle-spot fallback is wired
    — the legacy Chainlink resolver in ``execution.resolution`` is a test-only
    path and this job never supplies it a fetcher.

    Audit w19oxqf24 finding #1 supersedes the old REL-07 db_write_lock
    mitigation. The sweep now resolves its OWN connection from ``conn_factory``
    (one per scheduler-pool thread), so its ``UPDATE ledger`` + commit can no
    longer end the tick's open ``BEGIN IMMEDIATE`` transaction — they are on
    different connections, and SQLite's WAL file lock + busy_timeout serialize
    the two writers correctly. The in-process lock, which the tick never held,
    is gone.
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
    """Seed PartialRetEngine from Binance 1m klines so restart can score."""
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
    """Boot + serve_forever entry point. Returns process exit code."""
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
    from cryptobot.scheduler.resolution_sweep import run_resolution_sweep
    from cryptobot.strategy.partial_ret import PartialRetEngine
    from cryptobot.strategy.hgb_loader import validate_sleeves_against_manifest
    from cryptobot.strategy.rf_loader import warm_step_luts
    from cryptobot.strategy.portfolio_loader import load_sleeves

    # Step 2: DB
    # init_db applies schema.sql + migrations once on the boot (main) thread.
    # Audit w19oxqf24 finding #1 (P0): every other thread (tick worker,
    # scheduler-job pool, dashboard POST) must NOT share this connection —
    # they get their own via `conns`. For an in-memory DB (tests / --once
    # smoke) the handle shares the one boot connection, since a second
    # :memory: connection would see an empty DB. `conn` below is the main
    # thread's connection used for the boot gates, backfills, and _log_boot.
    boot_conn = init_db(config.DB_PATH)
    if str(config.DB_PATH) == ":memory:":
        conns = ThreadLocalConnections(config.DB_PATH, shared=boot_conn)
    else:
        conns = ThreadLocalConnections(config.DB_PATH)
        conns.register_current_thread(boot_conn)
    conn = conns.get()

    # Step 2.5 (plan §U2 / D-LIVE-7): Polymarket wallet topology gate. Boot
    # fails closed when SIG=3 (POLY_1271 deposit wallet) is set without a
    # POLY_FUNDER — without this guard the first LIVE order placement would
    # silently sign against the wrong address. The Pydantic singleton is
    # cached for the rest of boot via get_config().
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
            # Refuse to mask the underlying validation error if we can't
            # write the audit row — boot still fails closed.
            pass
        logger.error(msg)
        conns.close_all()
        return 1

    # D-LIVE-27: RELAYER_URL prefix check. Adversarial finding #12 wants
    # explicit operator acknowledgement so a typo / homoglyph URL can't
    # silently slip past CRITICAL logs that the operator missed.
    # `CRYPTOBOT_RELAYER_URL_OVERRIDE=1` is the explicit acknowledgement.
    # Without it, an unknown URL refuses boot (rather than the previous
    # log-and-continue) when LIVE wallets could become reachable. Phase 1
    # deploys with no relayer wired (the common shape) still need the
    # override because a future operator action could promote any wallet
    # to LIVE and immediately hit the bad relayer.
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

    # Step 3: portfolio + sha
    portfolio = load_sleeves()
    sha = portfolio.config_sha256

    # Step 3.5 (plan §U1 / §U4): portfolio-dependent backfill + seed.
    # _migrate_db is portfolio-free (D-LIVE-15) so we run the two helpers
    # here once load_sleeves() has the JSON-derived wallet→group mapping
    # and the wallet_default_modes dict. Idempotent: re-running on a
    # populated DB is a no-op.
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

    # Step 4: live-mode safety. The SSE /events stream has NO authentication
    # — its only access control is the loopback bind. Refuse to boot on a
    # non-loopback bind unless the operator has explicitly opted in via
    # CRYPTOBOT_ALLOW_REMOTE_DASHBOARD=1 (which assumes they have placed
    # the dashboard behind their own auth proxy).
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
        # Audit w19oxqf24 finding #4 (P1): a remote bind without TLS sends the
        # operator-control session cookie in cleartext and without the Secure
        # attribute (the cookie is only marked Secure when TLS_TERMINATED=1).
        # That cookie is a bearer token for wallet-mode flips, operator
        # start/stop, and fund transfers. Match the D-LIVE-13 spec: refuse boot
        # on a non-loopback bind unless BOTH ALLOW_REMOTE_DASHBOARD=1 AND
        # DASHBOARD_TLS_TERMINATED=1.
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

    # Security finding #1: refuse boot when CRYPTOBOT_DASHBOARD_PASS is
    # empty AND the operator has not explicitly opted out of auth via
    # CRYPTOBOT_DASHBOARD_AUTH_DISABLED=1. The opt-out is itself refused
    # on a non-loopback bind because "anyone on the network" cannot
    # safely be the audience for mode-flip / transfer POSTs without auth.
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

    # Review-pass #6 closeout: PartialRetEngine's prev-tick anchor (the
    # divergence #4 fix) only fires when the previous tick is within 5
    # seconds of the interval boundary. At TICK_SECONDS >= 6 the
    # previous tick is always stale and we silently fall back to
    # first-after-boundary, re-introducing the open-vs-close timing
    # bias the fix removed. Refuse to boot rather than degrade
    # silently. Operator override via CRYPTOBOT_TICK_SECONDS is the
    # source of truth — refuse at boot so the misconfiguration shows
    # up loudly before any bets are placed.
    # Validate the EFFECTIVE cadence the scheduler will actually run at:
    # start_scheduler uses `args.tick_seconds or config.TICK_SECONDS`
    # (tick.py IntervalTrigger), so an operator passing `--tick-seconds 10`
    # would otherwise boot cleanly yet trade at 10s, defeating this guard and
    # re-introducing the live/backtest entry-anchor divergence the >5s refusal
    # exists to prevent. Also reject sub-1s cadences.
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

    # Step 5: orphan reconcile. Main leaves resolvers unwired here so boot
    # avoids network dependency before the persistent clients exist; expired
    # rows stay PENDING and the scheduled Gamma-first sweep resolves them.
    # Guarded against DB exceptions (locked DB after a crash, schema
    # mismatch on a stale data dir, etc.) so a transient failure here
    # does NOT block RF+LUT load and the rest of boot. The orphan rows
    # stay PENDING and the next F3 sweep retries them.
    try:
        reconcile_orphan_pending(conn)
    except (sqlite3.DatabaseError, sqlite3.OperationalError) as exc:
        logger.warning(
            "orphan_reconcile_failed: %s — boot continuing; orphan PENDING "
            "rows will retry on the next F3 sweep",
            exc,
        )

    # Step 6: model artifacts (boot-blocking).
    #
    # 2026-07-31 (exp044 phase 2): the baseline rf_v1 joblib and the
    # decision_table LUT are NO LONGER loaded. The pair sleeves that consumed
    # the LUT and the lut|rf|blend scorer that consumed rf_v1 are gone, so both
    # were pure boot cost (rf_v1 alone is 62 MB). checkpoint_v1 below is the
    # only scoring artifact.
    #
    # Fail-closed: the runtime sleeves' serve-time bet-selection knobs
    # (threshold / ask_bucket / book_consumption_max / side_filter) are not
    # baked into the joblib pipeline, so a config typo would otherwise pass
    # every boot check and serve a band the backtest never validated
    # (alignment finding #4). Assert they match the manifest the model was
    # selected on — since the 2026-07-31 exp044 retirement every hgb sleeve is
    # served by checkpoint_v1, so the CHECKPOINT manifest is the one contract
    # (a baseline minute-entry hgb sleeve is rejected outright in there).
    if any(s.signal_source == "hgb" for s in portfolio.sleeves):
        if config.TWAP_SYMMETRIC_ENABLED:
            from cryptobot.strategy.twap_loader import (
                validate_twap_coin_universe,
                validate_twap_sleeves,
            )

            twap_manifest = json.loads(
                Path(config.TWAP_SYMMETRIC_MANIFEST_PATH).read_text(
                    encoding="utf-8"
                )
            )
            validate_twap_sleeves(portfolio.sleeves, twap_manifest)
            live_contract = json.loads(
                Path(config.V6_CONFIG_PATH).read_text(encoding="utf-8")
            )
            configured_coins = {
                str(coin).upper()
                for coin in live_contract.get("coin_universe", [])
            }
            validate_twap_coin_universe(
                config.COINS, configured_coins, twap_manifest
            )
        else:
            checkpoint_manifest = json.loads(
                Path(config.CHECKPOINT_MANIFEST_PATH).read_text(encoding="utf-8")
            )
            validate_sleeves_against_manifest(
                portfolio.sleeves, checkpoint_manifest
            )
    # checkpoint_v1: per-checkpoint-second RF + HGB for the sub-minute sleeves
    # (2026-07-05 parity fix). Loaded ONLY when a checkpoint sleeve exists — the
    # v6 loader drops those sleeves unless SUBMINUTE_ENABLED, so the baseline
    # deploy never loads or depends on the artifact. Fail-closed at boot.
    checkpoint_rf = None
    checkpoint_hgb = None
    twap_hgb = None
    if config.TWAP_SYMMETRIC_ENABLED:
        from cryptobot.strategy.twap_loader import load_twap_symmetric_model

        twap_hgb = load_twap_symmetric_model()
        logger.info(
            "TWAP symmetric trend scorer armed for both intervals "
            "(exact RTDS boundary anchors required)"
        )
    elif any(getattr(s, "checkpoint_offset_set", ()) for s in portfolio.sleeves):
        from cryptobot.strategy.checkpoint_loader import load_checkpoint_models
        checkpoint_rf, checkpoint_hgb = load_checkpoint_models()
        logger.info(
            "checkpoint_v1 loaded (%d RF slices) for sub-minute sleeves",
            len(checkpoint_rf.estimators),
        )
    # 2026-07-25 latency audit: build the exact step-function LUTs for the
    # univariate checkpoint RF forests on a daemon thread. score_cell_hgb calls
    # predict_proba once per cell (~5.3 cells/tick) — measured ~36ms of every
    # order tick. Slices serve the unchanged sklearn path until their LUT
    # lands, so this is non-blocking and fail-safe.
    if config.RF_STEP_LUT_ENABLED:
        if checkpoint_rf is not None:
            warm_step_luts(checkpoint_rf)
    else:
        logger.info("RF step-LUT DISABLED (CRYPTOBOT_RF_STEP_LUT=0) — sklearn path")
    partial_ret_engine = PartialRetEngine()
    twap_mirror = None
    if config.TWAP_SYMMETRIC_ENABLED:
        from cryptobot_polymarket.price_to_beat import fetch_price_to_beat
        from cryptobot_polymarket.twap_mirror import ChainlinkTwapRtdsMirror

        twap_mirror = ChainlinkTwapRtdsMirror(
            url=config.TWAP_RTDS_URL,
            opening_price_fetcher=fetch_price_to_beat,
        )
        twap_mirror.start()
        logger.info(
            "Chainlink TWAP RTDS mirror started "
            "(5m=60s, 15m=60s, official Price To Beat fallback armed)",
        )

    # Persistent HTTP clients — one per upstream host. Reused across every
    # tick so TLS handshakes amortize over the whole run (#11). Closed in
    # the shutdown finalizer below.
    #
    # A2 latency: the Polymarket read clients get http2=True (multiplexed
    # streams over one connection — the batch /books POST + any fallback
    # GETs share it) and explicit pool limits. Fail-soft to HTTP/1.1 when
    # the `h2` extra is absent so a missing optional dep can't block boot.
    # Read/write/pool timeout is the CRYPTOBOT_DECISION_READ_TIMEOUT_S knob
    # (default 3.0 = the previous hardcoded value, so an unpinned deploy is
    # behavior-identical); the operator can pin a tighter fail-fast timeout
    # on decision-path reads without a code change. connect stays at 2.0s.
    http_timeout = httpx.Timeout(config.DECISION_READ_TIMEOUT_S, connect=2.0)
    # keepalive_expiry=65.0: httpx's default 5s idle-expiry silently closed
    # the kept-alive sockets between spaced-out reads (spot cache refills,
    # per-window gamma lookups, the 60s scheduler jobs), so nearly every such
    # read paid a fresh TCP+TLS handshake (~50-150ms) despite the pool. 65s
    # comfortably outlives the 60s job cadences and the between-window gaps.
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
    # Step 7a: LIVE execution surface — OrderClient + scheduler jobs.
    # ------------------------------------------------------------------
    # OrderClient is fail-soft: when py_clob_client_v2 is missing OR
    # credentials are empty, we boot in DRY-only mode (Phase 1). The
    # pipeline + scheduler jobs treat `order_client=None` as
    # `clob_unavailable` — LIVE wallets emit SKIP, DRY wallets continue.
    from cryptobot.venues import get_adapter

    order_client: Optional[object] = get_adapter(config.VENUE).build_order_client(
        runtime_cfg=runtime_cfg
    )

    from cryptobot.execution.live_readiness import build_venue_balance_fetcher

    # A3 latency: cached funder balance — the tick reads this instead of a
    # synchronous CLOB round trip every LIVE tick; the 60s readiness job
    # keeps it fresh (store() below reuses that job's own fetch).
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
    # Reliability finding: import the relayer names in their OWN guarded block.
    # Previously RelayerSubmitError was imported inside the same try whose
    # `except RelayerSubmitError` referenced it; if that import itself raised
    # (missing transitive dep on a fresh LIVE box), evaluating the except clause
    # would hit NameError, which propagates past the broad `except Exception`
    # and crashes boot instead of degrading to DRY. Splitting the import out
    # guarantees the name is bound before the build-logic except runs.
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

    # Config finding: a wallet flipped LIVE places real CLOB orders, but with no
    # relayer wired (RELAYER_API_KEY / RELAYER_API_KEY_ADDRESS / poly_funder /
    # private_key / url) the transfer surface is silently dead — pUSD cannot be
    # withdrawn. Surface a CRITICAL pipeline_health row so the half-broken LIVE
    # state is loud (the relayer env stays optional for DRY-only deploys, so we
    # only escalate when a wallet is actually LIVE).
    # `wallet_modes` is the boot-resolved {id: mode} map.
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

    # Latency finding (stop-fill notify + venue-degradation alert): send_event
    # ran its blocking Telegram POST (5s timeout) on the calling thread — on
    # the tick worker that stalled the serial per-position held-position loop
    # and the post-POST reject tail (pipeline.py
    # _maybe_alert_venue_degradation) — so one Telegram RTT delayed the next
    # position's cut / next cell's POST and pushed the tick toward its hard
    # deadline. _notify now enqueues NON-CRITICAL events to a bounded daemon
    # dispatcher (the SSE publisher off-thread pattern) and returns
    # immediately; send_event runs UNCHANGED in the worker (rate limiter +
    # CRITICAL.log semantics intact). critical=True bypasses the queue and
    # sends inline — criticals are rare and allowed to block (their pre-diff
    # contract); queued they could be shed on overflow or lost to a crash
    # before dequeue. Every _notify caller already discards the NotifyResult
    # and the non-critical notify contract is documented best-effort, so
    # fire-and-forget is semantics-identical. Stopped (drained, bounded
    # join) on shutdown below.
    notify_dispatcher = NotifyDispatcher()
    notify_dispatcher.start()

    def _notify(event_type: str, message: str, *, wallet_id=None, critical=False) -> None:
        """Wrap `cryptobot.execution.notify.send_event` so pipeline + jobs
        share one dispatch path. Non-critical events are fire-and-forget:
        they queue to the dispatcher's daemon worker so the Telegram POST
        never runs on the calling (tick) thread. critical=True sends
        inline (blocking) — the pre-dispatcher guarantee. Failures are
        logged but never raised."""
        try:
            notify_dispatcher.enqueue(
                event_type, message, wallet_id=wallet_id, critical=critical
            )
        except Exception:  # noqa: BLE001
            logger.warning("notify dispatch failed", exc_info=True)

    # Step 7-8: scheduler + SSE
    broadcaster: Optional[Broadcaster] = None
    httpd = None
    post_router: Optional[PostRouter] = None
    if not args.no_sse:
        broadcaster = Broadcaster()
        # PostRouter wires the dashboard mutation surface (D-LIVE-13).
        # Relayer submitter is None on Phase 1 deploys — submit endpoint
        # returns 503 until U10 wires the real relayer client. `dashboard_pass`
        # and `auth_disabled` are resolved earlier in the boot gate above.
        #
        # Audit w19oxqf24 finding #1 (P0): the dashboard mutation surface gets
        # its OWN dedicated connection, not the tick's. PostRouter already
        # serializes every handler with its internal self._lock, so one
        # connection is safe across the HTTP server's per-request threads, and
        # it is isolated from the tick/sweep connections so an operator wallet
        # flip / transfer can never commit or roll back the tick's open
        # transaction. For an in-memory DB this returns the one shared conn.
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

    # Snapshot publisher: hook fires after each successful tick.
    #
    # Latency item #1: the SSE publish is OFF the tick-gating path. The
    # scheduler runs on_tick INLINE after the tick body (scheduler/tick.py,
    # max_instances=1), so the old synchronous build_payload + json.dumps +
    # broadcast counted against the wall time gating the NEXT tick fire.
    # on_tick now only raises a latest-wins pending flag and returns; a
    # dedicated daemon thread consumes the flag and does the whole payload
    # build/publish on its OWN sqlite connection (conns.get() resolves
    # per-thread — audit finding #1). Ordering is preserved by construction:
    # the flag is a single collapsed "a tick is waiting" signal and there is
    # exactly one consumer that always rebuilds from the NEWEST sqlite state,
    # so an older tick can never publish after a newer one — a busy publisher
    # simply skips the intermediate ticks (dashboard gains at most ~1 tick of
    # lag, accepted). `state` is now touched ONLY by the publisher thread, so
    # the old state_lock is gone.
    state = {
        "max_ledger_id": 0,
        "last_bootstrap_ts": 0.0,
        "last_bootstrap_max": -1,
    }
    publish_lock = threading.Lock()      # guards the pending flag only
    publish_pending = False              # True when a tick awaits publish
    publish_wake = threading.Event()
    publish_stop = threading.Event()

    # The scheduler invokes this with the TickResult; the payload no longer
    # reads it (the per-sleeve markets decision subtree was dropped with the
    # retired Live markets & gates tab) and it is not retained — the flag is
    # only the "a tick completed" token; the publisher rebuilds from sqlite.
    def on_tick(_tick_result):
        if broadcaster is None:
            return
        nonlocal publish_pending
        # Latest-wins signal: a burst of ticks collapses to one pending flag.
        with publish_lock:
            publish_pending = True
        publish_wake.set()

    def _publish_latest(tick_conn: sqlite3.Connection) -> None:
        # Same per-tick balance sizing reads (BalanceCache) → the cash headline;
        # the off-tick book source → the open-position mark. Bound once and
        # reused by both build_payload calls below (live tick + late-joiner
        # bootstrap).
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
        # Performance finding #6 (PERF-01): the late-joiner bootstrap
        # cache was being rebuilt on EVERY tick — a second full
        # `build_payload` call (full ledger scan + every aggregate) at
        # 1 Hz. Most ticks land no new ledger rows, so the bootstrap
        # only needs to refresh when new rows actually arrived OR a
        # cap-30s ceiling so a stale page still gets a periodic
        # update. Reuses the just-built payload when the ledger
        # contains every row needed.
        now = time.time()
        if new_max == last_bootstrap_max and (now - last_bootstrap_ts) < 30.0:
            return
        if last_seen <= 0:
            # First post-boot publish IS the bootstrap (we passed
            # last_seen_ledger_id=0 above, so `payload.ledger` is the
            # full snapshot). Reuse it without a second build.
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
        # This thread's OWN connection (ThreadLocalConnections hands the
        # publisher its own handle on a file-backed DB; the :memory: test
        # path shares the one boot connection exactly like every thread).
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
        # daemon=True: the publisher must never block process exit even if a
        # payload build hangs; _stop_publisher below is the polite half.
        publisher_thread = threading.Thread(
            target=_publisher_loop,
            name="cryptobot-sse-publisher",
            daemon=True,
        )
        publisher_thread.start()

    def _stop_publisher() -> None:
        """Signal + briefly join the publisher BEFORE conns.close_all() so a
        mid-build publish isn't cut off by its connection closing under it.
        The join is bounded — a hung build is abandoned (daemon thread)."""
        if publisher_thread is None:
            return
        publish_stop.set()
        publish_wake.set()
        publisher_thread.join(timeout=5.0)

    # Book read-ahead sources feeding the SAME TickDeps.book_prefetch seam.
    # Priority order (BookSourceChain.get returns the first fresh hit):
    #   1. WS mirror (CRYPTOBOT_BOOK_WS_ENABLED) — venue-timestamped snapshots
    #      off the CLOB market channel; real staleness (audit #12).
    #   2. A8 REST prefetch-ahead (CRYPTOBOT_BOOK_PREFETCH_ENABLED) — re-fetches
    #      the previous tick's POST /books token set LEAD_MS before each second
    #      boundary; freshness bounded by CRYPTOBOT_BOOK_PREFETCH_MAX_AGE_MS.
    # Whatever neither serves fresh falls through to the pipeline's inline REST
    # batch exactly as today. Both default OFF.
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
            # W3: the mirror runs ALONGSIDE REST and is NOT appended to
            # book_sources — REST stays authoritative for every decision.
            book_shadow = _ws_setup.shadow
        elif _ws_setup.mirror is not None:
            # Authoritative: sit FIRST in the chain so a fresh WS snapshot
            # outranks the REST prefetch cache.
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
        checkpoint_rf=checkpoint_rf,
        checkpoint_hgb=checkpoint_hgb,
        twap_hgb=twap_hgb,
        twap_mirror=twap_mirror,
        live_balance_fetcher=(
            balance_cache.get if balance_cache is not None else None
        ),
        book_prefetch=book_prefetch,
        book_shadow=book_shadow,
        # Executor-thread DB access for the Gamma fan-out: hand the pipeline the
        # same ThreadLocalConnections handle the tick/sweep/dashboard threads
        # use, so each fan-out worker gets its own connection (audit finding #1
        # pattern; SQLITE_MISUSE silent-miss fix).
        conn_factory=conns.get,
    )
    runner = TickRunner(
        conn_factory=conns.get,
        portfolio=portfolio,
        deps_factory=deps_factory,
        on_tick=on_tick,
    )

    # Step 9: boot log
    _log_boot(conn, sha=sha, bankroll=portfolio.bankroll,
              host=config.DASHBOARD_BIND_HOST, port=config.DASHBOARD_HTTP_PORT)

    # Review-pass #25 closeout: when Binance warm-start fails,
    # ~30 minutes of 1m-return history accumulate (training-parity
    # formula needs rv_1h_at_open). In that fallback window,
    # active cells fire SKIP:no_feature; without a distinct signal
    # this is indistinguishable from a broken feature pipeline. Write
    # ONE pipeline_health WARN so operators have a stable
    # bookmark — "this is warmup, not breakage".
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

    # Audit w19oxqf24 finding #1 (P0): cross-thread DB-transaction safety is
    # now provided by per-thread connections (each writer thread holds its own
    # connection; SQLite's WAL file lock + busy_timeout serialize BEGIN
    # IMMEDIATE writers across them). `db_write_lock` is retained only to
    # serialize the few scheduler jobs that share it; it is no longer
    # load-bearing for the tick race, which the tick never participated in.
    db_write_lock = threading.Lock()

    sweep_job = _build_sweep_job(
        conns.get,
        portfolio,
        gamma_client=gamma_client,
    )

    def _on_job_missed(job_id: str) -> None:
        """Review-pass #18 closeout: APScheduler may drop a fire when
        ``max_instances=1`` AND a prior fire is still running past the
        next schedule time. Surface the drop into pipeline_health so
        operators have a queryable trail when the sweep / tick
        cadence silently degrades.
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
    # Live-execution scheduler jobs (U7 + U8 + U9 + U10). Each is wired
    # only when the corresponding execution surface is available so a
    # Phase 1 deploy (no OrderClient) still boots cleanly.
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
                # Reliability finding REL-06: CLOB fetches (up to 30s each
                # x 3 calls) run OUTSIDE the db_write_lock so other jobs
                # (_on_job_missed and friends) aren't blocked for 90s.
                # Only the persist_run DB writes are serialized.
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
                # Reliability finding REL-08: CLOB balance check runs
                # OUTSIDE db_write_lock so other jobs aren't blocked
                # while CLOB hangs near its 30s timeout.
                conn = conns.get()  # this scheduler-pool thread's own connection
                prefetched = prefetch_readiness_state(
                    order_client=order_client,
                    chain_balance_fetcher=chain_balance_fetcher,
                )
                # A3 latency: feed the tick's balance cache from the balance
                # this job just fetched — zero extra network.
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

        # Audit w19oxqf24 findings #3 + #6: reconcile LIVE fills and release
        # stale SUBMITTING attempt reservations. Without this scheduled job,
        # LIVE PENDING rows never get their realized live_filled_size/price
        # (so finding #7's settlement starves), and a SUBMITTING attempt the
        # CLOB rejected counts toward the wallet-group cap forever. Runs every
        # 60s on its own thread connection; CLOB fetches are inside the
        # reconcilers and the DB writes are serialized via db_write_lock.
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

        # Latency finding #8: order-socket warmth ping. Between orders the
        # signing pool's ONLY steady traffic is the 60s readiness balance
        # read vs the pool's keepalive_expiry=65s — a 5s margin — and a
        # readiness fire delayed past misfire_grace_time=2 is dropped
        # outright, opening a ~120s gap that guarantees the socket expires
        # and the next order POST pays a cold DNS+TCP+TLS handshake inside
        # post_ms. This job fires a lightweight unauthenticated GET /ok on
        # the SAME pool every CLOB_PING_SECONDS (default 20s); the
        # skip-if-recent guard + short per-request timeout live in
        # OrderClient.ping_order_pool, so it never duplicates traffic or
        # pins a scheduler-pool worker. CRYPTOBOT_CLOB_PING_SECONDS=0
        # disables (start_scheduler registers no job — byte-identical).
        def _clob_ping_job() -> None:
            try:
                order_client.ping_order_pool()
            except Exception:  # noqa: BLE001 — warmth job must never crash the scheduler
                logger.warning("clob_ping job raised", exc_info=True)
        clob_ping_job = _clob_ping_job

    # Transfer watchdog always runs — it only inspects operator state
    # and doesn't need the OrderClient. Releases stuck TRANSFER_LOCKs
    # older than 15 minutes (D-LIVE-21 watchdog).
    #
    # The watchdog is CORE (panel verdict D4-1). It used to be imported
    # from cryptobot_polymarket, which meant this ALWAYS-ON job could not start
    # unless the Polymarket package was installed — on any venue. It does not
    # make a venue call: it is SQLite hygiene over operator_control_state,
    # a core-owned table.
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

    # Reject re-quote telemetry drain (observation only — no order is placed).
    # Re-reads the book ~1s after each LIVE FAK reject and back-fills
    # requote_ask/age. Uses only the read-only CLOB client + a scheduler-pool
    # connection, so it runs unconditionally (no OrderClient dependency).
    _rr_fetch_book = get_adapter(config.VENUE).fetch_top_book
    from cryptobot.execution.reject_requote import drain_requote_queue

    def _requote_drain_job() -> None:
        try:
            conn = conns.get()  # this scheduler-pool thread's own connection
            # write_lock is passed INTO the drain (REL-08 pattern): at the 2s
            # cadence the CLOB book re-reads must run outside db_write_lock so
            # a fetch hanging toward its timeout can't stall the other jobs;
            # the drain takes the lock only around its batched UPDATE and not
            # at all when idle.
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

    # Latency item #4: periodic PASSIVE WAL checkpoint. Pairs with
    # db/connection.py's wal_autocheckpoint=20000 — the raised bound keeps
    # SQLite's default inline checkpoint (which runs ON the committing
    # writer, i.e. usually the tick's own autocommit at 1 Hz) off the
    # decision path, and this job drains the WAL from a scheduler-pool
    # thread instead. PASSIVE never blocks readers or writers; cadence via
    # CRYPTOBOT_WAL_CHECKPOINT_SECONDS (default 60s, 0 disables).
    def _wal_checkpoint_job() -> None:
        try:
            passive_wal_checkpoint(conns.get())
        except Exception:  # noqa: BLE001 — hygiene job must never crash the scheduler
            logger.warning("wal_checkpoint job raised", exc_info=True)


    # Catch-all issue alert (operator decision 2026-07-03): several code
    # paths record CRITICAL pipeline_health rows without notifying (dd_halt,
    # boot relayer gap, future additions). This job forwards every NEW
    # CRITICAL row to Telegram so a recorded issue is never silent. Spam
    # control: send_event's per-(event_type) rate limit at 30 min — dd_halt
    # re-inserts a row every ~60s while halted but the operator gets one
    # ping per stage per half hour.
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

    # A7 latency: spot prefetch-ahead — a daemon thread warms the spot bucket
    # cache ~LEAD_MS before each second boundary so the tick's inline spot
    # fetch is a zero-network cache hit. Same adapter + client + bucket math
    # as deps.fetch_spots; kill switch CRYPTOBOT_SPOT_PREFETCH_ENABLED=false.
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

    # WS mirror stall watchdog (operator order 2026-08-06). The mirror's
    # freshness cap was removed because snapshot age = market quietness on these
    # books; the residual risk is a STALLED-BUT-CONNECTED subscription, which no
    # in-mirror signal can see. This job cross-checks only SUSPECT (old-ladder)
    # tokens against the venue's REST book on the SCHEDULER POOL — never the
    # tick/order path — and evicts a token whose top-of-book has drifted, so the
    # next entry-path read falls through to REST truth. Fail-open throughout.
    #
    # Wired only when the mirror is AUTHORITATIVE (not shadow — shadow already
    # cross-checks every tick against REST) and the venue's mirror exposes the
    # serving-set + evict API. Registered through the start_scheduler param
    # pattern (the clob_ping precedent) so start_scheduler stays the single
    # owner of the job set; flag-off wiring stays byte-identical (param defaults
    # None) and the import happens only inside the `if`.
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
                # Same venue-dispatched batch fetch + read-only client the A8
                # prefetcher uses; one POST /books per pass, one rate-limit
                # token regardless of token count.
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

    # On-chain miss forensics (operator 2026-07-30): classify every
    # venue-rejected FAK as FASTER_TAKER / REPRICED_ONLY / MAKER_CANCEL from
    # the trade tape and persist the verdict for the dashboard. Registered
    # through the start_scheduler param pattern (the clob_ping precedent) so
    # start_scheduler stays the single owner of the job set; flag-off wiring
    # stays byte-identical (param defaults None) and the heavy import happens
    # only inside the `if`.
    _miss_forensics_job: Optional[Callable[[], None]] = None
    if config.MISS_FORENSICS_ENABLED:
        from cryptobot.scheduler.miss_forensics import run_miss_forensics

        def _run_miss_forensics_job() -> None:
            try:
                # Own connection per scheduler-pool thread (audit finding #1).
                # The venue fetches run OUTSIDE db_write_lock (REL-06): the
                # lock is taken per-verdict inside run_miss_forensics, so a
                # slow data-api page can never block the sweep or the tick.
                counters = run_miss_forensics(
                    conns.get(),
                    funder=runtime_cfg.poly_funder,
                    rpc_url=runtime_cfg.polygon_rpc_url,
                    # Ride the process-wide read client's keep-alive pool
                    # instead of a fresh TLS handshake per row (the
                    # `fetch_book=partial(..., client=clob_client)` pattern).
                    client=clob_client,
                    write_lock=db_write_lock,
                )
                if counters.get("classified") or counters.get("pending"):
                    logger.info("miss_forensics %s", counters)
            except Exception:  # noqa: BLE001 — forensics must never crash the scheduler
                logger.warning("miss_forensics job raised", exc_info=True)

        _miss_forensics_job = _run_miss_forensics_job

    # Fast-trigger entry rungs (operator 2026-07-30): a 250ms in-window sweep +
    # event-driven firing off WS ask-side book pushes, both converging on the
    # tick's OWN single-cell evaluation (pipeline.run_fast_entry_scan ->
    # _resolve_net_cells -> _commit_cell), so every guard the tick binds — band,
    # gate, V2_EDGE_CAP, sizing/caps/DD-brake, one-per-window dedup,
    # _attempted_offsets, first-attempt-only, multifill — binds here unchanged.
    # Registered through the start_scheduler param pattern (the clob_ping
    # precedent) so start_scheduler stays the single owner of the job set;
    # MASTER OFF => no job passed, no WS callback registered, and the tick takes
    # no single-flight lock (byte-identical runtime).
    from cryptobot.scheduler.fast_entry import FastEntryTrigger, fast_trigger_seconds

    _fast_trigger: Optional[FastEntryTrigger] = None
    _fast_trigger_job: Optional[Callable[[], None]] = None
    _fast_trigger_seconds = 0.0
    if config.FAST_TRIGGER_ENABLED:
        _fast_trigger = FastEntryTrigger(
            # Own connection per calling thread (audit finding #1) — the sweep
            # runs on an APScheduler pool thread, the event rung on its own.
            conn_factory=conns.get,
            portfolio=portfolio,
            deps_factory=deps_factory,
            book_source=book_prefetch,
        )
        _fast_trigger_job = _fast_trigger.run_sweep
        _fast_trigger_seconds = fast_trigger_seconds()
        # EVENT rung: the chain fans the hook out over whatever sources expose
        # `set_ask_update_callback` (today the venue WS mirror) and reports how
        # many accepted, so a venue without the hook — or with no WS mirror at
        # all — keeps the sweep rung instead of failing the boot.
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

    # Step 10: SIGTERM handler
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
        # Drain any in-flight tick before closing the DB connection. With
        # wait=False a mid-tick record_bet inside BEGIN IMMEDIATE would be
        # rolled back by conn.close()'s implicit rollback — silent bet loss
        # on SIGTERM (#10). Reliability finding #59: APScheduler's
        # `shutdown(wait=True)` takes no timeout. Run on a helper thread
        # joined at 35s so a hung CLOB-touching job can't stall the
        # systemd grace window indefinitely.
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
        # Stop the fast-trigger event worker after the scheduler drains (so the
        # sweep can no longer fire) and BEFORE conns.close_all() closes the
        # worker's own connection. Daemon thread + bounded join, so a hung scan
        # can never stall the SIGTERM grace window.
        if _fast_trigger is not None:
            try:
                _fast_trigger.stop()
            except Exception:  # noqa: BLE001 — shutdown must not raise
                logger.warning("fast_trigger stop raised", exc_info=True)
        # Stop the SSE publisher after the scheduler drains (no more on_tick
        # enqueues) and BEFORE conns.close_all() closes its connection. It is
        # a daemon thread with a bounded join, so it can never stall the
        # SIGTERM grace window.
        _stop_publisher()
        # Drain + stop the notify dispatcher after the scheduler drains (tick
        # + jobs are the only producers) so queued non-critical events get a
        # best-effort flush before exit (criticals never queue — they send
        # inline at the call site). Daemon thread + bounded join — a hung
        # Telegram can't stall the SIGTERM grace window either.
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
        # Close every per-thread connection opened through the handle plus the
        # dashboard's dedicated connection (audit finding #1). The scheduler is
        # already drained above, so no writer thread is mid-transaction.
        conns.close_all()
        logger.info("shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
