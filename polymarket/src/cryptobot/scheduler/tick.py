"""APScheduler wrapper around run_scanner_tick.

The tick runs on an ``IntervalTrigger(seconds=TICK_SECONDS)`` with:

  - ``max_instances=1``: never queue a second copy;
  - ``misfire_grace_time=2``: drop a missed tick rather than catch up in a
    burst.

Errors:

  - a tick exception writes a ``pipeline_health`` row and increments a
    consecutive-failure counter;
  - every 5 consecutive failures send a CRITICAL alert through the notify
    callback;
  - a successful tick resets the counter.

Hard deadline: the tick body runs on a worker thread and the scheduler waits
with ``Future.result(timeout)``. Past ``TICK_HARD_DEADLINE_MS`` it stops
waiting (the worker may still finish in the background) and writes a CRITICAL
pipeline_health row.

``start_scheduler(...)`` returns the BackgroundScheduler; the caller stops it
at shutdown (main.py installs the SIGTERM handler).
"""

from __future__ import annotations

import logging
import sqlite3
import time
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    TimeoutError as FuturesTimeoutError,
)
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

# ``conn_factory`` returns the calling thread's own connection (a
# ``ThreadLocalConnections`` handle), so the tick worker and the scheduler
# thread never share one. SQLite's WAL lock and BEGIN IMMEDIATE serialize their
# writes.
ConnFactory = Callable[[], sqlite3.Connection]

from cryptobot import config
from cryptobot.execution.pipeline import TickDeps, TickResult, run_scanner_tick
from cryptobot.strategy.portfolio_loader import V6Portfolio

logger = logging.getLogger(__name__)


# Single worker for the tick body. If a tick times out the worker keeps
# running; TickRunner skips later fires until it finishes rather than start
# overlapping ticks against the same PartialRetEngine and OrderClient.
_TICK_WORKER = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cryptobot-tick")


@dataclass
class TickRunner:
    """Everything needed to drive a tick.

    ``conn_factory`` returns the calling thread's own sqlite3 connection. The
    tick body runs in ``_TICK_WORKER`` and resolves it there; the health-row
    writes in ``run_one_tick`` (timeout, overlap, slow tick) run on the
    scheduler thread and resolve their own. No transaction is ever shared
    across threads.
    """

    conn_factory: ConnFactory
    portfolio: V6Portfolio
    deps_factory: Callable[[], TickDeps]
    notify: Callable[[str, str], None] = field(default=lambda *_: None)
    consecutive_failures: int = 0
    # Alert again at every multiple of CONSECUTIVE_FAILURE_THRESHOLD (5, 10,
    # 15, ...), so a long failure streak does not go quiet after the first
    # alert. ``last_critical_count`` is the failure count at the last alert, so
    # calling run_one_tick twice in the same failing state does not alert
    # twice. A successful tick resets both fields.
    last_critical_count: int = 0
    on_tick: Optional[Callable[[TickResult], None]] = None
    last_result: Optional[TickResult] = None
    active_future: Optional[Future[TickResult]] = field(default=None, repr=False)

    def _should_fire_critical_alert(self) -> bool:
        """Send CRITICAL each time consecutive_failures reaches a multiple of
        the threshold (5, 10, 15, ...).
        """
        threshold = config.CONSECUTIVE_FAILURE_THRESHOLD
        if self.consecutive_failures < threshold:
            return False
        if self.consecutive_failures == self.last_critical_count:
            return False
        if self.consecutive_failures % threshold != 0:
            # Also alert on the first crossing.
            if self.consecutive_failures != threshold:
                return False
        self.last_critical_count = self.consecutive_failures
        return True

    def run_one_tick(self) -> TickResult:
        now_ts = int(time.time())
        hard_deadline_s = float(config.TICK_HARD_DEADLINE_MS) / 1000.0
        # These health-row writes run on the scheduler thread with its own
        # connection; an orphaned worker keeps its own, so this INSERT can
        # never join or commit the worker's open transaction.
        sched_conn = self.conn_factory()
        if self.active_future is not None:
            if not self.active_future.done():
                self.consecutive_failures += 1
                sched_conn.execute(
                    "INSERT INTO pipeline_health (stage, status, message) "
                    "VALUES ('tick', 'CRITICAL', ?)",
                    (
                        "previous tick is still running after hard-deadline "
                        "timeout; skipping this fire to prevent overlapping "
                        "scanner state",
                    ),
                )
                if self._should_fire_critical_alert():
                    self.notify(
                        "CRITICAL",
                        f"{self.consecutive_failures} consecutive tick errors",
                    )
                raise TimeoutError(
                    "previous tick still running after hard-deadline timeout"
                )
            self.active_future = None

        # Resolve the connection inside the worker thread so the tick uses the
        # worker's own connection.
        def _run_tick_body() -> TickResult:
            return run_scanner_tick(
                self.conn_factory(), self.portfolio,
                now_ts=now_ts, deps=self.deps_factory(),
            )

        future = _TICK_WORKER.submit(_run_tick_body)
        self.active_future = future
        try:
            result = future.result(timeout=hard_deadline_s)
        except FuturesTimeoutError:
            # The worker cannot be killed cleanly. Record the timeout in
            # pipeline_health and count it as a failure so the CRITICAL
            # threshold still applies.
            self.consecutive_failures += 1
            sched_conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('tick', 'CRITICAL', ?)",
                (
                    f"tick exceeded hard deadline of "
                    f"{config.TICK_HARD_DEADLINE_MS}ms — orphan worker may "
                    f"complete in background",
                ),
            )
            if self._should_fire_critical_alert():
                self.notify(
                    "CRITICAL",
                    f"{self.consecutive_failures} consecutive tick errors",
                )
            # Keep active_future set; later fires skip until this worker
            # finishes instead of starting a concurrent tick.
            raise TimeoutError(
                f"tick exceeded {config.TICK_HARD_DEADLINE_MS}ms hard deadline"
            )
        except Exception as exc:   # noqa: BLE001 — surface to pipeline_health
            self.active_future = None
            self.consecutive_failures += 1
            sched_conn.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('tick', 'ERROR', ?)",
                (f"{type(exc).__name__}: {exc}",),
            )
            if self._should_fire_critical_alert():
                sched_conn.execute(
                    "INSERT INTO pipeline_health (stage, status, message) "
                    "VALUES ('tick', 'CRITICAL', ?)",
                    (
                        f"{self.consecutive_failures} consecutive tick errors",
                    ),
                )
                self.notify(
                    "CRITICAL",
                    f"{self.consecutive_failures} consecutive tick errors",
                )
            raise

        # Success
        self.active_future = None
        self.consecutive_failures = 0
        self.last_critical_count = 0
        self.last_result = result

        # Slow tick warning, with the per-phase breakdown so a breach can be
        # pinned on spot, Gamma, books or the order without digging through the
        # latency rows.
        if result.duration_ms_total > config.TICK_SOFT_DEADLINE_MS:
            # ``other`` is the time the measured phases do not explain
            # (scoring, walker, SQLite commits, GIL waits). Without it a slow
            # tick whose phases add up to a fraction of the total cannot be
            # explained.
            _known_ms = (
                result.ms_spot + result.ms_live_ctx + result.ms_gamma
                + result.ms_book + result.ms_order_total
            )
            _other_ms = max(0.0, result.duration_ms_total - _known_ms)
            sched_conn.execute(
                "INSERT INTO pipeline_health (stage, status, duration_ms, message) "
                "VALUES ('tick', 'WARN', ?, ?)",
                (
                    int(result.duration_ms_total),
                    f"slow tick: {result.duration_ms_total:.1f}ms > soft deadline "
                    f"{config.TICK_SOFT_DEADLINE_MS}ms "
                    f"(spot={result.ms_spot:.0f} ctx={result.ms_live_ctx:.0f} "
                    f"gamma={result.ms_gamma:.0f}/{result.n_gamma_calls} "
                    f"book={result.ms_book:.0f}/{result.n_book_calls} "
                    f"order={result.ms_order_total:.0f}/{result.n_orders} "
                    f"other={_other_ms:.0f})",
                ),
            )

        if self.on_tick is not None:
            try:
                self.on_tick(result)
            except Exception:   # noqa: BLE001 — observers must not break the tick
                logger.exception("on_tick callback raised")

        return result


def start_scheduler(
    runner: TickRunner,
    *,
    tick_seconds: int | None = None,
    sweep_job: Optional[Callable[[], None]] = None,
    sweep_minutes: int | None = None,
    wallet_recon_job: Optional[Callable[[], None]] = None,
    wallet_recon_minutes: int = 10,
    readiness_job: Optional[Callable[[], None]] = None,
    readiness_seconds: int = 60,
    order_recon_job: Optional[Callable[[], None]] = None,
    order_recon_seconds: int = 60,
    transfer_watchdog_job: Optional[Callable[[], None]] = None,
    transfer_watchdog_seconds: int = 60,
    critical_alert_job: Optional[Callable[[], None]] = None,
    critical_alert_seconds: int = 60,
    requote_drain_job: Optional[Callable[[], None]] = None,
    requote_drain_seconds: int = 2,
    wal_checkpoint_job: Optional[Callable[[], None]] = None,
    wal_checkpoint_seconds: int = 60,
    clob_ping_job: Optional[Callable[[], None]] = None,
    clob_ping_seconds: int = 20,
    miss_forensics_job: Optional[Callable[[], None]] = None,
    miss_forensics_seconds: int = 300,
    book_ws_watchdog_job: Optional[Callable[[], None]] = None,
    book_ws_watchdog_seconds: float = 5.0,
    fast_trigger_job: Optional[Callable[[], None]] = None,
    fast_trigger_seconds: float = 0.0,
    on_job_missed: Optional[Callable[[str], None]] = None,
) -> "BackgroundScheduler":
    """Set up the BackgroundScheduler and its jobs.

    Always schedules the 1 Hz scanner tick. With ``sweep_job`` it also
    schedules the resolution sweep every ``sweep_minutes`` (default
    ``config.RESOLUTION_SWEEP_MINUTES``); without the sweep, PENDING rows never
    settle.

    APScheduler can drop a fire when ``max_instances=1`` and the previous run
    is still going. With ``on_job_missed``, an ``EVENT_JOB_MISSED`` listener
    calls it with the job id so the miss can be recorded (usually in
    pipeline_health).

    Returns the scheduler; the caller must call ``shutdown()`` on exit.
    """
    from apscheduler.events import EVENT_JOB_MISSED
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = BackgroundScheduler(
        job_defaults={"max_instances": 1, "misfire_grace_time": 2, "coalesce": True}
    )
    if on_job_missed is not None:
        def _missed_listener(event) -> None:
            try:
                on_job_missed(event.job_id)
            except Exception:   # noqa: BLE001 — listener errors must not break the scheduler
                logger.exception("on_job_missed callback raised for job_id=%s", event.job_id)
        scheduler.add_listener(_missed_listener, EVENT_JOB_MISSED)
    # Align ticks to whole seconds. A plain IntervalTrigger fires at start time
    # + k * interval, inheriting a random sub-second phase from boot, so a tick
    # meant for second S could run up to ~1s late. Starting at the next whole
    # second pins fires to S.000 plus a few ms of jitter, and since each fire
    # is computed from start_date the alignment never drifts.
    _aligned_start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(
        seconds=1
    )
    scheduler.add_job(
        runner.run_one_tick,
        trigger=IntervalTrigger(
            seconds=tick_seconds or config.TICK_SECONDS,
            start_date=_aligned_start,
        ),
        id="scanner_tick",
        replace_existing=True,
    )
    if sweep_job is not None:
        scheduler.add_job(
            sweep_job,
            trigger=IntervalTrigger(
                minutes=sweep_minutes or config.RESOLUTION_SWEEP_MINUTES
            ),
            id="resolution_sweep",
            replace_existing=True,
        )
    # Live-trading jobs. Each is optional so the bot still boots without a
    # relayer or OrderClient; main.py only passes a job when its dependencies
    # exist.
    if wallet_recon_job is not None:
        scheduler.add_job(
            wallet_recon_job,
            trigger=IntervalTrigger(minutes=wallet_recon_minutes),
            id="wallet_recon",
            replace_existing=True,
        )
    if readiness_job is not None:
        scheduler.add_job(
            readiness_job,
            trigger=IntervalTrigger(seconds=readiness_seconds),
            id="live_readiness",
            replace_existing=True,
        )
    # Order reconciliation, on a short cadence when an OrderClient exists, so
    # fills are recorded and stale SUBMITTING reservations are released instead
    # of holding group cap forever.
    if order_recon_job is not None:
        scheduler.add_job(
            order_recon_job,
            trigger=IntervalTrigger(seconds=order_recon_seconds),
            id="order_reconciliation",
            replace_existing=True,
        )
    # Ping the CLOB signing pool (``GET /ok``) so the order connection never
    # idles past its 65s keepalive (a dropped 60s balance read would otherwise
    # mean a cold TCP+TLS handshake inside the next order POST). Only when
    # main.py passes the job (OrderClient present) and the cadence is positive;
    # the skip and timeout logic is in OrderClient.ping_order_pool.
    if clob_ping_job is not None and clob_ping_seconds > 0:
        scheduler.add_job(
            clob_ping_job,
            trigger=IntervalTrigger(seconds=clob_ping_seconds),
            id="clob_ping",
            replace_existing=True,
        )
    # WS mirror stall watchdog: REST cross-check of tokens whose WS ladder has
    # not updated for a while. Only when main.py passes the job (mirror
    # authoritative, watchdog enabled) and the cadence is positive.
    if book_ws_watchdog_job is not None and book_ws_watchdog_seconds > 0:
        scheduler.add_job(
            book_ws_watchdog_job,
            trigger=IntervalTrigger(seconds=book_ws_watchdog_seconds),
            id="book_ws_watchdog",
            replace_existing=True,
        )
    if transfer_watchdog_job is not None:
        scheduler.add_job(
            transfer_watchdog_job,
            trigger=IntervalTrigger(seconds=transfer_watchdog_seconds),
            id="transfer_watchdog",
            replace_existing=True,
        )
    # Forward new CRITICAL pipeline_health rows (drawdown halt, breaker trips,
    # boot problems, ...) to Telegram so nothing recorded in the database goes
    # unnoticed.
    if critical_alert_job is not None:
        scheduler.add_job(
            critical_alert_job,
            trigger=IntervalTrigger(seconds=critical_alert_seconds),
            id="critical_alert",
            replace_existing=True,
        )
    # Reject re-quote telemetry: re-read the book ~1s after each live FAK
    # reject and record requote_ask/age. Observation only. Always scheduled
    # (only needs the read-only CLOB client). Keep the cadence at ~1-2s:
    # entries happen in the last 30 seconds, so a slow drain would read the
    # near-final book and measure the outcome instead of whether the order
    # could have filled.
    if requote_drain_job is not None:
        scheduler.add_job(
            requote_drain_job,
            trigger=IntervalTrigger(seconds=requote_drain_seconds),
            id="reject_requote_drain",
            replace_existing=True,
        )
    # Periodic PASSIVE WAL checkpoint on a scheduler thread, so the tick never
    # pays the checkpoint fsync (db/connection.py raises wal_autocheckpoint to
    # 20000 pages to keep the inline trigger out of reach).
    if wal_checkpoint_job is not None and wal_checkpoint_seconds > 0:
        scheduler.add_job(
            wal_checkpoint_job,
            trigger=IntervalTrigger(seconds=wal_checkpoint_seconds),
            id="wal_checkpoint",
            replace_existing=True,
        )
    # On-chain miss forensics: classifies each rejected FAK as FASTER_TAKER,
    # REPRICED_ONLY or MAKER_CANCEL from the trade tape and stores the verdict
    # for the dashboard. Never reads or writes anything the decision path uses,
    # and per-run call budgets cap the cost of an outage. Only when main.py
    # passes the job; 0 seconds disables it.
    if miss_forensics_job is not None and miss_forensics_seconds > 0:
        scheduler.add_job(
            miss_forensics_job,
            trigger=IntervalTrigger(seconds=miss_forensics_seconds),
            id="miss_forensics",
            replace_existing=True,
        )
    # Fast-trigger sweep: re-evaluate qualifying sub-minute cells every
    # FAST_TRIGGER_MS (default 250), so an ask that appears mid-second is seen
    # in ~125ms on average instead of ~500ms. Outside an entry window it
    # returns after a few integer comparisons. max_instances=1 and coalesce
    # (job_defaults) stop a slow scan from overlapping itself, and the scan
    # skips rather than waits when the tick holds the order lock. Only when
    # main.py passes the job; 0 seconds disables it.
    if fast_trigger_job is not None and fast_trigger_seconds > 0:
        scheduler.add_job(
            fast_trigger_job,
            trigger=IntervalTrigger(seconds=fast_trigger_seconds),
            id="fast_trigger_sweep",
            replace_existing=True,
        )
    scheduler.start()
    return scheduler
