"""APScheduler wrapper around run_scanner_tick.

`IntervalTrigger(seconds=TICK_SECONDS)` with:
  - `max_instances=1` — never queue a second copy of the tick
  - `misfire_grace_time=2` — drop a missed tick rather than thunder-herd

Error pattern:
  - per-tick exception writes a row to `pipeline_health` + increments a
    module-level consecutive-failure counter
  - 5 consecutive failures fire CRITICAL via the operator's notify
    callback (R-NEW-14)
  - successful tick resets the counter to 0

Hard-deadline enforcement (R-NEW-15): the tick runs in a worker thread
with a ``concurrent.futures.ThreadPoolExecutor.submit().result(timeout)``
wrapper. ``TICK_HARD_DEADLINE_MS`` aborts the wait (the worker may still
finish in the background, but the scheduler is freed to start the next
tick). A CRITICAL pipeline_health row records the timeout.

``start_scheduler(...)`` returns the BackgroundScheduler; the caller is
responsible for stopping it on shutdown (U14's main.py installs the
SIGTERM handler).
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

# Audit w19oxqf24 finding #1 (P0): the tick body must not share a
# sqlite3.Connection with the scheduler-job threads. ``conn_factory`` returns
# the *calling* thread's own connection (a ``ThreadLocalConnections`` handle),
# so the tick worker thread and the scheduler thread each get their own — and
# the cross-thread write serialization is provided by SQLite's WAL file lock +
# BEGIN IMMEDIATE rather than the in-process db_write_lock the tick never held.
ConnFactory = Callable[[], sqlite3.Connection]

from cryptobot import config
from cryptobot.execution.pipeline import TickDeps, TickResult, run_scanner_tick
from cryptobot.strategy.portfolio_loader import V6Portfolio

logger = logging.getLogger(__name__)


# Single worker for the tick body. If a tick times out, the worker keeps
# running in the background; TickRunner skips subsequent fires until that
# orphan finishes rather than starting overlapping ticks against the
# shared sqlite connection, PartialRetEngine, and OrderClient.
_TICK_WORKER = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cryptobot-tick")


@dataclass
class TickRunner:
    """Closure carrying everything needed to drive a tick.

    ``conn_factory`` returns the calling thread's own sqlite3 connection. The
    tick body (``run_scanner_tick``) runs inside ``_TICK_WORKER`` and resolves
    the factory there, so it operates on the worker thread's connection; the
    health-row writes in ``run_one_tick`` (timeout/overlap/slow paths) run on
    the scheduler thread and resolve the factory there. Each gets its own
    connection — no shared transaction state across threads (audit finding #1).
    """

    conn_factory: ConnFactory
    portfolio: V6Portfolio
    deps_factory: Callable[[], TickDeps]
    notify: Callable[[str, str], None] = field(default=lambda *_: None)
    consecutive_failures: int = 0
    # Reliability finding #4: `last_critical_logged` was set True at the
    # first threshold breach and only cleared on a successful tick. Once
    # the bot was in a sustained failure mode the alert never re-fired
    # for failures 6, 7, 8, ... — silent indefinite breakage. We now
    # re-fire on every multiple of CONSECUTIVE_FAILURE_THRESHOLD (5, 10,
    # 15, ...). `last_critical_count` is the consecutive_failures count
    # at the most-recent fire; equality short-circuits a re-fire if the
    # caller calls run_one_tick twice with the same failing state. A
    # successful tick resets both fields.
    last_critical_count: int = 0
    on_tick: Optional[Callable[[TickResult], None]] = None
    last_result: Optional[TickResult] = None
    active_future: Optional[Future[TickResult]] = field(default=None, repr=False)

    def _should_fire_critical_alert(self) -> bool:
        """Fire CRITICAL when consecutive_failures crosses a threshold
        boundary. Re-fires at every threshold multiple (5, 10, 15, ...)
        rather than once-ever."""
        threshold = config.CONSECUTIVE_FAILURE_THRESHOLD
        if self.consecutive_failures < threshold:
            return False
        if self.consecutive_failures == self.last_critical_count:
            return False
        if self.consecutive_failures % threshold != 0:
            # Fire on the first crossing too -- handle cases where
            # threshold is e.g. 5 and a single tick increments by 1.
            if self.consecutive_failures != threshold:
                return False
        self.last_critical_count = self.consecutive_failures
        return True

    def run_one_tick(self) -> TickResult:
        now_ts = int(time.time())
        hard_deadline_s = float(config.TICK_HARD_DEADLINE_MS) / 1000.0
        # Health-row writes below run on the scheduler thread; resolve the
        # scheduler thread's own connection (audit finding #1, #10). The
        # orphan worker keeps its separate worker-thread connection, so this
        # INSERT can never join or commit the worker's open transaction.
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

        # Resolve the connection INSIDE the worker thread so the tick body
        # uses the worker's own connection, not the scheduler thread's.
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
            # The worker thread cannot be killed cleanly, but we surface the
            # timeout to pipeline_health and treat it as a tick failure so
            # the consecutive-failure threshold can still fire CRITICAL.
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
            # Keep active_future set; later fires will skip until this
            # orphan finishes instead of launching a concurrent tick.
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

        # Success path
        self.active_future = None
        self.consecutive_failures = 0
        self.last_critical_count = 0
        self.last_result = result

        # Slow tick warning (R-NEW-15). A0: include the per-phase breakdown
        # so a breach is attributable (spot vs gamma vs book vs order) without
        # cross-referencing the stage='latency' trace rows.
        if result.duration_ms_total > config.TICK_SOFT_DEADLINE_MS:
            # `other` = wall time the instrumented phases don't explain
            # (scoring, walker, sqlite commits, GIL waits). Without it a
            # breach whose phase numbers sum to a fraction of the total
            # reads as unattributable (2026-07-03: ~1000ms ticks showed
            # only ~270ms of instrumented phases).
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
    """Wire the BackgroundScheduler with the IntervalTrigger jobs.

    Always schedules the 1Hz scanner tick. When ``sweep_job`` is supplied,
    also schedules the F3 resolution sweep at ``sweep_minutes`` cadence
    (defaults to ``config.RESOLUTION_SWEEP_MINUTES``). The plan documents
    "step 7: 1Hz tick + 5min F3 sweep" — without the sweep registration,
    PENDING ledger rows never settle.

    Review-pass #18 closeout: APScheduler can drop a fire when
    ``max_instances=1`` AND a prior fire is still running past the next
    schedule time. The default ``misfire_grace_time=2`` masks this in
    most cases but a long-cadence job is exposed if a load stalls. When
    ``on_job_missed`` is supplied, an APScheduler
    ``EVENT_JOB_MISSED`` listener calls it with the job id so the
    caller can surface the miss (typically into pipeline_health).

    Returns the scheduler instance — caller must hold it + call
    ``shutdown()`` on process exit.
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
    # A6 latency: anchor the tick to wall-clock second boundaries. A bare
    # IntervalTrigger fires at scheduler-start + k*interval — a boot-random
    # sub-second phase (e.g. XX.734) that every fire then inherits, so a
    # checkpoint targeting second S was evaluated up to ~1s late (now_ts is
    # int(time.time()) at fire time and truncates back to S). Pinning
    # start_date to the next whole second locks fires to S.000 + scheduler
    # wakeup jitter (single-digit ms). IntervalTrigger computes each next
    # fire from start_date, so alignment never drifts.
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
    # Per-strategy LIVE deploy schedulers (U7 / U8 / U9 / U10). Each is
    # optional so Phase 1 / Phase 2 deploys without the relayer / OrderClient
    # can still boot — the jobs are wired in main.py only when the
    # corresponding execution surface is available.
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
    # Audit w19oxqf24 findings #3 + #6: reconcile_open_orders /
    # reconcile_stale_attempts ship and are tested but were never scheduled, so
    # on a LIVE deploy fills were never reconciled and stale SUBMITTING
    # reservations never released (permanently consuming the group cap). Wire
    # the order-reconciliation job on a short cadence when an OrderClient
    # exists.
    if order_recon_job is not None:
        scheduler.add_job(
            order_recon_job,
            trigger=IntervalTrigger(seconds=order_recon_seconds),
            id="order_reconciliation",
            replace_existing=True,
        )
    # Latency finding #8: order-socket warmth ping — GET /ok on the CLOB
    # signing pool so the hot connection never idles past keepalive_expiry=65s
    # when a 60s readiness fire is dropped (misfire_grace_time=2 drops, not
    # delays — a ~120s gap otherwise guarantees a cold TCP+TLS handshake
    # inside the next order POST). Wired only when main.py supplies the job
    # (OrderClient present) AND the cadence is positive
    # (CRYPTOBOT_CLOB_PING_SECONDS=0 keeps the job set byte-identical); the
    # skip-if-recent guard + short timeout live in OrderClient.ping_order_pool.
    if clob_ping_job is not None and clob_ping_seconds > 0:
        scheduler.add_job(
            clob_ping_job,
            trigger=IntervalTrigger(seconds=clob_ping_seconds),
            id="clob_ping",
            replace_existing=True,
        )
    # WS mirror stall watchdog (operator order 2026-08-06): off-tick REST
    # cross-check of SUSPECT (old-ladder) mirror tokens. Wired only when main.py
    # supplies the job (mirror authoritative + watchdog enabled + venue mirror
    # exposes the API) AND the cadence is positive, so the flag-off job set is
    # byte-identical.
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
    # Catch-all operator alert: forward NEW CRITICAL pipeline_health rows
    # (dd_halt, subminute breaker, boot gaps, ...) to Telegram so an issue
    # recorded in the DB is never silent. Wired in main.py.
    if critical_alert_job is not None:
        scheduler.add_job(
            critical_alert_job,
            trigger=IntervalTrigger(seconds=critical_alert_seconds),
            id="critical_alert",
            replace_existing=True,
        )
    # Reject re-quote telemetry drain: re-reads the book ~1s after each LIVE
    # FAK reject and back-fills requote_ask/age. Observation only — no order is
    # placed. Always wired (needs only the read-only CLOB client). The cadence
    # MUST stay ~1-2s: entries land in the last-30s window, so a slow drain
    # re-reads the near-terminal book and the telemetry measures the market
    # OUTCOME instead of post-reject fillability (at the original 60s cadence
    # realized requote_age_ms was p50≈32s / p90≈57s — 2026-07-11 EC2 snapshot).
    if requote_drain_job is not None:
        scheduler.add_job(
            requote_drain_job,
            trigger=IntervalTrigger(seconds=requote_drain_seconds),
            id="reject_requote_drain",
            replace_existing=True,
        )
    # Latency item #4: periodic PASSIVE WAL checkpoint. The tick thread must
    # never pay the checkpoint fsync — db/connection.py raises
    # wal_autocheckpoint to 20000 pages so the inline (writer-borne) trigger
    # is effectively unreachable, and this job drains the WAL on a
    # scheduler-pool thread instead (PASSIVE never blocks readers/writers).
    if wal_checkpoint_job is not None and wal_checkpoint_seconds > 0:
        scheduler.add_job(
            wal_checkpoint_job,
            trigger=IntervalTrigger(seconds=wal_checkpoint_seconds),
            id="wal_checkpoint",
            replace_existing=True,
        )
    # On-chain miss forensics (CRYPTOBOT_MISS_FORENSICS_ENABLED, default ON):
    # classifies each new venue-rejected FAK as FASTER_TAKER / REPRICED_ONLY /
    # MAKER_CANCEL from the trade tape and persists the verdict to
    # miss_forensics for the dashboard. Pure observability — it never reads or
    # writes anything the decision path touches, and its per-run HTTP/RPC
    # budgets bound the cost of a venue outage. Wired ONLY when main.py passes
    # the job (flag on AND the venue is Polymarket); 0 seconds disables just
    # the job, keeping the job set byte-identical.
    if miss_forensics_job is not None and miss_forensics_seconds > 0:
        scheduler.add_job(
            miss_forensics_job,
            trigger=IntervalTrigger(seconds=miss_forensics_seconds),
            id="miss_forensics",
            replace_existing=True,
        )
    # Fast-trigger SWEEP rung (CRYPTOBOT_FAST_TRIGGER_ENABLED, default ON):
    # re-evaluates this instant's qualifying sub-minute cells every
    # FAST_TRIGGER_MS (default 250) so a fillable ask that appears mid-second is
    # contested in ~125ms mean instead of ~500ms. Outside a qualifying offset
    # window the job returns after pure integer math over the sleeves' offset
    # sets, so the cadence is ~free ~90% of the time — the 100ms
    # fast_requote_drain job above is the proof that sub-second APScheduler jobs
    # are fine on this box. max_instances=1 + coalesce (job_defaults) mean a slow
    # scan never overlaps itself or thunder-herds, and the scan itself abandons
    # on a contended single-flight lock rather than queueing behind the tick.
    # Wired ONLY when main.py passes the job (flag on); 0 seconds disables just
    # the job, keeping the job set byte-identical.
    if fast_trigger_job is not None and fast_trigger_seconds > 0:
        scheduler.add_job(
            fast_trigger_job,
            trigger=IntervalTrigger(seconds=fast_trigger_seconds),
            id="fast_trigger_sweep",
            replace_existing=True,
        )
    scheduler.start()
    return scheduler
