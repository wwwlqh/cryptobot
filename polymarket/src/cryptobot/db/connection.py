"""SQLite connection + schema bootstrap.

Mirrors `hightempbot/db/connection.py` pragma set:
  - journal_mode = WAL
  - synchronous = NORMAL
  - foreign_keys = ON
  - cache_size = -32_000 (32 MB)
  - journal_size_limit = 200_000_000

The `init_db()` entrypoint applies `schema.sql` once and then runs idempotent
`_migrate_db()` for future schema evolution (additive only — never destructive).

D-LIVE-15: schema migration is portfolio-free. Portfolio-dependent backfills
(`backfill_wallet_groups`, `seed_wallet_modes`) run AFTER `load_sleeves()` in
the boot path, so the source of truth for the wallet→group mapping exists
before the UPDATE fires.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Audit w19oxqf24 finding #1 (P0): one sqlite3.Connection was shared across the
# tick worker thread, every scheduler-job thread, and the dashboard POST thread.
# Because BEGIN IMMEDIATE / COMMIT / ROLLBACK operate on connection-global
# transaction state (not per-thread), a background job's commit() could
# prematurely commit the tick's open BEGIN IMMEDIATE transaction, and a tick's
# rollback could discard a sweep's settlement. The in-process db_write_lock only
# serialized the *jobs* against each other — the tick never participated.
#
# `busy_timeout=5000` makes a writer that hits SQLite's file lock wait-and-retry
# for up to 5s instead of raising "database is locked" immediately — the correct
# behavior once each thread owns its own connection and relies on SQLite's WAL
# file-level locking + BEGIN IMMEDIATE to serialize writers.
_BUSY_TIMEOUT_MS = 5000


class _PortfolioLike(Protocol):
    """Structural type for backfill helpers — keeps connection.py decoupled
    from the concrete V6Portfolio so unit tests can pass minimal stubs."""

    def wallet_group_for(self, wallet_id: str) -> str | None: ...

    @property
    def market_sleeves(self) -> Iterable[object]: ...


def utc_now_sql() -> str:
    """Current UTC time as `YYYY-MM-DD HH:MM:SS` (SQLite text format)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open (or create) a SQLite database with WAL pragmas applied.

    `check_same_thread=False` lets the scheduler thread and the SSE broadcast
    thread share a connection pool safely; SQLite serializes writes via its
    internal lock and the `BEGIN IMMEDIATE` slot-lock pattern guards
    concurrent slot writes (R-NEW-9).
    """
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # isolation_level=None puts the connection in autocommit mode so
    # explicit BEGIN IMMEDIATE / COMMIT / ROLLBACK control transaction
    # boundaries — required by the R-NEW-9 slot lock pattern (Python's
    # default sqlite3 auto-begins, which clashes with our explicit
    # BEGIN IMMEDIATE).
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-32000")
    conn.execute("PRAGMA journal_size_limit=200000000")
    # Latency item #4: the tick thread must never pay the checkpoint fsync.
    # SQLite's default wal_autocheckpoint=1000 pages runs the checkpoint
    # INLINE on whichever writer's commit crosses the threshold — at 1 Hz
    # that is usually the tick's own signals/ledger autocommit, stalling the
    # decision path for the WAL→db fsync. Raising the bound to 20000 pages
    # makes the inline trigger effectively unreachable between the periodic
    # PASSIVE checkpoints (the wal_checkpoint scheduler job in main.py, which
    # runs on a scheduler-pool thread and never blocks readers/writers).
    # journal_size_limit=200MB above still bounds WAL disk if the periodic
    # job ever stalls.
    conn.execute("PRAGMA wal_autocheckpoint=20000")
    # Audit finding #1: each thread owns its own connection, so a writer that
    # finds the WAL write-lock held must wait (and retry) rather than fail. With
    # one connection per writer thread, BEGIN IMMEDIATE on connection A blocks
    # BEGIN IMMEDIATE on connection B at the SQLite file level until A commits —
    # true cross-thread serialization that the old shared connection never had.
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    return conn


def passive_wal_checkpoint(conn: sqlite3.Connection) -> None:
    """Run ``PRAGMA wal_checkpoint(PASSIVE)`` — the off-hot-path half of the
    latency item #4 pairing with ``wal_autocheckpoint=20000`` above.

    PASSIVE copies as many WAL frames into the main db as it can WITHOUT
    blocking any reader or writer (it simply stops at the first busy page),
    so it is safe to fire from a scheduler-pool thread at any time. Called
    periodically from main.py's ``wal_checkpoint`` job so the WAL is drained
    on a background thread's clock instead of ambushing the tick's commit
    with an inline checkpoint fsync. A no-op (single harmless row) on a
    non-WAL / in-memory database.
    """
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()


class ThreadLocalConnections:
    """One sqlite3 connection per thread, opened lazily on first use.

    Audit w19oxqf24 finding #1 (P0): the live scanner runs the tick body in a
    dedicated worker thread while APScheduler runs the F3 sweep / lut-reload /
    reconciliation jobs on its own thread pool, and the dashboard serves POSTs
    on yet another thread. Sharing a single ``sqlite3.Connection`` across those
    threads is unsafe: transaction boundaries (``BEGIN IMMEDIATE`` / ``COMMIT``
    / ``ROLLBACK``) are connection-global, so one thread's ``commit()`` ends
    another thread's open transaction.

    This handle hands each thread its own connection. SQLite's WAL file-level
    locking plus the ``busy_timeout`` set in :func:`get_connection` then provide
    the cross-thread write serialization the shared connection lacked: a second
    thread's ``BEGIN IMMEDIATE`` blocks until the first thread commits, instead
    of splicing into its transaction.

    Thread-safe: ``get()`` guards the registry with a lock. ``close_all()`` is
    called once on shutdown from the main thread.
    """

    def __init__(
        self, db_path: str | Path, *, shared: sqlite3.Connection | None = None
    ) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._all: list[sqlite3.Connection] = []
        # A ``:memory:`` database is private to each connection — a second
        # connection sees an empty DB. So per-thread isolation is impossible
        # there; every thread must share one connection. This is only the
        # test / ``--once`` smoke path (production uses a file-backed DB).
        # An explicit ``shared`` connection forces the same single-connection
        # mode regardless of path.
        self._shared: sqlite3.Connection | None = shared
        if self._shared is None and str(db_path) == ":memory:":
            self._shared = get_connection(db_path)
        if self._shared is not None:
            self._all.append(self._shared)

    def get(self) -> sqlite3.Connection:
        """Return this thread's connection, opening one on first call.

        In shared mode (``:memory:`` or an injected connection) every thread
        gets the same connection — the only safe option for an in-memory DB.
        """
        if self._shared is not None:
            return self._shared
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = get_connection(self._db_path)
            self._local.conn = conn
            with self._lock:
                self._all.append(conn)
        return conn

    def __call__(self) -> sqlite3.Connection:
        # Convenience so the handle can be passed anywhere a zero-arg
        # ``conn_factory()`` callable is expected.
        return self.get()

    def register_current_thread(self, conn: sqlite3.Connection) -> None:
        """Adopt an already-open connection as the calling thread's own.

        Used by the boot path so the schema-bootstrap connection returned by
        :func:`init_db` becomes the main thread's connection (and is tracked
        for ``close_all``) rather than being orphaned. No-op in shared mode.
        """
        if self._shared is not None:
            return
        self._local.conn = conn
        with self._lock:
            if conn not in self._all:
                self._all.append(conn)

    def register_extra(self, conn: sqlite3.Connection) -> None:
        """Track a connection opened outside this handle so ``close_all``
        closes it on shutdown (e.g. the dashboard's dedicated connection).
        Not bound to any thread's thread-local slot."""
        with self._lock:
            if conn not in self._all:
                self._all.append(conn)

    def close_all(self) -> None:
        """Close every connection opened through this handle.

        Called once on process shutdown. Connections opened on threads that
        have since exited are still closed here because they are tracked in
        ``self._all`` regardless of which thread opened them.
        """
        with self._lock:
            conns = list(self._all)
            self._all.clear()
        for conn in conns:
            try:
                conn.close()
            except sqlite3.Error:
                pass


def _migrate_db(conn: sqlite3.Connection) -> None:
    """Idempotent additive migrations.

    `CREATE TABLE IF NOT EXISTS` handles new tables. This function adds
    columns or indexes that may be missing on an older DB. Nothing is
    destructive — operator can always roll forward.
    """
    # Review #2: strike_price column. Captures the underlying crypto spot at
    # decision time so the F3 sweep can compute won_up = (resolution_price >
    # strike_price). Older rows have NULL strike_price → sweep falls back to
    # yes_pre1_anchor for backward compatibility.
    _add_column_if_missing(conn, "ledger", "strike_price", "REAL")

    # Divergence #8 (2026-05-23): standard live settlement now records Gamma's
    # final Polymarket outcomePrices here. Legacy rows remain NULL, and the
    # oracle spot path is only used when no Polymarket resolver is wired.
    _add_column_if_missing(conn, "ledger", "polymarket_resolution_price", "REAL")
    _add_column_if_missing(conn, "ledger", "polymarket_resolution_source", "TEXT")

    # Divergence #13 fix (2026-05-21): window_open_spot is the live spot
    # at the window-open boundary, the same anchor the model trains
    # against and Polymarket Chainlink settles on. Populated by pipeline
    # at bet time from PartialRetEngine.window_open_spot(); consumed by
    # resolution_sweep._decide_won_up which prefers it over strike_price
    # (decision-time spot). Legacy rows have NULL → sweep falls back to
    # strike_price for backward compatibility. See schema.sql for the
    # column docstring + divergence #13 in AGENTS.md.
    _add_column_if_missing(conn, "ledger", "window_open_spot", "REAL")

    # Portfolio-v2 pair metadata. Nullable for legacy V6 rows; populated for
    # the two atomic legs of each admitted pair.
    _add_column_if_missing(conn, "ledger", "pair_group_id", "TEXT")
    _add_column_if_missing(conn, "ledger", "pair_role", "TEXT")
    _add_column_if_missing(conn, "ledger", "pair_component", "TEXT")
    _add_column_if_missing(conn, "ledger", "paired_coin", "TEXT")

    # Per-strategy dry-run wallets. Legacy rows can infer wallet_id from
    # sleeve/pair_component, so the migration is additive and nullable.
    _add_column_if_missing(conn, "ledger", "wallet_id", "TEXT")
    _add_column_if_missing(conn, "signals", "wallet_id", "TEXT")

    # D-LIVE-3 / D-LIVE-28 / D-LIVE-29: per-strategy LIVE/DRY mode + CLOB
    # linkage + pair-orphan flag + VWAP forensic columns. Additive on
    # legacy DBs; fresh installs get them from schema.sql directly.
    _add_column_if_missing(conn, "ledger", "mode", "TEXT NOT NULL DEFAULT 'DRY'")
    _add_column_if_missing(conn, "ledger", "wallet_group", "TEXT")
    _add_column_if_missing(conn, "ledger", "live_order_id", "TEXT")
    _add_column_if_missing(conn, "ledger", "live_client_order_id", "TEXT")
    _add_column_if_missing(conn, "ledger", "live_filled_size", "REAL")
    _add_column_if_missing(conn, "ledger", "live_filled_price", "REAL")
    _add_column_if_missing(conn, "ledger", "live_fill_ts", "TEXT")
    _add_column_if_missing(conn, "ledger", "pair_orphan", "INTEGER NOT NULL DEFAULT 0")
    _add_column_if_missing(conn, "ledger", "pair_top_up_used", "INTEGER NOT NULL DEFAULT 0")
    _add_column_if_missing(conn, "ledger", "top_ask_at_decision", "REAL")
    # U3 of the all_4 deploy plan: ledger rows written under post-haircut
    # `realized_edge` semantics carry edge_semantics_version=2. Legacy rows
    # stay NULL (reserve 1 for the pre-U3 implicit semantic if a backfill
    # is ever desired). Column is informational; no gate consumes it.
    _add_column_if_missing(conn, "ledger", "edge_semantics_version", "INTEGER")
    _add_column_if_missing(conn, "signals", "mode", "TEXT")
    _add_column_if_missing(conn, "signals", "wallet_group", "TEXT")

    # B1 sub-minute checkpoint add-on: offset_sec = seconds from window open
    # at which the cell/bet was admitted. NULL on every baseline (minute-
    # boundary) row — the sub-minute path is the ONLY writer, which is what
    # lets the fill-rate breaker + dashboards split sub-minute vs baseline
    # PnL with a bare IS NOT NULL.
    _add_column_if_missing(conn, "ledger", "offset_sec", "INTEGER")
    _add_column_if_missing(conn, "signals", "offset_sec", "INTEGER")

    # PURE_STOP_04 model stop-loss (operator 2026-07-09): early-exit metadata
    # written ONCE on the single successful stop-sell (outcome also transitions
    # to CANCELLED so the sweep skips the row and pnl_net is booked once).
    # Additive + nullable so legacy rows read back as "never stopped".
    _add_column_if_missing(conn, "ledger", "exit_price", "REAL")
    _add_column_if_missing(conn, "ledger", "exit_ts", "TEXT")
    _add_column_if_missing(conn, "ledger", "exit_kind", "TEXT")

    # T3 scale-in top-up tag (execution/scale_in.py): 1 on a T3 scale-in top-up
    # row so live grading can separate T3 top-ups from entries and from generic
    # within-minute top-ups. Additive + defaulted 0 so every existing caller and
    # legacy row is byte-identical. schema.sql seeds it on fresh installs; this
    # covers legacy DBs. The durable no-dip state table is created below in
    # _ensure_per_strategy_live_tables.
    _add_column_if_missing(
        conn, "ledger", "scalein_topup", "INTEGER NOT NULL DEFAULT 0"
    )

    # Venue dimension (deliverable 5): the trading venue each bet was placed on
    # ('polymarket' default / 'predictfun'). Additive + DEFAULT 'polymarket', so
    # (1) every legacy ledger row reads back as 'polymarket' after the ALTER —
    # the CRYPTOBOT_VENUE-unset path stays byte-identical, and (2) new bets are
    # stamped with config.VENUE by record_bet. schema.sql seeds it on fresh
    # installs; this covers legacy DBs (guarded by the PRAGMA table_info check
    # inside _add_column_if_missing). Idempotent: re-running init_db is a no-op.
    _add_column_if_missing(
        conn, "ledger", "venue", "TEXT NOT NULL DEFAULT 'polymarket'"
    )

    # New per-strategy LIVE tables. schema.sql's CREATE TABLE IF NOT EXISTS
    # handles fresh installs; explicit creation here makes legacy migrations
    # self-contained even if an operator only ran a partial schema.sql apply.
    _ensure_per_strategy_live_tables(conn)

    # v2 reject-retry (operator 2026-07-09): attempt = 1 on the first FAK, 2 on
    # the one-shot post-reject retry. Additive + nullable-defaulted so legacy
    # subminute_fills rows read back as attempt=1 (the pre-retry semantics).
    # Runs AFTER _ensure_per_strategy_live_tables so subminute_fills exists;
    # guarded by the PRAGMA table_info check inside _add_column_if_missing.
    _add_column_if_missing(conn, "subminute_fills", "attempt", "INTEGER DEFAULT 1")

    # Fast-trigger provenance (operator 2026-07-30): which trigger rung sent the
    # order — 'tick' (1Hz scanner), 'fast_sweep' (sub-second sweep job) or
    # 'fast_event' (WS book-update push). Additive + defaulted so every legacy
    # row reads back as 'tick', which is exactly what those rows were. Same
    # ordering constraint as `attempt` above (after
    # _ensure_per_strategy_live_tables so subminute_fills exists).
    _add_column_if_missing(
        conn, "subminute_fills", "trigger_src", "TEXT DEFAULT 'tick'"
    )

    # Composite slot-key index — primary access pattern for slot_state() at 1Hz.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_slot "
        "ON ledger(coin, sleeve, entry_minute, side, window_start_ts, outcome)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_bet_ts ON ledger(bet_ts)"
    )
    # count_window_entries() covering index — mirrors schema.sql so the LIVE DB
    # picks it up without a rebuild. Load-bearing since the 2026-07-25
    # every-qualifying order dropped the multifill equity floor to 0: the tally
    # now runs on every sub-minute qualifying second, and without this index it
    # falls back to a coin-partition scan of the ledger.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_window_entries "
        "ON ledger(coin, interval_min, window_start_ts, is_topup, outcome)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_pair_group ON ledger(pair_group_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_wallet_outcome "
        "ON ledger(wallet_id, outcome)"
    )
    # Group-cap fast path + pair-orphan banner scans (D-LIVE-14 / D-LIVE-22).
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_mode_group_outcome "
        "ON ledger(mode, wallet_group, outcome)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_pair_orphan "
        "ON ledger(pair_orphan) WHERE pair_orphan > 0"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals(outcome, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_wallet_created "
        "ON signals(wallet_id, created_at)"
    )
    # PERF-03 index retired 2026-07-14: its ONLY consumer was the dashboard
    # _gates_block GROUP BY, removed with the retired Live markets & gates
    # tab. The DROP migrates existing installs so the hot 1 Hz signals
    # writer stops paying maintenance on a dead index.
    conn.execute("DROP INDEX IF EXISTS idx_signals_created_at_skip_reason")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_health_stage_created "
        "ON pipeline_health(stage, created_at DESC)"
    )
    conn.commit()


def _ensure_per_strategy_live_tables(conn: sqlite3.Connection) -> None:
    """Idempotent CREATE TABLE IF NOT EXISTS for every new per-strategy
    LIVE table. schema.sql is the source of truth for fresh installs, but
    legacy DBs that booted before this plan landed need the same DDL
    re-applied here. Keep the column lists in lockstep with schema.sql.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS wallet_mode_state (
            wallet_id   TEXT PRIMARY KEY,
            mode        TEXT NOT NULL DEFAULT 'DRY',
            updated_by  TEXT,
            updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
            version     INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS operator_control_state (
            id            INTEGER PRIMARY KEY CHECK (id = 1),
            state         TEXT NOT NULL DEFAULT 'STOPPED_PROCESSING',
            boot_dry_run  INTEGER,
            reason        TEXT,
            updated_by    TEXT,
            updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
            version       INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS operator_control_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            scope       TEXT NOT NULL DEFAULT 'global',
            action      TEXT NOT NULL,
            from_state  TEXT,
            to_state    TEXT,
            actor       TEXT NOT NULL,
            reason      TEXT,
            detail      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_op_events_scope_created
            ON operator_control_events(scope, created_at DESC);

        CREATE TABLE IF NOT EXISTS live_readiness_reports (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            status            TEXT NOT NULL,
            mode              TEXT,
            funder            TEXT,
            signer            TEXT,
            clob_balance_usd  REAL,
            chain_balance_usd REAL,
            report_json       TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_readiness_created
            ON live_readiness_reports(created_at DESC);

        CREATE TABLE IF NOT EXISTS entry_scan_latches (
            created_at              TEXT NOT NULL DEFAULT (datetime('now')),
            coin                    TEXT NOT NULL,
            sleeve                  TEXT NOT NULL,
            entry_minute            INTEGER NOT NULL,
            window_start_ts         INTEGER NOT NULL,
            PRIMARY KEY (coin, sleeve, entry_minute, window_start_ts)
        );

        CREATE TABLE IF NOT EXISTS bankroll_peak (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            sampled_at        TEXT NOT NULL DEFAULT (datetime('now')),
            wallet_balance    REAL NOT NULL,
            realized_pnl      REAL NOT NULL,
            pending_exposure  REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_bankroll_peak_sampled
            ON bankroll_peak(sampled_at DESC);

        -- Operator-recorded account-level cash events merged into the equity
        -- replay (execution/capital.py). Sign is pinned per kind by the CHECK
        -- so `amount_usd` can be added blindly; 'adjustment' is either-way.
        CREATE TABLE IF NOT EXISTS capital_flows (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at       TEXT NOT NULL,
            kind              TEXT NOT NULL CHECK (
                                  kind IN ('deposit', 'withdrawal', 'rebate', 'adjustment')
                              ),
            amount_usd        REAL NOT NULL CHECK (
                                  (kind = 'deposit'    AND amount_usd > 0)
                               OR (kind = 'rebate'     AND amount_usd > 0)
                               OR (kind = 'withdrawal' AND amount_usd < 0)
                               OR  kind = 'adjustment'
                              ),
            note              TEXT,
            source            TEXT NOT NULL DEFAULT 'manual',
            created_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_capital_flows_occurred
            ON capital_flows(occurred_at);

        CREATE TABLE IF NOT EXISTS wallet_reconciliation_runs (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            sampled_at            TEXT NOT NULL DEFAULT (datetime('now')),
            wallet_address        TEXT,
            source_status         TEXT NOT NULL,
            clob_balance_usd      REAL,
            chain_balance_usd     REAL,
            open_orders_count     INTEGER,
            open_positions_count  INTEGER,
            warnings_json         TEXT,
            snapshot_json         TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_recon_runs_sampled
            ON wallet_reconciliation_runs(sampled_at DESC);

        CREATE TABLE IF NOT EXISTS wallet_reconciliation_records (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id            INTEGER NOT NULL REFERENCES wallet_reconciliation_runs(id) ON DELETE CASCADE,
            source_layer      TEXT NOT NULL,
            source_id         TEXT,
            record_type       TEXT NOT NULL,
            wallet_address    TEXT,
            order_id          TEXT,
            tx_hash           TEXT,
            token_id          TEXT,
            amount_usd        REAL,
            status            TEXT,
            matched_ledger_id INTEGER,
            match_status      TEXT,
            record_json       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_recon_records_run
            ON wallet_reconciliation_records(run_id);
        CREATE INDEX IF NOT EXISTS idx_recon_records_match_status
            ON wallet_reconciliation_records(match_status);

        CREATE TABLE IF NOT EXISTS live_order_attempts (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
            wallet_id         TEXT NOT NULL,
            wallet_group      TEXT,
            mode              TEXT NOT NULL DEFAULT 'LIVE',
            coin              TEXT NOT NULL,
            sleeve            TEXT NOT NULL,
            entry_minute      INTEGER NOT NULL,
            side              TEXT NOT NULL,
            window_start_ts   INTEGER NOT NULL,
            client_order_id   TEXT NOT NULL UNIQUE,
            poly_market_id    TEXT,
            poly_token_id     TEXT,
            price             REAL,
            amount_usd        REAL NOT NULL,
            status            TEXT NOT NULL,
            ledger_id         INTEGER,
            live_order_id     TEXT,
            signal_json       TEXT,
            error_kind        TEXT,
            error             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_loa_status_group
            ON live_order_attempts(status, wallet_group);
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_loa_active_slot
            ON live_order_attempts(wallet_id, coin, sleeve, entry_minute, side, window_start_ts)
            WHERE status IN ('SUBMITTING', 'SUBMITTED');

        CREATE TABLE IF NOT EXISTS transfer_requests (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
            actor             TEXT NOT NULL,
            from_wallet       TEXT NOT NULL,
            to_wallet         TEXT NOT NULL,
            source_wallet_id  TEXT,
            amount_usd        REAL NOT NULL,
            status            TEXT NOT NULL,
            idempotency_key   TEXT,
            confirmation      TEXT,
            relayer_tx_id     TEXT,
            tx_hash           TEXT,
            error             TEXT,
            preview_json      TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_transfer_submitting_per_from_wallet
            ON transfer_requests(from_wallet)
            WHERE status = 'SUBMITTING';
        CREATE INDEX IF NOT EXISTS idx_transfer_status_source
            ON transfer_requests(status, source_wallet_id);

        -- B1: per-offset scan latch for sub-minute checkpoint sleeves.
        -- Mirrors entry_scan_latches but keyed on offset_sec so every
        -- checkpoint within a window is its own admissible entry (the
        -- checkpoint-grid combo stacks multiple offsets per market).
        CREATE TABLE IF NOT EXISTS subminute_scan_latches (
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            coin              TEXT NOT NULL,
            sleeve            TEXT NOT NULL,
            offset_sec        INTEGER NOT NULL,
            window_start_ts   INTEGER NOT NULL,
            PRIMARY KEY (coin, sleeve, offset_sec, window_start_ts)
        );

        -- B4: requested-vs-realized fill record for every LIVE sub-minute
        -- order — the fill-rate breaker's raw material (ghost/stale quotes
        -- show up as unfilled/under-filled FAKs through the 250ms taker
        -- delay) and the rung-1 cumulative-cap accounting.
        CREATE TABLE IF NOT EXISTS subminute_fills (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            coin              TEXT NOT NULL,
            sleeve            TEXT NOT NULL,
            offset_sec        INTEGER NOT NULL,
            window_start_ts   INTEGER NOT NULL,
            side              TEXT NOT NULL,
            requested_usd     REAL NOT NULL,
            quote_ask         REAL,
            limit_price       REAL,
            filled_size       REAL,
            filled_price      REAL,
            filled_usd        REAL,
            order_id          TEXT,
            client_order_id   TEXT,
            outcome           TEXT NOT NULL,     -- accepted / rejected / error
            -- v2 reject-retry: 1 = first FAK attempt, 2 = the one-shot retry.
            attempt           INTEGER DEFAULT 1,
            -- Fast-trigger provenance: 'tick' / 'fast_sweep' / 'fast_event'.
            trigger_src       TEXT DEFAULT 'tick'
        );
        CREATE INDEX IF NOT EXISTS idx_subminute_fills_created
            ON subminute_fills(created_at DESC);

        -- Reject re-quote telemetry (observation only — no order is placed).
        -- One row per LIVE FAK rejected at the placement site; requote_ask /
        -- requote_age_ms are back-filled ~1-2s later by the fast (2s) drain,
        -- and resolved_outcome (UP/DOWN) by the resolution sweep
        -- post-settlement.
        CREATE TABLE IF NOT EXISTS reject_requotes (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            coin              TEXT NOT NULL,
            sleeve            TEXT NOT NULL,
            offset_sec        INTEGER,
            window_start_ts   INTEGER NOT NULL,
            side              TEXT NOT NULL,
            limit_price       REAL,
            requested_usd     REAL NOT NULL,
            requote_ask       REAL,
            requote_age_ms    INTEGER,
            resolved_outcome  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_reject_requotes_created
            ON reject_requotes(created_at DESC);

        -- ON-CHAIN miss verdict per venue-rejected FAK (operator 2026-07-30).
        -- Written ONLY by the off-path miss_forensics scheduler job; answers
        -- "faster taker vs maker cancel" from the trade tape, which the ~1s
        -- book re-read in reject_requotes cannot. fill_id UNIQUE so a
        -- re-classification of a PENDING row overwrites in place. See
        -- schema.sql for the column docstrings.
        CREATE TABLE IF NOT EXISTS miss_forensics (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            fill_id           INTEGER NOT NULL UNIQUE,
            verdict           TEXT NOT NULL,
            beat_dt_s         REAL,
            beat_price        REAL,
            beat_size         REAL,
            rival_addr        TEXT,
            lag_s_used        REAL,
            checked_at        TEXT NOT NULL DEFAULT (datetime('now')),
            detail_json       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_miss_forensics_checked
            ON miss_forensics(checked_at DESC);
        CREATE INDEX IF NOT EXISTS idx_miss_forensics_verdict
            ON miss_forensics(verdict);

        -- PURE_STOP_04 model stop-loss telemetry (operator 2026-07-09): one
        -- row per stop-sell ATTEMPT. Forward-OOS shadow record for manual
        -- fill-rate monitoring; PnL is never booked here (see schema.sql).
        CREATE TABLE IF NOT EXISTS stop_loss_attempts (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            ledger_id         INTEGER NOT NULL,
            coin              TEXT NOT NULL,
            sleeve            TEXT NOT NULL,
            window_start_ts   INTEGER NOT NULL,
            offset_sec        INTEGER,
            side              TEXT NOT NULL,
            entry_fill        REAL,
            model_p           REAL,
            bid               REAL,
            quoted_price      REAL,
            attempt           INTEGER NOT NULL,
            outcome           TEXT NOT NULL,
            fill_price        REAL,
            order_id          TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_stop_loss_attempts_ledger
            ON stop_loss_attempts(ledger_id);
        CREATE INDEX IF NOT EXISTS idx_stop_loss_attempts_created
            ON stop_loss_attempts(created_at DESC);

        -- T3 scale-in durable no-dip state (execution/scale_in.py). ONE row per
        -- ENTRY position; disarmed=1 is permanent (no-dip fail OR stop trigger).
        -- schema.sql is the source of truth for fresh installs; re-applied here
        -- for legacy DBs.
        CREATE TABLE IF NOT EXISTS scalein_state (
            ledger_id         INTEGER PRIMARY KEY,
            ask0              REAL,
            running_min_ask   REAL,
            disarmed          INTEGER NOT NULL DEFAULT 0,
            last_attempt_ts   INTEGER,
            updated_at        TEXT
        );
        """
    )


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, decl: str
) -> None:
    """Idempotent ALTER TABLE ADD COLUMN.

    SQLite's PRAGMA table_info returns the existing columns; we only run
    the ALTER when the column isn't already present. Safe to call on
    every boot.
    """
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db(db_path: str | Path) -> sqlite3.Connection:
    """Open the database and ensure the schema is in place.

    Idempotent — safe to call on every boot. Returns a live connection.
    """
    conn = get_connection(db_path)
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    _migrate_db(conn)
    return conn


def backfill_wallet_groups(
    conn: sqlite3.Connection, portfolio: _PortfolioLike
) -> dict[str, int]:
    """One-shot UPDATE that stamps `wallet_group` on legacy `ledger` and
    `signals` rows from the loaded portfolio's wallet→group mapping
    (D-LIVE-11 / D-LIVE-15).

    The migration helper runs AFTER `load_sleeves()` because the wallet→group
    mapping is JSON-derived and not available inside `_migrate_db()`. Only
    rows with `wallet_group IS NULL` are touched, so re-running is a no-op
    once every row carries its group.

    Returns row counts of `{"ledger": N, "signals": M}` for logging.
    """
    # Build the wallet_id → group_id mapping once. Sleeves with no group
    # assignment leave their existing rows' wallet_group as NULL.
    mapping: dict[str, str] = {}
    for sleeve in portfolio.market_sleeves:
        wallet_id = getattr(sleeve, "label", None)
        if not isinstance(wallet_id, str):
            continue
        group_id = portfolio.wallet_group_for(wallet_id)
        if group_id is None:
            continue
        mapping[wallet_id] = group_id

    if not mapping:
        return {"ledger": 0, "signals": 0}

    # Also backfill via sleeve/pair_component (long/short pair legs derive
    # their wallet_group from the canonical sleeve label). Both columns are
    # additive and may be NULL on legacy rows; the COALESCE-style logic
    # below covers all observed shapes.
    ledger_updated = 0
    signals_updated = 0
    for wallet_id, group_id in mapping.items():
        # Pair-leg sleeves stamp `wallet_id = wallet_label` OR
        # `pair_component = wallet_label` + `sleeve LIKE 'wallet_label_%'`.
        # The UPDATE covers all three patterns; rows already grouped are
        # skipped via the WHERE clause.
        cur = conn.execute(
            """
            UPDATE ledger
               SET wallet_group = ?
             WHERE wallet_group IS NULL
               AND (
                    wallet_id = ?
                 OR sleeve = ?
                 OR sleeve LIKE ? || '_%'
                 OR pair_component = ?
               )
            """,
            (group_id, wallet_id, wallet_id, wallet_id, wallet_id),
        )
        ledger_updated += cur.rowcount or 0
        cur = conn.execute(
            """
            UPDATE signals
               SET wallet_group = ?
             WHERE wallet_group IS NULL
               AND (
                    wallet_id = ?
                 OR sleeve = ?
                 OR sleeve LIKE ? || '_%'
               )
            """,
            (group_id, wallet_id, wallet_id, wallet_id),
        )
        signals_updated += cur.rowcount or 0

    conn.commit()
    return {"ledger": ledger_updated, "signals": signals_updated}
