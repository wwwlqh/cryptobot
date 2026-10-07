"""SQLite connection and schema bootstrap.

Pragmas:

  - journal_mode = WAL
  - synchronous = NORMAL
  - foreign_keys = ON
  - cache_size = -32_000 (32 MB)
  - journal_size_limit = 200_000_000

``init_db()`` applies ``schema.sql`` and then runs ``_migrate_db()``, which
only ever adds tables, columns and indexes.

Migration does not need the strategy config. Backfills that do
(``backfill_wallet_groups``, ``seed_wallet_modes``) run after
``load_sleeves()`` at boot, once the wallet-to-group mapping exists.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Every thread (tick worker, scheduler jobs, dashboard) gets its own
# connection, because BEGIN IMMEDIATE / COMMIT / ROLLBACK act on the whole
# connection: on a shared one, a background job's commit could end the tick's
# open transaction, or a tick rollback could discard a settlement.
#
# ``busy_timeout=5000`` makes a writer that finds the file locked wait and
# retry for up to 5s instead of failing with "database is locked"; SQLite's WAL
# locking plus BEGIN IMMEDIATE then serializes writers across threads.
_BUSY_TIMEOUT_MS = 5000


class _PortfolioLike(Protocol):
    """Structural type for the backfill helpers, so this module does not depend
    on V6Portfolio and tests can pass small stubs.
    """

    def wallet_group_for(self, wallet_id: str) -> str | None: ...

    @property
    def market_sleeves(self) -> Iterable[object]: ...


def utc_now_sql() -> str:
    """Current UTC time as ``YYYY-MM-DD HH:MM:SS`` (SQLite text format)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open (or create) a SQLite database with the WAL pragmas applied.

    ``check_same_thread=False`` allows the connection to be handed between
    threads; ``DbHandle`` still gives each thread its own connection, and slot
    writes use the ``BEGIN IMMEDIATE`` slot-lock pattern.
    """
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Autocommit mode, so explicit BEGIN IMMEDIATE / COMMIT / ROLLBACK define
    # transactions. Python's default implicit BEGIN would clash with the slot
    # lock.
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-32000")
    conn.execute("PRAGMA journal_size_limit=200000000")
    # Keep checkpoints off the tick thread. With the default wal_autocheckpoint
    # of 1000 pages, SQLite checkpoints inline in whichever commit crosses the
    # threshold, usually the tick's own, stalling the decision path on an
    # fsync. At 20000 pages the inline trigger is effectively never reached
    # between the periodic PASSIVE checkpoints run by main.py's wal_checkpoint
    # job. journal_size_limit still bounds the WAL if that job stalls.
    conn.execute("PRAGMA wal_autocheckpoint=20000")
    # With one connection per thread, a writer that finds the WAL write lock
    # held must wait rather than fail; BEGIN IMMEDIATE on one connection blocks
    # on another until it commits.
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    return conn


def passive_wal_checkpoint(conn: sqlite3.Connection) -> None:
    """Run ``PRAGMA wal_checkpoint(PASSIVE)``, the background half of the
    ``wal_autocheckpoint=20000`` setting above.

    PASSIVE copies as many WAL frames into the database as it can without
    blocking any reader or writer, so it is safe from a scheduler thread at any
    time. main.py's ``wal_checkpoint`` job calls it periodically. Harmless on a
    non-WAL or in-memory database.
    """
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()


class ThreadLocalConnections:
    """One sqlite3 connection per thread, opened lazily on first use.

    The tick runs in a worker thread, APScheduler runs the sweep and
    reconciliation jobs on its own pool, and the dashboard handles POSTs on
    another thread. Transactions (``BEGIN IMMEDIATE`` / ``COMMIT`` /
    ``ROLLBACK``) belong to a connection, not a thread, so sharing one
    connection lets one thread's ``commit()`` end another's transaction.

    With a connection per thread, SQLite's WAL locking plus the
    ``busy_timeout`` from :func:`get_connection` serialize writers: a second
    thread's ``BEGIN IMMEDIATE`` waits for the first to commit.

    ``get()`` guards the registry with a lock. ``close_all()`` is called once
    at shutdown from the main thread.
    """

    def __init__(
        self, db_path: str | Path, *, shared: sqlite3.Connection | None = None
    ) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._all: list[sqlite3.Connection] = []
        # A ``:memory:`` database is private to its connection (a second
        # connection sees an empty database), so every thread must share one.
        # That only happens in tests and the ``--once`` smoke run; production
        # uses a file. Passing ``shared`` forces the same mode for any path.
        self._shared: sqlite3.Connection | None = shared
        if self._shared is None and str(db_path) == ":memory:":
            self._shared = get_connection(db_path)
        if self._shared is not None:
            self._all.append(self._shared)

    def get(self) -> sqlite3.Connection:
        """Return this thread's connection, opening one on the first call.

        In shared mode (``:memory:`` or an injected connection) every thread
        gets the same connection.
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
        # Lets the handle be passed wherever a zero-argument ``conn_factory()``
        # is expected.
        return self.get()

    def register_current_thread(self, conn: sqlite3.Connection) -> None:
        """Adopt an already-open connection as the calling thread's own.

        Boot uses this so the connection from :func:`init_db` becomes the main
        thread's connection (and is closed by ``close_all``). No-op in shared
        mode.
        """
        if self._shared is not None:
            return
        self._local.conn = conn
        with self._lock:
            if conn not in self._all:
                self._all.append(conn)

    def register_extra(self, conn: sqlite3.Connection) -> None:
        """Track a connection opened outside this handle (e.g. the dashboard's)
        so ``close_all`` closes it at shutdown. Not tied to any thread.
        """
        with self._lock:
            if conn not in self._all:
                self._all.append(conn)

    def close_all(self) -> None:
        """Close every connection opened through this handle.

        Called once at shutdown. Connections from threads that have exited are
        still closed, since all are tracked in ``self._all``.
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
    """Additive, idempotent migrations.

    ``CREATE TABLE IF NOT EXISTS`` covers new tables; this adds columns and
    indexes an older database may lack. Nothing is ever dropped or rewritten.
    """
    # strike_price: the spot at decision time. Old rows have NULL and the sweep
    # falls back to yes_pre1_anchor.
    _add_column_if_missing(conn, "ledger", "strike_price", "REAL")

    # Polymarket's final outcomePrices, recorded at settlement. NULL on old
    # rows.
    _add_column_if_missing(conn, "ledger", "polymarket_resolution_price", "REAL")
    _add_column_if_missing(conn, "ledger", "polymarket_resolution_source", "TEXT")

    # window_open_spot: the spot at the window-open boundary, the anchor the
    # model trains on and Polymarket settles against. Written at bet time from
    # PartialRetEngine.window_open_spot(); resolution_sweep prefers it over
    # strike_price. Old rows have NULL and fall back to strike_price.
    _add_column_if_missing(conn, "ledger", "window_open_spot", "REAL")

    # Pair metadata; NULL on single-market rows.
    _add_column_if_missing(conn, "ledger", "pair_group_id", "TEXT")
    _add_column_if_missing(conn, "ledger", "pair_role", "TEXT")
    _add_column_if_missing(conn, "ledger", "pair_component", "TEXT")
    _add_column_if_missing(conn, "ledger", "paired_coin", "TEXT")

    # Strategy wallet id. Nullable; old rows infer it from
    # sleeve/pair_component.
    _add_column_if_missing(conn, "ledger", "wallet_id", "TEXT")
    _add_column_if_missing(conn, "signals", "wallet_id", "TEXT")

    # Live/dry mode, CLOB order linkage, orphan flag and VWAP columns. Fresh
    # databases get them from schema.sql.
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
    # edge_semantics_version=2 marks rows whose realized_edge is after the
    # slippage haircut. Informational only; no gate reads it.
    _add_column_if_missing(conn, "ledger", "edge_semantics_version", "INTEGER")
    _add_column_if_missing(conn, "signals", "mode", "TEXT")
    _add_column_if_missing(conn, "signals", "wallet_group", "TEXT")

    # offset_sec: seconds from window open at which the bet was admitted. NULL
    # on minute-boundary rows; only the sub-minute path writes it, so
    # dashboards and the fill-rate breaker can split the two with IS NOT NULL.
    _add_column_if_missing(conn, "ledger", "offset_sec", "INTEGER")
    _add_column_if_missing(conn, "signals", "offset_sec", "INTEGER")

    # Stop-loss exit columns, written once on a stop sell (outcome also becomes
    # CANCELLED so the sweep skips the row and pnl_net is booked once).
    # Nullable; NULL means never stopped.
    _add_column_if_missing(conn, "ledger", "exit_price", "REAL")
    _add_column_if_missing(conn, "ledger", "exit_ts", "TEXT")
    _add_column_if_missing(conn, "ledger", "exit_kind", "TEXT")

    # The venue each bet traded on, DEFAULT 'polymarket', so existing rows read
    # back as 'polymarket' and record_bet stamps config.VENUE on new ones.
    _add_column_if_missing(
        conn, "ledger", "venue", "TEXT NOT NULL DEFAULT 'polymarket'"
    )

    # Live-trading tables. schema.sql creates them on fresh databases; creating
    # them here keeps migrations self-contained for older ones.
    _ensure_per_strategy_live_tables(conn)

    # attempt: 1 for the first FAK, 2+ for retries after a reject. Defaults to
    # 1, which is what older rows were. Must run after
    # _ensure_per_strategy_live_tables so subminute_fills exists.
    _add_column_if_missing(conn, "subminute_fills", "attempt", "INTEGER DEFAULT 1")

    # Which trigger sent the order: 'tick' (1 Hz scanner), 'fast_sweep'
    # (sub-second sweep job) or 'fast_event' (WS book update). Defaults to
    # 'tick', which is what older rows were. Same ordering constraint as
    # ``attempt``.
    _add_column_if_missing(
        conn, "subminute_fills", "trigger_src", "TEXT DEFAULT 'tick'"
    )

    # Composite slot-key index: slot_state()'s lookup, run every second.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_slot "
        "ON ledger(coin, sleeve, entry_minute, side, window_start_ts, outcome)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_bet_ts ON ledger(bet_ts)"
    )
    # Covering index for count_window_entries(), which runs on every qualifying
    # sub-minute second; without it the query scans the coin's whole ledger.
    # Mirrors schema.sql.
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
    # Group-cap and orphan-leg scans.
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
    # This index has no remaining reader; dropping it saves the 1 Hz signals
    # insert the upkeep.
    conn.execute("DROP INDEX IF EXISTS idx_signals_created_at_skip_reason")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_health_stage_created "
        "ON pipeline_health(stage, created_at DESC)"
    )
    conn.commit()


def _ensure_per_strategy_live_tables(conn: sqlite3.Connection) -> None:
    """CREATE TABLE IF NOT EXISTS for each live-trading table. schema.sql is
    the source for fresh databases; older ones need the same DDL here. Keep
    the column lists identical to schema.sql.
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
        """
    )


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, decl: str
) -> None:
    """ALTER TABLE ADD COLUMN only if the column is missing (checked with
    PRAGMA table_info). Safe on every boot.
    """
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db(db_path: str | Path) -> sqlite3.Connection:
    """Open the database and make sure the schema is in place.

    Idempotent; safe on every boot. Returns an open connection.
    """
    conn = get_connection(db_path)
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    _migrate_db(conn)
    return conn


def backfill_wallet_groups(
    conn: sqlite3.Connection, portfolio: _PortfolioLike
) -> dict[str, int]:
    """Stamp ``wallet_group`` on ``ledger`` and ``signals`` rows that lack it,
    using the loaded portfolio's wallet-to-group mapping.

    Runs after ``load_sleeves()``, since the mapping comes from the strategy
    config and is not available in ``_migrate_db()``. Only rows with
    ``wallet_group IS NULL`` are touched, so a re-run is a no-op.

    Returns ``{"ledger": N, "signals": M}`` row counts for logging.
    """
    # Build the wallet_id -> group_id mapping once. Wallets without a group
    # leave their rows' wallet_group NULL.
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

    # Match rows by wallet_id, or by sleeve/pair_component for older rows where
    # wallet_id may be NULL.
    ledger_updated = 0
    signals_updated = 0
    for wallet_id, group_id in mapping.items():
        # A row belongs to the wallet if ``wallet_id`` equals its label, or
        # ``pair_component`` does, or ``sleeve`` starts with ``<label>_``. Rows
        # that already have a group are skipped by the WHERE clause.
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
