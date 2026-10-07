-- cryptobot live-scanner schema. Ledger (R17) + R-NEW additions per the
-- shipped 2026-05-17 live-scanner+dashboard plan. Later columns are added
-- idempotently by db/connection.py.

-- =========================================================================
-- ledger: one row per PENDING bet attempt; promoted to WIN/LOSS/CANCELLED
-- by the F3 resolution sweep (U7a). add-on top-ups land as separate rows
-- tagged is_topup=1. The composite slot key (coin, sleeve, entry_minute,
-- side, window_start_ts) is promoted to indexed columns so slot_state()
-- stays under p95 5ms at 1Hz (R-NEW-9, U1 latency test).
-- =========================================================================
CREATE TABLE IF NOT EXISTS ledger (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    bet_ts                  TEXT NOT NULL,            -- ISO8601 UTC placement timestamp
    -- slot key (first-class indexed columns, NOT JSON-extracted) -----------
    coin                    TEXT NOT NULL,            -- BTC / ETH / SOL / BNB / XRP / DOGE
    wallet_id               TEXT,                     -- dry-run strategy wallet; nullable on legacy rows
    sleeve                  TEXT NOT NULL,            -- v6_5m / v6_15m
    entry_minute            INTEGER NOT NULL,         -- 4 (5m) / 7,13 (15m)
    side                    TEXT NOT NULL,            -- YES / NO
    window_start_ts         INTEGER NOT NULL,         -- unix epoch sec of market window open
    window_end_ts           INTEGER NOT NULL,         -- unix epoch sec of market window close (settles at this time)
    -- market metadata -----------------------------------------------------
    interval_min            INTEGER NOT NULL,         -- 5 or 15
    poly_market_id          TEXT NOT NULL,            -- Polymarket conditionId
    poly_token_id           TEXT NOT NULL,            -- YES or NO clobTokenId depending on side
    fee_rate_bps            REAL NOT NULL,            -- as fetched from Gamma (carry-forward R9)
    -- scoring snapshot at decision time -----------------------------------
    model_p                 REAL NOT NULL,            -- lut_hit_rate (signal_source='lut') or rf score
    p_market                REAL NOT NULL,            -- top_ask at scan time (the fill price for parity, D2)
    fill_price              REAL NOT NULL,            -- top_ask at decision (NOT VWAP — D2)
    walked_vwap             REAL,                     -- multi-level VWAP from walker (forensic; D2)
    realized_edge           REAL NOT NULL,            -- fee_adjusted_edge - slippage_haircut (D7)
    partial_ret_z_1h        REAL,                     -- feature value at decision
    lut_p_bucket            INTEGER,                  -- 0..9 bucket from RF -> LUT (D13)
    lut_hit_rate            REAL,                     -- LUT bucket hit-rate (model_p when signal_source=lut)
    lut_n_cum               INTEGER,                  -- LUT bucket sample size at decision
    -- sizing --------------------------------------------------------------
    target_usd              REAL NOT NULL,            -- slot's full bet_size = bankroll*alloc*capital_frac*multiplier
    bet_size                REAL NOT NULL,            -- this row's notional (target - filled_pre for top-ups)
    slot_filled_pre         REAL NOT NULL DEFAULT 0,  -- pre-this-row fill amount; > 0 implies is_topup
    is_topup                INTEGER NOT NULL DEFAULT 0,  -- 1 when slot_filled_pre > 0
    scalein_topup           INTEGER NOT NULL DEFAULT 0,  -- 1 for a T3 scale-in top-up row (execution/scale_in.py); distinguishes T3 top-ups from generic within-minute top-ups for live grading
    entry_top_price         REAL NOT NULL,            -- sticky cap anchor (R-NEW-9)
    add_on_realized_edge_minus_initial_edge REAL,     -- forensic; populated when is_topup=1 (R-NEW-9)
    -- resolution strike (review #2) --------------------------------------
    strike_price            REAL,                     -- underlying spot at decision time (legacy — kept as forensic record per divergence #13)
    -- Divergence #13 fix (2026-05-21): the bot's self-resolution must
    -- grade against the window_open spot — the same anchor the model
    -- trains against and Polymarket Chainlink settles on. ``strike_price``
    -- captured at decision time (window_start + entry_minute*60s) is 4-10
    -- min INTO the window; grading bets against it systematically marks
    -- model-correct bets as losses whenever the directional move
    -- materialized before the bot acted. The new column is populated
    -- from PartialRetEngine.window_open_spot() at bet time and consumed
    -- by resolution_sweep._decide_won_up. NULL on legacy rows; sweep
    -- falls back to strike_price only when this column is NULL.
    window_open_spot        REAL,                     -- underlying spot at window_open (anchor used by _decide_won_up)
    -- walk-mask anchor (D3) ----------------------------------------------
    yes_pre1_anchor         REAL,                     -- top_ask at entry_ts - 60s (backtest walk-mask anchor)
    -- outcome -------------------------------------------------------------
    outcome                 TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING / WIN / LOSS / CANCELLED / GHOST_FILL
    won_up                  INTEGER,                  -- 1 if resolution went the YES way
    resolution_price        REAL,                     -- Gamma UP outcome price when source=polymarket_gamma; oracle spot otherwise
    resolution_source       TEXT,                     -- polymarket_gamma / chainlink / null when PENDING ('pyth_fallback' appears on pre-2026-07 rows; that source is gone)
    resolution_ts           TEXT,                     -- ISO UTC of settlement
    pnl_net                 REAL,                     -- net P&L for this row (fee-adjusted, parity with backtest)
    -- forensic / parity ---------------------------------------------------
    config_sha256           TEXT NOT NULL,            -- sha256 of dry_run_recommended.json at boot
    -- Divergence #8 (2026-05-23): live settlement now prefers Gamma's
    -- final Polymarket outcomePrices so ledger P&L matches the actual
    -- market result. The oracle spot path remains available only when
    -- a caller does not wire a Polymarket resolver (legacy tests/tools).
    polymarket_resolution_price  REAL,                -- Gamma UP outcome price when populated
    polymarket_resolution_source TEXT,                -- polymarket_gamma / manual / null
    -- portfolio-v2 pair metadata ------------------------------------------
    pair_group_id           TEXT,                     -- deterministic id shared by both pair legs
    pair_role               TEXT,                     -- long / short / null
    pair_component          TEXT,                     -- pairs_5m / pairs_15m / null
    paired_coin             TEXT,                     -- other leg coin for quick audit
    pair_orphan             INTEGER NOT NULL DEFAULT 0,  -- 0=healthy, 1=orphaned (mate failed), 2=orphaned-then-closed (D-LIVE-22)
    pair_top_up_used        INTEGER NOT NULL DEFAULT 0,  -- 1 after pair rebalance fires; gates a second rebalance (D-LIVE-28)
    -- per-strategy LIVE/DRY mode + wallet-group routing (D-LIVE-1 / D-LIVE-3) ---
    mode                    TEXT NOT NULL DEFAULT 'DRY',  -- 'DRY' (paper) or 'LIVE' (real CLOB); immutable after write
    wallet_group            TEXT,                     -- 'group_5m' / 'group_15m' / NULL — set from portfolio.wallet_group_for(wallet_id)
    -- LIVE CLOB linkage (NULL on DRY rows; D-LIVE-28) ----------------------
    live_order_id           TEXT,                     -- Polymarket CLOB orderId returned by OrderClient.place_gtc_limit_order
    live_client_order_id    TEXT,                     -- local correlation key, mirrors live_order_attempts.client_order_id
    live_filled_size        REAL,                     -- realized fill size in tokens
    live_filled_price       REAL,                     -- realized fill VWAP (D-LIVE-29 — same units as fill_price)
    live_fill_ts            TEXT,                     -- ISO UTC of CLOB fill payload
    -- forensic top-of-book at decision (D-LIVE-29) ------------------------
    top_ask_at_decision     REAL,                     -- top_ask captured by scanner-tick before any walk; fill_price now stores VWAP
    -- ghost-fill measurement (carry-forward R29) -------------------------
    ghost_fill              INTEGER NOT NULL DEFAULT 0,  -- 1 if measured no fill at the price within the window
    -- B1 sub-minute checkpoint add-on: seconds from window open at which the
    -- bet was admitted. NULL on every baseline (minute-boundary) row — the
    -- sub-minute path is the only writer, so IS NOT NULL splits sub-minute
    -- vs baseline PnL for the fill-rate breaker and the dashboard.
    offset_sec              INTEGER,
    -- PURE_STOP_04 model stop-loss (operator 2026-07-09). NULL on every row
    -- that was NOT closed early by the stop-loss; populated ONCE on the single
    -- successful stop-sell (the row's outcome also transitions PENDING ->
    -- CANCELLED so the F3 resolution sweep, WHERE outcome='PENDING', skips it
    -- and pnl_net is booked exactly once). exit_kind='stop_loss' tags it.
    exit_price              REAL,                     -- realized sell VWAP of the stop exit
    exit_ts                 TEXT,                     -- ISO UTC of the stop-sell fill
    exit_kind               TEXT,                     -- 'stop_loss' when closed early; NULL otherwise
    -- Venue dimension (deliverable 5). Every new bet is stamped with the
    -- config.VENUE it was placed on ('polymarket' default / 'predictfun'). The
    -- DEFAULT makes legacy rows and the CRYPTOBOT_VENUE-unset path read back as
    -- 'polymarket' with zero behavior change.
    venue                   TEXT NOT NULL DEFAULT 'polymarket',
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Composite slot-key index for slot_state(). The (coin, sleeve, entry_minute,
-- side, window_start_ts) tuple uniquely identifies a slot; combined with
-- outcome it indexes the PENDING-vs-settled split slot_state() needs.
CREATE INDEX IF NOT EXISTS idx_ledger_slot
    ON ledger(coin, sleeve, entry_minute, side, window_start_ts, outcome);
CREATE INDEX IF NOT EXISTS idx_ledger_outcome ON ledger(outcome);
CREATE INDEX IF NOT EXISTS idx_ledger_bet_ts ON ledger(bet_ts);
CREATE INDEX IF NOT EXISTS idx_ledger_window_end ON ledger(outcome, window_end_ts);
-- count_window_entries() — the multifill entry tally. Armed at every equity by
-- the 2026-07-25 every-qualifying order, so it now runs on EVERY sub-minute
-- qualifying second (30 offsets x 4 coins per window) instead of
-- short-circuiting on the equity gate. idx_ledger_slot cannot serve it: it
-- leads on coin but its second column `sleeve` is unconstrained here, so the
-- seek stops at the coin partition and every row in it needs a table lookup
-- (verified: SEARCH ledger USING INDEX idx_ledger_slot (coin=?)). This one is
-- COVERING for the whole predicate, which keeps the query flat as the ledger
-- grows -- and every-qualifying makes it grow faster.
CREATE INDEX IF NOT EXISTS idx_ledger_window_entries
    ON ledger(coin, interval_min, window_start_ts, is_topup, outcome);
-- Indexes on columns added by _migrate_db (mode, wallet_group, pair_orphan,
-- wallet_id, pair_group_id) live in connection.py — they fire AFTER the
-- ALTER TABLE ADD COLUMN so legacy DBs that lack those columns at
-- executescript(schema.sql) time don't crash.


-- =========================================================================
-- signals: one row per evaluated (coin, sleeve, entry_minute, side) per tick
-- whether it passed or skipped. Provides the dashboard gate-skip counts and
-- the post-hoc analysis surface for R32 / R-NEW-14 (D19 — observational only).
-- =========================================================================
CREATE TABLE IF NOT EXISTS signals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at              TEXT NOT NULL,            -- ISO8601 UTC tick timestamp
    coin                    TEXT NOT NULL,
    wallet_id               TEXT,
    sleeve                  TEXT NOT NULL,
    entry_minute            INTEGER NOT NULL,
    side                    TEXT NOT NULL,
    window_start_ts         INTEGER NOT NULL,
    model_p                 REAL,
    ask                     REAL,
    edge                    REAL,
    fee_rate_bps            REAL,
    partial_ret_z_1h        REAL,
    lut_p_bucket            INTEGER,
    lut_hit_rate            REAL,
    lut_n_cum               INTEGER,
    -- per-strategy mode + wallet-group context for skip-audit (D-LIVE-3) ---
    mode                    TEXT,                     -- 'DRY' / 'LIVE' / NULL on legacy rows
    wallet_group            TEXT,                     -- 'group_5m' / 'group_15m' / NULL
    -- B1 sub-minute checkpoint context (NULL on baseline rows).
    offset_sec              INTEGER,
    outcome                 TEXT NOT NULL,            -- WOULD_BET / SKIP:<reason>
    skip_reason             TEXT                      -- denormalized skip cause (ad-hoc ops queries; the dashboard gates panel that grouped on it retired 2026-07-14)
);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals(outcome, created_at);
-- (The PERF-03 composite idx_signals_created_at_skip_reason was retired
-- 2026-07-14 with the dashboard _gates_block, its only consumer;
-- connection.init_db drops it from existing installs.)


-- =========================================================================
-- entry_scan_latches: tiny forward-only latch table for initial-entry scans.
-- Keeps 1Hz live ticks from rescoring the same entry minute after a skip
-- without building a new index over historical signals rows during deploy.
-- =========================================================================
CREATE TABLE IF NOT EXISTS entry_scan_latches (
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    coin                    TEXT NOT NULL,
    sleeve                  TEXT NOT NULL,
    entry_minute            INTEGER NOT NULL,
    window_start_ts         INTEGER NOT NULL,
    PRIMARY KEY (coin, sleeve, entry_minute, window_start_ts)
);

-- B1: per-offset scan latch for sub-minute checkpoint sleeves. Mirrors
-- entry_scan_latches but keyed on offset_sec so every checkpoint within a
-- window is its own admissible entry (the checkpoint-grid combo stacks
-- multiple offsets per market). Keep in lockstep with
-- connection._ensure_per_strategy_live_tables.
CREATE TABLE IF NOT EXISTS subminute_scan_latches (
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    coin                    TEXT NOT NULL,
    sleeve                  TEXT NOT NULL,
    offset_sec              INTEGER NOT NULL,
    window_start_ts         INTEGER NOT NULL,
    PRIMARY KEY (coin, sleeve, offset_sec, window_start_ts)
);

-- B4: requested-vs-realized fill record for every LIVE sub-minute order —
-- the fill-rate breaker's raw material (ghost/stale quotes show up as
-- unfilled/under-filled FAKs through Polymarket's 250ms taker delay) and
-- the rung-1 cumulative-cap accounting. Keep in lockstep with
-- connection._ensure_per_strategy_live_tables.
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
    -- v2 reject-retry (operator 2026-07-09): 1 = first FAK attempt, 2 = the
    -- one-shot post-reject retry. Lets telemetry split first-vs-retry fills.
    attempt           INTEGER DEFAULT 1,
    -- Fast-trigger provenance (operator 2026-07-30): which trigger rung sent
    -- this order. 'tick' = the 1Hz boundary-aligned scanner (also the value on
    -- every legacy row and on the fast-requote replay), 'fast_sweep' = the
    -- sub-second sweep job, 'fast_event' = a WS book-update push. Grade the
    -- latency lever with `WHERE trigger_src IN ('fast_sweep','fast_event')`.
    trigger_src       TEXT DEFAULT 'tick'
);
CREATE INDEX IF NOT EXISTS idx_subminute_fills_created
    ON subminute_fills(created_at DESC);


-- =========================================================================
-- reject_requotes: reject re-quote TELEMETRY (observation only). One row per
-- LIVE FAK rejected at the order-placement site. `requote_ask`/`requote_age_ms`
-- are back-filled ~1-2s later by the fast (2s) scheduler drain (how the
-- top-of-book looked shortly after the reject, and how long after — a slow
-- drain would capture the near-terminal book, i.e. the outcome, instead);
-- `resolved_outcome` (UP/DOWN) is back-filled by the resolution sweep once
-- the market settles. NO order is ever placed from this path.
-- =========================================================================
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


-- =========================================================================
-- miss_forensics: ON-CHAIN verdict for each venue-rejected FAK (operator
-- 2026-07-30 — "classify every missed trade from CHAIN evidence, not just
-- bullshit"). One row per `subminute_fills` reject (fill_id UNIQUE, so a
-- re-classification overwrites in place). Written ONLY by the off-path
-- miss_forensics scheduler job; nothing in the decision path reads it.
--
-- reject_requotes answers "how did the book look ~1s later", which cannot
-- separate traded-away from pulled — the ambiguity that produced and then
-- refuted the "68% maker-cancel" inference. This table answers it from the
-- trade tape: FASTER_TAKER (a print inside the LEVEL BAND — from the ask we saw
-- up to our limit, +-0.5c — within the venue-match window), REPRICED_ONLY
-- (prints exist but all outside that band), MAKER_CANCEL (no print at all),
-- plus PENDING (transient fetch failure — retried) and UNRESOLVED (the venue no
-- longer exposes both tokens; terminal). The band's LOWER bound matters as much
-- as the upper: without it, prints deep on the other side of the book convict
-- as FASTER_TAKER (91.7% vs the hand-verified 84% on the first 48 rejects).
--
-- `lag_s_used` records the settlement-lag constant (data-api/block timestamps
-- run ~+3s after the venue match) that this row's dt was computed with, so a
-- stored verdict stays auditable after the constant is re-calibrated.
-- Keep in lockstep with connection._ensure_per_strategy_live_tables.
-- =========================================================================
CREATE TABLE IF NOT EXISTS miss_forensics (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    fill_id           INTEGER NOT NULL UNIQUE,  -- subminute_fills.id
    verdict           TEXT NOT NULL,            -- FASTER_TAKER / REPRICED_ONLY /
                                                -- MAKER_CANCEL / PENDING / UNRESOLVED
    beat_dt_s         REAL,     -- venue-match dt of the earliest beat-us print
                                -- (negative = it matched BEFORE our arrival)
    beat_price        REAL,     -- that print's price in OUR token's ask terms
                                -- (a complement SELL is mapped to 1-p)
    beat_size         REAL,     -- shares it took at our level
    rival_addr        TEXT,     -- resting order's owner (OrderFilled maker topic)
    lag_s_used        REAL,     -- settlement-lag constant applied to this row
    checked_at        TEXT NOT NULL DEFAULT (datetime('now')),
    detail_json       TEXT      -- windowed print list + token/condition ids
);
CREATE INDEX IF NOT EXISTS idx_miss_forensics_checked
    ON miss_forensics(checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_miss_forensics_verdict
    ON miss_forensics(verdict);


-- =========================================================================
-- stop_loss_attempts: PURE_STOP_04 model stop-loss TELEMETRY (operator
-- 2026-07-09). One row per stop-sell ATTEMPT (a trigger fires up to
-- CRYPTOBOT_STOPLOSS_MAX_ATTEMPTS placements). This is the forward-OOS shadow
-- record: the operator monitors the loss-cut fill rate from here (there is NO
-- automatic breaker). PnL is NEVER booked here — a successful stop-sell
-- updates the SINGLE ledger row (exit_price/exit_ts/exit_kind + outcome
-- CANCELLED); attempts are execution retries of ONE logical exit, never
-- separate bets. Keep in lockstep with
-- connection._ensure_per_strategy_live_tables.
-- =========================================================================
CREATE TABLE IF NOT EXISTS stop_loss_attempts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    ledger_id         INTEGER NOT NULL,   -- the position row being exited
    coin              TEXT NOT NULL,
    sleeve            TEXT NOT NULL,
    window_start_ts   INTEGER NOT NULL,
    offset_sec        INTEGER,            -- re-score offset (now - window_start) at the trigger
    side              TEXT NOT NULL,      -- held side (YES/NO)
    entry_fill        REAL,               -- realized entry fill price (underwater reference)
    model_p           REAL,               -- re-scored model_p_side(t) at the trigger
    bid               REAL,               -- current sell-side bid at the trigger
    quoted_price      REAL,               -- limit price the FOK sell was posted at
    attempt           INTEGER NOT NULL,   -- 1..CRYPTOBOT_STOPLOSS_MAX_ATTEMPTS
    outcome           TEXT NOT NULL,      -- 'fill' / 'reject' / 'miss' / 'below_venue_min' (sub-venue-min quote, never POSTed)
    fill_price        REAL,               -- realized sell VWAP when outcome='fill'
    order_id          TEXT
);
CREATE INDEX IF NOT EXISTS idx_stop_loss_attempts_ledger
    ON stop_loss_attempts(ledger_id);
CREATE INDEX IF NOT EXISTS idx_stop_loss_attempts_created
    ON stop_loss_attempts(created_at DESC);


-- =========================================================================
-- scalein_state: durable per-position no-dip state for the T3 scale-in
-- (execution/scale_in.py). ONE row per ENTRY ledger position (ledger_id).
-- ``ask0`` = the entry's scanned decision ask (ledger.p_market); the top-up
-- ladder stays armed only while ``running_min_ask`` (tick-observed running min
-- of the side best ask, crossed books skipped) has never fallen below
-- ``ask0 - no_dip``. The first violation sets ``disarmed=1`` PERMANENTLY (a
-- stop trigger also disarms). DB-derived so it survives a process restart — no
-- in-memory state to lose (the observation gap while the process is down is
-- inherent to tick observation; see reference memory).
-- =========================================================================
CREATE TABLE IF NOT EXISTS scalein_state (
    ledger_id         INTEGER PRIMARY KEY,   -- the ENTRY position row this state tracks
    ask0              REAL,                   -- entry scanned decision ask (ledger.p_market)
    running_min_ask   REAL,                   -- tick-observed running min of the side best ask
    disarmed          INTEGER NOT NULL DEFAULT 0,  -- 1 once no-dip fails OR a stop triggers (permanent)
    last_attempt_ts   INTEGER,                -- unix sec of the last top-up ATTEMPT (cadence throttle)
    updated_at        TEXT
);


-- =========================================================================
-- pipeline_health: per-stage health events for the dashboard's GatePanel
-- footer and post-hoc forensics. Mirrors hightempbot's shape.
-- =========================================================================
CREATE TABLE IF NOT EXISTS pipeline_health (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    stage                   TEXT NOT NULL,            -- tick / fetch / score / gate / walk / record / resolve / boot
    status                  TEXT NOT NULL,            -- OK / WARN / ERROR / CRITICAL
    coin                    TEXT,                     -- nullable for system-wide events
    message                 TEXT,
    duration_ms             INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pipeline_health_stage_created
    ON pipeline_health(stage, created_at DESC);


-- =========================================================================
-- fee_rate_cache: per-market_token_id fee_rate_bps with TTL refresh.
-- Carry-forward of R9 (fee-rate fail-closed): a missing or stale entry
-- causes the fee gate to fire SKIP:fee_unknown.
-- =========================================================================
CREATE TABLE IF NOT EXISTS fee_rate_cache (
    market_token_id         TEXT PRIMARY KEY,
    fee_rate_bps            REAL NOT NULL,
    fetched_at              TEXT NOT NULL DEFAULT (datetime('now'))
);


-- =========================================================================
-- Per-strategy DRY/LIVE deploy surface (D-LIVE-1 / D-LIVE-4 / D-LIVE-25).
-- All new tables below are additive — legacy installs migrate via
-- _migrate_db() in connection.py (CREATE TABLE IF NOT EXISTS is idempotent).
-- =========================================================================


-- =========================================================================
-- wallet_mode_state: one row per wallet_id; durable per-strategy DRY/LIVE.
-- Seeded at boot from V6Portfolio.wallet_default_modes (D-LIVE-1). Toggled
-- via dashboard POST or CLI; mode flips audit through operator_control_events
-- (scope='wallet:<id>') per D-LIVE-25. Optimistic-concurrency via version.
-- =========================================================================
CREATE TABLE IF NOT EXISTS wallet_mode_state (
    wallet_id               TEXT PRIMARY KEY,
    mode                    TEXT NOT NULL DEFAULT 'DRY',  -- 'DRY' / 'LIVE'
    updated_by              TEXT,                     -- 'boot' / 'dashboard' / 'cli' / actor
    updated_at              TEXT NOT NULL DEFAULT (datetime('now')),
    version                 INTEGER NOT NULL DEFAULT 0    -- optimistic-concurrency token
);


-- =========================================================================
-- operator_control_state: singleton (id=1) for global processing state.
-- Default state = STOPPED_PROCESSING so a fresh deploy never accidentally
-- fires LIVE on first boot (matches hightempbot's safe-default).
-- Per D-LIVE-12, STOPPED blocks LIVE only — DRY continues.
-- =========================================================================
CREATE TABLE IF NOT EXISTS operator_control_state (
    id                      INTEGER PRIMARY KEY CHECK (id = 1),
    state                   TEXT NOT NULL DEFAULT 'STOPPED_PROCESSING',  -- LIVE / STOPPED_PROCESSING / TRANSFER_LOCK
    boot_dry_run            INTEGER,                  -- captures the DRY_RUN env at first boot (forensic)
    reason                  TEXT,
    updated_by              TEXT,
    updated_at              TEXT NOT NULL DEFAULT (datetime('now')),
    version                 INTEGER NOT NULL DEFAULT 0
);


-- =========================================================================
-- operator_control_events: single audit table for global + per-wallet
-- transitions (D-LIVE-25). `scope` = 'global' or 'wallet:<wallet_id>'.
-- =========================================================================
CREATE TABLE IF NOT EXISTS operator_control_events (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    scope                   TEXT NOT NULL DEFAULT 'global',  -- 'global' / 'wallet:<wallet_id>'
    action                  TEXT NOT NULL,            -- start / stop / transfer_lock / mode_flip
    from_state              TEXT,
    to_state                TEXT,
    actor                   TEXT NOT NULL,            -- 'dashboard' / 'cli' / 'boot'
    reason                  TEXT,
    detail                  TEXT
);
CREATE INDEX IF NOT EXISTS idx_op_events_scope_created
    ON operator_control_events(scope, created_at DESC);


-- =========================================================================
-- live_readiness_reports: snapshot of pre-tick LIVE readiness checks.
-- Per D-LIVE-26b: report_json is built from an EXPLICIT field whitelist —
-- never via Config.model_dump(), which would leak SecretStr values.
-- =========================================================================
CREATE TABLE IF NOT EXISTS live_readiness_reports (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    status                  TEXT NOT NULL,            -- READY / NOT_READY / ERROR
    mode                    TEXT,                     -- snapshot of dominant mode at evaluation
    funder                  TEXT,
    signer                  TEXT,
    clob_balance_usd        REAL,
    chain_balance_usd       REAL,
    report_json             TEXT NOT NULL             -- field-whitelisted JSON; never raw Config
);
CREATE INDEX IF NOT EXISTS idx_readiness_created
    ON live_readiness_reports(created_at DESC);


-- =========================================================================
-- bankroll_peak: append-only snapshot of wallet-aggregate equity at
-- reconciliation time. Drives peak-equity drawdown halt + dashboard chart.
-- =========================================================================
CREATE TABLE IF NOT EXISTS bankroll_peak (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at              TEXT NOT NULL DEFAULT (datetime('now')),
    wallet_balance          REAL NOT NULL,
    realized_pnl            REAL NOT NULL,
    pending_exposure        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bankroll_peak_sampled
    ON bankroll_peak(sampled_at DESC);


-- =========================================================================
-- capital_flows: operator-recorded account-level cash events (deposits,
-- withdrawals, taker rebates, manual corrections). Merged into the equity
-- replay by `occurred_at` so the drawdown-from-peak metric that gates the
-- hard DD halt + the DD brake runs on TRUE funded equity: a deposit raises
-- equity at ITS OWN timestamp without counting as profit, so the peak only
-- moves when the flow-inclusive series makes a new high.
-- Flows cannot be attributed to a sleeve — the account is one funded pot, so
-- every wallet's replay sees the same series (see execution/capital.py).
-- =========================================================================
CREATE TABLE IF NOT EXISTS capital_flows (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at             TEXT NOT NULL,            -- 'YYYY-MM-DD HH:MM:SS' UTC (ledger spelling)
    kind                    TEXT NOT NULL CHECK (
                                kind IN ('deposit', 'withdrawal', 'rebate', 'adjustment')
                            ),
    -- SIGNED: money into the account is positive, money out is negative, so
    -- the replay can add `amount_usd` blindly. The sign is pinned per kind
    -- ('adjustment' is the deliberate either-way escape hatch) — a
    -- wrong-signed row would move the halt metric the wrong way.
    amount_usd              REAL NOT NULL CHECK (
                                (kind = 'deposit'    AND amount_usd > 0)
                             OR (kind = 'rebate'     AND amount_usd > 0)
                             OR (kind = 'withdrawal' AND amount_usd < 0)
                             OR  kind = 'adjustment'
                            ),
    note                    TEXT,
    source                  TEXT NOT NULL DEFAULT 'manual',
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_capital_flows_occurred
    ON capital_flows(occurred_at);


-- =========================================================================
-- wallet_reconciliation_runs + records: periodic CLOB ↔ chain ↔ ledger
-- forensics per D-LIVE-9. Disagreements emit warnings, never halt.
-- =========================================================================
CREATE TABLE IF NOT EXISTS wallet_reconciliation_runs (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at              TEXT NOT NULL DEFAULT (datetime('now')),
    wallet_address          TEXT,
    source_status           TEXT NOT NULL,            -- ok / clob_unreachable / chain_unreachable / partial
    clob_balance_usd        REAL,
    chain_balance_usd       REAL,
    open_orders_count       INTEGER,
    open_positions_count    INTEGER,
    warnings_json           TEXT,                     -- compact JSON; nullable when no warnings
    snapshot_json           TEXT
);
CREATE INDEX IF NOT EXISTS idx_recon_runs_sampled
    ON wallet_reconciliation_runs(sampled_at DESC);

CREATE TABLE IF NOT EXISTS wallet_reconciliation_records (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                  INTEGER NOT NULL REFERENCES wallet_reconciliation_runs(id) ON DELETE CASCADE,
    source_layer            TEXT NOT NULL,            -- clob / chain / ledger
    source_id               TEXT,
    record_type             TEXT NOT NULL,            -- order / position / balance
    wallet_address          TEXT,
    order_id                TEXT,
    tx_hash                 TEXT,
    token_id                TEXT,
    amount_usd              REAL,
    status                  TEXT,
    matched_ledger_id       INTEGER,
    match_status            TEXT,                     -- matched / orphan_clob / orphan_ledger / mismatch
    record_json             TEXT
);
CREATE INDEX IF NOT EXISTS idx_recon_records_run
    ON wallet_reconciliation_records(run_id);
CREATE INDEX IF NOT EXISTS idx_recon_records_match_status
    ON wallet_reconciliation_records(match_status);


-- =========================================================================
-- live_order_attempts: durable client-order-id reservation BEFORE the CLOB
-- call (D-LIVE-28). Counts toward group exposure while in SUBMITTING/
-- SUBMITTED without a linked ledger row. The active-slot partial unique
-- index prevents a crash/retry from submitting two LIVE orders against
-- the same slot.
-- =========================================================================
CREATE TABLE IF NOT EXISTS live_order_attempts (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at              TEXT NOT NULL DEFAULT (datetime('now')),
    wallet_id               TEXT NOT NULL,
    wallet_group            TEXT,
    mode                    TEXT NOT NULL DEFAULT 'LIVE',
    coin                    TEXT NOT NULL,
    sleeve                  TEXT NOT NULL,
    entry_minute            INTEGER NOT NULL,
    side                    TEXT NOT NULL,
    window_start_ts         INTEGER NOT NULL,
    client_order_id         TEXT NOT NULL UNIQUE,     -- local correlation key
    poly_market_id          TEXT,
    poly_token_id           TEXT,
    price                   REAL,
    amount_usd              REAL NOT NULL,
    status                  TEXT NOT NULL,            -- SUBMITTING / SUBMITTED / FAILED / LINKED
    ledger_id               INTEGER,                  -- NULL until linked to a ledger row
    live_order_id           TEXT,                     -- CLOB orderId once accepted
    signal_json             TEXT,                     -- serialized BetSignal for crash recovery
    error_kind              TEXT,
    error                   TEXT
);
-- Group-cap fast path: SUBMITTING / SUBMITTED rows count toward group exposure.
CREATE INDEX IF NOT EXISTS idx_loa_status_group
    ON live_order_attempts(status, wallet_group);
-- D-LIVE-28 active-slot guard: only one active attempt per slot key.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_loa_active_slot
    ON live_order_attempts(wallet_id, coin, sleeve, entry_minute, side, window_start_ts)
    WHERE status IN ('SUBMITTING', 'SUBMITTED');


-- =========================================================================
-- transfer_requests: per-strategy realized-PnL extraction (D-LIVE-21).
-- source_wallet_id NULL = legacy whole-funder transfer. Single-SUBMITTING
-- guard per from_wallet via the partial unique index.
-- =========================================================================
CREATE TABLE IF NOT EXISTS transfer_requests (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at              TEXT NOT NULL DEFAULT (datetime('now')),
    actor                   TEXT NOT NULL,
    from_wallet             TEXT NOT NULL,
    to_wallet               TEXT NOT NULL,
    source_wallet_id        TEXT,                     -- per-strategy PnL attribution (D-LIVE-21)
    amount_usd              REAL NOT NULL,
    status                  TEXT NOT NULL,            -- PREVIEW / SUBMITTING / SUBMITTED / CONFIRMED / FAILED
    idempotency_key         TEXT,
    confirmation            TEXT,
    relayer_tx_id           TEXT,
    tx_hash                 TEXT,
    error                   TEXT,
    preview_json            TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_transfer_submitting_per_from_wallet
    ON transfer_requests(from_wallet)
    WHERE status = 'SUBMITTING';
CREATE INDEX IF NOT EXISTS idx_transfer_status_source
    ON transfer_requests(status, source_wallet_id);


