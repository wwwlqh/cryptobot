"""Periodic CLOB ↔ chain ↔ ledger wallet reconciliation.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U7 / D-LIVE-9:

  * `WalletReconciler.sample(conn)` snapshots the live funder's CLOB pUSD
    balance + open orders + open positions, optionally cross-checks
    against the on-chain pUSD ERC20 balance via Polygon RPC, and writes
    everything into `wallet_reconciliation_runs` + per-record rows in
    `wallet_reconciliation_records`.
  * Disagreements (CLOB-vs-chain delta > `LIVE_BALANCE_TOLERANCE_USD`)
    surface as warnings on the run's `warnings_json` — NEVER a halt.
    Operator sees the warning on the dashboard's reconciliation card and
    decides whether to investigate.
  * Each open CLOB position is matched against LIVE PENDING ledger rows
    by (token_id, side, size). Mismatches surface as `orphan_clob` or
    `orphan_ledger` records that the dashboard's wallet card highlights.
  * A `bankroll_peak` row is appended on every successful run so the
    dashboard's peak-equity chart + the drawdown halt have a fresh snapshot.

The Polygon RPC client is optional and injected by the caller. When
absent (Phase 1 boot path or CI), `chain_balance_usd` is left NULL and
the CLOB-only path still produces a useful audit row.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.strategy_constants import LIVE_BALANCE_TOLERANCE_USD


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FetchedExternalState:
    """Carrier for `WalletReconciler.fetch_external_state` output. Holds
    the network-derived data needed by `persist_run`. Module-private --
    callers should treat the type as opaque."""

    clob_balance: Optional[float]
    open_orders: Optional[list[dict[str, Any]]]
    open_positions: Optional[list[dict[str, Any]]]
    chain_balance: Optional[float]


@dataclass(frozen=True)
class ReconciliationRecord:
    """One row written to `wallet_reconciliation_records` per CLOB-side
    open order / position / chain balance entry.

    `match_status`: 'matched' / 'orphan_clob' / 'orphan_ledger' / 'mismatch'.
    """

    source_layer: str  # 'clob' / 'chain' / 'ledger'
    record_type: str  # 'order' / 'position' / 'balance'
    source_id: Optional[str] = None
    wallet_address: Optional[str] = None
    order_id: Optional[str] = None
    tx_hash: Optional[str] = None
    token_id: Optional[str] = None
    amount_usd: Optional[float] = None
    status: Optional[str] = None
    matched_ledger_id: Optional[int] = None
    match_status: Optional[str] = None
    record_payload: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class ReconciliationRun:
    """In-memory view of one `wallet_reconciliation_runs` row + its records.

    `source_status`: 'ok' / 'clob_unreachable' / 'chain_unreachable' / 'partial'.
    """

    run_id: int
    sampled_at: str
    wallet_address: Optional[str]
    source_status: str
    clob_balance_usd: Optional[float]
    chain_balance_usd: Optional[float]
    open_orders_count: int
    open_positions_count: int
    warnings: list[str] = field(default_factory=list)
    records: list[ReconciliationRecord] = field(default_factory=list)


# ---------------------------------------------------------------------------
# WalletReconciler
# ---------------------------------------------------------------------------


class WalletReconciler:
    """Snapshot the live funder's CLOB + chain state and write a run row.

    Phase 2 (this unit) ships the class; Phase 2's
    `scheduler/wallet_recon.py` wires `sample(conn)` to an APScheduler job
    that fires every `WALLET_SNAPSHOT_INTERVAL_MINUTES` (default 10).

    Constructor injects three dependencies so tests can substitute stubs:

      * `order_client` — required. Must expose `check_balance()`,
        `_client.get_open_orders()` (delegated through), and either
        `_client.get_open_positions()` or an equivalent helper.
      * `chain_balance_fetcher` — optional callable `() -> float | None`
        that returns the on-chain pUSD balance for `funder_address`. When
        None, chain reconciliation is skipped and the run still produces
        a useful CLOB-only audit row.
      * `funder_address` — the deposit wallet whose state we're sampling.
        Pulled from `Config.poly_funder` in production.
    """

    def __init__(
        self,
        *,
        order_client: object,
        funder_address: str,
        chain_balance_fetcher: Optional[Callable[[], Optional[float]]] = None,
        chain_position_fetcher: Optional[
            Callable[[str], Optional[float]]
        ] = None,
        balance_tolerance_usd: float = LIVE_BALANCE_TOLERANCE_USD,
    ) -> None:
        self._order_client = order_client
        self._funder_address = funder_address
        self._chain_balance_fetcher = chain_balance_fetcher
        self._chain_position_fetcher = chain_position_fetcher
        self._balance_tolerance_usd = float(balance_tolerance_usd)

    # ---- public entry point ---------------------------------------------

    def sample(self, conn: sqlite3.Connection) -> ReconciliationRun:
        """One-shot snapshot. Always writes a run row, even when CLOB is
        unreachable — the row's `source_status` carries the failure mode.

        Reliability finding #1 (REL-06/REL-08): the scheduler job that
        wraps this in `db_write_lock` previously held the lock through
        every CLOB HTTP call (up to 30s per call x 3 calls = 90s),
        blocking `_lut_reload_job` and `_on_job_missed` writes for the
        entire window. `fetch_external_state` does the network work
        with no DB access; `persist_run` does only DB writes. Callers
        wrap only `persist_run` in the lock."""
        fetched = self.fetch_external_state()
        return self.persist_run(conn, fetched)

    def fetch_external_state(self) -> "_FetchedExternalState":
        """All network I/O — CLOB balance/orders/positions + chain balance.
        Safe to call without `db_write_lock`."""
        return _FetchedExternalState(
            clob_balance=self._fetch_clob_balance(),
            open_orders=self._fetch_clob_open_orders(),
            open_positions=self._fetch_clob_positions(),
            chain_balance=self._fetch_chain_balance(),
        )

    def persist_run(
        self,
        conn: sqlite3.Connection,
        fetched: "_FetchedExternalState",
    ) -> ReconciliationRun:
        """DB-write side of `sample`. Callers hold `db_write_lock` here
        only — the upstream `fetch_external_state` runs unlocked."""
        clob_balance = fetched.clob_balance
        open_orders = fetched.open_orders
        open_positions = fetched.open_positions
        chain_balance = fetched.chain_balance

        warnings: list[str] = []
        source_status = self._derive_source_status(
            clob_balance=clob_balance,
            clob_orders=open_orders,
            clob_positions=open_positions,
            chain_balance=chain_balance,
            warnings=warnings,
        )

        records = self._build_records(
            conn,
            clob_balance=clob_balance,
            chain_balance=chain_balance,
            orders=open_orders or [],
            positions=open_positions or [],
        )

        owns_tx = not conn.in_transaction
        try:
            if owns_tx:
                conn.execute("BEGIN IMMEDIATE")
            run = self._insert_run(
                conn,
                source_status=source_status,
                clob_balance_usd=clob_balance,
                chain_balance_usd=chain_balance,
                open_orders_count=len(open_orders or []),
                open_positions_count=len(open_positions or []),
                warnings=warnings,
                records=records,
            )

            if clob_balance is not None:
                self._append_bankroll_peak(conn, clob_balance=clob_balance)
            if owns_tx:
                conn.commit()
        except Exception:
            if owns_tx and conn.in_transaction:
                conn.rollback()
            raise

        return run

    # ---- CLOB fetches ---------------------------------------------------

    def _fetch_clob_balance(self) -> Optional[float]:
        try:
            return self._order_client.check_balance()
        except Exception:  # noqa: BLE001 — defensive against stub failure
            logger.warning("WalletReconciler: CLOB balance check raised", exc_info=True)
            return None

    def _fetch_clob_list(
        self, attr_names: tuple[str, ...]
    ) -> Optional[list[dict[str, Any]]]:
        """Best-effort CLOB list fetch via the first matching method name.

        Some clients use camelCase, some snake_case — try each in order.
        Returns None on hard failure (network, library missing); empty list
        is a legitimate "no items" reading.
        """
        client = getattr(self._order_client, "_client", None)
        if client is None:
            return []
        for attr in attr_names:
            fn = getattr(client, attr, None)
            if not callable(fn):
                continue
            try:
                result = fn() or []
                return self._normalize_dicts(result)
            except Exception:  # noqa: BLE001
                logger.debug(
                    "WalletReconciler: CLOB %s raised", attr, exc_info=True
                )
                return None
        return []

    def _fetch_clob_open_orders(self) -> Optional[list[dict[str, Any]]]:
        """Best-effort open-orders fetch through the OrderClient's _client.

        Returns None on hard failure (network, library missing); empty list
        is a legitimate "no open orders" reading.
        """
        return self._fetch_clob_list(
            ("get_open_orders", "open_orders", "list_open_orders")
        )

    def _fetch_clob_positions(self) -> Optional[list[dict[str, Any]]]:
        """Best-effort open-positions fetch. The CLOB exposes positions via
        the Data API; the OrderClient may or may not surface a helper. Phase
        2 ships the lookup as best-effort — Phase 3 (U13) can extend the
        OrderClient with a first-class positions method if needed."""
        return self._fetch_clob_list(
            ("get_positions", "open_positions", "list_positions")
        )

    def _fetch_chain_balance(self) -> Optional[float]:
        if self._chain_balance_fetcher is None:
            return None
        try:
            return self._chain_balance_fetcher()
        except Exception:  # noqa: BLE001
            logger.warning(
                "WalletReconciler: chain balance fetcher raised",
                exc_info=True,
            )
            return None

    # ---- source_status + warnings --------------------------------------

    def _derive_source_status(
        self,
        *,
        clob_balance: Optional[float],
        clob_orders: Optional[list[dict[str, Any]]],
        clob_positions: Optional[list[dict[str, Any]]],
        chain_balance: Optional[float],
        warnings: list[str],
    ) -> str:
        """Compose the run-level status from the four optional fetches."""
        if clob_balance is None and clob_orders is None and clob_positions is None:
            return "clob_unreachable"

        partial = clob_balance is None or clob_orders is None or clob_positions is None

        # Drift check (D-LIVE-9) — best-effort, warn but don't halt.
        if (
            clob_balance is not None
            and chain_balance is not None
            and math.isfinite(clob_balance)
            and math.isfinite(chain_balance)
        ):
            delta = abs(clob_balance - chain_balance)
            if delta > self._balance_tolerance_usd:
                warnings.append(
                    f"chain_clob_drift: clob={clob_balance:.4f} "
                    f"chain={chain_balance:.4f} delta={delta:.4f} "
                    f"tolerance={self._balance_tolerance_usd:.4f}"
                )

        if self._chain_balance_fetcher is not None and chain_balance is None:
            warnings.append("chain_unreachable")
            return "partial"
        if partial:
            return "partial"
        return "ok"

    # ---- record matching ------------------------------------------------

    def _build_records(
        self,
        conn: sqlite3.Connection,
        *,
        clob_balance: Optional[float],
        chain_balance: Optional[float],
        orders: list[dict[str, Any]],
        positions: list[dict[str, Any]],
    ) -> list[ReconciliationRecord]:
        records: list[ReconciliationRecord] = []

        # 1. Balance records.
        if clob_balance is not None:
            records.append(
                ReconciliationRecord(
                    source_layer="clob",
                    record_type="balance",
                    wallet_address=self._funder_address,
                    amount_usd=clob_balance,
                    status="ok",
                )
            )
        if chain_balance is not None:
            records.append(
                ReconciliationRecord(
                    source_layer="chain",
                    record_type="balance",
                    wallet_address=self._funder_address,
                    amount_usd=chain_balance,
                    status="ok",
                )
            )

        # 2. Match each open CLOB position to a LIVE PENDING ledger row.
        ledger_rows = self._load_live_pending_rows(conn)
        ledger_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in ledger_rows:
            key = (str(row["poly_token_id"]), str(row["side"]))
            ledger_by_key.setdefault(key, []).append(row)

        matched_ledger_ids: set[int] = set()
        for position in positions:
            token_id = str(
                position.get("token_id") or position.get("asset_id") or ""
            )
            side = str(position.get("side") or "YES").upper()
            raw_value = position.get("value")
            try:
                amount_usd = (
                    float(raw_value)
                    if raw_value is not None and raw_value != ""
                    else None
                )
            except (TypeError, ValueError):
                amount_usd = None

            candidates = ledger_by_key.get((token_id, side), [])
            matched = None
            mismatch = None
            for cand in candidates:
                if cand["id"] in matched_ledger_ids:
                    continue
                if self._position_matches_ledger(position, cand, amount_usd):
                    matched = cand
                    break
                if mismatch is None:
                    mismatch = cand

            if matched is None and mismatch is not None:
                matched = mismatch
                match_status = "mismatch"
            else:
                match_status = "matched"

            if matched is not None:
                matched_ledger_ids.add(int(matched["id"]))
                records.append(
                    ReconciliationRecord(
                        source_layer="clob",
                        record_type="position",
                        wallet_address=self._funder_address,
                        token_id=token_id,
                        amount_usd=amount_usd,
                        status="open",
                        matched_ledger_id=int(matched["id"]),
                        match_status=match_status,
                        record_payload=position,
                    )
                )
            else:
                records.append(
                    ReconciliationRecord(
                        source_layer="clob",
                        record_type="position",
                        wallet_address=self._funder_address,
                        token_id=token_id,
                        amount_usd=amount_usd,
                        status="open",
                        match_status="orphan_clob",
                        record_payload=position,
                    )
                )

        # 3. LIVE PENDING ledger rows without a matching CLOB position are
        #    orphan_ledger.
        for row in ledger_rows:
            if int(row["id"]) in matched_ledger_ids:
                continue
            records.append(
                ReconciliationRecord(
                    source_layer="ledger",
                    record_type="position",
                    wallet_address=self._funder_address,
                    token_id=str(row["poly_token_id"]),
                    matched_ledger_id=int(row["id"]),
                    match_status="orphan_ledger",
                )
            )

        # 4. Each open CLOB order surfaces as a record too (for the dashboard).
        for order in orders:
            order_id = str(order.get("id") or order.get("order_id") or "")
            token_id = str(
                order.get("asset_id") or order.get("token_id") or ""
            )
            records.append(
                ReconciliationRecord(
                    source_layer="clob",
                    record_type="order",
                    wallet_address=self._funder_address,
                    order_id=order_id,
                    token_id=token_id,
                    status=str(order.get("status") or "open"),
                    record_payload=order,
                )
            )

        return records

    # ---- helpers --------------------------------------------------------

    @staticmethod
    def _position_matches_ledger(
        position: dict[str, Any],
        ledger_row: sqlite3.Row,
        amount_usd: Optional[float],
    ) -> bool:
        pos_size = _coerce_float(
            position.get("size")
            or position.get("quantity")
            or position.get("balance")
            or position.get("shares")
        )
        live_size = _coerce_float(ledger_row["live_filled_size"])
        if pos_size is not None and live_size is not None:
            return _close_amount(pos_size, live_size, rel_tol=0.02, abs_tol=0.0001)

        if amount_usd is not None:
            expected_value = _ledger_expected_position_value(ledger_row)
            if expected_value is not None:
                return _close_amount(
                    amount_usd, expected_value, rel_tol=0.02, abs_tol=0.05
                )

        # Older rows/payloads may not expose comparable size/value on both
        # sides. In that case the token+side key is the strongest available
        # evidence.
        return amount_usd is None and (live_size is None or pos_size is None)

    @staticmethod
    def _normalize_dicts(rows: object) -> list[dict[str, Any]]:
        """Coerce mixed dict / object responses into a list of dicts."""
        out: list[dict[str, Any]] = []
        for row in rows or []:
            if isinstance(row, dict):
                out.append(row)
            elif hasattr(row, "__dict__"):
                out.append(dict(row.__dict__))
        return out

    @staticmethod
    def _load_live_pending_rows(
        conn: sqlite3.Connection,
    ) -> list[sqlite3.Row]:
        return conn.execute(
            """
            SELECT id, poly_token_id, side, bet_size, live_order_id,
                   live_filled_size, live_filled_price, mode, wallet_group
              FROM ledger
             WHERE outcome = 'PENDING' AND mode = 'LIVE'
             ORDER BY id ASC
            """,
        ).fetchall()

    def _insert_run(
        self,
        conn: sqlite3.Connection,
        *,
        source_status: str,
        clob_balance_usd: Optional[float],
        chain_balance_usd: Optional[float],
        open_orders_count: int,
        open_positions_count: int,
        warnings: list[str],
        records: list[ReconciliationRecord],
    ) -> ReconciliationRun:
        now = utc_now_sql()
        warnings_json = json.dumps(warnings) if warnings else None
        snapshot_json = json.dumps(
            {
                "clob_balance_usd": clob_balance_usd,
                "chain_balance_usd": chain_balance_usd,
                "open_orders": open_orders_count,
                "open_positions": open_positions_count,
            },
            sort_keys=True,
        )

        cur = conn.execute(
            """
            INSERT INTO wallet_reconciliation_runs
              (sampled_at, wallet_address, source_status, clob_balance_usd,
               chain_balance_usd, open_orders_count, open_positions_count,
               warnings_json, snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                self._funder_address,
                source_status,
                clob_balance_usd,
                chain_balance_usd,
                open_orders_count,
                open_positions_count,
                warnings_json,
                snapshot_json,
            ),
        )
        run_id = int(cur.lastrowid or 0)
        if run_id == 0:
            raise RuntimeError("WalletReconciler INSERT did not produce a run_id")

        for record in records:
            conn.execute(
                """
                INSERT INTO wallet_reconciliation_records
                  (run_id, source_layer, source_id, record_type, wallet_address,
                   order_id, tx_hash, token_id, amount_usd, status,
                   matched_ledger_id, match_status, record_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    record.source_layer,
                    record.source_id,
                    record.record_type,
                    record.wallet_address,
                    record.order_id,
                    record.tx_hash,
                    record.token_id,
                    record.amount_usd,
                    record.status,
                    record.matched_ledger_id,
                    record.match_status,
                    json.dumps(record.record_payload, sort_keys=True)
                    if record.record_payload is not None
                    else None,
                ),
            )

        return ReconciliationRun(
            run_id=run_id,
            sampled_at=now,
            wallet_address=self._funder_address,
            source_status=source_status,
            clob_balance_usd=clob_balance_usd,
            chain_balance_usd=chain_balance_usd,
            open_orders_count=open_orders_count,
            open_positions_count=open_positions_count,
            warnings=warnings,
            records=list(records),
        )

    def _append_bankroll_peak(
        self, conn: sqlite3.Connection, *, clob_balance: float
    ) -> None:
        """One row per reconciliation run — a LIVE-only forensic equity series
        for the dashboard peak-equity chart.

        NOTE: ``realized_pnl`` here sums ONLY mode='LIVE' settled rows and
        ``wallet_balance`` is the raw live CLOB pUSD balance. This is NOT the
        denominator that drives the DD halt: the halt is computed by
        ``get_wallet_snapshots`` / ``snapshot_builder.drawdown_pct_peak_frac``
        from INITIAL_BANKROLL + ALL settled rows (DRY+LIVE; see
        ``capital.get_capital_snapshot``). The two equity bases can diverge
        during the DRY/LIVE mixed phase, so do NOT read this chart as the
        halt state — it is a live-cash forensic view only."""
        row = conn.execute(
            """
            SELECT
              COALESCE(SUM(
                CASE WHEN outcome IN ('WIN', 'LOSS', 'PUSH', 'CANCELLED')
                     THEN pnl_net ELSE 0 END
              ), 0.0) AS realized_pnl,
              COALESCE(SUM(
                CASE WHEN outcome = 'PENDING' THEN bet_size ELSE 0 END
              ), 0.0) AS pending_exposure
              FROM ledger
             WHERE mode = 'LIVE'
            """,
        ).fetchone()
        realized = row["realized_pnl"] or 0.0
        pending = row["pending_exposure"] or 0.0
        conn.execute(
            """
            INSERT INTO bankroll_peak
              (wallet_balance, realized_pnl, pending_exposure)
            VALUES (?, ?, ?)
            """,
            (float(clob_balance), float(realized), float(pending)),
        )


def _ledger_expected_position_value(row: sqlite3.Row) -> Optional[float]:
    live_size = _coerce_float(row["live_filled_size"])
    live_price = _coerce_float(row["live_filled_price"])
    if live_size is not None and live_price is not None:
        return live_size * live_price
    return _coerce_float(row["bet_size"])


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _close_amount(
    left: float,
    right: float,
    *,
    rel_tol: float,
    abs_tol: float,
) -> bool:
    return abs(float(left) - float(right)) <= max(
        abs_tol, rel_tol * max(abs(float(right)), 1.0)
    )
