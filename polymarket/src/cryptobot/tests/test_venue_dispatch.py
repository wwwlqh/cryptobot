"""Venue seam: default = Polymarket implementations; predictfun switches them.

Covers the hard constraint that CRYPTOBOT_VENUE unset/"polymarket" keeps the
existing adapters + arithmetic, and that "predictfun" swaps the market/book/fee
implementations behind the flag.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from cryptobot import config


def _mock_client() -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    )


# ---- default venue ---------------------------------------------------------

def test_venue_default_is_polymarket():
    # The shipped default (CRYPTOBOT_VENUE unset in the test env).
    assert config.VENUE == "polymarket"


# ---- deps factory implementation selection ---------------------------------

def _build_factory():
    from cryptobot.main import _build_deps_factory

    c = _mock_client()
    return _build_deps_factory(
        None, "sha256",
        spot_client=c, gamma_client=c, clob_client=c,
    )


def _delegates_to(partial_obj, module, attr, monkeypatch) -> bool:
    """True iff calling ``partial_obj`` reaches ``module.attr``.

    Phase 2 made the deps factory bind the ADAPTER's bound methods rather than a
    module-level function, so the old ``partial.func is module.fn`` identity
    check no longer expresses the contract — and would pass vacuously if an
    adapter delegated to the wrong venue. Asserting the call actually lands is
    stronger: it exercises the registry lookup AND the adapter's lazy import.
    """
    seen: list[bool] = []
    monkeypatch.setattr(module, attr, lambda *a, **k: seen.append(True))
    partial_obj("x", now_ts=0.0)
    return seen == [True]


def test_deps_factory_default_selects_polymarket(monkeypatch):
    monkeypatch.setattr(config, "VENUE", "polymarket")
    from cryptobot_polymarket import clob as polymarket_clob
    from cryptobot_polymarket import gamma as polymarket_gamma

    deps = _build_factory()()
    assert _delegates_to(deps.fetch_market, polymarket_gamma, "fetch_active_market", monkeypatch)
    assert _delegates_to(deps.fetch_book, polymarket_clob, "fetch_top_book", monkeypatch)
    assert _delegates_to(deps.fetch_books, polymarket_clob, "fetch_books_batch", monkeypatch)


def test_deps_factory_predictfun_selects_predictfun_adapters(monkeypatch):
    monkeypatch.setattr(config, "VENUE", "predictfun")
    from cryptobot_predictfun import clob as pf_clob
    from cryptobot_predictfun import markets as pf_markets

    deps = _build_factory()()
    assert _delegates_to(deps.fetch_market, pf_markets, "fetch_active_market", monkeypatch)
    assert _delegates_to(deps.fetch_book, pf_clob, "fetch_top_book", monkeypatch)
    assert _delegates_to(deps.fetch_books, pf_clob, "fetch_books_batch", monkeypatch)


def test_deps_factory_binds_only_predictfun_book_fetch_kwargs(monkeypatch):
    """The PF-only bucket=/read_timeout= kwargs must NOT reach Polymarket.

    This is the invariant the pre-Phase-2 venue branch existed to protect:
    Polymarket's fetchers accept neither kwarg, so binding one would be a
    TypeError on every book read. It now lives in book_fetch_kwargs().
    """
    from cryptobot.venues import get_adapter

    assert get_adapter("polymarket").book_fetch_kwargs() == {}
    assert get_adapter("polymarket").prefetch_fetch_kwargs() == {}
    assert "bucket" in get_adapter("predictfun").book_fetch_kwargs()
    assert "bucket" in get_adapter("predictfun").prefetch_fetch_kwargs()


# ---- pipeline walk-fee dispatch --------------------------------------------

def test_pipeline_venue_walk_fee_fn_none_on_polymarket(monkeypatch):
    from cryptobot.execution import pipeline

    monkeypatch.setattr(config, "VENUE", "polymarket")
    assert pipeline._venue_walk_fee_fn() is None


def test_pipeline_venue_walk_fee_fn_callable_on_predictfun(monkeypatch):
    from cryptobot.execution import pipeline

    monkeypatch.setattr(config, "VENUE", "predictfun")
    fn = pipeline._venue_walk_fee_fn()
    assert callable(fn)
    assert fn(0.80) == pytest.approx(0.02 * 0.20)


# ---- walk_book fee_fn seam is byte-identical when None ----------------------

def test_walk_book_fee_fn_none_matches_parabola():
    from cryptobot.execution.walk_book import walk_book_edge_preserving

    book = {"asks": [{"price": 0.70, "size": 100}]}
    base = dict(
        prob_safe_floor=0.95, fee_theta=0.072, min_edge=-1.0,
        min_bet_usd=1.0, max_walk_price=0.05,
    )
    default = walk_book_edge_preserving(book, 10.0, **base)
    explicit_parabola = walk_book_edge_preserving(
        book, 10.0, fee_fn=lambda p: 0.072 * p * (1.0 - p), **base
    )
    assert default is not None
    # realized_edge (index 4) identical => byte-identical arithmetic.
    assert default[4] == explicit_parabola[4]


def test_walk_book_tent_fee_fn_changes_edge():
    from cryptobot.execution.walk_book import walk_book_edge_preserving

    book = {"asks": [{"price": 0.70, "size": 100}]}
    base = dict(
        prob_safe_floor=0.95, fee_theta=0.072, min_edge=-1.0,
        min_bet_usd=1.0, max_walk_price=0.05,
    )
    default = walk_book_edge_preserving(book, 10.0, **base)
    tent = walk_book_edge_preserving(
        book, 10.0, fee_fn=lambda p: 0.02 * min(p, 1.0 - p), **base
    )
    # tent fee (0.006) < parabola fee (0.01512) => higher realized edge.
    assert tent[4] > default[4]


# ---- COINS universe is venue-conditioned -----------------------------------

def _config_probe(env_overrides: dict) -> tuple[str, list[str]]:
    # parents[2] is the package root (polymarket/src locally, src/ in the deployed
    # bundle) — either way it is exactly what PYTHONPATH wants.
    src_root = Path(__file__).resolve().parents[2]  # legacy-path-ok: package root, same index in both layouts
    import os

    env = dict(os.environ)
    env["PYTHONPATH"] = str(src_root)
    env.pop("CRYPTOBOT_COINS", None)
    env.update(env_overrides)
    out = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "from cryptobot import config;"
            "print(config.VENUE);"
            "print(','.join(config.COINS))",
        ],
        env=env,
        text=True,
    )
    lines = out.strip().splitlines()
    return lines[0], lines[1].split(",")


def test_coins_default_polymarket_universe():
    venue, coins = _config_probe({"CRYPTOBOT_VENUE": "polymarket"})
    assert venue == "polymarket"
    assert "BTC" in coins and "ETH" in coins and "XRP" in coins
    assert "BNB" not in coins


def test_coins_predictfun_universe_is_btc_eth_bnb():
    venue, coins = _config_probe({"CRYPTOBOT_VENUE": "predictfun"})
    assert venue == "predictfun"
    assert coins == ["BTC", "ETH", "BNB"]


def test_explicit_coins_env_wins_over_venue():
    venue, coins = _config_probe(
        {"CRYPTOBOT_VENUE": "predictfun", "CRYPTOBOT_COINS": "BTC,ETH,SOL,XRP"}
    )
    assert venue == "predictfun"
    assert coins == ["BTC", "ETH", "SOL", "XRP"]


# ---- DB venue column (deliverable 5) ---------------------------------------

def _strip_venue(schema_sql: str) -> str:
    """Drop the ledger `venue` column line to simulate a pre-migration DB."""
    return "\n".join(
        line for line in schema_sql.splitlines()
        if not line.strip().startswith("venue ")
    )


def test_legacy_db_migrates_venue_column_and_defaults_polymarket(tmp_path):
    """Open a pre-migration schema (no venue), insert a row, migrate, re-run
    init_db — the migration is idempotent and the legacy row reads 'polymarket'.
    """
    import sqlite3

    from cryptobot.db import init_db

    schema = Path(__file__).resolve().parents[1] / "db" / "schema.sql"  # legacy-path-ok: package-relative, moves with the module
    legacy = _strip_venue(schema.read_text(encoding="utf-8"))
    path = tmp_path / "legacy_venue.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(legacy)
        # A legacy ledger row (no venue column exists yet).
        conn.execute(
            """
            INSERT INTO ledger
              (bet_ts, coin, sleeve, entry_minute, side, window_start_ts,
               window_end_ts, interval_min, poly_market_id, poly_token_id,
               fee_rate_bps, model_p, p_market, fill_price, realized_edge,
               target_usd, bet_size, entry_top_price, config_sha256)
            VALUES
              ('2026-07-21 00:00:00', 'BTC', 'v6_5m', 4, 'YES', 1715923200,
               1715923500, 5, 'M', 'T', 100.0, 0.78, 0.55, 0.55, 0.10, 5.0,
               5.0, 0.55, 'sha')
            """
        )
        conn.commit()
        cols = {r[1] for r in conn.execute("PRAGMA table_info(ledger)")}
        assert "venue" not in cols  # pre-migration
    finally:
        conn.close()

    migrated = init_db(path)
    try:
        cols = {r[1] for r in migrated.execute("PRAGMA table_info(ledger)")}
        assert "venue" in cols
        row = migrated.execute("SELECT venue FROM ledger LIMIT 1").fetchone()
        assert row[0] == "polymarket"  # DEFAULT stamps the legacy row
    finally:
        migrated.close()

    # Idempotent: a second init_db does not raise and the value is unchanged.
    again = init_db(path)
    try:
        row = again.execute("SELECT venue FROM ledger LIMIT 1").fetchone()
        assert row[0] == "polymarket"
    finally:
        again.close()


def test_record_bet_writes_config_venue(db, monkeypatch):
    from cryptobot.execution.ledger import record_bet
    from cryptobot.tests.test_slot_state import make_signal

    monkeypatch.setattr(config, "VENUE", "predictfun")
    pf_id = record_bet(db, make_signal(side="YES"))
    monkeypatch.setattr(config, "VENUE", "polymarket")
    poly_id = record_bet(db, make_signal(side="NO"))
    assert db.execute("SELECT venue FROM ledger WHERE id=?", (pf_id,)).fetchone()[0] == "predictfun"
    assert db.execute("SELECT venue FROM ledger WHERE id=?", (poly_id,)).fetchone()[0] == "polymarket"


def test_row_venue_defaults_polymarket_when_absent():
    from cryptobot.observability.snapshot_builder import _row_venue

    assert _row_venue({"venue": "predictfun"}) == "predictfun"
    assert _row_venue({}) == "polymarket"       # absent-in-DB => polymarket
    assert _row_venue({"venue": None}) == "polymarket"


# ---- snapshot.predictfun_wallet contract (deliverable 6) -------------------

def test_predictfun_wallet_block_none_on_polymarket(monkeypatch):
    from cryptobot.observability import snapshot_builder

    monkeypatch.setattr(config, "VENUE", "polymarket")
    assert snapshot_builder._predictfun_wallet_block(lambda: 10.0) is None


def test_predictfun_wallet_block_shape_on_predictfun(monkeypatch):
    from cryptobot_predictfun import wallet as pfw
    from cryptobot.observability import snapshot_builder

    monkeypatch.setattr(config, "VENUE", "predictfun")
    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "0x" + "1" * 64, raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "bearer", raising=False)
    monkeypatch.setattr(pfw, "derive_signer_address", lambda k: "0x" + "a" * 40)
    block = snapshot_builder._predictfun_wallet_block(lambda: 50.0)
    assert block == {
        "address": "0x" + "a" * 40,
        "chain": "bnb",
        "api_key_present": True,
        "collateral": {"symbol": "USDT", "balance": 50.0},
    }


def test_predictfun_wallet_block_null_collateral_when_no_balance(monkeypatch):
    from cryptobot_predictfun import wallet as pfw
    from cryptobot.observability import snapshot_builder

    monkeypatch.setattr(config, "VENUE", "predictfun")
    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "", raising=False)
    monkeypatch.setattr(config, "PREDICTFUN_API_KEY", "", raising=False)
    monkeypatch.setattr(pfw, "derive_signer_address", lambda k: None)
    block = snapshot_builder._predictfun_wallet_block(None)
    assert block["address"] is None
    assert block["api_key_present"] is False
    assert block["collateral"] is None  # fail-closed


# ---- live_readiness venue dispatch (deliverable 7) -------------------------

def test_build_venue_balance_fetcher_polymarket(monkeypatch):
    from types import SimpleNamespace

    from cryptobot.execution import live_readiness

    monkeypatch.setattr(config, "VENUE", "polymarket")
    # build_pusd_balance_fetcher only returns a closure (no network at build).
    fetcher = live_readiness.build_venue_balance_fetcher(
        SimpleNamespace(polygon_rpc_url="https://rpc", poly_funder="0x" + "1" * 40)
    )
    assert callable(fetcher)


def test_build_venue_balance_fetcher_predictfun_fail_closed(monkeypatch):
    from types import SimpleNamespace

    from cryptobot.execution import live_readiness

    monkeypatch.setattr(config, "VENUE", "predictfun")
    monkeypatch.setattr(config, "PREDICTFUN_PRIVATE_KEY", "", raising=False)
    fetcher = live_readiness.build_venue_balance_fetcher(SimpleNamespace())
    assert callable(fetcher)
    # Fail-closed: no wallet plumbing configured => raises => NOT_READY.
    with pytest.raises(RuntimeError):
        fetcher()
