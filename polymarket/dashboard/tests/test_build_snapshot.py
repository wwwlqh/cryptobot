"""U13b: verify build_snapshot.py emits the v6 portfolio block."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import re
import shutil
import subprocess
import tempfile
import textwrap
import urllib.request
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent


# ── shared Node VM harness for the client-side (data.jsx) helper tests ──────
# One copy of the fs/vm/context + `node -e` scaffold that ten tests used to
# carry inline (each with a slightly drifted sandbox allowlist). data.jsx runs
# in a bare sandbox; `js_body` then executes with `cb` (window.CB) and `w`
# (the sandbox window) in scope and must console.log exactly ONE JSON document,
# which is parsed and returned. `window_extra` is JS spliced into the sandbox
# `window` literal for sites needing browser shims (e.g. _LOCALSTORAGE_SHIM).
_LOCALSTORAGE_SHIM = (
    "localStorage: (() => { const store = {}; return { "
    "getItem: (k) => (k in store ? store[k] : null), "
    "setItem: (k, v) => { store[k] = String(v); } }; })()"
)


def _code_only(src: str) -> str:
    """Source with `//` line comments and `/* */` blocks stripped.

    The retirement guards below assert that removed identifiers
    (VENUE_SELECTORS, PF_LIVE, pollPredictfunPaper, ...) are absent. Those
    names deliberately SURVIVE in do-not-re-add comments — the comment is the
    institutional memory that keeps them removed — so a raw substring check
    would fail on the very note that documents the removal. Strip comments and
    assert against executable code only.

    Deliberately naive (no string-literal awareness): the dashboard bundle has
    no `//` inside string literals other than URLs, and none of these guards
    look at URLs.
    """
    no_block = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return "\n".join(
        re.sub(r"(^|\s)//.*$", "", line) for line in no_block.splitlines()
    )


def _run_client_js(js_body: str, *, window_extra: str = "") -> dict:
    if not shutil.which("node"):
        pytest.skip("node is not available for dashboard helper execution test")

    time_path = json.dumps(str(_REPO_ROOT / "polymarket" / "dashboard" / "time.js"))
    data_path = json.dumps(str(_REPO_ROOT / "polymarket" / "dashboard" / "data.jsx"))
    prelude = textwrap.dedent(
        f"""\
        const fs = require('fs');
        const vm = require('vm');
        const context = {{ window: {{ {window_extra} }}, console, Intl, Date, Math, Number,
          String, Set, Map, Array, Object, Boolean, RegExp, isFinite, parseInt, parseFloat }};
        vm.runInNewContext(fs.readFileSync({time_path}, 'utf8'), context, {{ filename: 'time.js' }});
        vm.runInNewContext(fs.readFileSync({data_path}, 'utf8'), context, {{ filename: 'data.jsx' }});
        const cb = context.window.CB;
        const w = context.window;
        """
    )
    result = subprocess.run(
        ["node", "-e", prelude + textwrap.dedent(js_body)],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def _run_bridge_js(js_body: str) -> dict:
    """Run the plain-JS SSE bridge with a deterministic browser shim."""
    if not shutil.which("node"):
        pytest.skip("node is not available for dashboard bridge execution test")

    time_path = json.dumps(str(_REPO_ROOT / "polymarket" / "dashboard" / "time.js"))
    bridge_path = json.dumps(str(_REPO_ROOT / "polymarket" / "dashboard" / "live-bridge.js"))
    prelude = textwrap.dedent(
        f"""\
        const fs = require('fs');
        const vm = require('vm');
        let authStatus = 200;
        class FakeEventSource {{
          constructor(url) {{ this.url = url; this.readyState = 1; this.events = {{}}; }}
          addEventListener(name, fn) {{ this.events[name] = fn; }}
          close() {{ this.readyState = 2; }}
          emit(name, value) {{ if (this.events[name]) this.events[name](value); }}
        }}
        class FakeCustomEvent {{
          constructor(type, init) {{ this.type = type; this.detail = init && init.detail; }}
        }}
        class FakeXHR {{
          open() {{}}
          send() {{ this.status = snapshotStatus; this.responseText = snapshotBody; }}
        }}
        let snapshotStatus = 401;
        let snapshotBody = '{{}}';
        const timers = [];
        const fakeSetTimeout = (fn, ms) => {{ timers.push({{ fn, ms }}); return timers.length; }};
        const fakeClearTimeout = () => {{}};
        const browserConsole = {{ log: console.log, info: () => {{}}, warn: () => {{}} }};
        const window = {{
          dispatchEvent: () => {{}},
          addEventListener: () => {{}},
          sessionStorage: {{ getItem: () => null, setItem: () => {{}} }},
          location: {{ reload: () => {{}} }},
        }};
        const context = {{ window, EventSource: FakeEventSource, CustomEvent: FakeCustomEvent,
          XMLHttpRequest: FakeXHR,
          fetch: (url, opts) => Promise.resolve({{ status: authStatus, ok: authStatus >= 200 && authStatus < 300 }}),
          console: browserConsole, Date, Math, Number, String, Set, Map, Array,
          setTimeout: fakeSetTimeout, clearTimeout: fakeClearTimeout,
          Object, Boolean, RegExp, isFinite, Promise }};
        vm.runInNewContext(fs.readFileSync({time_path}, 'utf8'), context,
          {{ filename: 'time.js' }});
        vm.runInNewContext(fs.readFileSync({bridge_path}, 'utf8'), context,
          {{ filename: 'live-bridge.js' }});
        const w = context.window;
        """
    )
    result = subprocess.run(
        ["node", "-e", prelude + textwrap.dedent(js_body)],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def _run_api_js(js_body: str) -> dict:
    """Run the plain dashboard mutation helper with a browser request shim."""
    if not shutil.which("node"):
        pytest.skip("node is not available for dashboard API helper execution test")

    api_path = json.dumps(str(_REPO_ROOT / "polymarket" / "dashboard" / "api.js"))
    prelude = textwrap.dedent(
        f"""\
        const fs = require('fs');
        const vm = require('vm');
        const calls = [];
        const context = {{
          window: {{}},
          document: {{ cookie: 'session_token=session; csrf_token=csrf%2Bvalue' }},
          fetch: (url, opts) => {{ calls.push({{ url, opts }}); return Promise.resolve({{
            ok: true, status: 200, json: () => Promise.resolve({{ ok: true }}),
          }}); }},
          console, JSON, Promise, Object, String, Error,
        }};
        vm.runInNewContext(fs.readFileSync({api_path}, 'utf8'), context,
          {{ filename: 'api.js' }});
        const w = context.window;
        """
    )
    result = subprocess.run(
        ["node", "-e", prelude + textwrap.dedent(js_body)],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "build_snapshot", _REPO_ROOT / "polymarket" / "dashboard" / "build_snapshot.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def _enable_subminute(monkeypatch):
    """The deployed config is v2 "step1last" — checkpoint-only, so load_sleeves
    (and build_snapshot.load_portfolio, which delegates to it) fails closed
    unless CRYPTOBOT_SUBMINUTE_ENABLED=1 (set by the live env). Pin it on so the
    static-fallback + portfolio-block cases build the real live portfolio.
    Tests that assert the disabled behavior flip it off themselves after the
    portfolio is built."""
    import sys

    for _src in (_REPO_ROOT / "polymarket" / "src", _REPO_ROOT / "src"):
        if _src.is_dir() and str(_src) not in sys.path:
            sys.path.insert(0, str(_src))
    from cryptobot import config

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)


def test_load_live_state_no_db_returns_defaults():
    """No --db-path → every operator field defaults to the schema's SQL
    DEFAULT (STOPPED_PROCESSING / no readiness report)."""
    bs = _load_module()
    live = bs.load_live_state(None)
    assert live["operator"]["state"] == "STOPPED_PROCESSING"
    assert live["operator"]["version"] == 0
    assert live["operator"]["readiness"]["status"] is None


def test_load_live_state_with_db_hydrates_mode_and_operator(tmp_path):
    """Pointed at a real cryptobot.db, the loader reads the operator state
    so the static snapshot matches the live payload shape."""
    bs = _load_module()
    from cryptobot.db import backfill_wallet_groups, init_db
    from cryptobot.execution.operator_control import boot_operator_state
    from cryptobot.strategy.portfolio_loader import load_sleeves

    db_path = tmp_path / "live.db"
    conn = init_db(db_path)
    try:
        portfolio = load_sleeves()
        backfill_wallet_groups(conn, portfolio)
        boot_operator_state(conn, portfolio, boot_dry_run=True)
    finally:
        conn.close()

    live = bs.load_live_state(db_path)
    # The wallet_groups cap-utilization block was dropped 2026-07-14 (no
    # dashboard surface read it) — the loader must not re-grow it.
    assert "wallet_groups" not in live
    # Default operator state.
    assert live["operator"]["state"] == "STOPPED_PROCESSING"


def test_load_live_state_hydrates_operator_console_payload(tmp_path):
    bs = _load_module()
    from cryptobot.db import init_db

    db_path = tmp_path / "operator.db"
    conn = init_db(db_path)
    try:
        conn.execute(
            "INSERT INTO live_readiness_reports "
            "(created_at, status, funder, clob_balance_usd, chain_balance_usd, report_json) "
            "VALUES ('2026-06-12 12:00:01', 'READY', '0xfunder', 103.55, "
            "        103.55, ?)",
            (json.dumps({
                "readiness_check_results": [
                    {"name": "clob_credentials", "passed": True, "detail": "ok"}
                ],
                "wallet_balance_freshness_s": 4,
            }),),
        )
        conn.commit()
    finally:
        conn.close()

    live = bs.load_live_state(db_path)

    assert live["operator"]["readiness"]["checks"][0]["name"] == "clob_credentials"


def test_load_portfolio_real_artifact():
    bs = _load_module()
    portfolio = bs.load_portfolio()
    assert portfolio is not None
    # Single-band amendment 2026-07-21 (FOLDF_HOLDOUT_20260721.md): the
    # deployed artifact (v2_step1last_live.json) carries only the two
    # sub-minute checkpoint sleeves in one shared-equity group_live; the
    # second sleeve pair trialled from 2026-07-10 is gone.
    labels = [s["label"] for s in portfolio["sleeves"]]
    assert labels == ["subminute_5m", "subminute_15m"]
    assert portfolio["shared_exposure_cap_frac"] == pytest.approx(1.0)
    assert portfolio["wallet_exposure_cap_fracs"]["subminute_5m"] == pytest.approx(1.0)
    assert portfolio["wallet_exposure_cap_fracs"]["subminute_15m"] == pytest.approx(1.0)
    assert portfolio["max_drawdown_halt_frac"] == pytest.approx(0.30)
    # max_walk_price MUST be the runtime override (config.MAX_WALK_PRICE),
    # never a per-sleeve value from the JSON.
    assert portfolio["max_walk_price"] == 0.05


def test_portfolio_block_has_no_orb():
    """ORB was retired 2026-07-09 — the portfolio block must no longer carry an
    `orb` key (the dashboard ORB panel is gone with it)."""
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    block = snapshot_builder._portfolio_block(load_sleeves())
    assert "orb" not in block


def test_portfolio_block_no_longer_carries_v2_key():
    """The dead live.portfolio.v2 config block (Strategy page deleted
    2026-07-09) is gone — sizing + spec now live under live.v2, not
    portfolio.v2, so the portfolio block must NOT carry a `v2` key."""
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    block = snapshot_builder._portfolio_block(load_sleeves())
    assert "v2" not in block


def test_v2_spec_bands_and_sizing_constants(monkeypatch):
    """The spec surfaces `bands` DERIVED from the loaded sleeves — the flat
    single-band ask_lo/ask_hi/gate presentation is gone (the payload stays a
    LIST so a second band could be re-admitted without a shape change). The
    frozen equity-fraction sizing constants still come off
    _v2_sizing_constants. (Replaces the deleted _v2_portfolio_config, which
    carried the stale single-band shape.)"""
    from cryptobot import config
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    portfolio = load_sleeves()

    spec = snapshot_builder._v2_spec(portfolio)
    # Band list, not a flat band, so the stale flat fields are not emitted.
    for stale in ("ask_lo", "ask_hi", "gate"):
        assert stale not in spec, f"stale flat spec field {stale!r} still emitted"
    assert spec["poly_fee_theta"] == pytest.approx(0.07)
    assert spec["one_per_market"] is True
    # Bands derived from the deployed sleeves — one band since 2026-07-21
    # (FOLDF_HOLDOUT_20260721.md: the second band left the artifact).
    assert [b["key"] for b in spec["bands"]] == ["mid"]
    bands = {b["key"]: b for b in spec["bands"]}
    assert bands["mid"]["sleeves"] == ["subminute_5m", "subminute_15m"]
    assert (bands["mid"]["ask_lo"], bands["mid"]["ask_hi"], bands["mid"]["gate"]) == (
        0.65, 0.85, 0.03,
    )

    consts = snapshot_builder._v2_sizing_constants()
    # Flat 1% on both intervals (2026-08-09 operator orders: 1% -> 2% -> 1%
    # around the same-day flip re-arm; was 3% flip-era).
    assert consts["base_frac_15m"] == pytest.approx(0.010)
    assert consts["base_frac_5m"] == pytest.approx(0.010)
    # Brake rule: THROTTLE REMOVED (operator 2026-07-31) — floor 1.0 pins the
    # multiplier at 1.0 at every drawdown depth. Scale stays 0.60 so the
    # throttle can be re-armed by moving the floor alone.
    assert consts["brake_scale"] == pytest.approx(0.60)
    assert consts["brake_floor"] == pytest.approx(1.0)
    # dd_rail (research budget, not a live control) is removed from the payload
    # (dashboard DD-display cleanup, operator 2026-07-09).
    assert "dd_rail" not in consts
    assert consts["poly_fee_theta"] == pytest.approx(0.07)


def test_v2_block_shape_and_brake(monkeypatch, tmp_path):
    """_v2_block emits spec/sizing/fill_rate/bets and computes the DD-brake as
    max(0.50, 1 − dd/0.60) off the current drawdown."""
    import sqlite3

    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    conn = init_db(tmp_path / "v2.db")
    conn.row_factory = sqlite3.Row
    try:
        kpis = {
            "drawdown_pct_peak": 15.0,
            "realized_capital": 100.0,
            "peak_capital": 117.6,
            "trading_halted": False,
        }
        block = snapshot_builder._v2_block(conn, load_sleeves(), kpis)
    finally:
        conn.close()
    assert block is not None
    assert set(block) >= {"enabled", "spec", "sizing", "fill_rate", "bets"}
    spec = block["spec"]
    # The spec carries `bands`, not a flat single-band presentation.
    for stale in ("ask_lo", "ask_hi", "gate"):
        assert stale not in spec, f"stale flat spec field {stale!r} still emitted"
    bands = {b["key"]: b for b in spec["bands"]}
    # Single-band artifact (2026-07-21): only the mid band is emitted.
    assert set(bands) == {"mid"}
    assert (bands["mid"]["ask_lo"], bands["mid"]["ask_hi"], bands["mid"]["gate"]) == (
        0.65, 0.85, 0.03,
    )
    assert spec["poly_fee_theta"] == pytest.approx(0.07)
    sizing = block["sizing"]
    # THROTTLE REMOVED (operator 2026-07-31): brake = max(1.00, 1 − 0.15/0.60)
    # = 1.00 — a 15% drawdown no longer downsizes. The hard halt (0.30) is the
    # only drawdown control, and 15% is well short of it.
    assert sizing["brake_multiplier"] == pytest.approx(1.0)
    assert sizing["hard_halt_frac"] == pytest.approx(0.30)
    assert sizing["equity"] == pytest.approx(100.0)
    assert sizing["trading_halted"] is False
    # DD-display cleanup (operator 2026-07-09): dd_rail gone; plain-language
    # throttle descriptors present. With the throttle removed the floor is
    # ×1.00 and its knee collapses to 0 DD — the dashboard reads that pair as
    # "throttle off" rather than "pinned at the floor".
    assert "dd_rail" not in sizing
    assert sizing["throttle_floor_frac"] == pytest.approx(1.0)
    assert sizing["throttle_dd_at_floor"] == pytest.approx(0.0)
    # No reconciled CLOB balance seeded -> equity falls back to the ledger
    # series and is explicitly labelled; the ledger paper series is carried
    # separately so no two unlabelled equities coexist on the tab.
    assert sizing["equity_basis"] == "ledger_fallback"
    assert sizing["ledger_paper_equity"] == pytest.approx(100.0)
    assert block["bets"] == []


def test_v2_bet_exposes_trade_journal_exec_and_realized_fill(monkeypatch, tmp_path):
    """Subminute prices must use the Journal's exec/fill contract.

    ``exec`` is the intended walked-VWAP price; ``fill`` is the venue-reported
    live fill. The strategy ask remains available separately for model-edge
    telemetry, but it is not a substitute for either displayed price.
    """
    import sqlite3

    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.execution.ledger import record_bet
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves
    from cryptobot.tests.test_slot_state import make_signal

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = init_db(tmp_path / "v2-exec-fill.db")
    conn.row_factory = sqlite3.Row
    try:
        rid = record_bet(
            conn,
            make_signal(
                coin="BTC", sleeve="subminute_5m", side="YES",
                bet_size=5.0, fill_price=0.70, walked_vwap=0.72,
            ),
            mode="LIVE", live_filled_size=6.0, live_filled_price=0.75,
        )
        conn.execute("UPDATE ledger SET offset_sec=285 WHERE id=?", (rid,))
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    assert block is not None
    bet = block["bets"][0]
    assert bet["ask"] == pytest.approx(0.70)
    assert bet["exec_price"] == pytest.approx(0.72)
    assert bet["live_filled_price"] == pytest.approx(0.75)


def test_v2_block_equity_uses_real_clob_balance(monkeypatch, tmp_path):
    """The v2 tab's equity + peak track the SAME per-tick live CLOB balance the
    hero uses (kpis.account_capital_live) — NOT the old _latest_live_balance
    reconciliation read — so Subminute and Overview can never diverge. equity_basis
    echoes the live source honestly (balance_cache), not the stale
    'live_clob_balance' mislabel; peak = max(live balance, historical peak)."""
    import sqlite3

    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    conn = init_db(tmp_path / "v2eq.db")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "INSERT INTO bankroll_peak (wallet_balance, realized_pnl, "
            "pending_exposure) VALUES (120.0, 0.0, 0.0)"
        )
        conn.commit()
        kpis = {
            "drawdown_pct_peak": 0.0,
            "realized_capital": 93.0,   # ledger paper (the confusing number)
            "peak_capital": 100.0,
            "trading_halted": False,
            # Per-tick live CLOB balance (the number sizing reads) + its source,
            # threaded into kpis by _kpis_block from balance_cache.get.
            "account_capital_live": 113.69,
            "account_capital_live_source": "balance_cache",
        }
        block = snapshot_builder._v2_block(conn, load_sleeves(), kpis)
    finally:
        conn.close()
    sizing = block["sizing"]
    assert sizing["equity_basis"] == "balance_cache"
    assert sizing["equity"] == pytest.approx(113.69)
    # Peak is the max of the live balance and the historical wallet peak.
    assert sizing["peak_capital"] == pytest.approx(120.0)
    # Ledger paper series still carried, explicitly labelled + distinct.
    assert sizing["ledger_paper_equity"] == pytest.approx(93.0)


def test_v2_block_surfaces_rejects_and_stats(monkeypatch, tmp_path):
    """The v2 block surfaces the rejected-order "fill tax": scored reject rows
    + aggregate reject/fill stats. Seed 3 resolved-or-pending reject_requotes
    and a mix of accepted/rejected subminute_fills (one pre-go-live, excluded).

    2026-07-23 missed-trades redesign: a resolution that eventually FILLED
    (accepted subminute row with filled_usd > 0 on the same coin/interval/
    window/direction) is ZERO missed trades — its rejects are retries of a
    bet that completed, so BTC (filled 10/10) and ETH (filled 4/8) are
    EXCLUDED from the per-resolution miss rollup (they used to leak their
    would-be PnL into the missed total on top of the realized ledger PnL).
    Only XRP (rejected, resolved) and SOL (rejected, pending) remain misses."""
    import sqlite3

    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = init_db(tmp_path / "rejects.db")
    conn.row_factory = sqlite3.Row
    try:
        # reject_requotes: A = YES won (UP), B = NO lost (UP), C = YES pending.
        conn.executemany(
            "INSERT INTO reject_requotes "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " limit_price, requested_usd, requote_ask, requote_age_ms, "
            " resolved_outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("2026-07-08 14:00:00", "BTC", "subminute_15m", 875, 1000, "YES",
                 0.70, 10.0, 0.73, 1100, "UP"),
                ("2026-07-08 14:01:00", "ETH", "subminute_5m", 285, 2000, "NO",
                 0.65, 8.0, 0.66, 1050, "UP"),
                ("2026-07-08 14:02:00", "XRP", "subminute_15m", 880, 3000, "YES",
                 0.75, 6.0, 0.78, 1200, "UP"),
                ("2026-07-08 14:03:00", "SOL", "subminute_5m", 290, 4000, "YES",
                 0.72, 5.0, None, None, None),
            ],
        )
        # subminute_fills since go-live: 2 accepted (10/10, 8/4), 1 rejected (6/0);
        # plus one pre-go-live accepted that MUST be excluded from the stats.
        conn.executemany(
            "INSERT INTO subminute_fills "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " requested_usd, filled_usd, outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("2026-07-08 14:00:00", "BTC", "subminute_15m", 875, 1000, "YES",
                 10.0, 10.0, "accepted"),
                ("2026-07-08 14:01:00", "ETH", "subminute_5m", 285, 2000, "NO",
                 8.0, 4.0, "accepted"),
                ("2026-07-08 14:02:00", "XRP", "subminute_15m", 880, 3000, "YES",
                 6.0, 0.0, "rejected"),
                ("2026-07-08 10:00:00", "BTC", "subminute_5m", 280, 500, "YES",
                 100.0, 0.0, "rejected"),
            ],
        )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    assert block is not None
    rejects = block["rejects"]
    assert len(rejects) == 4  # last 20, newest first
    by_coin = {r["coin"]: r for r in rejects}
    # Won YES @0.70/$10: 10*(1/0.70 − 1 − 0.07*0.70*0.30) ≈ +4.14.
    assert by_coin["BTC"]["missed_pnl"] == pytest.approx(4.14, abs=0.01)
    assert by_coin["BTC"]["requote_drift"] == pytest.approx(0.03, abs=1e-6)
    # Lost NO (market went UP): forfeits the whole stake.
    assert by_coin["ETH"]["missed_pnl"] == pytest.approx(-8.0)
    # Unresolved → null missed_pnl and null drift (no requote_ask).
    assert by_coin["SOL"]["missed_pnl"] is None
    assert by_coin["SOL"]["requote_drift"] is None

    # No probe-inferred cause label rides on a reject row any more (the
    # taxonomy the on-chain forensic refuted, 2026-07-30) — only the PRICE
    # metric (requote_drift) and, once the chain job classifies it, the verdict.
    assert "miss_category" not in by_coin["BTC"]
    assert by_coin["BTC"].get("chain_verdict") is None

    stats = block["reject_stats"]
    # subminute_fills since go-live only: 1 rejected, 2 accepted, 14/24 filled
    # (ATTEMPT counts — every retry is its own order, labeled so by the UI).
    assert stats["n_rejected"] == 1
    assert stats["n_accepted"] == 2
    assert stats["fill_rate_usd"] == pytest.approx(14.0 / 24.0, abs=1e-4)
    # BTC + ETH resolutions FILLED (accepted subminute rows with filled_usd>0)
    # → not missed trades; the only misses are XRP (resolved won) + SOL
    # (pending). 1 resolved miss < 3 → would-be WR withheld (None).
    assert stats["n_reject_resolutions_filled"] == 2
    assert stats["n_missed_resolutions"] == 2
    assert stats["n_resolved_misses"] == 1
    assert stats["reject_winrate"] is None
    # missed total = XRP alone: 6·(1/0.75 − 1 − 0.07·0.75·0.25) ≈ +1.92 —
    # the filled BTC (+4.13) / ETH (−8.0) counterfactuals no longer leak in.
    assert stats["missed_pnl_total"] == pytest.approx(1.92, abs=0.01)
    # Nothing is classified (no miss_forensics rows) → no cause money split at
    # all, and both misses land in the no-verdict bucket as plain book misses.
    assert stats["chain_counts"] == {}
    assert stats["chain_pnl"] == {}
    assert stats["missed_pnl_winnable"] == pytest.approx(0.0)
    assert stats["missed_pnl_never_winnable"] == pytest.approx(0.0)
    assert stats["n_no_chain_verdict"] == 2
    assert stats["no_chain_verdict_kinds"] == {"book_miss": 2}

    # ONE missed-trade row per resolution, newest first, attempts counted.
    missed = block["missed_trades"]
    assert [(t["coin"], t["attempts"], t["miss_kind"]) for t in missed] == [
        ("SOL", 1, "book_miss"), ("XRP", 1, "book_miss"),
    ]
    xrp = next(t for t in missed if t["coin"] == "XRP")
    assert xrp["chain_winnable"] is None      # no verdict → no claim either way
    assert xrp["missed_pnl"] == pytest.approx(1.92, abs=0.01)
    assert xrp["interval"] == "15m"


def test_v2_block_reject_stats_collapses_retries_per_window(monkeypatch, tmp_path):
    """step1last places ONE bet per market window, so retries logged as separate
    reject rows must be counted ONCE — not summed. Seed window A retried twice
    (same coin/sleeve/window_start_ts/side) plus two other single-attempt windows
    and assert the missed-PnL total and would-be WR count A once, using the
    full-size attempt (max requested) as the representative."""
    import sqlite3

    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = init_db(tmp_path / "rejects_retry.db")
    conn.row_factory = sqlite3.Row
    try:
        conn.executemany(
            "INSERT INTO reject_requotes "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " limit_price, requested_usd, requote_ask, requote_age_ms, "
            " resolved_outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                # Window A — SAME (coin, sleeve, window_start_ts, side), retried
                # twice. Different requested → representative is the full-size
                # attempt ($10 → +4.13), NOT the partial ($4) and NOT their sum.
                ("2026-07-08 14:00:00", "BTC", "subminute_5m", 280, 1000, "YES",
                 0.70, 4.0, 0.72, 1100, "UP"),
                ("2026-07-08 14:00:10", "BTC", "subminute_5m", 285, 1000, "YES",
                 0.70, 10.0, 0.73, 1100, "UP"),
                # Window B — single winning attempt (+4.13).
                ("2026-07-08 14:01:00", "ETH", "subminute_5m", 285, 2000, "YES",
                 0.70, 10.0, 0.71, 1050, "UP"),
                # Window C — single losing attempt (−8.0).
                ("2026-07-08 14:02:00", "XRP", "subminute_15m", 880, 3000, "NO",
                 0.65, 8.0, 0.66, 1200, "UP"),
            ],
        )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    assert block is not None
    stats = block["reject_stats"]
    # 3 resolved WINDOWS (A won, B won, C lost) — A's two rows collapse to one.
    # WR = 2/3, not 3/4 (which is what summing A's two rows would give).
    assert stats["reject_winrate"] == pytest.approx(2.0 / 3.0, abs=1e-4)
    # missed total = A(+4.14, counted once at full size) + B(+4.14) + C(−8.0).
    # Summing A's retries would give ≈ +4.40; the partial-size attempt would give
    # a different A term — both are wrong.
    assert stats["missed_pnl_total"] == pytest.approx(0.28, abs=0.02)


def test_v2_block_reject_stats_dedups_repeat_attempts(monkeypatch, tmp_path):
    """Repeat attempts on the SAME resolution (retry / top-up) must be counted
    ONCE. A resolution is (coin, interval, window_start_ts, side) and the
    per-second grid can attempt it several times, first-fill-wins. When every
    attempt no-fills, the miss must be counted ONCE — the fill-tax aggregate
    keys on the interval bucket, not the sleeve label or the attempt, so a
    resolution rejected twice is one window, not two. (Pre-fix, XRP 15m DOWN
    attempted twice double-counted the missed PnL and the would-be WR.)"""
    import sqlite3

    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = init_db(tmp_path / "rejects_xsleeve.db")
    conn.row_factory = sqlite3.Row
    try:
        conn.executemany(
            "INSERT INTO reject_requotes "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " limit_price, requested_usd, requote_ask, requote_age_ms, "
            " resolved_outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                # Window X — XRP 15m DOWN, attempted TWICE (offset 880 then a
                # repeat at 885), same coin/interval/window_start_ts/side, both
                # LOSS (market went UP → each forfeits its −$2.63 stake). Same
                # interval bucket "15m" → ONE window, counted ONCE.
                #
                # The two attempts carry DIFFERENT sleeve labels ON PURPOSE
                # (subminute_15m + other_15m, the neutral second-sleeve label
                # test_multifill/test_v2_pipeline_integration use). That is what
                # gives this test its teeth: _miss_res_key keys on
                # _sleeve_interval_tag(sleeve), so both labels reduce to "15m"
                # and fold. Label them identically and a regression that keyed
                # on the RAW sleeve would collapse them too and pass silently.
                ("2026-07-11 00:44:10", "XRP", "subminute_15m", 880, 1000, "NO",
                 0.90, 2.63, 0.91, 1100, "UP"),
                ("2026-07-11 00:44:20", "XRP", "other_15m", 885, 1000, "NO",
                 0.90, 2.63, 0.91, 1100, "UP"),
                # Window Y — ETH 5m UP, single winning attempt (+4.13).
                ("2026-07-11 00:45:00", "ETH", "subminute_5m", 280, 2000, "YES",
                 0.70, 10.0, 0.71, 1050, "UP"),
                # Window Z — BTC 5m UP, single winning attempt (+4.13).
                ("2026-07-11 00:46:00", "BTC", "subminute_5m", 285, 3000, "YES",
                 0.70, 10.0, 0.71, 1050, "UP"),
            ],
        )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    stats = block["reject_stats"]
    # 3 resolved WINDOWS (X lost, Y won, Z won) — X's two attempts collapse to
    # one. WR = 2/3, NOT 2/4 (what counting X's attempts separately gives).
    assert stats["reject_winrate"] == pytest.approx(2.0 / 3.0, abs=1e-4)
    # missed total = X(−2.63, once) + Y(+4.13) + Z(+4.13) = +5.64. Double-counting
    # X would give −2.63*2 + 8.27 = +3.01.
    assert stats["missed_pnl_total"] == pytest.approx(5.64, abs=0.02)


def test_v2_block_reject_stats_empty_db(monkeypatch, tmp_path):
    """Empty reject/fill tables degrade to zeros/None — no crash, JSON-safe."""
    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = init_db(tmp_path / "rejects_empty.db")
    try:
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()
    assert block["rejects"] == []
    assert block["missed_trades"] == []
    stats = block["reject_stats"]
    assert stats["n_rejected"] == 0
    assert stats["n_accepted"] == 0
    assert stats["fill_rate_usd"] is None
    assert stats["missed_pnl_total"] == 0.0
    assert stats["reject_winrate"] is None
    assert stats["n_missed_resolutions"] == 0
    assert stats["n_resolved_misses"] == 0
    assert stats["n_reject_resolutions_filled"] == 0
    assert stats["chain_counts"] == {}
    assert stats["chain_pnl"] == {}
    assert stats["missed_pnl_winnable"] == 0.0
    assert stats["missed_pnl_never_winnable"] == 0.0
    assert stats["n_no_chain_verdict"] == 0
    assert stats["no_chain_verdict_kinds"] == {}
    assert stats["chain_forensics"] is None


def _reject_db(tmp_path, name):
    import sqlite3

    from cryptobot.db import init_db

    conn = init_db(tmp_path / name)
    conn.row_factory = sqlite3.Row
    return conn


def _seed_rejects(conn, rows):
    """rows: (created_at, coin, sleeve, ws, side, limit, req, requote_ask,
    age_ms, resolved)."""
    conn.executemany(
        "INSERT INTO reject_requotes "
        "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
        " limit_price, requested_usd, requote_ask, requote_age_ms, "
        " resolved_outcome) VALUES (?, ?, ?, 285, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def test_v2_missed_trades_one_row_per_resolution(monkeypatch, tmp_path):
    """Operator 2026-07-23: 'for a bet, i may have 3 or 4 attempt, but u can
    write 3 or 4 missing trade — it is just 1 missing trade only.' Three FAK
    reject attempts on the SAME resolution emit ONE missed_trades row carrying
    the attempt count; the aggregates count ONE missed resolution."""
    from cryptobot import config
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = _reject_db(tmp_path, "missed_dedup.db")
    try:
        _seed_rejects(conn, [
            ("2026-07-22 14:00:01", "BTC", "subminute_5m", 1000, "YES",
             0.70, 4.0, 0.74, 1100, "UP"),
            ("2026-07-22 14:00:05", "BTC", "subminute_5m", 1000, "YES",
             0.71, 10.0, 0.75, 1150, "UP"),
            ("2026-07-22 14:00:09", "BTC", "subminute_5m", 1000, "YES",
             0.72, 10.0, None, 1200, "UP"),
        ])
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    missed = block["missed_trades"]
    assert len(missed) == 1
    assert missed[0]["attempts"] == 3
    assert missed[0]["coin"] == "BTC"
    stats = block["reject_stats"]
    assert stats["n_missed_resolutions"] == 1
    assert stats["n_resolved_misses"] == 1
    # The per-attempt display list still carries every retry (drill-down).
    assert len(block["rejects"]) == 3


def _seed_chain_verdict(conn, *, coin, sleeve, ws, side, verdict, created,
                        limit=0.70, dt=None, price=None, size=None, rival=None):
    """A rejected `subminute_fills` row + its `miss_forensics` verdict, keyed
    onto the SAME resolution (coin, interval-tag, window_start_ts, direction) as
    the seeded reject_requotes row so the verdict joins onto that missed trade.
    Mirrors what the off-path forensics job writes."""
    fid = conn.execute(
        "INSERT INTO subminute_fills "
        "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
        " requested_usd, filled_usd, outcome, limit_price) "
        "VALUES (?, ?, ?, 285, ?, ?, 10.0, 0.0, 'rejected', ?)",
        (created, coin, sleeve, ws, side, limit),
    ).lastrowid
    conn.execute(
        "INSERT INTO miss_forensics (fill_id, verdict, beat_dt_s, beat_price, "
        " beat_size, rival_addr, lag_s_used) VALUES (?, ?, ?, ?, ?, ?, 3.0)",
        (fid, verdict, dt, price, size, rival),
    )
    return fid


def test_v2_missed_trades_kind_is_about_our_order_never_the_book(
    monkeypatch, tmp_path
):
    """`miss_kind` is a fact about OUR order, read off OUR rows — it is NOT a
    cause (the cause is the chain verdict) and it never consults the
    post-reject book:
      * ghost_fill  — an ACCEPTED subminute order filled $0 (phantom);
      * venue_error — every FAILED live_order_attempts error_kind for the
                      resolution is a NON-liquidity error (auth/...): we broke,
                      the book was never contested;
      * book_miss   — the ordinary case, and the ONLY kind the chain job
                      adjudicates. A MIXED error set (auth + no_liquidity)
                      stays a book miss: liquidity was genuinely refused.
    The probe ask deliberately varies across the windows (0.69 <= limit, 0.74 >
    limit, absent) and changes NOTHING — the taxonomy that read a cause out of
    that comparison was refuted on-chain 2026-07-30 and deleted."""
    from cryptobot import config
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = _reject_db(tmp_path, "missed_kinds.db")
    try:
        _seed_rejects(conn, [
            # W1 probe ask BELOW our limit (the old "taker race" shape).
            ("2026-07-22 14:00:00", "BTC", "subminute_5m", 1000, "YES",
             0.70, 10.0, 0.69, 1200, "UP"),
            # W2 probe ask ABOVE our limit (the old "maker pull" shape).
            ("2026-07-22 14:01:00", "ETH", "subminute_5m", 2000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
            # W3 no probe ask at all (the old "book gone" shape).
            ("2026-07-22 14:02:00", "SOL", "subminute_5m", 3000, "YES",
             0.70, 10.0, None, 1500, "UP"),
            # W4 venue error: the failed attempts say auth.
            ("2026-07-22 14:05:00", "ETH", "subminute_15m", 6000, "YES",
             0.70, 10.0, 0.69, 1200, "UP"),
            # W5 ghost: an accepted-but-$0 subminute order exists.
            ("2026-07-22 14:06:00", "SOL", "subminute_15m", 7000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
            # W6 mixed error kinds (auth + no_liquidity) -> still a book miss.
            ("2026-07-22 14:07:00", "XRP", "subminute_15m", 8000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
        ])
        conn.executemany(
            "INSERT INTO live_order_attempts "
            "(created_at, wallet_id, mode, coin, sleeve, entry_minute, side, "
            " window_start_ts, client_order_id, amount_usd, status, error_kind) "
            "VALUES (?, 'w', 'LIVE', ?, ?, 0, 'YES', ?, ?, 10.0, 'FAILED', ?)",
            [
                ("2026-07-22 14:05:00", "ETH", "subminute_15m", 6000, "c1", "auth"),
                ("2026-07-22 14:07:00", "XRP", "subminute_15m", 8000, "c2", "auth"),
                ("2026-07-22 14:07:01", "XRP", "subminute_15m", 8000, "c3",
                 "no_liquidity"),
            ],
        )
        conn.execute(
            "INSERT INTO subminute_fills "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " requested_usd, filled_usd, outcome) "
            "VALUES ('2026-07-22 14:06:00', 'SOL', 'subminute_15m', 285, "
            "        7000, 'YES', 10.0, 0.0, 'accepted')"
        )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    by_ws = {t["window_start_ts"]: t for t in block["missed_trades"]}
    # The three differently-probed windows are the SAME kind — the probe no
    # longer labels anything.
    assert by_ws[1000]["miss_kind"] == "book_miss"
    assert by_ws[2000]["miss_kind"] == "book_miss"
    assert by_ws[3000]["miss_kind"] == "book_miss"
    assert by_ws[6000]["miss_kind"] == "venue_error"
    assert by_ws[7000]["miss_kind"] == "ghost_fill"
    assert by_ws[8000]["miss_kind"] == "book_miss"
    # The probe ask still rides along as a PRICE metric (how far the book
    # moved), which is the only role it kept.
    assert by_ws[2000]["requote_ask"] == pytest.approx(0.74)

    stats = block["reject_stats"]
    one = 10.0 * (1.0 / 0.70 - 1.0 - 0.07 * 0.70 * 0.30)
    # No verdicts seeded -> nothing is claimed about cause, and the whole
    # missed total sits unattributed. Every window is a YES win at 0.70/$10.
    assert stats["missed_pnl_total"] == pytest.approx(6 * one, abs=0.05)
    assert stats["missed_pnl_winnable"] == pytest.approx(0.0)
    assert stats["missed_pnl_never_winnable"] == pytest.approx(0.0)
    assert stats["n_no_chain_verdict"] == 6
    assert stats["no_chain_verdict_kinds"] == {
        "book_miss": 4, "venue_error": 1, "ghost_fill": 1,
    }
    assert stats["reject_winrate"] == pytest.approx(1.0)


def test_v2_missed_trades_chain_verdict_owns_the_cause_split(
    monkeypatch, tmp_path
):
    """The ON-CHAIN verdict is the only cause axis, and the money split follows
    from it: FASTER_TAKER = the tape proves our level traded, so a faster order
    would have won it (winnable); REPRICED_ONLY / MAKER_CANCEL = our level never
    traded, so no speed wins them (never-winnable, a mirage counterfactual);
    PENDING = no evidence YET, claimed neither way. A miss with no verdict row
    at all lands in the no-verdict bucket labeled by its KIND.

    Verdict maps come back in the producer's strongest-evidence-first order so
    the dashboard can iterate the payload instead of holding a second copy of
    the vocabulary."""
    from cryptobot import config
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = _reject_db(tmp_path, "missed_chain_split.db")
    try:
        _seed_rejects(conn, [
            ("2026-07-22 14:00:00", "BTC", "subminute_5m", 1000, "YES",
             0.70, 10.0, 0.69, 1200, "UP"),
            ("2026-07-22 14:01:00", "ETH", "subminute_5m", 2000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
            ("2026-07-22 14:02:00", "SOL", "subminute_5m", 3000, "YES",
             0.70, 10.0, None, 1500, "UP"),
            ("2026-07-22 14:03:00", "XRP", "subminute_5m", 4000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
            # No verdict row for this one at all.
            ("2026-07-22 14:04:00", "BTC", "subminute_15m", 5000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
        ])
        # Seeded so the payload order cannot come from the insertion order by
        # accident (MAKER_CANCEL first, FASTER_TAKER last).
        for ws, coin, sleeve, verdict, created in [
            (3000, "SOL", "subminute_5m", "MAKER_CANCEL", "2026-07-22 14:02:00"),
            (2000, "ETH", "subminute_5m", "REPRICED_ONLY", "2026-07-22 14:01:00"),
            (4000, "XRP", "subminute_5m", "PENDING", "2026-07-22 14:03:00"),
            (1000, "BTC", "subminute_5m", "FASTER_TAKER", "2026-07-22 14:00:00"),
        ]:
            faster = verdict == "FASTER_TAKER"
            _seed_chain_verdict(
                conn, coin=coin, sleeve=sleeve, ws=ws, side="YES",
                verdict=verdict, created=created,
                dt=-1.0 if faster else None,
                price=0.70 if faster else None,
                size=21.0 if faster else None,
                rival="0xrival" if faster else None,
            )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    by_ws = {t["window_start_ts"]: t for t in block["missed_trades"]}
    assert by_ws[1000]["chain_verdict"] == "FASTER_TAKER"
    assert by_ws[1000]["chain_winnable"] is True
    assert by_ws[1000]["chain_beat_dt_s"] == pytest.approx(-1.0)
    assert by_ws[1000]["chain_rival_addr"] == "0xrival"
    assert by_ws[2000]["chain_winnable"] is False       # repriced
    assert by_ws[3000]["chain_winnable"] is False       # maker cancel
    assert by_ws[4000]["chain_winnable"] is None        # pending — no claim
    assert by_ws[5000].get("chain_verdict") is None
    assert by_ws[5000]["chain_winnable"] is None

    stats = block["reject_stats"]
    one = 10.0 * (1.0 / 0.70 - 1.0 - 0.07 * 0.70 * 0.30)
    # Strongest evidence FIRST — the producer's order, not the seed order.
    assert list(stats["chain_counts"]) == [
        "FASTER_TAKER", "REPRICED_ONLY", "MAKER_CANCEL", "PENDING",
    ]
    assert stats["chain_counts"] == {
        "FASTER_TAKER": 1, "REPRICED_ONLY": 1, "MAKER_CANCEL": 1, "PENDING": 1,
    }
    assert list(stats["chain_pnl"]) == list(stats["chain_counts"])
    assert stats["chain_pnl"]["FASTER_TAKER"] == pytest.approx(one, abs=0.02)
    assert stats["missed_pnl_winnable"] == pytest.approx(one, abs=0.02)
    assert stats["missed_pnl_never_winnable"] == pytest.approx(2 * one, abs=0.03)
    # PENDING is in neither split, but still in the total.
    assert stats["missed_pnl_total"] == pytest.approx(5 * one, abs=0.05)
    assert stats["n_no_chain_verdict"] == 1
    assert stats["no_chain_verdict_kinds"] == {"book_miss": 1}
    # The unadjudicated cohort carries its own dollars, so the table that lists
    # it is a PARTITION: winnable + never-winnable + no-verdict + PENDING's own
    # chain_pnl row == the total. No missed dollar renders nowhere.
    assert stats["missed_pnl_no_verdict"] == pytest.approx(one, abs=0.02)
    assert (
        stats["missed_pnl_winnable"]
        + stats["missed_pnl_never_winnable"]
        + stats["missed_pnl_no_verdict"]
        + stats["chain_pnl"]["PENDING"]
    ) == pytest.approx(stats["missed_pnl_total"], abs=0.05)


def test_v2_missed_trades_priced_at_the_full_size_attempt(monkeypatch, tmp_path):
    """One rule for every miss: the representative attempt is the one carrying
    the full intended size (max requested_usd), so the counterfactual is priced
    at the limit we meant to buy. No cause-conditional pricing — the old
    "price a taker_race at a RACE attempt's limit" special case died with the
    probe taxonomy it depended on.

    Window: attempt A $10 @0.72, attempt B $4 @0.70 -> priced at A."""
    from cryptobot import config
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = _reject_db(tmp_path, "missed_rep_attempt.db")
    try:
        _seed_rejects(conn, [
            ("2026-07-22 14:00:00", "BTC", "subminute_5m", 1000, "YES",
             0.72, 10.0, 0.75, 1200, "UP"),
            ("2026-07-22 14:00:05", "BTC", "subminute_5m", 1000, "YES",
             0.70, 4.0, 0.70, 1200, "UP"),
        ])
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    missed = block["missed_trades"]
    assert len(missed) == 1
    t = missed[0]
    assert t["attempts"] == 2
    assert t["limit_price"] == pytest.approx(0.72)
    assert t["requested_usd"] == pytest.approx(10.0)
    expected = 10.0 * (1.0 / 0.72 - 1.0 - 0.07 * 0.72 * 0.28)
    assert t["missed_pnl"] == pytest.approx(expected, abs=0.01)
    stats = block["reject_stats"]
    assert stats["missed_pnl_total"] == pytest.approx(expected, abs=0.01)


def test_v2_missed_trades_excludes_filled_resolutions(monkeypatch, tmp_path):
    """A resolution that eventually FILLED is ZERO missed trades — whether the
    fill landed as an accepted subminute row (filled_usd > 0) or only as a
    reconciler-rescued ledger fill (live_filled_size > 0, the deferred
    fill-GET path). Its rejects are retries of a completed bet; only the
    truly no-fill window remains a miss."""
    from cryptobot import config
    from cryptobot.execution.ledger import record_bet
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves
    from cryptobot.tests.test_slot_state import make_signal

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = _reject_db(tmp_path, "missed_filled.db")
    try:
        _seed_rejects(conn, [
            # W1 — reject then an accepted subminute fill.
            ("2026-07-22 14:00:00", "BTC", "subminute_5m", 1000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
            # W2 — reject then a ledger row whose reconciler landed the fill.
            ("2026-07-22 14:01:00", "ETH", "subminute_5m", 2000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
            # W3 — reject, never filled: the only missed trade.
            ("2026-07-22 14:02:00", "SOL", "subminute_5m", 3000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
        ])
        conn.execute(
            "INSERT INTO subminute_fills "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " requested_usd, filled_usd, outcome) "
            "VALUES ('2026-07-22 14:00:05', 'BTC', 'subminute_5m', 290, "
            "        1000, 'YES', 10.0, 10.0, 'accepted')"
        )
        rid = record_bet(
            conn,
            make_signal(coin="ETH", sleeve="subminute_5m", side="YES",
                        window_start_ts=2000, bet_size=7.0, fill_price=0.70),
            mode="LIVE", live_filled_size=10.0, live_filled_price=0.70,
        )
        # make_signal derives interval_min=15 for non-v6_5m sleeves — pin the
        # real 5m so the ledger fill keys onto the same resolution bucket.
        conn.execute(
            "UPDATE ledger SET offset_sec = 290, interval_min = 5 WHERE id = ?",
            (rid,),
        )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    missed = block["missed_trades"]
    assert [t["coin"] for t in missed] == ["SOL"]
    stats = block["reject_stats"]
    assert stats["n_missed_resolutions"] == 1
    assert stats["n_reject_resolutions_filled"] == 2
    one = 10.0 * (1.0 / 0.70 - 1.0 - 0.07 * 0.70 * 0.30)
    assert stats["missed_pnl_total"] == pytest.approx(one, abs=0.02)


def test_v2_missed_trades_ghost_survives_unless_really_filled(
    monkeypatch, tmp_path
):
    """An accepted-but-$0 order marks the resolution ghost_fill, but a LATER
    real fill on the same resolution wins (first-fill-wins): the window is
    then a filled resolution, not a ghost miss."""
    from cryptobot import config
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    snapshot_builder.clear_aux_cache()
    conn = _reject_db(tmp_path, "missed_ghost.db")
    try:
        _seed_rejects(conn, [
            ("2026-07-22 14:00:00", "BTC", "subminute_5m", 1000, "YES",
             0.70, 10.0, 0.74, 1200, "UP"),
        ])
        conn.executemany(
            "INSERT INTO subminute_fills "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " requested_usd, filled_usd, outcome) VALUES (?, ?, ?, 285, ?, "
            " 'YES', 10.0, ?, 'accepted')",
            [
                ("2026-07-22 14:00:02", "BTC", "subminute_5m", 1000, 0.0),
                ("2026-07-22 14:00:06", "BTC", "subminute_5m", 1000, 10.0),
            ],
        )
        conn.commit()
        block = snapshot_builder._v2_block(conn, load_sleeves(), {})
    finally:
        conn.close()

    assert block["missed_trades"] == []
    assert block["reject_stats"]["n_reject_resolutions_filled"] == 1


def test_v2_block_none_when_disabled(monkeypatch, tmp_path):
    """No v2 block when the executor is disabled (page renders its hint)."""
    from cryptobot import config
    from cryptobot.db import init_db
    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves

    # Build the checkpoint portfolio while the flag is on (the checkpoint-only
    # config can't be loaded with it off), then disable so the _v2_block guard
    # — not a load failure — is what returns None.
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    portfolio = load_sleeves()
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", False)
    conn = init_db(tmp_path / "v2off.db")
    try:
        block = snapshot_builder._v2_block(conn, portfolio, {})
    finally:
        conn.close()
    assert block is None


def test_static_portfolio_shape_matches_live():
    """Review #16 — the static fallback must produce the same key set as
    the live SSE _portfolio_block so the dashboard fallback doesn't read
    undefined fields when SSE is unavailable."""
    bs = _load_module()
    static_portfolio = bs.load_portfolio()
    static_keys = set(static_portfolio.keys()) - {"sleeves"}

    from cryptobot.observability import snapshot_builder
    from cryptobot.strategy.portfolio_loader import load_sleeves
    live_portfolio = snapshot_builder._portfolio_block(load_sleeves())
    live_keys = set(live_portfolio.keys()) - {"sleeves"}

    assert static_keys == live_keys, (
        f"shape mismatch — only-in-static={static_keys - live_keys}, "
        f"only-in-live={live_keys - static_keys}"
    )


def test_load_portfolio_missing_returns_none(tmp_path, monkeypatch):
    bs = _load_module()
    monkeypatch.setattr(bs, "V6_CONFIG", tmp_path / "no-such-file.json")
    assert bs.load_portfolio() is None


def test_load_portfolio_sleeve_fields():
    bs = _load_module()
    portfolio = bs.load_portfolio()
    s5 = next(s for s in portfolio["sleeves"] if s["label"] == "subminute_5m")
    assert s5["alloc"] == pytest.approx(1.0)
    assert s5["interval"] == "5m"
    assert s5["entry_minutes"] == [0, 1, 2, 3, 4]
    assert s5["signal_source"] == "hgb"
    assert s5["min_lut_n"] == 0
    # 0.266 = the deployed both-hold 1%/1% twin's worst-fold maxDD (fold C),
    # replacing the pre-resize 0.188 (operator order 2026-07-31).
    assert s5["backtest_max_drawdown_frac"] == pytest.approx(0.266)


def test_dashboard_shell_has_no_mock_payload():
    index = (_REPO_ROOT / "polymarket" / "dashboard" / "index.html").read_text(encoding="utf-8")
    bridge = (_REPO_ROOT / "polymarket" / "dashboard" / "live-bridge.js").read_text(encoding="utf-8")
    app = (_REPO_ROOT / "polymarket" / "dashboard" / "app.jsx").read_text(encoding="utf-8")
    shell = (_REPO_ROOT / "polymarket" / "dashboard" / "shell.jsx").read_text(encoding="utf-8")
    config_pages = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-config.jsx").read_text(
        encoding="utf-8"
    )
    snapshot_builder = (_REPO_ROOT / "polymarket" / "dashboard" / "build_snapshot.py").read_text(
        encoding="utf-8"
    )

    assert not (_REPO_ROOT / "polymarket" / "dashboard" / "mock-payload.js").exists()
    assert "mock-payload" not in index
    assert "MOCK_PAYLOAD" not in bridge
    assert "key: 'research'" not in shell
    assert "research:" not in app
    assert "ResearchPage" not in config_pages
    assert "BNB" not in app
    assert "DOGE" not in app
    assert ("Portfolio" + "V2Panel") not in app
    assert ("portfolio" + "_v2_backtest") not in snapshot_builder
    assert "PORTFOLIO_V2_SUMMARY" not in snapshot_builder
    assert "sleeves-full" not in app


def test_live_bridge_rebounds_ledger_and_keeps_current_pending_rows():
    """The browser-side merge is bounded even when a delta follows bootstrap."""
    payload = _run_bridge_js(
        """
        const first = [{ id: 1, outcome: 'PENDING', bet_ts: '2026-08-02 00:00:01' }];
        for (let id = 2; id <= 500; id++) first.push({ id, outcome: 'LOSS' });
        const es = w.__SSE;
        es.emit('tick', { data: JSON.stringify({ kpis: {}, ledger: first }) });
        es.emit('tick', { data: JSON.stringify({ kpis: {}, ledger: [{ id: 501, outcome: 'WIN' }] }) });
        const ids = w.LIVE.ledger.map((r) => r.id);
        console.log(JSON.stringify({ length: ids.length, hasCurrent: ids.includes(501), hasPending: ids.includes(1) }));
        """
    )
    assert payload == {"length": 500, "hasCurrent": True, "hasPending": True}


def test_live_bridge_uses_server_ledger_limit_when_present():
    """A changed server retention window must change the browser cap too."""
    payload = _run_bridge_js(
        """
        const rows = [];
        for (let id = 1; id <= 5; id++) rows.push({ id, outcome: 'LOSS' });
        const es = w.__SSE;
        es.emit('tick', { data: JSON.stringify({ kpis: {}, ledger_limit: 3, ledger: rows }) });
        console.log(JSON.stringify({ length: w.LIVE.ledger.length }));
        """
    )
    assert payload == {"length": 3}


def test_live_bridge_backs_off_closed_stream_recovery():
    """A closed 503 stream must not reconnect in a tight browser loop."""
    payload = _run_bridge_js(
        """
        (async function () {
          const es = w.__SSE;
          es.readyState = 2;
          es.onerror();
          await Promise.resolve();
          await Promise.resolve();
          console.log(JSON.stringify({ delayMs: timers[0].ms, scheduled: timers.length }));
        })();
        """
    )
    assert payload == {"delayMs": 1000, "scheduled": 1}


def test_live_bridge_clears_stale_session_when_sse_auth_expires():
    """A 401 on the auth probe removes the last live payload and gates login."""
    payload = _run_bridge_js(
        """
        const es = w.__SSE;
        es.emit('tick', { data: JSON.stringify({ kpis: { pnl_net_usd: 1 }, ledger: [] }) });
        authStatus = 401;
        es.readyState = 1;
        es.onerror();
        Promise.resolve().then(() => Promise.resolve()).then(() => {
          console.log(JSON.stringify({ state: w.__SSE_STATE, live: w.LIVE }));
        });
        """
    )
    assert payload == {"state": "auth-required", "live": None}


def test_live_bridge_reloads_snapshot_after_login():
    """The authenticated static snapshot is retried after a 401 pre-login load."""
    payload = _run_bridge_js(
        """
        snapshotStatus = 401;
        snapshotBody = '{}';
        snapshotStatus = 200;
        snapshotBody = JSON.stringify({ generated_at: 'after-login' });
        w.CB_AUTH.login('password').then(() => {
          console.log(JSON.stringify({ snapshot: w.SNAPSHOT.generated_at }));
        });
        """
    )
    assert payload == {"snapshot": "after-login"}


def test_dashboard_api_helper_sends_session_bound_csrf_header():
    """The browser mutation helper must satisfy the server's CSRF contract."""
    payload = _run_api_js(
        """
        w.CB_API.post('/api/v2/operator/start', { reason: 'test' }).then(() => {
          console.log(JSON.stringify({
            url: calls[0].url,
            method: calls[0].opts.method,
            credentials: calls[0].opts.credentials,
            csrf: calls[0].opts.headers['X-CSRF-Token'],
            contentType: calls[0].opts.headers['Content-Type'],
            body: JSON.parse(calls[0].opts.body),
          }));
        });
        """
    )
    assert payload == {
        "url": "/api/v2/operator/start",
        "method": "POST",
        "credentials": "same-origin",
        "csrf": "csrf+value",
        "contentType": "application/json",
        "body": {"reason": "test"},
    }


def test_daily_range_excludes_the_boundary_bucket():
    """Daily MYT buckets must not turn a 7d range into an eighth day."""
    payload = _run_client_js(
        """
        const cut = cb.fmt.dateMyt(Date.now() - 7 * 86400000);
        const selected = cb.dailyInRange([
          { date: cut, pnl_net: 1 },
          { date: '2999-01-01', pnl_net: 2 },
        ], '7d');
        console.log(JSON.stringify(selected.map((row) => row.date)));
        """
    )
    assert payload[0] == "2999-01-01"


def test_miss_cause_surfaces_are_chain_verdicts_only():
    """Guard for the 2026-07-30 taxonomy replacement: the sub-minute page shows
    exactly ONE miss-cause axis, the on-chain verdict.

    The probe-inferred surfaces are DELETED, not hidden — a stale payload that
    still ships `miss_category` must find no renderer waiting for it. And the
    verdict VOCABULARY stays server-owned: the strip and the table iterate the
    payload's own key order (which snapshot_builder emits strongest-evidence
    first from the miss_forensics constants), so a verdict renamed or added
    upstream can never silently vanish from a panel."""
    page = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-subminute.jsx").read_text(
        encoding="utf-8"
    )
    data = (_REPO_ROOT / "polymarket" / "dashboard" / "data.jsx").read_text(encoding="utf-8")

    # No renderer, no lookup table, no payload field for the refuted taxonomy.
    for dead in ("MISS_CATEGORY", "MissCategoryPill", "miss_category",
                 "missed_pnl_reachable", "missed_pnl_unreachable",
                 "category_counts", "category_pnl", "pull_chase_shadow"):
        assert dead not in page, f"{dead} still lives in pages-subminute.jsx"
        assert dead not in data, f"{dead} still lives in data.jsx"

    # The chain verdict IS the cause presentation.
    assert "ChainVerdictChip" in page
    assert "ChainForensicsStrip" in page
    # Server-driven order: never Object.keys(CHAIN_VERDICT).
    assert "Object.keys(CHAIN_VERDICT)" not in page
    assert "Object.keys(counts)" in page          # the 7d strip
    assert "Object.keys(chainCounts)" in page     # the per-resolution table
    # An unknown verdict still renders (decoration falls back to the raw key).
    assert "function verdictMeta(" in page
    # Whether a cohort is a mirage comes from the server's chain_winnable flag,
    # not from a second copy of the never-winnable set in the client — and it is
    # read as THREE states, so an unadjudicated cohort is muted rather than toned
    # like a chain-proven one.
    assert "const winnable = verdictWinnable[k]" in page
    assert "const mirage = winnable === false" in page
    assert "winnable === true ? tone(chainPnl[k]) : 'muted'" in page
    assert "'REPRICED_ONLY' || " not in page
    # No verdict SPELLING may gate rendering: the payload's own fields decide.
    # (A renamed verdict would otherwise keep its chip and lose its evidence.)
    assert "verdict === '" not in page


def test_overview_and_journal_use_one_money_bundle():
    """The account maze collapsed to ONE selector (window.CB.money) read by the
    Overview hero and the Journal net-P&L cell. The old
    usesAccountCapital/capitalBase/accountCapitalValue/accountPnlTotal/
    accountPnlSub selectors are DELETED (defs + exports + call sites), and so is
    accountEquity (2026-07-22): its only consumer switched to the unrescaled
    lifetime series, retiring the affine cash re-anchor for good."""
    overview = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-overview.jsx").read_text(
        encoding="utf-8"
    )
    data = (_REPO_ROOT / "polymarket" / "dashboard" / "data.jsx").read_text(encoding="utf-8")
    live_page = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-live.jsx").read_text(encoding="utf-8")

    # The retired selector maze is gone from data.jsx AND every consumer.
    for retired in (
        "usesAccountCapital", "capitalBase", "accountCapitalValue",
        "accountPnlTotal", "accountPnlSub",
    ):
        assert f"function {retired}" not in data, f"{retired} def still present"
    # Comments may reference history, but no live call survives.
    for consumer in (overview, live_page):
        stripped = re.sub(r"//.*", "", consumer)
        for retired in (
            "usesAccountCapital", "capitalBase(", "accountCapitalValue(",
            "accountPnlTotal(", "accountPnlSub(",
        ):
            assert retired not in stripped, f"{retired} call still in a page"

    # The ONE money bundle + its provenance fields.
    assert "function money(k)" in data
    assert "kk.account_capital_live" in data
    assert "kk.account_pnl_total_live" in data
    # accountEquity is DELETED outright (def + export + comment references);
    # money() stays exported.
    assert "accountEquity" not in data
    assert "strategyBase, money, withPaperBasis," in data
    # F5 helper + the ONE net rule both live in data.jsx. Cash-first
    # (operator 2026-07-24): headline net = the era-anchored CASH result
    # (account_pnl_era_cash), so it reads green when the account is up and
    # matches the equity tile; paper only when no cash figure exists.
    assert "function withPaperBasis(m, paper)" in data
    assert "net: cashNet != null ? cashNet : bridgePaper," in data

    # Overview hero reads the bundle; the equity value + delta still render.
    assert "const m = useMemo(() => money(k), [k]);" in overview
    assert "const base = m.basis;" in overview
    assert "const capitalValue = m.cashLive != null ? m.cashLive : m.basis;" in overview
    assert "const capitalPnl = m.net;" in overview
    assert '<span className="hero-value">{fmt.money(capitalValue)}</span>' in overview
    # Canonical live-cash equity label (2026-07-11 real-time sweep). Item 25
    # (2026-07-21): "· live" is EARNED — a degraded cash sample (reconciliation
    # fallback) drops the live tag and surfaces its as-of instead.
    #
    # RE-PINNED 2026-08-05 (operator): the ternary is UNCHANGED and still the
    # single source of the basis string — it just renders into `title=` now,
    # not into the visible headline label, which is the bare word "equity".
    assert (
        "m.cashLive != null ? (m.degraded ? 'equity (CLOB) · cash' : "
        "'equity (CLOB) · cash · live') : 'equity · ledger paper'"
    ) in overview
    assert (
        '<div className="hero-label" title={m.cashLive != null ? '
        "(m.degraded ? 'equity (CLOB) · cash' : 'equity (CLOB) · cash · live') "
        ": 'equity · ledger paper'}>equity{capitalAsOf != null"
    ) in overview
    # No basis suffix survives in the VISIBLE label text.
    assert ">equity (CLOB)" not in overview
    assert ">equity · ledger paper" not in overview
    assert "const capitalAsOf = m.degraded ? m.asOfMs : null;" in overview
    # The hero's third metric is no longer a money figure at all (2026-08-05
    # declutter): the net story moved into the strip inside the same card and
    # the cell became a TRADE COUNT, so MoneyHeadline left the Overview hero.
    assert "MoneyHeadline" not in overview
    assert '<span className="k">trades</span>' in overview
    # New hero-rail mark items.
    assert "open value" in overview
    assert "equity incl open" in overview

    # Journal net-P&L cell is the SAME MoneyHeadline (kpi variant) when the
    # All-range cash basis exists; the range-sliced paper branch stays.
    assert "const m = useMemo(() => money(k), [k]);" in live_page
    # Venue-aware (2026-07-21): the account cash tile shows only in the ALL-venue
    # view; a specific venue falls back to the scoped ledger-paper sum.
    assert "const showWalletNet = range === 'All' && m.hasCash && sel === 'all';" in live_page
    assert '<MoneyHeadline variant="kpi" m={m} />' in live_page
    # All-venue paper fallback still comes from the canonical server net; a
    # specific-venue view uses the scoped client sum.
    assert "const allTimePaper = sel !== 'all'" in live_page
    assert "(k.pnl_net_usd != null ? k.pnl_net_usd : summary.pnl)" in live_page
    assert "account_strategy_pnl_total" not in re.sub(r"//.*", "", live_page)
    assert "account_capital_comparable === true" not in live_page
    # Journal open-positions table gained a mark column.
    assert '<th className="num">mark</th>' in live_page
    assert "showMark" in live_page


def test_subminute_prices_match_trade_journal_exec_and_fill():
    """The Subminute resolution row must use the Journal price semantics."""
    payload = _run_client_js(
        """
        const live = {
          mode: 'LIVE', fill_price: 0.70, exec_price: 0.72,
          effective_fill_price: 0.75,
          live_filled_price: 0.75,
        };
        const dry = { mode: 'DRY', fill_price: 0.70, walked_vwap: 0.72 };
        const groups = cb.groupResolutions([{
          coin: 'BTC', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
          window_start_ts: 3000, offset_sec: 285, ask: 0.70,
          fill_price: 0.70, exec_price: 0.72, live_filled_price: 0.75,
          live_filled_size: 6.0, stake_usd: 5.0, filled_usd: 4.5,
          ghost: false, outcome: 'PENDING', time: '2026-07-11 00:45:30',
        }], []);
        const attempt = groups[0].attemptRows[0];
        console.log(JSON.stringify({
          liveExec: cb.execPrice(live), liveFill: cb.fillPrice(live),
          dryExec: cb.execPrice(dry), dryFill: cb.fillPrice(dry),
          rowExec: attempt.price, rowFill: attempt.fillPx,
        }));
        """
    )
    assert payload == {
        "liveExec": 0.72,
        "liveFill": 0.75,
        "dryExec": 0.72,
        "dryFill": 0.72,
        "rowExec": 0.72,
        "rowFill": 0.75,
    }


def test_account_capital_dashboard_helpers_execute_expected_mapping():
    """The ONE money bundle (window.CB.money) maps the account-capital payload
    fields across its provenance cases. (The accountEquity curve rescaler this
    test also covered was deleted 2026-07-22 — the Overview reads the
    unrescaled lifetime series.)"""
    payload = _run_client_js(
        """
        const k = {
          account_capital_comparable: true,
          account_initial_capital: 103.55,
          account_capital: 114.19,
          account_pnl_total: 10.64,
          realized_capital: 110.85,
          initial_bankroll: 100.0,
        };
        // The ONE money bundle — read by MoneyHeadline (hero/kpi/headline).
        // Three provenance cases:
        //   legacy = only account_capital/_pnl_total (no live fields);
        //   live   = per-tick balance_cache values PREFERRED over reconciliation;
        //   degraded = reconciliation_sample fallback flagged degraded (as-of).
        const mLegacy = cb.money(k);
        const mLive = cb.money({ ...k,
          account_capital_live: 118.42, account_pnl_total_live: 14.87,
          account_capital_live_source: 'balance_cache', pnl_net_usd: 12.5 });
        const mDegraded = cb.money({ ...k,
          account_capital_live: 137.34, account_pnl_total_live: 26.01,
          account_capital_live_source: 'reconciliation_sample',
          account_capital_live_degraded: true, pnl_net_usd: 27.99,
          account_capital_live_ts: '2026-07-11 06:47:38' });
        const mPaper = cb.money({ realized_capital: 93.0, pnl_net_usd: 4.2 });
        console.log(JSON.stringify({
          mLegacy: { cashLive: mLegacy.cashLive, cashNet: mLegacy.cashNet, basis: mLegacy.basis, hasCash: mLegacy.hasCash, degraded: mLegacy.degraded },
          mLive: { cashLive: mLive.cashLive, cashNet: mLive.cashNet, paper: mLive.paper, drift: Number(mLive.drift.toFixed(2)), degraded: mLive.degraded },
          mDegraded: { cashNet: mDegraded.cashNet, paper: mDegraded.paper, drift: Number(mDegraded.drift.toFixed(2)), degraded: mDegraded.degraded, asOfMs: mDegraded.asOfMs },
          mPaper: { cashLive: mPaper.cashLive, cashNet: mPaper.cashNet, paper: mPaper.paper, hasCash: mPaper.hasCash },
        }));
        """
    )

    # money(k): legacy payload (no live fields) reads account_capital/_pnl_total.
    assert payload["mLegacy"] == {
        "cashLive": 114.19, "cashNet": 10.64, "basis": 103.55,
        "hasCash": True, "degraded": False,
    }
    # Per-tick live values are PREFERRED over the reconciliation sample; a
    # balance_cache source is NOT degraded; drift = cashNet − paper.
    assert payload["mLive"]["cashLive"] == 118.42
    assert payload["mLive"]["cashNet"] == 14.87
    assert payload["mLive"]["paper"] == 12.5
    assert payload["mLive"]["drift"] == 2.37
    assert payload["mLive"]["degraded"] is False
    # A reconciliation_sample fallback is degraded (as-of surfaced); the cash net
    # (26.01) with paper (27.99) closes via the drift bridge (−1.98).
    assert payload["mDegraded"]["cashNet"] == 26.01
    assert payload["mDegraded"]["paper"] == 27.99
    assert payload["mDegraded"]["drift"] == -1.98
    assert payload["mDegraded"]["degraded"] is True
    assert payload["mDegraded"]["asOfMs"] is not None
    # No cash basis at all (DRY / fresh deploy) → cashNet null ⇒ use paper.
    assert payload["mPaper"]["cashLive"] is None
    assert payload["mPaper"]["cashNet"] is None
    assert payload["mPaper"]["paper"] == 4.2
    assert payload["mPaper"]["hasCash"] is False


def test_resolution_grouping_dedups_repeat_attempts():
    """window.CB.groupResolutions folds every attempt (per-second retries, T3
    top-ups) into ONE row per resolution (coin, interval, window_start, side) —
    a miss (no attempt filled) is counted ONCE, a fill stays its own row, and
    the band badge reflects the band that attempted. This is the client-side
    counterpart to the server reject_stats dedup and the direct fix for the
    double-counted XRP 15m DOWN example."""
    payload = _run_client_js(
        """
        // XRP 15m DOWN — TWO attempts rejected, no fill (the operator bug:
        // rendered twice, −$2.63 each). ETH 15m UP — one attempt filled, a
        // second rejected (fill wins). BTC 5m UP — a clean single fill.
        //
        // The second attempt on each shared resolution carries a DIFFERENT
        // sleeve label (other_15m) ON PURPOSE: resKey takes its interval from
        // resInterval -> sleeveInterval, so both labels reduce to '15m' and
        // fold. Label every attempt identically and a regression that keyed on
        // the RAW sleeve would collapse them too and pass silently.
        const rejects = [
          { coin: 'XRP', sleeve: 'subminute_15m', side: 'NO', window_start_ts: 1000,
             offset_sec: 880, limit_price: 0.90, requested_usd: 2.63,
             requote_drift: 0.01, missed_pnl: -2.63, resolved_outcome: 'UP',
             time: '2026-07-11 00:44:10' },
          { coin: 'XRP', sleeve: 'other_15m', side: 'NO', window_start_ts: 1000,
             offset_sec: 885, limit_price: 0.90, requested_usd: 2.63,
             requote_drift: 0.01, missed_pnl: -2.63, resolved_outcome: 'UP',
             time: '2026-07-11 00:44:20' },
          { coin: 'ETH', sleeve: 'other_15m', side: 'YES', window_start_ts: 3000,
             offset_sec: 875, limit_price: 0.88, requested_usd: 4.0,
             requote_drift: 0.0, missed_pnl: 1.70, resolved_outcome: 'UP',
             time: '2026-07-11 00:45:20' },
        ];
        const bets = [
          { coin: 'ETH', sleeve: 'subminute_15m', interval: '15m', side: 'YES',
             window_start_ts: 3000, offset_sec: 870, ask: 0.80, fill_price: 0.80,
             stake_usd: 4.0, filled_usd: 4.0, ghost: false, outcome: 'WIN',
             pnl_net: 1.0, time: '2026-07-11 00:45:00' },
          { coin: 'BTC', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
             window_start_ts: 2000, offset_sec: 280, ask: 0.70, fill_price: 0.70,
             stake_usd: 5.0, filled_usd: 5.0, ghost: false, outcome: 'WIN',
             pnl_net: 2.1, time: '2026-07-11 00:45:30' },
        ];
        const groups = cb.groupResolutions(bets, rejects);
        const by = {};
        groups.forEach((g) => { by[g.coin] = {
          attempts: g.attempts, nRejects: g.nRejects, band: g.band,
          status: g.status, pnl: g.pnl, missedPnl: g.missedPnl,
          filled: g.filled, target: g.target,
        }; });
        console.log(JSON.stringify({ nGroups: groups.length, by }));
        """
    )

    # Three resolutions → three rows: XRP (no-fill), ETH (fill), BTC (fill).
    # Pre-fix the XRP resolution rendered as TWO rows (one per attempt).
    assert payload["nGroups"] == 3
    xrp = payload["by"]["XRP"]
    assert xrp["attempts"] == 2          # both attempts' rejects fold in
    assert xrp["nRejects"] == 2
    assert xrp["band"] == "MID"
    assert xrp["status"] == "NO_FILL"
    assert xrp["missedPnl"] == -2.63      # counted ONCE, not −5.26
    assert xrp["pnl"] is None
    # ETH — one attempt filled, a second rejected: ONE fill row, no miss.
    eth = payload["by"]["ETH"]
    assert eth["status"] == "WIN"
    assert eth["band"] == "MID"
    assert eth["attempts"] == 2           # 1 fill + 1 folded reject
    assert eth["nRejects"] == 1
    assert eth["missedPnl"] is None       # a fill exists → no counterfactual
    assert eth["filled"] == 4.0
    assert eth["pnl"] == 1.0
    # BTC — a clean single fill, own row.
    btc = payload["by"]["BTC"]
    assert btc["status"] == "WIN"
    assert btc["band"] == "MID"
    assert btc["missedPnl"] is None
    assert btc["pnl"] == 2.1


def test_subminute_rows_group_fills_into_positions_via_the_journal_helper():
    """Operator 2026-08-05: the Subminute resolution table gets the Trade
    journal's "N in 1" fold.

    A base bet and the T3 top-ups that landed in the same market window are ONE
    CLOB balance that settles once, so they collapse into ONE row: Σ size,
    Σ P&L, Σ requested target, and the SIZE-WEIGHTED effective price — with the
    individual fills kept in the drill-down, labelled base / top-up N.

    The membership test is the journal's OWN `groupTrades` helper (shared, not
    forked), so the two tabs can never disagree about what one trade is; the
    sub-minute payload is only RENAMED into the ledger field shape it reads.

    What stays PER-ORDER: `attempts` and `nRejects` still count individual
    orders (a top-up really is its own order attempt), and the drill-down still
    lists every attempt, one row each."""
    payload = _run_client_js(
        """
        // BTC 5m YES — a $5 base fill plus two T3 top-ups on the SAME window,
        // and one reject in between. One position, four order attempts.
        const bets = [
          { coin: 'BTC', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
            window_start_ts: 2000, offset_sec: 275, is_topup: false,
            exec_price: 0.70, live_filled_price: 0.70, mode: 'LIVE',
            stake_usd: 5.0, filled_usd: 5.0, ghost: false,
            outcome: 'WIN', pnl_net: 2.0, time: '2026-07-11 00:34:35' },
          { coin: 'BTC', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
            window_start_ts: 2000, offset_sec: 282, is_topup: true,
            exec_price: 0.80, live_filled_price: 0.80, mode: 'LIVE',
            stake_usd: 3.0, filled_usd: 3.0, ghost: false,
            outcome: 'WIN', pnl_net: 0.6, time: '2026-07-11 00:34:42' },
          { coin: 'BTC', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
            window_start_ts: 2000, offset_sec: 288, is_topup: true,
            exec_price: 0.90, live_filled_price: 0.90, mode: 'LIVE',
            stake_usd: 2.0, filled_usd: 2.0, ghost: false,
            outcome: 'WIN', pnl_net: 0.2, time: '2026-07-11 00:34:48' },
          // ETH 15m — an UNRELATED window; must never merge with the above.
          { coin: 'ETH', sleeve: 'subminute_15m', interval: '15m', side: 'NO',
            window_start_ts: 9000, offset_sec: 870, is_topup: false,
            exec_price: 0.75, live_filled_price: 0.75, mode: 'LIVE',
            stake_usd: 4.0, filled_usd: 4.0, ghost: false,
            outcome: 'LOSS', pnl_net: -4.0, time: '2026-07-11 00:44:30' },
        ];
        const rejects = [
          { coin: 'BTC', sleeve: 'subminute_5m', side: 'YES', window_start_ts: 2000,
            offset_sec: 279, limit_price: 0.75, requested_usd: 3.0,
            missed_pnl: 0.9, resolved_outcome: 'UP', time: '2026-07-11 00:34:39' },
        ];
        const groups = cb.groupResolutions(bets, rejects);
        const by = {};
        groups.forEach((g) => { by[g.coin] = {
          nFills: g.nFills, attempts: g.attempts, nRejects: g.nRejects,
          target: g.target, filled: g.filled, fillRatio: g.fillRatio,
          pnl: Number(g.pnl.toFixed(4)), status: g.status,
          execPx: Number(g.execPx.toFixed(4)), fillPx: Number(g.fillPx.toFixed(4)),
          attemptKinds: g.attemptRows.map((a) => a.kind),
          fillLabels: g.attemptRows.map((a) => a.fillLabel),
        }; });
        // A partially filled position still reads PARTIAL against the Σ intent.
        const partial = cb.groupResolutions([
          Object.assign({}, bets[0], { filled_usd: 5.0 }),
          Object.assign({}, bets[1], { filled_usd: 0.4 }),
        ], [])[0];
        // LIVE payload shape (2026-08-09): stake_usd is the REALIZED value
        // (ledger.bet_size == filled), so the intent must come from
        // requested_usd (ledger.target_usd) — without it this row read as a
        // full-size fill.
        const partialLive = cb.groupResolutions([
          Object.assign({}, bets[0], {
            requested_usd: 6.31, stake_usd: 1.26, filled_usd: 1.26 }),
        ], [])[0];
        console.log(JSON.stringify({
          nGroups: groups.length, by,
          partial: { nFills: partial.nFills, target: partial.target,
                     filled: partial.filled, status: partial.status },
          partialLive: { target: partialLive.target,
                         filled: partialLive.filled,
                         status: partialLive.status,
                         requested: partialLive.attemptRows[0].requested },
        }));
        """
    )

    # Two resolutions → two rows. Pre-fix the BTC window rendered THREE rows
    # (one per fill), each showing only its own slice of the position.
    assert payload["nGroups"] == 2

    btc = payload["by"]["BTC"]
    assert btc["nFills"] == 3                 # drives the "3 in 1" badge
    assert btc["target"] == 10.0              # Σ requested: 5 + 3 + 2
    assert btc["filled"] == 10.0              # Σ realized
    assert btc["fillRatio"] == 1.0
    assert btc["pnl"] == 2.8                  # Σ: 2.0 + 0.6 + 0.2
    assert btc["status"] == "WIN"
    # size-weighted, NOT the base bet's price: (5·.70 + 3·.80 + 2·.90)/10
    assert btc["execPx"] == 0.77
    assert btc["fillPx"] == 0.77
    # ATTEMPTS stay per-ORDER: 3 fills + 1 reject, each its own drill-down row.
    assert btc["attempts"] == 4
    assert btc["nRejects"] == 1
    assert btc["attemptKinds"] == ["fill", "reject", "fill", "fill"]  # by offset
    assert btc["fillLabels"] == ["base", None, "top-up 1", "top-up 2"]

    # A different window never merges.
    eth = payload["by"]["ETH"]
    assert eth["nFills"] == 1
    assert eth["pnl"] == -4.0
    assert eth["status"] == "LOSS"

    # PARTIAL is judged against the GROUP's Σ intent, not the base bet's.
    assert payload["partial"] == {
        "nFills": 2, "target": 8.0, "filled": 5.4, "status": "PARTIAL",
    }

    # LIVE shape: intent rides in requested_usd; stake_usd==filled_usd is the
    # realized value and must NOT mask the shortfall. The drill-down row's
    # `requested` column shows the intent too.
    assert payload["partialLive"] == {
        "target": 6.31, "filled": 1.26, "status": "PARTIAL", "requested": 6.31,
    }


def test_missed_trade_index_maps_chain_verdicts_onto_groups():
    """window.CB.missedTradeIndex / missedTradeForGroup join the server
    missed_trades list (one row per resolution, carrying the ON-CHAIN verdict)
    onto the client resolution groups: keyed on the DIRECTION axis (YES→UP)
    with the group's ms window converted back to seconds. An older payload (no
    missed_trades) yields an empty index and a null lookup — the page's
    guard-render path.

    The per-attempt CHAIN verdict rides through groupResolutions into the
    drill-down rows; the refuted probe-inferred `miss_category` does NOT — a
    stale payload still carrying it must leave no cause label behind."""
    payload = _run_client_js(
        """
        const rejects = [
          { coin: 'BTC', sleeve: 'subminute_5m', side: 'YES', window_start_ts: 1000,
             offset_sec: 280, limit_price: 0.70, requested_usd: 10.0,
             requote_drift: 0.04, missed_pnl: 4.13, resolved_outcome: 'UP',
             miss_category: 'maker_pulled', time: '2026-07-22 14:00:00' },
          { coin: 'BTC', sleeve: 'subminute_5m', side: 'YES', window_start_ts: 1000,
             offset_sec: 285, limit_price: 0.70, requested_usd: 4.0,
             requote_drift: 0.0, missed_pnl: 1.65, resolved_outcome: 'UP',
             chain_verdict: 'FASTER_TAKER', chain_beat_dt_s: -1.0,
             chain_beat_price: 0.70, chain_beat_size: 21.0,
             chain_rival_addr: '0xrival', time: '2026-07-22 14:00:05' },
        ];
        const missedTrades = [
          { coin: 'BTC', interval: '5m', window_start_ts: 1000, side: 'YES',
            attempts: 2, miss_kind: 'book_miss', chain_verdict: 'FASTER_TAKER',
            chain_winnable: true,
            limit_price: 0.70, requested_usd: 10.0, resolved_outcome: 'UP',
            missed_pnl: 4.13, time: '2026-07-22 14:00:05' },
        ];
        const groups = cb.groupResolutions([], rejects);
        const idx = cb.missedTradeIndex(missedTrades);
        const hit = cb.missedTradeForGroup(idx, groups[0]);
        const emptyIdx = cb.missedTradeIndex(undefined);
        console.log(JSON.stringify({
          nGroups: groups.length,
          attempts: groups[0].attempts,
          status: groups[0].status,
          attemptCategories: groups[0].attemptRows.map((a) => a.category || null),
          attemptVerdicts: groups[0].attemptRows.map((a) => a.chainVerdict),
          attemptRival: groups[0].attemptRows[1].chainRival,
          hitVerdict: hit && hit.chain_verdict,
          hitWinnable: hit && hit.chain_winnable,
          hitKind: hit && hit.miss_kind,
          hitPnl: hit && hit.missed_pnl,
          emptySize: emptyIdx.size,
          emptyLookup: cb.missedTradeForGroup(emptyIdx, groups[0]),
        }));
        """
    )

    # Two attempts fold into ONE group; the server row joins by resolution.
    assert payload["nGroups"] == 1
    assert payload["attempts"] == 2
    assert payload["status"] == "NO_FILL"
    # The refuted probe label is not carried onto attempt rows any more, even
    # when a stale payload still ships it.
    assert payload["attemptCategories"] == [None, None]
    assert payload["attemptVerdicts"] == [None, "FASTER_TAKER"]
    assert payload["attemptRival"] == "0xrival"
    # The resolution row carries the chain verdict, its winnable flag and the
    # server's at-our-limit counterfactual ($10 @0.70 — the full-size attempt).
    assert payload["hitVerdict"] == "FASTER_TAKER"
    assert payload["hitWinnable"] is True
    assert payload["hitKind"] == "book_miss"
    assert payload["hitPnl"] == 4.13
    # Legacy payload → empty index, null lookup (guard-render, no crash).
    assert payload["emptySize"] == 0
    assert payload["emptyLookup"] is None


def test_resolution_grouping_surfaces_stop_row():
    """window.CB.groupResolutions renders a HISTORICAL stop-closed bet
    (exit_kind='stop_loss') as ONE fill row carrying the realized loss, the exit
    price, and the held-to-resolution counterfactual. The standalone STOP badge
    was retired with the engine's stop-loss (2026-08-05): these rows now render
    as the LOSS they are, matching the journal. A legacy voided CANCELLED bet
    (exit_kind null) is unchanged — NOT treated as a stop, and NOT relabelled a
    loss."""
    payload = _run_client_js(
        """
        const bets = [
          // Stop-closed YES fill: sold @0.50 → realized −1.00; the market later
          // went UP so the held side WOULD have won → if held +1.50.
          { coin: 'BTC', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
             window_start_ts: 1000, offset_sec: 285, ask: 0.70, fill_price: 0.70,
             stake_usd: 3.5, filled_usd: 3.5, ghost: false, outcome: 'CANCELLED',
             pnl_net: -1.0, exit_kind: 'stop_loss', exit_price: 0.50,
             stop_if_held_pnl: 1.5, time: '2026-07-11 00:45:00' },
          // Legacy voided CANCELLED (no exit_kind): must NOT be a stop row.
          { coin: 'ETH', sleeve: 'subminute_5m', interval: '5m', side: 'YES',
             window_start_ts: 2000, offset_sec: 285, ask: 0.70, fill_price: 0.70,
             stake_usd: 3.5, filled_usd: 3.5, ghost: false, outcome: 'CANCELLED',
             pnl_net: 0, time: '2026-07-11 00:46:00' },
        ];
        const groups = cb.groupResolutions(bets, []);
        const by = {};
        groups.forEach((g) => { by[g.coin] = {
          isStop: g.isStop, status: g.status, pnl: g.pnl,
          exitPrice: g.exitPrice, ifHeldPnl: g.ifHeldPnl,
          attemptStatus: g.attemptRows[0].outcome,
        }; });
        console.log(JSON.stringify({ nGroups: groups.length, by }));
        """
    )

    assert payload["nGroups"] == 2
    btc = payload["by"]["BTC"]
    assert btc["isStop"] is True
    assert btc["status"] == "LOSS"           # realized loss, no STOP badge
    assert btc["pnl"] == -1.0                # realized loss counts on the row
    assert btc["exitPrice"] == 0.50          # exit price surfaced
    assert btc["ifHeldPnl"] == 1.5           # held-to-resolution counterfactual
    assert btc["attemptStatus"] == "LOSS"    # drill-down attempt row too
    # Legacy voided CANCELLED — unchanged, not a stop.
    eth = payload["by"]["ETH"]
    assert eth["isStop"] is False
    assert eth["status"] == "CANCELLED"


def test_overview_pnl_breakdown_card_and_money_headline_both_gone():
    """The standalone 'P&L breakdown' card is GONE, and so is the MoneyHeadline
    that briefly replaced it: the 2026-08-05 declutter turned that Overview hero
    cell into the trade count. MoneyHeadline itself survives only for the pages
    that still headline money (Journal, Calendar), so exactly ONE number
    dashboard-wide is called 'net P&L'. The PnlBreakdown component + its render
    + all its orphaned pd-*/pnl-decomp CSS are deleted with it."""
    overview = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-overview.jsx").read_text(
        encoding="utf-8"
    )
    styles = (_REPO_ROOT / "polymarket" / "dashboard" / "styles.css").read_text(encoding="utf-8")
    shell = (_REPO_ROOT / "polymarket" / "dashboard" / "shell.jsx").read_text(encoding="utf-8")

    # PnlBreakdown is fully removed (def + render).
    assert "PnlBreakdown" not in overview
    assert 'className="card pnl-decomp"' not in overview
    # Its orphaned CSS is gone (literal grep, incl. string-concat class names).
    for cls in ("pnl-decomp", "pd-box", "pd-net", ".pd-k", ".pd-v"):
        assert cls not in styles, f"orphaned {cls} CSS survived"
    # The Overview hero dropped MoneyHeadline in the 2026-08-05 declutter (that
    # cell is the trade count now); the component itself survives for the pages
    # that still headline money.
    assert "MoneyHeadline" not in overview
    live_page = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-live.jsx").read_text(encoding="utf-8")
    analytics = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-analytics.jsx").read_text(encoding="utf-8")
    assert '<MoneyHeadline variant="kpi" m={m} />' in live_page
    assert "MoneyHeadline" in analytics
    # MoneyHeadline is defined + exported in shell.jsx, reads the money bundle,
    # and renders JUST the net figure — no paper/"unsettled" sub-line (operator
    # 2026-07-24: nothing is unsettled; the cash-vs-paper gap is a measurement
    # artifact, not pending money — headline = "net P&L · cash · since reset").
    assert "function MoneyHeadline({ variant, m })" in shell
    assert "window.MoneyHeadline = MoneyHeadline;" in shell
    assert "' · unsettled '" not in shell           # the paper/unsettled sub is REMOVED
    assert "gas·rounding·unsettled" not in shell    # verbose wording retired
    assert "const net = m.net;" in shell            # net = cash-first, lives in money()
    # The paper-reconciliation bridge span is gone with the sub.
    assert "cal-headline-bridge" not in shell


def test_overview_pnl_strip_lives_in_the_hero_card_and_nets_rebates_in():
    """The hero strip reads gross → fees → rebates → NET, left to right, and the
    NET cell is the CLOB-reconciling figure: gross − fees + rebates.

    Ordering + semantics are the operator's 2026-08-05 call. The rebate (Σ
    capital_flows kind='rebate' — taker-fee rebates + venue rewards) is account
    cash that no ledger row carries, so it is added ON THE CLIENT for display:
    the payload's `pnl_net_usd` stays the PAPER net (gross − fees) because a
    pinned producer invariant asserts Σ(ledger_daily.pnl) == pnl_net_usd. The
    producer keeps the trio's null convention: a legacy-schema read yields None
    for all four.

    The strip renders INSIDE the equity card, under the drawdown chart, in the
    dead space the chart captions used to occupy."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    builder = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "observability"
        / "snapshot_builder.py"
    ).read_text(encoding="utf-8")

    # All four cells, in the operator's order, formatted as the strip.
    order = ["gross P&L", "fees paid", "rebates", "net"]
    idxs = [overview.index(f'<KpiCell label="{cell}"') for cell in order]
    assert idxs == sorted(idxs), f"strip cells out of order: {order}"
    assert "fmt.moneyDelta(k.pnl_rebate_usd)" in overview
    assert "valueClass={tone(k.pnl_rebate_usd)} valueSm" in overview

    # NET = paper net + rebates, computed CLIENT-side and null-safe: a missing
    # paper net shows '—'; a missing rebate is treated as 0 like the trio's
    # other absent halves.
    assert (
        "const netInclRebates = k.pnl_net_usd == null ? null\n"
        "    : k.pnl_net_usd + (k.pnl_rebate_usd || 0);"
    ) in overview
    assert "netInclRebates != null ? fmt.moneyDelta(netInclRebates) : '—'" in overview
    # 2026-08-05 declutter: the four strip SUB-LABELS are deleted; each cell is
    # label + number, and the accounting facts moved into title= tooltips.
    for gone in (
        'sub="pre-fee"',
        'sub="venue skim"',
        'sub="paid back by venue"',
        'sub="gross − fees + rebates · = CLOB"',
    ):
        assert gone not in overview, f"strip sub-label {gone!r} survived the declutter"
    assert 'title="pre-fee: ' in overview
    assert 'title="venue skim: ' in overview
    assert 'title="paid back by the venue' in overview
    assert 'title="gross − fees + rebates: ' in overview
    assert "the figure the venue CLOB balance reconciles to" in overview
    assert "NOT in the payload's paper net" in overview
    # The old "rebates sit outside net" framing is retired with the reorder.
    for stale in ("after fees · paper", "not in net · in equity", 'sub="gross − fees"'):
        assert stale not in overview, f"stale strip copy {stale!r} survived"

    # The strip is nested in the hero card (between the chart and the rail),
    # not a standalone band after </section>.
    assert '<div className="kpi-grid c4 hero-strip">' in overview
    chart = overview.index('<div className="hero-chart">')
    strip = overview.index('className="kpi-grid c4 hero-strip"')
    rail = overview.index('<aside className="hero-rail">')
    assert chart < strip < rail, "the P&L strip left the hero card"
    styles = (dash / "styles.css").read_text(encoding="utf-8")
    assert styles.count(".kpi-grid.hero-strip") >= 1
    # The strip spans the chart's own width inside .hero-main — both are plain
    # block children of the same padded box, and the strip is pinned to 100%.
    assert ".kpi-grid.hero-strip { margin: 12px 0 0; box-shadow: none; width: 100%; }" in styles

    # Producer: pnl_net_usd stays the PAPER net — the rebate is never folded in
    # server-side (the Σ(day.pnl) == pnl_net_usd invariant depends on it).
    assert '"pnl_net_usd": _round_money(net),' in builder
    assert 'if kind == "rebate"' in builder
    assert "load_capital_flows(\n            conn, _wallet_start_sql()\n        )" in builder
    assert '"pnl_rebate_usd": None,' in builder



def test_overview_declutter_drops_captions_tape_and_counts_trades():
    """Operator declutter 2026-08-05, Overview ONLY.

    Every explanatory caption sentence is deleted (not reworded): the equity/
    drawdown chart legend, the row-window stamps on the per-coin / histogram /
    slice / streak surfaces, and the wordy rail subs. NUMBERS stay — W·L
    counts, dd%, the halt threshold (a live control, not prose). The activity
    tape is gone from the page AND from shell.jsx (Overview was its only
    consumer), CSS included. The hero's third metric counts SETTLED TRADES in
    the active range window instead of restating a dollar figure."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    shell = (dash / "shell.jsx").read_text(encoding="utf-8")
    styles = (dash / "styles.css").read_text(encoding="utf-8")

    # ── captions deleted, verbatim ──
    for caption in (
        "top: cash equity since reset",
        "top: ledger-paper equity, unrescaled",
        "bottom: drawdown from peak",
        "hourly points",
        "chart-legend",
        "rolling 24h · by bet time",
        "· since reset",
        "marked · ",
        "at entry</span>",
        "+ open</span>",
        "strategy paper (peak − equity) / peak",
        "stop losses in gross loss",
        "daily · annualized",
        "full history · all bets",
        "· recent rows",
        "an.full ? 'full history' : 'row window'",
        "interval × entry-minute · realized",
    ):
        assert caption not in overview, f"caption {caption!r} survived the declutter"
    # rowWinLabel existed only to stamp those panels — the whole helper is gone.
    assert "rowWinLabel" not in overview
    # The ONE "· ledger paper" kept is the hero equity BASIS string — it says
    # which accounting the big number is, which is provenance, not caption.
    # 2026-08-05 (operator): it is now a `title=` TOOLTIP, not visible text.
    assert _code_only(overview).count("· ledger paper") == 1
    assert "'equity · ledger paper'" in overview
    assert '<div className="hero-label" title={' in overview

    # ── numbers kept ──
    assert "${life.wins}W · ${life.losses}L" in overview
    assert "halts at ${fmt.pctPoints(k.max_drawdown_halt_pct, 0)}" in overview
    assert "{fmt.pctPoints(dd)}" in overview
    assert "${summary.settled} settled" in overview     # histogram sub, number only

    # ── trades count: settled rows of the ACTIVE range window ──
    assert '<span className="k">trades</span>' in overview
    assert '<span className="v">{summary.settled}</span>' in overview
    assert "const summary = an.summary;" in overview     # an = selectAnalytics(scoped, range)
    assert "selectAnalytics(scoped, range)" in overview

    # ── activity tape deleted everywhere ──
    assert "BetTape" not in _code_only(overview)
    assert "BetTape" not in _code_only(shell)
    assert "window.BetTape" not in shell
    for cls in (".tape ", ".tape-item", "tape-in"):
        assert cls not in styles, f"orphaned {cls} CSS survived"
    assert ".chart-legend" not in styles


def test_subminute_declutter_drops_caption_subs_keeps_numbers():
    """Operator declutter 2026-08-05, THIRD wave: the Subminute console page
    and its Config spec page (both live in pages-subminute.jsx).

    Same rules as the Overview and journal/calendar/latency waves. Deleted: the
    prose `sub=` captions on the KPI cells and Panels, the MissedPnl chain
    captions, and the long Panel `note` paragraph on "missed trades" (which
    retired Panel's `note` prop outright — this page was its last consumer).
    KEPT: every number, count, unit and control value — accepted/rejected
    tallies, the '< 3 resolved' sample threshold, dd-from-peak, the brake floor,
    stake fractions, the fill-rate breaker threshold, and the whole Config spec
    contract table.

    Nothing HONEST was lost: each deleted caption's meaning moved into a
    `title=` / `titleHint=` hover tooltip on the same element."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    sub = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")

    # -- caption sentences deleted, verbatim --
    for caption in (
        "Σ filled / Σ requested · record window (v2 go-live / operator reset)\"",
        'sub="would-be, once per resolution"',
        "'resolutions that would have won'",
        "one missed trade per RESOLUTION — N FAK retries fold into 1",
        'sub="chain-proven live level · the only speed-winnable money"',
        'sub="the tape says our level never traded — no speed wins these"',
        "'chain: the level was live'",
        "'chain: unwinnable at any speed'",
        "newest-rows window (server bets/rejects caps) — lifetime totals live on Overview/Journal'",
        'sub="env-pinned knobs of THIS deploy',
        "one fill per resolution, first-fill-wins`} flush>",
        "'ledger paper (no CLOB balance yet)' : 'live wallet balance'}",
    ):
        assert caption not in sub, f"caption {caption!r} survived the declutter"
    # The page no longer passes Panel a `note` paragraph at all.
    assert "note={" not in sub

    # -- honesty relocated into tooltips, not lost --
    assert 'titleHint={"One missed trade per RESOLUTION' in sub
    assert "Every counterfactual below is priced AT OUR LIMIT." in sub
    assert 'title="HISTORICAL (full) fill rate: Σ filled / Σ requested over the record window' in sub
    assert "titleHint={(exactGrouping ? '' : 'Grouping is APPROXIMATE" in sub
    # NB: the "Env-pinned knobs of THIS deploy" titleHint belonged to
    # StrategyFlagsPanel, which the 2026-08-05 TRUTH PASS deleted outright (its
    # live rows folded into the one contract table) — see
    # test_strategy_tab_truth_pass_* below, which re-pins where each row went.
    assert "StrategyFlagsPanel" not in _code_only(sub)
    # The equity BASIS words moved out of the sub and into the cell tooltip —
    # the KPI label already says which basis it is, so the sub only carries
    # numbers now.
    assert "'ledger paper equity — no CLOB balance yet'" in sub
    assert "'live wallet balance from the CLOB'" in sub

    # -- numbers / counts / control values kept --
    # (the FOURTH wave, 2026-08-05 — test_subminute_fourth_wave_* below — moved
    # the '< 3 resolved' threshold, the sizing echo and the breaker window into
    # tooltips and made the DD-brake tile anomalous-only; the assertions here
    # pin what that wave deliberately left ON the page.)
    assert "${rs.n_accepted != null ? fmt.intc(rs.n_accepted) : 0} accepted attempts" in sub
    assert "dd from peak ${fmt.pct(sizing.dd_frac)}" in sub
    assert "floor ×${fmt.num(brakeFloor, 2)}" in sub
    assert "15m ${fmt.pct(sizing.base_frac_15m)}" in sub
    assert "peak ${fmt.money(sizing.peak_capital)}" in sub
    # "· net deposits $X (ledger)" was DELETED from this tab 2026-08-05
    # (operator): operator funding is not a sub-minute execution fact and it
    # read as part of the equity RESULT. The peak stays — it is the DD-brake /
    # hard-halt denominator, which IS an execution control.
    assert "net deposits" not in _code_only(sub)
    assert "sizing.net_deposits" not in _code_only(sub)
    # Config spec page keeps the full deployed contract table.
    assert '<Row k="orders" shared="FAK (fill-and-kill) taker"' in sub
    assert '<Row k="exits" shared="none — every filled position HOLDs to binary settlement"' in sub


def test_declutter_sweep_journal_calendar_latency_and_shell():
    """Operator declutter 2026-08-05, second wave: Trade journal, Calendar,
    Latency and the shared chrome.

    Same rules as the Overview wave. Deleted: the topbar page SUBTITLES (the
    NAV `sub:` field and its renderer, mechanism included — no page uses it),
    every qualifier fragment ('· since reset', '· ledger paper', 'Σ effective
    stake', 'settled only', 'newest N trades' window stamps), the over-
    explaining empty states, and the caption paragraphs under Latency's panels.
    KEPT: numbers, W·L tallies, pagination, filter pills, column headers, the
    'N in 1' trade-group badge.

    Nothing HONEST was lost — every deleted caption that carried an accounting
    basis or a window caveat moved into a `title=` hover tooltip: Panel gained a
    `titleHint` prop, TablePager renders its window `label` as a tooltip instead
    of visible text, and MoneyHeadline's basis tag became a tooltip."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    shell = (dash / "shell.jsx").read_text(encoding="utf-8")
    app = (dash / "app.jsx").read_text(encoding="utf-8")
    journal = (dash / "pages-live.jsx").read_text(encoding="utf-8")
    calendar = (dash / "pages-analytics.jsx").read_text(encoding="utf-8")
    latency = (dash / "pages-latency.jsx").read_text(encoding="utf-8")
    styles = (dash / "styles.css").read_text(encoding="utf-8")

    # ── topbar page subtitles: strings AND mechanism ──
    for sub in (
        "Live performance at a glance",
        "Processing state, readiness and the live funder wallet",
        "Last-30s per-second entries",
        "Open positions and the settled bet ledger",
        "Daily realized P&L heatmap",
        "Per-phase timing",
        "The deployed strategy contract",
    ):
        assert sub not in shell, f"page subtitle {sub!r} survived"
    assert "sub:" not in _code_only(shell)      # no NAV entry defines one any more
    assert "meta.sub" not in shell              # ...and the topbar renders none
    assert "topbar-sub" not in shell
    assert ".topbar-sub" not in styles    # orphaned CSS removed with it
    assert "sub: ''" not in app           # app.jsx's unknown-page fallback too

    # ── qualifier fragments gone from the visible chrome ──
    # (`_code_only` because the do-not-re-add comments quote them on purpose;
    #  surviving occurrences in executable code are all inside title= tooltips.)
    assert "· since reset'" not in _code_only(shell)
    assert "'ledger paper'" not in _code_only(shell)
    for gone in (
        "notional · Σ effective stake",
        "settled trades · since reset",
        "${life.lifetime ? ' · since reset' : ''}",
        "realized P&L · ledger paper",
        "settled only · ",
        "newest ${settled.length} trades",
        "table streams the newest rows only`,",   # the visible Panel sub form
        "every trade settled",
        "no settled trades in this window",
    ):
        assert gone not in journal, f"journal declutter target {gone!r} survived"
    assert "scope-note" not in _code_only(journal)
    assert ".scope-note" not in styles
    for gone in (
        "calendar shows realized days only",
        "the calendar fills as days resolve",
    ):
        assert gone not in calendar, f"calendar caption {gone!r} survived"

    # ── Latency: no caption paragraphs, no Panel `note` prose ──
    assert "note=" not in latency
    assert 'className="muted" style={{ fontSize: 12' not in latency
    assert "prefetch_age_instrumented" not in _code_only(latency)
    assert 'sub="a 10s-old sub-minute book is a different market"' not in latency
    assert 'sub="one order tick, p50' not in latency
    assert "· tick start → POST on the wire`" not in latency
    assert "ws-served slice · prefetch age unmeasured" not in latency
    assert "the scanner has not broadcast a live.latency block" not in latency
    # Panel `note` was retired outright in the THIRD wave (Subminute) — that
    # page was its last consumer, so the prop and its renderer are gone. The
    # .card-note CLASS survives: pages-config and the Subminute on-chain
    # forensics strip still style raw divs with it.
    assert "note," not in shell.split("function Panel({")[1].split(")")[0]
    assert '{note && <div className="card-note">{note}</div>}' not in shell
    assert ".card-note" in styles

    # ── honesty relocated, not lost ──
    assert "function Panel({ title, titleHint," in shell
    assert 'className="card-title" title={titleHint || undefined}' in shell
    assert 'className="pager-info" title={label || undefined}' in shell
    assert "showing {start}–{end} of {total}</span>" in shell   # no window note inline
    assert "const basis = netIsCash" in shell                   # MoneyHeadline basis tooltip
    assert "title={basis}" in shell
    assert "titleHint={life.lifetime && lifeSettled > settled.length" in journal
    assert 'title="ledger paper: Σ pnl_net over the settled rows in this window' in journal
    assert "pills filter the tables below" in journal           # now the kpi-grid tooltip
    assert 'title="the decision that opened this position' in journal
    assert "titleHint=" in latency
    # predict.fun's null taker delay is still branched INSIDE the moved copy —
    # the console must never render "nullms".
    assert "venueFloor.taker_delay_ms != null" in latency
    assert "vf.taker_delay_ms != null" in latency

    # ── numbers / controls kept ──
    assert 'q={`${settled.length}`}' in journal
    assert 'q={`${open.length}`}' in journal
    assert "${g.nFills} fills (base + ${g.nFills - 1} top-up" in journal   # trade badge
    assert "{g.nFills} in 1" in journal
    assert "<TablePager pager={pager} setPage={setPage}" in journal
    assert "const wlSub = `${life.wins}W · ${life.losses}L`;" in journal
    assert "{c.data.wins}W·{c.data.losses}L" in calendar
    assert "{wk.total.n} trades" in calendar
    assert "prev</button>" in shell and "next</button>" in shell


def test_overview_rail_drops_profit_factor_and_sharpe_tiles():
    """Operator 2026-08-05: the 'profit factor' (ROI + avg edge) and 'sharpe'
    (sortino · calmar · expectancy) rail tiles are GONE from Overview entirely.
    open bets / dd-from-peak / streak stay. With the sharpe tile went the page's
    only aggRatios call — the helper itself stays in data.jsx for the fallback
    analytics path, so this pins the CALL SITE, not the helper."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    code = _code_only(overview)

    for gone in (
        "profit factor",
        "fmt.ratio(summary.pf)",
        "avg edge {summary.avgEdge",
        "fmt.pct(summary.roi)",
        ">sharpe<",
        "ratios.sharpe",
        "ratios.sortino",
        "ratios.calmar",
        "ratios.expectancy",
        "aggRatios",
    ):
        assert gone not in code, f"deleted rail surface {gone!r} survived"

    # The kept rail tiles are still there.
    for kept in ("open bets", "dd from peak", "streak"):
        assert kept in overview, f"rail tile {kept!r} was removed by mistake"
    assert "streaks.longestW" in overview


def test_stop_loss_is_gone_from_the_dashboard_ui():
    """Operator 2026-08-05 — stop-loss is removed from the dashboard UI.

    * No 'SL' class in any count: Overview / Journal / Calendar render
      "NW · NL", never "NW · NL · NSL".
    * data.jsx keeps NO stop CLASSIFIER beyond `isBookedStop`, which exists
      only to classify a historical stop row (DB outcome 'CANCELLED') as a LOSS
      so it cannot silently leave the win-rate denominator. `groupResolutions`
      still READS `exit_kind` on historical rows (that is the read-path the
      sweep preserves), but no surface calls those rows 'SL'.
    * The Journal's STOP outcome pill / stop detail cells are gone.
    * The Subminute stop-loss EXITS panel is deleted.
    The engine sweep landed the same day (execution/stop_loss.py deleted, the
    per-second loop rehosted to execution/held_position_pass.py); this test
    covers only the dashboard half."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    data = (dash / "data.jsx").read_text(encoding="utf-8")
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    journal = (dash / "pages-live.jsx").read_text(encoding="utf-8")
    calendar = (dash / "pages-analytics.jsx").read_text(encoding="utf-8")
    subminute = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")

    # No "SL" token anywhere the operator reads counts, and no `.stops` read.
    for name, page in (("overview", overview), ("journal", journal),
                       ("calendar", calendar)):
        code = _code_only(page)
        assert not re.search(r"\bSL\b", code), f"an SL count survived on {name}"
        assert "stop" not in code.lower(), f"stop-loss wording survived on {name}"
        assert ".stops" not in code

    # Client analytics: the win rate is wins/(wins+losses) and the PF gross loss
    # no longer has a separate stop term.
    assert "function wr(wins, losses) {" in data
    assert "const decided = wins + losses;" in data
    assert "isStopRow" not in data
    # The one surviving stop reference, and what it is for.
    assert "function isBookedStop(r)" in data
    assert "function isLossRow(r) { return !!r && (r.outcome === 'LOSS' || isBookedStop(r)); }" in data

    # Journal: no STOP scope pill, no STOP display outcome, no stop detail cells.
    assert "const OUTCOME_SCOPES = ['WIN', 'LOSS', 'PENDING'];" in journal
    for gone in ("'STOP'", "stop exit px", "stop_if_held_pnl", "isStopRow"):
        assert gone not in journal, f"journal stop surface {gone!r} survived"

    # Subminute: the exits panel and its pager constant are deleted.
    code = _code_only(subminute)
    assert "function StopLossPanel" not in code
    assert "<StopLossPanel" not in code
    assert "STOP_PAGE_SIZE" not in code

    # Producer: the SL count fields that fed those panels are gone from kpis and
    # from every ledger rollup.
    builder = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "observability"
        / "snapshot_builder.py"
    ).read_text(encoding="utf-8")
    assert '"stops_24h": total_stops_24h,' not in builder
    assert '"stops": stops,' not in builder
    assert 'def _win_rate(wins: int, losses: int)' in builder


def test_calendar_drops_reconciliation_and_headlines_ledger_paper_net():
    """Calendar: the word 'reconciliation' is gone entirely, the Σ pnl_net
    (ledger paper) over the RENDERED cells is the marquee headline, and the
    cross-basis note line is removed entirely (operator 2026-07-12).

    2026-07-12 alignment: `daily` is now the SERVER all-time rollup
    (live.ledger_daily via selectDaily), whose population is identical to
    kpis.pnl_net_usd, so the headline (summed from that same array) equals the
    Overview P&L breakdown net by construction. The headline still sums the
    array and never reads the kpis.pnl_net_usd scalar, so cells and headline stay
    coupled to one source regardless of where `daily` came from."""
    analytics = (_REPO_ROOT / "polymarket" / "dashboard" / "pages-analytics.jsx").read_text(
        encoding="utf-8"
    )
    assert "reconciliation" not in analytics.lower()
    assert "cal-recon" not in analytics
    # Headline sums the SAME `daily` aggregate that fills the heatmap cells + the
    # win/loss-day KPIs, so the marquee can never contradict the visible cells.
    assert "const calTotalNet = daily.reduce((a, b) => a + b.pnl, 0);" in analytics
    # `daily` is the server-preferred all-time rollup (aligns headline to the
    # Overview net), and the headline never reads the kpis.pnl_net_usd scalar.
    # Venue-scoped (2026-07-21): selectDaily reads the scoped view (server daily
    # nulled for a specific venue so it recomputes over the scoped ledger).
    assert "selectDaily(scoped)" in analytics
    assert "live.ledger_daily" in analytics
    assert "k.pnl_net_usd" not in analytics
    # The headline is now the shared MoneyHeadline (headline variant); the
    # bridge's `paper` is overridden to Σ(rendered cells) so the marquee equals
    # its own heatmap. The .cal-headline* CSS is reused by MoneyHeadline.
    assert 'variant="headline"' in analytics
    # The paper override + drift bridge are delegated to the shared withPaperBasis
    # helper (data.jsx) — the calendar no longer hand-rolls a local calDrift.
    assert "withPaperBasis(mCal, calTotalNet)" in analytics
    assert "calDrift" not in analytics
    # The month panel's visible "ledger paper" TAG is gone (2026-08-05 declutter)
    # — the basis rides the month total's hover tooltip instead, so the honesty
    # survives without the chrome. The class itself is retired dashboard-wide.
    assert "cal-basis-tag" not in analytics
    assert "cal-basis-tag" not in (
        _REPO_ROOT / "polymarket" / "dashboard" / "shell.jsx"
    ).read_text(encoding="utf-8")
    assert "cal-basis-tag" not in (
        _REPO_ROOT / "polymarket" / "dashboard" / "styles.css"
    ).read_text(encoding="utf-8")
    assert 'title="ledger paper: Σ pnl_net' in analytics
    # The cross-basis note line was deleted per operator order — no basis strip.
    assert "cal-basis-note" not in analytics
    assert "calendar cells are ledger paper" not in analytics
    assert "basis not strictly comparable" not in analytics
    # Its CSS + the earlier reconciliation CSS are gone from the theme.
    styles = (_REPO_ROOT / "polymarket" / "dashboard" / "styles.css").read_text(encoding="utf-8")
    assert ".cal-recon" not in styles
    assert ".cal-recon-sep" not in styles
    assert ".cal-basis-note" not in styles


def test_calendar_prefers_server_daily_rollup():
    """window.CB.selectDaily returns the SERVER all-time rollup (live.ledger_daily)
    verbatim when present — so the calendar/journal cover the whole deployed
    history and the headline aligns to kpis.pnl_net_usd — and falls back to the
    client-derived aggDaily over the bounded streamed ledger only for older
    payloads / a static snapshot that predates the server block. An empty server
    array (a genuinely no-settled-day rollup) still wins over stale client rows."""
    payload = _run_client_js(
        """
        // A client ledger that aggDaily would bucket on a DIFFERENT day than the
        // server array — so we can tell which source selectDaily used.
        const clientLedger = [
          { outcome: 'WIN', pnl_net: 5.0, resolution_ts: '2026-07-01 03:00:00' },
        ];
        const serverDaily = [
          { date: '2026-06-02', pnl: 1.25, n: 2, wins: 1, losses: 1 },
        ];

        // 1) server present → returned verbatim (server population, not the ledger).
        const preferred = cb.selectDaily({ ledger_daily: serverDaily, ledger: clientLedger });
        // 2) empty server rollup still wins over stale client rows.
        const emptyServer = cb.selectDaily({ ledger_daily: [], ledger: clientLedger });
        // 3) no server block (older payload) → falls back to aggDaily(ledger).
        const fallback = cb.selectDaily({ ledger: clientLedger });
        // 4) null server block → same fallback.
        const nullServer = cb.selectDaily({ ledger_daily: null, ledger: clientLedger });
        // 5) nothing at all → empty series, no crash.
        const empty = cb.selectDaily({});

        console.log(JSON.stringify({
          preferredDates: preferred.map((d) => d.date),
          preferredPnl: preferred.reduce((a, b) => a + b.pnl, 0),
          emptyServerLen: emptyServer.length,
          fallbackDates: fallback.map((d) => d.date),
          fallbackPnl: fallback.reduce((a, b) => a + b.pnl, 0),
          nullServerDates: nullServer.map((d) => d.date),
          emptyLen: empty.length,
        }));
        """
    )

    # Server rollup preferred verbatim — its dates/pnl, NOT the client ledger's.
    assert payload["preferredDates"] == ["2026-06-02"]
    assert payload["preferredPnl"] == pytest.approx(1.25)
    # Empty server array wins over stale client rows (no masking).
    assert payload["emptyServerLen"] == 0
    # Fallback path aggregates the bounded client ledger (MYT day of the WIN).
    assert payload["fallbackDates"] == ["2026-07-01"]
    assert payload["fallbackPnl"] == pytest.approx(5.0)
    assert payload["nullServerDates"] == ["2026-07-01"]
    # Nothing present → empty, no throw.
    assert payload["emptyLen"] == 0


def test_dashboard_runtime_assets_match_live_coin_universe():
    """Coin glyphs cover BOTH venue universes: Polymarket (BTC/ETH/SOL/XRP) and
    the predict.fun port (BTC/ETH/BNB) — bnb.svg was added for the venue port."""
    coins_dir = _REPO_ROOT / "polymarket" / "dashboard" / "assets" / "coins"
    assert sorted(p.name for p in coins_dir.glob("*.svg")) == [
        "bnb.svg",
        "btc.svg",
        "eth.svg",
        "sol.svg",
        "xrp.svg",
    ]
    # bnb.svg matches the established coin-glyph shape (32-grid brand circle).
    bnb = (coins_dir / "bnb.svg").read_text(encoding="utf-8")
    assert 'viewBox="0 0 32 32"' in bnb
    assert "<circle" in bnb


def test_calibration_page_removed_and_venue_badge_wired():
    """The Calibration tab (reintroduced 2026-07-21 for the predict.fun port)
    was REMOVED the same day for the direct-DRY plan — this is the inverse
    guard: no page file, no index.html script tag, no app.jsx route, no
    shell.jsx NAV entry/icon. (Its live book-recorder KPIs were folded into the
    Operator page's predict.fun panel and then retired outright with the book
    recorders, 2026-08-04.) The venue switcher + the data.jsx venue registry
    survive unchanged, as does the ?v= cache-buster lockstep."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    index = (dash / "index.html").read_text(encoding="utf-8")
    app = (dash / "app.jsx").read_text(encoding="utf-8")
    shell = (dash / "shell.jsx").read_text(encoding="utf-8")
    data = (dash / "data.jsx").read_text(encoding="utf-8")

    # The page file is gone and nothing references it. (The sse_server static
    # allowlist never served pages-calibration.jsx, so on a real deploy the tab
    # 404'd — removal also closes that latent bug.)
    assert not (dash / "pages-calibration.jsx").exists()
    assert "pages-calibration" not in index
    assert "calibration: window.CalibrationPage" not in app
    assert "CalibrationPage" not in app
    assert "key: 'calibration'" not in shell
    assert "calibration:" not in shell  # NAV entry AND the I.calibration icon

    # Every ?v= cache-buster is still in lockstep (browser .jsx cache gotcha).
    tags = re.findall(r"\?v=([A-Za-z0-9._-]+)", index)
    assert tags, "no ?v= cache-buster tags found in index.html"
    assert len(set(tags)) == 1, f"cache-buster tags not in lockstep: {set(tags)}"
    assert "styles.css?v=" in index

    # Venue identity in the topbar is a PASSIVE BADGE (2026-08-01). The
    # segmented switcher and its VENUE_SELECTORS list were removed with the
    # cross-venue view: each console is single-venue by deployment, so a
    # switcher could only pin it to a venue it cannot render.
    assert "badge-venue" in shell
    assert "venue-seg" not in _code_only(shell)
    assert "VENUE_SELECTORS" not in _code_only(shell)
    assert "VENUE_SELECTORS" not in _code_only(data)
    styles = (dash / "styles.css").read_text(encoding="utf-8")
    assert ".badge-venue" in styles
    assert ".venue-seg" not in _code_only(styles)

    # data.jsx owns the venue registry + helpers (kept OUT of app.jsx so the
    # existing "no BNB in app" guard stays green).
    assert "BNB" not in app
    assert "const VENUE_META" in data
    assert "'PREDICT.FUN'" in data
    for fn in ("function venueOf", "function venueUniverse"):
        assert fn in data
    assert "venueOf, venueUniverse," in data


def test_venue_helpers_execute():
    """window.CB venue helpers behave for BOTH a payload lacking the venue field
    (older snapshot ⇒ polymarket default) and a synthetic predict.fun payload
    carrying it. This is the app-boots-with-both-snapshots smoke check at the
    data layer (the pages read exactly these helpers)."""
    p = _run_client_js(
        """
        // (a) legacy payload: no venue, no markets.
        const legacy = { tick_ts: null, markets: [] };
        // (b) synthetic predict.fun payload carrying the venue field.
        const pf = {
          venue: 'predictfun', tick_ts: '2026-07-21T08:00:00Z', markets: [],
        };
        console.log(JSON.stringify({
          legacyVenue: cb.venueOf(legacy),
          legacyUniverse: cb.venueUniverse(legacy),
          legacyCoins: cb.liveCoins(legacy),
          pfVenue: cb.venueOf(pf),
          pfUniverse: cb.venueUniverse(pf),
          pfCoinsFromFallback: cb.liveCoins(pf),
          marketsWin: cb.liveCoins({ venue: 'predictfun', markets: [{ coin: 'BTC' }] }),
          unknownVenue: cb.venueOf({ venue: 'kalshi' }),
        }));
        """
    )

    # (a) absent fields degrade to Polymarket — legacy behavior.
    assert p["legacyVenue"] == "polymarket"
    assert p["legacyUniverse"] == ["BTC", "ETH", "SOL", "XRP"]
    assert p["legacyCoins"] == ["BTC", "ETH", "SOL", "XRP"]
    # (b) predict.fun payload resolves the venue + its BTC/ETH/BNB universe.
    assert p["pfVenue"] == "predictfun"
    assert p["pfUniverse"] == ["BTC", "ETH", "BNB"]
    assert p["pfCoinsFromFallback"] == ["BTC", "ETH", "BNB"]
    # A live markets array always wins over the venue fallback.
    assert p["marketsWin"] == ["BTC"]
    # Unrecognized venue never blanks the UI — it degrades to polymarket.
    assert p["unknownVenue"] == "polymarket"


def test_venue_filter_scope_helpers_execute():
    """The shared venue scope helpers (CB.venueFilter/scopeLive/scopedCoins/
    predictfunWallet/stripCash) behave for BOTH untagged rows (absent venue ⇒
    polymarket) and per-row-tagged rows. This is the app-boots smoke check for
    the venue dimension.

    2026-08-01: loadVenueSel/saveVenueSel are GONE with the venue switcher.
    They persisted the operator's choice in localStorage, and on a
    single-venue console a stale pin (e.g. 'polymarket' carried onto the
    predict.fun box) would filter every row away and render an empty console
    on a live-money desk. The venue is now derived from the payload only."""
    p = _run_client_js(
        """
        const rows = [
          { id: 1, coin: 'BTC', venue: 'polymarket' },
          { id: 2, coin: 'ETH' },                 // ABSENT venue ⇒ polymarket
          { id: 3, coin: 'BNB', venue: 'predictfun' },
          { id: 4, coin: 'SOL', venue: 'kalshi' },// unknown ⇒ polymarket
        ];
        const live = { kpis: { a: 1 }, ledger: rows,
          ledger_rollup: { total: 9 }, ledger_equity: { x: 1 },
          ledger_daily: [{ d: 1 }], markets: [{ coin: 'BTC' }] };

        const scopeAll = cb.scopeLive(live, 'all');
        const scopePf = cb.scopeLive(live, 'predictfun');
        const m = { cashLive: 120, cashNet: 20, paper: 18, drift: 2, hasCash: true, degraded: true, basis: 100 };

        console.log(JSON.stringify({
          selectorHelpersGone: [cb.loadVenueSel, cb.saveVenueSel, cb.VENUE_SELECTORS,
            cb.pfPaperRows, cb.mergedLedger].every((x) => x === undefined),
          rowVenueAbsent: cb.rowVenue(rows[1]),
          rowVenueUnknown: cb.rowVenue(rows[3]),
          rowVenuePf: cb.rowVenue(rows[2]),
          filterAll: cb.venueFilter(rows, 'all').map((r) => r.id),
          filterPoly: cb.venueFilter(rows, 'polymarket').map((r) => r.id),
          filterPf: cb.venueFilter(rows, 'predictfun').map((r) => r.id),
          scopeAllSameRef: scopeAll === live,
          scopePfLedger: scopePf.ledger.map((r) => r.id),
          scopePfRollupNull: scopePf.ledger_rollup === null,
          scopePfEquityNull: scopePf.ledger_equity === null,
          scopePfDailyNull: scopePf.ledger_daily === null,
          scopePfKpisKept: scopePf.kpis === live.kpis,
          coinsPf: cb.scopedCoins(live, 'predictfun'),
          coinsPoly: cb.scopedCoins(live, 'polymarket'),
          coinsPolyUnion: cb.scopedCoins(live, 'polymarket', [{ coin: 'XRP' }, { coin: 'BTC' }]),
          coinsAll: cb.scopedCoins(live, 'all'),
          strip: cb.stripCash(m),
          walletAbsent: cb.predictfunWallet({}),
          walletPresent: cb.predictfunWallet({ predictfun_wallet: {
            address: '0x1234567890abcdef1234', chain: 'bnb', api_key_present: true,
            collateral: { symbol: 'USDT', balance: 250.5 } } }),
        }));
        """,
        window_extra=_LOCALSTORAGE_SHIM,
    )

    # The selector/merge seam is REMOVED from window.CB entirely — not just
    # unused. A dangling export is how a "removed" feature comes back.
    assert p["selectorHelpersGone"] is True
    # rowVenue: absent + unknown degrade to polymarket; a real tag is honored.
    assert p["rowVenueAbsent"] == "polymarket"
    assert p["rowVenueUnknown"] == "polymarket"
    assert p["rowVenuePf"] == "predictfun"
    # venueFilter: ALL removed 2026-07-23 — a legacy 'all'/unknown selector now
    # resolves to the DEPLOYMENT venue (polymarket by default in this harness,
    # window.__CB_DEPLOY_VENUE unset), so it behaves like the polymarket filter;
    # predictfun = only the tagged predictfun row.
    assert p["filterAll"] == [1, 2, 4]
    assert p["filterPoly"] == [1, 2, 4]
    assert p["filterPf"] == [3]
    # scopeLive: every selection now scopes to a venue (no passthrough) — a
    # legacy 'all' resolves to the deploy venue and returns a NEW filtered object.
    assert p["scopeAllSameRef"] is False
    assert p["scopePfLedger"] == [3]
    assert p["scopePfRollupNull"] is True
    assert p["scopePfEquityNull"] is True
    assert p["scopePfDailyNull"] is True
    assert p["scopePfKpisKept"] is True
    # scopedCoins: the DEPLOYED venue (this payload has no `venue` ⇒ polymarket)
    # reads the LIVE scan universe (markets array = config.COINS — so a retired
    # coin like XRP never stays pinned); the non-deployed venue falls back to
    # its registry universe (BNB only under predictfun); 'all' reads the live
    # scan universe; passing rows unions in historical coins still in view.
    assert p["coinsPf"] == ["BTC", "ETH", "BNB"]
    assert p["coinsPoly"] == ["BTC"]
    assert p["coinsPolyUnion"] == ["BTC", "XRP"]
    assert p["coinsAll"] == ["BTC"]
    # stripCash collapses net to the paper figure (cash is account-level).
    assert p["strip"]["hasCash"] is False
    assert p["strip"]["cashNet"] is None
    assert p["strip"]["net"] == 18
    # predictfunWallet: absent ⇒ placeholder; present ⇒ normalized fields.
    assert p["walletAbsent"] == {"present": False}
    assert p["walletPresent"]["present"] is True
    assert p["walletPresent"]["address"] == "0x1234567890abcdef1234"
    assert p["walletPresent"]["chain"] == "bnb"
    assert p["walletPresent"]["apiKeyPresent"] is True
    assert p["walletPresent"]["collateral"] == {"symbol": "USDT", "balance": 250.5}


def test_single_venue_console_wiring_and_wallet_panel():
    """SINGLE-VENUE console wiring (2026-08-01, predict.fun Tokyo cutover).

    The venue dimension is still first-class, but it is now DERIVED, never
    CHOSEN: app.jsx reads the venue off the live payload and threads it to
    every page, the Topbar shows a passive badge instead of a switcher, and
    there is no PF_LIVE payload swap. Every venue-specific label (wallet
    vocabulary, brand line) comes from the payload so the predict.fun console
    stops describing itself in Polymarket's terms."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    app = (dash / "app.jsx").read_text(encoding="utf-8")
    shell = (dash / "shell.jsx").read_text(encoding="utf-8")
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    live_page = (dash / "pages-live.jsx").read_text(encoding="utf-8")
    analytics = (dash / "pages-analytics.jsx").read_text(encoding="utf-8")
    config_page = (dash / "pages-config.jsx").read_text(encoding="utf-8")
    latency = (dash / "pages-latency.jsx").read_text(encoding="utf-8")

    # Capital flows show the configured opening balance as a display row, then
    # append future operator-recorded deposits/withdrawals. Rebate rows are
    # filtered OUT of this panel (operator 2026-08-05 — rebates live on the
    # Overview P&L strip), and the net row sums only the displayed flows so it
    # cannot silently include the hidden rebates (kpis.net_deposits does).
    assert "initial={(live.kpis || {}).initial_bankroll}" in config_page
    assert "startAt={(live.kpis || {}).wallet_start_at}" in config_page
    assert 'sub="deposits / withdrawals"' in config_page
    assert "f.kind !== 'rebate'" in config_page
    assert "net={(live.kpis || {}).net_deposits}" not in config_page
    assert "operator-recorded deposits, withdrawals and rebates" not in config_page

    # App: the venue is DERIVED from the payload, never persisted or chosen.
    assert "window.CB.loadVenueSel" not in _code_only(app)
    assert "window.CB.saveVenueSel" not in _code_only(app)
    assert "setVenue" not in _code_only(app)
    assert "window.CB.venueOf(live)" in app
    assert "venueSel={deployVenue}" in app  # to every Page
    # No cross-venue payload swap: the console renders THIS process's payload.
    assert "PF_LIVE" not in _code_only(app)
    assert "const view = live;" in app

    # Shell: no switcher, no setVenue prop; a passive venue badge instead.
    assert "function Topbar({ meta, live, range, setRange })" in shell
    assert "setVenue" not in _code_only(shell)
    assert "badge-venue" in shell
    # Brand line + the latency NAV subtitle are venue-derived / venue-neutral —
    # neither may hardcode Polymarket.
    assert "venueLabel(live)" in shell
    assert "Polymarket 5m/15m journal" not in _code_only(shell)
    assert "~300ms decision→fill floor" not in _code_only(shell)

    # Every ledger-derived page consumes the SHARED selector (scopeLive), not an
    # ad-hoc filter, and reads the venueSel prop.
    for page_src in (overview, live_page, analytics):
        assert "scopeLive(live, venueSel)" in page_src
    # Journal coin filter respects the venue universe ∪ the scoped rows' coins.
    assert "scopedCoins(live, venueSel, allLedger)" in live_page

    # Operator page: venue-aware wallet vocabulary. POLY_FUNDER / pUSD named a
    # wallet and a token that do not exist on predict.fun.
    assert "PREDICT_ACCOUNT wallet record" in config_page
    assert "POLY_FUNDER wallet record" in config_page
    assert "const collateral = isPf ? 'USDT' : 'pUSD';" in config_page
    assert 'title="POLY_FUNDER wallet record"' not in config_page
    # ...and the cross-venue read-only guard is gone with the snapshot swap.
    assert "is_snapshot" not in _code_only(config_page)
    assert "readOnly" not in _code_only(config_page)

    # Latency page: taker_delay_ms is null on predict.fun — never rendered as
    # "nullms", and never defaulted to Polymarket's 250ms.
    assert "vf.taker_delay_ms != null" in latency
    assert "vf ? vf.taker_delay_ms : 250" not in _code_only(latency)

    # The predict.fun funder-wallet + book-recorder-health + deployed-execution
    # -flags panels were REMOVED from the Operator page by operator request
    # 2026-07-23 (the recorder subsystem itself was retired 2026-08-04).
    assert "predict.fun funder wallet" not in config_page
    assert "predictfunWallet(live)" not in config_page
    assert "StrategyFlagsPanel" not in config_page



def test_pf_paper_merge_seam_is_gone():
    """The predict.fun DRY paper-instance merge seam is REMOVED (2026-08-01).

    window.PF_PAPER was populated by a 60s poll of predictfun_paper.json — a
    dump of a SECOND process's SQLite DB — and merged into this console's
    ledger. With each console single-venue by deployment, the dump has no
    producer and no consumer. The regression this guards: a stray PF_PAPER
    global (a stale browser tab, a re-added poller) must NOT be able to inject
    foreign rows into a live-money console's ledger ever again."""
    p = _run_client_js(
        """
        const live = { kpis: { a: 1 }, ledger: [
            { id: 1, coin: 'BTC' },
            { id: 2, coin: 'ETH', venue: 'polymarket' },
          ], ledger_rollup: { total: 9 }, markets: [] };

        // A stray global from a stale tab / re-added poller. It must be INERT.
        w.PF_PAPER = [
          { id: 2, coin: 'BTC', venue: 'predictfun' },
          { id: 7, coin: 'BNB', venue: 'predictfun' },
        ];
        const pf = cb.scopeLive(live, 'predictfun');
        const poly = cb.scopeLive(live, 'polymarket');

        console.log(JSON.stringify({
          mergeHelpersGone: cb.pfPaperRows === undefined && cb.mergedLedger === undefined,
          pfKeys: pf.ledger.map(cb.rowKey),
          polyKeys: poly.ledger.map(cb.rowKey),
          rowKeyAbsentVenue: cb.rowKey({ id: 3 }),
        }));
        """
    )

    # The helpers are gone from the shared namespace, not merely unused.
    assert p["mergeHelpersGone"] is True
    # PREDICT.FUN view = this payload's OWN predictfun rows only (none here) —
    # the stray PF_PAPER global contributed nothing.
    assert p["pfKeys"] == []
    # POLYMARKET view is exactly the payload's own rows.
    assert p["polyKeys"] == ["polymarket:1", "polymarket:2"]
    # rowKey still venue-qualifies (a box's ledger can hold historic rows from
    # a venue it no longer trades), defaulting untagged rows to polymarket.
    assert p["rowKeyAbsentVenue"] == "polymarket:3"


def test_snapshot_dump_producers_are_unreferenced():
    """The dashboard-snapshot dump seam is RETIRED (2026-08-01) and must stay
    that way. It existed only so the :8800 Polymarket console could render the
    predict.fun box:

        refresh_dashboard_snapshots.sh   -> dashboard/predictfun_paper.json
        dump_snapshot_json.py            -> dashboard/predictfun_live.json
        cryptobot-snapshot-refresh.{service,timer}      (ran the first)
        cryptobot-predictfun-snapshot.{service,timer}   (ran the second)

    Each console is single-venue by deployment now, so both dumps have no
    consumer — and serving trading rows / wallet addresses off a periodically
    dumped file that silently goes stale is pure attack surface.

    This test asserts the files are UNREFERENCED, not that they are absent, so
    it is green both before and after the orchestrator removes them. The
    surfaces checked are exactly the ones that would resurrect a producer: the
    deploy staging list, the remote installer, the client bundle, and the SSE
    server's static allowlist."""
    retired_files = (
        "refresh_dashboard_snapshots.sh",
        "dump_snapshot_json",
        "cryptobot-snapshot-refresh.service",
        "cryptobot-snapshot-refresh.timer",
        "cryptobot-predictfun-snapshot.service",
        "cryptobot-predictfun-snapshot.timer",
    )
    retired_outputs = ("predictfun_paper.json", "predictfun_live.json")

    deploy_sh = (_REPO_ROOT / "polymarket" / "infra" / "deploy.sh").read_text(
        encoding="utf-8"
    )
    # deploy.sh must not STAGE any of them. The `for src in ... do cp` block is
    # the only staging surface; a name may still appear in the do-not-re-add
    # comment, so match the staged-path form specifically.
    staged = re.search(r"^for src in \\\n(.*?)^do$", deploy_sh, re.S | re.M)
    assert staged, "deploy.sh staging list not found"
    for name in retired_files:
        assert name not in staged.group(1), f"{name} is still staged by deploy.sh"

    installer = (_REPO_ROOT / "polymarket" / "infra" / "install_remote.sh").read_text(
        encoding="utf-8"
    )
    # The installer may only mention them inside the CONVERGENCE list (which
    # disables + removes them) — never in the install loop or a chmod/enable.
    for name in retired_files:
        assert f'"$PROJECT_DIR/deploy/{name}"' not in installer, (
            f"installer still lays down / chmods {name}"
        )
        assert f"enable --now {name}" not in installer

    # Client bundle: no poller, no globals, no payload swap.
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    bridge = _code_only((dash / "live-bridge.js").read_text(encoding="utf-8"))
    app = _code_only((dash / "app.jsx").read_text(encoding="utf-8"))
    data = _code_only((dash / "data.jsx").read_text(encoding="utf-8"))
    for out in retired_outputs:
        assert f"fetch('{out}'" not in bridge
    assert "pollPredictfunPaper" not in bridge
    assert "pollPredictfunLive" not in bridge
    assert "window.PF_PAPER" not in data
    assert "window.PF_LIVE" not in app

    # SSE server: dropped from BOTH the static allowlist and the auth tuple —
    # they change together or an allowlisted JSON leaks unauthenticated.
    sse = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "observability" / "sse_server.py"
    ).read_text(encoding="utf-8")
    for out in retired_outputs:
        assert f'"/{out}",' not in sse, f"/{out} still in the sse_server allowlist"
    assert 'if path in ("/snapshot.json",)' in sse


def test_scope_live_rollup_retention_and_by_venue_preference():
    """Item 1 (operator's 500-row collapse): scopeLive KEEPS the server
    lifetime rollups when the venue filter removes ZERO rows (the payload's
    ledger IS that venue), and PREFERS the server per-venue rollups
    (ledger_rollup_by_venue/ledger_daily_by_venue, H5) whenever the producer
    emits them.

    2026-08-01: the `paperMerged` carve-out is gone with the static PF-paper
    merge — every row scopeLive sees now comes from THIS box's own stream, so
    the server rollups always describe the same population."""
    p = _run_client_js(
        """
        // (a) every row is (implicitly) polymarket, NO per-venue rollups: the
        // POLYMARKET view removes zero rows -> all-venue rollups KEPT; the
        // PREDICTFUN view removes every row -> rollups nulled (client agg).
        const liveA = { ledger: [{ id: 1 }, { id: 2, venue: 'polymarket' }],
          ledger_rollup: { total: 9 }, ledger_daily: [{ date: '2026-07-01', pnl: 1 }],
          ledger_equity: { points: [] }, markets: [] };
        const keepPoly = cb.scopeLive(liveA, 'polymarket');
        const emptyPf = cb.scopeLive(liveA, 'predictfun');

        // (b) per-venue rollups emitted: a venue view prefers ITS rollup even
        // though the filter removed rows; equity (no per-venue series) nulls.
        const liveB = { ledger: [{ id: 1 }, { id: 2, venue: 'predictfun' }],
          ledger_rollup: { total: 9 }, ledger_daily: [{ date: 'all', pnl: 0 }],
          ledger_equity: { points: [] },
          ledger_rollup_by_venue: {
            polymarket: { total: 5, wins: 3, losses: 1, stops: 1, win_rate: 0.6 },
            predictfun: { total: 4 } },
          ledger_daily_by_venue: {
            polymarket: [{ date: '2026-07-01', pnl: 1.5, n: 2, wins: 1, losses: 0, stops: 1 }],
            predictfun: [] },
          markets: [] };
        const polyB = cb.scopeLive(liveB, 'polymarket');
        const pfB = cb.scopeLive(liveB, 'predictfun');

        // (c) a stray PF_PAPER global must be INERT — the static merge is gone.
        w.PF_PAPER = [{ id: 9, venue: 'predictfun' }];
        const pfStray = cb.scopeLive(liveB, 'predictfun');

        // (d) EMPTY stream (right after a RESET_RECORDS cutover): a named-venue
        // view must NOT leak the all-venue rollups via the 0===0 keepAll edge
        // (the predict.fun tab rendered Polymarket lifetime numbers, 2026-07-22).
        const liveEmpty = { ledger: [], ledger_rollup: { total: 9 },
          ledger_daily: [{ date: '2026-07-01', pnl: 1 }],
          ledger_equity: { points: [] }, markets: [] };
        const pfEmpty = cb.scopeLive(liveEmpty, 'predictfun');
        const polyEmpty = cb.scopeLive(liveEmpty, 'polymarket');

        console.log(JSON.stringify({
          keepPolyRollupKept: keepPoly.ledger_rollup === liveA.ledger_rollup,
          keepPolyDailyKept: keepPoly.ledger_daily === liveA.ledger_daily,
          keepPolyEquityKept: keepPoly.ledger_equity === liveA.ledger_equity,
          emptyPfRollupNull: emptyPf.ledger_rollup === null,
          pfEmptyAllNull: pfEmpty.ledger_rollup === null
            && pfEmpty.ledger_daily === null && pfEmpty.ledger_equity === null,
          polyEmptyAllNull: polyEmpty.ledger_rollup === null
            && polyEmpty.ledger_daily === null && polyEmpty.ledger_equity === null,
          polyBRollupTotal: polyB.ledger_rollup.total,
          polyBDaily: polyB.ledger_daily,
          polyBEquityNull: polyB.ledger_equity === null,
          pfBRollupTotal: pfB.ledger_rollup.total,
          pfBDailyLen: pfB.ledger_daily.length,
          pfStrayRollupTotal: pfStray.ledger_rollup.total,
          pfStrayLedgerIds: pfStray.ledger.map((r) => r.id),
        }));
        """
    )

    # (a) zero-rows-removed keeps every server block by reference.
    assert p["keepPolyRollupKept"] is True
    assert p["keepPolyDailyKept"] is True
    assert p["keepPolyEquityKept"] is True
    # A venue with no rows (and no per-venue rollup) still nulls (client agg).
    assert p["emptyPfRollupNull"] is True
    # (d) empty stream: the 0===0 keepAll edge must not leak all-venue blocks
    # into ANY named-venue view (post-RESET_RECORDS regression, 2026-07-22).
    assert p["pfEmptyAllNull"] is True
    assert p["polyEmptyAllNull"] is True
    # (b) per-venue rollups win over both null and the all-venue block.
    assert p["polyBRollupTotal"] == 5
    assert p["polyBDaily"] == [
        {"date": "2026-07-01", "pnl": 1.5, "n": 2, "wins": 1, "losses": 0, "stops": 1}
    ]
    assert p["polyBEquityNull"] is True  # no per-venue equity series exists
    assert p["pfBRollupTotal"] == 4
    assert p["pfBDailyLen"] == 0
    # (c) a stray window.PF_PAPER global is INERT: the ledger is exactly this
    # payload's own predictfun row (id 2), and the server per-venue rollup is
    # still preferred because no foreign population was mixed in.
    assert p["pfStrayLedgerIds"] == [2]
    assert p["pfStrayRollupTotal"] == 4


def test_client_win_loss_daily_and_settle_ts_contract():
    """Client twin of the producer contracts: settleTs keys stop rows on their
    immutable exit_ts (mirroring the server _MYT_DAY_SQL /
    payload.contracts.day_bucket); aggDaily requires a NUMERIC pnl_net (SQL IS
    NOT NULL parity) and excludes voided cancels from n while keeping their 0
    pnl in the sum; aggSummary/lifetimeSummary implement win rate =
    wins/(wins+losses) with a BOOKED STOP EXIT counted as a plain LOSS
    (2026-08-05 — no `stops` term anywhere), the stop's realized loss inside the
    PF gross loss, avgEdge over settled trades only, ROI over the settled stake.

    Each row takes its own window_start_ts, so each is its own trade group —
    the grouping rule itself is exercised by the test below."""
    p = _run_client_js(
        """
        // Stop row: exit_ts 2026-07-01 20:00 UTC (MYT 2026-07-02) while a LATE
        // sweep backfilled resolution_ts to 2026-07-03 — the day bucket must
        // stay invariant to the backfill (key on exit_ts).
        const stopRow = { id: 3, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 1002,
          outcome: 'CANCELLED', exit_kind: 'stop_loss', pnl_net: -2,
          bet_size: 2, exit_ts: '2026-07-01 20:00:00',
          resolution_ts: '2026-07-03 10:00:00', bet_ts: '2026-07-01 19:00:00',
          realized_edge: 0.02 };
        const winRow = { id: 1, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 1000,
          outcome: 'WIN', pnl_net: 5, bet_size: 4,
          resolution_ts: '2026-07-01 03:00:00', bet_ts: '2026-07-01 02:00:00',
          realized_edge: 0.10 };
        const lossRow = { id: 2, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 1001,
          outcome: 'LOSS', pnl_net: -3, bet_size: 3,
          resolution_ts: '2026-07-01 04:00:00', bet_ts: '2026-07-01 03:00:00',
          realized_edge: -0.05 };
        const voidedRow = { id: 4, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 1003,
          outcome: 'CANCELLED', pnl_net: 0, bet_size: 1,
          resolution_ts: '2026-07-01 03:30:00', bet_ts: '2026-07-01 03:00:00' };
        const ghostNullPnl = { id: 5, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 1004,
          outcome: 'GHOST_FILL', pnl_net: null, bet_size: 0,
          resolution_ts: '2026-07-01 05:00:00', bet_ts: '2026-07-01 04:00:00' };
        const pendingRow = { id: 6, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 1005,
          outcome: 'PENDING', bet_size: 6, realized_edge: 0.9,
          bet_ts: '2026-07-01 06:00:00' };
        const rows = [winRow, lossRow, stopRow, voidedRow, ghostNullPnl, pendingRow];

        const daily = cb.aggDaily(rows);
        const s = cb.aggSummary(rows);
        const lifeFallback = cb.lifetimeSummary(null, rows);
        // A legacy payload that still carries a `stops` column folds it into
        // losses so the client can never render a stop-blind loss count.
        const lifeServer = cb.lifetimeSummary({ ledger_rollup: {
          total: 100, decided: 80, wins: 50, losses: 30, stops: 10, voided: 4,
          open: 6, win_rate: 50 / 90, pnl_net: 12.3, stake: 400, roi: 0.03,
          first_ts: '2026-06-12 00:00:00', last_ts: '2026-07-21 00:00:00' } }, []);

        console.log(JSON.stringify({
          settleStop: cb.settleTs(stopRow),
          settleStopNoExitTs: cb.settleTs({ ...stopRow, exit_ts: null }),
          settleWin: cb.settleTs(winRow),
          daily,
          dailyKeys: Object.keys(daily[0]),
          summaryKeys: Object.keys(s),
          summary: { wins: s.wins, losses: s.losses,
            voided: s.voided, winRate: s.winRate, pf: s.pf, pnl: s.pnl,
            stakeSettled: s.stakeSettled, roi: s.roi, avgEdge: s.avgEdge },
          lifeFallbackKeys: Object.keys(lifeFallback),
          lifeServerKeys: Object.keys(lifeServer),
          lifeFallback: { losses: lifeFallback.losses,
            voided: lifeFallback.voided, winRate: lifeFallback.winRate },
          lifeServer: { losses: lifeServer.losses,
            voided: lifeServer.voided, winRate: lifeServer.winRate },
        }));
        """
    )

    # settleTs: stop rows key on exit_ts; without one they fall through the
    # normal resolution_ts -> bet_ts chain; normal rows unchanged.
    assert p["settleStop"] == "2026-07-01 20:00:00"
    assert p["settleStopNoExitTs"] == "2026-07-03 10:00:00"
    assert p["settleWin"] == "2026-07-01 03:00:00"

    # aggDaily: MYT day buckets; ghost with NULL pnl excluded entirely; the
    # voided cancel excluded from n but its 0 pnl kept in the sum; the stop
    # lands on ITS exit_ts day (2026-07-02 MYT) and counts there AS A LOSS.
    assert p["daily"] == [
        {"date": "2026-07-01", "pnl": 2.0, "n": 2, "wins": 1, "losses": 1},
        {"date": "2026-07-02", "pnl": -2.0, "n": 1, "wins": 0, "losses": 1},
    ]
    assert "stops" not in p["dailyKeys"]

    # aggSummary: winRate = 1/(1+2) with the stop inside `losses`; PF gross loss
    # includes the stop (gl = 3+2); pnl over settled = 0; ROI denominator =
    # settled stake minus the voided refund (4+3+2=9); avgEdge over SETTLED
    # trades only (the pending 0.9 edge is excluded): (0.10 - 0.05 + 0.02)/3.
    assert p["summary"]["wins"] == 1
    assert p["summary"]["losses"] == 2
    assert "stops" not in p["summaryKeys"]     # the key does not exist any more
    assert p["summary"]["voided"] == 1
    assert p["summary"]["winRate"] == pytest.approx(1 / 3)
    assert p["summary"]["pf"] == pytest.approx(5 / 5)
    assert p["summary"]["pnl"] == pytest.approx(0.0)
    assert p["summary"]["stakeSettled"] == pytest.approx(9.0)
    assert p["summary"]["roi"] == pytest.approx(0.0)
    assert p["summary"]["avgEdge"] == pytest.approx((0.10 - 0.05 + 0.02) / 3)

    # lifetimeSummary: no `stops` on either path; a legacy server `stops` column
    # is folded into losses (30 + 10) and the server win_rate rides verbatim.
    assert "stops" not in p["lifeFallbackKeys"]
    assert p["lifeFallback"]["losses"] == 2
    assert p["lifeFallback"]["voided"] == 1
    assert p["lifeFallback"]["winRate"] == pytest.approx(1 / 3)
    assert "stops" not in p["lifeServerKeys"]
    assert p["lifeServer"]["losses"] == 40
    assert p["lifeServer"]["voided"] == 4
    assert p["lifeServer"]["winRate"] == pytest.approx(50 / 90)


def test_client_counts_trade_groups_topups_fold_into_their_base_bet():
    """CLOB semantics on the client (operator 2026-08-05): a base bet and its
    top-ups in the same market window are ONE trade everywhere a count is shown,
    while every $ figure stays the per-fill sum.

    Fixture: 3 BTC/YES fills (base + 2 top-ups) on one 5m window that WINS, a
    BTC/NO position in the SAME window (a different CLOB balance — never
    merged), and an independent ETH loss. 5 ledger rows, 3 trades. Also pins
    that legacy rows with NO window_start_ts degrade to one trade each rather
    than merging with each other."""
    p = _run_client_js(
        """
        const base = { id: 1, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 2000, wallet_id: 'w1', entry_minute: 3,
          outcome: 'WIN', pnl_net: 1.0, bet_size: 2, fill_price: 0.50,
          walked_vwap: 0.50, model_p: 0.7, realized_edge: 0.08,
          resolution_ts: '2026-07-01 03:00:00', bet_ts: '2026-07-01 02:00:00' };
        const top1 = { ...base, id: 2, is_topup: true, entry_minute: 4,
          pnl_net: 0.5, bet_size: 1, fill_price: 0.60, walked_vwap: 0.60,
          realized_edge: 0.01, bet_ts: '2026-07-01 02:01:00' };
        const top2 = { ...base, id: 3, is_topup: true, entry_minute: 4,
          pnl_net: 0.5, bet_size: 1, fill_price: 0.70, walked_vwap: 0.70,
          realized_edge: 0.00, bet_ts: '2026-07-01 02:02:00' };
        // Same window, OTHER side => a different CLOB position, never merged.
        const otherSide = { ...base, id: 4, side: 'NO', pnl_net: -2, bet_size: 2,
          outcome: 'LOSS' };
        const eth = { ...base, id: 5, coin: 'ETH', window_start_ts: 2300,
          outcome: 'LOSS', pnl_net: -4, bet_size: 4 };
        // Legacy rows with no window key at all.
        const legacyA = { id: 6, coin: 'SOL', side: 'YES', interval_min: 5,
          outcome: 'WIN', pnl_net: 0.2, bet_size: 1,
          resolution_ts: '2026-07-01 05:00:00', bet_ts: '2026-07-01 04:00:00' };
        const legacyB = { ...legacyA, id: 7, pnl_net: 0.3 };

        // Deliberately out of order: grouping must not depend on row order.
        const rows = [top2, base, top1, otherSide, eth];
        const groups = cb.groupTrades(rows);
        const btc = groups.find((g) => g.coin === 'BTC' && g.side === 'YES');
        const s = cb.aggSummary(rows);
        const coins = cb.aggCoins(rows);
        const slices = cb.aggSlices(rows);

        console.log(JSON.stringify({
          nGroups: groups.length,
          btc: { nFills: btc.nFills, size: btc.size, pnl: btc.pnl,
            avgFill: btc.avgFill, entry_minute: btc.entry_minute,
            realized_edge: btc.realized_edge, outcome: btc.outcome,
            fillIds: btc.fills.map((f) => f.id), anchorId: btc.anchor.id },
          summary: { total: s.total, settled: s.settled, wins: s.wins,
            losses: s.losses, pnl: s.pnl, stake: s.stake, winRate: s.winRate },
          btcCoin: (() => { const c = coins.find((x) => x.coin === 'BTC');
            return { bets: c.bets, resolved: c.resolved, pnl: c.pnl, stake: c.stake }; })(),
          sliceNs: slices.map((x) => [x.slice, x.n]),
          histogramN: cb.aggHistogram(rows).reduce((a, b) => a + b.n, 0),
          streakTape: cb.aggStreaks(rows).last.length,
          legacyGroups: cb.groupTrades([legacyA, legacyB]).length,
        }));
        """
    )

    # 3 groups from 5 rows: BTC/YES (3 fills), BTC/NO, ETH.
    assert p["nGroups"] == 3
    # The BTC/YES position: fills ordered base-first, aggregated size/pnl, and a
    # SIZE-WEIGHTED effective fill price (2@0.50 + 1@0.60 + 1@0.70)/4 = 0.575.
    assert p["btc"]["nFills"] == 3
    assert p["btc"]["fillIds"] == [1, 2, 3]
    assert p["btc"]["anchorId"] == 1
    assert p["btc"]["size"] == pytest.approx(4.0)
    assert p["btc"]["pnl"] == pytest.approx(2.0)
    assert p["btc"]["avgFill"] == pytest.approx(0.575)
    assert p["btc"]["outcome"] == "WIN"
    # Slice / edge describe the BASE bet, not the last top-up.
    assert p["btc"]["entry_minute"] == 3
    assert p["btc"]["realized_edge"] == pytest.approx(0.08)

    # Every count is 3 trades; every $ figure is the 5-fill sum.
    assert p["summary"]["total"] == 3
    assert p["summary"]["settled"] == 3
    assert p["summary"]["wins"] == 1
    assert p["summary"]["losses"] == 2
    assert p["summary"]["winRate"] == pytest.approx(1 / 3)
    assert p["summary"]["pnl"] == pytest.approx(-4.0)    # 2.0 - 2.0 - 4.0
    assert p["summary"]["stake"] == pytest.approx(10.0)  # 2+1+1+2+4
    assert p["btcCoin"]["bets"] == 2 and p["btcCoin"]["resolved"] == 2
    assert p["btcCoin"]["pnl"] == pytest.approx(0.0)
    assert p["btcCoin"]["stake"] == pytest.approx(6.0)
    # The base bet's slice owns the whole trade: every group is 5m_e3.
    assert dict(p["sliceNs"]) == {"5m_e3": 3}
    assert p["histogramN"] == 3
    assert p["streakTape"] == 3

    # Legacy rows with no window key never merge into one another.
    assert p["legacyGroups"] == 2


def test_client_trade_counts_exclude_voided_never_filled_cancels():
    """The client fallback path counts the SAME population as the server
    (operator 2026-08-09): settled TRADE GROUPS that actually OPENED.

    A voided never-filled cancel (outcome CANCELLED, no exit_kind) never opened
    a position, so it is not a trade — it belongs to `voided` alone. Before the
    fix aggSummary.settled, aggCoins.bets/.resolved, aggSlices.n, aggHistogram
    and the aggRatios expectancy denominator all counted it, and aggEquity even
    plotted a $0 point for it, so the fallback chart's x-axis disagreed with the
    server's per-trade curve.

    Fixture (5 rows → 4 groups): a BTC win folded from base + top-up, an ETH
    loss, a SOL voided cancel carrying a non-NULL 0.0 refund, and an XRP pending
    position. Trades = 2, voided = 1, open = 1.
    """
    p = _run_client_js(
        """
        const base = { id: 1, coin: 'BTC', side: 'YES', interval_min: 5,
          window_start_ts: 2000, wallet_id: 'w1', entry_minute: 3,
          outcome: 'WIN', pnl_net: 2.0, bet_size: 2, fill_price: 0.5,
          realized_edge: 0.08,
          resolution_ts: '2026-07-01 03:00:00', bet_ts: '2026-07-01 02:00:00' };
        const top = { ...base, id: 2, is_topup: true, entry_minute: 4,
          pnl_net: 1.0, bet_size: 1, bet_ts: '2026-07-01 02:01:00' };
        const eth = { ...base, id: 3, coin: 'ETH', window_start_ts: 2300,
          outcome: 'LOSS', pnl_net: -1.0, bet_size: 2, is_topup: false,
          resolution_ts: '2026-07-01 03:05:00' };
        // Voided never-filled cancel: pnl_net is NOT NULL (a 0.0 refund), so
        // only the GROUP-level voided rule can keep it out of the counts.
        const sol = { ...base, id: 4, coin: 'SOL', window_start_ts: 2600,
          outcome: 'CANCELLED', exit_kind: null, pnl_net: 0.0, bet_size: 2,
          is_topup: false, resolution_ts: '2026-07-01 03:10:00' };
        const xrp = { ...base, id: 5, coin: 'XRP', window_start_ts: 2900,
          outcome: 'PENDING', pnl_net: null, bet_size: 2, is_topup: false,
          resolution_ts: null };

        const rows = [base, top, eth, sol, xrp];
        const s = cb.aggSummary(rows);
        const coins = cb.aggCoins(rows);
        const eq = cb.aggEquity(rows, 100);
        const ratios = cb.aggRatios(rows, 100);
        const life = cb.lifetimeSummary(
          { ledger_rollup: { total: 4, decided: 2, wins: 1, losses: 1,
            voided: 1, settled_opened: 2, open: 1, win_rate: 0.5,
            pnl_net: 2.0, stake: 9, roi: null } }, rows);
        const legacyLife = cb.lifetimeSummary(
          { ledger_rollup: { total: 4, decided: 2, wins: 1, losses: 1,
            voided: 1, open: 1, win_rate: 0.5, pnl_net: 2.0, stake: 9 } }, rows);

        console.log(JSON.stringify({
          summary: { total: s.total, settled: s.settled, open: s.open,
            voided: s.voided, pnl: s.pnl },
          coinCounts: coins.map((c) => [c.coin, c.bets, c.resolved]),
          sliceN: cb.aggSlices(rows).reduce((a, x) => a + x.n, 0),
          histogramN: cb.aggHistogram(rows).reduce((a, b) => a + b.n, 0),
          equityN: eq.n, equityFinal: eq.final,
          expectancy: ratios.expectancy,
          lifeSettled: life.settled, legacyLifeSettled: legacyLife.settled,
        }));
        """
    )

    # total splits into three DISJOINT buckets, exactly like the server.
    assert p["summary"]["total"] == 4
    assert p["summary"]["settled"] == 2
    assert p["summary"]["open"] == 1
    assert p["summary"]["voided"] == 1
    # $ untouched: the voided group still contributes its 0.0.
    assert p["summary"]["pnl"] == pytest.approx(2.0)   # +3.0 −1.0 +0.0

    # The voided SOL group counts in NEITHER coin column; the open XRP group
    # counts as a bet but is not resolved.
    assert dict((c[0], (c[1], c[2])) for c in p["coinCounts"]) == {
        "BTC": (1, 1), "ETH": (1, 1), "SOL": (0, 0), "XRP": (1, 0),
    }
    assert p["sliceN"] == 2
    assert p["histogramN"] == 2
    # Fallback equity curve: one point per OPENED trade, so its n is the same
    # trade count the hero tile shows (was 3 rows + a $0 voided point).
    assert p["equityN"] == 2
    assert p["equityFinal"] == pytest.approx(102.0)     # 100 + 3.0 − 1.0
    # Expectancy over the 2 opened trades, not 3 with the $0 refund.
    assert p["expectancy"] == pytest.approx(1.0)
    # lifetimeSummary reads the server scalar, and a payload that predates it
    # falls back to total − open − voided (never the old total − open == 3).
    assert p["lifeSettled"] == 2
    assert p["legacyLifeSettled"] == 2


def test_open_bet_counts_are_trade_groups_not_pending_fills():
    """One meaning for "open bets" (operator 2026-08-09).

    The Overview rail tile, the hero 24h "N open excluded" sub and the sidebar
    Journal badge used to render kpis.n_pending — capital.py's per-FILL count of
    PENDING ledger rows — while the Journal's own "open positions" tile grouped
    them, so one console gave two answers. They now read the grouped
    kpis.n_pending_trades and only fall back to the per-fill number on a legacy
    payload, where the rail relabels itself "open fills" rather than lying.
    """
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    shell = (dash / "shell.jsx").read_text(encoding="utf-8")

    assert ("const openTrades = k.n_pending_trades != null "
            "? k.n_pending_trades : (k.n_pending || 0);") in overview
    assert "const openIsFills = k.n_pending_trades == null;" in overview
    # Rail tile renders the grouped count and names itself after it.
    assert '<span className="rail-v">{openTrades}</span>' in overview
    assert "open {openIsFills ? 'fills' : 'bets'}" in overview
    # 24h sub follows the same variable.
    assert ("`${openTrades} open ${openIsFills ? 'fills ' : ''}excluded`"
            in overview)
    # The bare sidebar badge cannot carry a caveat, so it must be the grouped one.
    assert ("journal: (k && (k.n_pending_trades != null "
            "? k.n_pending_trades : k.n_pending)) || null,") in shell


def test_money_bridge_prefers_live_strategy_paper():
    """Item 11: the cash drift bridge nets against account_strategy_pnl_total
    (LIVE-only ledger paper — cash only moves for LIVE rows) when the producer
    emits it, falling back to pnl_net_usd; withPaperBasis (Calendar override)
    re-bases BOTH the displayed paper and the bridge to Σ(rendered cells)."""
    p = _run_client_js(
        """
        const withStrategy = cb.money({
          account_capital_live: 118.42, account_pnl_total_live: 14.87,
          account_capital_live_source: 'balance_cache',
          pnl_net_usd: 12.5, account_strategy_pnl_total: 14.0 });
        const noStrategy = cb.money({ account_pnl_total: 10.0, pnl_net_usd: 8.0 });
        const rebased = cb.withPaperBasis(withStrategy, 20.0);

        console.log(JSON.stringify({
          ws: { paper: withStrategy.paper, paperLive: withStrategy.paperLive,
            bridgePaper: withStrategy.bridgePaper,
            drift: Number(withStrategy.drift.toFixed(2)), net: withStrategy.net },
          ns: { paperLive: noStrategy.paperLive, bridgePaper: noStrategy.bridgePaper,
            drift: Number(noStrategy.drift.toFixed(2)) },
          rb: { paper: rebased.paper, bridgePaper: rebased.bridgePaper,
            paperLive: rebased.paperLive,
            drift: Number(rebased.drift.toFixed(2)), net: rebased.net },
        }));
        """
    )

    # Strategy paper present: displayed paper stays the canonical all-rows
    # figure; the bridge basis + drift use the LIVE-only figure.
    assert p["ws"]["paper"] == 12.5
    assert p["ws"]["paperLive"] == 14.0
    assert p["ws"]["bridgePaper"] == 14.0
    assert p["ws"]["drift"] == 0.87  # 14.87 - 14.0, NOT - 12.5
    # Cash-first headline (operator 2026-07-24): net = the era-anchored CASH
    # figure (14.87); the ledger paper (14.0) now rides the sub as the
    # reconciliation (net = paper + drift = 14.0 + 0.87).
    assert p["ws"]["net"] == 14.87
    # Absent -> falls back to pnl_net_usd exactly as before.
    assert p["ns"]["paperLive"] is None
    assert p["ns"]["bridgePaper"] == 8.0
    assert p["ns"]["drift"] == 2.0
    # Calendar override re-bases paper AND the bridge to the rendered cells. The
    # DEFAULT headline is cash-first (operator 2026-07-24), but a calendar-selected
    # period is inherently PAPER (cash is one current balance, not a per-day
    # figure), so withPaperBasis keeps the override's headline on the rebased
    # paper — the calendar view still works, only the unfiltered headline is cash.
    assert p["rb"]["paper"] == 20.0
    assert p["rb"]["bridgePaper"] == 20.0
    assert p["rb"]["paperLive"] is None
    assert p["rb"]["drift"] == -5.13  # 14.87 - 20.0
    assert p["rb"]["net"] == 20.0  # calendar-selected period -> rebased paper


# ── Babel-parse check over every local dashboard module ─────────────────────
# The version + SRI hash MUST stay in lockstep with the babel.min.js script
# tag in dashboard/index.html — the check's whole point is compiling with the
# exact build the browser runs.
_BABEL_VERSION = "7.29.0"
_BABEL_SRI_B64 = "m08KidiNqLdpJqLq95G/LEi8Qvjl/xUYll3QILypMoQ65QorJ9Lvtp2RXYGBFj1y"


def _babel_standalone() -> Path:
    """The EXACT @babel/standalone build index.html pins, cached in the system
    temp dir and SRI-checked on every use: a missing/corrupt cache
    re-downloads; a fresh download that fails the pinned hash FAILS the test
    (supply-chain guard, same posture as index.html's integrity attribute);
    no network at all skips — never a false green."""
    cache = Path(tempfile.gettempdir()) / f"babel-standalone-{_BABEL_VERSION}.min.js"

    def _intact() -> bool:
        if not cache.is_file():
            return False
        digest = hashlib.sha384(cache.read_bytes()).digest()
        return base64.b64encode(digest).decode("ascii") == _BABEL_SRI_B64

    if not _intact():
        url = f"https://unpkg.com/@babel/standalone@{_BABEL_VERSION}/babel.min.js"
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                cache.write_bytes(resp.read())
        except OSError:
            pytest.skip("cannot fetch the pinned @babel/standalone (offline?)")
        assert _intact(), "downloaded @babel/standalone failed the index.html SRI pin"
    return cache


def test_dashboard_modules_compile_under_babel():
    """Browser-Babel dashboard = no build step, so a syntax slip in ANY module
    blanks the whole page at runtime. Compile every local module index.html
    loads (live-bridge.js plain-parsed, each text/babel .jsx through the react
    preset) with the SAME pinned Babel standalone build the browser uses. The
    module list is read from index.html's ?v= script tags, so a new module is
    covered the moment it is wired in."""
    if not shutil.which("node"):
        pytest.skip("node is not available for dashboard module compile test")
    babel_path = _babel_standalone()

    index = (_REPO_ROOT / "polymarket" / "dashboard" / "index.html").read_text(encoding="utf-8")
    modules = re.findall(r'<script[^>]*\ssrc="([^"?]+)\?v=', index)
    assert {"live-bridge.js", "data.jsx", "app.jsx"} <= set(modules), modules
    files = [_REPO_ROOT / "polymarket" / "dashboard" / m for m in modules]
    for f in files:
        assert f.is_file(), f"index.html references a missing module: {f}"


    script = textwrap.dedent(
        f"""\
        const fs = require('fs');
        const Babel = require({json.dumps(str(babel_path))});
        const files = {json.dumps([str(f) for f in files])};
        const errors = [];
        for (const f of files) {{
          const src = fs.readFileSync(f, 'utf8');
          const jsx = f.endsWith('.jsx');
          try {{
            Babel.transform(src, {{ presets: jsx ? ['react'] : [], filename: f }});
          }} catch (e) {{
            errors.push(f + ' :: ' + String(e && e.message ? e.message : e).split('\\n')[0]);
          }}
        }}
        console.log(JSON.stringify({{ checked: files.length, errors }}));
        """
    )
    result = subprocess.run(
        ["node", "-e", script],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    report = json.loads(result.stdout)
    assert report["checked"] == len(files)
    assert report["errors"] == [], "\n".join(report["errors"])


def test_dashboard_plain_assets_are_loaded_and_staged_in_order():
    """Every plain-JS dependency in index.html must ship in deploy.sh."""
    index = (_REPO_ROOT / "polymarket" / "dashboard" / "index.html").read_text(encoding="utf-8")
    deploy = (_REPO_ROOT / "polymarket" / "infra" / "deploy.sh").read_text(
        encoding="utf-8"
    )
    assert index.index('src="time.js?') < index.index('src="live-bridge.js?')
    assert index.index('src="live-bridge.js?') < index.index('src="api.js?')
    for asset in ("time.js", "live-bridge.js", "api.js"):
        assert f'cp "$REPO_ROOT/polymarket/dashboard/{asset}" "$STAGING_DIR/dashboard/"' in deploy
        assert f'"$STAGING_DIR/dashboard/{asset}"' in deploy


def test_select_analytics_prefers_the_full_ledger_block():
    """Historical/aggregate panels must describe the FULL ledger, not the
    bounded stream window (the newest LEDGER_BOOTSTRAP_LIMIT rows). Companion
    to the "operator's 500-row collapse" case above: selectAnalytics prefers
    live.ledger_analytics — server-computed over every row and pre-sliced by
    the SAME range toggle the client offers, because these are counts, not rows
    the client could slice itself — restores the Infinity PF sentinel strict
    JSON cannot carry, and falls back to the client aggregates only when the
    block (or the selected range key) is absent."""
    p = _run_client_js(
        """
        const server = {
          All: {
            summary: { total: 3000, settled: 2900, pnl: 12.5, pf: null, pf_unbounded: true },
            coins: [{ coin: 'BTC', bets: 3000, pf: null, pf_unbounded: true }],
            slices: [{ slice: '15m_e4', n: 2900 }],
            histogram: [{ mid: 0, n: 2900 }],
            streaks: { current: 3, longestW: 9, longestL: 4, last: ['w'] } },
          '7d': {
            summary: { total: 400, settled: 380, pnl: 1.5, pf: 1.2, pf_unbounded: false },
            coins: [], slices: [], histogram: [],
            streaks: { current: 0, longestW: 0, longestL: 0, last: [] } },
        };
        // The bounded stream carries only 2 rows — what the panels used to see.
        const rows = [
          { id: 1, coin: 'BTC', outcome: 'WIN', pnl_net: 1, bet_size: 1,
            interval_min: 15, entry_minute: 4, resolution_ts: '2026-07-27 00:00:00' },
          { id: 2, coin: 'BTC', outcome: 'LOSS', pnl_net: -1, bet_size: 1,
            interval_min: 15, entry_minute: 4, resolution_ts: '2026-07-27 00:00:00' },
        ];
        const live = { ledger: rows, ledger_analytics: server };
        const all = cb.selectAnalytics(live, 'All');
        const d7 = cb.selectAnalytics(live, '7d');
        const d30 = cb.selectAnalytics(live, '30d');      // key absent -> fallback
        const legacy = cb.selectAnalytics({ ledger: rows }, 'All');  // no block
        console.log(JSON.stringify({
          allFull: all.full, allTotal: all.summary.total,
          allPf: all.summary.pf === Infinity ? 'inf' : all.summary.pf,
          coinPf: all.coins[0].pf === Infinity ? 'inf' : all.coins[0].pf,
          allStreakBest: all.streaks.longestW, allSliceN: all.slices[0].n,
          d7Total: d7.summary.total, d7Pf: d7.summary.pf, d7Full: d7.full,
          d30Full: d30.full,
          legacyFull: legacy.full, legacyTotal: legacy.summary.total,
        }));
        """
    )
    # The server block wins: 3000 bets, not the 2 rows in the stream.
    assert p["allFull"] is True
    assert p["allTotal"] == 3000
    assert p["allStreakBest"] == 9
    assert p["allSliceN"] == 2900
    # pf=null + pf_unbounded round-trips back to the '∞' sentinel (summary AND
    # each coin row) rather than rendering '—' as if nothing had settled.
    assert p["allPf"] == "inf"
    assert p["coinPf"] == "inf"
    # The range toggle picks its own pre-sliced entry.
    assert p["d7Full"] is True
    assert p["d7Total"] == 400
    assert p["d7Pf"] == 1.2
    # A missing range key or a legacy payload degrades to the client aggregate
    # over the bounded window — flagged so the panels can label it honestly.
    assert p["d30Full"] is False
    assert p["legacyFull"] is False
    assert p["legacyTotal"] == 2


def test_scope_live_carries_ledger_analytics():
    """The analytics block gets the SAME venue treatment as the other server
    rollups: the per-venue variant wins, and the all-venue block survives only
    when the venue filter removed ZERO rows.

    2026-08-01: the merged-static-paper-rows carve-out is gone — a stray
    window.PF_PAPER global no longer reaches scopeLive at all."""
    p = _run_client_js(
        """
        const live = {
          ledger: [{ id: 1 }, { id: 2, venue: 'predictfun' }],
          ledger_rollup: { total: 9 }, ledger_daily: [], ledger_equity: { points: [] },
          ledger_analytics: { All: { summary: { total: 9 } } },
          ledger_analytics_by_venue: {
            polymarket: { All: { summary: { total: 5 } } },
            predictfun: { All: { summary: { total: 4 } } } },
          markets: [] };
        const poly = cb.scopeLive(live, 'polymarket');
        const pf = cb.scopeLive(live, 'predictfun');
        // Zero rows removed -> the all-venue block is KEPT by reference.
        const allPoly = { ...live, ledger: [{ id: 1 }],
          ledger_analytics_by_venue: undefined };
        const keptAll = cb.scopeLive(allPoly, 'polymarket');
        // A stray PF_PAPER global is inert: the per-venue block still wins.
        w.PF_PAPER = [{ id: 9, venue: 'predictfun' }];
        const pfStray = cb.scopeLive(live, 'predictfun');
        console.log(JSON.stringify({
          polyTotal: poly.ledger_analytics.All.summary.total,
          pfTotal: pf.ledger_analytics.All.summary.total,
          keptAllSame: keptAll.ledger_analytics === allPoly.ledger_analytics,
          pfStrayTotal: pfStray.ledger_analytics.All.summary.total,
          pfStrayIds: pfStray.ledger.map((r) => r.id),
        }));
        """
    )
    assert p["polyTotal"] == 5
    assert p["pfTotal"] == 4
    assert p["keptAllSame"] is True
    assert p["pfStrayTotal"] == 4
    assert p["pfStrayIds"] == [2]


def test_fourth_wave_declutter_overview_subminute_and_latency():
    """Operator declutter 2026-08-05, FOURTH wave.

    Overview: the 24h tile's "fills" qualifier, the four P&L-strip sub-labels
    and the weekly-P&L "full history" sub are deleted.
    Subminute: the whole DD-brake / hard-halt EXPLAINER cluster goes — the
    permanent "DD-brake ×1.00" banner + tile, the "Hard halt: bot stops at N%"
    line, the "Stake throttle: removed …" line, the "throttle removed (floor
    ×1.00)" sub, the "15m X% · 5m $Y" sizing echo, the "N orders · breaker < …"
    sub and the "< 3 resolved" sample notes.
    Latency: the "not reported by this scanner build" placeholders, the prose
    columns ("what it is" / "meaning") and the "uncancellable taker delay ·
    inside the POST" sub.

    NOTHING that is a live CONTROL was lost — see
    test_subminute_brake_and_breaker_telemetry_survives_the_declutter."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    sub = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")
    latency = (dash / "pages-latency.jsx").read_text(encoding="utf-8")

    # ── A. Overview ──
    assert "L fills`" not in overview
    assert "${k.wins_24h || 0}W · ${k.losses_24h || 0}L`" in overview
    assert 'sub={dailyLabel}' not in overview
    assert 'titleHint={`population: ${dailyLabel}`}' in overview

    # ── B. Subminute explainer cluster ──
    sub_code = _code_only(sub)
    for gone in (
        "Hard halt: bot stops at ",
        "Stake throttle: ",
        "throttle removed (floor ×1.00)",
        "brake floor ×1.00 — full size at every drawdown depth",
        "sub={`15m ${fmt.pct(sizing.base_frac_15m)}",
        "orders · breaker < ${fmt.pct(fr.min_fill_rate, 0)}",
        "'< 3 resolved'",
        "'< 3 resolved misses'",
        "&lt; 3 resolved faster-taker misses",
    ):
        assert gone not in sub_code, f"subminute cluster text {gone!r} survived"
    # The DD-brake tile and banner are ANOMALOUS-ONLY, not unconditional.
    assert "{showBrakeTile && (" in sub
    assert "const showBrakeTile = brake < 0.999" in sub
    assert "if (off && !throttling) return null;" in sub
    assert "if (!throttling && !atFloor) return null;" in sub
    # the sizing echo survives ONLY as a tooltip
    assert "title={`stake = equity × base frac × DD-brake · 15m " in sub

    # ── C. fill rate split: 24h on top, full history at the bottom ──
    assert 'label="fill rate · 24h"' in sub
    assert "fr.fill_rate_24h" in sub
    assert 'label="fill rate · all"' in sub
    assert "rs.fill_rate_usd" in sub

    # ── D. Latency ──
    assert "not reported by this scanner build" not in latency
    assert "<th>what it is</th>" not in latency
    assert "<th>meaning</th>" not in latency
    assert "'uncancellable taker delay · inside the POST'" not in latency
    assert "floorSub" not in latency
    # Prose relocated to row-label tooltips.
    assert '<td className="mono" title={r.why}>{r.k}</td>' in latency
    assert "title={NOTE[name] || undefined}" in latency

    # ── D3. taker delay is inside the total ──
    # (2026-08-06: the standalone ↳ taker row is gone — the residual row now
    #  carries the "taker delay" label; see test_taker_delay_labels_the_residual
    #  below. The measured accepted-POST view lives in the cohort panel.)
    assert "<StageTable stages={stages} takerDelayMs={takerDelay} />" in latency
    assert "function StageTable({ stages, takerDelayMs })" in latency
    # values read from the PAYLOAD venue block, never hardcoded on the client
    assert "const takerDelay = (vf && vf.taker_delay_ms != null) ? vf.taker_delay_ms : null;" in latency
    assert "250" not in _code_only(latency), "a hardcoded taker delay crept back in"
    # total = measured + the part of the delay the measured POST did not capture
    assert "const shortfall = (col) => takerDelayShortfall(takerDelayMs, post ? post[col] : null);" in latency
    assert "const add = isTotal ? shortfall : () => 0;" in latency
    assert "{ms(s.p50 + add('p50'))}" in latency


def test_overview_equity_and_drawdown_are_two_titled_panes():
    """Operator 2026-08-05: the hero used to stack the equity curve and the
    drawdown-from-peak curve flush inside one `.hero-chart` box, so they read
    as ONE chart with a strange tail. They are now two SEPARATE panes, each
    with its own header, separated by a gap + hairline (`.hero-pane`).

    The drawdown pane's title is MANDATORY — "max dd %" is the only place that
    curve is named anywhere on the page. Both panes stay INSIDE the hero card,
    above the P&L decomposition strip and left of the rail."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    styles = (dash / "styles.css").read_text(encoding="utf-8")

    # two panes, each with a header
    assert overview.count('<div className="hero-pane">') == 2
    assert '<div className="hero-pane-head">equity</div>' in overview
    assert '<div className="hero-pane-head" title=' in overview
    assert ">max dd %</div>" in overview
    # each pane wraps its OWN chart box (the graph-paper backdrop)
    assert overview.count('<div className="hero-chart">') == 2
    # the charts themselves are unchanged and still one per pane
    # (the `unit` prop was added 2026-08-09 so the crosshair says "hour" over
    # the hourly cash_equity fallback instead of mislabeling it "trade")
    assert "<EquityChart points={eq.points} base={eq.base} height={228}" in overview
    assert "unit={eq.basis === 'cash' ? 'hour' : 'trade'} />" in overview
    assert "<DrawdownChart points={eq.points} height={64} />" in overview
    charts = (dash / "charts.jsx").read_text(encoding="utf-8")
    assert "if (bi === 0) return 'start · ' + (hv.ts ? fmt.dateMyt(hv.ts) : '—');" in charts
    # the old flush-stacked block is gone
    assert (
        "<EquityChart points={eq.points} base={eq.base} height={228} />\n"
        "            <DrawdownChart"
    ) not in overview

    # visual separation is CSS, night-desk style: gap + hairline between panes
    assert ".hero-pane + .hero-pane {" in styles
    assert "border-top: 1px solid var(--border-faint);" in styles
    assert ".hero-pane-head {" in styles

    # both panes still sit inside the hero card, before the strip and rail
    pane = overview.index('<div className="hero-pane">')
    strip = overview.index('className="kpi-grid c4 hero-strip"')
    rail = overview.index('<aside className="hero-rail">')
    assert pane < strip < rail


def test_subminute_ledger_paper_equity_panel_is_deleted():
    """Operator 2026-08-05: the sub-minute tab is the EXECUTION console. Its
    ledger-paper equity/PnL panel (EquityChart + DrawdownChart over
    `lifetimeEquity(live)`) duplicated the Overview hero off the same selector
    and made the paper basis look like a second, independent account.

    The panel, its `eq` binding and the now-unused `lifetimeEquity` import are
    all gone. The chart COMPONENTS are shared and stay exported — Overview is
    their consumer — so nothing in charts.jsx is orphaned."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    sub = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")
    charts = (dash / "charts.jsx").read_text(encoding="utf-8")
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")

    code = _code_only(sub)
    assert "EquityChart" not in code
    assert "DrawdownChart" not in code
    assert "lifetimeEquity" not in code
    assert "lifetimeEquity(live)" not in sub
    assert "'ledger paper PnL'" not in sub
    assert "cash equity · since reset" not in sub

    # the shared components keep a live consumer — do NOT delete them
    assert "window.EquityChart = EquityChart;" in charts
    assert "window.DrawdownChart = DrawdownChart;" in charts
    assert "<EquityChart" in overview and "<DrawdownChart" in overview


def test_strategy_tab_truth_pass_matches_the_deployed_engine():
    """Operator 2026-08-05 TRUTH PASS on Config → Strategy (SubminuteSpecPage).

    The tab carried TWO tables — the contract table and StrategyFlagsPanel —
    which restated each other and, between them, described retired behaviour.
    It is now ONE compact per-band reference in six groups:
    bands · decision spec · sizing · T3 top-ups · execution · halt.

    Engine truth this pins (sources: config.py, execution/held_position_pass.py,
    execution/scale_in.py, snapshot_builder._v2_spec/_strategy_flags_block,
    infra/install_remote.sh):

    * StrategyFlagsPanel DELETED — its duplicate rows (entry FAK pad, min bet,
      hard halt, DD stake throttle) already existed in the contract table; its
      LIVE-only rows moved into the right group.
    * "per resolution: one bet" DELETED — stale since T3: top-ups add FILLS to
      the same position. Replaced by "entries per resolution" (payload-driven
      multifill) with the top-up caveat in the tooltip.
    * T3 top-ups get their OWN group of short rows, including the
      deterioration DISARM band (SCALEIN_T3_DISARM_BAND) — the ex-stop-loss
      trigger, which now disarms top-ups and never sells.
    * late-window skip is a DECISION gate (live at 5s on all four coins), not
      a flags footnote — it is why the entry window ends before close.
    * stop-loss is DELETED: "exits: none" is the only exit row, and no
      stop/chase row survives anywhere on the page.
    * fees are LIVE VENUE fees.
    * every value still reads from the payload — no hardcoded knob numbers."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    sub = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")
    code = _code_only(sub)

    # ── the second table is gone ──
    assert "StrategyFlagsPanel" not in code
    assert "deployed execution flags" not in code
    for dup in (
        "['entry FAK pad',",
        "['min bet',",
        "['DD stake throttle',",
        "['hard halt',",
        "['scale-in T3 top-ups',",
    ):
        assert dup not in sub, f"flags-panel row {dup!r} survived the fold-in"

    # ── six groups, in order ──
    groups = [
        "decision spec — shared",
        "sizing — shared · stake = equity × base frac × DD-brake × concurrency",
        "T3 top-ups — shared · taker adds to a filled position, never an exit",
        "execution — shared",
        "halt — shared",
    ]
    at = -1
    for g in groups:
        nxt = sub.index(f'<GroupRow label="{g}" />')
        assert nxt > at, f"group {g!r} is out of order"
        at = nxt

    # ── stale rows DELETED ──
    # "one bet per resolution" is false under T3 — top-ups add fills.
    assert '<Row k="per resolution" shared="one bet' not in sub
    assert '<Row k="multifill"' not in sub
    assert '<Row k="entries per resolution" shared={multifill}' in sub
    assert "T3 top-ups (below) add to that same position — they are not new entries." in sub
    # the Panel's own hint no longer says "one FILL per resolution" either
    assert "one fill per resolution" not in sub
    assert "one ENTRY per resolution, first-fill-wins. T3 top-ups add to that entry." in sub
    # stop-loss / stop-chase: no row, no prose, anywhere on the tab
    assert "stop chase" not in sub
    assert "stop-loss row" not in sub
    assert '<Row k="exits" shared="none — every filled position HOLDs to binary settlement"' in sub
    assert code.count('k="exits"') == 1

    # ── T3 group: the loop's real contract, payload-driven ──
    assert "const t3 = f && f.scalein_t3;" in sub
    assert '<Row k="status" shared={!t3 ? null : (t3.enabled ? \'on\' : \'off\')}' in sub
    assert "execution/held_position_pass.py" in sub
    assert "shared={`≤ ${t3.max_n} top-ups × ${fmt.pct(t3.frac, 0)} of base`}" in sub
    assert "price ≤ ${fmt.prob(t3.cap, 2)} · spread ≤ ${fmt.prob(t3.spread_max, 2)}" in sub
    assert "every ${t3.cadence_s}s · stops ${t3.end_buffer_s}s before close" in sub
    assert '<Row k="deterioration disarm"' in sub
    assert "bid ≥ model_p + ${fmt.num(t3.disarm_band, 2)} while underwater" in sub
    assert "SCALEIN_T3_DISARM_BAND" in sub
    assert "It never sells — the stop-loss exit was deleted 2026-08-05." in sub
    assert '<Row k="fresh gate" shared={!f ? null : (f.topup_fresh_gate_mode' in sub

    # ── decision spec gained the two live entry gates ──
    assert '<Row k="late-window skip"' in sub
    assert "lwsKeys.map((c) => `${c}: last ${lws[c]}s`)" in sub
    assert '<Row k="ask-band skip"' in sub
    # retries no longer claim the window runs to close
    assert "per-second re-eval until a fill or the entry window ends" in sub
    assert "until a fill or window end" not in sub

    # ── sizing: throttle removed, one short row + formula in the tooltip ──
    assert "const brakeRemoved = z.brake_floor != null && throttleOff(z.brake_floor);" in sub
    assert "'removed — floor ×1.00, full size at every drawdown depth'" in sub
    assert "so the hard halt is the only drawdown control" in sub
    assert '<Row k="DD-brake" shared={brakeRule} mono={false} hint={brakeHint} />' in sub
    assert '<Row k="tier-A upsize"' in sub

    # ── fees are the live venue fees ──
    assert '<Row k="fees" shared="live venue fees — charged and booked on every fill"' in sub
    assert "there is no paper-fee mode" in sub

    # ── halt group ──
    assert '<Row k="hard halt" shared={z.hard_halt_frac != null' in sub
    assert '<Row k="exposure caps" shared={p.shared_exposure_cap_frac != null' in sub

    # ── declutter style: caveats are tooltips, never a second prose line ──
    assert "const Row = ({ k, vals, shared, mono = true, hint }) => (" in sub
    assert "<tr title={hint || undefined}>" in sub
    # the muted second-line prose under DD-brake / pad / per-coin cap is gone
    assert '<br /><span className="muted">brake = max(' not in sub
    assert '<br /><span className="muted">limit = min(walked' not in sub
    assert "hard per-bet cap from the manifest (max_bet_usd_by_coin) · applied" not in sub
    assert 'hint="limit = min(walked, first_limit) + pad · CRYPTOBOT_V2_FAK_PAD"' in sub

    # ── nothing hardcoded: the knob numbers all come off the payload ──
    for hardcoded in ("0.04", "0.83", "1.5x", "×1.5", "0.0465", "BTC:5"):
        assert hardcoded not in code, f"hardcoded knob {hardcoded!r} on the spec page"


def test_strategy_tab_rows_match_config_and_env_pins():
    """The Strategy tab's payload FIELDS must be the ones the producer emits
    and the ones config.py actually defines — a row reading a field nobody
    writes renders a silent '—' forever, which is how the page went stale."""
    builder = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "observability"
        / "snapshot_builder.py"
    ).read_text(encoding="utf-8")
    cfg = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "config.py"
    ).read_text(encoding="utf-8")
    install = (
        _REPO_ROOT / "polymarket" / "infra" / "install_remote.sh"
    ).read_text(encoding="utf-8")

    # every strategy_flags field the tab now reads is emitted by the producer
    for field in (
        '"scalein_t3"', '"disarm_band"', '"topup_fresh_gate_mode"',
        '"late_window_skip"', '"ask_band_skip"', '"tier_a"',
    ):
        assert field in builder, f"the spec tab reads {field}, producer omits it"

    # engine truth behind the rewritten rows
    assert 'SCALEIN_T3_DISARM_BAND: float = _float_env(\n    "CRYPTOBOT_SCALEIN_T3_DISARM_BAND", 0.04\n)' in cfg
    assert 'V2_BRAKE_FLOOR: float = _float_env("CRYPTOBOT_V2_BRAKE_FLOOR", 1.0)' in cfg
    # deployed env pins: T3 armed, throttle removed, late-skip DISARMED
    # (flip RE-ARMED 2026-08-09 ~13:00 UTC: flip trades the FULL window
    # including the final 5s; last-15s gate stays off per operator order)
    assert 'set_env_line CRYPTOBOT_SCALEIN_T3_ENABLED "1"' in install
    assert 'set_env_line CRYPTOBOT_V2_BRAKE_FLOOR "1.0"' in install
    assert 'set_env_line CRYPTOBOT_LATE_WINDOW_SKIP ""' in install
    assert 'set_env_line CRYPTOBOT_ENTRY_LAST_SECS_ONLY "0"' in install
    # The TWAP-60 accounting era is deploy-durable: normal installs must not
    # overwrite the preserved cutover with a historical literal.  Only the
    # explicit RESET_RECORDS branch may advance it.
    assert 'set_env_line CRYPTOBOT_INITIAL_BANKROLL "58.575353"' in install
    assert 'set_env_line CRYPTOBOT_WALLET_START_AT "2026-' not in install
    assert 'set_env_line CRYPTOBOT_WALLET_START_AT "$WALLET_START_NOW"' in install
    # multifill / breaker stay DISARMED one-knob re-arms
    assert 'set_env_line CRYPTOBOT_V2_MULTIFILL_ENABLED "0"' in install

    # the stop-loss exit is gone from the engine, so "exits: none" is true
    assert not (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "execution" / "stop_loss.py"
    ).exists()
    held = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "execution"
        / "held_position_pass.py"
    ).read_text(encoding="utf-8")
    assert "_deterioration_disarm" in held
    assert "SCALEIN_T3_DISARM_BAND" in held


def test_subminute_brake_and_breaker_telemetry_survives_the_declutter():
    """The fourth wave deleted CLUTTER, never a warning path.

    * hard halt: the halt banner still renders on `trading_halted`, and the
      halt THRESHOLD stays permanently visible on Overview's dd rail tile and
      in the Config → Strategy contract table.
    * DD-brake: banner + KPI tile render the live multiplier the moment the
      brake is below full size or resting on a real (non-1.00) floor.
    * fill-rate breaker: its window/threshold reappear as a warn sub whenever
      the breaker is tripped or inside its trip condition."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    overview = (dash / "pages-overview.jsx").read_text(encoding="utf-8")
    sub = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")

    # halt banner intact
    assert "if (z.trading_halted) {" in sub
    assert '<span className="halt-status">hard halt</span>' in sub
    assert "-below-peak halt — all v2 placement paused" in sub
    # halt threshold still on screen elsewhere
    assert "halts at ${fmt.pctPoints(k.max_drawdown_halt_pct, 0)}" in overview
    assert '<Row k="hard halt" shared={z.hard_halt_frac != null' in sub
    # brake warn surfaces
    assert "const status = atFloor ? 'brake at floor' : 'throttling';" in sub
    assert "{status} · dd from peak {fmt.pct(ddFrac)} · floor ×{fmt.num(floor, 2)}" in sub
    assert 'valueClass={sizing.brake_at_floor ? \'neg\' : \'warn\'}' in sub
    # breaker warn surface
    assert "const breakerWarn = fillWarn || !!fr.tripped;" in sub
    assert "sub={breakerWarn" in sub
    assert "breaker ${fr.tripped ? 'TRIPPED' : 'warning'}" in sub
    assert "trips below ${fmt.pct(fr.min_fill_rate, 0)} over ${fr.min_orders" in sub


def test_taker_delay_labels_the_residual_row():
    """Operator 2026-08-06: "change unmeasured gap to be taker delay, n delete
    this 250ms [row]".

    The standalone ↳ taker-delay row (measured/documented variants) is GONE
    from the stage table, and the residual row — the one that makes every
    column sum exactly to the total — carries the "taker delay" label. The
    honesty moved into its tooltip: with no accepted order measured, the row
    is dominated by the venue's documented delay (the shortfall lands in the
    total and therefore here); once orders fill, the delay is measured inside
    `post` and the row shrinks to the uninstrumented tick work. The MEASURED
    accepted-POST view still renders in the accept/reject cohort panel."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    latency = (dash / "pages-latency.jsx").read_text(encoding="utf-8")

    # the old standalone row and its plumbing are gone
    assert "takerRow" not in latency
    assert "taker delay · measured" not in latency
    assert "taker delay · documented" not in latency
    assert "taker_delay_measured_ms" not in _code_only(latency)
    # the residual row wears the label, single render site
    assert '<td className="mono" title={RESIDUAL_TITLE}>taker delay</td>' in latency
    # the tooltip keeps the truth: documented-delay dominance, the shrink, and
    # the value stays payload-driven (never a hardcoded 250)
    assert "uncancellable ${takerDelayMs}ms taker delay" in latency
    assert "this row shrinks to the uninstrumented tick work" in latency
    assert "250" not in _code_only(latency)
    # the measured view survives where it always rendered: the cohort panel
    assert "post round trip · accepted vs rejected" in latency.lower()


def test_taker_delay_shortfall_never_double_counts_a_measured_post():
    """THE invariant behind the total-latency column.

    An accepted POST cannot return before the venue's enforced delay elapsed,
    so wherever accepted orders were measured the delay is ALREADY inside
    `post` → shortfall 0 → the total adds nothing (no double count). A
    reject-only window measures post ~0 and never waited the delay out → the
    shortfall is the whole documented delay, so the total cannot understate the
    wall. A venue documenting no delay (predict.fun) adds nothing, ever."""
    p = _run_client_js(
        """
        const f = cb.takerDelayShortfall;
        console.log(JSON.stringify({
          // measured accepted POST (415ms p50) already contains the 250ms wall
          accepted: f(250, 415),
          // ...and equally at exactly the floor
          atFloor: f(250, 250),
          // reject-only window: stage post aggregates to 0
          rejectOnly: f(250, 0),
          // partially captured
          partial: f(250, 100),
          // no stage value at all
          missing: f(250, null),
          // venue documents no delay
          noVenueDelay: f(null, 0),
          // a garbage stage value counts as NO measurement (fail-safe: the
          // total then still carries the full documented wall)
          nonFinite: f(250, Infinity),
        }));
        """
    )
    assert p == {
        "accepted": 0, "atFloor": 0, "rejectOnly": 250, "partial": 150,
        "missing": 250, "noVenueDelay": 0, "nonFinite": 250,
    }


def test_latency_stage_rows_sum_exactly_to_the_visible_total():
    """Operator 2026-08-05: "when sum up all ms, why not equal total ms?"

    Three reasons, and the table now shows all three instead of leaving the
    operator to add rows that were never addable:

      1. NESTING — `sign` and `post` are measured INSIDE `order` (and the
         taker-delay row is that same POST a third time), so adding them
         double- and triple-counts. Only LATENCY_ADDITIVE_STAGES is summed.
      2. UNMEASURED TICK WORK — only network-touching phases are instrumented,
         so scoring/gates/sizing, SQLite, the post-POST tail and the
         held-position book reads sit in `total` and in no row above it.
      3. PERCENTILE NON-ADDITIVITY — every cell is its own order statistic over
         its own tick population (the order stages see order-bearing ticks
         only), and a p50 of a sum is not a sum of p50s.

    (2) and (3) land in ONE signed residual row, computed per COLUMN against
    that column's own total. THE INVARIANT: Σ(additive rows) + residual ==
    the visible total, exactly, in every column — including when the total
    carries the taker-delay shortfall addend, and including when the residual
    goes negative (which is (3) showing itself; clamping it at zero would break
    the invariant, so it is never clamped)."""
    p = _run_client_js(
        """
        const add = cb.LATENCY_ADDITIVE_STAGES;
        const sum = cb.latencyStageSum, res = cb.latencyResidual;
        // sign/post are INSIDE order; `order`/`sign`/`post` aggregate
        // order-bearing ticks only, the rest every tick.
        const stages = {
          spot: { p50: 10, p95: 40, max: 90 },
          ctx:  { p50: 20, p95: 60, max: 120 },
          gamma:{ p50: 5,  p95: 30, max: 200 },
          book: { p50: 15, p95: 70, max: 300 },
          sign: { p50: 25, p95: 60, max: 150 },
          post: { p50: 400, p95: 700, max: 1200 },
          order:{ p50: 430, p95: 780, max: 1400 },
          total:{ p50: 520, p95: 1000, max: 2500 },
        };
        // The page's own total: measured tick wall + the venue-delay shortfall.
        const shortfall = (col) => cb.takerDelayShortfall(250, stages.post[col]);
        const shown = (col) => stages.total[col] + shortfall(col);
        const check = (col) => {
          const t = shown(col);
          return { total: t, sum: sum(stages, col), residual: res(stages, col, t),
                   tallies: sum(stages, col) + res(stages, col, t) === t };
        };
        // A reject-dominated window: post ~0, so the total carries the WHOLE
        // documented delay and the residual must still reconcile against it.
        const rej = Object.assign({}, stages, { post: { p50: 0, p95: 0, max: 0 } });
        const rejTotal = rej.total.p50 + cb.takerDelayShortfall(250, 0);
        // Components exceeding the same column's total (different populations)
        // ⇒ a NEGATIVE residual, never clamped.
        const tiny = Object.assign({}, stages, { total: { p50: 100, p95: 1, max: 1 } });
        console.log(JSON.stringify({
          additive: add,
          p50: check('p50'), p95: check('p95'), max: check('max'),
          rejectWindow: {
            total: rejTotal, sum: sum(rej, 'p50'),
            residual: res(rej, 'p50', rejTotal),
            tallies: sum(rej, 'p50') + res(rej, 'p50', rejTotal) === rejTotal,
          },
          negative: {
            residual: res(tiny, 'p50', 100),
            tallies: sum(tiny, 'p50') + res(tiny, 'p50', 100) === 100,
          },
          // a stage the payload omits contributes nothing (no NaN leak)
          missingStage: sum({ spot: { p50: 10 } }, 'p50'),
          noTotal: res(stages, 'p50', null),
          // ROUNDING BOUNDARY: the payload carries one decimal and the page
          // renders whole ms, so the page rounds every cell FIRST and sums the
          // rounded figures. Summing the raw decimals instead would reconcile
          // numbers the operator cannot see (here: 10.4+20.4 = 30.8 → 31
          // against a displayed 10+20 = 30).
          rawSum: sum({ spot: { p50: 10.4 }, ctx: { p50: 20.4 } }, 'p50'),
          shownSum: sum({ spot: { p50: 10 }, ctx: { p50: 20 } }, 'p50'),
        }));
        """
    )
    # `sign` and `post` are NOT in the sum set — they live inside `order`.
    assert p["additive"] == ["spot", "ctx", "gamma", "book", "order"]

    # p50: 10+20+5+15+430 = 480; total shown = 520 (+0 shortfall, the 400ms
    # accepted POST already contains the 250ms wall) ⇒ 40ms of unmeasured tick.
    assert p["p50"] == {"total": 520, "sum": 480, "residual": 40, "tallies": True}
    assert p["p95"] == {"total": 1000, "sum": 980, "residual": 20, "tallies": True}
    assert p["max"] == {"total": 2500, "sum": 2110, "residual": 390, "tallies": True}

    # reject-only window: post 0 ⇒ the total gains the full 250ms documented
    # wall (520+250=770) and the residual absorbs it (770−480).
    assert p["rejectWindow"] == {
        "total": 770, "sum": 480, "residual": 290, "tallies": True,
    }

    # a total BELOW the components (different tick populations / percentile
    # non-additivity) yields a negative residual — signed, so the column still
    # adds up exactly.
    assert p["negative"] == {"residual": -380, "tallies": True}

    assert p["missingStage"] == 10
    assert p["noTotal"] is None
    # ...and the raw-decimal sum really is the one the display cannot match.
    assert p["rawSum"] == pytest.approx(30.8)
    assert p["shownSum"] == 30

    # the page renders the residual row and marks the non-additive rows
    latency = (
        _REPO_ROOT / "polymarket" / "dashboard" / "pages-latency.jsx"
    ).read_text(encoding="utf-8")
    # (2026-08-06: the residual row is LABELLED "taker delay" by operator order;
    # arithmetically it is still the residual that makes every column add up.)
    assert '<td className="mono" title={RESIDUAL_TITLE}>taker delay</td>' in latency
    assert "const residual = (col) => latencyResidual(rounded, col, totalShown(col));" in latency
    # every summed figure is the ROUNDED, displayed one
    assert "const totalShown = (col) => (rounded.total ? rounded.total[col] + shortfall(col) : null);" in latency
    assert "const post = rounded.post || null;" in latency
    assert "const s = rounded[name];" in latency
    assert "const nested = !isTotal && LATENCY_ADDITIVE_STAGES.indexOf(name) < 0;" in latency
    # the caveat is stated where the number is, not only in a comment
    assert "a p50 of a sum is not the sum of p50s" in latency
    assert "can go NEGATIVE" in latency


def test_strategy_tab_shows_the_booked_fee_calibration():
    """Operator 2026-08-05 asked why theta reads 0.07 when realized friction
    implies less. The honest answer is POLY_FEE_CALIBRATION (0.864 — booked
    fees are 0.864x the published parabola; the flat-to-flat wallet-tape audit
    measured k*theta = 0.06048, re-based when theta moved 0.072 -> 0.07),
    so it gets its own Strategy-tab row under `taker fee`: payload-driven,
    never hardcoded, with the audit basis in the tooltip."""
    dash = _REPO_ROOT / "polymarket" / "dashboard"
    sub = (dash / "pages-subminute.jsx").read_text(encoding="utf-8")
    builder = (
        _REPO_ROOT / "polymarket" / "src" / "cryptobot" / "observability"
        / "snapshot_builder.py"
    ).read_text(encoding="utf-8")

    assert '<Row k="fee calibration" mono={false}' in sub
    assert "s.fee_calibration != null" in sub
    assert "×{fmt.num(s.fee_calibration, 2)} on BOOKED fees · decision math keeps raw theta" in sub
    # the VALUE is rendered from the payload, never typed into the page (the
    # number appears only inside the tooltip's audit narrative, as prose)
    assert "×0.84" not in sub
    assert "×0.864" not in sub
    assert "POLY_FEE_CALIBRATION" not in _code_only(sub)
    # ordering: the calibration explains the fee row directly above it
    assert sub.index('<Row k="taker fee"') < sub.index('<Row k="fee calibration"')
    # tooltip carries the audit basis + the decision/booking split
    assert "Flat-to-flat wallet-tape audit (2026-07-23" in sub
    assert "DECISION math" in sub
    # producer
    assert '"fee_calibration": _fee_calibration(),' in builder
    assert "def _fee_calibration()" in builder


def test_latency_freshness_panel_wires_the_stall_watchdog():
    """Operator order 2026-08-06: the WS freshness cap is gone (age = market
    quietness on these books), so the ONLY thing standing between us and a
    stalled-but-connected subscription is the out-of-band REST watchdog. The
    book-age tile must therefore say whether that cross-check is armed and
    agreeing — but DECLUTTER: tooltip-only until it has actually evicted."""
    latency = (
        _REPO_ROOT / "polymarket" / "dashboard" / "pages-latency.jsx"
    ).read_text(encoding="utf-8")

    # Reads the server-owned counters; never invents its own verdict.
    assert "const wd = freshness.watchdog || null;" in latency
    assert "const wdArmed = !!(wd && wd.armed);" in latency
    assert "const wdEvictions = wd ? (wd.evictions || 0) : 0;" in latency
    assert "verified · ${fmt.intc(wdEvictions)} evictions" in latency
    # Visible text ONLY once something was evicted; otherwise the tooltip.
    assert "sub={wdEvictions > 0" in latency
    assert "title={`venue-clock age of the WS ladder at decision time — ${wdTip}`}" in latency
    # An unarmed watchdog must say so rather than reading as "0 problems".
    assert "stall watchdog NOT armed" in latency
    # No second panel / KPI row for it — it rides the existing tile.
    assert "WatchdogPanel" not in latency
