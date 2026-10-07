"""The forward-OOS arbiter's SELECTION — the half that went silently blind.

Between the sleeve rename (live `signals.sleeve` = `subminute_5m` /
`subminute_15m`) and 2026-08-01 the harness still queried
`sleeve IN ('exp044_5m','exp044_15m')`. Every weekly run loaded zero rows,
printed "no signals in slice", wrote nothing and exited 0 — a passing button
that graded nothing for months.

So the tests here are about *selection reaching rows*, not about the decision
rule (which is pre-registered and deliberately untouched): the live labels must
load, the legacy labels must still load identically, and an UNKNOWN label must
fail loudly rather than look like a quiet week.
"""

from __future__ import annotations

import sqlite3

import pytest

from polymarket.backtest.lib import forward_oos_regrade as fr

# One 5m window that clears subminute_live_20260803's 5m gate
# (thr=0.03, ask in [0.65, 0.85), em 0-4): BTC ask 0.70 / edge 0.090 passes;
# ETH ask 0.55 is below the band AND edge 0.010 is under the floor, so it does not.
ROWS = [
    ("2026-07-31 12:00:30", "BTC", 4, "YES", 1785000000, 0.80, 0.70, 0.090),
    ("2026-07-31 12:00:30", "ETH", 4, "NO", 1785000000, 0.60, 0.55, 0.010),
]


def _db(path, sleeve: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE signals (id INTEGER PRIMARY KEY, created_at TEXT, coin TEXT,"
        " sleeve TEXT, entry_minute INT, side TEXT, window_start_ts INT,"
        " model_p REAL, ask REAL, edge REAL)")
    conn.executemany(
        "INSERT INTO signals (created_at, coin, sleeve, entry_minute, side,"
        " window_start_ts, model_p, ask, edge) VALUES (?,?,?,?,?,?,?,?,?)",
        [(r[0], r[1], sleeve, *r[2:]) for r in ROWS])
    conn.commit()
    conn.close()


def test_the_live_sleeve_label_actually_selects_rows(tmp_path):
    """The regression: `subminute_5m` is what the box writes."""
    path = tmp_path / "snap.db"
    _db(path, "subminute_5m")
    rows = fr._load_signals(path)
    assert len(rows) == 2


def test_selection_canonicalizes_to_the_frozen_config_key(tmp_path):
    """CONFIGS are keyed on the LIVE sleeve names since 2026-08-03 (the legacy
    exp044-era configs were deleted), so canonicalization is now an identity —
    but ``sleeve_live`` must still be carried so a run can say what the box wrote."""
    path = tmp_path / "snap.db"
    _db(path, "subminute_15m")
    row = fr._load_signals(path)[0]
    assert row["sleeve"] == "subminute_15m"
    assert row["sleeve_live"] == "subminute_15m"


def test_the_legacy_label_grades_identically(tmp_path):
    """An older cb_export snapshot must not change verdict under the alias."""
    live, legacy = tmp_path / "live.db", tmp_path / "legacy.db"
    _db(live, "subminute_5m")
    _db(legacy, "exp044_5m")
    strip = lambda rs: [{k: v for k, v in r.items() if k != "sleeve_live"} for r in rs]
    assert strip(fr._load_signals(live)) == strip(fr._load_signals(legacy))


def test_the_slice_bounds_still_bind(tmp_path):
    path = tmp_path / "snap.db"
    _db(path, "subminute_5m")
    assert fr._load_signals(path, "2026-07-31 12:00:00", "2026-07-31 12:01:00")
    assert not fr._load_signals(path, "2026-08-05 00:00:00", None)


def test_sleeves_in_slice_reports_the_raw_labels(tmp_path):
    path = tmp_path / "snap.db"
    _db(path, "subminute_5m")
    assert fr.sleeves_in_slice(path) == {"subminute_5m": 2}


def test_an_unmapped_sleeve_fails_loudly_instead_of_grading_nothing(tmp_path, capsys):
    """The next rename must break the button, not silence it."""
    path = tmp_path / "snap.db"
    _db(path, "kalshi_1h")
    code = fr.main(["--signal-db", str(path),
                    "--ledger", str(tmp_path / "ledger.json"),
                    "--res-cache", str(tmp_path / "res.sqlite"), "--no-fetch"])
    out = capsys.readouterr().out
    assert code == 2
    assert "kalshi_1h" in out and "renamed" in out
    assert not (tmp_path / "ledger.json").exists()   # never a partial write


def test_a_genuinely_empty_slice_is_still_a_quiet_zero(tmp_path):
    path = tmp_path / "snap.db"
    _db(path, "subminute_5m")
    assert fr.main(["--signal-db", str(path), "--since", "2026-09-01",
                    "--ledger", str(tmp_path / "ledger.json"),
                    "--res-cache", str(tmp_path / "res.sqlite"), "--no-fetch"]) == 0


def test_end_to_end_grade_writes_a_week_to_the_ledger(tmp_path, monkeypatch):
    """Selection -> settlement -> ledger, with settlement stubbed. The decision
    rule and the cash_pnl settlement math are NOT re-asserted here; they are
    frozen and covered by their own suites."""
    path = tmp_path / "snap.db"
    _db(path, "subminute_5m")
    monkeypatch.setattr(fr.Settler, "won_up", lambda self, slug: 1)
    ledger = tmp_path / "ledger.json"
    assert fr.main(["--signal-db", str(path), "--ledger", str(ledger),
                    "--res-cache", str(tmp_path / "res.sqlite"), "--no-fetch"]) == 0
    import json

    weeks = json.loads(ledger.read_text(encoding="utf-8"))["weeks"]
    assert len(weeks) == 1
    week = next(iter(weeks.values()))
    assert week["subminute_live_20260803"]["5m"]["n"] == 1  # BTC clears, ETH does not
    assert week["subminute_live_20260803"]["unresolved"] == 0
    # The legacy exp044/kronos/persec configs were deleted 2026-08-03; the
    # arbiter must grade the live gate and nothing else.
    assert set(week) == {"subminute_live_20260803"}


@pytest.mark.parametrize("live,canon", sorted(fr.LIVE_SLEEVE_ALIASES.items()))
def test_every_alias_target_is_a_gradable_sleeve(live, canon):
    assert canon in fr.SLEEVE_INTERVAL
