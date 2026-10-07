"""U5 v6 sleeve loader tests.

Reads the real `backtest/optimize_runs/v2_step1last_live.json` (the deployed
v2 "step1last" checkpoint-only config — the exp044/ORB minute strategy was
retired 2026-07-09) and verifies the runtime overrides land correctly. Legacy
lut/rf/blend + pairs + 2-wallet_groups back-compat is exercised against the
vendored fixture via `cryptobot.tests._legacy_artifact.legacy_portfolio()`.

The v2 config is checkpoint-only, so `load_sleeves()` requires
`config.SUBMINUTE_ENABLED` to be True (else it fails closed). The autouse
`_enable_subminute` fixture pins it on for the whole module.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cryptobot import config
from cryptobot.strategy.portfolio_loader import V6Portfolio, load_sleeves


@pytest.fixture(autouse=True)
def _enable_subminute(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployed v2 config is checkpoint-only; the loader fails closed
    unless the sub-minute flag is on. Pin it on for every test here."""
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)

# 2026-07-05 backtest-exposure mode (group_live.cap_tracks_equity=true): the
# concurrent-exposure ceiling tracks live equity and the fixed funder floor is
# RETIRED. NOTHING is hardcoded — cap_usd is omitted from the artifact entirely
# (optional under tracking, defaults to 0.0 as a fail-closed fallback), so
# cap_usd / all_group_caps_sum_usd() / the funder floor are all 0.0 and the 35%
# DD halt (trailing, on peak) is the sole stop.
CURRENT_LIVE_GROUP_CAP_USD = 0.0
CURRENT_FUNDER_FLOOR_USD = 0.0


def test_real_artifact_parses_to_two_subminute_sleeves() -> None:
    # The live artifact carries exactly the two sub-minute checkpoint sleeves.
    portfolio = load_sleeves()
    assert isinstance(portfolio, V6Portfolio)
    assert len(portfolio.sleeves) == 2
    labels = [s.label for s in portfolio.sleeves]
    assert labels == ["subminute_5m", "subminute_15m"]


def test_max_walk_price_is_overridden_to_config() -> None:
    portfolio = load_sleeves()
    for sleeve in portfolio.sleeves:
        assert sleeve.max_walk_price == config.MAX_WALK_PRICE == 0.05, (
            f"sleeve {sleeve.label} max_walk_price={sleeve.max_walk_price}"
        )


def test_entry_minute_sets_per_sleeve() -> None:
    # Pin the current sleeve entry sets — v2 step1last checkpoint sleeves.
    portfolio = load_sleeves()
    by_label = {s.label: s for s in portfolio.sleeves}
    assert by_label["subminute_5m"].entry_minute_set == (0, 1, 2, 3, 4)
    assert by_label["subminute_15m"].entry_minute_set == tuple(range(0, 15))


def test_alloc_split() -> None:
    # exp044 runs ONE shared-equity wallet group; each sleeve's `alloc`
    # is its standalone scale (both 1.0 â€” sizing is driven by capital_frac).
    portfolio = load_sleeves()
    allocs = {s.label: s.alloc for s in portfolio.sleeves}
    assert allocs["subminute_5m"] == pytest.approx(1.0)
    assert allocs["subminute_15m"] == pytest.approx(1.0)


def test_signal_sources_match_fraction_sizing_optimization() -> None:
    # v2 checkpoint sleeves (MID-only since 2026-07-21) score via the HGB model.
    portfolio = load_sleeves()
    sources = {s.label: s.signal_source for s in portfolio.sleeves}
    assert sources == {
        "subminute_5m": "hgb",
        "subminute_15m": "hgb",
    }


def test_target_bet_size_derived_from_alloc_capital_frac() -> None:
    """target_bet_size = bankroll * alloc * capital_frac * multiplier.
    Derive expected from the loaded sleeve so the assertion stays
    correct if the artifact's scales ever re-tune."""
    portfolio = load_sleeves()
    for s in portfolio.sleeves:
        expected = config.INITIAL_BANKROLL * s.alloc * s.capital_frac * config.BET_SIZE_MULTIPLIER
        assert s.target_bet_size == pytest.approx(expected, abs=1e-9)


def test_target_bet_size_scales_with_live_capital() -> None:
    portfolio = load_sleeves()
    live_capital = 132.20
    for s in portfolio.sleeves:
        expected = live_capital * s.alloc * s.capital_frac * config.BET_SIZE_MULTIPLIER
        assert s.target_bet_size_for(live_capital) == pytest.approx(expected, abs=1e-9)


def test_v2_four_coins_full_size_no_per_coin_caps() -> None:
    """OPERATOR OVERRIDE 2026-07-21 ("follow backtest, don't look at live
    performance"): ALL FOUR coins at FULL equal size — coin_stake_mult 1.0 for
    BTC/ETH/SOL/XRP and NO per-coin cap (max_bet_usd_by_coin {}; the SOL
    $1-floor probe was removed by operator order). The live sizing_v2 DD-brake
    + concurrency downweight do all the shaping."""
    portfolio = load_sleeves()
    by_label = {s.label: s for s in portfolio.sleeves}

    s5 = by_label["subminute_5m"]
    assert s5.max_bet_usd_by_coin == {}
    assert s5.coin_stake_mult == {"BTC": 1.0, "ETH": 1.0, "SOL": 1.0, "XRP": 1.0}
    # capital_frac 0.010 -> 50_000 * 0.010 = 500 base for EVERY coin
    # (2026-08-09 operator orders: 1% -> 2% -> 1% around the flip re-arm).
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        assert s5.target_bet_size_for(50_000.0, coin=coin) == pytest.approx(500.0)

    s15 = by_label["subminute_15m"]
    assert s15.max_bet_usd_by_coin == {}
    assert s15.coin_stake_mult == {"BTC": 1.0, "ETH": 1.0, "SOL": 1.0, "XRP": 1.0}
    # capital_frac 0.010 -> 50_000 * 0.010 = 500 base for EVERY coin
    # (2026-08-09 operator orders: 1% -> 2% -> 1% around the flip re-arm).
    for coin in ("BTC", "ETH", "SOL", "XRP"):
        assert s15.target_bet_size_for(50_000.0, coin=coin) == pytest.approx(500.0)


def test_all_deployed_sleeves_clear_min_bet_floor() -> None:
    portfolio = load_sleeves()
    sizes = {s.label: s.target_bet_size for s in portfolio.sleeves}
    too_small = {
        label: size
        for label, size in sizes.items()
        if size < config.MIN_BET_USD
    }
    assert too_small == {}, (
        f"configured sleeve target(s) below MIN_BET_USD={config.MIN_BET_USD}: "
        f"{too_small}"
    )


def test_config_sha256_is_64_hex_chars() -> None:
    portfolio = load_sleeves()
    assert len(portfolio.config_sha256) == 64
    int(portfolio.config_sha256, 16)  # valid hex


def test_run_id_recovered() -> None:
    # The loader echoes the json's run_id verbatim.
    portfolio = load_sleeves()
    assert portfolio.run_id == "v2_midonly4_live_2026_07_22"


def test_v2_wallet_risk_fractions() -> None:
    portfolio = load_sleeves()
    # Single shared exposure cap (1.0) applied per-wallet; DD halt 0.30
    # (throttle removal + 1%/1% resize, operator-ordered 2026-07-31 —
    # with the graduated brake gone the halt is the only drawdown guard).
    # The trio (config.py SHARED_EXPOSURE_CAP, the artifact's
    # concurrent_exposure_cap_fraction, and the box env) must all read 1.0.
    assert portfolio.shared_exposure_cap_fraction == pytest.approx(1.0)
    assert portfolio.max_drawdown_halt_frac == pytest.approx(0.30)
    assert portfolio.exposure_cap_fraction_for("subminute_15m") == pytest.approx(1.0)
    assert portfolio.exposure_cap_fraction_for("subminute_5m") == pytest.approx(1.0)


def test_v2_hgb_fields_populated_and_min_edge_inert() -> None:
    """The deployed v2 HGB checkpoint sleeves expose the model-defining fields
    (threshold / feature_groups / ask_bucket_ranges / book_consumption_max)
    while the legacy `min_edge` gate is parked at -1.0 (inert)."""
    portfolio = load_sleeves()
    by_label = {s.label: s for s in portfolio.sleeves}

    s5 = by_label["subminute_5m"]
    assert s5.signal_source == "hgb"
    # v2 pred_edge gate 0.03; ask band 0.65-0.85; 11-set features.
    assert s5.threshold == pytest.approx(0.03)
    assert s5.feature_groups == "rf,price,identity,time"
    assert s5.ask_bucket_ranges == ((0.65, 0.85),)
    assert s5.book_consumption_max == pytest.approx(0.30)
    assert s5.side_filter == "both"
    # min_edge is parked inert at -1.0; HGB gating uses `threshold`.
    assert s5.min_edge == pytest.approx(-1.0)

    s15 = by_label["subminute_15m"]
    assert s15.signal_source == "hgb"
    assert s15.threshold == pytest.approx(0.03)
    assert s15.feature_groups == "rf,price,identity,time"
    assert s15.ask_bucket_ranges == ((0.65, 0.85),)
    assert s15.book_consumption_max == pytest.approx(0.06)
    assert s15.side_filter == "both"
    assert s15.min_edge == pytest.approx(-1.0)


# ---- error cases --------------------------------------------------------


def _write_json(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "art.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def test_sum_alloc_above_one_now_allowed(tmp_path: Path) -> None:
    """All_4 chunk-CV topology: each sleeve's alloc is its standalone
    portfolio scale (v6_5m=0.842, v6_15m=0.676 â†’ sum=1.518). The legacy
    `sum(alloc) > 1.0 â†’ reject` guard is gone; per-sleeve cap stays."""
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [
                {
                    "label": "a",
                    "alloc": 0.842,
                    "config": _minimal_cfg("5m", [4]),
                },
                {
                    "label": "b",
                    "alloc": 0.676,
                    "config": _minimal_cfg("15m", [7]),
                },
            ],
        },
    )
    portfolio = load_sleeves(p)
    allocs = {s.label: s.alloc for s in portfolio.sleeves}
    assert allocs == {"a": pytest.approx(0.842), "b": pytest.approx(0.676)}


def test_per_sleeve_alloc_above_one_rejected(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [
                {
                    "label": "a",
                    "alloc": 1.5,
                    "config": _minimal_cfg("5m", [4]),
                },
            ],
        },
    )
    with pytest.raises(ValueError, match="exceeds 1.0"):
        load_sleeves(p)


def test_coin_stake_mult_weights_notional(tmp_path: Path) -> None:
    """A valid coin_stake_mult scales the per-coin target notional (a weight,
    not a cap); an unweighted coin falls back to 1.0 (uniform)."""
    cfg = _minimal_cfg("5m", [4])
    cfg["capital_frac"] = 0.04
    cfg["coin_stake_mult"] = {"BTC": 0.95, "ETH": 0.85, "XRP": 0.2}
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "a", "alloc": 1.0, "config": cfg}],
        },
    )
    s = load_sleeves(p).sleeves[0]
    assert s.target_bet_size_for(50_000.0, coin="BTC") == pytest.approx(1900.0)
    assert s.target_bet_size_for(50_000.0, coin="XRP") == pytest.approx(400.0)
    assert s.target_bet_size_for(50_000.0, coin="SOL") == pytest.approx(2000.0)


def test_coin_stake_mult_unknown_coin_rejected(tmp_path: Path) -> None:
    """Fail-closed: a typo'd coin key must raise, never silently fall to 1.0."""
    cfg = _minimal_cfg("5m", [4])
    cfg["coin_stake_mult"] = {"BTCC": 0.95}
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "a", "alloc": 1.0, "config": cfg}],
        },
    )
    with pytest.raises(ValueError, match="coin_stake_mult"):
        load_sleeves(p)


def test_missing_required_key_raises(tmp_path: Path) -> None:
    cfg = _minimal_cfg("5m", [4])
    del cfg["min_edge"]
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "x", "alloc": 0.4, "config": cfg}],
        },
    )
    with pytest.raises(KeyError, match="min_edge"):
        load_sleeves(p)


def test_empty_entry_minute_set_raises(tmp_path: Path) -> None:
    cfg = _minimal_cfg("5m", [])
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "x", "alloc": 0.4, "config": cfg}],
        },
    )
    with pytest.raises(ValueError, match="entry_minute_set"):
        load_sleeves(p)


def test_bankroll_drift_raises(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 250.0,  # disagrees with config.BACKTEST_BANKROLL=100
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "x", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
        },
    )
    with pytest.raises(ValueError, match="bankroll"):
        load_sleeves(p)


def test_exposure_cap_drift_raises(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": 0.99,  # disagrees with SHARED_EXPOSURE_CAP=0.30
            "sleeves": [{"label": "x", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
        },
    )
    with pytest.raises(ValueError, match="concurrent_exposure_cap_fraction"):
        load_sleeves(p)


# ============================================================================
# Per-strategy LIVE deploy plan Â§U3 â€” wallet_groups + wallet_default_modes
# ============================================================================


def test_v2_artifact_has_one_wallet_group() -> None:
    """v2 runs ONE shared-equity wallet group (`group_live`) holding both
    checkpoint sleeves at the full live equity."""
    portfolio = load_sleeves()
    assert len(portfolio.wallet_groups) == 1
    group = portfolio.wallet_groups[0]
    assert group.group_id == "group_live"
    # Both sub-minute sleeves share group_live (first-fill-wins arbitration
    # spans every sleeve, and they compound on ONE shared equity).
    assert group.members == ("subminute_5m", "subminute_15m")
    assert group.cap_usd == pytest.approx(CURRENT_LIVE_GROUP_CAP_USD)
    # Backtest-exposure mode: ceiling tracks equity, fixed funder floor retired.
    assert group.cap_tracks_equity is True
    assert portfolio.all_group_caps_sum_usd() == pytest.approx(CURRENT_LIVE_GROUP_CAP_USD)
    assert portfolio.funder_balance_floor_usd() == pytest.approx(CURRENT_FUNDER_FLOOR_USD)
    assert portfolio.wallet_group_for("subminute_5m") == "group_live"
    assert portfolio.wallet_group_for("subminute_15m") == "group_live"
    assert portfolio.group_by_id("group_live") is group
    assert portfolio.group_by_id("group_5m") is None


def test_v2_group_is_shared_equity() -> None:
    """Phase 5: the v2 wallet group is opt-in shared-equity — both sleeves
    resolve to the SAME WalletGroup via shared_equity_group_for."""
    portfolio = load_sleeves()
    group = portfolio.wallet_groups[0]
    assert group.shared_equity is True
    assert portfolio.shared_equity_group_for("subminute_5m") is group
    assert portfolio.shared_equity_group_for("subminute_15m") is group
    assert portfolio.shared_equity_group_for("not_a_wallet") is None


def test_legacy_groups_are_not_shared_equity() -> None:
    """The legacy 2-group artifact (no `shared_equity` key) defaults every
    group to shared_equity=False â€” isolated semantics stay unchanged."""
    from cryptobot.tests._legacy_artifact import legacy_portfolio
    portfolio = legacy_portfolio()
    assert all(g.shared_equity is False for g in portfolio.wallet_groups)
    assert portfolio.shared_equity_group_for("v6_5m") is None
    assert portfolio.shared_equity_group_for("pairs_15m") is None


def test_shared_equity_must_be_bool(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_groups": {
                "group_5m": {
                    "members": ["v6_5m"],
                    "cap_usd": 100.0,
                    "shared_equity": "yes",
                }
            },
        },
    )
    with pytest.raises(ValueError, match="shared_equity"):
        load_sleeves(p)


def test_shared_equity_absent_defaults_false(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_groups": {
                "group_5m": {"members": ["v6_5m"], "cap_usd": 100.0}
            },
        },
    )
    portfolio = load_sleeves(p)
    assert portfolio.wallet_groups[0].shared_equity is False


def test_u10_boot_smoke_v2() -> None:
    """U10: end-to-end smoke. Default config → load v2 artifact →
    build a snapshot payload → verify the operator-facing fields the
    dashboard reads (topology, params, exposure cap, DD halt) are
    populated correctly."""
    from cryptobot.db import init_db
    from cryptobot.observability.snapshot_builder import build_payload

    # No env overrides → loader picks up v2_step1last_live.json by default.
    portfolio = load_sleeves()
    assert portfolio.run_id == "v2_midonly4_live_2026_07_22"
    assert portfolio.config_sha256, "config_sha256 must be set for drift-pinning"
    assert len(portfolio.wallet_groups) == 1
    assert portfolio.shared_exposure_cap_fraction == pytest.approx(1.0)
    assert portfolio.max_drawdown_halt_frac == pytest.approx(0.30)

    conn = init_db(":memory:")
    try:
        payload, _ = build_payload(conn, portfolio)
        k = payload["kpis"]
        # One wallet_group → the dashboard renders the group rollup view.
        assert k["topology"] == "wallet_groups"
        assert k["max_drawdown_halt_pct"] == 30.0
        assert len(k["wallets"]) == 2
        labels = sorted(w["id"] for w in k["wallets"])
        assert labels == ["subminute_15m", "subminute_5m"]
        for w in k["wallets"]:
            # Runtime mode comes from the DB wallet_mode_state, which is empty
            # on a fresh in-memory DB → DRY fallback (D-LIVE-5: DRY always
            # allowed), independent of the artifact's LIVE default.
            assert w["mode"] == "DRY"
            assert w["exposure_cap_frac"] == pytest.approx(1.0)
            assert w["wallet_group"] == "group_live"
    finally:
        conn.close()


def test_u10_soft_revert_smoke() -> None:
    """U10: soft-revert smoke. The operator's no-redeploy revert path
    (config.py + cryptobot.env.example env trio) is captured by
    `legacy_portfolio()`; this test pins that it succeeds end-to-end."""
    from cryptobot.tests._legacy_artifact import legacy_portfolio
    portfolio = legacy_portfolio()
    assert portfolio.run_id == "per_strategy_compound_dd35_live"
    assert portfolio.shared_exposure_cap_fraction == pytest.approx(0.28767857660247903)
    assert portfolio.max_drawdown_halt_frac == pytest.approx(0.5)
    assert len(portfolio.wallet_groups) == 2


def test_v2_group_paths_populated() -> None:
    """U7: with one wallet_group the group-aware code paths must return
    populated values. Lock the integration smoke so a future refactor
    doesn't regress."""
    from cryptobot.db import init_db
    from cryptobot.db.connection import backfill_wallet_groups
    from cryptobot.execution.capital import get_group_state
    from cryptobot.execution.live_readiness import _required_balance

    portfolio = load_sleeves()
    conn = init_db(":memory:")
    try:
        # backfill_wallet_groups: empty in-memory DB has no legacy rows to
        # restamp, so the row counts are 0 even though the wallet→group
        # mapping is active.
        counts = backfill_wallet_groups(conn, portfolio)
        assert counts == {"ledger": 0, "signals": 0}

        # _required_balance mirrors funder_balance_floor_usd(); under
        # backtest-exposure mode (cap_tracks_equity) that floor is RETIRED (0),
        # so readiness passes at any balance and 35% DD is the sole stop.
        assert _required_balance(portfolio) == portfolio.funder_balance_floor_usd()
        assert _required_balance(portfolio) == pytest.approx(CURRENT_FUNDER_FLOOR_USD)

        # get_group_state (the LIVE group-exposure gate's source): one
        # populated entry for group_live. (The dashboard's _wallet_groups_block
        # panel was retired 2026-07-14 — the gate path is the surviving
        # group-aware consumer.)
        state = get_group_state(conn, portfolio)
        assert set(state) == {"group_live"}
        grp = state["group_live"]
        assert grp.cap_usd == pytest.approx(CURRENT_LIVE_GROUP_CAP_USD)
        assert sorted(grp.member_wallets) == [
            "subminute_15m", "subminute_5m",
        ]
        assert grp.pending_usd == 0.0
    finally:
        conn.close()


def test_v2_artifact_wallet_default_modes_all_live() -> None:
    """MID-only amendment 2026-07-21: both remaining MID sleeves default LIVE
    (the nl_* sleeves were removed from the artifact entirely)."""
    portfolio = load_sleeves()
    assert portfolio.wallet_default_modes == {
        "subminute_5m": "LIVE",
        "subminute_15m": "LIVE",
    }


def test_default_mode_for_unknown_wallet_falls_back_to_dry() -> None:
    """Defaults to 'DRY' on unknown wallets â€” safest possible default
    (matches D-LIVE-5 asymmetry â€” DRY is always allowed)."""
    portfolio = load_sleeves()
    assert portfolio.default_mode_for("not_a_real_wallet") == "DRY"


def test_legacy_2_group_artifact_still_parses() -> None:
    """The legacy artifact must still parse so the soft-revert procedure
    (env trio â†’ `dry_run_recommended.json`) works. Verifies group_5m /
    group_15m membership and the $200 group-cap sum."""
    from cryptobot.tests._legacy_artifact import legacy_portfolio
    portfolio = legacy_portfolio()
    assert len(portfolio.wallet_groups) == 2
    by_id = {g.group_id: g for g in portfolio.wallet_groups}
    assert by_id["group_5m"].members == ("v6_5m", "pairs_5m")
    assert by_id["group_15m"].members == ("v6_15m", "pairs_15m")
    assert portfolio.wallet_group_for("v6_5m") == "group_5m"
    assert portfolio.all_group_caps_sum_usd() == 200.0


def test_legacy_artifact_without_groups_parses_empty(tmp_path: Path) -> None:
    """Backwards-compat: pre-U3 artifacts have no `wallet_groups` field â€”
    must parse cleanly with `wallet_groups=()` so the group cap gate
    silently no-ops."""
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
        },
    )
    portfolio = load_sleeves(p)
    assert portfolio.wallet_groups == ()
    assert portfolio.wallet_default_modes == {}
    assert portfolio.wallet_group_for("v6_5m") is None


def test_unknown_wallet_in_group_raises(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_groups": {
                "group_5m": {"members": ["v6_99m"], "cap_usd": 100.0}
            },
        },
    )
    with pytest.raises(ValueError, match="unknown wallet id"):
        load_sleeves(p)


def test_wallet_in_two_groups_raises(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [
                {"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])},
                {"label": "v6_15m", "alloc": 0.6, "config": _minimal_cfg("15m", [7])},
            ],
            "wallet_groups": {
                "group_A": {"members": ["v6_5m"], "cap_usd": 100.0},
                "group_B": {"members": ["v6_5m", "v6_15m"], "cap_usd": 100.0},
            },
        },
    )
    with pytest.raises(ValueError, match="more than one wallet_group"):
        load_sleeves(p)


def test_cap_usd_must_be_positive(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_groups": {
                "group_5m": {"members": ["v6_5m"], "cap_usd": 0.0}
            },
        },
    )
    with pytest.raises(ValueError, match="cap_usd"):
        load_sleeves(p)


def test_unknown_wallet_in_default_modes_raises(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_default_modes": {"v6_99m": "DRY"},
        },
    )
    with pytest.raises(ValueError, match="unknown wallet id"):
        load_sleeves(p)


def test_invalid_default_mode_value_raises(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_default_modes": {"v6_5m": "PRODUCTION"},
        },
    )
    with pytest.raises(ValueError, match="DRY|LIVE"):
        load_sleeves(p)


def test_default_modes_normalize_case(tmp_path: Path) -> None:
    """`'live'` / `'dry'` are accepted and normalized to uppercase."""
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_default_modes": {"v6_5m": "live"},
        },
    )
    portfolio = load_sleeves(p)
    assert portfolio.wallet_default_modes["v6_5m"] == "LIVE"


def test_group_members_must_be_non_empty_list(tmp_path: Path) -> None:
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
            "wallet_groups": {
                "group_5m": {"members": [], "cap_usd": 100.0}
            },
        },
    )
    with pytest.raises(ValueError, match="members"):
        load_sleeves(p)


def _minimal_cfg(interval: str, entry: list[int]) -> dict:
    """The smallest sleeve config the loader accepts.

    2026-07-31: "hgb" is the ONLY servable signal_source (the lut|rf|blend
    scorer was retired with the pair sleeves), and the loader requires the
    three model-defining keys of every sleeve, so they are part of the minimum.
    """
    return {
        "interval": interval,
        "side": "model_signed",
        "strategy_archetype": "mid",
        "entry_minute_set": entry,
        "entry_strategy": "first_qualifying",
        "min_edge": 0.10,
        "max_edge": 0.70,
        "min_fill_price": 0.01,
        "max_fill_price": 0.95,
        "hour_bucket_set": "all",
        "dow_set": "all",
        "min_agreement": 1,
        "capital_frac": 0.10,
        "max_walk_price": 0.20,
        "dd_halve_at": None,
        "slippage_haircut": 0.05,
        "min_lut_n": 50,
        "signal_source": "hgb",
        "threshold": 0.05,
        "feature_groups": "rf,price,identity,time",
        "ask_bucket": "all:0:1",
    }


def _minimal_hgb_cfg(interval: str, entry: list[int]) -> dict:
    """`_minimal_cfg` plus the optional hgb knobs and an inert min_edge."""
    cfg = _minimal_cfg(interval, entry)
    cfg.update(
        {
            "min_edge": -1.0,
            "book_consumption_max": 0.15,
            "side_filter": "both",
        }
    )
    return cfg


# ---------------------------------------------------------------------------
# Chunk-CV field coverage (U1 of the all_4 deploy plan).
# ---------------------------------------------------------------------------


def test_partial_ret_z_floor_out_of_range_rejected(tmp_path: Path) -> None:
    cfg = _minimal_cfg("5m", [4])
    cfg["partial_ret_z_floor"] = -50.0
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "x", "alloc": 0.4, "config": cfg}],
        },
    )
    with pytest.raises(ValueError, match="below floor"):
        load_sleeves(p)


# ---------------------------------------------------------------------------
# exp044 HGB-sleeve loader coverage.
# ---------------------------------------------------------------------------


def _hgb_artifact(tmp_path: Path, cfg: dict) -> Path:
    return _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "exp044_5m", "alloc": 1.0, "config": cfg}],
        },
    )


def test_hgb_sleeve_parses_with_full_fields(tmp_path: Path) -> None:
    """A well-formed hgb sleeve parses and surfaces the model-defining
    fields; min_edge stays inert at -1.0."""
    p = _hgb_artifact(tmp_path, _minimal_hgb_cfg("5m", [1, 2, 3, 4]))
    portfolio = load_sleeves(p)
    s = portfolio.sleeves[0]
    assert s.signal_source == "hgb"
    assert s.threshold == pytest.approx(0.05)
    assert s.feature_groups == "rf,price,identity,time"
    assert s.ask_bucket_ranges == ((0.0, 1.0),)
    assert s.book_consumption_max == pytest.approx(0.15)
    assert s.min_edge == pytest.approx(-1.0)


def test_hgb_sleeve_parses_max_bet_usd_by_coin(tmp_path: Path) -> None:
    cfg = _minimal_hgb_cfg("5m", [1, 2, 3, 4])
    cfg["max_bet_usd_by_coin"] = {"BTC": 400, "ETH": 150}
    p = _hgb_artifact(tmp_path, cfg)
    portfolio = load_sleeves(p)
    s = portfolio.sleeves[0]
    assert s.max_bet_usd_by_coin == {"BTC": 400.0, "ETH": 150.0}
    assert s.target_bet_size_for(10_000.0, coin="BTC") == pytest.approx(400.0)
    assert s.target_bet_size_for(10_000.0, coin="ETH") == pytest.approx(150.0)


def test_hgb_sleeve_rejects_unknown_max_bet_coin(tmp_path: Path) -> None:
    cfg = _minimal_hgb_cfg("5m", [1, 2, 3, 4])
    cfg["max_bet_usd_by_coin"] = {"DOGE": 50}
    with pytest.raises(ValueError, match="unknown coin"):
        load_sleeves(_hgb_artifact(tmp_path, cfg))


def test_hgb_requires_feature_groups(tmp_path: Path) -> None:
    cfg = _minimal_hgb_cfg("5m", [4])
    del cfg["feature_groups"]
    with pytest.raises(ValueError, match="feature_groups"):
        load_sleeves(_hgb_artifact(tmp_path, cfg))


def test_hgb_requires_threshold(tmp_path: Path) -> None:
    cfg = _minimal_hgb_cfg("5m", [4])
    del cfg["threshold"]
    with pytest.raises(ValueError, match="threshold"):
        load_sleeves(_hgb_artifact(tmp_path, cfg))


def test_hgb_requires_ask_bucket(tmp_path: Path) -> None:
    cfg = _minimal_hgb_cfg("5m", [4])
    del cfg["ask_bucket"]
    with pytest.raises(ValueError, match="ask_bucket"):
        load_sleeves(_hgb_artifact(tmp_path, cfg))


def test_hgb_book_consumption_max_is_optional_for_edge_preserving_topups(tmp_path: Path) -> None:
    cfg = _minimal_hgb_cfg("5m", [4])
    del cfg["book_consumption_max"]
    s = load_sleeves(_hgb_artifact(tmp_path, cfg)).sleeves[0]
    assert s.book_consumption_max is None


def test_legacy_lut_sleeve_is_rejected(tmp_path: Path) -> None:
    """2026-07-31: the lut|rf|blend scorer went with the pair sleeves, so a
    legacy sleeve must FAIL AT BOOT rather than parse into something nothing
    can score."""
    for source in ("lut", "rf", "blend"):
        cfg = _minimal_cfg("5m", [4])
        cfg["signal_source"] = source
        p = _write_json(
            tmp_path,
            {
                "bankroll": 100.0,
                "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
                "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": cfg}],
            },
        )
        with pytest.raises(ValueError, match="signal_source"):
            load_sleeves(p)


def test_optional_hgb_fields_default_inert(tmp_path: Path) -> None:
    """A sleeve carrying only the REQUIRED model keys still loads, with every
    optional hgb field defaulting to its inert value."""
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}],
        },
    )
    s = load_sleeves(p).sleeves[0]
    assert s.signal_source == "hgb"
    assert s.book_consumption_max is None
    assert s.side_filter == "both"
    assert s.max_bet_usd is None


def test_signal_source_hgb_accepted(tmp_path: Path) -> None:
    p = _hgb_artifact(tmp_path, _minimal_hgb_cfg("5m", [4]))
    assert load_sleeves(p).sleeves[0].signal_source == "hgb"


def test_invalid_signal_source_raises(tmp_path: Path) -> None:
    cfg = _minimal_cfg("5m", [4])
    cfg["signal_source"] = "xgboost"
    p = _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [{"label": "v6_5m", "alloc": 0.4, "config": cfg}],
        },
    )
    with pytest.raises(ValueError, match="signal_source"):
        load_sleeves(p)
# ---------------------------------------------------------------------------
# V2_EDGE_CAP loader clamp (registered candidate 2026-07-22).
# ---------------------------------------------------------------------------


def _edge_cap_artifact(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path,
        {
            "bankroll": 100.0,
            "concurrent_exposure_cap_fraction": config.SHARED_EXPOSURE_CAP,
            "sleeves": [
                {"label": "v6_5m", "alloc": 0.4, "config": _minimal_cfg("5m", [4])}
            ],
        },
    )


