"""The venue fee DISPATCHER, and the two strictness levels it must keep apart.

Phase 2 moved these out of the predict.fun package (the old
``predictfun/fees.py`` held both venues' math AND the dispatcher). The shapes
themselves are tested inside their own venue packages.

The distinction pinned here is the one that has real money on both sides:

* **BOOT / DECISION — fail-closed.** A venue we cannot price must raise, because
  a wrong fee shape on the live path is an edge error. Before Phase 2 an
  unrecognized ``CRYPTOBOT_VENUE`` merely warned and traded Polymarket.
* **GRADE-TIME / ROW — permissive.** The ledger's ``venue`` column is
  ``NOT NULL DEFAULT 'polymarket'`` over years of mixed history; an unknown
  value there must grade as polymarket, because a raise strands rows PENDING
  and wedges the settlement sweep.

Note ``kalshi`` changed roles in Phase 2. It used to be this suite's
*unrecognized* sentinel; it is now a REGISTERED venue with a stub adapter, so
the unknown-venue cases use a genuinely unregistered string and kalshi gets its
own registered-but-unimplemented cases.
"""

from __future__ import annotations

import pytest

from cryptobot.venues import fees

# A string that is not — and should never become — a registry key.
UNREGISTERED = "no-such-venue"


# ---------------------------------------------------------------------------
# DECISION path — fail-closed
# ---------------------------------------------------------------------------


def test_dispatch_by_venue():
    assert fees.fee_per_share(0.80, venue="polymarket", theta=0.072) == pytest.approx(
        0.072 * 0.80 * 0.20
    )
    assert fees.fee_per_share(
        0.80, venue="predictfun", fee_rate_bps=200.0
    ) == pytest.approx(0.02 * 0.20)


def test_dispatch_predictfun_defaults_bps_when_missing():
    from cryptobot import config
    from cryptobot_predictfun.fees import predictfun_fee_per_share

    got = fees.fee_per_share(0.70, venue="predictfun")
    expected = predictfun_fee_per_share(
        0.70, float(config.PREDICTFUN_DEFAULT_FEE_RATE_BPS)
    )
    assert got == pytest.approx(expected)


def test_tent_and_parabola_differ_for_a_favorite():
    """The reason this is a dispatched FUNCTION and not a swapped constant.

    A cross-venue comparison lives in core: it is an assertion about the SEAM,
    and a venue package asserting it would have to import the other venue —
    which tools/importcheck.py forbids for good reason.
    """
    from cryptobot_polymarket.fees import poly_fee_per_share
    from cryptobot_predictfun.fees import predictfun_fee_per_share

    p = 0.80
    tent = predictfun_fee_per_share(p, 200.0)   # 0.004
    parabola = poly_fee_per_share(p, 0.072)     # 0.01152
    assert tent != pytest.approx(parabola)
    assert tent < parabola
    # ...and the dispatcher reproduces each one exactly.
    assert fees.fee_per_share(p, venue="predictfun", fee_rate_bps=200.0) == tent
    assert fees.fee_per_share(p, venue="polymarket", theta=0.072) == parabola


def test_dispatch_polymarket_requires_theta():
    with pytest.raises(ValueError):
        fees.fee_per_share(0.80, venue="polymarket")


def test_dispatch_unregistered_venue_fails_closed():
    # UnknownVenueError subclasses ValueError deliberately, so the pre-Phase-2
    # `except ValueError` guards at display call sites keep working.
    from cryptobot.venues import UnknownVenueError

    with pytest.raises(UnknownVenueError):
        fees.fee_per_share(0.80, venue=UNREGISTERED, theta=0.072)
    with pytest.raises(ValueError):
        fees.fee_per_share(0.80, venue=UNREGISTERED, theta=0.072)


def test_dispatch_registered_but_unimplemented_venue_fails_loud():
    # kalshi is in the registry so the map is total, but its adapter is a stub.
    # It must raise rather than quietly price as something else.
    with pytest.raises(NotImplementedError):
        fees.fee_per_share(0.80, venue="kalshi", theta=0.072)


def test_walk_fee_fn_none_on_polymarket_callable_on_predictfun():
    assert fees.walk_fee_fn("polymarket") is None
    fn = fees.walk_fee_fn("predictfun", fee_rate_bps=200.0)
    assert callable(fn)
    assert fn(0.80) == pytest.approx(0.02 * 0.20)


def test_walk_fee_fn_default_bps():
    from cryptobot import config
    from cryptobot_predictfun.fees import predictfun_fee_per_share

    fn = fees.walk_fee_fn("predictfun")
    assert fn(0.70) == pytest.approx(
        predictfun_fee_per_share(0.70, float(config.PREDICTFUN_DEFAULT_FEE_RATE_BPS))
    )


# ---------------------------------------------------------------------------
# GRADE-TIME path — row-permissive
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue", [None, "", "kalshi", UNREGISTERED, "KALSHI  "])
def test_row_fee_falls_back_to_polymarket_and_never_raises(venue):
    # Every venue we cannot price grades under the parabola, matching the
    # ledger column's NOT NULL DEFAULT. Includes the REGISTERED-but-stub venue:
    # a historic row saying 'kalshi' must still settle.
    expected = 0.072 * 0.70 * 0.30
    assert fees.row_fee_per_share(
        0.70, venue=venue, theta=0.072
    ) == pytest.approx(expected)


def test_row_fee_uses_the_tent_for_a_predictfun_row():
    assert fees.row_fee_per_share(
        0.80, venue="predictfun", fee_rate_bps=200.0, theta=0.072
    ) == pytest.approx(0.02 * 0.20)


def test_row_fee_is_case_and_whitespace_insensitive():
    assert fees.row_fee_per_share(
        0.80, venue="  PredictFun ", fee_rate_bps=200.0, theta=0.072
    ) == pytest.approx(0.02 * 0.20)


def test_row_fee_for_an_unimportable_known_venue_grades_and_logs_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
):
    """A venue we KNOW how to price whose package will not load on this box.

    The fallback must survive — grading never crashes — but it must not be
    silent: the row books an UNCALIBRATED polymarket parabola, because
    grade_fees.entry_fee_usd keys POLY_FEE_CALIBRATION on the ROW's venue
    string ('predictfun' => k withheld) while the SHAPE it got back is the
    parabola. Wrong curve and wrong k, with no other signal in the P&L surface.
    """
    from cryptobot.venues import registry

    monkeypatch.setitem(
        registry.VENUE_PACKAGES, "predictfun", "cryptobot_predictfun_not_here",
    )
    registry._reset_cache_for_tests()
    # Fresh seen-set: the once-per-venue latch is module state.
    monkeypatch.setattr(fees, "_errored_unimportable", set())

    with caplog.at_level("ERROR", logger=fees.logger.name):
        got = fees.row_fee_per_share(
            0.70, venue="predictfun", fee_rate_bps=200.0, theta=0.072
        )

    # Still graded — under the parabola, not the tent.
    assert got == pytest.approx(0.072 * 0.70 * 0.30)
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "predictfun" in errors[0].getMessage()
    assert "POLY_FEE_CALIBRATION" in errors[0].getMessage()

    # ...and only ONCE per venue name.
    caplog.clear()
    with caplog.at_level("ERROR", logger=fees.logger.name):
        fees.row_fee_per_share(0.70, venue="predictfun", theta=0.072)
    assert [r for r in caplog.records if r.levelname == "ERROR"] == []


def test_entry_fee_usd_books_the_uncalibrated_parabola_when_pf_is_unimportable(
    monkeypatch: pytest.MonkeyPatch,
):
    """The consequence the ERROR is warning about, pinned as behavior.

    entry_fee_usd keys on the ROW string, so an unimportable predictfun row
    takes the `key == VENUE_PREDICTFUN` branch (no POLY_FEE_CALIBRATION) over a
    shape that is really the polymarket parabola.
    """
    from cryptobot import config
    from cryptobot.execution.grade_fees import entry_fee_usd
    from cryptobot.venues import registry

    monkeypatch.setitem(
        registry.VENUE_PACKAGES, "predictfun", "cryptobot_predictfun_not_here",
    )
    registry._reset_cache_for_tests()
    monkeypatch.setattr(fees, "_errored_unimportable", set())

    got = entry_fee_usd(
        venue="predictfun", entry_price=0.70, shares=10.0, fee_rate_bps=200.0
    )
    parabola = float(config.POLY_FEE_THETA) * 0.70 * 0.30 * 10.0
    tent = 0.02 * 0.30 * 10.0
    assert got == pytest.approx(parabola)      # wrong shape...
    assert got != pytest.approx(tent)
    # ...and k withheld, i.e. NOT the calibrated polymarket booking either.
    assert got != pytest.approx(float(config.POLY_FEE_CALIBRATION) * parabola)


def test_row_fee_RAISES_for_our_OWN_venue_when_its_adapter_is_broken(
    monkeypatch: pytest.MonkeyPatch,
):
    """The one carve-out in the permissive grade-time path (2026-08-01).

    The fallback's whole justification is CROSS-VENUE LEGACY history: a
    Polymarket box holding old predict.fun rows must keep settling rather than
    wedge the sweep. Applied to the venue this process actually TRADES, the
    same fallback books poly-parabola fiction against live money — silently, on
    every row, forever, with the calibration exemption also withheld. A
    predict.fun box whose own adapter will not import is mis-provisioned, and
    must fail loudly at the first graded row.
    """
    from cryptobot import config
    from cryptobot.venues import registry

    monkeypatch.setitem(
        registry.VENUE_PACKAGES, "predictfun", "cryptobot_predictfun_not_here",
    )
    registry._reset_cache_for_tests()
    monkeypatch.setattr(fees, "_errored_unimportable", set())

    # (a) CROSS-VENUE (this process is polymarket): permissive, as before.
    monkeypatch.setattr(config, "VENUE", "polymarket")
    assert fees.row_fee_per_share(
        0.70, venue="predictfun", fee_rate_bps=200.0, theta=0.072
    ) == pytest.approx(0.072 * 0.70 * 0.30)

    # (b) OWN venue: raises. The ERROR is still emitted first (the operator
    # needs the diagnosis, not just the traceback).
    monkeypatch.setattr(config, "VENUE", "predictfun")
    monkeypatch.setattr(fees, "_errored_unimportable", set())
    with pytest.raises(ImportError):
        fees.row_fee_per_share(
            0.70, venue="predictfun", fee_rate_bps=200.0, theta=0.072
        )

    # (c) A polymarket row on a predict.fun box is still the fallback path (it
    # is not "our own venue"), so cross-venue history keeps settling.
    assert fees.row_fee_per_share(
        0.70, venue="polymarket", theta=0.072
    ) == pytest.approx(0.072 * 0.70 * 0.30)

    registry._reset_cache_for_tests()


# ---------------------------------------------------------------------------
# ENTRY-GATE dispatch — strategy.score._fee_adjusted_edge
# ---------------------------------------------------------------------------


def test_entry_gate_edge_charges_the_venue_fee_not_always_the_parabola():
    """The 07-29 guardrail — FEES ARE PER-VENUE — applied to the ENTRY GATE.

    Until 2026-08-01 ``_fee_adjusted_edge`` charged the Polymarket parabola on
    every venue, so the predict.fun gate priced a fee predict.fun does not
    levy. The walker already dispatched (``walk_fee_fn``); the gate did not, so
    the two disagreed about the same order.

    At the favourite prices these sleeves actually bet (ask 0.75), the parabola
    is ~2.7x the tent — enough to reject entries that clear the threshold.
    """
    from cryptobot.strategy.score import _fee_adjusted_edge

    model_p, ask = 0.80, 0.75
    poly_fee = 0.07 * ask * (1.0 - ask)           # 0.013125
    pf_fee = (200.0 / 10000.0) * min(ask, 1.0 - ask)  # 0.0050
    assert poly_fee == pytest.approx(0.013125)
    assert pf_fee == pytest.approx(0.0050)

    poly_edge = _fee_adjusted_edge(model_p, ask, venue="polymarket")
    pf_edge = _fee_adjusted_edge(
        model_p, ask, venue="predictfun", fee_rate_bps=200.0
    )
    # Polymarket is BYTE-IDENTICAL to the pre-dispatch inline formula.
    assert poly_edge == pytest.approx(model_p - ask - poly_fee, abs=0.0)
    assert poly_edge == model_p - ask - 0.07 * ask * (1.0 - ask)
    # predict.fun charges the tent, so its edge is HIGHER by the fee gap.
    assert pf_edge == pytest.approx(model_p - ask - pf_fee)
    assert pf_edge - poly_edge == pytest.approx(poly_fee - pf_fee)


def test_entry_gate_edge_falls_back_to_the_default_bps_on_predictfun():
    """No per-market fee in scope (the cheap pre-score, before any market is
    resolved) => the adapter's own config default, never the parabola."""
    from cryptobot import config
    from cryptobot.strategy.score import _fee_adjusted_edge

    bps = float(config.PREDICTFUN_DEFAULT_FEE_RATE_BPS)
    got = _fee_adjusted_edge(0.80, 0.75, venue="predictfun")
    assert got == pytest.approx(0.80 - 0.75 - (bps / 10000.0) * 0.25)


def test_entry_gate_edge_fails_closed_on_an_unknown_venue():
    """A wrong fee shape on the real-money decision path is an edge error, so
    the gate raises rather than guessing (same strictness as fee_per_share)."""
    from cryptobot.strategy.score import _fee_adjusted_edge
    from cryptobot.venues.registry import UnknownVenueError

    with pytest.raises(UnknownVenueError):
        _fee_adjusted_edge(0.80, 0.75, venue=UNREGISTERED)


# ---------------------------------------------------------------------------
# BOOT-level strictness
# ---------------------------------------------------------------------------


def test_boot_refuses_an_unknown_cryptobot_venue(monkeypatch):
    """A typo in a unit file must stop the boot, not trade Polymarket.

    Re-imports ``cryptobot.config`` from source under a bad env var, which is
    where the allowlist check lives.
    """
    import importlib.util
    import sys

    monkeypatch.setenv("CRYPTOBOT_VENUE", "polymarkt")  # the plausible typo
    spec = importlib.util.spec_from_file_location(
        "cryptobot._config_boot_probe", sys.modules["cryptobot.config"].__file__
    )
    module = importlib.util.module_from_spec(spec)
    with pytest.raises(ValueError, match="not a known venue"):
        spec.loader.exec_module(module)


def test_boot_allowlist_is_the_registry_map():
    # One source of truth: config validates against the registry, so a new
    # adapter cannot be reachable-but-unbootable (or vice versa).
    from cryptobot.venues.registry import KNOWN_VENUES, VENUE_PACKAGES

    assert set(KNOWN_VENUES) == set(VENUE_PACKAGES)
    assert {"polymarket", "predictfun", "kalshi"} == set(KNOWN_VENUES)


def test_every_registered_venue_resolves_to_an_adapter():
    # The registry map is TOTAL — that is why the kalshi stub package exists.
    from cryptobot.venues import get_adapter
    from cryptobot.venues.registry import KNOWN_VENUES

    for venue in KNOWN_VENUES:
        assert get_adapter(venue).name == venue
