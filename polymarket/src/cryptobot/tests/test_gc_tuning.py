"""Latency finding #6 (gc-pause): flag-gated GC tuning + pause telemetry.

Both main.py helpers self-gate on their config flags, so the OFF (default)
path is asserted to touch ``gc`` not at all — the byte-identical contract —
and the ON path is asserted to freeze the boot heap / raise thresholds /
install the pause callback. GC thresholds, freeze state, and ``gc.callbacks``
are process-global, so every ON-path test restores them in a finally block.
"""

from __future__ import annotations

import gc
import logging

from cryptobot import config
from cryptobot import main as main_mod


def test_gc_flags_default_off():
    """Unpinned env => both flags OFF and the documented threshold/cadence
    defaults — the byte-identical-when-OFF contract starts at config."""
    assert config.GC_TUNING is False
    assert config.GC_PAUSE_TELEMETRY is False
    assert config.GC_GEN0_THRESHOLD == 50000
    assert config.GC_GEN1_THRESHOLD == 20
    assert config.GC_GEN2_THRESHOLD == 20
    assert config.GC_COLLECT_SECONDS == 60


def test_apply_gc_tuning_off_is_noop(monkeypatch):
    """Flag OFF: the helper must not touch thresholds or freeze anything."""
    monkeypatch.setattr(config, "GC_TUNING", False)
    thresholds_before = gc.get_threshold()
    frozen_before = gc.get_freeze_count()
    main_mod._apply_gc_tuning()
    assert gc.get_threshold() == thresholds_before
    assert gc.get_freeze_count() == frozen_before


def test_apply_gc_tuning_freezes_and_raises_thresholds(monkeypatch):
    """Flag ON: boot heap moves to the permanent generation and the
    thresholds take the config values (collect-before-freeze ordering is a
    code invariant documented in the helper; here we pin the observable
    outcome)."""
    monkeypatch.setattr(config, "GC_TUNING", True)
    thresholds_before = gc.get_threshold()
    try:
        main_mod._apply_gc_tuning()
        assert gc.get_threshold() == (
            config.GC_GEN0_THRESHOLD,
            config.GC_GEN1_THRESHOLD,
            config.GC_GEN2_THRESHOLD,
        )
        # The test process's own tracked boot heap is non-empty, so a real
        # freeze must land a nonzero permanent generation.
        assert gc.get_freeze_count() > 0
    finally:
        gc.set_threshold(*thresholds_before)
        gc.unfreeze()


def test_gc_pause_telemetry_off_installs_nothing(monkeypatch):
    """Flag OFF: no callback appended, helper returns None."""
    monkeypatch.setattr(config, "GC_PAUSE_TELEMETRY", False)
    n_callbacks_before = len(gc.callbacks)
    assert main_mod._install_gc_pause_telemetry() is None
    assert len(gc.callbacks) == n_callbacks_before


def test_gc_pause_telemetry_logs_collections(monkeypatch, caplog):
    """Flag ON: the gc.callbacks hook times a real collection and logs it
    (min_pause_ms=0 so even a trivially fast test-process pass qualifies;
    ``gc.collect()`` is a gen-2 pass, so the gen-0 filter doesn't apply)."""
    monkeypatch.setattr(config, "GC_PAUSE_TELEMETRY", True)
    caplog.set_level(logging.INFO, logger="cryptobot.main")
    cb = main_mod._install_gc_pause_telemetry(min_pause_ms=0.0)
    assert cb is not None
    try:
        gc.collect()
    finally:
        gc.callbacks.remove(cb)
    assert any(
        "gc_pause" in record.getMessage() for record in caplog.records
    ), "expected a gc_pause log row for the explicit collection"


def test_gc_pause_telemetry_skips_gen0(monkeypatch, caplog):
    """Gen-0 passes fire ~every 700 allocations — the callback must not
    time or log them (only gen 1/2 can pause >= the report floor), or the
    instrument perturbs the tick path it measures. Automatic collection is
    disabled for the window so a background threshold trip can't inject a
    gen-1/2 row."""
    monkeypatch.setattr(config, "GC_PAUSE_TELEMETRY", True)
    caplog.set_level(logging.INFO, logger="cryptobot.main")
    cb = main_mod._install_gc_pause_telemetry(min_pause_ms=0.0)
    assert cb is not None
    gc.disable()
    try:
        gc.collect(0)
    finally:
        gc.enable()
        gc.callbacks.remove(cb)
    assert not any(
        "gc_pause" in record.getMessage() for record in caplog.records
    ), "gen-0 collections must not produce gc_pause rows"
