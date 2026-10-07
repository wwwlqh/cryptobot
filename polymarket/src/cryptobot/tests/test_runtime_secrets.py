"""U2 verification: runtime_secrets singleton + wallet topology gate.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U2:
  - get_config() returns a process-wide singleton (no torn .env reads).
  - set_config(None) forces a fresh re-read on next call.
  - _validate_wallet_config raises for SIG=3 + empty funder (D-LIVE-7).
  - SecretStr fields never appear in repr(cfg).
  - relayer_url_is_known() implements the D-LIVE-27 prefix gate.

Tests must NEVER read the real .env: `monkeypatch.setenv` + `set_config(None)`
flips the global into a stub-friendly state for the duration of each test.
"""

from __future__ import annotations

import pytest

from cryptobot_polymarket.walker import (
    _SIGNATURE_TYPE_LABELS,
    _validate_wallet_config,
)
from cryptobot.runtime_secrets import (
    Config,
    get_config,
    known_relayer_prefixes,
    set_config,
)


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Reset the Config singleton before and after each test so a stale
    cache from a prior test doesn't leak into this one."""
    set_config(None)
    yield
    set_config(None)


# ============================================================================
# Singleton lifecycle
# ============================================================================


class TestSingleton:
    def test_get_config_returns_same_instance_on_repeat_call(self) -> None:
        first = get_config()
        second = get_config()
        assert first is second

    def test_set_config_resets_singleton(self, monkeypatch) -> None:
        """`set_config(None)` forces re-construction on next get_config()."""
        first = get_config()
        set_config(None)
        second = get_config()
        # Different instances because we cleared the singleton.
        assert first is not second

    def test_set_config_with_stub_overrides_singleton(self) -> None:
        stub = Config(poly_signature_type=0, poly_funder="")
        set_config(stub)
        assert get_config() is stub

    def test_singleton_thread_safety_smoke(self) -> None:
        """Two threads racing on the first get_config() converge to one
        instance. Validates the double-checked locking pattern."""
        import threading

        results: list[Config] = []
        barrier = threading.Barrier(2)

        def worker() -> None:
            barrier.wait()
            results.append(get_config())

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert len(results) == 2
        assert results[0] is results[1]


# ============================================================================
# SecretStr masking
# ============================================================================


class TestSecretStrMasking:
    def test_repr_does_not_leak_private_key(self) -> None:
        cfg = Config(poly_private_key="super-sensitive-key")
        rendered = repr(cfg)
        assert "super-sensitive-key" not in rendered
        # Pydantic stamps SecretStr as '**********' in repr.
        assert "SecretStr" in rendered or "**" in rendered

    def test_repr_does_not_leak_api_key_or_secret_or_passphrase(self) -> None:
        cfg = Config(
            poly_api_key="apikey-XXX",
            poly_secret="secret-YYY",
            poly_passphrase="phrase-ZZZ",
        )
        rendered = repr(cfg)
        for needle in ("apikey-XXX", "secret-YYY", "phrase-ZZZ"):
            assert needle not in rendered, f"{needle!r} leaked into repr(cfg)"

    def test_get_secret_value_returns_raw_string(self) -> None:
        cfg = Config(poly_private_key="raw-value")
        assert cfg.poly_private_key.get_secret_value() == "raw-value"

    def test_dashboard_pass_is_secret_str(self, monkeypatch) -> None:
        # Isolate from the operator's on-disk .env / env so the init kwarg is
        # the only source (audit w19oxqf24 P2: field now carries an alias +
        # populate_by_name).
        monkeypatch.delenv("CRYPTOBOT_DASHBOARD_PASS", raising=False)
        monkeypatch.delenv("DASHBOARD_PASS", raising=False)
        cfg = Config(_env_file=None, dashboard_pass="topsecret-cookie-pass-xyzzy12345678")
        # SecretStr again
        assert "topsecret-cookie-pass" not in repr(cfg)
        assert cfg.dashboard_pass.get_secret_value() == "topsecret-cookie-pass-xyzzy12345678"


# ============================================================================
# Wallet topology validation (D-LIVE-7 / D-LIVE-5)
# ============================================================================


class TestValidateWalletConfig:
    def test_sig0_allows_empty_funder(self) -> None:
        """EOA mode signs from the same address as the wallet — funder is
        optional (will be auto-derived from the private key by py-clob-client)."""
        sig, funder = _validate_wallet_config(0, "")
        assert sig == 0
        assert funder == ""

    def test_sig0_accepts_funder_too(self) -> None:
        """EOA with an explicit funder address is allowed."""
        sig, funder = _validate_wallet_config(0, "0xabcd")
        assert sig == 0
        assert funder == "0xabcd"

    def test_sig3_requires_funder(self) -> None:
        """D-LIVE-7: POLY_1271 deposit wallet flow MUST carry a funder
        address — otherwise the first LIVE order signs against the wrong
        on-chain identity."""
        with pytest.raises(ValueError, match="POLY_FUNDER"):
            _validate_wallet_config(3, "")

    def test_sig3_accepts_funder(self) -> None:
        sig, funder = _validate_wallet_config(3, "0xfunder")
        assert sig == 3
        assert funder == "0xfunder"

    def test_sig3_strips_whitespace_from_funder(self) -> None:
        sig, funder = _validate_wallet_config(3, "   0xfunder   ")
        assert funder == "0xfunder"
        # The funder spaces alone should not satisfy the non-empty check.
        with pytest.raises(ValueError):
            _validate_wallet_config(3, "   ")

    def test_unknown_sig_type_raises_with_allowed_label_list(self) -> None:
        with pytest.raises(ValueError, match="must be one of"):
            _validate_wallet_config(99, "0xabcd")

    @pytest.mark.parametrize("sig", sorted(_SIGNATURE_TYPE_LABELS.keys()))
    def test_known_sig_types_pass_with_appropriate_funder(self, sig: int) -> None:
        funder = "0xabcd" if sig == 3 else ""
        out_sig, _ = _validate_wallet_config(sig, funder)
        assert out_sig == sig

    def test_none_signature_normalizes_to_zero(self) -> None:
        sig, _ = _validate_wallet_config(None, "")
        assert sig == 0

    def test_none_funder_normalizes_to_empty_string(self) -> None:
        _, funder = _validate_wallet_config(0, None)
        assert funder == ""


# ============================================================================
# RELAYER_URL prefix gate (D-LIVE-27)
# ============================================================================


class TestRelayerPrefixGate:
    def test_default_prefix_is_known(self) -> None:
        cfg = Config()
        assert cfg.relayer_url_is_known()

    def test_explicit_v1_endpoint_is_known(self) -> None:
        cfg = Config(relayer_url="https://relayer.polymarket.com")
        assert cfg.relayer_url_is_known()

    def test_random_endpoint_is_not_known(self) -> None:
        cfg = Config(relayer_url="https://evil.example.com/relayer")
        assert not cfg.relayer_url_is_known()

    def test_known_prefixes_export_includes_v2_endpoint(self) -> None:
        prefixes = known_relayer_prefixes()
        assert "https://relayer-v2.polymarket.com" in prefixes

    def test_trailing_slash_does_not_break_known(self) -> None:
        cfg = Config(relayer_url="https://relayer-v2.polymarket.com/")
        assert cfg.relayer_url_is_known()

    def test_prefix_host_spoof_is_not_known(self) -> None:
        cfg = Config(relayer_url="https://relayer-v2.polymarket.com.evil.example")
        assert not cfg.relayer_url_is_known()

    def test_userinfo_host_spoof_is_not_known(self) -> None:
        cfg = Config(relayer_url="https://relayer-v2.polymarket.com@evil.example")
        assert not cfg.relayer_url_is_known()

    def test_non_https_known_host_is_not_known(self) -> None:
        cfg = Config(relayer_url="http://relayer-v2.polymarket.com")
        assert not cfg.relayer_url_is_known()

    def test_non_default_port_is_not_known(self) -> None:
        cfg = Config(relayer_url="https://relayer-v2.polymarket.com:444")
        assert not cfg.relayer_url_is_known()

    def test_malformed_port_is_not_known(self) -> None:
        cfg = Config(relayer_url="https://relayer-v2.polymarket.com:notaport")
        assert not cfg.relayer_url_is_known()


# ============================================================================
# Defaults match the per-strategy LIVE plan
# ============================================================================


class TestPlanDefaults:
    """D-LIVE-* defaults pinned from the plan + env example."""

    def test_default_live_balance_tolerance(self) -> None:
        """D-LIVE-4 tolerance below which chain/CLOB drift triggers a halt."""
        cfg = Config()
        assert cfg.live_balance_tolerance_usd == 0.25

    def test_default_dashboard_host_is_loopback(self) -> None:
        cfg = Config()
        assert cfg.dashboard_bind_host == "127.0.0.1"

    def test_default_dry_run_true(self) -> None:
        """DRY_RUN sets the default for fresh wallets only; durable
        wallet_mode_state takes over after first boot."""
        cfg = Config()
        assert cfg.dry_run is True

    def test_default_initial_bankroll_one_hundred(self) -> None:
        cfg = Config()
        assert cfg.initial_bankroll == 100.0


# ============================================================================
# Env-var override smoke (Pydantic settings respects POLY_*)
# ============================================================================


class TestEnvVarOverrides:
    def test_poly_signature_type_reads_env(self, monkeypatch) -> None:
        monkeypatch.setenv("POLY_SIGNATURE_TYPE", "3")
        monkeypatch.setenv("POLY_FUNDER", "0xfromenv")
        cfg = Config()
        assert cfg.poly_signature_type == 3
        assert cfg.poly_funder == "0xfromenv"

    def test_dashboard_pass_reads_env(self, monkeypatch) -> None:
        # Audit w19oxqf24 (P2): DASHBOARD_PASS is kept as a backward-compat
        # secondary alias. _env_file=None isolates from the operator's on-disk
        # .env (which may set the prefixed var on a real checkout).
        monkeypatch.delenv("CRYPTOBOT_DASHBOARD_PASS", raising=False)
        monkeypatch.setenv("DASHBOARD_PASS", "envpass-32-chars-x" * 2)
        cfg = Config(_env_file=None)
        assert cfg.dashboard_pass.get_secret_value().startswith("envpass-32")

    def test_dashboard_pass_reads_prefixed_env(self, monkeypatch) -> None:
        """Audit w19oxqf24 (P2): the field must bind to the SAME prefixed env
        var the auth path reads (CRYPTOBOT_DASHBOARD_PASS), so an operator who
        configures the documented value actually affects auth."""
        monkeypatch.delenv("DASHBOARD_PASS", raising=False)
        monkeypatch.setenv("CRYPTOBOT_DASHBOARD_PASS", "prefixed-pass-xyzzy-0123456789")
        cfg = Config(_env_file=None)
        assert cfg.dashboard_pass.get_secret_value() == "prefixed-pass-xyzzy-0123456789"

    def test_dashboard_pass_prefixed_takes_precedence(self, monkeypatch) -> None:
        """When both are set, the prefixed (auth-path) var wins."""
        monkeypatch.setenv("DASHBOARD_PASS", "legacy-unprefixed-value-000000")
        monkeypatch.setenv("CRYPTOBOT_DASHBOARD_PASS", "prefixed-wins-value-111111111")
        cfg = Config(_env_file=None)
        assert cfg.dashboard_pass.get_secret_value() == "prefixed-wins-value-111111111"
