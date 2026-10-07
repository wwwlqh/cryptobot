"""U5 verification: OrderClient + CLOB error classification + book parsing.

Per the 2026-05-24-001 per-strategy LIVE deploy plan §U5:
  - _classify_clob_error buckets errors into auth / insufficient_funds /
    market_closed / network / unknown.
  - OrderClient.__init__ fails closed on missing py_clob_client_v2 or empty
    poly_private_key.
  - place_gtc_limit_order requires a non-empty client_order_id (D-LIVE-28
    durable reservation).
  - Stub-based tests never hit the real CLOB; we exercise the timeout
    envelope + the success / error paths.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from concurrent.futures import Future, TimeoutError as FuturesTimeoutError
from types import ModuleType, SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from cryptobot import config
from cryptobot_polymarket.walker import (
    ClobClientUnavailable,
    OrderClient,
    OrderResult,
    _classify_clob_error,
)


# ============================================================================
# _classify_clob_error
# ============================================================================


class TestClassifyClobError:
    def test_empty_returns_unknown(self) -> None:
        assert _classify_clob_error("") == "unknown"
        assert _classify_clob_error(None) == "unknown"

    @pytest.mark.parametrize(
        "msg",
        [
            "insufficient funds",
            "wallet balance too low",
            "your funds are below the minimum",
            "balance below minimum required",
            "not enough USDC",
        ],
    )
    def test_insufficient_funds_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "insufficient_funds"

    @pytest.mark.parametrize(
        "msg",
        [
            "invalid signature",
            "unauthorized",
            "forbidden",
            "invalid key",
            "missing api_key",
            "passphrase mismatch",
            "HTTP 401 Unauthorized",
            "HTTP 403 Forbidden",
            "maker address not allowed for this market",
            "deposit wallet flow requires funder",
            "signer address has to be the address of the api key",
        ],
    )
    def test_auth_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "auth"

    @pytest.mark.parametrize(
        "msg",
        [
            "market closed",
            "market is closed",
            "this market has expired",
            "market resolved",
            "asset not tradable",
            "token not tradeable",
        ],
    )
    def test_market_closed_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "market_closed"

    @pytest.mark.parametrize(
        "msg",
        [
            "timeout reading response",
            "connection reset",
            "ConnReset",
            "read timed out",
            "max retries exceeded",
            "remote disconnect",
            "HTTP 502 Bad Gateway",
            "HTTP 503 Service Unavailable",
            "HTTP 504 Gateway Timeout",
        ],
    )
    def test_network_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "network"

    def test_below_minimum_alone_is_unknown(self) -> None:
        """`below the minimum` without an explicit `balance`/`funds`
        qualifier is ambiguous (tick size, spread, etc.) — refuse to
        classify as insufficient_funds (matches hightempbot logic)."""
        assert _classify_clob_error("order size below the minimum") == "unknown"
        assert _classify_clob_error("price below the minimum tick size") == "unknown"

    def test_accepts_exception_instances(self) -> None:
        exc = RuntimeError("timeout: connection reset")
        assert _classify_clob_error(exc) == "network"

    @pytest.mark.parametrize(
        "msg",
        [
            # Verbatim 2026-06-19 / 06-24 burst (77 attempts) — HTTP 425.
            "PolyApiException[status_code=425, error_message="
            "{'error': 'order manager not ready, please retry'}]",
            "order manager not ready",
            "please retry",
        ],
    )
    def test_venue_unavailable_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "venue_unavailable"

    @pytest.mark.parametrize(
        "msg",
        [
            # Verbatim HTTP 503 post-only rejection. Must NOT fall through to
            # the network bucket (which also matches "503").
            "PolyApiException[status_code=503, error_message="
            "{'error': 'post-only mode: only post-only orders and cancels are "
            "allowed', 'code': 'post_only'}]",
            "post-only mode",
        ],
    )
    def test_post_only_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "post_only"

    @pytest.mark.parametrize(
        "msg",
        [
            # Verbatim HTTP 400 over-precise maker amount.
            "PolyApiException[status_code=400, error_message="
            "{'error': 'invalid amounts, the market buy orders maker amount "
            "supports a max accuracy of 2 decimals'}]",
            "the market buy orders maker amount supports a max accuracy of 2",
        ],
    )
    def test_invalid_amount_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "invalid_amount"

    @pytest.mark.parametrize(
        "msg",
        [
            # Verbatim HTTP 400 FAK-no-match (thin book).
            "PolyApiException[status_code=400, error_message="
            "{'error': 'no orders found to match with FAK order. FAK orders "
            "are partially filled or killed'}]",
            "no orders found to match with FAK order",
        ],
    )
    def test_no_liquidity_bucket(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "no_liquidity"

    def test_http_503_without_post_only_stays_network(self) -> None:
        """A bare 503 (not post-only) still classifies as network — the new
        post_only check must not steal generic 5xx transport errors."""
        assert _classify_clob_error("HTTP 503 Service Unavailable") == "network"

    @pytest.mark.parametrize(
        "msg",
        [
            # Verbatim live 2026-07-15 07:49:58 — the returned orderID hex
            # contains "401" ("…589401256…"), which the old bare-substring
            # auth check matched, turning a routine thin-book FAK kill into a
            # TERMINAL auth failure (silently ended the requote chain).
            "PolyApiException[status_code=400, error_message="
            "{'error': 'no orders found to match with FAK order. FAK orders "
            "are partially filled or killed if no match is found.', 'orderID': "
            "'0x1cbae589401256ea611200cd8318aa2fcfbac41e97e01ff198214bc8c90e347a'}]",
            # Verbatim live 2026-07-15 02:04:48 — hex contains "401"
            # ("…7440131…").
            "PolyApiException[status_code=400, error_message="
            "{'error': 'no orders found to match with FAK order. FAK orders "
            "are partially filled or killed if no match is found.', 'orderID': "
            "'0xc8aa0855044953ea0d9715b96ed8bc3bf2adf8cc137440131cd6005fb2cbc2e2'}]",
        ],
    )
    def test_status_codes_inside_hex_ids_do_not_classify(self, msg: str) -> None:
        assert _classify_clob_error(msg) == "no_liquidity"

    def test_status_code_field_still_classifies_auth(self) -> None:
        """`\\b`-delimited codes keep matching real status fields."""
        assert _classify_clob_error("status_code=401, error_message=x") == "auth"
        assert _classify_clob_error("PolyApiException[status_code=403]") == "auth"


# ============================================================================
# OrderClient construction
# ============================================================================


def _stub_py_clob_client_v2(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install a fake `py_clob_client_v2` package + submodules so OrderClient
    can construct without the real network library installed."""
    mock_client = MagicMock(name="ClobClient_instance")

    class _ApiCreds:
        def __init__(self, api_key=None, api_secret=None, api_passphrase=None):
            self.api_key = api_key
            self.api_secret = api_secret
            self.api_passphrase = api_passphrase

    class _ClobClient:
        def __init__(self, **kwargs):
            self.init_kwargs = kwargs
            self.mock = mock_client

        def __getattr__(self, name: str):
            return getattr(mock_client, name)

    class _AssetType:
        COLLATERAL = "collateral"
        CONDITIONAL = "conditional"

    class _BalanceAllowanceParams:
        def __init__(self, asset_type, token_id=None):
            self.asset_type = asset_type
            self.token_id = token_id

    class _OrderArgs:
        def __init__(self, price, size, side, token_id):
            self.price = price
            self.size = size
            self.side = side
            self.token_id = token_id

    class _MarketOrderArgs:
        def __init__(self, amount, side, token_id, price=0, order_type=None):
            self.amount = amount
            self.side = side
            self.token_id = token_id
            self.price = price
            self.order_type = order_type

    class _OrderType:
        GTC = "GTC"
        FOK = "FOK"
        FAK = "FAK"

    pkg = ModuleType("py_clob_client_v2")
    client_mod = ModuleType("py_clob_client_v2.client")
    client_mod.ClobClient = _ClobClient
    clob_types_mod = ModuleType("py_clob_client_v2.clob_types")
    clob_types_mod.ApiCreds = _ApiCreds
    clob_types_mod.AssetType = _AssetType
    clob_types_mod.BalanceAllowanceParams = _BalanceAllowanceParams
    clob_types_mod.OrderArgs = _OrderArgs
    clob_types_mod.MarketOrderArgs = _MarketOrderArgs
    clob_types_mod.OrderType = _OrderType
    constants_mod = ModuleType("py_clob_client_v2.order_builder.constants")
    constants_mod.BUY = "BUY"
    constants_mod.SELL = "SELL"
    builder_mod = ModuleType("py_clob_client_v2.order_builder")
    builder_mod.constants = constants_mod
    http_helpers_mod = ModuleType("py_clob_client_v2.http_helpers")
    helpers_submod = ModuleType("py_clob_client_v2.http_helpers.helpers")
    helpers_submod._http_client = None
    http_helpers_mod.helpers = helpers_submod
    endpoints_mod = ModuleType("py_clob_client_v2.endpoints")
    endpoints_mod.OK = "/ok"

    monkeypatch.setitem(sys.modules, "py_clob_client_v2", pkg)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.client", client_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.clob_types", clob_types_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.order_builder", builder_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.order_builder.constants", constants_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.http_helpers", http_helpers_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.http_helpers.helpers", helpers_submod)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2.endpoints", endpoints_mod)

    return SimpleNamespace(mock_client=mock_client)


def _make_config(
    *,
    private_key: str = "0xabcd",
    api_key: str = "ak",
    api_secret: str = "as",
    api_passphrase: str = "ap",
    signature_type: int = 0,
    funder: str = "",
):
    from cryptobot.runtime_secrets import Config

    return Config(
        poly_private_key=private_key,
        poly_api_key=api_key,
        poly_secret=api_secret,
        poly_passphrase=api_passphrase,
        poly_signature_type=signature_type,
        poly_funder=funder,
    )


class TestOrderClientInit:
    def test_raises_clob_unavailable_when_package_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No py_clob_client_v2 installed → init raises ClobClientUnavailable.

        Mocks the import via the package-level entrypoint that the
        constructor reads first."""
        # Make sure py_clob_client_v2 is NOT importable.
        for mod_name in list(sys.modules):
            if mod_name.startswith("py_clob_client_v2"):
                monkeypatch.delitem(sys.modules, mod_name, raising=False)
        # Stub the import to raise ModuleNotFoundError.
        import builtins

        real_import = builtins.__import__

        def _import_raising(name, *args, **kwargs):
            if name == "py_clob_client_v2.clob_types":
                raise ModuleNotFoundError("py_clob_client_v2")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _import_raising)
        with pytest.raises(ClobClientUnavailable):
            OrderClient(_make_config())

    def test_raises_value_error_when_private_key_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _stub_py_clob_client_v2(monkeypatch)
        with pytest.raises(ValueError, match="poly_private_key"):
            OrderClient(_make_config(private_key=""))

    def test_init_succeeds_for_sig_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _stub_py_clob_client_v2(monkeypatch)
        client = OrderClient(_make_config(signature_type=0))
        assert client._signature_type == 0
        assert client._funder == ""

    def test_init_succeeds_for_sig_3_with_funder(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _stub_py_clob_client_v2(monkeypatch)
        client = OrderClient(
            _make_config(signature_type=3, funder="0xfunder")
        )
        assert client._signature_type == 3
        assert client._funder == "0xfunder"

    def test_init_raises_for_sig_3_without_funder(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _stub_py_clob_client_v2(monkeypatch)
        with pytest.raises(ValueError, match="POLY_FUNDER"):
            OrderClient(_make_config(signature_type=3, funder=""))


# ============================================================================
# OrderClient runtime methods (mocked CLOB)
# ============================================================================


@pytest.fixture
def order_client(monkeypatch: pytest.MonkeyPatch):
    stubs = _stub_py_clob_client_v2(monkeypatch)
    client = OrderClient(_make_config())
    yield client, stubs.mock_client


class TestCheckBalance:
    def test_returns_balance_in_dollars(self, order_client) -> None:
        client, mock = order_client
        mock.get_balance_allowance.return_value = {"balance": str(200_000_000)}
        # 200_000_000 in 6-decimal pUSD = 200.0
        assert client.check_balance() == 200.0

    def test_returns_none_on_exception(self, order_client) -> None:
        client, mock = order_client
        mock.get_balance_allowance.side_effect = RuntimeError("clob down")
        assert client.check_balance() is None


# ============================================================================
# Signing-pool warmth ping (latency finding #8)
# ============================================================================


class _FakePool:
    """Stands in for the module-level py_clob_client_v2 httpx pool."""

    def __init__(self, status_code: int = 200, exc: Optional[Exception] = None):
        self.status_code = status_code
        self.exc = exc
        self.calls: list[tuple[str, object]] = []

    def get(self, url, timeout=None):
        self.calls.append((url, timeout))
        if self.exc is not None:
            raise self.exc
        return SimpleNamespace(status_code=self.status_code)


class TestPingOrderPool:
    def _install_pool(self, pool: _FakePool, monkeypatch) -> None:
        # F4: ping_order_pool uses the module-state pool ref captured at
        # install time — not a per-call deref of the library helpers module.
        import cryptobot_polymarket.walker as walker_mod

        monkeypatch.setattr(walker_mod, "_CLOB_HTTP_CLIENT", pool)

    def test_pings_ok_on_idle_pool(self, order_client, monkeypatch) -> None:
        import cryptobot_polymarket.walker as walker_mod

        client, mock = order_client
        mock.host = "https://clob.test"
        monkeypatch.setattr(walker_mod, "_CLOB_LAST_RESPONSE_MONOTONIC", 0.0)
        pool = _FakePool()
        self._install_pool(pool, monkeypatch)
        assert client.ping_order_pool() is True
        assert len(pool.calls) == 1
        url, timeout = pool.calls[0]
        assert url == "https://clob.test/ok"
        # Safe-verdict caveat (c): the ping must NOT inherit the pool's
        # read=30s — a hung /ok would pin a scheduler-pool worker. It passes
        # its own short per-request timeout override instead.
        assert timeout is not None

    def test_skips_when_pool_recently_active(self, order_client, monkeypatch) -> None:
        import cryptobot_polymarket.walker as walker_mod

        client, _ = order_client
        monkeypatch.setattr(
            walker_mod, "_CLOB_LAST_RESPONSE_MONOTONIC", time.monotonic()
        )
        pool = _FakePool()
        self._install_pool(pool, monkeypatch)
        # Recent readiness/order traffic already kept the socket hot — no GET.
        assert client.ping_order_pool() is False
        assert pool.calls == []

    def test_pings_when_activity_is_stale(self, order_client, monkeypatch) -> None:
        import cryptobot_polymarket.walker as walker_mod

        client, mock = order_client
        mock.host = "https://clob.test"
        monkeypatch.setattr(
            walker_mod,
            "_CLOB_LAST_RESPONSE_MONOTONIC",
            time.monotonic() - (config.CLOB_PING_SKIP_IF_ACTIVE_S + 1.0),
        )
        pool = _FakePool()
        self._install_pool(pool, monkeypatch)
        assert client.ping_order_pool() is True
        assert len(pool.calls) == 1

    def test_skip_window_default_covers_trading_band(self) -> None:
        """F4: the 40s skip window means the ping only fires after >=40s of
        pool silence — outside any active-trading band (a 15s window let a
        ping occupy the warm keepalive connection exactly when an order POST
        fired) while still landing safely inside keepalive_expiry=65s at the
        20s cadence."""
        assert config.CLOB_PING_SKIP_IF_ACTIVE_S == 40.0

    def test_falls_back_to_library_deref_when_captured_ref_absent(
        self, order_client, monkeypatch
    ) -> None:
        """F4: with no captured pool ref (install failed/skipped) the ping
        falls back to dereferencing the library-private helpers module."""
        import cryptobot_polymarket.walker as walker_mod

        client, mock = order_client
        mock.host = "https://clob.test"
        monkeypatch.setattr(walker_mod, "_CLOB_LAST_RESPONSE_MONOTONIC", 0.0)
        monkeypatch.setattr(walker_mod, "_CLOB_HTTP_CLIENT", None)
        pool = _FakePool()
        sys.modules["py_clob_client_v2.http_helpers.helpers"]._http_client = pool
        assert client.ping_order_pool() is True
        assert len(pool.calls) == 1

    def test_returns_false_on_pool_error(self, order_client, monkeypatch) -> None:
        import cryptobot_polymarket.walker as walker_mod

        client, mock = order_client
        mock.host = "https://clob.test"
        monkeypatch.setattr(walker_mod, "_CLOB_LAST_RESPONSE_MONOTONIC", 0.0)
        pool = _FakePool(exc=RuntimeError("conn reset"))
        self._install_pool(pool, monkeypatch)
        # Fail-soft: never raises into the scheduler job.
        assert client.ping_order_pool() is False

    def test_returns_false_on_non_200(self, order_client, monkeypatch) -> None:
        import cryptobot_polymarket.walker as walker_mod

        client, mock = order_client
        mock.host = "https://clob.test"
        monkeypatch.setattr(walker_mod, "_CLOB_LAST_RESPONSE_MONOTONIC", 0.0)
        pool = _FakePool(status_code=503)
        self._install_pool(pool, monkeypatch)
        assert client.ping_order_pool() is False
        assert len(pool.calls) == 1

    def test_response_hook_stamps_pool_activity(self, monkeypatch) -> None:
        import cryptobot_polymarket.walker as walker_mod

        monkeypatch.setattr(walker_mod, "_CLOB_LAST_RESPONSE_MONOTONIC", 0.0)
        walker_mod._note_clob_pool_response(object())
        assert walker_mod._CLOB_LAST_RESPONSE_MONOTONIC > 0.0


class TestClobPoolResponseHookInstall:
    """The response hook feeding the ping's skip guard is installed on the
    replacement pool ONLY when the ping is enabled — CLOB_PING_SECONDS=0
    must leave the client byte-identical to the pre-ping construction."""

    def test_hook_installed_when_ping_enabled(self, monkeypatch, caplog) -> None:
        import cryptobot_polymarket.walker as walker_mod

        _stub_py_clob_client_v2(monkeypatch)
        monkeypatch.setattr(walker_mod, "_HTTPX_TIMEOUT_INSTALLED", False)
        monkeypatch.setattr(walker_mod, "_CLOB_HTTP_CLIENT", None)
        monkeypatch.setattr(config, "CLOB_PING_SECONDS", 20)
        with caplog.at_level(logging.INFO):
            walker_mod._install_clob_http_timeout()
        pool = sys.modules["py_clob_client_v2.http_helpers.helpers"]._http_client
        assert pool is not None
        assert walker_mod._note_clob_pool_response in pool.event_hooks["response"]
        # F4: the pool ref is captured as module state for ping_order_pool.
        assert walker_mod._CLOB_HTTP_CLIENT is pool
        # F4 boot visibility: ONE line says the skip guard is live.
        assert "clob_ping warmth guard: active" in caplog.text

    def test_no_hook_when_ping_disabled(self, monkeypatch, caplog) -> None:
        import cryptobot_polymarket.walker as walker_mod

        _stub_py_clob_client_v2(monkeypatch)
        monkeypatch.setattr(walker_mod, "_HTTPX_TIMEOUT_INSTALLED", False)
        monkeypatch.setattr(walker_mod, "_CLOB_HTTP_CLIENT", None)
        monkeypatch.setattr(config, "CLOB_PING_SECONDS", 0)
        with caplog.at_level(logging.INFO):
            walker_mod._install_clob_http_timeout()
        pool = sys.modules["py_clob_client_v2.http_helpers.helpers"]._http_client
        assert pool is not None
        assert pool.event_hooks == {"request": [], "response": []}
        # No ping job wired → no guard line either way.
        assert "clob_ping warmth guard" not in caplog.text

    def test_boot_logs_guard_inert_when_install_fails(
        self, monkeypatch, caplog
    ) -> None:
        """F4: a failed pool install leaves the guard timestamp 0.0 forever
        (every ping fires) — the boot line must say so instead of degrading
        silently."""
        import cryptobot_polymarket.walker as walker_mod

        _stub_py_clob_client_v2(monkeypatch)
        # Force the pool replacement to fail: the helpers import can't
        # resolve (the stub package has no __path__ to import through).
        monkeypatch.delitem(sys.modules, "py_clob_client_v2.http_helpers.helpers")
        monkeypatch.delitem(sys.modules, "py_clob_client_v2.http_helpers")
        monkeypatch.setattr(walker_mod, "_HTTPX_TIMEOUT_INSTALLED", False)
        monkeypatch.setattr(walker_mod, "_CLOB_HTTP_CLIENT", None)
        monkeypatch.setattr(config, "CLOB_PING_SECONDS", 20)
        with caplog.at_level(logging.INFO):
            walker_mod._install_clob_http_timeout()
        assert walker_mod._CLOB_HTTP_CLIENT is None
        assert "clob_ping warmth guard: inert — pings unconditional" in caplog.text


# ============================================================================
# place_gtc_limit_order
# ============================================================================


def _make_signal(**overrides):
    """Minimal BetSignal stub matching the LIVE happy path."""
    from cryptobot.execution.ledger import BetSignal

    defaults = dict(
        coin="BTC",
        sleeve="v6_5m",
        entry_minute=4,
        side="YES",
        window_start_ts=1715923200,
        window_end_ts=1715923500,
        interval_min=5,
        poly_market_id="market-1",
        poly_token_id="token-yes",
        fee_rate_bps=100.0,
        model_p=0.78,
        p_market=0.55,
        fill_price=0.55,
        walked_vwap=0.55,
        realized_edge=0.10,
        partial_ret_z_1h=None,
        lut_p_bucket=None,
        lut_hit_rate=None,
        lut_n_cum=None,
        target_usd=5.0,
        bet_size=5.0,
        slot_filled_pre=0.0,
        entry_top_price=0.55,
        yes_pre1_anchor=0.55,
        config_sha256="sha-stub",
        wallet_id="v6_5m",
    )
    defaults.update(overrides)
    return BetSignal(**defaults)


class TestPlaceGtcLimitOrder:
    def test_requires_client_order_id(self, order_client) -> None:
        client, _ = order_client
        with pytest.raises(ValueError, match="client_order_id"):
            client.place_gtc_limit_order(_make_signal(), client_order_id="")

    def test_happy_path_returns_orderid(self, order_client) -> None:
        client, mock = order_client
        signed = {"order": {}}
        mock.create_market_order.return_value = signed
        mock.post_order.return_value = {"orderID": "clob-order-1"}
        result = client.place_gtc_limit_order(
            _make_signal(bet_size=5.0, fill_price=0.50),
            client_order_id="coid-1",
        )
        assert result.success is True
        assert result.order_id == "clob-order-1"
        assert result.client_order_id == "coid-1"
        assert result.limit_price == 0.50
        # Audit P1 #4: with no fill metadata in the response, the fill columns
        # are None (defer to reconciler) — NOT the optimistic full-stake
        # placeholder that let the sweep settle phantom shares.
        assert result.filled_size_tokens is None
        assert result.filled_price_vwap is None
        assert result.filled_value_usd is None

    def test_entry_uses_market_buy_amount_with_price_cap(self, order_client) -> None:
        """A FAK BUY is validated by Polymarket as a market buy."""
        client, mock = order_client
        mock.create_market_order.return_value = {"order": {}}
        mock.post_order.return_value = {"orderID": "clob-order-precision"}

        result = client.place_gtc_limit_order(
            _make_signal(bet_size=4.0, fill_price=0.50, limit_price=0.53),
            client_order_id="coid-precision",
        )

        assert result.success is True
        assert mock.create_order.call_count == 0
        order_args = mock.create_market_order.call_args.args[0]
        assert order_args.amount == pytest.approx(4.0)
        assert order_args.price == pytest.approx(0.53)
        assert result.limit_price == pytest.approx(0.53)
        assert order_args.side == "BUY"
        assert order_args.token_id == "token-yes"
        assert order_args.order_type == "FAK"

    def test_records_real_fak_fill_when_present(self, order_client) -> None:
        """Audit P1 #4: when the CLOB FAK response carries the matched
        size/price, record the REAL fill (a partial), not the requested size."""
        client, mock = order_client
        mock.create_market_order.return_value = {"order": {}}
        # Requested 10 tokens (5.0 / 0.50) but only 6 matched at 0.50.
        mock.post_order.return_value = {
            "orderID": "clob-order-partial",
            "size_matched": 6.0,
            "avg_fill_price": 0.50,
        }
        result = client.place_gtc_limit_order(
            _make_signal(bet_size=5.0, fill_price=0.50),
            client_order_id="coid-partial",
        )
        assert result.success is True
        assert result.filled_size_tokens == pytest.approx(6.0)
        assert result.filled_price_vwap == pytest.approx(0.50)
        assert result.filled_value_usd == pytest.approx(3.0)

    def test_zero_match_defers_settlement(self, order_client) -> None:
        """A FAK that matched nothing (size_matched=0) must NOT record a
        positive fill — leave None so the sweep defers."""
        client, mock = order_client
        mock.create_market_order.return_value = {"order": {}}
        mock.post_order.return_value = {
            "orderID": "clob-order-zero",
            "size_matched": 0,
            "avg_fill_price": 0,
        }
        result = client.place_gtc_limit_order(
            _make_signal(bet_size=5.0, fill_price=0.50),
            client_order_id="coid-zero",
        )
        assert result.success is True
        assert result.filled_size_tokens is None
        assert result.filled_value_usd is None

    def test_entry_posts_fill_and_kill_not_gtc(self, order_client) -> None:
        """Go-live blocker #1 (audit ww3qot1x3): the entry must post FAK so the
        venue kills any unfilled remainder — a GTC remainder would rest on the
        book, never get cancelled, and double-commit exposure vs the top-up."""
        client, mock = order_client
        mock.create_market_order.return_value = {"order": {}}
        mock.post_order.return_value = {"orderID": "clob-order-fak"}
        result = client.place_gtc_limit_order(
            _make_signal(), client_order_id="coid-fak"
        )
        assert result.success is True
        # post_order is called as (signed_order, OrderType.<X>); assert FAK.
        order_type_arg = mock.post_order.call_args.args[-1]
        assert order_type_arg == "FAK", (
            f"entry order must be FAK, got {order_type_arg!r} — a resting GTC "
            f"remainder is a real-money risk on the first live bet"
        )

    def test_missing_orderid_returns_failure(self, order_client) -> None:
        client, mock = order_client
        mock.create_market_order.return_value = {"order": {}}
        mock.post_order.return_value = {"error": "rejected"}
        result = client.place_gtc_limit_order(
            _make_signal(), client_order_id="coid-2"
        )
        assert result.success is False
        assert result.order_id is None
        assert result.error_kind == "unknown"

    def test_market_closed_terminal_error(self, order_client) -> None:
        client, mock = order_client
        mock.create_market_order.side_effect = RuntimeError("market is closed")
        result = client.place_gtc_limit_order(
            _make_signal(), client_order_id="coid-mc"
        )
        assert result.success is False
        assert result.error_kind == "market_closed"
        assert result.is_terminal_failure is True

    def test_network_error_is_retryable(self, order_client) -> None:
        client, mock = order_client
        mock.create_market_order.side_effect = RuntimeError("read timed out")
        result = client.place_gtc_limit_order(
            _make_signal(), client_order_id="coid-net"
        )
        assert result.error_kind == "network"
        assert result.is_terminal_failure is False

    def test_invalid_price_returns_failure(self, order_client) -> None:
        client, _ = order_client
        result = client.place_gtc_limit_order(
            _make_signal(fill_price=0.0), client_order_id="coid-zero"
        )
        assert result.success is False
        assert "<= 0" in str(result.error)

    def test_amount_quantized_down_to_two_decimals(self, order_client) -> None:
        """Polymarket rejects a market-buy maker amount with >2 decimals
        (HTTP 400 'max accuracy of 2'). The submitted amount must be
        quantized DOWN to cents so we never over-bet the walked stake."""
        client, mock = order_client
        mock.create_market_order.return_value = {"order": {}}
        mock.post_order.return_value = {"orderID": "clob-order-round"}
        result = client.place_gtc_limit_order(
            _make_signal(bet_size=4.3399, fill_price=0.50),
            client_order_id="coid-round",
        )
        assert result.success is True
        order_args = mock.create_market_order.call_args.args[0]
        # 4.3399 must floor to 4.33 — NOT round to 4.34 (never over-bet).
        assert order_args.amount == pytest.approx(4.33)

    def test_amount_rounding_below_cent_fails_terminal(self, order_client) -> None:
        """A sub-cent stake quantizes to $0.00 → terminal invalid_amount
        (retrying the same amount would just re-fail)."""
        client, _ = order_client
        result = client.place_gtc_limit_order(
            _make_signal(bet_size=0.004), client_order_id="coid-tiny"
        )
        assert result.success is False
        assert result.error_kind == "invalid_amount"
        assert result.is_terminal_failure is True

    def test_auth_error_classified(self, order_client) -> None:
        client, mock = order_client
        mock.create_market_order.side_effect = RuntimeError("invalid signature")
        result = client.place_gtc_limit_order(
            _make_signal(), client_order_id="coid-auth"
        )
        assert result.error_kind == "auth"
        assert result.is_terminal_failure is True


class TestFetchOrderFill:
    """PURE_STOP_04 immediate-fill GET (2026-07-10 root-cause fix): a FAK
    post_order response usually carries no fill, so the accept branch issues one
    immediate GET so the held-position (T3) pass can see the realized fill THIS tick
    instead of waiting for the 60s reconciler (which lands after window_end)."""

    def test_reads_fill_from_get_order(self, order_client) -> None:
        client, mock = order_client
        mock.get_order.return_value = {
            "order_id": "clob-1",
            "status": "MATCHED",
            "size_matched": 6.0,
            "avg_fill_price": 0.72,
            "matched_at": "2026-07-10T00:00:00Z",
        }
        size, price, ts = client.fetch_order_fill("clob-1")
        assert size == pytest.approx(6.0)
        assert price == pytest.approx(0.72)
        assert ts == "2026-07-10T00:00:00Z"

    def test_derives_price_from_limit_when_avg_absent(self, order_client) -> None:
        """A full FAK match reported with only the order limit `price` +
        original_size derives that price (same as the reconciler)."""
        client, mock = order_client
        mock.get_order.return_value = {
            "order_id": "clob-2",
            "status": "MATCHED",
            "price": "0.70",
            "original_size": "7.14",
            "size_matched": "7.142857",
        }
        size, price, _ts = client.fetch_order_fill("clob-2")
        assert size == pytest.approx(7.142857)
        assert price == pytest.approx(0.70)

    def test_no_fill_returns_none(self, order_client) -> None:
        client, mock = order_client
        mock.get_order.return_value = {"order_id": "clob-3", "status": "OPEN"}
        assert client.fetch_order_fill("clob-3") == (None, None, None)

    def test_zero_match_returns_none(self, order_client) -> None:
        client, mock = order_client
        mock.get_order.return_value = {
            "order_id": "clob-4", "status": "CANCELED", "size_matched": 0,
        }
        assert client.fetch_order_fill("clob-4") == (None, None, None)

    def test_get_order_exception_returns_none(self, order_client) -> None:
        client, mock = order_client
        mock.get_order.side_effect = RuntimeError("CLOB down")
        assert client.fetch_order_fill("clob-5") == (None, None, None)

    def test_empty_order_id_returns_none(self, order_client) -> None:
        client, _ = order_client
        assert client.fetch_order_fill("") == (None, None, None)


# ============================================================================
# OrderResult shape
# ============================================================================


class TestOrderResult:
    def test_default_is_failure(self) -> None:
        result = OrderResult()
        assert result.success is False
        assert result.order_id is None

    def test_is_terminal_failure_for_auth(self) -> None:
        result = OrderResult(success=False, error_kind="auth")
        assert result.is_terminal_failure is True

    def test_is_terminal_failure_for_market_closed(self) -> None:
        result = OrderResult(success=False, error_kind="market_closed")
        assert result.is_terminal_failure is True

    def test_network_is_not_terminal(self) -> None:
        result = OrderResult(success=False, error_kind="network")
        assert result.is_terminal_failure is False

    def test_unknown_is_not_terminal(self) -> None:
        result = OrderResult(success=False, error_kind="unknown")
        assert result.is_terminal_failure is False


# ============================================================================
# market-info warm: async Future retention + ensure join (A1 / L4 / A15)
# ============================================================================


def _wait_until(cond, timeout: float = 5.0) -> None:
    """Spin-wait for a background-warm side effect (executor worker)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met within timeout")


class TestMarketInfoWarm:
    def test_ensure_joins_in_flight_async_warm_single_get(
        self, order_client
    ) -> None:
        """A15: while the discovery-time async warm is still in flight,
        ensure_market_info_warm must JOIN its retained Future — ONE GET
        total, not the pre-A15 duplicate concurrent GET on the calling
        thread."""
        client, mock = order_client
        started = threading.Event()
        release = threading.Event()

        def _blocking_get(cid):
            started.set()
            assert release.wait(timeout=5.0)
            return {"condition_id": cid}

        mock.get_clob_market_info.side_effect = _blocking_get
        client.warm_market_info_async("0xcond-join")
        assert started.wait(timeout=5.0)  # worker GET is in flight
        assert "0xcond-join" in client._warm_futures
        # Release the held venue GET shortly after ensure starts joining it.
        threading.Timer(0.05, release.set).start()
        assert client.ensure_market_info_warm("0xcond-join") is True
        assert mock.get_clob_market_info.call_count == 1, (
            "ensure must join the in-flight warm, not fire a duplicate GET"
        )
        assert "0xcond-join" in client._warmed_condition_ids

    def test_async_warm_cleans_up_future_on_completion(
        self, order_client
    ) -> None:
        """The retained Future lives exactly as long as the in-flight marker;
        after completion ensure is the warm-cache fast path (no GET)."""
        client, mock = order_client
        mock.get_clob_market_info.return_value = {"ok": True}
        client.warm_market_info_async("0xcond-clean")
        _wait_until(lambda: "0xcond-clean" in client._warmed_condition_ids)
        assert "0xcond-clean" not in client._warm_futures
        assert "0xcond-clean" not in client._warming_condition_ids
        assert client.ensure_market_info_warm("0xcond-clean") is True
        assert mock.get_clob_market_info.call_count == 1

    def test_ensure_join_of_failed_warm_spends_the_one_attempt(
        self, order_client
    ) -> None:
        """Review F3: the join IS the market's one blocking attempt. A
        joined warm that FAILED must return False WITHOUT chaining the
        second up-to-30s sync GET behind it in the same call (the pre-fix
        fall-through doubled the ~30s worst case to ~60s serial)."""
        client, mock = order_client
        started = threading.Event()
        release = threading.Event()

        def _get(cid):
            started.set()
            assert release.wait(timeout=5.0)
            raise RuntimeError("venue 500")

        mock.get_clob_market_info.side_effect = _get
        client.warm_market_info_async("0xcond-fail")
        assert started.wait(timeout=5.0)  # worker GET is in flight
        threading.Timer(0.05, release.set).start()
        assert client.ensure_market_info_warm("0xcond-fail") is False
        assert mock.get_clob_market_info.call_count == 1, (
            "the failed join must not fall through to a sync GET"
        )
        assert "0xcond-fail" in client._warm_sync_attempted
        # Attempt spent — the next call returns immediately, still no GET.
        assert client.ensure_market_info_warm("0xcond-fail") is False
        assert mock.get_clob_market_info.call_count == 1

    def test_spend_sync_attempt_caps_and_dedups(self, order_client) -> None:
        """The shared L4 attempt bookkeeping (F3 helper): atomic
        already-spent detection + cap-and-clear at 4096 (fresh market every
        window; mirrors the _warmed_condition_ids cap)."""
        client, _ = order_client
        assert client._spend_sync_attempt("0xa") is True
        assert client._spend_sync_attempt("0xa") is False  # already spent
        client._warm_sync_attempted.update(f"0x{i}" for i in range(4200))
        assert client._spend_sync_attempt("0xb") is True
        assert client._warm_sync_attempted == {"0xb"}

    def test_ensure_sync_get_when_no_async_warm(self, order_client) -> None:
        """No async warm ever fired (e.g. stub order path) → the unchanged
        one-shot sync fallback runs exactly as before A15."""
        client, mock = order_client
        mock.get_clob_market_info.return_value = {"ok": True}
        assert client.ensure_market_info_warm("0xcond-sync") is True
        assert mock.get_clob_market_info.call_count == 1
        # Warm cache short-circuits the second call — still one GET.
        assert client.ensure_market_info_warm("0xcond-sync") is True
        assert mock.get_clob_market_info.call_count == 1

    def test_join_timeout_counts_as_the_one_blocking_attempt(
        self, order_client
    ) -> None:
        """A hung in-flight warm must not stack a second blocking GET behind
        it: the timed-out join consumes the per-cid one-shot (venue-down
        guard) and later calls return immediately."""
        client, mock = order_client
        client._timeout = 0.05
        client._warm_futures["0xcond-hung"] = Future()  # never completes
        assert client.ensure_market_info_warm("0xcond-hung") is False
        assert "0xcond-hung" in client._warm_sync_attempted
        assert mock.get_clob_market_info.call_count == 0
        # Guard: the second call must not block on the pending Future again.
        assert client.ensure_market_info_warm("0xcond-hung") is False
        assert mock.get_clob_market_info.call_count == 0

    def test_venue_down_guard_beats_join(self, order_client) -> None:
        """After a failed/spent sync attempt, ensure returns immediately —
        even if a fresh async warm is in flight (never blocks twice)."""
        client, mock = order_client
        client._warm_sync_attempted.add("0xcond-down")
        client._warm_futures["0xcond-down"] = Future()  # pending — ignored
        assert client.ensure_market_info_warm("0xcond-down") is False
        assert mock.get_clob_market_info.call_count == 0


# ============================================================================
# _parse_fak_fill — makingAmount/takingAmount orientation (2026-07-22 audit:
# every live stop-loss FOK sell fell through to the posted LIMIT because the
# post_order response reports fills as making/taking, none of the explicit
# avg-price fields — under-booking proceeds by up to the 0.10 chase).
# ============================================================================


def test_parse_fak_fill_making_taking_sell_orientation():
    from cryptobot_polymarket.walker import _parse_fak_fill

    # SELL: we MAKE tokens, TAKE USDC. 8.30 tokens sold for $2.822 -> 0.34.
    size, price = _parse_fak_fill(
        {"orderID": "0xabc", "makingAmount": "8.302", "takingAmount": "2.822"},
        side="SELL",
    )
    assert size == pytest.approx(8.302)
    assert price == pytest.approx(2.822 / 8.302)


def test_parse_fak_fill_making_taking_buy_orientation():
    from cryptobot_polymarket.walker import _parse_fak_fill

    # BUY: we MAKE USDC, TAKE tokens. $1.99 spent for 2.519 tokens -> 0.79.
    size, price = _parse_fak_fill(
        {"orderID": "0xabc", "makingAmount": "1.99", "takingAmount": "2.519"},
        side="BUY",
    )
    assert size == pytest.approx(2.519)
    assert price == pytest.approx(1.99 / 2.519)


def test_parse_fak_fill_explicit_fields_win_over_making_taking():
    from cryptobot_polymarket.walker import _parse_fak_fill

    size, price = _parse_fak_fill(
        {
            "size_matched": "2.0",
            "avg_fill_price": "0.45",
            "makingAmount": "2.0",
            "takingAmount": "0.60",
        },
        side="SELL",
    )
    assert size == pytest.approx(2.0)
    assert price == pytest.approx(0.45)


def test_parse_fak_fill_rejects_out_of_range_vwap_and_empty():
    from cryptobot_polymarket.walker import _parse_fak_fill

    # USD/tokens >= 1.0 is not a binary price — refuse to fabricate.
    size, price = _parse_fak_fill(
        {"makingAmount": "2.0", "takingAmount": "2.5"}, side="SELL"
    )
    assert size is None and price is None
    assert _parse_fak_fill({}, side="SELL") == (None, None)
    assert _parse_fak_fill(None, side="BUY") == (None, None)
