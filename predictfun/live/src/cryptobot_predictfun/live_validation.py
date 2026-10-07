"""Staged predict.fun mainnet go-live validation (operator-supervised).

The submit wire (POST /v1/orders + /auth JWT) was implemented 2026-07-23 from
the dev.predict.fun docs + /spec but MUST be exercised end-to-end with real
(minimal) money before the scanner is armed. This CLI is that protocol, one
explicit stage per invocation, in order:

  1. ``--stage preflight``  read-only: creds + API liveness, predict-account
     vs signer-EOA balances (USDT custody is ON the predict account; BNB gas
     on the EOA), exchange approvals check, and a live BTC market's payload
     (feeRateBps / isNegRisk / isYieldBearing / precision) for eyeballing.
  2. ``--stage auth``       run the /auth JWT flow; print expiry; GET the
     connected account and CHECK it matches the configured addresses.
  3. ``--stage build``      build + sign the intended order and PRINT every
     struct field (maker/signer/tokenId/amounts). NO network write.
  4. ``--stage reject``     POST a deliberately unfillable FOK BUY (priced at
     the minimum tick, far below the book) — validates auth-on-POST + error
     parsing + that a kill leaves nothing resting. Costs $0 when it kills as
     designed; REAL-MONEY GATED anyway because it posts a signed order.
  5. ``--stage order``      ONE minimal real FAK BUY at/inside the strategy
     band on a liquid market. Prints the fill parse and the amountFilled
     unit cross-check. This is the money stage.
  6. ``--stage sell``       FOK SELL of --size tokens of --token (validates
     the stop-loss path + ERC-1155 approval).

Stages 4-6 require ``--i-am-risking-real-money``. Run on the box with the
live env loaded (systemd-style: ``sudo bash -c 'set -a; . /etc/cryptobot.env;
set +a; .venv/bin/python -m cryptobot_predictfun.live_validation ...'``).

Exit codes: OK / NOGO / USAGE (cryptobot.cli._exitcodes).
"""

from __future__ import annotations

import argparse
import sys
import time
from types import SimpleNamespace
from typing import Optional

from cryptobot.cli._exitcodes import NOGO, OK, USAGE

_REAL_MONEY_STAGES = {"reject", "order", "sell"}


def _cfg():
    from cryptobot import config

    return config


def _build_client():
    from cryptobot_predictfun.clob import (
        PredictFunClientUnavailable,
        PredictFunOrderClient,
    )

    cfg = _cfg()
    if not cfg.PREDICTFUN_PRIVATE_KEY:
        raise SystemExit("PREDICTFUN_PRIVATE_KEY is empty — load the live env first")
    if not cfg.PREDICTFUN_API_KEY:
        raise SystemExit("PREDICTFUN_API_KEY is empty — load the live env first")
    try:
        return PredictFunOrderClient(
            api_base=cfg.PREDICTFUN_API_BASE,
            api_key=cfg.PREDICTFUN_API_KEY,
            private_key=cfg.PREDICTFUN_PRIVATE_KEY,
            chain_id=cfg.PREDICTFUN_CHAIN_ID,
            predict_account=cfg.PREDICTFUN_PREDICT_ACCOUNT,
        )
    except PredictFunClientUnavailable as exc:
        raise SystemExit(f"order client unavailable: {exc}")


def _discover_market(coin: str, interval_min: int, *, min_remaining_s: int = 150):
    """Resolve the CURRENT (or next) open up-down market for coin/interval and
    register its tokens so ``lookup_token_order_meta`` works."""
    from cryptobot_predictfun import markets as pf_markets

    now = int(time.time())
    width = interval_min * 60
    for window_start in (now // width * width, now // width * width + width):
        window_end = window_start + width
        if window_end - now < min_remaining_s:
            continue
        market = pf_markets.fetch_active_market(
            coin, interval_min=interval_min, window_start_ts=window_start
        )
        if market is not None and not isinstance(market, pf_markets.FeeUnknown):
            return market
    return None


def _print_market(market) -> None:
    print(f"  market_id={market.market_id} slug={market.slug}")
    print(f"  UP token   = {market.yes_token_id}")
    print(f"  DOWN token = {market.no_token_id}")
    print(
        f"  feeRateBps={market.fee_rate_bps}  isNegRisk={market.is_neg_risk}  "
        f"isYieldBearing={market.is_yield_bearing}"
    )


def _stage_preflight() -> int:
    from cryptobot_predictfun import wallet as pfw
    from cryptobot_predictfun._http import probe_markets

    cfg = _cfg()
    fails = 0

    key = cfg.PREDICTFUN_PRIVATE_KEY
    eoa = pfw.derive_signer_address(key) if key else None
    predict_account = cfg.PREDICTFUN_PREDICT_ACCOUNT or None
    print("== identities ==")
    print(f"  signer EOA      : {eoa or 'MISSING (set PREDICTFUN_PRIVATE_KEY)'}")
    print(f"  predict account : {predict_account or '<none: EOA is maker>'}")
    if not eoa:
        fails += 1

    print("== API ==")
    code, ok, detail = probe_markets(cfg.PREDICTFUN_API_BASE, cfg.PREDICTFUN_API_KEY)
    print(f"  liveness: {detail}")
    if not ok:
        fails += 1

    print("== balances (funds custody = predict account when set) ==")
    token = pfw.resolve_collateral_token(
        int(cfg.PREDICTFUN_CHAIN_ID), override=cfg.PREDICTFUN_COLLATERAL_TOKEN
    )
    for label, addr in (("maker", predict_account or eoa), ("signer EOA", eoa)):
        if not addr:
            continue
        bal = pfw.read_wallet_balances(
            rpc_url=cfg.PREDICTFUN_RPC_URL,
            wallet_address=addr,
            collateral_token=token,
            collateral_decimals=int(cfg.PREDICTFUN_COLLATERAL_DECIMALS),
        )
        print(
            f"  {label:11s} {addr[:10]}…  USDT="
            f"{bal.collateral_balance if bal.collateral_balance is not None else '?'}"
            f"  BNB={bal.gas_balance_bnb if bal.gas_balance_bnb is not None else '?'}"
        )
    maker_addr = predict_account or eoa
    if maker_addr and token:
        bal = pfw.read_wallet_balances(
            rpc_url=cfg.PREDICTFUN_RPC_URL,
            wallet_address=maker_addr,
            collateral_token=token,
            collateral_decimals=int(cfg.PREDICTFUN_COLLATERAL_DECIMALS),
        )
        if not bal.collateral_balance:
            print("  ⚠️ maker holds NO collateral — deposit before stage order")
            fails += 1

    print("== market payload (BTC 15m) ==")
    market = _discover_market("BTC", 15)
    if market is None:
        print("  no open BTC 15m market resolvable (off-hours or API issue)")
        fails += 1
    else:
        _print_market(market)

    print("== approvals (exchange allowance / ERC-1155) ==")
    try:
        client = _build_client()
        from predict_sdk import ApprovalScope

        neg = bool(market.is_neg_risk) if market else False
        yb = bool(market.is_yield_bearing) if market else False
        steps = client._builder.get_approval_steps(  # noqa: SLF001 — ops CLI
            ApprovalScope(operation="TRADE", is_neg_risk=neg, is_yield_bearing=yb)
        )
        checks = client._builder.check_approvals(steps)  # noqa: SLF001
        for chk in checks:
            mark = "OK " if chk.satisfied else "MISSING"
            print(f"  [{mark}] {chk.step.label} ({chk.step.type})")
            if not chk.satisfied:
                fails += 1
        if any(not c.satisfied for c in checks):
            print(
                "  -> fix: place one tiny manual trade in the predict.fun UI "
                "(its frontend sets approvals), then re-run preflight"
            )
        client.close()
    except (Exception, SystemExit) as exc:  # noqa: BLE001 — _build_client
        # SystemExits on a missing key; preflight must still print a full
        # report (the missing key is itself one of the findings).
        print(f"  approvals check unavailable: {exc}")
        fails += 1

    print()
    print("preflight:", "FAILED — resolve the items above" if fails else "ALL CLEAR")
    return NOGO if fails else OK


def _stage_auth() -> int:
    client = _build_client()
    try:
        t0 = time.monotonic()
        header = client._jwt.bearer_header()  # noqa: SLF001 — ops CLI
        ms = (time.monotonic() - t0) * 1000.0
        token = header["Authorization"].split(" ", 1)[1]
        from cryptobot_predictfun.auth import _jwt_expiry_ts

        exp = _jwt_expiry_ts(token)
        ttl = f"{exp - time.time():.0f}s" if exp else "no exp claim (fallback TTL)"
        print(f"JWT obtained in {ms:.0f}ms; expires in {ttl}")

        headers, err = client._headers_with_bearer()  # noqa: SLF001
        if headers is None:
            print(f"header build failed: {err}")
            return NOGO
        resp = client._client.get(  # noqa: SLF001
            f"{client._api_base}/account", headers=headers, timeout=10.0  # noqa: SLF001
        )
        print(f"GET /account -> HTTP {resp.status_code}")
        if resp.status_code == 200:
            print(f"  {resp.text[:400]}")
            print(
                "  ^ CHECK: the account addresses above must match the "
                "configured predict account / signer EOA"
            )
            return OK
        return NOGO
    finally:
        client.close()


def _resolve_side_token(market, side: str) -> str:
    return str(market.yes_token_id if side.upper() == "UP" else market.no_token_id)


def _stage_build(args) -> int:
    from predict_sdk import Side

    client = _build_client()
    try:
        market = _discover_market(args.coin, args.interval)
        if market is None:
            print("no open market — retry inside a window")
            return NOGO
        _print_market(market)
        token = _resolve_side_token(market, args.side)
        price = float(args.price if args.price is not None else 0.50)
        ticks = int(round(price * 100))
        price_wei = client._price_wei(price)  # noqa: SLF001
        from decimal import Decimal

        shares_wei = int((Decimal(str(args.usd)) * 10**20) // ticks)
        signed = client._build_signed(  # noqa: SLF001
            side=Side.BUY,
            token_id=token,
            price_wei=price_wei,
            quantity_wei=shares_wei,
            fee_rate_bps=market.fee_rate_bps,
            is_neg_risk=market.is_neg_risk,
            is_yield_bearing=market.is_yield_bearing,
        )
        wire = client._order_wire(signed)  # noqa: SLF001
        print("\n== signed ContractOrder (EYEBALL EVERY FIELD) ==")
        for k, v in wire.items():
            if k == "signature":
                v = str(v)[:20] + "…"
            print(f"  {k:14s} = {v}")
        cfg = _cfg()
        expected_maker = (cfg.PREDICTFUN_PREDICT_ACCOUNT or "").lower()
        checks = [
            ("tokenId == intended outcome", str(wire["tokenId"]) == token),
            (
                "maker == predict account" if expected_maker else "maker == signer",
                (wire["maker"].lower() == expected_maker)
                if expected_maker
                else (wire["maker"].lower() == wire["signer"].lower()),
            ),
            (
                "makerAmount ≈ usd wei (BUY)",
                abs(int(wire["makerAmount"]) - int(float(args.usd) * 1e18))
                <= 10**16,  # 1 cent
            ),
        ]
        ok = True
        print("\n== assertions ==")
        for label, passed in checks:
            print(f"  [{'OK ' if passed else 'FAIL'}] {label}")
            ok = ok and passed
        print("\nNO order was posted (build stage).")
        return OK if ok else NOGO
    finally:
        client.close()


def _mk_signal(token: str, usd: float, price: float, fee_bps: float):
    return SimpleNamespace(
        poly_token_id=token,
        bet_size=usd,
        limit_price=price,
        fill_price=price,
        fee_rate_bps=fee_bps,
    )


def _ctf_balance(client, token: str) -> Optional[float]:
    """On-chain ERC-1155 balance of the outcome `token` for the maker — the
    ACTUAL shares held, for the fill-unit cross-check.

    (predict_sdk's ``balance_of`` returns the maker's COLLATERAL balance, not
    the CTF position — verified 2026-07-23 — so read the ConditionalTokens
    ERC-1155 ``balanceOf(account, id)`` directly: selector 0x00fdd58e.)
    """
    from cryptobot_predictfun import wallet as pfw

    cfg = _cfg()
    ct = pfw.resolve_ctf_contract(
        int(cfg.PREDICTFUN_CHAIN_ID), override=cfg.PREDICTFUN_CTF_CONTRACT
    )
    acct = (cfg.PREDICTFUN_PREDICT_ACCOUNT or "").strip() or pfw.derive_signer_address(
        cfg.PREDICTFUN_PRIVATE_KEY
    )
    if not ct or not acct:
        return None
    try:
        data = (
            "0x00fdd58e"
            + acct[2:].lower().rjust(64, "0")
            + format(int(token), "064x")
        )
        raw = pfw._eth_rpc(
            cfg.PREDICTFUN_RPC_URL, "eth_call", [{"to": ct, "data": data}, "latest"]
        )
        return raw / 1e18
    except Exception:  # noqa: BLE001 — optional cross-check only
        return None


def _stage_reject(args) -> int:
    client = _build_client()
    try:
        market = _discover_market(args.coin, args.interval)
        if market is None:
            print("no open market — retry inside a window")
            return NOGO
        _print_market(market)
        token = _resolve_side_token(market, args.side)
        # Minimum tick: a BUY capped at 0.01 sits far below any live favorite
        # book — a FOK at that cap must kill (or reject) and rest NOTHING.
        signal = _mk_signal(token, float(args.usd), 0.01, market.fee_rate_bps)
        print(f"\nposting deliberately-unfillable FAK BUY ${args.usd} @ 0.01 …")
        res = client.place_gtc_limit_order(
            signal, client_order_id=f"pfval-reject-{int(time.time())}"
        )
        print(f"  success={res.success} error_kind={res.error_kind}")
        print(f"  error={res.error}")
        print(f"  order_id={res.order_id} filled={res.filled_size_tokens}")
        if res.success and (res.filled_size_tokens or 0) > 0:
            print("  ✗ UNEXPECTED FILL at 0.01?! — stop and investigate")
            return NOGO
        print(
            "  expected outcome: kill/reject with error_kind in "
            "{no_liquidity, invalid_amount} and nothing resting"
        )
        return OK
    finally:
        client.close()


def _stage_order(args) -> int:
    client = _build_client()
    try:
        market = _discover_market(args.coin, args.interval)
        if market is None:
            print("no open market — retry inside a window")
            return NOGO
        _print_market(market)
        token = _resolve_side_token(market, args.side)
        if args.price is None:
            print("--price is required for stage order (set it AT/1-tick above best ask)")
            return USAGE
        price = float(args.price)
        pre = _ctf_balance(client, token)
        print(f"\nCTF balance before: {pre if pre is not None else 'unreadable'}")
        signal = _mk_signal(token, float(args.usd), price, market.fee_rate_bps)
        print(f"posting REAL FAK BUY ${args.usd} @ {price} on {market.slug} …")
        res = client.place_gtc_limit_order(
            signal, client_order_id=f"pfval-order-{int(time.time())}"
        )
        print(f"  success={res.success} error_kind={res.error_kind} error={res.error}")
        print(
            f"  order_id={res.order_id} filled_size={res.filled_size_tokens} "
            f"vwap={res.filled_price_vwap} value=${res.filled_value_usd}"
        )
        print(f"  sign_ms={res.sign_ms} post_ms={res.post_ms}")
        post = _ctf_balance(client, token)
        print(f"CTF balance after : {post if post is not None else 'unreadable'}")
        unit_verified = False
        if pre is not None and post is not None and res.filled_size_tokens:
            delta = post - pre
            parsed = res.filled_size_tokens
            # RELATIVE tolerance (6%): the on-chain delta is NET of the ~2% tent
            # fee taken in shares, while amountFilled is the GROSS matched size,
            # so an exact match is not expected. 6% still cleanly separates the
            # shares unit (~within-fee) from the catastrophic collateral-wei
            # confusion (a BUY's collateral is ~price× smaller = ~40% off).
            rel = abs(delta - parsed) / parsed if parsed else 1.0
            unit_verified = rel < 0.06
            print(
                f"  amountFilled unit cross-check: on-chain delta={delta:.6f} "
                f"vs parsed {parsed:.6f} (rel={rel*100:.1f}% — "
                f"{'OK, shares-wei confirmed' if unit_verified else 'MISMATCH, DO NOT ARM'})"
            )
        elif res.filled_size_tokens:
            print(
                "  amountFilled unit cross-check: CTF balance UNREADABLE — the "
                "shares-wei unit is NOT pinned; DO NOT ARM on this run"
            )
        print(
            "\nnext: hold to resolution; verify settlement grades the correct "
            "side, then validate redeem on the first WIN."
        )
        # Exit code ENFORCES the pin (advisor 2026-07-23): a fill that could not
        # be unit-verified against the on-chain CTF delta is a NOGO even though
        # the POST succeeded — the whole fill-accounting stack rides on the
        # shares-wei assumption, so the operator must not arm without it green.
        if not res.success:
            return NOGO
        if not unit_verified:
            print("stage order: fill NOT unit-verified — NOGO (do not arm)")
            return NOGO
        return OK
    finally:
        client.close()


def _stage_sell(args) -> int:
    client = _build_client()
    try:
        if not args.token or args.size is None or args.price is None:
            print("stage sell requires --token --size --price")
            return USAGE
        # The market that minted --token must be re-registered for meta lookup.
        market = _discover_market(args.coin, args.interval)
        if market is not None:
            _print_market(market)
        print(f"\nposting REAL FOK SELL {args.size} tokens @ {args.price} …")
        res = client.place_stop_loss_sell(
            token_id=str(args.token),
            size_tokens=float(args.size),
            price=float(args.price),
        )
        print(f"  success={res.success} error_kind={res.error_kind} error={res.error}")
        print(
            f"  order_id={res.order_id} filled_size={res.filled_size_tokens} "
            f"vwap={res.filled_price_vwap}"
        )
        # Validation REQUIRES a real fill: a no_liquidity KILL proves the wire
        # posts but NOT that the stop can actually sell (advisor 2026-07-23). At
        # 2x live stakes an unproven stop wire is unacceptable, so gate on an
        # actual FILL — retry on a slice/price that will cross the bid.
        if res.success:
            return OK
        print(
            "stage sell: SELL did not FILL — the stop-loss wire is UNPROVEN on "
            "this result; DO NOT ARM (retry on a slice that crosses the bid)"
        )
        return NOGO
    finally:
        client.close()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cryptobot_predictfun.live_validation",
        description=__doc__.split("\n\n")[0],
    )
    p.add_argument(
        "--stage",
        required=True,
        choices=["preflight", "auth", "build", "reject", "order", "sell"],
    )
    p.add_argument("--coin", default="BTC")
    p.add_argument("--interval", type=int, default=15)
    p.add_argument("--side", default="UP", choices=["UP", "DOWN"])
    p.add_argument("--usd", type=float, default=1.20, help="cash size for buy stages")
    p.add_argument("--price", type=float, default=None, help="cap price (2-dec tick)")
    p.add_argument("--token", default=None, help="stage sell: outcome onChainId")
    p.add_argument("--size", type=float, default=None, help="stage sell: tokens")
    p.add_argument(
        "--i-am-risking-real-money",
        action="store_true",
        help="required for stages that post signed orders (reject/order/sell)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.stage in _REAL_MONEY_STAGES and not args.i_am_risking_real_money:
        print(
            f"stage {args.stage} posts a signed order — re-run with "
            "--i-am-risking-real-money",
            file=sys.stderr,
        )
        return USAGE
    if args.stage == "preflight":
        return _stage_preflight()
    if args.stage == "auth":
        return _stage_auth()
    if args.stage == "build":
        return _stage_build(args)
    if args.stage == "reject":
        return _stage_reject(args)
    if args.stage == "order":
        return _stage_order(args)
    if args.stage == "sell":
        return _stage_sell(args)
    return USAGE


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
