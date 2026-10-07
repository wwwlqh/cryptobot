"""Parse `dry_run_recommended.json` into typed Sleeve dataclasses.

Each sleeve mirrors the `Config` shape in `backtest/optimize.py:80-110` but
applies cryptobot's runtime overrides:
  - `max_walk_price` is forcibly set to `config.MAX_WALK_PRICE` (R-NEW-4)
  - `bankroll` is read from the JSON's top-level `bankroll` field and pinned
    to the research backtest base; live sizing uses config.INITIAL_BANKROLL

Refuses to start if:
  - required fields are missing
  - `sum(alloc)` exceeds 1.0 (over-allocation)
  - `entry_minute_set` is empty
  - the JSON's default `concurrent_exposure_cap_fraction` differs from
    `config.SHARED_EXPOSURE_CAP` (D20 boot-time drift detection)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from cryptobot import config


_VALID_WALLET_MODES: frozenset[str] = frozenset({"DRY", "LIVE"})
_VALID_COINS: frozenset[str] = frozenset({"btc", "eth", "sol", "xrp"})


@dataclass(frozen=True)
class Sleeve:
    """One sleeve's runtime config — direct port of the backtest's `Config`."""

    label: str
    alloc: float
    interval: str            # "5m" / "15m"
    interval_min: int
    side_strategy: str       # "model_signed"
    entry_minute_set: tuple[int, ...]
    entry_strategy: str      # "first_qualifying"
    min_edge: float
    max_edge: float
    min_fill_price: float
    max_fill_price: float
    min_agreement: int
    capital_frac: float
    max_walk_price: float    # OVERRIDDEN to config.MAX_WALK_PRICE
    slippage_haircut: float
    # Retired-LUT coverage gate. Kept because it is a field of the DEPLOYED
    # live artifact and is rendered by the dashboard sleeve panel
    # (observability/snapshot_builder.py); nothing scores against it any more.
    min_lut_n: int
    signal_source: str       # always "hgb" since 2026-07-31
    hour_bucket_set: str
    dow_set: str
    # Chunk-CV adds partial_ret_z gating on v6 (both null in current
    # staging configs; runtime branch added in U4 of the deploy plan).
    partial_ret_z_floor: Optional[float] = None
    partial_ret_z_ceiling: Optional[float] = None
    # HGB-sleeve fields. Declared optional because the dataclass predates the
    # hgb-only era; the loader now REQUIRES the model-defining ones
    # (threshold / feature_groups / ask_bucket) since every sleeve is hgb.
    # The HGB scoring path (strategy/hgb_*) consumes them.
    threshold: Optional[float] = None          # pred_edge >= threshold gate
    feature_groups: Optional[str] = None       # e.g. "rf,price,identity,time"
    ask_bucket: Optional[str] = None           # "name:lo:hi"
    ask_bucket_ranges: tuple[tuple[float, float], ...] = ()  # parsed lo:hi pairs
    book_consumption_max: Optional[float] = None  # optional per-tick: notional <= depth*frac
    side_filter: str = "both"
    stake_frac: Optional[float] = None         # forensic mirror of capital_frac
    max_bet_usd: Optional[float] = None
    max_bet_usd_by_coin: dict[str, float] = field(default_factory=dict)
    # Per-coin stake MULTIPLIER (not a cap): scales the target notional per coin
    # before the max-bet clamp. Mirrors the backtest portfolio's coin weights
    # (e.g. {"BTC": 0.95, "ETH": 0.85, "XRP": 0.20}). Empty dict => uniform (1.0
    # for every coin), so legacy sleeves are unaffected.
    coin_stake_mult: dict[str, float] = field(default_factory=dict)
    # B1/B2 sub-minute checkpoint add-on (checkpoint-grid combo port).
    # Offsets in SECONDS from window open at which this sleeve may enter
    # (e.g. step5 = 5,10,...,295 plus step1last = 270..299 for a 5m window).
    # Empty (default) = a normal minute-boundary sleeve; non-empty makes
    # this a sub-minute sleeve: entry qualification switches from
    # entry_minute_set to these offsets, dedup/slots become per-offset, and
    # the sleeve is dropped entirely at load time when
    # config.SUBMINUTE_ENABLED is off.
    checkpoint_offset_set: tuple[int, ...] = ()

    def max_bet_usd_for(self, coin: Optional[str] = None) -> Optional[float]:
        if coin is not None:
            value = self.max_bet_usd_by_coin.get(str(coin).strip().upper())
            if value is not None:
                return float(value)
        return self.max_bet_usd

    def coin_stake_mult_for(self, coin: Optional[str] = None) -> float:
        """Per-coin stake multiplier (1.0 if no weight is configured for it)."""
        if coin is not None and self.coin_stake_mult:
            return float(self.coin_stake_mult.get(str(coin).strip().upper(), 1.0))
        return 1.0

    def target_bet_size_for(self, bankroll: float, coin: Optional[str] = None) -> float:
        """USD notional per bet for this sleeve under the supplied bankroll."""
        target = (
            max(0.0, float(bankroll))
            * self.alloc
            * self.capital_frac
            * config.BET_SIZE_MULTIPLIER
        )
        # Per-coin stake weight (e.g. BTC 0.95 / ETH 0.85 / XRP 0.20) BEFORE the
        # max-bet clamp, matching the backtest portfolio's coin weighting.
        target *= self.coin_stake_mult_for(coin)
        cap = self.max_bet_usd_for(coin)
        if cap is not None:
            target = min(target, float(cap))
        return target

    @property
    def target_bet_size(self) -> float:
        """USD notional per bet for this sleeve under the $100 backtest base."""
        return self.target_bet_size_for(config.INITIAL_BANKROLL)


@dataclass(frozen=True)
class WalletGroup:
    """One virtual exposure-cap group (D-LIVE-2 / D-LIVE-4).

    The 2026-05-24 per-strategy LIVE deploy plan splits the single $200
    Polymarket funder into two virtual $100 groups in software. The
    `cap_usd` value is the static USD ceiling per group; the effective
    cap shrinks proportionally at gate-evaluation time when the live
    funder balance drops below `SUM(all_group_caps)` (D-LIVE-4 formula
    lives in `execution/gates.py`, not the loader).
    """

    group_id: str
    members: tuple[str, ...]
    cap_usd: float
    # Phase 5 (exp044 plan 2026-05-29-001 option B1): when True, the group's
    # member wallets compound on ONE shared equity, share ONE drawdown halt,
    # and share ONE exposure cap — faithful to exp044's single-wallet
    # backtest. OPT-IN: default False so isolated wallets and non-shared
    # groups (legacy lut/pairs/all_4 topologies) stay byte-for-byte unchanged.
    shared_equity: bool = False
    # 2026-07-05 backtest-exposure mode: when True, the group's concurrent
    # exposure ceiling TRACKS live equity (grows with the account) instead of
    # the static ``cap_usd``, and the fixed funder-balance floor is retired so
    # the 35% drawdown halt (``MAX_DRAWDOWN_HALT_FRAC``) is the SOLE risk stop —
    # matching the backtest, which sizes/deploys on full equity and stops only
    # at max-DD. ``cap_usd`` stays the display/fallback anchor. OPT-IN: default
    # False keeps the fixed-cap topology (freeze floor + static ceiling)
    # byte-for-byte unchanged. See ``funder_balance_floor_usd`` +
    # ``execution/gates.py:_gate_group_exposure``.
    cap_tracks_equity: bool = False

    def has_member(self, wallet_id: str) -> bool:
        return wallet_id in self.members


@dataclass(frozen=True)
class V6Portfolio:
    """Container for the loaded sleeves + the originating artifact fingerprint."""

    sleeves: tuple[Sleeve, ...]
    bankroll: float
    shared_exposure_cap_fraction: float
    max_drawdown_halt_frac: float
    exposure_cap_fraction_by_wallet: dict[str, float]
    backtest_max_drawdown_frac_by_wallet: dict[str, float]
    config_sha256: str
    run_id: str
    label_source: str = ""
    # Per-strategy LIVE deploy plan §U3 (D-LIVE-2, D-LIVE-4).
    wallet_groups: tuple[WalletGroup, ...] = ()
    wallet_default_modes: dict[str, str] = field(default_factory=dict)

    @property
    def market_sleeves(self) -> tuple[Sleeve, ...]:
        """Every sleeve that can place a market order.

        Identical to ``sleeves`` since the 2026-07-31 pair-sleeve retirement;
        kept as the name seven call sites (db/connection, capital,
        operator_control, snapshot_builder, main) already speak.
        """
        return self.sleeves

    def exposure_cap_fraction_for(self, wallet_id: str) -> float:
        return float(
            self.exposure_cap_fraction_by_wallet.get(
                wallet_id, self.shared_exposure_cap_fraction
            )
        )

    def backtest_max_drawdown_frac_for(self, wallet_id: str) -> Optional[float]:
        value = self.backtest_max_drawdown_frac_by_wallet.get(wallet_id)
        return float(value) if value is not None else None

    def wallet_group_for(self, wallet_id: str) -> Optional[str]:
        """Resolve the group_id that owns `wallet_id`, or None if ungrouped.

        Used by `record_bet` to stamp the `wallet_group` column on every
        ledger row + by `_gate_group_exposure` to read the group cap.
        Backwards-compat: when `wallet_groups=()` (legacy artifact), every
        lookup returns None and the group cap gate never fires.
        """
        for group in self.wallet_groups:
            if group.has_member(wallet_id):
                return group.group_id
        return None

    def group_by_id(self, group_id: str) -> Optional[WalletGroup]:
        for group in self.wallet_groups:
            if group.group_id == group_id:
                return group
        return None

    def shared_equity_group_for(self, wallet_id: str) -> Optional[WalletGroup]:
        """Return the `WalletGroup` owning `wallet_id` IF it is a
        shared-equity group, else None (Phase 5).

        Isolated wallets, ungrouped wallets, and members of non-shared
        groups (legacy lut/pairs/all_4) all return None — those callers
        keep the per-wallet isolated semantics unchanged.
        """
        for group in self.wallet_groups:
            if group.shared_equity and group.has_member(wallet_id):
                return group
        return None

    def all_group_caps_sum_usd(self) -> float:
        """SUM(cap_usd) across every group; denominator of the D-LIVE-4
        proportional shrinkage formula."""
        return sum(g.cap_usd for g in self.wallet_groups)

    def funder_balance_floor_usd(self) -> float:
        """Minimum funder balance the bot needs to keep operating.

        Two topologies, two formulas:
          * `wallet_groups`: SUM(cap_usd) — every group's cap must be
            simultaneously fundable.
          * isolated wallets (`wallet_groups=()`): SUM(per-wallet live
            initial bankroll * exposure_cap_frac) — each wallet's max pending
            exposure must be coverable concurrently. Correctness finding
            #22 (CORR-006): previously this was always
            `all_group_caps_sum_usd()`, so the all_4 isolated topology
            (with empty wallet_groups) silently bypassed the
            funder-balance gate. Now the isolated path computes a
            structurally-equivalent floor.

        2026-07-05 backtest-exposure mode: a `cap_tracks_equity` group has no
        fixed funder floor (35% DD is the sole stop), so it contributes 0 —
        retiring the freeze while non-tracking groups keep their cap floor.
        """
        if self.wallet_groups:
            return sum(
                g.cap_usd for g in self.wallet_groups if not g.cap_tracks_equity
            )
        floor = 0.0
        for sleeve in self.market_sleeves:
            wallet_id = getattr(sleeve, "label", None)
            if not isinstance(wallet_id, str):
                continue
            cap_frac = self.exposure_cap_fraction_for(wallet_id)
            floor += float(config.INITIAL_BANKROLL) * float(cap_frac)
        return floor

    def default_mode_for(self, wallet_id: str) -> str:
        """`wallet_default_modes` is consulted by `seed_wallet_modes` on
        first boot to seed `wallet_mode_state`. Defaults to 'DRY' when the
        wallet has no explicit entry — safest possible default."""
        return self.wallet_default_modes.get(wallet_id, "DRY")


def _required(cfg: dict, key: str, sleeve_label: str) -> object:
    if key not in cfg:
        raise KeyError(f"v6_loader: sleeve {sleeve_label!r} missing required key {key!r}")
    return cfg[key]


def _parse_entry_set(raw: object, label: str) -> tuple[int, ...]:
    if not isinstance(raw, list) or len(raw) == 0:
        raise ValueError(f"v6_loader: sleeve {label!r} entry_minute_set must be non-empty list")
    return tuple(int(x) for x in raw)


def _optional_float(
    cfg: dict,
    key: str,
    label: str,
    *,
    lo: Optional[float] = None,
    hi: Optional[float] = None,
) -> Optional[float]:
    """Read `cfg[key]` as Optional[float]. Absent or null → None.

    Numeric values are coerced to float and (optionally) range-checked.
    Strings or non-numeric types raise. Used for new chunk-CV fields
    (partial_ret_z_floor/ceiling, max_combined_edge, fill-price bands)
    that are null in some staging configs and need to remain Optional
    in the dataclass.
    """
    if key not in cfg:
        return None
    raw = cfg[key]
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(
            f"v6_loader: sleeve {label!r} {key} must be numeric or null"
        )
    value = float(raw)
    if lo is not None and value < lo:
        raise ValueError(
            f"v6_loader: sleeve {label!r} {key}={value} below floor {lo}"
        )
    if hi is not None and value > hi:
        raise ValueError(
            f"v6_loader: sleeve {label!r} {key}={value} above ceiling {hi}"
        )
    return value


def _parse_ask_bucket(raw: object, label: str) -> tuple[tuple[float, float], ...]:
    """Parse an exp044 `ask_bucket` string into (lo, hi) range tuples.

    Grammar mirrors `training.pmdata_target._parse_buckets` (relocated there
    2026-08-01 from the deleted `training.residual_matrix`) and the equivalent
    `training.pmdata_target._ask_bucket_ok`, which is what the live-scorer pool
    build actually calls: `name:lo:hi`,
    optionally `|`-joined for multiple ranges (exp044 uses a single range:
    `all:0:1`, `mid:0.4:0.6`). Returns () for None so non-hgb sleeves are inert.
    """
    if raw is None:
        return ()
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"v6_loader: sleeve {label!r} ask_bucket must be a non-empty string")
    ranges: list[tuple[float, float]] = []
    for piece in raw.split("|"):
        parts = piece.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"v6_loader: sleeve {label!r} ask_bucket piece {piece!r} not name:lo:hi"
            )
        lo, hi = float(parts[1]), float(parts[2])
        if not (0.0 <= lo < hi <= 1.0):
            raise ValueError(
                f"v6_loader: sleeve {label!r} ask_bucket range ({lo},{hi}) must satisfy 0<=lo<hi<=1"
            )
        ranges.append((lo, hi))
    return tuple(ranges)


def _parse_checkpoint_offsets(
    raw: object, label: str, interval_min: int
) -> tuple[int, ...]:
    """Parse the optional ``checkpoint_offset_set`` sleeve key (B1/B2).

    Accepts either:
      * a list of ints (explicit offsets in seconds), or
      * a compact range-spec string ``"start:end:step[,start:end:step...]"``
        with INCLUSIVE ends — e.g. the 5m step5+step1last grid is
        ``"5:295:5,270:299:1"``.

    Validation is fail-closed: every offset must satisfy
    ``1 <= offset < interval_min*60`` (offset 0 is the window boundary —
    that's the baseline sleeves' territory; >= interval end is a market
    that no longer exists). Returns a sorted de-duplicated tuple.
    """
    if raw is None:
        return ()
    window_s = interval_min * 60
    offsets: set[int] = set()
    if isinstance(raw, str):
        if not raw.strip():
            raise ValueError(
                f"v6_loader: sleeve {label!r} checkpoint_offset_set string must be non-empty"
            )
        for piece in raw.split(","):
            parts = piece.strip().split(":")
            if len(parts) != 3:
                raise ValueError(
                    f"v6_loader: sleeve {label!r} checkpoint_offset_set piece "
                    f"{piece!r} not start:end:step"
                )
            try:
                start, end, step = int(parts[0]), int(parts[1]), int(parts[2])
            except ValueError as exc:
                raise ValueError(
                    f"v6_loader: sleeve {label!r} checkpoint_offset_set piece "
                    f"{piece!r} must be integers"
                ) from exc
            if step <= 0 or end < start:
                raise ValueError(
                    f"v6_loader: sleeve {label!r} checkpoint_offset_set piece "
                    f"{piece!r} needs step>0 and end>=start"
                )
            offsets.update(range(start, end + 1, step))
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, bool) or not isinstance(item, int):
                raise ValueError(
                    f"v6_loader: sleeve {label!r} checkpoint_offset_set items must be ints"
                )
            offsets.add(int(item))
    else:
        raise ValueError(
            f"v6_loader: sleeve {label!r} checkpoint_offset_set must be a list "
            "of ints or a 'start:end:step' range-spec string"
        )
    for off in offsets:
        if not (1 <= off < window_s):
            raise ValueError(
                f"v6_loader: sleeve {label!r} checkpoint offset {off} outside "
                f"[1, {window_s - 1}] for interval {interval_min}m"
            )
    return tuple(sorted(offsets))


def _parse_sleeve(raw: dict) -> Sleeve:
    label = raw.get("label")
    if not isinstance(label, str) or not label:
        raise ValueError("v6_loader: sleeve missing 'label'")
    cfg = raw.get("config")
    if not isinstance(cfg, dict):
        raise ValueError(f"v6_loader: sleeve {label!r} missing 'config' dict")

    interval = str(_required(cfg, "interval", label))
    if interval not in ("5m", "15m"):
        raise ValueError(f"v6_loader: sleeve {label!r} interval must be 5m or 15m")
    interval_min = 5 if interval == "5m" else 15

    entry_set = _parse_entry_set(_required(cfg, "entry_minute_set", label), label)

    alloc_raw = raw.get("alloc")
    if alloc_raw is None or not isinstance(alloc_raw, (int, float)) or alloc_raw <= 0:
        raise ValueError(f"v6_loader: sleeve {label!r} alloc must be positive")
    if float(alloc_raw) > 1.0 + 1e-9:
        # Under per-strategy isolated wallets each sleeve's alloc is its
        # standalone portfolio scale (e.g. all_4 scale_v6_5m=0.842), so the
        # old shared-bankroll `sum(alloc) <= 1` semantic is dead. Per-sleeve
        # values are still capped at 1.0 — anything above means the operator
        # would size > 100% of the wallet realized capital per bet before
        # multiplying by capital_frac, which is almost certainly a typo.
        raise ValueError(
            f"v6_loader: sleeve {label!r} alloc={alloc_raw} exceeds 1.0"
        )

    # 2026-07-31 (exp044 phase 2): the lut|rf|blend scorers went with the
    # decision_table/LUT chain, so "hgb" is the ONLY servable signal_source.
    # A legacy artifact is now rejected at boot rather than parsed into a
    # sleeve nothing can score.
    signal_source = str(_required(cfg, "signal_source", label))
    if signal_source != "hgb":
        raise ValueError(
            f"v6_loader: sleeve {label!r} signal_source must be hgb "
            f"(got {signal_source!r} — the lut|rf|blend scorers were retired "
            "with the pair sleeves on 2026-07-31)"
        )

    # Model-defining keys. Required, so a sleeve can never silently run
    # without features (fail-closed).
    feature_groups = cfg.get("feature_groups")
    if feature_groups is not None and not isinstance(feature_groups, str):
        raise ValueError(f"v6_loader: sleeve {label!r} feature_groups must be a string")
    ask_bucket = cfg.get("ask_bucket")
    ask_bucket_ranges = _parse_ask_bucket(ask_bucket, label)
    side_filter_raw = cfg.get("side_filter", "both")
    if not isinstance(side_filter_raw, str) or side_filter_raw.lower() not in ("both", "up", "down"):
        raise ValueError(f"v6_loader: sleeve {label!r} side_filter must be both|up|down")
    side_filter = side_filter_raw.lower()
    threshold = _optional_float(cfg, "threshold", label, lo=-1.0, hi=1.0)
    book_consumption_max = _optional_float(cfg, "book_consumption_max", label, lo=0.0, hi=1.0)
    stake_frac = _optional_float(cfg, "stake_frac", label, lo=0.0, hi=1.0)
    max_bet_usd = _optional_float(cfg, "max_bet_usd", label, lo=0.0)
    raw_max_by_coin = cfg.get("max_bet_usd_by_coin", {})
    if raw_max_by_coin is None:
        raw_max_by_coin = {}
    if not isinstance(raw_max_by_coin, dict):
        raise ValueError(
            f"v6_loader: sleeve {label!r} max_bet_usd_by_coin must be a coin object"
        )
    max_bet_usd_by_coin: dict[str, float] = {}
    for coin, value in raw_max_by_coin.items():
        coin_key = str(coin).strip().upper()
        if coin_key not in {"BTC", "ETH", "SOL", "XRP"}:
            raise ValueError(
                f"v6_loader: sleeve {label!r} max_bet_usd_by_coin has unknown coin {coin!r}"
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) < 0:
            raise ValueError(
                f"v6_loader: sleeve {label!r} max_bet_usd_by_coin[{coin_key}] must be >= 0"
            )
        max_bet_usd_by_coin[coin_key] = float(value)
    # Per-coin stake MULTIPLIER (scales the notional, not a cap). Fail-closed,
    # same validation as max_bet_usd_by_coin so a typo'd coin key raises rather
    # than silently falling back to 1.0.
    raw_coin_stake_mult = cfg.get("coin_stake_mult", {})
    if raw_coin_stake_mult is None:
        raw_coin_stake_mult = {}
    if not isinstance(raw_coin_stake_mult, dict):
        raise ValueError(
            f"v6_loader: sleeve {label!r} coin_stake_mult must be a coin object"
        )
    coin_stake_mult: dict[str, float] = {}
    for coin, value in raw_coin_stake_mult.items():
        coin_key = str(coin).strip().upper()
        if coin_key not in {"BTC", "ETH", "SOL", "XRP"}:
            raise ValueError(
                f"v6_loader: sleeve {label!r} coin_stake_mult has unknown coin {coin!r}"
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) < 0:
            raise ValueError(
                f"v6_loader: sleeve {label!r} coin_stake_mult[{coin_key}] must be >= 0"
            )
        # Symmetric ceiling: coin_stake_mult scales target notional BEFORE the
        # max-bet clamp (target_bet_size_for), and the live artifact ships no
        # per-coin cap (max_bet_usd_by_coin: {}), so a fat-finger like 10
        # instead of 1.0 would place a 10x-oversized real-money bet with no
        # backstop. Backtest portfolio weights are all <= 1.0, so cap at 1.0
        # to fail closed at boot — mirrors the alloc guard above.
        if float(value) > 1.0 + 1e-9:
            raise ValueError(
                f"v6_loader: sleeve {label!r} coin_stake_mult[{coin_key}]={value} "
                "exceeds 1.0"
            )
        coin_stake_mult[coin_key] = float(value)

    if signal_source == "hgb":
        if not feature_groups:
            raise ValueError(
                f"v6_loader: hgb sleeve {label!r} requires a non-empty feature_groups"
            )
        if threshold is None:
            raise ValueError(f"v6_loader: hgb sleeve {label!r} requires a threshold")
        if not ask_bucket_ranges:
            raise ValueError(f"v6_loader: hgb sleeve {label!r} requires an ask_bucket")
        # HGB may intentionally leave book_consumption_max null for the
        # edge-preserving top-up regime: the walker stops on realized VWAP
        # edge instead of a fixed depth-consumption fraction.

    max_edge_val = float(_required(cfg, "max_edge", label))

    return Sleeve(
        label=label,
        alloc=float(alloc_raw),
        interval=interval,
        interval_min=interval_min,
        side_strategy=str(_required(cfg, "side", label)),
        entry_minute_set=entry_set,
        entry_strategy=str(_required(cfg, "entry_strategy", label)),
        min_edge=float(_required(cfg, "min_edge", label)),
        max_edge=max_edge_val,
        min_fill_price=float(_required(cfg, "min_fill_price", label)),
        max_fill_price=float(_required(cfg, "max_fill_price", label)),
        min_agreement=int(_required(cfg, "min_agreement", label)),
        capital_frac=float(_required(cfg, "capital_frac", label)),
        # R-NEW-4: override the JSON's max_walk_price with config.MAX_WALK_PRICE.
        max_walk_price=config.MAX_WALK_PRICE,
        slippage_haircut=float(_required(cfg, "slippage_haircut", label)),
        min_lut_n=int(_required(cfg, "min_lut_n", label)),
        signal_source=signal_source,
        hour_bucket_set=str(cfg.get("hour_bucket_set", "all")),
        dow_set=str(cfg.get("dow_set", "all")),
        partial_ret_z_floor=_optional_float(
            cfg, "partial_ret_z_floor", label, lo=-10.0, hi=10.0
        ),
        partial_ret_z_ceiling=_optional_float(
            cfg, "partial_ret_z_ceiling", label, lo=-10.0, hi=10.0
        ),
        threshold=threshold,
        feature_groups=feature_groups,
        ask_bucket=(str(ask_bucket) if ask_bucket is not None else None),
        ask_bucket_ranges=ask_bucket_ranges,
        book_consumption_max=book_consumption_max,
        side_filter=side_filter,
        stake_frac=stake_frac,
        max_bet_usd=max_bet_usd,
        max_bet_usd_by_coin=max_bet_usd_by_coin,
        coin_stake_mult=coin_stake_mult,
        checkpoint_offset_set=_parse_checkpoint_offsets(
            cfg.get("checkpoint_offset_set"), label, interval_min
        ),
    )


def _parse_wallet_float_map(
    raw: object,
    *,
    field_name: str,
    valid_wallet_ids: set[str],
    lower_bound: float,
    upper_bound: float | None,
) -> dict[str, float]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"v6_loader: {field_name} must be a wallet-id object")

    parsed: dict[str, float] = {}
    for key, value in raw.items():
        wallet_id = str(key)
        if wallet_id not in valid_wallet_ids:
            raise ValueError(
                f"v6_loader: {field_name} has unknown wallet id {wallet_id!r}"
            )
        if not isinstance(value, (int, float)):
            raise ValueError(
                f"v6_loader: {field_name}.{wallet_id} must be numeric"
            )
        numeric = float(value)
        if numeric < lower_bound or (upper_bound is not None and numeric > upper_bound):
            hi = "" if upper_bound is None else f" and <= {upper_bound}"
            raise ValueError(
                f"v6_loader: {field_name}.{wallet_id} must be >= {lower_bound}{hi}"
            )
        parsed[wallet_id] = numeric
    return parsed


def _parse_wallet_groups(
    raw: object, *, valid_wallet_ids: set[str]
) -> tuple[WalletGroup, ...]:
    """Parse the new `wallet_groups` top-level field (D-LIVE-2, D-LIVE-4).

    Backwards-compat:
      * `raw is None` (legacy artifact) → empty tuple. Every wallet is
        ungrouped; `wallet_group_for()` returns None and the group cap
        gate never fires.
      * The new shape is `{group_id: {"members": [...], "cap_usd": N}}`
        per D-LIVE-24 (flat cap_usd, no `bankroll × cap_frac` fraction).

    Validation:
      * Every member must be a known wallet_id.
      * Every wallet appears in at most ONE group.
      * `cap_usd > 0`.
      * `group_id` must be a non-empty string.
    """
    if raw is None:
        return ()
    if not isinstance(raw, dict):
        raise ValueError("v6_loader: wallet_groups must be an object when present")

    groups: list[WalletGroup] = []
    seen_members: set[str] = set()
    for group_id, body in raw.items():
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("v6_loader: wallet_groups keys must be non-empty strings")
        if not isinstance(body, dict):
            raise ValueError(
                f"v6_loader: wallet_groups.{group_id} must be an object"
            )

        members_raw = body.get("members")
        if not isinstance(members_raw, list) or not members_raw:
            raise ValueError(
                f"v6_loader: wallet_groups.{group_id}.members must be a non-empty list"
            )
        members: list[str] = []
        for m in members_raw:
            if not isinstance(m, str):
                raise ValueError(
                    f"v6_loader: wallet_groups.{group_id}.members items must be strings"
                )
            if m not in valid_wallet_ids:
                raise ValueError(
                    f"v6_loader: wallet_groups.{group_id}.members has unknown wallet id {m!r}"
                )
            if m in seen_members:
                raise ValueError(
                    f"v6_loader: wallet id {m!r} appears in more than one wallet_group"
                )
            seen_members.add(m)
            members.append(m)

        # 2026-07-05: opt-in backtest-exposure flag. Absent → False (fixed cap).
        cap_tracks_equity_raw = body.get("cap_tracks_equity", False)
        if not isinstance(cap_tracks_equity_raw, bool):
            raise ValueError(
                f"v6_loader: wallet_groups.{group_id}.cap_tracks_equity must be a bool"
            )

        # cap_usd is REQUIRED (> 0) for a fixed-cap group. Under
        # cap_tracks_equity it is OPTIONAL: the concurrent-exposure ceiling IS
        # live equity (nothing hardcoded), so cap_usd is only the fail-closed
        # fallback used if the balance fetch fails — absent → 0.0, which blocks
        # placement (and funder_balance_unknown also fires on that tick).
        cap_raw = body.get("cap_usd")
        if cap_tracks_equity_raw:
            cap_usd = (
                float(cap_raw)
                if isinstance(cap_raw, (int, float)) and float(cap_raw) > 0
                else 0.0
            )
        else:
            if not isinstance(cap_raw, (int, float)):
                raise ValueError(
                    f"v6_loader: wallet_groups.{group_id}.cap_usd must be numeric"
                )
            cap_usd = float(cap_raw)
            if cap_usd <= 0:
                raise ValueError(
                    f"v6_loader: wallet_groups.{group_id}.cap_usd must be > 0"
                )

        # Phase 5: opt-in shared-equity flag. Absent (legacy artifacts) → False.
        shared_equity_raw = body.get("shared_equity", False)
        if not isinstance(shared_equity_raw, bool):
            raise ValueError(
                f"v6_loader: wallet_groups.{group_id}.shared_equity must be a bool"
            )

        groups.append(
            WalletGroup(
                group_id=group_id,
                members=tuple(members),
                cap_usd=cap_usd,
                shared_equity=shared_equity_raw,
                cap_tracks_equity=cap_tracks_equity_raw,
            )
        )

    return tuple(groups)


def _parse_wallet_default_modes(
    raw: object, *, valid_wallet_ids: set[str]
) -> dict[str, str]:
    """Parse the new `wallet_default_modes` top-level field (D-LIVE-1).

    Backwards-compat: `raw is None` (legacy artifact) → empty dict, and
    `seed_wallet_modes` defaults every wallet to DRY. Keys outside
    `valid_wallet_ids` are rejected to catch typos at boot.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(
            "v6_loader: wallet_default_modes must be an object when present"
        )

    parsed: dict[str, str] = {}
    for key, value in raw.items():
        wallet_id = str(key)
        if wallet_id not in valid_wallet_ids:
            raise ValueError(
                f"v6_loader: wallet_default_modes has unknown wallet id {wallet_id!r}"
            )
        if not isinstance(value, str) or value.upper() not in _VALID_WALLET_MODES:
            raise ValueError(
                f"v6_loader: wallet_default_modes.{wallet_id} must be 'DRY' or 'LIVE'"
            )
        parsed[wallet_id] = value.upper()
    return parsed


def _canonical_json_sha256(data: object) -> str:
    """sha256 of a canonical-JSON re-serialization of the artifact.

    Canonical = sorted keys + compact separators. Lets the operator detect
    silent swaps that preserved the file mtime but changed the content.

    Takes the ALREADY-PARSED ``data`` object (not raw bytes) so the
    fingerprint provably describes the exact object that built the
    portfolio — the previous re-parse of raw_bytes was a redundant second
    json.loads that could, in principle, diverge from the parse that built
    the sleeves. The resulting hash value is byte-identical to before.
    """
    canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canon).hexdigest()


def load_sleeves(path: Path | str | None = None) -> V6Portfolio:
    """Read `dry_run_recommended.json` and build the portfolio.

    Returns a `V6Portfolio` whose `sleeves` order matches the JSON order
    (v6_5m first, v6_15m second). The `config_sha256` is logged at boot
    via `pipeline_health` (U14) so post-hoc analysis can attribute ledger
    rows to a specific config generation.
    """
    p = Path(path) if path is not None else config.V6_CONFIG_PATH
    raw_bytes = p.read_bytes()
    data = json.loads(raw_bytes)

    sleeves_raw = data.get("sleeves")
    if not isinstance(sleeves_raw, list) or not sleeves_raw:
        raise ValueError(f"v6_loader: {p} missing non-empty 'sleeves' list")

    sleeves = tuple(_parse_sleeve(s) for s in sleeves_raw)
    # Per-strategy isolated wallets: each sleeve's `alloc` is its standalone
    # portfolio scale (all_4: v6_5m=0.842, v6_15m=0.676). The legacy
    # `sum(alloc) <= 1.0` guard assumed sleeves shared one bankroll and is
    # no longer meaningful — the per-sleeve `alloc <= 1.0` check inside
    # `_parse_sleeve` is the live invariant. See deploy plan U1.

    # B2 flag-gating: sub-minute checkpoint sleeves ship in the artifact but
    # are dropped ENTIRELY when config.SUBMINUTE_ENABLED is off — the
    # baseline portfolio then behaves exactly like the pre-port bot (no
    # extra wallets, groups, caps, or signal rows). The dropped labels are
    # stripped from every wallet-scoped raw map BEFORE parsing so the
    # fail-closed "unknown wallet id" validation keeps protecting the
    # remaining config. NOTE: config_sha256 is computed over the EFFECTIVE
    # (post-drop) config — flag-on and flag-off deploys attribute ledger
    # rows to distinct shas, which is the honest behavior.
    if not config.SUBMINUTE_ENABLED:
        _dropped = {s.label for s in sleeves if s.checkpoint_offset_set}
        if _dropped:
            sleeves = tuple(s for s in sleeves if s.label not in _dropped)
            if not sleeves:
                # v2 step1last ships a checkpoint-ONLY config (no baseline
                # minute sleeves). With SUBMINUTE_ENABLED off every sleeve is
                # dropped and there is nothing to run — fail CLOSED with an
                # actionable message rather than booting an empty portfolio.
                raise ValueError(
                    "v6_loader: every sleeve is a sub-minute checkpoint sleeve "
                    "and CRYPTOBOT_SUBMINUTE_ENABLED is off — nothing to run. "
                    "The v2 step1last config requires "
                    "CRYPTOBOT_SUBMINUTE_ENABLED=1; set it (and keep "
                    "CRYPTOBOT_V6_CONFIG pointed at the v2 file)."
                )
            data["sleeves"] = [
                s for s in sleeves_raw
                if isinstance(s, dict) and s.get("label") not in _dropped
            ]
            for _field in (
                "wallet_exposure_cap_fracs",
                "backtest_max_drawdown_fracs",
                "wallet_default_modes",
            ):
                _m = data.get(_field)
                if isinstance(_m, dict):
                    for _label in _dropped:
                        _m.pop(_label, None)
            _groups_raw = data.get("wallet_groups")
            if isinstance(_groups_raw, dict):
                for _gid in list(_groups_raw.keys()):
                    _body = _groups_raw.get(_gid)
                    if not isinstance(_body, dict):
                        continue
                    _members = _body.get("members")
                    if isinstance(_members, list):
                        _kept = [m for m in _members if m not in _dropped]
                        if _kept:
                            _body["members"] = _kept
                        else:
                            _groups_raw.pop(_gid)

    # 2026-07-31: `pair_sleeves` is no longer parsed at all — the pair
    # candidate/commit path is gone. A legacy artifact that still carries a
    # NON-EMPTY list is rejected rather than silently running half a strategy.
    pair_sleeves_raw = data.get("pair_sleeves") or []
    if pair_sleeves_raw:
        raise ValueError(
            f"v6_loader: {p} declares pair_sleeves, but the pair-sleeve "
            "subsystem was retired on 2026-07-31 — remove the key"
        )
    wallet_ids = {s.label for s in sleeves}

    bankroll_raw = data.get("bankroll", config.BACKTEST_BANKROLL)
    bankroll = float(bankroll_raw)
    if abs(bankroll - config.BACKTEST_BANKROLL) > 1e-6:
        raise ValueError(
            f"v6_loader: bankroll={bankroll} disagrees with config.BACKTEST_BANKROLL="
            f"{config.BACKTEST_BANKROLL} for the research backtest base"
        )

    cap_frac = float(data.get("concurrent_exposure_cap_fraction", config.SHARED_EXPOSURE_CAP))
    if abs(cap_frac - config.SHARED_EXPOSURE_CAP) > 1e-6:
        raise ValueError(
            f"v6_loader: concurrent_exposure_cap_fraction={cap_frac} disagrees with "
            f"config.SHARED_EXPOSURE_CAP={config.SHARED_EXPOSURE_CAP} — D20 drift"
        )

    wallet_cap_fracs = _parse_wallet_float_map(
        data.get("wallet_exposure_cap_fracs"),
        field_name="wallet_exposure_cap_fracs",
        valid_wallet_ids=wallet_ids,
        lower_bound=0.0,
        upper_bound=1.0,
    )
    wallet_backtest_dd_fracs = _parse_wallet_float_map(
        data.get("backtest_max_drawdown_fracs"),
        field_name="backtest_max_drawdown_fracs",
        valid_wallet_ids=wallet_ids,
        lower_bound=0.0,
        upper_bound=None,
    )

    dd_halt = float(
        data.get(
            "max_drawdown_halt_frac",
            config.MAX_DRAWDOWN_HALT_FRAC,
        )
    )
    if abs(dd_halt - config.MAX_DRAWDOWN_HALT_FRAC) > 1e-6:
        raise ValueError(
            f"v6_loader: max_drawdown_halt_frac={dd_halt} disagrees with "
            f"config.MAX_DRAWDOWN_HALT_FRAC={config.MAX_DRAWDOWN_HALT_FRAC}"
        )

    # Per-strategy LIVE plan §U3 (D-LIVE-2, D-LIVE-4): wallet_groups +
    # wallet_default_modes are additive on the artifact. Legacy artifacts
    # without these fields parse cleanly — wallet_groups defaults to ()
    # and the group cap gate never fires.
    wallet_groups = _parse_wallet_groups(
        data.get("wallet_groups"), valid_wallet_ids=wallet_ids
    )
    wallet_default_modes = _parse_wallet_default_modes(
        data.get("wallet_default_modes"), valid_wallet_ids=wallet_ids
    )

    return V6Portfolio(
        sleeves=sleeves,
        bankroll=bankroll,
        shared_exposure_cap_fraction=cap_frac,
        max_drawdown_halt_frac=dd_halt,
        exposure_cap_fraction_by_wallet=wallet_cap_fracs,
        backtest_max_drawdown_frac_by_wallet=wallet_backtest_dd_fracs,
        config_sha256=_canonical_json_sha256(data),
        run_id=str(data.get("run_id", "")),
        label_source=str(data.get("label_source", "")),
        wallet_groups=wallet_groups,
        wallet_default_modes=wallet_default_modes,
    )
