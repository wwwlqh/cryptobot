"""Parse the strategy config JSON into typed Sleeve dataclasses.

Runtime overrides applied on load:

  - ``max_walk_price`` is always ``config.MAX_WALK_PRICE``;
  - ``bankroll`` comes from the JSON's top-level ``bankroll`` (the backtest
    base); live sizing uses config.INITIAL_BANKROLL.

Refuses to start if:

  - required fields are missing;
  - a sleeve's ``alloc`` exceeds 1.0;
  - ``entry_minute_set`` is empty;
  - the JSON's ``concurrent_exposure_cap_fraction`` differs from
    ``config.SHARED_EXPOSURE_CAP`` (drift check).
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
    """One sleeve's runtime config."""

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
    # A field of the deployed config that the dashboard's sleeve panel shows;
    # nothing scores against it.
    min_lut_n: int
    signal_source: str       # always "hgb" since 2026-07-31
    hour_bucket_set: str
    dow_set: str
    # Optional partial_ret_z gating bounds (null in the live config).
    partial_ret_z_floor: Optional[float] = None
    partial_ret_z_ceiling: Optional[float] = None
    # Model fields. Optional in the dataclass, but the loader requires the ones
    # that define the model (threshold, feature_groups, ask_bucket). Used by
    # the scoring path.
    threshold: Optional[float] = None          # pred_edge >= threshold gate
    feature_groups: Optional[str] = None       # e.g. "rf,price,identity,time"
    ask_bucket: Optional[str] = None           # "name:lo:hi"
    ask_bucket_ranges: tuple[tuple[float, float], ...] = ()  # parsed lo:hi pairs
    book_consumption_max: Optional[float] = None  # optional per-tick: notional <= depth*frac
    side_filter: str = "both"
    stake_frac: Optional[float] = None         # forensic mirror of capital_frac
    max_bet_usd: Optional[float] = None
    max_bet_usd_by_coin: dict[str, float] = field(default_factory=dict)
    # Per-coin stake multiplier (not a cap): scales the target notional per
    # coin before the max-bet clamp, like the backtest portfolio's coin
    # weights. An empty dict means 1.0 for every coin.
    coin_stake_mult: dict[str, float] = field(default_factory=dict)
    # Entry offsets in seconds from window open (e.g. 270..299 for the last 30s
    # of a 5m window). Non-empty makes this a sub-minute sleeve: entries
    # qualify on these offsets instead of entry_minute_set, dedup and slots are
    # per offset, and the sleeve is dropped at load time when
    # config.SUBMINUTE_ENABLED is off.
    checkpoint_offset_set: tuple[int, ...] = ()

    def max_bet_usd_for(self, coin: Optional[str] = None) -> Optional[float]:
        if coin is not None:
            value = self.max_bet_usd_by_coin.get(str(coin).strip().upper())
            if value is not None:
                return float(value)
        return self.max_bet_usd

    def coin_stake_mult_for(self, coin: Optional[str] = None) -> float:
        """Per-coin stake multiplier (1.0 if none is configured)."""
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
        # Apply the per-coin weight before the max-bet clamp, as the backtest
        # does.
        target *= self.coin_stake_mult_for(coin)
        cap = self.max_bet_usd_for(coin)
        if cap is not None:
            target = min(target, float(cap))
        return target

    @property
    def target_bet_size(self) -> float:
        """USD notional per bet under the configured bankroll."""
        return self.target_bet_size_for(config.INITIAL_BANKROLL)


@dataclass(frozen=True)
class WalletGroup:
    """One virtual exposure-cap group.

    Splits a single Polymarket funder into groups in software. ``cap_usd`` is
    the group's static USD ceiling; the gate shrinks it proportionally when the
    live funder balance falls below the sum of all group caps (formula in
    ``execution/gates.py``).
    """

    group_id: str
    members: tuple[str, ...]
    cap_usd: float
    # When True, the group's member wallets compound on one shared equity and
    # share one drawdown halt and one exposure cap, as in a single-wallet
    # backtest. Default False keeps wallets isolated.
    shared_equity: bool = False
    # When True, the group's exposure ceiling follows live equity instead of
    # the static ``cap_usd``, and there is no fixed funder-balance floor, so
    # the drawdown halt (``MAX_DRAWDOWN_HALT_FRAC``) is the only risk stop, as
    # in the backtest. ``cap_usd`` remains the display value and fallback.
    # Default False keeps the fixed cap. See ``funder_balance_floor_usd`` and
    # ``execution/gates.py:_gate_group_exposure``.
    cap_tracks_equity: bool = False

    def has_member(self, wallet_id: str) -> bool:
        return wallet_id in self.members


@dataclass(frozen=True)
class V6Portfolio:
    """The loaded sleeves plus the config's fingerprint."""

    sleeves: tuple[Sleeve, ...]
    bankroll: float
    shared_exposure_cap_fraction: float
    max_drawdown_halt_frac: float
    exposure_cap_fraction_by_wallet: dict[str, float]
    backtest_max_drawdown_frac_by_wallet: dict[str, float]
    config_sha256: str
    run_id: str
    label_source: str = ""
    # Exposure-cap groups and per-wallet default modes.
    wallet_groups: tuple[WalletGroup, ...] = ()
    wallet_default_modes: dict[str, str] = field(default_factory=dict)

    @property
    def market_sleeves(self) -> tuple[Sleeve, ...]:
        """Every sleeve that can place an order (the same as ``sleeves``;
        several callers use this name).
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
        """Return the group_id that owns ``wallet_id``, or None if ungrouped.

        ``record_bet`` uses it to stamp ``wallet_group`` on each ledger row,
        and ``_gate_group_exposure`` to find the group cap. With no groups
        configured, every lookup returns None and the group cap gate never
        fires.
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
        """Return the ``WalletGroup`` owning ``wallet_id`` if it is a
        shared-equity group, else None.

        Isolated wallets, ungrouped wallets and members of non-shared groups
        all return None and keep per-wallet accounting.
        """
        for group in self.wallet_groups:
            if group.shared_equity and group.has_member(wallet_id):
                return group
        return None

    def all_group_caps_sum_usd(self) -> float:
        """Sum of cap_usd across groups; the denominator of the proportional
        cap shrink.
        """
        return sum(g.cap_usd for g in self.wallet_groups)

    def funder_balance_floor_usd(self) -> float:
        """Minimum funder balance the bot needs to keep operating.

          * With ``wallet_groups``: the sum of cap_usd, so every group's cap is
            fundable at once.
          * With isolated wallets (no groups): the sum of each wallet's initial
            bankroll * exposure_cap_frac, so every wallet's maximum pending
            exposure is covered at once.

        A ``cap_tracks_equity`` group contributes 0, since the drawdown halt is
        its only stop.
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
        """Default mode for a wallet, used by ``seed_wallet_modes`` on first
        boot to fill ``wallet_mode_state``. Wallets without an entry default
        to 'DRY'.
        """
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
    """Read ``cfg[key]`` as Optional[float]; absent or null gives None.

    Numbers are coerced to float and optionally range-checked; strings and
    other types raise. For optional fields such as the partial_ret_z bounds and
    fill-price bands.
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
    """Parse an ``ask_bucket`` string into (lo, hi) range tuples.

    Grammar matches ``training.pmdata_target._parse_buckets`` and
    ``_ask_bucket_ok``: ``name:lo:hi``, optionally ``|``-joined for several
    ranges (e.g. ``all:0:1``). Returns () for None.
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
    """Parse the optional ``checkpoint_offset_set`` sleeve key.

    Accepts either:

      * a list of ints (offsets in seconds), or
      * a range string ``"start:end:step[,start:end:step...]"`` with inclusive
        ends, e.g. ``"270:299:1"``.

    Every offset must satisfy ``1 <= offset < interval_min*60`` (0 is the
    window boundary; anything at or past the interval end is a closed market).
    Returns a sorted, de-duplicated tuple.
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
        # Each sleeve's alloc is its own scale, so there is no ``sum(alloc) <=
        # 1`` rule, but a single value above 1.0 would size more than 100% of
        # the wallet per bet before capital_frac and is almost certainly a
        # typo.
        raise ValueError(
            f"v6_loader: sleeve {label!r} alloc={alloc_raw} exceeds 1.0"
        )

    # "hgb" is the only supported signal_source; anything else is rejected at
    # boot rather than loaded as a sleeve nothing can score.
    signal_source = str(_required(cfg, "signal_source", label))
    if signal_source != "hgb":
        raise ValueError(
            f"v6_loader: sleeve {label!r} signal_source must be hgb "
            f"(got {signal_source!r} — the lut|rf|blend scorers were retired "
            "with the pair sleeves on 2026-07-31)"
        )

    # Model-defining keys are required, so a sleeve can never run without
    # features.
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
    # Per-coin stake multiplier (scales the notional, not a cap). Validated
    # like max_bet_usd_by_coin, so a mistyped coin key raises instead of
    # defaulting to 1.0.
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
        # Cap at 1.0: the multiplier applies before the max-bet clamp and the
        # live config has no per-coin cap, so a typo like 10 instead of 1.0
        # would place a 10x bet with nothing to stop it. Backtest weights are
        # all <= 1.0.
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
        # book_consumption_max may be null: the edge-preserving walker then
        # stops on realized VWAP edge instead of a fixed depth fraction.

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
        # Always use config.MAX_WALK_PRICE, not the JSON's value.
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
    """Parse the optional ``wallet_groups`` top-level field.

      * ``raw is None`` -> empty tuple: every wallet is ungrouped,
        ``wallet_group_for()`` returns None and the group cap gate never fires.
      * Shape: ``{group_id: {"members": [...], "cap_usd": N}}``.

    Validation:

      * every member must be a known wallet_id;
      * each wallet appears in at most one group;
      * ``cap_usd > 0`` (unless ``cap_tracks_equity``);
      * ``group_id`` is a non-empty string.
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

        # Optional; absent means a fixed cap.
        cap_tracks_equity_raw = body.get("cap_tracks_equity", False)
        if not isinstance(cap_tracks_equity_raw, bool):
            raise ValueError(
                f"v6_loader: wallet_groups.{group_id}.cap_tracks_equity must be a bool"
            )

        # cap_usd is required (> 0) for a fixed-cap group. With
        # cap_tracks_equity it is optional: the ceiling is live equity, and
        # cap_usd is only the fallback if the balance fetch fails. Absent means
        # 0.0, which blocks placement (and funder_balance_unknown fires on that
        # tick).
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

        # Optional; absent means False.
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
    """Parse the optional ``wallet_default_modes`` top-level field.

    ``raw is None`` gives an empty dict, and ``seed_wallet_modes`` then
    defaults every wallet to DRY. Unknown wallet ids are rejected to catch
    typos at boot.
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
    """sha256 of the config re-serialized as canonical JSON (sorted keys,
    compact separators), so a changed file is detectable even if its mtime
    was kept. Hashes the already-parsed ``data``, so the fingerprint
    describes exactly the object that built the portfolio.
    """
    canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canon).hexdigest()


def load_sleeves(path: Path | str | None = None) -> V6Portfolio:
    """Read the strategy config and build the portfolio.

    Sleeves keep the JSON's order. ``config_sha256`` is logged at boot in
    ``pipeline_health`` so ledger rows can be traced to a config version.
    """
    p = Path(path) if path is not None else config.V6_CONFIG_PATH
    raw_bytes = p.read_bytes()
    data = json.loads(raw_bytes)

    sleeves_raw = data.get("sleeves")
    if not isinstance(sleeves_raw, list) or not sleeves_raw:
        raise ValueError(f"v6_loader: {p} missing non-empty 'sleeves' list")

    sleeves = tuple(_parse_sleeve(s) for s in sleeves_raw)

    # Sub-minute sleeves are dropped entirely when config.SUBMINUTE_ENABLED is
    # off. Their labels are removed from every wallet-keyed map before parsing,
    # so the "unknown wallet id" check still guards the rest of the config.
    # config_sha256 is computed over the effective (post-drop) config, so
    # flag-on and flag-off runs record different hashes.
    if not config.SUBMINUTE_ENABLED:
        _dropped = {s.label for s in sleeves if s.checkpoint_offset_set}
        if _dropped:
            sleeves = tuple(s for s in sleeves if s.label not in _dropped)
            if not sleeves:
                # The config holds only sub-minute sleeves, so with
                # SUBMINUTE_ENABLED off nothing would run. Fail with a clear
                # message instead of booting empty.
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

    # ``pair_sleeves`` is not supported; a config with a non-empty list is
    # rejected rather than running half a strategy.
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

    # wallet_groups and wallet_default_modes are optional; without them every
    # wallet is ungrouped and the group cap gate never fires.
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
