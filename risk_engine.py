from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from strategy_api import PositionSnapshot, RiskContext, OrderIntent


@dataclass
class RiskCaps:
    max_notional_per_symbol: Dict[str, float]
    max_units_per_symbol: Dict[str, float]


@dataclass
class ProfitLockConfig:
    take_profit_pct: Optional[float]  # % (e.g. 1.5)


@dataclass
class LossZoneConfig:
    no_rebuy_below_pct: Optional[float]  # % (e.g. -3.0)
    stop_loss_pct: Optional[float]       # % (e.g. -3.0)


@dataclass
class DailyFlattenConfig:
    enabled: bool
    flatten_hour_local: int
    flatten_minute_local: int


@dataclass
class TimeMultiplierConfig:
    day_start_hour_local: int
    night_start_hour_local: int
    day_multiplier: float
    night_multiplier: float


@dataclass
class AtrFloorConfig:
    floors: Dict[str, float]          # e.g. {"tier1": 0.6, "tier2": 0.9, "tier3": 1.2}
    tiers: Dict[str, Any]             # from risk.json["tiers"] (symbol membership per tier)


@dataclass
class RiskConfig:
    fee_rate_pct: float
    edge_multiple_vs_fee: float
    risk_caps: RiskCaps
    profit_lock: ProfitLockConfig
    loss_zone: LossZoneConfig
    daily_flatten: DailyFlattenConfig
    time_multiplier: TimeMultiplierConfig
    atr_floor: AtrFloorConfig
    raw: Dict[str, Any]


def _load_risk_dict() -> Dict[str, Any]:
    """
    Standalone loader for risk.json so risk_engine doesn't depend on app.py.
    """
    try:
        cfg_dir = os.getenv("POLICY_CFG_DIR", "policy_config")
        path = Path(cfg_dir) / "risk.json"
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _get(d: Any, key: str, default: Any = None) -> Any:
    return d.get(key, default) if isinstance(d, dict) else default


def _as_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return default


class RiskEngine:
    """
    Central risk engine for:
      - per-symbol caps
      - daily flatten cutoffs
      - profit-lock
      - loss-zone stop-loss & no-rebuy
      - ATR floor flattening by tier
    """

    def __init__(self, risk_cfg: Optional[Dict[str, Any]] = None):
        if risk_cfg is None:
            risk_cfg = _load_risk_dict()
        self._cfg_raw = risk_cfg or {}
        self.config = self._parse_config(self._cfg_raw)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_config(self, cfg: Dict[str, Any]) -> RiskConfig:
        fee_rate_pct = _as_float(_get(cfg, "fee_rate_pct", 0.26), 0.26)
        edge_multiple_vs_fee = _as_float(_get(cfg, "edge_multiple_vs_fee", 3.0), 3.0)

        risk_caps_cfg = _get(cfg, "risk_caps", {}) or {}
        rcaps = RiskCaps(
            max_notional_per_symbol=_get(risk_caps_cfg, "max_notional_per_symbol", {}) or {},
            max_units_per_symbol=_get(risk_caps_cfg, "max_units_per_symbol", {}) or {},
        )

        pl_cfg = _get(cfg, "profit_lock", {}) or {}
        profit_lock = ProfitLockConfig(
            take_profit_pct=_as_float(pl_cfg.get("take_profit_pct"), None),
        )

        lz_cfg = _get(cfg, "loss_zone", {}) or {}
        loss_zone = LossZoneConfig(
            no_rebuy_below_pct=_as_float(lz_cfg.get("no_rebuy_below_pct"), None),
            stop_loss_pct=_as_float(lz_cfg.get("stop_loss_pct"), None),
        )

        df_cfg = _get(cfg, "daily_flatten", {}) or {}
        daily_flatten = DailyFlattenConfig(
            enabled=bool(df_cfg.get("enabled", False)),
            flatten_hour_local=int(df_cfg.get("flatten_hour_local", 23)),
            flatten_minute_local=int(df_cfg.get("flatten_minute_local", 0)),
        )

        tm_cfg = _get(cfg, "time_multipliers", {}) or {}
        dn_cfg = _get(tm_cfg, "day_night", {}) or {}
        time_multiplier = TimeMultiplierConfig(
            day_start_hour_local=int(dn_cfg.get("day_start_hour_local", 6)),
            night_start_hour_local=int(dn_cfg.get("night_start_hour_local", 19)),
            day_multiplier=float(dn_cfg.get("day_multiplier", 1.0)),
            night_multiplier=float(dn_cfg.get("night_multiplier", 1.0)),
        )

        atr_cfg = _get(cfg, "atr_floor_pct", {}) or {}
        tiers_cfg = _get(cfg, "tiers", {}) or {}
        atr_floor = AtrFloorConfig(
            floors={k: float(v) for k, v in atr_cfg.items() if v is not None},
            tiers=tiers_cfg,
        )

        return RiskConfig(
            fee_rate_pct=fee_rate_pct,
            edge_multiple_vs_fee=edge_multiple_vs_fee,
            risk_caps=rcaps,
            profit_lock=profit_lock,
            loss_zone=loss_zone,
            daily_flatten=daily_flatten,
            time_multiplier=time_multiplier,
            atr_floor=atr_floor,
            raw=cfg,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def risk_context(self) -> RiskContext:
        return RiskContext(
            fee_rate_pct=self.config.fee_rate_pct / 100.0,
            edge_multiple_vs_fee=self.config.edge_multiple_vs_fee,
            raw_config=self._cfg_raw,
        )

    def time_multiplier_for_now(self, now: Optional[dt.datetime] = None) -> float:
        cfg = self.config.time_multiplier
        try:
            if now is None:
                try:
                    from zoneinfo import ZoneInfo
                    tz = os.getenv("TZ", "UTC")
                    now = dt.datetime.now(ZoneInfo(tz))
                except Exception:
                    now = dt.datetime.utcnow()
            hour = now.hour
            if cfg.day_start_hour_local <= hour < cfg.night_start_hour_local:
                return cfg.day_multiplier
            return cfg.night_multiplier
        except Exception:
            return 1.0

    def is_daily_flatten_active(self, now: Optional[dt.datetime] = None) -> bool:
        df = self.config.daily_flatten
        if not df.enabled:
            return False
        try:
            if now is None:
                try:
                    from zoneinfo import ZoneInfo
                    tz = os.getenv("TZ", "UTC")
                    now = dt.datetime.now(ZoneInfo(tz))
                except Exception:
                    now = dt.datetime.utcnow()
            return (now.hour, now.minute) >= (
                df.flatten_hour_local,
                df.flatten_minute_local,
            )
        except Exception:
            return False

    def compute_unrealized_pct(
        self,
        pos: PositionSnapshot,
        last_price_fn: Callable[[str], float],
    ) -> Optional[float]:
        if pos is None or abs(pos.qty) < 1e-10 or pos.avg_price is None:
            return None
        try:
            px = float(last_price_fn(pos.symbol))
            avg = float(pos.avg_price or 0.0)
        except Exception:
            return None
        if px <= 0.0 or avg <= 0.0:
            return None

        if pos.qty > 0:
            return (px - avg) / avg * 100.0
        else:
            return (avg - px) / avg * 100.0

    # ------------------------------------------------------------------
    # Per-symbol caps
    # ------------------------------------------------------------------

    def _norm_symbol_key(self, symbol: str) -> Tuple[str, str]:
        upper = symbol.upper()
        norm = "".join(ch for ch in upper if ch.isalnum())
        return upper, norm

    def symbol_caps(self, symbol: str, now: Optional[dt.datetime] = None) -> Tuple[Optional[float], Optional[float]]:
        """
        Return (max_notional_cap, max_units_cap) for the given symbol,
        *after* applying the time-of-day multiplier.
        """
        upper, norm = self._norm_symbol_key(symbol)
        rcaps = self.config.risk_caps

        cap_notional = rcaps.max_notional_per_symbol.get(
            upper,
            rcaps.max_notional_per_symbol.get(
                norm,
                rcaps.max_notional_per_symbol.get("default"),
            ),
        )
        cap_units = rcaps.max_units_per_symbol.get(
            upper,
            rcaps.max_units_per_symbol.get(
                norm,
                rcaps.max_units_per_symbol.get("default"),
            ),
        )

        if cap_notional is not None:
            cap_notional = float(cap_notional)
            cap_notional *= self.time_multiplier_for_now(now)

        if cap_units is not None:
            cap_units = float(cap_units)

        return cap_notional, cap_units

    def enforce_symbol_cap(
        self,
        symbol: str,
        strat: str,
        pos: PositionSnapshot,
        notional_value: float,
        last_price_fn: Callable[[str], float],
        now: Optional[dt.datetime] = None,
    ) -> Tuple[bool, float, Optional[str]]:
        """
        Enforce per-symbol caps for an *open* intent.

        Returns: (allowed, adjusted_notional, reason_if_blocked)
        """
        cap_notional, cap_units = self.symbol_caps(symbol, now)
        if cap_notional is None and cap_units is None:
            return True, notional_value, None

        qty_here = float(pos.qty or 0.0)
        px_here = float(last_price_fn(symbol) or 0.0)
        cur_notional = abs(qty_here) * px_here if px_here > 0 else 0.0

        # notional-based cap
        if cap_notional is not None and px_here > 0.0:
            max_additional = float(cap_notional) - float(cur_notional)
            if max_additional <= 0:
                return False, 0.0, f"cap_notional:{cur_notional:.2f}>={cap_notional}"
            if notional_value > max_additional:
                # clip to remaining capacity
                return True, max_additional, f"clipped_notional_to_cap:{max_additional:.2f}"

        # units-based cap (optional)
        if cap_units is not None and px_here > 0.0:
            cur_units = abs(qty_here)
            max_additional_units = float(cap_units) - float(cur_units)
            if max_additional_units <= 0:
                return False, 0.0, f"cap_units:{cur_units:.8f}>={cap_units}"

        return True, notional_value, None

    # ------------------------------------------------------------------
    # Global exits / loss-zone rules
    # ------------------------------------------------------------------

    def apply_global_exit_rules(
        self,
        pos: PositionSnapshot,
        unrealized_pct: Optional[float],
        atr_pct: Optional[float],
        now: Optional[dt.datetime] = None,
    ) -> Optional[str]:
        """
        Decide if global risk wants this position FLATTENED, and why.

        Returns:
            reason string if we should flatten, else None.
        """
        if pos is None or abs(pos.qty) < 1e-10:
            return None

        # 1) Daily flatten
        if self.is_daily_flatten_active(now):
            return "daily_flatten"

        if unrealized_pct is None:
            return None

        # 2) Profit-lock
        pl = self.config.profit_lock
        if pl.take_profit_pct is not None and unrealized_pct >= pl.take_profit_pct:
            return f"profit_lock:{unrealized_pct:.2f}>={pl.take_profit_pct:.2f}"

        # 3) Stop-loss
        lz = self.config.loss_zone
        if lz.stop_loss_pct is not None and unrealized_pct <= lz.stop_loss_pct:
            return f"stop_loss:{unrealized_pct:.2f}<={lz.stop_loss_pct:.2f}"

        # 4) ATR floor flatten (if ATR too small for this tier)
        if atr_pct is not None:
            tier_name = self._tier_for_symbol(pos.symbol)
            if tier_name is not None:
                floor = self.config.atr_floor.floors.get(tier_name)
                if floor is not None and atr_pct < float(floor):
                    return f"atr_floor_flat:{atr_pct:.4f}<{float(floor):.4f}"

        return None

    def _tier_for_symbol(self, symbol: str) -> Optional[str]:
        upper, norm = self._norm_symbol_key(symbol)
        tiers = self.config.atr_floor.tiers or {}
        for tname, symbols_list in tiers.items():
            if isinstance(symbols_list, str):
                symbols_iter = [symbols_list]
            else:
                symbols_iter = list(symbols_list) if symbols_list is not None else []
            if upper in symbols_iter or norm in symbols_iter:
                return tname
        return None

    def is_loss_zone_norebuy_block(
        self,
        unrealized_pct: Optional[float],
        is_entry_side: bool,
    ) -> bool:
        """
        Returns True if the loss-zone "no-rebuy-below" rule should block
        a *new* entry in the same direction.
        """
        if unrealized_pct is None:
            return False
        if not is_entry_side:
            return False
        lz = self.config.loss_zone
        if lz.no_rebuy_below_pct is None:
            return False
        return unrealized_pct <= lz.no_rebuy_below_pct
