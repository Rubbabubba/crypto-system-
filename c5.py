from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c5"


def _is_flat(pos: Optional[PositionSnapshot]) -> bool:
    return (pos is None) or (abs(pos.qty) < 1e-10)


def _is_long(pos: Optional[PositionSnapshot]) -> bool:
    return (pos is not None) and (pos.qty > 1e-10)


def _is_short(pos: Optional[PositionSnapshot]) -> bool:
    return (pos is not None) and (pos.qty < -1e-10)


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v is not None else default
    except Exception:
        return default


def _normalize_unrealized_pct(pct: Optional[float]) -> Optional[float]:
    if pct is None:
        return None
    if abs(pct) > 1.0:
        return pct / 100.0
    return pct


class C5Strategy:
    """
    C5 — Alt momentum.

    Entry:
      - Uses sig_c5_alt_mom via StrategyBook (we just see ScanResult).

    Exit:
      - LONG: exit when signal != "buy".
      - SHORT: exit when signal != "sell".
      - Similar to c3 but intended for shorter holding periods.

    TP / SL:
      - Tighter thresholds:
        * C5_TAKE_PROFIT_PCT default 1.5  (≈ +1.5%)
        * C5_STOP_LOSS_PCT   default -2.0 (≈ -2%)
    """

    STRAT_ID = STRAT_ID

    def entry_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if not _is_flat(position):
            return None
        if not getattr(scan, "selected", False):
            return None
        if scan.action not in ("buy", "sell"):
            return None
        if float(getattr(scan, "notional", 0.0) or 0.0) <= 0.0:
            return None

        notional = float(scan.notional or 0.0)

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=scan.action,
            kind="entry",
            notional=notional,
            reason=scan.reason or "c5_entry_alt_mom",
            meta={
                "score": float(getattr(scan, "score", 0.0) or 0.0),
                "atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if _is_flat(position):
            return None
        if scan.symbol != position.symbol:
            return None

        if _is_long(position) and scan.action != "buy":
            side = "sell"
        elif _is_short(position) and scan.action != "sell":
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="exit",
            notional=None,
            reason="c5_exit_momentum_fade_or_reverse",
            meta={"raw_action": scan.action},
        )

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if _is_flat(position):
            return None
        upnl = _normalize_unrealized_pct(position.unrealized_pct)
        if upnl is None:
            return None

        tp_pct = _env_float("C5_TAKE_PROFIT_PCT", 1.5) / 100.0
        if upnl < tp_pct:
            return None

        if _is_long(position):
            side = "sell"
        elif _is_short(position):
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=position.symbol,
            side=side,
            kind="take_profit",
            notional=None,
            reason=f"c5_take_profit_{tp_pct*100:.2f}pct",
            meta={"unrealized_pct": upnl},
        )

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if _is_flat(position):
            return None
        upnl = _normalize_unrealized_pct(position.unrealized_pct)
        if upnl is None:
            return None

        sl_pct = _env_float("C5_STOP_LOSS_PCT", -2.0) / 100.0
        if upnl > sl_pct:
            return None

        if _is_long(position):
            side = "sell"
        elif _is_short(position):
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=position.symbol,
            side=side,
            kind="stop_loss",
            notional=None,
            reason=f"c5_stop_loss_{sl_pct*100:.2f}pct",
            meta={"unrealized_pct": upnl},
        )

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        return False


c5 = C5Strategy()
