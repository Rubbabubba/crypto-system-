from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c3"


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


class C3Strategy:
    """
    C3 — Momentum / breakout fade.

    Entry:
      - Follow sig_c3_momentum when flat.

    Exit:
      - LONG: exit when signal != "buy" (momentum faded or reversed).
      - SHORT: exit when signal != "sell".
      - This captures both reversals *and* fades ("flat" signal).

    TP / SL:
      - Medium-tight thresholds:
        * C3_TAKE_PROFIT_PCT default 2.0  (≈ +2%)
        * C3_STOP_LOSS_PCT   default -3.0 (≈ -3%)
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
            reason=scan.reason or "c3_entry_momentum",
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

        # Momentum fading logic: exit whenever the signal no longer
        # points in our direction (including "flat").
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
            reason="c3_exit_momentum_fade_or_reverse",
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

        tp_pct = _env_float("C3_TAKE_PROFIT_PCT", 2.0) / 100.0
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
            reason=f"c3_take_profit_{tp_pct*100:.2f}pct",
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

        sl_pct = _env_float("C3_STOP_LOSS_PCT", -3.0) / 100.0
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
            reason=f"c3_stop_loss_{sl_pct*100:.2f}pct",
            meta={"unrealized_pct": upnl},
        )

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        return False


c3 = C3Strategy()
