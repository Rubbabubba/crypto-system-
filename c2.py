from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c2"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

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


# ---------------------------------------------------------------------
# C2 — Trend continuation (pullback in trend)
# ---------------------------------------------------------------------

class C2Strategy:
    """
    C2 — Trend continuation on pullbacks.

    Entry:
      - Go with the trend (sig_c2_trend) when flat.

    Exit:
      - Only on **explicit reverse signal**:
        * long -> exit when signal = "sell"
        * short -> exit when signal = "buy"
      - Trend "flat" / no signal by itself does NOT exit; we let global
        risk (profit_lock, stop_loss, daily_flatten) handle that.

    TP / SL:
      - Looser than c1, tuned for trends:
        * C2_TAKE_PROFIT_PCT default 3.0  (≈ +3%)
        * C2_STOP_LOSS_PCT   default -4.0 (≈ -4%)
    """

    STRAT_ID = STRAT_ID

    # ---- Entry ------------------------------------------------------

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
            reason=scan.reason or "c2_entry_trend",
            meta={
                "score": float(getattr(scan, "score", 0.0) or 0.0),
                "atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    # ---- Exit on trend reversal -------------------------------------

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

        # Long -> exit only on explicit "sell" signal
        if _is_long(position) and scan.action == "sell":
            side = "sell"
        # Short -> exit only on explicit "buy" signal
        elif _is_short(position) and scan.action == "buy":
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="exit",
            notional=None,
            reason="c2_exit_trend_reversal",
            meta={
                "raw_action": scan.action,
                "score": float(getattr(scan, "score", 0.0) or 0.0),
            },
        )

    # ---- Per-strategy TP --------------------------------------------

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

        tp_pct = _env_float("C2_TAKE_PROFIT_PCT", 3.0) / 100.0
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
            reason=f"c2_take_profit_{tp_pct*100:.2f}pct",
            meta={"unrealized_pct": upnl},
        )

    # ---- Per-strategy SL --------------------------------------------

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

        sl_pct = _env_float("C2_STOP_LOSS_PCT", -4.0) / 100.0
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
            reason=f"c2_stop_loss_{sl_pct*100:.2f}pct",
            meta={"unrealized_pct": upnl},
        )

    # ---- Scaling (off for now) --------------------------------------

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        # We can later allow pyramiding in strong trends
        return False


# Singleton for scheduler_core
c2 = C2Strategy()
