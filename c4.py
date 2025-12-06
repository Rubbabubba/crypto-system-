from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c4"

# --- Env-configurable knobs for c4 -----------------------------------------

C4_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C4_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

C4_ALLOW_SHORTS = (
    str(os.getenv("C4_ALLOW_SHORTS", "true")).lower() in ("1", "true", "yes", "on")
)

_raw = os.getenv("C4_MIN_ENTRY_SCORE")
C4_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

_raw = os.getenv("C4_MIN_ATR_PCT")
C4_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else None


def _is_flat(position: Optional[PositionSnapshot]) -> bool:
    if position is None:
        return True
    qty = getattr(position, "qty", 0.0) or 0.0
    return abs(qty) < 1e-10


def _is_long(position: Optional[PositionSnapshot]) -> bool:
    if position is None:
        return False
    qty = getattr(position, "qty", 0.0) or 0.0
    return qty > 1e-10


def _is_short(position: Optional[PositionSnapshot]) -> bool:
    if position is None:
        return False
    qty = getattr(position, "qty", 0.0) or 0.0
    return qty < -1e-10


class C4Strategy:
    """
    Strategy c4 – breakout / trend-acceleration, now two-sided.

    Semantics:
      - When FLAT:
          * "buy" action -> long breakout
          * "sell" action -> short breakdown (if C4_ALLOW_SHORTS)
      - When LONG:
          * "sell" actions may trigger exits (if C4_ENABLE_SIGNAL_EXIT)
      - When SHORT:
          * "buy" actions may trigger exits (if C4_ENABLE_SIGNAL_EXIT)
    """

    STRAT_ID: str = STRAT_ID

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

        action = getattr(scan, "action", None)
        if action not in ("buy", "sell"):
            return None

        if action == "buy":
            side = "buy"
        else:
            if not C4_ALLOW_SHORTS:
                return None
            side = "sell"

        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C4_MIN_ENTRY_SCORE is not None and score < C4_MIN_ENTRY_SCORE:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C4_MIN_ATR_PCT is not None and atr_pct < C4_MIN_ATR_PCT:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="entry",
            notional=notional,
            reason=f"c4_breakout_entry:{action}:{scan.reason}",
            meta={
                "scan_action": action,
                "scan_reason": scan.reason,
                "scan_score": score,
                "scan_atr_pct": atr_pct,
            },
        )

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if not C4_ENABLE_SIGNAL_EXIT:
            return None

        action = getattr(scan, "action", None)

        if _is_long(position):
            if action != "sell":
                return None
            side = "sell"  # exit long
        elif _is_short(position):
            if action != "buy":
                return None
            side = "buy"   # exit short
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="exit",
            notional=None,
            reason=f"c4_breakout_exit:{action}:{scan.reason}",
            meta={
                "scan_action": action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        return None

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        return None

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        # We can add pyramiding later.
        return False


c4 = C4Strategy()