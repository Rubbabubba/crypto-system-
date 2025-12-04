from __future__ import annotations

import logging
import os
from typing import Optional, Dict, Any

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c2"

# Env-configurable knobs for c2
C2_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C2_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

_raw = os.getenv("C2_MIN_ENTRY_SCORE")
C2_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

_raw = os.getenv("C2_MIN_ATR_PCT")
C2_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else None


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


class C2Strategy:
    """
    Trend continuation strategy (c2), long-only on spot.

    Behavior:
      - When FLAT:
          * Only "buy" ScanResult.action may open a long position.
          * "sell" actions are ignored as new entries.
      - When LONG:
          * "sell" actions may trigger exits (if C2_ENABLE_SIGNAL_EXIT is true).
      - Numeric TP/SL is primarily handled by the global risk engine
        (profit_lock, loss_zone). These hooks are provided for future
        per-strategy tuning.
    """

    STRAT_ID: str = STRAT_ID

    # --- Entry / Exit -------------------------------------------------

    def entry_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # Must be flat to open a new position
        if not _is_flat(position):
            return None

        # Respect selection from StrategyBook
        if not getattr(scan, "selected", False):
            return None

        # Long-only: ignore "sell" style actions as ENTRY
        if scan.action != "buy":
            return None

        # Notional must be positive
        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        # Optional per-strategy min score
        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C2_MIN_ENTRY_SCORE is not None and score < C2_MIN_ENTRY_SCORE:
            return None

        # Optional per-strategy ATR floor
        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C2_MIN_ATR_PCT is not None and atr_pct < C2_MIN_ATR_PCT:
            return None

        # Global per-symbol caps and loss-zone rules are applied later
        # by app.py using RiskEngine; we don't enforce them here.
        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="buy",
            kind="entry",
            notional=notional,
            reason=f"c2_trend_long_entry:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": score,
                "scan_atr_pct": atr_pct,
            },
        )
        return intent

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # No exit if not in a long
        if not _is_long(position):
            return None

        if not C2_ENABLE_SIGNAL_EXIT:
            return None

        # Only react to "sell" actions as exits for long positions
        if scan.action != "sell":
            return None

        # Request to close the entire position; higher layers can downsize.
        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="sell",
            kind="exit",
            notional=None,
            reason=f"c2_trend_long_exit:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )
        return intent

    # --- Per-strategy TP/SL hooks (currently delegate to global) -----

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # For now, use global profit_lock; no extra c2-specific TP.
        return None

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # For now, use global loss_zone; no extra c2-specific SL.
        return None

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        # No pyramiding for c2 yet.
        return False


# Module-level singleton expected by scheduler_core.get_strategy("c2")
c2 = C2Strategy()
