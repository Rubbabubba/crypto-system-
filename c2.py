from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c2"

# --- Env-configurable knobs for c2 -----------------------------------------

C2_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C2_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

# Allow shorts? If false, c2 behaves like our previous long-only version.
C2_ALLOW_SHORTS = (
    str(os.getenv("C2_ALLOW_SHORTS", "true")).lower() in ("1", "true", "yes", "on")
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


def _is_short(position: Optional[PositionSnapshot]) -> bool:
    if position is None:
        return False
    qty = getattr(position, "qty", 0.0) or 0.0
    return qty < -1e-10


class C2Strategy:
    """
    Trend continuation strategy (c2).

    Previously long-only; now supports BOTH long and short, controlled by
    C2_ALLOW_SHORTS.

    Behavior:
      - When FLAT:
          * "buy" action -> open long
          * "sell" action -> open short (if C2_ALLOW_SHORTS)
      - When LONG:
          * "sell" actions may trigger exits (if C2_ENABLE_SIGNAL_EXIT)
      - When SHORT:
          * "buy" actions may trigger exits (if C2_ENABLE_SIGNAL_EXIT)
      - Numeric TP/SL is still primarily handled by global risk engine
        (profit_lock, loss_zone).
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

        if not getattr(scan, "selected", False):
            return None

        action = getattr(scan, "action", None)
        if action not in ("buy", "sell"):
            return None

        # Map scan.action to order side + "direction"
        if action == "buy":
            side = "buy"   # long entry
        else:  # "sell"
            if not C2_ALLOW_SHORTS:
                # We'll log this via scheduler_core telemetry (entry_skip).
                return None
            side = "sell"  # short entry

        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C2_MIN_ENTRY_SCORE is not None and score < C2_MIN_ENTRY_SCORE:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C2_MIN_ATR_PCT is not None and atr_pct < C2_MIN_ATR_PCT:
            return None

        # Global per-symbol caps and loss-zone rules are applied later
        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="entry",
            notional=notional,
            reason=f"c2_trend_entry:{action}:{scan.reason}",
            meta={
                "scan_action": action,
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
        if not C2_ENABLE_SIGNAL_EXIT:
            return None

        action = getattr(scan, "action", None)

        # LONG position: exit on sell-style signals
        if _is_long(position):
            if action != "sell":
                return None
            side = "sell"  # flatten long

        # SHORT position: exit on buy-style signals
        elif _is_short(position):
            if action != "buy":
                return None
            side = "buy"  # flatten short

        else:
            # Flat -> no exit
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="exit",
            notional=None,  # "flatten" semantics
            reason=f"c2_trend_exit:{action}:{scan.reason}",
            meta={
                "scan_action": action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    # --- Per-strategy TP/SL hooks (still delegate to global) ----------

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # Let global profit_lock handle numeric TP for now.
        return None

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # Let global loss_zone handle numeric SL for now.
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