from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c5"

# --- Env-configurable knobs for c5 -----------------------------------------

C5_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C5_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

_raw = os.getenv("C5_MIN_ENTRY_SCORE")
C5_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

_raw = os.getenv("C5_MIN_ATR_PCT")
C5_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else None

# When already long, require a higher score to allow scale-ins
_raw = os.getenv("C5_SCALE_MIN_SCORE")
C5_SCALE_MIN_SCORE = float(_raw) if _raw not in (None, "") else 1.0  # default fairly high


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


class C5Strategy:
    """
    Strategy c5 – volume / conviction-based trend scaler, long-only.

    Semantics on spot:
      - When FLAT:
          * Only "buy" actions may open a new long.
      - When LONG:
          * "buy" actions with strong scores may allow scale-ins (should_scale).
          * "sell" actions may trigger exits (if C5_ENABLE_SIGNAL_EXIT is true).
      - Numeric TP/SL remains primarily global (profit_lock, loss_zone).
    """

    STRAT_ID: str = STRAT_ID

    # ------------------------------------------------------------------ #
    # Entry logic
    # ------------------------------------------------------------------ #

    def entry_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # Must be flat to open the *initial* position
        if not _is_flat(position):
            return None

        if not getattr(scan, "selected", False):
            return None

        if scan.action != "buy":
            return None

        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C5_MIN_ENTRY_SCORE is not None and score < C5_MIN_ENTRY_SCORE:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C5_MIN_ATR_PCT is not None and atr_pct < C5_MIN_ATR_PCT:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="buy",
            kind="entry",
            notional=notional,
            reason=f"c5_long_entry:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": score,
                "scan_atr_pct": atr_pct,
            },
        )

    # ------------------------------------------------------------------ #
    # Exit logic
    # ------------------------------------------------------------------ #

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        if not _is_long(position):
            return None

        if not C5_ENABLE_SIGNAL_EXIT:
            return None

        if scan.action != "sell":
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="sell",
            kind="exit",
            notional=None,
            reason=f"c5_long_exit:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    # ------------------------------------------------------------------ #
    # TP/SL hooks (delegating to global risk)
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    # Scaling logic
    # ------------------------------------------------------------------ #

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        """
        Allow scale-ins when already long and we see *strong* follow-through
        "buy" signals. The actual size is still governed by notional caps and
        global risk.
        """
        if not _is_long(position):
            return False

        if scan.action != "buy":
            return False

        if not getattr(scan, "selected", False):
            return False

        score = float(getattr(scan, "score", 0.0) or 0.0)
        return score >= C5_SCALE_MIN_SCORE


# Module-level singleton expected by scheduler_core.get_strategy("c5")
c5 = C5Strategy()
