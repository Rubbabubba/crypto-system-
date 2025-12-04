from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c3"

# --- Env-configurable knobs for c3 -----------------------------------------

# Whether c3 should use signal-based exits (in addition to global risk engine)
C3_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C3_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

# Optional minimum score for c3 entries. If None, rely on the book's min_score.
_raw = os.getenv("C3_MIN_ENTRY_SCORE")
C3_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

# Optional minimum ATR% floor for c3 (ignore ultra-low volatility environments)
_raw = os.getenv("C3_MIN_ATR_PCT")
C3_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else None


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


class C3Strategy:
    """
    Strategy c3 – momentum / momentum-fade, long-only on spot.

    Semantics:
      - When FLAT:
          * Only "buy" ScanResult.action may open a long.
          * "sell" actions are ignored as new entries (no naked shorts).
      - When LONG:
          * "sell" actions may trigger exits (if C3_ENABLE_SIGNAL_EXIT is true).
      - Numeric TP/SL is primarily handled by the global risk engine
        (profit_lock, loss_zone, daily_flatten). This strategy provides
        signal-shaped entries/exits; global config enforces hard guardrails.
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
        """
        Entry logic for c3 (long-only).

        Conditions:
          - Must be flat.
          - Scan must be selected (top-K & above global min_score).
          - Scan.action must be "buy".
          - Notional must be > 0.
          - Optional: C3_MIN_ENTRY_SCORE and C3_MIN_ATR_PCT thresholds.
        """
        # Must be flat to open a new position
        if not _is_flat(position):
            return None

        # Respect StrategyBook selection
        if not getattr(scan, "selected", False):
            return None

        # Long-only: ignore "sell" for NEW entries
        if scan.action != "buy":
            return None

        # Notional must be positive
        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        # Optional per-strategy min score
        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C3_MIN_ENTRY_SCORE is not None and score < C3_MIN_ENTRY_SCORE:
            return None

        # Optional per-strategy ATR floor
        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C3_MIN_ATR_PCT is not None and atr_pct < C3_MIN_ATR_PCT:
            return None

        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="buy",
            kind="entry",
            notional=notional,
            reason=f"c3_long_entry:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": score,
                "scan_atr_pct": atr_pct,
            },
        )
        return intent

    # ------------------------------------------------------------------ #
    # Exit logic
    # ------------------------------------------------------------------ #

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        """
        Exit logic for c3.

        While LONG:
          - Use "sell" actions as exits (e.g., momentum fade, trend cracks).
        """
        # No exit if not in a long
        if not _is_long(position):
            return None

        # Allow disabling signal-based exits if desired
        if not C3_ENABLE_SIGNAL_EXIT:
            return None

        # Only react to "sell" actions as exits
        if scan.action != "sell":
            return None

        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="sell",
            kind="exit",
            notional=None,  # "flatten this position"; router/risk can adjust size
            reason=f"c3_long_exit:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )
        return intent

    # ------------------------------------------------------------------ #
    # Optional per-strategy TP/SL hooks (currently rely on global risk)
    # ------------------------------------------------------------------ #

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # For now, delegate numeric TP to global profit_lock.
        return None

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # For now, delegate numeric SL to global loss_zone / stop-loss.
        return None

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        # No pyramiding for c3 yet.
        return False


# Module-level singleton expected by scheduler_core.get_strategy("c3")
c3 = C3Strategy()
