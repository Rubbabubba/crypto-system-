from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c1"

# --- Env-configurable knobs for c1 -----------------------------------------

# Whether c1 should use signal-based exits (in addition to global risk engine)
C1_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C1_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

# Optional minimum score for c1 entries. If None, rely on the book's min_score.
_raw = os.getenv("C1_MIN_ENTRY_SCORE")
C1_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

# Optional minimum ATR% floor for c1 (ignore ultra-low volatility environments)
_raw = os.getenv("C1_MIN_ATR_PCT")
C1_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else None


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


class C1Strategy:
    """
    Strategy c1 – mean-reversion / RSI-style long-only scalper.

    Semantics on spot:
      - When FLAT:
          * Only "buy" ScanResult.action may open a new long.
          * "sell" actions are ignored as new entries (we do not short).
      - When LONG:
          * "sell" actions may trigger exits (if C1_ENABLE_SIGNAL_EXIT is true).
      - Numeric TP/SL (exact percentages) are primarily handled by the global
        risk engine (profit_lock, loss_zone, daily_flatten). This strategy
        provides signal-shaped entries/exits, global config enforces the
        numeric guardrails.
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
        Entry logic for c1 (long-only).

        Conditions:
          - Must be flat.
          - Scan must be 'selected' by the book (top-K & above min_score).
          - Scan.action must be "buy".
          - Notional must be > 0.
          - Optional: C1_MIN_ENTRY_SCORE and C1_MIN_ATR_PCT thresholds.
        """
        # Must be flat to open a new position
        if not _is_flat(position):
            return None

        # Respect the StrategyBook's selection
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
        if C1_MIN_ENTRY_SCORE is not None and score < C1_MIN_ENTRY_SCORE:
            return None

        # Optional per-strategy ATR floor (e.g., require some volatility)
        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C1_MIN_ATR_PCT is not None and atr_pct < C1_MIN_ATR_PCT:
            return None

        # Risk caps and loss-zone are applied later by the global engine
        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="buy",
            kind="entry",
            notional=notional,
            reason=f"c1_long_entry:{scan.reason}",
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
        Exit logic for c1.

        While LONG:
          - Use "sell" actions as exits (e.g., RSI overbought / mean reversion
            completed). This is layered on top of global profit_lock / loss_zone.
        """
        # No exit if we are not in a long
        if not _is_long(position):
            return None

        # Allow disabling signal-based exits if desired
        if not C1_ENABLE_SIGNAL_EXIT:
            return None

        # Only react to "sell" actions as exits
        if scan.action != "sell":
            return None

        intent = OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="sell",
            kind="exit",
            notional=None,  # "flatten" semantics; router/risk can adjust
            reason=f"c1_long_exit:{scan.reason}",
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
        """
        Per-strategy profit-take hook for c1.

        For now, this returns None so that the global profit_lock rules
        in risk.json remain the single source of truth for numeric TP.
        """
        return None

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        """
        Per-strategy stop-loss hook for c1.

        For now, this returns None so that the global loss_zone / stop-loss
        rules remain the single source of truth for numeric SL.
        """
        return None

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        """
        Decide whether to scale INTO an existing long position.

        Currently disabled for c1; we rely on a single entry plus global risk.
        """
        return False


# Module-level singleton expected by scheduler_core.get_strategy("c1")
c1 = C1Strategy()
