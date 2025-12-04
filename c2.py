"""
Strategy c2 – trend continuation (long-only) for majors like BTC/ETH.

Design:
- Uses book / ScanResult to detect trend pullback setups.
- Only opens NEW positions on "buy" signals when flat (long-only).
- Uses "sell" / "trend_down_pb" style signals as exit triggers when long.
- Defers numeric TP / SL (exact % levels) to the global risk engine
  (profit_lock, loss_zone) for now, plus signal-based exits here.

This module is intentionally simple and opinionated so behavior is easy
to reason about from /debug/strategy_scan output.
"""

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any

from scheduler_core import (
    ScanResult,
    OrderIntent,
    PositionSnapshot,
    RiskContext,
)

# --- Env-configurable knobs for c2 -----------------------------------------

# In the current design, global profit_lock / loss_zone still handle the
# hard TP / SL. These knobs control how "eager" the strategy is to take
# signal-based exits rather than purely relying on globals.

C2_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C2_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

# Optional minimum score for c2 entries. If None, use the book's own min_score.
C2_MIN_ENTRY_SCORE = os.getenv("C2_MIN_ENTRY_SCORE")
C2_MIN_ENTRY_SCORE = float(C2_MIN_ENTRY_SCORE) if C2_MIN_ENTRY_SCORE not in (None, "") else None

# Optional minimum ATR% floor for c2 (ignore ultra-low volatility environments).
C2_MIN_ATR_PCT = os.getenv("C2_MIN_ATR_PCT")
C2_MIN_ATR_PCT = float(C2_MIN_ATR_PCT) if C2_MIN_ATR_PCT not in (None, "") else None


@dataclass
class StrategyC2:
    """
    Trend continuation strategy (c2), intended for majors (BTC, ETH).

    Long-only semantics on spot:
      - When flat:
          * Only "buy" actions may open a long.
          * "sell" actions are ignored as entry candidates.
      - When long:
          * "sell" actions may trigger exits / scale-outs.
          * Global risk engine still enforces caps, profit_lock, and loss_zone.
    """

    name: str = "c2"

    # --- Helpers -----------------------------------------------------------

    @staticmethod
    def _is_flat(position: Optional[PositionSnapshot]) -> bool:
        if position is None:
            return True
        qty = getattr(position, "qty", 0.0) or 0.0
        return abs(qty) < 1e-10

    @staticmethod
    def _in_long(position: Optional[PositionSnapshot]) -> bool:
        if position is None:
            return False
        qty = getattr(position, "qty", 0.0) or 0.0
        return qty > 1e-10

    # --- Entry / Exit API --------------------------------------------------

    def entry_signal(
        self,
        scan: ScanResult,
        position: Optional[PositionSnapshot],
        risk: RiskContext,
    ) -> Optional[OrderIntent]:
        """
        Entry logic for c2.

        Long-only:
          - Only "buy" actions can open a position when flat.
          - Requires scan.selected, positive notional, and (optionally) a
            per-strategy minimum score and ATR volatility floor.
        """
        # Must be flat to open a new position
        if not self._is_flat(position):
            return None

        # Respect whitelist / selection from the book
        if not getattr(scan, "selected", False):
            return None

        # Long-only: ignore "sell" actions as NEW entries
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

        # Optional per-strategy ATR floor (e.g. ignore ultra-dead markets)
        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if C2_MIN_ATR_PCT is not None and atr_pct < C2_MIN_ATR_PCT:
            return None

        # Let the risk context apply any per-symbol caps; if a helper is present,
        # use it, otherwise just pass through the book's suggested notional.
        symbol = scan.symbol

        capped_notional = notional
        cap = getattr(risk, "get_symbol_notional_cap", None)
        if callable(cap):
            capped_notional = cap(symbol, self.name, notional)  # type: ignore[assignment]

        if capped_notional <= 0.0:
            # Risk context decided we can't allocate anything new to this symbol.
            return None

        return OrderIntent(
            strategy=self.name,
            symbol=symbol,
            side="buy",
            kind="entry",
            notional=capped_notional,
            reason=f"c2_trend_up_entry:{scan.reason}",
            meta={
                "scan_score": score,
                "scan_reason": scan.reason,
                "scan_atr_pct": atr_pct,
            },
        )

    def exit_signal(
        self,
        scan: ScanResult,
        position: Optional[PositionSnapshot],
        risk: RiskContext,
    ) -> Optional[OrderIntent]:
        """
        Exit logic for c2.

        Uses "sell" / trend-down signals as exits when already long.
        This is layered on top of the global risk engine, which may also
        generate exits via profit_lock, loss_zone, daily_flatten, etc.
        """
        # No exit needed if we are flat
        if not self._in_long(position):
            return None

        # Optional flag to turn off signal-based exits (and rely purely on global risk)
        if not C2_ENABLE_SIGNAL_EXIT:
            return None

        # Only act on "sell" actions from the book while in a long position
        if scan.action != "sell":
            return None

        # We don't need a huge amount of logic here — if the book thinks this is a
        # trend-down pullback / break, and the position is long, propose an exit.
        # Global risk will still decide if this is allowed and in what size.
        symbol = scan.symbol

        # Basic behavior: request an exit of *entire* position; global risk / broker
        # can always down-size this if necessary.
        pos_qty = getattr(position, "qty", 0.0) or 0.0

        if pos_qty <= 0.0:
            return None

        return OrderIntent(
            strategy=self.name,
            symbol=symbol,
            side="sell",
            kind="exit",
            # Notional=None -> "flatten this position" semantics; if your OrderIntent
            # type instead prefers explicit notional, you can let the router compute it.
            notional=None,
            reason=f"c2_trend_down_exit:{scan.reason}",
            meta={
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_reason": scan.reason,
                "scan_atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    # --- Optional per-strategy TP/SL hooks --------------------------------

    def profit_take_rule(
        self,
        position: Optional[PositionSnapshot],
        risk: RiskContext,
    ) -> Optional[Dict[str, Any]]:
        """
        Per-strategy profit-take hook.

        Currently, c2 leans on the global profit_lock mechanism for numeric TP.
        This hook is provided for future extension; for now it returns None so
        that the global engine is the single source of truth for % thresholds.
        """
        return None

    def stop_loss_rule(
        self,
        position: Optional[PositionSnapshot],
        risk: RiskContext,
    ) -> Optional[Dict[str, Any]]:
        """
        Per-strategy stop-loss hook.

        As with profit_take_rule, we currently delegate numeric stop-loss to
        the global loss_zone / stop_loss_pct config. This hook is provided
        for future c2-specific SL logic (e.g. ATR-based trailing stops).
        """
        return None

    def should_scale(
        self,
        scan: ScanResult,
        position: Optional[PositionSnapshot],
        risk: RiskContext,
    ) -> bool:
        """
        Decide whether we should scale INTO an existing long.

        For now, c2 does not perform scale-ins; we rely on single entries plus
        global risk management. This can be extended later if you want trend
        pyramiding with strict caps.
        """
        return False


# Optional factory used by some schedulers/registries
def build() -> StrategyC2:
    """
    Factory for c2 strategy; some schedulers expect a build() that returns
    a strategy instance.
    """
    return StrategyC2()
