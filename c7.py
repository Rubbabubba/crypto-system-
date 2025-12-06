from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c7"

# --- Env-configurable knobs for c7 -----------------------------------------

# Enable/disable signal-based exits (global risk still runs either way)
C7_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C7_ENABLE_SIGNAL_EXIT", "true")).lower()
    in ("1", "true", "yes", "on")
)

# Purely short-only strategy – no longs
C7_ALLOW_SHORTS = (
    str(os.getenv("C7_ALLOW_SHORTS", "true")).lower()
    in ("1", "true", "yes", "on")
)

_raw = os.getenv("C7_MIN_ENTRY_SCORE")
C7_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

# Volatility band for entries (avoid dead markets, avoid insane spikes)
_raw = os.getenv("C7_MIN_ATR_PCT")
C7_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else 0.15

_raw = os.getenv("C7_MAX_ATR_PCT")
C7_MAX_ATR_PCT = float(_raw) if _raw not in (None, "") else 3.0

# Panic exit threshold – if volatility blows out above this, bail
_raw = os.getenv("C7_PANIC_ATR_PCT")
C7_PANIC_ATR_PCT = float(_raw) if _raw not in (None, "") else 5.0


def _is_flat(position: Optional[PositionSnapshot]) -> bool:
    if position is None:
        return True
    qty = getattr(position, "qty", 0.0) or 0.0
    return abs(qty) < 1e-10


def _is_short(position: Optional[PositionSnapshot]) -> bool:
    if position is None:
        return False
    qty = getattr(position, "qty", 0.0) or 0.0
    return qty < -1e-10


class C7Strategy:
    """
    Strategy c7 – Bear Trend Ride (short-only).

    Goals:
      - Turn medium/strong downtrends into profit.
      - Only enter on selected SELL signals, in a healthy volatility band.
      - Exit on BUY signals or when volatility spikes into "panic" territory.

    Behavior:
      - While FLAT:
          * Selected "sell" signals -> open SHORT, if C7_ALLOW_SHORTS.
      - While SHORT:
          * "buy" signals -> close short (take profit or cut loss).
          * ATR% >= C7_PANIC_ATR_PCT -> emergency exit.
    """

    STRAT_ID: str = STRAT_ID

    # ------------------------------------------------------------------ #
    # Entry logic (short-only)
    # ------------------------------------------------------------------ #

    def entry_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # Must be flat to open a new short
        if not _is_flat(position):
            return None

        if not C7_ALLOW_SHORTS:
            return None

        if not getattr(scan, "selected", False):
            return None

        action = getattr(scan, "action", None)
        if action != "sell":
            # This strategy is purely short-only; ignore "buy" entries.
            return None

        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C7_MIN_ENTRY_SCORE is not None and score < C7_MIN_ENTRY_SCORE:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if atr_pct < C7_MIN_ATR_PCT or atr_pct > C7_MAX_ATR_PCT:
            # Either too quiet to care, or already too chaotic.
            return None

        # Optional: we could further filter by reason substrings, e.g.
        #   "trend_down", "mom_down", "breakdown", etc.
        # For now we trust the book's selection + score to encode that.

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="sell",   # short entry
            kind="entry",
            notional=notional,
            reason=f"c7_short_entry:{scan.reason}",
            meta={
                "scan_action": action,
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
        if not C7_ENABLE_SIGNAL_EXIT:
            return None

        if not _is_short(position):
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        action = getattr(scan, "action", None)

        # Panic exit on extreme volatility (regardless of buy/sell label)
        if atr_pct >= C7_PANIC_ATR_PCT:
            return OrderIntent(
                strategy=self.STRAT_ID,
                symbol=scan.symbol,
                side="buy",  # close short
                kind="exit",
                notional=None,
                reason=f"c7_panic_vol_exit:atr_pct={atr_pct}",
                meta={
                    "scan_action": action,
                    "scan_reason": scan.reason,
                    "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                    "scan_atr_pct": atr_pct,
                },
            )

        # Normal exit: a BUY signal in a short position
        if action != "buy":
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="buy",   # close short
            kind="exit",
            notional=None,
            reason=f"c7_short_exit:{scan.reason}",
            meta={
                "scan_action": action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": atr_pct,
            },
        )

    # ------------------------------------------------------------------ #
    # TP/SL hooks – still delegate numeric rules to global risk
    # ------------------------------------------------------------------ #

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        # Global profit_lock and loss_zone still handle numeric brackets.
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
        # We can consider scaling into shorts later; keep it simple for now.
        return False


# Module-level singleton expected by scheduler_core.get_strategy("c7")
c7 = C7Strategy()
