from __future__ import annotations

import logging
import os
from typing import Optional

from book import ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c6"

# --- Env-configurable knobs for c6 -----------------------------------------

C6_ENABLE_SIGNAL_EXIT = (
    str(os.getenv("C6_ENABLE_SIGNAL_EXIT", "true")).lower() in ("1", "true", "yes", "on")
)

C6_ALLOW_SHORTS = (
    str(os.getenv("C6_ALLOW_SHORTS", "true")).lower() in ("1", "true", "yes", "on")
)

_raw = os.getenv("C6_MIN_ENTRY_SCORE")
C6_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

_raw = os.getenv("C6_MIN_ATR_PCT")
C6_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else 0.1

_raw = os.getenv("C6_MAX_ATR_PCT")
C6_MAX_ATR_PCT = float(_raw) if _raw not in (None, "") else 2.0

_raw = os.getenv("C6_PANIC_ATR_PCT")
C6_PANIC_ATR_PCT = float(_raw) if _raw not in (None, "") else 3.0


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


class C6Strategy:
    """
    Strategy c6 – volatility-aware, now two-sided.

    - Only enters when ATR% is within [C6_MIN_ATR_PCT, C6_MAX_ATR_PCT].
    - While flat:
        * "buy" -> long entry
        * "sell" -> short entry (if C6_ALLOW_SHORTS)
    - While in a position:
        * Panic exit if atr_pct >= C6_PANIC_ATR_PCT (any direction)
        * Otherwise:
            - LONG: exit on "sell"
            - SHORT: exit on "buy"
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
            if not C6_ALLOW_SHORTS:
                return None
            side = "sell"

        notional = float(getattr(scan, "notional", 0.0) or 0.0)
        if notional <= 0.0:
            return None

        score = float(getattr(scan, "score", 0.0) or 0.0)
        if C6_MIN_ENTRY_SCORE is not None and score < C6_MIN_ENTRY_SCORE:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        if atr_pct < C6_MIN_ATR_PCT or atr_pct > C6_MAX_ATR_PCT:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="entry",
            notional=notional,
            reason=f"c6_vol_band_entry:{action}:{scan.reason}",
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
        if not C6_ENABLE_SIGNAL_EXIT:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)
        action = getattr(scan, "action", None)

        # Panic exit on extreme volatility regardless of direction
        if ( _is_long(position) or _is_short(position) ) and atr_pct >= C6_PANIC_ATR_PCT:
            side = "sell" if _is_long(position) else "buy"
            return OrderIntent(
                strategy=self.STRAT_ID,
                symbol=scan.symbol,
                side=side,
                kind="exit",
                notional=None,
                reason=f"c6_panic_vol_exit:atr_pct={atr_pct}",
                meta={
                    "scan_action": action,
                    "scan_reason": scan.reason,
                    "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                    "scan_atr_pct": atr_pct,
                },
            )

        # Directional exits
        if _is_long(position):
            if action != "sell":
                return None
            side = "sell"
        elif _is_short(position):
            if action != "buy":
                return None
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="exit",
            notional=None,
            reason=f"c6_vol_band_exit:{action}:{scan.reason}",
            meta={
                "scan_action": action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": atr_pct,
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
        return False


c6 = C6Strategy()