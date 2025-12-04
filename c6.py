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

_raw = os.getenv("C6_MIN_ENTRY_SCORE")
C6_MIN_ENTRY_SCORE = float(_raw) if _raw not in (None, "") else None

# Volatility band for entries
_raw = os.getenv("C6_MIN_ATR_PCT")
C6_MIN_ATR_PCT = float(_raw) if _raw not in (None, "") else 0.1  # default 0.1%

_raw = os.getenv("C6_MAX_ATR_PCT")
C6_MAX_ATR_PCT = float(_raw) if _raw not in (None, "") else 2.0  # default 2%

# Panic exit threshold: if atr_pct exceeds this while long, be happy to exit
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


class C6Strategy:
    """
    Strategy c6 – volatility-aware long-only.

    Semantics:
      - Only enters when ATR% is within a configurable band (C6_MIN_ATR_PCT,
        C6_MAX_ATR_PCT).
      - When long, "sell" signals and/or ATR% above C6_PANIC_ATR_PCT can be
        used as exits.
      - Numeric TP/SL is delegated to global risk engine.
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
        if C6_MIN_ENTRY_SCORE is not None and score < C6_MIN_ENTRY_SCORE:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)

        # Require volatility inside a target band
        if atr_pct < C6_MIN_ATR_PCT or atr_pct > C6_MAX_ATR_PCT:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="buy",
            kind="entry",
            notional=notional,
            reason=f"c6_vol_band_long_entry:{scan.reason}",
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

        if not C6_ENABLE_SIGNAL_EXIT:
            return None

        atr_pct = float(getattr(scan, "atr_pct", 0.0) or 0.0)

        # If volatility blows out above panic threshold, we are happy to exit
        if atr_pct >= C6_PANIC_ATR_PCT:
            return OrderIntent(
                strategy=self.STRAT_ID,
                symbol=scan.symbol,
                side="sell",
                kind="exit",
                notional=None,
                reason=f"c6_panic_vol_exit:atr_pct={atr_pct}",
                meta={
                    "scan_action": scan.action,
                    "scan_reason": scan.reason,
                    "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                    "scan_atr_pct": atr_pct,
                },
            )

        # Otherwise, use "sell" actions as normal exits
        if scan.action != "sell":
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side="sell",
            kind="exit",
            notional=None,
            reason=f"c6_vol_band_long_exit:{scan.reason}",
            meta={
                "scan_action": scan.action,
                "scan_reason": scan.reason,
                "scan_score": float(getattr(scan, "score", 0.0) or 0.0),
                "scan_atr_pct": atr_pct,
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

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        # No explicit scaling logic for c6 (volatility-sensitive).
        return False


# Module-level singleton expected by scheduler_core.get_strategy("c6")
c6 = C6Strategy()
