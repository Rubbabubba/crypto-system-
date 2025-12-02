from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from book import StrategyBook, ScanRequest, ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext

logger = logging.getLogger(__name__)

STRAT_ID = "c1"


# ---------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------

def _is_flat(pos: Optional[PositionSnapshot]) -> bool:
    return (pos is None) or (abs(pos.qty) < 1e-10)


def _is_long(pos: Optional[PositionSnapshot]) -> bool:
    return (pos is not None) and (pos.qty > 1e-10)


def _is_short(pos: Optional[PositionSnapshot]) -> bool:
    return (pos is not None) and (pos.qty < -1e-10)


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v is not None else default
    except Exception:
        return default


def _normalize_unrealized_pct(pct: Optional[float]) -> Optional[float]:
    if pct is None:
        return None
    # Normalize if something like +2.5 instead of 0.025 comes in
    if abs(pct) > 1.0:
        return pct / 100.0
    return pct


# ---------------------------------------------------------------------
# Core C1 Strategy (RSI-based)
# ---------------------------------------------------------------------

class C1Strategy:
    """
    C1 — Adaptive RSI strategy.

    This class does NOT fetch bars or talk to the broker. It only:
      - interprets ScanResult from StrategyBook
      - decides entries, exits, take-profit, stop-loss, scaling
    """

    STRAT_ID = STRAT_ID

    # ---- Entry ------------------------------------------------------

    def entry_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        """
        Entry logic:

        - Only acts when FLAT.
        - If ScanResult is selected and action in {buy, sell}, emit entry.
        - Uses scan.notional as the intended order size.
        """
        if not _is_flat(position):
            return None

        if not getattr(scan, "selected", False):
            return None
        if scan.action not in ("buy", "sell"):
            return None
        if float(getattr(scan, "notional", 0.0) or 0.0) <= 0.0:
            return None

        notional = float(scan.notional or 0.0)

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=scan.action,           # "buy" or "sell"
            kind="entry",
            notional=notional,
            reason=scan.reason or "c1_entry_signal",
            meta={
                "score": float(getattr(scan, "score", 0.0) or 0.0),
                "atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
            },
        )

    # ---- Exit on reverse signal -------------------------------------

    def exit_signal(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        """
        Exit logic (RSI-based via opposite signal):

        - If LONG and ScanResult says "sell" -> exit.
        - If SHORT and ScanResult says "buy" -> exit.

        We emit EXIT only (no flip). The higher-level scheduler
        can decide whether to enter the opposite side after exit.
        """
        if _is_flat(position):
            return None
        if scan.symbol != position.symbol:
            return None

        if _is_long(position) and scan.action == "sell":
            side = "sell"
        elif _is_short(position) and scan.action == "buy":
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=scan.symbol,
            side=side,
            kind="exit",
            notional=None,     # close full position
            reason="c1_exit_on_reverse_signal",
            meta={
                "raw_action": scan.action,
                "score": float(getattr(scan, "score", 0.0) or 0.0),
            },
        )

    # ---- Per-strategy take-profit -----------------------------------

    def profit_take_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        """
        Per-strategy take-profit for c1.

        Env-tunable:
          C1_TAKE_PROFIT_PCT (default: 1.5, meaning +1.5%)

        This is *in addition* to your global profit_lock from risk.json.
        """
        if _is_flat(position):
            return None

        upnl = _normalize_unrealized_pct(position.unrealized_pct)
        if upnl is None:
            return None

        tp_pct = _env_float("C1_TAKE_PROFIT_PCT", 1.5) / 100.0
        if upnl < tp_pct:
            return None

        if _is_long(position):
            side = "sell"
        elif _is_short(position):
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=position.symbol,
            side=side,
            kind="take_profit",
            notional=None,  # close full position
            reason=f"c1_take_profit_{tp_pct*100:.2f}pct",
            meta={
                "unrealized_pct": upnl,
            },
        )

    # ---- Per-strategy stop-loss -------------------------------------

    def stop_loss_rule(
        self,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> Optional[OrderIntent]:
        """
        Per-strategy stop-loss for c1.

        Env-tunable:
          C1_STOP_LOSS_PCT (default: -3.0, meaning -3%)

        This is *in addition* to your global loss_zone from risk.json.
        """
        if _is_flat(position):
            return None

        upnl = _normalize_unrealized_pct(position.unrealized_pct)
        if upnl is None:
            return None

        sl_pct = _env_float("C1_STOP_LOSS_PCT", -3.0) / 100.0
        # Expect sl_pct to be negative (e.g. -0.03)
        if upnl > sl_pct:
            return None

        if _is_long(position):
            side = "sell"
        elif _is_short(position):
            side = "buy"
        else:
            return None

        return OrderIntent(
            strategy=self.STRAT_ID,
            symbol=position.symbol,
            side=side,
            kind="stop_loss",
            notional=None,
            reason=f"c1_stop_loss_{sl_pct*100:.2f}pct",
            meta={
                "unrealized_pct": upnl,
            },
        )

    # ---- Scaling -----------------------------------------------------

    def should_scale(
        self,
        scan: ScanResult,
        position: PositionSnapshot,
        risk: Optional[RiskContext] = None,
    ) -> bool:
        """
        Decide whether to add to an existing winning position.

        For now, we keep this conservative and return False.
        We can wire this up later if you want c1 to pyramid on
        especially strong RSI signals.
        """
        return False


# Singleton instance that scheduler_core can import
c1 = C1Strategy()


# ---------------------------------------------------------------------
# Optional legacy helper (if you used c1.scan in local tooling)
# ---------------------------------------------------------------------

def legacy_scan(req: Dict[str, Any], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Legacy scan-style helper so any old scripts that call c1.scan-like
    behavior can be pointed here if you want.

    It runs StrategyBook directly and returns a simple list of dicts
    similar to your old behavior, but without guard / risk.
    """
    try:
        tf = str(req.get("timeframe") or ctx.get("timeframe") or "5Min")
        limit = int(req.get("limit") or ctx.get("limit") or 300)
        syms = [s.upper() for s in (req.get("symbols") or ctx.get("symbols") or [])]
        notional = float(req.get("notional") or ctx.get("notional") or 25.0)

        book = StrategyBook(
            topk=2,
            min_score=0.10,
            risk_target_usd=notional,
            atr_stop_mult=1.0,
        )
        sreq = ScanRequest(
            strat=STRAT_ID,
            timeframe=tf,
            limit=limit,
            topk=2,
            min_score=0.10,
            notional=notional,
        )

        # Expect contexts of shape {sym: {"one": {...}, "five": {...}}}
        contexts = ctx.get("contexts") or {}
        results: List[ScanResult] = book.scan(sreq, contexts) or []

        intents: List[Dict[str, Any]] = []
        for r in results:
            if not getattr(r, "selected", False):
                continue
            if r.action not in ("buy", "sell"):
                continue
            if float(getattr(r, "notional", 0) or 0) <= 0:
                continue
            intents.append(
                {
                    "symbol": r.symbol,
                    "side": r.action,
                    "notional": min(notional, float(r.notional or notional)),
                    "score": float(getattr(r, "score", 0.0) or 0.0),
                    "atr_pct": float(getattr(r, "atr_pct", 0.0) or 0.0),
                    "reason": r.reason or "",
                }
            )
        return intents
    except Exception as e:
        logger.exception("[%s] legacy_scan() error: %s", STRAT_ID, e)
        return []
