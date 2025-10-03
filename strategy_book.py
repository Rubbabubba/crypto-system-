# strategy_book.py
# Minimal, self-contained StrategyBook with demo strategies (c1..c6).
# Safe for dry runs; produces lightweight, deterministic sample orders.

from __future__ import annotations
import time
import math
import random
from typing import Any, Dict, List, Optional

class StrategyBook:
    """
    Demo StrategyBook that understands .scan(req, contexts).
    It reads the normalized fields your app passes (strategy, symbols, one/default, etc.)
    and returns a list of order dicts. Designed to "just work" with app.py.
    """

    def __init__(self) -> None:
        # Seed randomness for stable-looking output across ticks
        random.seed(42)

    # ---- helpers ------------------------------------------------------------

    def _coerce_symbols(self, req: Dict[str, Any], contexts: Optional[Dict[str, Any]]) -> List[str]:
        syms = req.get("symbols")
        if not syms and isinstance(contexts, dict):
            syms = (contexts.get("one") or {}).get("symbols")
        if isinstance(syms, str):
            syms = [s.strip() for s in syms.split(",") if s.strip()]
        if not syms:
            syms = ["BTCUSDT", "ETHUSDT"]
        return syms

    def _px_model(self, symbol: str, t: float) -> float:
        """
        Tiny synthetic price model so orders don't all look identical.
        """
        base = {
            "BTCUSDT": 60000.0,
            "ETHUSDT": 2500.0,
            "SOLUSDT": 150.0,
            "AVAXUSDT": 40.0,
            "DOGEUSDT": 0.2,
        }.get(symbol, 100.0)
        # Smooth wiggle:
        return max(0.0001, base * (1.0 + 0.01 * math.sin(t / 60.0 + hash(symbol) % 10)))

    def _mk_order(self, *, symbol: str, strategy: str, notional: float, side: str, ts: float) -> Dict[str, Any]:
        px = self._px_model(symbol, ts)
        qty = max(0.0001, (notional or 100.0) / px)
        # Tiny PnL jitter just so the dashboard moves a bit
        pnl = round(random.uniform(-0.75, 0.75), 2)
        oid = f"{strategy}-{symbol}-{int(ts)}-{abs(hash((symbol, strategy, int(ts)))) % 10000}"
        return {
            "id": oid,
            "symbol": symbol,
            "side": side,
            "qty": round(qty, 6),
            "px": round(px, 2),
            "strategy": strategy,
            "pnl": pnl,
            "ts": ts,
        }

    # ---- simple demo strategies --------------------------------------------

    def _strat_c1(self, symbols: List[str], notional: float, ts: float) -> List[Dict[str, Any]]:
        return [self._mk_order(symbol=s, strategy="c1", notional=notional, side="BUY", ts=ts) for s in symbols[:2]]

    def _strat_c2(self, symbols: List[str], notional: float, ts: float) -> List[Dict[str, Any]]:
        return [self._mk_order(symbol=s, strategy="c2", notional=notional, side="SELL", ts=ts) for s in symbols[:1]]

    def _strat_c3(self, symbols: List[str], notional: float, ts: float) -> List[Dict[str, Any]]:
        orders = []
        for i, s in enumerate(symbols[:3]):
            side = "BUY" if i % 2 == 0 else "SELL"
            orders.append(self._mk_order(symbol=s, strategy="c3", notional=notional, side=side, ts=ts))
        return orders

    def _strat_c4(self, symbols: List[str], notional: float, ts: float) -> List[Dict[str, Any]]:
        # Place at most one order, alternating side by minute
        side = "BUY" if int(ts // 60) % 2 == 0 else "SELL"
        return [self._mk_order(symbol=symbols[0], strategy="c4", notional=notional, side=side, ts=ts)]

    def _strat_c5(self, symbols: List[str], notional: float, ts: float) -> List[Dict[str, Any]]:
        # Noisy but small; shows activity without spam
        if int(ts) % 3 == 0:
            return [self._mk_order(symbol=symbols[-1], strategy="c5", notional=notional * 0.5 or 50.0, side="BUY", ts=ts)]
        return []

    def _strat_c6(self, symbols: List[str], notional: float, ts: float) -> List[Dict[str, Any]]:
        # Rotates through symbols
        idx = int(ts // 60) % max(1, len(symbols))
        return [self._mk_order(symbol=symbols[idx], strategy="c6", notional=notional, side="SELL", ts=ts)]

    # ---- public API ---------------------------------------------------------

    def scan(self, req: Dict[str, Any], contexts: Optional[Dict[str, Any]] = None):
        """
        Entry point called by app.py.
        Expects normalized req from the app’s _scan_bridge (already provided).
        Returns a list[order] or {"orders": list} — the app handles both.
        """
        # Pull normalized fields (with robust fallbacks)
        strategy = req.get("strategy") or "c1"
        one = req.get("one") or (contexts or {}).get("one") or {}
        notional = float(req.get("notional") or one.get("notional") or 100.0)
        symbols = self._coerce_symbols(req, contexts)

        # (Optional) honor dry flag if you wire it differently later
        dry = bool(req.get("dry", False))

        ts = time.time()

        strat_map = {
            "c1": self._strat_c1,
            "c2": self._strat_c2,
            "c3": self._strat_c3,
            "c4": self._strat_c4,
            "c5": self._strat_c5,
            "c6": self._strat_c6,
        }
        fn = strat_map.get(strategy)
        if not fn:
            return []

        orders = fn(symbols, notional, ts)
        if dry:
            # In dry mode, return the intent but make it obvious it's a sim
            for o in orders:
                o["id"] = "DRY-" + o["id"]
            return {"orders": orders}

        return {"orders": orders}
