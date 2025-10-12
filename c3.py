#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c3.py — Channel breakout (Donchian-style)
Build: v2.0.0 (2025-10-11)

Goal
- Enter on breakout above recent channel high; exit on breakdown below channel low.

Optimized defaults
- ch_n = 55
- break_k = 1.0005   # +0.05% breakout buffer
- fail_k  = 0.997    # -0.3% breakdown exit
"""
__version__ = "2.0.0"

from typing import Any, Dict, List
import time

try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

STRAT = "c3"

def _highest(xs, n): return max(xs[-n:]) if len(xs) >= n else None
def _lowest(xs, n):  return min(xs[-n:]) if len(xs) >= n else None

def _mk(symbol: str, side: str, notional: float) -> Dict[str, Any]:
    ts = int(time.time())
    return {"symbol": symbol, "side": side, "notional": float(notional), "strategy": STRAT, "id": f"{STRAT}:{symbol}:{side}:{ts}", "ts": ts}

def scan(req: Dict[str, Any], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    syms: List[str] = [s.upper() for s in (req.get("symbols") or [])]
    tf = req.get("timeframe") or "5Min"
    limit = int(req.get("limit") or 300)
    notional = float(req.get("notional") or 25.0)
    raw = dict(req.get("raw") or {})
    ch_n = int(raw.get("ch_n", 55))
    break_k = float(raw.get("break_k", 1.0005))
    fail_k  = float(raw.get("fail_k", 0.997))

    out: List[Dict[str, Any]] = []
    for sym in syms:
        bars = br.get_bars(sym, timeframe=tf, limit=max(limit, ch_n+2))
        if not bars or len(bars) < ch_n+1: continue
        c = [b["c"] for b in bars]
        last = c[-1]
        hi = _highest(c, ch_n)
        lo = _lowest(c, ch_n)
        if hi is None or lo is None: continue

        if last >= hi * break_k:
            out.append(_mk(sym, "buy", notional))
            continue
        if last <= lo * fail_k:
            out.append(_mk(sym, "sell", notional))
            continue
    return out
