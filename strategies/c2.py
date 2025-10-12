#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c2.py — EMA trend w/ trailing exit
Build: v2.0.0 (2025-10-11)

Goal
- Go long when close > EMA(n). Trail out when close < EMA * EXIT_K.

Optimized defaults
- ema_n = 50
- exit_k = 0.997   # ~0.3% below EMA triggers exit
"""
__version__ = "2.0.0"

from typing import Any, Dict, List
import time

try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

STRAT = "c2"

def _ema(vals, n: int):
    if n <= 1 or len(vals) < n: return None
    k = 2.0/(n+1.0); e = vals[0]
    for v in vals[1:]:
        e = v*k + e*(1.0-k)
    return e

def _mk(symbol: str, side: str, notional: float) -> Dict[str, Any]:
    ts = int(time.time())
    return {"symbol": symbol, "side": side, "notional": float(notional), "strategy": STRAT, "id": f"{STRAT}:{symbol}:{side}:{ts}", "ts": ts}

def scan(req: Dict[str, Any], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    syms: List[str] = [s.upper() for s in (req.get("symbols") or [])]
    tf = req.get("timeframe") or "5Min"
    limit = int(req.get("limit") or 300)
    notional = float(req.get("notional") or 25.0)
    raw = dict(req.get("raw") or {})
    ema_n = int(raw.get("ema_n", 50))
    exit_k = float(raw.get("exit_k", 0.997))

    out: List[Dict[str, Any]] = []
    for sym in syms:
        bars = br.get_bars(sym, timeframe=tf, limit=limit)
        if not bars or len(bars) < ema_n: continue
        c = [b["c"] for b in bars]
        last = c[-1]
        ema = _ema(c, ema_n)
        if ema is None: continue

        if last > ema:
            out.append(_mk(sym, "buy", notional))
            continue
        if last < ema * exit_k:
            out.append(_mk(sym, "sell", notional))
            continue
    return out
