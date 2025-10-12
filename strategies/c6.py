#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c6.py — ATR-trend with EMA filter
Build: v2.0.0 (2025-10-11)

Goal
- Favor longs when close > EMA(n), but insist on “enough motion” using ATR(n).
- Exit when price loses EMA or momentum cools.

Optimized defaults
- atr_n = 14
- atr_mult = 1.2     # used as a soft threshold vs EMA distance
- ema_n = 20
- exit_k = 0.997     # ~0.3% below EMA exits
"""
__version__ = "2.0.0"

from typing import Any, Dict, List
import time

try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

from utils_volatility import atr_from_bars

STRAT = "c6"

def _ema(vals, n: int):
    if n<=1 or len(vals)<n: return None
    k = 2.0/(n+1.0); e = vals[0]
    for v in vals[1:]: e = v*k + e*(1.0-k)
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
    atr_n = int(raw.get("atr_n", 14))
    atr_mult = float(raw.get("atr_mult", 1.2))
    ema_n = int(raw.get("ema_n", 20))
    exit_k = float(raw.get("exit_k", 0.997))

    out: List[Dict[str, Any]] = []
    for sym in syms:
        bars = br.get_bars(sym, timeframe=tf, limit=max(limit, ema_n+atr_n+2))
        if not bars or len(bars) < max(ema_n, atr_n)+1: continue
        c = [b["c"] for b in bars]
        last = c[-1]
        ema = _ema(c, ema_n)
        if ema is None: continue

        atr = atr_from_bars(bars, n=atr_n, return_series=False) or 0.0
        strong_enough = (last - ema) > (atr_mult * atr / 10.0)

        if last > ema and strong_enough:
            out.append(_mk(sym, "buy", notional))
            continue
        if last < ema * exit_k:
            out.append(_mk(sym, "sell", notional))
            continue
    return out
