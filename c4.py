#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c4.py — MACD-ish trend with ATR confirmation
Build: v2.0.0 (2025-10-11)

Goal
- Prefer longs when EMA_fast > EMA_slow and price shows some strength vs EMA_slow.
- Exit when the fast slope flips / fast < slow.

Optimized defaults
- ema_fast = 12
- ema_slow = 26
- sig = 9              # reserved for future histogram filter
- min_vol = 0.0        # not used here
- ATR gate: atr_mult = 1.2 (use as a soft distance check vs EMA_slow)
"""
__version__ = "2.0.0"

from typing import Any, Dict, List
import time

try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

from utils_volatility import atr_from_bars

STRAT = "c4"

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
    ema_f = int(raw.get("ema_fast", 12))
    ema_s = int(raw.get("ema_slow", 26))
    atr_n = int(raw.get("atr_n", 14))
    atr_mult = float(raw.get("atr_mult", 1.2))

    out: List[Dict[str, Any]] = []
    for sym in syms:
        bars = br.get_bars(sym, timeframe=tf, limit=max(limit, ema_s+atr_n+2))
        if not bars or len(bars) < max(ema_s, atr_n)+1:
            continue
        c = [b["c"] for b in bars]
        last = c[-1]
        ef = _ema(c, ema_f); es = _ema(c, ema_s)
        if ef is None or es is None: continue

        atr = atr_from_bars(bars, n=atr_n, return_series=False)
        # strength vs slow EMA needs to be at least a fraction of ATR
        strength_ok = (last - es) > (atr_mult * (atr or 0) / 10.0)

        if ef > es and strength_ok:
            out.append(_mk(sym, "buy", notional))
            continue
        if ef < es or last < es:
            out.append(_mk(sym, "sell", notional))
            continue
    return out
