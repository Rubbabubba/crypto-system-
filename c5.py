#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c5.py — Breakout band with profit-protect exit
Build: v2.0.0 (2025-10-11)

Goal
- Enter on break above highest high (lookback N), with a small band multiplier.
- Exit when price fades back under a tighter exit multiple of that high.

Optimized defaults
- lookback = 20
- band_k   = 1.0010   # +0.10% band to confirm breakout
- exit_k   = 0.9990   # -0.10% fade below HH to exit
"""
__version__ = "2.0.0"

from typing import Any, Dict, List
import time

try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

STRAT = "c5"

def _highest(xs, n): return max(xs[-n:]) if len(xs)>=n else None

def _mk(symbol: str, side: str, notional: float) -> Dict[str, Any]:
    ts = int(time.time())
    return {"symbol": symbol, "side": side, "notional": float(notional), "strategy": STRAT, "id": f"{STRAT}:{symbol}:{side}:{ts}", "ts": ts}

def scan(req: Dict[str, Any], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    syms: List[str] = [s.upper() for s in (req.get("symbols") or [])]
    tf = req.get("timeframe") or "5Min"
    limit = int(req.get("limit") or 300)
    notional = float(req.get("notional") or 25.0)
    raw = dict(req.get("raw") or {})
    lookback = int(raw.get("lookback", 20))
    band_k   = float(raw.get("band_k", 1.0010))
    exit_k   = float(raw.get("exit_k", 0.9990))

    out: List[Dict[str, Any]] = []
    for sym in syms:
        bars = br.get_bars(sym, timeframe=tf, limit=max(limit, lookback+2))
        if not bars or len(bars) < lookback+1: continue
        c = [b["c"] for b in bars]
        last = c[-1]
        hh = _highest(c, lookback)
        if hh is None: continue

        if last >= hh * band_k:
            out.append(_mk(sym, "buy", notional))
            continue
        if last <= hh * exit_k:
            out.append(_mk(sym, "sell", notional))
            continue
    return out
