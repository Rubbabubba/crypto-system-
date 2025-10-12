#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c1.py — VWAP pullback pop (momentum poke)
Build: v2.0.0 (2025-10-11)

Goal
- When price pulls back to VWAP but holds above the EMA(n), take a momentum continuation BUY.
- If already “in”, a basic exit triggers when close loses the EMA or fails the VWAP retest.

Optimized defaults (from backtests / app.DEFAULT_STRAT_PARAMS)
- ema_n = 20
- vwap_pull = 0.0020   # >= +0.20% below VWAP counts as a pullback (positive when VWAP > close)

Contract
- scan(req, ctx) -> List[order_dict]
- req: { symbols, timeframe, limit, notional, raw:{ema_n, vwap_pull} }
- returns orders shaped for app: {"symbol","side","notional","strategy","id","ts"}
"""
__version__ = "2.0.0"

from typing import Any, Dict, List
import time
import math

try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

STRAT = "c1"

def _ema(vals, n: int):
    if n <= 1 or len(vals) < n: return None
    k = 2.0 / (n + 1.0)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1.0 - k)
    return e

def _vwap(o,h,l,c,v):
    # session VWAP approximation from the bars we fetched
    num = den = 0.0
    for i in range(len(c)):
        tp = (h[i] + l[i] + c[i]) / 3.0
        vol = v[i]
        num += tp * vol
        den += vol
    return (num / den) if den > 0 else None

def _mk(symbol: str, side: str, notional: float) -> Dict[str, Any]:
    ts = int(time.time())
    return {"symbol": symbol, "side": side, "notional": float(notional), "strategy": STRAT, "id": f"{STRAT}:{symbol}:{side}:{ts}", "ts": ts}

def scan(req: Dict[str, Any], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    syms: List[str] = [s.upper() for s in (req.get("symbols") or [])]
    tf = req.get("timeframe") or "5Min"
    limit = int(req.get("limit") or 300)
    notional = float(req.get("notional") or 25.0)
    raw = dict(req.get("raw") or {})
    ema_n = int(raw.get("ema_n", 20))
    vwap_pull = float(raw.get("vwap_pull", 0.0020))

    out: List[Dict[str, Any]] = []
    for sym in syms:
        bars = br.get_bars(sym, timeframe=tf, limit=limit)
        if not bars or len(bars) < max(ema_n, 20):
            continue
        o = [b["o"] for b in bars]
        h = [b["h"] for b in bars]
        l = [b["l"] for b in bars]
        c = [b["c"] for b in bars]
        v = [b["v"] for b in bars]
        last = c[-1]
        ema = _ema(c, ema_n)
        vwap_val = _vwap(o, h, l, c, v)

        if ema is None or vwap_val is None:
            continue

        # Pullback amount (positive when VWAP > last)
        pull_amt = (vwap_val - last) / max(vwap_val, 1e-12)

        # Entry: price pulled to/under VWAP by >= vwap_pull and still above EMA
        if pull_amt >= vwap_pull and last >= ema:
            out.append(_mk(sym, "buy", notional))
            continue

        # Basic exit condition: lose EMA
        if last < ema:
            out.append(_mk(sym, "sell", notional))
            continue

    return out
