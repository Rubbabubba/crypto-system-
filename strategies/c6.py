# strategies/c6.py
# Version: 1.2.1 (env-tunable, optimizer-friendly)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import broker as br

STRATEGY_NAME = "c6"
STRATEGY_VERSION = "1.2.1"

# === Tunables (env overrides) ===
C6_EMA    = int(os.getenv("C6_EMA", "50"))
C6_EXIT_K = float(os.getenv("C6_EXIT_K", "0.998"))

# expose for optimizers
EMA     = C6_EMA
EXIT_K  = C6_EXIT_K

def _sym(s: str) -> str:
    return s.replace("/", "")

def _bars(symbol: str, timeframe: str, limit: int) -> List[Dict[str, Any]]:
    try:
        m = br.get_bars(symbol, timeframe=timeframe, limit=limit)
        return m.get(symbol, [])
    except Exception:
        return []

def _positions() -> List[Dict[str, Any]]:
    try:
        return br.list_positions()
    except Exception:
        return []

def _has_long(symbol: str):
    sym = _sym(symbol)
    for p in _positions():
        psym = p.get("symbol") or p.get("asset_symbol")
        if psym == sym and p.get("side","long").lower() == "long":
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str):
    try:
        return br.place_order(symbol, side, notional, client_id)
    except Exception as ex:
        return {"error": str(ex)}

def _ema(vals, n):
    if not vals or n <= 0: return []
    k = 2/(n+1)
    out, ema = [], None
    for v in vals:
        ema = v if ema is None else (v*k + ema*(1-k))
        out.append(ema)
    return out

def _decide(symbol: str, bars: List[Dict[str, Any]]):
    need = max(int(C6_EMA) + 20, 120)
    if len(bars) < need:
        return {"symbol":symbol, "action":"flat", "reason":"insufficient_bars"}

    closes = [float(b["c"]) for b in bars]
    c = closes[-1]
    ema = _ema(closes, int(C6_EMA))
    e  = ema[-1]
    e_prev = ema[-2] if len(ema) >= 2 else e
    slope_up = e > e_prev
    have_long = _has_long(symbol) is not None

    # Entry: price above EMA with rising slope
    if c > e and slope_up:
        return {"symbol":symbol, "action":"buy", "reason":"ema_break_and_rising"}

    # Exit: weakness below EMA * EXIT_K (buffer) or cross back under EMA
    if have_long and (c < e * float(C6_EXIT_K) or c < e):
        return {"symbol":symbol, "action":"sell", "reason":"ema_weakness_exit"}

    return {"symbol":symbol, "action":"flat", "reason":"hold_in_pos" if have_long else "no_signal"}

def run_scan(symbols, timeframe, limit, notional, dry, extra):
    out, placed = [], []
    epoch = int(time.time())
    for s in symbols:
        dec = _decide(s, _bars(s, timeframe, limit))
        out.append(dec)
        if dry or dec["action"] == "flat":
            continue
        if dec["action"] == "sell" and not _has_long(s):
            continue
        coid = f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        res = _place(s, dec["action"], notional, coid)
        if "error" not in res:
            placed.append({
                "symbol": s, "side": dec["action"], "notional": notional,
                "status": res.get("status","accepted"),
                "client_order_id": res.get("client_order_id", coid),
                "filled_avg_price": res.get("filled_avg_price"),
                "id": res.get("id")
            })
    return {"strategy":STRATEGY_NAME, "version":STRATEGY_VERSION, "results": out, "placed": placed}
