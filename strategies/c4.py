# strategies/c4.py
# Version: 1.3.1 (env-tunable, optimizer-friendly)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import broker as br

STRATEGY_NAME = "c4"
STRATEGY_VERSION = "1.3.1"

# === Tunables (env overrides) ===
C4_EMA_FAST = int(os.getenv("C4_EMA_FAST", "12"))
C4_EMA_SLOW = int(os.getenv("C4_EMA_SLOW", "50"))
C4_ATR_LEN  = int(os.getenv("C4_ATR_LEN",  "21"))
C4_ATR_K    = float(os.getenv("C4_ATR_K",  "2.0"))

# Back-compat aliases for optimizers that introspect attributes
EMA_FAST = C4_EMA_FAST
EMA_SLOW = C4_EMA_SLOW
ATR_LEN  = C4_ATR_LEN
ATR_K    = C4_ATR_K

def _sym(s: str) -> str:
    return s.replace("/", "")

def _bars(symbol: str, timeframe: str, limit: int):
    try:
        m = br.get_bars(symbol, timeframe=timeframe, limit=limit)
        return m.get(symbol, [])
    except Exception:
        return []

def _positions():
    try:
        return br.list_positions()
    except Exception:
        return []

def _has_long(symbol):
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

def _atr(bars, n=14):
    if len(bars) < n+1: return []
    trs = []
    pc = float(bars[0]["c"])
    for i in range(1, len(bars)):
        h = float(bars[i]["h"]); l = float(bars[i]["l"]); c = float(bars[i]["c"])
        tr = max(h-l, abs(h-pc), abs(l-pc))
        trs.append(tr); pc = c
    if len(trs) < n: return []
    atr = sum(trs[:n]) / n
    out = [None]*n
    out.append(atr)
    for i in range(n, len(trs)):
        atr = (atr*(n-1) + trs[i]) / n
        out.append(atr)
    return out

def _decide(symbol, bars):
    need = max(C4_EMA_SLOW + 10, C4_ATR_LEN + 10, 120)
    if len(bars) < need:
        return {"symbol":symbol, "action":"flat", "reason":"insufficient_bars"}

    closes = [float(b["c"]) for b in bars]
    c = closes[-1]
    ema_f = _ema(closes, C4_EMA_FAST)[-1]
    ema_s = _ema(closes, C4_EMA_SLOW)[-1]
    atrs  = _atr(bars, C4_ATR_LEN)
    atr   = atrs[-1] if atrs and atrs[-1] is not None else None
    have_long = _has_long(symbol) is not None

    if atr is None:
        return {"symbol":symbol, "action":"flat", "reason":"atr_warmup"}

    # Entry: trend up and price extends above fast EMA by ATR_K * ATR
    if c > ema_s and ema_f > ema_s and c > (ema_f + C4_ATR_K * atr):
        return {"symbol":symbol, "action":"buy", "reason":"atr_breakout_trend_up"}

    # Exit: lose fast EMA or breach below a negative ATR band
    if have_long and (c < ema_f or c < (ema_f - C4_ATR_K * atr)):
        return {"symbol":symbol, "action":"sell", "reason":"atr_reversal_or_trend_loss"}

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
    return {"strategy":STRATEGY_NAME, "version":STRATEGY_VERSION, "results":out, "placed":placed}
