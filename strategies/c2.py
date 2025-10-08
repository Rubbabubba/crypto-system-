# strategies/c2.py
# Version: 1.4.0 (env-tunable, optimizer-friendly)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import broker as br

STRATEGY_NAME = "c2"
STRATEGY_VERSION = "1.4.0"

# === Tunables (env overrides) ===
C2_EMA_FAST = int(os.getenv("C2_EMA_FAST", "12"))
C2_EMA_SLOW = int(os.getenv("C2_EMA_SLOW", "50"))
C2_RSI_LEN  = int(os.getenv("C2_RSI_LEN",  os.getenv("C2_RSI", "14")))
C2_RSI_LOW  = int(os.getenv("C2_RSI_LOW",  "30"))
C2_RSI_HIGH = int(os.getenv("C2_RSI_HIGH", "70"))

# Back-compat aliases (silence optimizer warnings)
EMA_FAST = C2_EMA_FAST
EMA_SLOW = C2_EMA_SLOW
RSI_LEN  = C2_RSI_LEN
RSI_LO   = C2_RSI_LOW
RSI_HI   = C2_RSI_HIGH

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
    out = []
    ema = None
    for v in vals:
        ema = v if ema is None else (v*k + ema*(1-k))
        out.append(ema)
    return out

def _rsi(closes, n=14):
    if len(closes) < n+2: return []
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(0.0, ch))
        losses.append(max(0.0, -ch))
    avg_g = sum(gains[:n]) / n
    avg_l = sum(losses[:n]) / n
    rsis = [None]*n
    for i in range(n, len(gains)):
        avg_g = (avg_g*(n-1) + gains[i]) / n
        avg_l = (avg_l*(n-1) + losses[i]) / n
        rs = (avg_g / avg_l) if avg_l > 0 else float('inf')
        rsi = 100 - (100 / (1 + rs))
        rsis.append(rsi)
    return rsis

def _decide(symbol, bars):
    need = max(C2_EMA_SLOW + 10, C2_RSI_LEN + 10, 120)
    if len(bars) < need:
        return {"symbol":symbol, "action":"flat", "reason":"insufficient_bars"}

    closes = [float(b["c"]) for b in bars]
    c = closes[-1]
    ema_f = _ema(closes, C2_EMA_FAST)[-1]
    ema_s = _ema(closes, C2_EMA_SLOW)[-1]
    rsi_v = _rsi(closes, C2_RSI_LEN)[-1]
    have_long = _has_long(symbol) is not None

    # Entry: trend up and RSI recovering from oversold
    if c > ema_s and ema_f > ema_s and rsi_v is not None and rsi_v > C2_RSI_LOW:
        return {"symbol":symbol, "action":"buy", "reason":"trend_up_rsi_recover"}

    # Exit: momentum fade or trend loss
    if have_long and ((rsi_v is not None and rsi_v > C2_RSI_HIGH and c < ema_f) or (c < ema_s)):
        return {"symbol":symbol, "action":"sell", "reason":"momentum_faded_or_trend_lost"}

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
