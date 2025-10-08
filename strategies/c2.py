# strategies/c2.py
# Version: 1.3.0 (2025-09-30)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import requests
import broker as br, math

STRATEGY_NAME = "c2"
STRATEGY_VERSION = "1.3.1"

ALPACA_TRADE_HOST = os.getenv("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
ALPACA_DATA_HOST  = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets")
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

# Tunables (defaults are your current ones—adjust as needed)
C2_EMA_FAST = int(os.getenv("C2_EMA_FAST", "12"))
C2_EMA_SLOW = int(os.getenv("C2_EMA_SLOW", "50"))
C2_RSI_LOW  = int(os.getenv("C2_RSI_LOW",  "30"))
C2_RSI_HIGH = int(os.getenv("C2_RSI_HIGH", "70"))


def _hdr():
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY): return {}
    return {"APCA-API-KEY-ID": ALPACA_KEY_ID, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY, "Accept":"application/json","Content-Type":"application/json"}

def _sym(s:str)->str: return s.replace("/","")

def _bars(symbol: str, timeframe: str, limit: int) -> List[Dict[str,Any]]:
    try:
        m = br.get_bars(symbol, timeframe=timeframe, limit=limit)
        return m.get(symbol, [])
    except Exception:
        return []

def _positions() -> List[Dict[str,Any]]:
    try:
        return br.list_positions()
    except Exception:
        return []

def _has_long(symbol: str):
    sym = _sym(symbol)
    for p in _positions():
        if (p.get("symbol") or p.get("asset_symbol")) == sym and p.get("side","long")=="long":
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str) -> Dict[str,Any]:
    try:
        return br.place_order(symbol, side, notional, client_id)
    except Exception as ex:
        return {"error": str(ex)}

def _rsi(closes: List[float], n: int=14) -> List[float]:
    if len(closes) < n+1: return []
    gains, losses = 0.0, 0.0
    out=[]
    for i in range(1,n+1):
        d = closes[i]-closes[i-1]
        gains += max(0,d); losses += max(0,-d)
    avg_g, avg_l = gains/n, losses/n
    rs = (avg_g / avg_l) if avg_l>0 else 1e9
    out.append(100 - (100/(1+rs)))
    for i in range(n+1,len(closes)):
        d = closes[i]-closes[i-1]
        g, l = max(0,d), max(0,-d)
        avg_g = (avg_g*(n-1)+g)/n
        avg_l = (avg_l*(n-1)+l)/n
        rs = (avg_g/avg_l) if avg_l>0 else 1e9
        out.append(100 - (100/(1+rs)))
    return out

def _vwap(bars):
    cum_pv=cum_v=0.0
    out=[]
    for b in bars:
        h,l,c,v = float(b["h"]),float(b["l"]),float(b["c"]),float(b["v"])
        tp=(h+l+c)/3.0
        cum_pv+=tp*v; cum_v+=v
        out.append(cum_pv/max(1e-9,cum_v))
    return out

def _decide(symbol: str, bars: List[Dict[str,Any]]) -> Dict[str,str]:
    if len(bars) < 80:
        return {"symbol": symbol, "action":"flat", "reason":"insufficient_bars"}
    closes=[float(b["c"]) for b in bars]
    rsi=_rsi(closes,14)
    if not rsi: return {"symbol":symbol,"action":"flat","reason":"no_rsi"}
    r=rsi[-1]
    v=_vwap(bars)[-1]
    c=closes[-1]
    have_long = _has_long(symbol) is not None
    if r < 27:  # oversold
        return {"symbol":symbol,"action":"buy","reason":"rsi_oversold"}
    if have_long and (r > 60 or c >= v*1.01):
        return {"symbol":symbol,"action":"sell","reason":"mean_revert_exit_vwap"}
    return {"symbol":symbol,"action":"flat","reason":"hold_in_pos" if have_long else "no_signal"}

def run_scan(symbols, timeframe, limit, notional, dry, extra):
    results, placed = [], []
    epoch=int(time.time())
    for s in symbols:
        dec=_decide(s, _bars(s,timeframe,limit))
        results.append(dec)
        if dry or dec["action"]=="flat": continue
        if dec["action"]=="sell" and not _has_long(s): continue
        coid=f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        res=_place(s, dec["action"], notional, coid)
        if "error" not in res:
            placed.append({"symbol":s,"side":dec["action"],"notional":notional,
                           "status":res.get("status","accepted"),
                           "client_order_id":res.get("client_order_id",coid),
                           "filled_avg_price":res.get("filled_avg_price"),"id":res.get("id")})
    return {"strategy":STRATEGY_NAME,"version":STRATEGY_VERSION,"results":results,"placed":placed}
