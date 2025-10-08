# strategies/c4.py
# Version: 1.2.0 (2025-09-30)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import requests
import broker as br, math

STRATEGY_NAME="c4"
STRATEGY_VERSION="1.2.1"

ALPACA_TRADE_HOST=os.getenv("ALPACA_TRADE_HOST","https://paper-api.alpaca.markets")
ALPACA_DATA_HOST =os.getenv("ALPACA_DATA_HOST","https://data.alpaca.markets")
ALPACA_KEY_ID    =os.getenv("ALPACA_KEY_ID","")
ALPACA_SECRET_KEY=os.getenv("ALPACA_SECRET_KEY","")


C4_EMA_FAST = int(os.getenv("C4_EMA_FAST", "12"))
C4_EMA_SLOW = int(os.getenv("C4_EMA_SLOW", "50"))
C4_ATR_LEN  = int(os.getenv("C4_ATR_LEN",  "14"))
C4_ATR_K    = float(os.getenv("C4_ATR_K",  "2.0"))


def _hdr():
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY): return {}
    return {"APCA-API-KEY-ID":ALPACA_KEY_ID,"APCA-API-SECRET-KEY":ALPACA_SECRET_KEY,"Accept":"application/json","Content-Type":"application/json"}
def _sym(s): return s.replace("/","")

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

def _has_long(symbol):
    sym=_sym(symbol)
    for p in _positions():
        if (p.get("symbol") or p.get("asset_symbol"))==sym and p.get("side","long")=="long":
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str) -> Dict[str,Any]:
    try:
        return br.place_order(symbol, side, notional, client_id)
    except Exception as ex:
        return {"error": str(ex)}

def _ema(vals, n):
    if not vals: return []
    k=2/(n+1); out=[]; ema=None
    for v in vals:
        ema = v if ema is None else v*k + ema*(1-k)
        out.append(ema)
    return out

def _atr_lite(bars, n=14):
    trs=[]
    for i in range(1,len(bars)):
        h=float(bars[i]["h"]); l=float(bars[i]["l"]); pc=float(bars[i-1]["c"])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(trs)<n: return None
    return sum(trs[-n:])/n

def _decide(symbol, bars):
    if len(bars)<80: return {"symbol":symbol,"action":"flat","reason":"insufficient_bars"}
    closes=[float(b["c"]) for b in bars]
    ema50=_ema(closes,50)[-1]
    c=closes[-1]
    atr=_atr_lite(bars,14) or 0.0
    have_long=_has_long(symbol) is not None
    if c>ema50*1.001:
        return {"symbol":symbol,"action":"buy","reason":"trend_up_ema50"}
    if have_long and c < (ema50 - 1.5*atr):
        return {"symbol":symbol,"action":"sell","reason":"trailing_stop_ema50_atr"}
    return {"symbol":symbol,"action":"flat","reason":"hold_in_pos" if have_long else "no_signal"}

def run_scan(symbols, timeframe, limit, notional, dry, extra):
    results, placed=[],[]
    epoch=int(time.time())
    for s in symbols:
        dec=_decide(s,_bars(s,timeframe,limit))
        results.append(dec)
        if dry or dec["action"]=="flat": continue
        if dec["action"]=="sell" and not _has_long(s): continue
        coid=f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        res=_place(s,dec["action"],notional,coid)
        if "error" not in res:
            placed.append({"symbol":s,"side":dec["action"],"notional":notional,"status":res.get("status","accepted"),
                           "client_order_id":res.get("client_order_id",coid),"filled_avg_price":res.get("filled_avg_price"),
                           "id":res.get("id")})
    return {"strategy":STRATEGY_NAME,"version":STRATEGY_VERSION,"results":results,"placed":placed}
