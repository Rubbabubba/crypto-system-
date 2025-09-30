# strategies/c5.py
# Version: 1.2.0 (2025-09-30)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import requests
import broker as br

STRATEGY_NAME="c5"
STRATEGY_VERSION="1.2.1"

ALPACA_TRADE_HOST=os.getenv("ALPACA_TRADE_HOST","https://paper-api.alpaca.markets")
ALPACA_DATA_HOST =os.getenv("ALPACA_DATA_HOST","https://data.alpaca.markets")
ALPACA_KEY_ID    =os.getenv("ALPACA_KEY_ID","")
ALPACA_SECRET_KEY=os.getenv("ALPACA_SECRET_KEY","")

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

def _decide(symbol, bars):
    if len(bars)<100: return {"symbol":symbol,"action":"flat","reason":"insufficient_bars"}
    closes=[float(b["c"]) for b in bars]
    efast=_ema(closes,21)
    eslow=_ema(closes,55)
    have_long=_has_long(symbol) is not None
    # cross detection using last 2 points
    if len(efast)>=2 and len(eslow)>=2:
        prev=efast[-2]-eslow[-2]
        curr=efast[-1]-eslow[-1]
        if prev<=0 and curr>0:  # golden cross
            return {"symbol":symbol,"action":"buy","reason":"ema_golden_cross_21_55"}
        if have_long and prev>=0 and curr<0:  # death cross exit
            return {"symbol":symbol,"action":"sell","reason":"ema_death_cross_exit"}
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
