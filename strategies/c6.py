# strategies/c6.py
# Version: 1.1.0 (2025-09-30)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import requests
import broker as br

STRATEGY_NAME="c6"
STRATEGY_VERSION="1.1.1"

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

def _vwap(bars):
    cum_pv=cum_v=0.0; out=[]
    for b in bars:
        h,l,c,v=float(b["h"]),float(b["l"]),float(b["c"]),float(b["v"])
        tp=(h+l+c)/3.0
        cum_pv+=tp*v; cum_v+=v
        out.append(cum_pv/max(1e-9,cum_v))
    return out

def _decide(symbol, bars):
    if len(bars)<120: return {"symbol":symbol,"action":"flat","reason":"insufficient_bars"}
    closes=[float(b["c"]) for b in bars]
    highs=[float(b["h"]) for b in bars]
    lows =[float(b["l"]) for b in bars]
    vwap=_vwap(bars)
    c=closes[-1]; v=vwap[-1]
    window=60
    rng_high=max(highs[-window:]); rng_low=min(lows[-window:])
    have_long=_has_long(symbol) is not None
    # fade extremes towards VWAP only if price is not trending away too fast
    if c <= rng_low*1.001 and c < v*0.995:
        return {"symbol":symbol,"action":"buy","reason":"range_low_vwap_dislocation"}
    if have_long and (c >= v*1.01 or c >= rng_high*0.999):
        return {"symbol":symbol,"action":"sell","reason":"revert_to_vwap_or_range_cap"}
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
