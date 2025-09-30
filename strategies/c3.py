# strategies/c3.py
# Version: 1.2.0 (2025-09-30)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import requests

STRATEGY_NAME = "c3"
STRATEGY_VERSION = "1.2.0"

ALPACA_TRADE_HOST = os.getenv("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
ALPACA_DATA_HOST  = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets")
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

def _hdr():
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY): return {}
    return {"APCA-API-KEY-ID": ALPACA_KEY_ID, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY, "Accept":"application/json","Content-Type":"application/json"}
def _sym(s): return s.replace("/","")

def _bars(symbol, timeframe, limit):
    if not _hdr(): return []
    try:
        r = requests.get(f"{ALPACA_DATA_HOST}/v1beta3/crypto/us/bars",
                         headers=_hdr(),
                         params={"symbols":_sym(symbol),"timeframe":timeframe,"limit":limit,"feed":"us"},
                         timeout=20)
        return r.json().get("bars",{}).get(_sym(symbol),[]) if r.status_code==200 else []
    except Exception:
        return []

def _positions():
    if not _hdr(): return []
    try:
        r=requests.get(f"{ALPACA_TRADE_HOST}/v2/positions",headers=_hdr(),timeout=20)
        return r.json() if r.status_code==200 else []
    except Exception:
        return []

def _has_long(symbol):
    sym=_sym(symbol)
    for p in _positions():
        if (p.get("symbol") or p.get("asset_symbol"))==sym and p.get("side","long")=="long":
            return p
    return None

def _place(symbol, side, notional, client_id):
    if not _hdr(): return {"error":"no_alpaca_creds"}
    payload={"symbol":_sym(symbol),"side":side,"type":"market","time_in_force":"ioc","notional":str(notional),"client_order_id":client_id}
    try:
        r=requests.post(f"{ALPACA_TRADE_HOST}/v2/orders",headers=_hdr(),json=payload,timeout=20)
        return r.json() if r.status_code in (200,201) else {"error":f"order_http_{r.status_code}","details":r.text}
    except Exception as e:
        return {"error":"order_exc","details":str(e)}

def _decide(symbol, bars):
    if len(bars)<120: return {"symbol":symbol,"action":"flat","reason":"insufficient_bars"}
    highs=[float(b["h"]) for b in bars]
    closes=[float(b["c"]) for b in bars]
    c=closes[-1]
    look=60
    prior_max=max(highs[-(look+1):-1])
    have_long=_has_long(symbol) is not None
    # breakout
    if c>prior_max*1.002:
        return {"symbol":symbol,"action":"buy","reason":"breakout_lookback"}
    # failed breakout exit if we’re long: drop back under prior_max
    if have_long and c<prior_max*0.998:
        return {"symbol":symbol,"action":"sell","reason":"failed_breakout"}
    return {"symbol":symbol,"action":"flat","reason":"hold_in_pos" if have_long else "no_signal"}

def run_scan(symbols, timeframe, limit, notional, dry, extra):
    out, placed=[],[]
    epoch=int(time.time())
    for s in symbols:
        dec=_decide(s,_bars(s,timeframe,limit))
        out.append(dec)
        if dry or dec["action"]=="flat": continue
        if dec["action"]=="sell" and not _has_long(s): continue
        coid=f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        res=_place(s,dec["action"],notional,coid)
        if "error" not in res:
            placed.append({"symbol":s,"side":dec["action"],"notional":notional,
                           "status":res.get("status","accepted"),"client_order_id":res.get("client_order_id",coid),
                           "filled_avg_price":res.get("filled_avg_price"),"id":res.get("id")})
    return {"strategy":STRATEGY_NAME,"version":STRATEGY_VERSION,"results":out,"placed":placed}
