# strategies/c2.py
# Version: 1.3.0 (2025-09-30)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import requests, math

STRATEGY_NAME = "c2"
STRATEGY_VERSION = "1.3.0"

ALPACA_TRADE_HOST = os.getenv("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
ALPACA_DATA_HOST  = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets")
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

def _hdr():
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY): return {}
    return {"APCA-API-KEY-ID": ALPACA_KEY_ID, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY, "Accept":"application/json","Content-Type":"application/json"}

def _sym(s:str)->str: return s.replace("/","")

def _bars(symbol: str, timeframe: str, limit: int) -> List[Dict[str,Any]]:
    if not _hdr(): return []
    url = f"{ALPACA_DATA_HOST}/v1beta3/crypto/us/bars"
    params = {"symbols": _sym(symbol), "timeframe": timeframe, "limit": limit, "feed":"us"}
    try:
        r = requests.get(url, headers=_hdr(), params=params, timeout=20)
        if r.status_code != 200: return []
        return r.json().get("bars",{}).get(_sym(symbol),[])
    except Exception:
        return []

def _positions() -> List[Dict[str,Any]]:
    if not _hdr(): return []
    try:
        r = requests.get(f"{ALPACA_TRADE_HOST}/v2/positions", headers=_hdr(), timeout=20)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []

def _has_long(symbol: str):
    sym = _sym(symbol)
    for p in _positions():
        if (p.get("symbol") or p.get("asset_symbol")) == sym and p.get("side","long")=="long":
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str) -> Dict[str,Any]:
    if not _hdr(): return {"error":"no_alpaca_creds"}
    payload = {"symbol": _sym(symbol),"side":side,"type":"market","time_in_force":"ioc","notional":str(notional),"client_order_id":client_id}
    try:
        r = requests.post(f"{ALPACA_TRADE_HOST}/v2/orders", headers=_hdr(), json=payload, timeout=20)
        return r.json() if r.status_code in (200,201) else {"error": f"order_http_{r.status_code}", "details": r.text}
    except Exception as e:
        return {"error":"order_exc","details":str(e)}

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
