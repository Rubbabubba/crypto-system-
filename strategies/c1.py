# strategies/c1.py
# Version: 1.4.0 (2025-09-30)
from __future__ import annotations
import os, time, math, datetime as dt
from typing import Any, Dict, List
import requests

STRATEGY_NAME = "c1"
STRATEGY_VERSION = "1.4.0"

# ---------- Broker & Data helpers ----------
ALPACA_TRADE_HOST = os.getenv("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
ALPACA_DATA_HOST  = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets")
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

def _hdr():
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY):
        return {}
    return {
        "APCA-API-KEY-ID": ALPACA_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def _sym(s: str) -> str:
    return s.replace("/","")

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

def _has_long(symbol: str) -> Dict[str,Any] | None:
    sym = _sym(symbol)
    for p in _positions():
        psym = p.get("symbol") or p.get("asset_symbol")
        if psym == sym and (p.get("side","long").lower() == "long"):
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str) -> Dict[str,Any]:
    if not _hdr(): return {"error":"no_alpaca_creds"}
    payload = {
        "symbol": _sym(symbol),
        "side": side,
        "type": "market",
        "time_in_force": "ioc",
        "notional": str(notional),
        "client_order_id": client_id
    }
    try:
        r = requests.post(f"{ALPACA_TRADE_HOST}/v2/orders", headers=_hdr(), json=payload, timeout=20)
        ok = r.status_code in (200,201)
        return r.json() if ok else {"error": f"order_http_{r.status_code}", "details": r.text}
    except Exception as e:
        return {"error":"order_exc","details":str(e)}

# ---------- Indicators ----------
def _ema(vals: List[float], n: int) -> List[float]:
    if not vals or n<=0: return []
    k = 2/(n+1)
    out = []
    ema = None
    for v in vals:
        ema = v if ema is None else (v*k + ema*(1-k))
        out.append(ema)
    return out

def _vwap(bars: List[Dict[str,Any]]) -> List[float]:
    cum_pv = 0.0
    cum_v  = 0.0
    out=[]
    for b in bars:
        h,l,c,v = float(b["h"]), float(b["l"]), float(b["c"]), float(b["v"])
        tp = (h+l+c)/3.0
        cum_pv += tp*v
        cum_v  += v
        out.append(cum_pv/max(1e-9,cum_v))
    return out

# ---------- Logic ----------
def _decide(symbol: str, bars: List[Dict[str,Any]]) -> Dict[str,str]:
    # need enough bars
    if len(bars) < 60:
        return {"symbol": symbol, "action":"flat", "reason":"insufficient_bars"}
    closes = [float(b["c"]) for b in bars]
    vwap = _vwap(bars)
    ema = _ema(closes, 50)

    c = closes[-1]
    v = vwap[-1]
    e = ema[-1]

    have_long = _has_long(symbol) is not None
    # Trend filter: only long if price above EMA50
    if c > e and c < v * 0.997:         # pullback to/below VWAP in uptrend
        return {"symbol": symbol, "action":"buy", "reason":"vwap_pullback_uptrend"}
    if have_long and c < e and c < v:   # lose trend & under VWAP -> exit
        return {"symbol": symbol, "action":"sell", "reason":"trend_break_under_vwap"}
    return {"symbol": symbol, "action":"flat", "reason":"hold_in_pos" if have_long else "no_signal"}

# ---------- Public API ----------
def run_scan(symbols: List[str], timeframe: str, limit: int, notional: float, dry: bool, extra: Dict[str,Any]):
    results, placed = [], []
    epoch = int(time.time())
    for s in symbols:
        bars = _bars(s, timeframe, limit)
        decision = _decide(s, bars)
        results.append(decision)
        if dry or decision["action"] in ("flat",):
            continue
        # prevent sells if flat
        if decision["action"] == "sell" and not _has_long(s):
            continue
        coid = f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        ordres = _place(s, decision["action"], notional, coid)
        if "error" not in ordres:
            placed.append({
                "symbol": s, "side": decision["action"], "notional": notional,
                "status": ordres.get("status","accepted"), "client_order_id": ordres.get("client_order_id", coid),
                "filled_avg_price": ordres.get("filled_avg_price"), "id": ordres.get("id")
            })
    return {"strategy": STRATEGY_NAME, "version": STRATEGY_VERSION, "results": results, "placed": placed}
