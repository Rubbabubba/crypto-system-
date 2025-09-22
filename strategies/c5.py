# strategies/c5.py
# Version: 1.8.3
# - Percent breakout entries; exits on EMA giveback or ATR stop.
# - Robust order_id extraction and OHLC handling.
# - Passes params for client attribution.

from __future__ import annotations
from typing import Any, Dict, List, Tuple
import pandas as pd

def _p(d,k,dv):
    v=d.get(k,dv)
    try:
        if isinstance(dv,int): return int(v)
        if isinstance(dv,float): return float(v)
        if isinstance(dv,bool): return str(v).lower() not in ("0","false","")
        return v
    except Exception: return dv

def _resolve_ohlc(df)->Tuple[pd.Series,pd.Series,pd.Series]:
    cols=getattr(df,"columns",[])
    def first(*names):
        for n in names:
            if n in cols: return df[n]
        raise KeyError(f"missing columns {names}")
    h=first("h","high","High"); l=first("l","low","Low"); c=first("c","close","Close")
    return h,l,c

def _ema(s,span): return s.ewm(span=span, adjust=False).mean()
def _atr_from_hlc(h,l,c,length:int):
    prev=c.shift(1); a=(h-l).abs(); b=(h-prev).abs(); c_=(l-prev).abs()
    tr=a.combine(b,max).combine(c_,max); return tr.rolling(length).mean()
def _highest(s,n): return s.rolling(n).max()
def _lowest(s,n):  return s.rolling(n).min()

def _qty_from_positions(positions, symbol)->float:
    for p in positions or []:
        sym=p.get("symbol") or p.get("asset_symbol") or ""
        if sym==symbol:
            try: return float(p.get("qty") or p.get("quantity") or 0)
            except Exception: return 0.0
    return 0.0

def _order_id(res):
    if not res: return None
    if isinstance(res, dict):
        for k in ("id","order_id","client_order_id","clientOrderId"):
            v = res.get(k)
            if v: return v
        data = res.get("data")
        if isinstance(data, dict):
            for k in ("id","order_id","client_order_id","clientOrderId"):
                v = data.get(k)
                if v: return v
    return None

def run(market, broker, symbols, params, *, dry, log):
    tf=_p(params,"timeframe","5Min"); limit=_p(params,"limit",600); notional=_p(params,"notional",0.0)
    breakout_len=_p(params,"breakout_len",20); ema_len=_p(params,"ema_len",20)
    atr_len=_p(params,"atr_len",14); giveback_pct=_p(params,"giveback_pct",0.005); atr_stop_frac=_p(params,"atr_stop_frac",0.25)

    out={"ok":True,"strategy":"c5","dry":dry,"results":[]}

    positions=[]
    if not dry:
        try: positions=broker.positions()
        except Exception as e: log(event="positions_error", error=str(e))

    try:
        data=market.candles(symbols,timeframe=tf,limit=limit)
    except Exception as e:
        return {"ok":False,"strategy":"c5","error":f"candles_error:{e}"}

    for s in symbols:
        df=(data or {}).get(s)
        need=max(breakout_len,ema_len,atr_len)+2
        if df is None or len(df)<need:
            out["results"].append({"symbol":s,"action":"flat","reason":"insufficient_bars"}); continue
        try:
            h,l,c=_resolve_ohlc(df)
        except KeyError:
            out["results"].append({"symbol":s,"action":"flat","reason":"bad_columns"}); continue

        ema=_ema(c,ema_len); atr=_atr_from_hlc(h,l,c,atr_len); hh=_highest(h,breakout_len); ll=_lowest(c,max(5,atr_len))
        close=float(c.iloc[-1])
        ema_now=float(ema.iloc[-1]); atr_now=float(atr.iloc[-1]); hh_now=float(hh.iloc[-1]); ll_now=float(ll.iloc[-1])

        broke_out = close >= hh_now
        ema_giveback = close < ema_now * (1 - giveback_pct)
        atr_stop = close <= ll_now - atr_now * atr_stop_frac

        pos_qty=_qty_from_positions(positions,s) if not dry else 0.0
        action,reason,order_id="flat","no_signal",None

        if broke_out:
            action,reason="buy","breakout_hh"
            if not dry and notional>0:
                try:
                    res=broker.notional(s,"buy",usd=notional,params=params)
                    order_id=_order_id(res)
                except Exception as e:
                    action,reason="flat",f"buy_error:{e}"
        elif pos_qty>0 and (ema_giveback or atr_stop):
            action,reason="sell","exit_giveback" if ema_giveback else "exit_atr_stop"
            if not dry:
                try:
                    res=broker.paper_sell(s,qty=pos_qty,params=params)
                    order_id=_order_id(res)
                except Exception as e:
                    action,reason="flat",f"sell_error:{e}"

        out["results"].append({
            "symbol":s,"action":action,"reason":reason,"close":close,
            "hh":hh_now,"ema":ema_now,"atr":atr_now,"order_id":order_id,
            "notional": notional if (not dry and action=="buy") else 0.0
        })

    return out
