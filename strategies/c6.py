# strategies/c6.py
# Version: 1.8.2
# EMA fast/slow cross + HH confirmation for entries; exits on cross-down or ATR stop.
from __future__ import annotations
from typing import Any, Dict, List, Tuple
import math
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

def _qty_from_positions(positions, symbol)->float:
    for p in positions or []:
        sym=p.get("symbol") or p.get("asset_symbol") or ""
        if sym==symbol:
            try: return float(p.get("qty") or p.get("quantity") or 0)
            except Exception: return 0.0
    return 0.0

def run(market, broker, symbols, params, *, dry, log):
    tf=_p(params,"timeframe","5Min"); limit=_p(params,"limit",600); notional=_p(params,"notional",0.0)
    fast=_p(params,"ema_fast_len",12); slow=_p(params,"ema_slow_len",26); confirm_hh=_p(params,"confirm_hh_len",20)
    atr_len=_p(params,"atr_len",14); atr_mult_stop=_p(params,"atr_mult_stop",1.5)

    out={"ok":True,"strategy":"c6","dry":dry,"results":[]}

    positions=[]
    if not dry:
        try: positions=broker.positions()
        except Exception as e: log(event="positions_error", error=str(e))

    try:
        data=market.candles(symbols,timeframe=tf,limit=limit)
    except Exception as e:
        return {"ok":False,"strategy":"c6","error":f"candles_error:{e}"}

    for s in symbols:
        df=(data or {}).get(s)
        need=max(slow,confirm_hh,atr_len)+2
        if df is None or len(df)<need:
            out["results"].append({"symbol":s,"action":"flat","reason":"insufficient_bars"}); continue
        try:
            h,l,c=_resolve_ohlc(df)
        except KeyError:
            out["results"].append({"symbol":s,"action":"flat","reason":"bad_columns"}); continue

        ef=_ema(c,fast); es=_ema(c,slow); atr=_atr_from_hlc(h,l,c,atr_len); hh=_highest(h,confirm_hh)
        close=float(c.iloc[-1]); ef_now=float(ef.iloc[-1]); es_now=float(es.iloc[-1]); atr_now=float(atr.iloc[-1]); hh_now=float(hh.iloc[-1])
        ef_prev=float(ef.iloc[-2]); es_prev=float(es.iloc[-2])

        cross_up = ef_prev <= es_prev and ef_now > es_now
        cross_dn = ef_prev >= es_prev and ef_now < es_now
        confirm  = close >= hh_now

        pos_qty=_qty_from_positions(positions,s) if not dry else 0.0
        action,reason,order_id="flat","no_signal",None

        if cross_up and confirm:
            action,reason="buy","ema_cross_up_confirm_hh"
            if not dry and notional>0:
                try:
                    res=broker.notional(s,"buy",usd=notional,params=params)
                    order_id=(res or {}).get("id")
                except Exception as e:
                    action,reason="flat",f"buy_error:{e}"
        elif pos_qty>0 and (cross_dn or close < es_now - atr_now*atr_mult_stop):
            action,reason="sell","ema_cross_down" if cross_dn else "atr_stop"
            if not dry:
                try:
                    res=broker.paper_sell(s,qty=pos_qty,params=params)
                    order_id=(res or {}).get("id")
                except Exception as e:
                    action,reason="flat",f"sell_error:{e}"

        out["results"].append({
            "symbol":s,"action":action,"reason":reason,"close":close,
            "ema_fast":ef_now,"ema_slow":es_now,"hh":hh_now,"atr":atr_now,"order_id":order_id
        })

    return out
