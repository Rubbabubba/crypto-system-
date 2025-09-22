# strategies/c4.py
# Version: 1.8.2
# EMA + ATR filter; symmetric exits.
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

def _qty_from_positions(positions, symbol)->float:
    for p in positions or []:
        sym=p.get("symbol") or p.get("asset_symbol") or ""
        if sym==symbol:
            try: return float(p.get("qty") or p.get("quantity") or 0)
            except Exception: return 0.0
    return 0.0

def run(market, broker, symbols, params, *, dry, log):
    tf=_p(params,"timeframe","5Min"); limit=_p(params,"limit",600); notional=_p(params,"notional",0.0)
    ema_len=_p(params,"ema_len",20); atr_len=_p(params,"atr_len",14); delta_frac=_p(params,"delta_frac",1.0) # % of ATR

    out={"ok":True,"strategy":"c4","dry":dry,"results":[]}
    positions=[]
    if not dry:
        try: positions=broker.positions()
        except Exception as e: log(event="positions_error", error=str(e))

    try:
        data=market.candles(symbols,timeframe=tf,limit=limit)
    except Exception as e:
        return {"ok":False,"strategy":"c4","error":f"candles_error:{e}"}

    for s in symbols:
        df=(data or {}).get(s)
        if df is None or len(df)<max(ema_len,atr_len)+2:
            out["results"].append({"symbol":s,"action":"flat","reason":"insufficient_bars"}); continue
        try:
            h,l,c=_resolve_ohlc(df)
        except KeyError:
            out["results"].append({"symbol":s,"action":"flat","reason":"bad_columns"}); continue

        ema=_ema(c,ema_len); atr=_atr_from_hlc(h,l,c,atr_len)
        close=float(c.iloc[-1]); ema_now=float(ema.iloc[-1]); atr_now=float(atr.iloc[-1])
        # signal when price is above EMA by delta_frac * ATR; exit below EMA by same
        above = close >= ema_now + atr_now*delta_frac/100.0
        below = close <= ema_now - atr_now*delta_frac/100.0
        pos_qty=_qty_from_positions(positions,s) if not dry else 0.0

        action,reason,order_id="flat","no_signal",None
        if above:
            action,reason="buy","above_ema_by_delta"
            if not dry and notional>0:
                try:
                    res=broker.notional(s,"buy",usd=notional,params=params)
                    order_id=(res or {}).get("id")
                except Exception as e:
                    action,reason="flat",f"buy_error:{e}"
        elif pos_qty>0 and below:
            action,reason="sell","below_ema_by_delta"
            if not dry:
                try:
                    res=broker.paper_sell(s,qty=pos_qty,params=params)
                    order_id=(res or {}).get("id")
                except Exception as e:
                    action,reason="flat",f"sell_error:{e}"

        out["results"].append({"symbol":s,"action":action,"reason":reason,"close":close,"ema":ema_now,"atr":atr_now,"order_id":order_id})

    return out
