# strategies/c3.py
# Version: 1.8.2
# Simple MA cross with symmetric exits.
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

def _ma(s: pd.Series, n:int)->pd.Series: return s.rolling(n).mean()

def _qty_from_positions(positions, symbol)->float:
    for p in positions or []:
        sym=p.get("symbol") or p.get("asset_symbol") or ""
        if sym==symbol:
            try: return float(p.get("qty") or p.get("quantity") or 0)
            except Exception: return 0.0
    return 0.0

def run(market, broker, symbols, params, *, dry, log):
    tf=_p(params,"timeframe","5Min"); limit=_p(params,"limit",600); notional=_p(params,"notional",0.0)
    fast=_p(params,"ma_fast",10); slow=_p(params,"ma_slow",30)

    out={"ok":True,"strategy":"c3","dry":dry,"results":[]}
    positions=[]
    if not dry:
        try: positions=broker.positions()
        except Exception as e: log(event="positions_error", error=str(e))

    try:
        data=market.candles(symbols,timeframe=tf,limit=limit)
    except Exception as e:
        return {"ok":False,"strategy":"c3","error":f"candles_error:{e}"}

    for s in symbols:
        df=(data or {}).get(s)
        if df is None or len(df)<slow+2:
            out["results"].append({"symbol":s,"action":"flat","reason":"insufficient_bars"}); continue
        try:
            _,_,c=_resolve_ohlc(df)
        except KeyError:
            out["results"].append({"symbol":s,"action":"flat","reason":"bad_columns"}); continue

        ma1=_ma(c,fast); ma2=_ma(c,slow)
        m1_prev=float(ma1.iloc[-2]); m2_prev=float(ma2.iloc[-2])
        m1=float(ma1.iloc[-1]); m2=float(ma2.iloc[-1])
        close=float(c.iloc[-1])

        cross_up = m1_prev <= m2_prev and m1 > m2
        cross_dn = m1_prev >= m2_prev and m1 < m2

        pos_qty=_qty_from_positions(positions,s) if not dry else 0.0
        action,reason,order_id="flat","no_signal",None

        if cross_up:
            action,reason="buy","ma_cross_up"
            if not dry and notional>0:
                try:
                    res=broker.notional(s,"buy",usd=notional,params=params)
                    order_id=(res or {}).get("id")
                except Exception as e:
                    action,reason="flat",f"buy_error:{e}"
        elif pos_qty>0 and cross_dn:
            action,reason="sell","ma_cross_down"
            if not dry:
                try:
                    res=broker.paper_sell(s,qty=pos_qty,params=params)
                    order_id=(res or {}).get("id")
                except Exception as e:
                    action,reason="flat",f"sell_error:{e}"

        out["results"].append({"symbol":s,"action":action,"reason":reason,"close":close,"ma1":m1,"ma2":m2,"order_id":order_id})

    return out
