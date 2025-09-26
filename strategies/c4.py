# strategies/c4.py — v1.9.0
# Volume Bubbles Breakout (LuxAlgo-inspired) with stateless exits.
# Signature: run(df_map, params, positions) -> List[{symbol, action, reason}]

from __future__ import annotations
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

NAME="c4"; VERSION="1.9.0"

def _ok_df(df: pd.DataFrame)->bool:
    need={"open","high","low","close","volume"}; return isinstance(df,pd.DataFrame) and need.issubset(df.columns) and len(df)>=5
def _atr(df,n:int)->pd.Series:
    h,l,c=df["high"],df["low"],df["close"]; pc=c.shift(1)
    tr=pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return tr.rolling(int(n)).mean()

def _split_vol_row(o,h,l,c,v):
    bar_top=h-max(o,c); bar_bot=min(o,c)-l; bar_rng=h-l; bull=(c-o)>0
    buy_rng = bar_rng if bull else (bar_top+bar_bot)
    sell_rng= (bar_top+bar_bot) if bull else bar_rng
    total_r = bar_rng + bar_top + bar_bot
    if total_r<=0 or v<=0: return 0.0,0.0
    return float((buy_rng/total_r)*v), float((sell_rng/total_r)*v)

def _bubble_from_df(df_b: pd.DataFrame)->Optional[Dict[str,float]]:
    if not _ok_df(df_b) or len(df_b)<2: return None
    b=df_b.sort_index().iloc[-2]
    buy,sell=_split_vol_row(b["open"],b["high"],b["low"],b["close"],b["volume"])
    total=buy+sell; delta=buy-sell
    return dict(high=float(b["high"]), low=float(b["low"]), total=float(total), delta=float(delta))

def _bubble_emulate_from_intraday(df: pd.DataFrame, bars_per_bubble: int)->Optional[Dict[str,float]]:
    n=int(bars_per_bubble or 0)
    if not _ok_df(df) or n<5 or len(df)<n+1: return None
    window=df.sort_index().iloc[-(n+1):-1]
    highs=window["high"]; lows=window["low"]; opens=window["open"]; closes=window["close"]; vols=window["volume"]
    buy=sell=0.0
    for o,h,l,c,v in zip(opens,highs,lows,closes,vols):
        b,s=_split_vol_row(o,h,l,c,v); buy+=b; sell+=s
    return dict(high=float(highs.max()), low=float(lows.min()), total=float(buy+sell), delta=float(buy-sell))

def run(df_map: Dict[str,pd.DataFrame], params: Dict[str,Any], positions: Dict[str,float])->List[Dict[str,Any]]:
    min_total_vol=float(params.get("min_total_vol",0.0))
    min_delta_frac=float(params.get("min_delta_frac",0.30))
    break_k_atr=float(params.get("break_k_atr",0.0))
    atr_len=int(params.get("atr_len",14))
    atr_mult_stop=float(params.get("atr_mult_stop",1.0))
    fail_k_atr=float(params.get("fail_k_atr",0.0))
    bubble_map: Dict[str,pd.DataFrame]=params.get("bubble_df_map",{}) or {}
    bars_per_bubble=int(params.get("bars_per_bubble",60))

    out: List[Dict[str,Any]]=[]

    for sym, intraday in df_map.items():
        if not _ok_df(intraday):
            out.append({"symbol":sym,"action":"flat","reason":"insufficient_intraday"}); continue
        intraday=intraday.sort_index()
        if intraday.tail(2).isna().any().any():
            out.append({"symbol":sym,"action":"flat","reason":"nan_tail"}); continue
        if len(intraday) < max(atr_len+5, 50):
            out.append({"symbol":sym,"action":"flat","reason":"warming_up"}); continue

        c=float(intraday["close"].iloc[-1])
        atr_now=float(_atr(intraday,atr_len).iloc[-1]) if not pd.isna(_atr(intraday,atr_len).iloc[-1]) else np.nan
        if pd.isna(atr_now):
            out.append({"symbol":sym,"action":"flat","reason":"atr_nan"}); continue

        bdf=bubble_map.get(sym)
        bub=_bubble_from_df(bdf) if _ok_df(bdf) and len(bdf)>=2 else _bubble_emulate_from_intraday(intraday, bars_per_bubble)
        if not bub:
            out.append({"symbol":sym,"action":"flat","reason":"no_bubble"}); continue

        bub_hi=bub["high"]; bub_lo=bub["low"]; total=bub["total"]; delta=bub["delta"]
        frac=(abs(delta)/total) if total>0 else 0.0

        have=False
        try: have=float(positions.get(sym,0.0))>0.0
        except: have=False

        if have:
            if c <= (bub_hi - fail_k_atr*atr_now):
                out.append({"symbol":sym,"action":"sell","reason":"failed_breakout"}); continue
            if c <= (bub_lo - atr_mult_stop*atr_now):
                out.append({"symbol":sym,"action":"sell","reason":"atr_vs_bubble_low"}); continue
            out.append({"symbol":sym,"action":"flat","reason":"hold_in_pos"}); continue

        pass_total = (total >= min_total_vol)
        pass_delta = (frac >= min_delta_frac)
        thr = bub_hi + break_k_atr*atr_now
        if pass_total and pass_delta and c > thr:
            out.append({"symbol":sym,"action":"buy","reason":"bubble_breakout"})
        else:
            bits=[]
            if not pass_total: bits.append("total<th")
            if not pass_delta: bits.append("delta_frac<th")
            if not (c>thr): bits.append("no_break")
            out.append({"symbol":sym,"action":"flat","reason":" & ".join(bits) if bits else "no_signal"})

    return out
