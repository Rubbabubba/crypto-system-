# strategies/c6.py — v1.9.0
# EMA fast/slow (MACD-style) + higher-high confirmation; exits = cross-down or ATR-vs-slow stop.
# Signature: run(df_map, params, positions) -> List[{symbol, action, reason}]

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME="c6"; VERSION="1.9.0"

def _ok_df(df: pd.DataFrame)->bool:
    need={"open","high","low","close","volume"}; return isinstance(df,pd.DataFrame) and need.issubset(df.columns) and len(df)>=60
def _ema(s,n): return s.ewm(span=int(n),adjust=False).mean()
def _sma(s,n): return s.rolling(int(n)).mean()
def _atr(df,n):
    h,l,c=df["high"],df["low"],df["close"]; pc=c.shift(1)
    tr=pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return _sma(tr,int(n))
def _in_pos(positions: Dict[str,float], sym:str)->bool:
    try: return float(positions.get(sym,0.0))>0.0
    except: return False
def _cross_up(x,y): return len(x)>=2 and len(y)>=2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) and (x.iloc[-2] <= y.iloc[-2]) and (x.iloc[-1] > y.iloc[-1])
def _cross_dn(x,y): return len(x)>=2 and len(y)>=2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) and (x.iloc[-2] >= y.iloc[-2]) and (x.iloc[-1] < y.iloc[-1])

def run(df_map: Dict[str,pd.DataFrame], params: Dict[str,Any], positions: Dict[str,float])->List[Dict[str,Any]]:
    ef=int(params.get("ema_fast_len",12)); es=int(params.get("ema_slow_len",26))
    hh_n=int(params.get("confirm_hh_len",10))
    atr_len=int(params.get("atr_len",14)); atr_mult_stop=float(params.get("atr_mult_stop",1.0))
    use_vol=bool(params.get("use_vol",False)); vol_sma_len=int(params.get("vol_sma_len",20))

    out: List[Dict[str,Any]]=[]
    for sym, df in df_map.items():
        if not _ok_df(df): out.append({"symbol":sym,"action":"flat","reason":"insufficient_data"}); continue
        df=df.sort_index()
        if df.tail(2).isna().any().any(): out.append({"symbol":sym,"action":"flat","reason":"nan_tail"}); continue

        close=df["close"]; high=df["high"]; vol=df["volume"]
        fast=_ema(close,ef); slow=_ema(close,es)
        atr_now=_atr(df,atr_len).iloc[-1]
        if pd.isna(fast.iloc[-2]) or pd.isna(slow.iloc[-2]) or pd.isna(atr_now):
            out.append({"symbol":sym,"action":"flat","reason":"warming_up"}); continue

        have=_in_pos(positions,sym)
        c=close.iloc[-1]

        if len(high) < hh_n + 2:
            hh_confirm=False
        else:
            prior_max = high.rolling(hh_n).max().shift(1).iloc[-1]
            hh_confirm = pd.notna(prior_max) and (high.iloc[-1] > prior_max)

        if use_vol:
            if len(vol)<vol_sma_len+1: vol_ok=False
            else: vol_ok = vol.iloc[-1] > _sma(vol,vol_sma_len).iloc[-1]
        else:
            vol_ok=True

        up=_cross_up(fast,slow); dn=_cross_dn(fast,slow)

        if have:
            if dn:
                out.append({"symbol":sym,"action":"sell","reason":"ema_cross_down"}); continue
            stop_line = slow.iloc[-1] - atr_mult_stop*atr_now if pd.notna(slow.iloc[-1]) else None
            if (stop_line is not None) and (c < stop_line):
                out.append({"symbol":sym,"action":"sell","reason":"atr_vs_slow_ema_stop"}); continue
            out.append({"symbol":sym,"action":"flat","reason":"hold_in_pos"}); continue

        if up and hh_confirm and vol_ok:
            out.append({"symbol":sym,"action":"buy","reason":"cross_up_hh_confirm" + ("_vol_ok" if use_vol else "")})
        else:
            bits=[]
            if not up: bits.append("no_cross_up")
            if not hh_confirm: bits.append("no_higher_high")
            if not vol_ok: bits.append("vol_fail")
            out.append({"symbol":sym,"action":"flat","reason":" & ".join(bits) if bits else "no_signal"})

    return out
