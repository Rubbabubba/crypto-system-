# strategies/c5.py — v1.9.0
# Breakout above prior N-bar high with EMA/ATR context; exits = failed breakout, EMA cross-down.
# Signature: run(df_map, params, positions) -> List[{symbol, action, reason}]

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME="c5"; VERSION="1.9.0"

def _ok_df(df: pd.DataFrame)->bool:
    need={"open","high","low","close","volume"}
    return isinstance(df,pd.DataFrame) and need.issubset(df.columns) and len(df)>=60
def _ema(s,n): return s.ewm(span=int(n),adjust=False).mean()
def _sma(s,n): return s.rolling(int(n)).mean()
def _atr(df,n):
    h,l,c=df["high"],df["low"],df["close"]; pc=c.shift(1)
    tr=pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return _sma(tr,int(n))
def _in_pos(positions: Dict[str,float], sym:str)->bool:
    try: return float(positions.get(sym,0.0))>0.0
    except: return False

def run(df_map: Dict[str,pd.DataFrame], params: Dict[str,Any], positions: Dict[str,float])->List[Dict[str,Any]]:
    breakout_len=int(params.get("breakout_len",20))
    ema_len=int(params.get("ema_len",20))
    atr_len=int(params.get("atr_len",14))
    use_ema_filter=bool(params.get("use_ema_filter",True))
    use_vol=bool(params.get("use_vol",False))
    vol_sma_len=int(params.get("vol_sma_len",20))
    min_atr_frac=float(params.get("min_atr_frac",0.0))
    k_fail=float(params.get("k_fail",0.25))
    # k_trail retained in interface but stateless approximation is intentionally not used here

    results: List[Dict[str,Any]]=[]
    for sym, df in df_map.items():
        if not _ok_df(df): results.append({"symbol":sym,"action":"flat","reason":"insufficient_data"}); continue
        df=df.sort_index()
        if df.tail(2).isna().any().any(): results.append({"symbol":sym,"action":"flat","reason":"nan_tail"}); continue

        close=df["close"]; vol=df["volume"]
        roll = df["high"].rolling(breakout_len).max()
        if len(roll)<2 or pd.isna(roll.iloc[-2]):
            results.append({"symbol":sym,"action":"flat","reason":"warming_up"}); continue
        hh = roll.iloc[-2]

        ema_now=_ema(close,ema_len).iloc[-1]; atr_now=_atr(df,atr_len).iloc[-1]
        if pd.isna(ema_now) or pd.isna(atr_now):
            results.append({"symbol":sym,"action":"flat","reason":"warming_up"}); continue

        c = close.iloc[-1]
        ema_ok = (c > ema_now) if use_ema_filter else True
        if use_vol:
            if len(vol)<vol_sma_len+1: vol_ok=False
            else: vol_ok = vol.iloc[-1] > _sma(vol,vol_sma_len).iloc[-1]
        else:
            vol_ok=True
        atr_ok = True if min_atr_frac<=0 else ((atr_now / max(c,1e-9)) >= min_atr_frac)

        have=_in_pos(positions,sym)

        if have:
            if k_fail>=0 and c <= (hh - k_fail*atr_now):
                results.append({"symbol":sym,"action":"sell","reason":"failed_breakout"}); continue
            if c < ema_now:
                results.append({"symbol":sym,"action":"sell","reason":f"ema_cross_down_{ema_len}"}); continue
            results.append({"symbol":sym,"action":"flat","reason":"hold_in_pos"}); continue

        broke_out = c > hh
        if broke_out and ema_ok and vol_ok and atr_ok:
            results.append({"symbol":sym,"action":"buy","reason":f"breakout_{breakout_len}"+("_ema_ok" if use_ema_filter else "")+("_vol_ok" if use_vol else "")})
        else:
            bits=[]
            if not broke_out: bits.append("no_break")
            if use_ema_filter and not ema_ok: bits.append("ema_fail")
            if use_vol and not vol_ok: bits.append("vol_fail")
            if min_atr_frac>0 and not atr_ok: bits.append("atr_frac_fail")
            results.append({"symbol":sym,"action":"flat","reason":" & ".join(bits) if bits else "no_signal"})

    return results
