# strategies/c3.py — v1.9.0
# MA menu (EMA/SMA/WMA/HMA/DEMA/T3/VWMA/VWAP/HEMA) cross-up entries; exits = cross-down or ATR-vs-trend stop.
# Signature: run(df_map, params, positions) -> List[{symbol, action, reason}]

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME="c3"; VERSION="1.9.0"

def _ema(s,n): return s.ewm(span=int(n),adjust=False).mean()
def _sma(s,n): return s.rolling(int(n)).mean()
def _wma(s,n):
    if n<=1: return s.copy()
    w = pd.Series(range(1,int(n)+1), index=s.index[:int(n)])
    out=s.copy(); out.iloc[:int(n)-1]=pd.NA
    out.iloc[int(n)-1:] = (s.rolling(int(n)).apply(lambda x: (x*w.values).sum()/w.values.sum(), raw=False)).iloc[int(n)-1:]
    return out
def _hma(s,n): 
    n=int(n); 
    if n<=1: return s.copy()
    return _wma(2*_wma(s,n//2) - _wma(s,n), int(n**0.5) or 1)
def _dema(s,n): e1=_ema(s,n); e2=_ema(e1,n); return 2*e1 - e2
def _t3(s,n,b=0.7):
    e1=_ema(s,n); e2=_ema(e1,n); e3=_ema(e2,n); e4=_ema(e3,n); e5=_ema(e4,n); e6=_ema(e5,n)
    c1=-(b**3); c2=3*b*b+3*b**3; c3=-6*b*b-3*b-3*b**3; c4=1+3*b+3*b*b+b**3
    return c1*e6 + c2*e5 + c3*e4 + c4*e3
def _vwma(c,v,n): return (c.mul(v).rolling(int(n)).sum() / v.rolling(int(n)).sum()).fillna(method="bfill")
def _vwap_cont(df):
    tp=(df["high"]+df["low"]+df["close"])/3.0; vol=df["volume"].replace(0,pd.NA)
    return (tp*vol).cumsum() / vol.cumsum().fillna(method="ffill")
def _hema(df,n):
    o,h,l,c=df["open"],df["high"],df["low"],df["close"]
    ha_close=(o+h+l+c)/4.0
    ha_open=pd.Series(index=df.index, dtype="float64")
    ha_open.iloc[0]=(o.iloc[0]+c.iloc[0])/2.0
    for i in range(1,len(df)): ha_open.iloc[i]=(ha_open.iloc[i-1]+ha_close.iloc[i-1])/2.0
    return _ema(ha_open,int(n))
def _ma(df, kind, n):
    k=(str(kind) if kind else "EMA").upper(); c=df["close"]; n=int(n)
    if k=="SMA": return _sma(c,n)
    if k=="EMA": return _ema(c,n)
    if k=="WMA": return _wma(c,n)
    if k=="HMA": return _hma(c,n)
    if k=="DEMA":return _dema(c,n)
    if k=="T3":  return _t3(c,n)
    if k=="VWMA":return _vwma(c, df["volume"], n)
    if k=="VWAP":return _vwap_cont(df)
    if k=="HEMA":return _hema(df,n)
    return _ema(c,n)
def _atr(df,n):
    h,l,c=df["high"],df["low"],df["close"]; pc=c.shift(1)
    tr=pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return _sma(tr,int(n))
def _cross_up(x,y): return len(x)>=2 and len(y)>=2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) and (x.iloc[-2] <= y.iloc[-2]) and (x.iloc[-1] > y.iloc[-1])
def _cross_dn(x,y): return len(x)>=2 and len(y)>=2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) and (x.iloc[-2] >= y.iloc[-2]) and (x.iloc[-1] < y.iloc[-1])
def _ok_df(df): need={"open","high","low","close","volume"}; return isinstance(df,pd.DataFrame) and need.issubset(df.columns) and len(df)>=60
def _in_pos(positions,sym): 
    try: return float(positions.get(sym,0.0))>0.0
    except: return False

def run(df_map: Dict[str,pd.DataFrame], params: Dict[str,Any], positions: Dict[str,float])->List[Dict[str,Any]]:
    ma1_type=str(params.get("ma1_type","EMA")); ma2_type=str(params.get("ma2_type","EMA"))
    ma1_len=int(params.get("ma1_len",13)); ma2_len=int(params.get("ma2_len",34))
    atr_len=int(params.get("atr_len",14)); atr_mult=float(params.get("atr_mult",0.8))
    trend_leg=str(params.get("trend_leg","ma2")).lower()
    use_vol=bool(params.get("use_vol",False)); vol_sma_len=int(params.get("vol_sma_len",20))

    results: List[Dict[str,Any]]=[]
    for sym, df in df_map.items():
        if not _ok_df(df): results.append({"symbol":sym,"action":"flat","reason":"insufficient_data"}); continue
        df=df.sort_index()
        if df.tail(2).isna().any().any(): results.append({"symbol":sym,"action":"flat","reason":"nan_tail"}); continue

        ma1=_ma(df,ma1_type,ma1_len); ma2=_ma(df,ma2_type,ma2_len)
        if pd.isna(ma1.iloc[-2]) or pd.isna(ma2.iloc[-2]):
            results.append({"symbol":sym,"action":"flat","reason":"warming_up"}); continue

        have=_in_pos(positions,sym)
        close=df["close"]; vol=df["volume"]
        atr_now=_atr(df,atr_len).iloc[-1]
        trend_ma = ma2 if trend_leg=="ma2" else ma1

        vol_ok=True
        if use_vol:
            if len(vol)<vol_sma_len+1: vol_ok=False
            else: vol_ok = vol.iloc[-1] > _sma(vol,vol_sma_len).iloc[-1]

        up=_cross_up(ma1,ma2); dn=_cross_dn(ma1,ma2)

        if have:
            if dn:
                results.append({"symbol":sym,"action":"sell","reason":"ma_cross_down"}); continue
            if pd.notna(trend_ma.iloc[-1]) and pd.notna(atr_now):
                stop_line = trend_ma.iloc[-1] - atr_mult*atr_now
                if close.iloc[-1] < stop_line:
                    results.append({"symbol":sym,"action":"sell","reason":"atr_vs_trend_stop"}); continue
            results.append({"symbol":sym,"action":"flat","reason":"hold_in_pos"}); continue

        if up and vol_ok:
            results.append({"symbol":sym,"action":"buy","reason":"ma_cross_up" + ("_vol_ok" if use_vol else "")})
        else:
            bits=[]
            if not up: bits.append("no_cross_up")
            if not vol_ok: bits.append("vol_fail")
            results.append({"symbol":sym,"action":"flat","reason":" & ".join(bits) if bits else "no_signal"})

    return results
