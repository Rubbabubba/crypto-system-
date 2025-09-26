# strategies/c2.py — v1.9.1
# MACD up-cross with trend & volume gates; exits = MACD down or ATR-vs-trend stop.
# Signature: run(df_map, params, positions) -> List[{symbol, action, reason}]

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME = "c2"
VERSION = "1.9.1"

def _ema(s: pd.Series, n: int) -> pd.Series: return s.ewm(span=int(n), adjust=False).mean()
def _sma(s: pd.Series, n: int) -> pd.Series: return s.rolling(int(n)).mean()
def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h,l,c = df["high"], df["low"], df["close"]; pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return _sma(tr, int(n))
def _macd(s: pd.Series, f=12, sl=26, sig=9):
    e1=_ema(s,f); e2=_ema(s,sl); line=e1-e2; sigl=_ema(line,sig); return line, sigl, line-sigl
def _ok_df(df: pd.DataFrame) -> bool:
    need={"open","high","low","close","volume"}
    return isinstance(df,pd.DataFrame) and need.issubset(df.columns) and len(df)>=60
def _in_pos(positions: Dict[str,float], sym: str)->bool:
    try: return float(positions.get(sym,0.0))>0.0
    except: return False

def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    f=int(params.get("macd_fast",12)); s=int(params.get("macd_slow",26)); g=int(params.get("macd_signal",9))
    use_trend=bool(params.get("use_trend",True))
    trend_mode=str(params.get("trend_mode","intraday")).lower() # "daily"|"intraday"
    trend_sma_d=int(params.get("trend_sma_d",10)); ema_trend_len=int(params.get("ema_trend_len",50))
    use_vol=bool(params.get("use_vol",True)); vol_sma_len=int(params.get("vol_sma_len",20))
    atr_len=int(params.get("atr_len",14)); atr_mult=float(params.get("atr_mult",0.8))
    daily_map: Dict[str,pd.DataFrame]=params.get("daily_df_map",{}) or {}

    results: List[Dict[str,Any]]=[]

    for sym, df in df_map.items():
        if not _ok_df(df):
            results.append({"symbol":sym,"action":"flat","reason":"insufficient_intraday"}); continue
        df=df.sort_index()
        if df.tail(2).isna().any().any():
            results.append({"symbol":sym,"action":"flat","reason":"nan_tail"}); continue

        c=df["close"]; v=df["volume"]
        macd_line, sig_line, _ = _macd(c,f,s,g)
        if len(macd_line)<3 or pd.isna(macd_line.iloc[-2]) or pd.isna(sig_line.iloc[-2]):
            results.append({"symbol":sym,"action":"flat","reason":"warming_up"}); continue
        prev_diff=macd_line.iloc[-2]-sig_line.iloc[-2]
        last_diff=macd_line.iloc[-1]-sig_line.iloc[-1]
        macd_up=(prev_diff<=0) and (last_diff>0)
        macd_dn=(prev_diff>=0) and (last_diff<0)

        # volume gate
        if use_vol:
            if len(v)<vol_sma_len+1:
                vol_ok=False
            else:
                v_sma=_sma(v,vol_sma_len).iloc[-1]
                vol_ok = pd.notna(v_sma) and (v.iloc[-1] > v_sma)
        else:
            vol_ok=True

        # trend gate
        ema_trend_val=None
        if use_trend:
            if trend_mode=="daily":
                ddf=daily_map.get(sym)
                if _ok_df(ddf):
                    ddf=ddf.sort_index()
                    sma10=_sma(ddf["close"],trend_sma_d).iloc[-1]
                    trend_ok = pd.notna(sma10) and (ddf["close"].iloc[-1] > sma10)
                else:
                    trend_ok=False
            else:
                ema_trend_val=_ema(c,ema_trend_len).iloc[-1]
                trend_ok = pd.notna(ema_trend_val) and (c.iloc[-1] > ema_trend_val)
        else:
            trend_ok=True

        have=_in_pos(positions,sym)

        if have:
            atr_now=_atr(df,atr_len).iloc[-1]
            if ema_trend_val is None:
                ema_trend_val=_ema(c,max(ema_trend_len,s)).iloc[-1]
            if macd_dn:
                results.append({"symbol":sym,"action":"sell","reason":"macd_down_cross"}); continue
            if pd.notna(ema_trend_val) and pd.notna(atr_now):
                stop_line = ema_trend_val - atr_mult*atr_now
                if c.iloc[-1] < stop_line:
                    results.append({"symbol":sym,"action":"sell","reason":"atr_vs_trend_stop"}); continue
            results.append({"symbol":sym,"action":"flat","reason":"hold_in_pos"}); continue

        if (not have) and macd_up and trend_ok and vol_ok:
            results.append({"symbol":sym,"action":"buy","reason":"macd_up_cross_trend_vol_ok"})
        else:
            bits=[]
            if not macd_up: bits.append("no_macd_up")
            if not trend_ok: bits.append("trend_fail")
            if not vol_ok: bits.append("vol_fail")
            results.append({"symbol":sym,"action":"flat","reason":" & ".join(bits) if bits else "no_signal"})

    return results
