# strategies/c3.py
from __future__ import annotations
import math
from typing import Any, Dict, Iterable, List, Callable, Optional
import pandas as pd

__version__ = "1.1.2"
VERSION = (1, 1, 2)

# Helpers
def _nz(x, alt=0.0): 
    try:
        if x is None or (isinstance(x,float) and math.isnan(x)): return alt
        return x
    except Exception: return alt

def _last(s: pd.Series, default=float("nan")) -> float:
    try:
        return float(s.iloc[-1]) if s is not None and len(s)>0 else default
    except Exception:
        return default

def _len_ok(df: pd.DataFrame, n: int) -> bool:
    return isinstance(df, pd.DataFrame) and len(df) >= n

def _ensure_cols(df: pd.DataFrame, cols: Iterable[str]) -> bool:
    try: return all(c in df.columns for c in cols)
    except Exception: return False

def _safe_float(v, d: float) -> float:
    try: return float(v)
    except Exception: return d

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.astype(float).ewm(span=int(length), adjust=False).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.astype(float).rolling(int(length), min_periods=int(length)).mean()

MA = {"EMA": ema, "SMA": sma}

def atr(df: pd.DataFrame, length: int=14) -> pd.Series:
    h=df["high"].astype(float); l=df["low"].astype(float); c=df["close"].astype(float)
    pc=c.shift(1)
    tr=pd.concat([(h-l),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return tr.ewm(span=int(length), adjust=False).mean()

# Market/Broker adapters
def _fetch_df(market: Any, symbol: str, cfg: Dict[str, Any]) -> Optional[pd.DataFrame]:
    tf = cfg.get("CRYPTO_TF","5Min"); limit=int(cfg.get("CRYPTO_LOOKBACK",500))
    for name in ("get_history","history","get_ohlcv","fetch_ohlcv","bars","get_bars"):
        if hasattr(market,name):
            fn=getattr(market,name)
            for args in ((symbol,tf,limit),(symbol,tf),(symbol,)):
                try:
                    df=fn(*args)  # type: ignore
                    if isinstance(df,pd.DataFrame) and _ensure_cols(df,["open","high","low","close"]):
                        if "volume" not in df.columns: df["volume"]=0.0
                        return df
                except TypeError:
                    continue
                except Exception:
                    break
    return None

def _place_order(broker: Any, symbol: str, side: str, notional: float) -> Any:
    for name in ("place_order","submit_order","submit","order","create_order","buy" if side=="buy" else "sell"):
        if hasattr(broker,name):
            fn=getattr(broker,name)
            try:
                return fn(symbol=symbol, side=side, notional=notional)
            except TypeError:
                try:
                    return fn(symbol, side, notional)
                except Exception:
                    pass
            except Exception:
                pass
    return {"status":"no_broker_method"}

def _pwrite_from(kwargs: Dict[str, Any]):
    pw=kwargs.get("pwrite")
    return pw if callable(pw) else (lambda _m: None)

# Single-symbol core
def _run_single(symbol: str, df: pd.DataFrame, cfg: Dict[str,Any],
                broker: Optional[Any], dry: bool) -> Dict[str,Any]:
    """
    Fast/slow MA cross with ATR stops/targets (long-only).
    Knobs:
      C3_MA1_TYPE(EMA), C3_MA2_TYPE(EMA), C3_MA1_LEN(13), C3_MA2_LEN(34),
      C3_ATR_LEN(14), C3_RISK_M(0.8), C3_RR_MULT(1.0),
      C3_USE_LIMIT(1), C3_TRAIL_ON(1), C3_TRAIL_ATR_MULT(1.2), C3_RR_EXIT_FRAC(0.5),
      ORDER_NOTIONAL(25), C3_MIN_BARS
    """
    out={"symbol":symbol,"action":"flat"}
    if not _ensure_cols(df,["close","high","low"]):
        out.update({"reason":"missing_cols"}); return out

    ma1_type=str(cfg.get("C3_MA1_TYPE","EMA")).upper()
    ma2_type=str(cfg.get("C3_MA2_TYPE","EMA")).upper()
    ma1_len=int(cfg.get("C3_MA1_LEN",13))
    ma2_len=int(cfg.get("C3_MA2_LEN",34))
    atr_len=int(cfg.get("C3_ATR_LEN",14))
    risk_m=_safe_float(cfg.get("C3_RISK_M",0.8),0.8)
    rr_mult=_safe_float(cfg.get("C3_RR_MULT",1.0),1.0)
    use_limit=str(cfg.get("C3_USE_LIMIT","1"))=="1"
    trail_on=str(cfg.get("C3_TRAIL_ON","1"))=="1"
    trail_mult=_safe_float(cfg.get("C3_TRAIL_ATR_MULT",1.2),1.2)
    rr_exit_frac=_safe_float(cfg.get("C3_RR_EXIT_FRAC",0.5),0.5)
    notional=_safe_float(cfg.get("ORDER_NOTIONAL",25),25.0)
    min_bars=int(cfg.get("C3_MIN_BARS", max(150, ma2_len+atr_len+5)))

    if not _len_ok(df,min_bars):
        out.update({"reason":"not_enough_bars","have":len(df),"need":min_bars}); return out

    close_s=df["close"].astype(float)
    ma1_fn=MA.get(ma1_type, ema); ma2_fn=MA.get(ma2_type, ema)
    ma1_s=ma1_fn(close_s, ma1_len); ma2_s=ma2_fn(close_s, ma2_len)
    atr_s=atr(df, atr_len)

    ma1_prev=_nz(_last(ma1_s.iloc[-2:]), float("nan"))
    ma2_prev=_nz(_last(ma2_s.iloc[-2:]), float("nan"))
    ma1_now=_nz(_last(ma1_s), float("nan"))
    ma2_now=_nz(_last(ma2_s), float("nan"))
    close=_nz(_last(close_s), float("nan"))
    atr_v=_nz(_last(atr_s), float("nan"))

    if any(math.isnan(x) for x in [ma1_prev,ma2_prev,ma1_now,ma2_now,close,atr_v]):
        out["reason"]="nan_indicators"; return out

    cross_up=(ma1_prev<=ma2_prev) and (ma1_now>ma2_now)

    if cross_up and notional>0:
        stop=close - max(1e-12, risk_m*atr_v)
        risk=max(1e-12, close-stop)
        target=close + rr_mult*risk
        if dry or broker is None:
            out.update({"action":"paper_buy","close":round(close,4),"ma1":round(ma1_now,4),"ma2":round(ma2_now,4),
                        "atr":round(atr_v,6),"stop":round(stop,6),"target":round(target,6),"reason":"dry_run_ma_cross_up"})
        else:
            oid=_place_order(broker, symbol, "buy", notional)
            out.update({"action":"buy","order_id":oid,"close":round(close,4),"ma1":round(ma1_now,4),"ma2":round(ma2_now,4),
                        "atr":round(atr_v,6),"stop":round(stop,6),"target":round(target,6),"risk_model":{
                            "trail_on":trail_on,"trail_atr_mult":trail_mult,"rr_exit_frac":rr_exit_frac
                        },"reason":"ma_cross_up"})
    else:
        out.update({"action":"flat","close":round(close,4),"ma1":round(ma1_now,4),"ma2":round(ma2_now,4),"atr":round(atr_v,6),"reason":"no_signal"})
    return out

# Public API
def run(*args, **kwargs):
    dry=bool(kwargs.get("dry",False))
    cfg=kwargs.get("params") or kwargs.get("cfg") or {}

    if len(args)>=4 and not isinstance(args[0],str):
        market, broker, symbols, params=args[0], args[1], list(args[2]), dict(args[3] or {})
        cfg={**params, **cfg}; pwrite=_pwrite_from(kwargs)
        results=[]
        for sym in symbols:
            df=_fetch_df(market, sym, cfg)
            if df is None:
                results.append({"symbol":sym,"action":"error","error":"no_data"}); continue
            try:
                results.append(_run_single(sym, df, cfg, broker=None if dry else broker, dry=dry))
            except Exception as e:
                pwrite(f"[c3] error {sym}: {e}")
                results.append({"symbol":sym,"action":"error","error":f"{type(e).__name__}: {e}"})
        return results
    else:
        symbol=args[0] if len(args)>0 else kwargs.get("symbol")
        df=args[1] if len(args)>1 else kwargs.get("df")
        cfg_in=args[2] if len(args)>2 else kwargs.get("cfg") or {}
        cfg={**cfg_in, **cfg}; broker=kwargs.get("broker")
        if symbol is None or not isinstance(df, pd.DataFrame):
            return {"action":"error","error":"invalid_arguments"}
        return _run_single(symbol, df, cfg, broker=None if dry else broker, dry=dry)
