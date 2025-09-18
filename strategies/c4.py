# strategies/c4.py
from __future__ import annotations
import math
from typing import Any, Dict, Iterable, List, Callable, Optional
import pandas as pd
import numpy as np

__version__ = "1.1.2"
VERSION = (1, 1, 2)

# Helpers
def _nz(x, alt=0.0):
    try:
        if x is None or (isinstance(x,float) and math.isnan(x)): return alt
        return x
    except Exception:
        return alt

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

def atr(df: pd.DataFrame, length: int=14) -> pd.Series:
    h=df["high"].astype(float); l=df["low"].astype(float); c=df["close"].astype(float)
    pc=c.shift(1)
    tr=pd.concat([(h-l),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return tr.ewm(span=int(length), adjust=False).mean()

def _est_buy_sell(df: pd.DataFrame) -> pd.DataFrame:
    o=df["open"].astype(float); h=df["high"].astype(float)
    l=df["low"].astype(float);  c=df["close"].astype(float)
    v=df.get("volume", pd.Series(index=df.index, data=0.0)).fillna(0.0).astype(float)

    bar_top = h - np.maximum(o, c)
    bar_bot = np.minimum(o, c) - l
    bar_rng = (h - l).clip(lower=1e-12)
    bull = (c - o) > 0

    buy_rng  = np.where(bull, bar_rng, bar_top + bar_bot)
    sell_rng = np.where(bull, bar_top + bar_bot, bar_rng)
    tot_rng  = (bar_rng + bar_top + bar_bot).clip(lower=1e-12)

    buy_vol  = np.round((buy_rng / tot_rng) * v, 6)
    sell_vol = np.round((sell_rng / tot_rng) * v, 6)

    out = pd.DataFrame(index=df.index)
    out["buy_vol"]  = buy_vol
    out["sell_vol"] = sell_vol
    out["delta_vol"]= buy_vol - sell_vol
    out["tot_vol"]  = buy_vol + sell_vol
    return out

# Market/Broker adapters
def _fetch_df(market: Any, symbol: str, cfg: Dict[str, Any]) -> Optional[pd.DataFrame]:
    tf=cfg.get("CRYPTO_TF","5Min"); limit=int(cfg.get("CRYPTO_LOOKBACK",600))
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

# Single-symbol logic
def _run_single(symbol: str, df: pd.DataFrame, cfg: Dict[str,Any],
                broker: Optional[Any], dry: bool) -> Dict[str,Any]:
    """
    Volume-delta window + small ATR breakout with EMA trend filter (long-only).
    Knobs: C4_DELTA_WIN(20), C4_MIN_DELTA_FRAC(0.25),
           C4_BREAK_K_ATR(0.25), C4_ATR_LEN(14),
           C4_TREND_EMA_LEN(50), ORDER_NOTIONAL(25), C4_MIN_BARS
    """
    out={"symbol":symbol,"action":"flat"}
    if not _ensure_cols(df,["open","high","low","close"]):
        out.update({"reason":"missing_cols"}); return out

    delta_win=int(cfg.get("C4_DELTA_WIN",20))
    min_delta_frac=_safe_float(cfg.get("C4_MIN_DELTA_FRAC",0.25),0.25)
    k_atr=_safe_float(cfg.get("C4_BREAK_K_ATR",0.25),0.25)
    atr_len=int(cfg.get("C4_ATR_LEN",14))
    trend_len=int(cfg.get("C4_TREND_EMA_LEN",50))
    notional=_safe_float(cfg.get("ORDER_NOTIONAL",25),25.0)
    min_bars=int(cfg.get("C4_MIN_BARS", max(150, delta_win+trend_len+atr_len+5)))

    if not _len_ok(df, min_bars):
        out.update({"reason":"not_enough_bars","have":len(df),"need":min_bars}); return out

    close_s=df["close"].astype(float)
    high_s=df["high"].astype(float)
    parts=_est_buy_sell(df)
    atr_s=atr(df, atr_len)
    ema_trend=ema(close_s, trend_len)

    delta_roll=parts["delta_vol"].rolling(delta_win, min_periods=delta_win).sum()
    total_roll=parts["tot_vol"].rolling(delta_win, min_periods=delta_win).sum()

    close=_nz(_last(close_s))
    hh=_nz(_last(high_s.rolling(delta_win, min_periods=delta_win).max()))
    av=_nz(_last(atr_s))
    emv=_nz(_last(ema_trend))
    delta_sum=_nz(_last(delta_roll))
    total_sum=_nz(_last(total_roll), 0.0)

    if any(math.isnan(x) for x in [close, hh, av, emv, delta_sum]) or total_sum <= 0:
        out["reason"]="nan_or_zero_denominator"; return out

    delta_frac=abs(delta_sum)/max(1e-12,total_sum)
    skew_ok=delta_frac >= min_delta_frac
    breakout_up=close > (hh + k_atr*av)
    trend_ok=close > emv
    enter_long=bool(skew_ok and breakout_up and trend_ok)

    if enter_long and notional>0:
        if dry or broker is None:
            out.update({"action":"paper_buy","close":round(close,4),"hh":round(hh,4),"atr":round(av,6),
                        "ema":round(emv,4),"delta_frac":round(delta_frac,4),"reason":"dry_run_delta_skew_breakout_trend_ok"})
        else:
            oid=_place_order(broker, symbol, "buy", notional)
            out.update({"action":"buy","order_id":oid,"close":round(close,4),"hh":round(hh,4),"atr":round(av,6),
                        "ema":round(emv,4),"delta_frac":round(delta_frac,4),"reason":"delta_skew_breakout_trend_ok"})
    else:
        out.update({"action":"flat","close":round(close,4),"hh":round(hh,4),"atr":round(av,6),
                    "ema":round(emv,4),"delta_frac":round(delta_frac,4),"reason":"no_signal"})
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
                pwrite(f"[c4] error {sym}: {e}")
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
