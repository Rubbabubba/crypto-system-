# strategies/c3.py — v1.9.0
# MA crossover (TV-style) with flexible MA types + stateless exits for crypto.
#
# Interface (unchanged):
#   run(df_map, params, positions) -> List[ {symbol, action, reason} ]
#     df_map:     {"BTC/USD": intraday_df, ...}  (cols: open, high, low, close, volume; ascending index)
#     params:     optional overrides listed below
#     positions:  {"BTC/USD": qty_float, ...}
#
# Defaults (override via params if desired):
#   ma1_type="EMA", ma1_len=13
#   ma2_type="EMA", ma2_len=34
#   atr_len=14, atr_mult=0.8                  # tighter than 1.0; adjust to taste
#   trend_leg="ma2"                            # which MA to use as the trend line for the ATR stop ("ma2"|"ma1")
#   use_vol=False, vol_sma_len=20             # set use_vol=True to require vol > SMA20(vol) on entry
#
# Logic:
#   ENTRY  (flat)      : cross_up(ma1, ma2) AND ( !use_vol OR vol > SMA(vol) )
#   EXIT   (in-pos)    : cross_down(ma1, ma2) OR close < trend_ma - atr_mult * ATR
#   Otherwise          : flat / hold
#
# Notes:
#   • Stateless = no saved per-symbol state; works cleanly with your current engine loop.
#   • VWAP here is continuous (cumulative) — good for 24/7 crypto.
#   • HEMA approximated via EMA of Heikin-Ashi open (practical Pine-like proxy).
#   • Keep order sizing in your engine (notional/qty).

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME = "c3"
VERSION = "1.9.0"

# ---------- MA helpers ----------
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def _wma(s: pd.Series, n: int) -> pd.Series:
    if n <= 1: return s.copy()
    weights = pd.Series(range(1, n+1), index=s.index[:n])
    out = s.copy()
    out.iloc[:n-1] = pd.NA
    out.iloc[n-1:] = (s.rolling(n)
                        .apply(lambda x: (x * weights.values).sum() / weights.values.sum(), raw=False)
                     ).iloc[n-1:]
    return out

def _hma(s: pd.Series, n: int) -> pd.Series:
    if n <= 1: return s.copy()
    return _wma(2*_wma(s, n//2) - _wma(s, n), int(n**0.5) or 1)

def _dema(s: pd.Series, n: int) -> pd.Series:
    e1 = _ema(s, n); e2 = _ema(e1, n)
    return 2*e1 - e2

def _t3(s: pd.Series, n: int, b: float = 0.7) -> pd.Series:
    e1=_ema(s,n); e2=_ema(e1,n); e3=_ema(e2,n)
    e4=_ema(e3,n); e5=_ema(e4,n); e6=_ema(e5,n)
    c1 = -(b**3)
    c2 =  3*b*b + 3*b**3
    c3 = -6*b*b - 3*b - 3*b**3
    c4 =  1 + 3*b + 3*b*b + b**3
    return c1*e6 + c2*e5 + c3*e4 + c4*e3

def _vwma(close: pd.Series, vol: pd.Series, n: int) -> pd.Series:
    pv = close * vol
    return (pv.rolling(n).sum() / vol.rolling(n).sum()).fillna(method="bfill")

def _vwap_continuous(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = tp * df["volume"].replace(0, pd.NA)
    cum_pv = pv.cumsum()
    cum_v  = df["volume"].replace(0, pd.NA).cumsum()
    out = cum_pv / cum_v
    return out.fillna(method="ffill").fillna(df["close"])

def _hema(df: pd.DataFrame, n: int) -> pd.Series:
    # Practical proxy: EMA of Heikin-Ashi open
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    ha_close = (o + h + l + c) / 4.0
    ha_open = pd.Series(index=df.index, dtype="float64")
    ha_open.iloc[0] = (o.iloc[0] + c.iloc[0]) / 2.0
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2.0
    return _ema(ha_open, n)

def _ma(df: pd.DataFrame, kind: str, length: int) -> pd.Series:
    k = (kind or "EMA").upper()
    c = df["close"]
    if k == "SMA":   return _sma(c, length)
    if k == "EMA":   return _ema(c, length)
    if k == "WMA":   return _wma(c, length)
    if k == "HMA":   return _hma(c, length)
    if k == "DEMA":  return _dema(c, length)
    if k == "T3":    return _t3(c, length)
    if k == "VWMA":  return _vwma(c, df["volume"], length)
    if k == "VWAP":  return _vwap_continuous(df)
    if k == "HEMA":  return _hema(df, length)
    return _ema(c, length)

# ---------- risk/filters ----------
def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return _sma(tr, n)

def _vol_ok(v: pd.Series, n: int) -> bool:
    if len(v) < n + 1: return False
    return v.iloc[-1] > v.rolling(n).mean().iloc[-1]

def _cross_up(x: pd.Series, y: pd.Series) -> bool:
    return len(x) >= 2 and len(y) >= 2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) and (x.iloc[-2] <= y.iloc[-2]) and (x.iloc[-1] > y.iloc[-1])

def _cross_dn(x: pd.Series, y: pd.Series) -> bool:
    return len(x) >= 2 and len(y) >= 2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) and (x.iloc[-2] >= y.iloc[-2]) and (x.iloc[-1] < y.iloc[-1])

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try:
        return float(positions.get(sym, 0.0)) > 0.0
    except Exception:
        return False

def _ok_df(df: pd.DataFrame) -> bool:
    need = {"open","high","low","close","volume"}
    return isinstance(df, pd.DataFrame) and need.issubset(df.columns) and len(df) >= 60

# ---------- main ----------
def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    ma1_type    = str(params.get("ma1_type", "EMA"))
    ma2_type    = str(params.get("ma2_type", "EMA"))
    ma1_len     = int(params.get("ma1_len", 13))
    ma2_len     = int(params.get("ma2_len", 34))

    atr_len     = int(params.get("atr_len", 14))
    atr_mult    = float(params.get("atr_mult", 0.8))
    trend_leg   = str(params.get("trend_leg", "ma2")).lower()  # "ma2" | "ma1"

    use_vol     = bool(params.get("use_vol", False))
    vol_sma_len = int(params.get("vol_sma_len", 20))

    results: List[Dict[str, Any]] = []

    for sym, df in df_map.items():
        if not _ok_df(df):
            results.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()

        ma1 = _ma(df, ma1_type, ma1_len)
        ma2 = _ma(df, ma2_type, ma2_len)

        # guards: we need valid last two points
        if pd.isna(ma1.iloc[-2]) or pd.isna(ma2.iloc[-2]):
            results.append({"symbol": sym, "action": "flat", "reason": "warming_up"})
            continue

        have = _in_pos(positions, sym)
        macross_up  = _cross_up(ma1, ma2)
        macross_dn  = _cross_dn(ma1, ma2)

        # choose trend line for ATR stop
        trend_ma = ma2 if trend_leg == "ma2" else ma1
        atr_now  = _atr(df, atr_len).iloc[-1]
        close    = df["close"].iloc[-1]

        # Volume gate if enabled
        vol_ok = True
        if use_vol:
            vol_ok = _vol_ok(df["volume"], vol_sma_len)

        # --- Exits first if in position ---
        if have:
            # cross-down or ATR-vs-trend stop
            if macross_dn:
                results.append({"symbol": sym, "action": "sell", "reason": "ma_cross_down"})
                continue
            # defensive stop
            stop_line = (trend_ma.iloc[-1] - atr_mult * atr_now) if pd.notna(trend_ma.iloc[-1]) and pd.notna(atr_now) else None
            if (stop_line is not None) and (close < stop_line):
                results.append({"symbol": sym, "action": "sell", "reason": "atr_vs_trend_stop"})
                continue

            results.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # --- Entries when flat ---
        if macross_up and vol_ok:
            results.append({"symbol": sym, "action": "buy", "reason": "ma_cross_up" + ("_vol_ok" if use_vol else "")})
        else:
            reason_bits=[]
            if not macross_up: reason_bits.append("no_cross_up")
            if not vol_ok:     reason_bits.append("vol_fail")
            results.append({"symbol": sym, "action": "flat", "reason": " & ".join(reason_bits) if reason_bits else "no_signal"})

    return results
