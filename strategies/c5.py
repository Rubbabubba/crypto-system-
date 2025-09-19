# strategies/c5.py
# Version: 1.0.0
from __future__ import annotations
import math
from typing import Any, Dict, List
import numpy as np
import pandas as pd
from datetime import time as dtime

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False, min_periods=n).mean()

def _tr(true_high, true_low, prev_close):
    return np.maximum(true_high - true_low, np.maximum(abs(true_high - prev_close), abs(true_low - prev_close)))

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    tr = _tr(df['high'], df['low'], df['close'].shift(1))
    return pd.Series(tr).rolling(n, min_periods=n).mean()

def _in_session(ts: pd.Timestamp, start: str, end: str) -> bool:
    # start/end = "00:00".."23:59" (UTC)
    s_h, s_m = [int(x) for x in start.split(":")]
    e_h, e_m = [int(x) for x in end.split(":")]
    t = ts.time()
    return dtime(s_h, s_m) <= t <= dtime(e_h, e_m)

def _place_order(broker, symbol: str, side: str, qty=None, notional=None) -> Dict[str, Any]:
    # Try a few common broker method shapes
    candidates = [
        ("market_order", {"symbol": symbol, "side": side, "qty": qty, "notional": notional}),
        ("market_buy",   {"symbol": symbol, "qty": qty, "notional": notional}),
        ("market_sell",  {"symbol": symbol, "qty": qty, "notional": notional}),
        ("create_order", {"symbol": symbol, "side": side, "type": "market", "time_in_force": "gtc", "qty": qty, "notional": notional}),
    ]
    for name, payload in candidates:
        if hasattr(broker, name):
            try:
                return getattr(broker, name)(**{k:v for k,v in payload.items() if v is not None})
            except Exception as e:
                return {"status":"error","error":str(e)}
    return {"status":"no_broker_method"}

def _analyze_symbol(df: pd.DataFrame, symbol: str, p: Dict[str, Any]) -> Dict[str, Any]:
    if df is None or len(df) == 0:
        return {"symbol": symbol, "action": "error", "error": "no_data"}

    # Ensure required cols
    for c in ("ts", "open", "high", "low", "close", "volume"):
        if c not in df.columns:
            return {"symbol": symbol, "action": "error", "error": f"missing_col:{c}"}

    ma_fast = int(p.get("ma_fast", 9))
    ma_slow = int(p.get("ma_slow", 21))
    atr_len = int(p.get("atr_len", 14))
    atr_mult = float(p.get("atr_mult", 1.0))
    min_atr_frac = float(p.get("min_atr_frac", 0.0)) # e.g., 0.0005 to avoid dead markets

    start_utc = str(p.get("session_start", "00:00"))
    end_utc   = str(p.get("session_end",   "23:59"))
    last_ts = pd.to_datetime(df["ts"].iloc[-1], utc=True)
    if not _in_session(last_ts, start_utc, end_utc):
        return {"symbol": symbol, "action": "flat", "reason": "out_of_session"}

    close = df["close"]
    df["ema_fast"] = _ema(close, ma_fast)
    df["ema_slow"] = _ema(close, ma_slow)
    df["atr"] = _atr(df, atr_len)

    if df[["ema_fast","ema_slow","atr"]].tail(1).isna().any(axis=None):
        return {"symbol": symbol, "action": "error", "error": "warmup"}

    ema_fast_now = float(df["ema_fast"].iloc[-1])
    ema_slow_now = float(df["ema_slow"].iloc[-1])
    ema_fast_prev = float(df["ema_fast"].iloc[-2]) if len(df) > 1 else ema_fast_now
    ema_slow_prev = float(df["ema_slow"].iloc[-2]) if len(df) > 1 else ema_slow_now
    c = float(close.iloc[-1])
    a = float(df["atr"].iloc[-1])

    # simple liquidity / activity guard
    if min_atr_frac > 0:
        if a / max(1e-9, c) < min_atr_frac:
            return {"symbol": symbol, "action": "flat", "reason": "atr_too_small", "atr": a, "close": c}

    long_cross  = ema_fast_prev <= ema_slow_prev and ema_fast_now > ema_slow_now
    short_cross = ema_fast_prev >= ema_slow_prev and ema_fast_now < ema_slow_now

    # Optional ATR-based stop/target (informational)
    stop_long  = c - atr_mult * a
    target_long = c + atr_mult * a
    stop_short = c + atr_mult * a
    target_short = c - atr_mult * a

    if long_cross:
        return {"symbol": symbol, "action": "buy", "reason": "ema_cross_up",
                "close": c, "ema_fast": ema_fast_now, "ema_slow": ema_slow_now,
                "atr": a, "stop": stop_long, "target": target_long}

    if short_cross:
        return {"symbol": symbol, "action": "sell", "reason": "ema_cross_down",
                "close": c, "ema_fast": ema_fast_now, "ema_slow": ema_slow_now,
                "atr": a, "stop": stop_short, "target": target_short}

    return {"symbol": symbol, "action": "flat", "reason": "no_signal",
            "close": c, "ema_fast": ema_fast_now, "ema_slow": ema_slow_now, "atr": a}

def run(market, broker, symbols: List[str], params: Dict[str, Any], dry: bool, log, pwrite):
    # Fetch candles
    tf   = params.get("timeframe", "5Min")
    lim  = int(params.get("limit", 600))
    data = market.get_bars(symbols=symbols, timeframe=tf, limit=lim)  # expects dict[symbol] -> DataFrame

    notional = params.get("notional")
    qty      = params.get("qty")

    results = []
    for sym in symbols:
        df = data.get(sym)
        res = _analyze_symbol(df, sym, params)
        if not dry and res.get("action") in ("buy","sell"):
            side = "buy" if res["action"] == "buy" else "sell"
            order_id = _place_order(broker, sym, side, qty=qty, notional=notional)
            res["order_id"] = order_id
        results.append(res)

    return {"ok": True, "strategy": "c5", "results": results, "dry": bool(dry), "force": False}
