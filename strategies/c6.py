# strategies/c6.py
# Version: 1.0.0
from __future__ import annotations
from typing import Any, Dict, List
import numpy as np
import pandas as pd

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False, min_periods=n).mean()

def _tr(h, l, pc):
    return np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc)))

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    tr = _tr(df['high'], df['low'], df['close'].shift(1))
    return pd.Series(tr).rolling(n, min_periods=n).mean()

def _rsi(s: pd.Series, n: int) -> pd.Series:
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    ma_dn = down.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    rs = ma_up / ma_dn.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _place_order(broker, symbol: str, side: str, qty=None, notional=None):
    for name, payload in [
        ("market_order", {"symbol": symbol, "side": side, "qty": qty, "notional": notional}),
        ("market_buy",   {"symbol": symbol, "qty": qty, "notional": notional}),
        ("market_sell",  {"symbol": symbol, "qty": qty, "notional": notional}),
        ("create_order", {"symbol": symbol, "side": side, "type": "market", "time_in_force": "gtc", "qty": qty, "notional": notional}),
    ]:
        if hasattr(broker, name):
            try:
                return getattr(broker, name)(**{k:v for k,v in payload.items() if v is not None})
            except Exception as e:
                return {"status":"error","error":str(e)}
    return {"status":"no_broker_method"}

def _analyze_symbol(df: pd.DataFrame, symbol: str, p: Dict[str, Any]) -> Dict[str, Any]:
    if df is None or len(df) == 0:
        return {"symbol": symbol, "action": "error", "error": "no_data"}
    for c in ("ts","open","high","low","close","volume"):
        if c not in df.columns:
            return {"symbol": symbol, "action": "error", "error": f"missing_col:{c}"}

    kc_len = int(p.get("kc_len", 20))
    kc_mult = float(p.get("kc_mult", 2.0))
    atr_len = int(p.get("atr_len", 14))
    rsi_len = int(p.get("rsi_len", 14))
    rsi_buy = float(p.get("rsi_buy", 35))
    rsi_sell = float(p.get("rsi_sell", 65))

    close = df["close"]
    basis = _ema(close, kc_len)
    atr = _atr(df, atr_len)
    upper = basis + kc_mult * atr
    lower = basis - kc_mult * atr
    rsi = _rsi(close, rsi_len)

    if pd.isna(basis.iloc[-1]) or pd.isna(upper.iloc[-1]) or pd.isna(lower.iloc[-1]) or pd.isna(rsi.iloc[-1]):
        return {"symbol": symbol, "action": "error", "error": "warmup"}

    c = float(close.iloc[-1])
    b = float(basis.iloc[-1])
    u = float(upper.iloc[-1])
    l = float(lower.iloc[-1])
    r = float(rsi.iloc[-1])
    a = float(atr.iloc[-1])

    # Mean-reversion gates:
    if c <= l and r <= rsi_buy:
        return {"symbol": symbol, "action": "buy", "reason": "keltner_rsi_long", "close": c,
                "basis": b, "upper": u, "lower": l, "rsi": r, "atr": a}
    if c >= u and r >= rsi_sell:
        return {"symbol": symbol, "action": "sell", "reason": "keltner_rsi_short", "close": c,
                "basis": b, "upper": u, "lower": l, "rsi": r, "atr": a}

    return {"symbol": symbol, "action": "flat", "reason": "no_signal", "close": c,
            "basis": b, "upper": u, "lower": l, "rsi": r, "atr": a}

def run(market, broker, symbols, params, dry, log, pwrite):
    tf   = params.get("timeframe", "5Min")
    lim  = int(params.get("limit", 600))
    data = market.get_bars(symbols=symbols, timeframe=tf, limit=lim)

    notional = params.get("notional")
    qty      = params.get("qty")

    results = []
    for sym in symbols:
        res = _analyze_symbol(data.get(sym), sym, params)
        if not dry and res.get("action") in ("buy","sell"):
            side = "buy" if res["action"] == "buy" else "sell"
            res["order_id"] = _place_order(broker, sym, side, qty=qty, notional=notional)
        results.append(res)

    return {"ok": True, "strategy": "c6", "results": results, "dry": bool(dry), "force": False}
