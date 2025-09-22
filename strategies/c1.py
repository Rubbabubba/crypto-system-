# strategies/c1.py
# Version: 1.8.2
# RSI pullback into EMA trend. Buy when RSI recovers in uptrend; sell when RSI fades / below EMA.
from __future__ import annotations
from typing import Any, Dict, List, Tuple
import math
import pandas as pd

def _p(d: Dict[str, Any], k: str, dv: Any): 
    v = d.get(k, dv)
    try:
        if isinstance(dv, int): return int(v)
        if isinstance(dv, float): return float(v)
        if isinstance(dv, bool): return str(v).lower() not in ("0","false","")
        return v
    except Exception: return dv

def _resolve_ohlc(df) -> Tuple[pd.Series, pd.Series, pd.Series]:
    cols = getattr(df, "columns", [])
    def first(*names):
        for n in names:
            if n in cols: return df[n]
        raise KeyError(f"missing columns {names}")
    h = first("h","high","High")
    l = first("l","low","Low")
    c = first("c","close","Close")
    return h, l, c

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def _rsi(close: pd.Series, length=14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    gain = up.ewm(alpha=1/length, adjust=False).mean()
    loss = down.ewm(alpha=1/length, adjust=False).mean()
    rs = gain / (loss.replace(0, 1e-12))
    return 100 - (100 / (1 + rs))

def _qty_from_positions(positions: List[Dict[str, Any]], symbol: str) -> float:
    for p in positions or []:
        sym = p.get("symbol") or p.get("asset_symbol") or ""
        if sym == symbol:
            try: return float(p.get("qty") or p.get("quantity") or 0)
            except Exception: return 0.0
    return 0.0

def run(market, broker, symbols, params, *, dry, log):
    tf       = _p(params, "timeframe", "5Min")
    limit    = _p(params, "limit", 600)
    notional = _p(params, "notional", 0.0)

    ema_len  = _p(params, "ema_len", 50)
    rsi_len  = _p(params, "rsi_len", 14)
    rsi_buy  = _p(params, "rsi_buy", 55.0)
    rsi_sell = _p(params, "rsi_sell", 40.0)

    out = {"ok": True, "strategy": "c1", "dry": dry, "results": []}

    positions = []
    if not dry:
        try: positions = broker.positions()
        except Exception as e: log(event="positions_error", error=str(e))

    try:
        data = market.candles(symbols, timeframe=tf, limit=limit)
    except Exception as e:
        return {"ok": False, "strategy": "c1", "error": f"candles_error:{e}"}

    for s in symbols:
        df = (data or {}).get(s)
        if df is None or len(df) < max(ema_len, rsi_len) + 2:
            out["results"].append({"symbol": s, "action": "flat", "reason": "insufficient_bars"}); continue
        try:
            _, _, c = _resolve_ohlc(df)
        except KeyError:
            out["results"].append({"symbol": s, "action": "flat", "reason": "bad_columns"}); continue

        ema = _ema(c, ema_len)
        rsi = _rsi(c, rsi_len)
        close = float(c.iloc[-1]); ema_now = float(ema.iloc[-1]); rsi_now = float(rsi.iloc[-1])
        uptrend = close > ema_now
        pos_qty = _qty_from_positions(positions, s) if not dry else 0.0

        action, reason, order_id = "flat", "no_signal", None

        if uptrend and rsi_now >= rsi_buy:
            action, reason = "buy", "rsi_recovery_uptrend"
            if not dry and notional > 0:
                try:
                    res = broker.notional(s, "buy", usd=notional, params=params)
                    order_id = (res or {}).get("id")
                except Exception as e:
                    action, reason = "flat", f"buy_error:{e}"

        elif pos_qty > 0 and (rsi_now <= rsi_sell or close < ema_now):
            action, reason = "sell", "rsi_fade_or_below_ema"
            if not dry:
                try:
                    res = broker.paper_sell(s, qty=pos_qty, params=params)
                    order_id = (res or {}).get("id")
                except Exception as e:
                    action, reason = "flat", f"sell_error:{e}"

        out["results"].append({
            "symbol": s, "action": action, "reason": reason,
            "close": close, "ema": ema_now, "rsi": rsi_now, "order_id": order_id
        })

    return out
