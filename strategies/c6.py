# strategies/c6.py
# Version: 1.7.3
# Fast/slow EMA cross with ATR trend filter and HH confirmation.

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()


def run(market, broker, symbols: List[str], params: Dict[str, Any], *, dry: bool, log):
    tf = str(params.get("timeframe", "5Min"))
    limit = int(params.get("limit", 600))
    fast = int(params.get("fast", 12))
    slow = int(params.get("slow", 26))
    lookback = int(params.get("lookback", 20))
    hh_confirm = int(params.get("hh_confirm", 5))   # bars to confirm HH
    atr_min = float(params.get("atr_min", 0.0))
    notional = float(params.get("notional", 0.0))

    results: List[Dict[str, Any]] = []
    try:
        data = market.candles(symbols, timeframe=tf, limit=limit)
    except Exception as e:
        return {"ok": False, "strategy": "c6", "dry": dry, "error": f"candles fetch failed: {e}", "results": []}

    for sym in symbols:
        df = data.get(sym)
        if df is None or len(df) < max(slow + lookback + hh_confirm + 5, 60):
            results.append({"symbol": sym, "action": "error", "error": "no_data"})
            continue

        close = df["close"].astype(float)
        high = df["high"].astype(float)

        ema_f = _ema(close, fast)
        ema_s = _ema(close, slow)
        atr = _atr(df, 14)

        c = float(close.iloc[-1])
        ef = float(ema_f.iloc[-1])
        es = float(ema_s.iloc[-1])
        atr_last = float(atr.iloc[-1])

        crossed_up = ema_f.iloc[-2] <= ema_s.iloc[-2] and ef > es
        hh = float(high.rolling(lookback).max().iloc[-(1+hh_confirm)])

        action = "flat"
        reason = "no_signal"
        order_id: Any = None

        cond_atr = (atr_min == 0.0) or (atr_last >= atr_min)
        cond_hh = c >= hh

        if crossed_up and cond_atr and cond_hh:
            action = "buy"
            reason = "ema_cross_breakout"
            if not dry and notional > 0 and broker is not None:
                try:
                    order_id = broker.paper_buy(sym, notional=notional)
                except Exception as oe:
                    order_id = {"status": "order_error", "error": str(oe)}
            else:
                order_id = {"status": "paper_buy", "notional": notional}

        results.append({
            "symbol": sym,
            "action": action,
            "reason": reason,
            "close": round(c, 6),
            "ema_fast": round(ef, 6),
            "ema_slow": round(es, 6),
            "atr": atr_last,
            "hh": hh,
            "order_id": order_id,
        })

    return {"ok": True, "strategy": "c6", "dry": dry, "results": results}
