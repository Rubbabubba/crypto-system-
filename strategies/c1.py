# strategies/c1.py
# Version: 1.7.3
# RSI pullback into EMA trend. Expects app to call run(market, broker, symbols, params, *, dry, log)

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(com=length - 1, adjust=False).mean()
    ma_down = down.ewm(com=length - 1, adjust=False).mean()
    rs = ma_up / (ma_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def run(market, broker, symbols: List[str], params: Dict[str, Any], *, dry: bool, log):
    """
    Required by app.py:
      - returns dict with ok/strategy/dry/results
      - never raises for normal conditions

    params:
      timeframe (str)  default "5Min"
      limit (int)      default 600
      rsi_len (int)    default 14
      ema_len (int)    default 50
      rsi_buy (float)  default 60
      rsi_sell (float) default 40
      notional (float) default 0 (live only)
    """
    tf = str(params.get("timeframe", "5Min"))
    limit = int(params.get("limit", 600))
    rsi_len = int(params.get("rsi_len", 14))
    ema_len = int(params.get("ema_len", 50))
    rsi_buy = float(params.get("rsi_buy", 60))
    rsi_sell = float(params.get("rsi_sell", 40))
    notional = float(params.get("notional", 0.0))

    results: List[Dict[str, Any]] = []

    try:
        data = market.candles(symbols, timeframe=tf, limit=limit)  # Dict[str, DataFrame]
    except Exception as e:
        return {"ok": False, "error": f"candles fetch failed: {e}", "results": []}

    for sym in symbols:
        df = data.get(sym)
        if df is None or len(df) < max(ema_len, rsi_len) + 5:
            results.append({"symbol": sym, "action": "error", "error": "no_data"})
            continue

        # Expect columns: time/ts, open, high, low, close, volume (normalized by MarketCrypto)
        close = df["close"].astype(float)

        ema = _ema(close, ema_len)
        rsi = _rsi(close, rsi_len)

        c = float(close.iloc[-1])
        e = float(ema.iloc[-1])
        r = float(rsi.iloc[-1])

        action = "flat"
        reason = "no_signal"
        order_id: Any = None

        # Simple logic:
        # - Uptrend (price above EMA) & RSI crosses up through rsi_buy -> buy
        # - Downtrend (price below EMA) & RSI crosses down through rsi_sell -> (optional) sell/short (disabled here)
        if c > e and r >= rsi_buy:
            action = "buy"
            reason = "enter_long_rsi_pullback"
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
            "ema": round(e, 6),
            "rsi": round(r, 2),
            "order_id": order_id,
        })

    return {"ok": True, "strategy": "c1", "dry": dry, "results": results}
