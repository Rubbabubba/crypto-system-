# strategies/c6.py
# ---------------------------------------------------------------------
# Strategy C6 — MACD Trend + EMA Filter
# Version: 1.1.0
#
# Entry (long):
#   - MACD > Signal
#   - close > EMA(filter_len)
# Exit signal (reported as "sell", up to broker/risk module to act):
#   - MACD < Signal  OR  close < EMA(filter_len)
#
# Params (querystring, optional):
#   timeframe:   default "5Min"
#   limit:       default 600
#   fast:        default 12
#   slow:        default 26
#   signal:      default 9
#   filter_len:  default 200 (EMA trend filter)
#
# Optional order sizing:
#   notional: float
#   qty:      float
# ---------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

VERSION = "1.1.0"
NAME = "c6"

# -------------------------- indicators -------------------------------- #

def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = _ema(series, fast)
    ema_slow = _ema(series, slow)
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist

# -------------------------- core -------------------------------------- #

def _scan_symbol(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    fast       = int(params.get("fast", 12))
    slow       = int(params.get("slow", 26))
    sig_len    = int(params.get("signal", 9))
    filter_len = int(params.get("filter_len", 200))

    close = df["close"]
    macd, sig, hist = _macd(close, fast=fast, slow=slow, signal=sig_len)
    ema_filter = _ema(close, filter_len)

    c  = float(close.iloc[-1])
    m  = float(macd.iloc[-1])
    s  = float(sig.iloc[-1])
    h  = float(hist.iloc[-1])
    ef = float(ema_filter.iloc[-1])

    action = "flat"
    reason = "no_signal"

    # Entry
    if (m > s) and (c > ef):
        action = "buy"
        reason = "enter_long_macd_trend"

    # Exit (signal only)
    if (m < s) or (c < ef):
        if action != "buy":
            action = "flat"  # we report flat in scanner unless explicitly exiting a live position
        # reason remains no_signal when not acting; you can wire broker-side exits as needed

    return {
        "symbol": params["symbol"],
        "action": action,
        "reason": reason,
        "close": round(c, 6),
        "macd": round(m, 6),
        "signal": round(s, 6),
        "hist": round(h, 6),
        "ema_filter": round(ef, 6),
    }

def _place_order(broker, symbol: str, dry: bool, notional: Optional[float], qty: Optional[float]) -> Any:
    if dry:
        return {"status": "dry_run"}
    try:
        if notional is not None and hasattr(broker, "market_buy_notional"):
            return broker.market_buy_notional(symbol, float(notional))
        if qty is not None and hasattr(broker, "market_buy_qty"):
            return broker.market_buy_qty(symbol, float(qty))
        if notional is not None and hasattr(broker, "market_buy"):
            return broker.market_buy(symbol, notional=float(notional))
        if hasattr(broker, "market_buy"):
            return broker.market_buy(symbol, notional=10.0)
        return {"status": "no_broker_method"}
    except Exception as ex:
        return {"status": "error", "error": str(ex)}

# -------------------------- public API -------------------------------- #

def run(market, broker, symbols, params, *args, dry: bool = False, pwrite=print, log=None, notional: float | None = None, qty: float | None = None, **kwargs):
    """
    Compatible signature with the app router.
    - Accepts extra args/kwargs without breaking.
    """
    timeframe = params.get("timeframe", "5Min")
    limit     = int(params.get("limit", 600))

    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]

    candles: Dict[str, pd.DataFrame] = market.candles(symbols, timeframe=timeframe, limit=limit)

    results: List[Dict[str, Any]] = []
    for sym in symbols:
        df = candles.get(sym)
        if df is None or df.empty or len(df) < 50:
            results.append({"symbol": sym, "action": "error", "error": "no_data"})
            continue

        row = _scan_symbol(df, {**params, "symbol": sym})

        if row["action"] == "buy":
            order_id = _place_order(broker, sym, dry=dry, notional=notional, qty=qty)
            row["order_id"] = order_id

        results.append(row)

    return {
        "ok": True,
        "strategy": NAME,
        "dry": bool(dry),
        "results": results,
        "version": VERSION,
    }
