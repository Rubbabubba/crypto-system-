# strategies/c5.py
# ---------------------------------------------------------------------
# Strategy C5 — Multi-timeframe RSI + EMA Pullback
# Version: 1.1.0
#
# Entry (long):
#   - Trend filter: close > EMA(len)
#   - Pullback window: rsi_sell < RSI(len) < rsi_buy (e.g., 40 < RSI < 60)
# Exit / Flat:
#   - Otherwise no trade signal here (this is a scanner). Your risk module
#     can manage exits, or you can add a mirrored exit in C6 or broker rules.
#
# Params (querystring, optional):
#   timeframe:   default "5Min"
#   limit:       default 600
#   ema_len:     default 200
#   rsi_len:     default 14
#   rsi_buy:     default 60
#   rsi_sell:    default 40
#
# Optional order sizing:
#   notional: float (e.g., 25)
#   qty:      float (alternative to notional; not both)
# ---------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

VERSION = "1.1.0"
NAME = "c5"

# -------------------------- indicators -------------------------------- #

def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def _rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).rolling(length).mean()
    roll_down = pd.Series(down, index=series.index).rolling(length).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(method="bfill").fillna(50.0)

# -------------------------- core -------------------------------------- #

def _scan_symbol(df: pd.DataFrame, params: Dict[str, Any]) -> Dict[str, Any]:
    ema_len   = int(params.get("ema_len", 200))
    rsi_len   = int(params.get("rsi_len", 14))
    rsi_buy   = float(params.get("rsi_buy", 60))
    rsi_sell  = float(params.get("rsi_sell", 40))

    close = df["close"]
    ema   = _ema(close, ema_len)
    rsi   = _rsi(close, rsi_len)

    c = float(close.iloc[-1])
    e = float(ema.iloc[-1])
    r = float(rsi.iloc[-1])

    reason = "no_signal"
    action = "flat"

    if c > e and (rsi_sell < r < rsi_buy):
        action = "buy"
        reason = "enter_long_rsi_pullback"

    return {
        "symbol": params["symbol"],
        "action": action,
        "reason": reason,
        "close": round(c, 6),
        "ema": round(e, 6),
        "rsi": round(r, 2),
    }

def _place_order(broker, symbol: str, dry: bool, notional: Optional[float], qty: Optional[float]) -> Any:
    if dry:
        return {"status": "dry_run"}
    try:
        if notional is not None and hasattr(broker, "market_buy_notional"):
            return broker.market_buy_notional(symbol, float(notional))
        if qty is not None and hasattr(broker, "market_buy_qty"):
            return broker.market_buy_qty(symbol, float(qty))
        # Fallback common name used in your logs:
        if notional is not None and hasattr(broker, "market_buy"):
            return broker.market_buy(symbol, notional=float(notional))
        if hasattr(broker, "market_buy"):
            # If neither notional nor qty provided, attempt a minimal buy
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
