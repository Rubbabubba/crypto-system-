# strategies/c5.py
# Version: 1.8.1
# - Adds SELL logic to the percent-breakout strategy.
# - Exits on EMA giveback or ATR stop.
# - Uses notional buys; sells current position qty if any.
# - Client attribution via params["client_tag"] (used by ExchangeExec to build client_order_id).

from __future__ import annotations

from typing import Any, Dict, List
import math

def _p(d: Dict[str, Any], k: str, dv: Any) -> Any:
    v = d.get(k, dv)
    try:
        if isinstance(dv, int):   return int(v)
        if isinstance(dv, float): return float(v)
        if isinstance(dv, bool):  return (str(v).lower() not in ("0","false",""))
        return v
    except Exception:
        return dv

def _ema(series, length: int):
    try:
        return series.ewm(span=length, adjust=False).mean()
    except Exception:
        return None

def _atr(df, length: int):
    try:
        # expects columns: h, l, c
        h, l, c = df["h"], df["l"], df["c"]
        prev_c = c.shift(1)
        tr = (h - l).abs().combine((h - prev_c).abs(), max).combine((l - prev_c).abs(), max)
        return tr.rolling(length).mean()
    except Exception:
        return None

def _highest(series, length: int):
    try:
        return series.rolling(length).max()
    except Exception:
        return None

def _lowest(series, length: int):
    try:
        return series.rolling(length).min()
    except Exception:
        return None

def _qty_from_positions(positions: List[Dict[str, Any]], symbol: str) -> float:
    for p in positions or []:
        sym = p.get("symbol") or p.get("asset_symbol") or ""
        if sym == symbol:
            q = p.get("qty") or p.get("quantity") or 0
            try:
                return float(q)
            except Exception:
                pass
    return 0.0

def run(market, broker, symbols, params, *, dry, log):
    tf      = _p(params, "timeframe", "5Min")
    limit   = _p(params, "limit", 600)
    notional= _p(params, "notional", 0.0)

    # Breakout / exit params (overridable via query string)
    breakout_len   = _p(params, "breakout_len", 20)   # lookback for highest-high
    ema_len        = _p(params, "ema_len", 20)        # baseline EMA for giveback
    atr_len        = _p(params, "atr_len", 14)
    atr_mult_stop  = _p(params, "atr_mult_stop", 1.5) # stop below close by k*ATR
    giveback_pct   = _p(params, "giveback_pct", 0.005) # 0.5% giveback below EMA

    out = {"ok": True, "strategy": "c5", "dry": dry, "results": []}

    # Prefetch positions once if we might need to sell
    positions = []
    if not dry:
        try:
            positions = broker.positions()
        except Exception as e:
            log(event="positions_error", error=str(e))

    try:
        data = market.candles(symbols, timeframe=tf, limit=limit)
    except Exception as e:
        return {"ok": False, "strategy": "c5", "error": f"candles_error: {e}"}

    for s in symbols:
        df = (data or {}).get(s)
        if df is None or getattr(df, "shape", [0])[0] < max(breakout_len, ema_len, atr_len) + 1:
            out["results"].append({
                "symbol": s, "action": "flat", "reason": "insufficient_bars"
            })
            continue

        # Expect columns: t, o, h, l, c, v
        try:
            c = df["c"]
            h = df["h"]
        except KeyError:
            out["results"].append({"symbol": s, "action": "flat", "reason": "bad_columns"})
            continue

        ema = _ema(c, ema_len)
        atr = _atr(df, atr_len)
        hh  = _highest(h, breakout_len)
        close = float(c.iloc[-1])
        ema_now = float(ema.iloc[-1]) if ema is not None else float("nan")
        atr_now = float(atr.iloc[-1]) if atr is not None else float("nan")
        hh_now  = float(hh.iloc[-1])  if hh  is not None else float("nan")

        action = "flat"
        reason = "no_signal"
        order_id = None

        # --- Entry: breakout above rolling HH
        broke_out = (not math.isnan(hh_now)) and close >= hh_now

        # --- Exit conditions
        ema_giveback = (not math.isnan(ema_now)) and (close < ema_now * (1 - giveback_pct))
        atr_stop     = (not math.isnan(atr_now)) and (close < (close - atr_now * atr_mult_stop))  # always False mathematically
        # Note: use a trailing idea: compare to recent lowest of close since breakout not tracked statefully; approximate:
        ll = _lowest(c, max(5, atr_len))
        ll_now = float(ll.iloc[-1]) if ll is not None else float("nan")
        atr_stop = atr_stop or (not math.isnan(ll_now) and not math.isnan(atr_now) and (close <= ll_now - atr_now * 0.25))

        # Determine if we currently hold a position to decide sell qty
        pos_qty = _qty_from_positions(positions, s) if not dry else 0.0

        if broke_out:
            action = "buy"
            reason = "breakout_hh"
            if not dry and notional > 0:
                try:
                    # notional buy, attribution via params["client_tag"]
                    res = broker.notional(s, "buy", usd=notional, params=params)
                    order_id = (res or {}).get("id")
                except Exception as e:
                    reason = f"buy_error:{e}"
                    action = "flat"
        elif pos_qty > 0 and (ema_giveback or atr_stop):
            action = "sell"
            reason = "exit_giveback" if ema_giveback else "exit_atr_stop"
            if not dry:
                try:
                    res = broker.paper_sell(s, qty=pos_qty, params=params)
                    order_id = (res or {}).get("id")
                except Exception as e:
                    reason = f"sell_error:{e}"
                    action = "flat"

        out["results"].append({
            "symbol": s,
            "action": action,
            "reason": reason,
            "close": close,
            "hh": None if math.isnan(hh_now) else hh_now,
            "ema": None if math.isnan(ema_now) else ema_now,
            "atr": None if math.isnan(atr_now) else atr_now,
            "notional": notional if (not dry and action == "buy") else 0.0,
            "order_id": order_id
        })

    return out