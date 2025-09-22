# strategies/c6.py
# Version: 1.8.1
# - Adds SELL logic to EMA cross + HH confirmation.
# - Sell on EMA fast < EMA slow (cross-down) or ATR stop.
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
    tf       = _p(params, "timeframe", "5Min")
    limit    = _p(params, "limit", 600)
    notional = _p(params, "notional", 0.0)

    fast_len   = _p(params, "ema_fast_len", 12)
    slow_len   = _p(params, "ema_slow_len", 26)
    confirm_hh = _p(params, "confirm_hh_len", 20)  # require price > HH for confirmation
    atr_len    = _p(params, "atr_len", 14)
    atr_mult_stop = _p(params, "atr_mult_stop", 1.5)

    out = {"ok": True, "strategy": "c6", "dry": dry, "results": []}

    positions = []
    if not dry:
        try:
            positions = broker.positions()
        except Exception as e:
            log(event="positions_error", error=str(e))

    try:
        data = market.candles(symbols, timeframe=tf, limit=limit)
    except Exception as e:
        return {"ok": False, "strategy": "c6", "error": f"candles_error: {e}"}

    for s in symbols:
        df = (data or {}).get(s)
        need = max(slow_len, confirm_hh, atr_len) + 2
        if df is None or getattr(df, "shape", [0])[0] < need:
            out["results"].append({"symbol": s, "action": "flat", "reason": "insufficient_bars"})
            continue

        try:
            c = df["c"]
            h = df["h"]
        except KeyError:
            out["results"].append({"symbol": s, "action": "flat", "reason": "bad_columns"})
            continue

        ema_fast = _ema(c, fast_len)
        ema_slow = _ema(c, slow_len)
        atr = _atr(df, atr_len)
        hh  = _highest(h, confirm_hh)

        close = float(c.iloc[-1])
        ef_now = float(ema_fast.iloc[-1]) if ema_fast is not None else float("nan")
        es_now = float(ema_slow.iloc[-1]) if ema_slow is not None else float("nan")
        atr_now = float(atr.iloc[-1]) if atr is not None else float("nan")
        hh_now = float(hh.iloc[-1]) if hh is not None else float("nan")

        # Cross detection (use previous bar as well)
        ef_prev = float(ema_fast.iloc[-2]) if ema_fast is not None else float("nan")
        es_prev = float(ema_slow.iloc[-2]) if ema_slow is not None else float("nan")
        cross_up = (not math.isnan(ef_prev) and not math.isnan(es_prev) and not math.isnan(ef_now) and not math.isnan(es_now)
                    and ef_prev <= es_prev and ef_now > es_now)
        cross_dn = (not math.isnan(ef_prev) and not math.isnan(es_prev) and not math.isnan(ef_now) and not math.isnan(es_now)
                    and ef_prev >= es_prev and ef_now < es_now)

        # Confirmation: price above recent HH
        confirm = (not math.isnan(hh_now)) and (close >= hh_now)

        action = "flat"
        reason = "no_signal"
        order_id = None

        pos_qty = _qty_from_positions(positions, s) if not dry else 0.0

        # --- Entries
        if cross_up and confirm:
            action = "buy"
            reason = "ema_cross_up_confirm_hh"
            if not dry and notional > 0:
                try:
                    res = broker.notional(s, "buy", usd=notional, params=params)
                    order_id = (res or {}).get("id")
                except Exception as e:
                    reason = f"buy_error:{e}"
                    action = "flat"

        # --- Exits
        elif pos_qty > 0 and (cross_dn or (not math.isnan(atr_now) and close < es_now - atr_now * atr_mult_stop)):
            action = "sell"
            reason = "ema_cross_down" if cross_dn else "atr_stop"
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
            "ema_fast": None if math.isnan(ef_now) else ef_now,
            "ema_slow": None if math.isnan(es_now) else es_now,
            "hh": None if math.isnan(hh_now) else hh_now,
            "atr": None if math.isnan(atr_now) else atr_now,
            "notional": notional if (not dry and action == "buy") else 0.0,
            "order_id": order_id
        })

    return out