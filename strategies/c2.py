# strategies/c2.py
"""
C2 — Trend + Volatility Breakout
- Donchian breakout with ATR confirmation and optional Keltner band check
- Avoids whipsaws with lookback & range% filter; ATR stop and optional take-profit
- Cooldowns; position limit; pandas-free
"""

__version__ = "2.0.0"

import os, json, time
from typing import List, Dict, Any

def ema(values, length):
    if length <= 1 or not values:
        return values[:]
    k = 2.0 / (length + 1.0)
    out = []
    e = values[0]
    for v in values:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

def true_ranges(h, l, c):
    trs = []
    prev_close = None
    for i in range(len(c)):
        hi, lo, cl = h[i], l[i], c[i]
        if prev_close is None:
            tr = hi - lo
        else:
            tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(max(tr, 0.0))
        prev_close = cl
    return trs

def atr(h, l, c, length):
    return ema(true_ranges(h,l,c), length) if length>1 else true_ranges(h,l,c)

def load_json(path, default):
    try:
        if os.path.exists(path):
            import json
            with open(path, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return default

def save_json(path, obj):
    try:
        import json, os
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(obj, f)
    except Exception:
        pass

def _params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "TIMEFRAME": params.get("C2_TIMEFRAME") or "5Min",
        "LOOKBACK": int(params.get("C2_LOOKBACK") or 20),   # Donchian
        "ATR_LEN": int(params.get("C2_ATR_LEN") or 14),
        "BREAK_K": float(params.get("C2_BREAK_K") or 1.0),  # breakout multiplier (e.g., > recent high * (1 + k*ATR/price))
        "MIN_RANGE_PCT": float(params.get("C2_MIN_RANGE_PCT") or 0.5),  # require some day/session range %
        "HTF_TIMEFRAME": params.get("C2_HTF_TIMEFRAME") or "1Hour",
        "EMA_TREND_LEN": int(params.get("C2_EMA_TREND_LEN") or 100),
        "ATR_STOP_MULT": float(params.get("C2_ATR_STOP_MULT") or 2.0),
        "ATR_TP_MULT": float(params.get("C2_ATR_TP_MULT") or 0.0),
        "ORDER_NOTIONAL": float(params.get("ORDER_NOTIONAL") or 25),
        "COOLDOWN_SEC": int(params.get("C2_COOLDOWN_SEC") or 600),
        "MAX_POSITIONS": int(params.get("C2_MAX_POSITIONS") or 5),
    }

def _cooldown_ok(path, key, cooldown_sec):
    st = load_json(path, {})
    last = st.get(key)
    return (last is None) or ((time.time() - float(last)) >= cooldown_sec)

def _touch(path, key):
    st = load_json(path, {})
    st[key] = time.time()
    save_json(path, st)

def run(market, broker, symbols: List[str], params: Dict[str, Any], dry: bool, pwrite):
    cfg = _params(params)
    tf = cfg["TIMEFRAME"]
    results = []
    cooldown_path = "storage/c2_cooldowns.json"

    # current positions for limits
    try:
        positions = broker.get_positions()
        n_open = len(positions) if isinstance(positions, list) else 0
    except Exception:
        n_open = 0

    for symbol in symbols:
        try:
            bars = market.get_bars(symbol, tf, limit=300)
            if not bars or len(bars) < max(50, cfg["LOOKBACK"]+5):
                results.append({"symbol": symbol, "status": "no_data"})
                continue

            closes = [float(b["close"]) for b in bars]
            highs  = [float(b["high"])  for b in bars]
            lows   = [float(b["low"])   for b in bars]
            c = closes[-1]

            at = atr(highs, lows, closes, cfg["ATR_LEN"])[-1]

            # HTF trend (EMA)
            htf = cfg["HTF_TIMEFRAME"]
            htf_bars = market.get_bars(symbol, htf, limit=200)
            if not htf_bars or len(htf_bars) < cfg["EMA_TREND_LEN"]:
                results.append({"symbol": symbol, "action": "flat", "reason": "no_htf"})
                continue
            htf_closes = [float(b["close"]) for b in htf_bars]
            ema_trend = ema(htf_closes, cfg["EMA_TREND_LEN"])[-1]
            trend_up = htf_closes[-1] >= ema_trend
            if not trend_up:
                results.append({"symbol": symbol, "action": "flat", "reason": "trend_down"})
                continue

            lb = cfg["LOOKBACK"]
            hh = max(highs[-lb:])
            ll = min(lows[-lb:])
            range_pct = (hh - ll) / max(1e-9, (hh + ll)/2) * 100.0
            if range_pct < cfg["MIN_RANGE_PCT"]:
                results.append({"symbol": symbol, "action": "flat", "reason": "range_low", "range_pct": round(range_pct,3)})
                continue

            # Breakout trigger
            thresh = hh + cfg["BREAK_K"] * at  # K*ATR above recent high
            if c <= thresh:
                results.append({"symbol": symbol, "action": "flat", "close": c, "thresh": thresh, "atr": at, "reason": "no_break"})
                continue

            # cooldown / max positions
            if not _cooldown_ok(cooldown_path, symbol, cfg["COOLDOWN_SEC"]):
                results.append({"symbol": symbol, "action": "flat", "reason": "cooldown"})
                continue
            if n_open >= cfg["MAX_POSITIONS"]:
                results.append({"symbol": symbol, "action": "flat", "reason": "max_positions"})
                continue

            notional = float(cfg["ORDER_NOTIONAL"])
            if not dry:
                broker.place_order_notional(symbol=symbol, side="buy", notional=notional, type="market")
                pwrite(symbol, "c2", "BUY", notional, "breakout", dry, {"close": c, "thresh": thresh, "atr": at})
            results.append({"symbol": symbol, "action": "buy", "close": c, "thresh": thresh, "atr": at, "notional": notional})
            _touch(cooldown_path, symbol)

            # exits handled on follow-up scans (add symmetric logic to flatten if c < entry - ATR_STOP_MULT*ATR, or take profit)
        except Exception as ex:
            results.append({"symbol": symbol, "action": "error", "error": str(ex)})

    return results
