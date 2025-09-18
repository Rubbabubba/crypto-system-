# strategies/c4.py
"""
C4 — Volume Bubbles Breakout (LuxAlgo-inspired)
- Aggregate a "bubble" on a higher timeframe (bubble TF) using a wick/body volume split heuristic
- Use last COMPLETED bubble: total volume, delta (buy-sell), price range (hi/lo)
- Long entry when:
    * total >= C4_MIN_TOTAL_VOL
    * abs(delta)/total >= C4_MIN_DELTA_FRAC
    * current price breaks above bubble high (+ optional ATR buffer)
- Stop = bubble_low - ATR * C4_ATR_STOP_MULT
- Target = entry + RR * risk  (if C4_USE_LIMIT=1)
- Optional ATR trailing (C4_TRAIL_ON)
- Shorts optional (default off)
"""

__version__ = "1.0.0"

import os, json, time
from typing import List, Dict, Any

def ema(values: List[float], length: int) -> List[float]:
    if length <= 1 or not values: return values[:]
    k = 2.0 / (length + 1.0)
    out=[]; e=values[0]
    for v in values:
        e = v*k + e*(1-k)
        out.append(e)
    return out

def true_ranges(h, l, c):
    trs=[]; pc=None
    for i in range(len(c)):
        hi,lo,cl = h[i],l[i],c[i]
        tr = hi-lo if pc is None else max(hi-lo, abs(hi-pc), abs(lo-pc))
        trs.append(max(tr,0.0))
        pc=cl
    return trs

def atr(h,l,c,len_):
    return ema(true_ranges(h,l,c), len_) if len_>1 else true_ranges(h,l,c)

# ---- storage helpers ----
def _load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path,"r") as f: return json.load(f)
    except Exception: pass
    return default

def _save_json(path, obj):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path,"w") as f: json.dump(obj,f)
    except Exception: pass

def _cooldown_ok(path, key, cd):
    st = _load_json(path, {})
    last = st.get(key)
    return (last is None) or (time.time() - float(last) >= cd)

def _touch(path,key):
    st = _load_json(path, {}); st[key]=time.time(); _save_json(path, st)

def _cfg(params: Dict[str, Any]) -> Dict[str, Any]:
    g = lambda k, d=None: params.get(k, os.getenv(k, d))
    return {
        # Base TF for ATR and current price checks
        "TIMEFRAME":        g("C4_TIMEFRAME","5Min"),
        # Bubble TF (where volume bubble is built)
        "BUBBLE_TF":        g("C4_BUBBLE_TF","1Hour"),
        "LOOKBACK_BUBBLES": int(g("C4_LOOKBACK_BUBBLES", "24")),  # how many bubble bars to scan
        # Thresholds
        "MIN_TOTAL_VOL":    float(g("C4_MIN_TOTAL_VOL","0")),      # absolute; set >0 to enforce
        "MIN_DELTA_FRAC":   float(g("C4_MIN_DELTA_FRAC","0.30")),  # e.g., 0.30 → |delta| >= 30% of total
        "BREAK_K_ATR":      float(g("C4_BREAK_K_ATR","0.0")),      # add k*ATR to breakout threshold
        # Risk
        "ATR_LEN":          int(g("C4_ATR_LEN","14")),
        "ATR_STOP_MULT":    float(g("C4_ATR_STOP_MULT","1.0")),
        "USE_LIMIT":        str(g("C4_USE_LIMIT","1"))=="1",
        "RR_MULT":          float(g("C4_RR_MULT","1.2")),
        # Trailing
        "TRAIL_ON":         str(g("C4_TRAIL_ON","0"))=="1",
        "TRAIL_ATR_MULT":   float(g("C4_TRAIL_ATR_MULT","1.0")),
        "RR_EXIT_FRAC":     float(g("C4_RR_EXIT_FRAC","0.5")),
        # Shorts
        "ALLOW_SHORTS":     str(g("C4_ALLOW_SHORTS","0"))=="1",
        # Limits / timing
        "ORDER_NOTIONAL":   float(g("ORDER_NOTIONAL",25)),
        "COOLDOWN_SEC":     int(g("C4_COOLDOWN_SEC",600)),
        "MAX_POSITIONS":    int(g("C4_MAX_POSITIONS",5)),
    }

# Heuristic: split a bar's volume into buy/sell using wick/body like Lux script
def _split_vol(open_, high, low, close, vol):
    barTop  = high - max(open_, close)
    barBot  = min(open_, close) - low
    barRng  = high - low
    bull    = (close - open_) > 0
    buyRng  = barRng if bull else (barTop + barBot)
    sellRng = (barTop + barBot) if bull else barRng
    totalR  = barRng + barTop + barBot
    if totalR <= 0 or vol <= 0:
        return 0.0, 0.0
    buy = round((buyRng/totalR)*vol, 6)
    sell= round((sellRng/totalR)*vol, 6)
    return buy, sell

def _last_completed_bubble(bars):
    """
    bars: list of bubble timeframe bars [{open, high, low, close, volume, t}, ...]
    Return metrics for the last COMPLETED bubble (the penultimate bar if the last is still forming in real-time).
    """
    if not bars or len(bars) < 2: return None
    # Use the bar at index -2 as "completed"
    b = bars[-2]
    return b

def _build_bubble_metrics(bubble_bars):
    """Compute totals for the last completed bubble & some history stats."""
    if not bubble_bars or len(bubble_bars) < 2:
        return None
    # We'll compute buy/sell split on each bubble bar in the lookback, but entries depend on the LAST completed
    total_hist=[]
    delta_hist=[]
    rng_hist=[]
    for bb in bubble_bars[:-1]:  # exclude current forming bubble
        buy, sell = _split_vol(float(bb["open"]), float(bb["high"]), float(bb["low"]), float(bb["close"]), float(bb.get("volume", 0)))
        total = buy + sell
        delta = buy - sell
        rng   = float(bb["high"]) - float(bb["low"])
        total_hist.append(total); delta_hist.append(delta); rng_hist.append(rng)

    if not total_hist: return None
    last = bubble_bars[-2]
    buy, sell = _split_vol(float(last["open"]), float(last["high"]), float(last["low"]), float(last["close"]), float(last.get("volume", 0)))
    last_total = buy + sell
    last_delta = buy - sell
    return {
        "last": {
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            "open": float(last["open"]),
            "total": float(last_total),
            "delta": float(last_delta),
        },
        "hist": {
            "total_max": max(total_hist),
            "total_avg": sum(total_hist)/len(total_hist),
            "delta_max_abs": max(abs(x) for x in delta_hist),
            "rng_avg": sum(rng_hist)/len(rng_hist),
        }
    }

def run(market, broker, symbols: List[str], params: Dict[str, Any], dry: bool, pwrite):
    cfg = _cfg(params)
    results=[]
    cooldown_path="storage/c4_cooldowns.json"
    state_path="storage/c4_state.json"
    state=_load_json(state_path, {})  # per-symbol tracking {entry, stop, target, trail, armed}

    # positions cap
    try:
        positions = broker.get_positions()
        n_open = len(positions) if isinstance(positions, list) else 0
    except Exception:
        n_open = 0

    for sym in symbols:
        try:
            # pull base timeframe (for ATR & current)
            base = market.get_bars(sym, cfg["TIMEFRAME"], limit=300)
            if not base or len(base) < max(50, cfg["ATR_LEN"]+5):
                results.append({"symbol":sym,"status":"no_data_base"})
                continue
            closes=[float(x["close"]) for x in base]
            highs =[float(x["high"]) for x in base]
            lows  =[float(x["low"])  for x in base]
            c=closes[-1]
            a=atr(highs,lows,closes,cfg["ATR_LEN"])[-1]

            # bubble timeframe series
            bubbles = market.get_bars(sym, cfg["BUBBLE_TF"], limit=max(30, cfg["LOOKBACK_BUBBLES"]))
            if not bubbles or len(bubbles) < 3:
                results.append({"symbol":sym,"status":"no_data_bubble"})
                continue

            m = _build_bubble_metrics(bubbles)
            if not m:
                results.append({"symbol":sym,"status":"bubble_build_fail"})
                continue

            b_last = m["last"]
            bub_hi = b_last["high"]; bub_lo=b_last["low"]
            total  = b_last["total"]; delta=b_last["delta"]
            frac   = (abs(delta)/total) if total>0 else 0.0

            # manage open trade
            st = state.get(sym, {})
            in_trade = bool(st.get("entry"))
            if in_trade:
                entry=float(st["entry"]); stop=float(st.get("stop",0) or 0)
                target=st.get("target"); trail=st.get("trail"); armed=bool(st.get("armed", False))

                # arm trailing when a fraction towards target is reached
                if cfg["TRAIL_ON"] and cfg["RR_EXIT_FRAC"]>=0 and cfg["USE_LIMIT"] and target:
                    trigger = entry + cfg["RR_EXIT_FRAC"]*(target-entry)
                    if c >= trigger: armed=True
                elif cfg["TRAIL_ON"] and not cfg["USE_LIMIT"]:
                    armed=True

                # trail = max(prior trail, last bubble low + (?) or close - ATR*mult). We’ll use price-based trail: c - ATR*mult.
                if cfg["TRAIL_ON"] and armed:
                    new_trail = c - cfg["TRAIL_ATR_MULT"]*a
                    if not trail or new_trail > trail:
                        trail = new_trail

                active_stop = max(trail or -1e99, stop or -1e99)
                do_exit=False; reason="hold"
                if c <= active_stop:
                    do_exit=True; reason="stop/trail_hit"
                elif cfg["USE_LIMIT"] and target and c >= target:
                    do_exit=True; reason="target_hit"

                if do_exit:
                    if not dry:
                        try:
                            if hasattr(broker,"close_position"): broker.close_position(sym)
                            else: broker.place_order_notional(symbol=sym, side="sell", notional=cfg["ORDER_NOTIONAL"], type="market")
                        except Exception as ex:
                            results.append({"symbol":sym,"action":"error","error":str(ex)})
                    pwrite(sym,"c4","SELL",cfg["ORDER_NOTIONAL"],reason,dry,{"entry":entry,"stop":active_stop,"target":target,"trail":trail,"close":c})
                    state.pop(sym, None)
                    _touch(cooldown_path, sym)
                    results.append({"symbol":sym,"action":"sell","reason":reason,"close":c})
                    continue
                else:
                    state[sym]={"entry":entry,"stop":stop,"target":target,"trail":trail,"armed":armed}
                    results.append({"symbol":sym,"action":"hold","close":c,"entry":entry,"stop":stop,"target":target,"trail":trail})
                    continue

            # flat → evaluate entry
            if not _cooldown_ok(cooldown_path, sym, cfg["COOLDOWN_SEC"]):
                results.append({"symbol":sym,"action":"flat","reason":"cooldown"})
                continue
            if n_open >= cfg["MAX_POSITIONS"] and not dry:
                results.append({"symbol":sym,"action":"flat","reason":"max_positions"})
                continue

            # breakout above bubble high (+ K*ATR)
            thr = bub_hi + cfg["BREAK_K_ATR"]*a
            pass_total = (total >= cfg["MIN_TOTAL_VOL"])
            pass_delta = (frac >= cfg["MIN_DELTA_FRAC"])

            want_long = pass_total and pass_delta and (c > thr)

            if want_long:
                stop = bub_lo - cfg["ATR_STOP_MULT"]*a
                risk = max(1e-9, c - stop)
                tgt  = c + cfg["RR_MULT"]*risk if cfg["USE_LIMIT"] else None

                if not dry:
                    broker.place_order_notional(symbol=sym, side="buy", notional=cfg["ORDER_NOTIONAL"], type="market")
                pwrite(sym,"c4","BUY",cfg["ORDER_NOTIONAL"],"bubble_breakout",dry,{"close":c,"bubble_hi":bub_hi,"bubble_lo":bub_lo,"total":total,"delta":delta,"frac":frac,"atr":a})

                state[sym]={"entry":c,"stop":stop,"target":tgt,"trail":stop,"armed":False}
                _touch(cooldown_path, sym)
                results.append({"symbol":sym,"action":"buy","close":c,"stop":stop,"target":tgt,"atr":a,"bubble_hi":bub_hi})
                continue

            # (Optional) short logic (disabled by default)
            # e.g., c < (bub_lo - k*ATR) and negative delta. Not enabled unless ALLOW_SHORTS=1
            if cfg["ALLOW_SHORTS"]:
                thr_s = bub_lo - cfg["BREAK_K_ATR"]*a
                want_short = pass_total and pass_delta and (delta < 0) and (c < thr_s)
                if want_short:
                    stop = bub_hi + cfg["ATR_STOP_MULT"]*a
                    risk = max(1e-9, stop - c)
                    tgt  = c - cfg["RR_MULT"]*risk if cfg["USE_LIMIT"] else None
                    if not dry:
                        broker.place_order_notional(symbol=sym, side="sell", notional=cfg["ORDER_NOTIONAL"], type="market")
                    pwrite(sym,"c4","SELL",cfg["ORDER_NOTIONAL"],"bubble_breakdown",dry,{"close":c,"bubble_hi":bub_hi,"bubble_lo":bub_lo,"total":total,"delta":delta,"frac":frac,"atr":a})
                    state[sym]={"entry":c,"stop":stop,"target":tgt,"trail":stop,"armed":False}
                    _touch(cooldown_path, sym)
                    results.append({"symbol":sym,"action":"sell_short","close":c,"stop":stop,"target":tgt,"atr":a,"bubble_lo":bub_lo})
                    continue

            results.append({"symbol":sym,"action":"flat","close":c,"reason":"no_break_or_threshold","bubble_hi":bub_hi,"total":total,"frac":frac})

        except Exception as ex:
            results.append({"symbol":sym,"action":"error","error":str(ex)})

    _save_json(state_path, state)
    return results
