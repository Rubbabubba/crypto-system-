# strategies/c3.py
"""
C3 — MA crossover with ATR risk, R:R target, optional ATR trailing, session filter
Adapted from the provided TradingView Pine script for spot-crypto (long-first).
- Long entry on MA1 cross above MA2 (configurable types/lengths)
- Stop = swing(lowestLow, lookback) - ATR * RiskM
- Target = entry + R:R * risk (if enabled)
- Optional ATR trailing that arms after reaching a fraction of target (rrExit)
- Optional short entries (default OFF; spot crypto typically can't short)
- Cooldown between trades
- Rich 'reason' outputs for transparency
"""

__version__ = "2.0.0"

import os, json, time
from typing import List, Dict, Any

# ---------- math helpers (no pandas) ----------
def ema(values: List[float], length: int) -> List[float]:
    if length <= 1 or not values:
        return values[:]
    k = 2.0 / (length + 1.0)
    out=[]; e=values[0]
    for v in values:
        e = v*k + e*(1-k)
        out.append(e)
    return out

def sma(values: List[float], length: int) -> List[float]:
    if length <= 1 or not values:
        return values[:]
    out=[]; s=sum(values[:length]); out.extend([values[0]]*(length-1))
    for i in range(length-1, len(values)):
        if i == length-1:
            out.append(s/length)
        else:
            s += values[i] - values[i-length]
            out.append(s/length)
    return out

def wma(values: List[float], length: int) -> List[float]:
    if length <= 1 or not values:
        return values[:]
    weights = list(range(1, length+1))
    den = sum(weights)
    out=[]
    for i in range(len(values)):
        if i+1 < length:
            out.append(values[i])
        else:
            s=0.0
            for w,j in zip(weights, range(i-length+1, i+1)):
                s += values[j]*w
            out.append(s/den)
    return out

def hma(values: List[float], length: int) -> List[float]:
    if length <= 1 or not values:
        return values[:]
    w = wma(values, length//2)
    ww = wma(values, length)
    diff = [2*w[i] - ww[i] if i < len(w) and i < len(ww) else values[i] for i in range(len(values))]
    return wma(diff, int(length**0.5) or 1)

def dema(values: List[float], length: int) -> List[float]:
    e1 = ema(values, length)
    e2 = ema(e1, length)
    return [2*e1[i] - e2[i] for i in range(len(values))]

def t3(values: List[float], length: int, b: float = 0.7) -> List[float]:
    # Simplified Tim Tillson T3 via cascaded EMAs
    e1=ema(values,length); e2=ema(e1,length); e3=ema(e2,length)
    e4=ema(e3,length); e5=ema(e4,length); e6=ema(e5,length)
    c1 = -b**3
    c2 = 3*b*b + 3*b**3
    c3 = -6*b*b - 3*b - 3*b**3
    c4 = 1 + 3*b + 3*b*b + b**3
    return [c1*e6[i] + c2*e5[i] + c3*e4[i] + c4*e3[i] for i in range(len(values))]

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

def ma(values: List[float], typ: str, length: int) -> List[float]:
    t = (typ or "EMA").upper()
    if t == "SMA":  return sma(values, length)
    if t == "WMA":  return wma(values, length)
    if t == "HMA":  return hma(values, length)
    if t == "DEMA": return dema(values, length)
    if t == "T3":   return t3(values, length)
    # VWMA/VWAP need volume; omitted for safety
    return ema(values, length)

# ---------- tiny state io ----------
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

# ---------- config ----------
def _cfg(params: Dict[str, Any]) -> Dict[str, Any]:
    g = lambda k, d=None: params.get(k, os.getenv(k, d))
    return {
        # Timeframes
        "TIMEFRAME":      g("C3_TIMEFRAME","5Min"),
        # MA setup
        "MA1_TYPE":       g("C3_MA1_TYPE","EMA"),
        "MA2_TYPE":       g("C3_MA2_TYPE","EMA"),
        "MA1_LEN":        int(g("C3_MA1_LEN",21)),
        "MA2_LEN":        int(g("C3_MA2_LEN",50)),
        # ATR & swings
        "ATR_LEN":        int(g("C3_ATR_LEN",14)),
        "RISK_M":         float(g("C3_RISK_M",1.0)),  # stop buffer
        "SWING_LOOKBACK": int(g("C3_SWING_LOOKBACK",5)),
        # R:R target + trailing
        "USE_LIMIT":      str(g("C3_USE_LIMIT","1"))=="1",
        "RR_MULT":        float(g("C3_RR_MULT",1.0)),
        "TRAIL_ON":       str(g("C3_TRAIL_ON","0"))=="1",
        "TRAIL_ATR_MULT": float(g("C3_TRAIL_ATR_MULT",1.0)),
        "TRAIL_SOURCE":   g("C3_TRAIL_SOURCE","HL"),  # HL|CLOSE|OPEN
        "RR_EXIT_FRAC":   float(g("C3_RR_EXIT_FRAC",0.0)), # 0.5 → arm trail at 50% to target
        # Shorts (spot disabled by default)
        "ALLOW_SHORTS":   str(g("C3_ALLOW_SHORTS","0"))=="1",
        # Money & limits
        "ORDER_NOTIONAL": float(g("ORDER_NOTIONAL",25)),
        "COOLDOWN_SEC":   int(g("C3_COOLDOWN_SEC",600)),
        "MAX_POSITIONS":  int(g("C3_MAX_POSITIONS",5)),
        # Session filter
        "USE_SESSION":    str(g("C3_USE_SESSION","0"))=="1",
        "SESSION_GMT6":   g("C3_SESSION_IGN_GMT6","0000-0300"),  # ignore window
    }

# ---------- small helpers ----------
def _cooldown_ok(path, key, cd):
    st = _load_json(path, {})
    last = st.get(key)
    return (last is None) or (time.time() - float(last) >= cd)

def _touch(path,key):
    st = _load_json(path, {}); st[key]=time.time(); _save_json(path, st)

def _within_session(use_sess: bool, sess_str: str, now_utc: float) -> bool:
    # Pine used "ignore these hours". We do a simple UTC-hour ignore for GMT-6.
    # For simplicity, we just always allow if not enabled.
    if not use_sess: return True
    try:
        # Example "0000-0300" → ignore hours [0,1,2,3) GMT-6
        start = int(sess_str.split("-")[0][:2])
        end   = int(sess_str.split("-")[1][:2])
        # Convert UTC to GMT-6 hour
        hour_utc = int(time.gmtime(now_utc).tm_hour)
        hour_gmt6 = (hour_utc - 6) % 24
        ignore = (hour_gmt6 >= start and hour_gmt6 < end) if start < end else (hour_gmt6 >= start or hour_gmt6 < end)
        return not ignore
    except Exception:
        return True

# ---------- core run ----------
def run(market, broker, symbols: List[str], params: Dict[str, Any], dry: bool, pwrite):
    cfg = _cfg(params)
    tf = cfg["TIMEFRAME"]
    results=[]
    cooldown_path="storage/c3_cooldowns.json"
    state_path="storage/c3_state.json"
    state=_load_json(state_path, {})  # per-symbol state: {sym:{entry, stop, target, trail, armed}}

    # current positions to enforce MAX_POSITIONS
    try:
        positions = broker.get_positions()
        n_open = len(positions) if isinstance(positions, list) else 0
    except Exception:
        n_open = 0

    for sym in symbols:
        try:
            now = time.time()
            if not _within_session(cfg["USE_SESSION"], cfg["SESSION_GMT6"], now):
                results.append({"symbol": sym, "action": "flat", "reason": "session_filter"})
                continue

            bars = market.get_bars(sym, tf, limit=300)
            if not bars or len(bars) < max(cfg["MA1_LEN"], cfg["MA2_LEN"], cfg["ATR_LEN"], cfg["SWING_LOOKBACK"]) + 5:
                results.append({"symbol": sym, "status": "no_data"})
                continue

            closes=[float(b["close"]) for b in bars]
            highs =[float(b["high"])  for b in bars]
            lows  =[float(b["low"])   for b in bars]
            c=closes[-1]

            ma1 = ma(closes, cfg["MA1_TYPE"], cfg["MA1_LEN"])
            ma2 = ma(closes, cfg["MA2_TYPE"], cfg["MA2_LEN"])
            a   = atr(highs, lows, closes, cfg["ATR_LEN"])[-1]

            # swings
            ll=min(lows[-cfg["SWING_LOOKBACK"]:])
            hh=max(highs[-cfg["SWING_LOOKBACK"]:])

            # cross detection
            def crossed_up(x,y):  return x[-2] <= y[-2] and x[-1] > y[-1]
            def crossed_dn(x,y):  return x[-2] >= y[-2] and x[-1] < y[-1]

            want_long  = crossed_up(ma1, ma2)
            want_short = cfg["ALLOW_SHORTS"] and crossed_dn(ma1, ma2)

            # manage exits if in state
            st = state.get(sym, {})
            in_trade = bool(st.get("entry"))
            if in_trade:
                entry = float(st["entry"])
                stop  = float(st.get("stop", 0) or 0)
                target = st.get("target")
                trail  = st.get("trail")
                armed  = bool(st.get("armed", False))  # trailing armed after rrExit fraction

                risk = entry - stop if entry and stop else max(1e-9, a*cfg["RISK_M"])
                # Arm trailing if rrExit reached
                if cfg["TRAIL_ON"] and cfg["RR_EXIT_FRAC"]>0 and target:
                    rr_trigger = entry + cfg["RR_EXIT_FRAC"]*(target-entry)
                    if c >= rr_trigger: armed = True
                elif cfg["TRAIL_ON"] and cfg["RR_EXIT_FRAC"]==0:
                    armed = True

                # update trailing stop
                if cfg["TRAIL_ON"] and armed:
                    if cfg["TRAIL_SOURCE"].upper() == "CLOSE":
                        src = closes[-2]
                    elif cfg["TRAIL_SOURCE"].upper() == "OPEN":
                        src = float(bars[-2]["open"])
                    else:
                        src = min(lows[-cfg["SWING_LOOKBACK"]:])
                    new_trail = src - cfg["TRAIL_ATR_MULT"]*a
                    if not trail or new_trail > trail:
                        trail = new_trail

                # compute active stop (trail overrides if higher)
                active_stop = max(trail or -1e99, stop or -1e99)

                # exit conditions: stop or target
                do_exit = False
                reason="hold"
                if active_stop and c <= active_stop:
                    do_exit=True; reason="stop/trail"
                elif cfg["USE_LIMIT"] and target and c >= target:
                    do_exit=True; reason="target"

                if do_exit:
                    if not dry:
                        try:
                            # prefer close_position if available
                            if hasattr(broker, "close_position"):
                                broker.close_position(sym)
                            else:
                                broker.place_order_notional(symbol=sym, side="sell", notional=cfg["ORDER_NOTIONAL"], type="market")
                        except Exception as ex:
                            results.append({"symbol": sym, "action":"error", "error": str(ex)})
                    pwrite(sym, "c3", "SELL", cfg["ORDER_NOTIONAL"], reason, dry, {"entry": entry, "stop": active_stop, "target": target, "trail": trail, "close": c})
                    # clear state + cooldown
                    state.pop(sym, None)
                    _touch(cooldown_path, sym)
                    results.append({"symbol": sym, "action":"sell", "reason": reason, "close": c})
                    continue
                else:
                    # keep tracking
                    state[sym] = {"entry": entry, "stop": stop, "target": target, "trail": trail, "armed": armed}
                    results.append({"symbol": sym, "action":"hold", "close": c, "entry": entry, "stop": stop, "target": target, "trail": trail})
                    continue

            # if not in trade → evaluate entries
            if not _cooldown_ok(cooldown_path, sym, cfg["COOLDOWN_SEC"]):
                results.append({"symbol": sym, "action": "flat", "reason": "cooldown"})
                continue

            # position cap
            if n_open >= cfg["MAX_POSITIONS"] and not dry:
                results.append({"symbol": sym, "action": "flat", "reason": "max_positions"})
                continue

            # LONG entry
            if want_long:
                long_stop   = ll - a*cfg["RISK_M"]
                long_risk   = max(1e-9, c - long_stop)
                long_target = c + cfg["RR_MULT"]*long_risk if cfg["USE_LIMIT"] else None

                if not dry:
                    broker.place_order_notional(symbol=sym, side="buy", notional=cfg["ORDER_NOTIONAL"], type="market")
                pwrite(sym,"c3","BUY",cfg["ORDER_NOTIONAL"],"entry",dry,{"close":c,"stop":long_stop,"target":long_target,"atr":a})

                # initialize state
                st = {
                    "entry": c,
                    "stop": long_stop,
                    "target": long_target,
                    "trail": long_stop,  # start equals stop; trail will lift once armed
                    "armed": False
                }
                state[sym] = st
                _touch(cooldown_path, sym)
                results.append({"symbol": sym, "action":"buy", "close":c, "stop": long_stop, "target": long_target, "atr": a})
                continue

            # SHORT entry (optional; most spot brokers don't support)
            if want_short and cfg["ALLOW_SHORTS"]:
                short_stop  = hh + a*cfg["RISK_M"]
                short_risk  = max(1e-9, short_stop - c)
                short_tgt   = c - cfg["RR_MULT"]*short_risk if cfg["USE_LIMIT"] else None

                if not dry:
                    # If broker supports shorts; otherwise skip with reason
                    broker.place_order_notional(symbol=sym, side="sell", notional=cfg["ORDER_NOTIONAL"], type="market")
                pwrite(sym,"c3","SELL",cfg["ORDER_NOTIONAL"],"short_entry",dry,{"close":c,"stop":short_stop,"target":short_tgt,"atr":a})
                state[sym] = {"entry": c, "stop": short_stop, "target": short_tgt, "trail": short_stop, "armed": False}
                _touch(cooldown_path, sym)
                results.append({"symbol": sym, "action":"sell_short", "close":c, "stop": short_stop, "target": short_tgt, "atr": a})
                continue

            # otherwise flat
            results.append({"symbol": sym, "action": "flat", "close": c, "reason": "no_cross"})

        except Exception as ex:
            results.append({"symbol": sym, "action":"error", "error": str(ex)})

    _save_json(state_path, state)
    return results
