# c1.py — Baseline EMA Pullback (v1.8.6)
VERSION = "1.8.6"
NAME = "c1"

from statistics import mean
import time

def _ema(xs, n):
    if not xs: return None
    k = 2/(n+1)
    ema = xs[0]
    for v in xs[1:]:
        ema = v*k + ema*(1-k)
    return ema

def _atr(highs, lows, closes, n=14):
    trs=[]
    for i in range(1,len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    if len(trs)<1: return 0.0
    # simple moving average of TR as ATR
    return sum(trs[-n:])/min(n,len(trs))

class System:
    def __init__(self, version):
        self.version = version
        self.name = NAME

    def scan(self, get_bars, symbols, timeframe, limit, params, notional, dry, tag, broker):
        ema_len = int(params.get("ema_len", 20))
        atr_len = int(params.get("atr_len", 14))
        atr_mult = float(params.get("atr_mult", 1.5))
        out = []
        for s in symbols:
            bars = get_bars(s, timeframe, limit)
            if not bars:
                out.append({"symbol": s, "action": "flat", "reason": "no_bars"})
                continue
            closes = [b.get("c") or b.get("close") or 0 for b in bars]
            highs  = [b.get("h") or b.get("high") or 0 for b in bars]
            lows   = [b.get("l") or b.get("low") or 0 for b in bars]
            ema = _ema(closes, ema_len)
            atr = _atr(highs, lows, closes, atr_len)
            px = closes[-1]
            if px>ema:
                action = "buy"; reason="close_above_ema"
                if not dry and notional>0:
                    coid = f"{tag}-{s.replace('/','')}-{int(time.time())}"
                    broker.place_order(symbol=s, side="buy", notional=notional, client_order_id=coid)
            else:
                action = "flat"; reason="no_signal"
            out.append({"symbol": s, "action": action, "reason": reason, "close": px, "ema": ema, "atr": atr})
        return out

system = System(VERSION)