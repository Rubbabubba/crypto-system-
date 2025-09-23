# c3.py — MA Crossover (v1.8.6)
VERSION = "1.8.6"
NAME = "c3"
import time

from statistics import mean

def _sma(xs, n):
    if not xs: return None
    n = max(1,int(n))
    return sum(xs[-n:])/min(len(xs),n)

class System:
    def __init__(self, version):
        self.version = version
        self.name = NAME

    def scan(self, get_bars, symbols, timeframe, limit, params, notional, dry, tag, broker):
        ma_fast = int(params.get("ma_fast", 5))
        ma_slow = int(params.get("ma_slow", 9))
        out=[]
        for s in symbols:
            bars = get_bars(s, timeframe, limit)
            if not bars:
                out.append({"symbol": s, "action":"flat", "reason":"no_bars"}); continue
            closes=[b.get("c") or b.get("close") or 0 for b in bars]
            ma1=_sma(closes, ma_fast); ma2=_sma(closes, ma_slow); px=closes[-1]
            if ma1 and ma2 and ma1>ma2:
                action="buy"; reason="ma_fast>ma_slow"
                if not dry and notional>0:
                    coid=f"{tag}-{s.replace('/','')}-{int(time.time())}"
                    broker.place_order(symbol=s, side="buy", notional=notional, client_order_id=coid)
            else:
                action="flat"; reason="no_signal"
            out.append({"symbol":s, "action":action, "reason":reason, "close":px, "ma1":ma1, "ma2":ma2})
        return out

system = System(VERSION)