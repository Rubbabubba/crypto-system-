# c5.py — Breakout (v1.8.6)
VERSION = "1.8.6"
NAME = "c5"
import time

class System:
    def __init__(self, version):
        self.version=version
        self.name=NAME

    def scan(self, get_bars, symbols, timeframe, limit, params, notional, dry, tag, broker):
        length = int(params.get("breakout_len", 20))
        out=[]
        for s in symbols:
            bars = get_bars(s, timeframe, limit)
            if not bars or len(bars)<length:
                out.append({"symbol": s, "action":"flat", "reason":"no_bars"}); continue
            highs=[b.get("h") or b.get("high") or 0 for b in bars]
            closes=[b.get("c") or b.get("close") or 0 for b in bars]
            hh=max(highs[-length:]); px=closes[-1]
            if px>=hh:
                action="buy"; reason="close>=HH"
                if not dry and notional>0:
                    coid=f"{tag}-{s.replace('/','')}-{int(time.time())}"
                    broker.place_order(symbol=s, side="buy", notional=notional, client_order_id=coid)
            else:
                action="flat"; reason="no_signal"
            out.append({"symbol":s, "action":action, "reason":reason, "close":px, "hh":hh})
        return out

system = System(VERSION)