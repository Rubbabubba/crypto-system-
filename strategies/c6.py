# c6.py — Dual EMA + HH confirm (v1.8.6)
VERSION = "1.8.6"
NAME = "c6"
import time

def _ema(xs, n):
    if not xs: return None
    k = 2/(n+1)
    ema = xs[0]
    for v in xs[1:]:
        ema = v*k + ema*(1-k)
    return ema

class System:
    def __init__(self, version):
        self.version=version
        self.name=NAME

    def scan(self, get_bars, symbols, timeframe, limit, params, notional, dry, tag, broker):
        fast = int(params.get("ema_fast_len", 12))
        slow = int(params.get("ema_slow_len", 26))
        hh_n = int(params.get("confirm_hh_len", 10))
        out=[]
        for s in symbols:
            bars = get_bars(s, timeframe, limit)
            if not bars:
                out.append({"symbol": s, "action":"flat", "reason":"no_bars"}); continue
            closes=[b.get("c") or b.get("close") or 0 for b in bars]
            highs =[b.get("h") or b.get("high") or 0 for b in bars]
            ef=_ema(closes, fast); es=_ema(closes, slow); px=closes[-1]; hh=max(highs[-hh_n:]) if len(highs)>=hh_n else None
            if ef and es and px and hh and ef>es and px>=hh:
                action="buy"; reason="ema_fast>slow & close>=HH"
                if not dry and notional>0:
                    coid=f"{tag}-{s.replace('/','')}-{int(time.time())}"
                    broker.place_order(symbol=s, side="buy", notional=notional, client_order_id=coid)
            else:
                action="flat"; reason="no_signal"
            out.append({"symbol":s, "action":action, "reason":reason, "close":px, "ema_fast":ef, "ema_slow":es, "hh":hh})
        return out

system = System(VERSION)