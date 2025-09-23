# c4.py — RSI Reversion (v1.8.6)
VERSION = "1.8.6"
NAME = "c4"
import time

def _rsi(closes, n=14):
    if len(closes) < n+1: return 50.0
    gains=[]; losses=[]
    for i in range(1,len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(0,d)); losses.append(max(0,-d))
    avg_gain = sum(gains[-n:])/n
    avg_loss = sum(losses[-n:])/n
    if avg_loss == 0: return 100.0
    rs = avg_gain/avg_loss
    return 100 - (100/(1+rs))

class System:
    def __init__(self, version):
        self.version=version
        self.name=NAME

    def scan(self, get_bars, symbols, timeframe, limit, params, notional, dry, tag, broker):
        rsi_len = int(params.get("rsi_len", 14))
        rsi_buy = float(params.get("rsi_buy", 55))
        out=[]
        for s in symbols:
            bars = get_bars(s, timeframe, limit)
            if not bars:
                out.append({"symbol": s, "action":"flat", "reason":"no_bars"}); continue
            closes=[b.get("c") or b.get("close") or 0 for b in bars]
            rsi = _rsi(closes, rsi_len)
            px=closes[-1]
            if rsi >= rsi_buy:
                action="buy"; reason="rsi>=threshold"
                if not dry and notional>0:
                    coid=f"{tag}-{s.replace('/','')}-{int(time.time())}"
                    broker.place_order(symbol=s, side="buy", notional=notional, client_order_id=coid)
            else:
                action="flat"; reason="no_signal"
            out.append({"symbol":s, "action":action, "reason":reason, "close":px, "rsi":rsi})
        return out

system = System(VERSION)