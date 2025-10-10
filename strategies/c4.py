# strategies/c4.py
import os
try:
    import broker as br
except Exception:
    from strategies import broker as br
from strategies import utils_volatility as uv

STRAT = "c4"
VER   = os.getenv("C4_VER", "v2.0")

def _ema(vals, n):
    if n<=1 or len(vals)<n: return None
    k=2.0/(n+1.0); e=vals[0]
    for v in vals[1:]: e=v*k+e*(1-k)
    return e

def _atr_sma(highs, lows, closes, n):
    if len(closes) < n+1: return None
    trs=[]
    for i in range(1,len(closes)):
        h=highs[i]; l=lows[i]; pc=closes[i-1]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(trs) < n: return None
    return sum(trs[-n:])/n

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    for p in br.list_positions():
        if p.get("symbol")==sym or p.get("asset_symbol")==sym:
            return float(p.get("qty",0.0)), float(p.get("avg_entry_price",0.0))
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, live, ctx):
    EMA_FAST = int(os.getenv("C4_EMA_FAST", "12"))
    EMA_SLOW = int(os.getenv("C4_EMA_SLOW", "34"))
    ATR_LEN  = int(os.getenv("C4_ATR_LEN",  "14"))
    ATR_K    = float(os.getenv("C4_ATR_K",  "1.5"))

    VOL_ATR_LEN = int(os.getenv("VOL_ATR_LEN", "14"))
    MED_LEN     = int(os.getenv("VOL_MEDIAN_LEN", "20"))
    VOL_K       = float(os.getenv("VOL_K", "1.0"))

    for sym in (symbols if isinstance(symbols, list) else [symbols]):
        ok,_ = uv.is_tradeable(sym, timeframe, limit, atr_len=VOL_ATR_LEN, median_len=MED_LEN, k=VOL_K)
        if not ok: 
            continue

        data = br.get_bars(sym, timeframe=timeframe, limit=max(limit, max(EMA_SLOW, ATR_LEN) + 5))
        bars = data.get(sym, [])
        if len(bars) < max(EMA_SLOW, ATR_LEN) + 5:
            continue

        highs = [float(b["h"]) for b in bars]
        lows  = [float(b["l"]) for b in bars]
        closes= [float(b["c"]) for b in bars]
        last  = closes[-1]

        ema_f = _ema(closes[-EMA_FAST:], EMA_FAST)
        ema_s = _ema(closes[-EMA_SLOW:], EMA_SLOW)
        atr   = _atr_sma(highs, lows, closes, ATR_LEN)
        if None in (ema_f, ema_s, atr): 
            continue

        have, avg = _pos_for(sym)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and ema_f > ema_s and (last - ema_s) > ATR_K * atr / 10.0:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and (ema_f < ema_s or last < ema_s):
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return
