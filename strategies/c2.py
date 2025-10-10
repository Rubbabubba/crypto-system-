# strategies/c2.py
import os, math
from typing import List
try:
    import broker as br
except Exception:
    from strategies import broker as br
from strategies import utils_volatility as uv

STRAT = "c2"
VER   = os.getenv("C2_VER", "v2.0")

def _ema(vals, n):
    if n <= 1 or len(vals) < n: return None
    k = 2.0/(n+1.0); e = vals[0]
    for v in vals[1:]: e = v*k + e*(1-k)
    return e

def _rsi(closes, n=14):
    if len(closes) < n+1: return None
    gains = []; losses=[]
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d,0.0)); losses.append(max(-d,0.0))
    ag = sum(gains[-n:]) / n; al = sum(losses[-n:]) / n
    rs = (ag / al) if al>0 else float("inf")
    return 100.0 - (100.0 / (1.0 + rs))

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    for p in br.list_positions():
        if p.get("symbol")==sym or p.get("asset_symbol")==sym:
            return float(p.get("qty",0.0)), float(p.get("avg_entry_price",0.0))
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, live, ctx):
    EMA_FAST = int(os.getenv("C2_EMA_FAST", "12"))
    EMA_SLOW = int(os.getenv("C2_EMA_SLOW", "60"))
    RSI_LEN  = int(os.getenv("C2_RSI_LEN",  os.getenv("C2_RSI", "14")))
    RSI_LO   = int(os.getenv("C2_RSI_LOW",  "25"))
    RSI_HI   = int(os.getenv("C2_RSI_HIGH", "65"))

    ATR_LEN   = int(os.getenv("VOL_ATR_LEN", "14"))
    MED_LEN   = int(os.getenv("VOL_MEDIAN_LEN", "20"))
    VOL_K     = float(os.getenv("VOL_K", "1.0"))

    for sym in (symbols if isinstance(symbols, list) else [symbols]):
        ok,_ = uv.is_tradeable(sym, timeframe, limit, atr_len=ATR_LEN, median_len=MED_LEN, k=VOL_K)
        if not ok: 
            continue

        data = br.get_bars(sym, timeframe=timeframe, limit=max(limit, EMA_SLOW + 5))
        bars = data.get(sym, [])
        if len(bars) < EMA_SLOW + 5: 
            continue

        closes = [float(b["c"]) for b in bars]
        ema_f = _ema(closes[-EMA_FAST:], EMA_FAST)
        ema_s = _ema(closes[-EMA_SLOW:], EMA_SLOW)
        rsi = _rsi(closes, RSI_LEN)
        if ema_f is None or ema_s is None or rsi is None:
            continue

        last = closes[-1]
        have, avg_px = _pos_for(sym)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and rsi <= RSI_LO and ema_f >= ema_s:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and (rsi >= RSI_HI or ema_f < ema_s):
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return
