# strategies/c6.py
import os
try:
    import broker as br
except Exception:
    from strategies import broker as br
from strategies import utils_volatility as uv

STRAT = "c6"
VER   = os.getenv("C6_VER", "v2.0")

def _ema(vals, n):
    if n<=1 or len(vals)<n: return None
    k=2.0/(n+1.0); e=vals[0]
    for v in vals[1:]: e=v*k+e*(1-k)
    return e

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    for p in br.list_positions():
        if p.get("symbol")==sym or p.get("asset_symbol")==sym:
            return float(p.get("qty",0.0)), float(p.get("avg_entry_price",0.0))
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, live, ctx):
    EMA_LEN = int(os.getenv("C6_EMA", "20"))
    EXIT_K  = float(os.getenv("C6_EXIT_K", "1.0"))

    ATR_LEN   = int(os.getenv("VOL_ATR_LEN", "14"))
    MED_LEN   = int(os.getenv("VOL_MEDIAN_LEN", "20"))
    VOL_K     = float(os.getenv("VOL_K", "1.0"))

    for sym in (symbols if isinstance(symbols, list) else [symbols]):
        ok,_ = uv.is_tradeable(sym, timeframe, limit, atr_len=ATR_LEN, median_len=MED_LEN, k=VOL_K)
        if not ok: 
            continue

        data = br.get_bars(sym, timeframe=timeframe, limit=max(limit, EMA_LEN + 5))
        bars = data.get(sym, [])
        if len(bars) < EMA_LEN + 5:
            continue

        closes = [float(b["c"]) for b in bars]
        last   = closes[-1]
        ema    = _ema(closes[-EMA_LEN:], EMA_LEN)
        if ema is None:
            continue

        have, avg = _pos_for(sym)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and last > ema:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and last < ema * EXIT_K:
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return
