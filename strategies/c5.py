# strategies/c5.py
import os
try:
    import broker as br
except Exception:
    from strategies import broker as br
from strategies import utils_volatility as uv

STRAT = "c5"
VER   = os.getenv("C5_VER", "v2.0")

def _highest(xs, n): return max(xs[-n:]) if len(xs)>=n else None
def _lowest(xs, n):  return min(xs[-n:]) if len(xs)>=n else None

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    for p in br.list_positions():
        if p.get("symbol")==sym or p.get("asset_symbol")==sym:
            return float(p.get("qty",0.0)), float(p.get("avg_entry_price",0.0))
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, live, ctx):
    BAND_K = float(os.getenv("C5_BAND_K", "1.03"))
    EXIT_K = float(os.getenv("C5_EXIT_K", "0.985"))
    LOOK   = 40

    ATR_LEN   = int(os.getenv("VOL_ATR_LEN", "14"))
    MED_LEN   = int(os.getenv("VOL_MEDIAN_LEN", "20"))
    VOL_K     = float(os.getenv("VOL_K", "1.0"))

    for sym in (symbols if isinstance(symbols, list) else [symbols]):
        ok,_ = uv.is_tradeable(sym, timeframe, limit, atr_len=ATR_LEN, median_len=MED_LEN, k=VOL_K)
        if not ok: 
            continue

        data = br.get_bars(sym, timeframe=timeframe, limit=max(limit, LOOK + 5))
        bars = data.get(sym, [])
        if len(bars) < LOOK + 5:
            continue

        highs = [float(b["h"]) for b in bars]
        lows  = [float(b["l"]) for b in bars]
        last  = float(bars[-1]["c"])
        hh = _highest(highs, LOOK); ll = _lowest(lows, LOOK)
        if hh is None or ll is None: 
            continue

        have, avg = _pos_for(sym)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and last >= hh * BAND_K:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and last <= hh * EXIT_K:
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return
