# strategies/c3.py
import os
try:
    import broker as br
except Exception:
    from strategies import broker as br
from strategies import utils_volatility as uv

STRAT = "c3"
VER   = os.getenv("C3_VER", "v2.0")

def _highest(xs, n): 
    return max(xs[-n:]) if len(xs)>=n else None
def _lowest(xs, n): 
    return min(xs[-n:]) if len(xs)>=n else None

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    for p in br.list_positions():
        if p.get("symbol")==sym or p.get("asset_symbol")==sym:
            return float(p.get("qty",0.0)), float(p.get("avg_entry_price",0.0))
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, live, ctx):
    LOOK    = int(os.getenv("C3_LOOK", "40"))
    BREAK_K = float(os.getenv("C3_BREAK_K", "1.002"))
    FAIL_K  = float(os.getenv("C3_FAIL_K",  "0.997"))

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

        ch_hi = _highest(highs, LOOK)
        ch_lo = _lowest(lows, LOOK)
        if ch_hi is None or ch_lo is None:
            continue

        have, avg = _pos_for(sym)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and last >= ch_hi * BREAK_K:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and last <= ch_lo * FAIL_K:
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return
