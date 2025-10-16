# strategies/c5.py
import os
try:
    import br_router as br
except Exception:
import br_router as br
import utils_volatility as uv

STRAT = "c5"
VER   = os.getenv("C5_VER", "v2.0")

def _highest(xs, n): return max(xs[-n:]) if len(xs)>=n else None
def _lowest(xs, n):  return min(xs[-n:]) if len(xs)>=n else None

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    pos = br.list_positions() or []
    for p in pos:
        if p.get("symbol","").replace("/","") == sym:
            return float(p.get("qty",0.0) or 0.0), float(p.get("avg_entry_price",0.0) or 0.0)
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, dry, raw):
    N_BAND = int(raw.get("band_n", 20))
    BAND_K = float(raw.get("band_k", 1.0005))
    EXIT_K = float(raw.get("exit_k", 0.998))
    MIN_ATR = float(raw.get("min_atr", 0.0))

    for sym in symbols:
        bars = br.get_bars(sym, timeframe, limit) or []
        if len(bars) < max(30, N_BAND):
            continue
        atr = uv.atr_from_bars(bars, 14)
        if atr < MIN_ATR:
            continue

        closes = [b["c"] for b in bars]
        last = closes[-1]
        hh = _highest(closes, N_BAND)
        if hh is None:
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
