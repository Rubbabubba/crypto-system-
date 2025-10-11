# strategies/c6.py
import os
try:
    import br_router as br
except Exception:
    from strategies import br_router as br
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
    pos = br.list_positions() or []
    for p in pos:
        if p.get("symbol","").replace("/","") == sym:
            return float(p.get("qty",0.0) or 0.0), float(p.get("avg_entry_price",0.0) or 0.0)
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, dry, raw):
    N_EMA   = int(raw.get("ema_n", 34))
    EXIT_K  = float(raw.get("exit_k", 0.998))
    MIN_ATR = float(raw.get("min_atr", 0.0))

    for sym in symbols:
        bars = br.get_bars(sym, timeframe, limit) or []
        if len(bars) < max(40, N_EMA):
            continue

        atr = uv.atr_from_bars(bars, 14)
        if atr < MIN_ATR:
            continue

        closes = [b["c"] for b in bars]
        last = closes[-1]
        ema  = _ema(closes[-N_EMA:], N_EMA)
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
