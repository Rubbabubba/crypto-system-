# strategies/c4.py
import os
try:
    import br_router as br
except Exception:
    from strategies import br_router as br
from strategies import utils_volatility as uv

STRAT = "c4"
VER   = os.getenv("C4_VER", "v2.0")

def _ema(vals, n):
    if n<=1 or len(vals)<n: return None
    k=2.0/(n+1.0); e=vals[0]
    for v in vals[1:]: e=v*k+e*(1-k)
    return e

def _pos_for(symbol: str):
    sym = symbol.replace("/", "")
    pos = br.list_positions() or []
    for p in pos:
        if p.get("symbol","").replace("/","") == sym:
            return float(p.get("qty",0.0) or 0.0), float(p.get("avg_entry_price",0.0) or 0.0)
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, dry, raw):
    FAST = int(raw.get("ema_fast", 12))
    SLOW = int(raw.get("ema_slow", 26))
    ATR_K = float(raw.get("atr_k", 2.0))
    MIN_ATR = float(raw.get("min_atr", 0.0))

    for sym in symbols:
        bars = br.get_bars(sym, timeframe, limit) or []
        if len(bars) < max(60, SLOW):
            continue

        atr = uv.atr_from_bars(bars, 14)
        if atr < MIN_ATR:
            continue

        closes = [b["c"] for b in bars]
        last = closes[-1]
        ema_f = _ema(closes[-FAST:], FAST)
        ema_s = _ema(closes[-SLOW:], SLOW)
        if ema_f is None or ema_s is None:
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
