# strategies/c1.py
import os, math
from typing import List, Dict
try:
    import broker as br
except Exception:
    from strategies import broker as br
from strategies import utils_volatility as uv

STRAT = "c1"
VER   = os.getenv("C1_VER", "v2.0")

def _ema(vals, n):
    if n <= 1 or len(vals) < n: return None
    k = 2.0 / (n + 1.0)
    ema = vals[0]
    for v in vals[1:]:
        ema = v * k + ema * (1 - k)
    return ema

def _vwap(bars):
    num = 0.0; den = 0.0
    for b in bars:
        tp = (float(b["h"]) + float(b["l"]) + float(b["c"])) / 3.0
        v  = float(b["v"])
        num += tp * v; den += v
    return (num / den) if den > 0 else float(bars[-1]["c"])

def _pos_for(symbol):
    sym = symbol.replace("/", "")
    for p in br.list_positions():
        if p.get("symbol") == sym or p.get("asset_symbol") == sym:
            return float(p.get("qty", 0.0)), float(p.get("avg_entry_price", 0.0))
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, live, ctx):
    EMA = int(os.getenv("C1_EMA", "20"))
    VWAP_PULL = float(os.getenv("C1_VWAP_PULL", "0.003"))  # 0.3%

    ATR_LEN   = int(os.getenv("VOL_ATR_LEN", "14"))
    MED_LEN   = int(os.getenv("VOL_MEDIAN_LEN", "20"))
    VOL_K     = float(os.getenv("VOL_K", "1.0"))

    for sym in (symbols if isinstance(symbols, list) else [symbols]):
        ok, _ = uv.is_tradeable(sym, timeframe, limit, atr_len=ATR_LEN, median_len=MED_LEN, k=VOL_K)
        if not ok:
            continue

        data = br.get_bars(sym, timeframe=timeframe, limit=max(limit, EMA + 5))
        bars = data.get(sym, [])
        if len(bars) < EMA + 5: 
            continue

        closes = [float(b["c"]) for b in bars]
        ema_n = _ema(closes[-EMA:], EMA)
        if ema_n is None:
            continue

        vwap_val = _vwap(bars[-EMA:])
        last = float(bars[-1]["c"])

        have, avg_px = _pos_for(sym)
        pull = (vwap_val - last) / max(vwap_val, 1e-9)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and pull >= VWAP_PULL and last >= ema_n:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and (last >= vwap_val or last < ema_n):
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return
