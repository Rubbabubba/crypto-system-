# strategies/c5.py
# Version: 1.2.0 (env-tunable, optimizer-friendly)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import broker as br

STRATEGY_NAME = "c5"
STRATEGY_VERSION = "1.2.0"

# === Tunables (env overrides) ===
C5_BAND_K = float(os.getenv("C5_BAND_K", "0.997"))  # entry: price < vwap * C5_BAND_K (pullback)
C5_EXIT_K = float(os.getenv("C5_EXIT_K", "1.002"))  # exit:  price > vwap * C5_EXIT_K (revert pop)

# --- helpers ---
def _sym(s: str) -> str:
    return s.replace("/", "")

def _bars(symbol: str, timeframe: str, limit: int) -> List[Dict[str, Any]]:
    try:
        m = br.get_bars(symbol, timeframe=timeframe, limit=limit)
        return m.get(symbol, [])
    except Exception:
        return []

def _positions() -> List[Dict[str, Any]]:
    try:
        return br.list_positions()
    except Exception:
        return []

def _has_long(symbol: str):
    sym = _sym(symbol)
    for p in _positions():
        psym = p.get("symbol") or p.get("asset_symbol")
        if psym == sym and p.get("side", "long").lower() == "long":
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str):
    try:
        return br.place_order(symbol, side, notional, client_id)
    except Exception as ex:
        return {"error": str(ex)}

def _ema(vals, n):
    if not vals or n <= 0: return []
    k = 2 / (n + 1)
    out, ema = [], None
    for v in vals:
        ema = v if ema is None else (v * k + ema * (1 - k))
        out.append(ema)
    return out

def _vwap(bars):
    cum_pv = 0.0; cum_v = 0.0; out = []
    for b in bars:
        h,l,c,v = float(b["h"]), float(b["l"]), float(b["c"]), float(b["v"])
        tp = (h + l + c) / 3.0
        cum_pv += tp * v
        cum_v  += v
        out.append(cum_pv / max(1e-9, cum_v))
    return out

# --- logic ---
def _decide(symbol: str, bars: List[Dict[str, Any]]):
    # need decent history for vwap stability
    need = 150
    if len(bars) < need:
        return {"symbol": symbol, "action": "flat", "reason": "insufficient_bars"}

    closes = [float(b["c"]) for b in bars]
    c = closes[-1]
    v = _vwap(bars)[-1]
    # light trend filter: 50-EMA up vs 200-EMA
    ema50  = _ema(closes, 50)[-1]
    ema200 = _ema(closes, 200)[-1]
    have_long = _has_long(symbol) is not None

    # Entry: uptrend (ema50>ema200), price pulled under VWAP by band
    if ema50 > ema200 and c < v * float(C5_BAND_K):
        return {"symbol": symbol, "action": "buy", "reason": "vwap_pullback_in_uptrend"}

    # Exit: mean reversion pop above VWAP*exit_k OR trend deteriorates
    if have_long and (c > v * float(C5_EXIT_K) or ema50 < ema200):
        return {"symbol": symbol, "action": "sell", "reason": "vwap_revert_or_trend_loss"}

    return {"symbol": symbol, "action": "flat", "reason": "hold_in_pos" if have_long else "no_signal"}

# --- public api ---
def run_scan(symbols, timeframe, limit, notional, dry, extra):
    out, placed = [], []
    epoch = int(time.time())
    for s in symbols:
        dec = _decide(s, _bars(s, timeframe, limit))
        out.append(dec)
        if dry or dec["action"] == "flat":
            continue
        if dec["action"] == "sell" and not _has_long(s):
            continue
        coid = f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        res = _place(s, dec["action"], notional, coid)
        if "error" not in res:
            placed.append({
                "symbol": s, "side": dec["action"], "notional": notional,
                "status": res.get("status", "accepted"),
                "client_order_id": res.get("client_order_id", coid),
                "filled_avg_price": res.get("filled_avg_price"),
                "id": res.get("id")
            })
    return {"strategy": STRATEGY_NAME, "version": STRATEGY_VERSION, "results": out, "placed": placed}
