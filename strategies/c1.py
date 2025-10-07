# strategies/c1.py
# Version: 1.5.0 (env-tunable)
from __future__ import annotations
import os, time, math, datetime as dt
from typing import Any, Dict, List
import broker as br

STRATEGY_NAME = "c1"
STRATEGY_VERSION = "1.5.0"

# === Tunable knobs (defaults match your prior behavior) ===
# EMA period used as the trend filter
C1_EMA = int(os.getenv("C1_EMA", "50"))
# Buy only if price is above EMA and within this pullback below VWAP (e.g., 0.997 = within 0.3% under VWAP)
C1_VWAP_PULL = float(os.getenv("C1_VWAP_PULL", "0.997"))

# ---------- Helpers ----------
def _sym(s: str) -> str:
    return s.replace("/","")

def _bars(symbol: str, timeframe: str, limit: int) -> List[Dict[str,Any]]:
    try:
        m = br.get_bars(symbol, timeframe=timeframe, limit=limit)
        return m.get(symbol, [])
    except Exception:
        return []

def _positions() -> List[Dict[str,Any]]:
    try:
        return br.list_positions()
    except Exception:
        return []

def _has_long(symbol: str) -> Dict[str,Any] | None:
    sym = _sym(symbol)
    for p in _positions():
        psym = p.get("symbol") or p.get("asset_symbol")
        if psym == sym and (p.get("side","long").lower() == "long"):
            return p
    return None

def _place(symbol: str, side: str, notional: float, client_id: str) -> Dict[str,Any]:
    try:
        return br.place_order(symbol, side, notional, client_id)
    except Exception as ex:
        return {"error": str(ex)}

def _ema(vals: List[float], n: int) -> List[float]:
    if not vals or n <= 0: return []
    k = 2/(n+1)
    out, ema = [], None
    for v in vals:
        ema = v if ema is None else (v*k + ema*(1-k))
        out.append(ema)
    return out

def _vwap(bars: List[Dict[str,Any]]) -> List[float]:
    cum_pv = 0.0
    cum_v  = 0.0
    out=[]
    for b in bars:
        h,l,c,v = float(b["h"]), float(b["l"]), float(b["c"]), float(b["v"])
        tp = (h+l+c)/3.0
        cum_pv += tp*v
        cum_v  += v
        out.append(cum_pv/max(1e-9,cum_v))
    return out

# ---------- Logic ----------
def _decide(symbol: str, bars: List[Dict[str,Any]]) -> Dict[str,str]:
    # need enough bars for EMA and VWAP; keep a small cushion
    need = max(60, int(C1_EMA) + 10)
    if len(bars) < need:
        return {"symbol": symbol, "action":"flat", "reason":"insufficient_bars"}

    closes = [float(b["c"]) for b in bars]
    vwap   = _vwap(bars)
    ema    = _ema(closes, int(C1_EMA))

    c, v, e = closes[-1], vwap[-1], ema[-1]
    have_long = _has_long(symbol) is not None

    # Long only: in uptrend (price > EMA), dip to/below VWAP by pullback factor
    if c > e and c < v * float(C1_VWAP_PULL):
        return {"symbol": symbol, "action":"buy", "reason":"vwap_pullback_uptrend"}

    # Exit: lose trend and below VWAP
    if have_long and c < e and c < v:
        return {"symbol": symbol, "action":"sell", "reason":"trend_break_under_vwap"}

    return {"symbol": symbol, "action":"flat", "reason":"hold_in_pos" if have_long else "no_signal"}

# ---------- Public API ----------
def run_scan(symbols: List[str], timeframe: str, limit: int, notional: float, dry: bool, extra: Dict[str,Any]):
    results, placed = [], []
    epoch = int(time.time())
    for s in symbols:
        bars = _bars(s, timeframe, limit)
        decision = _decide(s, bars)
        results.append(decision)
        if dry or decision["action"] == "flat":
            continue
        # prevent sells if flat (backtester relies on this to realize PnL properly)
        if decision["action"] == "sell" and not _has_long(s):
            continue
        coid = f"{STRATEGY_NAME}-{epoch}-{_sym(s).lower()}"
        ordres = _place(s, decision["action"], notional, coid)
        if "error" not in ordres:
            placed.append({
                "symbol": s, "side": decision["action"], "notional": notional,
                "status": ordres.get("status","accepted"),
                "client_order_id": ordres.get("client_order_id", coid),
                "filled_avg_price": ordres.get("filled_avg_price"),
                "id": ordres.get("id")
            })
    return {"strategy": STRATEGY_NAME, "version": STRATEGY_VERSION, "results": results, "placed": placed}
