# strategies/c3.py
# Version: 1.4.0 (production tuned + env-tunable)
from __future__ import annotations
import os, time
from typing import Any, Dict, List
import broker as br

STRATEGY_NAME = "c3"
STRATEGY_VERSION = "1.4.0"

# === Tuned defaults (env-overridable) ===
# Best from optimization on 15Min / 30d / notional=250
C3_LOOK    = int(os.getenv("C3_LOOK", "40"))      # lookback bars to define prior high
C3_BREAK_K = float(os.getenv("C3_BREAK_K", "1.002"))  # buy if close > prior_high * C3_BREAK_K
C3_FAIL_K  = float(os.getenv("C3_FAIL_K",  "0.996"))  # exit if close < prior_high * C3_FAIL_K when long

# ---------- Helpers ----------
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

def _place(symbol: str, side: str, notional: float, client_order_id: str) -> Dict[str, Any]:
    try:
        return br.place_order(symbol, side, notional, client_order_id)
    except Exception as ex:
        return {"error": str(ex)}

# ---------- Logic ----------
def _decide(symbol: str, bars: List[Dict[str, Any]]) -> Dict[str, str]:
    need = max(120, int(C3_LOOK) + 5)  # ensure enough history
    if len(bars) < need:
        return {"symbol": symbol, "action": "flat", "reason": "insufficient_bars"}

    highs  = [float(b["h"]) for b in bars]
    closes = [float(b["c"]) for b in bars]
    c = closes[-1]

    look = int(C3_LOOK)
    prior_max = max(highs[-(look + 1):-1])
    have_long = _has_long(symbol) is not None

    # Breakout entry
    if c > prior_max * float(C3_BREAK_K):
        return {"symbol": symbol, "action": "buy", "reason": "breakout_lookback"}

    # Failed breakout exit
    if have_long and c < prior_max * float(C3_FAIL_K):
        return {"symbol": symbol, "action": "sell", "reason": "failed_breakout"}

    return {"symbol": symbol, "action": "flat", "reason": "hold_in_pos" if have_long else "no_signal"}

# ---------- Public API ----------
def run_scan(symbols, timeframe, limit, notional, dry, extra):
    out, placed = [], []
    epoch = int(time.time())
    for s in symbols:
        dec = _decide(s, _bars(s, timeframe, limit))
        out.append(dec)
        if dry or dec["action"] == "flat":
            continue
        if dec["action"] == "sell" and not _has_long(s):
            continue  # safety: never sell when flat
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
