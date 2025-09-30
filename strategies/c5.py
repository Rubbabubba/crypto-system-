# strategies/c5.py
# ============================================================
# Strategy: Trend Pullback Re-entry (Position-aware)
# Version: 1.3.0  (2025-09-29)
# ============================================================

from __future__ import annotations
import typing as T
import pandas as pd

STRAT_ID = "c5"
STRAT_VERSION = "1.3.0"

def _ema(s, n): return s.ewm(span=int(n), adjust=False).mean()

def _gate(symbol: str, desired: str, reason: str, params: dict) -> tuple[str, str]:
    pos = (params or {}).get("positions", {}).get(symbol)
    allow_add = bool((params or {}).get("allow_add", False))
    allow_conf = bool((params or {}).get("allow_conflicts", False))
    allow_shorts = bool((params or {}).get("allow_shorts", False))
    held_action = (params or {}).get("held_action", "flat")
    if not pos:
        if desired == "sell" and not allow_shorts:
            return "flat", "no_shorts"
        return desired, reason
    holder = pos.get("strategy")
    side = pos.get("side", "long")
    if side == "long":
        if holder and holder != STRAT_ID and not allow_conf:
            return (held_action if held_action in ("flat","note") else "flat", f"held_by_{holder}")
        if holder == STRAT_ID:
            return ("sell", reason) if desired == "sell" else (("buy", reason) if allow_add else ("flat","hold_in_pos"))
        return (desired, reason) if allow_conf else ("flat", f"held_by_{holder or 'unknown'}")
    return "flat", "unsupported_short_state"

def _decide_one(df: pd.DataFrame, params: dict):
    if df is None or df.empty or df.shape[0] < 210:
        return "flat", "insufficient_bars"
    c = df["close"]
    ema20  = _ema(c, int(params.get("ema_fast", 20)))
    ema50  = _ema(c, int(params.get("ema_mid", 50)))
    ema200 = _ema(c, int(params.get("ema_slow", 200)))

    c1, c2  = c.iloc[-2], c.iloc[-1]
    e201, e202 = ema20.iloc[-2], ema20.iloc[-1]
    e502, e2002 = ema50.iloc[-1], ema200.iloc[-1]

    if e502 > e2002:
        if (c1 < e201) and (c2 > e202):
            return "buy", "uptrend_recross_ema20"
    if e502 < e2002:
        if (c1 > e201) and (c2 < e202):
            return "sell", "downtrend_recross_ema20"
    return "flat", "hold_in_pos"

def run(df_map: T.Dict[str, pd.DataFrame], params: dict) -> dict:
    decisions = []
    p = params or {}
    for sym, df in (df_map or {}).items():
        try:
            desired, reason = _decide_one(df, p)
            action, gated_reason = _gate(sym, desired, reason, p)
        except Exception as e:
            action, gated_reason = "flat", f"error:{e}"
        decisions.append({"symbol": sym, "action": action, "reason": gated_reason})
    return {"strategy": STRAT_ID, "version": STRAT_VERSION, "decisions": decisions}
