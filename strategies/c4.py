# strategies/c4.py
# ============================================================
# Strategy: Failed Breakout Reversal (Position-aware)
# Version: 1.3.0  (2025-09-29)
# ============================================================

from __future__ import annotations
import typing as T
import pandas as pd

STRAT_ID = "c4"
STRAT_VERSION = "1.3.0"

def _don(df: pd.DataFrame, n: int):
    hh = df["high"].rolling(n, min_periods=n).max()
    ll = df["low"].rolling(n, min_periods=n).min()
    return hh, ll

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
    if df is None or df.empty:
        return "flat", "no_data"
    n = int(params.get("don_n", 20))
    if df.shape[0] < n + 2:
        return "flat", "insufficient_bars"
    hh, ll = _don(df, n)
    c = df["close"]
    c1, c2 = c.iloc[-2], c.iloc[-1]
    hh1, hh2 = hh.iloc[-2], hh.iloc[-1]
    ll1, ll2 = ll.iloc[-2], ll.iloc[-1]

    # Failed breakout up (reversal down -> SELL)
    if (c1 > hh1) and (c2 <= hh2):
        return "sell", "failed_breakout"

    # Failed breakdown down (reversal up -> BUY)
    if (c1 < ll1) and (c2 >= ll2):
        return "buy", "failed_breakdown"

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
