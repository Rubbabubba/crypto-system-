# strategies/c2.py
# ============================================================
# Strategy: Bollinger Mean Reversion (Position-aware)
# Version: 1.3.0  (2025-09-29)
# ============================================================

from __future__ import annotations
import typing as T
import pandas as pd

STRAT_ID = "c2"
STRAT_VERSION = "1.3.0"

def _bollinger(c: pd.Series, n: int, k: float):
    m = c.rolling(n, min_periods=n).mean()
    s = c.rolling(n, min_periods=n).std(ddof=0)
    return m, m + k * s, m - k * s

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
            return (held_action if held_action in ("flat", "note") else "flat", f"held_by_{holder}")
        if holder == STRAT_ID:
            return ("sell", reason) if desired == "sell" else (("buy", reason) if allow_add else ("flat","hold_in_pos"))
        # unknown/other
        return (desired, reason) if allow_conf else ("flat", f"held_by_{holder or 'unknown'}")
    return "flat", "unsupported_short_state"

def _decide_one(df: pd.DataFrame, params: dict):
    if df is None or df.empty:
        return "flat", "no_data"
    n = int(params.get("bb_n", 20))
    k = float(params.get("bb_k", 2.0))
    if df.shape[0] < n + 2:
        return "flat", "insufficient_bars"
    c = df["close"]
    mid, ub, lb = _bollinger(c, n, k)
    cl1, cl2 = c.iloc[-2], c.iloc[-1]
    ub1, ub2 = ub.iloc[-2], ub.iloc[-1]
    lb1, lb2 = lb.iloc[-2], lb.iloc[-1]
    if cl1 >= lb1 and cl2 < lb2:
        return "buy", "pierce_below_lower_bb"
    if cl1 <= ub1 and cl2 > ub2:
        return "sell", "pierce_above_upper_bb"
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
