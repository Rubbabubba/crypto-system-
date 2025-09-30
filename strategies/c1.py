# strategies/c1.py
# ============================================================
# Strategy: EMA Trend + VWAP Filter (Position-aware)
# Version: 1.4.0  (2025-09-29)
# ------------------------------------------------------------
# Logic:
#   • BUY when fast EMA > slow EMA AND close > rolling VWAP.
#   • SELL when fast EMA < slow EMA AND close < rolling VWAP.
# Position awareness (via params["positions"]):
#   • Flat: allow BUY; SELL only if allow_shorts.
#   • Long owned by c1: allow SELL exit; BUY only if allow_add.
#   • Long owned by another strategy: suppress unless allow_conflicts.
# ============================================================

from __future__ import annotations
import typing as T
import pandas as pd

STRAT_ID = "c1"
STRAT_VERSION = "1.4.0"

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=int(n), adjust=False).mean()

def _rolling_vwap(df: pd.DataFrame, win: int) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = tp * df["volume"]
    v = df["volume"].rolling(win, min_periods=1).sum()
    out = (pv.rolling(win, min_periods=1).sum()) / v
    return out.ffill().bfill()

def _gate(symbol: str, desired: str, reason: str, params: dict) -> tuple[str, str]:
    # desired: "buy" | "sell" as produced by the raw signal
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

    # Only long support by default
    if side == "long":
        if holder and holder != STRAT_ID and not allow_conf:
            return (held_action if held_action in ("flat", "note") else "flat",
                    f"held_by_{holder}")
        if holder == STRAT_ID:
            # We own it
            if desired == "sell":
                return "sell", reason
            # desired == buy
            return ("buy", reason) if allow_add else ("flat", "hold_in_pos")
        else:
            # Held by another or unknown owner
            if desired == "sell":
                # If we don't own it, don't force exit by default
                return ("sell", reason) if allow_conf else ("flat", f"held_by_{holder or 'unknown'}")
            else:
                return ("buy", reason) if allow_conf else ("flat", f"held_by_{holder or 'unknown'}")

    # If side == "short" and you want support, add logic here. Default: do nothing.
    return "flat", "unsupported_short_state"

def _decide_one(df: pd.DataFrame, params: dict) -> tuple[str, str]:
    if df is None or df.empty or df.shape[0] < 50:
        return "flat", "no_data"
    fast = int(params.get("ema_fast", 12))
    slow = int(params.get("ema_slow", 26))
    vwin = int(params.get("vwap_win", 50))

    c = df["close"]
    ema_f = _ema(c, fast)
    ema_s = _ema(c, slow)
    vwap = _rolling_vwap(df, vwin)

    cl = c.iloc[-1]
    if ema_f.iloc[-1] > ema_s.iloc[-1] and cl > vwap.iloc[-1]:
        return "buy", "trend_up_and_above_vwap"
    if ema_f.iloc[-1] < ema_s.iloc[-1] and cl < vwap.iloc[-1]:
        return "sell", "trend_down_and_below_vwap"
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
