# strategies/c4.py — v1.9.0
# Volume Bubbles Breakout (LuxAlgo-inspired) with stateless exits.
#
# Interface (unchanged):
#   run(df_map, params, positions) -> List[{symbol, action, reason}]
#   - df_map: {"BTC/USD": intraday_df, ...}  DataFrame cols: open,high,low,close,volume (ascending)
#   - params (optional):
#       bubble_df_map: {sym: bubble_df}         # preferred; higher-TF bars
#       bars_per_bubble: 60                     # fallback bubble size if no bubble_df_map
#       min_total_vol: 0.0                      # require this much bubble total volume
#       min_delta_frac: 0.30                    # |delta|/total >= this (0.30 = 30%)
#       break_k_atr: 0.0                        # add k·ATR to breakout threshold
#       atr_len: 14
#       atr_mult_stop: 1.0                      # defensive stop below bubble_low - k·ATR
#       fail_k_atr: 0.0                         # failure exit below bubble_high - k·ATR
#       allow_shorts: False                     # optional mirror for downside (off by default)
#
# Logic:
#   ENTRY (flat): thresholds pass AND close > bubble_high + break_k_atr*ATR
#   EXIT  (have): close <= bubble_high - fail_k_atr*ATR  (failed breakout)
#                 OR close <= bubble_low - atr_mult_stop*ATR (defensive stop)
#
# Notes:
#   • Last COMPLETED bubble = penultimate row of bubble DF (or last full window if emulated).
#   • Bubble buy/sell split uses wick/body heuristic from Lux-style scripts.
#   • Stateless exits so you don't need entry price or persisted state.

from __future__ import annotations
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

NAME = "c4"
VERSION = "1.9.0"

# ---- helpers ----
def _ok_df(df: pd.DataFrame) -> bool:
    need = {"open","high","low","close","volume"}
    return isinstance(df, pd.DataFrame) and need.issubset(df.columns) and len(df) >= 5

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def _split_vol_row(o, h, l, c, v):
    # Wick/body-based buy/sell split (Lux-inspired)
    bar_top  = h - max(o, c)
    bar_bot  = min(o, c) - l
    bar_rng  = h - l
    bull     = (c - o) > 0
    buy_rng  = bar_rng if bull else (bar_top + bar_bot)
    sell_rng = (bar_top + bar_bot) if bull else bar_rng
    total_r  = bar_rng + bar_top + bar_bot
    if total_r <= 0 or v <= 0:
        return 0.0, 0.0
    buy  = (buy_rng / total_r) * v
    sell = (sell_rng / total_r) * v
    return float(buy), float(sell)

def _bubble_from_df(df_b: pd.DataFrame) -> Optional[Dict[str, float]]:
    # Use the last COMPLETED bubble = penultimate row
    if not _ok_df(df_b) or len(df_b) < 2:
        return None
    b = df_b.iloc[-2]
    buy, sell = _split_vol_row(b["open"], b["high"], b["low"], b["close"], b["volume"])
    total = buy + sell
    delta = buy - sell
    return dict(
        high=float(b["high"]),
        low=float(b["low"]),
        open=float(b["open"]),
        close=float(b["close"]),
        total=float(total),
        delta=float(delta),
    )

def _bubble_emulate_from_intraday(df: pd.DataFrame, bars_per_bubble: int) -> Optional[Dict[str, float]]:
    n = int(bars_per_bubble or 0)
    if not _ok_df(df) or n < 5 or len(df) < n + 1:
        return None
    # emulate “last completed bubble” as the window ending at -2 (penultimate bar = completed)
    end = len(df) - 1  # last row index
    start = end - n
    if start < 0: return None
    window = df.iloc[start:end]  # exclude the last forming bar
    if len(window) < n: return None
    highs = window["high"]; lows = window["low"]; opens = window["open"]; closes = window["close"]; vols = window["volume"]
    buy_sum = sell_sum = 0.0
    for o,h,l,c,v in zip(opens, highs, lows, closes, vols):
        b,s = _split_vol_row(o,h,l,c,v)
        buy_sum += b; sell_sum += s
    return dict(
        high=float(highs.max()),
        low=float(lows.min()),
        open=float(opens.iloc[-1]),
        close=float(closes.iloc[-1]),
        total=float(buy_sum + sell_sum),
        delta=float(buy_sum - sell_sum),
    )

# ---- main ----
def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    # thresholds / risk
    min_total_vol  = float(params.get("min_total_vol", 0.0))
    min_delta_frac = float(params.get("min_delta_frac", 0.30))
    break_k_atr    = float(params.get("break_k_atr", 0.0))
    atr_len        = int(params.get("atr_len", 14))
    atr_mult_stop  = float(params.get("atr_mult_stop", 1.0))
    fail_k_atr     = float(params.get("fail_k_atr", 0.0))
    allow_shorts   = bool(params.get("allow_shorts", False))

    # bubble data
    bubble_map: Dict[str, pd.DataFrame] = params.get("bubble_df_map", {}) or {}
    bars_per_bubble = int(params.get("bars_per_bubble", 60))  # fallback when no bubble_df_map

    out: List[Dict[str, Any]] = []

    for sym, intraday in df_map.items():
        if not _ok_df(intraday):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_intraday"})
            continue

        intraday = intraday.sort_index()
        c = float(intraday["close"].iloc[-1])

        # ATR on intraday
        if len(intraday) < max(atr_len + 5, 50):
            out.append({"symbol": sym, "action": "flat", "reason": "warming_up"})
            continue
        atr_now = float(_atr(intraday, atr_len).iloc[-1])

        # find last completed bubble
        bdf = bubble_map.get(sym)
        if _ok_df(bdf) and len(bdf) >= 2:
            bub = _bubble_from_df(bdf.sort_index())
        else:
            bub = _bubble_emulate_from_intraday(intraday, bars_per_bubble)

        if not bub:
            out.append({"symbol": sym, "action": "flat", "reason": "no_bubble"})
            continue

        bub_hi = bub["high"]; bub_lo = bub["low"]; total = bub["total"]; delta = bub["delta"]
        frac = (abs(delta)/total) if total > 0 else 0.0

        have = False
        try:
            have = float(positions.get(sym, 0.0)) > 0.0
        except Exception:
            have = False

        # --- Exits first (stateless) ---
        if have:
            # Breakout failure (back under bubble high by a margin)
            if fail_k_atr >= 0 and c <= (bub_hi - fail_k_atr * atr_now):
                out.append({"symbol": sym, "action": "sell", "reason": "failed_breakout"})
                continue
            # Defensive stop vs. bubble low
            if atr_mult_stop > 0 and c <= (bub_lo - atr_mult_stop * atr_now):
                out.append({"symbol": sym, "action": "sell", "reason": "atr_vs_bubble_low"})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # --- Entries (flat) ---
        pass_total = (total >= min_total_vol)
        pass_delta = (frac >= min_delta_frac)
        thr = bub_hi + break_k_atr * atr_now

        if pass_total and pass_delta and c > thr:
            out.append({"symbol": sym, "action": "buy", "reason": "bubble_breakout"})
        else:
            why = []
            if not pass_total: why.append("total<th")
            if not pass_delta: why.append("delta_frac<th")
            if c <= thr:       why.append("no_break")
            out.append({"symbol": sym, "action": "flat", "reason": " & ".join(why) if why else "no_signal"})

        # Optional: shorts (disabled by default)
        # You can mirror the logic for downside using bub_lo - k*ATR, delta < 0, etc., if allow_shorts=True.

    return out