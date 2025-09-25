# strategies/c2.py — v1.9.1
# Crypto analog of S2: MACD up-cross + trend & volume gates, with clear exits.
#
# Defaults in 1.9.1:
#   • use_trend=True, trend_mode="intraday", ema_trend_len=50
#   • use_vol=True, vol_sma_len=20
#   • atr_len=14, atr_mult=0.8 (tighter than 1.0)
#
# ENTRY:
#   • MACD(12,26,9) up-cross on intraday bars
#   • AND price above intraday EMA(50) (trend gate)
#   • AND volume > SMA20(volume) (volume gate)
#
# EXIT (first hit):
#   • MACD down-cross
#   • OR close < EMA_trend - ATR*atr_mult  (uses intraday ATR)
#
# Inputs/Outputs:
#   run(df_map, params, positions) -> List[{symbol, action: "buy"|"sell"|"flat", reason}]
#   - df_map: { "BTC/USD": intraday_df, ... } with columns open,high,low,close,volume (ascending index)
#   - params: optional overrides for any defaults below
#   - positions: { "BTC/USD": qty_float, ... }
#
# Notes:
#   • Stateless: no persistent per-symbol state required.
#   • To use daily trend instead, pass params: {"trend_mode": "daily", "daily_df_map": {...}}
#   • Keep order sizing in your engine (notional/qty).

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME = "c2"
VERSION = "1.9.1"

# ---------- helpers ----------
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    hl = (h - l).abs()
    hc = (h - c.shift(1)).abs()
    lc = (l - c.shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = _ema(series, fast)
    ema_slow = _ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try:
        return float(positions.get(sym, 0.0)) > 0.0
    except Exception:
        return False

def _ok_df(df: pd.DataFrame, need_cols=("open","high","low","close","volume")) -> bool:
    return isinstance(df, pd.DataFrame) and all(c in df.columns for c in need_cols) and len(df) >= 60

# ---------- main ----------
def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    # Core MACD settings (S2-style)
    macd_fast   = int(params.get("macd_fast", 12))
    macd_slow   = int(params.get("macd_slow", 26))
    macd_signal = int(params.get("macd_signal", 9))

    # 1.9.1 defaults: require trend & volume by default
    use_trend     = bool(params.get("use_trend", True))
    trend_mode    = str(params.get("trend_mode", "intraday")).lower()  # "daily" | "intraday"
    trend_sma_d   = int(params.get("trend_sma_d", 10))                 # for daily trend
    ema_trend_len = int(params.get("ema_trend_len", 50))               # intraday trend EMA

    use_vol     = bool(params.get("use_vol", True))
    vol_sma_len = int(params.get("vol_sma_len", 20))

    # Tighter ATR multiple by default
    atr_len   = int(params.get("atr_len", 14))
    atr_mult  = float(params.get("atr_mult", 0.8))

    # Optional daily bars for trend check
    daily_map: Dict[str, pd.DataFrame] = params.get("daily_df_map", {}) or {}

    results: List[Dict[str, Any]] = []

    for sym, intraday in df_map.items():
        if not _ok_df(intraday):
            results.append({"symbol": sym, "action": "flat", "reason": "insufficient_intraday"})
            continue

        intraday = intraday.sort_index()
        c = intraday["close"]
        v = intraday["volume"]

        # --- Indicators (intraday) ---
        macd_line, sig_line, _ = _macd(c, macd_fast, macd_slow, macd_signal)
        if len(macd_line) < 3 or macd_line.isna().iloc[-2] or sig_line.isna().iloc[-2]:
            results.append({"symbol": sym, "action": "flat", "reason": "warming_up"})
            continue

        prev_diff = macd_line.iloc[-2] - sig_line.iloc[-2]
        last_diff = macd_line.iloc[-1] - sig_line.iloc[-1]
        macd_up   = (prev_diff <= 0) and (last_diff > 0)
        macd_dn   = (prev_diff >= 0) and (last_diff < 0)

        # Volume confirmation (required by default)
        vol_ok = True
        if len(v) >= vol_sma_len + 1:
            v_sma = _sma(v, vol_sma_len).iloc[-1]
            vol_ok = (v_sma is not None) and (v.iloc[-1] > v_sma)
        else:
            vol_ok = False  # enforce requirement if not enough history

        # Trend confirmation (required by default)
        trend_ok = True
        ema_trend_val = None

        if trend_mode == "daily":
            ddf = daily_map.get(sym)
            if _ok_df(ddf):
                ddf = ddf.sort_index()
                sma10 = _sma(ddf["close"], trend_sma_d).iloc[-1]
                trend_ok = (sma10 is not None) and (ddf["close"].iloc[-1] > sma10)
            else:
                # No daily data provided -> fail trend gate when daily mode is requested
                trend_ok = False
        else:
            # intraday trend via EMA(ema_trend_len)
            ema_trend_val = _ema(c, ema_trend_len).iloc[-1]
            trend_ok = c.iloc[-1] > ema_trend_val if pd.notna(ema_trend_val) else False

        have = _in_pos(positions, sym)

        # --- Exits first (if in position) ---
        if have:
            atr_now = _atr(intraday, atr_len).iloc[-1]
            if ema_trend_val is None:
                # defensible fallback for stop lane when daily mode used (or unavailable)
                ema_trend_val = _ema(c, max(ema_trend_len, macd_slow)).iloc[-1]

            atr_stop_line = (ema_trend_val - atr_mult * atr_now) if pd.notna(ema_trend_val) and pd.notna(atr_now) else None

            if macd_dn:
                results.append({"symbol": sym, "action": "sell", "reason": "macd_down_cross"})
                continue
            if atr_stop_line is not None and c.iloc[-1] < atr_stop_line:
                results.append({"symbol": sym, "action": "sell", "reason": "atr_vs_trend_stop"})
                continue

            results.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # --- Entries (flat) ---
        if (not have) and macd_up and trend_ok and vol_ok:
            results.append({"symbol": sym, "action": "buy", "reason": "macd_up_cross_trend_vol_ok"})
        else:
            reason_bits = []
            if not macd_up: reason_bits.append("no_macd_up")
            if not trend_ok: reason_bits.append("trend_fail")
            if not vol_ok: reason_bits.append("vol_fail")
            results.append({"symbol": sym, "action": "flat", "reason": " & ".join(reason_bits) if reason_bits else "no_signal"})

    return results
