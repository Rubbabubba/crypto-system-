# strategies/c6.py — v1.9.0
# Fast/Slow EMA (MACD-style) + Higher-High confirmation, with robust, stateless exits.
#
# Signature (unchanged from your current C6):
#   run(df_map, params, positions) -> List[{symbol, action, reason}]
#     df_map:     {"BTC/USD": df, ...} DataFrame with columns open,high,low,close,volume (ascending index)
#     params:     optional overrides listed below
#     positions:  {"BTC/USD": qty_float, ...}
#
# Defaults (override via params):
#   ema_fast_len=12
#   ema_slow_len=26
#   confirm_hh_len=10         # require a recent Higher-High vs. prior N highs
#   atr_len=14
#   atr_mult_stop=1.0         # defensive stop vs slow-EMA lane: close < ema_slow - k*ATR
#   use_vol=False             # optional volume gate
#   vol_sma_len=20
#
# Logic:
#   ENTRY (flat)  : (fast > slow)  AND  (high > rolling_max_high(confirm_hh_len) on the last bar)
#                   AND ( !use_vol OR volume > SMA20(volume) )
#   EXIT  (have)  : (fast < slow)  OR  (close < ema_slow - atr_mult_stop*ATR)
#
from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd

NAME = "c6"
VERSION = "1.9.0"

# ---- helpers ----
def _ok_df(df: pd.DataFrame) -> bool:
    need = {"open","high","low","close","volume"}
    return isinstance(df, pd.DataFrame) and need.issubset(df.columns) and len(df) >= 60

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return _sma(tr, n)

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try:
        return float(positions.get(sym, 0.0)) > 0.0
    except Exception:
        return False

def _cross_up(x: pd.Series, y: pd.Series) -> bool:
    return len(x) >= 2 and len(y) >= 2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) \
           and (x.iloc[-2] <= y.iloc[-2]) and (x.iloc[-1] > y.iloc[-1])

def _cross_dn(x: pd.Series, y: pd.Series) -> bool:
    return len(x) >= 2 and len(y) >= 2 and pd.notna(x.iloc[-2]) and pd.notna(y.iloc[-2]) \
           and (x.iloc[-2] >= y.iloc[-2]) and (x.iloc[-1] < y.iloc[-1])

# ---- main ----
def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    ef = int(params.get("ema_fast_len", 12))
    es = int(params.get("ema_slow_len", 26))
    hh_n = int(params.get("confirm_hh_len", 10))
    atr_len = int(params.get("atr_len", 14))
    atr_mult_stop = float(params.get("atr_mult_stop", 1.0))

    use_vol = bool(params.get("use_vol", False))
    vol_sma_len = int(params.get("vol_sma_len", 20))

    results: List[Dict[str, Any]] = []

    for sym, df in df_map.items():
        if not _ok_df(df):
            results.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()

        close = df["close"]
        high  = df["high"]
        vol   = df["volume"]

        fast = _ema(close, ef)
        slow = _ema(close, es)
        atr_now = _atr(df, atr_len).iloc[-1]

        # warmup guards
        if pd.isna(fast.iloc[-2]) or pd.isna(slow.iloc[-2]) or pd.isna(atr_now):
            results.append({"symbol": sym, "action": "flat", "reason": "warming_up"})
            continue

        have = _in_pos(positions, sym)
        c    = close.iloc[-1]

        # Higher-high confirm: compare today's high to the rolling max of the previous hh_n highs
        if len(high) < hh_n + 2:
            hh_confirm = False
        else:
            prior_max = high.rolling(hh_n).max().shift(1).iloc[-1]
            hh_confirm = pd.notna(prior_max) and (high.iloc[-1] > prior_max)

        # Volume gate (optional)
        vol_ok = True
        if use_vol:
            if len(vol) >= vol_sma_len + 1:
                vol_ok = vol.iloc[-1] > _sma(vol, vol_sma_len).iloc[-1]
            else:
                vol_ok = False

        # Cross states (bar-close)
        cross_up = _cross_up(fast, slow)
        cross_dn = _cross_dn(fast, slow)

        # ---- Exits first (if in position) ----
        if have:
            # 1) Cross-down
            if cross_dn:
                results.append({"symbol": sym, "action": "sell", "reason": "ema_cross_down"})
                continue
            # 2) Defensive ATR-vs-slow-EMA stop
            stop_line = slow.iloc[-1] - atr_mult_stop * atr_now if pd.notna(slow.iloc[-1]) else None
            if stop_line is not None and c < stop_line:
                results.append({"symbol": sym, "action": "sell", "reason": "atr_vs_slow_ema_stop"})
                continue

            results.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # ---- Entries (flat) ----
        if cross_up and hh_confirm and vol_ok:
            results.append({"symbol": sym, "action": "buy", "reason": "cross_up_hh_confirm" + ("_vol_ok" if use_vol else "")})
        else:
            why = []
            if not cross_up:   why.append("no_cross_up")
            if not hh_confirm: why.append("no_higher_high")
            if not vol_ok:     why.append("vol_fail")
            results.append({"symbol": sym, "action": "flat", "reason": " & ".join(why) if why else "no_signal"})

    return results
