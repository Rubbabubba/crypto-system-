# strategies/utils_volatility.py
# Volatility gate for C1–C6 (ATR > K * median(ATR))
# Uses existing broker.get_bars() patched by backtest_c1_c6.py

import os
from typing import List, Dict, Tuple
import math

try:
    # works after backtest patches add these to sys.path
    import broker as br
except Exception:
    # if strategies.broker is the path
import broker as br

# ---------- small numeric utils ----------
def _sma(xs, n):
    if n <= 0 or len(xs) < n:
        return None
    return sum(xs[-n:]) / n

def _median(xs):
    n = len(xs)
    if n == 0:
        return None
    xs2 = sorted(xs)
    mid = n // 2
    if n % 2 == 1:
        return xs2[mid]
    return 0.5 * (xs2[mid - 1] + xs2[mid])

def _true_range(h, l, pc):
    return max(h - l, abs(h - pc), abs(l - pc))

def _atr_sma(highs, lows, closes, atr_len: int) -> List[float]:
    """Simple ATR using SMA of True Range (not Wilder's), sufficient for gating."""
    if len(closes) < atr_len + 1:
        return []
    trs = []
    for i in range(1, len(closes)):
        trs.append(_true_range(highs[i], lows[i], closes[i - 1]))
    # trs length == len(closes) - 1
    atr = []
    win = []
    for tr in trs:
        win.append(tr)
        if len(win) > atr_len:
            win.pop(0)
        if len(win) == atr_len:
            atr.append(sum(win) / atr_len)
        else:
            atr.append(float("nan"))
    # pad to align with closes length
    pad = [float("nan")]
    return pad + atr  # length == len(closes)

# ---------- public API ----------
def is_tradeable(
    symbol: str,
    timeframe: str,
    limit: int,
    atr_len: int = None,
    median_len: int = None,
    k: float = None,
) -> Tuple[bool, Dict]:
    """
    Returns (ok, info) where ok==True means pass volatility gate.
    Gate: ATR_now > k * median(ATR_last_median_len)
    """
    # env defaults
    atr_len = int(os.getenv("VOL_ATR_LEN", atr_len or 14))
    median_len = int(os.getenv("VOL_MEDIAN_LEN", median_len or 20))
    k = float(os.getenv("VOL_K", k if k is not None else 1.0))
    verbose = os.getenv("VOL_VERBOSE", "0") == "1"

    # fetch bars (already patched by backtester)
    data = br.get_bars(symbol, timeframe=timeframe, limit=max(limit, atr_len + median_len + 5))
    bars = data.get(symbol, []) if isinstance(data, dict) else data
    if not bars or len(bars) < atr_len + median_len + 2:
        info = {"reason": "insufficient_bars", "bars": len(bars), "need": atr_len + median_len + 2}
        if verbose:
            print(f"[VOL] {symbol} insufficient bars: {info}")
        return False, info

    highs = [float(b["h"]) for b in bars]
    lows = [float(b["l"]) for b in bars]
    closes = [float(b["c"]) for b in bars]

    atr_series = _atr_sma(highs, lows, closes, atr_len=atr_len)
    # ensure we have enough good ATR values
    good_atr = [x for x in atr_series if not (x is None or math.isnan(x))]
    if len(good_atr) < median_len + 1:
        info = {"reason": "insufficient_atr", "atr_points": len(good_atr), "need": median_len + 1}
        if verbose:
            print(f"[VOL] {symbol} insufficient ATR: {info}")
        return False, info

    atr_now = good_atr[-1]
    base_slice = good_atr[-(median_len+1):-1]  # last M ATRs excluding current
    atr_median = _median(base_slice)

    threshold = (k * atr_median) if atr_median is not None else None
    ok = (atr_now is not None and atr_median is not None and atr_now > threshold)

    info = {
        "atr_len": atr_len,
        "median_len": median_len,
        "k": k,
        "atr_now": round(atr_now, 8) if atr_now is not None else None,
        "atr_median": round(atr_median, 8) if atr_median is not None else None,
        "threshold": round(threshold, 8) if threshold is not None else None,
        "pass": bool(ok),
    }
    if verbose:
        tag = "PASS" if ok else "FAIL"
        print(f"[VOL {tag}] {symbol} atr_now={info['atr_now']} > k*med={info['threshold']}? k={k} (len={atr_len},M={median_len})")

    return ok, info


def gate_all(
    symbols: List[str],
    timeframe: str,
    limit: int,
    atr_len: int = None,
    median_len: int = None,
    k: float = None,
) -> Dict[str, Dict]:
    """
    Check gate for each symbol. Returns dict[symbol] = {"pass": bool, ...diagnostics}
    """
    out = {}
    for sym in symbols:
        ok, info = is_tradeable(sym, timeframe, limit, atr_len=atr_len, median_len=median_len, k=k)
        out[sym] = {"pass": ok, **info}
    return out
