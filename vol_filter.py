# strategies/vol_filter.py
import numpy as np

def atr(highs, lows, closes, length=14):
    highs, lows, closes = map(np.array, (highs, lows, closes))
    tr = np.maximum(highs[1:], closes[:-1]) - np.minimum(lows[1:], closes[:-1])
    atr_vals = np.zeros_like(highs)
    atr_vals[length] = np.mean(tr[:length])
    for i in range(length+1, len(tr)):
        atr_vals[i] = (atr_vals[i-1]*(length-1) + tr[i-1]) / length
    return atr_vals

def atr_ok(bars, length=20):
    """Return True if current ATR > median(ATR n)."""
    highs = [b["h"] for b in bars]
    lows  = [b["l"] for b in bars]
    closes= [b["c"] for b in bars]
    a = atr(highs,lows,closes,length)
    curr = a[-1]
    med = np.median(a[-length:])
    return curr > med
