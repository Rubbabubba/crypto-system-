# strategies/book.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import pandas as pd
import math

# ====== utilities ======
def _roll_mean(a, n): return pd.Series(a).rolling(n).mean().to_numpy()
def _roll_std(a, n):  return pd.Series(a).rolling(n).std(ddof=0).to_numpy()

def _zscore(x, n):
    s = pd.Series(x)
    m = s.rolling(n).mean()
    sd = s.rolling(n).std(ddof=0).replace(0, np.nan)
    return ((s - m) / sd).to_numpy()

def _atr(high, low, close, n=14):
    h, l, c = map(pd.Series, (high, low, close))
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean().to_numpy()

def _rsi(values, n=14):
    s = pd.Series(values)
    d = s.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).to_numpy()

def _roc(values, n=12):
    v = pd.Series(values)
    return (v / v.shift(n) - 1.0).to_numpy()

# ====== data classes ======
@dataclass
class ScanRequest:
    strat: str
    timeframe: str
    limit: int
    topk: int
    min_score: float
    notional: float

@dataclass
class ScanResult:
    symbol: str
    action: str
    reason: str
    score: float
    atr: float
    atr_pct: float
    qty: float
    notional: float
    selected: bool

# ====== regime computation ======
@dataclass
class Regimes:
    trend_z: float
    atr: float
    atr_pct: float
    sma_fast: float
    sma_slow: float

def compute_regimes(close, high, low) -> Regimes:
    sma_f = _roll_mean(close, 20)
    sma_s = _roll_mean(close, 60)
    trend_z = _zscore(sma_f - sma_s, 60)
    atr = _atr(high, low, close, 14)
    atr_pct = pd.Series(atr).rolling(200).rank(pct=True).to_numpy()
    i = len(close) - 1
    def last(x): return float(x[i]) if len(x) else float("nan")
    return Regimes(
        trend_z=last(trend_z),
        atr=last(atr),
        atr_pct=last(atr_pct),
        sma_fast=last(sma_f),
        sma_slow=last(sma_s),
    )

# ====== raw signals ======
def sig_c1_adaptive_rsi(close, regimes: Regimes,
                        rsi_len=14, band_lookback=100, k=0.7, min_atr_pct=0.25):
    r = _rsi(close, rsi_len)
    s = pd.Series(r).rolling(max(30, band_lookback)).std(ddof=0).to_numpy()
    i = len(close) - 1
    lower = 50 - k * (s[i] if np.isfinite(s[i]) else 5)
    upper = 50 + k * (s[i] if np.isfinite(s[i]) else 5)
    action, score, reason = "flat", 0.0, "no_raw_signal"
    if (regimes.atr_pct or 0.0) < min_atr_pct:
        return "flat", 0.0, "filt_vol_too_low"
    if np.isfinite(r[i]):
        if r[i] < lower:
            action = "buy"
            score = float((lower - r[i]) / max(1.0, s[i] or 1.0))
            reason = "rsi_adaptive_long"
        elif r[i] > upper:
            action = "sell"
            score = float((r[i] - upper) / max(1.0, s[i] or 1.0))
            reason = "rsi_adaptive_short"
    return action, score, reason

def sig_c2_trend(close, regimes: Regimes,
                 f=20, s=60, pullback=5, min_atr_pct=0.35):
    i = len(close) - 1
    if (regimes.atr_pct or 0.0) < min_atr_pct:
        return "flat", 0.0, "filt_vol_too_low"
    sma_f = _roll_mean(close, f)
    sma_s = _roll_mean(close, s)
    up = sma_f[i] > sma_s[i]
    dn = sma_f[i] < sma_s[i]
    pb = close[i] < pd.Series(close).rolling(pullback).max().to_numpy()[i] if up else \
         close[i] > pd.Series(close).rolling(pullback).min().to_numpy()[i] if dn else False
    if up and pb:
        return "buy", float(abs(regimes.trend_z)), "trend_up_pb"
    if dn and pb:
        return "sell", float(abs(regimes.trend_z)), "trend_down_pb"
    return "flat", 0.0, "no_raw_signal"

def sig_c3_momentum(close, regimes: Regimes,
                    roc_len=12, rsi_slope_len=7, min_atr_pct=0.30):
    if (regimes.atr_pct or 0.0) < min_atr_pct:
        return "flat", 0.0, "filt_vol_too_low"
    roc = _roc(close, roc_len)
    rsi = _rsi(close, 14)
    i = len(close) - 1
    rsi_slope = pd.Series(rsi).diff().rolling(rsi_slope_len).mean().to_numpy()[i]
    if np.isfinite(roc[i]) and np.isfinite(rsi_slope):
        if roc[i] > 0 and rsi_slope > 0:
            return "buy", float(roc[i] + 0.1 * rsi_slope), "mom_up"
        if roc[i] < 0 and rsi_slope < 0:
            return "sell", float(abs(roc[i]) + 0.1 * abs(rsi_slope)), "mom_down"
    return "flat", 0.0, "no_raw_signal"

def sig_c4_breakout(close, regimes: Regimes,
                    don_len=20, min_bandwidth=0.75, min_atr_pct=0.30):
    if (regimes.atr_pct or 0.0) < min_atr_pct:
        return "flat", 0.0, "filt_vol_too_low"
    s = pd.Series(close)
    hi = s.rolling(don_len).max().to_numpy()
    lo = s.rolling(don_len).min().to_numpy()
    i  = len(close) - 1
    bandwidth = (hi[i] - lo[i]) / (regimes.atr or np.nan)
    if not np.isfinite(bandwidth) or bandwidth < min_bandwidth:
        return "flat", 0.0, "range_narrow"
    if close[i] > hi[i-1]:
        return "buy", float(bandwidth), "breakout_high"
    if close[i] < lo[i-1]:
        return "sell", float(bandwidth), "breakout_low"
    return "flat", 0.0, "range_no_break"

def sig_c5_alt_mom(close, regimes: Regimes):
    return sig_c3_momentum(close, regimes, roc_len=20, rsi_slope_len=9, min_atr_pct=0.30)

def sig_c6_rel_to_btc(close, regimes: Regimes, ref_close_btc: Optional[np.ndarray] = None):
    if ref_close_btc is None:
        return sig_c3_momentum(close, regimes, roc_len=10, rsi_slope_len=5, min_atr_pct=0.25)
    rel = (pd.Series(close) / pd.Series(ref_close_btc)).to_numpy()
    r = Regimes(trend_z=regimes.trend_z, atr=regimes.atr, atr_pct=regimes.atr_pct,
                sma_fast=regimes.sma_fast, sma_slow=regimes.sma_slow)
    return sig_c3_momentum(rel, r, roc_len=12, rsi_slope_len=7, min_atr_pct=0.25)

def mtf_confirm(action: str, reg5: Regimes) -> bool:
    if action == "buy":  return reg5.sma_fast > reg5.sma_slow
    if action == "sell": return reg5.sma_fast < reg5.sma_slow
    return True

def size_from_atr(price: float, atr: float, target_risk_usd: float = 10.0, atr_mult: float = 1.0):
    if not np.isfinite(atr) or atr <= 0 or not np.isfinite(price) or price <= 0:
        return 0.0, 0.0
    risk_per_unit = atr * atr_mult
    qty = target_risk_usd / risk_per_unit
    notional = qty * price
    return float(max(0.0, qty)), float(max(0.0, notional))

class StrategyBook:
    def __init__(self, topk=2, min_score=0.10, risk_target_usd=10.0, atr_stop_mult=1.5):
        self.topk = int(topk)
        self.min_score = float(min_score)
        self.risk_target_usd = float(risk_target_usd)
        self.atr_stop_mult = float(atr_stop_mult)

    def scan(self, req: ScanRequest, contexts: Dict[str, Optional[Dict[str, Any]]]) -> List[ScanResult]:
        results: List[ScanResult] = []
        ref_btc = None
        if "BTC/USD" in contexts and contexts["BTC/USD"]:
            ref_btc = contexts["BTC/USD"]["one"]["close"]

        for sym, ctx in contexts.items():
            if not ctx:
                results.append(ScanResult(sym, "flat", "no_bars", 0.0, 0.0, 0.0, 0.0, 0.0, False))
                continue
            one = ctx["one"]; five = ctx["five"]
            close1, high1, low1 = one["close"], one["high"], one["low"]
            close5, high5, low5 = five["close"], five["high"], five["low"]
            if len(close1) < 50 or len(close5) < 50:
                results.append(ScanResult(sym, "flat", "insufficient_bars", 0.0, 0.0, 0.0, 0.0, 0.0, False))
                continue

            reg1 = compute_regimes(close1, high1, low1)
            reg5 = compute_regimes(close5, high5, low5)

            action, score, reason = "flat", 0.0, "no_raw_signal"
            s = req.strat.lower()
            if s == "c1":
                action, score, reason = sig_c1_adaptive_rsi(close1, reg1)
            elif s == "c2":
                action, score, reason = sig_c2_trend(close1, reg1)
            elif s == "c3":
                action, score, reason = sig_c3_momentum(close1, reg1)
            elif s == "c4":
                action, score, reason = sig_c4_breakout(close1, reg1)
            elif s == "c5":
                action, score, reason = sig_c5_alt_mom(close1, reg1)
            elif s == "c6":
                action, score, reason = sig_c6_rel_to_btc(close1, reg1, ref_btc)
            else:
                action, score, reason = "flat", 0.0, "unknown_strategy"

            if action in ("buy", "sell") and not mtf_confirm(action, reg5):
                action, score, reason = "flat", 0.0, "filt_mtf_disagree"

            price = float(close1[-1])
            qty, notional = (0.0, 0.0)
            if action in ("buy", "sell") and score >= self.min_score:
                qty, notional = size_from_atr(price, reg1.atr, self.risk_target_usd, atr_mult=1.0)

            results.append(ScanResult(
                symbol=sym, action=action, reason=reason,
                score=float(score),
                atr=float(reg1.atr if np.isfinite(reg1.atr) else 0.0),
                atr_pct=float(reg1.atr_pct if np.isfinite(reg1.atr_pct) else 0.0),
                qty=qty, notional=notional, selected=False
            ))

        actionable = [r for r in results if r.action in ("buy","sell") and r.score >= self.min_score]
        actionable = sorted(actionable, key=lambda x: x.score, reverse=True)
        chosen = set([r.symbol for r in actionable[:max(0, self.topk)]])
        out: List[ScanResult] = []
        for r in results:
            r.selected = (r.symbol in chosen and r.action in ("buy","sell"))
            if (not r.selected) and r.action in ("buy","sell") and r.symbol not in chosen:
                r.action = "flat"
                r.reason = "score_below_cut"
                r.qty = 0.0
                r.notional = 0.0
            out.append(r)
        return out
