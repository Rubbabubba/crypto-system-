#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
symbol_map.py — Symbol & timeframe normalization (Kraken)
Build: v2.0.0 (2025-10-11)

Exports
- KRAKEN_PAIR_MAP: dict[str, str]  # UI symbol -> Kraken pair (e.g., "BTCUSD" -> "XBTUSD")
- to_kraken(ui_symbol: str) -> str
- from_kraken(pair: str) -> str
- tf_to_kraken(tf: str) -> int     # minutes for /0/public/OHLC

Notes
- UI symbols accepted with or without a slash: "BTCUSD" or "BTC/USD".
- "USDT" and "USDC" are normalized to "USD" for pairs (so BTCUSDT -> XBTUSD).
"""

from __future__ import annotations
from typing import Dict

__version__ = "2.0.0"

# -----------------------------------------------------------------------------
# UI -> Kraken pair map (extend as needed)
# -----------------------------------------------------------------------------
# Use NO slash and USD quote for UI keys in this dict, e.g., "BTCUSD".
KRAKEN_PAIR_MAP: Dict[str, str] = {
    # Majors
    "BTCUSD":  "XBTUSD",
    "ETHUSD":  "ETHUSD",
    "SOLUSD":  "SOLUSD",
    "ADAUSD":  "ADAUSD",
    "DOGEUSD": "XDGUSD",  # Kraken uses XDG
    "LTCUSD":  "LTCUSD",
    "XRPUSD":  "XRPUSD",
    "AVAXUSD": "AVAXUSD",
    "DOTUSD":  "DOTUSD",
    "MATICUSD":"MATICUSD",
    "LINKUSD": "LINKUSD",
    "BCHUSD":  "BCHUSD",
    # Add more here as you expand your universe
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _clean_ui(sym: str) -> str:
    """
    Normalize UI symbol:
    - Uppercase
    - Remove slash
    - Normalize USDT/USDC -> USD
    """
    s = (sym or "").strip().upper().replace("/", "")
    if s.endswith("USDT") or s.endswith("USDC"):
        s = s[:-5] + "USD"
    return s

def to_kraken(ui_symbol: str) -> str:
    """
    Convert UI symbol (BTCUSD or BTC/USD) to Kraken pair (XBTUSD, ETHUSD, ...).
    Falls back to cleaned UI if not found.
    """
    ui = _clean_ui(ui_symbol)
    return KRAKEN_PAIR_MAP.get(ui, ui)

def from_kraken(pair: str) -> str:
    """
    Convert Kraken pair back to UI symbol (best-effort), e.g., XBTUSD -> BTCUSD, XDGUSD -> DOGEUSD.
    """
    p = (pair or "").strip().upper().replace("/", "")
    # Reverse lookup if we have an exact mapping
    rev = {v: k for k, v in KRAKEN_PAIR_MAP.items()}
    if p in rev:
        return rev[p]
    # Heuristics for common assets
    if p.startswith("XBT"):
        return p.replace("XBT", "BTC")
    if p.startswith("XDG"):
        return p.replace("XDG", "DOGE")
    return p  # as-is

# -----------------------------------------------------------------------------
# Timeframe normalization
# -----------------------------------------------------------------------------
# Kraken OHLC interval values (minutes)
# https://docs.kraken.com/rest/#tag/Market-Data/operation/getOHLCData
_TF_MIN_ALIASES: Dict[str, int] = {
    # minutes
    "1": 1, "1m": 1, "1min": 1,
    "3": 3, "3m": 3,
    "5": 5, "5m": 5, "5min": 5, "5minut": 5, "5minu": 5,  # generous
    "15": 15, "15m": 15,
    "30": 30, "30m": 30,
    "60": 60, "1h": 60, "h": 60, "hour": 60,
    "240": 240, "4h": 240,
    "1440": 1440, "1d": 1440, "d": 1440, "day": 1440,
    "10080": 10080, "1w": 10080, "w": 10080, "week": 10080,
    # Kraken also supports 21600 (≈15 days); we expose "1mo" alias for convenience
    "21600": 21600, "1mo": 21600, "month": 21600,
}

def tf_to_kraken(tf: str) -> int:
    """
    Convert timeframe string to Kraken interval minutes.
    Accepts: "5m", "5Min", "5MIN", "1h", "4h", "1d", "1w", etc.
    Defaults to 5 (most scans are 5m).
    """
    if not tf:
        return 5
    k = tf.strip().lower()
    # normalize “5min/5Min/5MIN” into 5m
    if k.endswith("min"):
        k = k.replace("min", "m")
    return _TF_MIN_ALIASES.get(k, _TF_MIN_ALIASES.get(k.rstrip("s"), 5))
