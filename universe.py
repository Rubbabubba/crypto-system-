#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
universe.py — Symbol universe helpers
Build: v2.0.0 (2025-10-11)

Exports
- DEFAULT_SYMBOLS: List[str]          # UI symbols, no slashes (e.g., "BTCUSD")
- parse_symbols(s: str) -> List[str]  # robust comma/space-separated parser
- load_universe_from_env() -> List[str]
- UniverseConfig, UniverseBuilder     # existing classes (unchanged behavior)

Notes
- UI symbols are normalized to UPPERCASE and '/' removed.
- Stablecoin quotes like USDT/USDC are normalized to USD in parse_symbols().
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Any
import os

__version__ = "2.0.0"

# -----------------------------------------------------------------------------
# Defaults used by app.py when no SYMBOLS env is set
# -----------------------------------------------------------------------------
DEFAULT_SYMBOLS: List[str] = [
    "BTCUSD",
    "ETHUSD",
    "SOLUSD",
    "ADAUSD",
    "DOGEUSD",
]

def _normalize_ui(sym: str) -> str:
    s = (sym or "").strip().upper().replace("/", "")
    if s.endswith("USDT") or s.endswith("USDC"):
        s = s[:-5] + "USD"
    return s

def parse_symbols(s: str) -> List[str]:
    """
    Parse a user/env string like: "BTCUSD, ETHUSD SOLUSD,BTC/USDT"
    -> ["BTCUSD", "ETHUSD", "SOLUSD", "BTCUSD"]
    """
    if not s:
        return []
    parts = []
    # split on commas first, then split any leftover whitespace chunks
    for chunk in s.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.extend(chunk.split())
    out = []
    for p in parts:
        n = _normalize_ui(p)
        if n:
            out.append(n)
    # de-dup but preserve order
    seen = set()
    dedup = []
    for x in out:
        if x not in seen:
            dedup.append(x)
            seen.add(x)
    return dedup

def load_universe_from_env() -> List[str]:
    """
    Returns SYMBOLS from env if present, otherwise DEFAULT_SYMBOLS.
    """
    env = os.getenv("SYMBOLS", "")
    syms = parse_symbols(env)
    return syms if syms else list(DEFAULT_SYMBOLS)

# -----------------------------------------------------------------------------
# Existing classes (kept intact for compatibility)
# -----------------------------------------------------------------------------
@dataclass
class UniverseConfig:
    max_symbols: int = 24
    min_dollar_vol_24h: float = 5_000_000.0
    max_spread_bps: float = 15.0
    min_rows_1m: int = 1500
    min_rows_5m: int = 300

class UniverseBuilder:
    def __init__(self, cfg: UniverseConfig):
        self.cfg = cfg
        self.symbols: List[str] = []

    def refresh_from_cache_like_source(self, candidates: List[str], candle_cache) -> None:
        # In absence of live L2/volume data here, we gate purely on "enough bars" first
        # (Your production can enrich with dollarVol/spread signals from your market data layer.)
        rows_ok = []
        for s in candidates:
            r1 = candle_cache.rows(s, "1Min")
            r5 = candle_cache.rows(s, "5Min")
            if r1 >= self.cfg.min_rows_1m and r5 >= self.cfg.min_rows_5m:
                rows_ok.append(s)
        self.symbols = rows_ok[: self.cfg.max_symbols]
