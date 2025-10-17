# universe.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Any

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

# --- Simple env‑driven universe loader used by app.py
import os
def load_universe_from_env(defaults: list[str] | None = None) -> list[str]:
    """Reads SYMBOLS env var like 'BTCUSD,ETHUSD,SOLUSD'.
    Falls back to `defaults` (if provided) or an empty list."""
    raw = os.getenv("SYMBOLS","{}").strip()
    if not raw:
        return list(defaults or [])
    out = [s.strip().upper() for s in raw.split(",") if s.strip()]
    seen = set(); uniq = []
    for s in out:
        if s not in seen:
            uniq.append(s); seen.add(s)
    return uniq
