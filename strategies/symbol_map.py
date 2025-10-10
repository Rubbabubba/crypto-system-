# strategies/symbol_map.py
"""
Robust symbol + timeframe mapping for Kraken.

- Resolves "BASE/QUOTE" (e.g., BTC/USD) to Kraken's OHLC pair "altname" (e.g., XBTUSD)
  by fetching AssetPairs once and caching the result.
- Falls back to a small static map if the live fetch fails (startup/offline).
- Maps app timeframes (e.g., "5Min") to Kraken OHLC `interval` minutes.
"""

from __future__ import annotations
import os
import time
import threading
from typing import Dict, Optional

import requests

# ---- Timeframe map (add more if your app uses them) ----
TF_TO_KRAKEN: Dict[str, int] = {
    "1Min": 1,
    "5Min": 5,
    "15Min": 15,
    "30Min": 30,
    "1Hour": 60,
    "4Hour": 240,
    "1Day": 1440,
}

# ---- Fallback static guesses (used only if the live resolver can't load) ----
FALLBACK_TO_KRAKEN: Dict[str, str] = {
    "BTC/USD": "XBTUSD",
    "ETH/USD": "ETHUSD",
    "SOL/USD": "SOLUSD",
    "DOGE/USD": "DOGEUSD",   # If your account says XDGUSD, the live resolver will correct it
    "ADA/USD": "ADAUSD",
    "AVAX/USD": "AVAXUSD",
    "LTC/USD": "LTCUSD",
    "BCH/USD": "BCHUSD",
    "LINK/USD": "LINKUSD",
    "DOT/USD": "DOTUSD",
    "MATIC/USD": "MATICUSD",
    "XRP/USD": "XRPUSD",
    "TRX/USD": "TRXUSD",
    "ATOM/USD": "ATOMUSD",
    "FIL/USD": "FILUSD",
    "NEAR/USD": "NEARUSD",
    "APT/USD": "APTUSD",
    "ARB/USD": "ARBUSD",
    "OP/USD": "OPUSD",
    "ETC/USD": "ETCUSD",
    "ALGO/USD": "ALGOUSD",
    "SUI/USD": "SUIUSD",
    "AAVE/USD": "AAVEUSD",
    "UNI/USD": "UNIUSD",
}

# ---- Live resolver (downloads AssetPairs once, refreshable) ----

_ASSETPAIRS_CACHE_LOCK = threading.Lock()
_ASSETPAIRS_CACHE: Optional[Dict[str, dict]] = None
_ASSETPAIRS_LAST_LOAD: float = 0.0
_ASSETPAIRS_TTL_SEC = float(os.getenv("KRAKEN_PAIRS_TTL", "3600"))  # 1 hour default

def _load_assetpairs(force: bool = False) -> Optional[Dict[str, dict]]:
    global _ASSETPAIRS_CACHE, _ASSETPAIRS_LAST_LOAD
    now = time.time()
    with _ASSETPAIRS_CACHE_LOCK:
        if not force and _ASSETPAIRS_CACHE and (now - _ASSETPAIRS_LAST_LOAD) < _ASSETPAIRS_TTL_SEC:
            return _ASSETPAIRS_CACHE
        try:
            r = requests.get("https://api.kraken.com/0/public/AssetPairs", timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                return _ASSETPAIRS_CACHE  # keep old if present
            result = data.get("result") or {}
            # Normalize into a dict keyed by uppercase wsname and altname for quick lookups
            idx: Dict[str, dict] = {}
            for pair_code, meta in result.items():
                ws = (meta.get("wsname") or "").upper()      # e.g., "XBT/USD"
                alt = (meta.get("altname") or "").upper()    # e.g., "XBTUSD"
                if ws:
                    idx[ws] = meta
                if alt:
                    idx[alt] = meta
            _ASSETPAIRS_CACHE = idx
            _ASSETPAIRS_LAST_LOAD = now
            return _ASSETPAIRS_CACHE
        except Exception:
            # network/error — leave cache as-is to allow fallback
            return _ASSETPAIRS_CACHE

def to_kraken(symbol: str) -> str:
    """
    Convert "BTC/USD" style to Kraken OHLC 'pair' (altname), e.g. "XBTUSD".
    Strategy:
      1) Try exact wsname lookup (e.g., "XBT/USD") using live AssetPairs
      2) Try altname lookup (e.g., "XBTUSD")
      3) If symbol already looks like an altname (no slash), pass through
      4) Fallback to static guess
    """
    s = symbol.strip().upper()
    # Already altname style?
    if "/" not in s:
        return s

    # Load live pairs
    idx = _load_assetpairs(force=False)

    # 1) Try wsname direct (e.g., "BTC/USD" or "XBT/USD")
    if idx:
        # Kraken wsname for BTC/USD is usually "XBT/USD", so check either form
        if s in idx and idx[s].get("altname"):
            return idx[s]["altname"].upper()

        # Also try the 'crypto-standard' XBT/USD when user asked for BTC/USD
        if s.startswith("BTC/"):
            xbt_ws = "XBT/" + s.split("/", 1)[1]
            if xbt_ws in idx and idx[xbt_ws].get("altname"):
                return idx[xbt_ws]["altname"].upper()

    # 2) Try simple join (BASE+QUOTE) as altname (e.g., "BTCUSD"), or XBT tweak
    base, quote = s.split("/", 1)
    guesses = [base + quote]
    if base == "BTC":
        guesses.insert(0, "XBT" + quote)
    for g in guesses:
        if idx and g in idx:
            return idx[g].get("altname", g).upper()

    # 3) Fallback static
    return FALLBACK_TO_KRAKEN.get(s, guesses[0].upper())

def tf_to_kraken(tf: str) -> int:
    v = TF_TO_KRAKEN.get(tf)
    if v is None:
        raise ValueError(f"Unsupported timeframe for Kraken: {tf}")
    return v
