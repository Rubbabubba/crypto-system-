# strategies/symbol_map.py
"""
Symbol + timeframe normalization for Kraken.

Exports:
- KRAKEN_PAIR_MAP: dict[str, str] (UI symbol -> Kraken pair)
- to_kraken(ui_symbol: str) -> str
- tf_to_kraken(tf: str) -> int
"""

from __future__ import annotations
from typing import Dict

# --- UI -> Kraken pair map ---
# Add/adjust to match your traded universe. UI uses "BASE/QUOTE" (e.g., "BTC/USD").
KRAKEN_PAIR_MAP: Dict[str, str] = {
    "BTC/USD": "XBTUSD",   # Kraken uses XBT (not BTC) for the base
    "ETH/USD": "ETHUSD",
    "SOL/USD": "SOLUSD",
    "ADA/USD": "ADAUSD",
    "DOGE/USD": "DOGEUSD",
    "XRP/USD": "XRPUSD",
    "AVAX/USD": "AVAXUSD",
    "DOT/USD":  "DOTUSD",
    "MATIC/USD":"MATICUSD",
    "LINK/USD": "LINKUSD",
    "LTC/USD":  "LTCUSD",
    "BCH/USD":  "BCHUSD",
    # Stablecoins (if you ever trade them as symbols)
    "USDC/USD": "USDCUSD",
    "USDT/USD": "USDTUSD",
}

def _clean(sym: str) -> str:
    return (sym or "").strip().upper().replace("USDT", "USD").replace("USDC", "USD")

def to_kraken(ui_symbol: str) -> str:
    """
    Convert UI symbol 'BTC/USD' -> Kraken pair 'XBTUSD', with sensible fallbacks.
    If not found in KRAKEN_PAIR_MAP, try: remove '/', special-case BTC->XBT, append USD if needed.
    """
    s = _clean(ui_symbol)
    if s in KRAKEN_PAIR_MAP:
        return KRAKEN_PAIR_MAP[s]

    # Accept symbols without slash, add '/USD' if it looks like a bare base
    if "/" not in s and s.isalpha() and len(s) in (3,4,5):
        s = f"{s}/USD"

    # Now split
    if "/" in s:
        base, quote = s.split("/", 1)
    else:
        # last 3 chars as quote heuristic (e.g., ETHUSD)
        base, quote = s[:-3], s[-3:]

    if base == "BTC":
        base = "XBT"
    # Kraken pairs are e.g. XBTUSD, ETHUSD (no slash)
    return f"{base}{quote}"

# --- timeframe mapping ---
# Kraken supported intervals: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600
_TF_ALIASES = {
    "1": 1, "1m": 1, "1min": 1, "1minute": 1,
    "5": 5, "5m": 5, "5min": 5, "5minutes": 5,
    "15": 15, "15m": 15, "15min": 15,
    "30": 30, "30m": 30, "30min": 30,
    "60": 60, "1h": 60, "60m": 60,
    "240": 240, "4h": 240,
    "1440": 1440, "1d": 1440, "d": 1440, "day": 1440,
    "10080": 10080, "1w": 10080, "w": 10080, "week": 10080,
    "21600": 21600, "1M": 21600, "1mo": 21600, "month": 21600,  # Kraken's 21600 = ~15 days; Kraken docs call it "monthly" bucket
}

def tf_to_kraken(tf: str) -> int:
    """
    Convert a timeframe string to Kraken's interval minutes.
    Defaults to 5 if unknown, since most of your scans run on 5m.
    """
    if not tf:
        return 5
    k = tf.strip().lower()
    return _TF_ALIASES.get(k, _TF_ALIASES.get(k.replace("min", "m"), 5))
