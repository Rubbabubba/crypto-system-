# strategies/symbol_map.py
# Maps our internal symbols/timeframes <-> Kraken

# Kraken uses XBT (not BTC). Most others match 1:1 with "/USD".
_TO_KRAKEN = {
    "BTC/USD": "XBT/USD",
    "ETH/USD": "ETH/USD",
    "SOL/USD": "SOL/USD",
    "DOGE/USD": "DOGE/USD",
    "ADA/USD": "ADA/USD",
    "AVAX/USD": "AVAX/USD",
    "LTC/USD": "LTC/USD",
    "BCH/USD": "BCH/USD",
    "LINK/USD": "LINK/USD",
    "DOT/USD": "DOT/USD",
    "MATIC/USD": "MATIC/USD",
    "XRP/USD": "XRP/USD",
    "TRX/USD": "TRX/USD",
    "ATOM/USD": "ATOM/USD",
    "FIL/USD": "FIL/USD",
    "NEAR/USD": "NEAR/USD",
    "APT/USD": "APT/USD",   # if not listed by Kraken, you’ll get a 400 on bars/orders
    "ARB/USD": "ARB/USD",
    "OP/USD":  "OP/USD",
    "ETC/USD": "ETC/USD",
    "ALGO/USD":"ALGO/USD",
    "SUI/USD": "SUI/USD",
    "AAVE/USD":"AAVE/USD",
    "UNI/USD": "UNI/USD",
}

# Inverse map for convenience if ever needed
_FROM_KRAKEN = {v: k for k, v in _TO_KRAKEN.items()}

# Our internal timeframes are strings like "1Min", "5Min", "15Min", "60Min"
# Kraken OHLC intervals are ints: 1,5,15,60,240,1440,10080,21600
_TF_TO_KRAKEN = {
    "1Min": 1,
    "5Min": 5,
    "15Min": 15,
    "30Min": 30,   # Kraken doesn’t have 30 explicitly; some endpoints support it; if not, you’ll get a 400
    "60Min": 60,
    "240Min": 240,
    "1440Min": 1440,     # 1 day
}

def to_kraken(sym: str) -> str:
    """Return Kraken pair name for our internal 'XXX/USD'."""
    return _TO_KRAKEN.get(sym, sym)

def from_kraken(sym: str) -> str:
    """Return our internal pair for a Kraken pair."""
    return _FROM_KRAKEN.get(sym, sym)

def tf_to_kraken(tf: str) -> int:
    """Map '5Min' -> 5 (minutes) for Kraken OHLC endpoints."""
    if tf in _TF_TO_KRAKEN:
        return _TF_TO_KRAKEN[tf]
    # Fallback: try to parse "<N>Min"
    if tf.endswith("Min"):
        try:
            return int(tf[:-3])
        except:
            pass
    # Safe default: 5
    return 5
