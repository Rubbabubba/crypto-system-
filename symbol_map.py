from __future__ import annotations

# Basic map; extend as needed
KRAKEN_PAIR_MAP = {
    "BTC/USD": "XBTUSD",
    "ETH/USD": "ETHUSD",
    "SOL/USD": "SOLUSD",
    "ADA/USD": "ADAUSD",
    "XRP/USD": "XRPUSD",
    "DOGE/USD": "DOGEUSD",
    "LTC/USD": "LTCUSD",
    "BCH/USD": "BCHUSD",
}

def to_kraken(pair: str) -> str:
    """
    Convert UI 'BASE/QUOTE' or compact 'BTCUSD' into Kraken's pair symbol.
    """
    if not pair:
        return pair
    p = pair.replace(" ", "").replace(":", "/").upper()
    if "/" not in p and len(p) >= 6:
        # compact like BTCUSD
        return p.replace("BTC", "XBT")
    ui = p.replace("BTC", "XBT")
    return KRAKEN_PAIR_MAP.get(ui.title().replace("/", "/"), ui.replace("/", ""))

def from_kraken(pair: str) -> str:
    """
    Convert Kraken pair strings (XBTUSD, ETHUSD, XXBTZUSD) to UI 'BASE/QUOTE' (BTC/USD).
    """
    if not pair:
        return pair
    p = str(pair).strip().upper().replace(":", "").replace("/", "")
    inv = {v.upper(): k.upper() for k, v in KRAKEN_PAIR_MAP.items()}
    if p in inv:
        return inv[p]
    # Kraken legacy prefixes (X for base, Z for quote)
    p = p.replace("XXBT", "XBT")
    p = p.replace("XETH", "ETH").replace("ZUSD", "USD").replace("ZEUR", "EUR").replace("ZUSDT", "USDT")
    p = p.replace("XBT", "BTC")
    # Try 4-letter quote first
    for q in ("USDT", "USDC", "DAI", "BUSD"):
        if p.endswith(q):
            base = p[:-len(q)]
            return f"{base}/{q}"
    # Fallback to 3-letter split
    if len(p) >= 6:
        return f"{p[:-3]}/{p[-3:]}"
    return pair

def tf_to_kraken(tf: str) -> str:
    """
    Map common timeframe strings to Kraken API codes.
    """
    m = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "1h": "60",
        "4h": "240",
        "1d": "1440",
    }
    return m.get(str(tf).lower(), str(tf))
