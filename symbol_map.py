from __future__ import annotations

# UI -> Kraken pair map (extend as needed)
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
    Convert UI pair to Kraken pair. Accepts 'BTC/USD' or 'BTCUSD' and returns the Kraken pair code.
    """
    s = str(pair or "").upper().replace(" ", "")
    if "/" not in s and len(s) >= 6:
        # inject slash before USD/USDT if missing, e.g., BTCUSD -> BTC/USD
        if s.endswith("USD"):
            s = s[:-3] + "/" + s[-3:]
        elif s.endswith("USDT"):
            s = s[:-4] + "/" + s[-4:]
    return KRAKEN_PAIR_MAP.get(s, s.replace("/", ""))  # default: strip slash

def from_kraken(pair: str) -> str:
    """
    Convert Kraken pair back to a UI symbol (best-effort).
    Also normalizes common Kraken asset-code variants like:
      - XETHZUSD -> ETH/USD
      - XLTCZUSD -> LTC/USD
      - XXRPZUSD -> XRP/USD
      - XBTUSD   -> BTC/USD
    """
    p = str(pair or "").upper().replace("/", "").replace(" ", "")
    rev = {v: k for k, v in KRAKEN_PAIR_MAP.items()}
    if p in rev:
        return rev[p]

    # Best-effort decode of Kraken asset codes.
    # Example: XXRPZUSD -> base=XXRPZ, quote=USD
    if p.endswith("USD") and len(p) > 3:
        base = p[:-3]
        # Strip leading X/XX and trailing Z patterns that Kraken sometimes uses.
        if base.startswith("XX"):
            base = base[2:]
        elif base.startswith("X"):
            base = base[1:]
        if base.endswith("Z") and len(base) > 1:
            base = base[:-1]
        if base == "XBT":
            base = "BTC"
        # If the base is now a simple alpha ticker, normalize to UI form.
        if base.isalpha():
            return f"{base}/USD"

    # Fallback: return raw
    return p

def tf_to_kraken(tf: str) -> str:
    """
    Map common timeframe strings to Kraken API 'interval' numbers (as strings).
    Accepts: '1Min','5Min','15Min','1m','5m','15m','60','1h','4h','1d','1440'.
    """
    s = str(tf or "").strip()
    s = s.replace("Minute", "Min")
    s_low = s.lower()
    if s_low.endswith("min"):
        try:
            n = int(s_low[:-3])
            s_low = f"{n}m"
        except Exception:
            pass
    m = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "45m": "45",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "1d": "1440",
        "7d": "10080",
    }
    if s_low in m:
        return m[s_low]
    try:
        int(s_low)
        return s_low
    except Exception:
        return s
