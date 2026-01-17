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


# Kraken sometimes uses "X"/"Z" prefixed asset codes (e.g., XXBTZUSD, XETHZUSD, XLTCZUSD).
# These helpers normalize those to UI symbols like "BTC/USD" for journaling/review/reporting.
_BASE_MAP = {
    "XBT": "BTC",
    "BTC": "BTC",
    "ETH": "ETH",
    "LTC": "LTC",
    "BCH": "BCH",
    "ADA": "ADA",
    "SOL": "SOL",
    "DOT": "DOT",
    "AVAX": "AVAX",
    "NEAR": "NEAR",
    "SUI": "SUI",
    "XRP": "XRP",
    "XDG": "DOGE",
    "DOGE": "DOGE",
}

_KNOWN_QUOTES = ["USDT", "USD", "EUR", "GBP", "CAD", "AUD", "JPY"]


def _normalize_base(base: str) -> str:
    b = (base or "").upper()
    # Kraken often prefixes crypto assets with leading X (sometimes double X).
    # Strip leading X only if it helps match a known mapping.
    while b.startswith("X") and len(b) > 3:
        cand = b[1:]
        if cand in _BASE_MAP:
            b = cand
        else:
            break
    return _BASE_MAP.get(b, b)


def _normalize_quote(quote: str) -> str:
    q = (quote or "").upper()
    # Kraken sometimes prefixes fiat quotes with Z (e.g., ZUSD, ZEUR)
    if q.startswith("Z") and len(q) > 1:
        q = q[1:]
    return q


def kraken_pair_to_ui(pair: str) -> str:
    """Best-effort conversion of Kraken pair codes to UI symbols (e.g., XLTCZUSD -> LTC/USD)."""
    p = str(pair or "").upper().replace(" ", "").replace("/", "")
    if not p:
        return p

    # Prefer longest quote match (e.g., USDT before USD)
    for q in sorted(_KNOWN_QUOTES, key=len, reverse=True):
        # Handle Z-prefixed quotes first (e.g., ZUSD)
        if p.endswith("Z" + q):
            base = p[:-(len(q) + 1)]
            return f"{_normalize_base(base)}/{_normalize_quote(q)}"
        if p.endswith(q):
            base = p[:-len(q)]
            return f"{_normalize_base(base)}/{_normalize_quote(q)}"

    return p

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
    """Convert Kraken pair back to a UI symbol (best-effort)."""
    p = str(pair or "").upper()
    rev = {v: k for k, v in KRAKEN_PAIR_MAP.items()}
    # Fast path for known explicit mappings
    if p in rev:
        return rev[p]
    # Otherwise, normalize common Kraken pair formats (e.g., XXBTZUSD, XETHZUSD, XLTCZUSD).
    return kraken_pair_to_ui(p)

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
