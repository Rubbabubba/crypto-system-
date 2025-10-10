# strategies/symbol_map.py
KRAKEN_PAIR_MAP = {
    "BTC/USD": "XBTUSD",
    "ETH/USD": "ETHUSD",
    "SOL/USD": "SOLUSD",
    "DOGE/USD": "DOGEUSD",
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
def to_kraken(sym: str) -> str:
    return KRAKEN_PAIR_MAP.get(sym, sym.replace("/",""))
def tf_to_kraken(tf: str) -> int:
    tf = tf.lower()
    return {"1min":1,"5min":5,"15min":15,"30min":30,"60min":60,"4h":240,"240min":240,"1d":1440}.get(tf,5)
