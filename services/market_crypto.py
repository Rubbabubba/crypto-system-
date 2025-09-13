# services/market_crypto.py
import os, time, requests, datetime as dt
import pandas as pd

class MarketCrypto:
    def __init__(self, api_key=None, api_secret=None, data_base=None):
        self.api_key = api_key or os.getenv("CRYPTO_API_KEY")
        self.api_secret = api_secret or os.getenv("CRYPTO_API_SECRET")
        self.data_base = (data_base or os.getenv("CRYPTO_DATA_BASE_URL", "")).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        })

    @staticmethod
    def _now_iso():
        return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

    def get_bars(self, symbol: str, timeframe: str = "5Min", limit: int = 300) -> pd.DataFrame:
        """
        symbol: like 'BTC/USD'
        timeframe: 1Min, 5Min, 15Min, 1Hour, 1Day
        """
        url = f"{self.data_base}/bars"
        params = {
            "symbols": symbol,
            "timeframe": timeframe,
            "limit": limit,
            # Optional start/end; Alpaca paginates if needed.
        }
        r = self.session.get(url, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        raw = js.get("bars", {}).get(symbol, [])
        if not raw:
            return pd.DataFrame()
        df = pd.DataFrame(raw)
        # Standardize columns
        # Alpaca v1beta3 returns: t (time), o,h,l,c,v, n (num trades), vw (vwap)
        df.rename(columns={"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume","vw":"vwap"}, inplace=True)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df.set_index("time", inplace=True)
        return df

    def last_price(self, symbol: str) -> float:
        bars = self.get_bars(symbol, timeframe="1Min", limit=1)
        if bars.empty: return float("nan")
        return float(bars["close"].iloc[-1])
