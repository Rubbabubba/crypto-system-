# services/market_crypto.py
# Version: 0.3.1
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

import requests
import pandas as pd

def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v if v is not None else default

@dataclass
class BarResult:
    symbol: str
    frame: pd.DataFrame  # columns: ts, open, high, low, close, volume

_TIMEFRAME_MAP = {
    # minutes
    "1m": "1Min", "3m": "3Min", "5m": "5Min", "15m": "15Min", "30m": "30Min",
    # hours
    "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "8h": "8H", "12h": "12H",
    # days/weeks/months
    "1d": "1D", "1w": "1W", "1mo": "1M",
}
def _normalize_tf(tf: str | None, default: str = "5Min") -> str:
    if not tf:
        return default
    t = tf.strip()
    # already Alpaca-style?
    if any(t.endswith(suf) for suf in ("Min", "H", "D", "W", "M")):
        return t
    return _TIMEFRAME_MAP.get(t.lower(), default)

class MarketCrypto:
    """
    Minimal Alpaca crypto data client focused on v1beta3 crypto/us bars.
    - Symbols MUST be slash pairs like 'BTC/USD' (requests will URL-encode the slash).
    - Do NOT pass unsupported params (feed, adjustment) to v1beta3.
    - Accepts alias kwargs your strategies might send (tf, timeframe, bars, limit, start).
    """

    def __init__(self,
                 data_base: Optional[str] = None,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 session: Optional[requests.Session] = None) -> None:
        self.data_base = data_base or _env("CRYPTO_DATA_BASE") or _env("ALPACA_DATA_BASE") or "https://data.alpaca.markets"
        self.api_key = api_key or _env("CRYPTO_API_KEY") or _env("APCA_API_KEY_ID") or ""
        self.api_secret = api_secret or _env("CRYPTO_API_SECRET") or _env("APCA_API_SECRET_KEY") or ""
        self.session = session or requests.Session()
        self.last_url: str = ""
        self.last_error: Optional[str] = None

    @classmethod
    def from_env(cls) -> "MarketCrypto":
        return cls()

    def _headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Accept": "application/json",
        }

    def _bars_url(self) -> str:
        # v1beta3 path (US crypto)
        return f"{self.data_base}/v1beta3/crypto/us/bars"

    def candles(self,
                symbols: List[str],
                timeframe: str = "5Min",
                limit: int = 300,
                start: Optional[str] = None,
                **kwargs) -> Dict[str, BarResult]:
        """
        Fetch recent bars for given symbols (slash pairs) and return dict of BarResult.

        Accepts aliases via kwargs:
          - tf / timeframe: bar size (e.g., '5m', '15m', '1h', or '5Min')
          - bars / limit: number of rows
          - start: ISO8601
        """
        # Aliases from strategy calls
        tf = kwargs.get("tf")
        if tf and not timeframe:
            timeframe = tf
        timeframe = _normalize_tf(timeframe or tf, default="5Min")

        if "bars" in kwargs and not kwargs.get("limit"):
            try:
                limit = int(kwargs["bars"])
            except Exception:
                pass
        if "limit" in kwargs:
            try:
                limit = int(kwargs["limit"])
            except Exception:
                pass
        if kwargs.get("start") and not start:
            start = kwargs.get("start")

        self.last_error = None

        # IMPORTANT: keep slash symbols ('BTC/USD')
        syms_param = ",".join(symbols)
        params: Dict[str, Any] = {
            "symbols": syms_param,
            "timeframe": timeframe,
            "limit": str(limit),
        }
        if start:
            params["start"] = start

        url = self._bars_url()
        self.last_url = requests.Request("GET", url, params=params).prepare().url or url  # for diag
        try:
            r = self.session.get(url, headers=self._headers(), params=params, timeout=20)
            r.raise_for_status()
        except requests.HTTPError as e:
            self.last_error = f"HTTP {r.status_code}: {r.text}" if 'r' in locals() else str(e)
            raise
        except Exception as e:
            self.last_error = str(e)
            raise

        j = r.json()
        data = j.get("bars") or {}
        out: Dict[str, BarResult] = {}

        # Ensure every requested symbol appears in output, even if empty
        for sym in symbols:
            rows = data.get(sym)
            if not isinstance(rows, list) or not rows:
                out[sym] = BarResult(sym, pd.DataFrame(columns=["ts","open","high","low","close","volume"]))
                continue
            df = pd.DataFrame(rows)
            df = df.rename(columns={"t":"ts","o":"open","h":"high","l":"low","c":"close","v":"volume"})
            keep = [c for c in ["ts","open","high","low","close","volume"] if c in df.columns]
            df = df[keep].copy()
            if "ts" in df.columns:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
            for col in ["open","high","low","close","volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            out[sym] = BarResult(sym, df)

        return out
