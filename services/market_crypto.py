# services/market_crypto.py
# Version: 0.4.0
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
    if any(t.endswith(suf) for suf in ("Min", "H", "D", "W", "M")):
        return t
    return _TIMEFRAME_MAP.get(t.lower(), default)

class MarketCrypto:
    """
    Robust Alpaca crypto data client.

    Primary: v1beta3 /crypto/us/bars with slash pairs (BTC/USD).
    Fallback: v2 /crypto/bars?feed=us (some accounts intermittently return empties on multi-symbol v1beta3).
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
        self._last_used_path: str = "/v1beta3/crypto/us/bars"

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
        return f"{self.data_base}{self._last_used_path}"

    # ---------- core ----------
    def _request_v1beta3(self, symbols: List[str], timeframe: str, limit: int, start: Optional[str]) -> Dict[str, Any]:
        self._last_used_path = "/v1beta3/crypto/us/bars"
        url = f"{self.data_base}{self._last_used_path}"
        params: Dict[str, Any] = {
            "symbols": ",".join(symbols),   # KEEP slashes
            "timeframe": timeframe,
            "limit": str(limit),
        }
        if start:
            params["start"] = start
        self.last_url = requests.Request("GET", url, params=params).prepare().url or url
        r = self.session.get(url, headers=self._headers(), params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    def _request_v2(self, symbols: List[str], timeframe: str, limit: int, start: Optional[str]) -> Dict[str, Any]:
        self._last_used_path = "/v2/crypto/bars"
        url = f"{self.data_base}{self._last_used_path}"
        params: Dict[str, Any] = {
            "symbols": ",".join(symbols),   # KEEP slashes
            "timeframe": timeframe,
            "limit": str(limit),
            "feed": "us",
        }
        if start:
            params["start"] = start
        self.last_url = requests.Request("GET", url, params=params).prepare().url or url
        r = self.session.get(url, headers=self._headers(), params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    def _build_out(self, symbols: List[str], payload: Dict[str, Any]) -> Dict[str, BarResult]:
        data = payload.get("bars") or {}
        out: Dict[str, BarResult] = {}
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

    def candles(self,
                symbols: List[str],
                timeframe: str = "5Min",
                limit: int = 300,
                start: Optional[str] = None,
                **kwargs) -> Dict[str, BarResult]:
        """
        Fetch bars for given symbols (slash pairs) and return {sym: BarResult}.

        Accepts aliases:
          - tf / timeframe: bar size (e.g., '5m', '15m', '1h', or '5Min')
          - bars / limit: number of rows
          - start: ISO8601
        """
        # Alias handling
        tf = kwargs.get("tf")
        if tf and not timeframe:
            timeframe = tf
        timeframe = _normalize_tf(timeframe or tf, default="5Min")

        if "bars" in kwargs and not kwargs.get("limit"):
            try: limit = int(kwargs["bars"])
            except Exception: pass
        if "limit" in kwargs:
            try: limit = int(kwargs["limit"])
            except Exception: pass
        if kwargs.get("start") and not start:
            start = kwargs.get("start")

        self.last_error = None

        # First try v1beta3
        try:
            payload = self._request_v1beta3(symbols, timeframe, limit, start)
            out = self._build_out(symbols, payload)
            if any(len(v.frame) for v in out.values()):
                return out
        except requests.HTTPError as e:
            self.last_error = f"v1beta3 HTTP {getattr(e.response, 'status_code', '?')}: {getattr(e.response, 'text', str(e))}"
        except Exception as e:
            self.last_error = f"v1beta3 error: {str(e)}"

        # Fallback to v2 if v1beta3 was empty or errored
        try:
            payload2 = self._request_v2(symbols, timeframe, limit, start)
            out2 = self._build_out(symbols, payload2)
            return out2
        except requests.HTTPError as e2:
            self.last_error = f"{self.last_error or ''} | v2 HTTP {getattr(e2.response, 'status_code', '?')}: {getattr(e2.response, 'text', str(e2))}".strip()
            # Return empty frames for consistency
            return {sym: BarResult(sym, pd.DataFrame(columns=["ts","open","high","low","close","volume"])) for sym in symbols}
        except Exception as e2:
            self.last_error = f"{self.last_error or ''} | v2 error: {str(e2)}".strip()
            return {sym: BarResult(sym, pd.DataFrame(columns=["ts","open","high","low","close","volume"])) for sym in symbols}
