# services/market_crypto.py
# Version: 0.3.0
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

class MarketCrypto:
    """
    Minimal Alpaca crypto data client focused on v1beta3 crypto/us bars.
    - Symbols MUST be slash pairs like 'BTC/USD'.
    - Do NOT pass unsupported params (feed, adjustment) to v1beta3.
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
                start: Optional[str] = None) -> Dict[str, BarResult]:
        """
        Fetch recent bars for given symbols (slash pairs) and return dict of BarResult.
        """
        self.last_error = None
        # IMPORTANT: keep slash symbols ('BTC/USD') – requests will URL-encode them as %2F
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

        for sym, rows in data.items():
            # rows: list of dicts with keys: t,o,h,l,c,v, etc.
            if not isinstance(rows, list) or not rows:
                out[sym] = BarResult(sym, pd.DataFrame(columns=["ts","open","high","low","close","volume"]))
                continue
            df = pd.DataFrame(rows)
            # Normalize columns
            # Alpaca returns 't' ISO timestamp
            df = df.rename(columns={"t":"ts","o":"open","h":"high","l":"low","c":"close","v":"volume"})
            # Keep only relevant cols if present
            keep = [c for c in ["ts","open","high","low","close","volume"] if c in df.columns]
            df = df[keep].copy()
            # Ensure proper types
            if "ts" in df.columns:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
            for col in ["open","high","low","close","volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            out[sym] = BarResult(sym, df)

        return out
