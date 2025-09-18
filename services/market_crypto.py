# services/market_crypto.py
from __future__ import annotations
import os
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Iterable
from datetime import datetime, timezone
import requests

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

# ---- Env helpers -------------------------------------------------------------

def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(key)
    return v if (v is not None and str(v).strip() != "") else default

APCA_KEY    = _env("CRYPTO_API_KEY", _env("APCA_API_KEY_ID"))
APCA_SECRET = _env("CRYPTO_API_SECRET", _env("APCA_API_SECRET_KEY"))
DATA_BASE   = _env("CRYPTO_DATA_BASE", _env("ALPACA_DATA_BASE", "https://data.alpaca.markets"))
DATA_SCOPE  = _env("CRYPTO_DATA_SCOPE", "v1beta3/crypto/us")  # path piece after base

DEFAULT_TIMEFRAME = _env("CRYPTO_TIMEFRAME", "5Min")  # v1beta3 uses "1Min","5Min","15Min","1Hour","1Day", etc.
DEFAULT_LIMIT = int(_env("CRYPTO_BARS_LIMIT", "500") or "500")


@dataclass
class BarsResult:
    """Container for one symbol worth of bars."""
    symbol: str
    frame: "pd.DataFrame"  # columns: o,h,l,c,v ; index: datetime (UTC)


class MarketCrypto:
    """
    Thin wrapper around Alpaca v1beta3 Crypto bars endpoint.

    Endpoints:
      GET {DATA_BASE}/{DATA_SCOPE}/bars?symbols=BTC/USD,ETH/USD&timeframe=5Min&limit=300

    Methods exposed:
      - candles(symbols: Iterable[str], timeframe="5Min", limit=300) -> Dict[str, BarsResult]
      - last_price(symbol: str) -> Optional[float]
      - now_utc() -> datetime
    """

    def __init__(
        self,
        data_base: Optional[str] = None,
        data_scope: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        session: Optional[requests.Session] = None,
        default_timeframe: str = DEFAULT_TIMEFRAME,
        default_limit: int = DEFAULT_LIMIT,
    ):
        self.data_base = (data_base or DATA_BASE or "https://data.alpaca.markets").rstrip("/")
        self.data_scope = (data_scope or DATA_SCOPE or "v1beta3/crypto/us").strip("/")
        self.key = api_key or APCA_KEY
        self.secret = api_secret or APCA_SECRET
        self.s = session or requests.Session()
        self.default_timeframe = default_timeframe
        self.default_limit = default_limit

        if not self.key or not self.secret:
            raise RuntimeError("MarketCrypto: missing CRYPTO_API_KEY/CRYPTO_API_SECRET (or APCA_API_KEY_ID/APCA_API_SECRET_KEY)")

    # Factory expected by app.py
    @classmethod
    def from_env(cls) -> "MarketCrypto":
        return cls()

    # Basic utilities
    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _hdrs(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.key or "",
            "APCA-API-SECRET-KEY": self.secret or "",
        }

    def _bars_url(self) -> str:
        return f"{self.data_base}/{self.data_scope}/bars"

    def candles(
        self,
        symbols: Iterable[str],
        timeframe: Optional[str] = None,
        limit: Optional[int] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        adjustment: str = "raw",
        feed: Optional[str] = None,
    ) -> Dict[str, BarsResult]:
        """
        Fetch OHLCV bars for 1..N symbols.
        Returns a dict symbol -> BarsResult(pd.DataFrame with columns o,h,l,c,v, ts (UTC))
        """
        sym_list = [s.strip() for s in symbols if s and str(s).strip()]
        if not sym_list:
            return {}

        params: Dict[str, Any] = {
            "symbols": ",".join(sym_list),
            "timeframe": timeframe or self.default_timeframe,
            "limit": str(limit or self.default_limit),
            "adjustment": adjustment,
        }
        if start: params["start"] = start
        if end: params["end"] = end
        if feed: params["feed"] = feed

        r = self.s.get(self._bars_url(), headers=self._hdrs(), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()  # expected: {"bars": { "BTC/USD": [ { "t": "...", "o":..., "h":..., "l":..., "c":..., "v":...}, ... ]}}

        out: Dict[str, BarsResult] = {}
        bars_map = (data or {}).get("bars") or {}
        for sym, rows in bars_map.items():
            # Build DataFrame (fall back to list-of-dicts if pandas missing)
            if pd is None:
                # Minimal structure w/o pandas
                frame = rows  # type: ignore
                out[sym] = BarsResult(symbol=sym, frame=frame)  # type: ignore
                continue

            df = pd.DataFrame(rows or [])
            if not df.empty:
                # Normalize columns
                if "t" in df.columns:
                    df["ts"] = pd.to_datetime(df["t"], utc=True)
                    df = df.drop(columns=["t"])
                else:
                    # Some feeds might return "timestamp"
                    if "timestamp" in df.columns:
                        df["ts"] = pd.to_datetime(df["timestamp"], utc=True)
                        df = df.drop(columns=["timestamp"])
                # Ensure standard column names exist; fill if missing
                for col in ("o","h","l","c","v"):
                    if col not in df.columns:
                        df[col] = None
                df = df[["ts","o","h","l","c","v"]].sort_values("ts").set_index("ts")
            else:
                # Create empty frame with the right columns
                df = pd.DataFrame(columns=["o","h","l","c","v"])
            out[sym] = BarsResult(symbol=sym, frame=df)
        return out

    def last_price(self, symbol: str) -> Optional[float]:
        res = self.candles([symbol], limit=1)
        br = res.get(symbol)
        if not br:
            return None
        frame = br.frame
        if pd is None:
            # list mode
            return (frame[-1].get("c") if frame else None)  # type: ignore
        if frame is None or len(frame) == 0:
            return None
        return float(frame["c"].iloc[-1])
