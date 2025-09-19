# services/market_crypto.py
# ---------------------------------------------------------------------
# MarketCrypto — Alpaca Crypto Data (v1beta3) client
# Version: 1.2.0
#
# - Primary method: candles(symbols, timeframe, limit, start=None, end=None)
# - Backward-compat method: get_bars(...) -> calls candles(...)
# - Normalizes output to: dict[str -> pandas.DataFrame]
#   Each DataFrame has columns: ['ts','open','high','low','close','volume']
# - Diagnostics (for /diag/candles):
#     .diag_snapshot() returns:
#       {
#         "last_attempts": [<urls tried in order>],
#         "last_error": str|None,
#         "last_url": str|None,     # final successful or last attempted URL
#         "limit": int,
#         "rows": {symbol: rowcount, ...},
#         "symbols": [ ... ],
#         "timeframe": "5Min" (normalized)
#       }
#
# Environment variables (used by from_env()):
#   ALPACA_KEY_ID
#   ALPACA_SECRET_KEY
#   DATA_BASE (default: "https://data.alpaca.markets")
#
# Notes:
# - Uses Alpaca v1beta3 endpoint:
#     /v1beta3/crypto/us/bars?symbols=...&timeframe=...&limit=...
# - Fetch strategy:
#     1) Try batch (multi-symbol) request
#     2) For any symbol missing/empty, retry per-symbol requests
# - Timeframes are normalized to Alpaca style (e.g., "5m" -> "5Min")
# ---------------------------------------------------------------------

from __future__ import annotations

import os
import json
import time
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd


__all__ = ["MarketCrypto"]
__version__ = "1.2.0"


# ------------------------------- Helpers ------------------------------------ #

_TIMEFRAME_MAP = {
    # minutes
    "1": "1Min", "1m": "1Min", "1min": "1Min", "1MIN": "1Min",
    "3m": "3Min", "5m": "5Min", "15m": "15Min", "30m": "30Min",
    "45m": "45Min",
    # hours
    "1h": "1Hour", "2h": "2Hour", "4h": "4Hour", "6h": "6Hour", "8h": "8Hour",
    "12h": "12Hour",
    # days
    "1d": "1Day", "day": "1Day", "1D": "1Day",
    # already-in-Alpaca style should pass through unchanged
}

def _normalize_timeframe(tf: str) -> str:
    if not tf:
        return "5Min"
    key = tf.strip()
    return _TIMEFRAME_MAP.get(key, key)

def _ensure_list_symbols(symbols) -> List[str]:
    if isinstance(symbols, str):
        # Allow comma-separated string
        parts = [s.strip() for s in symbols.split(",") if s.strip()]
        return parts
    return list(symbols or [])

def _bars_json_to_df(bars: List[dict]) -> pd.DataFrame:
    """
    Convert a list of bar dicts from Alpaca v1beta3 to a normalized DataFrame.
    Expected keys in each bar item: t, o, h, l, c, v
      - t: ISO 8601 timestamp
      - o/h/l/c/v: floats/ints
    """
    if not bars:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(bars)
    # Rename keys if present; guard if some keys are missing
    rename_map = {
        "t": "ts",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # Coerce dtypes
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sort by time just in case
    if "ts" in df.columns:
        df.sort_values("ts", inplace=True)

    # Final column order
    cols = ["ts", "open", "high", "low", "close", "volume"]
    # Ensure all expected columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = pd.Series(dtype="float64") if c != "ts" else pd.Series(dtype="datetime64[ns, UTC]")
    return df[cols].reset_index(drop=True)


# ------------------------------- Class -------------------------------------- #

class MarketCrypto:
    """
    Simple Alpaca Crypto market data client (v1beta3), optimized for your strategies.

    Use:
      market = MarketCrypto.from_env()
      data = market.candles(["BTC/USD","ETH/USD"], timeframe="5Min", limit=600)
      # data is dict[str -> DataFrame(ts, open, high, low, close, volume)]
    """

    VERSION = __version__

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        data_base: str = "https://data.alpaca.markets",
        session: Optional[requests.Session] = None,
        default_feed: str = "us",  # reserved for future use; v1beta3 crypto US route includes feed implicitly
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.data_base = data_base.rstrip("/")
        self.session = session or requests.Session()
        self.default_feed = default_feed

        # Diagnostics ring
        self._diag_last_attempts: List[str] = []
        self._diag_last_error: Optional[str] = None
        self._diag_last_url: Optional[str] = None
        self._diag_last_rows: Dict[str, int] = {}
        self._diag_last_tf: Optional[str] = None
        self._diag_last_limit: Optional[int] = None
        self._diag_last_symbols: List[str] = []

    # -------------------------- Construction helpers ------------------------ #

    @classmethod
    def from_env(cls) -> "MarketCrypto":
        api_key = os.getenv("ALPACA_KEY_ID", "").strip()
        api_secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
        data_base = os.getenv("DATA_BASE", "https://data.alpaca.markets").strip()
        if not api_key or not api_secret:
            raise RuntimeError("Missing ALPACA_KEY_ID / ALPACA_SECRET_KEY in environment.")
        return cls(api_key=api_key, api_secret=api_secret, data_base=data_base)

    # -------------------------- Diagnostics access -------------------------- #

    def diag_snapshot(self) -> dict:
        """
        Return the latest candle fetch diagnostics in a JSON-safe dict.
        """
        return {
            "last_attempts": list(self._diag_last_attempts),
            "last_error": self._diag_last_error,
            "last_url": self._diag_last_url,
            "limit": self._diag_last_limit,
            "rows": dict(self._diag_last_rows),
            "symbols": list(self._diag_last_symbols),
            "timeframe": self._diag_last_tf,
        }

    # -------------------------- Core data fetching -------------------------- #

    def candles(
        self,
        symbols: List[str] | str,
        timeframe: str = "5Min",
        limit: int = 600,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch crypto candles for one or more symbols from Alpaca v1beta3.

        Returns:
          dict: { "BTC/USD": DataFrame, "ETH/USD": DataFrame, ... }
        """
        syms = _ensure_list_symbols(symbols)
        tf = _normalize_timeframe(timeframe)
        lim = int(limit)

        # Reset diagnostics
        self._diag_last_attempts = []
        self._diag_last_error = None
        self._diag_last_url = None
        self._diag_last_rows = {s: 0 for s in syms}
        self._diag_last_tf = tf
        self._diag_last_limit = lim
        self._diag_last_symbols = list(syms)

        if not syms:
            self._diag_last_error = "no_symbols"
            return {}

        # Headers
        headers = {
            "Apca-Api-Key-Id": self.api_key,
            "Apca-Api-Secret-Key": self.api_secret,
            "Accept": "application/json",
        }

        base_url = f"{self.data_base}/v1beta3/crypto/us/bars"

        # 1) Try batch (multi-symbol) request first
        joined = ",".join(syms)
        params = {"symbols": joined, "timeframe": tf, "limit": lim}
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        batch_url = f"{base_url}?symbols={requests.utils.quote(joined)}&timeframe={tf}&limit={lim}"
        if start:
            batch_url += f"&start={requests.utils.quote(start)}"
        if end:
            batch_url += f"&end={requests.utils.quote(end)}"

        self._diag_last_attempts.append(batch_url)
        data_map: Dict[str, pd.DataFrame] = {s: pd.DataFrame() for s in syms}

        try:
            r = self.session.get(base_url, headers=headers, params=params, timeout=20)
            self._diag_last_url = batch_url
            if r.status_code == 200:
                payload = r.json()
                bars = payload.get("bars", {}) if isinstance(payload, dict) else {}
                # bars is expected to be a dict: symbol -> list[bar]
                for sym in syms:
                    sym_bars = bars.get(sym) or []
                    df = _bars_json_to_df(sym_bars)
                    self._diag_last_rows[sym] = int(len(df))
                    data_map[sym] = df
            else:
                self._diag_last_error = f"HTTP {r.status_code}: {r.text}"
        except Exception as ex:
            self._diag_last_error = f"{type(ex).__name__}: {ex}"

        # 2) Per-symbol fallback for any empty df
        for sym in syms:
            if len(data_map[sym]) > 0:
                continue  # already have data

            single_params = {"symbols": sym, "timeframe": tf, "limit": lim}
            if start:
                single_params["start"] = start
            if end:
                single_params["end"] = end

            single_url = f"{base_url}?symbols={requests.utils.quote(sym)}&timeframe={tf}&limit={lim}"
            if start:
                single_url += f"&start={requests.utils.quote(start)}"
            if end:
                single_url += f"&end={requests.utils.quote(end)}"

            self._diag_last_attempts.append(single_url)
            try:
                r = self.session.get(base_url, headers=headers, params=single_params, timeout=20)
                self._diag_last_url = single_url
                if r.status_code == 200:
                    payload = r.json()
                    bars = payload.get("bars", {})
                    sym_bars = bars.get(sym) or []
                    df = _bars_json_to_df(sym_bars)
                    self._diag_last_rows[sym] = int(len(df))
                    data_map[sym] = df
                else:
                    # Keep last_error but continue other symbols
                    self._diag_last_error = (self._diag_last_error or "") + \
                        (f" | single {sym} HTTP {r.status_code}: {r.text}")
            except Exception as ex:
                self._diag_last_error = (self._diag_last_error or "") + f" | single {sym} {type(ex).__name__}: {ex}"

        return data_map

    # -------------------------- Backwards compatibility --------------------- #

    def get_bars(
        self,
        symbols: List[str] | str,
        timeframe: str = "5Min",
        limit: int = 600,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Compatibility wrapper that mirrors older strategies.
        Delegates to candles(...).
        """
        return self.candles(symbols=symbols, timeframe=timeframe, limit=limit, start=start, end=end)
