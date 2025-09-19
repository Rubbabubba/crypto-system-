# services/market_crypto.py
# Version: 0.4.2
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
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

    Order of attempts:
      1) v1beta3 multi-symbol
      2) v2 multi-symbol (feed=us)
      3) per-symbol fan-out: v1beta3 then v2 for each *empty* symbol
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
        self.last_attempts: List[str] = []

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

    # ---------- HTTP helpers ----------
    def _prep(self, path: str, params: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
        url = f"{self.data_base}{path}"
        prepared = requests.Request("GET", url, params=params).prepare()
        full = prepared.url or url
        return url, params, full

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url, params, full = self._prep(path, params)
        self._last_used_path = path
        self.last_url = full
        self.last_attempts.append(full)
        r = self.session.get(url, headers=self._headers(), params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    def _request_v1beta3(self, symbols: List[str], timeframe: str, limit: int, start: Optional[str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbols": ",".join(symbols), "timeframe": timeframe, "limit": str(limit)}
        if start: params["start"] = start
        return self._request("/v1beta3/crypto/us/bars", params)

    def _request_v2(self, symbols: List[str], timeframe: str, limit: int, start: Optional[str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbols": ",".join(symbols), "timeframe": timeframe, "limit": str(limit), "feed": "us"}
        if start: params["start"] = start
        return self._request("/v2/crypto/bars", params)

    # ---------- payload → frames ----------
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

    def _empty_symbols(self, out: Dict[str, BarResult]) -> List[str]:
        return [s for s, br in out.items() if len(br.frame) == 0]

    # ---------- public ----------
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
        # Reset attempt log per call
        self.last_attempts = []
        self.last_error = None

        # Aliases
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

        # 1) Try multi v1beta3
        out: Dict[str, BarResult] = {}
        try:
            payload = self._request_v1beta3(symbols, timeframe, limit, start)
            out = self._build_out(symbols, payload)
            missing = self._empty_symbols(out)
            if not missing:
                return out
        except requests.HTTPError as e:
            self.last_error = f"v1beta3 HTTP {getattr(e.response, 'status_code', '?')}: {getattr(e.response, 'text', str(e))}"
            missing = symbols[:]  # treat as all missing
            out = {s: BarResult(s, pd.DataFrame(columns=["ts","open","high","low","close","volume"])) for s in symbols}
        except Exception as e:
            self.last_error = f"v1beta3 error: {str(e)}"
            missing = symbols[:]
            out = {s: BarResult(s, pd.DataFrame(columns=["ts","open","high","low","close","volume"])) for s in symbols}

        # 2) Try multi v2 for any missing
        if missing:
            try:
                payload2 = self._request_v2(missing, timeframe, limit, start)
                out2 = self._build_out(missing, payload2)
                for s in missing:
                    if len(out2.get(s, BarResult(s, pd.DataFrame())).frame) > 0:
                        out[s] = out2[s]
                missing = self._empty_symbols(out)
            except requests.HTTPError as e2:
                self.last_error = f"{self.last_error or ''} | v2 HTTP {getattr(e2.response, 'status_code', '?')}: {getattr(e2.response, 'text', str(e2))}".strip()
            except Exception as e2:
                self.last_error = f"{self.last_error or ''} | v2 error: {str(e2)}".strip()

        # 3) Per-symbol fan-out (v1beta3 then v2) for any still-missing
        for sym in list(missing):
            # v1beta3 single
            try:
                p1 = self._request_v1beta3([sym], timeframe, limit, start)
                r1 = self._build_out([sym], p1)
                if len(r1[sym].frame) > 0:
                    out[sym] = r1[sym]
                    continue
            except Exception:
                pass
            # v2 single
            try:
                p2 = self._request_v2([sym], timeframe, limit, start)
                r2 = self._build_out([sym], p2)
                out[sym] = r2[sym]
            except Exception:
                out[sym] = BarResult(sym, pd.DataFrame(columns=["ts","open","high","low","close","volume"]))

        return out
