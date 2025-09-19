# services/market_crypto.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Iterable
from datetime import datetime, timezone, timedelta
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

RAW_BASE    = _env("CRYPTO_DATA_BASE", _env("ALPACA_DATA_BASE", "https://data.alpaca.markets"))
DEFAULT_SCOPE = "v1beta3/crypto/us"

DEFAULT_TIMEFRAME = _env("CRYPTO_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(_env("CRYPTO_BARS_LIMIT", "500") or "500")
DEFAULT_FEED = _env("CRYPTO_FEED", "us")  # force 'us' unless explicitly overridden


# ---- Helpers for symbol normalization ---------------------------------------

def _to_data_sym(sym: str) -> str:
    s = (sym or "").strip().upper()
    return s.replace("/", "") if "/" in s else s

def _from_data_sym(sym: str) -> str:
    s = (sym or "").strip().upper()
    if "/" in s or len(s) < 6:
        return s
    return f"{s[:3]}/{s[3:]}"


@dataclass
class BarsResult:
    symbol: str
    frame: "pd.DataFrame"  # columns: o,h,l,c,v ; index: datetime (UTC)


class MarketCrypto:
    """
    Alpaca crypto bars client
      • Only uses v1beta3/crypto/us/bars (v2 crypto endpoint does not exist)
      • Symbol translation BTC/USD <-> BTCUSD
      • feed=us and a sane default start window (now-48h) to ensure data
    """
    __version__ = "1.1.1"

    def __init__(
        self,
        data_base: Optional[str] = None,
        data_scope: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        session: Optional[requests.Session] = None,
        default_timeframe: str = DEFAULT_TIMEFRAME,
        default_limit: int = DEFAULT_LIMIT,
        default_feed: Optional[str] = DEFAULT_FEED,
    ):
        raw_base = (data_base or RAW_BASE or "https://data.alpaca.markets").rstrip("/")

        # Always v1beta3/crypto/us
        lowered = raw_base.lower()
        if "v1beta3/crypto" in lowered:
            self.data_base = raw_base
            self.data_scope = ""
        else:
            self.data_base = raw_base
            self.data_scope = (data_scope or DEFAULT_SCOPE).strip("/")

        self.key = api_key or APCA_KEY
        self.secret = api_secret or APCA_SECRET
        self.s = session or requests.Session()
        self.default_timeframe = default_timeframe
        self.default_limit = default_limit
        self.default_feed = default_feed or "us"
        self.last_error: Optional[str] = None
        self.last_url: Optional[str] = None

        if not self.key or not self.secret:
            raise RuntimeError("MarketCrypto: missing CRYPTO_API_KEY/CRYPTO_API_SECRET (or APCA_API_KEY_ID/APCA_API_SECRET_KEY)")

    # Factory used by app.py
    @classmethod
    def from_env(cls) -> "MarketCrypto":
        return cls()

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _hdrs(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.key or "",
            "APCA-API-SECRET-KEY": self.secret or "",
            "Accept": "application/json",
        }

    def _bars_url(self) -> str:
        if self.data_scope:
            return f"{self.data_base}/{self.data_scope}/bars"
        return f"{self.data_base}/bars"

    # ---- Normalization -------------------------------------------------------

    @staticmethod
    def _df_from_rows(rows: List[Dict[str, Any]]) -> "pd.DataFrame":
        if pd is None:
            return rows  # type: ignore
        df = pd.DataFrame(rows or [])
        if df.empty:
            return pd.DataFrame(columns=["o","h","l","c","v"]).astype({"o":"float64","h":"float64","l":"float64","c":"float64","v":"float64"})
        ts_key = "t" if "t" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
        if ts_key:
            df["ts"] = pd.to_datetime(df[ts_key], utc=True)
        else:
            df["ts"] = pd.to_datetime("now", utc=True)
        for col in ("o","h","l","c","v"):
            if col not in df.columns:
                df[col] = None
        df = df[["ts","o","h","l","c","v"]].sort_values("ts").set_index("ts")
        return df

    @staticmethod
    def _unpack_response(json_obj: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        bars = (json_obj or {}).get("bars")
        if isinstance(bars, dict):
            return {k: (v or []) for k, v in bars.items()}
        if isinstance(bars, list):
            out: Dict[str, List[Dict[str, Any]]] = {}
            for row in bars:
                sym = row.get("S") or row.get("symbol") or row.get("sym") or "UNKNOWN"
                out.setdefault(sym, []).append(row)
            return out
        return {}

    def _fetch_json(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.last_url = f"{url}?{requests.compat.urlencode(params)}"
        try:
            r = self.s.get(url, headers=self._hdrs(), params=params, timeout=30)
            if r.status_code >= 400:
                try:
                    self.last_error = f"HTTP {r.status_code}: {r.json()}"
                except Exception:
                    self.last_error = f"HTTP {r.status_code}: {r.text}"
                return None
            try:
                return r.json()
            except Exception as e:
                self.last_error = f"JSON decode error: {e}"
                return None
        except Exception as e:
            self.last_error = str(e)
            return None

    # ---- Core fetch ----------------------------------------------------------

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
        req_syms = [s.strip() for s in symbols if s and str(s).strip()]
        if not req_syms:
            return {}

        data_syms = [_to_data_sym(s) for s in req_syms]

        # Default start = now-48h to ensure non-empty results
        if not start:
            start_dt = self.now_utc() - timedelta(hours=48)
            # RFC3339 with Z
            start = start_dt.isoformat().replace("+00:00", "Z")

        params: Dict[str, Any] = {
            "symbols": ",".join(data_syms),
            "timeframe": timeframe or self.default_timeframe,
            "limit": str(limit or self.default_limit),
            "adjustment": adjustment,
            "feed": feed or self.default_feed or "us",
            "start": start,
        }
        if end:
            params["end"] = end

        url = self._bars_url()
        data = self._fetch_json(url, params)

        out: Dict[str, BarsResult] = {}
        if data is None:
            # return empty frames but keep last_error/last_url for diagnostics
            for sym in req_syms:
                if pd is None:
                    out[sym] = BarsResult(sym, [])  # type: ignore
                else:
                    out[sym] = BarsResult(sym, pd.DataFrame(columns=["o","h","l","c","v"]))
            return out

        bars_map = self._unpack_response(data)
        rev_map = { _to_data_sym(req): req for req in req_syms }

        for key, rows in (bars_map or {}).items():
            req_sym = rev_map.get(key) or rev_map.get(_to_data_sym(key)) or _from_data_sym(key)
            frame = self._df_from_rows(rows or [])
            out[req_sym] = BarsResult(symbol=req_sym, frame=frame)

        for sym in req_syms:
            if sym not in out:
                if pd is None:
                    out[sym] = BarsResult(sym, [])  # type: ignore
                else:
                    out[sym] = BarsResult(sym, pd.DataFrame(columns=["o","h","l","c","v"]))
        return out

    def last_price(self, symbol: str) -> Optional[float]:
        res = self.candles([symbol], limit=1)
        br = res.get(symbol)
        if not br:
            return None
        frame = br.frame
        if pd is None:
            try:
                return float(frame[-1].get("c")) if frame else None  # type: ignore
            except Exception:
                return None
        if frame is None or len(frame) == 0:
            return None
        return float(frame["c"].iloc[-1])
