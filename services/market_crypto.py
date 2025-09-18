# services/market_crypto.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Iterable, Tuple
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

# May be the host only OR already include the venue scope.
RAW_BASE    = _env("CRYPTO_DATA_BASE", _env("ALPACA_DATA_BASE", "https://data.alpaca.markets"))
DEFAULT_SCOPE = "v1beta3/crypto/us"

DEFAULT_TIMEFRAME = _env("CRYPTO_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(_env("CRYPTO_BARS_LIMIT", "500") or "500")

# Optional default feed for crypto. If not set, we’ll add feed=us on retry only.
DEFAULT_FEED = _env("CRYPTO_FEED", None)


@dataclass
class BarsResult:
    """Container for one symbol worth of bars."""
    symbol: str
    frame: "pd.DataFrame"  # columns: o,h,l,c,v ; index: datetime (UTC)


class MarketCrypto:
    """
    Thin wrapper around Alpaca Crypto bars.

    Resolution order:
      1) <base>/bars                               (if base already includes v1beta3/crypto/<venue>)
      2) <base>/<scope>/bars                       (if base is just the host)
      * On HTTP 4xx, retry adding feed=us
      * If still failing, fallback to <host>/v2/crypto/bars (compat)
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
        default_feed: Optional[str] = DEFAULT_FEED,
    ):
        raw_base = (data_base or RAW_BASE or "https://data.alpaca.markets").rstrip("/")

        # Detect if base already contains v1beta3/crypto
        lowered = raw_base.lower()
        if "v1beta3/crypto" in lowered:
            self.data_base = raw_base      # full path already; no separate scope
            self.data_scope = ""           # <- IMPORTANT
        else:
            self.data_base = raw_base
            self.data_scope = (data_scope or DEFAULT_SCOPE).strip("/")

        self.key = api_key or APCA_KEY
        self.secret = api_secret or APCA_SECRET
        self.s = session or requests.Session()
        self.default_timeframe = default_timeframe
        self.default_limit = default_limit
        self.default_feed = default_feed
        self.last_error: Optional[str] = None
        self.last_url: Optional[str] = None

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
            "Accept": "application/json",
        }

    def _bars_url(self) -> str:
        if self.data_scope:
            return f"{self.data_base}/{self.data_scope}/bars"
        return f"{self.data_base}/bars"

    def _bars_url_v2(self) -> str:
        # Fallback path against host only
        # If data_base already has v1beta3 path, extract host.
        base = self.data_base
        if "v1beta3/crypto" in base.lower():
            # strip after host
            # e.g. https://data.alpaca.markets/v1beta3/crypto/us -> https://data.alpaca.markets
            base = base.split("/v1beta3/crypto", 1)[0]
        return f"{base}/v2/crypto/bars"

    # ---- Normalization -------------------------------------------------------

    @staticmethod
    def _df_from_rows(rows: List[Dict[str, Any]]) -> "pd.DataFrame":
        if pd is None:
            # Fallback to raw list for environments w/o pandas
            return rows  # type: ignore
        df = pd.DataFrame(rows or [])
        if df.empty:
            return pd.DataFrame(columns=["o","h","l","c","v"]).astype({"o":"float64","h":"float64","l":"float64","c":"float64","v":"float64"})
        # v1beta3 & v2 both provide an ISO timestamp key (usually "t")
        ts_key = "t" if "t" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
        if ts_key:
            df["ts"] = pd.to_datetime(df[ts_key], utc=True)
        else:
            # fabricate ts if truly missing
            df["ts"] = pd.to_datetime("now", utc=True)
        # ensure columns exist
        for col in ("o","h","l","c","v"):
            if col not in df.columns:
                df[col] = None
        df = df[["ts","o","h","l","c","v"]].sort_values("ts").set_index("ts")
        return df

    @staticmethod
    def _unpack_response(json_obj: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Supports both:
          { "bars": { "BTC/USD": [ ... ] } }
          and
          { "bars": [ {...,"S":"BTC/USD"}, ... ] }   # some v2 variants
        Returns map symbol -> rows
        """
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

    # ---- Core fetch with retries --------------------------------------------

    def _fetch_bars_with_strategy(self, url: str, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        self.last_url = f"{url}?{requests.compat.urlencode(params)}"
        r = self.s.get(url, headers=self._hdrs(), params=params, timeout=30)
        status = r.status_code
        if status >= 400:
            # propagate JSON error content if any
            try:
                err = r.json()
                self.last_error = f"HTTP {status}: {err}"
            except Exception:
                self.last_error = f"HTTP {status}: {r.text}"
            return None, status
        try:
            return r.json(), status
        except Exception as e:
            self.last_error = f"JSON decode error: {e}"
            return None, status

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

        # Base params
        base_params: Dict[str, Any] = {
            "symbols": ",".join(sym_list),
            "timeframe": timeframe or self.default_timeframe,
            "limit": str(limit or self.default_limit),
            "adjustment": adjustment,
        }
        if start: base_params["start"] = start
        if end: base_params["end"] = end
        if feed: base_params["feed"] = feed

        # 1) v1beta3 (as-is)
        url1 = self._bars_url()
        data, status = self._fetch_bars_with_strategy(url1, dict(base_params))
        # 2) v1beta3 + feed=us retry (only if 4xx or empty)
        if (data is None and status is not None and 400 <= status < 500) or (data and not (data.get("bars") or {})):
            retry_params = dict(base_params)
            retry_params.setdefault("feed", self.default_feed or "us")
            data, status = self._fetch_bars_with_strategy(url1, retry_params)
        # 3) v2 fallback
        if data is None or not (data.get("bars") or {}):
            url2 = self._bars_url_v2()
            v2_params = dict(base_params)
            v2_params.pop("adjustment", None)  # not needed for v2 crypto
            v2_params.setdefault("feed", self.default_feed or "us")
            data, status = self._fetch_bars_with_strategy(url2, v2_params)

        if data is None:
            # Bubble up as empty frames; callers can inspect last_error
            out_empty: Dict[str, BarsResult] = {}
            for sym in sym_list:
                if pd is None:
                    out_empty[sym] = BarsResult(sym, [])  # type: ignore
                else:
                    out_empty[sym] = BarsResult(sym, pd.DataFrame(columns=["o","h","l","c","v"]))
            return out_empty

        # Normalize
        out: Dict[str, BarsResult] = {}
        bars_map = self._unpack_response(data)
        for sym in sym_list:
            rows = bars_map.get(sym) or []
            frame = self._df_from_rows(rows)
            out[sym] = BarsResult(symbol=sym, frame=frame)
        return out

    # Convenience
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
