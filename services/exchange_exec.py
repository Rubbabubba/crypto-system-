# services/exchange_exec.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Optional
import requests

DEFAULT_TRADING_BASE = (
    os.environ.get("CRYPTO_TRADING_BASE")
    or os.environ.get("ALPACA_TRADING_BASE")
    or "https://paper-api.alpaca.markets/v2"
)
API_KEY    = os.environ.get("CRYPTO_API_KEY") or os.environ.get("APCA_API_KEY_ID")
API_SECRET = os.environ.get("CRYPTO_API_SECRET") or os.environ.get("APCA_API_SECRET_KEY")

class ExchangeExec:
    """
    Minimal Alpaca-crypto execution wrapper.

    Exposes a bunch of method names so different strategies can discover one:
    - place_order, submit_order, order, submit, create_order
    - buy, sell (directional helpers)

    Uses market orders with either `notional` (USD) or `qty`.
    """

    def __init__(
        self,
        trading_base: str = DEFAULT_TRADING_BASE,
        api_key: Optional[str] = API_KEY,
        api_secret: Optional[str] = API_SECRET,
        session: Optional[requests.Session] = None,
    ):
        self.base = trading_base.rstrip("/")
        self.key = api_key
        self.secret = api_secret
        self.s = session or requests.Session()
        if not self.key or not self.secret:
            raise RuntimeError("Missing CRYPTO_API_KEY / CRYPTO_API_SECRET (or APCA_*) for ExchangeExec")

    @classmethod
    def from_env(cls) -> "ExchangeExec":
        return cls()

    # ---------------- core ----------------
    def _hdrs(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.key,
            "APCA-API-SECRET-KEY": self.secret,
            "Content-Type": "application/json",
        }

    def _post_order(
        self,
        symbol: str,
        side: str,
        *,
        notional: Optional[float] = None,
        qty: Optional[float] = None,
        tif: str = "gtc",
    ) -> Dict[str, Any]:
        """
        POST /v2/orders (Alpaca). Crypto requires asset_class='crypto'.
        Send either notional (USD) or qty.
        """
        url = f"{self.base}/orders"
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "market",
            "time_in_force": tif,
            "asset_class": "crypto",
        }
        if notional is not None:
            payload["notional"] = float(notional)
        elif qty is not None:
            payload["qty"] = float(qty)
        else:
            raise ValueError("provide notional or qty")

        r = self.s.post(url, headers=self._hdrs(), data=json.dumps(payload), timeout=30)
        try:
            data = r.json()
        except Exception:
            data = {"status_code": r.status_code, "text": r.text}
        if r.status_code >= 300:
            return {"error": True, "status_code": r.status_code, "data": data}
        return {"error": False, "status_code": r.status_code, "data": data}

    # -------------- names strategies might call --------------
    def place_order(self, symbol: str, side: str, notional: float) -> Dict[str, Any]:
        return self._post_order(symbol, side, notional=notional)

    def submit_order(self, symbol: str, side: str, notional: float) -> Dict[str, Any]:
        return self._post_order(symbol, side, notional=notional)

    def order(self, symbol: str, side: str, notional: float) -> Dict[str, Any]:
        return self._post_order(symbol, side, notional=notional)

    def submit(self, symbol: str, side: str, notional: float) -> Dict[str, Any]:
        return self._post_order(symbol, side, notional=notional)

    def create_order(self, symbol: str, side: str, notional: float) -> Dict[str, Any]:
        return self._post_order(symbol, side, notional=notional)

    # Directional convenience
    def buy(self, symbol: str, side: str = "buy", notional: float = 0.0) -> Dict[str, Any]:
        return self._post_order(symbol, "buy", notional=notional or None)

    def sell(self, symbol: str, side: str = "sell", notional: float = 0.0) -> Dict[str, Any]:
        return self._post_order(symbol, "sell", notional=notional or None)
