# services/exchange_exec.py
# Version: 1.7.3
# Alpaca paper-trading wrapper used by the app and strategies.

from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict, List, Optional

import requests


class ExchangeExec:
    """
    Thin wrapper around Alpaca Paper Trading + Crypto Data for simple actions.
    Exposes:
      - from_env()
      - trading_base (property)
      - sample_account()
      - recent_orders(status="all", limit=50)
      - positions()
      - submit_order(symbol, side, notional=None, qty=None, type="market", tif="gtc")
      - paper_buy(symbol, notional=None, qty=None)
      - paper_sell(symbol, notional=None, qty=None)
    """

    def __init__(self, key_id: str, secret_key: str, trading_base: str):
        self.key_id = key_id
        self.secret_key = secret_key
        self.trading_base = trading_base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ---------- Constructors ----------

    @classmethod
    def from_env(cls) -> "ExchangeExec":
        key = os.environ.get("ALPACA_KEY_ID")
        sec = os.environ.get("ALPACA_SECRET_KEY")
        trading_base = os.environ.get("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
        if not key or not sec:
            raise RuntimeError("Missing ALPACA_KEY_ID / ALPACA_SECRET_KEY in environment.")
        return cls(key, sec, trading_base)

    # ---------- Basic helpers ----------

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.trading_base}{path}"
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.trading_base}{path}"
        r = self.session.post(url, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()

    # ---------- Public API used by the app ----------

    def sample_account(self) -> Dict[str, Any]:
        """
        Lightweight account fetch for the /diag/crypto endpoint.
        """
        return self._get("/v2/account")

    def recent_orders(self, status: str = "all", limit: int = 50) -> List[Dict[str, Any]]:
        """
        Returns a list of recent orders.
        """
        params = {"status": status, "limit": limit, "nested": "true"}
        return self._get("/v2/orders", params=params)

    def positions(self) -> List[Dict[str, Any]]:
        """
        Returns open positions.
        """
        return self._get("/v2/positions")

    # ---------- Order helpers for strategies ----------

    def submit_order(
        self,
        symbol: str,
        side: str,
        notional: Optional[float] = None,
        qty: Optional[float] = None,
        type: str = "market",
        tif: str = "gtc",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Submit a crypto order. You can use notional (USD) or qty (units).
        """
        if notional is None and qty is None:
            raise ValueError("submit_order requires notional or qty")

        payload: Dict[str, Any] = {
            "symbol": symbol,             # e.g., "BTC/USD"
            "side": side,                 # "buy" | "sell"
            "type": type,                 # "market" (simple)
            "time_in_force": tif,         # "gtc"
        }
        if client_order_id:
            payload["client_order_id"] = client_order_id
        else:
            payload["client_order_id"] = f"cs-{int(time.time())}-{uuid.uuid4().hex[:8]}"

        if notional is not None:
            payload["notional"] = str(notional)
        if qty is not None:
            payload["qty"] = str(qty)

        return self._post("/v2/orders", payload)

    def paper_buy(self, symbol: str, *, notional: Optional[float] = None, qty: Optional[float] = None) -> Dict[str, Any]:
        return self.submit_order(symbol=symbol, side="buy", notional=notional, qty=qty)

    def paper_sell(self, symbol: str, *, notional: Optional[float] = None, qty: Optional[float] = None) -> Dict[str, Any]:
        return self.submit_order(symbol=symbol, side="sell", notional=notional, qty=qty)
