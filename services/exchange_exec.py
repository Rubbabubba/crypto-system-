# services/exchange_exec.py
# Version: 1.8.1
# - Centralized client_order_id attribution
# - Compatible with Alpaca Crypto paper/live endpoints
# - Shapes align with prior usage in app.py (recent_orders, positions, sample_account)
#
# Attribution notes:
# • app v1.8.1 passes params["client_tag"] = strategy name (e.g., "c2")
# • We generate a client_order_id = "{client_tag}-{SYMBOL}-{unix_ts}" if none provided
# • Strategies do NOT need changes if they pass a params dict through (recommended)

from __future__ import annotations

import os
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import requests


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


class ExchangeExec:
    """
    Thin broker wrapper for Alpaca Crypto.

    Exposed methods (used by your app/strategies):
      - from_env()
      - sample_account()
      - recent_orders(status="all", limit=50)
      - positions()
      - submit_order(symbol, side, qty=None, notional=None, type="market", time_in_force="gtc", params=None, **kwargs)
      - paper_buy / paper_sell (helpers; both route to submit_order)
      - notional (helper: submit notional-based order)

    Centralized attribution:
      • submit_order injects client_order_id if not set, using params["client_tag"] or "unknown".
      • client_order_id format: "<client_tag>-<SYMBOL_NO_SLASHES>-<unix_ts>"
    """

    def __init__(self, key: str, secret: str, trading_base: str):
        self.key = key
        self.secret = secret
        # e.g., https://paper-api.alpaca.markets or https://api.alpaca.markets
        self.trading_base = trading_base.rstrip("/")
        # cache for lightweight diag
        self._last_account: Dict[str, Any] | None = None

    # ---------- Construction ----------

    @classmethod
    def from_env(cls) -> "ExchangeExec":
        """
        Required env:
          - ALPACA_KEY_ID
          - ALPACA_SECRET_KEY
        Optional:
          - ALPACA_TRADE_HOST (default: paper endpoint)
        """
        key = _env("ALPACA_KEY_ID")
        secret = _env("ALPACA_SECRET_KEY")
        if not key or not secret:
            raise RuntimeError("Missing ALPACA_KEY_ID / ALPACA_SECRET_KEY in environment")

        trading_base = _env("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
        return cls(key=key, secret=secret, trading_base=trading_base)

    # ---------- HTTP helpers ----------

    def _headers(self) -> Dict[str, str]:
        # Alpaca broker API headers
        return {
            "APCA-API-KEY-ID": self.key,
            "APCA-API-SECRET-KEY": self.secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: Dict[str, Any] | None = None) -> Any:
        url = f"{self.trading_base}{path}"
        r = requests.get(url, headers=self._headers(), params=params or {}, timeout=30)
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.trading_base}{path}"
        r = requests.post(url, headers=self._headers(), data=json.dumps(payload), timeout=30)
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    # ---------- Public API ----------

    def sample_account(self) -> Dict[str, Any]:
        """
        Light-weight account sample for /diag/crypto.
        """
        try:
            acct = self._get("/v2/account")
            # Keep a small subset
            out = {
                "id": acct.get("id"),
                "status": acct.get("status"),
                "currency": acct.get("currency"),
                "buying_power": acct.get("buying_power"),
                "cash": acct.get("cash"),
                "pattern_day_trader": acct.get("pattern_day_trader"),
                "multi_party_customer": acct.get("multi_party_customer"),
            }
            self._last_account = out
            return out
        except Exception as e:
            return {"error": str(e)}

    # ---- Orders & Positions ----

    def recent_orders(self, status: str = "all", limit: int = 50) -> List[Dict[str, Any]]:
        """
        Returns a list of recent orders (Alpaca v2 /orders).
        status: 'open' | 'closed' | 'all'
        """
        params = {
            "status": status,
            "limit": max(1, min(int(limit or 50), 1000)),
            # Crypto is extended; no date filters here so the caller can filter
        }
        try:
            items = self._get("/v2/orders", params=params)
            if isinstance(items, list):
                return items
            # sometimes APIs paginate or wrap; normalize to list
            if isinstance(items, dict) and "orders" in items:
                return items["orders"]
            return []
        except Exception as e:
            return [{"error": str(e)}]

    def positions(self) -> List[Dict[str, Any]]:
        """
        Returns open positions (Alpaca v2 /positions).
        """
        try:
            items = self._get("/v2/positions")
            if isinstance(items, list):
                return items
            return []
        except Exception as e:
            return [{"error": str(e)}]

    # ---- Client Order ID Attribution ----

    @staticmethod
    def _coid_from(params: Optional[Dict[str, Any]], symbol: str, fallback: str = "unknown") -> str:
        tag = (params or {}).get("client_tag") or fallback
        sym = (symbol or "").replace("/", "")
        return f"{tag}-{sym}-{int(time.time())}"

    # ---- Order placement ----

    def submit_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: Optional[float] = None,
        notional: Optional[float] = None,
        type: str = "market",
        time_in_force: str = "gtc",
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Place an order. For crypto on Alpaca:
          - Use either qty OR notional (not both).
          - time_in_force can be 'gtc' for market orders.
        Centralizes client_order_id injection for attribution.
        """
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "time_in_force": time_in_force,
        }

        if qty is not None and notional is not None:
            # Prefer explicit qty if both passed accidentally
            notional = None

        if qty is not None:
            payload["qty"] = str(qty)  # Alpaca accepts string quantities
        if notional is not None:
            payload["notional"] = str(notional)

        # If caller specified client_order_id explicitly in kwargs, honor it.
        coid = (
            kwargs.get("client_order_id")
            or payload.get("client_order_id")
            or self._coid_from(params, symbol)
        )
        payload["client_order_id"] = coid

        # Allow callers to pass through extra broker-specific fields via kwargs
        for k, v in (kwargs or {}).items():
            if k == "client_order_id":
                continue
            payload[k] = v

        try:
            out = self._post("/v2/orders", payload)
        except requests.HTTPError as he:
            return {"ok": False, "error": str(he), "payload": payload}
        except Exception as e:
            return {"ok": False, "error": str(e), "payload": payload}

        # Normalize a lightweight response
        if isinstance(out, dict):
            out.setdefault("ok", True)
            # keep key fields commonly used by the app / audits
            return {
                "ok": True,
                "id": out.get("id"),
                "symbol": out.get("symbol") or symbol,
                "side": out.get("side") or side,
                "qty": out.get("qty") or payload.get("qty"),
                "notional": out.get("notional") or payload.get("notional"),
                "status": out.get("status"),
                "client_order_id": out.get("client_order_id") or coid,
                "created_at": out.get("created_at"),
                "filled_at": out.get("filled_at"),
                "filled_avg_price": out.get("filled_avg_price"),
                "raw": out,  # keep the raw for debugging/forensics
            }
        return {"ok": True, "raw": out}

    # ---- Convenience helpers used by some strategies ----

    def paper_buy(
        self,
        symbol: str,
        *,
        qty: Optional[float] = None,
        notional: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return self.submit_order(
            symbol=symbol,
            side="buy",
            qty=qty,
            notional=notional,
            type="market",
            time_in_force="gtc",
            params=params,
            **kwargs,
        )

    def paper_sell(
        self,
        symbol: str,
        *,
        qty: Optional[float] = None,
        notional: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return self.submit_order(
            symbol=symbol,
            side="sell",
            qty=qty,
            notional=notional,
            type="market",
            time_in_force="gtc",
            params=params,
            **kwargs,
        )

    def notional(
        self,
        symbol: str,
        side: str,
        *,
        usd: float,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Place a notional order (e.g., $25 worth of BTC).
        """
        return self.submit_order(
            symbol=symbol,
            side=side,
            notional=usd,
            type="market",
            time_in_force="gtc",
            params=params,
            **kwargs,
        )

    # ---- Maintenance (optional, handy in dev) ----

    def cancel_all_orders(self) -> Dict[str, Any]:
        try:
            url = f"{self.trading_base}/v2/orders"
            r = requests.delete(url, headers=self._headers(), timeout=30)
            r.raise_for_status()
            return {"ok": True, "result": r.json() if r.content else {}}
        except Exception as e:
            return {"ok": False, "error": str(e)}
