# services/exchange_exec.py — v1.8.8
import os
import json
from datetime import timezone
from typing import List, Dict

import requests
import pandas as pd

UTC = timezone.utc

class Broker:
    def __init__(self, api_key=None, secret_key=None, trading_base=None, data_base=None):
        self.api_key = api_key or os.environ.get("APCA_API_KEY_ID","")
        self.secret_key = secret_key or os.environ.get("APCA_API_SECRET_KEY","")
        self.trading_base = trading_base or os.environ.get("APCA_API_BASE_URL","https://paper-api.alpaca.markets")
        self.data_base = data_base or os.environ.get("DATA_API_BASE_URL","https://data.alpaca.markets")
        self._sess = requests.Session()
        self._sess.headers.update({
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # -------- Orders / Positions --------
    def submit_order(self, symbol:str, side:str, notional:float, client_order_id:str, time_in_force:str="gtc", type:str="market"):
        url = f"{self.trading_base}/v2/orders"
        payload = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "time_in_force": time_in_force,
            "notional": str(notional),
            "client_order_id": client_order_id,
        }
        r = self._sess.post(url, data=json.dumps(payload), timeout=20)
        r.raise_for_status()
        return r.json()

    def list_orders(self, status="all", limit=50):
        url = f"{self.trading_base}/v2/orders"
        params = {"status": status, "limit": str(limit), "direction": "desc", "nested": "false"}
        r = self._sess.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    def list_positions(self):
        url = f"{self.trading_base}/v2/positions"
        r = self._sess.get(url, timeout=20)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json()

    # -------- Market Data (Crypto) --------
    def get_bars(self, symbols:List[str], timeframe:str="1Min", limit:int=600, merge=False) -> Dict[str, pd.DataFrame]:
        if not symbols:
            return {}
        url = f"{self.data_base}/v1beta3/crypto/us/bars"
        params = {"symbols": ",".join(symbols), "timeframe": timeframe, "limit": str(limit)}
        r = self._sess.get(url, params=params, timeout=30)
        r.raise_for_status()
        js = r.json() or {}
        bars = js.get("bars") or {}
        out = {}
        for sym in symbols:
            arr = bars.get(sym) or []
            if not arr:
                out[sym] = pd.DataFrame(columns=["open","high","low","close","volume"])
                continue
            df = pd.DataFrame(arr)
            df["t"] = pd.to_datetime(df["t"], utc=True)
            df = df.set_index("t").sort_index()
            df = df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume"})
            out[sym] = df[["open","high","low","close","volume"]]
        return out
