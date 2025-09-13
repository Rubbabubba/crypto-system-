# services/exchange_exec.py
import os, requests, json, time

class ExchangeExec:
    def __init__(self, api_key=None, api_secret=None, trading_base=None):
        self.api_key = api_key or os.getenv("CRYPTO_API_KEY")
        self.api_secret = api_secret or os.getenv("CRYPTO_API_SECRET")
        self.base = (trading_base or os.getenv("CRYPTO_TRADING_BASE_URL", "")).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Content-Type": "application/json",
        })

    def place_order_notional(self, symbol: str, side: str, notional_usd: float, tif="day", client_order_id=None, dry=True):
        """
        Alpaca Crypto supports notional orders (USD). Market order for simplicity here.
        symbol: 'BTC/USD' style
        """
        payload = {
            "symbol": symbol.replace("-", "/"),
            "side": side.lower(),
            "type": "market",
            "notional": str(round(float(notional_usd), 2)),
            "time_in_force": tif,
            "client_order_id": client_order_id or f"c_{int(time.time()*1000)}"
        }
        if dry:
            return {"dry_run": True, "payload": payload}

        r = self.session.post(f"{self.base}/orders", data=json.dumps(payload), timeout=20)
        r.raise_for_status()
        return r.json()

    def cancel_all(self):
        r = self.session.delete(f"{self.base}/orders", timeout=20)
        r.raise_for_status()
        return r.json()

    def get_positions(self):
        r = self.session.get(f"{self.base}/positions", timeout=20)
        r.raise_for_status()
        return r.json()

    def get_account(self):
        r = self.session.get(f"{self.base}/account", timeout=20)
        r.raise_for_status()
        return r.json()
