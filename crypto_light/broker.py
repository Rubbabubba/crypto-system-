from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from . import broker_kraken


@dataclass
class SpotPosition:
    base: str
    qty: float


def last_price(symbol: str) -> float:
    return float(broker_kraken.last_price(symbol))


def market_notional(symbol: str, side: str, notional: float, strategy: str, price: Optional[float] = None) -> Dict:
    return broker_kraken.market_notional(symbol=symbol, side=side, notional=float(notional), price=price, strategy=strategy)


def limit_notional(
    symbol: str,
    side: str,
    notional: float,
    limit_price: float,
    strategy: str,
    price: Optional[float] = None,
) -> Dict:
    return broker_kraken.limit_notional(
        symbol=symbol,
        side=side,
        notional=float(notional),
        limit_price=float(limit_price),
        price=price,
        strategy=strategy,
    )


def cancel_order(txid: str) -> Dict:
    return broker_kraken.cancel_order(txid)


def balances_by_asset() -> Dict[str, float]:
    out: Dict[str, float] = {}
    for item in broker_kraken.positions():
        if not isinstance(item, dict):
            continue
        if "error" in item:
            continue
        asset = str(item.get("asset", "")).upper().strip()
        try:
            qty = float(item.get("qty", 0) or 0)
        except Exception:
            qty = 0.0
        if asset and qty > 0:
            out[asset] = qty
    return out


def base_asset(symbol: str) -> str:
    s = str(symbol).upper().strip()
    return s.split("/", 1)[0] if "/" in s else s


def get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300):
    return broker_kraken.get_bars(symbol=symbol, timeframe=timeframe, limit=int(limit))
