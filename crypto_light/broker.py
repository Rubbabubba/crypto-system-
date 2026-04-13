from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import time

from . import broker_kraken


def _maybe_record_trade(
    resp: Dict,
    *,
    exchange: str,
    side: str,
    fallback_price: Optional[float],
    strategy: str,
) -> None:
    """Best-effort trade persistence.

    We only record filled orders. This powers /trades and /trades/summary so we
    can analyze win-rate, avg win/loss, expectancy, and fee drag.
    """
    try:
        if not isinstance(resp, dict) or not resp.get("ok") or resp.get("filled") is not True:
            return

        from . import trades_db  # local import avoids circular imports

        pair = str(resp.get("pair") or "")
        qty = float(resp.get("volume") or 0.0)
        cost = float(resp.get("notional") or 0.0)

        px = resp.get("avg_price")
        if px is None:
            px = resp.get("price")
        if px is None:
            px = resp.get("limit_price")
        if px is None:
            px = fallback_price
        price = float(px or 0.0)

        if not pair or qty <= 0 or price <= 0:
            return

        txid = resp.get("txid")
        if isinstance(txid, list):
            txid = txid[0] if txid else ""
        txid = str(txid or "")

        # Persist in the same schema used by /trades and /trades/summary.
        trades_db.upsert_trades(
            [
                {
                    "txid": txid or f"local-{int(time.time()*1000)}",
                    "pair": pair,
                    "type": str(side),
                    "ordertype": str(resp.get("execution") or "market"),
                    "price": price,
                    "vol": qty,
                    "cost": cost,
                    "fee": float(resp.get("fee") or 0.0),
                    "time": time.time(),
                    "raw": resp,
                }
            ]
        )
    except Exception:
        # Never allow trade persistence issues to impact live trading.
        return

# Last Balance error (for diagnostics)
LAST_BALANCE_ERROR: str | None = None

def last_balance_error() -> str | None:
    return LAST_BALANCE_ERROR



@dataclass
class SpotPosition:
    base: str
    qty: float


def last_price(symbol: str) -> float:
    return float(broker_kraken.last_price(symbol))


def market_notional(
    symbol: str,
    side: str,
    notional: float,
    strategy: str,
    price: Optional[float] = None,
    exec_mode_override: Optional[str] = None,
    post_offset_override: Optional[float] = None,
    chase_sec_override: Optional[int] = None,
    chase_steps_override: Optional[int] = None,
    market_fallback_override: Optional[bool] = None,
) -> Dict:
    resp = broker_kraken.market_notional(
        symbol=symbol,
        side=side,
        notional=float(notional),
        price=price,
        strategy=strategy,
        exec_mode_override=exec_mode_override,
        post_offset_override=post_offset_override,
        chase_sec_override=chase_sec_override,
        chase_steps_override=chase_steps_override,
        market_fallback_override=market_fallback_override,
    )

    _maybe_record_trade(resp, exchange="kraken", side=side, fallback_price=price, strategy=strategy)
    return resp


def limit_notional(
    symbol: str,
    side: str,
    notional: float,
    limit_price: float,
    strategy: str,
    price: Optional[float] = None,
) -> Dict:
    resp = broker_kraken.limit_notional(
        symbol=symbol,
        side=side,
        notional=float(notional),
        limit_price=float(limit_price),
        price=price,
        strategy=strategy,
    )

    _maybe_record_trade(resp, exchange="kraken", side=side, fallback_price=limit_price, strategy=strategy)
    return resp


def cancel_order(txid: str) -> Dict:
    return broker_kraken.cancel_order(txid)


def balances_by_asset() -> Dict[str, float]:
    """Return spot balances by asset code.

    Uses cached Kraken Balance truth seeded by the background balance refresher.

    On failure, returns an empty dict but records the error string in
    LAST_BALANCE_ERROR for surfacing in /worker diagnostics.
    """
    global LAST_BALANCE_ERROR
    LAST_BALANCE_ERROR = None

    out: Dict[str, float] = {}
    try:
        bal = broker_kraken.get_cached_balances()  # cache-only read
    except Exception as e:
        LAST_BALANCE_ERROR = f"{type(e).__name__}: {e}"
        bal = {}

    if isinstance(bal, dict):
        for k, v in bal.items():
            asset = str(k or "").upper().strip()
            try:
                qty = float(v or 0.0)
            except Exception:
                qty = 0.0
            if asset and qty > 0:
                out[asset] = qty

    if LAST_BALANCE_ERROR is None:
        try:
            meta = broker_kraken.get_balance_cache_meta()
        except Exception:
            meta = {}
        if not bool(meta.get("seeded")):
            LAST_BALANCE_ERROR = "balance cache unseeded"
        elif not out and isinstance(bal, dict) and bal:
            LAST_BALANCE_ERROR = "Balance parsed empty (all zero or non-numeric)"

    return out


def base_asset(symbol: str) -> str:
    s = str(symbol).upper().strip()
    return s.split("/", 1)[0] if "/" in s else s




def best_bid_ask(symbol: str) -> tuple[float | None, float | None]:
    """Return (bid, ask) for a UI symbol like 'ADA/USD' (best-effort).

    Uses Kraken public Depth endpoint under the hood.
    """
    ui = str(symbol).upper().strip()
    pair = broker_kraken.to_kraken(ui)
    return broker_kraken._best_bid_ask(pair)


def get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300):
    return broker_kraken.get_bars(symbol=symbol, timeframe=timeframe, limit=int(limit))