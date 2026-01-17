from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class TradePlan:
    symbol: str
    side: str  # "buy" (spot long) only for now
    notional_usd: float
    entry_price: float
    stop_price: float
    take_price: float
    strategy: str
    opened_ts: float


class InMemoryState:
    def __init__(self) -> None:
        self.plans: Dict[str, TradePlan] = {}  # symbol -> plan
        self.last_exit_ts_by_symbol: Dict[str, float] = {}
        self.last_flatten_utc_date: Optional[str] = None  # YYYY-MM-DD
        self.trades_today_by_symbol: Dict[str, int] = {}
        self.trades_today_utc_date: Optional[str] = None

    def can_exit(self, symbol: str, cooldown_sec: int) -> bool:
        last = float(self.last_exit_ts_by_symbol.get(symbol, 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def mark_exit(self, symbol: str) -> None:
        self.last_exit_ts_by_symbol[symbol] = time.time()

    def reset_daily_counters_if_needed(self, utc_date: str) -> None:
        if self.trades_today_utc_date != utc_date:
            self.trades_today_utc_date = utc_date
            self.trades_today_by_symbol = {}

    def inc_trade(self, symbol: str) -> int:
        self.trades_today_by_symbol[symbol] = int(self.trades_today_by_symbol.get(symbol, 0)) + 1
        return self.trades_today_by_symbol[symbol]
