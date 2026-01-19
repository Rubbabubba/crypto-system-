from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional, List, Any


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
        # Pending exit orders (best-effort): symbol -> {"ts": float, "reason": str, "txid": str|None}
        # Used to avoid re-submitting multiple exit orders while a limit exit is working.
        self.pending_exits: Dict[str, Dict[str, Any]] = {}
        self.last_flatten_utc_date: Optional[str] = None  # YYYY-MM-DD
        self.trades_today_by_symbol: Dict[str, int] = {}
        self.trades_today_utc_date: Optional[str] = None

        # Lightweight telemetry buffer (in-memory, best-effort).
        # Render logs are still the primary source of truth.
        self.telemetry: List[Dict[str, Any]] = []
        self.telemetry_max: int = 500

    def can_exit(self, symbol: str, cooldown_sec: int) -> bool:
        last = float(self.last_exit_ts_by_symbol.get(symbol, 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def mark_exit(self, symbol: str) -> None:
        self.last_exit_ts_by_symbol[symbol] = time.time()

    def set_pending_exit(self, symbol: str, reason: str, txid: Optional[str] = None) -> None:
        self.pending_exits[symbol] = {"ts": time.time(), "reason": reason, "txid": txid}

    def clear_pending_exit(self, symbol: str) -> None:
        try:
            self.pending_exits.pop(symbol, None)
        except Exception:
            pass

    def reset_daily_counters_if_needed(self, utc_date: str) -> None:
        if self.trades_today_utc_date != utc_date:
            self.trades_today_utc_date = utc_date
            self.trades_today_by_symbol = {}

    def inc_trade(self, symbol: str) -> int:
        self.trades_today_by_symbol[symbol] = int(self.trades_today_by_symbol.get(symbol, 0)) + 1
        return self.trades_today_by_symbol[symbol]

    def record_event(self, event: Dict[str, Any]) -> None:
        """Append a telemetry event (best-effort)."""
        try:
            self.telemetry.append(event)
            if len(self.telemetry) > self.telemetry_max:
                self.telemetry = self.telemetry[-self.telemetry_max :]
        except Exception:
            # Never break trading for telemetry.
            pass
