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
    high_watermark: float = 0.0
    trailing_stop_price: float = 0.0
    last_exit_attempt_ts: float = 0.0


class InMemoryState:
    """Best-effort in-memory state.

    IMPORTANT: On Render, in-memory state is per-instance. If you scale web instances > 1,
    these guards will not be shared across instances. For deterministic protection, keep
    web instances = 1, or add a shared store (Redis/Postgres) later.
    """

    def __init__(self) -> None:
        self.plans: Dict[str, TradePlan] = {}  # symbol -> plan

        # Entry/exit throttles
        self.last_entry_ts_by_symbol: Dict[str, float] = {}
        self.last_exit_ts_by_symbol: Dict[str, float] = {}

        # Pending exit orders (best-effort): symbol -> {"ts": float, "reason": str, "txid": str|None}
        self.pending_exits: Dict[str, Dict[str, Any]] = {}

        # Daily flatten bookkeeping
        self.last_flatten_utc_date: Optional[str] = None  # YYYY-MM-DD

        # Trade frequency (per UTC day)
        self.trades_today_by_symbol: Dict[str, int] = {}
        self.trades_today_total: int = 0
        self.trades_today_utc_date: Optional[str] = None

        # Daily P&L guardrails (best-effort). Updated only when we can compute it.
        self.daily_pnl_usd: float = 0.0
        self.daily_pnl_utc_date: Optional[str] = None

        # Idempotency (webhook retries)
        # signal_id -> last_seen_ts
        self.seen_signal_ids: Dict[str, float] = {}

        # Lightweight telemetry buffer (in-memory, best-effort).
        self.telemetry: List[Dict[str, Any]] = []
        self.telemetry_max: int = 500

    # --------- Entry guards ---------
    def can_enter(self, symbol: str, cooldown_sec: int) -> bool:
        if cooldown_sec <= 0:
            return True
        last = float(self.last_entry_ts_by_symbol.get(symbol, 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def mark_enter(self, symbol: str) -> None:
        self.last_entry_ts_by_symbol[symbol] = time.time()

    # --------- Exit guards ---------
    def can_exit(self, symbol: str, cooldown_sec: int) -> bool:
        last = float(self.last_exit_ts_by_symbol.get(symbol, 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def mark_exit(self, symbol: str) -> None:
        self.last_exit_ts_by_symbol[symbol] = time.time()

    # --------- Pending exits ---------
    def set_pending_exit(self, symbol: str, reason: str, txid: Optional[str] = None) -> None:
        self.pending_exits[symbol] = {"ts": time.time(), "reason": reason, "txid": txid}

    def clear_pending_exit(self, symbol: str) -> None:
        try:
            self.pending_exits.pop(symbol, None)
        except Exception:
            pass

    # --------- Daily counters ---------
    def reset_daily_counters_if_needed(self, utc_date: str) -> None:
        if self.trades_today_utc_date != utc_date:
            self.trades_today_utc_date = utc_date
            self.trades_today_by_symbol = {}
            self.trades_today_total = 0
        # Keep daily P&L window aligned to UTC day as well.
        if self.daily_pnl_utc_date != utc_date:
            self.daily_pnl_utc_date = utc_date
            self.daily_pnl_usd = 0.0

    def can_trade_symbol_today(self, symbol: str, max_trades_per_day: int) -> bool:
        """Per-symbol daily trade limiter.

        Some parts of the app call a helper method rather than reading
        `trades_today_by_symbol` directly. Keep this here so entry execution
        can't crash when the limiter is enabled.
        """
        try:
            m = int(max_trades_per_day)
        except Exception:
            m = 0

        # 0 or negative means "unlimited"
        if m <= 0:
            return True

        return int(self.trades_today_by_symbol.get(symbol, 0)) < m

    def inc_trade(self, symbol: str) -> int:
        self.trades_today_by_symbol[symbol] = int(self.trades_today_by_symbol.get(symbol, 0)) + 1
        return self.trades_today_by_symbol[symbol]


    def inc_trade_total(self) -> int:
        self.trades_today_total = int(getattr(self, 'trades_today_total', 0) or 0) + 1
        return self.trades_today_total
    # --------- Webhook idempotency ---------
    def seen_recent_signal(self, signal_id: str, ttl_sec: int) -> bool:
        """Return True if we saw this signal_id within ttl_sec, else record and return False."""
        if not signal_id:
            return False
        now = time.time()
        last = float(self.seen_signal_ids.get(signal_id, 0.0) or 0.0)
        if ttl_sec > 0 and (now - last) < float(ttl_sec):
            return True
        self.seen_signal_ids[signal_id] = now

        # Opportunistic cleanup to avoid unbounded growth (best-effort)
        try:
            if len(self.seen_signal_ids) > 5000:
                items = sorted(self.seen_signal_ids.items(), key=lambda kv: kv[1], reverse=True)
                self.seen_signal_ids = dict(items[:4000])
        except Exception:
            pass

        return False

    # --------- Telemetry ---------
    def record_event(self, event: Dict[str, Any]) -> None:
        """Append a telemetry event (best-effort)."""
        try:
            self.telemetry.append(event)
            if len(self.telemetry) > self.telemetry_max:
                self.telemetry = self.telemetry[-self.telemetry_max :]
        except Exception:
            pass