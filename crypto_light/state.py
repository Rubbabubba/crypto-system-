from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import uuid4
from typing import Dict, Optional, List, Any

from . import plans_db
from . import lifecycle_db


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
    trade_plan_id: str = ""
    position_id: str = ""
    signal_id: str = ""
    status: str = "active"
    entry_mode: str = ""
    risk_snapshot: Dict[str, Any] | None = None

    # Optional time-based exit (0 disables). Used by some mean-reversion / maker modes.
    max_hold_sec: int = 0


    breakeven_armed: bool = False
    breakeven_triggered_ts: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "trade_plan_id": self.trade_plan_id,
            "position_id": self.position_id,
            "signal_id": self.signal_id,
            "status": self.status,
            "entry_mode": self.entry_mode,
            "risk_snapshot_json": self.risk_snapshot or {},
            "side": self.side,
            "notional_usd": float(self.notional_usd),
            "entry_price": float(self.entry_price),
            "stop_price": float(self.stop_price),
            "take_price": float(self.take_price),
            "strategy": self.strategy,
            "opened_ts": float(self.opened_ts),
            "max_hold_sec": int(self.max_hold_sec),
            "breakeven_armed": bool(self.breakeven_armed),
            "breakeven_triggered_ts": float(self.breakeven_triggered_ts),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TradePlan":
        return cls(
            symbol=str(d.get("symbol") or ""),
            trade_plan_id=str(d.get("trade_plan_id") or ""),
            position_id=str(d.get("position_id") or ""),
            signal_id=str(d.get("signal_id") or ""),
            status=str(d.get("status") or "active"),
            entry_mode=str(d.get("entry_mode") or ""),
            risk_snapshot=(d.get("risk_snapshot_json") or d.get("risk_snapshot") or {}),
            side=str(d.get("side") or "buy"),
            notional_usd=float(d.get("notional_usd") or 0.0),
            entry_price=float(d.get("entry_price") or 0.0),
            stop_price=float(d.get("stop_price") or 0.0),
            take_price=float(d.get("take_price") or 0.0),
            strategy=str(d.get("strategy") or "unknown"),
            opened_ts=float(d.get("opened_ts") or 0.0),
            max_hold_sec=int(d.get("max_hold_sec") or 0),
            breakeven_armed=bool(d.get("breakeven_armed") or False),
            breakeven_triggered_ts=float(d.get("breakeven_triggered_ts") or 0.0),
        )

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
        self.last_entry_ts_global: float = 0.0
        self.last_exit_ts_by_symbol: Dict[str, float] = {}
        # Stopout cooldown latch: symbol -> last stop exit ts
        self.last_stopout_ts_by_symbol: Dict[str, float] = {}

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

        # Operational status snapshots
        self.last_scan_status: Dict[str, Any] = {}
        self.last_exit_status: Dict[str, Any] = {}
        self.last_scan_route_truth: Dict[str, Any] = {}
        self.last_exit_route_truth: Dict[str, Any] = {}
        self.blocked_trades: List[Dict[str, Any]] = []
        self.blocked_trades_max: int = 500

        # Best-effort order locks to reduce duplicate order submission across
        # rapid retries / slow broker state propagation. Key format is typically
        # "<side>:<symbol>". This is intentionally in-memory only.
        self.order_locks: Dict[str, Dict[str, Any]] = {}

        # Last broker-truth reconcile snapshot (best-effort).
        self.last_reconcile_ts: float = 0.0
        self.last_reconcile_result: Dict[str, Any] = {}

        # Operational risk counters (best-effort, in-memory).
        self.consecutive_entry_rejections: int = 0
        self.consecutive_stopouts: int = 0
        self.ops_lockout_until_ts: float = 0.0
        self.last_ops_lock_reason: str = ""

        # Reload persisted plans after deploy/restart so exits remain correct
        self._load_plans_from_db()

    def set_plan(self, plan: TradePlan) -> None:
        """Set plan in-memory and persist to sqlite + lifecycle store."""
        if not getattr(plan, "trade_plan_id", ""):
            plan.trade_plan_id = str(uuid4())
        if not getattr(plan, "position_id", ""):
            plan.position_id = str(uuid4())
        self.plans[plan.symbol] = plan
        try:
            plans_db.upsert_plan(plan.to_dict())
        except Exception:
            pass
        try:
            lifecycle_db.upsert_trade_plan({
                "trade_plan_id": plan.trade_plan_id,
                "symbol": plan.symbol,
                "strategy_id": plan.strategy,
                "signal_id": plan.signal_id,
                "status": plan.status or "active",
                "direction": plan.side,
                "entry_mode": plan.entry_mode,
                "entry_ref_price": plan.entry_price,
                "stop_price": plan.stop_price,
                "target_price": plan.take_price,
                "time_stop_sec": plan.max_hold_sec,
                "requested_notional_usd": plan.notional_usd,
                "approved_notional_usd": plan.notional_usd,
                "risk_snapshot_json": plan.risk_snapshot or {},
                "legacy_symbol_key": plan.symbol,
            })
            lifecycle_db.upsert_position_ledger({
                "position_id": plan.position_id,
                "trade_plan_id": plan.trade_plan_id,
                "symbol": plan.symbol,
                "side": plan.side,
                "qty": 0.0,
                "avg_entry_price": plan.entry_price,
                "notional_usd": plan.notional_usd,
                "realized_pnl_usd": 0.0,
                "unrealized_pnl_usd": 0.0,
                "fees_usd": 0.0,
                "status": "open",
                "broker_position_qty": 0.0,
                "opened_ts": plan.opened_ts,
            })
        except Exception:
            pass

    def remove_plan(self, symbol: str) -> None:
        plan = self.plans.pop(symbol, None)
        try:
            plans_db.delete_plan(symbol)
        except Exception:
            pass
        try:
            if plan and getattr(plan, "trade_plan_id", ""):
                lifecycle_db.update_trade_plan_status(plan.trade_plan_id, "closed", closed_ts=time.time())
                lifecycle_db.upsert_position_ledger({
                    "position_id": getattr(plan, "position_id", "") or str(uuid4()),
                    "trade_plan_id": plan.trade_plan_id,
                    "symbol": plan.symbol,
                    "side": plan.side,
                    "qty": 0.0,
                    "avg_entry_price": plan.entry_price,
                    "notional_usd": plan.notional_usd,
                    "realized_pnl_usd": 0.0,
                    "unrealized_pnl_usd": 0.0,
                    "fees_usd": 0.0,
                    "status": "closed",
                    "broker_position_qty": 0.0,
                    "opened_ts": plan.opened_ts,
                    "closed_ts": time.time(),
                })
        except Exception:
            pass


    def note_entry_rejection(self) -> int:
        self.consecutive_entry_rejections = int(getattr(self, "consecutive_entry_rejections", 0) or 0) + 1
        return self.consecutive_entry_rejections

    def clear_entry_rejections(self) -> None:
        self.consecutive_entry_rejections = 0

    def note_stopout(self) -> int:
        self.consecutive_stopouts = int(getattr(self, "consecutive_stopouts", 0) or 0) + 1
        return self.consecutive_stopouts

    def clear_stopout_streak(self) -> None:
        self.consecutive_stopouts = 0

    def set_ops_lockout(self, reason: str, duration_sec: int) -> None:
        until = time.time() + max(0, int(duration_sec or 0))
        self.ops_lockout_until_ts = max(float(getattr(self, "ops_lockout_until_ts", 0.0) or 0.0), float(until))
        self.last_ops_lock_reason = str(reason or "")

    def ops_lockout_remaining_sec(self) -> int:
        remain = float(getattr(self, "ops_lockout_until_ts", 0.0) or 0.0) - time.time()
        return int(remain) if remain > 0 else 0

    def clear_ops_lockout(self) -> None:
        self.ops_lockout_until_ts = 0.0
        self.last_ops_lock_reason = ""

    def _load_plans_from_db(self) -> None:
        try:
            lifecycle_db.ensure_schema()
        except Exception:
            pass
        try:
            rows = plans_db.load_plans()
        except Exception:
            return
        for d in rows:
            try:
                plan = TradePlan.from_dict(d)
                if not getattr(plan, "trade_plan_id", ""):
                    plan.trade_plan_id = str(uuid4())
                if not getattr(plan, "position_id", ""):
                    plan.position_id = str(uuid4())
                self.plans[d["symbol"]] = plan
                try:
                    lifecycle_db.upsert_trade_plan({
                        "trade_plan_id": plan.trade_plan_id,
                        "symbol": plan.symbol,
                        "strategy_id": plan.strategy,
                        "signal_id": plan.signal_id,
                        "status": plan.status or "active",
                        "direction": plan.side,
                        "entry_mode": plan.entry_mode,
                        "entry_ref_price": plan.entry_price,
                        "stop_price": plan.stop_price,
                        "target_price": plan.take_price,
                        "time_stop_sec": plan.max_hold_sec,
                        "requested_notional_usd": plan.notional_usd,
                        "approved_notional_usd": plan.notional_usd,
                        "risk_snapshot_json": plan.risk_snapshot or {},
                        "legacy_symbol_key": plan.symbol,
                    })
                except Exception:
                    pass
            except Exception:
                continue

    # --------- Entry guards ---------
    def can_enter(self, symbol: str, cooldown_sec: int) -> bool:
        if cooldown_sec <= 0:
            return True
        last = float(self.last_entry_ts_by_symbol.get(symbol, 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def can_enter_global(self, cooldown_sec: int) -> bool:
        if cooldown_sec <= 0:
            return True
        last = float(getattr(self, "last_entry_ts_global", 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def mark_enter_global(self) -> None:
        self.last_entry_ts_global = time.time()

    def can_enter_after_stopout(self, symbol: str, cooldown_sec: int) -> bool:
        if cooldown_sec <= 0:
            return True
        last = float(self.last_stopout_ts_by_symbol.get(symbol, 0.0) or 0.0)
        return (time.time() - last) >= float(cooldown_sec)

    def mark_stopout(self, symbol: str) -> None:
        self.last_stopout_ts_by_symbol[symbol] = time.time()

    def clear_stopout(self, symbol: str) -> None:
        try:
            self.last_stopout_ts_by_symbol.pop(symbol, None)
        except Exception:
            pass

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

    def clear_stale_pending_exits(self, ttl_sec: int) -> int:
        if ttl_sec <= 0:
            return 0
        now = time.time()
        cleared = 0
        for symbol, meta in list(self.pending_exits.items()):
            ts = float((meta or {}).get("ts", 0.0) or 0.0)
            if ts <= 0.0 or (now - ts) >= float(ttl_sec):
                self.pending_exits.pop(symbol, None)
                cleared += 1
        return cleared

    def has_pending_exit(self, symbol: str, ttl_sec: int = 0) -> bool:
        meta = self.pending_exits.get(symbol) or {}
        if not meta:
            return False
        ts = float(meta.get("ts", 0.0) or 0.0)
        if ttl_sec > 0 and ts > 0.0 and (time.time() - ts) >= float(ttl_sec):
            self.pending_exits.pop(symbol, None)
            return False
        return True

    def set_last_scan_status(self, payload: Dict[str, Any]) -> None:
        self.last_scan_status = dict(payload or {})

    def set_last_exit_status(self, payload: Dict[str, Any]) -> None:
        self.last_exit_status = dict(payload or {})

    def set_last_scan_route_truth(self, payload: Dict[str, Any]) -> None:
        self.last_scan_route_truth = dict(payload or {})

    def set_last_exit_route_truth(self, payload: Dict[str, Any]) -> None:
        self.last_exit_route_truth = dict(payload or {})

    def record_blocked_trade(self, event: Dict[str, Any]) -> None:
        try:
            self.blocked_trades.append(dict(event or {}))
            if len(self.blocked_trades) > self.blocked_trades_max:
                self.blocked_trades = self.blocked_trades[-self.blocked_trades_max :]
        except Exception:
            pass

    # --------- Order locks ---------
    def set_order_lock(self, key: str, *, meta: Dict[str, Any] | None = None) -> None:
        self.order_locks[str(key)] = {"ts": time.time(), **(dict(meta or {}))}

    def clear_order_lock(self, key: str) -> None:
        try:
            self.order_locks.pop(str(key), None)
        except Exception:
            pass

    def has_order_lock(self, key: str, ttl_sec: int = 0) -> bool:
        meta = self.order_locks.get(str(key)) or {}
        if not meta:
            return False
        ts = float(meta.get("ts", 0.0) or 0.0)
        if ttl_sec > 0 and ts > 0.0 and (time.time() - ts) >= float(ttl_sec):
            self.order_locks.pop(str(key), None)
            return False
        return True

    def clear_stale_order_locks(self, ttl_sec: int) -> int:
        if ttl_sec <= 0:
            return 0
        now = time.time()
        cleared = 0
        for key, meta in list(self.order_locks.items()):
            ts = float((meta or {}).get("ts", 0.0) or 0.0)
            if ts <= 0.0 or (now - ts) >= float(ttl_sec):
                self.order_locks.pop(key, None)
                cleared += 1
        return cleared

    def should_reconcile(self, cooldown_sec: int) -> bool:
        try:
            cd = int(cooldown_sec)
        except Exception:
            cd = 0
        if cd <= 0:
            return True
        return (time.time() - float(getattr(self, 'last_reconcile_ts', 0.0) or 0.0)) >= float(cd)

    def mark_reconcile(self, result: Dict[str, Any] | None = None) -> None:
        self.last_reconcile_ts = time.time()
        self.last_reconcile_result = dict(result or {})
