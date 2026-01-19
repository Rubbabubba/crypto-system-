from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


def _getenv(key: str, default: str | None = None) -> str:
    v = os.getenv(key, default)
    return "" if v is None else str(v)


def _csv(key: str, default: str = "") -> List[str]:
    raw = _getenv(key, default)
    parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
    return parts


@dataclass(frozen=True)
class Settings:
    # Security
    webhook_secret: str
    worker_secret: str

    # Trading universe
    allowed_symbols: List[str]

    # Core trading params
    default_notional_usd: float
    min_order_notional_usd: float
    exit_min_notional_usd: float

    # Exits
    stop_pct: float
    take_pct: float
    exit_cooldown_sec: int
    # Stop execution quality
    # When a stop triggers, we prefer submitting a limit sell slightly below the stop
    # to reduce slippage. If it doesn't fill within stop_limit_timeout_sec, we cancel
    # (best-effort) and fall back to a market sell.
    stop_limit_buffer_pct: float
    stop_limit_timeout_sec: int

    # Discipline
    daily_flatten_time_utc: str  # HH:MM
    max_trades_per_symbol_per_day: int

    # Runtime
    log_level: str


def load_settings() -> Settings:
    return Settings(
        webhook_secret=_getenv("WEBHOOK_SECRET", ""),
        worker_secret=_getenv("WORKER_SECRET", ""),
        allowed_symbols=_csv("ALLOWED_SYMBOLS", "BTC/USD,ETH/USD"),
        default_notional_usd=float(_getenv("DEFAULT_NOTIONAL_USD", _getenv("DEFAULT_NOTIONAL", "50")) or 50),
        min_order_notional_usd=float(_getenv("MIN_ORDER_NOTIONAL_USD", "5") or 5),
        exit_min_notional_usd=float(_getenv("EXIT_MIN_NOTIONAL_USD", "6") or 6),
        stop_pct=float(_getenv("STOP_PCT", "0.01") or 0.01),
        take_pct=float(_getenv("TAKE_PCT", "0.02") or 0.02),
        exit_cooldown_sec=int(float(_getenv("EXIT_COOLDOWN_SEC", "20") or 20)),
        stop_limit_buffer_pct=float(_getenv("STOP_LIMIT_BUFFER_PCT", "0.15") or 0.15),
        stop_limit_timeout_sec=int(float(_getenv("STOP_LIMIT_TIMEOUT_SEC", "60") or 60)),
        daily_flatten_time_utc=_getenv("DAILY_FLATTEN_TIME_UTC", "23:55"),
        max_trades_per_symbol_per_day=int(float(_getenv("MAX_TRADES_PER_SYMBOL_PER_DAY", "3") or 3)),
        log_level=_getenv("LOG_LEVEL", "INFO"),
    )
