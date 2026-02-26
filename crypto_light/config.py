from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


def _getenv(key: str, default: str | None = None) -> str:
    v = os.getenv(key, default)
    return "" if v is None else str(v)


def _getbool(key: str, default: str = "0") -> bool:
    v = _getenv(key, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


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
    trading_enabled: bool
    default_notional_usd: float
    min_order_notional_usd: float
    exit_min_notional_usd: float

    # Sizing
    sizing_mode: str  # 'fixed', 'equity_fraction', or 'risk_pct_equity'
    risk_per_trade: float
    equity_fraction_per_trade: float
    max_notional_usd: float  # 0 disables



    # Execution (fees)
    execution_mode: str  # 'market' or 'maker_first'
    post_only_offset_pct: float
    limit_chase_sec: int
    limit_chase_steps: int
    market_fallback: bool

    # Anti-churn
    global_entry_cooldown_sec: int
    stopout_cooldown_sec: int
    min_hold_sec_before_stop: int

    # Position detection / dust
    min_position_notional_usd: float

    # Exposure caps (0 disables)
    max_total_exposure_usd: float
    max_symbol_exposure_usd: float


    # Entries / discipline
    entry_cooldown_sec: int
    no_new_entries_after_utc: str  # HH:MM or "" for 24/7
    max_trades_per_symbol_per_day: int

    # Idempotency / anti-spam
    signal_dedupe_ttl_sec: int

    # Exits
    stop_pct: float
    take_pct: float
    exit_cooldown_sec: int
    max_hold_sec: int  # 0 disables
    time_exit_grace_sec: int
    exit_dry_run: bool
    exit_diagnostics: bool

    # Daily risk

    # Break-even
    breakeven_enabled: bool
    breakeven_trigger_pct: float
    breakeven_offset_pct: float

    # Daily risk
    max_daily_loss_usd: float


    # Stop execution quality
    stop_limit_buffer_pct: float
    stop_limit_timeout_sec: int

    # Daily flatten behavior
    enforce_daily_flatten: bool
    daily_flatten_time_utc: str  # HH:MM
    block_entries_after_flatten: bool  # if true, blocks buys during flatten window

    # Runtime
    log_level: str


def load_settings() -> Settings:
    return Settings(
        # Security
        webhook_secret=_getenv("WEBHOOK_SECRET", ""),
        worker_secret=_getenv("WORKER_SECRET", ""),

        # Universe
        allowed_symbols=_csv("ALLOWED_SYMBOLS", "BTC/USD,ETH/USD"),

        # Core
        trading_enabled=_getbool("TRADING_ENABLED", "1"),
        default_notional_usd=float(_getenv("DEFAULT_NOTIONAL_USD", _getenv("DEFAULT_NOTIONAL", "50")) or 50),
        min_order_notional_usd=float(_getenv("MIN_ORDER_NOTIONAL_USD", "5") or 5),
        exit_min_notional_usd=float(_getenv("EXIT_MIN_NOTIONAL_USD", "6") or 6),
        # Sizing
        sizing_mode=_getenv("SIZING_MODE", "fixed").strip().lower() or "fixed",
        risk_per_trade=float(_getenv("RISK_PER_TRADE", "0.03") or 0.03),
        equity_fraction_per_trade=float(_getenv("EQUITY_FRACTION_PER_TRADE", "0.05") or 0.05),
        max_notional_usd=float(_getenv("MAX_NOTIONAL_USD", "0") or 0),



        # Execution (fees)
        execution_mode=_getenv("EXECUTION_MODE", "market").strip().lower() or "market",
        post_only_offset_pct=float(_getenv("POST_ONLY_OFFSET_PCT", "0.0002") or 0.0002),
        limit_chase_sec=int(float(_getenv("LIMIT_CHASE_SEC", "10") or 10)),
        limit_chase_steps=int(float(_getenv("LIMIT_CHASE_STEPS", "1") or 1)),
        market_fallback=_getbool("MARKET_FALLBACK", "1"),

        # Anti-churn
        global_entry_cooldown_sec=int(float(_getenv("GLOBAL_ENTRY_COOLDOWN_SEC", "0") or 0)),
        stopout_cooldown_sec=int(float(_getenv("STOPOUT_COOLDOWN_SEC", "0") or 0)),
        min_hold_sec_before_stop=int(float(_getenv("MIN_HOLD_SEC_BEFORE_STOP", "0") or 0)),

        # Position detection / dust
        min_position_notional_usd=float(_getenv("MIN_POSITION_NOTIONAL_USD", "10") or 10),

        # Exposure caps
        max_total_exposure_usd=float(_getenv("MAX_TOTAL_EXPOSURE_USD", _getenv("MAX_EXPOSURE_USD", "0")) or 0),
        max_symbol_exposure_usd=float(_getenv("MAX_SYMBOL_EXPOSURE_USD", "0") or 0),

        # Entries / discipline
        entry_cooldown_sec=int(float(_getenv("ENTRY_COOLDOWN_SEC", "0") or 0)),
        no_new_entries_after_utc=_getenv("NO_NEW_ENTRIES_AFTER_UTC", ""),  # "" = 24/7
        max_trades_per_symbol_per_day=int(float(_getenv("MAX_TRADES_PER_SYMBOL_PER_DAY", "999") or 999)),

        # Idempotency
        signal_dedupe_ttl_sec=int(float(_getenv("SIGNAL_DEDUPE_TTL_SEC", "90") or 90)),

        # Exits
        stop_pct=float(_getenv("STOP_PCT", "0.01") or 0.01),
        take_pct=float(_getenv("TAKE_PCT", "0.02") or 0.02),
        exit_cooldown_sec=int(float(_getenv("EXIT_COOLDOWN_SEC", "20") or 20)),
        max_hold_sec=int(float(_getenv("MAX_HOLD_SEC", "0") or 0)),
        time_exit_grace_sec=int(float(_getenv("TIME_EXIT_GRACE_SEC", "60") or 60)),
        exit_dry_run=_getbool("EXIT_DRY_RUN", "0"),
        exit_diagnostics=_getbool("EXIT_DIAGNOSTICS", "0"),

        # Break-even
        breakeven_enabled=_getbool("BREAKEVEN_ENABLED", "0"),
        breakeven_trigger_pct=float(_getenv("BREAKEVEN_TRIGGER_PCT", "0.015") or 0.015),
        breakeven_offset_pct=float(_getenv("BREAKEVEN_OFFSET_PCT", "0.0") or 0.0),

        # Daily risk
        max_daily_loss_usd=float(_getenv("MAX_DAILY_LOSS_USD", "25") or 25),

        # Stop execution quality
        stop_limit_buffer_pct=float(_getenv("STOP_LIMIT_BUFFER_PCT", "0.15") or 0.15),
        stop_limit_timeout_sec=int(float(_getenv("STOP_LIMIT_TIMEOUT_SEC", "60") or 60)),

        # Daily flatten
        enforce_daily_flatten=_getbool("ENFORCE_DAILY_FLATTEN", "1"),
        daily_flatten_time_utc=_getenv("DAILY_FLATTEN_TIME_UTC", "23:55"),
        block_entries_after_flatten=_getbool("BLOCK_ENTRIES_AFTER_FLATTEN", "0"),

        # Runtime
        log_level=_getenv("LOG_LEVEL", "INFO"),
    )
