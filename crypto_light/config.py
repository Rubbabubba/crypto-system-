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
    sizing_mode: str  # 'fixed', 'equity_fraction', 'risk_pct', or 'risk_pct_equity'
    risk_per_trade: float
    equity_fraction_per_trade: float
    max_notional_usd: float  # 0 disables



    # Execution (fees)
    execution_mode: str  # 'market', 'maker_first', or 'limit_aggressive'
    entry_fee_bps: float
    exit_fee_bps: float
    slippage_bps: float
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
    entry_failure_cooldown_bars: int
    entry_engine_timeframe_min: int
    workflow_lock_ttl_sec: int
    no_new_entries_after_utc: str  # HH:MM or "" for 24/7
    max_trades_per_symbol_per_day: int

    # Idempotency / anti-spam
    signal_dedupe_ttl_sec: int
    signal_fingerprint_ttl_sec: int

    # Exits
    stop_pct: float
    take_pct: float
    exit_cooldown_sec: int
    max_hold_sec: int  # 0 disables
    rb1_max_hold_sec: int
    tc1_max_hold_sec: int
    adopted_time_exit_enabled: bool
    adopted_max_hold_sec: int

    # Brackets
    atr_brackets_enabled: bool
    atr_bracket_len: int
    atr_stop_mult: float
    atr_take_mult: float
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

    # Risk admission
    min_effective_stop_pct: float
    max_effective_stop_pct: float
    min_risk_reward_ratio: float
    max_consecutive_rejections: int
    max_consecutive_stopouts: int
    ops_lockout_sec: int

    # Startup self-check / lockout
    startup_self_check_enabled: bool
    startup_apply_reconcile: bool
    startup_lockout_on_critical_reconcile: bool
    startup_lockout_sec: int
    startup_max_orphan_broker_orders: int
    startup_max_orphan_internal_intents: int
    startup_max_stale_pending_exits: int

    # Pre-trade health gate
    pretrade_health_gate_enabled: bool
    pretrade_require_startup_self_check_ok: bool
    pretrade_block_on_worker_stale: bool
    pretrade_block_on_balance_error: bool
    pretrade_block_on_reconcile_anomaly: bool

    # Broker-truth account guardrails
    require_broker_balance_ok_for_entry: bool
    min_cash_buffer_usd: float
    max_account_utilization_pct: float
    auto_reconcile_state_on_entry: bool
    reconcile_cooldown_sec: int

    # Stop execution quality
    stop_limit_buffer_pct: float
    stop_limit_timeout_sec: int

    # Daily flatten behavior
    enforce_daily_flatten: bool
    daily_flatten_time_utc: str  # HH:MM
    block_entries_after_flatten: bool  # if true, blocks buys during flatten window

    # Runtime
    log_level: str

    # Strategy mode / optional strategies
    strategy_mode: str  # 'auto' (default) or 'legacy'
    enable_cr1: bool
    enable_mm1: bool

    # Regime filter (quiet vs expansion)
    regime_filter_enabled: bool
    regime_benchmark_symbol: str
    regime_timeframe: str
    regime_limit_bars: int
    regime_quiet_max_24h_range_pct: float
    regime_quiet_atr_lookback: int
    regime_quiet_atr_now_lt_median_mult: float

    # CR1 (Compression Range Reversion)
    cr1_range_lookback_bars: int
    cr1_bottom_pct: float
    cr1_atr_len: int
    cr1_atr_falling_lookback: int
    cr1_atr_now_lt_median_mult: float
    cr1_stop_atr_mult: float
    cr1_take_atr_mult: float
    cr1_max_hold_sec: int
    cr1_maker_only: bool

    # MM1 (Controlled passive maker capture)
    mm1_spread_min_pct: float
    mm1_spread_max_pct: float
    mm1_take_pct: float
    mm1_stop_pct: float
    mm1_maker_only: bool
    mm1_chase_sec: int
    mm1_post_only_offset_pct: float


def load_settings() -> Settings:
    sizing_mode = (_getenv("SIZING_MODE", "fixed").strip().lower() or "fixed")
    if sizing_mode == "risk_pct":
        sizing_mode = "risk_pct_equity"

    execution_mode = (_getenv("EXECUTION_MODE", "market").strip().lower() or "market")
    if execution_mode == "limit_aggressive":
        execution_mode = "limit_aggressive"

    signal_dedupe_ttl_sec = int(float(_getenv("SIGNAL_DEDUPE_TTL_SEC", "90") or 90))
    signal_fingerprint_ttl_raw = _getenv("SIGNAL_FINGERPRINT_TTL_SEC", "").strip()
    signal_fingerprint_ttl_sec = int(float(signal_fingerprint_ttl_raw or signal_dedupe_ttl_sec or 900))
    signal_dedupe_ttl_sec = max(signal_dedupe_ttl_sec, min(signal_fingerprint_ttl_sec, 900))

    return Settings(
        # Security
        webhook_secret=_getenv("WEBHOOK_SECRET", ""),
        worker_secret=_getenv("WORKER_SECRET", ""),

        # Universe
        allowed_symbols=_csv("ALLOWED_SYMBOLS", "BTC/USD,ETH/USD,SOL/USD,ADA/USD,XRP/USD,DOGE/USD,LINK/USD,AVAX/USD,LTC/USD,DOT/USD"),

        # Core
        trading_enabled=_getbool("TRADING_ENABLED", "1"),
        default_notional_usd=float(_getenv("DEFAULT_NOTIONAL_USD", _getenv("DEFAULT_NOTIONAL", "50")) or 50),
        min_order_notional_usd=float(_getenv("MIN_ORDER_NOTIONAL_USD", "5") or 5),
        exit_min_notional_usd=float(_getenv("EXIT_MIN_NOTIONAL_USD", "6") or 6),
        # Sizing
        sizing_mode=sizing_mode,
        risk_per_trade=float(_getenv("RISK_PER_TRADE", "0.03") or 0.03),
        equity_fraction_per_trade=float(_getenv("EQUITY_FRACTION_PER_TRADE", "0.05") or 0.05),
        max_notional_usd=float(_getenv("MAX_NOTIONAL_USD", "0") or 0),



        # Execution (fees)
        execution_mode=execution_mode,
        entry_fee_bps=float(_getenv("ENTRY_FEE_BPS", "26") or 26),
        exit_fee_bps=float(_getenv("EXIT_FEE_BPS", "26") or 26),
        slippage_bps=float(_getenv("SLIPPAGE_BPS", "8") or 8),
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
        entry_failure_cooldown_bars=int(float(_getenv("ENTRY_FAILURE_COOLDOWN_BARS", "2") or 2)),
        entry_engine_timeframe_min=int(float(_getenv("ENTRY_ENGINE_TIMEFRAME_MIN", "5") or 5)),
        workflow_lock_ttl_sec=int(float(_getenv("WORKFLOW_LOCK_TTL_SEC", "300") or 300)),
        no_new_entries_after_utc=_getenv("NO_NEW_ENTRIES_AFTER_UTC", ""),  # "" = 24/7
        max_trades_per_symbol_per_day=int(float(_getenv("MAX_TRADES_PER_SYMBOL_PER_DAY", "999") or 999)),

        # Idempotency
        signal_dedupe_ttl_sec=signal_dedupe_ttl_sec,
        signal_fingerprint_ttl_sec=signal_fingerprint_ttl_sec,

        # Exits
        stop_pct=float(_getenv("STOP_PCT", "0.01") or 0.01),
        take_pct=float(_getenv("TAKE_PCT", "0.02") or 0.02),
        exit_cooldown_sec=int(float(_getenv("EXIT_COOLDOWN_SEC", "20") or 20)),
        max_hold_sec=int(float(_getenv("MAX_HOLD_SEC", "0") or 0)),
        rb1_max_hold_sec=int(float(_getenv("RB1_MAX_HOLD_SEC", "0") or 0)),
        tc1_max_hold_sec=int(float(_getenv("TC1_MAX_HOLD_SEC", "0") or 0)),
        adopted_time_exit_enabled=_getbool("ADOPTED_TIME_EXIT_ENABLED", "1"),
        adopted_max_hold_sec=int(float(_getenv("ADOPTED_MAX_HOLD_SEC", _getenv("MAX_HOLD_SEC", "7200")) or 7200)),

        atr_brackets_enabled=_getbool("ATR_BRACKETS_ENABLED", "0"),
        atr_bracket_len=int(float(_getenv("ATR_BRACKET_LEN", "14") or 14)),
        atr_stop_mult=float(_getenv("ATR_STOP_MULT", "2.0") or 2.0),
        atr_take_mult=float(_getenv("ATR_TAKE_MULT", "4.0") or 4.0),
        time_exit_grace_sec=int(float(_getenv("TIME_EXIT_GRACE_SEC", "60") or 60)),
        exit_dry_run=_getbool("EXIT_DRY_RUN", "0"),
        exit_diagnostics=_getbool("EXIT_DIAGNOSTICS", "0"),

        # Break-even
        breakeven_enabled=_getbool("BREAKEVEN_ENABLED", "0"),
        breakeven_trigger_pct=float(_getenv("BREAKEVEN_TRIGGER_PCT", "0.015") or 0.015),
        breakeven_offset_pct=float(_getenv("BREAKEVEN_OFFSET_PCT", "0.0") or 0.0),

        # Daily risk
        max_daily_loss_usd=float(_getenv("MAX_DAILY_LOSS_USD", "25") or 25),

        # Risk admission
        min_effective_stop_pct=float(_getenv("MIN_EFFECTIVE_STOP_PCT", "0.003") or 0.003),
        max_effective_stop_pct=float(_getenv("MAX_EFFECTIVE_STOP_PCT", "0.05") or 0.05),
        min_risk_reward_ratio=float(_getenv("MIN_RISK_REWARD_RATIO", "1.2") or 1.2),
        max_consecutive_rejections=int(float(_getenv("MAX_CONSECUTIVE_REJECTIONS", "3") or 3)),
        max_consecutive_stopouts=int(float(_getenv("MAX_CONSECUTIVE_STOPOUTS", "2") or 2)),
        ops_lockout_sec=int(float(_getenv("OPS_LOCKOUT_SEC", "900") or 900)),

        # Startup self-check / lockout
        startup_self_check_enabled=_getbool("STARTUP_SELF_CHECK_ENABLED", "1"),
        startup_apply_reconcile=_getbool("STARTUP_APPLY_RECONCILE", "1"),
        startup_lockout_on_critical_reconcile=_getbool("STARTUP_LOCKOUT_ON_CRITICAL_RECONCILE", "1"),
        startup_lockout_sec=int(float(_getenv("STARTUP_LOCKOUT_SEC", _getenv("OPS_LOCKOUT_SEC", "900")) or 900)),
        startup_max_orphan_broker_orders=int(float(_getenv("STARTUP_MAX_ORPHAN_BROKER_ORDERS", "0") or 0)),
        startup_max_orphan_internal_intents=int(float(_getenv("STARTUP_MAX_ORPHAN_INTERNAL_INTENTS", "0") or 0)),
        startup_max_stale_pending_exits=int(float(_getenv("STARTUP_MAX_STALE_PENDING_EXITS", "0") or 0)),

        # Pre-trade health gate
        pretrade_health_gate_enabled=_getbool("PRETRADE_HEALTH_GATE_ENABLED", "1"),
        pretrade_require_startup_self_check_ok=_getbool("PRETRADE_REQUIRE_STARTUP_SELF_CHECK_OK", "1"),
        pretrade_block_on_worker_stale=_getbool("PRETRADE_BLOCK_ON_WORKER_STALE", "1"),
        pretrade_block_on_balance_error=_getbool("PRETRADE_BLOCK_ON_BALANCE_ERROR", "1"),
        pretrade_block_on_reconcile_anomaly=_getbool("PRETRADE_BLOCK_ON_RECONCILE_ANOMALY", "1"),

        # Broker-truth account guardrails
        require_broker_balance_ok_for_entry=_getbool("REQUIRE_BROKER_BALANCE_OK_FOR_ENTRY", "1"),
        min_cash_buffer_usd=float(_getenv("MIN_CASH_BUFFER_USD", "25") or 25),
        max_account_utilization_pct=float(_getenv("MAX_ACCOUNT_UTILIZATION_PCT", "0.85") or 0.85),
        auto_reconcile_state_on_entry=_getbool("AUTO_RECONCILE_STATE_ON_ENTRY", "1"),
        reconcile_cooldown_sec=int(float(_getenv("RECONCILE_COOLDOWN_SEC", "30") or 30)),

        # Stop execution quality
        stop_limit_buffer_pct=float(_getenv("STOP_LIMIT_BUFFER_PCT", "0.15") or 0.15),
        stop_limit_timeout_sec=int(float(_getenv("STOP_LIMIT_TIMEOUT_SEC", "60") or 60)),

        # Daily flatten
        enforce_daily_flatten=_getbool("ENFORCE_DAILY_FLATTEN", "1"),
        daily_flatten_time_utc=_getenv("DAILY_FLATTEN_TIME_UTC", "23:55"),
        block_entries_after_flatten=_getbool("BLOCK_ENTRIES_AFTER_FLATTEN", "0"),

        # Runtime
        log_level=_getenv("LOG_LEVEL", "INFO"),

        # Strategy mode / optional strategies
        strategy_mode=_getenv("STRATEGY_MODE", "auto").strip().lower() or "auto",
        enable_cr1=_getbool("ENABLE_CR1", "0"),
        enable_mm1=_getbool("ENABLE_MM1", "0"),

        # Regime filter
        regime_filter_enabled=_getbool("REGIME_FILTER_ENABLED", "1"),
        regime_benchmark_symbol=_getenv("REGIME_BENCHMARK_SYMBOL", "BTC/USD").strip().upper() or "BTC/USD",
        regime_timeframe=_getenv("REGIME_TIMEFRAME", "60").strip() or "60",
        regime_limit_bars=int(float(_getenv("REGIME_LIMIT_BARS", "220") or 220)),
        regime_quiet_max_24h_range_pct=float(_getenv("REGIME_QUIET_MAX_24H_RANGE_PCT", "0.04") or 0.04),
        regime_quiet_atr_lookback=int(float(_getenv("REGIME_QUIET_ATR_LOOKBACK", "14") or 14)),
        regime_quiet_atr_now_lt_median_mult=float(_getenv("REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT", "1.0") or 1.0),

        # CR1
        cr1_range_lookback_bars=int(float(_getenv("CR1_RANGE_LOOKBACK_BARS", "288") or 288)),
        cr1_bottom_pct=float(_getenv("CR1_BOTTOM_PCT", "0.20") or 0.20),
        cr1_atr_len=int(float(_getenv("CR1_ATR_LEN", "14") or 14)),
        cr1_atr_falling_lookback=int(float(_getenv("CR1_ATR_FALLING_LOOKBACK", "50") or 50)),
        cr1_atr_now_lt_median_mult=float(_getenv("CR1_ATR_NOW_LT_MEDIAN_MULT", "1.0") or 1.0),
        cr1_stop_atr_mult=float(_getenv("CR1_STOP_ATR_MULT", "1.2") or 1.2),
        cr1_take_atr_mult=float(_getenv("CR1_TAKE_ATR_MULT", "1.5") or 1.5),
        cr1_max_hold_sec=int(float(_getenv("CR1_MAX_HOLD_SEC", "2700") or 2700)),
        cr1_maker_only=_getbool("CR1_MAKER_ONLY", "1"),

        # MM1
        mm1_spread_min_pct=float(_getenv("MM1_SPREAD_MIN_PCT", "0.0015") or 0.0015),
        mm1_spread_max_pct=float(_getenv("MM1_SPREAD_MAX_PCT", "0.0035") or 0.0035),
        mm1_take_pct=float(_getenv("MM1_TAKE_PCT", "0.0025") or 0.0025),
        mm1_stop_pct=float(_getenv("MM1_STOP_PCT", "0.006") or 0.006),
        mm1_maker_only=_getbool("MM1_MAKER_ONLY", "1"),
        mm1_chase_sec=int(float(_getenv("MM1_CHASE_SEC", "12") or 12)),
        mm1_post_only_offset_pct=float(_getenv("MM1_POST_ONLY_OFFSET_PCT", _getenv("POST_ONLY_OFFSET_PCT", "0.0002")) or 0.0002),
    )
