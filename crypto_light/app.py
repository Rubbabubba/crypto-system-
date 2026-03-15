from __future__ import annotations

from . import trades_db
from . import lifecycle_db
from . import broker_kraken
from . import trade_journal
from . import execution_state
from .broker_kraken import trades_history

import logging
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, List, Tuple
from uuid import uuid4

import requests
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse

log = logging.getLogger("crypto_light")

from .broker import balances_by_asset as _balances_by_asset
from .broker import last_balance_error as _last_balance_error
from .broker import base_asset as _base_asset
from .broker import last_price as _last_price
from .broker import market_notional as _market_notional
from .broker import get_bars as _get_bars
from .broker import best_bid_ask as _best_bid_ask
from .sizing import compute_risk_pct_equity_notional, compute_equity_fraction_notional, compute_risk_based_notional_actual
from .config import load_settings
from .models import WebhookPayload, WorkerExitPayload, WorkerScanPayload, WorkerExitDiagnosticsPayload, WorkerAdoptPositionsPayload
from .risk import compute_brackets, compute_atr_brackets, compute_effective_stop_pct, compute_rr_ratio, compute_stop_distance_pct
from .state import InMemoryState, TradePlan
from .symbol_map import normalize_symbol

settings = load_settings()


def _portfolio_exposure_usd_from_balances(balances: dict[str, float]) -> float:
    """Best-effort mark-to-market exposure for non-USD *non-stable* assets.

    We treat USD-like stables as cash elsewhere.
    We price each asset against USD using broker_kraken.last_trade_map().
    Missing prices are ignored (we prefer a scan that runs over a hard fail).
    """
    stables = {'USD', 'USDC', 'USDT'}
    exposure = 0.0
    assets = [a for a in balances.keys() if a not in stables]
    if not assets:
        return 0.0
    pairs = [f"{a}/USD" for a in assets]
    try:
        last_map = broker_kraken.last_trade_map(pairs)
    except Exception:
        return 0.0
    for a in assets:
        qty = float(balances.get(a, 0.0) or 0.0)
        if qty <= 0:
            continue
        px = last_map.get(f"{a}/USD")
        if px is None:
            continue
        try:
            exposure += qty * float(px)
        except Exception:
            continue
    return float(exposure)


def _stable_cash_usd(balances: dict[str, float]) -> float:
    """Return USD-like cash from balances.

    Kraken may report USD cash as USD or ZUSD depending on endpoint/account.
    Some accounts primarily hold stables (USDC/USDT). Treat those as cash-equivalent
    for sizing and eligibility checks.
    """
    keys = ('USD', 'ZUSD', 'USDC', 'ZUSDC', 'USDT', 'ZUSDT')
    total = 0.0
    for k in keys:
        try:
            total += float(balances.get(k, 0.0) or 0.0)
        except Exception:
            continue
    return float(total)

# Keep a normalized set for fast membership checks.
ALLOWED_SYMBOLS = set(getattr(settings, 'allowed_symbols', []) or [])

state = InMemoryState()
app = FastAPI(title="Crypto Light", version="1.0.4")

try:
    lifecycle_db.ensure_schema()
except Exception:
    pass

STARTUP_SELF_CHECK_RESULT: dict[str, Any] = {}
STARTUP_SELF_CHECK_TS: float = 0.0


def _startup_self_check(*, rerun: bool = False, apply: bool | None = None) -> dict:
    global STARTUP_SELF_CHECK_RESULT, STARTUP_SELF_CHECK_TS
    if apply is None:
        apply = bool(getattr(settings, 'startup_apply_reconcile', True))
    if (not rerun) and STARTUP_SELF_CHECK_RESULT:
        return dict(STARTUP_SELF_CHECK_RESULT)

    enabled = bool(getattr(settings, 'startup_self_check_enabled', True))
    if not enabled:
        STARTUP_SELF_CHECK_TS = time.time()
        STARTUP_SELF_CHECK_RESULT = {
            'ok': True,
            'utc': utc_now_iso(),
            'enabled': False,
            'apply_changes': False,
            'critical': False,
            'critical_reasons': [],
            'lockout_applied': False,
            'lockout_reason': '',
            'recovery_reconcile': None,
        }
        return dict(STARTUP_SELF_CHECK_RESULT)

    try:
        recovery = _recovery_reconcile_summary(apply=bool(apply))
        critical_reasons: list[str] = []
        if not bool(recovery.get('ok')):
            critical_reasons.append('recovery_reconcile_failed')
        if not bool(recovery.get('broker_open_orders_ok', True)):
            critical_reasons.append('broker_open_orders_unavailable')
        if int(recovery.get('orphan_broker_order_count', 0) or 0) > int(getattr(settings, 'startup_max_orphan_broker_orders', 0) or 0):
            critical_reasons.append('orphan_broker_orders_present')
        if int(recovery.get('orphan_internal_intent_count', 0) or 0) > int(getattr(settings, 'startup_max_orphan_internal_intents', 0) or 0):
            critical_reasons.append('orphan_internal_intents_present')
        if int(recovery.get('stale_pending_exit_count', 0) or 0) > int(getattr(settings, 'startup_max_stale_pending_exits', 0) or 0):
            critical_reasons.append('stale_pending_exits_present')

        lockout_applied = False
        lockout_reason = ''
        if critical_reasons and bool(getattr(settings, 'startup_lockout_on_critical_reconcile', True)) and hasattr(state, 'set_ops_lockout'):
            lockout_reason = 'startup_' + critical_reasons[0]
            state.set_ops_lockout(lockout_reason, int(getattr(settings, 'startup_lockout_sec', 0) or 0))
            lockout_applied = True
            try:
                lifecycle_db.record_anomaly('startup_self_check_lockout', 'error', details={
                    'critical_reasons': critical_reasons,
                    'recovery_reconcile': recovery,
                })
            except Exception:
                pass

        STARTUP_SELF_CHECK_TS = time.time()
        STARTUP_SELF_CHECK_RESULT = {
            'ok': len(critical_reasons) == 0,
            'utc': utc_now_iso(),
            'enabled': True,
            'apply_changes': bool(apply),
            'critical': len(critical_reasons) > 0,
            'critical_reasons': critical_reasons,
            'lockout_applied': lockout_applied,
            'lockout_reason': lockout_reason,
            'lockout_remaining_sec': int(state.ops_lockout_remaining_sec() if hasattr(state, 'ops_lockout_remaining_sec') else 0),
            'recovery_reconcile': recovery,
        }
    except Exception as e:
        lockout_applied = False
        lockout_reason = ''
        if bool(getattr(settings, 'startup_lockout_on_critical_reconcile', True)) and hasattr(state, 'set_ops_lockout'):
            lockout_reason = 'startup_self_check_exception'
            state.set_ops_lockout(lockout_reason, int(getattr(settings, 'startup_lockout_sec', 0) or 0))
            lockout_applied = True
        STARTUP_SELF_CHECK_TS = time.time()
        STARTUP_SELF_CHECK_RESULT = {
            'ok': False,
            'utc': utc_now_iso(),
            'enabled': True,
            'apply_changes': bool(apply),
            'critical': True,
            'critical_reasons': ['startup_self_check_exception'],
            'lockout_applied': lockout_applied,
            'lockout_reason': lockout_reason,
            'lockout_remaining_sec': int(state.ops_lockout_remaining_sec() if hasattr(state, 'ops_lockout_remaining_sec') else 0),
            'error': str(e),
            'recovery_reconcile': None,
        }
    return dict(STARTUP_SELF_CHECK_RESULT)


@app.on_event("startup")
def _run_startup_self_check():
    _startup_self_check(rerun=True)


@app.middleware("http")
async def _normalize_path_slashes(request: Request, call_next):
    # Render / some clients occasionally send paths with double slashes (//trades/refresh).
    # FastAPI treats that as a different path, so we 307-redirect to the normalized path.
    path = request.scope.get('path', '')
    if '//' in path:
        while '//' in path:
            path = path.replace('//', '/')
        if path and not path.startswith('/'):
            path = '/' + path
        # Preserve query string
        qs = request.scope.get('query_string', b'')
        url = path
        if qs:
            url = url + '?' + qs.decode('utf-8', errors='ignore')
        return RedirectResponse(url=url, status_code=307)
    return await call_next(request)


def _require_worker_secret(provided: str | None) -> tuple[bool, str | None]:
    """Validate worker secret for /worker/* endpoints.

    Behavior:
    - If WORKER_SECRET is not configured (empty), allow requests.
    - If configured, require the caller to pass the same secret.
    """
    expected = (settings.worker_secret or "").strip()
    if not expected:
        return True, None

    provided = (provided or "").strip()
    if not provided:
        return False, "missing worker_secret"
    if provided != expected:
        return False, "invalid worker_secret"
    return True, None


def _record_blocked_trade(reason: str, **fields) -> None:
    try:
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "reason": str(reason or "unknown"),
            **(fields or {}),
        }
        state.record_blocked_trade(evt)
    except Exception:
        pass



def _risk_snapshot_for_entry(*, symbol: str, strategy: str, px: float, stop_price: float, take_price: float, notional: float) -> Dict[str, Any]:
    balances = _balances_by_asset() or {}
    stable_cash = _stable_cash_usd(balances)
    total_exposure = _portfolio_exposure_usd_from_balances(balances)
    stop_gap = max(0.0, float(px) - float(stop_price))
    stop_gap_pct = compute_stop_distance_pct(float(px), float(stop_price))
    rr_ratio = compute_rr_ratio(float(px), float(stop_price), float(take_price))
    effective_stop_pct = compute_effective_stop_pct(
        entry_price=float(px),
        stop_price=float(stop_price),
        entry_fee_bps=float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
        exit_fee_bps=float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
        slippage_bps=float(getattr(settings, "slippage_bps", 0.0) or 0.0),
    )
    est_qty = (float(notional) / float(px)) if float(px) > 0 else 0.0
    est_risk_usd = float(notional) * float(effective_stop_pct)
    return {
        "symbol": symbol,
        "strategy": strategy,
        "entry_price": float(px),
        "stop_price": float(stop_price),
        "take_price": float(take_price),
        "requested_notional_usd": float(notional),
        "estimated_qty": float(est_qty),
        "estimated_stop_gap_usd": float(stop_gap),
        "estimated_stop_gap_pct": float(stop_gap_pct),
        "effective_stop_pct": float(effective_stop_pct),
        "risk_reward_ratio": float(rr_ratio),
        "estimated_stop_risk_usd": float(est_risk_usd),
        "stable_cash_usd": float(stable_cash),
        "portfolio_exposure_usd": float(total_exposure),
        "max_daily_loss_usd": float(getattr(settings, "max_daily_loss_usd", 0.0) or 0.0),
        "entry_fee_bps": float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
        "exit_fee_bps": float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
        "slippage_bps": float(getattr(settings, "slippage_bps", 0.0) or 0.0),
    }


def _state_model_summary() -> Dict[str, Any]:
    try:
        return lifecycle_db.summary()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _execution_state_summary() -> Dict[str, Any]:
    try:
        return lifecycle_db.execution_state_summary(limit=200)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _record_state_model_anomaly(kind: str, severity: str = "warn", **fields: Any) -> None:
    try:
        lifecycle_db.record_anomaly(kind, severity, symbol=fields.get("symbol"), trade_plan_id=fields.get("trade_plan_id"), intent_id=fields.get("intent_id"), details=fields)
    except Exception:
        pass


def _ops_risk_snapshot() -> Dict[str, Any]:
    return {
        "consecutive_entry_rejections": int(getattr(state, "consecutive_entry_rejections", 0) or 0),
        "consecutive_stopouts": int(getattr(state, "consecutive_stopouts", 0) or 0),
        "ops_lockout_remaining_sec": int(state.ops_lockout_remaining_sec() if hasattr(state, "ops_lockout_remaining_sec") else 0),
        "ops_lockout_reason": str(getattr(state, "last_ops_lock_reason", "") or ""),
        "max_consecutive_rejections": int(getattr(settings, "max_consecutive_rejections", 0) or 0),
        "max_consecutive_stopouts": int(getattr(settings, "max_consecutive_stopouts", 0) or 0),
        "ops_lockout_sec": int(getattr(settings, "ops_lockout_sec", 0) or 0),
    }


def _risk_admission_check(*, symbol: str, strategy: str, px: float, stop_price: float, take_price: float, account_truth: Dict[str, Any]) -> Dict[str, Any]:
    effective_stop_pct = compute_effective_stop_pct(
        entry_price=float(px),
        stop_price=float(stop_price),
        entry_fee_bps=float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
        exit_fee_bps=float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
        slippage_bps=float(getattr(settings, "slippage_bps", 0.0) or 0.0),
    )
    raw_stop_pct = compute_stop_distance_pct(float(px), float(stop_price))
    rr_ratio = compute_rr_ratio(float(px), float(stop_price), float(take_price))
    min_stop = float(getattr(settings, "min_effective_stop_pct", 0.0) or 0.0)
    max_stop = float(getattr(settings, "max_effective_stop_pct", 0.0) or 0.0)
    min_rr = float(getattr(settings, "min_risk_reward_ratio", 0.0) or 0.0)
    checks = {
        "effective_stop_pct_min": (effective_stop_pct >= min_stop) if min_stop > 0 else True,
        "effective_stop_pct_max": (effective_stop_pct <= max_stop) if max_stop > 0 else True,
        "min_risk_reward_ratio": (rr_ratio >= min_rr) if min_rr > 0 else True,
    }
    violations = []
    if not checks["effective_stop_pct_min"]:
        violations.append("effective_stop_pct_too_tight")
    if not checks["effective_stop_pct_max"]:
        violations.append("effective_stop_pct_too_wide")
    if not checks["min_risk_reward_ratio"]:
        violations.append("risk_reward_too_low")
    return {
        "ok": not violations,
        "symbol": symbol,
        "strategy": strategy,
        "entry_price": float(px),
        "stop_price": float(stop_price),
        "take_price": float(take_price),
        "raw_stop_pct": float(raw_stop_pct),
        "effective_stop_pct": float(effective_stop_pct),
        "risk_reward_ratio": float(rr_ratio),
        "min_effective_stop_pct": float(min_stop),
        "max_effective_stop_pct": float(max_stop),
        "min_risk_reward_ratio": float(min_rr),
        "entry_fee_bps": float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
        "exit_fee_bps": float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
        "slippage_bps": float(getattr(settings, "slippage_bps", 0.0) or 0.0),
        "equity_usd": float(account_truth.get("equity_usd", 0.0) or 0.0),
        "cash_usd": float(account_truth.get("cash_usd", 0.0) or 0.0),
        "checks": checks,
        "violations": violations,
    }


# ---------- Entry engine (optional; replaces TradingView) ----------
ENTRY_ENGINE_ENABLED = (os.getenv("ENTRY_ENGINE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on"))
ENTRY_ENGINE_STRATEGIES_LIST = [s.strip().lower() for s in os.getenv("ENTRY_ENGINE_STRATEGIES", "rb1,tc1").split(",") if s.strip()]
ENTRY_ENGINE_STRATEGIES = set(ENTRY_ENGINE_STRATEGIES_LIST)
ENTRY_ENGINE_TIMEFRAME = os.getenv("ENTRY_ENGINE_TIMEFRAME", "5Min").strip() or "5Min"   # must match broker get_bars
ENTRY_ENGINE_LIMIT_BARS = int(float(os.getenv("ENTRY_ENGINE_LIMIT_BARS", "300") or 300))

# Strategy enablement / mode
# fixed: only consider strategies listed in ENTRY_ENGINE_STRATEGIES, preserving env order
# auto:  preserve legacy preference ordering across eligible strategies
# legacy: rb1/tc1 only, old preference behavior
STRATEGY_MODE = (os.getenv("STRATEGY_MODE", "auto") or "auto").strip().lower()  # fixed|auto|legacy
ENABLE_RB1 = (os.getenv("ENABLE_RB1", "1").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_TC1 = (os.getenv("ENABLE_TC1", "1").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_CR1 = (os.getenv("ENABLE_CR1", "0").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_MM1 = (os.getenv("ENABLE_MM1", "0").strip().lower() in ("1", "true", "yes", "on"))
_ALLOWED_STRATEGY_NAMES = ("rb1", "tc1", "cr1", "mm1")
if not ENTRY_ENGINE_STRATEGIES_LIST:
    ENTRY_ENGINE_STRATEGIES_LIST = ["rb1", "tc1"]
ENTRY_ENGINE_STRATEGIES_LIST = [s for s in ENTRY_ENGINE_STRATEGIES_LIST if s in _ALLOWED_STRATEGY_NAMES]
ENTRY_ENGINE_STRATEGIES = set(ENTRY_ENGINE_STRATEGIES_LIST)

# Regime filter (benchmark is typically BTC/USD)
REGIME_FILTER_ENABLED = (os.getenv("REGIME_FILTER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on"))
REGIME_BENCHMARK_SYMBOL = (os.getenv("REGIME_BENCHMARK_SYMBOL", "BTC/USD") or "BTC/USD").strip().upper()
REGIME_TIMEFRAME = (os.getenv("REGIME_TIMEFRAME", "60") or "60").strip()
REGIME_LIMIT_BARS = int(float(os.getenv("REGIME_LIMIT_BARS", "220") or 220))
REGIME_QUIET_MAX_24H_RANGE_PCT = float(os.getenv("REGIME_QUIET_MAX_24H_RANGE_PCT", "0.04") or 0.04)
REGIME_QUIET_ATR_LOOKBACK = int(float(os.getenv("REGIME_QUIET_ATR_LOOKBACK", "14") or 14))
REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT = float(os.getenv("REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT", "1.0") or 1.0)

# CR1 params (5m)
CR1_RANGE_LOOKBACK_BARS = int(float(os.getenv("CR1_RANGE_LOOKBACK_BARS", "288") or 288))  # ~24h of 5m
CR1_BOTTOM_PCT = float(os.getenv("CR1_BOTTOM_PCT", "0.20") or 0.20)
CR1_ATR_LEN = int(float(os.getenv("CR1_ATR_LEN", "14") or 14))
CR1_ATR_FALLING_LOOKBACK = int(float(os.getenv("CR1_ATR_FALLING_LOOKBACK", "50") or 50))
CR1_ATR_NOW_LT_MEDIAN_MULT = float(os.getenv("CR1_ATR_NOW_LT_MEDIAN_MULT", "1.0") or 1.0)
CR1_STOP_ATR_MULT = float(os.getenv("CR1_STOP_ATR_MULT", "1.2") or 1.2)
CR1_TAKE_ATR_MULT = float(os.getenv("CR1_TAKE_ATR_MULT", "1.5") or 1.5)
CR1_MAX_HOLD_SEC = int(float(os.getenv("CR1_MAX_HOLD_SEC", "2700") or 2700))
CR1_MAKER_ONLY = (os.getenv("CR1_MAKER_ONLY", "1").strip().lower() in ("1", "true", "yes", "on"))

# MM1 params
MM1_SPREAD_MIN_PCT = float(os.getenv("MM1_SPREAD_MIN_PCT", "0.0015") or 0.0015)
MM1_SPREAD_MAX_PCT = float(os.getenv("MM1_SPREAD_MAX_PCT", "0.0035") or 0.0035)
MM1_TAKE_PCT = float(os.getenv("MM1_TAKE_PCT", "0.0025") or 0.0025)
MM1_STOP_PCT = float(os.getenv("MM1_STOP_PCT", "0.006") or 0.006)
MM1_MAKER_ONLY = (os.getenv("MM1_MAKER_ONLY", "1").strip().lower() in ("1", "true", "yes", "on"))
MM1_CHASE_SEC = int(float(os.getenv("MM1_CHASE_SEC", "12") or 12))
MM1_POST_ONLY_OFFSET_PCT = float(os.getenv("MM1_POST_ONLY_OFFSET_PCT", os.getenv("POST_ONLY_OFFSET_PCT", "0.0002")) or 0.0002)
MM1_MAX_HOLD_SEC = int(float(os.getenv("MM1_MAX_HOLD_SEC", "1800") or 1800))

# RB1 params (5m)
RB1_LOOKBACK_BARS = int(float(os.getenv("RB1_LOOKBACK_BARS", "48") or 48))
RB1_BREAKOUT_BUFFER_PCT = float(os.getenv("RB1_BREAKOUT_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%

# RB1 near-breakout params (optional)
# If RB1_NEAR_BREAKOUT_PCT > 0, we will also fire when price is within this percent below breakout level
# and momentum is positive (close > prev_close) unless RB1_NEAR_REQUIRE_UP=0.
RB1_NEAR_BREAKOUT_PCT = float(os.getenv("RB1_NEAR_BREAKOUT_PCT", "0") or 0)  # e.g. 0.0015 = 0.15%
RB1_NEAR_REQUIRE_UP = int(float(os.getenv("RB1_NEAR_REQUIRE_UP", "1") or 1)) == 1
RB1_NEAR_UP_MODE = (os.getenv("RB1_NEAR_UP_MODE", "gt") or "gt").strip().lower()  # gt (default) or ge

# TC1 params
TC1_LTF_EMA = int(float(os.getenv("TC1_LTF_EMA", "20") or 20))
TC1_HTF_TIMEFRAME = os.getenv("TC1_HTF_TIMEFRAME", "60Min").strip() or "60Min"
TC1_HTF_LIMIT_BARS = int(float(os.getenv("TC1_HTF_LIMIT_BARS", "300") or 300))
TC1_HTF_FAST = int(float(os.getenv("TC1_HTF_FAST", "50") or 50))
TC1_HTF_SLOW = int(float(os.getenv("TC1_HTF_SLOW", "200") or 200))
TC1_RECLAIM_BUFFER_PCT = float(os.getenv("TC1_RECLAIM_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%

# TC1 trend-soften epsilon (optional)
# If >0, allow HTF uptrend when EMA_fast is within epsilon below EMA_slow (still requires EMA_fast rising).
TC1_TREND_SOFTEN_EPSILON = float(os.getenv("TC1_TREND_SOFTEN_EPSILON", "0") or 0)  # e.g. 0.002 = 0.2%


# ---------- Scanner config (soft allow) ----------
SCANNER_URL = os.getenv("SCANNER_URL", "").strip()
SCANNER_REFRESH_SEC = int(float(os.getenv("SCANNER_REFRESH_SEC", "300") or 300))
SCANNER_SOFT_ALLOW = (os.getenv("SCANNER_SOFT_ALLOW", "1").strip().lower() in ("1", "true", "yes", "on"))
SCANNER_TIMEOUT_SEC = float(os.getenv("SCANNER_TIMEOUT_SEC", "10") or 10)

# ---------- Universe normalization / safety constraints ----------
SCANNER_DRIVEN_UNIVERSE = (os.getenv('SCANNER_DRIVEN_UNIVERSE', '1').strip().lower() in ('1','true','yes','on'))
SCANNER_TARGET_N = int(float(os.getenv('SCANNER_TARGET_N', os.getenv('TOP_N', '5')) or 5))
UNIVERSE_USD_ONLY = (os.getenv('UNIVERSE_USD_ONLY', '0').strip().lower() in ('1','true','yes','on'))
UNIVERSE_PREFER_USD_FOR_STABLES = (os.getenv('UNIVERSE_PREFER_USD_FOR_STABLES', '0').strip().lower() in ('1','true','yes','on'))
FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS = (os.getenv('FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS', '0').strip().lower() in ('1','true','yes','on'))

MAX_OPEN_POSITIONS = int(float(os.getenv('MAX_OPEN_POSITIONS', '2') or 2))
MAX_ENTRIES_PER_SCAN = int(float(os.getenv('MAX_ENTRIES_PER_SCAN', '1') or 1))
MAX_ENTRIES_PER_DAY = int(float(os.getenv('MAX_ENTRIES_PER_DAY', '5') or 5))

ENABLE_CANDIDATE_RANKING = (os.getenv('ENABLE_CANDIDATE_RANKING', '1').strip().lower() in ('1','true','yes','on'))
MAX_SPREAD_PCT = float(os.getenv('MAX_SPREAD_PCT', '0.004') or 0.004)  # 0 disables
RANK_ATR_LEN = int(float(os.getenv('RANK_ATR_LEN', '14') or 14))
RANK_VOL_AVG_LEN = int(float(os.getenv('RANK_VOL_AVG_LEN', '20') or 20))

ENABLE_STALE_BAR_GUARD = (os.getenv('ENABLE_STALE_BAR_GUARD', '1').strip().lower() in ('1','true','yes','on'))
BAR_STALE_MULTIPLIER = float(os.getenv('BAR_STALE_MULTIPLIER', '1.8') or 1.8)
BAR_STALE_EXTRA_SEC = int(float(os.getenv('BAR_STALE_EXTRA_SEC', '15') or 15))
ORDER_RECONCILE_TIMEOUT_SEC = int(float(os.getenv('ORDER_RECONCILE_TIMEOUT_SEC', '8') or 8))
STOP_EXIT_MODE = (os.getenv('STOP_EXIT_MODE', 'stop_limit').strip().lower() or 'stop_limit')
ENABLE_BROKER_OPEN_ORDER_GUARD = (os.getenv('ENABLE_BROKER_OPEN_ORDER_GUARD', '1').strip().lower() in ('1','true','yes','on'))
OPEN_ORDER_LOCK_TTL_SEC = int(float(os.getenv('OPEN_ORDER_LOCK_TTL_SEC', '45') or 45))
WORKER_STALE_AFTER_SEC = int(float(os.getenv('WORKER_STALE_AFTER_SEC', '600') or 600))

_SCANNER_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "active_symbols": set(),   # type: Set[str]
    "ok": False,
    "last_error": None,
    "raw": None,
}


def _log_event(level: str, event: Dict[str, Any]) -> None:
    try:
        state.record_event(event)
    except Exception:
        pass
    try:
        print(json.dumps(event, default=str))
    except Exception:
        print(str(event))





def get_positions() -> list[dict]:
    """Return open positions derived from Kraken balances + live USD prices.

    - Uses normalized balances from `balances_by_asset()` (e.g., "BTC", "ADA").
    - Prices are fetched per-symbol via `last_price("ASSET/USD")`.
    - Ignores *dust* positions below `settings.min_position_notional_usd` to prevent
      repeated exit/entry churn around exchange minimums.

    Return format:
      [{"symbol": "BTC/USD", "asset": "BTC", "qty": 0.01, "price": 65000, "notional_usd": 650}]
    """
    try:
        balances = _balances_by_asset() or {}
    except Exception as e:
        log.exception("balances_by_asset failed: %s", e)
        return []

    positions: list[dict] = []
    for asset, qty in balances.items():
        asset_u = str(asset).upper().strip()
        if asset_u == "USD":
            continue

        try:
            qty_f = float(qty or 0.0)
        except Exception:
            continue
        if qty_f <= 0:
            continue

        sym = f"{asset_u}/USD"
        try:
            px = float(_last_price(sym) or 0.0)
        except Exception:
            px = 0.0
        if px <= 0:
            continue

        notional = qty_f * px
        if notional < float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0):
            continue

        positions.append(
            {
                "symbol": sym,
                "asset": asset_u,
                "qty": qty_f,
                "price": px,
                "notional_usd": notional,
            }
        )

    positions.sort(key=lambda x: float(x.get("notional_usd", 0.0) or 0.0), reverse=True)
    return positions

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_date_str(now: Optional[datetime] = None) -> str:
    n = now or datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%d")

def _timeframe_seconds(tf: str) -> int:
    raw = str(tf or '').strip().lower()
    if raw in ('1', '1m', '1min', '1minute'):
        return 60
    if raw in ('5', '5m', '5min', '5minute'):
        return 300
    if raw in ('15', '15m', '15min', '15minute'):
        return 900
    if raw in ('30', '30m', '30min', '30minute'):
        return 1800
    if raw in ('60', '60m', '60min', '1h', '1hr', '60minute'):
        return 3600
    return 300


def _bars_freshness_meta(bars: list[dict], timeframe: str) -> dict:
    if not bars:
        return {"ok": False, "reason": "no_bars", "timeframe": timeframe}
    try:
        last_ts = int(float(bars[-1].get('t') or 0))
    except Exception:
        last_ts = 0
    now_ts = int(time.time())
    tf_sec = _timeframe_seconds(timeframe)
    max_age = int(tf_sec * float(BAR_STALE_MULTIPLIER)) + int(BAR_STALE_EXTRA_SEC)
    age = (now_ts - last_ts) if last_ts > 0 else None
    ok = bool(last_ts > 0 and age is not None and age <= max_age)
    return {"ok": ok, "timeframe": timeframe, "last_bar_ts": last_ts, "age_sec": age, "max_age_sec": max_age}


def _guard_fresh_bars(bars: list[dict], timeframe: str) -> tuple[bool, dict]:
    meta = _bars_freshness_meta(bars, timeframe)
    if not ENABLE_STALE_BAR_GUARD:
        meta['guard_enabled'] = False
        return True, meta
    meta['guard_enabled'] = True
    if meta.get('ok'):
        return True, meta
    meta['reason'] = 'stale_bars' if bars else 'no_bars'
    return False, meta


def _is_after_utc_hhmm(now: datetime, hhmm_utc: str) -> bool:
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    return (now.hour, now.minute) >= (h, m)


def _ema(values: list[float], period: int) -> list[float]:
    """Simple EMA series; returns list same length as values."""
    if period <= 1 or not values:
        return values[:]
    k = 2.0 / (period + 1.0)
    out: list[float] = []
    ema = float(values[0])
    out.append(ema)
    for v in values[1:]:
        ema = (float(v) * k) + (ema * (1.0 - k))
        out.append(ema)
    return out


def _atr_from_bars(bars: list[dict], length: int = 14) -> tuple[float | None, list[float]]:
    """Return (atr_now, atr_series) using simple moving average of True Range."""
    if not bars or len(bars) < (length + 2):
        return None, []
    trs: list[float] = []
    prev_c = float(bars[0].get("c") or 0.0)
    for b in bars[1:]:
        h = float(b.get("h") or 0.0)
        l = float(b.get("l") or 0.0)
        c = float(b.get("c") or 0.0)
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(float(tr))
        prev_c = c
    if len(trs) < length:
        return None, []
    atrs: list[float] = []
    for i in range(length - 1, len(trs)):
        window = trs[i - (length - 1) : i + 1]
        atrs.append(sum(window) / float(length))
    return (float(atrs[-1]) if atrs else None), atrs


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(float(x) for x in xs)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


_regime_cache: dict[str, tuple[float, dict]] = {"quiet": (0.0, {})}


def _rank_candidate(symbol: str, strategy: str, sig_debug: dict) -> dict:
    """Compute a simple quality score for candidate prioritization (higher is better)."""
    out = {"score": 0.0, "components": {}}
    try:
        bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(120, RANK_VOL_AVG_LEN + RANK_ATR_LEN + 5))
    except Exception:
        bars = []
    if not bars or len(bars) < (RANK_VOL_AVG_LEN + 2):
        return out
    fresh_ok, fresh_meta = _guard_fresh_bars(bars, ENTRY_ENGINE_TIMEFRAME)
    if not fresh_ok:
        out["components"] = {**out.get("components", {}), "stale_guard": fresh_meta}
        return out

    # Volume ratio (current / avg)
    vols = [float(b.get("v") or 0.0) for b in bars]
    v_now = float(vols[-1] or 0.0)
    v_avg = float(sum(vols[-RANK_VOL_AVG_LEN:]) / float(RANK_VOL_AVG_LEN)) if RANK_VOL_AVG_LEN > 0 else 0.0
    vol_ratio = (v_now / v_avg) if v_avg > 0 else 0.0

    # ATR (not directly scored heavily yet, but recorded)
    atr_now, _ = _atr_from_bars(bars, length=int(RANK_ATR_LEN or 14))

    score = 0.0
    components: dict = {
        "vol_ratio": vol_ratio,
        "atr_now": atr_now,
    }

    strat = (strategy or "").lower().strip()
    if strat == "rb1":
        meta = (sig_debug or {}).get("rb1") or {}
        dist = meta.get("dist_to_level_pct")
        near_pct = float(meta.get("near_pct") or 0.0)
        prev_close = float(meta.get("prev_close") or 0.0)
        close = float(meta.get("close") or 0.0)
        momentum = ((close - prev_close) / prev_close) if prev_close > 0 else 0.0

        # proximity: within near window gets credit; closer to level is better
        prox = 0.0
        if isinstance(dist, (int, float)) and near_pct and near_pct > 0:
            prox = max(0.0, 1.0 - (float(dist) / float(near_pct)))
        score += 2.0 * prox

        # volume expansion above avg
        score += max(0.0, vol_ratio - 1.0)

        # gentle momentum bonus
        score += max(0.0, momentum) * 0.5

        components.update({"prox": prox, "dist_to_level_pct": dist, "near_pct": near_pct, "momentum": momentum})

    elif strat == "tc1":
        meta = (sig_debug or {}).get("tc1") or {}
        close = float(meta.get("close") or 0.0)
        ltf = float(meta.get("ltf_ema") or 0.0)
        edge = ((close - ltf) / ltf) if ltf > 0 else 0.0

        score += max(0.0, edge) * 2.0
        score += max(0.0, vol_ratio - 1.0)
        components.update({"edge": edge})

    else:
        # quiet-regime strategies may have their own filters; just lightly prefer better volume
        score += max(0.0, vol_ratio - 1.0)

    out["score"] = float(score)
    out["components"] = components
    return out



def _regime_is_quiet() -> tuple[bool, dict]:
    """Best-effort regime classifier.

    Quiet means: benchmark 24h range is modest AND ATR is not expanding.
    Cached briefly to avoid hammering OHLC.
    """
    if not REGIME_FILTER_ENABLED:
        return True, {"enabled": False}

    now = time.time()
    ttl = 60.0
    cached_ts, cached_meta = _regime_cache.get("quiet", (0.0, {}))
    if (now - cached_ts) < ttl and cached_meta:
        return bool(cached_meta.get("quiet")), cached_meta

    bars = _get_bars(REGIME_BENCHMARK_SYMBOL, timeframe=REGIME_TIMEFRAME, limit=max(120, REGIME_LIMIT_BARS))
    if not bars or len(bars) < 50:
        meta = {"quiet": True, "reason": "insufficient_bars", "enabled": True}
        _regime_cache["quiet"] = (now, meta)
        return True, meta
    fresh_ok, fresh_meta = _guard_fresh_bars(bars, REGIME_TIMEFRAME)
    if not fresh_ok:
        meta = {"quiet": True, "reason": "stale_bars", "enabled": True, "freshness": fresh_meta}
        _regime_cache["quiet"] = (now, meta)
        return True, meta

    closes = [float(b.get("c") or 0.0) for b in bars]
    highs = [float(b.get("h") or 0.0) for b in bars]
    lows = [float(b.get("l") or 0.0) for b in bars]
    last = float(closes[-1]) if closes else 0.0

    n24 = 24 if REGIME_TIMEFRAME in ("60", "1H", "1h", "1HR") else min(len(bars), 288)
    recent_high = max(highs[-n24:])
    recent_low = min(lows[-n24:])
    range_pct = (recent_high - recent_low) / last if last > 0 else 0.0

    atr_now, atr_series = _atr_from_bars(bars, length=int(REGIME_QUIET_ATR_LOOKBACK))
    med = _median(atr_series[-60:]) if atr_series else None
    atr_ok = True
    if atr_now is not None and med is not None and med > 0:
        atr_ok = float(atr_now) <= float(med) * float(REGIME_QUIET_ATR_NOW_LT_MEDIAN_MULT)

    quiet = (float(range_pct) <= float(REGIME_QUIET_MAX_24H_RANGE_PCT)) and bool(atr_ok)
    meta = {
        "enabled": True,
        "quiet": bool(quiet),
        "benchmark": REGIME_BENCHMARK_SYMBOL,
        "timeframe": REGIME_TIMEFRAME,
        "range_24h_pct": float(range_pct),
        "atr_now": float(atr_now) if atr_now is not None else None,
        "atr_median": float(med) if med is not None else None,
        "atr_ok": bool(atr_ok),
        "range_ok": bool(float(range_pct) <= float(REGIME_QUIET_MAX_24H_RANGE_PCT)),
    }
    _regime_cache["quiet"] = (now, meta)
    return bool(quiet), meta


def _signal_cr1(symbol: str) -> tuple[bool, dict]:
    bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, CR1_RANGE_LOOKBACK_BARS + CR1_ATR_LEN + 10))
    if not bars or len(bars) < (CR1_ATR_LEN + 10):
        return False, {"reason": "insufficient_bars", "bars": len(bars) if bars else 0}
    fresh_ok, fresh_meta = _guard_fresh_bars(bars, ENTRY_ENGINE_TIMEFRAME)
    if not fresh_ok:
        return False, {"reason": "stale_bars", "bars": len(bars), "freshness": fresh_meta}

    closes = [float(b.get("c") or 0.0) for b in bars]
    highs = [float(b.get("h") or 0.0) for b in bars]
    lows = [float(b.get("l") or 0.0) for b in bars]
    cur_close = float(closes[-1])
    prev_close = float(closes[-2])

    look = min(len(bars) - 1, int(CR1_RANGE_LOOKBACK_BARS))
    recent_high = max(highs[-look:])
    recent_low = min(lows[-look:])
    rng = max(1e-12, recent_high - recent_low)
    pos = (cur_close - recent_low) / rng

    atr_now, atr_series = _atr_from_bars(bars, length=int(CR1_ATR_LEN))
    med = _median(atr_series[-int(CR1_ATR_FALLING_LOOKBACK):]) if atr_series else None
    atr_ok = True
    if atr_now is not None and med is not None and med > 0:
        atr_ok = float(atr_now) <= float(med) * float(CR1_ATR_NOW_LT_MEDIAN_MULT)

    bottom_ok = float(pos) <= float(CR1_BOTTOM_PCT)
    bounce_ok = cur_close > prev_close

    ok = bool(bottom_ok and atr_ok and bounce_ok)
    dbg = {
        "range_high": float(recent_high),
        "range_low": float(recent_low),
        "pos_in_range": float(pos),
        "bottom_pct": float(CR1_BOTTOM_PCT),
        "atr_now": float(atr_now) if atr_now is not None else None,
        "atr_median": float(med) if med is not None else None,
        "atr_ok": bool(atr_ok),
        "prev_close": float(prev_close),
        "close": float(cur_close),
        "bounce_ok": bool(bounce_ok),
        "bar_ts": int(float(bars[-1].get("t") or 0)),
    }
    return ok, dbg


def _signal_mm1(symbol: str) -> tuple[bool, dict]:
    """Maker-capture trigger (only valid in quiet regime)."""
    try:
        from . import broker_kraken as _bk
        pair = _bk.to_kraken(symbol)
        bid, ask = _bk._best_bid_ask(pair)  # type: ignore[attr-defined]
    except Exception:
        bid, ask = None, None

    if not bid or not ask or float(bid) <= 0 or float(ask) <= 0:
        return False, {"reason": "no_bid_ask"}

    mid = (float(bid) + float(ask)) / 2.0
    spr = (float(ask) - float(bid)) / mid if mid > 0 else 0.0
    # Use settings (env-driven) rather than module constants so deploy-time env changes
    # are always reflected in decisions + diagnostics.
    min_pct = float(getattr(settings, "mm1_spread_min_pct", MM1_SPREAD_MIN_PCT) or MM1_SPREAD_MIN_PCT)
    max_pct = float(getattr(settings, "mm1_spread_max_pct", MM1_SPREAD_MAX_PCT) or MM1_SPREAD_MAX_PCT)
    spr_ok = (float(spr) >= float(min_pct)) and (float(spr) <= float(max_pct))
    dbg = {
        "bid": float(bid),
        "ask": float(ask),
        "mid": float(mid),
        "spread_pct": float(spr),
        "spr_ok": bool(spr_ok),
        "min_spread_pct": float(min_pct),
        "max_spread_pct": float(max_pct),
    }
    return bool(spr_ok), dbg


def _strategy_max_hold_sec(strategy: str) -> int:
    s = (strategy or "").strip().lower()
    if s == "cr1":
        return int(CR1_MAX_HOLD_SEC)
    if s == "mm1":
        return int(MM1_MAX_HOLD_SEC)
    if s == "rb1":
        return int(getattr(settings, "rb1_max_hold_sec", 0) or 0)
    if s == "tc1":
        return int(getattr(settings, "tc1_max_hold_sec", 0) or 0)
    return 0


def _rb1_long_signal(symbol: str) -> tuple[bool, dict]:
    """True breakout: close crosses above prior N-bar high (edge-trigger)."""
    bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, RB1_LOOKBACK_BARS + 5))
    if not bars or len(bars) < (RB1_LOOKBACK_BARS + 3):
        return False, {"reason": "insufficient_bars", "bars": len(bars) if bars else 0}
    fresh_ok, fresh_meta = _guard_fresh_bars(bars, ENTRY_ENGINE_TIMEFRAME)
    if not fresh_ok:
        return False, {"reason": "stale_bars", "bars": len(bars), "freshness": fresh_meta}

    highs = [float(b["h"]) for b in bars]
    closes = [float(b["c"]) for b in bars]
    ts = [int(float(b["t"])) for b in bars]

    # prior range high excludes current bar
    look = highs[-(RB1_LOOKBACK_BARS+1):-1]
    range_high = max(look)
    prev_close = closes[-2]
    cur_close = closes[-1]

    level = range_high * (1.0 + RB1_BREAKOUT_BUFFER_PCT)

    # Primary breakout (edge-trigger)
    breakout = (prev_close <= level) and (cur_close > level)

    # Optional near-breakout: allow early entry when close is within X% of the level.
    near = False
    near_threshold = None
    dist_pct = None
    if RB1_NEAR_BREAKOUT_PCT and RB1_NEAR_BREAKOUT_PCT > 0:
        near_threshold = level * (1.0 - RB1_NEAR_BREAKOUT_PCT)
        dist_pct = (level - cur_close) / level if level else None
        momentum_ok = ((cur_close >= prev_close) if RB1_NEAR_UP_MODE == 'ge' else (cur_close > prev_close)) if RB1_NEAR_REQUIRE_UP else True
        # Still require we haven't already broken out in prior bar to avoid repeated triggers.
        near = (prev_close < level) and (cur_close >= near_threshold) and momentum_ok

    fired = breakout or near

    meta = {
        "range_high": range_high,
        "level": level,
        "prev_close": prev_close,
        "close": cur_close,
        "bar_ts": ts[-1],
        "breakout": breakout,
        "near": near,
        "near_pct": RB1_NEAR_BREAKOUT_PCT,
        "near_threshold": near_threshold,
        "dist_to_level_pct": dist_pct,
        "require_up": RB1_NEAR_REQUIRE_UP,
        "up_mode": RB1_NEAR_UP_MODE,
    }
    return fired, meta


def _tc1_long_signal(symbol: str) -> tuple[bool, dict]:
    """Trend pullback continuation:
    - HTF uptrend: EMA_fast > EMA_slow AND EMA_fast rising
    - LTF reclaim: close crosses back above LTF EMA after being below
    """
    # HTF trend
    htf = _get_bars(symbol, timeframe=TC1_HTF_TIMEFRAME, limit=max(TC1_HTF_LIMIT_BARS, TC1_HTF_SLOW + 5))
    if not htf or len(htf) < (TC1_HTF_SLOW + 3):
        return False, {"reason": "insufficient_htf_bars", "bars": len(htf) if htf else 0}
    htf_fresh_ok, htf_fresh_meta = _guard_fresh_bars(htf, TC1_HTF_TIMEFRAME)
    if not htf_fresh_ok:
        return False, {"reason": "stale_htf_bars", "bars": len(htf), "freshness": htf_fresh_meta}

    htf_closes = [float(b["c"]) for b in htf]
    ema_fast = _ema(htf_closes, TC1_HTF_FAST)
    ema_slow = _ema(htf_closes, TC1_HTF_SLOW)
    raw_uptrend = (ema_fast[-1] > ema_slow[-1]) and (ema_fast[-1] > ema_fast[-2])
    eps = TC1_TREND_SOFTEN_EPSILON if TC1_TREND_SOFTEN_EPSILON and TC1_TREND_SOFTEN_EPSILON > 0 else 0.0
    softened = (ema_fast[-1] >= (ema_slow[-1] * (1.0 - eps))) and (ema_fast[-1] > ema_fast[-2])
    uptrend = softened

    # LTF reclaim
    ltf = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, TC1_LTF_EMA + 10))
    if not ltf or len(ltf) < (TC1_LTF_EMA + 5):
        return False, {"reason": "insufficient_ltf_bars", "bars": len(ltf) if ltf else 0, "uptrend": uptrend}
    ltf_fresh_ok, ltf_fresh_meta = _guard_fresh_bars(ltf, ENTRY_ENGINE_TIMEFRAME)
    if not ltf_fresh_ok:
        return False, {"reason": "stale_ltf_bars", "bars": len(ltf), "uptrend": uptrend, "freshness": ltf_fresh_meta}

    ltf_closes = [float(b["c"]) for b in ltf]
    ltf_ts = [int(float(b["t"])) for b in ltf]
    ltf_ema = _ema(ltf_closes, TC1_LTF_EMA)

    prev_close = ltf_closes[-2]
    cur_close = ltf_closes[-1]
    prev_ema = ltf_ema[-2]
    cur_ema = ltf_ema[-1]

    buffer = cur_ema * TC1_RECLAIM_BUFFER_PCT
    fired = uptrend and (prev_close < (prev_ema - buffer)) and (cur_close > (cur_ema + buffer))

    meta = {
        "uptrend": uptrend,
        "uptrend_raw": raw_uptrend,
        "trend_soften_epsilon": eps,
        "htf_fast": ema_fast[-1],
        "htf_fast_prev": ema_fast[-2],
        "htf_slow": ema_slow[-1],
        "ltf_ema": cur_ema,
        "prev_close": prev_close,
        "close": cur_close,
        "bar_ts": ltf_ts[-1],
    }
    return fired, meta



def _within_minutes_after_utc_hhmm(now: datetime, hhmm_utc: str, window_min: int) -> bool:
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    start = h * 60 + m
    cur = now.hour * 60 + now.minute
    return start <= cur < (start + max(1, int(window_min)))


def _is_flatten_time(now: datetime, hhmm_utc: str) -> bool:
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        h, m = 23, 55
    return (now.hour, now.minute) >= (h, m)


def _validate_symbol(symbol: str) -> str:
    sym = normalize_symbol(symbol)
    if sym not in settings.allowed_symbols:
        raise HTTPException(status_code=400, detail=f"symbol_not_allowed: {sym}")
    return sym


def _has_position(symbol: str) -> tuple[bool, float]:
    """Return (has_position, position_notional_usd).

    IMPORTANT: treat tiny 'dust' balances as *not* a position so they don't block entries/exits.
    """
    bal = _balances_by_asset()
    base = _base_asset(symbol)
    qty = float(bal.get(base, 0.0) or 0.0)
    if qty <= 0:
        return False, 0.0

    px = float(_last_price(symbol) or 0.0)
    notional = float(qty * px) if px > 0 else 0.0

    # IMPORTANT:
    # Use the module-level `settings` (loaded from env at startup). A previous refactor
    # attempted to call `_get_settings()` here, but that function does not exist, which
    # silently disabled the dust filter (caught by the broad exception handler).
    try:
        min_pos = float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0)
        if min_pos and notional < min_pos:
            return False, notional
    except Exception:
        # If anything goes wrong, fail *open* (treat as position) to avoid accidental
        # double-entry. Callers will still have other safety gates.
        return True, notional

    return True, notional


# ---------- Scanner helpers ----------
def _scanner_should_refresh() -> bool:
    if not SCANNER_URL:
        return False
    return (time.time() - float(_SCANNER_CACHE["ts"] or 0.0)) >= float(SCANNER_REFRESH_SEC)


def _scanner_fetch_active_symbols() -> list[str]:
    """Fetch scanner / knows how to normalize symbols and apply dedup/filters.
    Returns a list of *normalized* symbols (e.g. BTC/USD) suitable for allow-checks.
    Never raises; returns [] on failure.
    """
    try:
        url = settings.scanner_url
        if not url:
            return []
        r = requests.get(url, timeout=settings.scanner_timeout_sec)
        r.raise_for_status()
        data = r.json()
        syms = data.get("active_symbols") or []
        out: list[str] = []
        for s in syms:
            try:
                out.append(normalize_symbol(str(s)))
            except Exception:
                continue
        # de-dupe preserve order
        seen=set()
        dedup=[]
        for s in out:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)
        return dedup
    except Exception:
        return []



def _scanner_fetch_active_symbols_and_meta() -> tuple[bool, str | None, dict, list[str]]:
    """Fetch active symbols + include scanner meta/diagnostics.

    Returns: (ok, reason, meta, symbols)
    - ok: True if HTTP 200 and JSON parsed
    - reason: short string when ok=False
    - meta: dict of useful fields (status_code, url, last_error, last_refresh_utc, etc)
    - symbols: list[str] of normalized symbols (e.g., 'ETH/USD')

    Never raises.
    """
    url = (SCANNER_URL or os.getenv("SCANNER_URL", "").strip())
    meta: dict = {"scanner_url": url}
    if not url:
        return False, "missing_scanner_url", meta, []

    try:
        r = requests.get(url, timeout=SCANNER_TIMEOUT_SEC)
        meta["status_code"] = r.status_code
        try:
            meta["elapsed_ms"] = int(getattr(getattr(r, "elapsed", None), "total_seconds", lambda: 0)() * 1000)
        except Exception:
            meta["elapsed_ms"] = None

        if r.status_code != 200:
            meta["body_snippet"] = (r.text or "")[:500]
            return False, f"http_{r.status_code}", meta, []

        data = r.json() if r.content else {}
        meta["scanner_ok"] = bool(data.get("ok", True))
        meta["last_error"] = data.get("last_error")
        meta["last_refresh_utc"] = data.get("last_refresh_utc")
        if isinstance(data.get("meta"), dict):
            meta["scanner_meta"] = data.get("meta")

        raw_syms = data.get("active_symbols")
        if not isinstance(raw_syms, list):
            return False, "invalid_active_symbols", meta, []

        clean: list[str] = []
        for s in raw_syms:
            if not isinstance(s, str):
                continue
            try:
                clean.append(normalize_symbol(s))
            except Exception:
                continue

        # de-dupe preserve order
        seen: set[str] = set()
        dedup: list[str] = []
        for s in clean:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)

        return True, None, meta, dedup

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        return False, "exception", meta, []
def _build_universe(payload, scanner_syms: list[str]) -> list[str]:
    """Build the scan universe safely.

    Priority:
      1) Explicit payload.symbols (if provided and non-empty)
      2) Scanner symbols (scanner_syms)  <-- primary mode
      3) Fallback to settings.allowed_symbols

    Optional behaviors (env-driven):
      - SCANNER_TARGET_N: cap universe size from scanner (default 5)
      - UNIVERSE_USD_ONLY: keep only */USD
      - UNIVERSE_PREFER_USD_FOR_STABLES: when USD-only, convert BASE/USDT or BASE/USDC -> BASE/USD
        *only if BASE/USD exists in scanner list*, else drop it.
      - FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS: intersect universe with ALLOWED_SYMBOLS (default off)
    """
    try:
        requested = getattr(payload, "symbols", None) or []
        force_scan = bool(getattr(payload, "force_scan", False))
    except Exception:
        requested, force_scan = [], False

    def _norm_list(items):
        out: list[str] = []
        seen: set[str] = set()
        for s in items or []:
            try:
                sym = normalize_symbol(str(s))
            except Exception:
                continue
            if sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
        return out

    requested_norm = _norm_list(requested)
    scanner_norm = _norm_list(scanner_syms)

    # Build raw universe
    if requested_norm:
        universe = requested_norm
    elif scanner_norm:
        universe = scanner_norm[: max(1, int(SCANNER_TARGET_N or 5))] if SCANNER_DRIVEN_UNIVERSE else scanner_norm
    else:
        universe = _norm_list(getattr(settings, "allowed_symbols", []) or [])

    # USD-only filtering / conversion
    if UNIVERSE_USD_ONLY:
        scanner_set = set(scanner_norm)
        out: list[str] = []
        for s in universe:
            try:
                base, quote = s.split("/", 1)
            except Exception:
                continue
            if quote == "USD":
                out.append(s)
                continue
            if UNIVERSE_PREFER_USD_FOR_STABLES and quote in ("USDT", "USDC"):
                cand = f"{base}/USD"
                if cand in scanner_set:
                    out.append(cand)
                # else: drop (don't silently trade a different quote)
        # de-dupe preserve order
        universe = list(dict.fromkeys(out))

    # Allowed-symbol filtering (off by default for scanner-driven universe)
    allowed = set(getattr(settings, "allowed_symbols", []) or [])
    if allowed and FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS and not (force_scan and SCANNER_SOFT_ALLOW):
        universe = [s for s in universe if s in allowed]

    # Stable fallback: if universe empty but allowed configured, use allowed
    if not universe and allowed:
        universe = list(dict.fromkeys(list(allowed)))

    return universe


def _scanner_refresh() -> None:
    if not SCANNER_URL:
        return

    try:
        r = requests.get(SCANNER_URL, timeout=SCANNER_TIMEOUT_SEC)
        r.raise_for_status()
        j = r.json()

        # Expected: {"ok": true, "active_symbols": ["SOL/USD", ...], ...}
        active = j.get("active_symbols") or j.get("active_coins") or []
        active_norm = set()

        for s in active:
            try:
                active_norm.add(normalize_symbol(str(s)))
            except Exception:
                continue

        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["active_symbols"] = active_norm
        _SCANNER_CACHE["ok"] = bool(j.get("ok", True)) and len(active_norm) > 0
        _SCANNER_CACHE["last_error"] = None
        _SCANNER_CACHE["raw"] = j

    except Exception as e:
        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["ok"] = False
        _SCANNER_CACHE["last_error"] = str(e)


def _phase1_safety_report() -> Dict[str, Any]:
    strategy_mode_ok = STRATEGY_MODE == "fixed"
    strategy_count = len(ENTRY_ENGINE_STRATEGIES_LIST)
    allowed_symbols_sorted = sorted(list(ALLOWED_SYMBOLS))
    active_strategy_flags = {
        "rb1": bool(ENABLE_RB1),
        "tc1": bool(ENABLE_TC1),
        "cr1": bool(ENABLE_CR1),
        "mm1": bool(ENABLE_MM1),
    }
    active_enabled = [k for k, v in active_strategy_flags.items() if v]

    checks = {
        "allowed_symbols_locked": allowed_symbols_sorted == ["BTC/USD", "ETH/USD"],
        "filter_universe_by_allowed_symbols": bool(FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS),
        "scanner_new_symbols_disabled": not (os.getenv("ALLOW_SCANNER_NEW_SYMBOLS", "0").strip().lower() in ("1", "true", "yes", "on")),
        "scanner_driven_universe_disabled": not bool(SCANNER_DRIVEN_UNIVERSE),
        "scanner_soft_allow_disabled": not bool(SCANNER_SOFT_ALLOW),
        "strategy_mode_fixed": strategy_mode_ok,
        "single_strategy_requested": strategy_count == 1,
        "single_strategy_enabled": len(active_enabled) == 1,
        "mm1_disabled": not bool(ENABLE_MM1),
        "max_open_positions_safe": int(MAX_OPEN_POSITIONS) <= 1,
        "max_entries_per_day_safe": int(MAX_ENTRIES_PER_DAY) <= 3,
    }
    violations = [k for k, v in checks.items() if not v]
    return {
        "ok": len(violations) == 0,
        "checks": checks,
        "violations": violations,
        "details": {
            "allowed_symbols": allowed_symbols_sorted,
            "entry_engine_strategies": ENTRY_ENGINE_STRATEGIES_LIST,
            "strategy_mode": STRATEGY_MODE,
            "enabled_strategies": active_enabled,
            "scanner_driven_universe": bool(SCANNER_DRIVEN_UNIVERSE),
            "scanner_soft_allow": bool(SCANNER_SOFT_ALLOW),
            "filter_universe_by_allowed_symbols": bool(FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS),
            "max_open_positions": int(MAX_OPEN_POSITIONS),
            "max_entries_per_day": int(MAX_ENTRIES_PER_DAY),
        },
    }


def _symbol_allowed_by_scanner(symbol: str) -> tuple[bool, str, Dict[str, Any]]:
    """
    Returns (allowed, reason, meta)
    Soft allow:
      - If scanner is down OR returns empty => allow (fallback)
      - If scanner is healthy => allow only if symbol is in active set
    """
    if not SCANNER_URL:
        return True, "scanner_disabled", {"scanner_url": ""}

    if _scanner_should_refresh():
        _scanner_refresh()

    ok = bool(_SCANNER_CACHE.get("ok"))
    active = _SCANNER_CACHE.get("active_symbols") or set()
    err = _SCANNER_CACHE.get("last_error")

    meta = {
        "scanner_ok": ok,
        "scanner_error": err,
        "scanner_active_count": len(active),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
    }

    if not ok or len(active) == 0:
        if SCANNER_SOFT_ALLOW:
            return True, "scanner_fallback_allow", meta
        return False, "scanner_unavailable_block", meta

    if symbol in active:
        return True, "scanner_active_allow", meta

    return False, "not_in_active_set", meta



def _worker_status_meta(snapshot: dict | None, *, stale_after_sec: int | None = None) -> dict:
    snap = dict(snapshot or {})
    last_ts = None
    try:
        utc_raw = snap.get('utc')
        if utc_raw:
            last_ts = datetime.fromisoformat(str(utc_raw).replace('Z', '+00:00')).timestamp()
    except Exception:
        last_ts = None
    if last_ts is None:
        return {"seen": False, "stale": True, "age_sec": None, "stale_after_sec": int(stale_after_sec or WORKER_STALE_AFTER_SEC)}
    age = max(0.0, time.time() - float(last_ts))
    lim = int(stale_after_sec or WORKER_STALE_AFTER_SEC)
    return {"seen": True, "stale": bool(age > lim), "age_sec": round(age, 3), "stale_after_sec": lim}


def _broker_open_orders_summary() -> dict:
    try:
        raw = broker_kraken.orders() or {}
    except Exception as e:
        return {"ok": False, "error": str(e), "buy_symbols": set(), "sell_symbols": set(), "items": []}
    open_map = (((raw or {}).get('open') or {}) if isinstance(raw, dict) else {})
    items = []
    buy_symbols, sell_symbols = set(), set()
    for txid, row in (open_map or {}).items():
        descr = (row or {}).get('descr') or {}
        pair = str(descr.get('pair') or '')
        side = str(descr.get('type') or '').strip().lower()
        symbol = normalize_symbol(pair) if pair else None
        item = {
            'txid': txid,
            'symbol': symbol,
            'side': side,
            'status': row.get('status'),
            'vol': float(row.get('vol', 0) or 0.0),
            'vol_exec': float(row.get('vol_exec', 0) or 0.0),
            'descr': descr,
        }
        items.append(item)
        if symbol and side == 'buy':
            buy_symbols.add(symbol)
        elif symbol and side == 'sell':
            sell_symbols.add(symbol)
    return {"ok": True, "buy_symbols": buy_symbols, "sell_symbols": sell_symbols, "items": items}



def _sum_open_order_notional_usd(summary: dict, *, side: str | None = None) -> float:
    total = 0.0
    side_l = str(side or '').strip().lower() if side else None
    for item in (summary.get('items') or []):
        try:
            item_side = str(item.get('side') or '').strip().lower()
            if side_l and item_side != side_l:
                continue
            descr = item.get('descr') or {}
            price = float(descr.get('price') or descr.get('price2') or 0.0)
            vol = float(item.get('vol', 0.0) or 0.0)
            vol_exec = float(item.get('vol_exec', 0.0) or 0.0)
            remaining = max(0.0, vol - vol_exec)
            if remaining <= 0 or price <= 0:
                continue
            total += remaining * price
        except Exception:
            continue
    return float(total)


def _account_truth_snapshot() -> dict:
    balances = {}
    balance_ok = True
    balance_error = None
    try:
        balances = _balances_by_asset() or {}
        balance_error = _last_balance_error()
        if balance_error:
            balance_ok = False
    except Exception as e:
        balance_ok = False
        balance_error = str(e)
        balances = {}

    positions = []
    try:
        positions = get_positions()
    except Exception:
        positions = []

    open_orders = _broker_open_orders_summary()
    realized_today = _journal_sync_daily_realized_pnl()
    cash_usd = float(_stable_cash_usd(balances) or 0.0)
    position_exposure_usd = sum(float(p.get('notional_usd', 0.0) or 0.0) for p in positions)
    open_buy_order_notional_usd = _sum_open_order_notional_usd(open_orders, side='buy') if open_orders.get('ok') else 0.0
    equity_usd = float(cash_usd + position_exposure_usd)
    utilization_pct = (float(position_exposure_usd + open_buy_order_notional_usd) / float(equity_usd)) if equity_usd > 0 else 0.0
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'balance_ok': bool(balance_ok),
        'balance_error': balance_error,
        'cash_usd': float(cash_usd),
        'position_exposure_usd': float(position_exposure_usd),
        'open_buy_order_notional_usd': float(open_buy_order_notional_usd),
        'equity_usd': float(equity_usd),
        'utilization_pct': float(utilization_pct),
        'positions_count': len(positions),
        'open_order_count': len(open_orders.get('items') or []),
        'buy_order_symbols': sorted(list(open_orders.get('buy_symbols') or [])),
        'sell_order_symbols': sorted(list(open_orders.get('sell_symbols') or [])),
        'realized_today_usd': float(realized_today),
        'last_reconcile': dict(getattr(state, 'last_reconcile_result', {}) or {}),
    }




def _pretrade_health_gate_summary(*, rerun_startup_check: bool = False) -> dict:
    enabled = bool(getattr(settings, 'pretrade_health_gate_enabled', True))
    worker_health = {
        'scan': _worker_status_meta(getattr(state, 'last_scan_status', {}) or {}),
        'exit': _worker_status_meta(getattr(state, 'last_exit_status', {}) or {}),
    }
    worker_health['overall_stale'] = bool(worker_health['scan'].get('stale') or worker_health['exit'].get('stale'))

    startup = _startup_self_check(rerun=bool(rerun_startup_check), apply=None)
    account_truth = _account_truth_snapshot()
    reconcile = _recovery_reconcile_summary(apply=False)
    ops_remaining = int(state.ops_lockout_remaining_sec() if hasattr(state, 'ops_lockout_remaining_sec') else 0)

    violations: list[str] = []
    if enabled:
        if bool(getattr(settings, 'pretrade_require_startup_self_check_ok', True)):
            if not bool(startup.get('ok')):
                violations.append('startup_self_check_not_ok')
            elif bool((startup.get('startup_self_check') or {}).get('critical')):
                violations.append('startup_self_check_critical')

        if bool(getattr(settings, 'pretrade_block_on_worker_stale', True)) and bool(worker_health.get('overall_stale')):
            if bool(worker_health['scan'].get('stale')):
                violations.append('scan_worker_stale')
            if bool(worker_health['exit'].get('stale')):
                violations.append('exit_worker_stale')

        if bool(getattr(settings, 'pretrade_block_on_balance_error', True)) and not bool(account_truth.get('balance_ok')):
            violations.append('broker_balance_unavailable')

        if bool(getattr(settings, 'pretrade_block_on_reconcile_anomaly', True)):
            if not bool(reconcile.get('broker_open_orders_ok')):
                violations.append('broker_open_orders_unavailable')
            if int(reconcile.get('orphan_broker_order_count') or 0) > 0:
                violations.append('orphan_broker_orders_present')
            if int(reconcile.get('orphan_internal_intent_count') or 0) > 0:
                violations.append('orphan_internal_intents_present')
            if int(reconcile.get('stale_pending_exit_count') or 0) > 0:
                violations.append('stale_pending_exits_present')

        if ops_remaining > 0:
            violations.append('ops_risk_lockout_active')

    return {
        'ok': True,
        'utc': utc_now_iso(),
        'enabled': enabled,
        'gate_open': bool((not enabled) or (len(violations) == 0)),
        'violations': violations,
        'worker_health': worker_health,
        'startup_self_check': startup,
        'account_truth': {
            'balance_ok': bool(account_truth.get('balance_ok')),
            'balance_error': account_truth.get('balance_error'),
            'cash_usd': float(account_truth.get('cash_usd', 0.0) or 0.0),
            'equity_usd': float(account_truth.get('equity_usd', 0.0) or 0.0),
            'utilization_pct': float(account_truth.get('utilization_pct', 0.0) or 0.0),
            'open_order_count': int(account_truth.get('open_order_count', 0) or 0),
            'positions_count': int(account_truth.get('positions_count', 0) or 0),
        },
        'recovery_reconcile': {
            'ok': bool(reconcile.get('ok')),
            'broker_open_orders_ok': bool(reconcile.get('broker_open_orders_ok')),
            'broker_open_order_count': int(reconcile.get('broker_open_order_count', 0) or 0),
            'orphan_broker_order_count': int(reconcile.get('orphan_broker_order_count', 0) or 0),
            'orphan_internal_intent_count': int(reconcile.get('orphan_internal_intent_count', 0) or 0),
            'stale_pending_exit_count': int(reconcile.get('stale_pending_exit_count', 0) or 0),
        },
        'ops_risk': {
            'ops_lockout_remaining_sec': ops_remaining,
            'ops_lockout_reason': str(getattr(state, 'last_ops_lock_reason', '') or ''),
        },
        'settings': {
            'pretrade_require_startup_self_check_ok': bool(getattr(settings, 'pretrade_require_startup_self_check_ok', True)),
            'pretrade_block_on_worker_stale': bool(getattr(settings, 'pretrade_block_on_worker_stale', True)),
            'pretrade_block_on_balance_error': bool(getattr(settings, 'pretrade_block_on_balance_error', True)),
            'pretrade_block_on_reconcile_anomaly': bool(getattr(settings, 'pretrade_block_on_reconcile_anomaly', True)),
        },
    }

def _maybe_reconcile_state_before_entry() -> dict | None:
    if not bool(getattr(settings, 'auto_reconcile_state_on_entry', True)):
        return None
    cooldown = int(getattr(settings, 'reconcile_cooldown_sec', 30) or 30)
    if not state.should_reconcile(cooldown):
        return getattr(state, 'last_reconcile_result', {}) or None
    result = _reconcile_runtime_state()
    try:
        state.mark_reconcile(result)
    except Exception:
        pass
    return result


def _has_broker_open_order(symbol: str, side: str) -> tuple[bool, dict]:
    side_l = str(side or '').strip().lower()
    summary = _broker_open_orders_summary()
    if not summary.get('ok'):
        return False, summary
    syms = summary.get('buy_symbols') if side_l == 'buy' else summary.get('sell_symbols')
    matched = [it for it in (summary.get('items') or []) if it.get('symbol') == symbol and it.get('side') == side_l]
    return bool(symbol in (syms or set())), {
        'ok': True,
        'matched_count': len(matched),
        'matched': matched[:10],
    }




def _recovery_reconcile_summary(*, apply: bool = False) -> dict:
    base = _reconcile_runtime_state() if apply else diagnostics_position_reconcile(apply=0)
    try:
        broker_open = _broker_open_orders_summary()
    except Exception as e:
        broker_open = {"ok": False, "error": str(e), "items": []}

    live_positions = get_positions()
    live_syms = {p.get('symbol') for p in live_positions if p.get('symbol')}
    openish_states = {'created','validated','submitted','acknowledged','partial','replace_pending','cancel_pending','failed_reconcile'}
    openish_intents = lifecycle_db.list_openish_order_intents(limit=200)
    openish_trade_plans = lifecycle_db.list_openish_trade_plans(limit=200)
    pending_exits = dict(getattr(state, 'pending_exits', {}) or {})

    broker_items = list(broker_open.get('items') or []) if broker_open.get('ok') else []
    broker_buy = {(str(it.get('symbol') or ''), 'buy'): it for it in broker_items if str(it.get('side') or '').lower() == 'buy' and it.get('symbol')}
    broker_sell = {(str(it.get('symbol') or ''), 'sell'): it for it in broker_items if str(it.get('side') or '').lower() == 'sell' and it.get('symbol')}

    orphan_broker_orders = []
    matched_intent_ids = set()
    for bo in broker_items:
        bsym = str(bo.get('symbol') or '')
        bside = str(bo.get('side') or '').lower()
        btxid = str(bo.get('txid') or '')
        matched = None
        for it in openish_intents:
            if str(it.get('symbol') or '') != bsym or str(it.get('side') or '').lower() != bside:
                continue
            if btxid and str(it.get('broker_txid') or '') == btxid:
                matched = it
                break
        if matched is None:
            for it in openish_intents:
                if str(it.get('symbol') or '') == bsym and str(it.get('side') or '').lower() == bside:
                    matched = it
                    break
        if matched is not None:
            if matched.get('intent_id'):
                matched_intent_ids.add(str(matched.get('intent_id')))
        else:
            orphan_broker_orders.append(bo)

    orphan_internal_intents = []
    marked_failed = []
    for it in openish_intents:
        iid = str(it.get('intent_id') or '')
        sym = str(it.get('symbol') or '')
        side = str(it.get('side') or '').lower()
        if iid in matched_intent_ids:
            continue
        broker_match = broker_buy.get((sym, side)) if side == 'buy' else broker_sell.get((sym, side))
        has_live_position = sym in live_syms
        is_orphan = False
        if side == 'buy' and (broker_match is None) and (not has_live_position):
            is_orphan = True
        elif side == 'sell' and (broker_match is None) and (sym not in pending_exits) and (not has_live_position):
            is_orphan = True
        elif side == 'sell' and (broker_match is None) and (sym in pending_exits) and (not has_live_position):
            is_orphan = True
        if is_orphan:
            orphan_internal_intents.append({
                'intent_id': iid,
                'symbol': sym,
                'side': side,
                'state': str(it.get('state') or ''),
                'broker_txid': str(it.get('broker_txid') or ''),
                'trade_plan_id': str(it.get('trade_plan_id') or ''),
            })
            if apply and iid:
                try:
                    lifecycle_db.transition_order_intent(iid, 'failed_reconcile', reject_reason='reconcile_missing_broker_order', last_broker_status='missing_on_broker')
                    lifecycle_db.record_anomaly('orphan_internal_intent', 'warn', symbol=sym or None, trade_plan_id=str(it.get('trade_plan_id') or '') or None, intent_id=iid, details={'side': side, 'previous_state': str(it.get('state') or '')})
                    marked_failed.append(iid)
                except Exception:
                    pass

    orphan_trade_plans = []
    closed_trade_plans = []
    live_runtime_plan_syms = set((getattr(state, 'plans', {}) or {}).keys())
    intent_plan_ids = {str(it.get('trade_plan_id') or '') for it in openish_intents if str(it.get('trade_plan_id') or '')}
    now_ts = time.time()
    orphan_plan_age_sec = max(int(ORDER_RECONCILE_TIMEOUT_SEC or 10) * 6, 120)
    for tp in openish_trade_plans:
        trade_plan_id = str(tp.get('trade_plan_id') or '')
        sym = str(tp.get('symbol') or '')
        status = str(tp.get('status') or '')
        updated_ts = float(tp.get('updated_ts') or tp.get('created_ts') or 0.0)
        age_sec = (now_ts - updated_ts) if updated_ts > 0 else None
        has_open_intent = trade_plan_id in intent_plan_ids
        has_live_position = bool(sym) and (sym in live_syms)
        has_runtime_plan = bool(sym) and (sym in live_runtime_plan_syms)
        is_orphan_plan = (not has_open_intent) and (not has_live_position) and (not has_runtime_plan)
        if is_orphan_plan:
            orphan_trade_plans.append({
                'trade_plan_id': trade_plan_id,
                'symbol': sym,
                'status': status,
                'updated_ts': updated_ts,
                'age_sec': age_sec,
            })
            if apply and age_sec is not None and age_sec >= orphan_plan_age_sec and trade_plan_id:
                try:
                    lifecycle_db.close_trade_plan(trade_plan_id, 'failed_reconcile')
                    lifecycle_db.record_anomaly('orphan_trade_plan', 'warn', symbol=sym or None, trade_plan_id=trade_plan_id, details={'previous_status': status, 'age_sec': age_sec})
                    closed_trade_plans.append(trade_plan_id)
                except Exception:
                    pass

    stale_pending_exit_symbols = []
    cleared_pending_exit_symbols = []
    broker_sell_syms = {str(it.get('symbol') or '') for it in broker_items if str(it.get('side') or '').lower() == 'sell' and it.get('symbol')}
    for sym in list(pending_exits.keys()):
        if sym not in live_syms and sym not in broker_sell_syms:
            stale_pending_exit_symbols.append(sym)
            if apply:
                try:
                    state.clear_pending_exit(sym)
                    cleared_pending_exit_symbols.append(sym)
                except Exception:
                    pass

    if apply:
        for bo in orphan_broker_orders:
            try:
                lifecycle_db.record_anomaly('orphan_broker_order', 'warn', symbol=str(bo.get('symbol') or '') or None, details=bo)
            except Exception:
                pass

    return {
        'ok': True,
        'utc': utc_now_iso(),
        'base_reconcile': base,
        'broker_open_orders_ok': bool(broker_open.get('ok')),
        'broker_open_order_count': len(broker_items),
        'orphan_broker_orders': orphan_broker_orders,
        'orphan_broker_order_count': len(orphan_broker_orders),
        'orphan_internal_intents': orphan_internal_intents,
        'orphan_internal_intent_count': len(orphan_internal_intents),
        'orphan_trade_plans': orphan_trade_plans,
        'orphan_trade_plan_count': len(orphan_trade_plans),
        'stale_pending_exit_symbols': stale_pending_exit_symbols,
        'stale_pending_exit_count': len(stale_pending_exit_symbols),
        'apply_changes': bool(apply),
        'applied': {
            'marked_failed_reconcile_intents': marked_failed,
            'closed_orphan_trade_plans': closed_trade_plans,
            'cleared_pending_exit_symbols': cleared_pending_exit_symbols,
        },
    }


def _reconcile_runtime_state() -> dict:
    positions = get_positions()
    live_syms = {p.get('symbol') for p in positions if p.get('symbol')}
    plan_syms = set((getattr(state, 'plans', {}) or {}).keys())
    stale_plan_syms = sorted([s for s in plan_syms if s not in live_syms])
    missing_plan_syms = sorted([s for s in live_syms if s not in plan_syms])
    cleared_plans = 0
    cleared_pending = 0
    adopted = 0
    now_ts = time.time()
    for sym in stale_plan_syms:
        state.remove_plan(sym)
        state.clear_pending_exit(sym)
        cleared_plans += 1
        cleared_pending += 1
    for pos in positions:
        sym = pos.get('symbol')
        if not sym or sym not in missing_plan_syms:
            continue
        px = float(pos.get('price') or 0.0)
        qty = float(pos.get('qty') or 0.0)
        notional = float(pos.get('notional_usd') or 0.0)
        if qty <= 0 or px <= 0 or notional <= 0:
            continue
        stop_px, take_px = compute_brackets(px, settings.stop_pct, settings.take_pct)
        state.set_plan(TradePlan(symbol=sym, side='buy', notional_usd=notional, entry_price=px, stop_price=stop_px, take_price=take_px, strategy='adopted', opened_ts=now_ts, max_hold_sec=_strategy_max_hold_sec('adopted')))
        adopted += 1
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'live_position_symbols': sorted(list(live_syms)),
        'plan_symbols_before': sorted(list(plan_syms)),
        'stale_plan_symbols_cleared': stale_plan_syms,
        'missing_plan_symbols_adopted': missing_plan_syms,
        'cleared_plans': cleared_plans,
        'cleared_pending_exits': cleared_pending,
        'adopted_plans': adopted,
    }



@app.get("/diagnostics/account_truth")
def diagnostics_account_truth():
    return _account_truth_snapshot()


@app.get("/diagnostics/position_reconcile")
def diagnostics_position_reconcile(apply: int = 0):
    if int(apply or 0) == 1:
        result = _reconcile_runtime_state()
        try:
            state.mark_reconcile(result)
        except Exception:
            pass
        return result
    positions = get_positions()
    live_syms = {p.get('symbol') for p in positions if p.get('symbol')}
    plan_syms = set((getattr(state, 'plans', {}) or {}).keys())
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'live_position_symbols': sorted(list(live_syms)),
        'plan_symbols': sorted(list(plan_syms)),
        'stale_plan_symbols': sorted([s for s in plan_syms if s not in live_syms]),
        'missing_plan_symbols': sorted([s for s in live_syms if s not in plan_syms]),
    }


@app.get("/diagnostics/open_orders")
def diagnostics_open_orders():
    summary = _broker_open_orders_summary()
    return {
        'ok': bool(summary.get('ok')),
        'utc': utc_now_iso(),
        'buy_symbols': sorted(list(summary.get('buy_symbols') or [])),
        'sell_symbols': sorted(list(summary.get('sell_symbols') or [])),
        'count': len(summary.get('items') or []),
        'items': summary.get('items') or [],
        'error': summary.get('error'),
    }


@app.get("/diagnostics/worker_health")
def diagnostics_worker_health():
    scan_meta = _worker_status_meta(getattr(state, 'last_scan_status', {}) or {})
    exit_meta = _worker_status_meta(getattr(state, 'last_exit_status', {}) or {})
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'scan': {**scan_meta, 'snapshot': getattr(state, 'last_scan_status', {}) or {}},
        'exit': {**exit_meta, 'snapshot': getattr(state, 'last_exit_status', {}) or {}},
        'overall_stale': bool(scan_meta.get('stale') or exit_meta.get('stale')),
    }


@app.get("/health")
def health():
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()
    open_orders = _broker_open_orders_summary()
    scan_meta = _worker_status_meta(getattr(state, 'last_scan_status', {}) or {})
    exit_meta = _worker_status_meta(getattr(state, 'last_exit_status', {}) or {})
    account_truth = _account_truth_snapshot()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "scanner_url": SCANNER_URL or None,
        "scanner_ok": scanner_ok and (len(scanner_syms) > 0),
        "scanner_reason": scanner_reason,
        "scanner_active_count": len(scanner_syms),
        "scanner_last_refresh_utc": (scanner_meta or {}).get("last_refresh_utc"),
        "scanner_last_error": (scanner_meta or {}).get("last_error"),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
        "worker_health": {
            "scan": scan_meta,
            "exit": exit_meta,
            "overall_stale": bool(scan_meta.get('stale') or exit_meta.get('stale')),
        },
        "account_truth": {
            "balance_ok": bool(account_truth.get('balance_ok')),
            "cash_usd": float(account_truth.get('cash_usd', 0.0) or 0.0),
            "equity_usd": float(account_truth.get('equity_usd', 0.0) or 0.0),
            "utilization_pct": float(account_truth.get('utilization_pct', 0.0) or 0.0),
            "realized_today_usd": float(account_truth.get('realized_today_usd', 0.0) or 0.0),
        },
        "broker_open_orders": {
            "ok": bool(open_orders.get('ok')),
            "count": len(open_orders.get('items') or []),
            "buy_symbols": sorted(list(open_orders.get('buy_symbols') or []))[:20],
            "sell_symbols": sorted(list(open_orders.get('sell_symbols') or []))[:20],
        },
    }



# --- Trades persistence + performance endpoints (SQLite on Render disk) ---

def _refresh_trades_from_kraken(
    since_hours: float = 24.0,
    pair: str | None = None,
    upsert: bool = True,
    limit: int = 250,
) -> dict:
    # Fetch recent trades from Kraken private TradesHistory.
    since_ts = time.time() - float(since_hours) * 3600.0
    # Use the dedicated since+limit helper to avoid signature mismatches and reduce payload size.
    items = broker_kraken.trades_history_since(since_ts=since_ts, pair=pair, limit=int(limit)) or []

    # Sort by time ascending for nicer inserts
    items.sort(key=lambda x: float(x.get("time") or 0))
    if upsert:
        up = trades_db.upsert_trades(items)
    else:
        up = {"ok": True, "db_path": trades_db.ensure_db(), "inserted": 0, "updated": 0, "received": len(items), "note": "upsert disabled"}
    return {"ok": True, "since_hours": float(since_hours), "pair": pair, "limit": int(limit), "count": len(items), "upsert": up}

@app.get("/trades")
def get_trades(
    since_hours: float = 24.0,
    pair: str | None = None,
    limit: int = 200,
    refresh: bool = False,
    refresh_upsert: bool | None = None,
):
    if refresh:
        _refresh_trades_from_kraken(since_hours=since_hours, pair=pair, upsert=True if refresh_upsert is None else bool(refresh_upsert))
    return trades_db.query_trades(since_hours=since_hours, pair=pair, limit=limit)

@app.get("/trades/refresh")
def refresh_trades_get(
    since_hours: int = 24,
    pair: str | None = None,
    upsert: bool = True,
    limit: int = 250,
):
    """Back-compat convenience endpoint (GET).

    Preferred is POST /trades/refresh with JSON body.
    """
    return _refresh_trades_from_kraken(since_hours=since_hours, pair=pair, upsert=upsert, limit=limit)


@app.post("/trades/refresh")
def refresh_trades(body: dict | bool | None = Body(default=None)):
    # Defensive: some clients accidentally send boolean bodies ("true"/"false")
    payload = body if isinstance(body, dict) else {}
    since_hours = float(payload.get("since_hours", 24.0))
    pair = payload.get("pair")
    upsert = bool(payload.get("upsert", True))
    return _refresh_trades_from_kraken(since_hours=since_hours, pair=pair, upsert=upsert)

@app.get("/trades/summary")
def trades_summary(days: float = 7.0, refresh: bool = False, refresh_since_hours: float = 168.0):
    if refresh:
        _refresh_trades_from_kraken(since_hours=refresh_since_hours, pair=None, upsert=True)
    return trades_db.summary(days=days)


@app.get("/telemetry")
def telemetry(limit: int = 100):
    lim = max(1, min(int(limit), 500))
    return {"ok": True, "count": len(state.telemetry), "items": state.telemetry[-lim:]}



def _journal_sync_daily_realized_pnl() -> float:
    """Refresh in-memory daily P&L from the lifecycle journal."""
    try:
        pnl = float(trade_journal.today_realized_pnl_utc() or 0.0)
        state.daily_pnl_usd = pnl
        return pnl
    except Exception:
        return float(getattr(state, "daily_pnl_usd", 0.0) or 0.0)


def _extract_execution_metrics(res: dict | None, *, fallback_price: float = 0.0, fallback_qty: float = 0.0) -> dict:
    """Best-effort normalized fill metrics from broker results.

    Handles:
    - plain market executions (reconciled)
    - maker-first partial -> market fallback
    - stop-limit partial -> market fallback
    """
    res = res or {}
    qty = 0.0
    cost = 0.0
    fee = 0.0

    def _acc(rec: dict | None, *, default_price: float = 0.0, default_qty: float = 0.0):
        nonlocal qty, cost, fee
        if not rec:
            if default_qty > 0 and default_price > 0:
                qty += float(default_qty)
                cost += float(default_qty) * float(default_price)
            return
        q = float(rec.get('vol_exec') or rec.get('volume') or default_qty or 0.0)
        c = rec.get('cost')
        f = rec.get('fee')
        px = rec.get('avg_price') or rec.get('price') or default_price or 0.0
        qty += float(q or 0.0)
        if c is not None:
            cost += float(c or 0.0)
        elif q and px:
            cost += float(q) * float(px)
        fee += float(f or 0.0)

    recon = res.get('reconciled') or {}
    _acc(recon, default_price=fallback_price, default_qty=float(res.get('volume') or fallback_qty or 0.0))

    maker_first = res.get('maker_first') or {}
    if maker_first:
        last_err = maker_first.get('last_error') or {}
        part_qty = float(last_err.get('vol_exec') or 0.0)
        part_px = float(last_err.get('limit_price') or fallback_price or 0.0)
        # Kraken order-query path used here does not expose reliable partial fee/cost.
        # We approximate cost from the post-only limit price and leave fee at zero.
        if part_qty > 0:
            qty += part_qty
            cost += part_qty * part_px

    stop_limit_first = res.get('stop_limit_first') or {}
    if stop_limit_first:
        _acc(stop_limit_first.get('reconciled') or {}, default_price=float(stop_limit_first.get('limit_price') or fallback_price or 0.0))

    avg_price = (cost / qty) if qty > 0 else (float(fallback_price) if fallback_price > 0 else None)
    if qty <= 0 and fallback_qty > 0:
        qty = float(fallback_qty)
        if cost <= 0 and fallback_price > 0:
            cost = qty * float(fallback_price)
            avg_price = float(fallback_price)

    return {
        'qty': float(qty),
        'cost': float(cost),
        'fee': float(fee),
        'avg_price': float(avg_price) if avg_price is not None else None,
        'execution': str(res.get('execution') or ''),
        'txid': res.get('txid'),
    }


def _record_open_trade_journal(*, symbol: str, strategy: str, source: str, signal_name: str | None, signal_id: str | None, req_id: str, requested_notional_usd: float, stop_price: float, take_price: float, px_fallback: float, res: dict, opened_ts: float | None = None) -> None:
    metrics = _extract_execution_metrics(res, fallback_price=float(px_fallback or 0.0), fallback_qty=(float(requested_notional_usd or 0.0) / float(px_fallback or 1.0)) if float(px_fallback or 0.0) > 0 else 0.0)
    trade_journal.upsert_open_trade({
        'symbol': symbol,
        'opened_ts': float(opened_ts or time.time()),
        'strategy': strategy,
        'source': source,
        'signal_name': signal_name,
        'signal_id': signal_id,
        'req_id': req_id,
        'entry_txid': metrics.get('txid'),
        'entry_execution': metrics.get('execution'),
        'entry_price': metrics.get('avg_price') or float(px_fallback or 0.0),
        'entry_qty': metrics.get('qty'),
        'entry_cost': metrics.get('cost'),
        'entry_fee': metrics.get('fee'),
        'requested_notional_usd': float(requested_notional_usd),
        'stop_price': float(stop_price),
        'take_price': float(take_price),
        'meta': {'order_result': res},
    })


def _record_exit_trade_journal(*, symbol: str, reason: str, px_fallback: float, res: dict, closed_ts: float | None = None) -> dict | None:
    metrics = _extract_execution_metrics(res, fallback_price=float(px_fallback or 0.0))
    out = trade_journal.close_trade(symbol, {
        'closed_ts': float(closed_ts or time.time()),
        'exit_txid': metrics.get('txid'),
        'exit_execution': metrics.get('execution'),
        'exit_price': metrics.get('avg_price') or float(px_fallback or 0.0),
        'exit_qty': metrics.get('qty'),
        'exit_cost': metrics.get('cost'),
        'exit_fee': metrics.get('fee'),
        'exit_reason': reason,
        'meta': {'order_result': res},
    })
    try:
        _journal_sync_daily_realized_pnl()
    except Exception:
        pass
    return out


def _execute_long_entry(
    *,
    symbol: str,
    strategy: str,
    signal_name: str | None,
    signal_id: str | None,
    notional: float | None,
    source: str,
    req_id: str,
    dry_run: bool = False,
    client_ip: str | None = None,
    extra: dict | None = None,
    px_override: float | None = None,
    stop_price: float | None = None,
    take_price: float | None = None,
):
    def ignored(reason: str, **extra_fields):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": source,
            "status": "ignored",
            "reason": reason,
            **(extra_fields or {}),
        }
        _log_event("warning", evt)
        _record_blocked_trade(reason, req_id=req_id, source=source, **(extra_fields or {}))
        return {"ok": True, "ignored": True, "reason": reason, **(extra_fields or {})}

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored("duplicate_signal_id", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    if not bool(settings.trading_enabled):
        return ignored("trading_disabled", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    # global daily entry cap
    total_today = int(getattr(state, "trades_today_total", 0) or 0)
    if MAX_ENTRIES_PER_DAY > 0 and total_today >= int(MAX_ENTRIES_PER_DAY):
        return ignored("max_entries_per_day_reached", symbol=symbol, strategy=strategy, entries_today=total_today, max_entries_per_day=MAX_ENTRIES_PER_DAY)

    # global entry cooldown
    if not state.can_enter_global(int(getattr(settings, "global_entry_cooldown_sec", 0) or 0)):
        return ignored("global_entry_cooldown", symbol=symbol, strategy=strategy, cooldown_sec=int(getattr(settings, "global_entry_cooldown_sec", 0) or 0))

    # daily loss cap (journal-backed)
    realized_today = _journal_sync_daily_realized_pnl()
    if realized_today <= -float(settings.max_daily_loss_usd):
        return ignored("max_daily_loss_reached", symbol=symbol, strategy=strategy, daily_pnl_usd=realized_today)

    # cooldown
    if not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored("entry_cooldown", symbol=symbol, cooldown_sec=int(settings.entry_cooldown_sec))

    # max trades per day per symbol
    if not state.can_trade_symbol_today(symbol, int(settings.max_trades_per_symbol_per_day)):
        return ignored("max_trades_per_symbol_per_day", symbol=symbol, max_per_day=int(settings.max_trades_per_symbol_per_day))

    # one position per symbol
    has_pos, pos_notional = _has_position(symbol)
    if has_pos:
        return ignored("position_already_open", symbol=symbol, position_notional_usd=pos_notional, strategy=strategy)

    entry_lock_key = f"buy:{symbol}"
    if state.has_order_lock(entry_lock_key, OPEN_ORDER_LOCK_TTL_SEC):
        return ignored("entry_order_lock_active", symbol=symbol, strategy=strategy, ttl_sec=int(OPEN_ORDER_LOCK_TTL_SEC))
    if ENABLE_BROKER_OPEN_ORDER_GUARD:
        has_buy_order, buy_meta = _has_broker_open_order(symbol, 'buy')
        if has_buy_order:
            return ignored("broker_open_buy_order_exists", symbol=symbol, strategy=strategy, **buy_meta)

    reconcile_meta = _maybe_reconcile_state_before_entry()

    # Determine reference price for fills/brackets early because real risk sizing must use the actual stop distance.
    px = float(px_override) if px_override is not None else float(_last_price(symbol))

    # If caller didn't supply brackets, compute them from `px`.
    if stop_price is None or take_price is None:
        s = (strategy or "").strip().lower()
        if s == "cr1":
            bars_for_atr = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, CR1_ATR_LEN + 40))
            atr_now, _ = _atr_from_bars(bars_for_atr or [], length=int(CR1_ATR_LEN))
            if atr_now is not None and atr_now > 0:
                stop_price = float(px) - float(atr_now) * float(CR1_STOP_ATR_MULT)
                take_price = float(px) + float(atr_now) * float(CR1_TAKE_ATR_MULT)
            else:
                stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
        elif s == "mm1":
            stop_price, take_price = compute_brackets(px, float(MM1_STOP_PCT), float(MM1_TAKE_PCT))
        else:
            if bool(getattr(settings, "atr_brackets_enabled", False)):
                try:
                    atr_len = int(getattr(settings, "atr_bracket_len", 14) or 14)
                    bars_for_atr = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, atr_len + 40))
                    atr_now, _ = _atr_from_bars(bars_for_atr or [], length=int(atr_len))
                    if atr_now is not None and float(atr_now) > 0:
                        stop_price, take_price = compute_atr_brackets(
                            float(px),
                            float(atr_now),
                            float(getattr(settings, "atr_stop_mult", 2.0) or 2.0),
                            float(getattr(settings, "atr_take_mult", 4.0) or 4.0),
                        )
                    else:
                        stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
                except Exception:
                    stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
            else:
                stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)

    account_truth = _account_truth_snapshot()
    if bool(getattr(settings, 'require_broker_balance_ok_for_entry', True)) and not bool(account_truth.get('balance_ok')):
        return ignored('broker_balance_unavailable', symbol=symbol, strategy=strategy, balance_error=account_truth.get('balance_error'), reconcile=reconcile_meta)

    ops_remaining = int(state.ops_lockout_remaining_sec() if hasattr(state, 'ops_lockout_remaining_sec') else 0)
    if ops_remaining > 0:
        return ignored('ops_risk_lockout_active', symbol=symbol, strategy=strategy, remaining_sec=ops_remaining, lock_reason=getattr(state, 'last_ops_lock_reason', ''))

    pretrade_gate = _pretrade_health_gate_summary(rerun_startup_check=False)
    if not bool(pretrade_gate.get('gate_open')):
        violations = list(pretrade_gate.get('violations') or [])
        return ignored(str(violations[0] if violations else 'pretrade_health_gate_closed'), symbol=symbol, strategy=strategy, pretrade_health_gate=pretrade_gate, reconcile=reconcile_meta)

    risk_admission = _risk_admission_check(symbol=symbol, strategy=strategy, px=float(px), stop_price=float(stop_price), take_price=float(take_price), account_truth=account_truth)
    if not bool(risk_admission.get('ok')):
        _record_state_model_anomaly('risk_admission_reject', 'warn', symbol=symbol, strategy=strategy, risk_admission=risk_admission)
        return ignored(str((risk_admission.get('violations') or ['risk_admission_reject'])[0]), symbol=symbol, strategy=strategy, risk_admission=risk_admission)

    # Exposure caps (0 disables). We may need current exposure for sizing.
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)

    positions: list[dict] = []
    total_exposure = 0.0
    sym_exposure = 0.0

    sizing_mode = str(getattr(settings, "sizing_mode", "fixed") or "fixed").strip().lower()
    if sizing_mode == "risk_pct":
        sizing_mode = "risk_pct_equity"

    if max_total or max_symbol or (notional is None and sizing_mode in ("risk_pct_equity", "equity_fraction")):
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)

    # Determine entry notional (fixed by default; risk sizing is opt-in).
    sizing_meta: dict | None = None
    if notional is not None:
        _notional = float(notional)
    elif sizing_mode == "equity_fraction":
        bals = _balances_by_asset()
        usd_cash = float(_stable_cash_usd(bals) or 0.0)
        equity_total = float(usd_cash) + float(_portfolio_exposure_usd_from_balances(bals))
        res = compute_equity_fraction_notional(
            equity_usd=equity_total,
            fraction=float(getattr(settings, "equity_fraction_per_trade", 0.05) or 0.05),
            min_order_notional_usd=float(settings.min_order_notional_usd),
            available_cash_usd=usd_cash,
            max_total_exposure_usd=max_total,
            current_total_exposure_usd=total_exposure,
            max_symbol_exposure_usd=max_symbol,
            current_symbol_exposure_usd=sym_exposure,
            max_notional_usd=float(getattr(settings, "max_notional_usd", 0.0) or 0.0),
        )
        sizing_meta = res.to_dict()
        sizing_meta.pop("reason", None)
        if not res.ok:
            return ignored(res.reason, symbol=symbol, strategy=strategy, risk_admission=risk_admission, **(sizing_meta or {}))
        _notional = float(res.notional_usd)
    elif sizing_mode == "risk_pct_equity":
        bals = _balances_by_asset()
        usd_cash = float(_stable_cash_usd(bals) or 0.0)
        equity_total = float(usd_cash) + float(_portfolio_exposure_usd_from_balances(bals))
        res = compute_risk_based_notional_actual(
            equity_usd=equity_total,
            risk_per_trade=float(getattr(settings, "risk_per_trade", 0.03) or 0.03),
            effective_stop_pct=float(risk_admission.get('effective_stop_pct') or 0.0),
            min_order_notional_usd=float(settings.min_order_notional_usd),
            available_cash_usd=usd_cash,
            max_total_exposure_usd=max_total,
            current_total_exposure_usd=total_exposure,
            max_symbol_exposure_usd=max_symbol,
            current_symbol_exposure_usd=sym_exposure,
            max_notional_usd=float(getattr(settings, "max_notional_usd", 0.0) or 0.0),
        )
        sizing_meta = res.to_dict()
        sizing_meta.pop("reason", None)
        if not res.ok:
            return ignored(res.reason, symbol=symbol, strategy=strategy, risk_admission=risk_admission, **(sizing_meta or {}))
        _notional = float(res.notional_usd)
    else:
        _notional = float(settings.default_notional_usd)

    if _notional < float(settings.min_order_notional_usd):
        return ignored(
            "notional_below_minimum",
            symbol=symbol,
            strategy=strategy,
            notional_usd=_notional,
            min_notional_usd=float(settings.min_order_notional_usd),
            risk_admission=risk_admission,
            **(sizing_meta or {}),
        )

    # Enforce exposure caps (0 disables)
    if max_total or max_symbol:
        if max_total and (total_exposure + _notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, _notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd", **(sizing_meta or {})}
        if max_symbol and (sym_exposure + _notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, _notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd", **(sizing_meta or {})}

    cash_usd = float(account_truth.get('cash_usd', 0.0) or 0.0)
    if cash_usd + 1e-9 < float(_notional):
        return ignored('insufficient_cash_usd_estimate', symbol=symbol, strategy=strategy, cash_usd=cash_usd, requested_notional_usd=float(_notional), reconcile=reconcile_meta, risk_admission=risk_admission)

    min_cash_buffer_usd = float(getattr(settings, 'min_cash_buffer_usd', 0.0) or 0.0)
    cash_after = float(cash_usd - float(_notional))
    if min_cash_buffer_usd > 0 and cash_after < float(min_cash_buffer_usd):
        return ignored('min_cash_buffer_breach', symbol=symbol, strategy=strategy, cash_usd=cash_usd, cash_after_usd=cash_after, requested_notional_usd=float(_notional), min_cash_buffer_usd=min_cash_buffer_usd, reconcile=reconcile_meta, risk_admission=risk_admission)

    max_util = float(getattr(settings, 'max_account_utilization_pct', 0.0) or 0.0)
    equity_usd = float(account_truth.get('equity_usd', 0.0) or 0.0)
    if max_util > 0 and equity_usd > 0:
        util_after = (float(account_truth.get('position_exposure_usd', 0.0) or 0.0) + float(account_truth.get('open_buy_order_notional_usd', 0.0) or 0.0) + float(_notional)) / float(equity_usd)
        if util_after > max_util:
            return ignored('max_account_utilization_pct', symbol=symbol, strategy=strategy, equity_usd=equity_usd, utilization_after_pct=util_after, max_account_utilization_pct=max_util, requested_notional_usd=float(_notional), reconcile=reconcile_meta, risk_admission=risk_admission)

    # Execution profile overrides (e.g., maker-only for CR1/MM1)
    exec_mode_override = None
    post_offset_override = None
    chase_sec_override = None
    chase_steps_override = None
    market_fallback_override = None
    s = (strategy or "").strip().lower()
    if s == "cr1" and CR1_MAKER_ONLY:
        exec_mode_override = "maker_first"
        market_fallback_override = False
    if s == "mm1" and MM1_MAKER_ONLY:
        exec_mode_override = "maker_first"
        market_fallback_override = False
        chase_sec_override = int(MM1_CHASE_SEC)
        chase_steps_override = 1
        post_offset_override = float(MM1_POST_ONLY_OFFSET_PCT)

    trade_plan_id = str(uuid4())
    intent_id = str(uuid4())
    position_id = str(uuid4())
    signal_key = str(req_id or uuid4())
    risk_snapshot = _risk_snapshot_for_entry(symbol=symbol, strategy=strategy, px=float(px), stop_price=float(stop_price), take_price=float(take_price), notional=float(_notional))
    risk_snapshot["risk_admission"] = risk_admission
    risk_snapshot["sizing"] = sizing_meta or {}
    plan_created_ts = time.time()
    plan_time_stop_sec = int(_strategy_max_hold_sec(strategy) or 0)
    plan_expires_ts = plan_created_ts + float(plan_time_stop_sec if plan_time_stop_sec > 0 else 3600)
    lifecycle_db.upsert_trade_plan({
        "trade_plan_id": trade_plan_id,
        "symbol": symbol,
        "strategy_id": strategy,
        "signal_id": signal_key,
        "status": "approved",
        "direction": "buy",
        "entry_mode": exec_mode_override or settings.execution_mode,
        "entry_ref_price": float(px),
        "stop_price": float(stop_price),
        "target_price": float(take_price),
        "time_stop_sec": plan_time_stop_sec,
        "requested_notional_usd": float(_notional),
        "approved_notional_usd": float(_notional),
        "risk_snapshot_json": risk_snapshot,
        "legacy_symbol_key": symbol,
        "created_ts": plan_created_ts,
        "expires_ts": plan_expires_ts,
        "closed_ts": None,
    })
    client_order_key = f"entry:{signal_key}:{symbol}:{strategy}"
    lifecycle_db.upsert_order_intent({
        "intent_id": intent_id,
        "trade_plan_id": trade_plan_id,
        "symbol": symbol,
        "side": "buy",
        "order_type": exec_mode_override or settings.execution_mode,
        "strategy_id": strategy,
        "state": "validated",
        "desired_qty": (float(_notional) / float(px)) if float(px) > 0 else 0.0,
        "desired_notional_usd": float(_notional),
        "limit_price": float(px),
        "client_order_key": client_order_key,
        "raw_json": {"req_id": req_id, "source": source, "dry_run": dry_run},
    })

    state.set_order_lock(entry_lock_key, meta={"symbol": symbol, "side": "buy", "strategy": strategy, "req_id": req_id, "trade_plan_id": trade_plan_id, "intent_id": intent_id})
    lifecycle_db.transition_order_intent(intent_id, "submitted", client_order_key=client_order_key, submitted_ts=time.time())
    lifecycle_db.update_trade_plan_status(trade_plan_id, "submitted")
    res = _market_notional(
        symbol=symbol,
        side="buy",
        notional=_notional,
        strategy=strategy,
        price=px,
        exec_mode_override=exec_mode_override,
        post_offset_override=post_offset_override,
        chase_sec_override=chase_sec_override,
        chase_steps_override=chase_steps_override,
        market_fallback_override=market_fallback_override,
    )
    if not (res and res.get("ok")):
        state.clear_order_lock(entry_lock_key)
        classified = execution_state.classify_order_result(res or {})
        next_state = classified.get("state") or "rejected"
        lifecycle_db.transition_order_intent(
            intent_id,
            str(next_state),
            broker_txid=classified.get("broker_txid"),
            filled_qty=classified.get("filled_qty"),
            avg_fill_price=classified.get("avg_fill_price"),
            fees_usd=classified.get("fees_usd"),
            reject_reason=str(classified.get("error") or (res or {}).get("error") or "order_failed"),
            cancel_reason=str(classified.get("error") or "") if str(next_state) == "cancelled" else None,
            last_broker_status=str(classified.get("execution") or ""),
            raw_json=res or {},
        )
        lifecycle_db.update_trade_plan_status(trade_plan_id, "rejected" if str(next_state) == "rejected" else "submitted")
        rej_count = state.note_entry_rejection() if hasattr(state, "note_entry_rejection") else 0
        if int(rej_count) >= int(getattr(settings, "max_consecutive_rejections", 0) or 0) and int(getattr(settings, "max_consecutive_rejections", 0) or 0) > 0 and hasattr(state, "set_ops_lockout"):
            state.set_ops_lockout("consecutive_entry_rejections", int(getattr(settings, "ops_lockout_sec", 0) or 0))
        _record_state_model_anomaly("entry_order_rejected", "warn", symbol=symbol, trade_plan_id=trade_plan_id, intent_id=intent_id, response=res or {}, classified=classified, consecutive_entry_rejections=int(getattr(state, "consecutive_entry_rejections", 0) or 0))
    evt = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "req_id": req_id,
        "kind": source,
        "status": "dry_run" if dry_run else "executed",
        "action": "buy",
        "symbol": symbol,
        "strategy": strategy,
        "signal": signal_name,
        "signal_id": signal_id,
        "price": px,
        "notional_usd": _notional,
        "stop": float(stop_price),
        "take": float(take_price),
        "order": res,
        "client_ip": client_ip,
    }
    if extra:
        evt.update(extra)
    _log_event("info", evt)

    if res.get("ok"):
        opened_ts = time.time()
        try:
            _record_open_trade_journal(
                symbol=symbol,
                strategy=strategy,
                source=source,
                signal_name=signal_name,
                signal_id=signal_id,
                req_id=req_id,
                requested_notional_usd=float(_notional),
                stop_price=float(stop_price),
                take_price=float(take_price),
                px_fallback=float(px),
                res=res,
                opened_ts=opened_ts,
            )
        except Exception:
            pass
        if hasattr(state, "clear_entry_rejections"):
            state.clear_entry_rejections()
        metrics = _extract_execution_metrics(res, fallback_price=float(px), fallback_qty=(float(_notional) / float(px)) if float(px) > 0 else 0.0)
        plan_entry_px = float(metrics.get('avg_price') or px)
        fees_usd = float(metrics.get('fee') or 0.0)
        qty = float(metrics.get('qty') or ((float(_notional) / float(plan_entry_px)) if float(plan_entry_px) > 0 else 0.0))
        classified = execution_state.classify_order_result(res or {})
        broker_txid = classified.get("broker_txid")
        lifecycle_db.transition_order_intent(intent_id, "acknowledged", broker_txid=broker_txid, last_broker_status=str(classified.get("execution") or ""), raw_json=res or {})
        lifecycle_db.transition_order_intent(intent_id, "filled", broker_txid=broker_txid, filled_qty=qty, avg_fill_price=float(plan_entry_px), fees_usd=fees_usd, remaining_qty=0.0, last_broker_status=str(classified.get("execution") or ""), raw_json=res or {})
        lifecycle_db.update_trade_plan_status(trade_plan_id, "active", approved_notional_usd=float(_notional))
        lifecycle_db.upsert_position_ledger({
            "position_id": position_id,
            "trade_plan_id": trade_plan_id,
            "symbol": symbol,
            "side": "buy",
            "qty": qty,
            "avg_entry_price": float(plan_entry_px),
            "notional_usd": float(_notional),
            "realized_pnl_usd": 0.0,
            "unrealized_pnl_usd": 0.0,
            "fees_usd": fees_usd,
            "status": "open",
            "broker_position_qty": qty,
            "opened_ts": opened_ts,
        })
        lifecycle_db.insert_fill_event({
            "fill_id": f"{intent_id}:entry",
            "intent_id": intent_id,
            "trade_plan_id": trade_plan_id,
            "symbol": symbol,
            "side": "buy",
            "price": float(plan_entry_px),
            "qty": qty,
            "notional_usd": float(_notional),
            "fee_usd": fees_usd,
            "fill_ts": opened_ts,
            "broker_txid": broker_txid,
            "raw_json": metrics,
        })
        state.mark_enter(symbol)
        state.set_plan(TradePlan(
            symbol=symbol,
            trade_plan_id=trade_plan_id,
            position_id=position_id,
            signal_id=signal_key,
            status="active",
            entry_mode=str(exec_mode_override or settings.execution_mode),
            risk_snapshot=risk_snapshot,
            strategy=strategy,
            side="buy",
            entry_price=float(plan_entry_px),
            stop_price=float(stop_price),
            take_price=float(take_price),
            notional_usd=float(_notional),
            opened_ts=opened_ts,
            max_hold_sec=_strategy_max_hold_sec(strategy),
        ))
        return {"ok": True, "executed": True, "symbol": symbol, "strategy": strategy, "price": plan_entry_px, "stop": float(stop_price), "take": float(take_price), "trade_plan_id": trade_plan_id, "intent_id": intent_id, "position_id": position_id}

    # Do NOT create a plan or cooldown on failed orders (e.g., insufficient funds).
    return {"ok": False, "executed": False, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price), "error": res.get("error") or "order_failed", "trade_plan_id": trade_plan_id, "intent_id": intent_id, "position_id": position_id}


@app.post("/webhook")
def webhook(payload: WebhookPayload, request: Request):
    req_id = request.headers.get("x-request-id") or str(uuid4())
    client_ip = getattr(request.client, "host", None)

    def ignored(reason: str, **extra):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "ignored",
            "reason": reason,
            "client_ip": client_ip,
            **extra,
        }
        _log_event("warning", evt)
        return {"ok": True, "ignored": True, "reason": reason, **extra}

    # Secret
    if not settings.webhook_secret or payload.secret != settings.webhook_secret:
        _log_event(
            "warning",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "req_id": req_id,
                "kind": "webhook",
                "status": "rejected",
                "reason": "invalid_secret",
                "client_ip": client_ip,
            },
        )
        raise HTTPException(status_code=401, detail="invalid secret")

    # Symbol allowlist (static env allowlist)
    symbol = _validate_symbol(payload.symbol)

    # Scanner gate (dynamic top5) — soft allow enabled by env
    allowed, allow_reason, allow_meta = _symbol_allowed_by_scanner(symbol)
    if not allowed:
        return ignored(
            allow_reason,
            symbol=symbol,
            strategy=str(payload.strategy),
            signal=(payload.signal or None),
            signal_id=(payload.signal_id or None),
            **allow_meta,
        )

    side = str(payload.side).lower().strip()
    strategy = str(payload.strategy).strip()
    signal_name = (payload.signal or "").strip() or None
    signal_id = (payload.signal_id or "").strip() or None

    if side != "buy":
        return ignored("long_only_mode", symbol=symbol, side=side, strategy=strategy)

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    # Idempotency / TV retries
    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored(
            "duplicate_signal_id",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            ttl_sec=int(settings.signal_dedupe_ttl_sec),
        )

    # Master kill switch
    if not bool(settings.trading_enabled):
        return ignored("trading_disabled", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    # 24/7 by default; cutoff enforced only if set
    if settings.no_new_entries_after_utc and _is_after_utc_hhmm(now, settings.no_new_entries_after_utc):
        return ignored(
            "entries_disabled_after_cutoff",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            cutoff_utc=settings.no_new_entries_after_utc,
            utc=now.isoformat(),
        )

    # Optional short flatten window block (off by default)
    if settings.enforce_daily_flatten and settings.block_entries_after_flatten:
        if _within_minutes_after_utc_hhmm(now, settings.daily_flatten_time_utc, window_min=3):
            return ignored(
                "entries_blocked_during_flatten_window",
                symbol=symbol,
                strategy=strategy,
                signal=signal_name,
                signal_id=signal_id,
                flatten_utc=settings.daily_flatten_time_utc,
                utc=now.isoformat(),
            )

    # Entry cooldown
    if not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored(
            "entry_cooldown_active",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            cooldown_sec=int(settings.entry_cooldown_sec),
        )

    # Max trades/day per symbol
    trades = int(state.trades_today_by_symbol.get(symbol, 0) or 0)
    if trades >= int(settings.max_trades_per_symbol_per_day):
        return ignored(
            "max_trades_reached",
            symbol=symbol,
            strategy=strategy,
            trades_today=trades,
            max_trades=int(settings.max_trades_per_symbol_per_day),
        )

    # One position per symbol
    has_pos, pos_notional = _has_position(symbol)
    if has_pos:
        return ignored("position_already_open", symbol=symbol, strategy=strategy, position_notional_usd=pos_notional)

    # Notional
    notional = float(payload.notional_usd or settings.default_notional_usd)
    if notional < float(settings.min_order_notional_usd):
        return ignored(
            "notional_below_minimum",
            symbol=symbol,
            strategy=strategy,
            notional_usd=notional,
            min_notional_usd=float(settings.min_order_notional_usd),
        )

    # Execute BUY
    stop_price = payload.stop_price
    take_price = payload.take_price
    # Determine entry price for brackets/notional. If the alert didn't include a price,
    # fall back to the latest trade price.
    px = float(getattr(payload, "price", 0.0) or 0.0)
    if px <= 0:
        px = float(getattr(payload, "entry_price", 0.0) or 0.0)
    if px <= 0:
        px = float(_last_price(symbol))
    
    if stop_price is None or take_price is None:
        stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)

    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    res = _market_notional(symbol=symbol, side="buy", notional=notional, strategy=strategy, price=px)

    # Log the attempt
    _log_event(
        "info",
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "executed" if (res and res.get("ok")) else "error",
            "action": "buy",
            "symbol": symbol,
            "strategy": strategy,
            "signal": signal_name,
            "signal_id": signal_id,
            "price": px,
            "notional_usd": notional,
            "stop": float(stop_price),
            "take": float(take_price),
            "scanner_gate": {"allowed": True, "reason": allow_reason, **allow_meta},
            "order": res,
            "client_ip": client_ip,
        },
    )

    # Guard: only create the plan (and count the trade) if the entry order actually succeeded.
    if not (res and res.get("ok")):
        return {"ok": False, "action": "buy", "symbol": symbol, "error": (res or {}).get("error") or "entry_order_failed", "order": res}

    state.mark_enter(symbol)
    state.set_plan(TradePlan(
        symbol=symbol,
        side="buy",
        notional_usd=notional,
        entry_price=px,
        stop_price=stop_price,
        take_price=take_price,
        strategy=strategy,
        opened_ts=now.timestamp(),
        max_hold_sec=_strategy_max_hold_sec(strategy),
    ))
    n = state.inc_trade(symbol)

    return {
        "ok": True,
        "action": "buy",
        "symbol": symbol,
        "price": px,
        "stop": stop_price,
        "take": take_price,
        "trade_count_today": n,
        "signal": signal_name,
        "signal_id": signal_id,
        "scanner_gate": {"reason": allow_reason, **allow_meta},
        "result": res,
    }


@app.post("/worker/exit")
def worker_exit(payload: WorkerExitPayload):
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    try:
        now = datetime.now(timezone.utc)
        utc_date = _utc_date_str(now)

        did_flatten = False
        if settings.enforce_daily_flatten and _is_flatten_time(now, settings.daily_flatten_time_utc) and state.last_flatten_utc_date != utc_date:
            did_flatten = True
            state.last_flatten_utc_date = utc_date

        exits: list[dict] = []
        evaluations: list[dict] = []
        bal = _balances_by_asset()
        stale_order_locks_cleared = state.clear_stale_order_locks(OPEN_ORDER_LOCK_TTL_SEC)
        try:
            stale_pending_cleared = int(state.clear_stale_pending_exits(PENDING_EXIT_TTL_SEC))
        except Exception:
            stale_pending_cleared = 0

        # --- Deterministic lifecycle cycle ---
        # Universe = balances-derived holdings + state.open_positions + any planned symbols.
        # We evaluate each symbol once per cycle and take at most one action.
        pos_syms = {p.get("symbol") for p in get_positions() if p.get("symbol")}
        plan_syms = set(getattr(state, "plans", {}).keys()) if hasattr(state, "plans") else set()
        state_open = set(getattr(state, "open_positions", []) or [])
        symbols = sorted({sym for sym in (pos_syms | plan_syms | state_open) if sym})

        # Config knobs
        dust_floor = float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0)
        adopt_floor = max(
            float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0),
            float(getattr(settings, "exit_min_notional_usd", 0.0) or 0.0),
        )
        max_hold_default = int(getattr(settings, "max_hold_sec", 0) or 0)
        grace = int(getattr(settings, "time_exit_grace_sec", 0) or 0)
        cooldown = int(getattr(settings, "exit_cooldown_sec", 0) or 0)
        dry_run = bool(getattr(settings, "exit_dry_run", False))
        diagnostics = bool(getattr(settings, "exit_diagnostics", False))

        def _ensure_plan(symbol: str, *, qty: float, px: float) -> TradePlan | None:
            """Return an existing plan, or adopt a holding into a plan if eligible.

            Adoption is intentionally conservative:
            - Never create plans for dust holdings.
            - Never create plans below adopt_floor.
            - Adopted plans start their clock at adoption time to avoid breaking live legacy balances.
            """
            plan0 = state.plans.get(symbol)
            if plan0:
                return plan0
            pos_notional = float(qty) * float(px)
            if dust_floor and pos_notional < dust_floor:
                # Dust holdings: do not track or exit.
                state.remove_plan(symbol)
                state.clear_pending_exit(symbol)
                return None
            if pos_notional < adopt_floor:
                return None

            entry_px = float(px) if px > 0 else float(_last_price(symbol) or 0.0)
            if entry_px <= 0:
                return None
            stop_px, take_px = compute_brackets(entry_px, settings.stop_pct, settings.take_pct)
            plan_new = TradePlan(
                symbol=symbol,
                side="buy",
                notional_usd=float(pos_notional),
                entry_price=float(entry_px),
                stop_price=float(stop_px),
                take_price=float(take_px),
                strategy="adopted",
                opened_ts=now.timestamp(),
                max_hold_sec=int(max_hold_default),
                breakeven_armed=False,
                breakeven_triggered_ts=0.0,
            )
            state.set_plan(plan_new)
            try:
                if not trade_journal.get_open_trade(symbol):
                    trade_journal.upsert_open_trade({
                        'symbol': symbol,
                        'opened_ts': now.timestamp(),
                        'strategy': 'adopted',
                        'source': 'adopted',
                        'signal_name': 'adopted',
                        'signal_id': None,
                        'req_id': f'adopt:{symbol}:{int(now.timestamp())}',
                        'entry_txid': None,
                        'entry_execution': 'adopted',
                        'entry_price': float(entry_px),
                        'entry_qty': float(qty),
                        'entry_cost': float(pos_notional),
                        'entry_fee': 0.0,
                        'requested_notional_usd': float(pos_notional),
                        'stop_price': float(stop_px),
                        'take_price': float(take_px),
                        'meta': {'adopted': True},
                    })
            except Exception:
                pass
            return plan_new

        def _maybe_apply_breakeven(plan: TradePlan, *, px: float, entry_px: float) -> None:
            """Move stop to break-even once, if enabled and trigger reached."""
            if not bool(getattr(settings, "breakeven_enabled", False)):
                return
            if bool(getattr(plan, "breakeven_armed", False)):
                return
            trigger = float(getattr(settings, "breakeven_trigger_pct", 0.0) or 0.0)
            offset = float(getattr(settings, "breakeven_offset_pct", 0.0) or 0.0)
            if trigger <= 0 or entry_px <= 0 or px <= 0:
                return
            if px < (entry_px * (1.0 + trigger)):
                return

            be_stop = entry_px * (1.0 + offset)
            # Never loosen the stop; only tighten it upward.
            if be_stop > float(getattr(plan, "stop_price", 0.0) or 0.0):
                plan.stop_price = float(be_stop)
            plan.breakeven_armed = True
            plan.breakeven_triggered_ts = now.timestamp()
            # Persist tightened stop / breakeven state
            state.set_plan(plan)
            _log_event(
                "info",
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "kind": "plan_update",
                    "status": "breakeven_set",
                    "symbol": plan.symbol,
                    "strategy": plan.strategy,
                    "entry_price": float(entry_px),
                    "price": float(px),
                    "stop_price": float(plan.stop_price),
                    "trigger_pct": float(trigger),
                    "offset_pct": float(offset),
                },
            )

        def _choose_exit_reason(*, px: float, plan: TradePlan, age_sec: float, max_hold_sec: int) -> tuple[str | None, dict]:
            """Select a single exit action for this cycle (or None), plus a decision trace."""
            eligible = {
                "eligible_daily_flatten": bool(did_flatten),
                "eligible_stop": (px <= float(plan.stop_price)) and (age_sec >= float(getattr(settings, "min_hold_sec_before_stop", 0) or 0)),
                "eligible_take": px >= float(plan.take_price),
                "eligible_time": (max_hold_sec > 0 and age_sec >= float(max_hold_sec + grace)),
            }
            if eligible["eligible_daily_flatten"]:
                return "daily_flatten", eligible
            if eligible["eligible_stop"]:
                return "stop", eligible
            if eligible["eligible_take"]:
                return "take", eligible
            if eligible["eligible_time"]:
                return "time", eligible
            return None, eligible

        for symbol in symbols:
            # Phase 1: snapshot current holding
            try:
                px = float(_last_price(symbol) or 0.0)
            except Exception:
                px = 0.0
            if px <= 0:
                continue

            base = _base_asset(symbol)
            qty = float(bal.get(base, 0.0) or 0.0)
            if qty <= 0:
                # No holding: clear stale state.
                state.remove_plan(symbol)
                state.clear_pending_exit(symbol)
                continue

            if state.has_pending_exit(symbol, PENDING_EXIT_TTL_SEC):
                if diagnostics:
                    evaluations.append({
                        "symbol": symbol,
                        "decision": None,
                        "skip_reason": "pending_exit_inflight",
                        "pending_exit": state.pending_exits.get(symbol),
                    })
                continue

            # Phase 2: plan resolution / conservative adoption
            plan = _ensure_plan(symbol, qty=qty, px=px)
            if not plan:
                continue

            # Phase 3: derived metrics
            entry_px = float(getattr(plan, "entry_price", 0.0) or 0.0) or float(px)
            opened_ts = float(getattr(plan, "opened_ts", 0.0) or 0.0)
            age_sec = (now.timestamp() - opened_ts) if opened_ts > 0 else 0.0
            plan_max = int(getattr(plan, "max_hold_sec", 0) or 0)
            max_hold = int(plan_max if plan_max > 0 else max_hold_default)

            # Phase 4: lifecycle updates (state mutation only)
            be_before = bool(getattr(plan, "breakeven_armed", False))
            try:
                _maybe_apply_breakeven(plan, px=px, entry_px=entry_px)
            except Exception:
                # Lifecycle updates should never crash the exit loop.
                pass
            be_after = bool(getattr(plan, "breakeven_armed", False))

            # Phase 5: decide action (at most one)
            reason, trace = _choose_exit_reason(px=px, plan=plan, age_sec=age_sec, max_hold_sec=max_hold)
            # Optional diagnostics per symbol
            if diagnostics:
                evaluations.append({
                    "symbol": symbol,
                    "strategy": plan.strategy,
                    "price": float(px),
                    "qty": float(qty),
                    "entry_price": float(entry_px),
                    "stop_price": float(plan.stop_price),
                    "take_price": float(plan.take_price),
                    "age_sec": round(age_sec, 3),
                    "max_hold_sec": int(getattr(settings, "max_hold_sec", 0) or 0),
                    "breakeven_before": bool(be_before),
                    "breakeven_after": bool(be_after),
                    "breakeven_set": bool((not be_before) and be_after),
                    "decision": reason,
                    **trace,
                    "can_exit": bool(state.can_exit(symbol, cooldown)),
                    "dry_run": bool(dry_run),
                })
            if not reason:
                continue
            can_exit_now = state.can_exit(symbol, cooldown)
            if not can_exit_now:
                continue

            # Phase 6: execute
            notional_exit = max(float(settings.exit_min_notional_usd), float(plan.notional_usd))
            exit_intent_id = str(uuid4())
            exit_client_order_key = f"exit:{getattr(plan, 'trade_plan_id', '') or symbol}:{reason}"
            lifecycle_db.upsert_order_intent({
                "intent_id": exit_intent_id,
                "trade_plan_id": getattr(plan, "trade_plan_id", "") or None,
                "symbol": symbol,
                "side": "sell",
                "order_type": STOP_EXIT_MODE if reason == "stop" else str(getattr(settings, "execution_mode", "market") or "market"),
                "strategy_id": plan.strategy,
                "state": "validated",
                "desired_qty": float(qty),
                "desired_notional_usd": float(notional_exit),
                "limit_price": float(px),
                "client_order_key": exit_client_order_key,
                "raw_json": {"reason": reason, "dry_run": dry_run},
            })
            exit_lock_key = f"sell:{symbol}"
            if state.has_order_lock(exit_lock_key, OPEN_ORDER_LOCK_TTL_SEC):
                if diagnostics:
                    evaluations.append({"symbol": symbol, "decision": reason, "skip_reason": "exit_order_lock_active", "ttl_sec": int(OPEN_ORDER_LOCK_TTL_SEC)})
                continue
            if ENABLE_BROKER_OPEN_ORDER_GUARD:
                has_sell_order, sell_meta = _has_broker_open_order(symbol, 'sell')
                if has_sell_order:
                    if diagnostics:
                        evaluations.append({"symbol": symbol, "decision": reason, "skip_reason": "broker_open_sell_order_exists", "meta": sell_meta})
                    continue
            if dry_run:
                lifecycle_db.transition_order_intent(exit_intent_id, "acknowledged", client_order_key=exit_client_order_key, last_broker_status="dry_run")
                res = {"ok": True, "dry_run": True, "side": "sell", "symbol": symbol, "notional": notional_exit, "reason": reason}
            else:
                state.set_order_lock(exit_lock_key, meta={"symbol": symbol, "side": "sell", "reason": reason, "intent_id": exit_intent_id})
                lifecycle_db.transition_order_intent(exit_intent_id, "submitted", client_order_key=exit_client_order_key, submitted_ts=time.time())
                if reason == "stop" and STOP_EXIT_MODE == "stop_limit":
                    res = broker_kraken.stop_limit_notional(
                        symbol=symbol,
                        notional=notional_exit,
                        stop_price=float(plan.stop_price),
                        strategy=plan.strategy,
                        limit_buffer_pct=float(getattr(settings, 'stop_limit_buffer_pct', 0.01) or 0.01),
                        timeout_sec=int(getattr(settings, 'stop_limit_timeout_sec', 60) or 60),
                    )
                else:
                    res = _market_notional(symbol=symbol, side="sell", notional=notional_exit, strategy=plan.strategy, price=px)
                classified_exit = execution_state.classify_order_result(res or {})
                if not bool(res.get("ok")):
                    state.clear_order_lock(exit_lock_key)
                    lifecycle_db.transition_order_intent(
                        exit_intent_id,
                        str(classified_exit.get("state") or "rejected"),
                        broker_txid=classified_exit.get("broker_txid"),
                        filled_qty=classified_exit.get("filled_qty"),
                        avg_fill_price=classified_exit.get("avg_fill_price"),
                        fees_usd=classified_exit.get("fees_usd"),
                        reject_reason=str(classified_exit.get("error") or (res or {}).get("error") or "exit_order_failed"),
                        cancel_reason=str(classified_exit.get("error") or "") if str(classified_exit.get("state") or "") == "cancelled" else None,
                        last_broker_status=str(classified_exit.get("execution") or ""),
                        raw_json=res or {},
                    )
                    _record_state_model_anomaly("exit_order_failed", "warn", symbol=symbol, trade_plan_id=getattr(plan, "trade_plan_id", "") or None, intent_id=exit_intent_id, response=res or {}, classified=classified_exit)
                journal_close = None
                if bool(res.get("ok")):
                    state.mark_exit(symbol)
                    state.set_pending_exit(symbol, reason=reason, txid=res.get("txid"))
                    try:
                        journal_close = _record_exit_trade_journal(symbol=symbol, reason=reason, px_fallback=float(px), res=res, closed_ts=now.timestamp())
                    except Exception:
                        journal_close = None
                    metrics_exit = _extract_execution_metrics(res, fallback_price=float(px), fallback_qty=float(qty))
                    exit_px = float(metrics_exit.get("avg_price") or px)
                    exit_qty = float(metrics_exit.get("qty") or qty)
                    exit_fees = float(metrics_exit.get("fee") or 0.0)
                    lifecycle_db.transition_order_intent(exit_intent_id, "acknowledged", broker_txid=classified_exit.get("broker_txid"), last_broker_status=str(classified_exit.get("execution") or ""), raw_json=res or {})
                    lifecycle_db.transition_order_intent(exit_intent_id, "filled", broker_txid=classified_exit.get("broker_txid"), filled_qty=exit_qty, avg_fill_price=exit_px, fees_usd=exit_fees, remaining_qty=0.0, last_broker_status=str(classified_exit.get("execution") or ""), raw_json=res or {})
                    if getattr(plan, "trade_plan_id", ""):
                        lifecycle_db.update_trade_plan_status(getattr(plan, "trade_plan_id", ""), "closed", closed_ts=now.timestamp())
                    lifecycle_db.upsert_position_ledger({
                        "position_id": getattr(plan, "position_id", "") or str(uuid4()),
                        "trade_plan_id": getattr(plan, "trade_plan_id", "") or None,
                        "symbol": symbol,
                        "side": plan.side,
                        "qty": 0.0,
                        "avg_entry_price": float(getattr(plan, "entry_price", 0.0) or 0.0),
                        "notional_usd": float(getattr(plan, "notional_usd", 0.0) or 0.0),
                        "realized_pnl_usd": float((exit_px - float(getattr(plan, "entry_price", 0.0) or 0.0)) * exit_qty - exit_fees),
                        "unrealized_pnl_usd": 0.0,
                        "fees_usd": exit_fees,
                        "status": "closed",
                        "broker_position_qty": 0.0,
                        "opened_ts": float(getattr(plan, "opened_ts", now.timestamp()) or now.timestamp()),
                        "closed_ts": now.timestamp(),
                    })
                    lifecycle_db.insert_fill_event({
                        "fill_id": f"{exit_intent_id}:exit",
                        "intent_id": exit_intent_id,
                        "trade_plan_id": getattr(plan, "trade_plan_id", "") or None,
                        "symbol": symbol,
                        "side": "sell",
                        "price": exit_px,
                        "qty": exit_qty,
                        "notional_usd": float(exit_px * exit_qty),
                        "fee_usd": exit_fees,
                        "fill_ts": now.timestamp(),
                        "broker_txid": classified_exit.get("broker_txid"),
                        "raw_json": metrics_exit,
                    })
                else:
                    # Do not latch exit cooldowns or pending state if the order failed.
                    state.clear_pending_exit(symbol)
                # Stopout cooldown latch: only on successful orders (anti-churn)
                try:
                    if bool(res.get("ok")):
                        if reason == "stop" and hasattr(state, "mark_stopout"):
                            state.mark_stopout(symbol)
                            streak = state.note_stopout() if hasattr(state, "note_stopout") else 0
                            if int(streak) >= int(getattr(settings, "max_consecutive_stopouts", 0) or 0) and int(getattr(settings, "max_consecutive_stopouts", 0) or 0) > 0 and hasattr(state, "set_ops_lockout"):
                                state.set_ops_lockout("consecutive_stopouts", int(getattr(settings, "ops_lockout_sec", 0) or 0))
                        elif reason == "take" and hasattr(state, "clear_stopout"):
                            state.clear_stopout(symbol)
                            if hasattr(state, "clear_stopout_streak"):
                                state.clear_stopout_streak()
                except Exception:
                    pass

            evt = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "kind": "exit",
                "status": "dry_run" if dry_run else "executed",
                "symbol": symbol,
                "strategy": plan.strategy,
                "reason": reason,
                "price": px,
                "notional_usd": notional_exit,
                "age_sec": round(age_sec, 3),
                "max_hold_sec": int(getattr(settings, "max_hold_sec", 0) or 0),
                "journal": (journal_close if not dry_run else None),
            }
            _log_event("info", evt)
            exits.append({"symbol": symbol, "reason": reason, "price": px, "age_sec": round(age_sec, 3), "dry_run": bool(dry_run), "result": res})

        resp = {"ok": True, "utc": now.isoformat(), "did_flatten": did_flatten, "exits": exits, "stale_pending_cleared": int(stale_pending_cleared)}
        if diagnostics:
            resp["evaluations"] = evaluations
        try:
            state.set_last_exit_status({
                "utc": now.isoformat(),
                "ok": True,
                "did_flatten": bool(did_flatten),
                "exits_count": len(exits),
                "symbols_evaluated": len(symbols),
                "stale_pending_cleared": int(stale_pending_cleared),
                "pending_exits": len(getattr(state, 'pending_exits', {}) or {}),
            })
        except Exception:
            pass
        return resp

    except HTTPException:
        raise
    except Exception as e:
        try:
            state.set_last_exit_status({
                "utc": utc_now_iso(),
                "ok": False,
                "error": str(e),
            })
        except Exception:
            pass
        _log_event(
            "error",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "kind": "worker_exit",
                "status": "error",
                "error": str(e),
            },
        )
        return JSONResponse(status_code=200, content={"ok": False, "error": str(e)})



def _entry_signals_for_symbol(symbol: str, *, regime_quiet: bool) -> tuple[dict, dict]:
    """Return (signals, debug) for the given symbol.

    signals: {"rb1": bool, "tc1": bool, "cr1"?: bool, "mm1"?: bool}
    debug:   {"rb1": {...}, "tc1": {...}, ...}
    """
    signals: dict = {}
    debug: dict = {}

    wants_rb1 = ENABLE_RB1 and (STRATEGY_MODE != "fixed" or "rb1" in ENTRY_ENGINE_STRATEGIES)
    wants_tc1 = ENABLE_TC1 and (STRATEGY_MODE != "fixed" or "tc1" in ENTRY_ENGINE_STRATEGIES)
    wants_cr1 = ENABLE_CR1 and (STRATEGY_MODE != "fixed" or "cr1" in ENTRY_ENGINE_STRATEGIES)
    wants_mm1 = ENABLE_MM1 and (STRATEGY_MODE != "fixed" or "mm1" in ENTRY_ENGINE_STRATEGIES)

    if wants_rb1:
        rb1_fired, rb1_meta = _rb1_long_signal(symbol)
        signals["rb1"] = bool(rb1_fired)
        debug["rb1"] = rb1_meta

    if wants_tc1:
        tc1_fired, tc1_meta = _tc1_long_signal(symbol)
        signals["tc1"] = bool(tc1_fired)
        debug["tc1"] = tc1_meta

    # Optional strategies for quiet / choppy regimes unless fixed mode explicitly requests them.
    if wants_cr1 and (STRATEGY_MODE == "fixed" or (STRATEGY_MODE != "legacy" and regime_quiet)):
        cr1_fired, cr1_meta = _signal_cr1(symbol)
        signals["cr1"] = bool(cr1_fired)
        debug["cr1"] = cr1_meta
    if wants_mm1 and (STRATEGY_MODE == "fixed" or (STRATEGY_MODE != "legacy" and regime_quiet)):
        mm1_fired, mm1_meta = _signal_mm1(symbol)
        signals["mm1"] = bool(mm1_fired)
        debug["mm1"] = mm1_meta

    return signals, debug



def _scanner_signal_id(symbol: str, strategy: str) -> str:
    """Deterministic idempotency key for scanner-driven entries.

    Bucket by minute so worker retries in the same minute do not submit duplicate
    entries, while fresh scans later can still place a new trade if allowed.
    """
    bucket = int(time.time() // 60)
    return f"scan:{normalize_symbol(symbol)}:{str(strategy or '').strip().lower()}:{bucket}"


def place_entry(symbol: str, *, strategy: str, req_id: str | None = None, client_ip: str | None = None, notional: float | None = None):
    """
    Wrapper used by /worker/scan_entries to execute an entry in live mode.

    Returns a tuple: (ok, reason, meta)
      - ok: bool
      - reason: str | None (present when ok=False or executed=False)
      - meta: dict (full structured execution result)
    """
    rid = req_id or str(uuid4())
    res = _execute_long_entry(
        symbol=symbol,
        strategy=strategy,
        signal_name=strategy,
        signal_id=_scanner_signal_id(symbol, strategy),
        notional=notional,
        source="scan_entries",
        req_id=rid,
        client_ip=client_ip,
        extra={"strategy": strategy},
    )
    # _execute_long_entry returns a dict like:
    #   {"ok": True, "executed": True|False, "reason": "..."} or {"ok": False, "error": "..."}
    ok = bool(res.get("ok", False))
    reason = res.get("reason") or res.get("error")
    # If ok but not executed, treat as not-ok for scan_entries "placed" reporting.
    if ok and not res.get("executed", False):
        return False, reason or "not_executed", res
    return ok, reason, res



def _count_open_positions(open_set: set[str]) -> int:
    """Count live open positions only.

    We intentionally ignore in-memory plans here because a stale plan after a
    restart must not block fresh entries when the broker reports no position.
    """
    return len(open_set or set())








@app.post("/worker/exit_diagnostics")
def worker_exit_diagnostics(payload: WorkerExitDiagnosticsPayload):
    """Dry-run style view of what /worker/exit would attempt.

    No orders are placed. This is purely diagnostic.
    """
    _require_worker_secret(payload.worker_secret)

    balances = _balances_by_asset()
    symbols = payload.symbols or []
    selected = set([normalize_symbol(s) for s in symbols]) if symbols else None

    diagnostics: list[dict] = []
    for symbol, plan in list(state.plans.items()):
        sym = normalize_symbol(symbol)
        if selected is not None and sym not in selected:
            continue

        base = _base_asset(sym)
        qty = float(balances.get(base, 0.0) or 0.0)
        px = float(_last_price(sym) or 0.0)
        notional = qty * px if (qty > 0 and px > 0) else 0.0

        stop_px = float(getattr(plan, "stop_price", 0.0) or 0.0)
        take_px = float(getattr(plan, "take_price", 0.0) or 0.0)

        should_exit = False
        reason = None
        if qty <= 0 or px <= 0:
            reason = "no_qty_or_price"
        elif stop_px and px <= stop_px:
            should_exit = True
            reason = "stop_hit"
        elif take_px and px >= take_px:
            should_exit = True
            reason = "take_hit"
        else:
            reason = "no_exit_signal"

        exit_floor = float(getattr(settings, "exit_min_notional_usd", 0.0) or 0.0)
        target_notional = max(exit_floor, float(getattr(plan, "notional_usd", 0.0) or 0.0))

        ok_to_place = should_exit
        if should_exit:
            if notional <= 0:
                ok_to_place = False
            elif notional < target_notional * 0.98:
                # likely not enough qty to satisfy a market-notional sell request
                ok_to_place = False
                reason = (reason or "exit") + "_insufficient_qty_for_target_notional"

        diagnostics.append({
            "symbol": sym,
            "qty": qty,
            "price": px,
            "position_notional_usd": notional,
            "plan": {
                "side": getattr(plan, "side", None),
                "notional_usd": float(getattr(plan, "notional_usd", 0.0) or 0.0),
                "entry_price": float(getattr(plan, "entry_price", 0.0) or 0.0),
                "stop_price": stop_px,
                "take_price": take_px,
                "strategy": getattr(plan, "strategy", None),
                "opened_ts": getattr(plan, "opened_ts", None),
            },
            "should_exit": should_exit,
            "ok_to_place": ok_to_place,
            "reason": reason,
            "target_exit_notional_usd": target_notional,
        })

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "plans_count": len(state.plans),
        "diagnostics": diagnostics,
    }


@app.post("/worker/adopt_positions")
def worker_adopt_positions(payload: WorkerAdoptPositionsPayload):
    """Create TradePlans for current Kraken balances (so exits can work).

    This does NOT place any orders — it only builds plans in memory.
    """
    _require_worker_secret(payload.worker_secret)

    balances = _balances_by_asset()

    if payload.reset_plans:
        state.plans.clear()

    include_dust = bool(payload.include_dust)
    min_notional = float(payload.min_notional_usd) if payload.min_notional_usd is not None else None

    adopted: list[dict] = []
    skipped: list[dict] = []

    # Adopt all non-USD assets with a USD price
    for asset, qty_raw in balances.items():
        asset_u = str(asset).upper().strip()
        if asset_u == "USD":
            continue
        qty = float(qty_raw or 0.0)
        if qty <= 0:
            continue

        sym = normalize_symbol(f"{asset_u}/USD")
        px = float(_last_price(sym) or 0.0)
        if px <= 0:
            skipped.append({"asset": asset_u, "symbol": sym, "reason": "no_price"})
            continue

        notional = qty * px
        floor = min_notional if min_notional is not None else float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0)
        if (not include_dust) and (notional < floor):
            skipped.append({"asset": asset_u, "symbol": sym, "notional_usd": notional, "reason": "dust"})
            continue

        if sym in state.plans:
            skipped.append({"asset": asset_u, "symbol": sym, "notional_usd": notional, "reason": "plan_exists"})
            continue

        stop_px, take_px = compute_brackets(
            entry_price=px,
            stop_pct=float(settings.stop_pct),
            take_pct=float(settings.take_pct),
        )
        plan = TradePlan(
            symbol=sym,
            side="buy",
            notional_usd=float(notional),
            entry_price=float(px),
            stop_price=float(stop_px),
            take_price=float(take_px),
            strategy="adopted",
            opened_ts=time.time(),
        )
        state.plans[sym] = plan
        adopted.append({
            "symbol": sym,
            "asset": asset_u,
            "qty": qty,
            "price": px,
            "notional_usd": notional,
            "stop_price": stop_px,
            "take_price": take_px,
        })

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "adopted_count": len(adopted),
        "skipped_count": len(skipped),
        "adopted": adopted,
        "skipped": skipped,
    }

@app.post("/worker/reset_plans")
def worker_reset_plans(payload: WorkerScanPayload):
    """Admin endpoint to clear stale in-memory plans.
    Uses the same worker secret gate as scan_entries/exit.
    NOTE: In-memory state is per-instance; this clears only the current instance.
    """
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    cleared = len(state.plans)
    state.plans.clear()
    return {"ok": True, "utc": utc_now_iso(), "cleared_plans": cleared}


@app.get("/diagnostics/risk_admission")
def diagnostics_risk_admission():
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "risk_admission": {
            "sizing_mode": str(getattr(settings, "sizing_mode", "fixed") or "fixed"),
            "risk_per_trade": float(getattr(settings, "risk_per_trade", 0.0) or 0.0),
            "entry_fee_bps": float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
            "exit_fee_bps": float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
            "slippage_bps": float(getattr(settings, "slippage_bps", 0.0) or 0.0),
            "min_effective_stop_pct": float(getattr(settings, "min_effective_stop_pct", 0.0) or 0.0),
            "max_effective_stop_pct": float(getattr(settings, "max_effective_stop_pct", 0.0) or 0.0),
            "min_risk_reward_ratio": float(getattr(settings, "min_risk_reward_ratio", 0.0) or 0.0),
            "max_notional_usd": float(getattr(settings, "max_notional_usd", 0.0) or 0.0),
            "min_order_notional_usd": float(getattr(settings, "min_order_notional_usd", 0.0) or 0.0),
        },
        "ops_risk": _ops_risk_snapshot(),
    }


@app.get("/diagnostics/ops_risk")
def diagnostics_ops_risk():
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "ops_risk": _ops_risk_snapshot(),
    }


@app.get("/diagnostics/execution_state")
def diagnostics_execution_state(limit: int = 50):
    limit = max(1, min(int(limit), 500))
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "execution_state": _execution_state_summary(),
        "order_intents": lifecycle_db.list_rows("order_intents", limit=limit, order_by="updated_ts DESC"),
    }


@app.get("/diagnostics/runtime")
def diagnostics_runtime():
    positions = get_positions()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "trading_enabled": bool(settings.trading_enabled),
        "entry_engine_enabled": bool(ENTRY_ENGINE_ENABLED),
        "stale_bar_guard_enabled": bool(ENABLE_STALE_BAR_GUARD),
        "stop_exit_mode": STOP_EXIT_MODE,
        "order_reconcile_timeout_sec": int(ORDER_RECONCILE_TIMEOUT_SEC),
        "plans_count": len(getattr(state, 'plans', {}) or {}),
        "pending_exits": getattr(state, 'pending_exits', {}),
        "order_locks": getattr(state, 'order_locks', {}),
        "positions_count": len(positions),
        "positions": positions,
        "last_balance_error": _last_balance_error(),
        "phase1_safety": _phase1_safety_report(),
        "state_model": _state_model_summary(),
        "execution_state": _execution_state_summary(),
        "startup_self_check": _startup_self_check(rerun=False),
        "recovery_reconcile": _recovery_reconcile_summary(apply=False),
        "pretrade_health_gate": _pretrade_health_gate_summary(rerun_startup_check=False),
        "ops_risk": _ops_risk_snapshot(),
        "risk_admission": {
            "sizing_mode": str(getattr(settings, "sizing_mode", "fixed") or "fixed"),
            "risk_per_trade": float(getattr(settings, "risk_per_trade", 0.0) or 0.0),
            "entry_fee_bps": float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
            "exit_fee_bps": float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
            "slippage_bps": float(getattr(settings, "slippage_bps", 0.0) or 0.0),
            "min_effective_stop_pct": float(getattr(settings, "min_effective_stop_pct", 0.0) or 0.0),
            "max_effective_stop_pct": float(getattr(settings, "max_effective_stop_pct", 0.0) or 0.0),
            "min_risk_reward_ratio": float(getattr(settings, "min_risk_reward_ratio", 0.0) or 0.0),
        },
    }

@app.get("/diagnostics/phase1_safety")
def diagnostics_phase1_safety():
    rep = _phase1_safety_report()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "phase1_safety": rep,
    }


@app.get("/diagnostics/state_model_summary")
def diagnostics_state_model_summary():
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "state_model": _state_model_summary(),
        "execution_state": _execution_state_summary(),
        "ops_risk": _ops_risk_snapshot(),
        "risk_admission": {
            "sizing_mode": str(getattr(settings, "sizing_mode", "fixed") or "fixed"),
            "risk_per_trade": float(getattr(settings, "risk_per_trade", 0.0) or 0.0),
            "entry_fee_bps": float(getattr(settings, "entry_fee_bps", 0.0) or 0.0),
            "exit_fee_bps": float(getattr(settings, "exit_fee_bps", 0.0) or 0.0),
            "slippage_bps": float(getattr(settings, "slippage_bps", 0.0) or 0.0),
            "min_effective_stop_pct": float(getattr(settings, "min_effective_stop_pct", 0.0) or 0.0),
            "max_effective_stop_pct": float(getattr(settings, "max_effective_stop_pct", 0.0) or 0.0),
            "min_risk_reward_ratio": float(getattr(settings, "min_risk_reward_ratio", 0.0) or 0.0),
        },
    }


@app.get("/diagnostics/summary")
def diagnostics_summary(limit: int = 25):
    limit = max(1, min(int(limit), 200))
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "summary": _state_model_summary(),
        "execution_state": _execution_state_summary(),
        "trade_plans": lifecycle_db.list_rows("trade_plans", limit=limit, order_by="updated_ts DESC"),
        "order_intents": lifecycle_db.list_rows("order_intents", limit=limit, order_by="updated_ts DESC"),
        "positions": lifecycle_db.list_rows("position_ledger", limit=limit, order_by="updated_ts DESC"),
        "fills": lifecycle_db.list_rows("fill_events", limit=limit, order_by="created_ts DESC"),
        "anomalies": lifecycle_db.unresolved_anomalies(limit=limit),
    }


@app.get("/diagnostics/state_model")
def diagnostics_state_model(limit: int = 25):
    limit = max(1, min(int(limit), 200))
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "summary": _state_model_summary(),
        "trade_plans": lifecycle_db.list_rows("trade_plans", limit=limit, order_by="updated_ts DESC"),
        "order_intents": lifecycle_db.list_rows("order_intents", limit=limit, order_by="updated_ts DESC"),
        "positions": lifecycle_db.list_rows("position_ledger", limit=limit, order_by="updated_ts DESC"),
        "fills": lifecycle_db.list_rows("fill_events", limit=limit, order_by="created_ts DESC"),
        "anomalies": lifecycle_db.unresolved_anomalies(limit=limit),
    }


@app.get("/diagnostics/order_intents")
def diagnostics_order_intents(limit: int = 50, state_filter: str | None = None):
    where = ''
    args = []
    if state_filter:
        where = 'state = ?'
        args = [str(state_filter)]
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "items": lifecycle_db.list_rows("order_intents", limit=limit, where=where, args=args, order_by="updated_ts DESC"),
    }


@app.get("/diagnostics/pretrade_health_gate")
def diagnostics_pretrade_health_gate(rerun_startup_check: int = 0):
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "pretrade_health_gate": _pretrade_health_gate_summary(rerun_startup_check=bool(int(rerun_startup_check or 0))),
    }


@app.get("/diagnostics/startup_self_check")
def diagnostics_startup_self_check(rerun: int = 0, apply: int = -1):
    apply_flag = None if int(apply) < 0 else bool(int(apply or 0))
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "startup_self_check": _startup_self_check(rerun=bool(int(rerun or 0)), apply=apply_flag),
    }


@app.get("/diagnostics/recovery_reconcile")
def diagnostics_recovery_reconcile(apply: int = 0):
    return _recovery_reconcile_summary(apply=bool(int(apply or 0)))


@app.get("/diagnostics/reconcile_anomalies")
def diagnostics_reconcile_anomalies(limit: int = 50):
    limit = max(1, min(int(limit or 50), 200))
    items = [
        it for it in lifecycle_db.unresolved_anomalies(limit=limit * 3)
        if str(it.get('kind') or '') in {'orphan_broker_order', 'orphan_internal_intent', 'entry_order_rejected', 'exit_order_failed'}
    ][:limit]
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "items": items,
    }


@app.get("/diagnostics/anomalies")
def diagnostics_anomalies(limit: int = 50):
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "items": lifecycle_db.unresolved_anomalies(limit=limit),
    }


@app.get("/diagnostics/last_scan")
def diagnostics_last_scan():
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "last_scan": getattr(state, 'last_scan_status', {}) or {},
    }


@app.get("/diagnostics/last_exit")
def diagnostics_last_exit():
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "last_exit": getattr(state, 'last_exit_status', {}) or {},
    }


@app.get("/diagnostics/blocked_trades")
def diagnostics_blocked_trades(limit: int = 50):
    lim = max(1, min(int(limit or 50), int(BLOCKED_TRADES_LIMIT)))
    items = list(getattr(state, 'blocked_trades', []) or [])[-lim:]
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "limit": lim,
        "count": len(items),
        "items": items,
    }


@app.post("/test/place_trade")
def test_place_trade(payload: Dict[str, Any] = Body(default={})):
    secret = (payload.get('worker_secret') or '').strip()
    ok, reason = _require_worker_secret(secret)
    if not ok:
        raise HTTPException(status_code=401, detail=reason)

    symbol = normalize_symbol(str(payload.get('symbol') or ''))
    side = str(payload.get('side') or 'buy').strip().lower()
    strategy = str(payload.get('strategy') or 'manual_test').strip() or 'manual_test'
    notional = payload.get('notional_usd')
    dry_run = bool(payload.get('dry_run', False))
    if not symbol:
        raise HTTPException(status_code=400, detail='missing symbol')
    if side not in ('buy', 'sell'):
        raise HTTPException(status_code=400, detail='side must be buy or sell')

    if dry_run:
        return {
            'ok': True,
            'dry_run': True,
            'symbol': symbol,
            'side': side,
            'strategy': strategy,
            'notional_usd': float(notional) if notional is not None else None,
        }

    if side == 'buy':
        return _execute_long_entry(
            symbol=symbol,
            strategy=strategy,
            signal_name='manual_test',
            signal_id=f'manual:{symbol}:{int(time.time())}',
            notional=float(notional) if notional is not None else None,
            source='manual_test',
            req_id=str(uuid4()),
            client_ip='manual_test',
            extra={'manual_test': True},
        )

    px = float(_last_price(symbol) or 0.0)
    if px <= 0:
        raise HTTPException(status_code=400, detail='no price available')
    use_notional = float(notional) if notional is not None else float(getattr(settings, 'exit_min_notional_usd', 0.0) or 0.0)
    return _market_notional(symbol=symbol, side='sell', notional=use_notional, strategy=strategy, price=px)


@app.post("/worker/scan_entries")
def scan_entries(payload: WorkerScanPayload):
    """
    Scan the current universe for entry signals (RB1/TC1), optionally executing orders.

    This endpoint ALWAYS returns a rich diagnostics payload so you can see exactly why
    you got 0 entries (no signals, already in position, symbol not allowed, etc.).
    """
    ok, reason = _require_worker_secret(payload.worker_secret)
    if not ok:
        try:
            state.set_last_scan_status({"utc": utc_now_iso(), "ok": False, "error": reason})
        except Exception:
            pass
        return JSONResponse(status_code=401, content={"ok": False, "utc": utc_now_iso(), "error": reason})

    # 1) Scanner → symbols (soft allowlist) + meta
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()

    # 2) Universe (explicit symbols > allowed list (+ scanner if soft allow))
    universe = _build_universe(payload, scanner_syms)

    # 3) Snapshot positions once
    positions = get_positions()
    open_set = {p["symbol"] for p in positions if p.get("qty", 0) > 0}
    open_positions_count = _count_open_positions(open_set)

    # Regime (used to optionally enable CR1/MM1 in quiet markets)
    regime_quiet, regime_meta = _regime_is_quiet()

    # 4) Evaluate signals + build diagnostics
    per_symbol: Dict[str, Any] = {}
    candidates: List[Dict[str, Any]] = []  # [{symbol,strategy,score,rank}]
    for sym in universe:
        d: Dict[str, Any] = {
            "symbol": sym,
            "in_position": sym in open_set,
            "signals": {},
            "eligible": False,
            "skip": [],
        }

        if sym in open_set:
            d["skip"].append("already_in_position")
            per_symbol[sym] = d
            continue

        if open_positions_count >= MAX_OPEN_POSITIONS:
            d["skip"].append("max_open_positions_reached")
            d["max_open_positions"] = MAX_OPEN_POSITIONS
            per_symbol[sym] = d
            continue

        try:
            fired, sig_debug = _entry_signals_for_symbol(sym, regime_quiet=bool(regime_quiet))  # (signals, debug)
        except Exception as e:
            d["skip"].append(f"signal_error:{type(e).__name__}")
            d["signal_error"] = str(e)
            per_symbol[sym] = d
            continue

        d["signals"] = fired
        d["signal_debug"] = sig_debug

        fired_strats = [k for k, v in fired.items() if v]
        if not fired_strats:
            d["skip"].append("no_signal")
            per_symbol[sym] = d
            continue

        # Strategy preference:
        # - fixed: preserve ENTRY_ENGINE_STRATEGIES env order and ignore all others
        # - auto: in quiet regime, prefer MM1/CR1 over RB1/TC1; otherwise RB1 over TC1
        # - legacy: RB1 over TC1 only
        if STRATEGY_MODE == "fixed":
            strategy = None
            for s in ENTRY_ENGINE_STRATEGIES_LIST:
                if fired.get(s):
                    strategy = s
                    break
            if not strategy:
                strategy = fired_strats[0]
        elif STRATEGY_MODE != "legacy" and bool(regime_quiet):
            if fired.get("mm1"):
                strategy = "mm1"
            elif fired.get("cr1"):
                strategy = "cr1"
            elif fired.get("rb1"):
                strategy = "rb1"
            else:
                strategy = fired_strats[0]
        else:
            strategy = "rb1" if fired.get("rb1") else fired_strats[0]
        d["eligible"] = True
        d["chosen_strategy"] = strategy
        rank = _rank_candidate(sym, strategy, sig_debug)
        d["rank"] = rank
        candidates.append({"symbol": sym, "strategy": strategy, "score": float(rank.get("score") or 0.0), "rank": rank})
        per_symbol[sym] = d

    # Apply per-scan entry cap *after* we have all candidates so we can prioritize.
    # If ENABLE_CANDIDATE_RANKING, we also sort by a simple quality score within the same
    # strategy-priority bucket (higher score = better).
    if MAX_ENTRIES_PER_SCAN > 0 and len(candidates) > MAX_ENTRIES_PER_SCAN:
        def _pri(s: str) -> int:
            s = (s or "").lower()
            if bool(regime_quiet) and STRATEGY_MODE != "legacy":
                # Quiet regime: prioritize inventory-friendly / maker-capture first.
                return {"mm1": 0, "cr1": 1, "rb1": 2, "tc1": 3}.get(s, 9)
            # Non-quiet: prioritize directional edge.
            return {"rb1": 0, "tc1": 1, "cr1": 2, "mm1": 3}.get(s, 9)

        def _uix(sym: str) -> int:
            try:
                return universe.index(sym)
            except Exception:
                return 10**9

        if ENABLE_CANDIDATE_RANKING:
            candidates_sorted = sorted(
                candidates,
                key=lambda c: (_pri(c.get("strategy")), -(float(c.get("score") or 0.0)), _uix(c.get("symbol"))),
            )
        else:
            candidates_sorted = sorted(
                candidates,
                key=lambda c: (_pri(c.get("strategy")), _uix(c.get("symbol"))),
            )

        winners = candidates_sorted[:MAX_ENTRIES_PER_SCAN]
        winner_keys = {(w.get("symbol"), w.get("strategy")) for w in winners}

        # Mark losers in diagnostics
        for c in candidates_sorted[MAX_ENTRIES_PER_SCAN:]:
            sym = c.get("symbol")
            d = per_symbol.get(sym) or {"symbol": sym, "skip": []}
            d.setdefault("skip", [])
            d["eligible"] = False
            if "max_entries_per_scan_reached" not in d["skip"]:
                d["skip"].append("max_entries_per_scan_reached")
            per_symbol[sym] = d

        candidates = winners
    else:
        # still attach a stable ordering for diagnostics
        if ENABLE_CANDIDATE_RANKING and candidates:
            candidates = sorted(
                candidates,
                key=lambda c: (-(float(c.get("score") or 0.0)), universe.index(c.get("symbol")) if c.get("symbol") in universe else 10**9),
            )
    # 5) Execute (or dry-run)
    results: List[Dict[str, Any]] = []
    for c in candidates:
        sym = c.get('symbol')
        strategy = c.get('strategy')
        score = float(c.get('score') or 0.0)
        if payload.dry_run:
            results.append({"symbol": sym, "strategy": strategy, "status": "dry_run", "score": score})
            continue

        # Spread protection (skip very wide spreads to avoid bad fills)
        bid, ask = _best_bid_ask(sym)
        spread_pct = None
        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid else None
        if MAX_SPREAD_PCT and MAX_SPREAD_PCT > 0 and spread_pct is not None and spread_pct > MAX_SPREAD_PCT:
            meta2 = {"ok": False, "ignored": True, "reason": "spread_too_wide", "symbol": sym, "strategy": strategy, "bid": bid, "ask": ask, "spread_pct": spread_pct, "max_spread_pct": MAX_SPREAD_PCT, "score": score}
            results.append({"symbol": sym, "strategy": strategy, "ok": False, "status": "skipped", "reason": "spread_too_wide", "meta": meta2})
            # Also annotate per_symbol so diagnostics show why it didn't execute.
            d = per_symbol.get(sym) or {"symbol": sym, "skip": []}
            d.setdefault("skip", [])
            if "spread_too_wide" not in d["skip"]:
                d["skip"].append("spread_too_wide")
            d["eligible"] = False
            per_symbol[sym] = d
            continue

        ok2, reason2, meta2 = place_entry(sym, strategy=strategy)
        results.append(
            {
                "symbol": sym,
                "strategy": strategy,
                "ok": ok2,
                "status": "executed" if ok2 else "skipped",
                "reason": reason2,
                "meta": (meta2 | {"score": score}) if isinstance(meta2, dict) else meta2,
            }
        )

    # --- Equity debug (helps diagnose no_equity quickly) ---
    bal_dbg = _balances_by_asset()
    stable_cash_dbg = _stable_cash_usd(bal_dbg)
    bal_keys_dbg = sorted(list(bal_dbg.keys()))[:20]
    bal_err_dbg = _last_balance_error()
    
    # 6) Return diagnostics
    strategy_summary: Dict[str, Any] = {}
    for sym, d in (per_symbol or {}).items():
        fired = d.get('signals') or {}
        chosen = d.get('chosen_strategy')
        for strat, flag in fired.items():
            bucket = strategy_summary.setdefault(strat, {'signals': 0, 'chosen': 0, 'executed': 0})
            if flag:
                bucket['signals'] += 1
        if chosen:
            strategy_summary.setdefault(chosen, {'signals': 0, 'chosen': 0, 'executed': 0})['chosen'] += 1
    for r in (results or []):
        if r.get('status') == 'executed':
            strat = str(r.get('strategy') or '')
            strategy_summary.setdefault(strat, {'signals': 0, 'chosen': 0, 'executed': 0})['executed'] += 1
    try:
        state.set_last_scan_status({
            "utc": utc_now_iso(),
            "ok": True,
            "scanner_ok": bool(scanner_ok),
            "scanner_reason": scanner_reason,
            "universe_count": len(universe),
            "results_count": len(results),
            "dry_run": bool(payload.dry_run),
        })
    except Exception:
        pass

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "universe_count": len(universe),
        "scanner": {
            "ok": scanner_ok,
            "reason": scanner_reason,
            "active_count": len(scanner_syms),
            "last_refresh_utc": (scanner_meta or {}).get("last_refresh_utc"),
            "last_error": (scanner_meta or {}).get("last_error"),
            "active_symbols": scanner_syms,
        },
        "results": results,
        "diagnostics": {
            "request": {
                "dry_run": payload.dry_run,
                "force_scan": getattr(payload, "force_scan", False),
                "symbols_provided": (getattr(payload, "symbols", None) or []),
            },
            "config": {
                "scanner_url": SCANNER_URL,
                "scanner_soft_allow": SCANNER_SOFT_ALLOW,
                "allowed_symbols_count": len(ALLOWED_SYMBOLS),
                "allowed_symbols_sample": sorted(list(ALLOWED_SYMBOLS))[:50],
                "scanner_driven_universe": SCANNER_DRIVEN_UNIVERSE,
                "scanner_target_n": SCANNER_TARGET_N,
                "universe_usd_only": UNIVERSE_USD_ONLY,
                "prefer_usd_for_stables": UNIVERSE_PREFER_USD_FOR_STABLES,
                "filter_universe_by_allowed_symbols": FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS,
                "strategy_mode": STRATEGY_MODE,
                "entry_engine_strategies": ENTRY_ENGINE_STRATEGIES_LIST,
                "enable_rb1": ENABLE_RB1,
                "enable_tc1": ENABLE_TC1,
                "enable_cr1": ENABLE_CR1,
                "enable_mm1": ENABLE_MM1,
                "max_open_positions": MAX_OPEN_POSITIONS,
                "max_entries_per_scan": MAX_ENTRIES_PER_SCAN,
                "max_entries_per_day": MAX_ENTRIES_PER_DAY,
            },
            "regime": regime_meta,
            "equity_debug": {"stable_cash_usd": stable_cash_dbg, "balance_error": bal_err_dbg, "balance_keys": bal_keys_dbg},
            "positions_count": len(positions),
            "plans_count": len(getattr(state, "plans", {}) or {}),
            "open_positions_count": open_positions_count,
            "positions_open": sorted(list(open_set))[:50],
            "candidates": [{"symbol": c.get("symbol"), "strategy": c.get("strategy"), "score": float(c.get("score") or 0.0), "rank": (c.get("rank") or {}).get("components")} for c in candidates],
            "strategy_summary": strategy_summary,
            "per_symbol": per_symbol,
        },
    }

@app.get("/dashboard")
def dashboard():
    # Dashboard is a JSON snapshot. Keep it resilient: failures here should not break the whole service.
    try:
        positions = get_positions()
    except Exception:
        positions = []

    # Safe serialization for plans/state (TradePlan is a dataclass)
    open_plans = []
    try:
        from dataclasses import asdict, is_dataclass
        for p in getattr(state, "plans", {}).values():
            open_plans.append(asdict(p) if is_dataclass(p) else p)
    except Exception:
        open_plans = []

    # Telemetry buffer is best-effort and may be absent/non-serializable
    telemetry_out = None
    try:
        telemetry_out = list(getattr(state, "telemetry", []) or [])
    except Exception:
        telemetry_out = None

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "settings": {
            "max_open_positions": MAX_OPEN_POSITIONS,
            "max_entries_per_day": MAX_ENTRIES_PER_DAY,
            "max_entries_per_scan": MAX_ENTRIES_PER_SCAN,
            "scanner_url": SCANNER_URL,
        },
        "open_positions": positions,
        "open_plans": open_plans,
        "telemetry": telemetry_out,
    }

    t = None
    try:
        t = telemetry.snapshot()  # type: ignore[attr-defined]
    except Exception:
        try:
            t = telemetry.to_dict()  # type: ignore[attr-defined]
        except Exception:
            t = None

    return {
        "ok": True,
        "time": utc_now_iso(),
        "mode": getattr(settings, "mode", None),
        "allowed_symbols": getattr(settings, "allowed_symbols", None),
        "positions": positions,
        "open_plans": list(plans.values()) if isinstance(plans, dict) else None,
        "telemetry": t,
    }


@app.get("/performance")
def performance():
    """Very simple P&L proxy using current positions (mark-to-market).

    If you're logging fills elsewhere, you can swap this for realized P&L.
    """
    positions = get_positions()
    total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
    return {
        "ok": True,
        "time": utc_now_iso(),
        "open_positions": positions,
        "total_open_notional_usd": total_exposure,
    }