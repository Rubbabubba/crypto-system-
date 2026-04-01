from __future__ import annotations

from . import trades_db
from . import lifecycle_db
from . import broker_kraken
from . import trade_journal
from . import execution_state
from . import telemetry_db
from .broker_kraken import trades_history

import logging
import json
import os
import time
import traceback
import hashlib
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
from .models import WebhookPayload, WorkerExitPayload, WorkerScanPayload, WorkerExitDiagnosticsPayload, WorkerAdoptPositionsPayload, WorkerRouteTruthPayload
from .risk import compute_brackets, compute_atr_brackets, compute_effective_stop_pct, compute_rr_ratio, compute_stop_distance_pct
from .state import InMemoryState, TradePlan
from .symbol_map import normalize_symbol, from_kraken

# Patch 026 hotfix: preserve legacy helper name in active backfill/exit paths.
_normalize_symbol = normalize_symbol


def _canonicalize_trade_symbol(symbol: str) -> str:
    s = str(symbol or '').strip().upper()
    if not s:
        return ''

    compact = s.replace('/', '').replace('-', '').replace('_', '')
    if ':' in compact:
        compact = compact.split(':', 1)[1]

    quote = ''
    base = compact
    for q in ('USDT', 'USDC', 'USD', 'EUR'):
        if compact.endswith(q):
            quote = q
            base = compact[:-len(q)]
            break

    alias_map = {
        'XXBT': 'BTC',
        'XBT': 'BTC',
        'XXBTZ': 'BTC',
        'XBTZ': 'BTC',
        'ZXXBT': 'BTC',
        'ZXBT': 'BTC',
        'XETH': 'ETH',
    }
    base = alias_map.get(base, base)
    if base.startswith(('X', 'Z')) and len(base) > 3:
        base = alias_map.get(base[1:], base[1:])
    if base.endswith(('X', 'Z')) and len(base) > 3:
        base = alias_map.get(base[:-1], base[:-1])

    if quote and base:
        if base == 'XBT':
            base = 'BTC'
        return f'{base}/{quote}'

    try:
        norm = normalize_symbol(s)
        b, q = norm.split('/', 1)
        return f"{'BTC' if b == 'XBT' else b}/{q}"
    except Exception:
        pass
    try:
        norm = from_kraken(compact)
        if norm == 'BT/USD':
            return 'BTC/USD'
        return norm
    except Exception:
        return s

from .build_info import build_payload

settings = load_settings()

# Patch 022 hotfix: define pending-exit TTL in the active exit path so
# live worker/exit cycles cannot crash on a missing module global.
PENDING_EXIT_TTL_SEC = int(getattr(settings, 'pending_exit_ttl_sec', 900) or 900)
DUST_NOTIONAL_CLOSE_THRESHOLD_USD = float(getattr(settings, 'dust_notional_close_threshold_usd', 5.0) or 5.0)
DUST_MIN_VOLUME_BUFFER_PCT = float(getattr(settings, 'dust_min_volume_buffer_pct', 0.05) or 0.05)


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


def _canonical_asset(asset: str) -> str:
    """Collapse Kraken/raw balance aliases into one canonical asset code."""
    a = str(asset or "").upper().strip()
    if not a:
        return ""
    if a.endswith(".F"):
        a = a[:-2]
    alias_map = {
        "ZUSD": "USD",
        "USD": "USD",
        "ZUSDC": "USDC",
        "USDC": "USDC",
        "ZUSDT": "USDT",
        "USDT": "USDT",
        "XXBT": "BTC",
        "XBT": "BTC",
        "BTC": "BTC",
        "XETH": "ETH",
        "ETH": "ETH",
        "XDG": "DOGE",
        "DOGE": "DOGE",
        "XXRP": "XRP",
        "XRP": "XRP",
        "XLTC": "LTC",
        "LTC": "LTC",
        "XBCH": "BCH",
        "BCH": "BCH",
    }
    return alias_map.get(a, a)


def _canonicalize_balances(balances: dict[str, float], *, aggregate: str = "sum") -> dict[str, float]:
    """Normalize balances into canonical asset codes.

    aggregate="sum" keeps additive behavior for truly distinct rows within a
    single source. aggregate="max" is used for economic truth when the same
    broker inventory appears under multiple aliases like BTC/XXBT or USD/ZUSD.
    """
    out: dict[str, float] = {}
    mode = str(aggregate or "sum").strip().lower()
    for asset, qty_raw in (balances or {}).items():
        asset_c = _canonical_asset(asset)
        try:
            qty = float(qty_raw or 0.0)
        except Exception:
            qty = 0.0
        if not asset_c or qty <= 0:
            continue
        if mode == "max":
            out[asset_c] = max(float(out.get(asset_c, 0.0) or 0.0), qty)
        else:
            out[asset_c] = float(out.get(asset_c, 0.0) or 0.0) + qty
    return out


def _economic_balance_truth(parsed: dict[str, float] | None, positions_api: dict[str, float] | None) -> dict[str, Any]:
    """Choose one economic balance per canonical asset instead of summing views.

    Kraken can surface the same inventory through multiple balance views. Those
    views improve visibility, but they must not be added together for account
    truth, exposure, or exit sizing.
    """
    parsed_canonical = _canonicalize_balances(parsed or {}, aggregate="max")
    positions_canonical = _canonicalize_balances(positions_api or {}, aggregate="max")
    assets = sorted(set(parsed_canonical.keys()) | set(positions_canonical.keys()))
    balances: dict[str, float] = {}
    selected_sources: dict[str, list[str]] = {}
    agreement: dict[str, dict[str, Any]] = {}
    for asset in assets:
        parsed_qty = float(parsed_canonical.get(asset, 0.0) or 0.0)
        positions_qty = float(positions_canonical.get(asset, 0.0) or 0.0)
        if parsed_qty > 0 and positions_qty > 0:
            selected_qty = max(parsed_qty, positions_qty)
            source_used = "positions_api" if positions_qty >= parsed_qty else "parsed"
            selection_rule = "max_of_sources"
        elif positions_qty > 0:
            selected_qty = positions_qty
            source_used = "positions_api"
            selection_rule = "positions_api_only"
        elif parsed_qty > 0:
            selected_qty = parsed_qty
            source_used = "parsed"
            selection_rule = "parsed_only"
        else:
            continue
        balances[asset] = float(selected_qty)
        selected_sources[asset] = [source_used]
        agreement[asset] = {
            "parsed_qty": float(parsed_qty),
            "positions_api_qty": float(positions_qty),
            "selected_qty": float(selected_qty),
            "selected_source": source_used,
            "selection_rule": selection_rule,
            "source_delta": float(abs(parsed_qty - positions_qty)),
            "sources_match": bool(abs(parsed_qty - positions_qty) <= 1e-12),
        }
    return {
        "balances": balances,
        "selected_sources": selected_sources,
        "agreement": agreement,
        "parsed_canonical": parsed_canonical,
        "positions_api_canonical": positions_canonical,
    }


def _canonicalize_balance_sources(sources: dict[str, list[str]] | None) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    for asset, source_list in (sources or {}).items():
        asset_c = _canonical_asset(asset)
        if not asset_c:
            continue
        bucket = out.setdefault(asset_c, set())
        for source in (source_list or []):
            if source:
                bucket.add(str(source))
    return {asset: sorted(list(vals)) for asset, vals in out.items()}


def _stable_cash_usd(balances: dict[str, float]) -> float:
    """Return USD-like cash from canonicalized balances.

    Kraken may report USD cash as USD or ZUSD depending on endpoint/account.
    Some accounts primarily hold stables (USDC/USDT). Treat those as cash-equivalent
    for sizing and eligibility checks.
    """
    canonical = _canonicalize_balances(balances)
    keys = ('USD', 'USDC', 'USDT')
    total = 0.0
    for k in keys:
        try:
            total += float(canonical.get(k, 0.0) or 0.0)
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

PATCH_BUILD = build_payload()




def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_csv(name: str) -> list[str]:
    raw = str(os.getenv(name, "") or "")
    return [s.strip() for s in raw.split(",") if s.strip()]


def _live_promotion_guardrails_snapshot(compatibility: dict[str, Any] | None = None, gate: dict[str, Any] | None = None) -> dict[str, Any]:
    compatibility = compatibility if isinstance(compatibility, dict) else _compatibility_snapshot()
    gate = gate if isinstance(gate, dict) else _pretrade_health_gate_summary(rerun_startup_check=False)
    scanner_contract = dict((compatibility or {}).get("scanner_contract") or {})
    btc_alignment = dict((compatibility or {}).get("btc_only_live_alignment") or {})
    account_truth = _account_truth_snapshot()
    open_orders = _broker_open_orders_summary()
    positions = get_positions()
    live_validation = telemetry_db.live_validation_summary()

    release_stage = str(PATCH_BUILD.get("release_stage_configured") or "paper")
    live_stage_requested = release_stage == "live"
    scanner_alignment_enabled = bool(scanner_contract.get("btc_only_live_alignment", {}).get("alignment_enabled") or btc_alignment.get("scanner_alignment_enabled"))

    checks = {
        "release_stage_live": live_stage_requested,
        "readiness_green": bool((compatibility or {}).get("contract_compatible")) and bool(gate.get("gate_open")),
        "btc_only_alignment_ready": bool(btc_alignment.get("alignment_ready")),
        "scanner_ready": bool((compatibility or {}).get("scanner_ok")),
        "workers_healthy": not bool((gate.get("worker_health") or {}).get("overall_stale")),
        "open_orders_clear": int(open_orders.get("count") or 0) == 0,
        "positions_clear": len(positions or []) == 0,
        "balance_ok": bool(account_truth.get("balance_ok")),
        "trading_enabled": bool(getattr(settings, "trading_enabled", False)),
        "worker_secret_present": bool(getattr(settings, "worker_secret", "")),
        "webhook_secret_present": bool(getattr(settings, "webhook_secret", "")),
        "kraken_key_present": bool(os.getenv("KRAKEN_API_KEY")),
        "kraken_secret_present": bool(os.getenv("KRAKEN_API_SECRET")),
        "scanner_url_present": bool(os.getenv("SCANNER_URL")),
        "scanner_alignment_enabled": scanner_alignment_enabled,
        "live_validation_mode_enabled": _env_bool("LIVE_VALIDATION_MODE", True),
        "execution_canary_enabled": _env_bool("EXECUTION_CANARY_ENABLED", False),
    }

    blockers = []
    if not checks["readiness_green"]:
        blockers.append("system_not_ready")
    if not checks["btc_only_alignment_ready"]:
        blockers.append("btc_alignment_not_ready")
    if not checks["scanner_ready"]:
        blockers.append("scanner_not_ready")
    if not checks["workers_healthy"]:
        blockers.append("worker_health_not_green")
    if not checks["open_orders_clear"]:
        blockers.append("open_orders_present")
    if not checks["positions_clear"]:
        blockers.append("positions_present")
    if not checks["balance_ok"]:
        blockers.append("broker_balance_not_ok")
    if not checks["trading_enabled"]:
        blockers.append("trading_disabled")
    if not checks["worker_secret_present"]:
        blockers.append("worker_secret_missing")
    if not checks["webhook_secret_present"]:
        blockers.append("webhook_secret_missing")
    if not checks["kraken_key_present"]:
        blockers.append("kraken_api_key_missing")
    if not checks["kraken_secret_present"]:
        blockers.append("kraken_api_secret_missing")
    if not checks["scanner_url_present"]:
        blockers.append("scanner_url_missing")
    if not checks["scanner_alignment_enabled"]:
        blockers.append("scanner_alignment_disabled")

    env_guidance = {
        "release_stage": release_stage,
        "recommended_env_changes_for_live": {
            "main": [
                "Set RELEASE_STAGE=live when you are ready to cut over from paper-label mode.",
                "Keep TRADING_ENABLED=1.",
                "Keep SCANNER_URL set to your scanner service.",
                "Keep WORKER_SECRET and WEBHOOK_SECRET populated.",
                "Use live Kraken API credentials in KRAKEN_API_KEY and KRAKEN_API_SECRET.",
            ],
            "scanner": [
                "Keep BTC_ONLY_ALIGNMENT_ENABLED=1.",
                "Keep SCANNER_FORCE_EMIT_SYMBOLS=BTC/USD.",
                "Keep SCANNER_EMIT_ONLY_SYMBOLS=1 until you intentionally leave BTC-only mode.",
            ],
            "do_not_change_yet": [
                "Do not enable multi-symbol admission yet.",
                "Do not disable fee/churn guardrails for Path B.",
            ],
        },
    }

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": release_stage,
        },
        "checks": checks,
        "promotion_blockers": blockers,
        "promotion_ready": len(blockers) == 0,
        "compatibility_reason": (compatibility or {}).get("compatibility_reason"),
        "scanner_contract": scanner_contract,
        "btc_only_live_alignment": btc_alignment,
        "pretrade_health_gate": gate,
        "account_truth": {
            "balance_ok": bool(account_truth.get("balance_ok")),
            "cash_usd": account_truth.get("cash_usd"),
            "equity_usd": account_truth.get("equity_usd"),
            "positions_count": account_truth.get("positions_count"),
            "open_order_count": account_truth.get("open_order_count"),
        },
        "live_validation": live_validation,
        "env_guidance": env_guidance,
        "scanner_alignment_env": {
            "BTC_ONLY_ALIGNMENT_ENABLED": os.getenv("BTC_ONLY_ALIGNMENT_ENABLED"),
            "SCANNER_FORCE_EMIT_SYMBOLS": os.getenv("SCANNER_FORCE_EMIT_SYMBOLS"),
            "SCANNER_EMIT_ONLY_SYMBOLS": os.getenv("SCANNER_EMIT_ONLY_SYMBOLS"),
        },
    }


def _route_truth_from_payload(payload: Any, kind: str) -> dict:
    obj = payload
    return {
        "worker_kind": str(getattr(obj, "worker_kind", "") or getattr(obj, "heartbeat_kind", "") or kind),
        "heartbeat_kind": str(getattr(obj, "heartbeat_kind", "") or kind),
        "heartbeat_utc": str(getattr(obj, "heartbeat_utc", "") or utc_now_iso()),
        "heartbeat_ts": getattr(obj, "heartbeat_ts", None),
        "heartbeat_seq": getattr(obj, "heartbeat_seq", None),
        "loop_interval_sec": getattr(obj, "loop_interval_sec", None),
        "loop_pid": getattr(obj, "loop_pid", None),
        "heartbeat_source": getattr(obj, "heartbeat_source", None),
        "worker_request_id": getattr(obj, "worker_request_id", None),
        "phase": getattr(obj, "phase", None),
        "ok": getattr(obj, "ok", None),
        "target_base_url": getattr(obj, "target_base_url", None),
        "target_path": getattr(obj, "target_path", None),
        "target_url": getattr(obj, "target_url", None),
        "status_code": getattr(obj, "status_code", None),
        "elapsed_ms": getattr(obj, "elapsed_ms", None),
        "auth_present": getattr(obj, "auth_present", None),
        "error": getattr(obj, "error", None),
        "response_excerpt": getattr(obj, "response_excerpt", None),
        "route_truth_utc": str(getattr(obj, "route_truth_utc", "") or utc_now_iso()),
    }


def _classify_route_truth_reason(route_truth: dict | None) -> str | None:
    rt = dict(route_truth or {})
    if not rt:
        return None
    code = rt.get("status_code")
    phase = str(rt.get("phase") or "")
    err = str(rt.get("error") or "")
    ok = bool(rt.get("ok"))
    if ok and (phase in ("success", "result") or (isinstance(code, int) and 200 <= code < 300)):
        return "fresh"
    if code == 401 or phase == "auth_failed" or "401" in err:
        return "auth_failed"
    if code == 404 or "404" in err:
        return "route_not_found"
    if phase == "post_failed":
        return "heartbeat_post_failed"
    if phase in ("attempt", "result", "started", "success"):
        return "loop_not_invoked" if not ok and not code else "loop_invoked_no_heartbeat"
    return "route_truth_seen_no_heartbeat"


def _route_truth_summary(kind: str) -> dict:
    snap = dict(getattr(state, f"last_{kind}_route_truth", {}) or {})
    if not snap:
        return {"seen": False, "worker_kind": kind}
    return {"seen": True, **snap, "derived_reason": _classify_route_truth_reason(snap)}


def _startup_dependency_guard() -> dict[str, Any]:
    required = {
        'PENDING_EXIT_TTL_SEC': PENDING_EXIT_TTL_SEC,
        '_adopted_lifecycle_policy': _adopted_lifecycle_policy,
        '_plan_origin': _plan_origin,
        '_plan_policy_source': _plan_policy_source,
        '_effective_plan_max_hold_sec': _effective_plan_max_hold_sec,
        '_normalize_plan_lifecycle_policy': _normalize_plan_lifecycle_policy,
    }
    missing: list[str] = []
    for name, value in required.items():
        if value is None:
            missing.append(name)
    return {
        'ok': len(missing) == 0,
        'missing': missing,
        'pending_exit_ttl_sec': int(PENDING_EXIT_TTL_SEC),
    }


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

    lifecycle_repairs = {'expired_workflow_locks_released': 0, 'legacy_trade_lifecycle_events_backfilled': 0, 'expired_signal_fingerprints_purged': 0, 'reconciled_exit_truth_repaired': 0, 'reconciled_exit_duplicates_removed': 0, 'reconciled_strategy_attribution_repaired': 0, 'trade_journal_persistence_path_verified': 0}
    try:
        _repair = trade_journal.repair_reconciled_exit_truth(lookback_days=30.0)
        lifecycle_repairs['reconciled_exit_truth_repaired'] = int(_repair.get('repaired_count') or 0)
        lifecycle_repairs['reconciled_exit_duplicates_removed'] = int(_repair.get('deduped_count') or 0)
    except Exception:
        pass
    try:
        _strategy_repair = trade_journal.repair_reconciled_strategy_attribution(lookback_days=30.0)
        lifecycle_repairs['reconciled_strategy_attribution_repaired'] = int(_strategy_repair.get('updated_count') or 0)
    except Exception:
        pass
    dependency_guard = _startup_dependency_guard()
    if not bool(dependency_guard.get('ok')):
        STARTUP_SELF_CHECK_TS = time.time()
        STARTUP_SELF_CHECK_RESULT = {
            'ok': False,
            'utc': utc_now_iso(),
            'enabled': True,
            'apply_changes': bool(apply),
            'critical': True,
            'critical_reasons': ['startup_dependency_guard_failed'],
            'lockout_applied': False,
            'lockout_reason': '',
            'lockout_remaining_sec': int(state.ops_lockout_remaining_sec() if hasattr(state, 'ops_lockout_remaining_sec') else 0),
            'dependency_guard': dependency_guard,
            'recovery_reconcile': None,
            'lifecycle_repairs': lifecycle_repairs,
        }
        return dict(STARTUP_SELF_CHECK_RESULT)
    try:
        lifecycle_repairs = lifecycle_db.repair_lifecycle_integrity(
            stale_age_sec=int(getattr(settings, 'workflow_lock_ttl_sec', 300) or 300),
            backfill_limit=2000,
        )
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
            'dependency_guard': dependency_guard,
            'recovery_reconcile': recovery,
            'lifecycle_repairs': lifecycle_repairs,
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
            'dependency_guard': dependency_guard,
            'recovery_reconcile': None,
            'lifecycle_repairs': lifecycle_repairs,
        }
    return dict(STARTUP_SELF_CHECK_RESULT)


@app.on_event("startup")
def _run_startup_self_check():
    try:
        lifecycle_db.repair_lifecycle_integrity(
            stale_age_sec=int(getattr(settings, 'workflow_lock_ttl_sec', 300) or 300),
            backfill_limit=5000,
        )
    except Exception:
        pass
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
    balances = _merged_balances_by_asset() or {}
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
ENTRY_ENGINE_STRATEGIES_LIST = [s.strip().lower() for s in os.getenv("ENTRY_ENGINE_STRATEGIES", "tc0,rb1").split(",") if s.strip()]
if ENTRY_ENGINE_STRATEGIES_LIST == ["tc0"]:
    ENTRY_ENGINE_STRATEGIES_LIST = ["tc0", "rb1"]
ENTRY_ENGINE_STRATEGIES = set(ENTRY_ENGINE_STRATEGIES_LIST)
ENTRY_ENGINE_TIMEFRAME = os.getenv("ENTRY_ENGINE_TIMEFRAME", "5Min").strip() or "5Min"   # must match broker get_bars
ENTRY_ENGINE_LIMIT_BARS = int(float(os.getenv("ENTRY_ENGINE_LIMIT_BARS", "300") or 300))

# Strategy enablement / mode
# fixed: only consider strategies listed in ENTRY_ENGINE_STRATEGIES, preserving env order
# auto:  preserve legacy preference ordering across eligible strategies
# legacy: rb1/tc1 only, old preference behavior
STRATEGY_MODE = (os.getenv("STRATEGY_MODE", "auto") or "auto").strip().lower()  # fixed|auto|legacy
ENABLE_RB1 = ("rb1" in ENTRY_ENGINE_STRATEGIES_LIST) or (os.getenv("ENABLE_RB1", "1").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_TC0 = (os.getenv("ENABLE_TC0", "1").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_TC1 = (os.getenv("ENABLE_TC1", "1").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_CR1 = (os.getenv("ENABLE_CR1", "0").strip().lower() in ("1", "true", "yes", "on"))
ENABLE_MM1 = (os.getenv("ENABLE_MM1", "0").strip().lower() in ("1", "true", "yes", "on"))
_ALLOWED_STRATEGY_NAMES = ("tc0", "rb1", "tc1", "cr1", "mm1")
if not ENTRY_ENGINE_STRATEGIES_LIST:
    ENTRY_ENGINE_STRATEGIES_LIST = ["tc0", "tc1"]
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

# TC0 params (5m)
TC0_LOOKBACK_BARS = int(float(os.getenv("TC0_LOOKBACK_BARS", "8") or 8))
TC0_BREAKOUT_BUFFER_PCT = max(float(os.getenv("TC0_BREAKOUT_BUFFER_PCT", "0.0") or 0.0), 0.0005)
TC0_ATR_LEN = int(float(os.getenv("TC0_ATR_LEN", "14") or 14))
TC0_MIN_ATR_PCT = max(float(os.getenv("TC0_MIN_ATR_PCT", "0.0005") or 0.0005), 0.0010)
TC0_REQUIRE_VWAP = (os.getenv("TC0_REQUIRE_VWAP", "0").strip().lower() in ("1", "true", "yes", "on"))
TC0_VWAP_LOOKBACK_BARS = int(float(os.getenv("TC0_VWAP_LOOKBACK_BARS", "20") or 20))
TC0_MAX_SPREAD_PCT = float(os.getenv("TC0_MAX_SPREAD_PCT", os.getenv("MAX_SPREAD_PCT", "0.004")) or 0.004)
TC0_MAX_HOLD_SEC = max(int(float(os.getenv("TC0_MAX_HOLD_SEC", "1800") or 1800)), 5400)
TC0_TIME_EXIT_EXTENSION_SEC = int(float(os.getenv("TC0_TIME_EXIT_EXTENSION_SEC", "5400") or 5400))
TC0_TIME_EXIT_MIN_FEE_MULT = float(os.getenv("TC0_TIME_EXIT_MIN_FEE_MULT", "1.25") or 1.25)

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





def _merged_balances_snapshot() -> dict[str, Any]:
    """Return broker holdings truth merged across multiple Kraken balance views.

    Why this exists:
    - The main system historically trusted only `balances_by_asset()`.
    - In live trading we observed cases where broker-held BTC existed, but the
      runtime position model did not see it.
    - Broker truth must win over local/runtime state.

    Output shape:
      {
        "parsed": {"BTC": 0.01, "USD": 1000.0},
        "positions_api": {"BTC": 0.01, "USD": 1000.0},
        "merged": {"BTC": 0.01, "USD": 1000.0},
        "sources": {"BTC": ["parsed", "positions_api"]},
        "raw": {...},
        "raw_ok": True,
        "raw_error": None,
      }
    """
    parsed: dict[str, float] = {}
    positions_api: dict[str, float] = {}
    raw: dict[str, Any] = {}
    raw_ok = True
    raw_error = None
    parsed_error = None
    positions_error = None

    try:
        parsed = _balances_by_asset() or {}
    except Exception as e:
        parsed_error = f"{type(e).__name__}: {e}"
        parsed = {}

    try:
        raw = broker_kraken._fetch_balances() or {}
        if not isinstance(raw, dict):
            raw = {}
    except Exception as e:
        raw_ok = False
        raw_error = f"{type(e).__name__}: {e}"
        raw = {}

    try:
        for row in (broker_kraken.positions() or []):
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset") or "").upper().strip()
            if not asset:
                continue
            try:
                qty = float(row.get("qty") or 0.0)
            except Exception:
                qty = 0.0
            if qty <= 0:
                continue
            positions_api[asset] = max(float(positions_api.get(asset, 0.0) or 0.0), qty)
    except Exception as e:
        positions_error = f"{type(e).__name__}: {e}"
        positions_api = {}

    merged: dict[str, float] = {}
    sources: dict[str, list[str]] = {}
    for source_name, source_map in (("parsed", parsed), ("positions_api", positions_api)):
        for asset, qty_raw in (source_map or {}).items():
            asset_u = str(asset or "").upper().strip()
            try:
                qty = float(qty_raw or 0.0)
            except Exception:
                qty = 0.0
            if not asset_u or qty <= 0:
                continue
            merged[asset_u] = max(float(merged.get(asset_u, 0.0) or 0.0), qty)
            sources.setdefault(asset_u, []).append(source_name)

    visibility_canonical = _canonicalize_balances(merged, aggregate="max")
    canonical_sources = _canonicalize_balance_sources(sources)
    economic = _economic_balance_truth(parsed, positions_api)

    return {
        "parsed": parsed,
        "positions_api": positions_api,
        "merged": merged,
        "canonical_merged": visibility_canonical,
        "economic_balances": dict(economic.get("balances") or {}),
        "economic_balance_sources": dict(economic.get("selected_sources") or {}),
        "economic_balance_agreement": dict(economic.get("agreement") or {}),
        "parsed_canonical": dict(economic.get("parsed_canonical") or {}),
        "positions_api_canonical": dict(economic.get("positions_api_canonical") or {}),
        "sources": sources,
        "canonical_sources": canonical_sources,
        "raw": raw,
        "raw_ok": bool(raw_ok),
        "raw_error": raw_error,
        "parsed_error": parsed_error,
        "positions_error": positions_error,
    }


def _merged_balances_by_asset(canonical: bool = True) -> dict[str, float]:
    snap = _merged_balances_snapshot() or {}
    key = "economic_balances" if canonical else "merged"
    return dict(snap.get(key) or {})


def get_positions() -> list[dict]:
    """Return open positions derived from merged broker holdings + live USD prices.

    We intentionally merge multiple Kraken balance views so live broker-held
    inventory is still recognized even if one parsing path misses it.
    """
    try:
        snap = _merged_balances_snapshot() or {}
        balances = dict(snap.get("economic_balances") or snap.get("canonical_merged") or snap.get("merged") or {})
        balance_sources = dict(snap.get("economic_balance_sources") or snap.get("canonical_sources") or snap.get("sources") or {})
    except Exception as e:
        log.exception("merged balances failed: %s", e)
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

        sym = normalize_symbol(f"{asset_u}/USD")
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
                "balance_sources": list(balance_sources.get(asset_u, [])),
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




def _vwap_from_bars(bars: list[dict], lookback: int | None = None) -> float | None:
    if not bars:
        return None
    use = bars[-int(lookback):] if lookback and int(lookback) > 0 else bars
    num = 0.0
    den = 0.0
    for b in use:
        h = float(b.get("h") or 0.0)
        l = float(b.get("l") or 0.0)
        c = float(b.get("c") or 0.0)
        v = float(b.get("v") or 0.0)
        if v <= 0:
            continue
        tp = (h + l + c) / 3.0
        num += tp * v
        den += v
    return (num / den) if den > 0 else None
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
    if s == "tc0":
        return int(TC0_MAX_HOLD_SEC)
    if s == "rb1":
        return int(getattr(settings, "rb1_max_hold_sec", 0) or 0)
    if s == "tc1":
        return int(getattr(settings, "tc1_max_hold_sec", 0) or 0)
    return 0


def _adopted_lifecycle_policy() -> dict[str, Any]:
    enabled = bool(getattr(settings, 'adopted_time_exit_enabled', True))
    max_hold = int(getattr(settings, 'adopted_max_hold_sec', 0) or 0)
    if not enabled:
        max_hold = 0
    return {
        'origin': 'adopted',
        'policy_source': 'adopted_defaults',
        'time_exit_enabled': bool(enabled and max_hold > 0),
        'max_hold_sec': int(max_hold),
    }


def _plan_origin(plan: TradePlan | None) -> str:
    if plan is None:
        return ''
    strategy = str(getattr(plan, 'strategy', '') or '').strip().lower()
    if strategy == 'adopted':
        return 'adopted'
    return 'strategy' if strategy else 'unknown'


def _plan_policy_source(plan: TradePlan | None) -> str:
    if plan is None:
        return ''
    rs = dict(getattr(plan, 'risk_snapshot', {}) or {})
    lp = dict(rs.get('lifecycle_policy') or {})
    source = str(lp.get('policy_source') or '').strip()
    if source:
        return source
    return 'adopted_defaults' if _plan_origin(plan) == 'adopted' else 'strategy_defaults'


def _effective_plan_max_hold_sec(plan: TradePlan | None) -> int:
    if plan is None:
        return 0
    rs = dict(getattr(plan, 'risk_snapshot', {}) or {})
    lp = dict(rs.get('lifecycle_policy') or {})
    if 'max_hold_sec' in lp:
        try:
            return int(lp.get('max_hold_sec') or 0)
        except Exception:
            return 0
    plan_max = int(getattr(plan, 'max_hold_sec', 0) or 0)
    if plan_max > 0:
        return plan_max
    if _plan_origin(plan) == 'adopted' and bool(getattr(settings, 'adopted_time_exit_enabled', True)):
        return int(getattr(settings, 'adopted_max_hold_sec', 0) or 0)
    strategy = str(getattr(plan, 'strategy', '') or '')
    return int(_strategy_max_hold_sec(strategy) or 0)


def _normalize_plan_lifecycle_policy(plan: TradePlan | None, *, now_ts: float | None = None, persist: bool = False) -> tuple[TradePlan | None, bool]:
    if plan is None:
        return None, False
    changed = False
    rs = dict(getattr(plan, 'risk_snapshot', {}) or {})
    lp = dict(rs.get('lifecycle_policy') or {})
    origin = _plan_origin(plan)
    if origin == 'adopted':
        desired = _adopted_lifecycle_policy()
    else:
        eff_max = int(getattr(plan, 'max_hold_sec', 0) or _strategy_max_hold_sec(str(getattr(plan, 'strategy', '') or '')) or 0)
        desired = {
            'origin': origin or 'strategy',
            'policy_source': 'strategy_defaults',
            'time_exit_enabled': bool(eff_max > 0),
            'max_hold_sec': int(eff_max),
        }
    merged = dict(desired)
    merged.update({k: v for k, v in lp.items() if k not in {'origin', 'policy_source', 'time_exit_enabled', 'max_hold_sec'}})
    # hard normalize the lifecycle control keys so legacy adopted plans with max_hold=0 get repaired
    for key in ('origin', 'policy_source', 'time_exit_enabled', 'max_hold_sec'):
        if lp.get(key) != desired.get(key):
            changed = True
    merged['origin'] = desired['origin']
    merged['policy_source'] = desired['policy_source']
    merged['time_exit_enabled'] = desired['time_exit_enabled']
    merged['max_hold_sec'] = desired['max_hold_sec']
    rs['lifecycle_policy'] = merged
    if (getattr(plan, 'risk_snapshot', {}) or {}) != rs:
        plan.risk_snapshot = rs
        changed = True
    desired_plan_max = int(desired.get('max_hold_sec', 0) or 0)
    if int(getattr(plan, 'max_hold_sec', 0) or 0) != desired_plan_max:
        plan.max_hold_sec = desired_plan_max
        changed = True
    if persist and changed:
        try:
            state.set_plan(plan)
        except Exception:
            pass
    return plan, changed


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


def _tc0_long_signal(symbol: str) -> tuple[bool, dict]:
    """Simple breakout continuation (TC0-lite).

    Fires when the latest closed 5m bar closes above the prior N-bar high,
    subject to optional ATR, VWAP, and spread filters.
    """
    need = max(TC0_LOOKBACK_BARS + 5, TC0_ATR_LEN + 5, TC0_VWAP_LOOKBACK_BARS + 5)
    bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, need))
    if not bars or len(bars) < need:
        return False, {"reason": "insufficient_bars", "bars": len(bars) if bars else 0}
    fresh_ok, fresh_meta = _guard_fresh_bars(bars, ENTRY_ENGINE_TIMEFRAME)
    if not fresh_ok:
        return False, {"reason": "stale_bars", "bars": len(bars), "freshness": fresh_meta}

    highs = [float(b["h"]) for b in bars]
    closes = [float(b["c"]) for b in bars]
    ts = [int(float(b["t"])) for b in bars]
    cur_close = closes[-1]
    prev_close = closes[-2]
    look = highs[-(TC0_LOOKBACK_BARS+1):-1]
    breakout_level = max(look)
    trigger_level = breakout_level * (1.0 + TC0_BREAKOUT_BUFFER_PCT)
    breakout = cur_close > trigger_level
    breakout_distance_pct = ((cur_close / trigger_level) - 1.0) if trigger_level > 0 else None

    atr_now, _ = _atr_from_bars(bars, length=int(TC0_ATR_LEN))
    atr_pct = (float(atr_now) / float(cur_close)) if atr_now and cur_close > 0 else None
    atr_ok = (atr_pct is not None and atr_pct >= float(TC0_MIN_ATR_PCT))

    vwap = _vwap_from_bars(bars, lookback=TC0_VWAP_LOOKBACK_BARS)
    vwap_ok = (not bool(TC0_REQUIRE_VWAP)) or (vwap is not None and cur_close >= float(vwap))

    bid = ask = spread_pct = None
    spread_ok = True
    try:
        bid, ask = _best_bid_ask(symbol)
        bid = float(bid or 0.0)
        ask = float(ask or 0.0)
        mid = ((bid + ask) / 2.0) if (bid > 0 and ask > 0) else 0.0
        spread_pct = ((ask - bid) / mid) if mid > 0 else None
        if TC0_MAX_SPREAD_PCT and spread_pct is not None:
            spread_ok = spread_pct <= float(TC0_MAX_SPREAD_PCT)
    except Exception:
        spread_ok = True

    reason = None
    if not breakout:
        reason = "below_breakout"
    elif not atr_ok:
        reason = "atr_too_low"
    elif not vwap_ok:
        reason = "below_vwap"
    elif not spread_ok:
        reason = "spread_too_wide"

    meta = {
        "reason": reason or "signal",
        "lookback_bars": int(TC0_LOOKBACK_BARS),
        "breakout_buffer_pct": float(TC0_BREAKOUT_BUFFER_PCT),
        "min_atr_pct": float(TC0_MIN_ATR_PCT),
        "require_vwap": bool(TC0_REQUIRE_VWAP),
        "max_spread_pct": float(TC0_MAX_SPREAD_PCT),
        "breakout": bool(breakout),
        "breakout_level": float(breakout_level),
        "trigger_level": float(trigger_level),
        "prev_close": float(prev_close),
        "close": float(cur_close),
        "bar_ts": ts[-1],
        "breakout_distance_pct": float(breakout_distance_pct) if breakout_distance_pct is not None else None,
        "atr": float(atr_now) if atr_now is not None else None,
        "atr_pct": float(atr_pct) if atr_pct is not None else None,
        "atr_ok": bool(atr_ok),
        "vwap": float(vwap) if vwap is not None else None,
        "vwap_ok": bool(vwap_ok),
        "bid": bid,
        "ask": ask,
        "spread_pct": float(spread_pct) if spread_pct is not None else None,
        "spread_ok": bool(spread_ok),
    }
    return (bool(breakout and atr_ok and vwap_ok and spread_ok), meta)


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
    bal = _merged_balances_by_asset()
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
        if isinstance(data.get("compatibility"), dict):
            meta["compatibility"] = data.get("compatibility")

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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int = 0) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _env_float(name: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_symbol_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    out: list[str] = []
    seen: set[str] = set()
    for item in str(raw or "").split(","):
        token = str(item or "").strip()
        if not token:
            continue
        try:
            sym = normalize_symbol(token)
        except Exception:
            sym = token.upper()
        if sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out

def _safe_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        try:
            return max(0, int(value))
        except Exception:
            return 0
    if isinstance(value, dict):
        return len(value)
    if isinstance(value, (list, tuple, set)):
        return len(value)
    try:
        return len(value)  # type: ignore[arg-type]
    except Exception:
        return 0


def _spread_bps_from_guardrails(scanner_guardrails: dict[str, Any]) -> float:
    try:
        max_spread_pct = float((scanner_guardrails or {}).get("max_spread_pct") or 0.0)
    except Exception:
        max_spread_pct = 0.0
    return max(0.0, max_spread_pct * 10000.0)


def _fee_churn_truth_snapshot(scanner_contract: dict[str, Any], *, max_active_symbols: int, scanner_rank_cap: int) -> dict[str, Any]:
    scanner_guardrails = dict((scanner_contract or {}).get("guardrails") or {})
    entry_fee_bps = _env_float("ENTRY_FEE_BPS", 0.0)
    exit_fee_bps = _env_float("EXIT_FEE_BPS", 0.0)
    slippage_bps_each_side = _env_float("EXPECTED_SLIPPAGE_BPS", _env_float("SLIPPAGE_BPS", 0.0))
    spread_bps_each_side = _env_float("PATH_B_SPREAD_BPS_EACH_SIDE", _spread_bps_from_guardrails(scanner_guardrails) / 2.0)
    estimated_round_trip_bps = float(entry_fee_bps + exit_fee_bps + (2.0 * slippage_bps_each_side) + (2.0 * spread_bps_each_side))
    max_est_round_trip_bps = _env_float("PATH_B_MAX_EST_ROUND_TRIP_BPS", 80.0)
    fee_over_limit_bps = max(0.0, estimated_round_trip_bps - max_est_round_trip_bps)
    signal_dedupe_ttl_sec = _env_int("SIGNAL_DEDUPE_TTL_SEC", 0)
    bar_lock_sec = max(60, _env_int("SCANNER_BAR_LOCK_SEC", int((scanner_guardrails or {}).get("bar_lock_sec") or 60)))
    inflight_holdoff_sec = max(60, _env_int("SCANNER_INFLIGHT_HOLDOFF_SEC", int((scanner_guardrails or {}).get("inflight_holdoff_sec") or 60)))
    churn_thresholds = {
        "max_open_positions_lte": 1,
        "max_entries_per_day_lte": 3,
        "max_active_symbols_lte": 7,
        "signal_dedupe_ttl_sec_gte": 120,
        "scanner_bar_lock_sec_gte": 60,
        "scanner_inflight_holdoff_sec_gte": 60,
        "scanner_rank_cap_lte": 7,
    }
    churn_inputs = {
        "max_open_positions": int(MAX_OPEN_POSITIONS),
        "max_entries_per_day": int(MAX_ENTRIES_PER_DAY),
        "max_active_symbols": int(max_active_symbols),
        "signal_dedupe_ttl_sec": int(signal_dedupe_ttl_sec),
        "scanner_bar_lock_sec": int(bar_lock_sec),
        "scanner_inflight_holdoff_sec": int(inflight_holdoff_sec),
        "scanner_rank_cap": int(scanner_rank_cap),
    }
    failing_checks = []
    if churn_inputs["max_open_positions"] > churn_thresholds["max_open_positions_lte"]:
        failing_checks.append("max_open_positions")
    if churn_inputs["max_entries_per_day"] > churn_thresholds["max_entries_per_day_lte"]:
        failing_checks.append("max_entries_per_day")
    if churn_inputs["max_active_symbols"] > churn_thresholds["max_active_symbols_lte"]:
        failing_checks.append("max_active_symbols")
    if churn_inputs["signal_dedupe_ttl_sec"] < churn_thresholds["signal_dedupe_ttl_sec_gte"]:
        failing_checks.append("signal_dedupe_ttl_sec")
    if churn_inputs["scanner_bar_lock_sec"] < churn_thresholds["scanner_bar_lock_sec_gte"]:
        failing_checks.append("scanner_bar_lock_sec")
    if churn_inputs["scanner_inflight_holdoff_sec"] < churn_thresholds["scanner_inflight_holdoff_sec_gte"]:
        failing_checks.append("scanner_inflight_holdoff_sec")
    if churn_inputs["scanner_rank_cap"] > churn_thresholds["scanner_rank_cap_lte"]:
        failing_checks.append("scanner_rank_cap")
    churn_guard_pass = len(failing_checks) == 0
    return {
        "fee_model": {
            "entry_fee_bps": round(entry_fee_bps, 3),
            "exit_fee_bps": round(exit_fee_bps, 3),
            "expected_slippage_bps_each_side": round(slippage_bps_each_side, 3),
            "expected_spread_bps_each_side": round(spread_bps_each_side, 3),
            "estimated_round_trip_bps": round(estimated_round_trip_bps, 3),
            "max_est_round_trip_bps": round(max_est_round_trip_bps, 3),
            "fee_over_limit_bps": round(fee_over_limit_bps, 3),
            "fee_guard_pass": bool(estimated_round_trip_bps <= max_est_round_trip_bps),
        },
        "churn_model": {
            "inputs": churn_inputs,
            "thresholds": churn_thresholds,
            "failing_checks": failing_checks,
            "churn_guard_pass": churn_guard_pass,
        },
    }



def _derive_scanner_compatibility_url(scanner_url: str) -> str:
    url = str(scanner_url or "").strip()
    if not url:
        return ""
    if url.endswith("/active_coins"):
        return url[: -len("/active_coins")] + "/compatibility"
    if url.endswith("/compatibility"):
        return url
    return url.rstrip("/") + "/compatibility"


def _scanner_contract_snapshot() -> dict[str, Any]:
    scanner_url = (SCANNER_URL or os.getenv("SCANNER_URL", "").strip())
    active_fetch_ok, active_reason, meta, active_symbols = _scanner_fetch_active_symbols_and_meta()
    compat_url = _derive_scanner_compatibility_url(scanner_url)
    compatibility: dict[str, Any] = {}
    compat_fetch_ok = False
    compat_fetch_reason = None
    symbols_source = "active_endpoint" if active_fetch_ok else "none"
    if compat_url:
        try:
            r = requests.get(compat_url, timeout=SCANNER_TIMEOUT_SEC)
            meta = {**(meta or {}), "compatibility_status_code": r.status_code}
            if r.status_code == 200:
                payload = r.json() if r.content else {}
                compat_fetch_ok = True
                compatibility = payload.get("compatibility") if isinstance(payload, dict) else {}
                if isinstance(compatibility, dict):
                    compat_active = compatibility.get("active_symbols") or compatibility.get("active_symbols_sample") or []
                    clean: list[str] = []
                    seen: set[str] = set()
                    for s in compat_active:
                        if not isinstance(s, str):
                            continue
                        try:
                            ns = normalize_symbol(s)
                        except Exception:
                            continue
                        if ns in seen:
                            continue
                        seen.add(ns)
                        clean.append(ns)
                    if clean and (not active_fetch_ok or not active_symbols):
                        active_symbols = clean
                        symbols_source = "compatibility_fallback"
                    elif clean:
                        symbols_source = "active_endpoint"
                else:
                    compat_fetch_reason = "invalid_compatibility_payload"
            else:
                compat_fetch_reason = f"http_{r.status_code}"
        except Exception as e:
            compat_fetch_reason = f"{type(e).__name__}: {e}"
            meta = {**(meta or {}), "compatibility_error": compat_fetch_reason}
    compat_active_count = None
    if isinstance(compatibility, dict):
        try:
            compat_active_count = int(compatibility.get("active_count"))
        except Exception:
            compat_active_count = None
    active_count = len(active_symbols)
    if compat_active_count is not None and compat_active_count > active_count:
        active_count = compat_active_count

    active_fetch_ok_effective = bool(active_fetch_ok or (compat_fetch_ok and active_count > 0))
    reachable = bool(active_fetch_ok_effective or compat_fetch_ok)
    scanner_ok = bool(reachable and active_count > 0)
    reason = None
    if not reachable:
        reason = str(active_reason or compat_fetch_reason or "scanner_unavailable")
    elif active_count <= 0:
        reason = "scanner_empty_active_set"

    mode = (compatibility or {}).get("mode") or (meta or {}).get("mode") or "unknown"
    multi_symbol_capable = bool((compatibility or {}).get("supports_multi_symbol", active_count > 1))
    guardrails = dict((compatibility or {}).get("fee_churn_guardrails") or {})
    raw_alignment = dict((compatibility or {}).get("alignment") or {}) if isinstance(compatibility, dict) else {}
    btc_alignment = dict((compatibility or {}).get("btc_only_live_alignment") or {}) if isinstance(compatibility, dict) else {}
    return {
        "reachable": reachable,
        "scanner_ok": scanner_ok,
        "reason": reason,
        "scanner_url": scanner_url or None,
        "compatibility_url": compat_url or None,
        "active_symbols": active_symbols,
        "active_count": active_count,
        "active_symbols_sample": active_symbols[:12],
        "active_symbols_source": symbols_source,
        "last_refresh_utc": (compatibility or {}).get("last_refresh_utc") or (meta or {}).get("last_refresh_utc"),
        "mode": mode,
        "multi_symbol_capable": multi_symbol_capable,
        "guardrails": guardrails,
        "alignment": raw_alignment,
        "btc_only_live_alignment": btc_alignment,
        "raw_compatibility": compatibility if isinstance(compatibility, dict) else {},
        "meta": {
            **(meta or {}),
            "active_fetch_ok": active_fetch_ok_effective,
            "active_fetch_ok_raw": bool(active_fetch_ok),
            "active_fetch_reason": active_reason,
            "compat_fetch_ok": bool(compat_fetch_ok),
            "compat_fetch_reason": compat_fetch_reason,
            "active_symbols_source": symbols_source,
        },
    }


def _path_b_admission_snapshot(scanner_contract: dict[str, Any]) -> dict[str, Any]:
    admission_enabled = _env_bool("PATH_B_ADMISSION_ENABLED", True)
    pilot_gate_enabled = _env_bool("PATH_B_PILOT_GATE_ENABLED", True)

    env_allowlist = _env_symbol_list("PATH_B_PILOT_ALLOWLIST")
    legacy_allowlist = _env_symbol_list("PATH_B_ALLOWED_SYMBOLS")
    allowed_pilot_symbols = env_allowlist or legacy_allowlist or ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "LINK/USD", "AVAX/USD", "DOT/USD"]
    _contract_safe_lock_7 = ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "LINK/USD", "AVAX/USD", "DOT/USD"]
    _contract_safe_lock_7_set = set(_contract_safe_lock_7)
    _contract_unsafe_10 = {"BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "XRP/USD", "DOGE/USD", "LINK/USD", "AVAX/USD", "LTC/USD", "DOT/USD"}
    configured_allowed_symbols = set(ALLOWED_SYMBOLS)
    if set(allowed_pilot_symbols) == _contract_unsafe_10:
        allowed_pilot_symbols = list(_contract_safe_lock_7)
    effective_allowed_symbols = _contract_safe_lock_7_set if configured_allowed_symbols == _contract_unsafe_10 else configured_allowed_symbols

    max_active_symbols = max(1, _env_int("PATH_B_PILOT_MAX_ADMITTED_SYMBOLS", _env_int("PATH_B_MAX_ACTIVE_SYMBOLS", 7)))
    scanner_rank_cap = max(1, _env_int("PATH_B_PILOT_SCANNER_RANK_CAP", _env_int("PATH_B_SCANNER_RANK_CAP", max_active_symbols)))

    fee_churn_truth = _fee_churn_truth_snapshot(
        scanner_contract,
        max_active_symbols=max_active_symbols,
        scanner_rank_cap=scanner_rank_cap,
    )
    fee_model = dict(fee_churn_truth.get("fee_model") or {})
    churn_model = dict(fee_churn_truth.get("churn_model") or {})
    fee_guard_pass = bool(fee_model.get("fee_guard_pass"))
    churn_guard_pass = bool(churn_model.get("churn_guard_pass"))
    coordination = _scanner_coordination_snapshot(
        lookback_sec=_env_int("PATH_B_COORDINATION_LOOKBACK_SEC", 900),
        limit=max(25, scanner_rank_cap * 10),
    )
    coordination_guard_pass = bool(coordination.get('ok'))
    active_symbols = list((scanner_contract or {}).get("active_symbols") or [])
    ranked_symbols = list((scanner_contract or {}).get("ranked_active_symbols") or [])
    candidate_symbols = ranked_symbols or active_symbols
    admitted_symbols = candidate_symbols[:scanner_rank_cap]
    if not allowed_pilot_symbols and effective_allowed_symbols == _contract_safe_lock_7_set:
        allowed_pilot_symbols = list(_contract_safe_lock_7)
    if allowed_pilot_symbols:
        allowed_set = set(allowed_pilot_symbols)
        admitted_symbols = [s for s in admitted_symbols if s in allowed_set]
    admitted_symbols = admitted_symbols[:max_active_symbols]
    blockers: list[str] = []
    if not fee_guard_pass:
        blockers.append('fee_guard_fail')
    if not churn_guard_pass:
        blockers.append('churn_guard_fail')
    if not coordination_guard_pass:
        blockers.append('coordination_guard_fail')
    if admission_enabled is False:
        blockers.append('admission_disabled')
    if pilot_gate_enabled and not admitted_symbols:
        blockers.append('pilot_gate_no_symbols')
    pilot_gate_ready = bool(pilot_gate_enabled and fee_guard_pass and churn_guard_pass and coordination_guard_pass and len(admitted_symbols) > 0)
    pilot_gate_open = bool(pilot_gate_ready and admission_enabled)

    allowlist_source = "env:PATH_B_PILOT_ALLOWLIST" if env_allowlist else ("env:PATH_B_ALLOWED_SYMBOLS" if legacy_allowlist else "scanner_ranked_candidates")
    pilot_controls = {
        "pilot_gate_enabled": pilot_gate_enabled,
        "admission_enabled": admission_enabled,
        "allowlist_source": allowlist_source,
        "allowlist_count": len(allowed_pilot_symbols),
        "allowlist_sample": allowed_pilot_symbols[:12],
        "max_admitted_symbols": max_active_symbols,
        "scanner_rank_cap": scanner_rank_cap,
        "candidate_symbols_count": len(candidate_symbols),
        "candidate_symbols_sample": candidate_symbols[:12],
        "admitted_symbols_count": len(admitted_symbols),
        "admitted_symbols_sample": admitted_symbols[:12],
        "requirements": {
            "require_contract_compatible": True,
            "require_fee_guard": True,
            "require_churn_guard": True,
            "require_coordination_guard": True,
        },
    }

    return {
        "multi_symbol_capable": bool((scanner_contract or {}).get("multi_symbol_capable")),
        "multi_symbol_admission_enabled": admission_enabled,
        "path_b_enabled": admission_enabled,
        "pilot_gate_enabled": pilot_gate_enabled,
        "pilot_gate_ready": pilot_gate_ready,
        "pilot_gate_open": pilot_gate_open,
        "pilot_gate_blockers": blockers,
        "pilot_symbol_list": allowed_pilot_symbols,
        "pilot_allowlist_source": allowlist_source,
        "pilot_config": {
            "max_active_symbols": max_active_symbols,
            "scanner_rank_cap": scanner_rank_cap,
            "require_contract_compatible": True,
            "require_fee_guard": True,
            "require_churn_guard": True,
            "require_coordination_guard": True,
        },
        "pilot_controls": pilot_controls,
        "fee_guard_pass": fee_guard_pass,
        "churn_guard_pass": churn_guard_pass,
        "coordination_guard_pass": coordination_guard_pass,
        "fee_churn_truth": fee_churn_truth,
        "coordination": {
            'ok': bool(coordination.get('ok')),
            'reason': coordination.get('reason'),
            'suppressed_symbols_count': int(((coordination.get('counts') or {}).get('suppressed_symbols')) or _safe_count(coordination.get('suppressed_symbols'))),
            'suppressed_symbols_sample': list(coordination.get('suppressed_symbols') or [])[:12],
            'active_workflow_locks_count': int(((coordination.get('counts') or {}).get('active_workflow_locks')) or _safe_count(coordination.get('active_workflow_locks'))),
            'recent_admission_passed_count': int(((coordination.get('counts') or {}).get('recent_admission_passed')) or _safe_count(coordination.get('recent_admission_passed'))),
            'active_signal_fingerprints_count': int(((coordination.get('counts') or {}).get('active_signal_fingerprints')) or _safe_count(coordination.get('active_signal_fingerprints'))),
        },
        "admitted_symbols_count": len(admitted_symbols),
        "admitted_symbols_sample": admitted_symbols[:12],
        "candidate_symbols_count": len(candidate_symbols),
        "candidate_symbols_sample": candidate_symbols[:12],
    }

def _extract_symbols_for_coordination(items: list[Any] | None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items or []:
        sym = None
        if isinstance(item, dict):
            sym = item.get("symbol") or item.get("symbol_id") or item.get("pair")
        elif isinstance(item, str):
            sym = item
        s = str(sym or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _scanner_coordination_snapshot(*, lookback_sec: int = 900, limit: int = 50) -> dict[str, Any]:
    now_ts = time.time()
    lookback_sec = max(60, int(lookback_sec or 900))
    limit = max(1, min(int(limit or 50), 500))
    since_ts = now_ts - float(lookback_sec)
    errors: list[str] = []
    try:
        active_workflow_locks = lifecycle_db.list_active_workflow_locks(limit=limit)
    except Exception as e:
        active_workflow_locks = []
        errors.append(f"workflow_locks:{type(e).__name__}: {e}")
    try:
        recent_admission_passed = lifecycle_db.list_recent_admission_events(admission_result='passed', since_ts=since_ts, limit=limit)
    except Exception as e:
        recent_admission_passed = []
        errors.append(f"admission_events:{type(e).__name__}: {e}")
    try:
        active_signal_fingerprints = lifecycle_db.list_rows(
            'signal_fingerprints',
            limit=limit,
            where='expires_ts IS NULL OR expires_ts > ?',
            args=[now_ts],
            order_by='last_seen_ts DESC',
        )
    except Exception as e:
        active_signal_fingerprints = []
        errors.append(f"signal_fingerprints:{type(e).__name__}: {e}")

    suppressed_symbols: list[str] = []
    seen: set[str] = set()
    for sym in (
        _extract_symbols_for_coordination(active_workflow_locks)
        + _extract_symbols_for_coordination(recent_admission_passed)
        + _extract_symbols_for_coordination(active_signal_fingerprints)
    ):
        if sym in seen:
            continue
        seen.add(sym)
        suppressed_symbols.append(sym)

    return {
        'ok': len(errors) == 0,
        'reason': None if not errors else '; '.join(errors),
        'lookback_sec': lookback_sec,
        'active_workflow_locks': active_workflow_locks,
        'recent_admission_passed': recent_admission_passed,
        'active_signal_fingerprints': active_signal_fingerprints,
        'suppressed_symbols': suppressed_symbols,
        'hard_suppressed_symbols': suppressed_symbols,
        'counts': {
            'active_workflow_locks': len(active_workflow_locks),
            'recent_admission_passed': len(recent_admission_passed),
            'active_signal_fingerprints': len(active_signal_fingerprints),
            'suppressed_symbols': len(suppressed_symbols),
        },
    }

def _btc_only_live_alignment_snapshot(scanner_contract: dict[str, Any], allowed_symbols: list[str], active_symbols: list[str], invalid_active_symbols: list[str]) -> dict[str, Any]:
    allowed_set = set(allowed_symbols)
    admissible = [s for s in active_symbols if s in allowed_set]
    extra = [s for s in active_symbols if s not in allowed_set]
    single_symbol_mode = bool(len(allowed_symbols) == 1 and FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS and not SCANNER_DRIVEN_UNIVERSE and not SCANNER_SOFT_ALLOW and not _env_bool("ALLOW_SCANNER_NEW_SYMBOLS", False))
    scanner_alignment = dict(scanner_contract.get("btc_only_live_alignment") or {})
    return {
        "single_symbol_live_mode": single_symbol_mode,
        "required_symbols": allowed_symbols[:12],
        "required_symbol_count": len(allowed_symbols),
        "admissible_active_symbols_count": len(admissible),
        "admissible_active_symbols_sample": admissible[:12],
        "extra_active_symbols_count": len(extra),
        "extra_active_symbols_sample": extra[:12],
        "scanner_alignment_enabled": bool(scanner_alignment.get("enabled")),
        "scanner_emit_only": bool(scanner_alignment.get("emit_only")),
        "scanner_force_emit_symbols": list(scanner_alignment.get("force_emit_symbols") or []),
        "scanner_active_symbols_all_admissible": bool(scanner_alignment.get("active_symbols_all_admissible")),
        "alignment_ready": bool(single_symbol_mode and len(admissible) > 0 and len(extra) == 0),
        "future_architecture_preserved": True,
    }


def _compatibility_snapshot() -> dict[str, Any]:
    scanner_contract = _scanner_contract_snapshot()
    allowed_symbols = sorted(list(ALLOWED_SYMBOLS))
    allow_scanner_new_symbols = _env_bool("ALLOW_SCANNER_NEW_SYMBOLS", False)
    active_symbols = list(scanner_contract.get("active_symbols") or [])
    allowed_set = set(allowed_symbols)
    admissible_active_symbols = [s for s in active_symbols if s in allowed_set] if allowed_symbols else list(active_symbols)
    invalid_active_symbols = []
    if active_symbols and FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS and allowed_symbols and not allow_scanner_new_symbols:
        invalid_active_symbols = [s for s in active_symbols if s not in allowed_set]
    blockers: list[str] = []
    reason = "ok"
    if not scanner_contract.get("reachable"):
        reason = str(scanner_contract.get("reason") or "scanner_unavailable")
        blockers.append("scanner_unavailable")
    elif not scanner_contract.get("scanner_ok") or not active_symbols:
        reason = str(scanner_contract.get("reason") or "scanner_empty_active_set")
        blockers.append("scanner_empty_active_set")
    elif invalid_active_symbols:
        reason = "invalid_active_symbols"
        blockers.append("invalid_active_symbols")
    path_b = _path_b_admission_snapshot(scanner_contract)
    contract_compatible = len(blockers) == 0
    multi_symbol_contract_compatible = bool(
        scanner_contract.get("multi_symbol_capable")
        and not invalid_active_symbols
        and bool(path_b.get("fee_guard_pass"))
        and bool(path_b.get("churn_guard_pass"))
    )
    btc_alignment = _btc_only_live_alignment_snapshot(scanner_contract, allowed_symbols, active_symbols, invalid_active_symbols)
    return {
        "contract_compatible": contract_compatible,
        "compatibility_reason": reason,
        "blockers": blockers,
        "scanner_ok": bool(scanner_contract.get("scanner_ok")),
        "scanner_reachable": bool(scanner_contract.get("reachable")),
        "scanner_healthy": bool(scanner_contract.get("scanner_ok")),
        "scanner_contract": scanner_contract,
        "scanner_mode": scanner_contract.get("mode"),
        "allowed_symbols_count": len(allowed_symbols),
        "allowed_symbols_sample": allowed_symbols[:12],
        "scanner_active_count": len(active_symbols),
        "scanner_active_symbols_sample": active_symbols[:12],
        "admissible_active_symbols_count": len(admissible_active_symbols),
        "admissible_active_symbols_sample": admissible_active_symbols[:12],
        "invalid_active_symbols_count": len(invalid_active_symbols),
        "invalid_active_symbols_sample": invalid_active_symbols[:12],
        "allow_scanner_new_symbols": allow_scanner_new_symbols,
        "scanner_driven_universe": bool(SCANNER_DRIVEN_UNIVERSE),
        "scanner_soft_allow": bool(SCANNER_SOFT_ALLOW),
        "filter_universe_by_allowed_symbols": bool(FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS),
        "path_b_guardrails": path_b,
        "btc_only_live_alignment": btc_alignment,
        "multi_symbol_capable": bool(scanner_contract.get("multi_symbol_capable")),
        "multi_symbol_contract_compatible": multi_symbol_contract_compatible,
        "multi_symbol_admission_enabled": bool(path_b.get("multi_symbol_admission_enabled")),
    }

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
        "tc0": bool(ENABLE_TC0),
        "rb1": bool(ENABLE_RB1),
        "tc1": bool(ENABLE_TC1),
        "cr1": bool(ENABLE_CR1),
        "mm1": bool(ENABLE_MM1),
    }
    active_enabled = [k for k, v in active_strategy_flags.items() if v]

    checks = {
        "allowed_symbols_locked": bool(allowed_symbols_sorted) and bool(FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS) and not bool(SCANNER_DRIVEN_UNIVERSE) and not bool(SCANNER_SOFT_ALLOW) and not (os.getenv("ALLOW_SCANNER_NEW_SYMBOLS", "0").strip().lower() in ("1", "true", "yes", "on")),
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



def _worker_status_meta(snapshot: dict | None, *, route_truth: dict | None = None, stale_after_sec: int | None = None) -> dict:
    snap = dict(snapshot or {})
    rt = dict(route_truth or {})
    last_ts = None
    try:
        utc_raw = snap.get('heartbeat_utc') or snap.get('completed_utc') or snap.get('utc')
        if utc_raw:
            last_ts = datetime.fromisoformat(str(utc_raw).replace('Z', '+00:00')).timestamp()
    except Exception:
        last_ts = None
    lim = int(stale_after_sec or WORKER_STALE_AFTER_SEC)
    route_fields = {
        'route_truth_seen': bool(rt),
        'route_phase': rt.get('phase'),
        'route_status_code': rt.get('status_code'),
        'route_error': rt.get('error'),
        'target_path': rt.get('target_path'),
        'target_url': rt.get('target_url'),
        'auth_present': rt.get('auth_present'),
        'worker_request_id': rt.get('worker_request_id'),
        'route_elapsed_ms': rt.get('elapsed_ms'),
        'last_route_truth_utc': rt.get('route_truth_utc'),
    }
    if last_ts is None:
        reason = _classify_route_truth_reason(rt) or 'never_started'
        return {
            "seen": False,
            "stale": True,
            "age_sec": None,
            "stale_after_sec": lim,
            "reason": reason,
            "phase": str(snap.get('phase') or rt.get('phase') or 'unknown'),
            "source": snap.get('heartbeat_source') or snap.get('source') or rt.get('heartbeat_source'),
            "heartbeat_seq": snap.get('heartbeat_seq') if snap.get('heartbeat_seq') is not None else rt.get('heartbeat_seq'),
            "last_success_utc": snap.get('last_success_utc'),
            "last_error": snap.get('error') or rt.get('error'),
            "last_duration_ms": snap.get('duration_ms'),
            **route_fields,
        }
    age = max(0.0, time.time() - float(last_ts))
    stale = bool(age > lim)
    phase = str(snap.get('phase') or 'unknown')
    reason = 'stale_heartbeat' if stale else 'fresh'
    if phase == 'failure' and not stale:
        reason = 'last_cycle_failed'
    return {
        "seen": True,
        "stale": stale,
        "age_sec": round(age, 3),
        "stale_after_sec": lim,
        "reason": reason,
        "phase": phase,
        "source": snap.get('heartbeat_source') or snap.get('source'),
        "heartbeat_seq": snap.get('heartbeat_seq'),
        "last_success_utc": snap.get('last_success_utc'),
        "last_error": snap.get('error'),
        "last_duration_ms": snap.get('duration_ms'),
        "loop_interval_sec": snap.get('loop_interval_sec'),
        "loop_pid": snap.get('loop_pid'),
        **route_fields,
    }


def _worker_heartbeat_from_payload(payload: Any, kind: str) -> dict:
    obj = payload
    hb_kind = str(getattr(obj, 'heartbeat_kind', '') or kind)
    hb_utc = str(getattr(obj, 'heartbeat_utc', '') or utc_now_iso())
    hb_ts = getattr(obj, 'heartbeat_ts', None)
    try:
        hb_ts = float(hb_ts) if hb_ts is not None else None
    except Exception:
        hb_ts = None
    return {
        'worker_kind': kind,
        'heartbeat_kind': hb_kind,
        'heartbeat_utc': hb_utc,
        'heartbeat_ts': hb_ts,
        'heartbeat_seq': getattr(obj, 'heartbeat_seq', None),
        'loop_interval_sec': getattr(obj, 'loop_interval_sec', None),
        'loop_pid': getattr(obj, 'loop_pid', None),
        'heartbeat_source': getattr(obj, 'heartbeat_source', None),
    }


def _record_worker_status(kind: str, *, phase: str, payload: Any | None = None, ok: bool | None = None, error: str | None = None, extra: dict | None = None, started_at: float | None = None) -> dict:
    heartbeat = _worker_heartbeat_from_payload(payload, kind) if payload is not None else {'worker_kind': kind, 'heartbeat_kind': kind, 'heartbeat_utc': utc_now_iso()}
    completed_utc = utc_now_iso()
    status_payload = {
        **heartbeat,
        'utc': completed_utc,
        'completed_utc': completed_utc,
        'phase': phase,
        'ok': bool(ok) if ok is not None else (phase != 'failure'),
        'error': error,
    }
    if started_at is not None:
        status_payload['duration_ms'] = round((time.time() - float(started_at)) * 1000.0, 3)
    if extra:
        status_payload.update(dict(extra or {}))
    if phase == 'success':
        status_payload['last_success_utc'] = completed_utc
    elif kind == 'scan':
        prev = dict(getattr(state, 'last_scan_status', {}) or {})
        if prev.get('last_success_utc') and 'last_success_utc' not in status_payload:
            status_payload['last_success_utc'] = prev.get('last_success_utc')
    else:
        prev = dict(getattr(state, 'last_exit_status', {}) or {})
        if prev.get('last_success_utc') and 'last_success_utc' not in status_payload:
            status_payload['last_success_utc'] = prev.get('last_success_utc')
    if kind == 'scan':
        state.set_last_scan_status(status_payload)
    else:
        state.set_last_exit_status(status_payload)
    return status_payload


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
    raw_balances = {}
    canonical_balances = {}
    balance_sources = {}
    balance_ok = True
    balance_error = None
    try:
        snap = _merged_balances_snapshot() or {}
        raw_balances = dict(snap.get('merged') or {})
        canonical_balances = dict(snap.get('economic_balances') or snap.get('canonical_merged') or {})
        balance_sources = dict(snap.get('economic_balance_sources') or snap.get('canonical_sources') or snap.get('sources') or {})
        balances = canonical_balances or _merged_balances_by_asset() or {}
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
    cash_balances = {k: float(v or 0.0) for k, v in (balances or {}).items() if k in ('USD', 'USDC', 'USDT')}
    cash_usd = float(sum(cash_balances.values()) or 0.0)
    position_exposure_usd = sum(float(p.get('notional_usd', 0.0) or 0.0) for p in positions)
    open_buy_order_notional_usd = _sum_open_order_notional_usd(open_orders, side='buy') if open_orders.get('ok') else 0.0
    equity_usd = float(cash_usd + position_exposure_usd)
    utilization_pct = (float(position_exposure_usd + open_buy_order_notional_usd) / float(equity_usd)) if equity_usd > 0 else 0.0
    position_contributions = []
    for p in positions:
        position_contributions.append({
            'symbol': p.get('symbol'),
            'asset': p.get('asset'),
            'qty': float(p.get('qty', 0.0) or 0.0),
            'price': float(p.get('price', 0.0) or 0.0),
            'notional_usd': float(p.get('notional_usd', 0.0) or 0.0),
            'balance_sources': list(p.get('balance_sources') or []),
        })
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
        'raw_merged_balances': raw_balances,
        'canonical_balances': canonical_balances,
        'canonical_balance_sources': balance_sources,
        'balance_agreement': dict(snap.get('economic_balance_agreement') or {}) if 'snap' in locals() else {},
        'cash_components_usd': cash_balances,
        'position_components': position_contributions,
        'equity_components': {
            'cash_usd': float(cash_usd),
            'position_exposure_usd': float(position_exposure_usd),
        },
    }




def _pretrade_health_gate_summary(*, rerun_startup_check: bool = False) -> dict:
    enabled = bool(getattr(settings, 'pretrade_health_gate_enabled', True))
    worker_health = {
        'scan': _worker_status_meta(getattr(state, 'last_scan_status', {}) or {}, route_truth=getattr(state, 'last_scan_route_truth', {}) or {}),
        'exit': _worker_status_meta(getattr(state, 'last_exit_status', {}) or {}, route_truth=getattr(state, 'last_exit_route_truth', {}) or {}),
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
        'worker_route_truth': {
            'scan': _route_truth_summary('scan'),
            'exit': _route_truth_summary('exit'),
        },
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
    openish_states = {'created','validated','submitted','acknowledged','partial','replace_pending','cancel_pending'}
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
    open_trade_symbols = {str(r.get('symbol') or '') for r in (trade_journal.list_open_trades(limit=5000) or []) if str(r.get('symbol') or '')}
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
        has_open_trade = bool(sym) and (sym in open_trade_symbols)
        is_orphan_plan = (not has_open_intent) and (not has_live_position) and (not has_runtime_plan)
        if is_orphan_plan:
            orphan_trade_plans.append({
                'trade_plan_id': trade_plan_id,
                'symbol': sym,
                'status': status,
                'updated_ts': updated_ts,
                'age_sec': age_sec,
                'has_open_trade': has_open_trade,
            })
            close_now = bool(apply and trade_plan_id and (not has_open_trade) and age_sec is not None and age_sec >= 0.0)
            if close_now:
                try:
                    lifecycle_db.close_trade_plan(trade_plan_id, 'failed_reconcile')
                    lifecycle_db.record_anomaly('orphan_trade_plan', 'warn', symbol=sym or None, trade_plan_id=trade_plan_id, details={'previous_status': status, 'age_sec': age_sec, 'closed_by_patch': 'patch_026'})
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

    orphan_open_trades = []
    cleared_open_trade_symbols = []
    for ot in trade_journal.list_open_trades(limit=5000) or []:
        sym = str(ot.get('symbol') or '')
        if not sym:
            continue
        has_live_position = sym in live_syms
        has_runtime_plan = sym in live_runtime_plan_syms
        has_open_intent = False
        for it in openish_intents:
            if str(it.get('symbol') or '') == sym:
                has_open_intent = True
                break
        has_open_trade_plan = False
        for tp in openish_trade_plans:
            if str(tp.get('symbol') or '') == sym:
                has_open_trade_plan = True
                break
        if (not has_live_position) and (not has_runtime_plan) and (not has_open_intent) and (not has_open_trade_plan):
            orphan_open_trades.append({'symbol': sym, 'opened_ts': float(ot.get('opened_ts') or 0.0)})
            if apply:
                try:
                    trade_journal.delete_open_trade(sym)
                    cleared_open_trade_symbols.append(sym)
                    lifecycle_db.record_anomaly('orphan_open_trade', 'warn', symbol=sym or None, details={'closed_by_patch': 'patch_026'})
                except Exception:
                    pass

    resolved_anomaly_ids = []
    resolved_anomaly_count = 0
    if apply:
        try:
            unresolved = lifecycle_db.unresolved_anomalies(limit=500)
        except Exception:
            unresolved = []
        for an in unresolved:
            try:
                kind = str(an.get('kind') or '')
                aid = int(an.get('id') or 0)
                if not aid:
                    continue
                if kind == 'orphan_trade_plan':
                    tp_id = str(an.get('trade_plan_id') or '')
                    tp = lifecycle_db.get_trade_plan(tp_id) if tp_id else None
                    if (not tp) or tp.get('closed_ts') is not None or str(tp.get('status') or '') not in lifecycle_db.OPENISH_TRADE_PLAN_STATUSES:
                        lifecycle_db.resolve_anomaly(aid)
                        resolved_anomaly_ids.append(aid)
                elif kind == 'orphan_internal_intent':
                    iid = str(an.get('intent_id') or '')
                    if iid:
                        rows = lifecycle_db.list_rows('order_intents', limit=1, where='intent_id = ?', args=[iid], order_by='updated_ts DESC')
                        it = rows[0] if rows else None
                        if (not it) or str(it.get('state') or '') not in openish_states:
                            lifecycle_db.resolve_anomaly(aid)
                            resolved_anomaly_ids.append(aid)
            except Exception:
                pass
        resolved_anomaly_count = len(resolved_anomaly_ids)

    if apply:
        for bo in orphan_broker_orders:
            try:
                lifecycle_db.record_anomaly('orphan_broker_order', 'warn', symbol=str(bo.get('symbol') or '') or None, details=bo)
            except Exception:
                pass

    # Patch 026 hotfix: always define broker_trade_backfill before returning.
    if apply:
        try:
            broker_trade_backfill = _backfill_closed_trades_from_broker_history(now_ts=now_ts)
        except Exception as e:
            broker_trade_backfill = {'ok': False, 'error': str(e), 'backfilled': [], 'skipped': [], 'matched_sell_count': 0}
    else:
        broker_trade_backfill = {'ok': True, 'skipped_apply_false': True, 'backfilled': [], 'skipped': [], 'matched_sell_count': 0}

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
        'orphan_open_trades': orphan_open_trades,
        'orphan_open_trade_count': len(orphan_open_trades),
        'stale_pending_exit_symbols': stale_pending_exit_symbols,
        'stale_pending_exit_count': len(stale_pending_exit_symbols),
        'apply_changes': bool(apply),
        'applied': {
            'marked_failed_reconcile_intents': marked_failed,
            'closed_orphan_trade_plans': closed_trade_plans,
            'cleared_open_trade_symbols': cleared_open_trade_symbols,
            'cleared_pending_exit_symbols': cleared_pending_exit_symbols,
            'resolved_anomaly_ids': resolved_anomaly_ids,
            'resolved_anomaly_count': resolved_anomaly_count,
        },
        'broker_trade_backfill': broker_trade_backfill,
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
        adopted_policy = _adopted_lifecycle_policy()
        state.set_plan(TradePlan(symbol=sym, side='buy', notional_usd=notional, entry_price=px, stop_price=stop_px, take_price=take_px, strategy='adopted', opened_ts=now_ts, max_hold_sec=int(adopted_policy.get('max_hold_sec', 0) or 0), risk_snapshot={'lifecycle_policy': adopted_policy}))
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


@app.get("/diagnostics/holdings_truth")
def diagnostics_holdings_truth(include_raw: int = 1):
    snap = _merged_balances_snapshot() or {}
    merged = dict(snap.get('merged') or {})
    canonical_merged = dict(snap.get('economic_balances') or snap.get('canonical_merged') or {})
    parsed = dict(snap.get('parsed') or {})
    positions_api = dict(snap.get('positions_api') or {})
    plan_syms = set((getattr(state, 'plans', {}) or {}).keys())
    state_open = set(getattr(state, 'open_positions', []) or [])
    pending = dict(getattr(state, 'pending_exits', {}) or {})

    rows = []
    parsed_canonical = dict(snap.get('parsed_canonical') or _canonicalize_balances(parsed, aggregate="max"))
    positions_api_canonical = dict(snap.get('positions_api_canonical') or _canonicalize_balances(positions_api, aggregate="max"))
    agreement = dict(snap.get('economic_balance_agreement') or {})
    for asset in sorted(canonical_merged.keys()):
        if str(asset).upper() == 'USD':
            continue
        qty = float(canonical_merged.get(asset, 0.0) or 0.0)
        symbol = normalize_symbol(f"{asset}/USD")
        try:
            px = float(_last_price(symbol) or 0.0)
        except Exception:
            px = 0.0
        notional = float(qty * px) if qty > 0 and px > 0 else 0.0
        qualifies = bool(notional >= float(getattr(settings, 'min_position_notional_usd', 0.0) or 0.0)) if px > 0 else False
        plan_obj = state.plans.get(symbol)
        if plan_obj is not None:
            plan_obj, _ = _normalize_plan_lifecycle_policy(plan_obj, now_ts=time.time(), persist=False)
        has_plan = symbol in plan_syms
        pending_exit = symbol in pending
        in_state_open = symbol in state_open
        plan_policy = dict((getattr(plan_obj, 'risk_snapshot', {}) or {}).get('lifecycle_policy') or {}) if plan_obj is not None else {}
        if px <= 0:
            classification = 'unpriced_broker_inventory'
        elif not qualifies:
            classification = 'dust_inventory'
        elif has_plan:
            classification = 'managed_open_position'
        else:
            classification = 'unmanaged_broker_inventory'
        rows.append({
            'asset': asset,
            'symbol': symbol,
            'qty_merged': qty,
            'qty_parsed': float(parsed_canonical.get(asset, 0.0) or 0.0),
            'qty_positions_api': float(positions_api_canonical.get(asset, 0.0) or 0.0),
            'sources': list((snap.get('economic_balance_sources') or snap.get('canonical_sources') or {}).get(asset, [])),
            'source_agreement': dict(agreement.get(asset) or {}),
            'price': px,
            'notional_usd': notional,
            'qualifies_as_position': qualifies,
            'has_plan': has_plan,
            'state_open': in_state_open,
            'pending_exit': pending_exit,
            'plan_origin': _plan_origin(plan_obj) if plan_obj is not None else None,
            'plan_policy_source': str(plan_policy.get('policy_source') or _plan_policy_source(plan_obj) if plan_obj is not None else ''),
            'time_exit_enabled': bool(plan_policy.get('time_exit_enabled', _effective_plan_max_hold_sec(plan_obj) > 0 if plan_obj is not None else False)),
            'max_hold_sec': int(plan_policy.get('max_hold_sec', _effective_plan_max_hold_sec(plan_obj) if plan_obj is not None else 0) or 0),
            'classification': classification,
        })

    return {
        'ok': True,
        'utc': utc_now_iso(),
        'build': PATCH_BUILD,
        'raw_ok': bool(snap.get('raw_ok', True)),
        'raw_error': snap.get('raw_error'),
        'parsed_error': snap.get('parsed_error'),
        'positions_error': snap.get('positions_error'),
        'min_position_notional_usd': float(getattr(settings, 'min_position_notional_usd', 0.0) or 0.0),
        'exit_min_notional_usd': float(getattr(settings, 'exit_min_notional_usd', 0.0) or 0.0),
        'merged_balances': merged,
        'canonical_balances': canonical_merged,
        'parsed_balances': parsed,
        'positions_api_balances': positions_api,
        'parsed_canonical_balances': parsed_canonical,
        'positions_api_canonical_balances': positions_api_canonical,
        'balance_agreement': agreement,
        'holdings': rows,
        'raw_balances': snap.get('raw') if int(include_raw or 0) == 1 else None,
    }


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


@app.get("/diagnostics/exit_execution_truth")
def diagnostics_exit_execution_truth(limit: int = 10):
    try:
        lim = max(1, min(int(limit or 10), 50))
    except Exception:
        lim = 10
    history = list(getattr(state, 'exit_execution_history', []) or [])
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'build': PATCH_BUILD,
        'last_exit_execution': dict(getattr(state, 'last_exit_execution', {}) or {}),
        'recent_exit_executions': history[-lim:],
        'pending_exits': dict(getattr(state, 'pending_exits', {}) or {}),
        'last_exit_status': dict(getattr(state, 'last_exit_status', {}) or {}),
        'last_exit_route_truth': dict(getattr(state, 'last_exit_route_truth', {}) or {}),
        'last_reconcile_result': dict(getattr(state, 'last_reconcile_result', {}) or {}),
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
    scan_meta = _worker_status_meta(getattr(state, 'last_scan_status', {}) or {}, route_truth=getattr(state, 'last_scan_route_truth', {}) or {})
    exit_meta = _worker_status_meta(getattr(state, 'last_exit_status', {}) or {}, route_truth=getattr(state, 'last_exit_route_truth', {}) or {})
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'scan': {**scan_meta, 'snapshot': getattr(state, 'last_scan_status', {}) or {}},
        'exit': {**exit_meta, 'snapshot': getattr(state, 'last_exit_status', {}) or {}},
        'overall_stale': bool(scan_meta.get('stale') or exit_meta.get('stale')),
    }


@app.get("/diagnostics/worker_heartbeat_truth")
def diagnostics_worker_heartbeat_truth():
    gate = _pretrade_health_gate_summary(rerun_startup_check=False)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'worker_health': gate.get('worker_health'),
        'gate_open': gate.get('gate_open'),
        'violations': gate.get('violations') or [],
        'heartbeat_truth': {
            'scan': getattr(state, 'last_scan_status', {}) or {},
            'exit': getattr(state, 'last_exit_status', {}) or {},
        },
        'worker_route_truth': {
            'scan': _route_truth_summary('scan'),
            'exit': _route_truth_summary('exit'),
        },
    }


@app.get("/diagnostics/worker_route_truth")
def diagnostics_worker_route_truth():
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'build': PATCH_BUILD,
        'worker_route_truth': {
            'scan': _route_truth_summary('scan'),
            'exit': _route_truth_summary('exit'),
        },
        'worker_health': {
            'scan': _worker_status_meta(getattr(state, 'last_scan_status', {}) or {}, route_truth=getattr(state, 'last_scan_route_truth', {}) or {}),
            'exit': _worker_status_meta(getattr(state, 'last_exit_status', {}) or {}, route_truth=getattr(state, 'last_exit_route_truth', {}) or {}),
        },
    }


@app.get("/health")
def health():
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()
    open_orders = _broker_open_orders_summary()
    scan_meta = _worker_status_meta(getattr(state, 'last_scan_status', {}) or {}, route_truth=getattr(state, 'last_scan_route_truth', {}) or {})
    exit_meta = _worker_status_meta(getattr(state, 'last_exit_status', {}) or {}, route_truth=getattr(state, 'last_exit_route_truth', {}) or {})
    account_truth = _account_truth_snapshot()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
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


def _fingerprint_ttl_sec() -> int:
    ttl = int(getattr(settings, 'signal_fingerprint_ttl_sec', 0) or 0)
    if ttl > 0:
        return ttl
    return max(int(getattr(settings, 'signal_dedupe_ttl_sec', 90) or 90), _entry_failure_cooldown_sec(), 60)


def _structured_reason(reason: str) -> str:
    return str(lifecycle_db.normalize_terminal_reason(reason) or str(reason or '').strip())


def _float_or_none(v: Any) -> float | None:
    try:
        if v is None or v == '':
            return None
        return float(v)
    except Exception:
        return None


def _int_or_none(v: Any) -> int | None:
    try:
        if v is None or v == '':
            return None
        return int(float(v))
    except Exception:
        return None


def _build_signal_fingerprint(*, symbol: str, strategy: str, bar_ts: Any = None, trigger_price: Any = None, side: str = 'buy', signal_id: str | None = None, explicit_fingerprint: str | None = None) -> str:
    if explicit_fingerprint:
        return str(explicit_fingerprint).strip()
    norm_symbol = normalize_symbol(symbol)
    norm_strategy = str(strategy or '').strip().lower()
    norm_side = str(side or 'buy').strip().lower()
    bar_bucket = _int_or_none(bar_ts)
    trigger = _float_or_none(trigger_price)
    if trigger is not None:
        trigger_key = f"{trigger:.8f}"
    elif signal_id:
        trigger_key = str(signal_id).strip()
    else:
        trigger_key = 'na'
    parts = [norm_symbol, norm_strategy, norm_side, str(bar_bucket or 'na'), trigger_key]
    base = '|'.join(parts)
    digest = hashlib.sha1(base.encode('utf-8')).hexdigest()[:16]
    return f"fp:{base}:{digest}"


def _build_admission_context(*, symbol: str, strategy: str, signal_name: str | None, signal_id: str | None, source: str, extra: dict | None = None, px_hint: float | None = None) -> dict:
    extra = dict(extra or {})
    signal_meta = dict(extra.get('signal_meta') or {})
    rank = dict(extra.get('rank') or {})
    fingerprint = _build_signal_fingerprint(
        symbol=symbol,
        strategy=strategy,
        bar_ts=extra.get('bar_ts', signal_meta.get('bar_ts')),
        trigger_price=extra.get('trigger_price', signal_meta.get('level', signal_meta.get('range_high', signal_meta.get('ltf_ema', px_hint)))),
        signal_id=signal_id,
        explicit_fingerprint=extra.get('fingerprint'),
    )
    range_high = _float_or_none(signal_meta.get('range_high'))
    range_low = _float_or_none(signal_meta.get('range_low'))
    reference_price = _float_or_none(extra.get('reference_price', px_hint))
    range_width_pct = None
    if range_high is not None and range_low is not None and range_low >= 0:
        denom = reference_price if reference_price and reference_price > 0 else range_high
        if denom and denom > 0:
            range_width_pct = (range_high - range_low) / denom
    breakout_level = _float_or_none(signal_meta.get('level', signal_meta.get('range_high', signal_meta.get('ltf_ema'))))
    breakout_distance_pct = None
    if breakout_level and breakout_level > 0 and reference_price is not None:
        breakout_distance_pct = (reference_price - breakout_level) / breakout_level
    return {
        'symbol': symbol,
        'strategy': strategy,
        'signal_name': signal_name,
        'signal_id': signal_id,
        'source': source,
        'fingerprint': fingerprint,
        'bar_ts': _int_or_none(extra.get('bar_ts', signal_meta.get('bar_ts'))),
        'trigger_price': _float_or_none(extra.get('trigger_price', breakout_level)),
        'reference_price': reference_price,
        'atr': _float_or_none(signal_meta.get('atr_now')),
        'range_high': range_high,
        'range_low': range_low,
        'range_width_pct': range_width_pct,
        'breakout_level': breakout_level,
        'breakout_distance_pct': breakout_distance_pct,
        'ranking_score': _float_or_none(extra.get('ranking_score', rank.get('score'))),
        'spread_pct': _float_or_none(extra.get('spread_pct', signal_meta.get('spread_pct'))),
        'regime_state': 'quiet' if bool(extra.get('regime_quiet')) else 'expansion',
        'signal_meta': signal_meta,
        'rank': rank,
        'extra': extra,
    }


def _record_admission_event(ctx: dict, *, admission_result: str, reject_reason: str | None = None, payload: dict | None = None) -> None:
    try:
        lifecycle_db.record_admission_event(
            symbol=str(ctx.get('symbol') or ''),
            strategy_id=(ctx.get('strategy') or None),
            signal_id=(ctx.get('signal_id') or None),
            signal_name=(ctx.get('signal_name') or None),
            fingerprint=(ctx.get('fingerprint') or None),
            source=(ctx.get('source') or None),
            bar_ts=_int_or_none(ctx.get('bar_ts')),
            trigger_price=_float_or_none(ctx.get('trigger_price')),
            reference_price=_float_or_none(ctx.get('reference_price')),
            atr=_float_or_none(ctx.get('atr')),
            range_high=_float_or_none(ctx.get('range_high')),
            range_low=_float_or_none(ctx.get('range_low')),
            range_width_pct=_float_or_none(ctx.get('range_width_pct')),
            breakout_level=_float_or_none(ctx.get('breakout_level')),
            breakout_distance_pct=_float_or_none(ctx.get('breakout_distance_pct')),
            ranking_score=_float_or_none(ctx.get('ranking_score')),
            spread_pct=_float_or_none(ctx.get('spread_pct')),
            regime_state=(ctx.get('regime_state') or None),
            admission_result=admission_result,
            reject_reason=_structured_reason(reject_reason or ''),
            payload=payload or {},
        )
    except Exception:
        pass


def _record_rejected_admission(ctx: dict, reason: str, *, payload: dict | None = None) -> None:
    structured = _structured_reason(reason)
    _record_ops_event('admission_rejected', symbol=ctx.get('symbol'), strategy=ctx.get('strategy'), signal_id=ctx.get('signal_id'), reason=structured, payload=payload or {}, fingerprint=ctx.get('fingerprint'))
    _record_admission_event(ctx, admission_result='rejected', reject_reason=structured, payload=payload or {})


def _entry_failure_cooldown_sec() -> int:
    bars = int(getattr(settings, 'entry_failure_cooldown_bars', 0) or 0)
    tf_min = int(getattr(settings, 'entry_engine_timeframe_min', 0) or 0)
    if bars <= 0 or tf_min <= 0:
        return 0
    return int(bars * tf_min * 60)


def _record_ops_event(event_type: str, *, symbol: str | None = None, strategy: str | None = None, signal_id: str | None = None, reason: str | None = None, payload: dict | None = None, fingerprint: str | None = None) -> None:
    try:
        lifecycle_db.record_ops_event(
            event_type,
            symbol=symbol,
            strategy_id=strategy,
            signal_id=signal_id,
            fingerprint=fingerprint,
            reason=reason,
            payload=payload or {},
        )
    except Exception:
        pass


def can_start_new_entry(symbol: str, strategy: str) -> dict:
    cooldown_sec = _entry_failure_cooldown_sec()
    try:
        return lifecycle_db.can_start_new_entry(symbol, strategy, cooldown_sec=cooldown_sec)
    except Exception as e:
        return {'ok': False, 'reason': 'workflow_gate_error', 'blocking_objects': {'error': str(e)}, 'cooldown_remaining_sec': 0}


def _entry_submit_path_snapshot(*, symbol: str | None = None, strategy: str | None = None, limit: int = 50) -> dict:
    limit = max(1, min(int(limit), 500))
    where_bits = []
    args = []
    if symbol:
        where_bits.append('symbol = ?')
        args.append(str(symbol))
    if strategy:
        where_bits.append('strategy_id = ?')
        args.append(str(strategy))
    where = ' AND '.join(where_bits) if where_bits else None
    try:
        admission_events = lifecycle_db.list_rows('admission_events', limit=limit, where=where, args=args, order_by='created_ts DESC')
        order_intents = lifecycle_db.list_rows('order_intents', limit=limit, where=where, args=args, order_by='updated_ts DESC')
        broker_orders = lifecycle_db.list_rows('broker_orders', limit=limit, where=where, args=args, order_by='updated_ts DESC')
        trade_lifecycle = lifecycle_db.list_rows('trade_lifecycle_events', limit=limit, where=where, args=args, order_by='created_ts DESC')
        ops_events = lifecycle_db.list_rows('ops_events', limit=limit, where=where, args=args, order_by='created_ts DESC')
    except Exception as e:
        return {'ok': False, 'error': str(e)}

    accepted = [r for r in admission_events if str(r.get('admission_result') or '') == 'accepted']
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'filters': {'symbol': symbol, 'strategy': strategy, 'limit': limit},
        'counts': {
            'accepted_admissions': len(accepted),
            'order_intents': len(order_intents),
            'broker_orders': len(broker_orders),
            'trade_lifecycle_events': len(trade_lifecycle),
            'ops_events': len(ops_events),
        },
        'recent_accepted_admissions': accepted[:limit],
        'recent_order_intents': order_intents[:limit],
        'recent_broker_orders': broker_orders[:limit],
        'recent_trade_lifecycle_events': trade_lifecycle[:limit],
        'recent_ops_events': ops_events[:limit],
        'active_workflow_locks': lifecycle_db.list_active_workflow_locks(symbol=symbol, strategy_id=strategy, limit=limit),
    }


def _accepted_not_submitted_snapshot(*, symbol: str | None = None, strategy: str | None = None, lookback_sec: int = 86400, min_age_sec: int = 5, limit: int = 50) -> dict:
    now_ts = time.time()
    rows = lifecycle_db.list_trade_lifecycle_events(limit=max(int(limit) * 20, 200), symbol=symbol, strategy_id=strategy)
    accepted = {}
    order_intents = set()
    broker_orders = set()
    trade_plans = set()
    recent_events = []
    for row in rows:
        evt_ts = float(row.get('event_ts') or row.get('created_ts') or 0.0)
        if lookback_sec > 0 and evt_ts > 0 and (now_ts - evt_ts) > float(lookback_sec):
            continue
        intent_id = str(row.get('intent_id') or '')
        evt_type = str(row.get('event_type') or '')
        if evt_type == 'candidate_accepted' and intent_id:
            accepted[intent_id] = row
        elif evt_type in {'intent_created', 'order_intent_created'} and intent_id:
            order_intents.add(intent_id)
        elif evt_type in {'submit_attempted', 'broker_acknowledged', 'broker_terminal', 'position_opened'} and intent_id:
            broker_orders.add(intent_id)
        if evt_type in {'trade_plan_created', 'position_opened'}:
            tp = str(row.get('trade_plan_id') or '')
            if tp:
                trade_plans.add(tp)
        if evt_type in {'candidate_accepted', 'workflow_lock_acquired', 'entry_packet_build_started', 'entry_packet_build_completed', 'order_intent_create_started', 'intent_created', 'submit_attempted', 'entry_abort_pre_intent', 'submit_failed'}:
            recent_events.append(row)
    stranded = []
    for intent_id, row in accepted.items():
        evt_ts = float(row.get('event_ts') or row.get('created_ts') or 0.0)
        if min_age_sec > 0 and evt_ts > 0 and (now_ts - evt_ts) < float(min_age_sec):
            continue
        if intent_id in order_intents or intent_id in broker_orders:
            continue
        stranded.append(row)
    stranded = sorted(stranded, key=lambda r: float(r.get('event_ts') or r.get('created_ts') or 0.0), reverse=True)[: int(limit)]
    recent_events = sorted(recent_events, key=lambda r: float(r.get('event_ts') or r.get('created_ts') or 0.0), reverse=True)[: max(int(limit) * 4, 50)]
    return {
        'utc': utc_now_iso(),
        'symbol': symbol,
        'strategy': strategy,
        'lookback_sec': int(lookback_sec),
        'min_age_sec': int(min_age_sec),
        'accepted_admissions_without_intent': len(stranded),
        'recent_stranded_acceptances': stranded,
        'recent_events': recent_events,
    }


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
    admission_ctx = _build_admission_context(symbol=symbol, strategy=strategy, signal_name=signal_name, signal_id=signal_id, source=source, extra=extra or {}, px_hint=px_override)

    def ignored(reason: str, **extra_fields):
        structured_reason = _structured_reason(reason)
        merged_payload = {'raw_reason': reason, **(extra_fields or {})}
        if merged_payload.get('reference_price') is not None:
            admission_ctx['reference_price'] = _float_or_none(merged_payload.get('reference_price'))
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": source,
            "status": "ignored",
            "reason": structured_reason,
            **merged_payload,
        }
        _log_event("warning", evt)
        _record_blocked_trade(structured_reason, req_id=req_id, source=source, **merged_payload)
        _record_ops_event('entry_ignored', symbol=symbol, strategy=strategy, signal_id=signal_id, reason=structured_reason, payload=evt, fingerprint=admission_ctx.get('fingerprint'))
        _record_rejected_admission(admission_ctx, structured_reason, payload=evt)
        return {"ok": True, "ignored": True, "reason": structured_reason, **merged_payload}

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    rb1_fingerprint_only_dedupe = (
        str(strategy or '').strip().lower() == 'rb1'
        and os.getenv("RB1_FINGERPRINT_ONLY_DEDUPE", "1").strip().lower() in ("1", "true", "yes", "on")
    )
    if signal_id and (not rb1_fingerprint_only_dedupe) and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored("duplicate_signal_id", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    fp_ttl = _fingerprint_ttl_sec()
    fp_result = lifecycle_db.register_signal_fingerprint(
        str(admission_ctx.get('fingerprint') or ''),
        symbol=symbol,
        strategy_id=strategy,
        signal_id=signal_id,
        ttl_sec=fp_ttl,
        metadata={
            'source': source,
            'signal_name': signal_name,
            'bar_ts': admission_ctx.get('bar_ts'),
            'trigger_price': admission_ctx.get('trigger_price'),
        },
    )
    if bool(fp_result.get('duplicate')):
        return ignored('duplicate_signal_fingerprint', symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id, fingerprint=admission_ctx.get('fingerprint'), fingerprint_ttl_sec=fp_ttl, existing=fp_result.get('existing'))

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

    workflow_gate = can_start_new_entry(symbol, strategy)
    if not bool(workflow_gate.get('ok')):
        return ignored(str(workflow_gate.get('reason') or 'entry_workflow_blocked'), symbol=symbol, strategy=strategy, workflow_gate=workflow_gate)

    reconcile_meta = _maybe_reconcile_state_before_entry()

    # Determine reference price for fills/brackets early because real risk sizing must use the actual stop distance.
    px = float(px_override) if px_override is not None else float(_last_price(symbol))
    admission_ctx['reference_price'] = float(px)
    if admission_ctx.get('breakout_level') and float(admission_ctx.get('breakout_level') or 0.0) > 0:
        admission_ctx['breakout_distance_pct'] = (float(px) - float(admission_ctx.get('breakout_level'))) / float(admission_ctx.get('breakout_level'))

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
    client_order_key = f"entry:{signal_key}:{symbol}:{strategy}"
    workflow_lock_key = f"entry:{symbol}:{strategy}"

    _record_admission_event(admission_ctx, admission_result='accepted', payload={'req_id': req_id, 'risk_admission': risk_admission, 'sizing': sizing_meta or {}, 'reference_price': px, 'notional_usd': float(_notional)})
    _record_ops_event('admission_passed', symbol=symbol, strategy=strategy, signal_id=signal_id, reason='admission_passed', payload={'req_id': req_id, 'fingerprint': admission_ctx.get('fingerprint')}, fingerprint=admission_ctx.get('fingerprint'))
    lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='candidate_accepted', trade_plan_id=None, intent_id=intent_id, symbol=symbol, strategy_id=strategy, reason='admission_passed', payload={'req_id': req_id, 'signal_id': signal_id, 'fingerprint': admission_ctx.get('fingerprint'), 'trigger_price': float(px), 'requested_notional_usd': float(_notional)})

    acquired_lock = lifecycle_db.acquire_workflow_lock(
        workflow_lock_key,
        symbol=symbol,
        strategy_id=strategy,
        stage='entry_submit',
        owner_req_id=req_id,
        ttl_sec=int(getattr(settings, 'workflow_lock_ttl_sec', 300) or 300),
        metadata={'signal_id': signal_key, 'client_order_key': client_order_key, 'source': source},
    )
    if not acquired_lock:
        active_lock = lifecycle_db.get_active_workflow_lock(workflow_lock_key)
        return ignored('active_workflow_lock', symbol=symbol, strategy=strategy, workflow_lock=active_lock or {})

    lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='workflow_lock_acquired', trade_plan_id=None, intent_id=intent_id, broker_order_id=None, position_id=None, symbol=symbol, strategy_id=strategy, payload={'req_id': req_id, 'lock_key': workflow_lock_key})
    res = None
    intent_created = False
    try:
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='entry_packet_build_started', trade_plan_id=None, intent_id=intent_id, broker_order_id=None, position_id=None, symbol=symbol, strategy_id=strategy, payload={'req_id': req_id, 'signal_id': signal_id, 'fingerprint': admission_ctx.get('fingerprint')})
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='entry_packet_build_completed', trade_plan_id=None, intent_id=intent_id, broker_order_id=None, position_id=None, symbol=symbol, strategy_id=strategy, payload={'req_id': req_id, 'client_order_key': client_order_key, 'requested_notional_usd': float(_notional), 'reference_price': float(px), 'stop_price': float(stop_price), 'take_price': float(take_price)})
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='order_intent_create_started', trade_plan_id=None, intent_id=intent_id, broker_order_id=None, position_id=None, symbol=symbol, strategy_id=strategy, payload={'req_id': req_id, 'client_order_key': client_order_key})
        lifecycle_db.upsert_order_intent(
            {
                "intent_id": intent_id,
                "trade_plan_id": None,
                "symbol": symbol,
                "side": "buy",
                "order_type": exec_mode_override or settings.execution_mode,
                "strategy_id": strategy,
                "state": "created",
                "desired_qty": (float(_notional) / float(px)) if float(px) > 0 else 0.0,
                "desired_notional_usd": float(_notional),
                "limit_price": float(px),
                "client_order_key": client_order_key,
                "raw_json": {"req_id": req_id, "source": source, "dry_run": dry_run, "stage": "pre_submit"},
            },
        )
        intent_created = True
        lifecycle_db.upsert_broker_order({
            "broker_order_id": intent_id,
            "intent_id": intent_id,
            "trade_plan_id": None,
            "symbol": symbol,
            "strategy_id": strategy,
            "side": "buy",
            "order_type": exec_mode_override or settings.execution_mode,
            "lifecycle_stage": "entry",
            "status": "created",
            "client_order_key": client_order_key,
            "requested_qty": (float(_notional) / float(px)) if float(px) > 0 else 0.0,
            "requested_notional_usd": float(_notional),
            "limit_price": float(px),
            "raw_json": {"req_id": req_id, "source": source, "dry_run": dry_run, "stage": "pre_submit"},
        })
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='intent_created', trade_plan_id=None, intent_id=intent_id, broker_order_id=intent_id, symbol=symbol, strategy_id=strategy, payload={'req_id': req_id, 'client_order_key': client_order_key, 'requested_notional_usd': float(_notional)})

        state.set_order_lock(entry_lock_key, meta={"symbol": symbol, "side": "buy", "strategy": strategy, "req_id": req_id, "intent_id": intent_id})
        lifecycle_db.transition_order_intent(intent_id, 'submitted', raw_json={"req_id": req_id, "source": source, "dry_run": dry_run, "stage": "submit_attempt"})
        lifecycle_db.upsert_broker_order({
            "broker_order_id": intent_id, "intent_id": intent_id, "trade_plan_id": None, "symbol": symbol, "strategy_id": strategy,
            "side": "buy", "order_type": exec_mode_override or settings.execution_mode, "lifecycle_stage": "entry",
            "status": "submitted", "client_order_key": client_order_key, "requested_qty": (float(_notional) / float(px)) if float(px) > 0 else 0.0,
            "requested_notional_usd": float(_notional), "limit_price": float(px),
            "raw_json": {"req_id": req_id, "source": source, "dry_run": dry_run, "stage": "submit_attempt"},
        })
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='submit_attempted', trade_plan_id=None, intent_id=intent_id, broker_order_id=intent_id, symbol=symbol, strategy_id=strategy, payload={'req_id': req_id, 'client_order_key': client_order_key})
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
    except Exception as e:
        try:
            state.clear_order_lock(entry_lock_key)
        except Exception:
            pass
        err_payload = {
            "error": str(e),
            "exception_type": type(e).__name__,
            "traceback": traceback.format_exc(limit=12),
            "symbol": symbol,
            "strategy": strategy,
            "stage": "entry_submit" if intent_created else "pre_intent",
            "req_id": req_id,
            "signal_id": signal_id,
            "fingerprint": admission_ctx.get('fingerprint'),
            "client_order_key": client_order_key,
        }
        if intent_created:
            lifecycle_db.transition_order_intent(
                intent_id,
                "failed",
                reject_reason="submit_failed_pre_ack",
                last_broker_status="entry_exception_before_ack",
                raw_json=err_payload,
            )
            lifecycle_db.upsert_broker_order({
                "broker_order_id": intent_id, "intent_id": intent_id, "trade_plan_id": None, "symbol": symbol, "strategy_id": strategy,
                "side": "buy", "order_type": exec_mode_override or settings.execution_mode, "lifecycle_stage": "entry",
                "status": "failed", "client_order_key": client_order_key, "reject_reason": "submit_failed_pre_ack",
                "raw_json": err_payload, "closed_ts": time.time(),
            })
            lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='submit_failed', trade_plan_id=None, intent_id=intent_id, broker_order_id=intent_id, symbol=symbol, strategy_id=strategy, reason='submit_failed_pre_ack', payload=err_payload)
            _record_ops_event('entry_submit_failed', symbol=symbol, strategy=strategy, signal_id=signal_id, reason='submit_failed_pre_ack', payload={**err_payload, "intent_id": intent_id}, fingerprint=admission_ctx.get('fingerprint'))
        else:
            lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='entry_abort_pre_intent', trade_plan_id=None, intent_id=intent_id, broker_order_id=None, symbol=symbol, strategy_id=strategy, reason='entry_abort_pre_intent', payload=err_payload)
            _record_ops_event('entry_abort_pre_intent', symbol=symbol, strategy=strategy, signal_id=signal_id, reason='entry_abort_pre_intent', payload={**err_payload, "intent_id": intent_id}, fingerprint=admission_ctx.get('fingerprint'))
        lifecycle_db.release_workflow_lock(workflow_lock_key)
        return {"ok": False, "executed": False, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price), "error": str(e), "trade_plan_id": None, "intent_id": intent_id, "position_id": None, "exception_type": type(e).__name__, "pre_intent": not intent_created}

    if not (res and res.get("ok")):
        state.clear_order_lock(entry_lock_key)
        classified = execution_state.classify_order_result(res or {})
        next_state = str(classified.get("state") or "rejected")
        if next_state == 'failed_reconcile':
            next_state = 'failed'
        reject_reason = str(classified.get("error") or (res or {}).get("error") or "order_failed")
        lifecycle_db.transition_order_intent(
            intent_id,
            next_state,
            broker_txid=classified.get("broker_txid"),
            filled_qty=classified.get("filled_qty"),
            avg_fill_price=classified.get("avg_fill_price"),
            fees_usd=classified.get("fees_usd"),
            reject_reason=reject_reason,
            cancel_reason=reject_reason if next_state == "cancelled" else None,
            last_broker_status=str(classified.get("execution") or ""),
            raw_json=res or {},
        )
        lifecycle_db.upsert_broker_order({
            "broker_order_id": intent_id, "intent_id": intent_id, "trade_plan_id": None, "symbol": symbol, "strategy_id": strategy,
            "side": "buy", "order_type": exec_mode_override or settings.execution_mode, "lifecycle_stage": "entry",
            "status": next_state, "client_order_key": client_order_key, "broker_txid": classified.get("broker_txid"),
            "avg_fill_price": classified.get("avg_fill_price"), "filled_qty": classified.get("filled_qty"), "fees_usd": classified.get("fees_usd"),
            "reject_reason": reject_reason, "raw_json": res or {}, "closed_ts": time.time(),
        })
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='broker_terminal', trade_plan_id=None, intent_id=intent_id, broker_order_id=intent_id, symbol=symbol, strategy_id=strategy, reason=_structured_reason(reject_reason), payload={'response': res or {}, 'classified': classified, 'state': next_state})
        rej_count = state.note_entry_rejection() if hasattr(state, "note_entry_rejection") else 0
        if int(rej_count) >= int(getattr(settings, "max_consecutive_rejections", 0) or 0) and int(getattr(settings, "max_consecutive_rejections", 0) or 0) > 0 and hasattr(state, "set_ops_lockout"):
            state.set_ops_lockout("consecutive_entry_rejections", int(getattr(settings, "ops_lockout_sec", 0) or 0))
        _record_state_model_anomaly("entry_order_rejected", "warn", symbol=symbol, intent_id=intent_id, response=res or {}, classified=classified, consecutive_entry_rejections=int(getattr(state, "consecutive_entry_rejections", 0) or 0))
        failure_event_type = 'entry_broker_cancelled' if next_state == 'cancelled' else 'entry_broker_rejected' if next_state == 'rejected' else 'entry_failed'
        _record_ops_event(failure_event_type, symbol=symbol, strategy=strategy, signal_id=signal_id, reason=_structured_reason(reject_reason), payload={"intent_id": intent_id, "req_id": req_id, "response": res or {}, "classified": classified}, fingerprint=admission_ctx.get('fingerprint'))
        _record_admission_event(admission_ctx, admission_result='rejected', reject_reason=_structured_reason(reject_reason), payload={"intent_id": intent_id, "req_id": req_id, "response": res or {}, "classified": classified})
        lifecycle_db.release_workflow_lock(workflow_lock_key)

        # Do NOT create a plan on failed orders. Cooldown is enforced by recent ops events.
        return {"ok": False, "executed": False, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price), "error": res.get("error") or "order_failed", "trade_plan_id": None, "intent_id": intent_id, "position_id": None}

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
        lifecycle_db.link_trade_plan_to_intent_atomic(
            intent_id,
            {
                "trade_plan_id": trade_plan_id,
                "symbol": symbol,
                "strategy_id": strategy,
                "signal_id": signal_key,
                "status": "submitted",
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
            },
            intent_fields={
                "state": "acknowledged",
                "broker_txid": broker_txid,
                "last_broker_status": str(classified.get("execution") or ""),
                "raw_json": res or {},
                "acknowledged_ts": time.time(),
            },
        )
        lifecycle_db.upsert_broker_order({
            "broker_order_id": intent_id, "intent_id": intent_id, "trade_plan_id": trade_plan_id, "symbol": symbol, "strategy_id": strategy,
            "side": "buy", "order_type": exec_mode_override or settings.execution_mode, "lifecycle_stage": "entry",
            "status": "acknowledged", "client_order_key": client_order_key, "broker_txid": broker_txid,
            "requested_qty": qty, "requested_notional_usd": float(_notional), "limit_price": float(px),
            "avg_fill_price": float(plan_entry_px), "filled_qty": qty, "fees_usd": fees_usd,
            "acknowledged_ts": time.time(), "raw_json": res or {},
        })
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='broker_acknowledged', trade_plan_id=trade_plan_id, intent_id=intent_id, broker_order_id=intent_id, position_id=position_id, symbol=symbol, strategy_id=strategy, payload={'broker_txid': broker_txid, 'avg_fill_price': float(plan_entry_px), 'qty': qty})
        lifecycle_db.transition_order_intent(intent_id, "filled", broker_txid=broker_txid, filled_qty=qty, avg_fill_price=float(plan_entry_px), fees_usd=fees_usd, remaining_qty=0.0, last_broker_status=str(classified.get("execution") or ""), raw_json=res or {})
        lifecycle_db.upsert_broker_order({
            "broker_order_id": intent_id, "intent_id": intent_id, "trade_plan_id": trade_plan_id, "symbol": symbol, "strategy_id": strategy,
            "side": "buy", "order_type": exec_mode_override or settings.execution_mode, "lifecycle_stage": "entry",
            "status": "filled", "client_order_key": client_order_key, "broker_txid": broker_txid,
            "requested_qty": qty, "requested_notional_usd": float(_notional), "limit_price": float(px),
            "avg_fill_price": float(plan_entry_px), "filled_qty": qty, "remaining_qty": 0.0, "fees_usd": fees_usd,
            "acknowledged_ts": time.time(), "closed_ts": time.time(), "raw_json": res or {},
        })
        lifecycle_db.record_trade_lifecycle_event(stage='entry', event_type='position_opened', trade_plan_id=trade_plan_id, intent_id=intent_id, broker_order_id=intent_id, position_id=position_id, symbol=symbol, strategy_id=strategy, payload={'broker_txid': broker_txid, 'entry_price': float(plan_entry_px), 'qty': qty, 'fees_usd': fees_usd})
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
        _record_ops_event('entry_executed', symbol=symbol, strategy=strategy, signal_id=signal_id, reason='entry_executed', payload={'req_id': req_id, 'trade_plan_id': trade_plan_id, 'intent_id': intent_id, 'position_id': position_id}, fingerprint=admission_ctx.get('fingerprint'))
        lifecycle_db.release_workflow_lock(workflow_lock_key)
        return {"ok": True, "executed": True, "symbol": symbol, "strategy": strategy, "price": plan_entry_px, "stop": float(stop_price), "take": float(take_price), "trade_plan_id": trade_plan_id, "intent_id": intent_id, "position_id": position_id, "fingerprint": admission_ctx.get('fingerprint')}

    return {"ok": False, "executed": False, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price), "error": res.get("error") or "order_failed", "trade_plan_id": None, "intent_id": intent_id, "position_id": None}


@app.get("/diagnostics/entry_submit_path")
def diagnostics_entry_submit_path(symbol: str | None = None, strategy: str | None = None, limit: int = 50):
    return _entry_submit_path_snapshot(symbol=symbol, strategy=strategy, limit=limit)


@app.get("/diagnostics/accepted_not_submitted")
def diagnostics_accepted_not_submitted(symbol: str | None = None, strategy: str | None = None, lookback_sec: int = 86400, min_age_sec: int = 5, limit: int = 50):
    return _accepted_not_submitted_snapshot(symbol=symbol, strategy=strategy, lookback_sec=lookback_sec, min_age_sec=min_age_sec, limit=limit)


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


def _symbol_min_volume(symbol: str) -> float:
    try:
        pair = broker_kraken.to_kraken(symbol)
        meta = broker_kraken._pair_meta(pair) or {}
        return float(meta.get('ordermin') or 0.0)
    except Exception:
        return 0.0


def _dust_snapshot(symbol: str, *, qty: float, px: float) -> dict[str, Any]:
    min_volume = float(_symbol_min_volume(symbol) or 0.0)
    notional = float(qty or 0.0) * float(px or 0.0) if float(qty or 0.0) > 0 and float(px or 0.0) > 0 else 0.0
    buffer_mult = 1.0 + max(0.0, float(DUST_MIN_VOLUME_BUFFER_PCT or 0.0))
    below_min_volume = bool(min_volume > 0.0 and float(qty or 0.0) > 0.0 and float(qty or 0.0) < (min_volume * buffer_mult))
    below_close_notional = bool(notional > 0.0 and notional < float(DUST_NOTIONAL_CLOSE_THRESHOLD_USD or 0.0))
    terminal = bool(float(qty or 0.0) <= 0.0 or below_min_volume or below_close_notional)
    return {
        'qty': float(qty or 0.0),
        'price': float(px or 0.0),
        'notional_usd': float(notional),
        'exchange_min_qty': float(min_volume),
        'below_min_volume': bool(below_min_volume),
        'below_close_notional': bool(below_close_notional),
        'terminal_dust': bool(terminal),
        'dust_notional_close_threshold_usd': float(DUST_NOTIONAL_CLOSE_THRESHOLD_USD or 0.0),
    }




def _find_recent_exit_fill(symbol: str, *, now_ts: float, lookback_sec: float = 6 * 3600.0) -> dict[str, Any] | None:
    try:
        since_ts = max(0.0, float(now_ts) - float(lookback_sec))
        raw = broker_kraken.trades_history_since(since_ts=since_ts, limit=100) or []
    except Exception:
        return None
    base = _base_asset(symbol)
    best = None
    best_ts = -1.0
    for item in raw:
        if not isinstance(item, dict):
            continue
        side = str(item.get('type') or '').lower()
        if side != 'sell':
            continue
        pair_raw = str(item.get('pair') or '')
        pair_norm = _canonicalize_trade_symbol(pair_raw) if pair_raw else ''
        if pair_norm and pair_norm != symbol:
            continue
        if not pair_norm:
            if base and base not in pair_raw:
                continue
        t = float(item.get('time') or 0.0)
        if t <= 0.0 or t > float(now_ts) + 300.0:
            continue
        if t > best_ts:
            best_ts = t
            best = {
                'txid': item.get('txid'),
                'time': t,
                'price': float(item.get('price') or 0.0),
                'qty': float(item.get('vol') or 0.0),
                'fee': float(item.get('fee') or 0.0),
                'cost': float(item.get('cost') or 0.0),
                'execution': str(item.get('ordertype') or 'reconciled_broker_state'),
                'pair': pair_norm or pair_raw,
                'raw': item,
            }
    return best


def _closed_trade_exists_for_exit_txid(txid: str, *, lookback_sec: float = 30 * 86400.0) -> bool:
    key = str(txid or '').strip()
    if not key:
        return False
    try:
        rows = trade_journal.list_closed_trades(since=max(0.0, time.time() - float(lookback_sec)), limit=2000)
    except Exception:
        return False
    for row in rows:
        if str(row.get('exit_txid') or '').strip() == key:
            return True
    return False


def _backfill_closed_trades_from_broker_history(*, now_ts: float, lookback_sec: float = 24 * 3600.0) -> dict[str, Any]:
    out = {
        'ok': True,
        'lookback_sec': float(lookback_sec),
        'backfilled': [],
        'skipped': [],
        'matched_sell_count': 0,
    }
    try:
        raw = broker_kraken.trades_history_since(since_ts=max(0.0, float(now_ts) - float(lookback_sec)), limit=250) or []
    except Exception as e:
        return {'ok': False, 'error': str(e), 'backfilled': [], 'skipped': [], 'matched_sell_count': 0}

    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        pair_raw = str(item.get('pair') or '')
        pair_norm = _canonicalize_trade_symbol(pair_raw) if pair_raw else ''
        side = str(item.get('type') or '').lower()
        if not pair_norm or side not in {'buy', 'sell'}:
            continue
        rec = {
            'txid': str(item.get('txid') or ''),
            'time': float(item.get('time') or 0.0),
            'price': float(item.get('price') or 0.0),
            'qty': float(item.get('vol') or 0.0),
            'fee': float(item.get('fee') or 0.0),
            'cost': float(item.get('cost') or 0.0),
            'side': side,
            'symbol': pair_norm,
            'raw': item,
        }
        if rec['time'] <= 0.0 or rec['qty'] <= 0.0:
            continue
        by_symbol.setdefault(pair_norm, []).append(rec)

    for symbol, items in by_symbol.items():
        try:
            live_syms = {p.get('symbol') for p in get_positions() if p.get('symbol')}
        except Exception:
            live_syms = set()
        if symbol in live_syms or symbol in (getattr(state, 'plans', {}) or {}) or trade_journal.get_open_trade(symbol):
            out['skipped'].append({'symbol': symbol, 'reason': 'live_or_open_state_present'})
            continue
        items = sorted(items, key=lambda r: (float(r.get('time') or 0.0), str(r.get('txid') or '')))
        buys = [r for r in items if r.get('side') == 'buy']
        sells = [r for r in items if r.get('side') == 'sell']
        used_buy_ids: set[str] = set()
        for sell in sells:
            sell_txid = str(sell.get('txid') or '')
            if not sell_txid:
                out['skipped'].append({'symbol': symbol, 'reason': 'sell_missing_txid'})
                continue
            if _closed_trade_exists_for_exit_txid(sell_txid, lookback_sec=max(lookback_sec, 30 * 86400.0)):
                out['skipped'].append({'symbol': symbol, 'txid': sell_txid, 'reason': 'already_journaled'})
                continue
            candidates = [b for b in buys if float(b.get('time') or 0.0) <= float(sell.get('time') or 0.0) and str(b.get('txid') or '') not in used_buy_ids]
            if not candidates:
                out['skipped'].append({'symbol': symbol, 'txid': sell_txid, 'reason': 'no_prior_buy_match'})
                continue
            buy = candidates[-1]
            used_buy_ids.add(str(buy.get('txid') or ''))
            sell_qty = float(sell.get('qty') or 0.0)
            buy_qty = float(buy.get('qty') or 0.0)
            entry_qty = min(sell_qty, buy_qty) if buy_qty > 0.0 and sell_qty > 0.0 else max(sell_qty, buy_qty)
            if entry_qty <= 0.0:
                out['skipped'].append({'symbol': symbol, 'txid': sell_txid, 'reason': 'invalid_qty_match'})
                continue
            buy_ratio = min(1.0, entry_qty / buy_qty) if buy_qty > 0 else 1.0
            sell_ratio = min(1.0, entry_qty / sell_qty) if sell_qty > 0 else 1.0
            open_payload = {
                'symbol': symbol,
                'opened_ts': float(buy.get('time') or now_ts),
                'strategy': 'adopted',
                'source': 'broker_trade_history_backfill',
                'signal_name': None,
                'signal_id': None,
                'req_id': f'broker_backfill:{symbol}:{sell_txid}',
                'entry_txid': str(buy.get('txid') or ''),
                'entry_execution': str((buy.get('raw') or {}).get('ordertype') or 'broker_trade_history_backfill'),
                'entry_price': float(buy.get('price') or 0.0),
                'entry_qty': float(entry_qty),
                'entry_cost': float((buy.get('cost') or 0.0) * buy_ratio) if float(buy.get('cost') or 0.0) > 0 else float(entry_qty * float(buy.get('price') or 0.0)),
                'entry_fee': float((buy.get('fee') or 0.0) * buy_ratio),
                'requested_notional_usd': float(entry_qty * float(buy.get('price') or 0.0)),
                'stop_price': 0.0,
                'take_price': 0.0,
                'meta': {
                    'reconstructed_open_trade': True,
                    'reconstruction_source': 'broker_trade_history_backfill',
                    'matched_exit_txid': sell_txid,
                    'matched_entry_txid': str(buy.get('txid') or ''),
                },
            }
            try:
                trade_journal.upsert_open_trade(open_payload)
                closed = trade_journal.close_trade(symbol, {
                    'closed_ts': float(sell.get('time') or now_ts),
                    'exit_txid': sell_txid,
                    'exit_execution': str((sell.get('raw') or {}).get('ordertype') or 'broker_trade_history_backfill'),
                    'exit_price': float(sell.get('price') or 0.0),
                    'exit_qty': float(entry_qty),
                    'exit_cost': float((sell.get('cost') or 0.0) * sell_ratio) if float(sell.get('cost') or 0.0) > 0 else float(entry_qty * float(sell.get('price') or 0.0)),
                    'exit_fee': float((sell.get('fee') or 0.0) * sell_ratio),
                    'exit_reason': 'reconciled_fill_backfill',
                    'meta': {
                        'reconciled_close': True,
                        'journal_backfill': True,
                        'backfill_source': 'broker_trade_history',
                        'matched_entry_txid': str(buy.get('txid') or ''),
                        'matched_exit_txid': sell_txid,
                    },
                })
            except Exception as e:
                out['skipped'].append({'symbol': symbol, 'txid': sell_txid, 'reason': f'journal_exception:{e}'})
                try:
                    trade_journal.delete_open_trade(symbol)
                except Exception:
                    pass
                continue
            if not (isinstance(closed, dict) and closed.get('ok')):
                out['skipped'].append({'symbol': symbol, 'txid': sell_txid, 'reason': 'journal_close_failed', 'result': closed})
                try:
                    trade_journal.delete_open_trade(symbol)
                except Exception:
                    pass
                continue
            out['matched_sell_count'] += 1
            out['backfilled'].append({
                'symbol': symbol,
                'entry_txid': str(buy.get('txid') or ''),
                'exit_txid': sell_txid,
                'entry_qty': float(entry_qty),
                'exit_qty': float(entry_qty),
                'entry_price': float(buy.get('price') or 0.0),
                'exit_price': float(sell.get('price') or 0.0),
                'closed_ts': float(sell.get('time') or now_ts),
            })

    try:
        telemetry_db.sync_from_trade_journal(limit=500)
    except Exception as e:
        out['telemetry_sync_error'] = str(e)
    try:
        _journal_sync_daily_realized_pnl()
    except Exception as e:
        out['pnl_sync_error'] = str(e)
    return out


def _ensure_reconciled_open_trade(symbol: str, *, plan: TradePlan | None, now_ts: float, px: float) -> dict[str, Any] | None:
    open_trade = trade_journal.get_open_trade(symbol)
    if open_trade:
        return open_trade
    if plan is None:
        return None
    entry_price = float(getattr(plan, 'entry_price', 0.0) or 0.0) or float(px or 0.0)
    requested_notional = float(getattr(plan, 'notional_usd', 0.0) or 0.0)
    entry_qty = requested_notional / entry_price if entry_price > 0 and requested_notional > 0 else 0.0
    if entry_qty <= 0.0:
        return None
    opened_ts = float(getattr(plan, 'opened_ts', 0.0) or 0.0) or float(now_ts)
    risk_snapshot = dict(getattr(plan, 'risk_snapshot', {}) or {})
    lifecycle_policy = dict(risk_snapshot.get('lifecycle_policy') or {})
    meta = {
        'reconstructed_open_trade': True,
        'reconstruction_source': 'plan_state',
        'plan_origin': _plan_origin(plan),
        'plan_policy_source': _plan_policy_source(plan),
        'max_hold_sec': int(_effective_plan_max_hold_sec(plan) or 0),
        'lifecycle_policy': lifecycle_policy,
    }
    trade_journal.upsert_open_trade({
        'symbol': symbol,
        'opened_ts': float(opened_ts),
        'strategy': str(getattr(plan, 'strategy', '') or 'adopted'),
        'source': 'reconciled_plan_state',
        'signal_name': None,
        'signal_id': None,
        'req_id': getattr(plan, 'trade_plan_id', None) or getattr(plan, 'position_id', None) or f'reconciled:{symbol}:{int(opened_ts)}',
        'entry_txid': None,
        'entry_execution': 'reconstructed_plan_state',
        'entry_price': float(entry_price),
        'entry_qty': float(entry_qty),
        'entry_cost': float(entry_qty * entry_price),
        'entry_fee': 0.0,
        'requested_notional_usd': float(requested_notional or (entry_qty * entry_price)),
        'stop_price': float(getattr(plan, 'stop_price', 0.0) or 0.0),
        'take_price': float(getattr(plan, 'take_price', 0.0) or 0.0),
        'meta': meta,
    })
    return trade_journal.get_open_trade(symbol)

def _reconciled_close_trade(symbol: str, *, plan: TradePlan | None, reason: str, px: float, now_ts: float, post_reconcile: dict | None = None, broker_result: dict | None = None, classified: dict | None = None) -> dict[str, Any] | None:
    post = dict(post_reconcile or {})
    open_trade = _ensure_reconciled_open_trade(symbol, plan=plan, now_ts=float(now_ts), px=float(px or post.get('price') or 0.0))
    if not open_trade and not plan:
        return None

    remaining_qty = float(post.get('economic_qty', 0.0) or 0.0)
    dust = _dust_snapshot(symbol, qty=remaining_qty, px=float(px or post.get('price') or 0.0))
    qualifies = bool(post.get('qualifies_as_position'))
    if not (remaining_qty <= 0.0 or (not qualifies and dust.get('terminal_dust'))):
        return None

    entry_qty_total = 0.0
    entry_px = 0.0
    requested_notional = 0.0
    if open_trade:
        entry_qty_total = float(open_trade.get('entry_qty') or 0.0)
        entry_px = float(open_trade.get('entry_price') or 0.0)
        requested_notional = float(open_trade.get('requested_notional_usd') or 0.0)
    if entry_qty_total <= 0.0 and plan is not None:
        entry_qty_total = float(getattr(plan, 'notional_usd', 0.0) or 0.0) / float(getattr(plan, 'entry_price', 0.0) or 1.0) if float(getattr(plan, 'entry_price', 0.0) or 0.0) > 0 else 0.0
        entry_px = float(getattr(plan, 'entry_price', 0.0) or 0.0)
        requested_notional = float(getattr(plan, 'notional_usd', 0.0) or 0.0)

    if entry_qty_total <= 0.0:
        return None

    sold_qty = max(0.0, float(entry_qty_total) - max(0.0, float(remaining_qty)))
    force_terminal = bool(dust.get('terminal_dust'))
    if sold_qty <= 0.0 and not force_terminal:
        return None

    exit_qty = float(entry_qty_total if force_terminal else sold_qty)
    exit_price = float(px or entry_px or 0.0)
    approx_exit_cost = float(exit_qty * exit_price) if exit_qty > 0 and exit_price > 0 else float(requested_notional or 0.0)
    exit_fee = 0.0
    if isinstance(classified, dict):
        exit_fee = float(classified.get('fees_usd') or 0.0)
    matched_fill = _find_recent_exit_fill(symbol, now_ts=float(now_ts))
    if matched_fill:
        exit_price = float(matched_fill.get('price') or exit_price or 0.0)
        exit_qty = float(matched_fill.get('qty') or exit_qty or 0.0)
        approx_exit_cost = float(matched_fill.get('cost') or (exit_qty * exit_price) or approx_exit_cost)
        exit_fee = float(matched_fill.get('fee') or exit_fee or 0.0)
    close_mode = 'reconciled_fill' if sold_qty > 0 else 'terminal_dust'
    close_meta = {
        'reconciled_close': True,
        'terminal_dust': bool(force_terminal),
        'remaining_qty': float(remaining_qty),
        'remaining_notional_usd': float(dust.get('notional_usd') or 0.0),
        'dust': dust,
        'broker_result': broker_result or {},
        'classified': classified or {},
        'close_mode': close_mode,
        'matched_fill': matched_fill or {},
    }
    out = None
    if open_trade:
        try:
            out = trade_journal.close_trade(symbol, {
                'closed_ts': float(matched_fill.get('time') or now_ts),
                'exit_txid': ((classified or {}).get('broker_txid') if isinstance(classified, dict) else None) or (matched_fill or {}).get('txid'),
                'exit_execution': ((classified or {}).get('execution') if isinstance(classified, dict) else None) or (matched_fill or {}).get('execution') or 'reconciled_broker_state',
                'exit_price': float(exit_price),
                'exit_qty': float(exit_qty),
                'exit_cost': float(approx_exit_cost),
                'exit_fee': float(exit_fee),
                'exit_reason': close_mode,
                'meta': close_meta,
            })
        except Exception:
            out = None
        if force_terminal:
            try:
                trade_journal.delete_open_trade(symbol)
            except Exception:
                pass
        try:
            telemetry_db.sync_from_trade_journal(limit=500)
        except Exception:
            pass
        try:
            _journal_sync_daily_realized_pnl()
        except Exception:
            pass

    try:
        state.clear_pending_exit(symbol)
    except Exception:
        pass
    try:
        state.remove_plan(symbol)
    except Exception:
        pass

    return {
        'symbol': symbol,
        'closed': True,
        'close_reason': close_mode,
        'force_terminal': bool(force_terminal),
        'sold_qty_estimate': float(sold_qty),
        'remaining_qty': float(remaining_qty),
        'dust': dust,
        'matched_fill': matched_fill or {},
        'journal_close': out,
        'journal_written': bool(out and out.get('ok')),
    }


def _exit_broker_reconcile_snapshot(symbol: str) -> dict[str, Any]:
    snap = _merged_balances_snapshot() or {}
    economic = dict(snap.get('economic_balances') or snap.get('canonical_merged') or {})
    base = _base_asset(symbol)
    qty = float(economic.get(base, 0.0) or 0.0)
    try:
        px = float(_last_price(symbol) or 0.0)
    except Exception:
        px = 0.0
    notional = float(qty * px) if qty > 0 and px > 0 else 0.0
    qualifies = bool(notional >= float(getattr(settings, 'min_position_notional_usd', 0.0) or 0.0)) if px > 0 else False
    return {
        'symbol': symbol,
        'base_asset': base,
        'economic_qty': float(qty),
        'price': float(px),
        'notional_usd': float(notional),
        'qualifies_as_position': bool(qualifies),
        'has_plan': bool(symbol in (getattr(state, 'plans', {}) or {})),
        'pending_exit': bool(symbol in (getattr(state, 'pending_exits', {}) or {})),
        'open_position_symbols': sorted(list(getattr(state, 'open_positions', []) or [])),
        'raw_ok': bool(snap.get('raw_ok', True)),
        'parsed_error': snap.get('parsed_error'),
        'positions_error': snap.get('positions_error'),
        'balance_sources': list((snap.get('economic_balance_sources') or snap.get('canonical_sources') or {}).get(base, [])),
    }


def _compact_exit_result(res: Any) -> Any:
    if not isinstance(res, dict):
        return res
    recon = dict(res.get('reconciled') or {})
    result = dict(res.get('result') or {})
    return {
        'ok': bool(res.get('ok')),
        'filled': bool(res.get('filled')),
        'execution': res.get('execution'),
        'pair': res.get('pair'),
        'side': res.get('side'),
        'notional': res.get('notional'),
        'volume': res.get('volume'),
        'txid': res.get('txid'),
        'descr': res.get('descr'),
        'status': res.get('status'),
        'vol_exec': res.get('vol_exec'),
        'limit_price': res.get('limit_price'),
        'error': res.get('error'),
        'reconciled': {
            'filled': bool(recon.get('filled')),
            'status': recon.get('status'),
            'avg_price': recon.get('avg_price'),
            'cost': recon.get('cost'),
            'fee': recon.get('fee'),
            'vol_exec': recon.get('vol_exec'),
        } if recon else {},
        'result': {
            'txid': result.get('txid'),
            'descr': result.get('descr'),
            'error': result.get('error'),
        } if result else {},
    }


def _record_exit_execution_truth(payload: dict[str, Any]) -> None:
    snap = dict(payload or {})
    snap.setdefault('utc', utc_now_iso())
    snap.setdefault('build', PATCH_BUILD)
    try:
        state.record_exit_execution(snap)
    except Exception:
        pass


@app.post("/worker/route_truth")
def worker_route_truth(payload: WorkerRouteTruthPayload):
    ok_secret, reason = _require_worker_secret(getattr(payload, 'worker_secret', None))
    kind = str(getattr(payload, 'worker_kind', '') or getattr(payload, 'heartbeat_kind', '') or 'unknown').strip().lower()
    if kind not in ('scan', 'exit'):
        raise HTTPException(status_code=400, detail='invalid worker kind')
    route_payload = _route_truth_from_payload(payload, kind)
    route_payload['auth'] = 'ok' if ok_secret else 'failed'
    route_payload['ingested_utc'] = utc_now_iso()
    if not ok_secret:
        route_payload['phase'] = 'auth_failed'
        route_payload['ok'] = False
        route_payload['error'] = reason
    if kind == 'scan':
        state.set_last_scan_route_truth(route_payload)
    else:
        state.set_last_exit_route_truth(route_payload)
    if not ok_secret:
        raise HTTPException(status_code=401, detail=reason)
    return {'ok': True, 'utc': utc_now_iso(), 'worker_kind': kind, 'phase': route_payload.get('phase'), 'status_code': route_payload.get('status_code')}


@app.post("/worker/exit")
def worker_exit(payload: WorkerExitPayload):
    started_at = time.time()
    try:
        _record_worker_status('exit', phase='started', payload=payload, ok=True, extra={'auth': 'pending'}, started_at=started_at)
    except Exception:
        pass
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            try:
                _record_worker_status('exit', phase='failure', payload=payload, ok=False, error='invalid worker secret', extra={'auth': 'failed'}, started_at=started_at)
            except Exception:
                pass
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
        bal = _merged_balances_by_asset()
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
            adopted_policy = _adopted_lifecycle_policy()
            plan_new = TradePlan(
                symbol=symbol,
                side="buy",
                notional_usd=float(pos_notional),
                entry_price=float(entry_px),
                stop_price=float(stop_px),
                take_price=float(take_px),
                strategy="adopted",
                opened_ts=now.timestamp(),
                max_hold_sec=int(adopted_policy.get('max_hold_sec', 0) or 0),
                risk_snapshot={'lifecycle_policy': adopted_policy},
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
                strategy = str(getattr(plan, "strategy", "") or "").strip().lower()
                if strategy == "tc0":
                    entry_px = float(getattr(plan, "entry_price", 0.0) or 0.0)
                    plan_notional = float(getattr(plan, "notional_usd", 0.0) or 0.0)
                    est_qty = (plan_notional / entry_px) if (entry_px > 0 and plan_notional > 0) else 0.0
                    gross_now = ((float(px) - entry_px) * est_qty) if est_qty > 0 else 0.0
                    fee_bps = float(getattr(settings, "entry_fee_bps", 0.0) or 0.0) + float(getattr(settings, "exit_fee_bps", 0.0) or 0.0)
                    est_fee_usd = (plan_notional * fee_bps / 10000.0) if (plan_notional > 0 and fee_bps > 0) else 0.0
                    extended_time_ok = age_sec >= float(max_hold_sec + grace + TC0_TIME_EXIT_EXTENSION_SEC)
                    eligible["tc0_est_gross_pnl_usd"] = round(gross_now, 6)
                    eligible["tc0_est_roundtrip_fee_usd"] = round(est_fee_usd, 6)
                    eligible["tc0_time_exit_extension_sec"] = int(TC0_TIME_EXIT_EXTENSION_SEC)
                    eligible["tc0_time_exit_fee_mult"] = float(TC0_TIME_EXIT_MIN_FEE_MULT)
                    eligible["tc0_extended_time_exit"] = bool(extended_time_ok)
                    if (not extended_time_ok) and gross_now < (est_fee_usd * float(TC0_TIME_EXIT_MIN_FEE_MULT)):
                        eligible["eligible_time"] = False
                        eligible["tc0_time_exit_deferred"] = True
                        return None, eligible
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
                # No holding: reconcile and clear stale state.
                reconciled_close = _reconciled_close_trade(symbol, plan=state.plans.get(symbol), reason='broker_flat', px=float(px), now_ts=now.timestamp(), post_reconcile=_exit_broker_reconcile_snapshot(symbol), broker_result={}, classified={'state': 'reconciled_flat'})
                if diagnostics and reconciled_close:
                    evaluations.append({
                        'symbol': symbol,
                        'decision': None,
                        'skip_reason': 'broker_flat_reconciled_close',
                        'reconciled_close': reconciled_close,
                    })
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
            plan, _ = _normalize_plan_lifecycle_policy(plan, now_ts=now.timestamp(), persist=True)
            if not plan:
                continue

            # Phase 3: derived metrics
            entry_px = float(getattr(plan, "entry_price", 0.0) or 0.0) or float(px)
            opened_ts = float(getattr(plan, "opened_ts", 0.0) or 0.0)
            age_sec = (now.timestamp() - opened_ts) if opened_ts > 0 else 0.0
            plan_max = int(getattr(plan, "max_hold_sec", 0) or 0)
            max_hold = int(_effective_plan_max_hold_sec(plan) or max_hold_default)
            lifecycle_policy = dict((getattr(plan, 'risk_snapshot', {}) or {}).get('lifecycle_policy') or {})

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
                    "max_hold_sec": int(max_hold),
                    "plan_origin": _plan_origin(plan),
                    "plan_policy_source": str(lifecycle_policy.get('policy_source') or _plan_policy_source(plan)),
                    "time_exit_enabled": bool(lifecycle_policy.get('time_exit_enabled', max_hold > 0)),
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

            pre_reconcile = _exit_broker_reconcile_snapshot(symbol)
            exit_attempt_base = {
                'phase': 'decision_ready',
                'symbol': symbol,
                'strategy': plan.strategy,
                'reason': reason,
                'decision_price': float(px),
                'entry_price': float(entry_px),
                'qty': float(qty),
                'age_sec': round(age_sec, 3),
                'max_hold_sec': int(max_hold),
                'plan_origin': _plan_origin(plan),
                'plan_policy_source': _plan_policy_source(plan),
                'pending_exit_before': bool(state.has_pending_exit(symbol, PENDING_EXIT_TTL_SEC)),
                'pre_reconcile': pre_reconcile,
            }

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
            lifecycle_db.upsert_broker_order({
                "broker_order_id": exit_intent_id, "intent_id": exit_intent_id, "trade_plan_id": getattr(plan, "trade_plan_id", "") or None, "symbol": symbol,
                "strategy_id": getattr(plan, "strategy", "") or None, "side": "sell", "order_type": STOP_EXIT_MODE if reason == "stop" else settings.execution_mode,
                "lifecycle_stage": "exit", "status": "created", "client_order_key": exit_client_order_key, "requested_qty": float(qty),
                "requested_notional_usd": float(notional_exit), "limit_price": float(px), "raw_json": {"reason": reason, "dry_run": dry_run},
            })
            lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='intent_created', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=reason, payload={'client_order_key': exit_client_order_key, 'notional_exit': float(notional_exit)})
            _record_exit_execution_truth({
                **exit_attempt_base,
                'phase': 'intent_created',
                'intent_id': exit_intent_id,
                'client_order_key': exit_client_order_key,
                'dry_run': bool(dry_run),
                'requested_notional_usd': float(notional_exit),
                'requested_qty': float(qty),
                'execution_mode': STOP_EXIT_MODE if reason == 'stop' else settings.execution_mode,
                'order_lock_key': exit_lock_key,
            })
            if dry_run:
                lifecycle_db.transition_order_intent(exit_intent_id, "acknowledged", client_order_key=exit_client_order_key, last_broker_status="dry_run")
                lifecycle_db.upsert_broker_order({
                    "broker_order_id": exit_intent_id, "intent_id": exit_intent_id, "trade_plan_id": getattr(plan, "trade_plan_id", "") or None, "symbol": symbol, "strategy_id": getattr(plan, "strategy", "") or None,
                    "side": "sell", "order_type": STOP_EXIT_MODE if reason == "stop" else settings.execution_mode, "lifecycle_stage": "exit",
                    "status": "acknowledged", "client_order_key": exit_client_order_key, "requested_qty": float(qty), "requested_notional_usd": float(notional_exit),
                    "limit_price": float(px), "acknowledged_ts": time.time(), "raw_json": {"reason": reason, "dry_run": True}
                })
                lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='dry_run_acknowledged', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=reason, payload={'notional_exit': float(notional_exit)})
                res = {"ok": True, "dry_run": True, "side": "sell", "symbol": symbol, "notional": notional_exit, "reason": reason}
                _record_exit_execution_truth({
                    **exit_attempt_base,
                    'phase': 'dry_run_acknowledged',
                    'intent_id': exit_intent_id,
                    'client_order_key': exit_client_order_key,
                    'requested_notional_usd': float(notional_exit),
                    'requested_qty': float(qty),
                    'broker_result': _compact_exit_result(res),
                    'classified': {'state': 'acknowledged', 'execution': 'dry_run'},
                    'post_reconcile': _exit_broker_reconcile_snapshot(symbol),
                })
            else:
                state.set_order_lock(exit_lock_key, meta={"symbol": symbol, "side": "sell", "reason": reason, "intent_id": exit_intent_id})
                lifecycle_db.transition_order_intent(exit_intent_id, "submitted", client_order_key=exit_client_order_key, submitted_ts=time.time())
                lifecycle_db.upsert_broker_order({
                    "broker_order_id": exit_intent_id, "intent_id": exit_intent_id, "trade_plan_id": getattr(plan, "trade_plan_id", "") or None, "symbol": symbol, "strategy_id": getattr(plan, "strategy", "") or None,
                    "side": "sell", "order_type": STOP_EXIT_MODE if reason == "stop" else settings.execution_mode, "lifecycle_stage": "exit",
                    "status": "submitted", "client_order_key": exit_client_order_key, "requested_qty": float(qty), "requested_notional_usd": float(notional_exit),
                    "limit_price": float(px), "raw_json": {"reason": reason, "dry_run": False}
                })
                lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='submit_attempted', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=reason, payload={'notional_exit': float(notional_exit)})
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
                _record_exit_execution_truth({
                    **exit_attempt_base,
                    'phase': 'broker_response',
                    'intent_id': exit_intent_id,
                    'client_order_key': exit_client_order_key,
                    'requested_notional_usd': float(notional_exit),
                    'requested_qty': float(qty),
                    'broker_result': _compact_exit_result(res),
                    'classified': dict(classified_exit or {}),
                    'pending_exit_after_broker': bool(state.has_pending_exit(symbol, PENDING_EXIT_TTL_SEC)),
                })
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
                    lifecycle_db.upsert_broker_order({
                        "broker_order_id": exit_intent_id, "intent_id": exit_intent_id, "trade_plan_id": getattr(plan, "trade_plan_id", "") or None, "symbol": symbol, "strategy_id": getattr(plan, "strategy", "") or None,
                        "side": "sell", "order_type": STOP_EXIT_MODE if reason == "stop" else settings.execution_mode, "lifecycle_stage": "exit",
                        "status": str(classified_exit.get("state") or "rejected"), "client_order_key": exit_client_order_key, "broker_txid": classified_exit.get("broker_txid"),
                        "requested_qty": float(qty), "requested_notional_usd": float(notional_exit), "limit_price": float(px), "avg_fill_price": classified_exit.get("avg_fill_price"),
                        "filled_qty": classified_exit.get("filled_qty"), "fees_usd": classified_exit.get("fees_usd"), "reject_reason": str(classified_exit.get("error") or (res or {}).get("error") or "exit_order_failed"),
                        "raw_json": res or {}, "closed_ts": time.time(),
                    })
                    lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='broker_terminal', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=str(classified_exit.get("error") or (res or {}).get("error") or "exit_order_failed"), payload={'classified': classified_exit, 'response': res or {}})
                    _record_exit_execution_truth({
                        **exit_attempt_base,
                        'phase': 'broker_terminal',
                        'intent_id': exit_intent_id,
                        'client_order_key': exit_client_order_key,
                        'requested_notional_usd': float(notional_exit),
                        'requested_qty': float(qty),
                        'broker_result': _compact_exit_result(res),
                        'classified': dict(classified_exit or {}),
                        'post_reconcile': _exit_broker_reconcile_snapshot(symbol),
                        'pending_exit_after_terminal': bool(state.has_pending_exit(symbol, PENDING_EXIT_TTL_SEC)),
                    })
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
                    lifecycle_db.upsert_broker_order({
                        "broker_order_id": exit_intent_id, "intent_id": exit_intent_id, "trade_plan_id": getattr(plan, "trade_plan_id", "") or None, "symbol": symbol, "strategy_id": getattr(plan, "strategy", "") or None,
                        "side": "sell", "order_type": STOP_EXIT_MODE if reason == "stop" else settings.execution_mode, "lifecycle_stage": "exit",
                        "status": "acknowledged", "client_order_key": exit_client_order_key, "broker_txid": classified_exit.get("broker_txid"),
                        "requested_qty": float(qty), "requested_notional_usd": float(notional_exit), "limit_price": float(px),
                        "avg_fill_price": exit_px, "filled_qty": exit_qty, "fees_usd": exit_fees, "acknowledged_ts": time.time(), "raw_json": res or {},
                    })
                    lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='broker_acknowledged', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=reason, payload={'broker_txid': classified_exit.get("broker_txid"), 'avg_fill_price': exit_px, 'qty': exit_qty})
                    lifecycle_db.transition_order_intent(exit_intent_id, "filled", broker_txid=classified_exit.get("broker_txid"), filled_qty=exit_qty, avg_fill_price=exit_px, fees_usd=exit_fees, remaining_qty=0.0, last_broker_status=str(classified_exit.get("execution") or ""), raw_json=res or {})
                    lifecycle_db.upsert_broker_order({
                        "broker_order_id": exit_intent_id, "intent_id": exit_intent_id, "trade_plan_id": getattr(plan, "trade_plan_id", "") or None, "symbol": symbol, "strategy_id": getattr(plan, "strategy", "") or None,
                        "side": "sell", "order_type": STOP_EXIT_MODE if reason == "stop" else settings.execution_mode, "lifecycle_stage": "exit",
                        "status": "filled", "client_order_key": exit_client_order_key, "broker_txid": classified_exit.get("broker_txid"),
                        "requested_qty": float(qty), "requested_notional_usd": float(notional_exit), "limit_price": float(px), "avg_fill_price": exit_px,
                        "filled_qty": exit_qty, "remaining_qty": 0.0, "fees_usd": exit_fees, "acknowledged_ts": time.time(), "closed_ts": time.time(), "raw_json": res or {},
                    })
                    lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='position_closed', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=reason, payload={'broker_txid': classified_exit.get("broker_txid"), 'exit_price': exit_px, 'qty': exit_qty, 'fees_usd': exit_fees})
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
                    _record_exit_execution_truth({
                        **exit_attempt_base,
                        'phase': 'position_closed',
                        'intent_id': exit_intent_id,
                        'client_order_key': exit_client_order_key,
                        'requested_notional_usd': float(notional_exit),
                        'requested_qty': float(qty),
                        'broker_result': _compact_exit_result(res),
                        'classified': dict(classified_exit or {}),
                        'fill_metrics': {'avg_price': float(exit_px), 'qty': float(exit_qty), 'fee_usd': float(exit_fees)},
                        'journal_close': journal_close,
                        'post_reconcile': _exit_broker_reconcile_snapshot(symbol),
                        'pending_exit_after_close': bool(state.has_pending_exit(symbol, PENDING_EXIT_TTL_SEC)),
                    })
                else:
                    # Do not latch exit cooldowns or pending state if the order failed.
                    state.clear_pending_exit(symbol)
                    post_reconcile = _exit_broker_reconcile_snapshot(symbol)
                    reconciled_close = _reconciled_close_trade(
                        symbol,
                        plan=plan,
                        reason=reason,
                        px=float(px),
                        now_ts=now.timestamp(),
                        post_reconcile=post_reconcile,
                        broker_result=_compact_exit_result(res),
                        classified=dict(classified_exit or {}),
                    )
                    _record_exit_execution_truth({
                        **exit_attempt_base,
                        'phase': 'order_failed',
                        'intent_id': exit_intent_id,
                        'client_order_key': exit_client_order_key,
                        'requested_notional_usd': float(notional_exit),
                        'requested_qty': float(qty),
                        'broker_result': _compact_exit_result(res),
                        'classified': dict(classified_exit or {}),
                        'post_reconcile': post_reconcile,
                        'reconciled_close': reconciled_close,
                        'pending_exit_after_failure_clear': bool(state.has_pending_exit(symbol, PENDING_EXIT_TTL_SEC)),
                    })
                    if reconciled_close:
                        lifecycle_db.record_trade_lifecycle_event(stage='exit', event_type='reconciled_close', trade_plan_id=getattr(plan, 'trade_plan_id', '') or None, intent_id=exit_intent_id, broker_order_id=exit_intent_id, position_id=getattr(plan, 'position_id', '') or None, symbol=symbol, strategy_id=getattr(plan, 'strategy', '') or None, reason=str(reconciled_close.get('close_reason') or 'reconciled_close'), payload=reconciled_close)
                        exits.append({
                            'symbol': symbol,
                            'reason': str(reconciled_close.get('close_reason') or reason),
                            'price': float(px),
                            'age_sec': round(age_sec, 3),
                            'dry_run': bool(dry_run),
                            'result': {'ok': True, 'reconciled_close': reconciled_close},
                        })
                        continue
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
            _record_worker_status('exit', phase='success', payload=payload, ok=True, started_at=started_at, extra={
                "did_flatten": bool(did_flatten),
                "exits_count": len(exits),
                "symbols_evaluated": len(symbols),
                "stale_pending_cleared": int(stale_pending_cleared),
                "pending_exits": len(getattr(state, 'pending_exits', {}) or {}),
                "auth": 'ok',
            })
        except Exception:
            pass
        return resp

    except HTTPException:
        raise
    except Exception as e:
        try:
            _record_worker_status('exit', phase='failure', payload=payload, ok=False, error=str(e), started_at=started_at, extra={'auth': 'ok'})
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

    signals: {"tc0": bool, "rb1": bool, "tc1": bool, "cr1"?: bool, "mm1"?: bool}
    debug:   {"tc0": {...}, "rb1": {...}, "tc1": {...}, ...}
    """
    signals: dict = {}
    debug: dict = {}

    wants_tc0 = ENABLE_TC0 and (STRATEGY_MODE != "fixed" or "tc0" in ENTRY_ENGINE_STRATEGIES)
    wants_rb1 = ENABLE_RB1 and (STRATEGY_MODE != "fixed" or "rb1" in ENTRY_ENGINE_STRATEGIES)
    wants_tc1 = ENABLE_TC1 and (STRATEGY_MODE != "fixed" or "tc1" in ENTRY_ENGINE_STRATEGIES)
    wants_cr1 = ENABLE_CR1 and (STRATEGY_MODE != "fixed" or "cr1" in ENTRY_ENGINE_STRATEGIES)
    wants_mm1 = ENABLE_MM1 and (STRATEGY_MODE != "fixed" or "mm1" in ENTRY_ENGINE_STRATEGIES)

    if wants_tc0:
        tc0_fired, tc0_meta = _tc0_long_signal(symbol)
        signals["tc0"] = bool(tc0_fired)
        debug["tc0"] = tc0_meta

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


def place_entry(symbol: str, *, strategy: str, req_id: str | None = None, client_ip: str | None = None, notional: float | None = None, candidate_meta: dict | None = None):
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
        extra={"strategy": strategy, **(candidate_meta or {})},
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

    balances = _merged_balances_by_asset()
    symbols = payload.symbols or []
    selected = set([normalize_symbol(s) for s in symbols]) if symbols else None

    diagnostics: list[dict] = []
    for symbol, plan in list(state.plans.items()):
        sym = normalize_symbol(symbol)
        if selected is not None and sym not in selected:
            continue
        plan, _ = _normalize_plan_lifecycle_policy(plan, now_ts=time.time(), persist=False)

        base = _base_asset(sym)
        qty = float(balances.get(base, 0.0) or 0.0)
        px = float(_last_price(sym) or 0.0)
        notional = qty * px if (qty > 0 and px > 0) else 0.0

        stop_px = float(getattr(plan, "stop_price", 0.0) or 0.0)
        take_px = float(getattr(plan, "take_price", 0.0) or 0.0)

        opened_ts = float(getattr(plan, "opened_ts", 0.0) or 0.0)
        age_sec = (time.time() - opened_ts) if opened_ts > 0 else 0.0
        plan_max_hold = int(_effective_plan_max_hold_sec(plan) or 0)
        grace_sec = int(getattr(settings, "time_exit_grace_sec", 0) or 0)
        eligible_time_exit = bool(plan_max_hold > 0 and age_sec >= float(plan_max_hold + grace_sec))
        lifecycle_policy = dict((getattr(plan, 'risk_snapshot', {}) or {}).get('lifecycle_policy') or {})

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
        elif eligible_time_exit:
            strategy = str(getattr(plan, "strategy", "") or "").strip().lower()
            if strategy == "tc0":
                plan_notional = float(getattr(plan, "notional_usd", 0.0) or 0.0)
                est_qty = (plan_notional / float(getattr(plan, "entry_price", 0.0) or 0.0)) if (float(getattr(plan, "entry_price", 0.0) or 0.0) > 0 and plan_notional > 0) else 0.0
                gross_now = ((float(px) - float(getattr(plan, "entry_price", 0.0) or 0.0)) * est_qty) if est_qty > 0 else 0.0
                fee_bps = float(getattr(settings, "entry_fee_bps", 0.0) or 0.0) + float(getattr(settings, "exit_fee_bps", 0.0) or 0.0)
                est_fee_usd = (plan_notional * fee_bps / 10000.0) if (plan_notional > 0 and fee_bps > 0) else 0.0
                extended_time_ok = age_sec >= float(plan_max_hold + grace_sec + TC0_TIME_EXIT_EXTENSION_SEC)
                if (not extended_time_ok) and gross_now < (est_fee_usd * float(TC0_TIME_EXIT_MIN_FEE_MULT)):
                    reason = "tc0_time_exit_deferred_for_edge"
                else:
                    should_exit = True
                    reason = "time_exit"
            else:
                should_exit = True
                reason = "time_exit"
        else:
            reason = "no_exit_signal"

        exit_floor = float(getattr(settings, "exit_min_notional_usd", 0.0) or 0.0)
        target_notional = max(exit_floor, float(getattr(plan, "notional_usd", 0.0) or 0.0))
        dust = _dust_snapshot(sym, qty=float(qty), px=float(px))

        ok_to_place = should_exit
        if should_exit:
            if bool(dust.get('terminal_dust')) and not bool(notional >= target_notional * 0.98):
                ok_to_place = False
                reason = 'terminal_dust_reconciled_close'
            elif notional <= 0:
                ok_to_place = False
            elif notional < target_notional * 0.98:
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
                "origin": _plan_origin(plan),
                "policy_source": str(lifecycle_policy.get('policy_source') or _plan_policy_source(plan)),
                "time_exit_enabled": bool(lifecycle_policy.get('time_exit_enabled', plan_max_hold > 0)),
                "opened_ts": getattr(plan, "opened_ts", None),
                "max_hold_sec": plan_max_hold,
            },
            "age_sec": round(age_sec, 3),
            "eligible_time_exit": eligible_time_exit,
            "time_exit_grace_sec": grace_sec,
            "should_exit": should_exit,
            "ok_to_place": ok_to_place,
            "reason": reason,
            "target_exit_notional_usd": target_notional,
            "dust": dust,
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

    balances = _merged_balances_by_asset()

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
        adopted_policy = _adopted_lifecycle_policy()
        plan = TradePlan(
            symbol=sym,
            side="buy",
            notional_usd=float(notional),
            entry_price=float(px),
            stop_price=float(stop_px),
            take_price=float(take_px),
            strategy="adopted",
            opened_ts=time.time(),
            max_hold_sec=int(adopted_policy.get('max_hold_sec', 0) or 0),
            risk_snapshot={'lifecycle_policy': adopted_policy},
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
        "live_config": diagnostics_live_config(),
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


@app.get("/diagnostics/ops_events")
def diagnostics_ops_events(limit: int = 100, lookback_hours: int = 24):
    limit = max(1, min(int(limit), 500))
    lookback_hours = max(1, min(int(lookback_hours), 24 * 30))
    since_ts = time.time() - (float(lookback_hours) * 3600.0)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'lookback_hours': lookback_hours,
        'summary': lifecycle_db.summarize_ops_events(since_ts=since_ts),
        'events': lifecycle_db.list_recent_ops_events(since_ts=since_ts, limit=limit),
    }


@app.get("/diagnostics/admission_events")
def diagnostics_admission_events(limit: int = 100, lookback_hours: int = 24):
    limit = max(1, min(int(limit), 500))
    lookback_hours = max(1, min(int(lookback_hours), 24 * 30))
    since_ts = time.time() - (float(lookback_hours) * 3600.0)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'lookback_hours': lookback_hours,
        'summary': lifecycle_db.summarize_admission_events(since_ts=since_ts),
        'events': lifecycle_db.list_recent_admission_events(since_ts=since_ts, limit=limit),
    }


@app.get("/diagnostics/workflow_locks")
def diagnostics_workflow_locks(limit: int = 100):
    limit = max(1, min(int(limit), 500))
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'active_locks': lifecycle_db.list_active_workflow_locks(limit=limit),
    }


@app.get("/diagnostics/entry_pipeline")
def diagnostics_entry_pipeline(limit: int = 100, lookback_hours: int = 24):
    limit = max(1, min(int(limit), 500))
    lookback_hours = max(1, min(int(lookback_hours), 24 * 30))
    since_ts = time.time() - (float(lookback_hours) * 3600.0)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'lookback_hours': lookback_hours,
        'ops_summary': lifecycle_db.summarize_ops_events(since_ts=since_ts),
        'admission_summary': lifecycle_db.summarize_admission_events(since_ts=since_ts),
        'recent_ops_events': lifecycle_db.list_recent_ops_events(since_ts=since_ts, limit=limit),
        'recent_admission_events': lifecycle_db.list_recent_admission_events(since_ts=since_ts, limit=limit),
        'active_workflow_locks': lifecycle_db.list_active_workflow_locks(limit=limit),
        'terminal_reasons': lifecycle_db.summarize_terminal_reasons(since_ts=since_ts),
        'integrity': lifecycle_db.lifecycle_integrity_report(limit=min(limit, 50), stale_age_sec=max(300, int(lookback_hours * 300))),
    }


@app.get("/diagnostics/broker_orders")
def diagnostics_broker_orders(limit: int = 100, lookback_hours: int = 24):
    limit = max(1, min(int(limit), 500))
    lookback_hours = max(1, min(int(lookback_hours), 24 * 30))
    since_ts = time.time() - (float(lookback_hours) * 3600.0)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'lookback_hours': lookback_hours,
        'summary': lifecycle_db.summarize_broker_orders(since_ts=since_ts),
        'orders': lifecycle_db.list_recent_broker_orders(since_ts=since_ts, limit=limit),
    }


@app.get("/diagnostics/trade_lifecycle")
def diagnostics_trade_lifecycle(limit: int = 100, lookback_hours: int = 24):
    limit = max(1, min(int(limit), 500))
    lookback_hours = max(1, min(int(lookback_hours), 24 * 30))
    since_ts = time.time() - (float(lookback_hours) * 3600.0)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'lookback_hours': lookback_hours,
        'summary': lifecycle_db.summarize_trade_lifecycle(since_ts=since_ts),
        'events': lifecycle_db.list_recent_trade_lifecycle_events(since_ts=since_ts, limit=limit),
    }



@app.get("/diagnostics/terminal_reasons")
def diagnostics_terminal_reasons(lookback_hours: int = 24):
    lookback_hours = max(1, min(int(lookback_hours), 24 * 30))
    since_ts = time.time() - (float(lookback_hours) * 3600.0)
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'lookback_hours': lookback_hours,
        'summary': lifecycle_db.summarize_terminal_reasons(since_ts=since_ts),
    }


@app.get("/diagnostics/lifecycle_integrity")
def diagnostics_lifecycle_integrity(limit: int = 100, stale_age_sec: int = 900):
    limit = max(1, min(int(limit), 500))
    stale_age_sec = max(60, min(int(stale_age_sec), 24 * 3600))
    return {
        'ok': True,
        'utc': utc_now_iso(),
        'report': lifecycle_db.lifecycle_integrity_report(limit=limit, stale_age_sec=stale_age_sec),
    }


@app.get("/diagnostics/live_config")
def diagnostics_live_config():
    runtime_order = []
    for s in ENTRY_ENGINE_STRATEGIES_LIST:
        if s == "tc0" and ENABLE_TC0:
            runtime_order.append(s)
        elif s == "rb1" and ENABLE_RB1:
            runtime_order.append(s)
        elif s == "tc1" and ENABLE_TC1:
            runtime_order.append(s)
        elif s == "cr1" and ENABLE_CR1:
            runtime_order.append(s)
        elif s == "mm1" and ENABLE_MM1:
            runtime_order.append(s)
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "raw_entry_engine_strategies": [s.strip().lower() for s in os.getenv("ENTRY_ENGINE_STRATEGIES", "tc0,tc1").split(",") if s.strip()],
        "entry_engine_strategies": runtime_order,
        "strategy_mode": STRATEGY_MODE,
        "enable_tc0": bool(ENABLE_TC0),
        "enable_rb1": bool(ENABLE_RB1),
        "enable_tc1": bool(ENABLE_TC1),
        "enable_cr1": bool(ENABLE_CR1),
        "enable_mm1": bool(ENABLE_MM1),
        "tc0_params": {
            "lookback_bars": int(TC0_LOOKBACK_BARS),
            "breakout_buffer_pct": float(TC0_BREAKOUT_BUFFER_PCT),
            "atr_len": int(TC0_ATR_LEN),
            "min_atr_pct": float(TC0_MIN_ATR_PCT),
            "require_vwap": bool(TC0_REQUIRE_VWAP),
            "vwap_lookback_bars": int(TC0_VWAP_LOOKBACK_BARS),
            "max_spread_pct": float(TC0_MAX_SPREAD_PCT),
            "max_hold_sec": int(TC0_MAX_HOLD_SEC),
        },
    }


@app.get("/diagnostics/persistence_self_test")
def diagnostics_persistence_self_test():
    import os
    import sqlite3
    import tempfile
    tmp = tempfile.NamedTemporaryFile(prefix="patch51-", suffix=".sqlite3", delete=False)
    path = tmp.name
    tmp.close()
    old_path = os.environ.get("LIFECYCLE_DB_PATH")
    try:
        os.environ["LIFECYCLE_DB_PATH"] = path
        lifecycle_db.ensure_schema()
        lifecycle_db.upsert_order_intent({
            "intent_id": "selftest-intent",
            "symbol": "BTC/USD",
            "side": "buy",
            "order_type": "limit_aggressive",
            "strategy_id": "tc0",
            "state": "created",
        })
        lifecycle_db.upsert_broker_order({
            "broker_order_id": "selftest-broker-order",
            "intent_id": "selftest-intent",
            "symbol": "BTC/USD",
            "strategy_id": "tc0",
            "side": "buy",
            "order_type": "limit_aggressive",
            "lifecycle_stage": "pre_submit",
            "status": "created",
        })
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        irow = con.execute("SELECT intent_id, broker_txid FROM order_intents WHERE intent_id = ?", ("selftest-intent",)).fetchone()
        brow = con.execute("SELECT broker_order_id, broker_txid FROM broker_orders WHERE broker_order_id = ?", ("selftest-broker-order",)).fetchone()
        con.close()
        return {
            "ok": True,
            "build": PATCH_BUILD,
            "intent": dict(irow) if irow else None,
            "broker_order": dict(brow) if brow else None,
        }
    finally:
        if old_path is None:
            os.environ.pop("LIFECYCLE_DB_PATH", None)
        else:
            os.environ["LIFECYCLE_DB_PATH"] = old_path
        try:
            os.unlink(path)
        except Exception:
            pass


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
        "entry_pipeline": {
            "signal_fingerprint_ttl_sec": int(getattr(settings, 'signal_fingerprint_ttl_sec', 0) or 0),
            "entry_failure_cooldown_sec": int(_entry_failure_cooldown_sec()),
            "workflow_lock_ttl_sec": int(getattr(settings, 'workflow_lock_ttl_sec', 0) or 0),
        },
        "live_config": diagnostics_live_config(),
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
        "entry_pipeline": {
            "signal_fingerprint_ttl_sec": int(getattr(settings, 'signal_fingerprint_ttl_sec', 0) or 0),
            "entry_failure_cooldown_sec": int(_entry_failure_cooldown_sec()),
            "workflow_lock_ttl_sec": int(getattr(settings, 'workflow_lock_ttl_sec', 0) or 0),
        },
        "live_config": diagnostics_live_config(),
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
@app.get("/diagnostics/summary/")
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
        "broker_orders": lifecycle_db.list_rows("broker_orders", limit=limit, order_by="updated_ts DESC"),
        "trade_lifecycle_events": lifecycle_db.list_rows("trade_lifecycle_events", limit=limit, order_by="created_ts DESC"),
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
        "broker_orders": lifecycle_db.list_rows("broker_orders", limit=limit, order_by="updated_ts DESC"),
        "trade_lifecycle_events": lifecycle_db.list_rows("trade_lifecycle_events", limit=limit, order_by="created_ts DESC"),
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
    Scan the current universe for entry signals (TC0/RB1/TC1), optionally executing orders.

    This endpoint ALWAYS returns a rich diagnostics payload so you can see exactly why
    you got 0 entries (no signals, already in position, symbol not allowed, etc.).
    """
    started_at = time.time()
    _record_worker_status('scan', phase='started', payload=payload, ok=True, extra={'auth': 'pending'}, started_at=started_at)
    ok, reason = _require_worker_secret(payload.worker_secret)
    if not ok:
        try:
            _record_worker_status('scan', phase='failure', payload=payload, ok=False, error=reason, extra={'auth': 'failed'}, started_at=started_at)
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
            scan_ctx = _build_admission_context(symbol=sym, strategy='scanner', signal_name=None, signal_id=_scanner_signal_id(sym, 'scanner'), source='scan_entries', extra={'regime_quiet': bool(regime_quiet)}, px_hint=None)
            _record_rejected_admission(scan_ctx, 'position_already_open', payload=d)
            per_symbol[sym] = d
            continue

        if open_positions_count >= MAX_OPEN_POSITIONS:
            d["skip"].append("max_open_positions_reached")
            d["max_open_positions"] = MAX_OPEN_POSITIONS
            scan_ctx = _build_admission_context(symbol=sym, strategy='scanner', signal_name=None, signal_id=_scanner_signal_id(sym, 'scanner'), source='scan_entries', extra={'regime_quiet': bool(regime_quiet)}, px_hint=None)
            _record_rejected_admission(scan_ctx, 'rejected_max_open_positions', payload=d)
            per_symbol[sym] = d
            continue

        try:
            fired, sig_debug = _entry_signals_for_symbol(sym, regime_quiet=bool(regime_quiet))  # (signals, debug)
        except Exception as e:
            d["skip"].append(f"signal_error:{type(e).__name__}")
            d["signal_error"] = str(e)
            scan_ctx = _build_admission_context(symbol=sym, strategy='scanner', signal_name=None, signal_id=_scanner_signal_id(sym, 'scanner'), source='scan_entries', extra={'regime_quiet': bool(regime_quiet)}, px_hint=None)
            _record_rejected_admission(scan_ctx, 'signal_error', payload=d | {'error': str(e)})
            per_symbol[sym] = d
            continue

        d["signals"] = fired
        d["signal_debug"] = sig_debug

        fired_strats = [k for k, v in fired.items() if v]
        if not fired_strats:
            d["skip"].append("no_signal")
            scan_ctx = _build_admission_context(symbol=sym, strategy='scanner', signal_name=None, signal_id=_scanner_signal_id(sym, 'scanner'), source='scan_entries', extra={'regime_quiet': bool(regime_quiet), 'signal_meta': sig_debug.get('rb1') or {}}, px_hint=None)
            _record_rejected_admission(scan_ctx, 'rejected_no_signal', payload=d)
            per_symbol[sym] = d
            continue

        # Strategy preference:
        # - fixed: preserve ENTRY_ENGINE_STRATEGIES env order and ignore all others
        # - auto: in quiet regime, prefer MM1/CR1, then TC0/TC1/RB1; otherwise TC0 over TC1 over RB1
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
            elif fired.get("tc0"):
                strategy = "tc0"
            elif fired.get("tc1"):
                strategy = "tc1"
            elif fired.get("rb1"):
                strategy = "rb1"
            else:
                strategy = fired_strats[0]
        else:
            if fired.get("tc0"):
                strategy = "tc0"
            elif fired.get("tc1"):
                strategy = "tc1"
            elif fired.get("rb1"):
                strategy = "rb1"
            else:
                strategy = fired_strats[0]
        d["eligible"] = True
        d["chosen_strategy"] = strategy
        rank = _rank_candidate(sym, strategy, sig_debug)
        d["rank"] = rank
        candidates.append({"symbol": sym, "strategy": strategy, "score": float(rank.get("score") or 0.0), "rank": rank, "signal_meta": dict((sig_debug or {}).get(strategy) or {}), "regime_quiet": bool(regime_quiet)})
        per_symbol[sym] = d

    # Apply per-scan entry cap *after* we have all candidates so we can prioritize.
    # If ENABLE_CANDIDATE_RANKING, we also sort by a simple quality score within the same
    # strategy-priority bucket (higher score = better).
    if MAX_ENTRIES_PER_SCAN > 0 and len(candidates) > MAX_ENTRIES_PER_SCAN:
        def _pri(s: str) -> int:
            s = (s or "").lower()
            if bool(regime_quiet) and STRATEGY_MODE != "legacy":
                # Quiet regime: prioritize inventory-friendly / maker-capture first.
                return {"mm1": 0, "cr1": 1, "tc0": 2, "tc1": 3, "rb1": 4}.get(s, 9)
            # Non-quiet: prioritize directional edge.
            return {"tc0": 0, "tc1": 1, "rb1": 2, "cr1": 3, "mm1": 4}.get(s, 9)

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
            loser_ctx = _build_admission_context(symbol=sym, strategy=str(c.get('strategy') or ''), signal_name=str(c.get('strategy') or ''), signal_id=_scanner_signal_id(sym, str(c.get('strategy') or 'scanner')), source='scan_entries', extra={'rank': c.get('rank') or {}, 'signal_meta': c.get('signal_meta') or {}, 'regime_quiet': c.get('regime_quiet'), 'ranking_score': c.get('score')}, px_hint=None)
            _record_rejected_admission(loser_ctx, 'rejected_max_entries_per_scan', payload=d | {'score': c.get('score')})
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
            scan_ctx = _build_admission_context(symbol=sym, strategy=strategy, signal_name=strategy, signal_id=_scanner_signal_id(sym, strategy), source='scan_entries', extra={'rank': c.get('rank') or {}, 'signal_meta': c.get('signal_meta') or {}, 'regime_quiet': c.get('regime_quiet'), 'ranking_score': score, 'spread_pct': spread_pct}, px_hint=None)
            _record_rejected_admission(scan_ctx, 'spread_too_wide', payload=meta2)
            results.append({"symbol": sym, "strategy": strategy, "ok": False, "status": "skipped", "reason": "rejected_spread_too_wide", "meta": meta2})
            # Also annotate per_symbol so diagnostics show why it didn't execute.
            d = per_symbol.get(sym) or {"symbol": sym, "skip": []}
            d.setdefault("skip", [])
            if "spread_too_wide" not in d["skip"]:
                d["skip"].append("spread_too_wide")
            d["eligible"] = False
            per_symbol[sym] = d
            continue

        ok2, reason2, meta2 = place_entry(sym, strategy=strategy, candidate_meta={'rank': c.get('rank') or {}, 'signal_meta': c.get('signal_meta') or {}, 'regime_quiet': c.get('regime_quiet'), 'ranking_score': c.get('score'), 'spread_pct': spread_pct})
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
        _record_worker_status('scan', phase='success', payload=payload, ok=True, started_at=started_at, extra={
            "scanner_ok": bool(scanner_ok),
            "scanner_reason": scanner_reason,
            "universe_count": len(universe),
            "results_count": len(results),
            "candidate_count": len(candidates),
            "dry_run": bool(payload.dry_run),
            "auth": 'ok',
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
                "enable_tc0": ENABLE_TC0,
                "enable_rb1": ENABLE_RB1,
                "enable_tc1": ENABLE_TC1,
                "enable_cr1": ENABLE_CR1,
                "enable_mm1": ENABLE_MM1,
                "tc0_params": {
                    "lookback_bars": TC0_LOOKBACK_BARS,
                    "breakout_buffer_pct": TC0_BREAKOUT_BUFFER_PCT,
                    "atr_len": TC0_ATR_LEN,
                    "min_atr_pct": TC0_MIN_ATR_PCT,
                    "require_vwap": TC0_REQUIRE_VWAP,
                    "vwap_lookback_bars": TC0_VWAP_LOOKBACK_BARS,
                    "max_spread_pct": TC0_MAX_SPREAD_PCT,
                    "max_hold_sec": TC0_MAX_HOLD_SEC,
                },
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

@app.get("/build")
def build_info_endpoint():
    return {**PATCH_BUILD}


@app.get("/runtime")
def runtime_endpoint():
    data = diagnostics_runtime()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": PATCH_BUILD.get("release_stage_configured"),
        },
        "runtime": data,
        "compatibility": _compatibility_snapshot(),
    }




@app.get("/compatibility")
def compatibility_endpoint():
    snapshot = _compatibility_snapshot()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": PATCH_BUILD.get("release_stage_configured"),
        },
        "compatibility": snapshot,
    }




@app.get("/diagnostics/live_promotion_guardrails")
def diagnostics_live_promotion_guardrails():
    return _live_promotion_guardrails_snapshot()


@app.get("/diagnostics/live_readiness_summary")
def diagnostics_live_readiness_summary():
    snapshot = _live_promotion_guardrails_snapshot()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": snapshot.get("service"),
        "promotion_ready": snapshot.get("promotion_ready"),
        "promotion_blockers": snapshot.get("promotion_blockers") or [],
        "checks": snapshot.get("checks") or {},
        "btc_only_live_alignment": snapshot.get("btc_only_live_alignment") or {},
        "compatibility_reason": snapshot.get("compatibility_reason"),
        "live_validation": snapshot.get("live_validation") or {},
    }

@app.get("/diagnostics/btc_only_live_alignment")
def diagnostics_btc_only_live_alignment():
    snapshot = _compatibility_snapshot()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": PATCH_BUILD.get("release_stage_configured"),
        },
        "btc_only_live_alignment": snapshot.get("btc_only_live_alignment"),
        "compatibility_reason": snapshot.get("compatibility_reason"),
        "contract_compatible": snapshot.get("contract_compatible"),
    }


@app.get("/ready")
def ready_endpoint():
    startup = _startup_self_check(rerun=False, apply=None)
    gate = _pretrade_health_gate_summary(rerun_startup_check=False)
    compatibility = _compatibility_snapshot()
    startup_ok = bool(startup.get("ok")) and not bool((startup.get("startup_self_check") or {}).get("critical"))
    gate_open = bool(gate.get("gate_open"))
    scanner_ready = bool(compatibility.get("scanner_ok"))
    issues = list(gate.get("violations") or [])
    if not startup_ok and "startup_self_check_not_ok" not in issues:
        issues.append("startup_self_check_not_ok")
    for blocker in list(compatibility.get("blockers") or []):
        if blocker not in issues:
            issues.append(blocker)
    ready = bool(startup_ok and gate_open and bool(compatibility.get("contract_compatible")))
    return {
        "ok": True,
        "ready": ready,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": PATCH_BUILD.get("release_stage_configured"),
        },
        "issues": issues,
        "startup_self_check_ok": startup_ok,
        "pretrade_gate_open": gate_open,
        "scanner_ready": scanner_ready,
        "scanner_soft_allow": bool(SCANNER_SOFT_ALLOW),
        "scanner_reason": compatibility.get("compatibility_reason"),
        "scanner_last_refresh_utc": ((compatibility.get("scanner_contract") or {}).get("last_refresh_utc")),
        "worker_health": gate.get("worker_health"),
        "worker_route_truth": gate.get("worker_route_truth"),
        "startup_self_check": startup,
        "pretrade_health_gate": gate,
        "compatibility": compatibility,
    }


@app.get("/diagnostics/scanner_coordination")
def diagnostics_scanner_coordination(lookback_sec: int = 900, limit: int = 50):
    coordination = _scanner_coordination_snapshot(lookback_sec=lookback_sec, limit=limit)
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": PATCH_BUILD.get("release_stage_configured"),
        },
        "coordination": coordination,
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _serialize_recent_trade(row: dict[str, Any]) -> dict[str, Any]:
    meta = {}
    try:
        meta = json.loads(str(row.get("meta_json") or "{}")) if isinstance(row.get("meta_json"), str) else dict(row.get("meta_json") or {})
    except Exception:
        meta = {}
    source_meta = {}
    try:
        source_meta = meta.get("source_meta") or {}
        if not isinstance(source_meta, dict):
            source_meta = {}
    except Exception:
        source_meta = {}

    entry_price = row.get("entry_fill_price")
    if entry_price is None:
        entry_price = source_meta.get("entry_price")
    exit_price = row.get("exit_fill_price")
    if exit_price is None:
        exit_price = source_meta.get("exit_price")
    qty = row.get("exit_qty")
    if qty is None:
        qty = row.get("entry_qty")
    trade_id = row.get("trade_key") or row.get("trade_id")

    return {
        "trade_id": trade_id,
        "symbol": _canonicalize_trade_symbol(str(row.get("symbol") or "")),
        "strategy": row.get("strategy"),
        "entry_ts": row.get("opened_ts"),
        "closed_ts": row.get("closed_ts"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "qty": qty,
        "fees_total": row.get("fees_total"),
        "gross_pnl_usd": row.get("gross_pnl_usd"),
        "net_pnl_usd": row.get("net_pnl_usd"),
        "exit_reason": row.get("exit_reason"),
        "clean_trade": bool(row.get("clean_trade")),
        "max_realized_slippage_bps": row.get("max_realized_slippage_bps"),
        "alert_flags_json": row.get("alert_flags_json"),
        "updated_utc": row.get("updated_utc"),
    }


def _performance_snapshot(days: float = 30.0, recent_limit: int = 25) -> dict[str, Any]:
    positions = get_positions()
    open_notional = sum(_safe_float(p.get("notional_usd")) for p in positions)
    account_truth = _account_truth_snapshot()
    realized_today = _journal_sync_daily_realized_pnl()

    try:
        recent_raw = telemetry_db.recent_trades(limit=max(1, int(recent_limit)))
    except Exception:
        recent_raw = []
    recent = [_serialize_recent_trade(dict(r)) for r in (recent_raw or [])]

    try:
        journal_1d = trade_journal.summary(days=1.0)
    except Exception:
        journal_1d = {"ok": False}
    try:
        journal_7d = trade_journal.summary(days=7.0)
    except Exception:
        journal_7d = {"ok": False}
    try:
        journal_window = trade_journal.summary(days=float(days))
    except Exception:
        journal_window = {"ok": False, "days": float(days)}
    try:
        telemetry_window = telemetry_db.summary(days=float(days))
    except Exception:
        telemetry_window = {"ok": False, "days": float(days)}
    try:
        validation = telemetry_db.live_validation_summary()
    except Exception:
        validation = {"ok": False}

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "service": {
            "name": PATCH_BUILD.get("system_name"),
            "role": PATCH_BUILD.get("service_role"),
            "env_name": PATCH_BUILD.get("env_name"),
            "release_stage": PATCH_BUILD.get("release_stage_configured"),
        },
        "window_days": float(days),
        "account": {
            "cash_usd": account_truth.get("cash_usd"),
            "equity_usd": account_truth.get("equity_usd"),
            "balance_ok": bool(account_truth.get("balance_ok")),
        },
        "open_positions": {
            "count": len(positions),
            "total_open_notional_usd": open_notional,
            "positions": positions,
        },
        "pnl": {
            "realized_today_usd": realized_today,
            "journal_1d": journal_1d,
            "journal_7d": journal_7d,
            "journal_window": journal_window,
            "telemetry_window": telemetry_window,
        },
        "recent_trades": {
            "count": len(recent),
            "trades": recent,
        },
        "live_validation": validation,
    }


@app.get("/dashboard")
def dashboard(recent_limit: int = 15):
    snapshot_utc = utc_now_iso()
    try:
        compatibility = _compatibility_snapshot()
    except Exception:
        compatibility = {"ok": False}
    try:
        gate = _pretrade_health_gate_summary(rerun_startup_check=False)
    except Exception:
        gate = {"ok": False}
    try:
        promotion = _live_promotion_guardrails_snapshot(compatibility=compatibility, gate=gate)
    except Exception:
        promotion = {"ok": False}

    perf = _performance_snapshot(days=30.0, recent_limit=recent_limit)

    open_plans = []
    try:
        from dataclasses import asdict, is_dataclass
        for p in getattr(state, "plans", {}).values():
            open_plans.append(asdict(p) if is_dataclass(p) else p)
    except Exception:
        open_plans = []

    return {
        "ok": True,
        "utc": snapshot_utc,
        "snapshot_utc": snapshot_utc,
        "build": PATCH_BUILD,
        "service": perf.get("service") or {},
        "ready": bool((promotion or {}).get("checks", {}).get("readiness_green")),
        "promotion_ready": bool((promotion or {}).get("promotion_ready")),
        "compatibility": compatibility,
        "pretrade_health_gate": gate,
        "promotion_guardrails": promotion,
        "performance": perf,
        "open_plans": open_plans,
        "snapshot_consistency": {
            "ok": True,
            "shared_snapshot_inputs": ["compatibility", "pretrade_health_gate", "promotion_guardrails"],
            "note": "dashboard now reuses a single fresh compatibility/pretrade snapshot for promotion readiness fields",
        },
    }


@app.get("/performance")
def performance(days: float = 30.0, recent_limit: int = 25):
    return _performance_snapshot(days=days, recent_limit=recent_limit)


@app.get("/diagnostics/recent_trades")
def diagnostics_recent_trades(limit: int = 25):
    snapshot = _performance_snapshot(days=30.0, recent_limit=limit)
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "build": PATCH_BUILD,
        "recent_trades": snapshot.get("recent_trades") or {"count": 0, "trades": []},
    }
