from __future__ import annotations
import json, os, time
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
    _CT = ZoneInfo("America/Chicago")
except Exception:
    _CT = None

_CFG_DIR = Path(os.environ.get("POLICY_CFG_DIR", "policy_config"))
_WINDOWS_PATH  = _CFG_DIR / "windows.json"
_WHITELIST_PATH = _CFG_DIR / "whitelist.json"
_RISK_PATH     = _CFG_DIR / "risk.json"

@dataclass
class _Cfg:
    windows: Dict[str, Any]
    whitelist: Dict[str, Any]
    risk: Dict[str, Any]
    mtimes: Dict[str, float]

_cfg: Optional[_Cfg] = None
_symbol_locks: Dict[str, Dict[str, Any]] = {}
_symbol_cooldowns: Dict[str, float] = {}

def _load_json(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}

def _reload_if_needed():
    global _cfg
    paths = {str(_WINDOWS_PATH): _WINDOWS_PATH, str(_WHITELIST_PATH): _WHITELIST_PATH, str(_RISK_PATH): _RISK_PATH}
    mt = {k: (v.stat().st_mtime if v.exists() else 0.0) for k, v in paths.items()}
    if _cfg is None or any(_cfg.mtimes.get(k, -1) != mt[k] for k in mt):
        _cfg = _Cfg(
            windows=_load_json(_WINDOWS_PATH),
            whitelist=_load_json(_WHITELIST_PATH),
            risk=_load_json(_RISK_PATH),
            mtimes=mt
        )

def _now_ct(ts: Optional[float] = None) -> datetime:
    dt = datetime.utcfromtimestamp(ts or time.time())
    try:
        if _CT:
            return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(_CT)  # type: ignore
    except Exception:
        pass
    return dt

def _dow_short(dt: datetime) -> str:
    try:
        return dt.strftime("%a")
    except Exception:
        return "Mon"

def _in_window(strategy: str, dt: datetime) -> bool:
    win = (_cfg.windows or {}).get("windows", {}).get(strategy, {})
    hours = set(win.get("hours", []))
    dows  = set(win.get("dows", []))
    return (dt.hour in hours) and ((_dow_short(dt) in dows) if dows else True)

def _whitelisted(strategy: str, symbol: str) -> bool:
    wl = (_cfg.whitelist or {}).get(strategy, [])
    return bool(wl) and (symbol.upper() in {x.upper() for x in wl})

def _atr_floor(symbol: str) -> float:
    tiers = (_cfg.risk or {}).get("tiers", {})
    floors = (_cfg.risk or {}).get("atr_floor_pct", {})
    s = symbol.upper()
    if s in {x.upper() for x in tiers.get("tier1", [])}: return float(floors.get("tier1", 0.6))
    if s in {x.upper() for x in tiers.get("tier2", [])}: return float(floors.get("tier2", 0.9))
    return float(floors.get("tier3", 1.2))

def _edge_ok(expected_move_pct: float) -> bool:
    fee = float((_cfg.risk or {}).get("fee_rate_pct", 0.26))
    multiple = float((_cfg.risk or {}).get("edge_multiple_vs_fee", 3.0))
    return expected_move_pct >= (multiple * fee)

def _avoid(symbol: str) -> bool:
    avoid = {x.upper() for x in (_cfg.risk or {}).get("avoid_pairs", [])}
    return symbol.upper() in avoid

def _mutex_allows(symbol: str, strategy: str, now_ts: Optional[float] = None):
    now_ts = now_ts or time.time()
    cd_until = _symbol_cooldowns.get(symbol.upper(), 0.0)
    if cd_until and cd_until > now_ts:
        return False, f"cooldown_active_until={int(cd_until)}"
    lock = _symbol_locks.get(symbol.upper())
    if not lock or lock.get('until', 0) <= now_ts:
        return True, "free"
    if lock.get('owner') == strategy:
        return True, "owner_reentry"
    return False, f"locked_by={lock.get('owner')}_until={int(lock.get('until', 0))}"

def _claim(symbol: str, strategy: str, now_ts: Optional[float] = None):
    now_ts = now_ts or time.time()
    ttl_min = int((_cfg.risk or {}).get("symbol_mutex_minutes", 60))
    _symbol_locks[symbol.upper()] = {'until': now_ts + 60 * ttl_min, 'owner': strategy}

def _release(symbol: str, strategy: str, now_ts: Optional[float] = None):
    now_ts = now_ts or time.time()
    lock = _symbol_locks.get(symbol.upper())
    if lock and lock.get('owner') == strategy:
        _symbol_locks.pop(symbol.upper(), None)
    mr_list = set((_cfg.risk or {}).get("mr_strategies", []))
    if strategy in mr_list:
        cd_min = int((_cfg.risk or {}).get("cooldown_minutes_after_exit_for_mr", 30))
        _symbol_cooldowns[symbol.upper()] = now_ts + 60 * cd_min

def guard_allows(strategy: str, symbol: str, expected_move_pct: Optional[float], atr_pct: Optional[float], now_ts: Optional[float] = None):
    _reload_if_needed()
    s = (strategy or '').strip()
    sym = (symbol or '').strip().upper()
    if not s or not sym:
        return False, "missing_strategy_or_symbol"
    if _avoid(sym):
        return False, "avoid_pair"
    dt = _now_ct(now_ts)
    if not _in_window(s, dt):
        return False, f"outside_window hour={getattr(dt, 'hour', -1)} dow={_dow_short(dt)}"
    if not _whitelisted(s, sym):
        return False, "not_in_strategy_whitelist"
    if (atr_pct is not None) and (atr_pct < _atr_floor(sym)):
        return False, f"atr_below_floor floor={_atr_floor(sym)}"
    if (expected_move_pct is not None) and (not _edge_ok(expected_move_pct)):
        return False, "edge_lt_fee_multiple"
    ok, r = _mutex_allows(sym, s, now_ts=now_ts)
    if not ok:
        return False, f"mutex_block:{r}"
    return True, "ok"

def note_trade_event(event: str, strategy: str, symbol: str, now_ts: Optional[float] = None):
    _reload_if_needed()
    if event == "claim":
        _claim(symbol, strategy, now_ts=now_ts)
    elif event == "release":
        _release(symbol, strategy, now_ts=now_ts)
