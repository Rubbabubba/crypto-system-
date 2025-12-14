from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

# Controls whether the strategy whitelist is a hard gate or advisory-only.
ENFORCE_STRAT_WHITELIST = str(
    os.getenv("ENFORCE_STRAT_WHITELIST", "true")
).lower() in ("1", "true", "yes", "on")


# ---------- Helpers ----------

def _norm_symbol(s: str) -> str:
    """Normalize symbols so BTC/USD, BTC-USD, btcusd -> BTCUSD."""
    return "".join(ch for ch in s.upper() if ch.isalnum())

def _norm_strategy(s: str) -> str:
    return s.strip().lower()

def _read_json(p: Path) -> Optional[dict]:
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _dow_str(dt: datetime) -> str:
    return ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][dt.weekday()]

@dataclass
class Policy:
    # strategy -> normalized whitelist symbols (or {"*"} for all)
    whitelist: Dict[str, Set[str]]
    # strategy -> window config dict
    windows: Dict[str, dict]

class _Cache:
    def __init__(self):
        self.lock = threading.Lock()
        self.loaded: Optional[Policy] = None
        self.dir: Optional[Path] = None
        self.mtimes: Dict[str, float] = {}

_CACHE = _Cache()

def load_policy(cfg_dir: Optional[str] = None) -> Policy:
    """
    Load whitelist and windows policy from cfg_dir.
    If files are missing/invalid, they are treated as permissive (fail-open).
    """
    cfg = Path(cfg_dir or os.getenv("POLICY_CFG_DIR", str(Path(__file__).parent / "policy_config")))

    wlist_path = cfg / "whitelist.json"
    win_path   = cfg / "windows.json"

    with _CACHE.lock:
        needs_reload = False
        if _CACHE.loaded is None or _CACHE.dir != cfg:
            needs_reload = True
        else:
            for p in (wlist_path, win_path):
                m = p.stat().st_mtime if p.exists() else -1.0
                if _CACHE.mtimes.get(str(p)) != m:
                    needs_reload = True
                    break

        if not needs_reload:
            return _CACHE.loaded  # type: ignore[return-value]

        # read files
        wlist_raw = _read_json(wlist_path) or {}
        wins_raw  = _read_json(win_path) or {}

        whitelist: Dict[str, Set[str]] = {}
        if isinstance(wlist_raw, dict):
            for strat, items in wlist_raw.items():
                s = _norm_strategy(strat)
                if not s:
                    continue

                if items == "*" or (isinstance(items, str) and items.strip() == "*"):
                    whitelist[s] = {"*"}
                else:
                    symbols: Set[str] = set()
                    if isinstance(items, (list, tuple)):
                        for x in items:
                            if isinstance(x, str):
                                symbols.add(_norm_symbol(x))
                    whitelist[s] = symbols

        windows: Dict[str, dict] = {}
        if isinstance(wins_raw, dict):
            for strat, cfg in wins_raw.items():
                if isinstance(cfg, dict):
                    windows[_norm_strategy(strat)] = cfg

        policy = Policy(whitelist=whitelist, windows=windows)

        # update cache
        _CACHE.loaded = policy
        _CACHE.dir = cfg
        for p in (wlist_path, win_path):
            _CACHE.mtimes[str(p)] = p.stat().st_mtime if p.exists() else -1.0

        return policy

def _in_window(now: datetime, win: dict) -> bool:
    """Check if now (UTC) is within the configured window dict."""
    if not win:
        return True

    # Allowed days, if provided
    days = win.get("days")
    if isinstance(days, list) and days:
        if _dow_str(now) not in set(d[:3].title() for d in days):
            return False

    # Explicit hour list e.g. [9,10,11]
    hours = win.get("hours")
    if isinstance(hours, list) and hours:
        try:
            allowed_hours = {int(h) for h in hours}
        except Exception:
            allowed_hours = set()
        if now.hour not in allowed_hours:
            return False

    # Range style hour_start / hour_end (inclusive start, exclusive end, overnight supported)
    hs = win.get("hour_start")
    he = win.get("hour_end")
    if hs is not None and he is not None:
        try:
            hs_i = int(hs)
            he_i = int(he)
            if hs_i == he_i:
                pass  # full-day (no restriction)
            elif hs_i < he_i:
                if not (hs_i <= now.hour < he_i):
                    return False
            else:
                # overnight wrap e.g. 20..6
                if not (now.hour >= hs_i or now.hour < he_i):
                    return False
        except Exception:
            pass

    return True

def guard_allows(strategy: str, symbol: str, now: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    Core policy guard for entries.

    Returns:
        (allowed: bool, reason: str)

    Semantics:
        - Whitelist can be HARD (blocks) or SOFT (advisory only) depending on
          ENFORCE_STRAT_WHITELIST.
        - Avoid list and windows ALWAYS hard-block.
    """
    policy = load_policy()
    s = _norm_strategy(strategy)
    sym = _norm_symbol(symbol)

    whitelist_reason = "ok"

    # --------------------
    # Strategy whitelist
    # --------------------
    if policy.whitelist:
        allowed_syms = policy.whitelist.get(s)
        in_whitelist = False

        if allowed_syms is not None:
            # '*' means "all symbols allowed" for that strategy
            if "*" in allowed_syms or sym in allowed_syms:
                in_whitelist = True

        if not in_whitelist:
            if ENFORCE_STRAT_WHITELIST:
                # Hard mode: block entries for non-whitelisted pairs.
                return False, "not_in_strategy_whitelist"
            else:
                # Soft/advisory mode: allow, but keep the reason so callers
                # can log a warning via telemetry.
                whitelist_reason = "not_in_strategy_whitelist"

    # --------------------
    # Hard blocks: avoid list
    # --------------------
    avoid_set, avoid_by_strategy = _load_avoid_set()
    if sym in avoid_set:
        return False, "in_avoid_pairs"

    # --------------------
    # Hard blocks: windows
    # --------------------
    win = policy.windows.get(s)
    if win:
        if now is None:
            now = datetime.now(timezone.utc)
        if not _is_now_within_window(now, win):
            return False, "outside_window"

    # If we reached here, symbol is allowed. If there was a soft whitelist
    # violation, surface that reason so callers can log it as a warning.
    if whitelist_reason != "ok":
        return True, whitelist_reason

    return True, "ok"

def filter_allowed_now(strategies: Iterable[str], symbols: Iterable[str], now: Optional[datetime] = None):
    """
    Convenience: return {strategy: [symbols...]} that are allowed *now*.
    """
    res: Dict[str, List[str]] = {}
    for strat in strategies:
        ok_syms: List[str] = []
        for sym in symbols:
            allowed, _ = guard_allows(strat, sym, now=now)
            if allowed:
                ok_syms.append(sym)
        res[strat] = ok_syms
    return res


# ---------- Risk config loader ----------

from functools import lru_cache

@lru_cache(maxsize=1)
def _load_avoid_set() -> tuple[set[str], dict[str, set[str]]]:
    """Load avoid-pairs configuration.

    Backwards compatible formats (in risk.json key: "avoid_pairs"):

    1) List[str] -> global avoid list
       {"avoid_pairs": ["BTC/USD", "ETH/USD"]}

    2) Dict -> global + per-strategy
       {"avoid_pairs": {"global": [...], "by_strategy": {"c2": ["ADA/USD"]}}}

    3) List[dict] entries w/ optional strategy and until
       {"avoid_pairs": [{"symbol":"ADA/USD","strategy":"c2","until":"2025-12-14T20:00:00Z"}]}

    Any entry with an "until" timestamp in the past is ignored.
    """
    raw = (_RISK_CFG or {}).get("avoid_pairs", [])
    now = datetime.now(timezone.utc)

    global_set: set[str] = set()
    by_strategy: dict[str, set[str]] = {}

    def _is_active(until_val) -> bool:
        if not until_val:
            return True
        try:
            if isinstance(until_val, (int, float)):
                until_dt = datetime.fromtimestamp(float(until_val), tz=timezone.utc)
            else:
                s = str(until_val).strip()
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                until_dt = datetime.fromisoformat(s)
                if until_dt.tzinfo is None:
                    until_dt = until_dt.replace(tzinfo=timezone.utc)
                else:
                    until_dt = until_dt.astimezone(timezone.utc)
            return until_dt > now
        except Exception:
            # If we can't parse it, treat as active
            return True

    def _add(sym: str, strat: Optional[str] = None):
        n = _norm_symbol(sym)
        if strat and isinstance(strat, str) and strat.strip():
            k = strat.strip()
            by_strategy.setdefault(k, set()).add(n)
        else:
            global_set.add(n)

    if isinstance(raw, dict):
        for sym in raw.get("global", []) or []:
            _add(sym)
        bs = raw.get("by_strategy", {}) or {}
        if isinstance(bs, dict):
            for strat, syms in bs.items():
                for sym in (syms or []):
                    _add(sym, strat)
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                _add(item)
            elif isinstance(item, dict):
                sym = item.get("symbol") or item.get("sym") or item.get("pair")
                if not sym:
                    continue
                if not _is_active(item.get("until")):
                    continue
                _add(sym, item.get("strategy"))
    # else: unknown type -> ignore

    return global_set, by_strategy

def load_risk_config(cfg_dir: Optional[str] = None) -> dict:
    """
    Load risk.json from a config directory.

    Priority:
      1) RISK_CONFIG_JSON env (inline JSON string)
      2) RISK_CONFIG_PATH env (full path to a JSON file)
      3) POLICY_CFG_DIR env (directory containing risk.json)
      4) Default: <repo_root>/policy_config/risk.json

    Always returns a dict ({} on any error).
    """
    # 1) Inline JSON override
    raw_inline = os.getenv("RISK_CONFIG_JSON")
    if raw_inline:
        try:
            cfg = json.loads(raw_inline)
            if isinstance(cfg, dict):
                return cfg
        except Exception:
            # fall through to file-based options
            pass

    # 2) Full path override
    path_env = os.getenv("RISK_CONFIG_PATH")
    if path_env:
        rpath = Path(path_env)
    else:
        # 3) Directory override via arg / env
        if cfg_dir is not None:
            base = Path(cfg_dir)
        else:
            dir_env = os.getenv("POLICY_CFG_DIR")
            if dir_env:
                base = Path(dir_env)
            else:
                # 4) Default: repo_root/policy_config/risk.json
                # __file__ = <repo_root>/policy/guard.py
                # parent        -> policy/
                # parent.parent -> repo_root/
                base = Path(__file__).resolve().parent.parent / "policy_config"
        rpath = base / "risk.json"

    try:
        if not rpath.exists():
            return {}
        data = _read_json(rpath)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}