from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

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
    Return (allowed, reason). reason is 'ok' if allowed, otherwise a short block reason.
    """
    policy = load_policy()
    s = _norm_strategy(strategy)
    sym = _norm_symbol(symbol)

    # Whitelist
    if policy.whitelist:
        allowed_syms = policy.whitelist.get(s)
        if allowed_syms is None:
            return False, "not_in_strategy_whitelist"
        if "*" not in allowed_syms and sym not in allowed_syms:
            return False, "not_in_strategy_whitelist"

    # Windows
    win = policy.windows.get(s)
    if win:
        t = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if not _in_window(t, win):
            return False, f"outside_window hour={t.hour} dow={_dow_str(t)}"

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
