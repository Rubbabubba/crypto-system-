# guard.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Any, Tuple
from datetime import datetime
import pytz

POLICY_CFG_DIR = Path(os.getenv("POLICY_CFG_DIR", Path(__file__).parent / "policy_config")).resolve()

_WHITELIST_PATH = POLICY_CFG_DIR / "whitelist.json"
_WINDOWS_PATH = POLICY_CFG_DIR / "windows.json"

_policy: Dict[str, Any] | None = None

def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def load_policy() -> Dict[str, Any]:
    """
    Returns a dict with keys:
      - whitelist: {strategy: [symbols...]}
      - windows: {strategy: {days: [...], hours: [...]} or {hour_start: int, hour_end: int, days: [...]}}
    Missing files are treated as empty (allow-all).
    """
    global _policy
    if _policy is not None:
        return _policy

    whitelist = _load_json(_WHITELIST_PATH)
    windows = _load_json(_WINDOWS_PATH)

    _policy = {"whitelist": whitelist or {}, "windows": windows or {}}
    return _policy

def _now_utc(dt: datetime | None = None) -> datetime:
    if dt is None:
        return datetime.now(tz=pytz.UTC)
    return dt if dt.tzinfo else pytz.UTC.localize(dt)

def _dow_str(dt: datetime) -> str:
    return dt.strftime("%a")  # Mon, Tue, ...

def _hour(dt: datetime) -> int:
    return int(dt.astimezone(pytz.UTC).strftime("%H"))

def guard_allows(strategy: str, symbol: str, now: datetime | None = None) -> Tuple[bool, str]:
    """
    Returns (allowed, reason). If configs are empty or missing, returns (True, "no_policy").
    - Checks whitelist per-strategy. If whitelist[strategy] exists and symbol not in it -> block.
    - Checks time windows per-strategy in UTC.
      Supported formats:
        windows[strategy] = {
          "days": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],   # optional
          "hours": [9,10,...,16]                                 # OR
          "hour_start": 9, "hour_end": 16                        # inclusive start, exclusive end
        }
    """
    cfg = load_policy()
    wl = cfg.get("whitelist", {})
    win = cfg.get("windows", {})

    # If both are empty, allow by default
    if not wl and not win:
        return True, "no_policy"

    s = strategy
    sym = symbol.replace("/", "").replace("-", "")
    # Try both 'BTCUSD' and 'BTC/USD' styles in lists
    wl_list = wl.get(s) or wl.get(s.lower()) or wl.get(s.upper())
    if isinstance(wl_list, list) and wl_list:
        norm_list = [x.replace("/", "").replace("-", "") for x in wl_list]
        if sym not in norm_list:
            return False, "not_in_strategy_whitelist"

    w = win.get(s) or win.get(s.lower()) or win.get(s.upper()) or {}
    if isinstance(w, dict) and w:
        dt = _now_utc(now)
        dow = _dow_str(dt)
        hr = _hour(dt)

        days = w.get("days")  # e.g., ["Mon","Tue","Wed","Thu","Fri"]
        if isinstance(days, list) and days:
            if dow not in days:
                return False, f"outside_window dow={dow}"

        hours = w.get("hours")
        if isinstance(hours, list) and hours:
            if hr not in hours:
                return False, f"outside_window hour={hr}"

        hs = w.get("hour_start")
        he = w.get("hour_end")
        if isinstance(hs, int) and isinstance(he, int):
            # treat as [hs, he) in UTC
            if hs <= he:
                ok = (hs <= hr < he)
            else:
                # overnight window, e.g., 20..4
                ok = (hr >= hs or hr < he)
            if not ok:
                return False, f"outside_window hour={hr}"

    return True, "ok"
