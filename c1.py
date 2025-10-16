from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

try:
    from policy.guard import guard_allows, note_trade_event
except Exception as e:
    logger.debug(f"[policy] guard import skipped: {e}")
    def guard_allows(*args, **kwargs): return (True, "ok")
    def note_trade_event(*args, **kwargs): pass

try:
    import utils_volatility as uv
except Exception as e:
    logger.debug(f"[c1] utils_volatility import skipped: {e}")
try:
    import vol_filter as vf
except Exception as e:
    logger.debug(f"[c1] vol_filter import skipped: {e}")

def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c1", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c1 blocked {symbol}: {reason}")
        return False
    return True

def policy_claim(symbol):
    try: note_trade_event("claim", strategy="c1", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c1 claim failed: {e}")

def policy_release(symbol):
    try: note_trade_event("release", strategy="c1", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c1 release failed: {e}")

def scan(*args, **kwargs):
    logger.info("[c1] scan() placeholder called; returning no signals.")
    return {
        "strategy": "c1",
        "ok": True,
        "signals": [],
        "message": "placeholder scan; no-op"
    }

# Strategy metadata for app schedulers that expect dict-like attributes
CONFIG = {"enabled": True, "name": "c1", "version": 1}
META = {"id": "c1", "display": "C1", "group": "default"}
