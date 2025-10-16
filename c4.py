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
    logger.debug(f"[c4] utils_volatility import skipped: {e}")
try:
    import vol_filter as vf
except Exception as e:
    logger.debug(f"[c4] vol_filter import skipped: {e}")

def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c4", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c4 blocked {symbol}: {reason}")
        return False
    return True

def policy_claim(symbol):
    try: note_trade_event("claim", strategy="c4", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c4 claim failed: {e}")

def policy_release(symbol):
    try: note_trade_event("release", strategy="c4", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c4 release failed: {e}")

def scan(*args, **kwargs):
    logger.info("[c4] scan() placeholder called; returning no signals.")
    return {
        "strategy": "c4",
        "ok": True,
        "signals": [],
        "message": "placeholder scan; no-op"
    }

# Strategy metadata for app schedulers that expect dict-like attributes
CONFIG = {"enabled": True, "name": "c4", "version": 1}
META = {"id": "c4", "display": "C4", "group": "default"}
