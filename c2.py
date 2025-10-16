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
    logger.debug(f"[c2] utils_volatility import skipped: {e}")
try:
    import vol_filter as vf
except Exception as e:
    logger.debug(f"[c2] vol_filter import skipped: {e}")

def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c2", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c2 blocked {symbol}: {reason}")
        return False
    return True

def policy_claim(symbol):
    try: note_trade_event("claim", strategy="c2", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c2 claim failed: {e}")

def policy_release(symbol):
    try: note_trade_event("release", strategy="c2", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c2 release failed: {e}")

def scan(*args, **kwargs):
    logger.info("[c2] scan() placeholder called; returning no signals.")
    return {
        "strategy": "c2",
        "ok": True,
        "signals": [],
        "message": "placeholder scan; no-op"
    }
