# c5.py — header (drop-in)
from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

try:
    from policy.guard import guard_allows, note_trade_event
except Exception as e:
    logger.debug(f"[policy] guard import skipped: {e}")
    def guard_allows(*args, **kwargs): return True, "ok"
    def note_trade_event(*args, **kwargs): pass

import utils_volatility as uv
import vol_filter as vf  # if used

def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c5", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c5 blocked {symbol}: {reason}")
        return False
    return True

def policy_claim(symbol):
    try: note_trade_event("claim", strategy="c5", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c5 claim failed: {e}")

def policy_release(symbol):
    try: note_trade_event("release", strategy="c5", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c5 release failed: {e}")
