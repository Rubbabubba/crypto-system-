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
    logger.debug(f"[c6] utils_volatility import skipped: {e}")
try:
    import vol_filter as vf
except Exception as e:
    logger.debug(f"[c6] vol_filter import skipped: {e}")

CONFIG = {"enabled": True, "name": "c6", "version": 1}
META   = {"id": "c6", "display": "C6", "group": "default"}

def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c6", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c6 blocked {{symbol}}: {{reason}}")
        return False
    return True

def policy_claim(symbol):
    try: note_trade_event("claim", strategy="c6", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c6 claim failed: {e}")

def policy_release(symbol):
    try: note_trade_event("release", strategy="c6", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c6 release failed: {e}")

def scan(req: dict, ctx: dict):
    """Return a LIST of order intents.
    Placeholder returns [] so the scheduler won't error when iterating orders.
    """
    logger.info("[c6] scan() placeholder called; returning empty list.")
    return []
