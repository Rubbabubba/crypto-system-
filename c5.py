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
    logger.debug(f"[c5] utils_volatility import skipped: {e}")
try:
    import vol_filter as vf
except Exception as e:
    logger.debug(f"[c5] vol_filter import skipped: {e}")

CONFIG = {"enabled": True, "name": "c5", "version": 1}
META   = {"id": "c5", "display": "C5", "group": "default"}

def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c5", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c5 blocked {{symbol}}: {{reason}}")
        return False
    return True

def policy_claim(symbol):
    try: note_trade_event("claim", strategy="c5", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c5 claim failed: {e}")

def policy_release(symbol):
    try: note_trade_event("release", strategy="c5", symbol=symbol)
    except Exception as e: logger.debug(f"[policy] c5 release failed: {e}")

def scan(req: dict, ctx: dict):
    """Return a LIST of order intents.
    Placeholder returns [] so the scheduler won't error when iterating orders.
    """
    logger.info("[c5] scan() placeholder called; returning empty list.")
    return []
