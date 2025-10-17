from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

# Policy guard (module lives in the 'policy' package)
try:
    from policy.guard import guard_allows, note_trade_event
except Exception as e:
    logger.debug("[policy] guard import skipped: %s", e)
    def guard_allows(*args, **kwargs): return (True, "ok")  # permissive if policy not present
    def note_trade_event(*args, **kwargs): pass

# Broker + engine
import pandas as pd
import broker_kraken as br
from book import StrategyBook, ScanRequest

STRAT_ID = "c5"

def _df(rows):
    if not rows:
        return None
    df = pd.DataFrame(rows)
    # Normalize columns expected by StrategyBook
    return df.rename(columns={"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"})

def _ctx_for(sym: str, tf: str, limit: int) -> dict | None:
    try:
        one = br.get_bars(sym, timeframe="1Min", limit=max(300, limit*5))
        five = br.get_bars(sym, timeframe=tf,      limit=limit)
        d1 = _df(one); d5 = _df(five)
        if d1 is None or d5 is None:
            return None
        return {
            "one":  d1.to_dict(orient="list"),
            "five": d5.to_dict(orient="list"),
        }
    except Exception as e:
        logger.warning("[%s] failed to build context for %s: %s", STRAT_ID, sym, e)
        return None

def scan(req: dict, ctx: dict):
    """Return a LIST of actionable order intents:
      [{"symbol": "BTCUSD", "side": "buy"|"sell", "notional": 25.0}, ...]
    The app will place these as market notional orders when TRADING is enabled.
    """
    try:
        tf      = str(req.get("timeframe") or ctx.get("timeframe") or "5Min")
        limit   = int(req.get("limit") or ctx.get("limit") or 300)
        syms    = [s.upper() for s in (req.get("symbols") or ctx.get("symbols") or [])]
        notional= float(req.get("notional") or ctx.get("notional") or 25.0)

        # StrategyBook parameters can be tuned per‑strategy if desired
        book = StrategyBook(topk=2, min_score=0.10, risk_target_usd=notional, atr_stop_mult=1.0)
        sreq = ScanRequest(strat=STRAT_ID, timeframe=tf, limit=limit, topk=2, min_score=0.10, notional=notional)

        # Build bar contexts for all symbols
        contexts = { sym: _ctx_for(sym, tf, limit) for sym in syms }

        results = book.scan(sreq, contexts) or []
        intents = []
        for r in results:
            if not r.selected or r.action not in ("buy","sell") or (r.notional or 0) <= 0:
                continue

            # Lightweight guard: use ATR% as a proxy for expected move
            expected_move_pct = float(r.atr_pct or 0.0)
            ok, reason = guard_allows(strategy=STRAT_ID, symbol=r.symbol,
                                      expected_move_pct=expected_move_pct, atr_pct=float(r.atr_pct or 0.0))
            if not ok:
                logger.info("[guard] %s blocked %s: %s", STRAT_ID, r.symbol, reason)
                continue

            intents.append({
                "symbol":   r.symbol,
                "side":     "buy" if r.action == "buy" else "sell",
                "notional": min(notional, float(r.notional or notional))
            })
        return intents
    except Exception as e:
        logger.exception("[%s] scan() error: %s", STRAT_ID, e)
        return []
