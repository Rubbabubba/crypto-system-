from __future__ import annotations
import logging
logger = logging.getLogger(__name__)

from guard import guard_allows, note_trade_event  # local guard module
import broker_kraken as br
import pandas as pd
from book import StrategyBook, ScanRequest

STRAT_ID = "c1"

def _to_df(rows):
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df.rename(columns={"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"}, inplace=True)
    return df

def _ctx_for(symbol: str, tf: str, limit: int):
    one = br.get_bars(symbol, timeframe="1Min", limit=max(300, limit*5))
    five = br.get_bars(symbol, timeframe=tf,      limit=limit)
    return {
        "one":  _to_df(one).to_dict(orient="list") if one else None,
        "five": _to_df(five).to_dict(orient="list") if five else None,
    }

def scan(req: dict, ctx: dict):
    """
    Build contexts, run StrategyBook for c1, filter with guard, and emit order intents.
    req: {timeframe, limit, symbols, notional}
    ctx: includes scheduler-level defaults (symbols, notional)
    """
    tf       = str(req.get("timeframe") or "5Min")
    limit    = int(req.get("limit") or 300)
    symbols  = [s.upper() for s in (req.get("symbols") or ctx.get("symbols") or [])]
    notional = float(req.get("notional") or ctx.get("notional") or 25.0)

    book = StrategyBook(topk=2, min_score=0.10, risk_target_usd=notional, atr_stop_mult=1.0)
    sreq = ScanRequest(strat=STRAT_ID, timeframe=tf, limit=limit, topk=2, min_score=0.10, notional=notional)

    contexts = { sym: _ctx_for(sym, tf, limit) for sym in symbols }
    results = book.scan(sreq, contexts)

    intents = []
    for r in results:
        if not r.selected or r.action not in ("buy","sell") or r.notional <= 0:
            continue
        expected_move_pct = r.atr_pct  # simple proxy; replace with model score later
        ok, reason = guard_allows(strategy=STRAT_ID, symbol=r.symbol, expected_move_pct=expected_move_pct, atr_pct=r.atr_pct)
        if not ok:
            logger.info("[guard] %s blocked %s: %s", STRAT_ID, r.symbol, reason)
            continue
        intents.append({
            "symbol":  r.symbol,
            "side":    "buy" if r.action == "buy" else "sell",
            "notional": min(notional, r.notional)
        })
    return intents
