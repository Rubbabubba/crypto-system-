from policy.guard import guard_allows, note_trade_event
import logging
logger = logging.getLogger(__name__)

# strategies/c3.py
import os
try:
    import br_router as br
except Exception:
    from strategies import br_router as br
from strategies import utils_volatility as uv

STRAT = "c3"
VER   = os.getenv("C3_VER", "v2.0")

def _highest(xs, n): 
    return max(xs[-n:]) if len(xs)>=n else None
def _lowest(xs, n): 
    return min(xs[-n:]) if len(xs)>=n else None

def _pos_for(symbol: str):
    sym = symbol.replace("/", "")
    pos = br.list_positions() or []
    for p in pos:
        if p.get("symbol","").replace("/","") == sym:
            return float(p.get("qty",0.0) or 0.0), float(p.get("avg_entry_price",0.0) or 0.0)
    return 0.0, 0.0

def run_scan(symbols, timeframe, limit, notional, dry, raw):
    N_CH     = int(raw.get("ch_n", 55))
    BREAK_K  = float(raw.get("break_k", 1.0005))
    FAIL_K   = float(raw.get("fail_k", 0.997))
    MIN_ATR  = float(raw.get("min_atr", 0.0))

    for sym in symbols:
        bars = br.get_bars(sym, timeframe, limit) or []
        if len(bars) < max(60, N_CH):
            continue

        atr = uv.atr_from_bars(bars, 14)
        if atr < MIN_ATR:
            continue

        closes = [b["c"] for b in bars]
        last = closes[-1]
        ch_hi = _highest(closes, N_CH)
        ch_lo = _lowest(closes, N_CH)
        if ch_hi is None or ch_lo is None:
            continue

        have, avg = _pos_for(sym)
        symclean = sym.replace("/", "").lower()

        if have <= 1e-12 and last >= ch_hi * BREAK_K:
            cid = f"{STRAT}-{VER}-buy-{symclean}"
            br.place_order(sym, "buy", notional, cid)
            return

        if have > 1e-12 and last <= ch_lo * FAIL_K:
            cid = f"{STRAT}-{VER}-sell-{symclean}"
            br.place_order(sym, "sell", notional, cid)
            return


def guarded_place(symbol, expected_move_pct=None, atr_pct=None):
    ok, reason = guard_allows(strategy="c3", symbol=symbol,
                              expected_move_pct=expected_move_pct, atr_pct=atr_pct)
    if not ok:
        logger.info(f"[guard] c3 blocked {symbol}: {reason}")
        return False
    return True

def policy_claim(symbol):
    try:
        note_trade_event("claim", strategy="c3", symbol=symbol)
    except Exception as e:
        logger.debug(f"[policy] c3 claim failed: {e}")

def policy_release(symbol):
    try:
        note_trade_event("release", strategy="c3", symbol=symbol)
    except Exception as e:
        logger.debug(f"[policy] c3 release failed: {e}")
